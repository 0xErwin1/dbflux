use crate::DbError;
use crate::connection::hook::ScriptLanguage;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// An entry in the scripts directory tree.
#[derive(Debug, Clone)]
pub enum ScriptEntry {
    File {
        path: PathBuf,
        name: String,
        extension: String,
    },
    Folder {
        path: PathBuf,
        name: String,
        children: Vec<ScriptEntry>,
    },
}

impl ScriptEntry {
    pub fn path(&self) -> &Path {
        match self {
            ScriptEntry::File { path, .. } | ScriptEntry::Folder { path, .. } => path,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            ScriptEntry::File { name, .. } | ScriptEntry::Folder { name, .. } => name,
        }
    }

    pub fn is_folder(&self) -> bool {
        matches!(self, ScriptEntry::Folder { .. })
    }
}

/// A folder outside the managed scripts root whose scripts DBFlux lists and
/// edits in place, without copying them.
///
/// `path` is the canonical path the folder had when it was registered, so two
/// registrations of the same folder through different spellings compare equal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalScriptRoot {
    pub id: Uuid,
    pub path: PathBuf,
    pub label: String,
}

impl ExternalScriptRoot {
    /// Builds a root for `path` with a fresh id, labelled after the folder name.
    pub fn new(path: PathBuf) -> Self {
        let label = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| path.display().to_string());

        Self {
            id: Uuid::new_v4(),
            path,
            label,
        }
    }
}

/// Whether the folder behind an external root could be listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptRootAvailability {
    /// Registered but not scanned yet.
    Pending,
    Available,
    /// The folder could not be read: moved, deleted, unmounted or denied.
    /// The registration is kept so the root comes back once the folder does.
    Unavailable {
        reason: String,
    },
}

/// An external root together with the result of its last scan.
#[derive(Debug, Clone)]
pub struct MountedScriptRoot {
    root: ExternalScriptRoot,
    entries: Vec<ScriptEntry>,
    availability: ScriptRootAvailability,
    /// Bumped by every change made through [`ScriptsDirectory`], so a scan
    /// requested before the change is recognized as stale and dropped.
    generation: u64,
}

impl MountedScriptRoot {
    fn pending(root: ExternalScriptRoot) -> Self {
        Self {
            root,
            entries: Vec::new(),
            availability: ScriptRootAvailability::Pending,
            generation: 0,
        }
    }

    fn adopt(&mut self, scanned: Result<Vec<ScriptEntry>, String>) {
        match scanned {
            Ok(entries) => {
                self.entries = entries;
                self.availability = ScriptRootAvailability::Available;
            }
            Err(reason) => {
                self.entries = Vec::new();
                self.availability = ScriptRootAvailability::Unavailable { reason };
            }
        }
    }

    pub fn root(&self) -> &ExternalScriptRoot {
        &self.root
    }

    pub fn id(&self) -> Uuid {
        self.root.id
    }

    pub fn path(&self) -> &Path {
        &self.root.path
    }

    pub fn label(&self) -> &str {
        &self.root.label
    }

    pub fn entries(&self) -> &[ScriptEntry] {
        &self.entries
    }

    pub fn availability(&self) -> &ScriptRootAvailability {
        &self.availability
    }
}

/// The roots to scan, detached from [`ScriptsDirectory`] so the walk can run
/// on a background thread. Each root carries the generation it had when the
/// request was taken.
#[derive(Debug, Clone)]
pub struct ScriptsScanRequest {
    managed: Option<(PathBuf, u64)>,
    external: Vec<(Uuid, PathBuf, u64)>,
}

impl ScriptsScanRequest {
    /// Walks the requested roots. Blocks for as long as the slowest folder
    /// takes to answer, so call it off the thread that renders.
    pub fn run(self) -> ScriptsScan {
        let managed = self.managed.map(|(root, generation)| {
            let entries = scan_directory(&root);
            (root, generation, entries)
        });

        let external = self
            .external
            .into_iter()
            .map(|(id, path, generation)| {
                let scanned =
                    scan_tree(&path, ScanMode::External).map_err(|error| error.to_string());
                ExternalScan {
                    id,
                    path,
                    generation,
                    scanned,
                }
            })
            .collect();

        ScriptsScan { managed, external }
    }
}

/// The outcome of a [`ScriptsScanRequest`], applied with
/// [`ScriptsDirectory::adopt_full_scan`].
#[derive(Debug, Clone)]
pub struct ScriptsScan {
    managed: Option<(PathBuf, u64, Vec<ScriptEntry>)>,
    external: Vec<ExternalScan>,
}

/// One external root's part of a [`ScriptsScan`]: the error text when the
/// folder could not be read.
#[derive(Debug, Clone)]
struct ExternalScan {
    id: Uuid,
    path: PathBuf,
    generation: u64,
    scanned: Result<Vec<ScriptEntry>, String>,
}

/// Manages the scripts the sidebar lists.
///
/// The managed root at `~/.local/share/dbflux/scripts/` belongs to DBFlux: new
/// queries and hook scripts are created there. External roots are folders the
/// user registered; their files are listed and edited where they are and are
/// never copied. Every filesystem operation is confined to one root: a path
/// outside all of them, or an operation that would cross from one root into
/// another, is refused. Inside an external root the check is made on the
/// resolved path, so a symlink leading out of the folder is neither listed nor
/// written through.
///
/// The roots never overlap: an external root cannot sit inside the managed root
/// or another external root, nor contain one. That keeps "which root owns this
/// path" a single answer.
///
/// Changes made through this type update the cached trees in memory and never
/// walk the disk, so they are safe to call from the UI thread whatever the
/// folder sits on. Walking happens only through [`ScriptsScanRequest`], whose
/// result is dropped for any root changed since the request was taken.
pub struct ScriptsDirectory {
    root: PathBuf,
    entries: Vec<ScriptEntry>,
    managed_generation: u64,
    external: Vec<MountedScriptRoot>,
}

/// The cached tree of one root, borrowed for an in-memory edit.
struct RootTree<'a> {
    root: &'a Path,
    mode: ScanMode,
    entries: &'a mut Vec<ScriptEntry>,
    generation: &'a mut u64,
}

impl ScriptsDirectory {
    pub fn new() -> Result<Self, DbError> {
        let data_dir = dirs::data_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find data directory"))
        })?;

        let root = data_dir.join("dbflux").join("scripts");
        fs::create_dir_all(&root).map_err(DbError::IoError)?;

        // External roots are stored canonical; the managed root must be too, or
        // the overlap checks miss a data directory reached through a symlink.
        let root = fs::canonicalize(&root).unwrap_or(root);
        let entries = scan_directory(&root);

        Ok(Self {
            root,
            entries,
            managed_generation: 0,
            external: Vec::new(),
        })
    }

    /// The managed root, where DBFlux creates its own scripts.
    pub fn root_path(&self) -> &Path {
        &self.root
    }

    /// The entries of the managed root.
    pub fn entries(&self) -> &[ScriptEntry] {
        &self.entries
    }

    /// The registered external roots, in registration order.
    pub fn external_roots(&self) -> &[MountedScriptRoot] {
        &self.external
    }

    pub fn external_root(&self, id: Uuid) -> Option<&MountedScriptRoot> {
        self.external.iter().find(|mounted| mounted.id() == id)
    }

    /// The external root registered at exactly `path`.
    pub fn external_root_at(&self, path: &Path) -> Option<&MountedScriptRoot> {
        self.external.iter().find(|mounted| mounted.path() == path)
    }

    pub fn hooks_directory(&self) -> Result<PathBuf, DbError> {
        let hooks_dir = self.root.join("hooks");

        if !hooks_dir.exists() {
            fs::create_dir_all(&hooks_dir).map_err(DbError::IoError)?;
        }

        Ok(hooks_dir)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Registers external roots without scanning them.
    ///
    /// They stay [`ScriptRootAvailability::Pending`] until a scan is adopted, so
    /// startup never waits on a slow or unmounted folder. Replaces any roots
    /// registered before. A root that overlaps the managed root or one listed
    /// before it is logged and skipped, so one path never has two owners.
    pub fn register_external_roots(&mut self, roots: Vec<ExternalScriptRoot>) {
        self.external.clear();

        for root in roots {
            match self.ensure_no_overlap(&root.path) {
                Ok(()) => self.external.push(MountedScriptRoot::pending(root)),
                Err(error) => log::warn!(
                    "Skipping external scripts folder {}: {error}",
                    root.path.display()
                ),
            }
        }
    }

    /// Resolves `path` to the canonical folder an external root would use, and
    /// refuses a folder that cannot be one.
    ///
    /// Touches the filesystem (canonicalization), so a caller on the UI thread
    /// should run it in the background.
    pub fn validate_external_root(&self, path: &Path) -> Result<PathBuf, DbError> {
        Self::validate_external_root_against(&self.root, &self.external_paths(), path)
    }

    /// The pure form of [`Self::validate_external_root`], for callers that only
    /// hold the root paths (for example on a background thread).
    pub fn validate_external_root_against(
        managed_root: &Path,
        external_roots: &[PathBuf],
        path: &Path,
    ) -> Result<PathBuf, DbError> {
        let canonical = fs::canonicalize(path).map_err(DbError::IoError)?;

        if !canonical.is_dir() {
            return Err(io_error(format!("Not a folder: {}", canonical.display())));
        }

        let managed = fs::canonicalize(managed_root).unwrap_or_else(|_| managed_root.to_path_buf());

        if overlaps(&canonical, &managed) {
            return Err(io_error(format!(
                "{} overlaps the DBFlux scripts folder",
                canonical.display()
            )));
        }

        if let Some(existing) = external_roots
            .iter()
            .find(|existing| overlaps(&canonical, existing))
        {
            return Err(io_error(format!(
                "{} overlaps the external folder {}",
                canonical.display(),
                existing.display()
            )));
        }

        Ok(canonical)
    }

    /// Registers an external root and scans it synchronously.
    ///
    /// The root's path must already be canonical, as
    /// [`Self::validate_external_root`] returns it; the overlap checks run again
    /// here against the current registrations. The scan blocks; the app uses
    /// [`Self::add_external_root_pending`] and scans in the background.
    pub fn add_external_root(&mut self, root: ExternalScriptRoot) -> Result<(), DbError> {
        self.ensure_no_overlap(&root.path)?;

        let mut mounted = MountedScriptRoot::pending(root);
        mounted.adopt(scan_tree(mounted.path(), ScanMode::External).map_err(|e| e.to_string()));
        self.external.push(mounted);

        Ok(())
    }

    /// Registers an external root without scanning it, for callers that scan in
    /// the background afterwards.
    pub fn add_external_root_pending(&mut self, root: ExternalScriptRoot) -> Result<(), DbError> {
        self.ensure_no_overlap(&root.path)?;
        self.external.push(MountedScriptRoot::pending(root));
        Ok(())
    }

    /// Forgets an external root. The folder and its files are left untouched.
    pub fn remove_external_root(&mut self, id: Uuid) -> Option<ExternalScriptRoot> {
        let index = self
            .external
            .iter()
            .position(|mounted| mounted.id() == id)?;
        Some(self.external.remove(index).root)
    }

    fn external_paths(&self) -> Vec<PathBuf> {
        self.external
            .iter()
            .map(|mounted| mounted.path().to_path_buf())
            .collect()
    }

    fn ensure_no_overlap(&self, path: &Path) -> Result<(), DbError> {
        if overlaps(path, &self.root) {
            return Err(io_error(format!(
                "{} overlaps the DBFlux scripts folder",
                path.display()
            )));
        }

        if let Some(existing) = self
            .external
            .iter()
            .find(|mounted| overlaps(path, mounted.path()))
        {
            return Err(io_error(format!(
                "{} overlaps the external folder {}",
                path.display(),
                existing.path().display()
            )));
        }

        Ok(())
    }

    /// The root `path` belongs to by its spelling, or `None` when it is outside
    /// every root. Operations also check the resolved path inside external
    /// roots; see [`Self::ensure_contained`].
    pub fn owning_root(&self, path: &Path) -> Option<&Path> {
        if path.starts_with(&self.root) {
            return Some(&self.root);
        }

        self.external
            .iter()
            .map(MountedScriptRoot::path)
            .find(|root| path.starts_with(root))
    }

    /// Whether `path` is the managed root or an external root itself.
    pub fn is_root(&self, path: &Path) -> bool {
        path == self.root || self.external_root_at(path).is_some()
    }

    /// Whether every path in `sources` belongs to the same root as `target`,
    /// which is what a move needs. Decided from the paths alone, without
    /// touching the disk, so drag feedback can ask it on every pointer move.
    pub fn share_root(&self, sources: &[PathBuf], target: &Path) -> bool {
        let Some(target_root) = self.owning_root(target) else {
            return false;
        };

        sources
            .iter()
            .all(|source| self.owning_root(source) == Some(target_root))
    }

    fn require_owning_root(&self, path: &Path, what: &str) -> Result<PathBuf, DbError> {
        self.owning_root(path)
            .map(Path::to_path_buf)
            .ok_or_else(|| io_error(format!("{what} is outside the script folders")))
    }

    /// Refuses a directory of an external root that resolves outside it, such
    /// as a symlinked subfolder pointing elsewhere: writing there would leave
    /// the folder the user registered.
    ///
    /// The managed root keeps checking by spelling only, as it always has, so a
    /// symlink the user placed in DBFlux's own folder keeps working.
    fn ensure_contained(&self, root: &Path, dir: &Path) -> Result<(), DbError> {
        if root == self.root {
            return Ok(());
        }

        let resolved = fs::canonicalize(dir).map_err(DbError::IoError)?;
        if resolved.starts_with(root) {
            Ok(())
        } else {
            Err(io_error(format!(
                "{} resolves outside the scripts folder {}",
                dir.display(),
                root.display()
            )))
        }
    }

    /// Re-scan every root synchronously and update the cached trees.
    ///
    /// Blocks on every external folder; the app uses [`Self::scan_request`] off
    /// the UI thread instead.
    pub fn refresh(&mut self) {
        let scan = self.scan_request().run();
        self.adopt_full_scan(scan);
    }

    /// The roots a full scan would walk, detached so the walk can run on a
    /// background thread.
    pub fn scan_request(&self) -> ScriptsScanRequest {
        ScriptsScanRequest {
            managed: Some((self.root.clone(), self.managed_generation)),
            external: self
                .external
                .iter()
                .map(|mounted| {
                    (
                        mounted.id(),
                        mounted.path().to_path_buf(),
                        mounted.generation,
                    )
                })
                .collect(),
        }
    }

    /// A scan of only the root that owns `path`, or `None` when `path` is
    /// outside every root.
    pub fn scan_request_for(&self, path: &Path) -> Option<ScriptsScanRequest> {
        if path.starts_with(&self.root) {
            return Some(ScriptsScanRequest {
                managed: Some((self.root.clone(), self.managed_generation)),
                external: Vec::new(),
            });
        }

        let mounted = self
            .external
            .iter()
            .find(|mounted| path.starts_with(mounted.path()))?;

        Some(ScriptsScanRequest {
            managed: None,
            external: vec![(
                mounted.id(),
                mounted.path().to_path_buf(),
                mounted.generation,
            )],
        })
    }

    /// Applies a scan.
    ///
    /// A root is updated only while it is still registered at the same path and
    /// unchanged since the request was taken: a late scan never resurrects a
    /// removed root nor overwrites a change made after it started. A dropped
    /// root keeps its in-memory tree until the next scan.
    pub fn adopt_full_scan(&mut self, scan: ScriptsScan) {
        if let Some((root, generation, entries)) = scan.managed
            && root == self.root
            && generation == self.managed_generation
        {
            self.entries = entries;
        }

        for external in scan.external {
            if let Some(mounted) = self.external.iter_mut().find(|mounted| {
                mounted.id() == external.id
                    && mounted.path() == external.path
                    && mounted.generation == external.generation
            }) {
                mounted.adopt(external.scanned);
            }
        }
    }

    /// Removes `path` when it still holds exactly `expected_bytes`.
    ///
    /// The comparison and the removal happen in one step, so nothing can write
    /// into the file between them. `Ok(false)` means the file is gone or no longer
    /// holds those bytes — a foreign change, which is kept — and `Ok(true)` means
    /// it was removed. An error is reserved for a file that could not be read or
    /// removed; keeping a file deliberately is not an error.
    ///
    /// Pure filesystem work: it neither reads nor updates the cached tree, so the
    /// caller can run it off the UI thread and rescan afterwards.
    pub fn remove_if_unchanged(
        root: &Path,
        path: &Path,
        expected_bytes: &str,
    ) -> Result<bool, DbError> {
        Self::ensure_deletable(root, path)?;

        match fs::read_to_string(path) {
            Ok(on_disk) if on_disk == expected_bytes => {
                fs::remove_file(path).map_err(DbError::IoError)?;
                Ok(true)
            }
            Ok(_) => Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(DbError::IoError(e)),
        }
    }

    /// Refuses a path that is not a removable entry of `root`.
    fn ensure_deletable(root: &Path, path: &Path) -> Result<(), DbError> {
        if !path.starts_with(root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Path is outside scripts root",
            )));
        }

        if path == root {
            return Err(DbError::IoError(std::io::Error::other(
                "Cannot delete scripts root",
            )));
        }

        Ok(())
    }

    /// Returns the next available name like "Query 1", "Query 2", etc.
    /// that doesn't collide with existing files at the managed root.
    pub fn next_available_name(&self, prefix: &str, extension: &str) -> String {
        let existing: HashSet<String> = self
            .entries
            .iter()
            .filter_map(|entry| match entry {
                ScriptEntry::File { name, .. } => Some(name.to_lowercase()),
                _ => None,
            })
            .collect();

        for n in 1.. {
            let candidate = format!("{} {}.{}", prefix, n, extension);
            if !existing.contains(&candidate.to_lowercase()) {
                return format!("{} {}", prefix, n);
            }
        }

        unreachable!()
    }

    /// Create an empty script file. `parent` defaults to the managed root.
    /// Returns the full path of the created file.
    pub fn create_file(
        &mut self,
        parent: Option<&Path>,
        name: &str,
        extension: &str,
    ) -> Result<PathBuf, DbError> {
        let dir = parent.unwrap_or(&self.root).to_path_buf();
        let root = self.require_owning_root(&dir, "Target directory")?;
        self.ensure_contained(&root, &dir)?;

        let filename = if name.contains('.') {
            name.to_string()
        } else {
            format!("{}.{}", name, extension)
        };

        let path = dir.join(&filename);
        if path.exists() {
            return Err(io_error(format!("File already exists: {}", filename)));
        }

        fs::write(&path, "").map_err(DbError::IoError)?;
        self.record_insert(&path, false);
        Ok(path)
    }

    /// Create a subdirectory. `parent` defaults to the managed root.
    /// Returns the full path.
    pub fn create_folder(&mut self, parent: Option<&Path>, name: &str) -> Result<PathBuf, DbError> {
        let dir = parent.unwrap_or(&self.root).to_path_buf();
        let root = self.require_owning_root(&dir, "Target directory")?;
        self.ensure_contained(&root, &dir)?;

        let path = dir.join(name);
        if path.exists() {
            return Err(io_error(format!("Folder already exists: {}", name)));
        }

        fs::create_dir_all(&path).map_err(DbError::IoError)?;
        self.record_insert(&path, true);
        Ok(path)
    }

    /// Rename a file or folder. Returns the new path.
    ///
    /// A root itself cannot be renamed: renaming an external root would rename
    /// the user's folder, which is not DBFlux's to rename.
    pub fn rename(&mut self, old_path: &Path, new_name: &str) -> Result<PathBuf, DbError> {
        if new_name.contains('/') || new_name.contains('\\') || new_name.contains("..") {
            return Err(io_error(
                "Invalid name: must not contain path separators or '..'".to_string(),
            ));
        }

        let root = self.require_owning_root(old_path, "Path")?;

        if self.is_root(old_path) {
            return Err(io_error("Cannot rename a scripts root".to_string()));
        }

        let parent = old_path
            .parent()
            .ok_or_else(|| io_error("Cannot rename root".to_string()))?;
        self.ensure_contained(&root, parent)?;

        let new_path = parent.join(new_name);
        if new_path.exists() {
            return Err(io_error(format!("Already exists: {}", new_name)));
        }

        fs::rename(old_path, &new_path).map_err(DbError::IoError)?;
        self.record_move(old_path, &new_path);
        Ok(new_path)
    }

    /// Delete a file or folder (recursive for folders) inside any root.
    /// A root itself is never deleted; an external root is unregistered with
    /// [`Self::remove_external_root`] instead. A symlink is removed, never the
    /// entry it points at.
    pub fn delete(&mut self, path: &Path) -> Result<(), DbError> {
        let root = self.require_owning_root(path, "Path")?;
        Self::ensure_deletable(&root, path)?;

        if let Some(parent) = path.parent() {
            self.ensure_contained(&root, parent)?;
        }

        let is_link = fs::symlink_metadata(path)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false);

        if path.is_dir() && !is_link {
            fs::remove_dir_all(path).map_err(DbError::IoError)?;
        } else {
            remove_file_or_link(path).map_err(DbError::IoError)?;
        }

        self.record_remove(path);
        Ok(())
    }

    /// Move a file or folder to a different directory of the same root.
    /// Returns the new path of the moved entry.
    ///
    /// Moving between roots is refused: it would move a file out of the user's
    /// folder (or into it), and across filesystems `rename` cannot do it anyway.
    pub fn move_entry(&mut self, source: &Path, target_dir: &Path) -> Result<PathBuf, DbError> {
        let source_root = self.require_owning_root(source, "Source")?;
        let target_root = self.require_owning_root(target_dir, "Target")?;

        if source_root != target_root {
            return Err(io_error(
                "Cannot move between different script folders".to_string(),
            ));
        }

        if self.is_root(source) {
            return Err(io_error("Cannot move a scripts root".to_string()));
        }

        // Prevent moving a folder into itself or its descendants
        if source.is_dir() && target_dir.starts_with(source) {
            return Err(io_error("Cannot move a folder into itself".to_string()));
        }

        let file_name = source
            .file_name()
            .ok_or_else(|| io_error("Source has no file name".to_string()))?;

        let dest = target_dir.join(file_name);

        // Already in the target directory
        if source.parent() == Some(target_dir) {
            return Ok(source.to_path_buf());
        }

        if let Some(parent) = source.parent() {
            self.ensure_contained(&source_root, parent)?;
        }

        if dest.exists() {
            return Err(io_error(format!("Already exists: {}", dest.display())));
        }

        self.ensure_contained(&target_root, target_dir)?;
        fs::create_dir_all(target_dir).map_err(DbError::IoError)?;
        fs::rename(source, &dest).map_err(DbError::IoError)?;
        self.record_move(source, &dest);
        Ok(dest)
    }

    /// Copy an external file into a folder of any root (the managed root by
    /// default).
    pub fn import(&mut self, source: &Path, target_dir: Option<&Path>) -> Result<PathBuf, DbError> {
        let dir = target_dir.unwrap_or(&self.root).to_path_buf();
        let root = self.require_owning_root(&dir, "Target directory")?;
        self.ensure_contained(&root, &dir)?;

        let filename = source
            .file_name()
            .ok_or_else(|| io_error("Source has no filename".to_string()))?;

        let dest = dir.join(filename);
        if dest.exists() {
            return Err(io_error(format!(
                "File already exists: {}",
                filename.to_string_lossy()
            )));
        }

        fs::copy(source, &dest).map_err(DbError::IoError)?;
        self.record_insert(&dest, false);
        Ok(dest)
    }

    /// The cached tree of the root that owns `path`.
    fn tree_for_mut(&mut self, path: &Path) -> Option<RootTree<'_>> {
        if path.starts_with(&self.root) {
            return Some(RootTree {
                root: &self.root,
                mode: ScanMode::Managed,
                entries: &mut self.entries,
                generation: &mut self.managed_generation,
            });
        }

        self.external
            .iter_mut()
            .find(|mounted| path.starts_with(&mounted.root.path))
            .map(|mounted| RootTree {
                root: &mounted.root.path,
                mode: ScanMode::External,
                entries: &mut mounted.entries,
                generation: &mut mounted.generation,
            })
    }

    /// Adds a just-created entry to the cached tree, as a scan would list it.
    fn record_insert(&mut self, path: &Path, is_dir: bool) {
        let Some(tree) = self.tree_for_mut(path) else {
            return;
        };

        *tree.generation += 1;

        let entry = if is_dir {
            ScriptEntry::Folder {
                path: path.to_path_buf(),
                name: file_name_of(path),
                children: Vec::new(),
            }
        } else if tree.mode.lists_file(path) {
            file_entry(path.to_path_buf())
        } else {
            return;
        };

        insert_into_tree(tree.entries, tree.root, entry);
    }

    /// Drops a removed entry from the cached tree.
    fn record_remove(&mut self, path: &Path) {
        let Some(tree) = self.tree_for_mut(path) else {
            return;
        };

        *tree.generation += 1;
        remove_from_tree(tree.entries, tree.root, path);
    }

    /// Moves a cached entry, and everything under it, to `new_path` in the same
    /// root.
    fn record_move(&mut self, old_path: &Path, new_path: &Path) {
        let Some(tree) = self.tree_for_mut(old_path) else {
            return;
        };

        *tree.generation += 1;

        let Some(entry) = remove_from_tree(tree.entries, tree.root, old_path) else {
            return;
        };

        let moved = rebase_entry(entry, new_path.to_path_buf());
        if let ScriptEntry::File { path, .. } = &moved
            && !tree.mode.lists_file(path)
        {
            return;
        }

        insert_into_tree(tree.entries, tree.root, moved);
    }
}

fn io_error(message: String) -> DbError {
    DbError::IoError(std::io::Error::other(message))
}

/// Whether one of the two folders contains the other (or they are the same).
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// Removes a file, or a symlink whatever it points at. A symlink to a folder is
/// a directory entry on Windows, so it needs `remove_dir` there.
fn remove_file_or_link(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if cfg!(windows) && path.is_dir() => fs::remove_dir(path).or(Err(error)),
        Err(error) => Err(error),
    }
}

fn file_name_of(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default()
}

fn file_entry(path: PathBuf) -> ScriptEntry {
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    ScriptEntry::File {
        name: file_name_of(&path),
        path,
        extension,
    }
}

/// The children of the folder at `dir` in a tree rooted at `root`, or `None`
/// when the cached tree does not hold that folder.
fn children_mut<'a>(
    entries: &'a mut Vec<ScriptEntry>,
    root: &Path,
    dir: &Path,
) -> Option<&'a mut Vec<ScriptEntry>> {
    if dir == root {
        return Some(entries);
    }

    for entry in entries.iter_mut() {
        if let ScriptEntry::Folder { path, children, .. } = entry
            && dir.starts_with(path.as_path())
        {
            return children_mut(children, path, dir);
        }
    }

    None
}

/// Inserts `entry` under its parent folder in scan order (folders first, then
/// files, each by case-insensitive name), replacing an entry at the same path.
/// A parent missing from the cached tree is left for the next scan.
fn insert_into_tree(entries: &mut Vec<ScriptEntry>, root: &Path, entry: ScriptEntry) {
    let Some(parent) = entry.path().parent() else {
        return;
    };

    let Some(siblings) = children_mut(entries, root, parent) else {
        return;
    };

    siblings.retain(|sibling| sibling.path() != entry.path());

    let sort_key = |entry: &ScriptEntry| (!entry.is_folder(), entry.name().to_lowercase());
    let key = sort_key(&entry);
    let index = siblings
        .iter()
        .position(|sibling| sort_key(sibling) > key)
        .unwrap_or(siblings.len());

    siblings.insert(index, entry);
}

fn remove_from_tree(
    entries: &mut Vec<ScriptEntry>,
    root: &Path,
    path: &Path,
) -> Option<ScriptEntry> {
    let siblings = children_mut(entries, root, path.parent()?)?;
    let index = siblings.iter().position(|entry| entry.path() == path)?;
    Some(siblings.remove(index))
}

/// The same entry at `new_path`, with every descendant's path following it.
fn rebase_entry(entry: ScriptEntry, new_path: PathBuf) -> ScriptEntry {
    match entry {
        ScriptEntry::File { .. } => file_entry(new_path),
        ScriptEntry::Folder { children, .. } => {
            let children = children
                .into_iter()
                .map(|child| {
                    let child_path = new_path.join(file_name_of(child.path()));
                    rebase_entry(child, child_path)
                })
                .collect();

            ScriptEntry::Folder {
                name: file_name_of(&new_path),
                path: new_path,
                children,
            }
        }
    }
}

pub fn hook_script_path(hooks_dir: &Path, hook_id: &str, language: ScriptLanguage) -> PathBuf {
    hooks_dir.join(format!("{}.{}", hook_id, language.extension()))
}

/// Extensions openable in the code editor (recognized by `QueryLanguage::from_path`).
const OPENABLE_EXTENSIONS: &[&str] = &[
    "sql", "js", "mongodb", "redis", "red", "cypher", "cyp", "influxql", "flux", "cql", "lua",
    "py", "sh", "bash",
];

fn has_file_extension(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()).is_some()
}

/// How a root is walked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanMode {
    /// DBFlux's own folder: every file with an extension, symlinks followed as
    /// before external roots existed.
    Managed,
    /// A folder the user registered, often a repository full of unrelated
    /// files: only files the editor opens, folders that lead to one or are
    /// empty, and nothing reached through a symlink that leaves the folder.
    External,
}

impl ScanMode {
    fn lists_file(self, path: &Path) -> bool {
        has_file_extension(path) && (self == ScanMode::Managed || is_openable_script(path))
    }
}

/// Recursively scan the managed root, returning sorted entries (folders first,
/// then files). A root that cannot be read yields an empty tree.
fn scan_directory(dir: &Path) -> Vec<ScriptEntry> {
    scan_tree(dir, ScanMode::Managed).unwrap_or_else(|e| {
        log::warn!("Failed to read scripts directory {:?}: {}", dir, e);
        Vec::new()
    })
}

/// What every level of one walk shares.
struct ScanContext {
    mode: ScanMode,
    /// The resolved root, for the external containment check.
    root: PathBuf,
}

/// Recursively scan `root`, returning sorted entries (folders first, then
/// files).
///
/// Fails only when `root` itself cannot be read; an unreadable subfolder is
/// logged and listed empty. A folder that resolves to one of its own ancestors
/// is skipped, so a symlink cycle does not repeat the tree.
fn scan_tree(root: &Path, mode: ScanMode) -> Result<Vec<ScriptEntry>, std::io::Error> {
    let read_dir = fs::read_dir(root)?;
    let resolved_root = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());

    let context = ScanContext {
        mode,
        root: resolved_root.clone(),
    };
    let mut ancestors = vec![resolved_root];

    Ok(scan_entries(read_dir, &context, &mut ancestors).entries)
}

/// The entries of one folder, and whether it held anything visible at all
/// (before filtering), which decides if an empty result is pruned.
struct FolderScan {
    entries: Vec<ScriptEntry>,
    had_visible_entries: bool,
}

fn scan_folder(dir: &Path, context: &ScanContext, ancestors: &mut Vec<PathBuf>) -> FolderScan {
    match fs::read_dir(dir) {
        Ok(read_dir) => scan_entries(read_dir, context, ancestors),
        Err(e) => {
            log::warn!("Failed to read scripts directory {:?}: {}", dir, e);
            FolderScan {
                entries: Vec::new(),
                had_visible_entries: false,
            }
        }
    }
}

fn scan_entries(
    read_dir: fs::ReadDir,
    context: &ScanContext,
    ancestors: &mut Vec<PathBuf>,
) -> FolderScan {
    let mut folders = Vec::new();
    let mut files = Vec::new();
    let mut had_visible_entries = false;

    for entry in read_dir.flatten() {
        let path = entry.path();
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };

        // Skip hidden files/folders
        if name.starts_with('.') {
            continue;
        }

        had_visible_entries = true;

        let is_link = entry
            .file_type()
            .map(|file_type| file_type.is_symlink())
            .unwrap_or(false);

        if is_link && context.mode == ScanMode::External && !keeps_external_link(&path, context) {
            continue;
        }

        if path.is_dir() {
            let resolved = fs::canonicalize(&path).ok();

            if resolved
                .as_ref()
                .is_some_and(|resolved| ancestors.contains(resolved))
            {
                continue;
            }

            if let Some(resolved) = &resolved {
                ancestors.push(resolved.clone());
            }

            let scanned = scan_folder(&path, context, ancestors);

            if resolved.is_some() {
                ancestors.pop();
            }

            // An external folder whose content is all unrelated files is noise;
            // an empty one is kept, since the user may have just made it.
            if context.mode == ScanMode::External
                && scanned.entries.is_empty()
                && scanned.had_visible_entries
            {
                continue;
            }

            folders.push(ScriptEntry::Folder {
                path,
                name,
                children: scanned.entries,
            });
        } else if context.mode.lists_file(&path) {
            files.push(file_entry(path));
        }
    }

    folders.sort_by_key(|a| a.name().to_lowercase());
    files.sort_by_key(|a| a.name().to_lowercase());

    FolderScan {
        entries: folders.into_iter().chain(files).collect(),
        had_visible_entries,
    }
}

/// Whether a symlink inside an external root is listed: only one to a file
/// that stays inside the root. A link to a folder inside the root would repeat
/// a folder already listed by its real path, in an order that depends on the
/// directory listing; a link leading out of the root is outside it.
fn keeps_external_link(path: &Path, context: &ScanContext) -> bool {
    match fs::canonicalize(path) {
        Ok(target) => target.starts_with(&context.root) && !target.is_dir(),
        Err(_) => false,
    }
}

/// Collect all openable file extensions for use in file dialogs.
pub fn all_script_extensions() -> Vec<&'static str> {
    OPENABLE_EXTENSIONS.to_vec()
}

/// Returns `true` if the file extension is openable in the code editor.
pub fn is_openable_script(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| OPENABLE_EXTENSIONS.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Filter a tree of entries by name query (case-insensitive).
/// Keeps parent folders that have matching descendants.
pub fn filter_entries(entries: &[ScriptEntry], query: &str) -> Vec<ScriptEntry> {
    if query.is_empty() {
        return entries.to_vec();
    }

    let lower_query = query.to_lowercase();
    entries
        .iter()
        .filter_map(|entry| filter_entry(entry, &lower_query))
        .collect()
}

fn filter_entry(entry: &ScriptEntry, lower_query: &str) -> Option<ScriptEntry> {
    match entry {
        ScriptEntry::File { name, .. } => {
            if name.to_lowercase().contains(lower_query) {
                Some(entry.clone())
            } else {
                None
            }
        }
        ScriptEntry::Folder {
            path,
            name,
            children,
        } => {
            let filtered_children: Vec<ScriptEntry> = children
                .iter()
                .filter_map(|child| filter_entry(child, lower_query))
                .collect();

            // Keep folder if its name matches or it has matching descendants
            if name.to_lowercase().contains(lower_query) || !filtered_children.is_empty() {
                Some(ScriptEntry::Folder {
                    path: path.clone(),
                    name: name.clone(),
                    children: filtered_children,
                })
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn make_dir(root: &Path) -> ScriptsDirectory {
        ScriptsDirectory {
            root: root.to_path_buf(),
            entries: scan_directory(root),
            managed_generation: 0,
            external: Vec::new(),
        }
    }

    /// A managed root and an external folder in separate temp directories,
    /// with the external folder registered and scanned.
    fn with_external_root() -> (TempDir, TempDir, ScriptsDirectory, Uuid) {
        let managed = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let mut dir = make_dir(managed.path());

        let canonical = dir.validate_external_root(external.path()).unwrap();
        let root = ExternalScriptRoot::new(canonical);
        let id = root.id;
        dir.add_external_root(root).unwrap();

        (managed, external, dir, id)
    }

    fn external_path(dir: &ScriptsDirectory, id: Uuid) -> PathBuf {
        dir.external_root(id).unwrap().path().to_path_buf()
    }

    #[test]
    fn external_root_lists_scripts_in_place_without_copying() {
        let managed = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        fs::write(external.path().join("report.sql"), "SELECT 1;").unwrap();
        fs::create_dir(external.path().join("migrations")).unwrap();
        fs::write(external.path().join("migrations/001.sql"), "SELECT 2;").unwrap();

        let mut dir = make_dir(managed.path());
        let canonical = dir.validate_external_root(external.path()).unwrap();
        let root = ExternalScriptRoot::new(canonical.clone());
        let id = root.id;
        dir.add_external_root(root).unwrap();

        let mounted = dir.external_root(id).unwrap();
        assert_eq!(mounted.availability(), &ScriptRootAvailability::Available);
        assert_eq!(mounted.entries().len(), 2);
        assert_eq!(mounted.entries()[0].name(), "migrations");
        assert_eq!(mounted.entries()[1].path(), canonical.join("report.sql"));

        assert!(
            dir.entries().is_empty(),
            "nothing is copied into the managed root"
        );
    }

    #[test]
    fn external_root_lists_only_scripts_and_hides_folders_of_unrelated_files() {
        let (_managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);

        fs::write(root.join("README.md"), "# docs").unwrap();
        fs::write(root.join("query.sql"), "SELECT 1;").unwrap();
        fs::create_dir(root.join("assets")).unwrap();
        fs::write(root.join("assets/logo.png"), "png").unwrap();
        fs::create_dir(root.join("empty")).unwrap();
        dir.refresh();

        let names: Vec<&str> = dir
            .external_root(id)
            .unwrap()
            .entries()
            .iter()
            .map(ScriptEntry::name)
            .collect();
        assert_eq!(
            names,
            vec!["empty", "query.sql"],
            "an empty folder may be one the user just made; one of unrelated files is noise"
        );
    }

    #[test]
    fn missing_external_root_is_kept_and_reported_unavailable() {
        let managed = TempDir::new().unwrap();
        let mut dir = make_dir(managed.path());
        let gone = managed
            .path()
            .with_file_name("dbflux-missing-external-root");

        dir.register_external_roots(vec![ExternalScriptRoot::new(gone.clone())]);
        assert_eq!(
            dir.external_roots()[0].availability(),
            &ScriptRootAvailability::Pending
        );

        dir.refresh();

        let mounted = &dir.external_roots()[0];
        assert!(matches!(
            mounted.availability(),
            ScriptRootAvailability::Unavailable { .. }
        ));
        assert!(mounted.entries().is_empty());
        assert_eq!(mounted.path(), gone);
    }

    #[test]
    fn operations_work_inside_an_external_root() {
        let (_managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);

        let folder = dir.create_folder(Some(&root), "reports").unwrap();
        let file = dir.create_file(Some(&folder), "daily", "sql").unwrap();
        assert!(file.exists());
        assert_eq!(dir.external_root(id).unwrap().entries().len(), 1);

        let renamed = dir.rename(&file, "weekly.sql").unwrap();
        assert!(renamed.exists());

        let moved = dir.move_entry(&renamed, &root).unwrap();
        assert_eq!(moved, root.join("weekly.sql"));

        dir.delete(&moved).unwrap();
        assert!(!moved.exists());
    }

    #[test]
    fn roots_themselves_cannot_be_renamed_deleted_or_moved() {
        let (managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);

        assert!(dir.rename(&root, "renamed").is_err());
        assert!(dir.delete(&root).is_err());
        assert!(dir.rename(managed.path(), "renamed").is_err());
        assert!(dir.delete(managed.path()).is_err());
        assert!(root.is_dir(), "an external root is never touched on disk");
    }

    #[test]
    fn moving_between_roots_is_refused() {
        let (managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);

        let managed_file = dir.create_file(None, "local", "sql").unwrap();
        let external_file = dir.create_file(Some(&root), "shared", "sql").unwrap();

        assert!(dir.move_entry(&managed_file, &root).is_err());
        assert!(dir.move_entry(&external_file, managed.path()).is_err());
        assert!(managed_file.exists());
        assert!(external_file.exists());
    }

    #[test]
    fn removing_an_external_root_keeps_its_files() {
        let (_managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);
        let file = dir.create_file(Some(&root), "keep", "sql").unwrap();

        let removed = dir.remove_external_root(id).unwrap();

        assert_eq!(removed.path, root);
        assert!(dir.external_roots().is_empty());
        assert!(file.exists(), "unregistering never deletes the folder");
        assert!(
            dir.delete(&file).is_err(),
            "a forgotten root is outside the script folders again"
        );
    }

    #[test]
    fn overlapping_external_roots_are_refused() {
        let (managed, external, dir, _id) = with_external_root();

        let nested = external.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let inside_managed = managed.path().join("inner");
        fs::create_dir(&inside_managed).unwrap();

        assert!(dir.validate_external_root(external.path()).is_err());
        assert!(dir.validate_external_root(&nested).is_err());
        assert!(dir.validate_external_root(&inside_managed).is_err());
        assert!(dir.validate_external_root(managed.path()).is_err());
    }

    #[test]
    fn validating_a_missing_or_non_folder_path_fails() {
        let managed = TempDir::new().unwrap();
        let other = TempDir::new().unwrap();
        let dir = make_dir(managed.path());
        let file = other.path().join("file.sql");
        fs::write(&file, "SELECT 1;").unwrap();

        assert!(
            dir.validate_external_root(&other.path().join("nope"))
                .is_err()
        );
        assert!(dir.validate_external_root(&file).is_err());
    }

    #[test]
    fn a_late_scan_does_not_resurrect_a_removed_root() {
        let (_managed, _external, mut dir, id) = with_external_root();

        let request = dir.scan_request();
        dir.remove_external_root(id);
        dir.adopt_full_scan(request.run());

        assert!(dir.external_roots().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_back_to_an_ancestor_is_walked_once() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/query.sql"), "SELECT 1;").unwrap();
        std::os::unix::fs::symlink(tmp.path(), tmp.path().join("sub/loop")).unwrap();

        let dir = make_dir(tmp.path());

        assert_eq!(dir.entries().len(), 1);
        let ScriptEntry::Folder { children, .. } = &dir.entries()[0] else {
            panic!("Expected folder");
        };
        let names: Vec<&str> = children.iter().map(ScriptEntry::name).collect();
        assert_eq!(names, vec!["query.sql"]);
    }

    #[test]
    fn test_create_file_and_folder() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        let folder_path = dir.create_folder(None, "project-a").unwrap();
        assert!(folder_path.is_dir());

        let file_path = dir.create_file(Some(&folder_path), "init", "sql").unwrap();
        assert!(file_path.exists());
        assert_eq!(file_path.file_name().unwrap(), "init.sql");

        assert_eq!(dir.entries().len(), 1);
        if let ScriptEntry::Folder { children, .. } = &dir.entries()[0] {
            assert_eq!(children.len(), 1);
        } else {
            panic!("Expected folder");
        }
    }

    #[test]
    fn test_rename_and_delete() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        let path = dir.create_file(None, "old", "sql").unwrap();
        assert_eq!(dir.entries().len(), 1);

        let new_path = dir.rename(&path, "new.sql").unwrap();
        assert!(!path.exists());
        assert!(new_path.exists());
        assert_eq!(dir.entries().len(), 1);

        dir.delete(&new_path).unwrap();
        assert!(dir.entries().is_empty());
    }

    #[test]
    fn remove_if_unchanged_removes_only_the_bytes_it_was_given() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());
        let root = tmp.path();
        let path = dir.create_file(None, "query", "sql").unwrap();

        // A change made outside dbflux is kept, and is not an error.
        fs::write(&path, "FOREIGN;").unwrap();
        assert!(!ScriptsDirectory::remove_if_unchanged(root, &path, "").unwrap());
        assert_eq!(fs::read_to_string(&path).unwrap(), "FOREIGN;");

        // A file that is already gone is kept gone, and is not recreated.
        fs::remove_file(&path).unwrap();
        assert!(!ScriptsDirectory::remove_if_unchanged(root, &path, "").unwrap());
        assert!(!path.exists());

        // The document's own bytes are the ones that are removed.
        fs::write(&path, "MINE;").unwrap();
        assert!(ScriptsDirectory::remove_if_unchanged(root, &path, "MINE;").unwrap());
        assert!(!path.exists());
    }

    #[test]
    fn remove_if_unchanged_refuses_the_root_and_paths_outside_it() {
        let tmp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let victim = outside.path().join("victim.sql");
        fs::write(&victim, "MINE;").unwrap();

        assert!(ScriptsDirectory::remove_if_unchanged(tmp.path(), &victim, "MINE;").is_err());
        assert!(ScriptsDirectory::remove_if_unchanged(tmp.path(), tmp.path(), "MINE;").is_err());
        assert!(victim.exists(), "a path outside the root is never removed");
    }

    #[test]
    fn an_adopted_scan_replaces_the_cached_tree() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());
        fs::write(tmp.path().join("later.sql"), "SELECT 1;").unwrap();

        assert!(
            dir.entries().is_empty(),
            "the cached tree is stale until a scan replaces it"
        );

        // The off-thread shape: the scan runs apart from the owner of the cache,
        // which adopts the result once it has one.
        let scanned = dir.scan_request().run();
        dir.adopt_full_scan(scanned);

        assert_eq!(dir.entries().len(), 1);
        assert_eq!(dir.entries()[0].name(), "later.sql");
    }

    #[test]
    fn a_change_updates_the_tree_without_walking_the_disk() {
        let (_managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);

        // Written behind DBFlux's back: only a scan can find it.
        fs::write(root.join("foreign.sql"), "SELECT 1;").unwrap();

        let created = dir.create_file(Some(&root), "mine", "sql").unwrap();

        let names: Vec<&str> = dir
            .external_root(id)
            .unwrap()
            .entries()
            .iter()
            .map(ScriptEntry::name)
            .collect();
        assert_eq!(
            names,
            vec!["mine.sql"],
            "no scan ran, so the foreign file is unseen"
        );
        assert_eq!(dir.external_root(id).unwrap().entries()[0].path(), created);
    }

    #[test]
    fn a_scan_requested_before_a_change_is_dropped() {
        let (_managed, _external, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);

        let stale = dir.scan_request();
        dir.create_file(Some(&root), "after", "sql").unwrap();
        dir.adopt_full_scan(stale.run());

        assert_eq!(
            dir.external_root(id).unwrap().entries().len(),
            1,
            "the stale scan would have dropped the new file"
        );

        fs::write(root.join("foreign.sql"), "SELECT 1;").unwrap();
        let fresh = dir.scan_request_for(&root).unwrap();
        dir.adopt_full_scan(fresh.run());

        assert_eq!(dir.external_root(id).unwrap().entries().len(), 2);
    }

    #[test]
    fn renaming_and_moving_a_folder_rebases_its_cached_children() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        let folder = dir.create_folder(None, "reports").unwrap();
        dir.create_file(Some(&folder), "daily", "sql").unwrap();
        let target = dir.create_folder(None, "archive").unwrap();

        let renamed = dir.rename(&folder, "monthly").unwrap();
        let moved = dir.move_entry(&renamed, &target).unwrap();

        let in_memory = dir.entries().to_vec();
        dir.refresh();
        assert_eq!(
            format!("{in_memory:?}"),
            format!("{:?}", dir.entries()),
            "the cached tree matches what a scan finds"
        );

        let ScriptEntry::Folder { children, .. } = &dir.entries()[0] else {
            panic!("Expected folder");
        };
        assert_eq!(children[0].path(), moved);
    }

    #[test]
    fn registering_skips_roots_that_overlap() {
        let managed = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let mut dir = make_dir(managed.path());

        dir.register_external_roots(vec![
            ExternalScriptRoot::new(managed.path().join("inner")),
            ExternalScriptRoot::new(external.path().to_path_buf()),
            ExternalScriptRoot::new(external.path().join("nested")),
        ]);

        let paths: Vec<&Path> = dir
            .external_roots()
            .iter()
            .map(MountedScriptRoot::path)
            .collect();
        assert_eq!(paths, vec![external.path()]);
    }

    #[test]
    fn share_root_tells_moves_within_a_root_from_moves_across() {
        let (managed, _external, dir, id) = with_external_root();
        let root = external_path(&dir, id);
        let managed_file = managed.path().join("a.sql");
        let external_file = root.join("b.sql");

        assert!(dir.share_root(std::slice::from_ref(&managed_file), managed.path()));
        assert!(dir.share_root(std::slice::from_ref(&external_file), &root));
        assert!(!dir.share_root(std::slice::from_ref(&managed_file), &root));
        assert!(!dir.share_root(&[managed_file, external_file], &root));
    }

    #[cfg(unix)]
    #[test]
    fn an_external_root_neither_lists_nor_writes_through_links_that_leave_it() {
        let (_managed, elsewhere, mut dir, id) = with_external_root();
        let root = external_path(&dir, id);
        let outside = TempDir::new().unwrap();
        fs::write(outside.path().join("secret.sql"), "SELECT 1;").unwrap();
        fs::create_dir(root.join("real")).unwrap();
        fs::write(root.join("real/inside.sql"), "SELECT 2;").unwrap();

        std::os::unix::fs::symlink(outside.path(), root.join("escape")).unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join("alias")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret.sql"), root.join("secret.sql"))
            .unwrap();
        dir.refresh();

        let names: Vec<&str> = dir
            .external_root(id)
            .unwrap()
            .entries()
            .iter()
            .map(ScriptEntry::name)
            .collect();
        assert_eq!(
            names,
            vec!["real"],
            "a link out of the folder, and a second path to a listed folder, are skipped"
        );

        assert!(
            dir.create_file(Some(&root.join("escape")), "x", "sql")
                .is_err()
        );
        assert!(
            dir.import(
                &elsewhere.path().join("missing.sql"),
                Some(&root.join("escape"))
            )
            .is_err()
        );
        assert!(!outside.path().join("x.sql").exists());
    }

    #[test]
    fn test_import() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        // Create a temp file outside the scripts root
        let ext_dir = TempDir::new().unwrap();
        let source = ext_dir.path().join("my_query.sql");
        fs::write(&source, "SELECT 1;").unwrap();

        let imported = dir.import(&source, None).unwrap();
        assert!(imported.exists());
        assert_eq!(fs::read_to_string(&imported).unwrap(), "SELECT 1;");
    }

    #[test]
    fn test_shows_all_files_with_extensions() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("notes.txt"), "hello").unwrap();
        fs::write(tmp.path().join("query.sql"), "SELECT 1").unwrap();
        fs::write(tmp.path().join("hook.lua"), "print('hi')").unwrap();
        fs::write(tmp.path().join("setup.py"), "pass").unwrap();
        fs::write(tmp.path().join("deploy.sh"), "echo ok").unwrap();

        let dir = make_dir(tmp.path());
        assert_eq!(dir.entries().len(), 5);

        let names: Vec<&str> = dir.entries().iter().map(|e| e.name()).collect();
        assert!(names.contains(&"query.sql"));
        assert!(names.contains(&"hook.lua"));
        assert!(names.contains(&"setup.py"));
        assert!(names.contains(&"deploy.sh"));
        assert!(names.contains(&"notes.txt"));
    }

    #[test]
    fn test_skips_files_without_extension() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("Makefile"), "all:").unwrap();
        fs::write(tmp.path().join("query.sql"), "SELECT 1").unwrap();

        let dir = make_dir(tmp.path());
        assert_eq!(dir.entries().len(), 1);
        assert_eq!(dir.entries()[0].name(), "query.sql");
    }

    #[test]
    fn test_is_openable_script() {
        assert!(is_openable_script(Path::new("test.sql")));
        assert!(is_openable_script(Path::new("hook.lua")));
        assert!(is_openable_script(Path::new("setup.py")));
        assert!(is_openable_script(Path::new("deploy.sh")));
        assert!(is_openable_script(Path::new("run.bash")));
        assert!(!is_openable_script(Path::new("notes.txt")));
        assert!(!is_openable_script(Path::new("image.png")));
        assert!(!is_openable_script(Path::new("Makefile")));
    }

    #[test]
    fn test_filter_entries() {
        let entries = vec![
            ScriptEntry::File {
                path: PathBuf::from("/a/setup.sql"),
                name: "setup.sql".into(),
                extension: "sql".into(),
            },
            ScriptEntry::Folder {
                path: PathBuf::from("/a/migrations"),
                name: "migrations".into(),
                children: vec![ScriptEntry::File {
                    path: PathBuf::from("/a/migrations/001_init.sql"),
                    name: "001_init.sql".into(),
                    extension: "sql".into(),
                }],
            },
            ScriptEntry::File {
                path: PathBuf::from("/a/cleanup.redis"),
                name: "cleanup.redis".into(),
                extension: "redis".into(),
            },
        ];

        let filtered = filter_entries(&entries, "init");
        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].is_folder());

        let filtered = filter_entries(&entries, "setup");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name(), "setup.sql");

        let all = filter_entries(&entries, "");
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_hidden_files_ignored() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join(".hidden.sql"), "SELECT 1").unwrap();
        fs::write(tmp.path().join("visible.sql"), "SELECT 2").unwrap();

        let dir = make_dir(tmp.path());
        assert_eq!(dir.entries().len(), 1);
        assert_eq!(dir.entries()[0].name(), "visible.sql");
    }

    #[test]
    fn test_move_entry() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        dir.create_file(None, "query", "sql").unwrap();
        dir.create_folder(None, "subfolder").unwrap();

        let source = tmp.path().join("query.sql");
        let target = tmp.path().join("subfolder");
        assert!(source.exists());

        let new_path = dir.move_entry(&source, &target).unwrap();
        assert_eq!(new_path, target.join("query.sql"));
        assert!(!source.exists());
        assert!(new_path.exists());
    }

    #[test]
    fn test_move_entry_to_same_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        dir.create_file(None, "query", "sql").unwrap();

        let source = tmp.path().join("query.sql");
        let result = dir.move_entry(&source, tmp.path()).unwrap();
        assert_eq!(result, source);
        assert!(source.exists());
    }

    #[test]
    fn test_move_entry_prevents_cycle() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        dir.create_folder(None, "parent").unwrap();
        dir.create_folder(Some(Path::new(&tmp.path().join("parent"))), "child")
            .unwrap();

        let parent = tmp.path().join("parent");
        let child = tmp.path().join("parent").join("child");

        assert!(dir.move_entry(&parent, &child).is_err());
    }

    #[test]
    fn test_prevents_operations_outside_root() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());
        let outside = PathBuf::from("/tmp/somewhere_else");

        assert!(dir.create_file(Some(&outside), "bad", "sql").is_err());
        assert!(dir.create_folder(Some(&outside), "bad").is_err());
        assert!(dir.rename(&outside.join("file.sql"), "new.sql").is_err());
        assert!(dir.delete(&outside.join("file.sql")).is_err());
        assert!(
            dir.move_entry(&outside.join("file.sql"), tmp.path())
                .is_err()
        );
        assert!(
            dir.move_entry(&tmp.path().join("file.sql"), &outside)
                .is_err()
        );
    }

    #[test]
    fn test_rename_rejects_path_traversal_names() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        let source = dir.create_file(None, "query", "sql").unwrap();

        assert!(dir.rename(&source, "../outside.sql").is_err());
        assert!(dir.rename(&source, "..\\outside.sql").is_err());
        assert!(dir.rename(&source, "folder/name.sql").is_err());
    }

    #[test]
    fn test_hooks_directory_is_created() {
        let tmp = TempDir::new().unwrap();
        let dir = make_dir(tmp.path());

        let hooks_dir = dir.hooks_directory().unwrap();

        assert_eq!(hooks_dir, tmp.path().join("hooks"));
        assert!(hooks_dir.exists());
        assert!(hooks_dir.is_dir());
    }

    #[test]
    fn test_hook_script_path_uses_language_extension() {
        let hooks_dir = PathBuf::from("/tmp/dbflux-hooks");

        assert_eq!(
            hook_script_path(&hooks_dir, "setup", ScriptLanguage::Bash),
            hooks_dir.join("setup.sh")
        );
        assert_eq!(
            hook_script_path(&hooks_dir, "seed", ScriptLanguage::Python),
            hooks_dir.join("seed.py")
        );
    }
}
