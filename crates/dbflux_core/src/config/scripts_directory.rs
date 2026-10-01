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
}

impl MountedScriptRoot {
    fn pending(root: ExternalScriptRoot) -> Self {
        Self {
            root,
            entries: Vec::new(),
            availability: ScriptRootAvailability::Pending,
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
/// on a background thread.
#[derive(Debug, Clone)]
pub struct ScriptsScanRequest {
    managed: PathBuf,
    external: Vec<(Uuid, PathBuf)>,
}

impl ScriptsScanRequest {
    /// Walks every root. Blocks for as long as the slowest folder takes to
    /// answer, so call it off the thread that renders.
    pub fn run(self) -> ScriptsScan {
        let managed = scan_directory(&self.managed);

        let external = self
            .external
            .into_iter()
            .map(|(id, path)| {
                let scanned =
                    scan_tree(&path, ScanFilter::OpenableOnly).map_err(|error| error.to_string());
                (id, path, scanned)
            })
            .collect();

        ScriptsScan {
            managed_root: self.managed,
            managed,
            external,
        }
    }
}

/// The outcome of a [`ScriptsScanRequest`], applied with
/// [`ScriptsDirectory::adopt_full_scan`].
#[derive(Debug, Clone)]
pub struct ScriptsScan {
    managed_root: PathBuf,
    managed: Vec<ScriptEntry>,
    external: Vec<(Uuid, PathBuf, Result<Vec<ScriptEntry>, String>)>,
}

/// Manages the scripts the sidebar lists.
///
/// The managed root at `~/.local/share/dbflux/scripts/` belongs to DBFlux: new
/// queries and hook scripts are created there. External roots are folders the
/// user registered; their files are listed and edited where they are and are
/// never copied. Every filesystem operation is confined to one root: a path
/// outside all of them, or an operation that would cross from one root into
/// another, is refused.
///
/// The roots never overlap: an external root cannot sit inside the managed root
/// or another external root, nor contain one. That keeps "which root owns this
/// path" a single answer.
pub struct ScriptsDirectory {
    root: PathBuf,
    entries: Vec<ScriptEntry>,
    external: Vec<MountedScriptRoot>,
}

impl ScriptsDirectory {
    pub fn new() -> Result<Self, DbError> {
        let data_dir = dirs::data_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find data directory"))
        })?;

        let root = data_dir.join("dbflux").join("scripts");
        fs::create_dir_all(&root).map_err(DbError::IoError)?;

        let entries = scan_directory(&root);

        Ok(Self {
            root,
            entries,
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
    /// registered before.
    pub fn register_external_roots(&mut self, roots: Vec<ExternalScriptRoot>) {
        self.external = roots.into_iter().map(MountedScriptRoot::pending).collect();
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

    /// Registers an external root and scans it.
    ///
    /// The root's path must already be canonical, as
    /// [`Self::validate_external_root`] returns it; the overlap checks run again
    /// here against the current registrations.
    pub fn add_external_root(&mut self, root: ExternalScriptRoot) -> Result<(), DbError> {
        self.ensure_no_overlap(&root.path)?;

        let mut mounted = MountedScriptRoot::pending(root);
        mounted
            .adopt(scan_tree(mounted.path(), ScanFilter::OpenableOnly).map_err(|e| e.to_string()));
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

    /// The root `path` belongs to, or `None` when it is outside every root.
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

    fn require_owning_root(&self, path: &Path, what: &str) -> Result<PathBuf, DbError> {
        self.owning_root(path)
            .map(Path::to_path_buf)
            .ok_or_else(|| io_error(format!("{what} is outside the script folders")))
    }

    /// Re-scan every root synchronously and update the cached trees.
    ///
    /// Blocks on every external folder; prefer [`Self::scan_request`] from the
    /// UI thread.
    pub fn refresh(&mut self) {
        let scan = self.scan_request().run();
        self.adopt_full_scan(scan);
    }

    /// Re-scan only the root that owns `path`.
    fn refresh_root_of(&mut self, path: &Path) {
        if path.starts_with(&self.root) {
            self.adopt_scan(Self::scan(&self.root));
            return;
        }

        if let Some(mounted) = self
            .external
            .iter_mut()
            .find(|mounted| path.starts_with(mounted.path()))
        {
            let scanned =
                scan_tree(mounted.path(), ScanFilter::OpenableOnly).map_err(|e| e.to_string());
            mounted.adopt(scanned);
        }
    }

    /// The roots a full scan would walk, detached so the walk can run on a
    /// background thread.
    pub fn scan_request(&self) -> ScriptsScanRequest {
        ScriptsScanRequest {
            managed: self.root.clone(),
            external: self
                .external
                .iter()
                .map(|mounted| (mounted.id(), mounted.path().to_path_buf()))
                .collect(),
        }
    }

    /// Applies a full scan.
    ///
    /// A root removed or re-registered at another path since the request was
    /// taken keeps its current state, so a late scan never resurrects a root.
    pub fn adopt_full_scan(&mut self, scan: ScriptsScan) {
        if scan.managed_root == self.root {
            self.entries = scan.managed;
        }

        for (id, path, scanned) in scan.external {
            if let Some(mounted) = self
                .external
                .iter_mut()
                .find(|mounted| mounted.id() == id && mounted.path() == path)
            {
                mounted.adopt(scanned);
            }
        }
    }

    /// Scans the managed root, without touching the cached tree.
    ///
    /// Split from [`Self::refresh`] so the walking can happen off the thread that
    /// renders: a directory on a stalled mount blocks for as long as the disk
    /// takes to answer, and a close gesture must not inherit that wait.
    pub fn scan(root: &Path) -> Vec<ScriptEntry> {
        scan_directory(root)
    }

    /// Replaces the managed root's cached tree with entries scanned elsewhere.
    ///
    /// The entries are taken as given: this does not check that they still
    /// describe the root, because only the caller that scanned them knows how
    /// fresh they are.
    pub fn adopt_scan(&mut self, entries: Vec<ScriptEntry>) {
        self.entries = entries;
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
    /// caller can run it off the UI thread and hand the result to
    /// [`Self::adopt_scan`] afterwards.
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
        self.require_owning_root(&dir, "Target directory")?;

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
        self.refresh_root_of(&path);
        Ok(path)
    }

    /// Create a subdirectory. `parent` defaults to the managed root.
    /// Returns the full path.
    pub fn create_folder(&mut self, parent: Option<&Path>, name: &str) -> Result<PathBuf, DbError> {
        let dir = parent.unwrap_or(&self.root).to_path_buf();
        self.require_owning_root(&dir, "Target directory")?;

        let path = dir.join(name);
        if path.exists() {
            return Err(io_error(format!("Folder already exists: {}", name)));
        }

        fs::create_dir_all(&path).map_err(DbError::IoError)?;
        self.refresh_root_of(&path);
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

        self.require_owning_root(old_path, "Path")?;

        if self.is_root(old_path) {
            return Err(io_error("Cannot rename a scripts root".to_string()));
        }

        let parent = old_path
            .parent()
            .ok_or_else(|| io_error("Cannot rename root".to_string()))?;

        let new_path = parent.join(new_name);
        if new_path.exists() {
            return Err(io_error(format!("Already exists: {}", new_name)));
        }

        fs::rename(old_path, &new_path).map_err(DbError::IoError)?;
        self.refresh_root_of(&new_path);
        Ok(new_path)
    }

    /// Delete a file or folder (recursive for folders) inside any root.
    /// A root itself is never deleted; an external root is unregistered with
    /// [`Self::remove_external_root`] instead.
    pub fn delete(&mut self, path: &Path) -> Result<(), DbError> {
        let root = self.require_owning_root(path, "Path")?;
        Self::ensure_deletable(&root, path)?;

        if path.is_dir() {
            fs::remove_dir_all(path).map_err(DbError::IoError)?;
        } else {
            fs::remove_file(path).map_err(DbError::IoError)?;
        }

        self.refresh_root_of(&root);
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

        if dest.exists() {
            return Err(io_error(format!("Already exists: {}", dest.display())));
        }

        fs::create_dir_all(target_dir).map_err(DbError::IoError)?;
        fs::rename(source, &dest).map_err(DbError::IoError)?;
        self.refresh_root_of(&dest);
        Ok(dest)
    }

    /// Copy an external file into a folder of any root (the managed root by
    /// default).
    pub fn import(&mut self, source: &Path, target_dir: Option<&Path>) -> Result<PathBuf, DbError> {
        let dir = target_dir.unwrap_or(&self.root).to_path_buf();
        self.require_owning_root(&dir, "Target directory")?;

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
        self.refresh_root_of(&dest);
        Ok(dest)
    }
}

fn io_error(message: String) -> DbError {
    DbError::IoError(std::io::Error::other(message))
}

/// Whether one of the two folders contains the other (or they are the same).
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
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

/// Which files a scan keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanFilter {
    /// Every file with an extension: the managed root holds only what DBFlux
    /// or the user put there for it.
    AnyExtension,
    /// Only files the editor opens, and only folders that lead to one: an
    /// external folder is often a repository full of unrelated files.
    OpenableOnly,
}

/// Recursively scan the managed root, returning sorted entries (folders first,
/// then files). A root that cannot be read yields an empty tree.
fn scan_directory(dir: &Path) -> Vec<ScriptEntry> {
    scan_tree(dir, ScanFilter::AnyExtension).unwrap_or_else(|e| {
        log::warn!("Failed to read scripts directory {:?}: {}", dir, e);
        Vec::new()
    })
}

/// Recursively scan `root`, returning sorted entries (folders first, then
/// files).
///
/// Fails only when `root` itself cannot be read; an unreadable subfolder is
/// logged and listed empty. Each folder is walked once by its canonical path, so
/// a symlink back to an ancestor does not repeat the tree.
fn scan_tree(root: &Path, filter: ScanFilter) -> Result<Vec<ScriptEntry>, std::io::Error> {
    let read_dir = fs::read_dir(root)?;

    let mut visited = HashSet::new();
    if let Ok(canonical) = fs::canonicalize(root) {
        visited.insert(canonical);
    }

    Ok(scan_entries(read_dir, filter, &mut visited))
}

fn scan_folder(dir: &Path, filter: ScanFilter, visited: &mut HashSet<PathBuf>) -> Vec<ScriptEntry> {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) => {
            log::warn!("Failed to read scripts directory {:?}: {}", dir, e);
            return Vec::new();
        }
    };

    scan_entries(read_dir, filter, visited)
}

fn scan_entries(
    read_dir: fs::ReadDir,
    filter: ScanFilter,
    visited: &mut HashSet<PathBuf>,
) -> Vec<ScriptEntry> {
    let mut folders = Vec::new();
    let mut files = Vec::new();

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

        if path.is_dir() {
            let first_visit = match fs::canonicalize(&path) {
                Ok(canonical) => visited.insert(canonical),
                Err(e) => {
                    log::warn!("Failed to resolve scripts folder {:?}: {}", path, e);
                    false
                }
            };

            if !first_visit {
                continue;
            }

            let children = scan_folder(&path, filter, visited);

            if filter == ScanFilter::OpenableOnly && children.is_empty() {
                continue;
            }

            folders.push(ScriptEntry::Folder {
                path,
                name,
                children,
            });
        } else if has_file_extension(&path) {
            if filter == ScanFilter::OpenableOnly && !is_openable_script(&path) {
                continue;
            }

            let extension = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();

            files.push(ScriptEntry::File {
                path,
                name,
                extension,
            });
        }
    }

    folders.sort_by_key(|a| a.name().to_lowercase());
    files.sort_by_key(|a| a.name().to_lowercase());

    folders.into_iter().chain(files).collect()
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
    fn external_root_lists_only_openable_scripts_and_prunes_empty_folders() {
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
        assert_eq!(names, vec!["query.sql"]);
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
        let scanned = ScriptsDirectory::scan(tmp.path());
        dir.adopt_scan(scanned);

        assert_eq!(dir.entries().len(), 1);
        assert_eq!(dir.entries()[0].name(), "later.sql");
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
