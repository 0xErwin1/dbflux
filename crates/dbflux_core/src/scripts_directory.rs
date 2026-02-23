use crate::DbError;
use std::fs;
use std::path::{Path, PathBuf};

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

/// Manages the centralized scripts directory at `~/.local/share/dbflux/scripts/`.
///
/// Scans the filesystem on demand and provides CRUD operations for script files
/// and folders. Only files with recognized query language extensions are included.
pub struct ScriptsDirectory {
    root: PathBuf,
    entries: Vec<ScriptEntry>,
}

impl ScriptsDirectory {
    pub fn new() -> Result<Self, DbError> {
        let data_dir = dirs::data_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find data directory"))
        })?;

        let root = data_dir.join("dbflux").join("scripts");
        fs::create_dir_all(&root).map_err(DbError::IoError)?;

        let entries = scan_directory(&root);

        Ok(Self { root, entries })
    }

    pub fn root_path(&self) -> &Path {
        &self.root
    }

    pub fn entries(&self) -> &[ScriptEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Re-scan the filesystem and update the cached entry tree.
    pub fn refresh(&mut self) {
        self.entries = scan_directory(&self.root);
    }

    /// Create an empty script file. Returns the full path of the created file.
    pub fn create_file(
        &mut self,
        parent: Option<&Path>,
        name: &str,
        extension: &str,
    ) -> Result<PathBuf, DbError> {
        let dir = parent.unwrap_or(&self.root);
        if !dir.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Target directory is outside scripts root",
            )));
        }

        let filename = if name.contains('.') {
            name.to_string()
        } else {
            format!("{}.{}", name, extension)
        };

        let path = dir.join(&filename);
        if path.exists() {
            return Err(DbError::IoError(std::io::Error::other(format!(
                "File already exists: {}",
                filename
            ))));
        }

        fs::write(&path, "").map_err(DbError::IoError)?;
        self.refresh();
        Ok(path)
    }

    /// Create a subdirectory. Returns the full path.
    pub fn create_folder(&mut self, parent: Option<&Path>, name: &str) -> Result<PathBuf, DbError> {
        let dir = parent.unwrap_or(&self.root);
        if !dir.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Target directory is outside scripts root",
            )));
        }

        let path = dir.join(name);
        if path.exists() {
            return Err(DbError::IoError(std::io::Error::other(format!(
                "Folder already exists: {}",
                name
            ))));
        }

        fs::create_dir_all(&path).map_err(DbError::IoError)?;
        self.refresh();
        Ok(path)
    }

    /// Rename a file or folder. Returns the new path.
    pub fn rename(&mut self, old_path: &Path, new_name: &str) -> Result<PathBuf, DbError> {
        if !old_path.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Path is outside scripts root",
            )));
        }

        let parent = old_path
            .parent()
            .ok_or_else(|| DbError::IoError(std::io::Error::other("Cannot rename root")))?;

        let new_path = parent.join(new_name);
        if new_path.exists() {
            return Err(DbError::IoError(std::io::Error::other(format!(
                "Already exists: {}",
                new_name
            ))));
        }

        fs::rename(old_path, &new_path).map_err(DbError::IoError)?;
        self.refresh();
        Ok(new_path)
    }

    /// Delete a file or folder (recursive for folders).
    pub fn delete(&mut self, path: &Path) -> Result<(), DbError> {
        if !path.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Path is outside scripts root",
            )));
        }

        if path == self.root {
            return Err(DbError::IoError(std::io::Error::other(
                "Cannot delete scripts root",
            )));
        }

        if path.is_dir() {
            fs::remove_dir_all(path).map_err(DbError::IoError)?;
        } else {
            fs::remove_file(path).map_err(DbError::IoError)?;
        }

        self.refresh();
        Ok(())
    }

    /// Move a file or folder to a different directory within the scripts root.
    /// Returns the new path of the moved entry.
    pub fn move_entry(&mut self, source: &Path, target_dir: &Path) -> Result<PathBuf, DbError> {
        if !source.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Source is outside scripts root",
            )));
        }

        if !target_dir.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Target is outside scripts root",
            )));
        }

        if source == self.root {
            return Err(DbError::IoError(std::io::Error::other(
                "Cannot move scripts root",
            )));
        }

        // Prevent moving a folder into itself or its descendants
        if source.is_dir() && target_dir.starts_with(source) {
            return Err(DbError::IoError(std::io::Error::other(
                "Cannot move a folder into itself",
            )));
        }

        let file_name = source
            .file_name()
            .ok_or_else(|| DbError::IoError(std::io::Error::other("Source has no file name")))?;

        let dest = target_dir.join(file_name);

        // Already in the target directory
        if source.parent() == Some(target_dir) {
            return Ok(source.to_path_buf());
        }

        if dest.exists() {
            return Err(DbError::IoError(std::io::Error::other(format!(
                "Already exists: {}",
                dest.display()
            ))));
        }

        fs::create_dir_all(target_dir).map_err(DbError::IoError)?;
        fs::rename(source, &dest).map_err(DbError::IoError)?;
        self.refresh();
        Ok(dest)
    }

    /// Copy an external file into the scripts directory (or a subfolder).
    pub fn import(&mut self, source: &Path, target_dir: Option<&Path>) -> Result<PathBuf, DbError> {
        let dir = target_dir.unwrap_or(&self.root);
        if !dir.starts_with(&self.root) {
            return Err(DbError::IoError(std::io::Error::other(
                "Target directory is outside scripts root",
            )));
        }

        let filename = source
            .file_name()
            .ok_or_else(|| DbError::IoError(std::io::Error::other("Source has no filename")))?;

        let dest = dir.join(filename);
        if dest.exists() {
            return Err(DbError::IoError(std::io::Error::other(format!(
                "File already exists: {}",
                filename.to_string_lossy()
            ))));
        }

        fs::copy(source, &dest).map_err(DbError::IoError)?;
        self.refresh();
        Ok(dest)
    }
}

/// All extensions recognized by `QueryLanguage::from_path`.
const RECOGNIZED_EXTENSIONS: &[&str] = &[
    "sql", "js", "mongodb", "redis", "red", "cypher", "cyp", "influxql", "flux", "cql",
];

fn is_recognized_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| RECOGNIZED_EXTENSIONS.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Recursively scan a directory, returning sorted entries (folders first, then files).
fn scan_directory(dir: &Path) -> Vec<ScriptEntry> {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) => {
            log::warn!("Failed to read scripts directory {:?}: {}", dir, e);
            return Vec::new();
        }
    };

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
            let children = scan_directory(&path);
            folders.push(ScriptEntry::Folder {
                path,
                name,
                children,
            });
        } else if is_recognized_extension(&path) {
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

/// Collect all recognized file extensions for use in file dialogs.
pub fn all_script_extensions() -> Vec<&'static str> {
    RECOGNIZED_EXTENSIONS.to_vec()
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
        }
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
    fn test_import() {
        let tmp = TempDir::new().unwrap();
        let mut dir = make_dir(tmp.path());

        let external = tmp.path().join("..").join("external.sql");
        // Create a temp file outside the scripts root
        let ext_dir = TempDir::new().unwrap();
        let source = ext_dir.path().join("my_query.sql");
        fs::write(&source, "SELECT 1;").unwrap();

        let imported = dir.import(&source, None).unwrap();
        assert!(imported.exists());
        assert_eq!(fs::read_to_string(&imported).unwrap(), "SELECT 1;");
    }

    #[test]
    fn test_ignores_unrecognized_extensions() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("notes.txt"), "hello").unwrap();
        fs::write(tmp.path().join("query.sql"), "SELECT 1").unwrap();
        fs::write(tmp.path().join("image.png"), &[0u8]).unwrap();

        let dir = make_dir(tmp.path());
        assert_eq!(dir.entries().len(), 1);
        assert_eq!(dir.entries()[0].name(), "query.sql");
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
        assert!(dir
            .move_entry(&outside.join("file.sql"), tmp.path())
            .is_err());
        assert!(dir
            .move_entry(&tmp.path().join("file.sql"), &outside)
            .is_err());
    }
}
