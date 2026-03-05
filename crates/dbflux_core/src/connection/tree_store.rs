use crate::{ConnectionTree, DbError};
use std::fs;
use std::path::PathBuf;

/// Persistent storage for the connection tree structure.
///
/// Stores the hierarchical organization of connection profiles into folders.
/// The tree data is stored separately from the profiles themselves, so even
/// if the tree file is corrupted or missing, the profiles remain intact.
pub struct ConnectionTreeStore {
    path: PathBuf,
}

impl ConnectionTreeStore {
    /// Creates a new connection tree store.
    ///
    /// Creates the config directory if it doesn't exist.
    pub fn new() -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        Ok(Self {
            path: app_dir.join("connections_tree.json"),
        })
    }

    /// Loads the connection tree from disk.
    ///
    /// Returns an empty tree if the file doesn't exist or is corrupted.
    /// Corruption is logged but doesn't fail the load.
    pub fn load(&self) -> Result<ConnectionTree, DbError> {
        if !self.path.exists() {
            return Ok(ConnectionTree::new());
        }

        let content = fs::read_to_string(&self.path).map_err(DbError::IoError)?;

        match serde_json::from_str::<ConnectionTree>(&content) {
            Ok(mut tree) => {
                let repaired = tree.repair_orphans();
                if repaired > 0 {
                    log::info!("Repaired {} orphaned nodes in connection tree", repaired);
                }
                Ok(tree)
            }
            Err(e) => {
                log::warn!(
                    "Failed to parse connection tree ({}), starting with empty tree",
                    e
                );
                Ok(ConnectionTree::new())
            }
        }
    }

    /// Saves the connection tree to disk.
    pub fn save(&self, tree: &ConnectionTree) -> Result<(), DbError> {
        let content = serde_json::to_string_pretty(tree)
            .map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        fs::write(&self.path, content).map_err(DbError::IoError)?;

        Ok(())
    }
}

impl Default for ConnectionTreeStore {
    fn default() -> Self {
        Self::new().expect("Failed to create connection tree store")
    }
}
