//! SQLite-backed tree store for connection tree persistence.
//! Replaces the JSON file store at `~/.config/dbflux/connections_tree.json`.

use crate::error::StorageError;
use crate::repositories::connection_folders::ConnectionFoldersRepository;
use crate::sqlite;
use dbflux_core::{ConnectionTree, DbError, TreeStore};
use std::path::PathBuf;

/// SQLite-backed tree store for connection tree persistence.
///
/// This store opens a fresh connection for each operation to avoid
/// threading issues with rusqlite::Connection (which is not Sync).
///
/// The tree data is stored in `dbflux.db` (`cfg_connection_folders` table),
/// separate from the profiles themselves, so even if the tree data is
/// corrupted or missing, the profiles remain intact.
pub struct SqliteTreeStore {
    config_db_path: PathBuf,
}

impl SqliteTreeStore {
    pub fn new(config_db_path: PathBuf) -> Self {
        Self { config_db_path }
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn with_repo<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(ConnectionFoldersRepository) -> Result<T, StorageError>,
    {
        #[allow(clippy::arc_with_non_send_sync)]
        let conn = sqlite::open_database(&self.config_db_path)?;
        let repo = ConnectionFoldersRepository::new(std::sync::Arc::new(conn));
        f(repo)
    }
}

impl TreeStore for SqliteTreeStore {
    fn load(&self) -> Result<ConnectionTree, DbError> {
        self.with_repo(|repo| repo.load_tree()).map_err(Into::into)
    }

    fn save(&self, tree: &ConnectionTree) -> Result<(), DbError> {
        self.with_repo(|repo| repo.save_tree(tree))
            .map_err(Into::into)
    }
}
