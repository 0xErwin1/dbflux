//! Repository for connection folders in dbflux.db.
//!
//! Connection folders store the hierarchical folder structure for organizing
//! connection profiles in the connection tree.

use log::{info, warn};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

const ROOT_FOLDER_SENTINEL_ID: &str = "00000000-0000-0000-0000-000000000001";
const ROOT_FOLDER_SENTINEL_NAME: &str = "__dbflux_root__";

/// Data transfer object for a connection folder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionFolderDto {
    pub id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub position: i32,
    pub collapsed: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Data transfer object for a connection folder item (folder -> profile relationship).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionFolderItemDto {
    pub id: String,
    pub folder_id: String,
    pub profile_id: String,
    pub position: i32,
}

/// Repository for managing connection folders and their items.
pub struct ConnectionFoldersRepository {
    conn: OwnedConnection,
}

impl ConnectionFoldersRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    fn is_root_folder_sentinel(folder_id: &str) -> bool {
        folder_id == ROOT_FOLDER_SENTINEL_ID
    }

    fn root_folder_sentinel() -> ConnectionFolderDto {
        let timestamp = chrono::Utc::now().to_rfc3339();

        ConnectionFolderDto {
            id: ROOT_FOLDER_SENTINEL_ID.to_string(),
            parent_id: None,
            name: ROOT_FOLDER_SENTINEL_NAME.to_string(),
            position: i32::MIN,
            collapsed: false,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        }
    }

    fn parse_uuid(value: &str, field: &str) -> Result<Uuid, StorageError> {
        Uuid::parse_str(value).map_err(|error| {
            StorageError::Data(format!("invalid {} uuid '{}': {}", field, value, error))
        })
    }

    fn parse_optional_uuid(
        value: Option<String>,
        field: &str,
    ) -> Result<Option<Uuid>, StorageError> {
        value
            .map(|value| Self::parse_uuid(&value, field))
            .transpose()
    }

    /// Checks if a profile exists in cfg_connection_profiles.
    fn profile_exists(&self, profile_id: &str) -> bool {
        let result = self.conn().query_row(
            "SELECT 1 FROM cfg_connection_profiles WHERE id = ?1",
            [profile_id],
            |_row| Ok(()),
        );
        result.is_ok()
    }

    /// Fetches all folders.
    pub fn all_folders(&self) -> Result<Vec<ConnectionFolderDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, parent_id, name, position, collapsed, created_at, updated_at
                FROM cfg_connection_folders
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let folders = stmt
            .query_map([], |row| {
                Ok(ConnectionFolderDto {
                    id: row.get(0)?,
                    parent_id: row.get(1)?,
                    name: row.get(2)?,
                    position: row.get(3)?,
                    collapsed: row.get::<_, i32>(4)? != 0,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for folder in folders {
            match folder {
                Ok(f) => result.push(f),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            });
        }

        Ok(result
            .into_iter()
            .filter(|folder| !Self::is_root_folder_sentinel(&folder.id))
            .collect())
    }

    /// Fetches a single folder by ID.
    pub fn get_folder(&self, id: &str) -> Result<Option<ConnectionFolderDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, parent_id, name, position, collapsed, created_at, updated_at
                FROM cfg_connection_folders
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let folder = stmt
            .query_row(params![id], |row| {
                Ok(ConnectionFolderDto {
                    id: row.get(0)?,
                    parent_id: row.get(1)?,
                    name: row.get(2)?,
                    position: row.get(3)?,
                    collapsed: row.get::<_, i32>(4)? != 0,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .ok();

        Ok(folder.filter(|folder| !Self::is_root_folder_sentinel(&folder.id)))
    }

    /// Fetches all root folders (folders with no parent).
    pub fn root_folders(&self) -> Result<Vec<ConnectionFolderDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, parent_id, name, position, collapsed, created_at, updated_at
                FROM cfg_connection_folders
                WHERE parent_id IS NULL
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let folders = stmt
            .query_map([], |row| {
                Ok(ConnectionFolderDto {
                    id: row.get(0)?,
                    parent_id: row.get(1)?,
                    name: row.get(2)?,
                    position: row.get(3)?,
                    collapsed: row.get::<_, i32>(4)? != 0,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for folder in folders {
            match folder {
                Ok(f) => result.push(f),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            });
        }

        Ok(result
            .into_iter()
            .filter(|folder| !Self::is_root_folder_sentinel(&folder.id))
            .collect())
    }

    /// Fetches child folders of a given parent folder.
    pub fn child_folders(&self, parent_id: &str) -> Result<Vec<ConnectionFolderDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, parent_id, name, position, collapsed, created_at, updated_at
                FROM cfg_connection_folders
                WHERE parent_id = ?1
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let folders = stmt
            .query_map(params![parent_id], |row| {
                Ok(ConnectionFolderDto {
                    id: row.get(0)?,
                    parent_id: row.get(1)?,
                    name: row.get(2)?,
                    position: row.get(3)?,
                    collapsed: row.get::<_, i32>(4)? != 0,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for folder in folders {
            match folder {
                Ok(f) => result.push(f),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            });
        }

        Ok(result
            .into_iter()
            .filter(|folder| !Self::is_root_folder_sentinel(&folder.id))
            .collect())
    }

    /// Inserts a new folder.
    pub fn insert_folder(&self, dto: &ConnectionFolderDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_folders (id, parent_id, name, position, collapsed, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    dto.id,
                    dto.parent_id,
                    dto.name,
                    dto.position,
                    dto.collapsed as i32,
                    dto.created_at,
                    dto.updated_at,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Updates an existing folder.
    pub fn update_folder(&self, dto: &ConnectionFolderDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                UPDATE cfg_connection_folders
                SET parent_id = ?2, name = ?3, position = ?4, collapsed = ?5, updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    dto.id,
                    dto.parent_id,
                    dto.name,
                    dto.position,
                    dto.collapsed as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Deletes a folder and all its items (cascade).
    pub fn delete_folder(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM cfg_connection_folders WHERE id = ?1",
                params![id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Fetches all items in a folder.
    pub fn folder_items(
        &self,
        folder_id: &str,
    ) -> Result<Vec<ConnectionFolderItemDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, folder_id, profile_id, position
                FROM cfg_connection_folder_items
                WHERE folder_id = ?1
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let items = stmt
            .query_map(params![folder_id], |row| {
                Ok(ConnectionFolderItemDto {
                    id: row.get(0)?,
                    folder_id: row.get(1)?,
                    profile_id: row.get(2)?,
                    position: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for item in items {
            match item {
                Ok(i) => result.push(i),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            });
        }

        Ok(result)
    }

    /// Fetches all items (folder -> profile relationships).
    pub fn all_items(&self) -> Result<Vec<ConnectionFolderItemDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, folder_id, profile_id, position
                FROM cfg_connection_folder_items
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let items = stmt
            .query_map([], |row| {
                Ok(ConnectionFolderItemDto {
                    id: row.get(0)?,
                    folder_id: row.get(1)?,
                    profile_id: row.get(2)?,
                    position: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for item in items {
            match item {
                Ok(i) => result.push(i),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            });
        }

        Ok(result)
    }

    /// Fetches an item by profile ID.
    pub fn get_item_by_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<ConnectionFolderItemDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, folder_id, profile_id, position
                FROM cfg_connection_folder_items
                WHERE profile_id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let item = stmt
            .query_row(params![profile_id], |row| {
                Ok(ConnectionFolderItemDto {
                    id: row.get(0)?,
                    folder_id: row.get(1)?,
                    profile_id: row.get(2)?,
                    position: row.get(3)?,
                })
            })
            .ok();

        Ok(item)
    }

    /// Inserts a new folder item.
    pub fn insert_item(&self, dto: &ConnectionFolderItemDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT OR IGNORE INTO cfg_connection_folder_items (id, folder_id, profile_id, position)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![dto.id, dto.folder_id, dto.profile_id, dto.position],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Updates an existing folder item.
    pub fn update_item(&self, dto: &ConnectionFolderItemDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                UPDATE cfg_connection_folder_items
                SET folder_id = ?2, profile_id = ?3, position = ?4
                WHERE id = ?1
                "#,
                params![dto.id, dto.folder_id, dto.profile_id, dto.position],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Deletes a folder item.
    pub fn delete_item(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM cfg_connection_folder_items WHERE id = ?1",
                params![id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Deletes a folder item by profile ID.
    pub fn delete_item_by_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM cfg_connection_folder_items WHERE profile_id = ?1",
                params![profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Clears all folders and items.
    pub fn clear_all(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM cfg_connection_folder_items", [])
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        self.conn()
            .execute("DELETE FROM cfg_connection_folders", [])
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Saves the entire tree (folders and items) from a ConnectionTree.
    /// This clears existing data and replaces it with the new tree.
    pub fn save_tree(&self, tree: &dbflux_core::ConnectionTree) -> Result<(), StorageError> {
        // Clear existing data
        self.clear_all()?;

        self.insert_folder(&Self::root_folder_sentinel())?;

        // Collect all folders and sort by depth (root folders first, then children)
        // This ensures parent folders are inserted before their children
        let mut folders: Vec<_> = tree.nodes.iter().filter(|n| n.is_folder()).collect();

        // Sort folders by depth: calculate depth for each folder and sort
        fn calculate_depth(
            node: &dbflux_core::ConnectionTreeNode,
            tree: &dbflux_core::ConnectionTree,
        ) -> i32 {
            match node.parent_id {
                None => 0,
                Some(parent_id) => {
                    if let Some(parent) = tree.nodes.iter().find(|n| n.id == parent_id) {
                        calculate_depth(parent, tree) + 1
                    } else {
                        0
                    }
                }
            }
        }

        folders.sort_by_key(|n| calculate_depth(n, tree));

        // Insert folders in depth order (parents first)
        for node in folders {
            let dto = ConnectionFolderDto {
                id: node.id.to_string(),
                parent_id: node.parent_id.map(|p| p.to_string()),
                name: node.name.clone(),
                position: node.sort_index,
                collapsed: node.collapsed,
                created_at: chrono::Utc::now().to_rfc3339(),
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            self.insert_folder(&dto)?;
        }

        // Insert folder items (connection refs)
        for node in tree.nodes.iter().filter(|n| n.is_connection_ref()) {
            if let Some(profile_id) = node.profile_id {
                let profile_id_str = profile_id.to_string();
                let folder_id = node
                    .parent_id
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| ROOT_FOLDER_SENTINEL_ID.to_string());

                // Check if the profile exists in cfg_connection_profiles before inserting
                // (it might not exist if it was deleted or never imported)
                if !self.profile_exists(&profile_id_str) {
                    info!(
                        "Skipping folder item for non-existent profile '{}' in folder '{}'",
                        profile_id_str, folder_id
                    );
                    continue;
                }

                let item = ConnectionFolderItemDto {
                    id: Uuid::new_v4().to_string(),
                    folder_id,
                    profile_id: profile_id_str,
                    position: node.sort_index,
                };
                self.insert_item(&item)?;
            }
        }

        info!(
            "Saved connection tree with {} folders and {} items",
            tree.folders().len(),
            tree.nodes.iter().filter(|n| n.is_connection_ref()).count()
        );

        Ok(())
    }

    /// Loads a ConnectionTree from the database.
    pub fn load_tree(&self) -> Result<dbflux_core::TreeLoadResult, StorageError> {
        use dbflux_core::ConnectionTreeNode;
        use dbflux_core::ConnectionTreeNodeKind;

        let mut tree = dbflux_core::ConnectionTree::new();
        let mut recovered_from_error = false;

        // Load folders as nodes
        let folders = self.all_folders()?;
        for folder in folders {
            let folder_id = match Self::parse_uuid(&folder.id, "folder.id") {
                Ok(folder_id) => folder_id,
                Err(error) => {
                    recovered_from_error = true;
                    warn!(
                        "Skipping invalid connection folder '{}': {}",
                        folder.id, error
                    );
                    continue;
                }
            };

            let parent_id = match Self::parse_optional_uuid(folder.parent_id, "folder.parent_id") {
                Ok(parent_id) => parent_id,
                Err(error) => {
                    recovered_from_error = true;
                    warn!(
                        "Loading connection folder '{}' at root because its parent is invalid: {}",
                        folder_id, error
                    );
                    None
                }
            };

            let node = ConnectionTreeNode {
                id: folder_id,
                kind: ConnectionTreeNodeKind::Folder,
                parent_id,
                sort_index: folder.position,
                name: folder.name,
                profile_id: None,
                collapsed: folder.collapsed,
            };
            tree.add_node(node);
        }

        // Load items as connection ref nodes
        let items = self.all_items()?;
        for item in items {
            let parent_id = if item.folder_id.is_empty()
                || Self::is_root_folder_sentinel(&item.folder_id)
            {
                None
            } else {
                match Self::parse_uuid(&item.folder_id, "folder_item.folder_id") {
                    Ok(folder_id) => Some(folder_id),
                    Err(error) => {
                        recovered_from_error = true;
                        warn!(
                            "Loading connection folder item '{}' at root because its folder is invalid: {}",
                            item.id, error
                        );
                        None
                    }
                }
            };

            let profile_id = match Self::parse_uuid(&item.profile_id, "folder_item.profile_id") {
                Ok(profile_id) => profile_id,
                Err(error) => {
                    recovered_from_error = true;
                    warn!(
                        "Skipping connection folder item '{}' because its profile id is invalid: {}",
                        item.id, error
                    );
                    continue;
                }
            };

            let node = ConnectionTreeNode {
                id: Uuid::new_v4(),
                kind: ConnectionTreeNodeKind::ConnectionRef,
                parent_id,
                sort_index: item.position,
                name: String::new(),
                profile_id: Some(profile_id),
                collapsed: false,
            };
            tree.add_node(node);
        }

        let repaired_orphans = tree.repair_orphans();
        if repaired_orphans > 0 {
            recovered_from_error = true;
            warn!(
                "Repaired {} orphaned connection tree nodes after loading persisted folders",
                repaired_orphans
            );
        }

        Ok(dbflux_core::TreeLoadResult {
            tree,
            recovered_from_error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::StorageRuntime;

    fn temp_repo() -> (tempfile::TempDir, ConnectionFoldersRepository) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let runtime = StorageRuntime::for_path(temp_dir.path().join("dbflux.db")).expect("runtime");

        let repo = ConnectionFoldersRepository::new(runtime.dbflux_db());

        (temp_dir, repo)
    }

    #[test]
    fn load_tree_skips_invalid_folder_item_profile_uuid_without_failing() {
        let (_temp_dir, repo) = temp_repo();
        let folder_id = Uuid::new_v4().to_string();
        let valid_profile_id = Uuid::new_v4().to_string();

        repo.insert_folder(&ConnectionFolderDto {
            id: folder_id.clone(),
            parent_id: None,
            name: "Work".to_string(),
            position: 0,
            collapsed: false,
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        })
        .expect("insert folder");

        repo.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_profiles (
                    id, name, driver_id, kind, created_at, updated_at
                ) VALUES (?1, 'Valid Profile', 'postgres', 'postgres', datetime('now'), datetime('now'))
                "#,
                params![valid_profile_id],
            )
            .expect("insert valid profile id row");

        repo.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_profiles (
                    id, name, driver_id, kind, created_at, updated_at
                ) VALUES (?1, 'Broken Profile', 'postgres', 'postgres', datetime('now'), datetime('now'))
                "#,
                params!["not-a-uuid"],
            )
            .expect("insert invalid profile id row");

        repo.conn()
            .execute(
                "INSERT INTO cfg_connection_folder_items (id, folder_id, profile_id, position) VALUES (?1, ?2, ?3, 1000)",
                params![Uuid::new_v4().to_string(), folder_id, valid_profile_id],
            )
            .expect("insert valid folder item");

        repo.conn()
            .execute(
                "INSERT INTO cfg_connection_folder_items (id, folder_id, profile_id, position) VALUES (?1, ?2, ?3, 0)",
                params![Uuid::new_v4().to_string(), folder_id, "not-a-uuid"],
            )
            .expect("insert invalid folder item");

        let load_result = repo
            .load_tree()
            .expect("invalid folder item should be skipped, not fail");

        assert!(load_result.recovered_from_error);
        assert_eq!(load_result.tree.folders().len(), 1);
        assert_eq!(
            load_result
                .tree
                .nodes
                .iter()
                .filter(|node| node.is_connection_ref())
                .count(),
            1
        );
    }

    #[test]
    fn load_tree_repairs_invalid_folder_references_without_failing() {
        let (_temp_dir, repo) = temp_repo();
        let valid_folder_id = Uuid::new_v4().to_string();
        let valid_profile_id = Uuid::new_v4().to_string();

        repo.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_folders (
                    id, parent_id, name, position, collapsed, created_at, updated_at
                ) VALUES (?1, NULL, 'Broken Parent', 0, 0, datetime('now'), datetime('now'))
                "#,
                params!["not-a-uuid"],
            )
            .expect("insert invalid parent folder");

        repo.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_folders (
                    id, parent_id, name, position, collapsed, created_at, updated_at
                ) VALUES (?1, NULL, 'Valid Folder', 500, 0, datetime('now'), datetime('now'))
                "#,
                params![valid_folder_id],
            )
            .expect("insert valid folder");

        repo.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_folders (
                    id, parent_id, name, position, collapsed, created_at, updated_at
                ) VALUES (?1, ?2, 'Broken Child', 1000, 0, datetime('now'), datetime('now'))
                "#,
                params![Uuid::new_v4().to_string(), "not-a-uuid"],
            )
            .expect("insert invalid child folder");

        repo.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_profiles (
                    id, name, driver_id, kind, created_at, updated_at
                ) VALUES (?1, 'Valid Profile', 'postgres', 'postgres', datetime('now'), datetime('now'))
                "#,
                params![valid_profile_id],
            )
            .expect("insert valid profile");

        repo.conn()
            .execute(
                "INSERT INTO cfg_connection_folder_items (id, folder_id, profile_id, position) VALUES (?1, ?2, ?3, 0)",
                params![Uuid::new_v4().to_string(), valid_folder_id, valid_profile_id],
            )
            .expect("insert valid folder item");

        let load_result = repo
            .load_tree()
            .expect("invalid folders should be repaired, not fail");

        let folders = load_result.tree.folders();
        let repaired_child = folders
            .iter()
            .find(|folder| folder.name == "Broken Child")
            .expect("broken child folder should be kept");

        assert!(load_result.recovered_from_error);
        assert_eq!(folders.len(), 2);
        assert!(repaired_child.parent_id.is_none());
    }
}
