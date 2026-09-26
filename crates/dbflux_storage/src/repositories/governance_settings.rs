//! Repository for cfg_governance_settings and child tables in dbflux.db.
//!
//! These tables store the normalized governance settings as native columns,
//! replacing the JSON blob previously stored in app_settings.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing governance settings.
pub struct GovernanceSettingsRepository {
    conn: OwnedConnection,
}

impl GovernanceSettingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets the governance settings row.
    pub fn get(&self) -> Result<Option<GovernanceSettingsDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, mcp_enabled_by_default, updated_at
                FROM cfg_governance_settings WHERE id = 1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let result = stmt.query_row([], |row| {
            Ok(GovernanceSettingsDto {
                id: row.get(0)?,
                mcp_enabled_by_default: row.get(1)?,
                updated_at: row.get(2)?,
            })
        });

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            }),
        }
    }

    /// Upserts the governance settings.
    pub fn upsert(&self, settings: &GovernanceSettingsDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT OR IGNORE INTO cfg_governance_settings (id, mcp_enabled_by_default, updated_at)
                VALUES (1, ?1, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    mcp_enabled_by_default = excluded.mcp_enabled_by_default,
                    updated_at = datetime('now')
                "#,
                params![settings.mcp_enabled_by_default],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!("Upserted governance settings");
        Ok(())
    }

    /// Gets all trusted clients.
    pub fn get_trusted_clients(&self) -> Result<Vec<TrustedClientDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, governance_id, client_id, name, issuer, active
                FROM cfg_trusted_clients
                WHERE governance_id = 1
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(TrustedClientDto {
                    id: row.get(0)?,
                    governance_id: row.get(1)?,
                    client_id: row.get(2)?,
                    name: row.get(3)?,
                    issuer: row.get(4)?,
                    active: row.get(5)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Replaces all trusted clients.
    pub fn replace_trusted_clients(
        &self,
        clients: &[TrustedClientDto],
    ) -> Result<(), StorageError> {
        let in_transaction = !self.conn().is_autocommit();

        if in_transaction {
            // Already in a transaction, execute directly
            self.conn()
                .execute(
                    "DELETE FROM cfg_trusted_clients WHERE governance_id = 1",
                    [],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

            for client in clients {
                self.conn()
                    .execute(
                        r#"
                        INSERT INTO cfg_trusted_clients
                            (id, governance_id, client_id, name, issuer, active)
                        VALUES (?1, 1, ?2, ?3, ?4, ?5)
                        "#,
                        params![
                            client.id,
                            client.client_id,
                            client.name,
                            client.issuer,
                            client.active,
                        ],
                    )
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;
            }
        } else {
            let tx =
                self.conn()
                    .unchecked_transaction()
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;

            tx.execute(
                "DELETE FROM cfg_trusted_clients WHERE governance_id = 1",
                [],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

            for client in clients {
                tx.execute(
                    r#"
                    INSERT INTO cfg_trusted_clients
                        (id, governance_id, client_id, name, issuer, active)
                    VALUES (?1, 1, ?2, ?3, ?4, ?5)
                    "#,
                    params![
                        client.id,
                        client.client_id,
                        client.name,
                        client.issuer,
                        client.active,
                    ],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;
            }

            tx.commit().map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        }

        info!("Replaced {} trusted clients", clients.len());
        Ok(())
    }

    /// Gets all policy roles.
    pub fn get_policy_roles(&self) -> Result<Vec<PolicyRoleDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, governance_id, role_id
                FROM cfg_policy_roles
                WHERE governance_id = 1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(PolicyRoleDto {
                    id: row.get(0)?,
                    governance_id: row.get(1)?,
                    role_id: row.get(2)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Replaces all policy roles.
    pub fn replace_policy_roles(&self, roles: &[PolicyRoleDto]) -> Result<(), StorageError> {
        let in_transaction = !self.conn().is_autocommit();

        if in_transaction {
            self.conn()
                .execute("DELETE FROM cfg_policy_roles WHERE governance_id = 1", [])
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

            for role in roles {
                self.conn()
                    .execute(
                        r#"
                        INSERT INTO cfg_policy_roles (id, governance_id, role_id)
                        VALUES (?1, 1, ?2)
                        "#,
                        params![role.id, role.role_id],
                    )
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;
            }
        } else {
            let tx =
                self.conn()
                    .unchecked_transaction()
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;

            tx.execute("DELETE FROM cfg_policy_roles WHERE governance_id = 1", [])
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

            for role in roles {
                tx.execute(
                    r#"
                    INSERT INTO cfg_policy_roles (id, governance_id, role_id)
                    VALUES (?1, 1, ?2)
                    "#,
                    params![role.id, role.role_id],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;
            }

            tx.commit().map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        }

        info!("Replaced {} policy roles", roles.len());
        Ok(())
    }

    /// Gets all tool policies.
    ///
    /// After migration 0005, this reads the policy rows and joins with child tables
    /// to populate allowed_tools and allowed_classes; migration 034 adds
    /// approval_classes.
    pub fn get_tool_policies(&self) -> Result<Vec<ToolPolicyDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, governance_id, policy_id
                FROM cfg_tool_policies
                WHERE governance_id = 1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let policy_rows: Vec<(String, i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut result = Vec::new();
        for (id, governance_id, policy_id) in policy_rows {
            let allowed_tools = self.get_allowed_tools(&id)?;
            let allowed_classes = self.get_allowed_classes(&id)?;
            let approval_classes = self.get_approval_classes(&id)?;
            result.push(ToolPolicyDto {
                id,
                governance_id,
                policy_id,
                allowed_tools,
                allowed_classes,
                approval_classes,
            });
        }
        Ok(result)
    }

    /// Gets the allowed tools for a given tool policy.
    pub fn get_allowed_tools(&self, policy_id: &str) -> Result<Vec<String>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT tool_name FROM cfg_tool_policy_allowed_tools
                WHERE tool_policy_id = ?1
                ORDER BY tool_name
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([policy_id], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Gets the allowed classes for a given tool policy.
    pub fn get_allowed_classes(&self, policy_id: &str) -> Result<Vec<String>, StorageError> {
        self.get_policy_classes(ALLOWED_CLASSES_TABLE, policy_id)
    }

    /// Gets the classes that require approval for a given tool policy.
    pub fn get_approval_classes(&self, policy_id: &str) -> Result<Vec<String>, StorageError> {
        self.get_policy_classes(APPROVAL_CLASSES_TABLE, policy_id)
    }

    fn get_policy_classes(
        &self,
        table: &'static str,
        policy_id: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(&format!(
                "SELECT class_name FROM {table} WHERE tool_policy_id = ?1 ORDER BY class_name"
            ))
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([policy_id], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Replaces all allowed tools for a given tool policy.
    pub fn replace_allowed_tools(
        &self,
        policy_id: &str,
        tools: &[String],
    ) -> Result<(), StorageError> {
        let in_transaction = !self.conn().is_autocommit();

        if in_transaction {
            self.conn()
                .execute(
                    "DELETE FROM cfg_tool_policy_allowed_tools WHERE tool_policy_id = ?1",
                    [policy_id],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

            for tool in tools {
                let id = uuid::Uuid::new_v4().to_string();
                self.conn()
                    .execute(
                        "INSERT INTO cfg_tool_policy_allowed_tools (id, tool_policy_id, tool_name) VALUES (?1, ?2, ?3)",
                        params![id, policy_id, tool],
                    )
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;
            }
        } else {
            let tx =
                self.conn()
                    .unchecked_transaction()
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;

            tx.execute(
                "DELETE FROM cfg_tool_policy_allowed_tools WHERE tool_policy_id = ?1",
                [policy_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

            for tool in tools {
                let id = uuid::Uuid::new_v4().to_string();
                tx.execute(
                    "INSERT INTO cfg_tool_policy_allowed_tools (id, tool_policy_id, tool_name) VALUES (?1, ?2, ?3)",
                    params![id, policy_id, tool],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;
            }

            tx.commit().map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        }

        Ok(())
    }

    /// Replaces all allowed classes for a given tool policy.
    pub fn replace_allowed_classes(
        &self,
        policy_id: &str,
        classes: &[String],
    ) -> Result<(), StorageError> {
        self.replace_policy_classes(ALLOWED_CLASSES_TABLE, policy_id, classes)
    }

    /// Replaces all classes that require approval for a given tool policy.
    pub fn replace_approval_classes(
        &self,
        policy_id: &str,
        classes: &[String],
    ) -> Result<(), StorageError> {
        self.replace_policy_classes(APPROVAL_CLASSES_TABLE, policy_id, classes)
    }

    fn replace_policy_classes(
        &self,
        table: &'static str,
        policy_id: &str,
        classes: &[String],
    ) -> Result<(), StorageError> {
        let in_transaction = !self.conn().is_autocommit();

        if in_transaction {
            write_policy_classes(self.conn(), table, policy_id, classes)
        } else {
            let tx =
                self.conn()
                    .unchecked_transaction()
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;

            write_policy_classes(&tx, table, policy_id, classes)?;

            tx.commit().map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })
        }
    }

    /// Replaces all tool policies.
    ///
    /// This method handles both the parent cfg_tool_policies table and the
    /// child cfg_tool_policy_allowed_tools and cfg_tool_policy_allowed_classes tables.
    pub fn replace_tool_policies(&self, policies: &[ToolPolicyDto]) -> Result<(), StorageError> {
        let in_transaction = !self.conn().is_autocommit();

        if in_transaction {
            // Delete existing policies (cascade deletes child table rows via FK)
            self.conn()
                .execute("DELETE FROM cfg_tool_policies WHERE governance_id = 1", [])
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

            // Insert policies and their child rows
            for policy in policies {
                self.conn()
                    .execute(
                        r#"
                        INSERT INTO cfg_tool_policies (id, governance_id, policy_id)
                        VALUES (?1, 1, ?2)
                        "#,
                        params![policy.id, policy.policy_id],
                    )
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;

                // Insert allowed tools
                self.replace_allowed_tools(&policy.id, &policy.allowed_tools)?;

                // Insert allowed classes
                self.replace_allowed_classes(&policy.id, &policy.allowed_classes)?;
                self.replace_approval_classes(&policy.id, &policy.approval_classes)?;
            }
        } else {
            let tx =
                self.conn()
                    .unchecked_transaction()
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;

            tx.execute("DELETE FROM cfg_tool_policies WHERE governance_id = 1", [])
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

            for policy in policies {
                tx.execute(
                    r#"
                    INSERT INTO cfg_tool_policies (id, governance_id, policy_id)
                    VALUES (?1, 1, ?2)
                    "#,
                    params![policy.id, policy.policy_id],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;

                // Insert allowed tools
                for tool in &policy.allowed_tools {
                    let id = uuid::Uuid::new_v4().to_string();
                    tx.execute(
                        "INSERT INTO cfg_tool_policy_allowed_tools (id, tool_policy_id, tool_name) VALUES (?1, ?2, ?3)",
                        params![id, policy.id, tool],
                    )
                    .map_err(|source| StorageError::Sqlite {
                        path: "dbflux.db".into(),
                        source,
                    })?;
                }

                write_policy_classes(
                    &tx,
                    ALLOWED_CLASSES_TABLE,
                    &policy.id,
                    &policy.allowed_classes,
                )?;
                write_policy_classes(
                    &tx,
                    APPROVAL_CLASSES_TABLE,
                    &policy.id,
                    &policy.approval_classes,
                )?;
            }

            tx.commit().map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        }

        info!("Replaced {} tool policies", policies.len());
        Ok(())
    }
}

const ALLOWED_CLASSES_TABLE: &str = "cfg_tool_policy_allowed_classes";
const APPROVAL_CLASSES_TABLE: &str = "cfg_tool_policy_approval_classes";

/// Deletes the class rows of `policy_id` in `table` and inserts `classes`,
/// on a connection whose transaction the caller owns.
fn write_policy_classes(
    conn: &rusqlite::Connection,
    table: &'static str,
    policy_id: &str,
    classes: &[String],
) -> Result<(), StorageError> {
    conn.execute(
        &format!("DELETE FROM {table} WHERE tool_policy_id = ?1"),
        [policy_id],
    )
    .map_err(|source| StorageError::Sqlite {
        path: "dbflux.db".into(),
        source,
    })?;

    for class in classes {
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            &format!("INSERT INTO {table} (id, tool_policy_id, class_name) VALUES (?1, ?2, ?3)"),
            params![id, policy_id, class],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;
    }

    Ok(())
}

/// DTO for cfg_governance_settings table.
#[derive(Debug, Clone)]
pub struct GovernanceSettingsDto {
    pub id: i64,
    pub mcp_enabled_by_default: i32,
    pub updated_at: String,
}

/// DTO for cfg_trusted_clients table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedClientDto {
    pub id: String,
    pub governance_id: i64,
    pub client_id: String,
    pub name: String,
    pub issuer: Option<String>,
    pub active: i32,
}

/// DTO for cfg_policy_roles table.
#[derive(Debug, Clone)]
pub struct PolicyRoleDto {
    pub id: String,
    pub governance_id: i64,
    pub role_id: String,
}

/// DTO for cfg_tool_policies table.
///
/// After migration 0005, allowed_tools and allowed_classes are stored in
/// normalized child tables (cfg_tool_policy_allowed_tools and cfg_tool_policy_allowed_classes),
/// not as JSON columns on this struct. Migration 034 adds approval_classes in
/// cfg_tool_policy_approval_classes: a class in `allowed_classes` is Allow, a
/// class in `approval_classes` is Ask, and a class in neither is Deny.
#[derive(Debug, Clone)]
pub struct ToolPolicyDto {
    pub id: String,
    pub governance_id: i64,
    pub policy_id: String,
    pub allowed_tools: Vec<String>,
    pub allowed_classes: Vec<String>,
    pub approval_classes: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_governance_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn upsert_and_get_trusted_clients() {
        let path = temp_db("upsert_get");
        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = GovernanceSettingsRepository::new(Arc::new(conn));

        let dto = GovernanceSettingsDto {
            id: 1,
            mcp_enabled_by_default: 1,
            updated_at: String::new(),
        };
        repo.upsert(&dto)
            .expect("should upsert governance settings");

        let clients = vec![TrustedClientDto {
            id: uuid::Uuid::new_v4().to_string(),
            governance_id: 1,
            client_id: "client-1".to_string(),
            name: "Client One".to_string(),
            issuer: Some("issuer-1".to_string()),
            active: 1,
        }];
        repo.replace_trusted_clients(&clients)
            .expect("should replace clients");

        let fetched = repo.get().expect("should get").expect("should exist");
        assert_eq!(fetched.mcp_enabled_by_default, 1);

        let fetched_clients = repo.get_trusted_clients().expect("should get clients");
        assert_eq!(fetched_clients.len(), 1);
        assert_eq!(fetched_clients[0].client_id, "client-1");

        let _ = std::fs::remove_file(&path);
    }

    fn open_migrated(path: &std::path::Path) -> Connection {
        let conn = open_database(path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");
        conn
    }

    fn policy(policy_id: &str, allowed: &[&str], approval: &[&str]) -> ToolPolicyDto {
        ToolPolicyDto {
            id: uuid::Uuid::new_v4().to_string(),
            governance_id: 1,
            policy_id: policy_id.to_string(),
            allowed_tools: vec!["select_data".to_string()],
            allowed_classes: allowed.iter().map(|class| class.to_string()).collect(),
            approval_classes: approval.iter().map(|class| class.to_string()).collect(),
        }
    }

    fn seed_settings(repo: &GovernanceSettingsRepository) {
        repo.upsert(&GovernanceSettingsDto {
            id: 1,
            mcp_enabled_by_default: 1,
            updated_at: String::new(),
        })
        .expect("should upsert governance settings");
    }

    #[test]
    fn tool_policies_round_trip_allowed_and_approval_classes() {
        let path = temp_db("approval_round_trip");
        let conn = open_migrated(&path);

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = GovernanceSettingsRepository::new(Arc::new(conn));
        seed_settings(&repo);

        repo.replace_tool_policies(&[policy(
            "analyst",
            &["metadata", "read"],
            &["write", "destructive"],
        )])
        .expect("should replace policies");

        let fetched = repo.get_tool_policies().expect("should get policies");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].allowed_classes, vec!["metadata", "read"]);
        assert_eq!(fetched[0].approval_classes, vec!["destructive", "write"]);

        repo.replace_approval_classes(&fetched[0].id, &["admin".to_string()])
            .expect("should replace approval classes");
        assert_eq!(
            repo.get_approval_classes(&fetched[0].id)
                .expect("should get approval classes"),
            vec!["admin"]
        );

        let _ = std::fs::remove_file(&path);
    }

    /// Rolls a migrated database back to its shape before migration 034,
    /// writes policies the way a pre-034 binary did, and migrates again.
    #[test]
    fn migration_034_moves_allowed_mutating_classes_to_approval() {
        let path = temp_db("approval_migration");
        let conn = open_migrated(&path);

        conn.execute_batch(
            "
            DROP TABLE cfg_tool_policy_approval_classes;
            DELETE FROM sys_migrations WHERE name = '034_cfg_tool_policy_approval_classes';
            INSERT OR IGNORE INTO cfg_governance_settings (id, mcp_enabled_by_default, updated_at)
                VALUES (1, 1, datetime('now'));
            INSERT INTO cfg_tool_policies (id, governance_id, policy_id)
                VALUES ('row-everything', 1, 'everything'),
                       ('row-reader', 1, 'reader'),
                       ('row-writer', 1, 'writer');
            INSERT INTO cfg_tool_policy_allowed_tools (id, tool_policy_id, tool_name)
                VALUES ('tool-1', 'row-everything', 'drop_table'),
                       ('tool-2', 'row-reader', 'select_data'),
                       ('tool-3', 'row-writer', 'update_records');
            INSERT INTO cfg_tool_policy_allowed_classes (id, tool_policy_id, class_name)
                VALUES ('e-1', 'row-everything', 'metadata'),
                       ('e-2', 'row-everything', 'read'),
                       ('e-3', 'row-everything', 'write'),
                       ('e-4', 'row-everything', 'destructive'),
                       ('e-5', 'row-everything', 'admin_safe'),
                       ('e-6', 'row-everything', 'admin'),
                       ('e-7', 'row-everything', 'admin_destructive'),
                       ('r-1', 'row-reader', 'metadata'),
                       ('r-2', 'row-reader', 'read'),
                       ('w-1', 'row-writer', 'read'),
                       ('w-2', 'row-writer', 'write');
            ",
        )
        .expect("legacy state should be written");

        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration 034 should apply to legacy data");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = GovernanceSettingsRepository::new(Arc::new(conn));
        let mut fetched = repo.get_tool_policies().expect("should get policies");
        fetched.sort_by(|left, right| left.policy_id.cmp(&right.policy_id));

        let summary: Vec<(&str, Vec<String>, Vec<String>)> = fetched
            .iter()
            .map(|policy| {
                (
                    policy.policy_id.as_str(),
                    policy.allowed_classes.clone(),
                    policy.approval_classes.clone(),
                )
            })
            .collect();

        let strings = |values: &[&str]| -> Vec<String> {
            values.iter().map(|value| value.to_string()).collect()
        };

        assert_eq!(
            summary,
            vec![
                (
                    "everything",
                    strings(&["metadata", "read"]),
                    strings(&[
                        "admin",
                        "admin_destructive",
                        "admin_safe",
                        "destructive",
                        "write"
                    ]),
                ),
                ("reader", strings(&["metadata", "read"]), Vec::new()),
                ("writer", strings(&["read"]), strings(&["write"])),
            ]
        );
        assert_eq!(fetched[0].allowed_tools, vec!["drop_table"]);

        let _ = std::fs::remove_file(&path);
    }
}
