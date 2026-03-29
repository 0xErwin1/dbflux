//! Repository for session and tab metadata in state.db.
//!
//! Session metadata (which tabs are open, their kind, paths, positions, active index)
//! lives in `state.db`. Actual file content (scratch files, shadow files) stays on disk
//! in `~/.local/share/dbflux/sessions/` via `ArtifactStore`.
//!
//! `restore_session()` provides the authoritative session data for the app:
//! it returns a `SessionManifest` (the same shape the old JSON-based `SessionStore` used)
//! so that callers in `actions.rs` and elsewhere don't need to change.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

use crate::artifacts::ArtifactStore;
use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Payload stored in `session_tabs.restore_payload_json` — enough to reconstruct a `SessionTab`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TabRestorePayload {
    id: String,
    tab_kind: String, // "Scratch" or "FileBacked"
    language: String,
    exec_ctx_json: String,
    title: String,
    scratch_path: Option<String>,
    shadow_path: Option<String>,
    file_path: Option<String>,
    position: i32,
    is_pinned: bool,
}

/// Full session data assembled from `sessions` + `session_tabs` rows.
#[derive(Debug, Clone)]
pub(crate) struct FullSession {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub active_index: Option<usize>,
    pub tabs: Vec<FullTab>,
}

#[derive(Debug, Clone)]
pub(crate) struct FullTab {
    pub id: String,
    pub title: String,
    pub tab_kind: String,
    pub language: String,
    pub position: i32,
    pub is_pinned: bool,
    pub scratch_file_path: Option<String>,
    pub shadow_file_path: Option<String>,
    /// Raw JSON stored in the `restore_payload_json` column, so callers can
    /// fully deserialize the tab metadata (including `file_path` and `exec_ctx_json`).
    pub restore_payload_json: Option<String>,
}

/// Session repository — manages session and tab metadata in state.db.
pub struct SessionRepository {
    conn: OwnedConnection,
}

impl SessionRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Returns all sessions ordered by `last_opened_at` descending.
    pub fn all(&self) -> Result<Vec<SessionDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, name, kind, created_at, updated_at, last_opened_at, is_last_active
                 FROM sessions ORDER BY last_opened_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(SessionDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    kind: row.get(2)?,
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                    last_opened_at: row.get(5)?,
                    is_last_active: row.get::<_, i32>(6)? != 0,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for row in rows {
            match row {
                Ok(r) => result.push(r),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "state.db".into(),
                source: e,
            });
        }

        Ok(result)
    }

    /// Returns the last-active session, if any.
    pub(crate) fn last_active(&self) -> Result<Option<FullSession>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id FROM sessions WHERE is_last_active = 1 ORDER BY last_opened_at DESC LIMIT 1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let session_id: Option<String> = stmt.query_row([], |row| row.get(0)).ok();

        match session_id {
            Some(id) => self.get_full_session(&id),
            None => Ok(None),
        }
    }

    /// Returns a full session by ID with its tabs assembled.
    pub(crate) fn get_full_session(&self, id: &str) -> Result<Option<FullSession>, StorageError> {
        let mut session_stmt = self
            .conn()
            .prepare(
                "SELECT id, name, kind, active_index, created_at, updated_at, last_opened_at
                 FROM sessions WHERE id = ?1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let session_row: Option<(String, String, String, Option<i64>, String, String, String)> =
            session_stmt
                .query_row([id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                })
                .ok();

        let Some((session_id, name, kind, db_active_index, _created_at, _updated_at, _last_opened)) =
            session_row
        else {
            return Ok(None);
        };

        // Convert from database INTEGER to Option<usize>.
        // We use the persisted active_index rather than inferring from tab positions.
        let active_index = db_active_index.map(|i| i as usize);

        let mut tab_stmt = self
            .conn()
            .prepare(
                "SELECT id, tab_kind, title, position, is_pinned, restore_payload_json,
                        scratch_file_path, shadow_file_path, created_at, updated_at
                 FROM session_tabs WHERE session_id = ?1 ORDER BY position ASC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let tab_rows = tab_stmt
            .query_map([&session_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i32>(3)?,
                    row.get::<_, i32>(4)? != 0,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                ))
            })
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut tabs = Vec::new();
        let mut last_err = None;

        for tab_row in tab_rows {
            match tab_row {
                Ok((
                    tab_id,
                    tab_kind,
                    title,
                    position,
                    is_pinned,
                    restore_payload_json,
                    scratch_file_path,
                    shadow_file_path,
                    _tab_created,
                    _tab_updated,
                )) => {
                    let language: String = serde_json::from_str(&restore_payload_json)
                        .ok()
                        .and_then(|p: TabRestorePayload| Some(p.language))
                        .unwrap_or_else(|| "sql".to_string());

                    tabs.push(FullTab {
                        id: tab_id,
                        title,
                        tab_kind,
                        language,
                        position,
                        is_pinned,
                        scratch_file_path,
                        shadow_file_path,
                        restore_payload_json: Some(restore_payload_json),
                    });
                }
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "state.db".into(),
                source: e,
            });
        }

        Ok(Some(FullSession {
            id: session_id,
            name,
            kind,
            active_index,
            tabs,
        }))
    }

    /// Upserts a session. Creates a new session or updates an existing one,
    /// clearing `is_last_active` on all others if this session becomes the active one.
    pub fn upsert(&self, dto: &SessionDto) -> Result<(), StorageError> {
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        if dto.is_last_active {
            tx.execute("UPDATE sessions SET is_last_active = 0", [])
                .map_err(|source| StorageError::Sqlite {
                    path: "state.db".into(),
                    source,
                })?;
        }

        tx.execute(
            r#"
            INSERT INTO sessions (id, name, kind, created_at, updated_at, last_opened_at, is_last_active)
            VALUES (?1, ?2, ?3, datetime('now'), datetime('now'), datetime('now'), ?4)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                kind = excluded.kind,
                updated_at = datetime('now'),
                last_opened_at = datetime('now'),
                is_last_active = excluded.is_last_active
            "#,
            params![dto.id, dto.name, dto.kind, dto.is_last_active as i32],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        info!("Upserted session: {} ({})", dto.name, dto.id);
        Ok(())
    }

    /// Inserts or updates a session tab. Replaces the full restore payload.
    pub fn upsert_tab(&self, dto: &SessionTabDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO session_tabs (id, session_id, tab_kind, title, position, is_pinned,
                                         restore_payload_json, scratch_file_path, shadow_file_path,
                                         created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'), datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    tab_kind = excluded.tab_kind,
                    title = excluded.title,
                    position = excluded.position,
                    is_pinned = excluded.is_pinned,
                    restore_payload_json = excluded.restore_payload_json,
                    scratch_file_path = excluded.scratch_file_path,
                    shadow_file_path = excluded.shadow_file_path,
                    updated_at = datetime('now')
                "#,
                params![
                    dto.id,
                    dto.session_id,
                    dto.tab_kind,
                    dto.title,
                    dto.position as i32,
                    dto.is_pinned as i32,
                    dto.restore_payload_json,
                    dto.scratch_file_path,
                    dto.shadow_file_path,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Removes a tab by ID.
    pub fn remove_tab(&self, tab_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM session_tabs WHERE id = ?1", [tab_id])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Removes all tabs for a session.
    pub fn clear_session_tabs(&self, session_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM session_tabs WHERE session_id = ?1",
                [session_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Deletes a session and its tabs (cascade).
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM sessions WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        info!("Deleted session: {}", id);
        Ok(())
    }

    /// Clears all sessions and tabs (for reset).
    pub fn clear_all(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM session_tabs", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        self.conn()
            .execute("DELETE FROM sessions", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Builds a `SessionManifest` from the last-active session in the database.
    ///
    /// This is the primary entry point for app startup: it returns the manifest
    /// the UI uses to restore tabs. Calls `artifact_store.cleanup_orphans()` with
    /// all scratch/shadow paths found in the manifest before returning.
    ///
    /// Returns `None` if there is no active session or if the session has no tabs.
    pub fn restore_session(
        &self,
        artifact_store: &ArtifactStore,
    ) -> Result<Option<RestoredSession>, StorageError> {
        let Some(session) = self.last_active()? else {
            return Ok(None);
        };

        let referenced_paths: Vec<std::path::PathBuf> = session
            .tabs
            .iter()
            .filter_map(|tab| {
                let scratch = tab.scratch_file_path.as_ref();
                let shadow = tab.shadow_file_path.as_ref();
                if scratch.is_some() {
                    scratch.map(PathBuf::from)
                } else {
                    shadow.map(PathBuf::from)
                }
            })
            .collect();

        artifact_store.cleanup_orphans(&referenced_paths);

        if session.tabs.is_empty() {
            return Ok(None);
        }

        let manifest = RestoredSession {
            id: session.id,
            name: session.name,
            kind: session.kind,
            active_index: session.active_index,
            tabs: session
                .tabs
                .into_iter()
                .map(|tab| {
                    // Deserialize the full restore payload so we can extract file_path,
                    // exec_ctx_json, and all other metadata that was saved.
                    let payload: TabRestorePayload =
                        serde_json::from_str(tab.restore_payload_json.as_deref().unwrap_or("{}"))
                            .unwrap_or_else(|_| TabRestorePayload::default());

                    RestoredTab {
                        id: tab.id,
                        title: tab.title,
                        tab_kind: tab.tab_kind,
                        language: tab.language,
                        scratch_path: tab.scratch_file_path.map(PathBuf::from),
                        shadow_path: tab.shadow_file_path.map(PathBuf::from),
                        file_path: payload.file_path.map(PathBuf::from),
                        exec_ctx_json: payload.exec_ctx_json,
                        position: tab.position,
                        is_pinned: tab.is_pinned,
                    }
                })
                .collect(),
        };

        Ok(Some(manifest))
    }

    /// Saves the current workspace session from the old JSON manifest shape.
    ///
    /// This bridges the gap: callers (e.g. `actions.rs`) already have a
    /// `SessionManifest` from the app's document state. We convert it to
    /// DB storage and replace the session+tabs atomically.
    ///
    /// We always reuse the single active workspace session so that repeated
    /// saves do not accumulate stale rows. If no workspace session exists yet,
    /// we create one with a stable UUID that persists across saves.
    pub fn save_workspace_session(
        &self,
        manifest: &WorkspaceSessionManifest,
    ) -> Result<(), StorageError> {
        // Reuse the single active workspace session, or create one with a stable ID.
        // We name the session "workspace" and kind "workspace" so we can find it again.
        let session_id = match self.find_workspace_session_id() {
            Some(id) => id,
            None => {
                let id = Uuid::new_v4().to_string();
                let tx =
                    self.conn()
                        .unchecked_transaction()
                        .map_err(|source| StorageError::Sqlite {
                            path: "state.db".into(),
                            source,
                        })?;

                tx.execute(
                    r#"
                    INSERT INTO sessions (id, name, kind, created_at, updated_at, last_opened_at, is_last_active)
                    VALUES (?1, 'workspace', 'workspace', datetime('now'), datetime('now'), datetime('now'), 1)
                    "#,
                    params![id],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "state.db".into(),
                    source,
                })?;

                tx.commit().map_err(|source| StorageError::Sqlite {
                    path: "state.db".into(),
                    source,
                })?;

                id
            }
        };

        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        // Deactivate all other sessions so this one is the sole active workspace.
        tx.execute("UPDATE sessions SET is_last_active = 0", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        // Mark our session as active and update its metadata, including active_index.
        tx.execute(
            r#"
            UPDATE sessions
            SET name = 'workspace',
                kind = 'workspace',
                active_index = ?2,
                updated_at = datetime('now'),
                last_opened_at = datetime('now'),
                is_last_active = 1
            WHERE id = ?1
            "#,
            params![
                session_id,
                manifest.active_index.map(|i| i as i64).unwrap_or(-1)
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        // Delete all existing tabs for this session so we can replace them cleanly.
        tx.execute(
            "DELETE FROM session_tabs WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        // Insert all tabs from the manifest.
        for tab in &manifest.tabs {
            let scratch_path_str: Option<String> = tab
                .scratch_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string());
            let shadow_path_str: Option<String> = tab
                .shadow_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string());
            let file_path_str: Option<String> = tab
                .file_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string());

            let exec_ctx_json =
                serde_json::to_string(&tab.exec_ctx).unwrap_or_else(|_| "{}".to_string());

            let payload = TabRestorePayload {
                id: tab.id.clone(),
                tab_kind: tab.tab_kind.clone(),
                language: tab.language.clone(),
                exec_ctx_json: exec_ctx_json.clone(),
                title: tab.title.clone(),
                scratch_path: scratch_path_str.clone(),
                shadow_path: shadow_path_str.clone(),
                file_path: file_path_str.clone(),
                position: tab.position as i32,
                is_pinned: tab.is_pinned,
            };

            let payload_json = serde_json::to_string(&payload)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .map_err(|source| StorageError::Io {
                    path: PathBuf::from("state.db"),
                    source,
                })?;

            tx.execute(
                r#"
                INSERT INTO session_tabs (id, session_id, tab_kind, title, position, is_pinned,
                                         restore_payload_json, scratch_file_path, shadow_file_path,
                                         created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'), datetime('now'))
                "#,
                params![
                    tab.id,
                    session_id,
                    tab.tab_kind,
                    tab.title,
                    tab.position as i32,
                    tab.is_pinned as i32,
                    payload_json,
                    scratch_path_str,
                    shadow_path_str,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        }

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        info!(
            "Saved workspace session with {} tabs (id={})",
            manifest.tabs.len(),
            session_id
        );
        Ok(())
    }

    /// Returns the ID of the single active workspace session, if any.
    fn find_workspace_session_id(&self) -> Option<String> {
        self.conn()
            .query_row(
                "SELECT id FROM sessions WHERE name = 'workspace' AND kind = 'workspace' AND is_last_active = 1 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok()
    }
}

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

/// DTO for a session row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDto {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub created_at: String,
    pub updated_at: String,
    pub last_opened_at: String,
    pub is_last_active: bool,
}

/// DTO for a session tab row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTabDto {
    pub id: String,
    pub session_id: String,
    pub tab_kind: String,
    pub title: String,
    pub position: usize,
    pub is_pinned: bool,
    pub restore_payload_json: String,
    pub scratch_file_path: Option<String>,
    pub shadow_file_path: Option<String>,
}

/// A session manifest restored from state.db.
///
/// This is what `actions.rs` expects — the same shape the old `SessionStore`
/// returned from `load_manifest()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoredSession {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub active_index: Option<usize>,
    pub tabs: Vec<RestoredTab>,
}

/// A tab restored from state.db.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoredTab {
    pub id: String,
    pub title: String,
    pub tab_kind: String,
    pub language: String,
    pub scratch_path: Option<std::path::PathBuf>,
    pub shadow_path: Option<std::path::PathBuf>,
    pub file_path: Option<std::path::PathBuf>,
    pub exec_ctx_json: String,
    pub position: i32,
    pub is_pinned: bool,
}

impl Default for TabRestorePayload {
    fn default() -> Self {
        Self {
            id: String::new(),
            tab_kind: "Scratch".to_string(),
            language: "sql".to_string(),
            exec_ctx_json: "{}".to_string(),
            title: String::new(),
            scratch_path: None,
            shadow_path: None,
            file_path: None,
            position: 0,
            is_pinned: false,
        }
    }
}

/// A session manifest used when saving from app state (workspace session).
///
/// This mirrors the old `SessionManifest` type from `dbflux_core::storage::session`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSessionManifest {
    pub version: u32,
    pub active_index: Option<usize>,
    pub tabs: Vec<WorkspaceTab>,
}

/// A tab when saving workspace state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceTab {
    pub id: String,
    pub tab_kind: String,
    pub language: String,
    pub exec_ctx: dbflux_core::ExecutionContext,
    pub scratch_path: Option<std::path::PathBuf>,
    pub shadow_path: Option<std::path::PathBuf>,
    pub file_path: Option<std::path::PathBuf>,
    pub title: String,
    pub position: usize,
    pub is_pinned: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::ArtifactStore;
    use crate::migrations::state::run_state_migrations;
    use crate::sqlite::open_database;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn temp_db(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_sessions_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn upsert_and_list_sessions() {
        let path = temp_db("upsert");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let dto = SessionDto {
            id: Uuid::new_v4().to_string(),
            name: "Test Session".to_string(),
            kind: "workspace".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_opened_at: chrono::Utc::now().to_rfc3339(),
            is_last_active: true,
        };

        repo.upsert(&dto).expect("should upsert");

        let all = repo.all().expect("should list");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "Test Session");
    }

    #[test]
    fn upsert_clears_last_active() {
        let path = temp_db("last_active");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let dto1 = SessionDto {
            id: Uuid::new_v4().to_string(),
            name: "First".to_string(),
            kind: "workspace".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_opened_at: chrono::Utc::now().to_rfc3339(),
            is_last_active: true,
        };
        repo.upsert(&dto1).expect("upsert first");

        let dto2 = SessionDto {
            id: Uuid::new_v4().to_string(),
            name: "Second".to_string(),
            kind: "workspace".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_opened_at: chrono::Utc::now().to_rfc3339(),
            is_last_active: true,
        };
        repo.upsert(&dto2).expect("upsert second");

        let all = repo.all().expect("should list");
        let active: Vec<_> = all.iter().filter(|s| s.is_last_active).collect();
        assert_eq!(active.len(), 1, "only one session should be last_active");
        assert_eq!(active[0].name, "Second");
    }

    #[test]
    fn upsert_tabs_and_restore() {
        let path = temp_db("tabs");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let session_id = Uuid::new_v4().to_string();
        let tab_id = Uuid::new_v4().to_string();
        let scratch_path = "/tmp/scratch-test.sql";

        repo.upsert(&SessionDto {
            id: session_id.clone(),
            name: "Test".to_string(),
            kind: "workspace".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_opened_at: chrono::Utc::now().to_rfc3339(),
            is_last_active: true,
        })
        .expect("upsert session");

        repo.upsert_tab(&SessionTabDto {
            id: tab_id.clone(),
            session_id: session_id.clone(),
            tab_kind: "Scratch".to_string(),
            title: "Query 1".to_string(),
            position: 0,
            is_pinned: false,
            restore_payload_json: r#"{"id":"tab-1","tab_kind":"Scratch","language":"sql","exec_ctx_json":"{}","title":"Query 1","scratch_path":null,"shadow_path":null,"file_path":null,"position":0,"is_pinned":false}"#.to_string(),
            scratch_file_path: Some(scratch_path.to_string()),
            shadow_file_path: None,
        }).expect("upsert tab");

        let full = repo
            .get_full_session(&session_id)
            .expect("get session")
            .expect("session exists");
        assert_eq!(full.tabs.len(), 1);
        assert_eq!(full.tabs[0].title, "Query 1");
    }

    #[test]
    fn save_and_restore_workspace_session() {
        let path = temp_db("workspace");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let manifest = WorkspaceSessionManifest {
            version: 1,
            active_index: Some(0),
            tabs: vec![WorkspaceTab {
                id: Uuid::new_v4().to_string(),
                tab_kind: "Scratch".to_string(),
                language: "sql".to_string(),
                exec_ctx: dbflux_core::ExecutionContext::default(),
                scratch_path: Some(PathBuf::from("/tmp/test-scratch.sql")),
                shadow_path: None,
                file_path: None,
                title: "Query 1".to_string(),
                position: 0,
                is_pinned: false,
            }],
        };

        repo.save_workspace_session(&manifest)
            .expect("save session");

        // Verify the session was saved
        let all = repo.all().expect("list sessions");
        assert_eq!(all.len(), 1);
        assert!(all[0].is_last_active);

        // Verify tab was saved
        let full = repo
            .get_full_session(&all[0].id)
            .expect("get")
            .expect("exists");
        assert_eq!(full.tabs.len(), 1);
    }

    #[test]
    fn delete_clears_session_and_tabs() {
        let path = temp_db("delete");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let session_id = Uuid::new_v4().to_string();
        repo.upsert(&SessionDto {
            id: session_id.clone(),
            name: "To Delete".to_string(),
            kind: "workspace".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_opened_at: chrono::Utc::now().to_rfc3339(),
            is_last_active: true,
        })
        .expect("upsert");

        repo.delete(&session_id).expect("delete");

        let all = repo.all().expect("list");
        assert_eq!(all.len(), 0);
    }

    #[test]
    fn restore_session_cleans_orphans_via_artifact_store() {
        // Integration test: session restore should trigger artifact orphan cleanup.
        // We create a temp artifact store, add a session with one referenced tab
        // and one orphan file, then verify only the orphan is removed.
        let path = temp_db("restore_orphan");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        // Create a temp artifact store directory
        let artifact_root = std::env::temp_dir().join(format!(
            "dbflux_test_artifacts_{}_{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let store = ArtifactStore::for_root(artifact_root.clone()).expect("temp store");

        // Create a session with one tab that references a scratch path
        let session_id = Uuid::new_v4().to_string();
        let tab_id = Uuid::new_v4().to_string();
        let scratch_file = store.scratch_path("tab-referenced", "sql");
        store
            .write_content(&scratch_file, "referenced content")
            .expect("write");

        repo.upsert(&SessionDto {
            id: session_id.clone(),
            name: "Test".to_string(),
            kind: "workspace".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_opened_at: chrono::Utc::now().to_rfc3339(),
            is_last_active: true,
        })
        .expect("upsert session");

        repo.upsert_tab(&SessionTabDto {
            id: tab_id.clone(),
            session_id: session_id.clone(),
            tab_kind: "Scratch".to_string(),
            title: "Test Tab".to_string(),
            position: 0,
            is_pinned: false,
            restore_payload_json: r#"{"id":"tab-1","tab_kind":"Scratch","language":"sql","exec_ctx_json":"{}","title":"Test Tab","scratch_path":null,"shadow_path":null,"file_path":null,"position":0,"is_pinned":false}"#.to_string(),
            scratch_file_path: Some(scratch_file.to_string_lossy().to_string()),
            shadow_file_path: None,
        }).expect("upsert tab");

        // Add an orphan file that is NOT referenced by any tab
        let orphan_file = store.scratch_path("tab-orphan", "sql");
        store
            .write_content(&orphan_file, "orphan content")
            .expect("write orphan");

        // Verify both files exist before restore
        assert!(
            scratch_file.exists(),
            "referenced file should exist before restore"
        );
        assert!(
            orphan_file.exists(),
            "orphan file should exist before restore"
        );

        // Restore session — this should clean up the orphan
        let result = repo
            .restore_session(&store)
            .expect("restore should succeed");
        assert!(result.is_some(), "should return a session");

        // Referenced file should still exist; orphan should be gone
        assert!(
            scratch_file.exists(),
            "referenced file should survive restore"
        );
        assert!(
            !orphan_file.exists(),
            "orphan file should be cleaned up after restore"
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&artifact_root);
    }

    #[test]
    fn save_and_restore_file_backed_tab() {
        // Verifies that file_path round-trips correctly through save and restore.
        let path = temp_db("file_backed");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        // Create a temp artifact store so we have a real file to reference.
        let artifact_root = std::env::temp_dir().join(format!(
            "dbflux_test_file_backed_{}_{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let store = ArtifactStore::for_root(artifact_root.clone()).expect("store");
        let file_path = store.scratch_path("my-script", "sql");
        store.write_content(&file_path, "SELECT 1;").expect("write");

        let exec_ctx = dbflux_core::ExecutionContext {
            connection_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            database: Some("testdb".into()),
            schema: Some("public".into()),
            container: None,
        };

        let manifest = WorkspaceSessionManifest {
            version: 1,
            active_index: Some(0),
            tabs: vec![WorkspaceTab {
                id: Uuid::new_v4().to_string(),
                tab_kind: "FileBacked".to_string(),
                language: "sql".to_string(),
                exec_ctx,
                scratch_path: None,
                shadow_path: None,
                file_path: Some(file_path.clone()),
                title: "my-script.sql".to_string(),
                position: 0,
                is_pinned: false,
            }],
        };

        repo.save_workspace_session(&manifest).expect("save");

        let restored = repo.restore_session(&store).expect("restore");
        let restored = restored.expect("should have a session");

        assert_eq!(restored.tabs.len(), 1);
        let tab = &restored.tabs[0];
        assert_eq!(tab.tab_kind, "FileBacked");
        assert!(tab.file_path.is_some(), "file_path must round-trip");
        assert_eq!(tab.file_path.as_ref().unwrap(), &file_path);

        // Verify exec_ctx round-tripped correctly.
        let exec_ctx_restored: dbflux_core::ExecutionContext =
            serde_json::from_str(&tab.exec_ctx_json).expect("exec_ctx_json must deserialize");
        assert_eq!(
            exec_ctx_restored.connection_id,
            Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
        );
        assert_eq!(exec_ctx_restored.database.as_deref(), Some("testdb"));
        assert_eq!(exec_ctx_restored.schema.as_deref(), Some("public"));

        let _ = std::fs::remove_dir_all(&artifact_root);
    }

    #[test]
    fn exec_context_roundtrip_in_restore_payload() {
        // Verifies that the full ExecutionContext survives through the JSON payload.
        let path = temp_db("exec_ctx");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let exec_ctx = dbflux_core::ExecutionContext {
            connection_id: Some(Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()),
            database: Some("analytics".into()),
            schema: Some("metrics".into()),
            container: Some("events".into()),
        };

        let manifest = WorkspaceSessionManifest {
            version: 1,
            active_index: Some(0),
            tabs: vec![WorkspaceTab {
                id: Uuid::new_v4().to_string(),
                tab_kind: "Scratch".to_string(),
                language: "sql".to_string(),
                exec_ctx,
                scratch_path: Some(PathBuf::from("/tmp/scratch-exec-ctx.sql")),
                shadow_path: None,
                file_path: None,
                title: "Query with context".to_string(),
                position: 0,
                is_pinned: false,
            }],
        };

        repo.save_workspace_session(&manifest).expect("save");

        // Restore and verify exec_ctx persisted — use the same Arc'd connection.
        let artifact_root = std::env::temp_dir().join(format!(
            "dbflux_test_exec_ctx2_{}_{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let store = ArtifactStore::for_root(artifact_root.clone()).expect("store");

        let restored = repo.restore_session(&store).expect("restore");
        let restored = restored.expect("should have a session");

        assert_eq!(restored.tabs.len(), 1);
        let tab = &restored.tabs[0];

        let restored_ctx: dbflux_core::ExecutionContext =
            serde_json::from_str(&tab.exec_ctx_json).expect("must deserialize");
        assert_eq!(
            restored_ctx.connection_id,
            Some(Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap())
        );
        assert_eq!(restored_ctx.database.as_deref(), Some("analytics"));
        assert_eq!(restored_ctx.schema.as_deref(), Some("metrics"));
        assert_eq!(restored_ctx.container.as_deref(), Some("events"));

        let _ = std::fs::remove_dir_all(&artifact_root);
    }

    #[test]
    fn active_index_persisted_and_restored() {
        // Verifies that active_index is stored on the session row and restored correctly.
        let path = temp_db("active_idx");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        let manifest = WorkspaceSessionManifest {
            version: 1,
            active_index: Some(2),
            tabs: vec![
                WorkspaceTab {
                    id: "tab-0".to_string(),
                    tab_kind: "Scratch".to_string(),
                    language: "sql".to_string(),
                    exec_ctx: dbflux_core::ExecutionContext::default(),
                    scratch_path: Some(PathBuf::from("/tmp/tab0.sql")),
                    shadow_path: None,
                    file_path: None,
                    title: "Query 0".to_string(),
                    position: 0,
                    is_pinned: false,
                },
                WorkspaceTab {
                    id: "tab-1".to_string(),
                    tab_kind: "Scratch".to_string(),
                    language: "sql".to_string(),
                    exec_ctx: dbflux_core::ExecutionContext::default(),
                    scratch_path: Some(PathBuf::from("/tmp/tab1.sql")),
                    shadow_path: None,
                    file_path: None,
                    title: "Query 1".to_string(),
                    position: 1,
                    is_pinned: false,
                },
                WorkspaceTab {
                    id: "tab-2".to_string(),
                    tab_kind: "Scratch".to_string(),
                    language: "sql".to_string(),
                    exec_ctx: dbflux_core::ExecutionContext::default(),
                    scratch_path: Some(PathBuf::from("/tmp/tab2.sql")),
                    shadow_path: None,
                    file_path: None,
                    title: "Query 2 — active".to_string(),
                    position: 2,
                    is_pinned: false,
                },
            ],
        };

        repo.save_workspace_session(&manifest).expect("save");

        // Restore and verify active_index
        let artifact_root = std::env::temp_dir().join(format!(
            "dbflux_test_active_idx_{}_{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let store = ArtifactStore::for_root(artifact_root.clone()).expect("store");

        let restored = repo.restore_session(&store).expect("restore");
        let restored = restored.expect("should have a session");

        assert_eq!(
            restored.active_index,
            Some(2),
            "active_index must be restored"
        );

        let _ = std::fs::remove_dir_all(&artifact_root);
    }

    #[test]
    fn repeated_saves_do_not_create_duplicate_sessions() {
        // Verifies that repeated save_workspace_session calls reuse the same
        // session row rather than accumulating new rows.
        let path = temp_db("repeat_save");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SessionRepository::new(Arc::new(conn));

        for i in 0..3 {
            let manifest = WorkspaceSessionManifest {
                version: 1,
                active_index: Some(0),
                tabs: vec![WorkspaceTab {
                    id: format!("tab-save-{}", i),
                    tab_kind: "Scratch".to_string(),
                    language: "sql".to_string(),
                    exec_ctx: dbflux_core::ExecutionContext::default(),
                    scratch_path: Some(PathBuf::from(format!("/tmp/scratch-save-{}.sql", i))),
                    shadow_path: None,
                    file_path: None,
                    title: format!("Save {}", i),
                    position: 0,
                    is_pinned: false,
                }],
            };

            repo.save_workspace_session(&manifest)
                .expect("save session");
        }

        // After 3 saves, there should still be exactly 1 session.
        let all = repo.all().expect("list sessions");
        assert_eq!(
            all.len(),
            1,
            "repeated saves must not create duplicate sessions — got {}",
            all.len()
        );

        // And the session should still be active.
        assert!(
            all[0].is_last_active,
            "workspace session must remain active"
        );

        // And get_full_session should work.
        let full = repo
            .get_full_session(&all[0].id)
            .expect("get full session")
            .expect("session exists");
        assert_eq!(full.tabs.len(), 1, "latest tab must be present");
        assert_eq!(
            full.tabs[0].title, "Save 2",
            "latest tab must be from last save"
        );
    }
}
