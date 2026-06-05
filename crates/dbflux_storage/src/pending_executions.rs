use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use dbflux_approval::{
    ExecutionPlan, PendingExecution, PendingExecutionStore, PendingStatus, PendingStoreError,
};
use rusqlite::Connection;
use uuid::Uuid;

/// SQLite-backed store for pending MCP executions awaiting human approval.
///
/// Holds a shared `Arc<Mutex<Connection>>` so it can be created cheaply from
/// the unified `StorageRuntime` connection without opening an extra file handle.
/// Access is serialized through the mutex; this matches the pattern used by
/// the viz repositories.
pub struct SqlitePendingExecutionStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqlitePendingExecutionStore {
    /// Creates a new store backed by the given shared connection.
    ///
    /// Migration 018 must have been applied before this is called (migration
    /// registry handles this during `StorageRuntime::for_path`).
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, PendingStoreError> {
        Ok(Self { conn })
    }
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn classification_to_str(c: dbflux_approval::store::PendingStatus) -> &'static str {
    match c {
        PendingStatus::Pending => "pending",
        PendingStatus::Approved => "approved",
        PendingStatus::Rejected => "rejected",
    }
}

fn status_from_str(s: &str) -> Result<PendingStatus, PendingStoreError> {
    match s {
        "pending" => Ok(PendingStatus::Pending),
        "approved" => Ok(PendingStatus::Approved),
        "rejected" => Ok(PendingStatus::Rejected),
        other => Err(PendingStoreError::Serialization(format!(
            "unknown pending status: {other}"
        ))),
    }
}

type RawRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    Option<i64>,
);

fn row_to_execution(raw: RawRow) -> Result<PendingExecution, PendingStoreError> {
    let (
        id_str,
        tool_id,
        connection_id,
        actor_id,
        class_json,
        payload_json,
        status_str,
        created_at,
        expires_at,
    ) = raw;

    let id = Uuid::parse_str(&id_str)
        .map_err(|e| PendingStoreError::Serialization(format!("invalid uuid in store: {e}")))?;

    let classification = serde_json::from_str(&class_json).map_err(|e| {
        PendingStoreError::Serialization(format!("classification parse error: {e}"))
    })?;

    let payload: serde_json::Value = serde_json::from_str(&payload_json)
        .map_err(|e| PendingStoreError::Serialization(format!("payload parse error: {e}")))?;

    let status = status_from_str(&status_str)?;

    Ok(PendingExecution {
        id,
        status,
        plan: ExecutionPlan {
            connection_id,
            actor_id,
            tool_id,
            classification,
            payload,
        },
        created_at,
        expires_at,
    })
}

impl PendingExecutionStore for SqlitePendingExecutionStore {
    fn create_pending(
        &mut self,
        plan: &ExecutionPlan,
        expires_at: Option<i64>,
    ) -> Result<PendingExecution, PendingStoreError> {
        let id = Uuid::new_v4();
        let created_at = now_epoch_ms();
        let classification_json = serde_json::to_string(&plan.classification)
            .map_err(|e| PendingStoreError::Serialization(e.to_string()))?;
        let payload_json = serde_json::to_string(&plan.payload)
            .map_err(|e| PendingStoreError::Serialization(e.to_string()))?;

        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        conn.execute(
            "INSERT INTO app_pending_executions
                (id, tool_id, connection_id, actor_id, classification, payload_json,
                 status, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?8)",
            rusqlite::params![
                id.to_string(),
                plan.tool_id,
                plan.connection_id,
                plan.actor_id,
                classification_json,
                payload_json,
                created_at,
                expires_at,
            ],
        )
        .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        Ok(PendingExecution {
            id,
            status: PendingStatus::Pending,
            plan: plan.clone(),
            created_at,
            expires_at,
        })
    }

    fn get_pending(&self, id: Uuid) -> Result<Option<PendingExecution>, PendingStoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let result = conn.query_row(
            "SELECT id, tool_id, connection_id, actor_id, classification, payload_json,
                    status, created_at, expires_at
             FROM app_pending_executions
             WHERE id = ?1",
            rusqlite::params![id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                ))
            },
        );

        match result {
            Ok(row) => Ok(Some(row_to_execution(row)?)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PendingStoreError::Backend(e.to_string())),
        }
    }

    fn update_status(
        &mut self,
        id: Uuid,
        status: PendingStatus,
    ) -> Result<Option<PendingExecution>, PendingStoreError> {
        let status_str = classification_to_str(status);
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let rows_changed = conn
            .execute(
                "UPDATE app_pending_executions SET status = ?1 WHERE id = ?2",
                rusqlite::params![status_str, id.to_string()],
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        if rows_changed == 0 {
            return Ok(None);
        }

        let result = conn
            .query_row(
                "SELECT id, tool_id, connection_id, actor_id, classification, payload_json,
                    status, created_at, expires_at
             FROM app_pending_executions
             WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                    ))
                },
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        Ok(Some(row_to_execution(result)?))
    }

    fn list_pending(&self) -> Result<Vec<PendingExecution>, PendingStoreError> {
        let now = now_epoch_ms();
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, tool_id, connection_id, actor_id, classification, payload_json,
                        status, created_at, expires_at
                 FROM app_pending_executions
                 WHERE status = 'pending'
                   AND (expires_at IS NULL OR expires_at > ?1)",
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let rows = stmt
            .query_map(rusqlite::params![now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                ))
            })
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let mut executions = Vec::new();
        for row in rows {
            let raw = row.map_err(|e| PendingStoreError::Backend(e.to_string()))?;
            executions.push(row_to_execution(raw)?);
        }

        Ok(executions)
    }
}
