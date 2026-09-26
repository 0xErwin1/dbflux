use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use dbflux_approval::{
    ExecutionPlan, PendingExecution, PendingExecutionStore, PendingStatus, PendingStoreError,
    approval_matches_plan,
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

fn status_to_str(c: PendingStatus) -> &'static str {
    match c {
        PendingStatus::Pending => "pending",
        PendingStatus::Approved => "approved",
        PendingStatus::Rejected => "rejected",
        PendingStatus::Consumed => "consumed",
    }
}

fn status_from_str(s: &str) -> Result<PendingStatus, PendingStoreError> {
    match s {
        "pending" => Ok(PendingStatus::Pending),
        "approved" => Ok(PendingStatus::Approved),
        "rejected" => Ok(PendingStatus::Rejected),
        "consumed" => Ok(PendingStatus::Consumed),
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
    Option<String>,
);

fn read_raw_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
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
        row.get::<_, Option<String>>(9)?,
    ))
}

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
        rejection_reason,
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
        rejection_reason,
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
            rejection_reason: None,
        })
    }

    fn get_pending(&self, id: Uuid) -> Result<Option<PendingExecution>, PendingStoreError> {
        let now = now_epoch_ms();
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let result = conn.query_row(
            "SELECT id, tool_id, connection_id, actor_id, classification, payload_json,
                    status, created_at, expires_at, rejection_reason
             FROM app_pending_executions
             WHERE id = ?1
               AND status = 'pending'
               AND (expires_at IS NULL OR expires_at > ?2)",
            rusqlite::params![id.to_string(), now],
            read_raw_row,
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
        let status_str = status_to_str(status);
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
                    status, created_at, expires_at, rejection_reason
             FROM app_pending_executions
             WHERE id = ?1",
                rusqlite::params![id.to_string()],
                read_raw_row,
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        Ok(Some(row_to_execution(result)?))
    }

    fn get_execution(&self, id: Uuid) -> Result<Option<PendingExecution>, PendingStoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let result = conn.query_row(
            "SELECT id, tool_id, connection_id, actor_id, classification, payload_json,
                    status, created_at, expires_at, rejection_reason
             FROM app_pending_executions
             WHERE id = ?1",
            rusqlite::params![id.to_string()],
            read_raw_row,
        );

        match result {
            Ok(row) => Ok(Some(row_to_execution(row)?)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PendingStoreError::Backend(e.to_string())),
        }
    }

    fn record_rejection(
        &mut self,
        id: Uuid,
        reason: Option<&str>,
    ) -> Result<Option<PendingExecution>, PendingStoreError> {
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

            let rows_changed = conn
                .execute(
                    "UPDATE app_pending_executions
                     SET status = 'rejected', rejection_reason = ?1
                     WHERE id = ?2",
                    rusqlite::params![reason, id.to_string()],
                )
                .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

            if rows_changed == 0 {
                return Ok(None);
            }
        }

        self.get_execution(id)
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
                        status, created_at, expires_at, rejection_reason
                 FROM app_pending_executions
                 WHERE status = 'pending'
                   AND (expires_at IS NULL OR expires_at > ?1)",
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let rows = stmt
            .query_map(rusqlite::params![now], read_raw_row)
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let mut executions = Vec::new();
        for row in rows {
            let raw = row.map_err(|e| PendingStoreError::Backend(e.to_string()))?;
            executions.push(row_to_execution(raw)?);
        }

        Ok(executions)
    }

    fn consume_approved(
        &mut self,
        plan: &ExecutionPlan,
    ) -> Result<Option<PendingExecution>, PendingStoreError> {
        let now = now_epoch_ms();
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, tool_id, connection_id, actor_id, classification, payload_json,
                        status, created_at, expires_at, rejection_reason
                 FROM app_pending_executions
                 WHERE status = 'approved'
                   AND actor_id = ?1
                   AND connection_id = ?2
                   AND tool_id = ?3
                   AND (expires_at IS NULL OR expires_at > ?4)
                 ORDER BY created_at",
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let rows = stmt
            .query_map(
                rusqlite::params![plan.actor_id, plan.connection_id, plan.tool_id, now],
                read_raw_row,
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let mut candidates = Vec::new();
        for row in rows {
            let raw = row.map_err(|e| PendingStoreError::Backend(e.to_string()))?;
            candidates.push(row_to_execution(raw)?);
        }
        drop(stmt);

        for mut candidate in candidates {
            if !approval_matches_plan(&candidate.plan, plan) {
                continue;
            }

            // The status guard makes the transition atomic against another
            // process consuming the same approval between the read and here.
            let rows_changed = conn
                .execute(
                    "UPDATE app_pending_executions SET status = 'consumed'
                     WHERE id = ?1 AND status = 'approved'",
                    rusqlite::params![candidate.id.to_string()],
                )
                .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

            if rows_changed == 1 {
                candidate.status = PendingStatus::Consumed;
                return Ok(Some(candidate));
            }
        }

        Ok(None)
    }

    fn purge_terminal_and_expired(&mut self, now_ms: i64) -> Result<usize, PendingStoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        let rows_deleted = conn
            .execute(
                "DELETE FROM app_pending_executions
                 WHERE status IN ('rejected', 'consumed')
                    OR (expires_at IS NOT NULL AND expires_at <= ?1)",
                rusqlite::params![now_ms],
            )
            .map_err(|e| PendingStoreError::Backend(e.to_string()))?;

        Ok(rows_deleted)
    }
}
