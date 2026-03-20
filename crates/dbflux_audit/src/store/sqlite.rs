use std::path::{Path, PathBuf};

use rusqlite::types::Value;
use rusqlite::{Connection, params, params_from_iter};

use crate::query::AuditQueryFilter;
use crate::{AuditError, AuditEvent};

pub struct SqliteAuditStore {
    path: PathBuf,
}

impl SqliteAuditStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = path.as_ref().to_path_buf();

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let store = Self { path };
        store.init_schema()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(
        &self,
        actor_id: &str,
        tool_id: &str,
        decision: &str,
        reason: Option<&str>,
        created_at_epoch_ms: i64,
    ) -> Result<AuditEvent, AuditError> {
        let conn = Connection::open(&self.path)?;

        conn.execute(
            "INSERT INTO audit_events (actor_id, tool_id, decision, reason, created_at_epoch_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![actor_id, tool_id, decision, reason, created_at_epoch_ms],
        )?;

        let id = conn.last_insert_rowid();

        Ok(AuditEvent {
            id,
            actor_id: actor_id.to_string(),
            tool_id: tool_id.to_string(),
            decision: decision.to_string(),
            reason: reason.map(ToOwned::to_owned),
            created_at_epoch_ms,
        })
    }

    pub fn get(&self, id: i64) -> Result<Option<AuditEvent>, AuditError> {
        let conn = Connection::open(&self.path)?;

        let mut statement = conn.prepare(
            "SELECT id, actor_id, tool_id, decision, reason, created_at_epoch_ms FROM audit_events WHERE id = ?1",
        )?;

        let mut rows = statement.query(params![id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        Ok(Some(AuditEvent {
            id: row.get(0)?,
            actor_id: row.get(1)?,
            tool_id: row.get(2)?,
            decision: row.get(3)?,
            reason: row.get(4)?,
            created_at_epoch_ms: row.get(5)?,
        }))
    }

    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEvent>, AuditError> {
        let mut sql = String::from(
            "SELECT id, actor_id, tool_id, decision, reason, created_at_epoch_ms FROM audit_events",
        );

        let mut clauses = Vec::new();
        let mut values = Vec::new();

        if let Some(actor_id) = &filter.actor_id {
            clauses.push("actor_id = ?".to_string());
            values.push(Value::from(actor_id.clone()));
        }

        if let Some(tool_id) = &filter.tool_id {
            clauses.push("tool_id = ?".to_string());
            values.push(Value::from(tool_id.clone()));
        }

        if let Some(decision) = &filter.decision {
            clauses.push("decision = ?".to_string());
            values.push(Value::from(decision.clone()));
        }

        if let Some(start) = filter.start_epoch_ms {
            clauses.push("created_at_epoch_ms >= ?".to_string());
            values.push(Value::from(start));
        }

        if let Some(end) = filter.end_epoch_ms {
            clauses.push("created_at_epoch_ms <= ?".to_string());
            values.push(Value::from(end));
        }

        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }

        sql.push_str(" ORDER BY id ASC");

        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        let conn = Connection::open(&self.path)?;
        let mut statement = conn.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(values.iter()))?;
        let mut events = Vec::new();

        while let Some(row) = rows.next()? {
            events.push(AuditEvent {
                id: row.get(0)?,
                actor_id: row.get(1)?,
                tool_id: row.get(2)?,
                decision: row.get(3)?,
                reason: row.get(4)?,
                created_at_epoch_ms: row.get(5)?,
            });
        }

        Ok(events)
    }

    fn init_schema(&self) -> Result<(), AuditError> {
        let conn = Connection::open(&self.path)?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_id TEXT NOT NULL,
                tool_id TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason TEXT,
                created_at_epoch_ms INTEGER NOT NULL
            );
            ",
        )?;

        Ok(())
    }
}
