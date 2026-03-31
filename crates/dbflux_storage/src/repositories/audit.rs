//! Repository for audit events in the unified database.
//!
//! Uses the `aud_audit_events` table from the unified schema.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::error::RepositoryError;
use crate::repositories::traits::Repository;

/// DTO for audit events stored in the unified database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventDto {
    pub id: i64,
    pub actor_id: String,
    pub tool_id: String,
    pub decision: String,
    pub reason: Option<String>,
    pub profile_id: Option<String>,
    pub classification: Option<String>,
    pub duration_ms: Option<i64>,
    pub created_at: String,
    pub created_at_epoch_ms: i64,
}

/// Filter for querying audit events.
#[derive(Debug, Clone, Default)]
pub struct AuditQueryFilter {
    pub id: Option<i64>,
    pub actor_id: Option<String>,
    pub tool_id: Option<String>,
    pub decision: Option<String>,
    pub profile_id: Option<String>,
    pub classification: Option<String>,
    pub start_epoch_ms: Option<i64>,
    pub end_epoch_ms: Option<i64>,
    pub limit: Option<usize>,
}

/// Input struct for appending an audit event.
#[derive(Debug, Clone)]
pub struct AppendAuditEvent<'a> {
    pub actor_id: &'a str,
    pub tool_id: &'a str,
    pub decision: &'a str,
    pub reason: Option<&'a str>,
    pub profile_id: Option<&'a str>,
    pub classification: Option<&'a str>,
    pub duration_ms: Option<i64>,
    pub created_at_epoch_ms: i64,
}

/// Repository for audit events.
pub struct AuditRepository {
    conn: Arc<Mutex<Connection>>,
}

impl AuditRepository {
    /// Creates a new repository with the given connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Appends a new audit event and returns the created record.
    pub fn append(&self, event: AppendAuditEvent<'_>) -> Result<AuditEventDto, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        conn.execute(
            r#"
            INSERT INTO aud_audit_events (
                actor_id, tool_id, decision, reason,
                profile_id, classification, duration_ms, created_at_epoch_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                event.actor_id,
                event.tool_id,
                event.decision,
                event.reason,
                event.profile_id,
                event.classification,
                event.duration_ms,
                event.created_at_epoch_ms
            ],
        )?;

        let id = conn.last_insert_rowid();

        Ok(AuditEventDto {
            id,
            actor_id: event.actor_id.to_string(),
            tool_id: event.tool_id.to_string(),
            decision: event.decision.to_string(),
            reason: event.reason.map(ToOwned::to_owned),
            profile_id: event.profile_id.map(ToOwned::to_owned),
            classification: event.classification.map(ToOwned::to_owned),
            duration_ms: event.duration_ms,
            created_at: chrono::Utc::now().to_rfc3339(),
            created_at_epoch_ms: event.created_at_epoch_ms,
        })
    }

    /// Queries audit events with the given filter.
    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEventDto>, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        let mut sql = String::from(
            "SELECT id, actor_id, tool_id, decision, reason,
                    profile_id, classification, duration_ms, created_at, created_at_epoch_ms
             FROM aud_audit_events",
        );

        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(id) = filter.id {
            conditions.push("id = ?");
            values.push(Box::new(id));
        }

        if let Some(ref actor_id) = filter.actor_id {
            conditions.push("actor_id = ?");
            values.push(Box::new(actor_id.clone()));
        }

        if let Some(ref tool_id) = filter.tool_id {
            conditions.push("tool_id = ?");
            values.push(Box::new(tool_id.clone()));
        }

        if let Some(ref decision) = filter.decision {
            conditions.push("decision = ?");
            values.push(Box::new(decision.clone()));
        }

        if let Some(ref profile_id) = filter.profile_id {
            conditions.push("profile_id = ?");
            values.push(Box::new(profile_id.clone()));
        }

        if let Some(ref classification) = filter.classification {
            conditions.push("classification = ?");
            values.push(Box::new(classification.clone()));
        }

        if let Some(start) = filter.start_epoch_ms {
            conditions.push("created_at_epoch_ms >= ?");
            values.push(Box::new(start));
        }

        if let Some(end) = filter.end_epoch_ms {
            conditions.push("created_at_epoch_ms <= ?");
            values.push(Box::new(end));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY id ASC");

        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        let mut rows = stmt.query(params_refs.as_slice())?;

        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(AuditEventDto {
                id: row.get(0)?,
                actor_id: row.get(1)?,
                tool_id: row.get(2)?,
                decision: row.get(3)?,
                reason: row.get(4)?,
                profile_id: row.get(5)?,
                classification: row.get(6)?,
                duration_ms: row.get(7)?,
                created_at: row.get(8)?,
                created_at_epoch_ms: row.get(9)?,
            });
        }

        Ok(events)
    }

    /// Returns the count of audit events.
    pub fn count(&self) -> Result<i64, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM aud_audit_events", [], |row| {
            row.get(0)
        })?;
        Ok(count)
    }

    /// Clears all audit events.
    pub fn clear(&self) -> Result<(), RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;
        conn.execute("DELETE FROM aud_audit_events", [])?;
        Ok(())
    }

    /// Finds an audit event by ID.
    pub fn find_by_id(&self, id: i64) -> Result<Option<AuditEventDto>, RepositoryError> {
        let filter = AuditQueryFilter {
            id: Some(id),
            ..Default::default()
        };
        let mut events = self.query(&filter)?;
        if events.is_empty() {
            return Ok(None);
        }
        Ok(Some(events.remove(0)))
    }
}

impl Repository for AuditRepository {
    type Entity = AuditEventDto;
    type Id = i64;

    fn all(&self) -> Result<Vec<Self::Entity>, RepositoryError> {
        self.query(&AuditQueryFilter {
            limit: Some(10000),
            ..Default::default()
        })
    }

    fn find_by_id(&self, id: &Self::Id) -> Result<Option<Self::Entity>, RepositoryError> {
        self.find_by_id(*id)
    }

    fn upsert(&self, _entity: &Self::Entity) -> Result<(), RepositoryError> {
        // Audit events are append-only; upsert is not applicable
        Err(RepositoryError::NotFound(
            "Audit events are append-only and do not support upsert".to_string(),
        ))
    }

    fn delete(&self, id: &Self::Id) -> Result<(), RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;
        conn.execute("DELETE FROM aud_audit_events WHERE id = ?1", [id])?;
        Ok(())
    }
}
