//! Repository for audit events in the unified database.
//!
//! Uses the `aud_audit_events` table from the unified schema.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::error::RepositoryError;
use crate::repositories::traits::Repository;

/// Extended DTO for audit events stored in the unified database.
///
/// This extends the legacy MCP-only schema with the full RF-050/RF-051 fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventDto {
    pub id: i64,
    // Legacy MCP fields (still used)
    pub actor_id: String,
    pub tool_id: String,
    pub decision: String,
    pub reason: Option<String>,
    pub profile_id: Option<String>,
    pub classification: Option<String>,
    pub duration_ms: Option<i64>,
    pub created_at: String,
    pub created_at_epoch_ms: i64,
    // Extended RF-050/RF-051 fields
    pub level: Option<String>,
    pub category: Option<String>,
    pub action: Option<String>,
    pub outcome: Option<String>,
    pub actor_type: Option<String>,
    pub source_id: Option<String>,
    pub summary: Option<String>,
    pub connection_id: Option<String>,
    pub database_name: Option<String>,
    pub driver_id: Option<String>,
    pub object_type: Option<String>,
    pub object_id: Option<String>,
    pub details_json: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub session_id: Option<String>,
    pub correlation_id: Option<String>,
}

impl AuditEventDto {
    pub fn project_legacy_tool_id(action: Option<&str>, tool_id: Option<&str>) -> String {
        match action.filter(|value| !value.is_empty()) {
            Some("mcp_approve_execution") => "approve_execution".to_string(),
            Some("mcp_reject_execution") => "reject_execution".to_string(),
            Some(action) => action.to_string(),
            None => tool_id.unwrap_or_default().to_string(),
        }
    }

    pub fn project_legacy_decision(
        action: Option<&str>,
        outcome: Option<&str>,
        decision: Option<&str>,
    ) -> String {
        match (
            action.filter(|value| !value.is_empty()),
            outcome.filter(|value| !value.is_empty()),
        ) {
            (Some("mcp_approve_execution"), Some("success")) => "allow".to_string(),
            (Some("mcp_reject_execution"), Some("failure")) => "deny".to_string(),
            (_, Some("cancelled")) => "failure".to_string(),
            (_, Some("pending")) => "failure".to_string(),
            (_, Some(outcome)) => outcome.to_string(),
            _ => decision.unwrap_or_default().to_string(),
        }
    }

    pub fn legacy_tool_id(&self) -> String {
        Self::project_legacy_tool_id(self.action.as_deref(), Some(self.tool_id.as_str()))
    }

    pub fn legacy_decision(&self) -> String {
        Self::project_legacy_decision(
            self.action.as_deref(),
            self.outcome.as_deref(),
            Some(self.decision.as_str()),
        )
    }
}

/// Input struct for appending an audit event with extended fields.
#[derive(Debug, Clone)]
pub struct AppendAuditEventExtended<'a> {
    pub actor_id: &'a str,
    pub tool_id: &'a str,
    pub decision: &'a str,
    pub reason: Option<&'a str>,
    pub profile_id: Option<&'a str>,
    pub classification: Option<&'a str>,
    pub duration_ms: Option<i64>,
    pub created_at_epoch_ms: i64,
    // Extended fields
    pub level: Option<&'a str>,
    pub category: Option<&'a str>,
    pub action: Option<&'a str>,
    pub outcome: Option<&'a str>,
    pub actor_type: Option<&'a str>,
    pub source_id: Option<&'a str>,
    pub summary: Option<&'a str>,
    pub connection_id: Option<&'a str>,
    pub database_name: Option<&'a str>,
    pub driver_id: Option<&'a str>,
    pub object_type: Option<&'a str>,
    pub object_id: Option<&'a str>,
    pub details_json: Option<&'a str>,
    pub error_code: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub correlation_id: Option<&'a str>,
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
    pub offset: Option<usize>,
    // Extended filter fields
    pub level: Option<String>,
    pub category: Option<String>,
    pub action: Option<String>,
    /// Filter for multiple categories (OR'd together). Takes precedence over `category`.
    pub categories: Option<Vec<String>>,
    pub source_id: Option<String>,
    pub outcome: Option<String>,
    pub connection_id: Option<String>,
    pub driver_id: Option<String>,
    pub actor_type: Option<String>,
    pub object_type: Option<String>,
    pub free_text: Option<String>,
    /// Filter by correlation_id to find related events (for audit trails).
    pub correlation_id: Option<String>,
    /// Filter by session_id to find events in the same session.
    pub session_id: Option<String>,
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
#[derive(Clone)]
pub struct AuditRepository {
    conn: Arc<Mutex<Connection>>,
}

impl AuditRepository {
    fn canonical_tool_alias(tool_id: &str) -> Option<&'static str> {
        match tool_id {
            "approve_execution" => Some("mcp_approve_execution"),
            "reject_execution" => Some("mcp_reject_execution"),
            "mcp_approve_execution" => Some("approve_execution"),
            "mcp_reject_execution" => Some("reject_execution"),
            _ => None,
        }
    }

    fn canonical_decision_alias(decision: &str) -> Option<&'static str> {
        match decision {
            "allow" => Some("success"),
            "success" => Some("allow"),
            "deny" => Some("failure"),
            "failure" => Some("deny"),
            _ => None,
        }
    }

    fn build_where_clause(filter: &AuditQueryFilter) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
        let mut conditions: Vec<String> = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(id) = filter.id {
            conditions.push("id = ?".to_string());
            values.push(Box::new(id));
        }

        if let Some(ref actor_id) = filter.actor_id {
            conditions.push("actor_id = ?".to_string());
            values.push(Box::new(actor_id.clone()));
        }

        if let Some(ref tool_id) = filter.tool_id {
            if let Some(alias) = Self::canonical_tool_alias(tool_id) {
                conditions
                    .push("(tool_id = ? OR action = ? OR tool_id = ? OR action = ?)".to_string());
                values.push(Box::new(tool_id.clone()));
                values.push(Box::new(tool_id.clone()));
                values.push(Box::new(alias.to_string()));
                values.push(Box::new(alias.to_string()));
            } else {
                conditions.push("(tool_id = ? OR action = ?)".to_string());
                values.push(Box::new(tool_id.clone()));
                values.push(Box::new(tool_id.clone()));
            }
        }

        if let Some(ref decision) = filter.decision {
            if decision == "deny" {
                conditions.push(
                    "(decision = ? OR ((decision = ? OR outcome = ?) AND (COALESCE(tool_id, '') IN (?, ?) OR COALESCE(action, '') IN (?, ?))))".to_string(),
                );
                values.push(Box::new(decision.clone()));
                values.push(Box::new("failure".to_string()));
                values.push(Box::new("failure".to_string()));
                values.push(Box::new("reject_execution".to_string()));
                values.push(Box::new("mcp_reject_execution".to_string()));
                values.push(Box::new("reject_execution".to_string()));
                values.push(Box::new("mcp_reject_execution".to_string()));
            } else if decision == "allow" {
                conditions.push(
                    "(decision = ? OR ((outcome = ? OR decision = ?) AND (COALESCE(tool_id, '') IN (?, ?) OR COALESCE(action, '') IN (?, ?))))".to_string(),
                );
                values.push(Box::new(decision.clone()));
                values.push(Box::new("success".to_string()));
                values.push(Box::new("success".to_string()));
                values.push(Box::new("approve_execution".to_string()));
                values.push(Box::new("mcp_approve_execution".to_string()));
                values.push(Box::new("approve_execution".to_string()));
                values.push(Box::new("mcp_approve_execution".to_string()));
            } else if decision == "failure" {
                conditions.push(
                    "(decision = ? OR (outcome = ? AND COALESCE(tool_id, '') NOT IN (?, ?) AND COALESCE(action, '') NOT IN (?, ?)))".to_string(),
                );
                values.push(Box::new(decision.clone()));
                values.push(Box::new(decision.clone()));
                values.push(Box::new("reject_execution".to_string()));
                values.push(Box::new("mcp_reject_execution".to_string()));
                values.push(Box::new("reject_execution".to_string()));
                values.push(Box::new("mcp_reject_execution".to_string()));
            } else if let Some(alias) = Self::canonical_decision_alias(decision) {
                conditions.push(
                    "(decision = ? OR outcome = ? OR decision = ? OR outcome = ?)".to_string(),
                );
                values.push(Box::new(decision.clone()));
                values.push(Box::new(decision.clone()));
                values.push(Box::new(alias.to_string()));
                values.push(Box::new(alias.to_string()));
            } else {
                conditions.push("(decision = ? OR outcome = ?)".to_string());
                values.push(Box::new(decision.clone()));
                values.push(Box::new(decision.clone()));
            }
        }

        if let Some(ref profile_id) = filter.profile_id {
            conditions.push("profile_id = ?".to_string());
            values.push(Box::new(profile_id.clone()));
        }

        if let Some(ref classification) = filter.classification {
            conditions.push("classification = ?".to_string());
            values.push(Box::new(classification.clone()));
        }

        if let Some(start) = filter.start_epoch_ms {
            conditions.push("created_at_epoch_ms >= ?".to_string());
            values.push(Box::new(start));
        }

        if let Some(end) = filter.end_epoch_ms {
            conditions.push("created_at_epoch_ms <= ?".to_string());
            values.push(Box::new(end));
        }

        if let Some(ref level) = filter.level {
            conditions.push("level = ?".to_string());
            values.push(Box::new(level.clone()));
        }

        if let Some(ref action) = filter.action {
            conditions.push("action = ?".to_string());
            values.push(Box::new(action.clone()));
        }

        if let Some(ref categories) = filter.categories
            && !categories.is_empty()
        {
            let placeholders: Vec<String> = categories.iter().map(|_| "?".to_string()).collect();
            conditions.push(format!("category IN ({})", placeholders.join(", ")));
            for category in categories {
                values.push(Box::new(category.clone()));
            }
        } else if let Some(ref category) = filter.category {
            conditions.push("category = ?".to_string());
            values.push(Box::new(category.clone()));
        }

        if let Some(ref source_id) = filter.source_id {
            conditions.push("source_id = ?".to_string());
            values.push(Box::new(source_id.clone()));
        }

        if let Some(ref outcome) = filter.outcome {
            conditions.push("outcome = ?".to_string());
            values.push(Box::new(outcome.clone()));
        }

        if let Some(ref connection_id) = filter.connection_id {
            conditions.push("connection_id = ?".to_string());
            values.push(Box::new(connection_id.clone()));
        }

        if let Some(ref driver_id) = filter.driver_id {
            conditions.push("driver_id = ?".to_string());
            values.push(Box::new(driver_id.clone()));
        }

        if let Some(ref actor_type) = filter.actor_type {
            conditions.push("actor_type = ?".to_string());
            values.push(Box::new(actor_type.clone()));
        }

        if let Some(ref object_type) = filter.object_type {
            conditions.push("object_type = ?".to_string());
            values.push(Box::new(object_type.clone()));
        }

        if let Some(ref correlation_id) = filter.correlation_id {
            conditions.push("correlation_id = ?".to_string());
            values.push(Box::new(correlation_id.clone()));
        }

        if let Some(ref session_id) = filter.session_id {
            conditions.push("session_id = ?".to_string());
            values.push(Box::new(session_id.clone()));
        }

        if let Some(ref free_text) = filter.free_text {
            conditions.push(
                "(summary LIKE ? OR action LIKE ? OR error_message LIKE ? OR details_json LIKE ?)"
                    .to_string(),
            );
            let pattern = format!("%{}%", free_text);
            values.push(Box::new(pattern.clone()));
            values.push(Box::new(pattern.clone()));
            values.push(Box::new(pattern.clone()));
            values.push(Box::new(pattern));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        (where_clause, values)
    }

    /// Creates a new repository with the given connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Appends a new audit event with extended fields and returns the created record.
    pub fn append_extended(
        &self,
        event: AppendAuditEventExtended<'_>,
    ) -> Result<AuditEventDto, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        conn.execute(
            r#"
            INSERT INTO aud_audit_events (
                actor_id, tool_id, decision, reason,
                profile_id, classification, duration_ms, created_at_epoch_ms,
                level, category, action, outcome, actor_type, source_id, summary,
                connection_id, database_name, driver_id, object_type, object_id,
                details_json, error_code, error_message, session_id, correlation_id
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                ?16, ?17, ?18, ?19, ?20,
                ?21, ?22, ?23, ?24, ?25
            )
            "#,
            params![
                event.actor_id,
                event.tool_id,
                event.decision,
                event.reason,
                event.profile_id,
                event.classification,
                event.duration_ms,
                event.created_at_epoch_ms,
                event.level,
                event.category,
                event.action,
                event.outcome,
                event.actor_type,
                event.source_id,
                event.summary,
                event.connection_id,
                event.database_name,
                event.driver_id,
                event.object_type,
                event.object_id,
                event.details_json,
                event.error_code,
                event.error_message,
                event.session_id,
                event.correlation_id,
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
            level: event.level.map(ToOwned::to_owned),
            category: event.category.map(ToOwned::to_owned),
            action: event.action.map(ToOwned::to_owned),
            outcome: event.outcome.map(ToOwned::to_owned),
            actor_type: event.actor_type.map(ToOwned::to_owned),
            source_id: event.source_id.map(ToOwned::to_owned),
            summary: event.summary.map(ToOwned::to_owned),
            connection_id: event.connection_id.map(ToOwned::to_owned),
            database_name: event.database_name.map(ToOwned::to_owned),
            driver_id: event.driver_id.map(ToOwned::to_owned),
            object_type: event.object_type.map(ToOwned::to_owned),
            object_id: event.object_id.map(ToOwned::to_owned),
            details_json: event.details_json.map(ToOwned::to_owned),
            error_code: event.error_code.map(ToOwned::to_owned),
            error_message: event.error_message.map(ToOwned::to_owned),
            session_id: event.session_id.map(ToOwned::to_owned),
            correlation_id: event.correlation_id.map(ToOwned::to_owned),
        })
    }

    /// Queries audit events with the given filter.
    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEventDto>, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        let mut sql = String::from(
            "SELECT id, actor_id, tool_id, decision, reason,
                    profile_id, classification, duration_ms, created_at, created_at_epoch_ms,
                    level, category, action, outcome, actor_type, source_id, summary,
                    connection_id, database_name, driver_id, object_type, object_id,
                    details_json, error_code, error_message, session_id, correlation_id
             FROM aud_audit_events",
        );

        let (where_clause, values) = Self::build_where_clause(filter);
        sql.push_str(&where_clause);

        sql.push_str(" ORDER BY created_at_epoch_ms DESC, id DESC");

        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = filter.offset {
            if filter.limit.is_none() {
                sql.push_str(" LIMIT -1");
            }
            sql.push_str(&format!(" OFFSET {}", offset));
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
                level: row.get(10)?,
                category: row.get(11)?,
                action: row.get(12)?,
                outcome: row.get(13)?,
                actor_type: row.get(14)?,
                source_id: row.get(15)?,
                summary: row.get(16)?,
                connection_id: row.get(17)?,
                database_name: row.get(18)?,
                driver_id: row.get(19)?,
                object_type: row.get(20)?,
                object_id: row.get(21)?,
                details_json: row.get(22)?,
                error_code: row.get(23)?,
                error_message: row.get(24)?,
                session_id: row.get(25)?,
                correlation_id: row.get(26)?,
            });
        }

        Ok(events)
    }

    pub fn count_filtered(&self, filter: &AuditQueryFilter) -> Result<i64, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        let (where_clause, values) = Self::build_where_clause(filter);
        let sql = format!("SELECT COUNT(*) FROM aud_audit_events{}", where_clause);

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        let count = stmt.query_row(params_refs.as_slice(), |row| row.get(0))?;

        Ok(count)
    }

    /// Returns the count of audit events.
    pub fn count(&self) -> Result<i64, RepositoryError> {
        self.count_filtered(&AuditQueryFilter::default())
    }

    /// Clears all audit events.
    pub fn clear(&self) -> Result<(), RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;
        conn.execute("DELETE FROM aud_audit_events", [])?;
        Ok(())
    }

    /// Deletes audit events older than the given cutoff timestamp.
    ///
    /// ## Arguments
    ///
    /// * `cutoff_ms` - Unix timestamp in milliseconds
    /// * `limit` - Maximum number of events to delete
    ///
    /// ## Returns
    ///
    /// The number of events deleted.
    pub fn delete_older_than(&self, cutoff_ms: i64, limit: usize) -> Result<i64, RepositoryError> {
        let conn = self.conn.lock().map_err(|e| RepositoryError::Sqlite {
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        let deleted = conn.execute(
            "DELETE FROM aud_audit_events WHERE created_at_epoch_ms < ?1 LIMIT ?2",
            rusqlite::params![cutoff_ms, limit],
        )?;

        Ok(deleted as i64)
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

    /// Records a panic event using a non-blocking lock attempt.
    ///
    /// If the mutex cannot be acquired immediately (i.e., another thread holds it),
    /// this returns `Ok(None)` to indicate the fallback path should be used.
    /// If the lock is poisoned, logs and returns `Ok(None)`.
    ///
    /// # Arguments
    ///
    /// * `event` — The event to record (should be a `system_panic` event).
    /// * `panic_info_str` — A string describing the panic for the fallback log.
    ///
    /// # Returns
    ///
    /// `Ok(Some(_))` with the stored event ID on success.
    /// `Ok(None)` when the lock could not be acquired (caller should log fallback).
    /// `Err(_)` only on actual storage errors.
    pub fn try_record_panic(
        &self,
        event: AppendAuditEventExtended<'_>,
        panic_info_str: &str,
    ) -> Result<Option<i64>, RepositoryError> {
        use std::sync::TryLockError;

        // Arc<Mutex<T>>::try_lock returns Result<MutexGuard<'_, T>, TryLockError<MutexGuard<'_, T>>>
        match self.conn.try_lock() {
            Ok(conn) => {
                // Lock acquired — write the event inline
                conn.execute(
                    r#"
                    INSERT INTO aud_audit_events (
                        actor_id, tool_id, decision, reason,
                        profile_id, classification, duration_ms, created_at_epoch_ms,
                        level, category, action, outcome, actor_type, source_id, summary,
                        connection_id, database_name, driver_id, object_type, object_id,
                        details_json, error_code, error_message, session_id, correlation_id
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                        ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                        ?16, ?17, ?18, ?19, ?20,
                        ?21, ?22, ?23, ?24, ?25
                    )
                    "#,
                    params![
                        event.actor_id,
                        event.tool_id,
                        event.decision,
                        event.reason,
                        event.profile_id,
                        event.classification,
                        event.duration_ms,
                        event.created_at_epoch_ms,
                        event.level,
                        event.category,
                        event.action,
                        event.outcome,
                        event.actor_type,
                        event.source_id,
                        event.summary,
                        event.connection_id,
                        event.database_name,
                        event.driver_id,
                        event.object_type,
                        event.object_id,
                        event.details_json,
                        event.error_code,
                        event.error_message,
                        event.session_id,
                        event.correlation_id,
                    ],
                )?;

                let id = conn.last_insert_rowid();
                Ok(Some(id))
            }
            Err(TryLockError::WouldBlock) => {
                // Lock held by another thread — caller should log fallback
                Ok(None)
            }
            Err(TryLockError::Poisoned(_)) => {
                // Process is in bad state — log and continue
                eprintln!(
                    "[dbflux_audit] panic event (lock poisoned): {}",
                    panic_info_str
                );
                Ok(None)
            }
        }
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

#[cfg(test)]
mod tests {
    use dbflux_core::observability::actions::{CONFIG_CHANGE, QUERY_EXECUTE};

    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("dbflux_repo_audit_{}_{}", name, std::process::id()));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));

        path
    }

    #[test]
    fn query_filters_by_action_and_object_type() {
        let path = temp_db("action_object_type");
        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations should run");

        let repo = AuditRepository::new(Arc::new(Mutex::new(conn)));

        repo.append_extended(AppendAuditEventExtended {
            actor_id: "alice",
            tool_id: "",
            decision: "",
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: Some(10),
            created_at_epoch_ms: 1000,
            level: Some("info"),
            category: Some("query"),
            action: Some(QUERY_EXECUTE.as_str()),
            outcome: Some("success"),
            actor_type: Some("user"),
            source_id: Some("local"),
            summary: Some("Query executed"),
            connection_id: Some("conn-1"),
            database_name: Some("main"),
            driver_id: Some("sqlite"),
            object_type: Some("table"),
            object_id: Some("users"),
            details_json: Some("{}"),
            error_code: None,
            error_message: None,
            session_id: None,
            correlation_id: None,
        })
        .expect("first insert should succeed");

        repo.append_extended(AppendAuditEventExtended {
            actor_id: "alice",
            tool_id: "",
            decision: "",
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at_epoch_ms: 1001,
            level: Some("info"),
            category: Some("config"),
            action: Some(CONFIG_CHANGE.as_str()),
            outcome: Some("success"),
            actor_type: Some("user"),
            source_id: Some("local"),
            summary: Some("Config changed"),
            connection_id: None,
            database_name: None,
            driver_id: None,
            object_type: Some("profile"),
            object_id: Some("local-dev"),
            details_json: Some("{}"),
            error_code: None,
            error_message: None,
            session_id: None,
            correlation_id: None,
        })
        .expect("second insert should succeed");

        let query_events = repo
            .query(&AuditQueryFilter {
                action: Some(QUERY_EXECUTE.as_str().to_string()),
                object_type: Some("table".to_string()),
                ..Default::default()
            })
            .expect("query filter should succeed");

        assert_eq!(query_events.len(), 1);
        assert_eq!(
            query_events[0].action.as_deref(),
            Some(QUERY_EXECUTE.as_str())
        );
        assert_eq!(query_events[0].object_type.as_deref(), Some("table"));

        let config_events = repo
            .query(&AuditQueryFilter {
                action: Some(CONFIG_CHANGE.as_str().to_string()),
                object_type: Some("profile".to_string()),
                ..Default::default()
            })
            .expect("query filter should succeed");

        assert_eq!(config_events.len(), 1);
        assert_eq!(config_events[0].object_id.as_deref(), Some("local-dev"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_matches_legacy_and_canonical_tool_and_decision_aliases() {
        let path = temp_db("tool_decision_aliases");
        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations should run");

        let repo = AuditRepository::new(Arc::new(Mutex::new(conn)));

        repo.append_extended(AppendAuditEventExtended {
            actor_id: "reviewer-a",
            tool_id: "approve_execution",
            decision: "allow",
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at_epoch_ms: 1000,
            level: None,
            category: None,
            action: Some("approve_execution"),
            outcome: Some("allow"),
            actor_type: None,
            source_id: None,
            summary: None,
            connection_id: None,
            database_name: None,
            driver_id: None,
            object_type: None,
            object_id: None,
            details_json: None,
            error_code: None,
            error_message: None,
            session_id: None,
            correlation_id: None,
        })
        .expect("legacy insert should succeed");

        repo.append_extended(AppendAuditEventExtended {
            actor_id: "reviewer-b",
            tool_id: "",
            decision: "",
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at_epoch_ms: 1001,
            level: Some("info"),
            category: Some("mcp"),
            action: Some("mcp_approve_execution"),
            outcome: Some("success"),
            actor_type: Some("user"),
            source_id: Some("mcp"),
            summary: Some("Approved pending execution"),
            connection_id: Some("conn-1"),
            database_name: None,
            driver_id: None,
            object_type: Some("pending_execution"),
            object_id: Some("pending-1"),
            details_json: Some("{}"),
            error_code: None,
            error_message: None,
            session_id: None,
            correlation_id: None,
        })
        .expect("canonical insert should succeed");

        repo.append_extended(AppendAuditEventExtended {
            actor_id: "reviewer-c",
            tool_id: "",
            decision: "",
            reason: Some("unsafe change"),
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at_epoch_ms: 1002,
            level: Some("warn"),
            category: Some("mcp"),
            action: Some("mcp_reject_execution"),
            outcome: Some("failure"),
            actor_type: Some("mcp_client"),
            source_id: Some("mcp"),
            summary: Some("Rejected pending execution"),
            connection_id: Some("conn-1"),
            database_name: None,
            driver_id: None,
            object_type: Some("pending_execution"),
            object_id: Some("pending-2"),
            details_json: Some("{}"),
            error_code: Some("rejected"),
            error_message: Some("unsafe change"),
            session_id: None,
            correlation_id: None,
        })
        .expect("canonical reject insert should succeed");

        let legacy_query = repo
            .query(&AuditQueryFilter {
                tool_id: Some("approve_execution".to_string()),
                decision: Some("allow".to_string()),
                ..Default::default()
            })
            .expect("legacy query should succeed");
        assert_eq!(legacy_query.len(), 2);

        let canonical_query = repo
            .query(&AuditQueryFilter {
                tool_id: Some("mcp_approve_execution".to_string()),
                decision: Some("success".to_string()),
                ..Default::default()
            })
            .expect("canonical query should succeed");
        assert_eq!(canonical_query.len(), 2);

        let deny_query = repo
            .query(&AuditQueryFilter {
                decision: Some("deny".to_string()),
                ..Default::default()
            })
            .expect("deny query should succeed");
        assert_eq!(deny_query.len(), 1);
        assert!(
            deny_query
                .iter()
                .all(|event| event.action.as_deref() == Some("mcp_reject_execution"))
        );

        let failure_query = repo
            .query(&AuditQueryFilter {
                decision: Some("failure".to_string()),
                ..Default::default()
            })
            .expect("failure query should succeed");
        assert!(failure_query.is_empty());

        let _ = std::fs::remove_file(&path);
    }
}
