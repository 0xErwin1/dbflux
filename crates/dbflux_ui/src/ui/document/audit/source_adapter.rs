//! Audit source adapter implementing the `EventSource` seam over `AuditRepository`.
//!
//! This thin adapter translates between the `EventSource` trait API (`EventQuery`,
//! `EventRecord`, `EventDetail`) and the storage-layer DTOs (`AuditQueryFilter`,
//! `AuditEventDto`). It allows `AuditDocument` to be decoupled from the raw storage
//! API and enables future sources (CloudWatch, Loki) to be swapped in without
//! changing the document logic.

use dbflux_core::observability::query::EventDetail;
use dbflux_core::observability::source::{EventSource as _, EventSourceError};
use dbflux_core::observability::{
    AuditQuerySource, EventActorType, EventCategory, EventOutcome, EventQuery, EventRecord,
    EventSeverity, EventSource, EventSourceId,
};
use dbflux_storage::repositories::audit::{AuditEventDto, AuditQueryFilter, AuditRepository};

/// Adapter exposing the `EventSource` interface over the `AuditRepository` storage layer.
#[derive(Clone)]
pub struct AuditSourceAdapter {
    repo: AuditRepository,
}

impl AuditSourceAdapter {
    /// Creates a new adapter wrapping the given repository.
    pub fn new(repo: AuditRepository) -> Self {
        Self { repo }
    }

    /// Translates an `EventQuery` into a storage-layer `AuditQueryFilter`.
    #[allow(clippy::field_reassign_with_default)]
    fn query_to_filter(query: &EventQuery, default_limit: Option<usize>) -> AuditQueryFilter {
        let mut filter = AuditQueryFilter::default();

        filter.id = query.id;
        filter.start_epoch_ms = query.from_ts_ms;
        filter.end_epoch_ms = query.to_ts_ms;
        filter.limit = query.limit.or(default_limit);
        filter.offset = query.offset;

        if let Some(level) = &query.level {
            filter.level = Some(level.as_str().to_string());
        }
        if let Some(category) = &query.category {
            filter.category = Some(category.as_str().to_string());
        }
        if let Some(outcome) = &query.outcome {
            filter.outcome = Some(outcome.as_str().to_string());
        }
        if let Some(actor_type) = &query.actor_type {
            filter.actor_type = Some(actor_type.as_str().to_string());
        }
        if let Some(source_id) = &query.source_id {
            filter.source_id = Some(source_id.as_str().to_string());
        }
        if let Some(actor_id) = &query.actor_id {
            filter.actor_id = Some(actor_id.clone());
        }
        if let Some(connection_id) = &query.connection_id {
            filter.connection_id = Some(connection_id.clone());
        }
        if let Some(driver_id) = &query.driver_id {
            filter.driver_id = Some(driver_id.clone());
        }
        if let Some(free_text) = &query.free_text {
            filter.free_text = Some(free_text.clone());
        }
        if let Some(action) = &query.action {
            filter.action = Some(action.clone());
        }
        if let Some(object_type) = &query.object_type {
            filter.object_type = Some(object_type.clone());
        }

        filter
    }

    /// Maps an `AuditEventDto` from storage to an `EventRecord`.
    fn dto_to_record(dto: &AuditEventDto) -> EventRecord {
        let level = dto
            .level
            .as_ref()
            .and_then(|l| EventSeverity::from_str_repr(l))
            .unwrap_or(EventSeverity::Info);

        let category = dto
            .category
            .as_ref()
            .and_then(|c| EventCategory::from_str_repr(c))
            .unwrap_or(EventCategory::System);

        let outcome = dto
            .outcome
            .as_ref()
            .and_then(|o| EventOutcome::from_str_repr(o))
            .unwrap_or(match dto.decision.as_str() {
                "deny" | "rejected" => EventOutcome::Failure,
                "pending" => EventOutcome::Pending,
                _ => EventOutcome::Success,
            });

        let actor_type = dto
            .actor_type
            .as_ref()
            .and_then(|a| EventActorType::from_str_repr(a))
            .unwrap_or(EventActorType::System);

        let source_id = dto
            .source_id
            .as_ref()
            .and_then(|s| EventSourceId::from_str_repr(s))
            .unwrap_or(EventSourceId::System);

        let summary = dto
            .summary
            .as_deref()
            .filter(|value| !value.is_empty())
            .or(dto.reason.as_deref().filter(|value| !value.is_empty()))
            .unwrap_or(dto.tool_id.as_str());
        let action = dto
            .action
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(dto.tool_id.as_str());
        let actor_id = (!dto.actor_id.is_empty()).then(|| dto.actor_id.clone());

        EventRecord {
            id: Some(dto.id),
            ts_ms: dto.created_at_epoch_ms,
            level,
            category,
            action: action.to_string(),
            outcome,
            actor_type,
            actor_id,
            source_id,
            connection_id: dto.connection_id.clone(),
            database_name: dto.database_name.clone(),
            driver_id: dto.driver_id.clone(),
            object_type: dto.object_type.clone(),
            object_id: dto.object_id.clone(),
            summary: summary.to_string(),
            details_json: dto.details_json.clone(),
            error_code: dto.error_code.clone(),
            error_message: dto.error_message.clone(),
            duration_ms: dto.duration_ms,
            session_id: dto.session_id.clone(),
            correlation_id: dto.correlation_id.clone(),
        }
    }
}

impl AuditSourceAdapter {
    /// Exports events to the given format (json/csv) using an unlimited query.
    ///
    /// Unlike `EventSource::export_events`, this accepts an `AuditQueryFilter`
    /// directly and uses a very high internal limit so exports are not
    /// silently truncated at the viewer page size.
    pub fn export_filtered(
        &self,
        filter: &AuditQueryFilter,
        format: &str,
    ) -> Result<Vec<u8>, String> {
        let events = self.repo.query(filter).map_err(|e| e.to_string())?;
        export_audit_events_extended(&events, format)
            .map_err(|e| format!("serialization failed: {}", e))
    }
}

impl AuditSourceAdapter {
    /// Queries audit events using a storage-layer filter and returns DTOs.
    ///
    /// This is the primary query method used by `AuditDocument` to populate
    /// the event table. Unlike `EventSource::query`, this works directly with
    /// `AuditQueryFilter` and returns `AuditEventDto` without conversion.
    pub fn query_filter(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEventDto>, String> {
        self.repo
            .query(filter)
            .map_err(|e| format!("audit query failed: {}", e))
    }

    pub fn count_filter(&self, filter: &AuditQueryFilter) -> Result<u64, String> {
        self.repo
            .count_filtered(filter)
            .map(|count| count.max(0) as u64)
            .map_err(|e| format!("audit count failed: {}", e))
    }

    /// Queries using the `EventQuery` abstraction and returns an `EventPage`.
    ///
    /// Implements `EventSource::query` for swappable-source support.
    pub fn query_abstract(
        &self,
        query: &EventQuery,
    ) -> Result<dbflux_core::observability::query::EventPage, EventSourceError> {
        let filter = Self::query_to_filter(query, Some(500));
        let total = self
            .repo
            .count_filtered(&filter)
            .map_err(|e| EventSourceError::Query(format!("audit count failed: {}", e)))?
            .max(0) as usize;
        let events = self
            .repo
            .query(&filter)
            .map_err(|e| EventSourceError::Query(format!("audit query failed: {}", e)))?;

        let limit = query.limit.unwrap_or(500);
        let offset = query.offset.unwrap_or(0);
        let has_more = offset + events.len() < total;

        let records: Vec<EventRecord> = events.iter().map(Self::dto_to_record).collect();

        Ok(dbflux_core::observability::query::EventPage::new(
            records,
            Some(total),
            has_more,
            offset,
            limit,
        ))
    }
}

impl EventSource for AuditSourceAdapter {
    fn query(
        &self,
        query: &EventQuery,
    ) -> Result<dbflux_core::observability::query::EventPage, EventSourceError> {
        self.query_abstract(query)
    }

    fn read_detail(&self, id: i64) -> Result<EventDetail, EventSourceError> {
        let dto = self
            .repo
            .find_by_id(id)
            .map_err(|e| EventSourceError::Query(format!("audit find_by_id failed: {}", e)))?
            .ok_or(EventSourceError::NotFound(id))?;

        let record = Self::dto_to_record(&dto);
        Ok(EventDetail::from_record(record))
    }

    fn export_events(&self, query: &EventQuery, format: &str) -> Result<Vec<u8>, EventSourceError> {
        let filter = Self::query_to_filter(query, None);
        let events = self.repo.query(&filter).map_err(|e| {
            EventSourceError::Export(format!("audit query for export failed: {}", e))
        })?;

        let data = export_audit_events_extended(&events, format)
            .map_err(|e| EventSourceError::Export(format!("export encoding failed: {}", e)))?;

        Ok(data)
    }
}

/// Exports audit events (extended DTO) to JSON or CSV format.
fn export_audit_events_extended(
    events: &[AuditEventDto],
    format: &str,
) -> Result<Vec<u8>, serde_json::Error> {
    match format {
        "json" => {
            let json = serde_json::to_string_pretty(events)?;
            Ok(json.into_bytes())
        }
        "csv" => Ok(csv_extended(events).into_bytes()),
        _ => {
            // Default to JSON for unknown formats
            let json = serde_json::to_string_pretty(events)?;
            Ok(json.into_bytes())
        }
    }
}

/// Exports extended audit events to CSV format.
fn csv_extended(events: &[AuditEventDto]) -> String {
    let mut output = String::new();

    // Header row
    output.push_str("id,actor_id,tool_id,decision,reason,profile_id,classification,duration_ms,created_at,created_at_epoch_ms,level,category,action,outcome,actor_type,source_id,summary,connection_id,database_name,driver_id,object_type,object_id,details_json,error_code,error_message,session_id,correlation_id\n");

    for event in events {
        let escape = |s: &str| -> String { s.replace('"', "\"\"").replace('\n', " ") };

        let reason = event.reason.as_deref().unwrap_or_default();
        let profile_id = event.profile_id.as_deref().unwrap_or_default();
        let classification = event.classification.as_deref().unwrap_or_default();
        let duration_ms = event.duration_ms.map(|d| d.to_string()).unwrap_or_default();
        let level = event.level.as_deref().unwrap_or_default();
        let category = event.category.as_deref().unwrap_or_default();
        let action = event.action.as_deref().unwrap_or_default();
        let outcome = event.outcome.as_deref().unwrap_or_default();
        let actor_type = event.actor_type.as_deref().unwrap_or_default();
        let source_id = event.source_id.as_deref().unwrap_or_default();
        let summary = event.summary.as_deref().unwrap_or_default();
        let connection_id = event.connection_id.as_deref().unwrap_or_default();
        let database_name = event.database_name.as_deref().unwrap_or_default();
        let driver_id = event.driver_id.as_deref().unwrap_or_default();
        let object_type = event.object_type.as_deref().unwrap_or_default();
        let object_id = event.object_id.as_deref().unwrap_or_default();
        let details_json = event.details_json.as_deref().unwrap_or_default();
        let error_code = event.error_code.as_deref().unwrap_or_default();
        let error_message = event.error_message.as_deref().unwrap_or_default();
        let session_id = event.session_id.as_deref().unwrap_or_default();
        let correlation_id = event.correlation_id.as_deref().unwrap_or_default();

        output.push_str(&format!(
            "{},\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",{},{},{},\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n",
            event.id,
            escape(&event.actor_id),
            escape(&event.tool_id),
            escape(&event.decision),
            escape(reason),
            escape(profile_id),
            escape(classification),
            duration_ms,
            escape(&event.created_at),
            event.created_at_epoch_ms,
            escape(level),
            escape(category),
            escape(action),
            escape(outcome),
            escape(actor_type),
            escape(source_id),
            escape(summary),
            escape(connection_id),
            escape(database_name),
            escape(driver_id),
            escape(object_type),
            escape(object_id),
            escape(details_json),
            escape(error_code),
            escape(error_message),
            escape(session_id),
            escape(correlation_id),
        ));
    }

    output
}
