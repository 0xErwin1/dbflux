use dbflux_audit::export::AuditExportFormat as AuditStoreExportFormat;
use dbflux_audit::query::AuditQueryFilter;
use dbflux_audit::{AuditError, AuditEvent, AuditService};

use crate::governance_service::{AuditExportFormat, AuditQuery};

pub fn query_audit_logs(
    audit_service: &AuditService,
    query: &AuditQuery,
) -> Result<Vec<AuditEvent>, AuditError> {
    audit_service.query(&to_filter(query))
}

pub fn get_audit_entry(
    audit_service: &AuditService,
    id: i64,
) -> Result<Option<AuditEvent>, AuditError> {
    audit_service.get(id)
}

pub fn export_audit_logs(
    audit_service: &AuditService,
    query: &AuditQuery,
    format: AuditExportFormat,
) -> Result<String, AuditError> {
    let format = match format {
        AuditExportFormat::Csv => AuditStoreExportFormat::Csv,
        AuditExportFormat::Json => AuditStoreExportFormat::Json,
    };

    audit_service.export(&to_filter(query), format)
}

fn to_filter(query: &AuditQuery) -> AuditQueryFilter {
    AuditQueryFilter {
        actor_id: query.actor_id.clone(),
        tool_id: query.tool_id.clone(),
        decision: query.decision.clone(),
        start_epoch_ms: query.start_epoch_ms,
        end_epoch_ms: query.end_epoch_ms,
        limit: query.limit,
    }
}
