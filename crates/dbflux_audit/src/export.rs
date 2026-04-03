use crate::AuditEvent;
use dbflux_storage::repositories::audit::AuditEventDto;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditExportFormat {
    Csv,
    Json,
}

pub fn export_entries(
    entries: &[AuditEvent],
    format: AuditExportFormat,
) -> Result<String, serde_json::Error> {
    match format {
        AuditExportFormat::Csv => Ok(export_csv(entries)),
        AuditExportFormat::Json => serde_json::to_string_pretty(entries),
    }
}

fn export_csv(entries: &[AuditEvent]) -> String {
    let mut output = String::from("id,actor_id,tool_id,decision,reason,created_at_epoch_ms\n");

    for entry in entries {
        let escaped_reason = entry
            .reason
            .as_deref()
            .unwrap_or_default()
            .replace('"', "\"\"");

        output.push_str(&format!(
            "{},{},{},{},\"{}\",{}\n",
            entry.id,
            entry.actor_id,
            entry.tool_id,
            entry.decision,
            escaped_reason,
            entry.created_at_epoch_ms
        ));
    }

    output
}

/// Exports extended audit events (full DTO schema) to JSON or CSV format.
pub fn export_extended(
    events: &[AuditEventDto],
    format: AuditExportFormat,
) -> Result<String, serde_json::Error> {
    let normalized: Vec<_> = events
        .iter()
        .cloned()
        .map(|mut event| {
            if event.tool_id.trim().is_empty() {
                event.tool_id = event.legacy_tool_id();
            }

            if event.decision.trim().is_empty() {
                event.decision = event.legacy_decision();
            }

            event
        })
        .collect();

    match format {
        AuditExportFormat::Csv => Ok(export_extended_csv(&normalized)),
        AuditExportFormat::Json => serde_json::to_string_pretty(&normalized),
    }
}

/// Exports extended audit events to CSV format with all DTO fields.
fn export_extended_csv(events: &[AuditEventDto]) -> String {
    let mut output = String::new();

    // Header row with all extended fields
    output.push_str(
        "id,actor_id,tool_id,decision,reason,profile_id,classification,duration_ms,\
         created_at,created_at_epoch_ms,level,category,action,outcome,actor_type,source_id,\
         summary,connection_id,database_name,driver_id,object_type,object_id,\
         details_json,error_code,error_message,session_id,correlation_id\n",
    );

    for event in events {
        let escape = |s: Option<&str>| {
            s.unwrap_or_default()
                .replace('"', "\"\"")
                .replace('\n', " ")
        };

        output.push_str(&format!(
            "{},\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",{},{},{},\"{}\",\"{}\",\"{}\",\
             \"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\
             \"{}\",\"{}\",\"{}\"\n",
            event.id,
            escape(Some(&event.actor_id)),
            escape(Some(&event.tool_id)),
            escape(Some(&event.decision)),
            escape(event.reason.as_deref()),
            escape(event.profile_id.as_deref()),
            escape(event.classification.as_deref()),
            event.duration_ms.map(|d| d.to_string()).unwrap_or_default(),
            escape(Some(&event.created_at)),
            event.created_at_epoch_ms,
            escape(event.level.as_deref()),
            escape(event.category.as_deref()),
            escape(event.action.as_deref()),
            escape(event.outcome.as_deref()),
            escape(event.actor_type.as_deref()),
            escape(event.source_id.as_deref()),
            escape(event.summary.as_deref()),
            escape(event.connection_id.as_deref()),
            escape(event.database_name.as_deref()),
            escape(event.driver_id.as_deref()),
            escape(event.object_type.as_deref()),
            escape(event.object_id.as_deref()),
            escape(event.details_json.as_deref()),
            escape(event.error_code.as_deref()),
            escape(event.error_message.as_deref()),
            escape(event.session_id.as_deref()),
            escape(event.correlation_id.as_deref()),
        ));
    }

    output
}
