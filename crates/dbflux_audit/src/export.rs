use crate::AuditEvent;

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
