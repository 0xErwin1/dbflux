use dbflux_mcp::{AuditExportFormat, AuditQuery, McpGovernanceService};

use crate::state::ServerState;
use crate::error_messages;

use super::{optional_str, require_str};

pub fn handle(
    tool_id: &str,
    args: &serde_json::Value,
    state: &ServerState,
) -> Result<serde_json::Value, String> {
    match tool_id {
        "query_audit_logs" => query_audit_logs(args, state),
        "get_audit_entry" => get_audit_entry(args, state),
        "export_audit_logs" => export_audit_logs(args, state),
        _ => Err(format!("Unknown audit tool: {tool_id}")),
    }
}

fn query_audit_logs(
    args: &serde_json::Value,
    state: &ServerState,
) -> Result<serde_json::Value, String> {
    let query = AuditQuery {
        actor_id: optional_str(args, "actor_id").map(str::to_string),
        tool_id: optional_str(args, "tool_id").map(str::to_string),
        decision: optional_str(args, "decision").map(str::to_string),
        start_epoch_ms: args
            .get("start_epoch_ms")
            .and_then(serde_json::Value::as_i64),
        end_epoch_ms: args.get("end_epoch_ms").and_then(serde_json::Value::as_i64),
        limit: args
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .map(|v| v as usize),
    };

    let entries = McpGovernanceService::query_audit_entries(state.runtime.as_ref(), &query)
        .map_err(|e| error_messages::audit_error("query audit logs", e))?;

    let items: Vec<serde_json::Value> = entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "id": entry.id,
                "actor_id": entry.actor_id,
                "tool_id": entry.tool_id,
                "decision": entry.decision,
                "reason": entry.reason,
                "created_at_epoch_ms": entry.created_at_epoch_ms,
            })
        })
        .collect();

    Ok(serde_json::json!({ "entries": items }))
}

fn get_audit_entry(
    args: &serde_json::Value,
    state: &ServerState,
) -> Result<serde_json::Value, String> {
    let entry_id = require_str(args, "entry_id", "get_audit_entry")?;

    // Query by ID — filter from a full query.
    let query = AuditQuery {
        actor_id: None,
        tool_id: None,
        decision: None,
        start_epoch_ms: None,
        end_epoch_ms: None,
        limit: None,
    };

    let entries = McpGovernanceService::query_audit_entries(state.runtime.as_ref(), &query)
        .map_err(|e| error_messages::audit_error("get audit entry", e))?;

    let entry = entries
        .into_iter()
        .find(|e| e.id == entry_id)
        .ok_or_else(|| format!("Audit entry not found: {entry_id}"))?;

    Ok(serde_json::json!({
        "id": entry.id,
        "actor_id": entry.actor_id,
        "tool_id": entry.tool_id,
        "decision": entry.decision,
        "reason": entry.reason,
        "created_at_epoch_ms": entry.created_at_epoch_ms,
    }))
}

fn export_audit_logs(
    args: &serde_json::Value,
    state: &ServerState,
) -> Result<serde_json::Value, String> {
    let format_str = require_str(args, "format", "export_audit_logs")?;
    let format = match format_str {
        "csv" => AuditExportFormat::Csv,
        "json" => AuditExportFormat::Json,
        other => return Err(format!("Unsupported export format: {other}")),
    };

    let query = AuditQuery {
        actor_id: optional_str(args, "actor_id").map(str::to_string),
        tool_id: optional_str(args, "tool_id").map(str::to_string),
        decision: optional_str(args, "decision").map(str::to_string),
        start_epoch_ms: args
            .get("start_epoch_ms")
            .and_then(serde_json::Value::as_i64),
        end_epoch_ms: args.get("end_epoch_ms").and_then(serde_json::Value::as_i64),
        limit: None,
    };

    let exported = McpGovernanceService::export_audit_entries(state.runtime.as_ref(), &query, format)
        .map_err(|e| error_messages::audit_error("export audit logs", e))?;

    Ok(serde_json::json!({
        "format": format_str,
        "data": exported,
    }))
}
