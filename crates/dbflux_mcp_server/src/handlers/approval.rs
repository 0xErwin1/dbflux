use dbflux_mcp::McpGovernanceService;
use dbflux_policy::ExecutionClassification;

use crate::bootstrap::ServerState;

use super::require_str;

pub fn handle(
    tool_id: &str,
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    match tool_id {
        "request_execution" => request_execution(args, state),
        "list_pending_executions" => list_pending_executions(state),
        "get_pending_execution" => get_pending_execution(args, state),
        "approve_execution" => approve_execution(args, state),
        "reject_execution" => reject_execution(args, state),
        _ => Err(format!("Unknown approval tool: {tool_id}")),
    }
}

fn request_execution(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let tool_id = require_str(args, "tool_id")?;
    let payload = args
        .get("payload")
        .cloned()
        .unwrap_or(serde_json::Value::Object(Default::default()));

    let classification = ExecutionClassification::Write;

    let plan = state.runtime.classify_plan(
        classification,
        payload,
        state.client_id.clone(),
        connection_id.to_string(),
        tool_id.to_string(),
    );

    let summary = state.runtime.request_execution_mut(plan);

    Ok(serde_json::json!({
        "id": summary.id,
        "actor_id": summary.actor_id,
        "connection_id": summary.connection_id,
        "tool_id": summary.tool_id,
        "status": summary.status,
        "created_at_epoch_ms": summary.created_at_epoch_ms,
    }))
}

fn list_pending_executions(state: &ServerState) -> Result<serde_json::Value, String> {
    let executions = McpGovernanceService::list_pending_executions(&state.runtime)
        .map_err(|e| format!("list_pending_executions failed: {e}"))?;

    let items: Vec<serde_json::Value> = executions
        .iter()
        .map(|e| {
            serde_json::json!({
                "id": e.id,
                "actor_id": e.actor_id,
                "connection_id": e.connection_id,
                "tool_id": e.tool_id,
                "status": e.status,
                "created_at_epoch_ms": e.created_at_epoch_ms,
            })
        })
        .collect();

    Ok(serde_json::json!({ "pending_executions": items }))
}

fn get_pending_execution(
    args: &serde_json::Value,
    state: &ServerState,
) -> Result<serde_json::Value, String> {
    let pending_id = require_str(args, "pending_id")?;

    let detail = McpGovernanceService::get_pending_execution(&state.runtime, pending_id)
        .map_err(|e| format!("get_pending_execution failed: {e}"))?;

    Ok(serde_json::json!({
        "id": detail.summary.id,
        "actor_id": detail.summary.actor_id,
        "connection_id": detail.summary.connection_id,
        "tool_id": detail.summary.tool_id,
        "status": detail.summary.status,
        "plan": detail.plan,
    }))
}

fn approve_execution(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let pending_id = require_str(args, "pending_id")?;

    let entry = state
        .runtime
        .approve_pending_execution_mut(pending_id)
        .map_err(|e| format!("approve_execution failed: {e}"))?;

    Ok(serde_json::json!({
        "audit_entry_id": entry.id,
        "decision": entry.decision,
    }))
}

fn reject_execution(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let pending_id = require_str(args, "pending_id")?;

    let entry = state
        .runtime
        .reject_pending_execution_mut(pending_id)
        .map_err(|e| format!("reject_execution failed: {e}"))?;

    Ok(serde_json::json!({
        "audit_entry_id": entry.id,
        "decision": entry.decision,
    }))
}
