use crate::bootstrap::ServerState;

use super::{get_or_connect, require_str};

pub fn handle(
    tool_id: &str,
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    match tool_id {
        "list_connections" => list_connections(state),
        "get_connection" => get_connection(args, state),
        "get_connection_metadata" => get_connection_metadata(args, state),
        _ => Err(format!("Unknown discovery tool: {tool_id}")),
    }
}

fn list_connections(state: &ServerState) -> Result<serde_json::Value, String> {
    let connections: Vec<serde_json::Value> = state
        .profile_manager
        .profiles
        .iter()
        .map(|profile| {
            serde_json::json!({
                "id": profile.id.to_string(),
                "name": profile.name,
                "driver_id": profile.driver_id(),
                "kind": format!("{:?}", profile.kind()),
                "mcp_enabled": profile.mcp_governance
                    .as_ref()
                    .map(|g| g.enabled)
                    .unwrap_or(state.mcp_enabled_by_default),
            })
        })
        .collect();

    Ok(serde_json::json!({ "connections": connections }))
}

fn get_connection(
    args: &serde_json::Value,
    state: &ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;

    let profile_uuid = connection_id
        .parse::<uuid::Uuid>()
        .map_err(|_| format!("Invalid connection_id: {connection_id}"))?;

    let profile = state
        .profile_manager
        .find_by_id(profile_uuid)
        .ok_or_else(|| format!("Connection not found: {connection_id}"))?;

    Ok(serde_json::json!({
        "id": profile.id.to_string(),
        "name": profile.name,
        "driver_id": profile.driver_id(),
        "kind": format!("{:?}", profile.kind()),
        "mcp_enabled": profile.mcp_governance
            .as_ref()
            .map(|g| g.enabled)
            .unwrap_or(state.mcp_enabled_by_default),
    }))
}

fn get_connection_metadata(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;

    let connection = get_or_connect(state, connection_id)?;
    let metadata = connection.metadata();

    Ok(serde_json::json!({
        "connection_id": connection_id,
        "driver_name": metadata.display_name,
        "category": format!("{:?}", metadata.category),
        "capabilities": format!("{:?}", metadata.capabilities),
    }))
}
