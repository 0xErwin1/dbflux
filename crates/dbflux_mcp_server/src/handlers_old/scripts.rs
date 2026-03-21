use std::path::Path;

use dbflux_core::{QueryRequest, ScriptEntry, ScriptsDirectory};

use crate::state::ServerState;
use crate::error_messages;

use super::{get_or_connect, optional_str, require_str};

pub fn handle(
    tool_id: &str,
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    match tool_id {
        "list_scripts" => list_scripts(),
        "get_script" => get_script(args),
        "create_script" => create_script(args),
        "update_script" => update_script(args),
        "delete_script" => delete_script(args),
        "run_script" => run_script(args, state),
        _ => Err(format!("Unknown scripts tool: {tool_id}")),
    }
}

fn scripts_dir() -> Result<ScriptsDirectory, String> {
    ScriptsDirectory::new().map_err(|e| format!("Failed to open scripts directory: {e}"))
}

fn list_scripts() -> Result<serde_json::Value, String> {
    let scripts_dir = scripts_dir()?;

    let entries = flatten_entries(scripts_dir.entries());

    Ok(serde_json::json!({ "scripts": entries }))
}

fn flatten_entries(entries: &[ScriptEntry]) -> Vec<serde_json::Value> {
    let mut result = Vec::new();

    for entry in entries {
        match entry {
            ScriptEntry::File {
                path,
                name,
                extension,
            } => {
                result.push(serde_json::json!({
                    "id": path.to_string_lossy(),
                    "name": name,
                    "extension": extension,
                    "kind": "file",
                }));
            }
            ScriptEntry::Folder { name, children, .. } => {
                result.push(serde_json::json!({
                    "name": name,
                    "kind": "folder",
                    "children": flatten_entries(children),
                }));
            }
        }
    }

    result
}

fn get_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let script_id = require_str(args, "script_id", "get_script")?;
    let path = Path::new(script_id);

    let content = std::fs::read_to_string(path)
        .map_err(|e| error_messages::script_error("read", script_id, e))?;

    Ok(serde_json::json!({
        "id": script_id,
        "content": content,
    }))
}

fn create_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let name = require_str(args, "name", "create_script")?;
    let content = require_str(args, "content", "create_script")?;
    let extension = optional_str(args, "extension").unwrap_or("sql");

    let mut scripts_dir = scripts_dir()?;

    let path = scripts_dir
        .create_file(None, name, extension)
        .map_err(|e| error_messages::script_error("create", name, e))?;

    let script_id = path.to_string_lossy();
    std::fs::write(&path, content)
        .map_err(|e| error_messages::script_error("write", &script_id, e))?;

    Ok(serde_json::json!({
        "id": script_id,
        "name": name,
    }))
}

fn update_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let script_id = require_str(args, "script_id", "update_script")?;
    let content = require_str(args, "content", "update_script")?;
    let path = Path::new(script_id);

    if !path.exists() {
        return Err(error_messages::script_error(
            "update",
            script_id,
            "file not found",
        ));
    }

    std::fs::write(path, content)
        .map_err(|e| error_messages::script_error("write", script_id, e))?;

    Ok(serde_json::json!({ "id": script_id, "updated": true }))
}

fn delete_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let script_id = require_str(args, "script_id", "delete_script")?;
    let path = Path::new(script_id);

    let mut scripts_dir = scripts_dir()?;

    scripts_dir
        .delete(path)
        .map_err(|e| error_messages::script_error("delete", script_id, e))?;

    Ok(serde_json::json!({ "id": script_id, "deleted": true }))
}

fn run_script(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id", "run_script")?;
    let script_id = require_str(args, "script_id", "run_script")?;

    let content = std::fs::read_to_string(script_id)
        .map_err(|e| error_messages::script_error("read", script_id, e))?;

    let connection = get_or_connect(state, connection_id)?;
    let driver = state
        .profile_manager
        .find_by_id(connection_id.parse().unwrap())
        .map(|p| p.driver_id())
        .unwrap_or_else(|| "unknown".to_string());

    let result = connection
        .execute(&QueryRequest::new(content))
        .map_err(|e| {
            error_messages::query_execution_error("run_script", connection_id, None, &driver, e)
        })?;

    Ok(crate::handlers::schema::serialize_query_result(&result))
}
