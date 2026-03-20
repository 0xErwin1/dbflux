use std::path::Path;

use dbflux_core::{QueryRequest, ScriptEntry, ScriptsDirectory};

use crate::bootstrap::ServerState;

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
    let script_id = require_str(args, "script_id")?;
    let path = Path::new(script_id);

    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read script '{script_id}': {e}"))?;

    Ok(serde_json::json!({
        "id": script_id,
        "content": content,
    }))
}

fn create_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let name = require_str(args, "name")?;
    let content = require_str(args, "content")?;
    let extension = optional_str(args, "extension").unwrap_or("sql");

    let mut scripts_dir = scripts_dir()?;

    let path = scripts_dir
        .create_file(None, name, extension)
        .map_err(|e| format!("Failed to create script: {e}"))?;

    std::fs::write(&path, content).map_err(|e| format!("Failed to write script content: {e}"))?;

    Ok(serde_json::json!({
        "id": path.to_string_lossy(),
        "name": name,
    }))
}

fn update_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let script_id = require_str(args, "script_id")?;
    let content = require_str(args, "content")?;
    let path = Path::new(script_id);

    if !path.exists() {
        return Err(format!("Script not found: {script_id}"));
    }

    std::fs::write(path, content)
        .map_err(|e| format!("Failed to write script '{script_id}': {e}"))?;

    Ok(serde_json::json!({ "id": script_id, "updated": true }))
}

fn delete_script(args: &serde_json::Value) -> Result<serde_json::Value, String> {
    let script_id = require_str(args, "script_id")?;
    let path = Path::new(script_id);

    let mut scripts_dir = scripts_dir()?;

    scripts_dir
        .delete(path)
        .map_err(|e| format!("Failed to delete script '{script_id}': {e}"))?;

    Ok(serde_json::json!({ "id": script_id, "deleted": true }))
}

fn run_script(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let script_id = require_str(args, "script_id")?;

    let content = std::fs::read_to_string(script_id)
        .map_err(|e| format!("Failed to read script '{script_id}': {e}"))?;

    let connection = get_or_connect(state, connection_id)?;

    let result = connection
        .execute(&QueryRequest::new(content))
        .map_err(|e| format!("run_script failed: {e}"))?;

    Ok(crate::handlers::schema::serialize_query_result(&result))
}
