use dbflux_core::{ExplainRequest, QueryRequest, TableRef};

use crate::bootstrap::ServerState;

use super::{get_or_connect, optional_str, require_str};
use crate::handlers::schema::serialize_query_result;

pub fn handle(
    tool_id: &str,
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    match tool_id {
        "read_query" => read_query(args, state),
        "explain_query" => explain_query(args, state),
        "preview_mutation" => preview_mutation(args, state),
        _ => Err(format!("Unknown query tool: {tool_id}")),
    }
}

fn read_query(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let sql = require_str(args, "sql")?;
    let database = optional_str(args, "database");
    let limit = args
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as u32);
    let offset = args
        .get("offset")
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as u32);

    let connection = get_or_connect(state, connection_id)?;

    let mut request = QueryRequest::new(sql);
    if let Some(db) = database {
        request = request.with_database(Some(db.to_string()));
    }
    if let Some(l) = limit {
        request = request.with_limit(l);
    }
    if let Some(o) = offset {
        request = request.with_offset(o);
    }

    let result = connection
        .execute(&request)
        .map_err(|e| format!("read_query failed: {e}"))?;

    Ok(serialize_query_result(&result))
}

fn explain_query(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let sql = optional_str(args, "sql");
    let table_name = optional_str(args, "table");
    let connection = get_or_connect(state, connection_id)?;

    let table_ref = TableRef {
        schema: None,
        name: table_name.unwrap_or("").to_string(),
    };

    let mut request = ExplainRequest::new(table_ref);
    if let Some(query) = sql {
        request = request.with_query(query);
    }

    let result = connection
        .explain(&request)
        .map_err(|e| format!("explain_query failed: {e}"))?;

    Ok(serialize_query_result(&result))
}

fn preview_mutation(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let sql = require_str(args, "sql")?;
    let connection = get_or_connect(state, connection_id)?;

    // Build an EXPLAIN request for the mutation SQL.
    let table_ref = TableRef {
        schema: None,
        name: String::new(),
    };

    let request = ExplainRequest::new(table_ref).with_query(sql);

    let result = connection
        .explain(&request)
        .map_err(|e| format!("preview_mutation failed: {e}"))?;

    Ok(serde_json::json!({
        "preview": serialize_query_result(&result),
        "note": "This is an execution plan preview — the mutation was NOT executed.",
    }))
}
