use dbflux_core::{ExplainRequest, QueryRequest, TableRef};

use crate::state::ServerState;
use crate::error_messages;

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
    let connection_id = require_str(args, "connection_id", "read_query")?;
    let sql = require_str(args, "sql", "read_query")?;
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
    let driver = state
        .profile_manager
        .find_by_id(connection_id.parse().unwrap())
        .map(|p| p.driver_id())
        .unwrap_or_else(|| "unknown".to_string());

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

    let result = connection.execute(&request).map_err(|e| {
        error_messages::query_execution_error("read_query", connection_id, database, &driver, e)
    })?;

    Ok(serialize_query_result(&result))
}

fn explain_query(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id", "explain_query")?;
    let sql = optional_str(args, "sql");
    let table_name = optional_str(args, "table");
    let database = optional_str(args, "database");
    let connection = get_or_connect(state, connection_id)?;
    let driver = state
        .profile_manager
        .find_by_id(connection_id.parse().unwrap())
        .map(|p| p.driver_id())
        .unwrap_or_else(|| "unknown".to_string());

    let table_ref = TableRef {
        schema: None,
        name: table_name.unwrap_or("").to_string(),
    };

    let mut request = ExplainRequest::new(table_ref);
    if let Some(query) = sql {
        request = request.with_query(query);
    }

    let result = connection.explain(&request).map_err(|e| {
        error_messages::query_execution_error("explain_query", connection_id, database, &driver, e)
    })?;

    Ok(serialize_query_result(&result))
}

fn preview_mutation(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id", "preview_mutation")?;
    let sql = require_str(args, "sql", "preview_mutation")?;
    let database = optional_str(args, "database");
    let connection = get_or_connect(state, connection_id)?;
    let driver = state
        .profile_manager
        .find_by_id(connection_id.parse().unwrap())
        .map(|p| p.driver_id())
        .unwrap_or_else(|| "unknown".to_string());

    // Build an EXPLAIN request for the mutation SQL.
    let table_ref = TableRef {
        schema: None,
        name: String::new(),
    };

    let request = ExplainRequest::new(table_ref).with_query(sql);

    let result = connection.explain(&request).map_err(|e| {
        error_messages::query_execution_error(
            "preview_mutation",
            connection_id,
            database,
            &driver,
            e,
        )
    })?;

    Ok(serde_json::json!({
        "preview": serialize_query_result(&result),
        "note": "This is an execution plan preview — the mutation was NOT executed.",
    }))
}
