use dbflux_core::{DataStructure, DescribeRequest, QueryResult, TableRef, Value};

use crate::bootstrap::ServerState;

use super::{get_or_connect, optional_str, require_str};

pub fn handle(
    tool_id: &str,
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    match tool_id {
        "list_databases" => list_databases(args, state),
        "list_schemas" => list_schemas(args, state),
        "list_tables" | "list_collections" => list_tables(args, state),
        "describe_object" => describe_object(args, state),
        _ => Err(format!("Unknown schema tool: {tool_id}")),
    }
}

fn list_databases(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let connection = get_or_connect(state, connection_id)?;

    let databases = connection
        .list_databases()
        .map_err(|e| format!("list_databases failed: {e}"))?;

    let items: Vec<serde_json::Value> = databases
        .iter()
        .map(|db| {
            serde_json::json!({
                "name": db.name,
                "is_current": db.is_current,
            })
        })
        .collect();

    Ok(serde_json::json!({ "databases": items }))
}

fn list_schemas(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let connection = get_or_connect(state, connection_id)?;

    let snapshot = connection
        .schema()
        .map_err(|e| format!("list_schemas failed: {e}"))?;

    let schemas: Vec<serde_json::Value> = match &snapshot.structure {
        DataStructure::Relational(relational) => relational
            .schemas
            .iter()
            .map(|s| serde_json::json!({ "name": s.name }))
            .collect(),
        DataStructure::Document(doc) => doc
            .databases
            .iter()
            .map(|db| serde_json::json!({ "name": db.name }))
            .collect(),
        _ => vec![serde_json::json!({ "name": "default" })],
    };

    Ok(serde_json::json!({ "schemas": schemas }))
}

fn list_tables(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let database = optional_str(args, "database").unwrap_or("");
    let connection = get_or_connect(state, connection_id)?;

    let schema_info = connection
        .schema_for_database(database)
        .map_err(|e| format!("list_tables failed: {e}"))?;

    let mut tables: Vec<serde_json::Value> = schema_info
        .tables
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "schema": t.schema,
                "kind": "Table",
            })
        })
        .collect();

    let views: Vec<serde_json::Value> = schema_info
        .views
        .iter()
        .map(|v| {
            serde_json::json!({
                "name": v.name,
                "schema": v.schema,
                "kind": "View",
            })
        })
        .collect();

    tables.extend(views);

    Ok(serde_json::json!({ "tables": tables }))
}

fn describe_object(
    args: &serde_json::Value,
    state: &mut ServerState,
) -> Result<serde_json::Value, String> {
    let connection_id = require_str(args, "connection_id")?;
    let name = require_str(args, "name")?;
    let schema = optional_str(args, "schema");
    let connection = get_or_connect(state, connection_id)?;

    let table_ref = TableRef {
        schema: schema.map(str::to_string),
        name: name.to_string(),
    };

    let request = DescribeRequest::new(table_ref);
    let result = connection
        .describe_table(&request)
        .map_err(|e| format!("describe_object failed: {e}"))?;

    Ok(serialize_query_result(&result))
}

/// Serializes a `QueryResult` into a JSON value suitable for MCP responses.
pub fn serialize_query_result(result: &QueryResult) -> serde_json::Value {
    let columns: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();

    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (col, cell) in columns.iter().zip(row.iter()) {
                obj.insert((*col).to_string(), value_to_json(cell));
            }
            serde_json::Value::Object(obj)
        })
        .collect();

    serde_json::json!({
        "columns": columns,
        "rows": rows,
        "row_count": result.rows.len(),
    })
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(f.to_string())),
        Value::Text(s)
        | Value::Json(s)
        | Value::Decimal(s)
        | Value::ObjectId(s)
        | Value::Unsupported(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::json!({ "_type": "bytes", "length": b.len() }),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Document(doc) => {
            let map: serde_json::Map<_, _> = doc
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}
