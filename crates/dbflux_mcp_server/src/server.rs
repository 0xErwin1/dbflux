use std::io::{BufRead, Write};

use dbflux_mcp::{
    CANONICAL_V1_TOOLS,
    server::{
        authorization::{AuthorizationRequest, authorize_request},
        router::{RouteTarget, route_tool},
    },
};
use dbflux_policy::PolicyEngine;

use crate::{
    bootstrap::{ServerState, is_mcp_enabled_for_connection},
    handlers,
    transport::{codes, error_response, success_response, write_message},
};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "dbflux-mcp-server";

/// Runs the main stdio dispatch loop.
///
/// Reads JSON-RPC messages from `reader` and writes responses to `writer`
/// until EOF or a fatal I/O error. Returns when the client closes stdin.
pub fn run(
    state: &mut ServerState,
    reader: &mut impl BufRead,
    writer: &mut impl Write,
) -> anyhow::Result<()> {
    loop {
        let message = match crate::transport::read_message(reader) {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                log::info!("Client closed stdin — shutting down");
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                // Malformed JSON line: send a parse error and continue.
                let response = error_response(
                    &serde_json::Value::Null,
                    codes::PARSE_ERROR,
                    "Parse error",
                    Some(serde_json::json!(e.to_string())),
                );
                write_message(writer, &response)?;
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let id = message
            .get("id")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let method = message
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");

        // Notifications (no id) are acknowledged but not responded to.
        let is_notification = message.get("id").is_none();

        log::debug!("Received method={method}");

        let response = dispatch(state, &id, method, message.get("params"));

        if !is_notification {
            if let Some(resp) = response {
                write_message(writer, &resp)?;
            }
        }
    }
}

fn dispatch(
    state: &mut ServerState,
    id: &serde_json::Value,
    method: &str,
    params: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    match method {
        "initialize" => Some(handle_initialize(id, params)),
        "initialized" => None,
        "tools/list" => Some(handle_tools_list(id, params)),
        "tools/call" => Some(handle_tools_call(state, id, params)),
        other => {
            log::warn!("Unknown method: {other}");
            Some(error_response(
                id,
                codes::METHOD_NOT_FOUND,
                &format!("Method not found: {other}"),
                None,
            ))
        }
    }
}

fn handle_initialize(
    id: &serde_json::Value,
    _params: Option<&serde_json::Value>,
) -> serde_json::Value {
    success_response(
        id,
        serde_json::json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "serverInfo": {
                "name": SERVER_NAME,
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "tools": {},
            },
        }),
    )
}

fn handle_tools_list(
    id: &serde_json::Value,
    _params: Option<&serde_json::Value>,
) -> serde_json::Value {
    let tools: Vec<serde_json::Value> = CANONICAL_V1_TOOLS
        .iter()
        .map(|tool_id| build_tool_descriptor(tool_id))
        .collect();

    success_response(id, serde_json::json!({ "tools": tools }))
}

fn build_tool_descriptor(tool_id: &str) -> serde_json::Value {
    let (description, input_schema) = tool_metadata(tool_id);

    serde_json::json!({
        "name": tool_id,
        "description": description,
        "inputSchema": input_schema,
    })
}

fn tool_metadata(tool_id: &str) -> (&'static str, serde_json::Value) {
    match tool_id {
        "list_connections" => (
            "List all configured database connections.",
            serde_json::json!({ "type": "object", "properties": {} }),
        ),
        "get_connection" => (
            "Get details for a specific connection by ID.",
            serde_json::json!({
                "type": "object",
                "properties": { "connection_id": { "type": "string" } },
                "required": ["connection_id"],
            }),
        ),
        "get_connection_metadata" => (
            "Get metadata (driver, category, capabilities) for a connection.",
            serde_json::json!({
                "type": "object",
                "properties": { "connection_id": { "type": "string" } },
                "required": ["connection_id"],
            }),
        ),
        "list_databases" => (
            "List databases available on a connection.",
            serde_json::json!({
                "type": "object",
                "properties": { "connection_id": { "type": "string" } },
                "required": ["connection_id"],
            }),
        ),
        "list_schemas" => (
            "List schemas in a database.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "database": { "type": "string" },
                },
                "required": ["connection_id"],
            }),
        ),
        "list_tables" => (
            "List tables and views in a schema.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "database": { "type": "string" },
                    "schema": { "type": "string" },
                },
                "required": ["connection_id"],
            }),
        ),
        "list_collections" => (
            "List collections in a MongoDB database.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "database": { "type": "string" },
                },
                "required": ["connection_id"],
            }),
        ),
        "describe_object" => (
            "Describe the structure of a table, view, or collection.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "database": { "type": "string" },
                    "schema": { "type": "string" },
                    "name": { "type": "string" },
                },
                "required": ["connection_id", "name"],
            }),
        ),
        "read_query" => (
            "Execute a read-only query and return the results.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "sql": { "type": "string" },
                    "database": { "type": "string" },
                    "limit": { "type": "integer" },
                    "offset": { "type": "integer" },
                },
                "required": ["connection_id", "sql"],
            }),
        ),
        "explain_query" => (
            "Explain the execution plan for a query.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "sql": { "type": "string" },
                    "table": { "type": "string" },
                    "database": { "type": "string" },
                },
                "required": ["connection_id"],
            }),
        ),
        "preview_mutation" => (
            "Preview the effects of a mutation without executing it.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "sql": { "type": "string" },
                },
                "required": ["connection_id", "sql"],
            }),
        ),
        "list_scripts" => (
            "List saved scripts.",
            serde_json::json!({ "type": "object", "properties": {} }),
        ),
        "get_script" => (
            "Get the content of a saved script by ID.",
            serde_json::json!({
                "type": "object",
                "properties": { "script_id": { "type": "string" } },
                "required": ["script_id"],
            }),
        ),
        "create_script" => (
            "Create a new saved script.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "content": { "type": "string" },
                },
                "required": ["name", "content"],
            }),
        ),
        "update_script" => (
            "Update an existing saved script.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "script_id": { "type": "string" },
                    "content": { "type": "string" },
                },
                "required": ["script_id", "content"],
            }),
        ),
        "delete_script" => (
            "Delete a saved script.",
            serde_json::json!({
                "type": "object",
                "properties": { "script_id": { "type": "string" } },
                "required": ["script_id"],
            }),
        ),
        "run_script" => (
            "Run a saved script against a connection.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "script_id": { "type": "string" },
                },
                "required": ["connection_id", "script_id"],
            }),
        ),
        "request_execution" => (
            "Request approval to execute a write or destructive operation.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "connection_id": { "type": "string" },
                    "tool_id": { "type": "string" },
                    "payload": { "type": "object" },
                },
                "required": ["connection_id", "tool_id", "payload"],
            }),
        ),
        "list_pending_executions" => (
            "List executions pending operator approval.",
            serde_json::json!({ "type": "object", "properties": {} }),
        ),
        "get_pending_execution" => (
            "Get details of a specific pending execution.",
            serde_json::json!({
                "type": "object",
                "properties": { "pending_id": { "type": "string" } },
                "required": ["pending_id"],
            }),
        ),
        "approve_execution" => (
            "Approve a pending execution (admin only).",
            serde_json::json!({
                "type": "object",
                "properties": { "pending_id": { "type": "string" } },
                "required": ["pending_id"],
            }),
        ),
        "reject_execution" => (
            "Reject a pending execution (admin only).",
            serde_json::json!({
                "type": "object",
                "properties": { "pending_id": { "type": "string" } },
                "required": ["pending_id"],
            }),
        ),
        "query_audit_logs" => (
            "Query the audit log for past tool executions.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "actor_id": { "type": "string" },
                    "tool_id": { "type": "string" },
                    "decision": { "type": "string" },
                    "limit": { "type": "integer" },
                },
            }),
        ),
        "get_audit_entry" => (
            "Get a specific audit log entry by ID.",
            serde_json::json!({
                "type": "object",
                "properties": { "entry_id": { "type": "string" } },
                "required": ["entry_id"],
            }),
        ),
        "export_audit_logs" => (
            "Export audit logs in CSV or JSON format.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "format": { "type": "string", "enum": ["csv", "json"] },
                },
                "required": ["format"],
            }),
        ),
        _ => (
            "Unknown tool.",
            serde_json::json!({ "type": "object", "properties": {} }),
        ),
    }
}

fn handle_tools_call(
    state: &mut ServerState,
    id: &serde_json::Value,
    params: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(params) = params else {
        return error_response(id, codes::INVALID_PARAMS, "Missing params", None);
    };

    let tool_id = match params.get("name").and_then(serde_json::Value::as_str) {
        Some(t) => t,
        None => {
            return error_response(id, codes::INVALID_PARAMS, "Missing tool name", None);
        }
    };

    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or(serde_json::Value::Object(Default::default()));

    let connection_id = args
        .get("connection_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();

    // Validate tool and route it.
    let route = match route_tool(tool_id) {
        Ok(route) => route,
        Err(e) => {
            return error_response(id, codes::METHOD_NOT_FOUND, &e.to_string(), None);
        }
    };

    // Determine classification for the tool.
    let classification = classification_for_tool(tool_id);

    // Check whether MCP is enabled for this connection.
    let mcp_enabled_for_connection = is_mcp_enabled_for_connection(
        &state.profile_manager,
        state.mcp_enabled_by_default,
        &connection_id,
    );

    // Authorization gate.
    let trusted_clients = state.runtime.trusted_client_registry();
    let roles = state.runtime.roles_for_engine();
    let policies = state.runtime.policies_for_engine();
    let assignments = state.runtime.policy_assignments_for_engine();

    let policy_engine = PolicyEngine::new(assignments, roles, policies);

    let auth_request = AuthorizationRequest {
        identity: dbflux_mcp::server::request_context::RequestIdentity {
            client_id: state.client_id.clone(),
            issuer: None,
        },
        connection_id: connection_id.clone(),
        tool_id: tool_id.to_string(),
        classification,
        mcp_enabled_for_connection,
    };

    let outcome = match authorize_request(
        &trusted_clients,
        &policy_engine,
        state.runtime.audit_service(),
        &auth_request,
        now_epoch_ms(),
    ) {
        Ok(outcome) => outcome,
        Err(e) => {
            return error_response(
                id,
                codes::INTERNAL_ERROR,
                &format!("Authorization error: {e}"),
                None,
            );
        }
    };

    if !outcome.allowed {
        return error_response(
            id,
            codes::AUTHORIZATION_DENIED,
            outcome
                .deny_reason
                .as_deref()
                .unwrap_or("authorization denied"),
            outcome
                .deny_code
                .map(|code| serde_json::json!({ "code": code })),
        );
    }

    // Dispatch to handler.
    let result = match route {
        RouteTarget::Discovery => handlers::discovery::handle(tool_id, &args, state),
        RouteTarget::Schema => handlers::schema::handle(tool_id, &args, state),
        RouteTarget::Query => handlers::query::handle(tool_id, &args, state),
        RouteTarget::Scripts => handlers::scripts::handle(tool_id, &args, state),
        RouteTarget::Approval => handlers::approval::handle(tool_id, &args, state),
        RouteTarget::Audit => handlers::audit::handle(tool_id, &args, state),
    };

    match result {
        Ok(content) => success_response(
            id,
            serde_json::json!({
                "content": [{ "type": "text", "text": content.to_string() }],
            }),
        ),
        Err(e) => error_response(id, codes::INTERNAL_ERROR, &e, None),
    }
}

fn classification_for_tool(tool_id: &str) -> dbflux_policy::ExecutionClassification {
    use dbflux_policy::ExecutionClassification;

    match tool_id {
        "list_connections" | "get_connection" | "get_connection_metadata" => {
            ExecutionClassification::Metadata
        }
        "list_databases" | "list_schemas" | "list_tables" | "list_collections"
        | "describe_object" => ExecutionClassification::Metadata,
        "read_query" | "explain_query" => ExecutionClassification::Read,
        "preview_mutation" => ExecutionClassification::Read,
        "list_scripts" | "get_script" => ExecutionClassification::Metadata,
        "create_script" | "update_script" | "run_script" | "request_execution" => {
            ExecutionClassification::Write
        }
        "delete_script" => ExecutionClassification::Destructive,
        "list_pending_executions" | "get_pending_execution" => ExecutionClassification::Metadata,
        "approve_execution" | "reject_execution" => ExecutionClassification::Admin,
        "query_audit_logs" | "get_audit_entry" => ExecutionClassification::Metadata,
        "export_audit_logs" => ExecutionClassification::Admin,
        _ => ExecutionClassification::Metadata,
    }
}

fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
