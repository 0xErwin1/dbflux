//! End-to-end coverage for the MCP server as an agent consumes it.
//!
//! Every call here crosses the real JSON-RPC boundary: the server is served over
//! an in-memory transport and driven by a real MCP client. That covers the
//! handshake, the advertised capabilities, tool argument deserialization, the
//! governance decision (which surfaces as a JSON-RPC *error*, not as a tool
//! result carrying `is_error`), the approval lifecycle, and the audit trail as a
//! client actually observes them.
//!
//! Data comes from the SQLite driver against a temporary file. No Docker, no OS
//! keyring, no network.

use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::{
    AuthProfileManager, ConnectionProfile, DbConfig, DbDriver, NoopSecretStore, ProfileManager,
    SecretManager,
};
use dbflux_mcp::{
    ConnectionPolicyAssignmentDto, McpRuntime, TrustedClientDto, builtin_policies, builtin_roles,
};
use dbflux_mcp_server::{
    connection_cache::ConnectionCache, server::DbFluxServer, state::ServerState,
};
use dbflux_policy::{ConnectionPolicyAssignment, PolicyBindingScope};
use rmcp::{
    RoleClient, RoleServer, ServiceExt,
    model::{CallToolRequestParams, CallToolResult, ErrorCode, Tool},
    service::RunningService,
    transport::async_rw::AsyncRwTransport,
};
use serde_json::{Value, json};
use tokio::sync::RwLock;

/// Actor id every call in this file runs as.
const AGENT: &str = "e2e-agent";
const ADMIN_ROLE: &str = "builtin/admin";
const READ_ONLY_ROLE: &str = "builtin/read-only";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A live session: an initialized MCP client plus the server serving it.
///
/// The server half is held so the transport stays alive for the whole test.
struct Agent {
    client: RunningService<RoleClient, ()>,
    _server: RunningService<RoleServer, DbFluxServer>,
}

impl Agent {
    /// `tools/list`, paging through the whole catalog.
    async fn tools(&self) -> Vec<Tool> {
        self.client
            .peer()
            .list_all_tools()
            .await
            .expect("tools/list should succeed")
    }

    /// `tools/call`, keeping the protocol error instead of panicking on it.
    async fn try_call(
        &self,
        tool: &str,
        arguments: Value,
    ) -> Result<CallToolResult, rmcp::ServiceError> {
        let arguments = arguments.as_object().cloned().unwrap_or_default();
        self.client
            .peer()
            .call_tool(CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments))
            .await
    }

    /// `tools/call` for the happy path.
    async fn call(&self, tool: &str, arguments: Value) -> CallToolResult {
        self.try_call(tool, arguments)
            .await
            .unwrap_or_else(|error| panic!("tool '{tool}' failed at the protocol level: {error}"))
    }

    /// `tools/call` whose text content is the JSON document the tool publishes.
    async fn call_json(&self, tool: &str, arguments: Value) -> Value {
        let result = self.call(tool, arguments).await;
        let text = text_content(&result);
        serde_json::from_str(&text).unwrap_or_else(|error| {
            panic!("tool '{tool}' returned content that is not JSON ({error}): {text}")
        })
    }
}

/// Serves the server over an in-memory transport and completes the MCP
/// handshake with a real client.
async fn start_agent(
    role: &str,
    connection: Option<(Arc<dyn DbDriver>, ConnectionProfile)>,
) -> Agent {
    let state = build_state(role, connection);
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);

    let server_transport = {
        let (read, write) = tokio::io::split(server_io);
        AsyncRwTransport::new_server(read, write)
    };
    let client_transport = {
        let (read, write) = tokio::io::split(client_io);
        AsyncRwTransport::new_client(read, write)
    };

    let (server, client) = tokio::try_join!(
        async move {
            DbFluxServer::new(state)
                .serve(server_transport)
                .await
                .map_err(|error| error.to_string())
        },
        async move {
            rmcp::serve_client((), client_transport)
                .await
                .map_err(|error| error.to_string())
        },
    )
    .expect("the MCP handshake should complete on both ends");

    Agent {
        client,
        _server: server,
    }
}

/// Concatenates the text blocks of a tool result.
fn text_content(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|block| block.as_text())
        .map(|text| text.text.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

/// A SQLite profile over a file the test owns.
fn sqlite_profile(directory: &tempfile::TempDir) -> ConnectionProfile {
    ConnectionProfile::new(
        "agent-sqlite",
        DbConfig::SQLite {
            path: directory.path().join("agent.sqlite"),
            connection_id: None,
        },
    )
}

/// The driver registry entry, keyed the way the connection factory resolves it.
fn sqlite_driver() -> Arc<dyn DbDriver> {
    Arc::new(dbflux_driver_sqlite::SqliteDriver::new())
}

fn build_state(
    role: &str,
    connection: Option<(Arc<dyn DbDriver>, ConnectionProfile)>,
) -> ServerState {
    let connection_id = connection
        .as_ref()
        .map(|(_, profile)| profile.id.to_string());

    let mut profile_manager = ProfileManager::new_in_memory();
    if let Some((_, profile)) = &connection {
        profile_manager.add(profile.clone());
    }

    let driver_registry = match &connection {
        Some((driver, profile)) => HashMap::from([(profile.driver_id(), driver.clone())]),
        None => HashMap::new(),
    };

    ServerState {
        client_id: AGENT.to_string(),
        runtime: Arc::new(RwLock::new(build_runtime(connection_id.as_deref(), role))),
        profile_manager: Arc::new(RwLock::new(profile_manager)),
        auth_profile_manager: Arc::new(RwLock::new(AuthProfileManager::default())),
        driver_registry: Arc::new(driver_registry),
        auth_provider_registry: Arc::new(HashMap::new()),
        driver_settings: Arc::new(HashMap::new()),
        connection_cache: Arc::new(RwLock::new(ConnectionCache::new())),
        connection_setup_lock: Arc::new(tokio::sync::Mutex::new(())),
        secret_manager: Arc::new(SecretManager::new(Box::new(NoopSecretStore))),
        mcp_enabled_by_default: true,
    }
}

/// Mirrors the runtime `ServerState::new` builds from `dbflux.db`: built-in
/// roles and policies, one trusted client, and the global read-only assignment
/// plus a connection-scoped assignment for the same actor.
fn build_runtime(connection_id: Option<&str>, role: &str) -> McpRuntime {
    let audit_path = dbflux_audit::temp_sqlite_path("agent_e2e_audit.sqlite");
    let audit_service =
        dbflux_audit::AuditService::new_sqlite(&audit_path).expect("create the audit database");
    let mut runtime = McpRuntime::new(
        audit_service,
        Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
    );

    for builtin_role in builtin_roles() {
        runtime
            .upsert_role_mut(builtin_role)
            .expect("register built-in role");
    }
    for policy in builtin_policies() {
        runtime
            .upsert_policy_mut(policy)
            .expect("register built-in policy");
    }

    runtime
        .upsert_trusted_client_mut(TrustedClientDto {
            id: AGENT.to_string(),
            name: "MCP e2e agent".to_string(),
            issuer: None,
            active: true,
        })
        .expect("register the trusted client");

    let mut scopes = vec![String::new()];
    if let Some(connection_id) = connection_id {
        scopes.push(connection_id.to_string());
    }

    for scope in scopes {
        runtime
            .save_connection_policy_assignment_mut(ConnectionPolicyAssignmentDto {
                connection_id: scope.clone(),
                assignments: vec![ConnectionPolicyAssignment {
                    actor_id: AGENT.to_string(),
                    scope: PolicyBindingScope {
                        connection_id: scope.clone(),
                    },
                    role_ids: vec![role.to_string()],
                    policy_ids: vec![],
                }],
            })
            .expect("save the policy assignment");
    }

    runtime.drain_events();
    runtime
}

/// Connects, creates `items` and inserts two rows, returning the connection id.
async fn prepare_items_table(agent: &Agent, connection_id: &str) -> Value {
    agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;

    agent
        .call_json(
            "create_table",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "columns": [
                    { "name": "id", "type": "integer", "primary_key": true },
                    { "name": "label", "type": "text", "nullable": false }
                ]
            }),
        )
        .await;

    agent
        .call_json(
            "insert_record",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "records": [
                    { "id": 1, "label": "alpha" },
                    { "id": 2, "label": "beta" }
                ]
            }),
        )
        .await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agent_handshake_advertises_the_tool_catalog() {
    let agent = start_agent(ADMIN_ROLE, None).await;

    let info = agent
        .client
        .peer_info()
        .expect("the handshake should have negotiated server info")
        .clone();
    assert!(
        info.capabilities.tools.is_some(),
        "the server must advertise the tools capability"
    );
    assert!(
        info.capabilities.resources.is_none() && info.capabilities.prompts.is_none(),
        "the server advertises no resources or prompts, and must not claim otherwise"
    );

    let tools = agent.tools().await;
    assert!(
        tools.len() >= 30,
        "the catalog should expose the full tool surface, got {}",
        tools.len()
    );

    for tool in &tools {
        let schema_type = tool.input_schema.get("type").and_then(Value::as_str);
        assert_eq!(
            schema_type,
            Some("object"),
            "tool '{}' must publish an object input schema",
            tool.name
        );
    }

    for expected in [
        "list_connections",
        "list_tables",
        "select_data",
        "count_records",
        "preview_mutation",
        "create_table",
        "insert_record",
        "delete_records",
        "request_execution",
        "approve_execution",
        "query_audit_logs",
    ] {
        assert!(
            tools.iter().any(|tool| tool.name.as_ref() == expected),
            "tool '{expected}' is missing from the catalog"
        );
    }
}

#[tokio::test]
async fn agent_connects_and_browses_a_sqlite_profile() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    let connections = agent.call_json("list_connections", json!({})).await;
    let listed = connections["connections"]
        .as_array()
        .expect("list_connections should return a connections array");
    assert_eq!(
        listed.len(),
        1,
        "only the connection assigned to this actor should be listed"
    );
    assert_eq!(listed[0]["id"], json!(connection_id));
    assert_eq!(listed[0]["driver_id"], json!("sqlite"));

    let connected = agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;
    assert_eq!(connected["success"], json!(true));

    let tables = agent
        .call_json("list_tables", json!({ "connection_id": connection_id }))
        .await;
    assert!(
        tables["tables"]
            .as_array()
            .expect("list_tables should return a tables array")
            .is_empty(),
        "a fresh database has no tables"
    );

    let info = agent
        .call_json(
            "get_connection_info",
            json!({ "connection_id": connection_id }),
        )
        .await;
    assert!(
        info.is_object(),
        "get_connection_info should describe the live connection, got {info}"
    );

    let disconnected = agent
        .call_json("disconnect", json!({ "connection_id": connection_id }))
        .await;
    assert_eq!(disconnected["success"], json!(true));
}

#[tokio::test]
async fn agent_creates_inserts_and_reads_rows_over_the_wire() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    let inserted = prepare_items_table(&agent, &connection_id).await;
    assert_eq!(inserted["inserted"], json!(2));

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "where": { "label": "alpha" },
                "limit": 10
            }),
        )
        .await;

    let rows = selected["rows"]
        .as_array()
        .expect("select_data should return a rows array");
    assert_eq!(rows.len(), 1, "the filter should select exactly one row");
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["label"], json!("alpha"));

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(2));

    let tables = agent
        .call_json("list_tables", json!({ "connection_id": connection_id }))
        .await;
    let table_names: Vec<&str> = tables["tables"]
        .as_array()
        .expect("list_tables should return a tables array")
        .iter()
        .filter_map(|table| table.get("name").and_then(Value::as_str))
        .collect();
    assert!(
        table_names.contains(&"items"),
        "the table the agent created should be visible through list_tables, got {table_names:?}"
    );
}

#[tokio::test]
async fn read_only_agent_denial_is_a_jsonrpc_error_and_is_audited() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(READ_ONLY_ROLE, Some((sqlite_driver(), profile))).await;

    let error = agent
        .try_call(
            "insert_record",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "records": [{ "id": 1, "label": "alpha" }]
            }),
        )
        .await
        .expect_err("a read-only actor must not be allowed to write");

    // A policy denial is a protocol error, not a tool result: an agent sees
    // `error.code` / `error.data.code`, never `is_error`.
    let rmcp::ServiceError::McpError(error) = error else {
        panic!("expected an MCP error from the denial, got a different transport error");
    };
    assert_eq!(error.code, ErrorCode::INVALID_REQUEST);
    assert_eq!(error.message.as_ref(), "tool denied by policy");
    assert_eq!(
        error.data.as_ref().and_then(|data| data.get("code")),
        Some(&json!("policy_denied"))
    );

    // The denial is the agent's own audited evidence. `query_audit_logs` returns
    // the legacy projection (`id, actor_id, tool_id, decision, reason`), where
    // `tool_id` is the audit action rather than the MCP tool name and the deny
    // reason is not surfaced, so the decision is what an agent can read back.
    let events = agent
        .call_json(
            "query_audit_logs",
            json!({ "actor_id": AGENT, "limit": 50 }),
        )
        .await;
    let entries = events
        .as_array()
        .expect("query_audit_logs should return an array of events");
    assert!(
        !entries.is_empty(),
        "the agent should be able to read its own audit trail: {events}"
    );

    let denials: Vec<&Value> = entries
        .iter()
        .filter(|entry| entry["decision"] == json!("failure"))
        .collect();
    assert_eq!(
        denials.len(),
        1,
        "exactly one authorization denial should be recorded: {events}"
    );
    assert_eq!(denials[0]["actor_id"], json!(AGENT));
}

#[tokio::test]
async fn agent_requests_approval_and_replays_the_approved_plan() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id).await;

    let requested = agent
        .call_json(
            "request_execution",
            json!({
                "tool_id": "delete_records",
                "connection_id": connection_id,
                "params": {
                    "connection_id": connection_id,
                    "table": "items",
                    "where": { "id": 1 }
                }
            }),
        )
        .await;
    let pending_id = requested["pending_id"]
        .as_str()
        .expect("request_execution should return a pending id")
        .to_string();
    assert_eq!(requested["status"], json!("pending"));

    let pending = agent
        .call_json("list_pending_executions", json!({ "actor_id": AGENT }))
        .await;
    assert!(
        pending.to_string().contains(&pending_id),
        "the pending execution should be listed: {pending}"
    );

    let approved = agent
        .call_json("approve_execution", json!({ "pending_id": pending_id }))
        .await;
    assert_eq!(approved["approved"], json!(true));
    let replay = &approved["replay_plan"];
    assert_eq!(replay["tool_id"], json!("delete_records"));
    assert_eq!(
        replay["params"]["where"],
        json!({ "id": 1 }),
        "the approval returns the plan the agent must replay"
    );

    // Replaying the plan the server handed back is the step an agent performs.
    let executed = agent
        .call_json("delete_records", replay["params"].clone())
        .await;
    assert_eq!(executed["deleted"], json!(1));

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(1));
}
