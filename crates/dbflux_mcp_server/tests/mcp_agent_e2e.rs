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

use dbflux_core::observability::EventOrigin;
use dbflux_core::{
    AuthProfileManager, ConnectionProfile, DbConfig, DbDriver, NoopSecretStore, ProfileManager,
    SecretManager,
};
use dbflux_mcp::{
    ConnectionPolicyAssignmentDto, McpRuntime, PolicyRoleDto, ToolPolicyDto, TrustedClientDto,
    builtin_policies, builtin_roles,
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
/// A custom role whose policy allows every class without approval: the
/// explicit opt-in a user makes with "Allow all without approval".
const ALLOW_ALL_ROLE: &str = "e2e/allow-all";
/// Custom roles that allow `select_data` and `count_records` and, for the
/// second one, `list_tables` as well.
const READ_TOOLS_ONLY_ROLE: &str = "e2e/read-tools-only";
const READ_AND_LIST_TABLES_ROLE: &str = "e2e/read-and-list-tables";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A live session: an initialized MCP client plus the server serving it.
///
/// The server half is held so the transport stays alive for the whole test.
struct Agent {
    client: RunningService<RoleClient, ()>,
    _server: RunningService<RoleServer, DbFluxServer>,
    runtime: Arc<RwLock<McpRuntime>>,
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

    /// `tools/call` that a policy sends to approval: the first call must be
    /// queued, a person approves it the way the DBFlux UI does, and the same
    /// call is repeated to run it.
    async fn call_json_with_approval(&self, tool: &str, arguments: Value) -> Value {
        let pending_id = expect_queued(self.try_call(tool, arguments.clone()).await);
        self.approve_as_person(&pending_id).await;
        self.call_json(tool, arguments).await
    }

    /// Approves a pending execution through the runtime, as the DBFlux UI does.
    async fn approve_as_person(&self, pending_id: &str) {
        self.runtime
            .write()
            .await
            .approve_pending_execution_with_origin_mut(pending_id, "local", EventOrigin::local())
            .expect("a person should be able to approve the pending execution");
    }
}

/// Asserts that a call was queued for approval and returns its pending id.
fn expect_queued(result: Result<CallToolResult, rmcp::ServiceError>) -> String {
    let error = result.expect_err("the call should wait for approval instead of running");
    let rmcp::ServiceError::McpError(error) = error else {
        panic!("expected an MCP error for a queued call");
    };

    assert_eq!(error.code, ErrorCode::INVALID_REQUEST);
    let data = error.data.expect("a queued call carries error data");
    assert_eq!(data["code"], json!("approval_required"));
    assert_eq!(data["status"], json!("pending"));
    assert_eq!(
        data["next_action"],
        json!("wait_for_human_approval_then_repeat_identical_call")
    );
    assert_eq!(data["status_tool"], json!("get_pending_execution"));

    let pending_id = data["pending_id"].as_str().unwrap_or_default();
    assert!(
        error
            .message
            .contains(&format!("pending execution {pending_id}"))
            && error.message.contains("repeat this exact call"),
        "the error must tell the agent what to do next: {}",
        error.message
    );

    data["pending_id"]
        .as_str()
        .expect("a queued call carries its pending id")
        .to_string()
}

/// Serves the server over an in-memory transport and completes the MCP
/// handshake with a real client.
async fn start_agent(
    role: &str,
    connection: Option<(Arc<dyn DbDriver>, ConnectionProfile)>,
) -> Agent {
    let state = build_state(role, connection);
    let runtime = state.runtime.clone();
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
        runtime,
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

    let admin_tools = builtin_policies()
        .into_iter()
        .find(|policy| policy.id == ADMIN_ROLE)
        .expect("the built-in admin policy exists")
        .allowed_tools;
    runtime
        .upsert_policy_mut(ToolPolicyDto {
            id: ALLOW_ALL_ROLE.to_string(),
            allowed_tools: admin_tools,
            allowed_classes: [
                "metadata",
                "read",
                "write",
                "destructive",
                "admin_safe",
                "admin",
                "admin_destructive",
            ]
            .iter()
            .map(|class| class.to_string())
            .collect(),
            approval_classes: Vec::new(),
        })
        .expect("register the allow-all policy");
    runtime
        .upsert_role_mut(PolicyRoleDto {
            id: ALLOW_ALL_ROLE.to_string(),
            policy_ids: vec![ALLOW_ALL_ROLE.to_string()],
        })
        .expect("register the allow-all role");

    let narrow_roles = [
        (READ_TOOLS_ONLY_ROLE, vec!["select_data", "count_records"]),
        (
            READ_AND_LIST_TABLES_ROLE,
            vec!["select_data", "count_records", "list_tables"],
        ),
    ];
    for (narrow_role, tools) in narrow_roles {
        runtime
            .upsert_policy_mut(ToolPolicyDto {
                id: narrow_role.to_string(),
                allowed_tools: tools.into_iter().map(str::to_string).collect(),
                allowed_classes: vec!["metadata".to_string(), "read".to_string()],
                approval_classes: Vec::new(),
            })
            .expect("register a narrow policy");
        runtime
            .upsert_role_mut(PolicyRoleDto {
                id: narrow_role.to_string(),
                policy_ids: vec![narrow_role.to_string()],
            })
            .expect("register a narrow role");
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

/// Connects, creates `items` and inserts two rows, returning the insert
/// result. With `approve` set, each mutating call is approved by a person
/// before it runs, as the built-in admin policy requires.
async fn prepare_items_table(agent: &Agent, connection_id: &str, approve: bool) -> Value {
    agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;

    let create_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "columns": [
            { "name": "id", "type": "integer", "primary_key": true },
            { "name": "label", "type": "text", "nullable": false }
        ]
    });
    let insert_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "records": [
            { "id": 1, "label": "alpha" },
            { "id": 2, "label": "beta" }
        ]
    });

    if approve {
        agent
            .call_json_with_approval("create_table", create_arguments)
            .await;
        return agent
            .call_json_with_approval("insert_record", insert_arguments)
            .await;
    }

    agent.call_json("create_table", create_arguments).await;
    agent.call_json("insert_record", insert_arguments).await
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
    assert_eq!(
        connected["current_database"],
        json!("main"),
        "connect should name the database the session is on, got {connected}"
    );
    assert!(
        connected.get("databases").is_none(),
        "SQLite lists no databases, so the field is omitted, got {connected}"
    );

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
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    let inserted = prepare_items_table(&agent, &connection_id, false).await;
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
async fn list_tables_names_only_returns_names_as_strings() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let detailed = agent
        .call_json("list_tables", json!({ "connection_id": connection_id }))
        .await;
    let entries = detailed["tables"]
        .as_array()
        .expect("list_tables should return a tables array");
    assert_eq!(entries.len(), 1, "got {detailed}");

    let entry = entries[0]
        .as_object()
        .expect("without names_only each entry is an object");
    assert_eq!(entry["name"], json!("items"));
    assert_eq!(entry["kind"], json!("Table"));
    assert!(entry.contains_key("schema"), "got {detailed}");
    assert_eq!(entry.len(), 3, "got {detailed}");

    let explicit_default = agent
        .call_json(
            "list_tables",
            json!({ "connection_id": connection_id, "names_only": false }),
        )
        .await;
    assert_eq!(
        explicit_default, detailed,
        "names_only: false is the same as leaving it out"
    );

    let names = agent
        .call_json(
            "list_tables",
            json!({ "connection_id": connection_id, "names_only": true }),
        )
        .await;
    assert_eq!(names, json!({ "tables": ["items"] }));

    let collection_names = agent
        .call_json(
            "list_collections",
            json!({ "connection_id": connection_id, "names_only": true }),
        )
        .await;
    assert_eq!(collection_names, names);
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
async fn ask_call_is_queued_approved_by_a_person_and_runs_exactly_once() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    let inserted = prepare_items_table(&agent, &connection_id, true).await;
    assert_eq!(inserted["inserted"], json!(2));

    let delete_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "where": { "id": 1 }
    });

    let pending_id = expect_queued(
        agent
            .try_call("delete_records", delete_arguments.clone())
            .await,
    );

    let pending = agent
        .call_json("list_pending_executions", json!({ "actor_id": AGENT }))
        .await;
    assert!(
        pending.to_string().contains(&pending_id),
        "the queued call should be listed: {pending}"
    );

    let counted_before = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(
        counted_before["count"],
        json!(2),
        "a queued call must not run before it is approved"
    );

    agent.approve_as_person(&pending_id).await;

    let executed = agent
        .call_json("delete_records", delete_arguments.clone())
        .await;
    assert_eq!(executed["deleted"], json!(1));

    let second_pending = expect_queued(agent.try_call("delete_records", delete_arguments).await);
    assert_ne!(
        second_pending, pending_id,
        "one approval authorizes one call; repeating it queues a new request"
    );
}

#[tokio::test]
async fn rejected_call_never_runs() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, true).await;

    let delete_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "where": { "id": 2 }
    });
    let pending_id = expect_queued(
        agent
            .try_call("delete_records", delete_arguments.clone())
            .await,
    );

    agent
        .runtime
        .write()
        .await
        .reject_pending_execution_with_origin_mut(
            &pending_id,
            "local",
            Some("not approved"),
            EventOrigin::local(),
        )
        .expect("a person should be able to reject the pending execution");

    let status = agent
        .call_json("get_pending_execution", json!({ "pending_id": pending_id }))
        .await;
    assert_eq!(status["status"], json!("rejected"));
    assert_eq!(
        status["reason"],
        json!("not approved"),
        "the agent should read the reason the person gave: {status}"
    );

    expect_queued(agent.try_call("delete_records", delete_arguments).await);

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(2));
}

#[tokio::test]
async fn agent_requests_approval_and_cannot_approve_it_itself() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, true).await;

    let delete_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "where": { "id": 1 }
    });

    let requested = agent
        .call_json(
            "request_execution",
            json!({
                "tool_id": "delete_records",
                "connection_id": connection_id,
                "params": delete_arguments
            }),
        )
        .await;
    let pending_id = requested["pending_id"]
        .as_str()
        .expect("request_execution should return a pending id")
        .to_string();
    assert_eq!(requested["status"], json!("pending"));

    for tool in ["approve_execution", "reject_execution"] {
        let error = agent
            .try_call(tool, json!({ "pending_id": pending_id }))
            .await
            .expect_err("an MCP client must never resolve a pending execution");
        let rmcp::ServiceError::McpError(error) = error else {
            panic!("expected an MCP error for {tool}");
        };
        assert_eq!(
            error.data.as_ref().and_then(|data| data.get("code")),
            Some(&json!("self_approval_forbidden")),
            "{tool}"
        );
    }

    agent.approve_as_person(&pending_id).await;

    let executed = agent.call_json("delete_records", delete_arguments).await;
    assert_eq!(executed["deleted"], json!(1));

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(1));
}

// ---------------------------------------------------------------------------
// Not-found hints
// ---------------------------------------------------------------------------

const NOT_LISTED_REASON: &str = "It may not exist, or this connection may not have access to it.";

/// Returns the message of a call that must fail as a tool error.
async fn tool_error(agent: &Agent, tool: &str, arguments: Value) -> String {
    let error = agent
        .try_call(tool, arguments)
        .await
        .expect_err("the call should fail");

    let rmcp::ServiceError::McpError(error) = error else {
        panic!("expected an MCP error from '{tool}', got {error}");
    };

    error.message.to_string()
}

/// A relational driver for the hint tests. Queries and `describe_table` fail
/// with a generic query error, `count_table` with a permission error. The
/// schema lists `public.users` and `public.orders`; `users` has the columns
/// `id` and `email`, and `orders` reports an empty column list.
struct HintDriver {
    inner: dbflux_test_support::FakeDriver,
}

struct HintConnection {
    inner: Box<dyn dbflux_core::Connection>,
}

impl HintDriver {
    fn with_databases(databases: &[&str]) -> Arc<dyn DbDriver> {
        use dbflux_core::{DatabaseInfo, DbKind, DbSchemaInfo, RelationalSchema, SchemaSnapshot};

        let tables = ["users", "orders"]
            .into_iter()
            .map(|name| hint_table(name, None))
            .collect();

        let schema = SchemaSnapshot::relational(RelationalSchema {
            databases: databases
                .iter()
                .map(|name| DatabaseInfo {
                    name: (*name).to_string(),
                    is_current: *name == "postgres",
                })
                .collect(),
            current_database: Some("postgres".to_string()),
            schemas: vec![DbSchemaInfo {
                name: "public".to_string(),
                tables,
                views: Vec::new(),
                custom_types: None,
            }],
            tables: Vec::new(),
            views: Vec::new(),
        });

        let inner = dbflux_test_support::FakeDriver::new(DbKind::Postgres)
            .with_schema(schema)
            .with_default_error("engine says no");

        Arc::new(Self { inner })
    }
}

fn hint_table(name: &str, columns: Option<&[&str]>) -> dbflux_core::TableInfo {
    let columns = columns.map(|names| {
        names
            .iter()
            .map(|column| {
                json!({
                    "name": column,
                    "type_name": "text",
                    "nullable": true,
                    "is_primary_key": false,
                    "default_value": null
                })
            })
            .collect::<Vec<_>>()
    });

    serde_json::from_value(json!({
        "name": name,
        "schema": "public",
        "columns": columns,
        "indexes": null
    }))
    .expect("build the table metadata")
}

impl DbDriver for HintDriver {
    fn kind(&self) -> dbflux_core::DbKind {
        self.inner.kind()
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        self.inner.metadata()
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        self.inner.driver_key()
    }

    fn form_definition(&self) -> &dbflux_core::DriverFormDef {
        self.inner.form_definition()
    }

    fn build_config(
        &self,
        values: &dbflux_core::FormValues,
    ) -> Result<DbConfig, dbflux_core::DbError> {
        self.inner.build_config(values)
    }

    fn extract_values(&self, config: &DbConfig) -> dbflux_core::FormValues {
        self.inner.extract_values(config)
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&dbflux_core::secrecy::SecretString>,
        ssh_secret: Option<&dbflux_core::secrecy::SecretString>,
    ) -> Result<Box<dyn dbflux_core::Connection>, dbflux_core::DbError> {
        let inner = self
            .inner
            .connect_with_secrets(profile, password, ssh_secret)?;

        Ok(Box::new(HintConnection { inner }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), dbflux_core::DbError> {
        self.inner.test_connection(profile)
    }
}

impl dbflux_core::Connection for HintConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        self.inner.metadata()
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        self.inner.ping()
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        self.inner.close()
    }

    fn execute(
        &self,
        request: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        self.inner.execute(request)
    }

    fn cancel(&self, handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        self.inner.cancel(handle)
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        self.inner.schema()
    }

    fn list_databases(&self) -> Result<Vec<dbflux_core::DatabaseInfo>, dbflux_core::DbError> {
        self.inner.list_databases()
    }

    fn active_database(&self) -> Option<String> {
        Some("postgres".to_string())
    }

    fn kind(&self) -> dbflux_core::DbKind {
        self.inner.kind()
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        self.inner.schema_loading_strategy()
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        self.inner.dialect()
    }

    fn count_table(
        &self,
        _request: &dbflux_core::TableCountRequest,
    ) -> Result<u64, dbflux_core::DbError> {
        Err(dbflux_core::DbError::permission_denied("no access"))
    }

    fn describe_table(
        &self,
        _request: &dbflux_core::DescribeRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::query_failed("engine says no"))
    }

    fn table_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, dbflux_core::DbError> {
        match table {
            "users" => Ok(hint_table(table, Some(&["id", "email"]))),
            "orders" => Ok(hint_table(table, Some(&[]))),
            _ => Err(dbflux_core::DbError::object_not_found(table)),
        }
    }
}

/// Starts an agent with `role` on the hint driver and returns it with the
/// connection id.
async fn start_hint_agent(role: &str, databases: &[&str]) -> (Agent, String) {
    let profile = ConnectionProfile::new("agent-fake", DbConfig::default_postgres());
    let connection_id = profile.id.to_string();
    let driver = HintDriver::with_databases(databases);
    let agent = start_agent(role, Some((driver, profile))).await;

    (agent, connection_id)
}

#[tokio::test]
async fn unlisted_table_gets_a_suggestion_from_every_read_tool() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let calls = [
        (
            "select_data",
            json!({ "connection_id": connection_id, "table": "itms" }),
        ),
        (
            "count_records",
            json!({ "connection_id": connection_id, "table": "itms" }),
        ),
        (
            "aggregate_data",
            json!({
                "connection_id": connection_id,
                "table": "itms",
                "group_by": [],
                "aggregations": [{ "function": "count", "column": "*", "alias": "total" }]
            }),
        ),
    ];

    for (tool, arguments) in calls {
        let message = tool_error(&agent, tool, arguments).await;

        assert!(
            message.starts_with(&format!(
                "Table 'itms' is not listed in database 'main'. {NOT_LISTED_REASON}\n\
                 Did you mean: items?\n\nOriginal error: "
            )),
            "{tool} should say where the table was looked up, got: {message}"
        );
        assert!(
            message.contains("no such table"),
            "{tool} should keep the driver error, got: {message}"
        );
        assert!(
            !message.contains("another database"),
            "SQLite lists no other database, got: {message}"
        );
    }

    let qualified = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "other.itms" }),
    )
    .await;
    assert!(
        qualified.starts_with("Select error: "),
        "a qualifier that is not a known database keeps the driver error, got: {qualified}"
    );
}

#[tokio::test]
async fn misspelled_column_gets_a_suggestion() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let correct = agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "items", "columns": ["label"] }),
        )
        .await;
    assert_eq!(correct["columns"], json!(["label"]));
    assert_eq!(correct["row_count"], json!(2));

    let projected = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "items", "columns": ["id", "labl"] }),
    )
    .await;
    assert_eq!(
        projected,
        "Select error: column 'labl' not found in result set. Did you mean: label? \
         Available columns: id, label"
    );

    // SQLite reads an unknown double-quoted identifier as a string literal, so
    // a misspelled column alone does not fail. `$regex` renders an operator
    // SQLite rejects, so the call fails, which is when the hint is added.
    let expected = "Column 'labl' is not listed among the columns of table 'items'.\n\
                    Did you mean: label?\n\
                    Available columns: id, label\n\nOriginal error: ";

    let filtered = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "labl": { "$regex": "^a" } }
        }),
    )
    .await;
    assert!(filtered.starts_with(expected), "got: {filtered}");

    let sorted = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "label": { "$regex": "^a" } },
            "order_by": [{ "column": "labl", "direction": "desc" }]
        }),
    )
    .await;
    assert!(sorted.starts_with(expected), "got: {sorted}");

    let counted = tool_error(
        &agent,
        "count_records",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "labl": { "$regex": "^a" } }
        }),
    )
    .await;
    assert!(counted.starts_with(expected), "got: {counted}");
}

#[tokio::test]
async fn failure_on_an_existing_table_with_valid_columns_keeps_the_original_error() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "label": { "$regex": "^a" } }
        }),
    )
    .await;

    assert!(message.starts_with("Select error: "), "got: {message}");
    assert!(!message.contains("not listed"), "got: {message}");
    assert!(!message.contains("Did you mean"), "got: {message}");
}

#[tokio::test]
async fn another_database_note_needs_several_databases_and_no_database_argument() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres", "analytics"]).await;

    let without_database = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres" }),
    )
    .await;
    assert_eq!(
        without_database,
        format!(
            "Table 'usres' is not listed in schema 'public' of database 'postgres'. \
             {NOT_LISTED_REASON}\n\
             Did you mean: users?\n\
             This call did not pass `database`, so the table may be in another database. \
             Pass `database` to select one (`list_databases` lists them).\n\n\
             Original error: Select error: engine says no"
        )
    );

    let with_database = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres", "database": "postgres" }),
    )
    .await;
    assert!(
        with_database.contains("Did you mean: users?"),
        "got: {with_database}"
    );
    assert!(
        !with_database.contains("another database"),
        "the call chose its database, got: {with_database}"
    );

    let described = tool_error(
        &agent,
        "describe_object",
        json!({ "connection_id": connection_id, "name": "ordrs" }),
    )
    .await;
    assert!(
        described.starts_with(&format!(
            "Table 'ordrs' is not listed in schema 'public' of database 'postgres'. \
             {NOT_LISTED_REASON}\nDid you mean: orders?\n"
        )),
        "describe_object looks in the default schema only, got: {described}"
    );
}

#[tokio::test]
async fn another_database_note_is_absent_with_a_single_database() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres"]).await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres" }),
    )
    .await;

    assert!(message.contains("Did you mean: users?"), "got: {message}");
    assert!(!message.contains("another database"), "got: {message}");
}

#[tokio::test]
async fn hint_is_skipped_when_the_metadata_cannot_settle_the_failure() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres", "analytics"]).await;
    let original = "Select error: engine says no";

    let cases = [
        ("an existing table", json!({ "table": "users" })),
        (
            "a qualifier that is not a schema of the database",
            json!({ "table": "nope.usres" }),
        ),
        (
            "an order_by expression",
            json!({ "table": "users", "order_by": [{ "column": "lower(emial)" }] }),
        ),
        (
            "a table with an empty column list",
            json!({ "table": "orders", "where": { "totl": 1 } }),
        ),
    ];

    for (case, mut arguments) in cases {
        arguments["connection_id"] = json!(connection_id);

        let message = tool_error(&agent, "select_data", arguments).await;

        assert_eq!(message, original, "{case} keeps the driver error");
    }

    let denied = tool_error(
        &agent,
        "count_records",
        json!({ "connection_id": connection_id, "table": "usres" }),
    )
    .await;
    assert_eq!(
        denied, "Count error: Permission denied: no access",
        "a permission error is never turned into a not-listed hint"
    );

    let column = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "users", "where": { "emial": "a" } }),
    )
    .await;
    assert_eq!(
        column,
        format!(
            "Column 'emial' is not listed among the columns of table 'users'.\n\
             Did you mean: email?\n\
             Available columns: id, email\n\n\
             Original error: {original}"
        )
    );
}

#[tokio::test]
async fn hint_only_includes_what_the_client_may_list() {
    let original = "Select error: engine says no";
    let unlisted_table = json!({ "table": "usres" });
    let misspelled_column = json!({ "table": "users", "where": { "emial": "a" } });

    let (read_only, connection_id) =
        start_hint_agent(READ_TOOLS_ONLY_ROLE, &["postgres", "analytics"]).await;

    for mut arguments in [unlisted_table.clone(), misspelled_column.clone()] {
        arguments["connection_id"] = json!(connection_id);

        let message = tool_error(&read_only, "select_data", arguments).await;

        assert_eq!(
            message, original,
            "a client that may not list tables or describe objects gets no names"
        );
    }

    let (with_tables, connection_id) =
        start_hint_agent(READ_AND_LIST_TABLES_ROLE, &["postgres", "analytics"]).await;

    let mut arguments = unlisted_table;
    arguments["connection_id"] = json!(connection_id);
    let message = tool_error(&with_tables, "select_data", arguments).await;
    assert_eq!(
        message,
        format!(
            "Table 'usres' is not listed in schema 'public'. {NOT_LISTED_REASON}\n\
             Did you mean: users?\n\n\
             Original error: {original}"
        ),
        "list_tables allows the table names, and nothing about databases"
    );

    let mut arguments = misspelled_column;
    arguments["connection_id"] = json!(connection_id);
    let message = tool_error(&with_tables, "select_data", arguments).await;
    assert_eq!(
        message, original,
        "column names need describe_object, which this client may not call"
    );
}

// ---------------------------------------------------------------------------
// select_data joins
// ---------------------------------------------------------------------------

/// Starts an allow-all agent on a SQLite file holding three tables:
/// `customers` (Ada, Bo and Cy, who has no order), `orders` (two for Ada, one
/// for Bo) and `payments` (one, for Ada's first order).
async fn start_join_agent() -> (Agent, String, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;

    let tables = [
        (
            "customers",
            json!([
                { "name": "id", "type": "integer", "primary_key": true },
                { "name": "name", "type": "text", "nullable": false }
            ]),
            json!([
                { "id": 1, "name": "Ada" },
                { "id": 2, "name": "Bo" },
                { "id": 3, "name": "Cy" }
            ]),
        ),
        (
            "orders",
            json!([
                { "name": "id", "type": "integer", "primary_key": true },
                { "name": "customer_id", "type": "integer", "nullable": false },
                { "name": "total", "type": "integer", "nullable": false }
            ]),
            json!([
                { "id": 10, "customer_id": 1, "total": 30 },
                { "id": 11, "customer_id": 1, "total": 5 },
                { "id": 12, "customer_id": 2, "total": 20 }
            ]),
        ),
        (
            "payments",
            json!([
                { "name": "id", "type": "integer", "primary_key": true },
                { "name": "order_id", "type": "integer", "nullable": false },
                { "name": "method", "type": "text", "nullable": false }
            ]),
            json!([{ "id": 100, "order_id": 10, "method": "card" }]),
        ),
    ];

    for (table, columns, records) in tables {
        agent
            .call_json(
                "create_table",
                json!({ "connection_id": connection_id, "table": table, "columns": columns }),
            )
            .await;
        agent
            .call_json(
                "insert_record",
                json!({ "connection_id": connection_id, "table": table, "records": records }),
            )
            .await;
    }

    (agent, connection_id, directory)
}

/// The `select_data` arguments for `customers` joined to `orders`.
fn customers_join_orders(connection_id: &str, join_type: &str) -> Value {
    json!({
        "connection_id": connection_id,
        "table": "customers",
        "columns": ["name", "orders.total"],
        "joins": [{
            "type": join_type,
            "table": "orders",
            "on": "customers.id = orders.customer_id"
        }],
        "order_by": [{ "column": "customers.id" }, { "column": "orders.id" }]
    })
}

#[tokio::test]
async fn inner_join_returns_matched_rows_under_the_names_written_in_columns() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            customers_join_orders(&connection_id, "inner"),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["name", "orders.total"],
            "rows": [
                { "name": "Ada", "orders.total": 30 },
                { "name": "Ada", "orders.total": 5 },
                { "name": "Bo", "orders.total": 20 }
            ],
            "row_count": 3
        })
    );
}

#[tokio::test]
async fn left_join_keeps_unmatched_rows_with_nulls() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json("select_data", customers_join_orders(&connection_id, "LEFT"))
        .await;

    assert_eq!(selected["row_count"], json!(4), "got {selected}");
    assert_eq!(
        selected["rows"][3],
        json!({ "name": "Cy", "orders.total": null })
    );
}

#[tokio::test]
async fn join_filters_sorts_and_limits_on_joined_columns() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "columns": ["customers.name", "o.total"],
                "joins": [{
                    "type": "inner",
                    "table": "orders",
                    "alias": "o",
                    "on": "customers.id = o.customer_id"
                }],
                "where": { "o.total": { "$gte": 20 }, "name": { "$in": ["Ada", "Bo"] } },
                "order_by": [{ "column": "o.total", "direction": "desc" }],
                "limit": 1,
                "offset": 1
            }),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["customers.name", "o.total"],
            "rows": [{ "customers.name": "Bo", "o.total": 20 }],
            "row_count": 1
        })
    );
}

#[tokio::test]
async fn two_joins_chain_through_an_earlier_join() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "columns": ["name", "orders.total", "payments.method"],
                "joins": [
                    {
                        "type": "inner",
                        "table": "orders",
                        "on": "customers.id = orders.customer_id"
                    },
                    {
                        "type": "inner",
                        "table": "payments",
                        "on": "payments.order_id = orders.id AND payments.id > orders.id"
                    }
                ]
            }),
        )
        .await;

    assert_eq!(
        selected["rows"],
        json!([{ "name": "Ada", "orders.total": 30, "payments.method": "card" }])
    );
}

#[tokio::test]
async fn join_without_columns_returns_every_column_and_numbers_repeated_names() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "joins": [{
                    "type": "inner",
                    "table": "orders",
                    "on": "customers.id = orders.customer_id"
                }],
                "where": { "orders.id": 12 }
            }),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["id", "name", "id_2", "customer_id", "total"],
            "rows": [{ "id": 2, "name": "Bo", "id_2": 12, "customer_id": 2, "total": 20 }],
            "row_count": 1
        })
    );
}

#[tokio::test]
async fn join_columns_field_selects_columns_of_the_joined_table() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "columns": ["name"],
                "joins": [{
                    "type": "inner",
                    "table": "orders",
                    "alias": "o",
                    "on": "customers.id = o.customer_id",
                    "columns": ["total"]
                }],
                "where": { "o.id": 12 }
            }),
        )
        .await;

    assert_eq!(selected["rows"], json!([{ "name": "Bo", "o.total": 20 }]));
}

#[tokio::test]
async fn hostile_join_input_is_rejected_and_leaves_the_tables_intact() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let join = |on: &str| json!({ "type": "inner", "table": "orders", "on": on });
    let base = |extra: Value| {
        let mut arguments = json!({ "connection_id": connection_id, "table": "customers" });
        arguments
            .as_object_mut()
            .expect("arguments are an object")
            .extend(extra.as_object().cloned().unwrap_or_default());
        arguments
    };
    let valid_on = "customers.id = orders.customer_id";

    let rejected = [
        (
            base(
                json!({ "joins": [join("customers.id = orders.customer_id; DROP TABLE orders")] }),
            ),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = orders.customer_id OR 1=1")] })),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = (SELECT 1)")] })),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = orders.customer_id -- x")] })),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = payments.order_id")] })),
            "not a table in this query",
        ),
        (
            base(json!({ "joins": [{
                "type": "inner",
                "table": "orders\" ON 1=1; DROP TABLE orders; --",
                "on": valid_on
            }] })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [{
                "type": "inner", "table": "orders", "alias": "o; DROP TABLE orders", "on": valid_on
            }] })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [{ "type": "cross", "table": "orders", "on": valid_on }] })),
            "join type",
        ),
        (
            base(json!({ "joins": [{
                "type": "inner", "table": "orders", "alias": "customers", "on": valid_on
            }] })),
            "already used",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "columns": ["name\" FROM customers; --"] })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "columns": ["ghost.name"] })),
            "not a table in this query",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "where": { "ghost.id": 1 } })),
            "not a table in this query",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "where": { "na?me": "x" } })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "where": { "name": { "$regex": "^A" } } })),
            "$regex",
        ),
        (
            base(json!({
                "joins": [join(valid_on)],
                "order_by": [{ "column": "name; DROP TABLE orders" }]
            })),
            "plain identifier",
        ),
    ];

    for (arguments, expected) in rejected {
        let error = tool_error(&agent, "select_data", arguments.clone()).await;

        assert!(
            error.contains(expected),
            "the rejection of {arguments} should mention '{expected}': {error}"
        );
    }

    // A value is data: it is rendered as a literal and matches nothing.
    let literal = agent
        .call_json(
            "select_data",
            base(json!({
                "joins": [join(valid_on)],
                "where": { "name": "x' OR '1'='1" }
            })),
        )
        .await;
    assert_eq!(literal["row_count"], json!(0), "got {literal}");

    for (table, expected) in [("customers", 3), ("orders", 3), ("payments", 1)] {
        let counted = agent
            .call_json(
                "count_records",
                json!({ "connection_id": connection_id, "table": table }),
            )
            .await;

        assert_eq!(counted["count"], json!(expected), "table {table}");
    }
}

#[tokio::test]
async fn select_without_joins_is_unchanged() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let arguments = json!({
        "connection_id": connection_id,
        "table": "orders",
        "columns": ["id", "total"],
        "where": { "customer_id": 1 },
        "order_by": [{ "column": "total" }]
    });
    let expected = json!({
        "columns": ["id", "total"],
        "rows": [{ "id": 11, "total": 5 }, { "id": 10, "total": 30 }],
        "row_count": 2
    });

    assert_eq!(
        agent.call_json("select_data", arguments.clone()).await,
        expected
    );

    let mut with_empty_joins = arguments;
    with_empty_joins["joins"] = json!([]);
    assert_eq!(
        agent.call_json("select_data", with_empty_joins).await,
        expected,
        "an empty joins array is the same as leaving it out"
    );
}

/// The `details_json` of the latest audited execution of `select_data`.
async fn latest_select_data_audit_details(agent: &Agent) -> Value {
    let runtime = agent.runtime.read().await;

    let events = runtime
        .audit_service()
        .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
        .expect("query the audit log");

    events
        .into_iter()
        .filter(|event| event.object_id.as_deref() == Some("select_data"))
        .filter(|event| {
            event
                .action
                .as_deref()
                .is_some_and(|action| action.ends_with("execute") && !action.ends_with("authorize"))
        })
        .max_by_key(|event| event.id)
        .and_then(|event| event.details_json)
        .and_then(|details| serde_json::from_str(&details).ok())
        .expect("select_data should have an audited execution with details")
}

#[tokio::test]
async fn join_call_records_the_generated_query_in_the_audit_log() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "customers" }),
        )
        .await;
    let plain = latest_select_data_audit_details(&agent).await;
    assert!(
        plain.get("query").is_none(),
        "a call without joins keeps its audit details: {plain}"
    );

    agent
        .call_json(
            "select_data",
            customers_join_orders(&connection_id, "inner"),
        )
        .await;
    let joined = latest_select_data_audit_details(&agent).await;

    let query = joined["query"]
        .as_str()
        .unwrap_or_else(|| panic!("a join call records its query: {joined}"));
    assert!(
        query.starts_with("[FINGERPRINT:"),
        "the audit sink fingerprints the query by default: {query}"
    );
    assert!(joined["query_length"].as_u64().unwrap_or(0) > 0, "{joined}");
}

#[tokio::test]
async fn joins_on_a_driver_without_join_support_get_an_explicit_error() {
    let join_arguments = |connection_id: &str| {
        json!({
            "connection_id": connection_id,
            "table": "users",
            "database": "postgres",
            "joins": [{ "type": "inner", "table": "orders", "on": "users.id = orders.user_id" }]
        })
    };

    // Declares join support but has no structured SELECT generator.
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres"]).await;
    let error = tool_error(&agent, "select_data", join_arguments(&connection_id)).await;
    assert!(
        error.contains("does not support joins in select_data"),
        "got: {error}"
    );

    // Declares no join support.
    for kind in [dbflux_core::DbKind::MongoDB, dbflux_core::DbKind::Redis] {
        let profile = ConnectionProfile::new("agent-fake", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let driver = dbflux_test_support::FakeDriver::new(kind).as_driver_arc();
        let agent = start_agent(ALLOW_ALL_ROLE, Some((driver, profile))).await;

        let error = tool_error(&agent, "select_data", join_arguments(&connection_id)).await;
        assert!(
            error.contains("does not support joins in select_data"),
            "driver {kind:?} got: {error}"
        );
    }
}

#[tokio::test]
async fn failed_join_query_gets_the_not_found_hint_for_the_main_table() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "custmers",
            "joins": [{
                "type": "inner",
                "table": "orders",
                "on": "custmers.id = orders.customer_id"
            }]
        }),
    )
    .await;

    assert!(
        message.starts_with(&format!(
            "Table 'custmers' is not listed in database 'main'. {NOT_LISTED_REASON}\n\
             Did you mean: customers?"
        )),
        "got: {message}"
    );
    assert!(message.contains("no such table"), "got: {message}");
}
