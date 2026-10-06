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

mod core_flows;
mod not_found_hints;
mod select_data_column_check;
mod select_data_joins;
mod select_data_pseudo_columns;
mod sqlite_name_resolution;

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
/// `id` and `email` and the pseudo-column `rowid`, and `orders` reports an
/// empty column list. The connection has no query generator.
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
            "users" => {
                let mut users = hint_table(table, Some(&["id", "email"]));
                users.pseudo_columns = Box::new(["rowid".to_string()]);
                Ok(users)
            }
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

const NOT_RUN: &str = "The query was not run.";

/// Creates `notes`, a rowid table with no `INTEGER PRIMARY KEY` (so `rowid` is
/// not an alias of a listed column), and `codes`, a `WITHOUT ROWID` table, in
/// the profile's file before the agent connects.
fn create_rowid_tables(directory: &tempfile::TempDir) {
    let connection = rusqlite::Connection::open(directory.path().join("agent.sqlite"))
        .expect("open the test database");

    connection
        .execute_batch(
            "CREATE TABLE notes (label TEXT NOT NULL);
             INSERT INTO notes (label) VALUES ('alpha'), ('beta'), ('gamma');
             CREATE TABLE codes (code TEXT PRIMARY KEY, label TEXT NOT NULL) WITHOUT ROWID;
             INSERT INTO codes (code, label) VALUES ('a', 'alpha'), ('b', 'beta');",
        )
        .expect("create the rowid test tables");
}
