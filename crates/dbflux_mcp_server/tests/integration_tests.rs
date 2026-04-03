//! Integration coverage for stable MCP contracts that can be exercised without
//! Docker-backed databases.

use dbflux_core::{NoopSecretStore, SecretManager};
use dbflux_mcp::{
    ConnectionPolicyAssignmentDto, McpRuntime, TrustedClientDto, builtin_policies, builtin_roles,
};
use dbflux_mcp_server::{
    connection_cache::ConnectionCache,
    governance::GovernanceMiddleware,
    state::ServerState,
    tools::{SelectDataParams, query::PreviewMutationParams},
};
use dbflux_policy::{ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope};
use rmcp::{model::CallToolResult, schemars::schema_for};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Test setup helpers
// ---------------------------------------------------------------------------

fn build_runtime_with_role(connection_id: &str, role_id: &str) -> McpRuntime {
    let audit_path = dbflux_audit::temp_sqlite_path(&format!(
        "integration_test_{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos()
    ));
    let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
        .expect("failed to create test audit service");
    let mut runtime = McpRuntime::new(audit_service);

    for role in builtin_roles() {
        let _ = runtime.upsert_role_mut(role);
    }

    for policy in builtin_policies() {
        let _ = runtime.upsert_policy_mut(policy);
    }

    let _ = runtime.upsert_trusted_client_mut(TrustedClientDto {
        id: "test-client".to_string(),
        name: "Test Client".to_string(),
        issuer: None,
        active: true,
    });

    let _ = runtime.save_connection_policy_assignment_mut(ConnectionPolicyAssignmentDto {
        connection_id: connection_id.to_string(),
        assignments: vec![ConnectionPolicyAssignment {
            actor_id: "test-client".to_string(),
            scope: PolicyBindingScope {
                connection_id: connection_id.to_string(),
            },
            role_ids: vec![role_id.to_string()],
            policy_ids: vec![],
        }],
    });

    runtime.drain_events();
    runtime
}

fn build_state_with_role(connection_id: &str, role_id: &str) -> ServerState {
    let mut profile_manager = dbflux_core::ProfileManager::new_in_memory();
    let mut profile = dbflux_core::ConnectionProfile::new(
        "governed-test",
        dbflux_core::DbConfig::default_postgres(),
    );
    profile.id = connection_id
        .parse()
        .expect("test connection id should be a valid uuid");
    profile_manager.add(profile);

    ServerState {
        client_id: "test-client".to_string(),
        runtime: Arc::new(RwLock::new(build_runtime_with_role(connection_id, role_id))),
        profile_manager: Arc::new(RwLock::new(profile_manager)),
        auth_profile_manager: Arc::new(RwLock::new(dbflux_core::AuthProfileManager::default())),
        driver_registry: Arc::new(HashMap::new()),
        auth_provider_registry: Arc::new(HashMap::new()),
        driver_settings: Arc::new(HashMap::new()),
        connection_cache: Arc::new(RwLock::new(ConnectionCache::new())),
        connection_setup_lock: Arc::new(tokio::sync::Mutex::new(())),
        secret_manager: Arc::new(SecretManager::new(Box::new(NoopSecretStore))),
        mcp_enabled_by_default: true,
    }
}

fn property_schema<'a>(schema: &'a Value, field: &str) -> &'a Value {
    &schema["properties"][field]
}

/// Creates a test ServerState with a trusted client and admin role assignment.
///
/// This is a stub - in a full implementation, this would:
/// - Create temporary config directory
/// - Initialize config with trusted client
/// - Grant admin role to test client
#[allow(dead_code)]
async fn setup_test_server() -> ServerState {
    // TODO: Implement full setup
    // For now, this serves as a placeholder showing the intended structure
    unimplemented!("setup_test_server requires full config initialization")
}

// ---------------------------------------------------------------------------
// 1. Connection Tools Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_connection_tools() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server with trusted client
    // 2. Create PostgreSQL profile and add to server
    // 3. Test list_connections - verify profile appears
    // 4. Test connect - establish connection
    // 5. Test get_connection_info - verify connection metadata
    // 6. Test disconnect - remove from cache

    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Schema Tools Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_schema_tools() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server and connect to PostgreSQL
    // 2. Create test table with columns
    // 3. Test list_databases - verify postgres database exists
    // 4. Test list_tables - verify test table appears
    // 5. Test list_collections (alias) - same as list_tables
    // 6. Test describe_object - verify column metadata

    Ok(())
}

// ---------------------------------------------------------------------------
// 3. CRUD Tools Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_crud_insert_and_select() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server and connect to PostgreSQL
    // 2. Create test table
    // 3. Test insert_record - insert test data
    // 4. Test select_data with filters, ORDER BY, LIMIT
    // 5. Verify results match expected data

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_crud_count() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server and insert test data
    // 2. Test count_records without filters
    // 3. Test count_records with filters
    // 4. Verify counts match expectations

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_crud_update() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server and insert test data
    // 2. Test update_records with WHERE clause
    // 3. Verify update applied correctly
    // 4. Test that update without WHERE clause is rejected

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_crud_upsert() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server with table having unique constraint
    // 2. Test upsert_record - insert new record
    // 3. Test upsert_record again - update existing
    // 4. Verify only one record exists with updated value

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_crud_delete_with_where_clause() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server and insert test data
    // 2. Test delete_records with WHERE clause
    // 3. Verify deletion
    // 4. Test that delete without WHERE clause is rejected (safety check)

    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Script Tools Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_script_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Test create_script - create SQL script
    // 2. Test list_scripts - verify it appears
    // 3. Test get_script - read content
    // 4. Test update_script - modify content
    // 5. Test execute_script - run against connection
    // 6. Test delete_script - remove (with confirmation)

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_script_execution_with_connection() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server and create script
    // 2. Connect to PostgreSQL
    // 3. Execute script against connection
    // 4. Verify query results

    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Approval Flow Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_approval_request_and_approve() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server
    // 2. Test request_execution - create pending destructive operation
    // 3. Test list_pending_executions - verify appears
    // 4. Test get_pending_execution - get details
    // 5. Test approve_execution - approve and get replay plan
    // 6. Verify replay plan can be executed

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_approval_reject() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Setup test server
    // 2. Create pending execution
    // 3. Test reject_execution with reason
    // 4. Verify removed from pending list

    Ok(())
}

// ---------------------------------------------------------------------------
// 6. Audit Tools Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_audit_query_by_actor() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Create audit service with temp database
    // 2. Log test events with different actors
    // 3. Test query_audit_logs filtered by actor
    // 4. Verify results contain only matching actor

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_audit_query_by_tool() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Log events with different tools
    // 2. Test query_audit_logs filtered by tool_id
    // 3. Verify results contain only matching tool

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_audit_query_by_date_range() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Log events at different times
    // 2. Test query_audit_logs filtered by date range
    // 3. Verify only events in range are returned

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_audit_get_entry() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Log test event
    // 2. Test get_audit_entry with specific ID
    // 3. Verify correct event returned

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_audit_export_csv() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Log test events
    // 2. Test export_audit_logs as CSV
    // 3. Verify CSV contains expected data

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_audit_export_json() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Log test events
    // 2. Test export_audit_logs as JSON
    // 3. Parse and verify JSON structure

    Ok(())
}

// ---------------------------------------------------------------------------
// 7. Governance Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_governance_readonly_role_restrictions() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Create server state with readonly-client
    // 2. Grant builtin/read-only role
    // 3. Verify client CANNOT call delete_records (should deny)
    // 4. Verify client CAN call select_data (should allow)

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_governance_write_role_restrictions() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Create server state with write-client
    // 2. Grant builtin/write role
    // 3. Verify client CAN call delete_records (should allow)
    // 4. Verify client CANNOT call drop_table (should deny)

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_governance_admin_role_full_access() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Create server state with admin-client
    // 2. Grant builtin/admin role
    // 3. Verify client CAN call select_data (read)
    // 4. Verify client CAN call delete_records (destructive)
    // 5. Verify client CAN call drop_table (admin)

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker daemon and full implementation"]
async fn test_governance_connection_specific_policies() -> Result<(), Box<dyn std::error::Error>> {
    // Test Structure:
    // 1. Create two connections with different policies
    // 2. Verify client has admin on connection A
    // 3. Verify client has readonly on connection B
    // 4. Test that permissions are enforced per-connection

    Ok(())
}

// ---------------------------------------------------------------------------
// Integration Test Examples
// ---------------------------------------------------------------------------

/// Example showing how to use dbflux_test_support containers when implemented
#[allow(dead_code)]
fn example_postgres_test_pattern() {
    // This is an example pattern - not a real test
    //
    // use dbflux_test_support::containers;
    //
    // containers::with_postgres_url(|uri| {
    //     tokio::runtime::Runtime::new().unwrap().block_on(async {
    //         let state = setup_test_server().await;
    //         // ... test logic here
    //         Ok::<(), Box<dyn std::error::Error>>(())
    //     })
    // })
}

/// Example showing connection retry pattern
#[allow(dead_code)]
fn example_connection_retry_pattern() {
    // This is an example pattern - not a real test
    //
    // use dbflux_test_support::containers;
    //
    // let connection = containers::retry_db_operation(
    //     Duration::from_secs(30),
    //     || {
    //         let conn = driver.connect(profile)?;
    //         conn.ping()?;
    //         Ok(conn)
    //     }
    // )?;
}

#[test]
fn test_placeholder() {
    // Placeholder test to ensure the file compiles
    assert!(true);
}

#[test]
fn select_data_schema_keeps_shared_filter_and_pagination_contract() {
    let schema =
        serde_json::to_value(schema_for!(SelectDataParams)).expect("schema should serialize");

    assert_eq!(
        property_schema(&schema, "where")["description"],
        "Filter conditions as JSON object"
    );
    assert_eq!(
        property_schema(&schema, "limit")["description"],
        "Maximum rows to return (default: 100, max: 10000)"
    );
    assert_eq!(
        property_schema(&schema, "offset")["description"],
        "Number of rows to skip"
    );
    assert_eq!(
        property_schema(&schema, "database")["description"],
        "Optional database/schema name"
    );

    let required = schema["required"]
        .as_array()
        .expect("required fields should be an array");
    assert!(required.contains(&Value::String("connection_id".to_string())));
    assert!(required.contains(&Value::String("table".to_string())));
    assert!(!required.contains(&Value::String("where".to_string())));
}

#[test]
fn preview_mutation_schema_keeps_mutation_preview_contract() {
    let schema =
        serde_json::to_value(schema_for!(PreviewMutationParams)).expect("schema should serialize");

    assert_eq!(
        property_schema(&schema, "connection_id")["description"],
        "Connection ID"
    );
    assert_eq!(
        property_schema(&schema, "sql")["description"],
        "SQL mutation query to preview (INSERT, UPDATE, DELETE, etc.)"
    );
    assert_eq!(
        property_schema(&schema, "database")["description"],
        "Optional database/schema name"
    );

    let required = schema["required"]
        .as_array()
        .expect("required fields should be an array");
    assert!(required.contains(&Value::String("connection_id".to_string())));
    assert!(required.contains(&Value::String("sql".to_string())));
    assert!(!required.contains(&Value::String("database".to_string())));
}

#[tokio::test]
async fn preview_mutation_remains_read_governed_for_readonly_clients() {
    let connection_id = uuid::Uuid::new_v4().to_string();
    let middleware =
        GovernanceMiddleware::new(build_state_with_role(&connection_id, "builtin/read-only"));

    let preview = middleware
        .authorize_and_execute(
            "preview_mutation",
            Some(&connection_id),
            ExecutionClassification::Read,
            || async { Ok(CallToolResult::success(vec![])) },
        )
        .await;

    assert!(
        preview.is_ok(),
        "preview_mutation should stay readable for readonly clients"
    );

    let delete = middleware
        .authorize_and_execute(
            "delete_records",
            Some(&connection_id),
            ExecutionClassification::Destructive,
            || async { Ok(CallToolResult::success(vec![])) },
        )
        .await;

    assert!(
        delete.is_err(),
        "readonly clients must still be denied destructive tools"
    );
}

#[tokio::test]
async fn mcp_execution_writes_correlated_audit_events() {
    use dbflux_core::observability::{
        EventCategory,
        actions::{MCP_AUTHORIZE, QUERY_EXECUTE},
    };

    let connection_id = uuid::Uuid::new_v4().to_string();
    let state = build_state_with_role(&connection_id, "builtin/read-only");
    let middleware = GovernanceMiddleware::new(state.clone());

    let result = middleware
        .authorize_and_execute(
            "select_data",
            Some(&connection_id),
            ExecutionClassification::Read,
            || async { Ok(CallToolResult::success(vec![])) },
        )
        .await;

    assert!(
        result.is_ok(),
        "authorized tool should execute successfully"
    );

    let runtime = state.runtime.read().await;
    let audit_service = runtime.audit_service();

    let all_mcp_events = audit_service
        .query_extended(&dbflux_audit::query::AuditQueryFilter {
            category: Some(EventCategory::Mcp.as_str().to_string()),
            ..Default::default()
        })
        .expect("audit query should succeed");

    assert!(
        !all_mcp_events.is_empty(),
        "at least one MCP event should be recorded"
    );

    let mut correlation_groups: std::collections::HashMap<Option<String>, Vec<_>> =
        std::collections::HashMap::new();
    for event in &all_mcp_events {
        correlation_groups
            .entry(event.correlation_id.clone())
            .or_default()
            .push(event);
    }

    let mut max_group_size = 0;
    for (_, group) in &correlation_groups {
        if group.len() > max_group_size {
            max_group_size = group.len();
        }
    }

    assert_eq!(
        max_group_size, 2,
        "expected exactly 2 correlated events (mcp_authorize + query_execute), got {}",
        max_group_size
    );

    let correlated_events: Vec<_> = correlation_groups
        .into_iter()
        .filter(|(_, events)| events.len() == 2)
        .flat_map(|(_, events)| events)
        .collect();

    let actions: Vec<&str> = correlated_events
        .iter()
        .filter_map(|e| e.action.as_deref())
        .collect();
    assert!(
        actions.contains(&MCP_AUTHORIZE.as_str()),
        "correlated events should include mcp_authorize"
    );
    assert!(
        actions.contains(&QUERY_EXECUTE.as_str()),
        "correlated events should include query_execute for select_data tool"
    );
}
