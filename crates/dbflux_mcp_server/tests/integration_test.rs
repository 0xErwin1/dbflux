//! Integration tests for DBFlux MCP server using rmcp SDK.
//!
//! These tests verify that the server correctly handles MCP protocol requests
//! and returns properly formatted responses.

use dbflux_mcp_server::McpServerArgs;
use tokio::io::DuplexStream;

/// Helper to create a test server with in-memory transport
#[allow(dead_code)]
async fn create_test_server() -> (Box<dyn std::any::Any>, DuplexStream) {
    // Create bidirectional in-memory stream
    let (_client_stream, _server_stream) = tokio::io::duplex(8192);

    // Create server state
    let _args = McpServerArgs {
        client_id: "test-client".to_string(),
        config_dir: None,
    };

    // Initialize server (this would normally be done in run_mcp_server)
    // For testing, we need to create a minimal setup
    // TODO: This requires refactoring run_mcp_server to return the service
    //       for testing purposes

    todo!("Requires ServerState::new() to be accessible and DbFluxServer to be constructible")
}

#[tokio::test]
#[ignore] // Ignored until test infrastructure is set up
async fn test_server_initialization() {
    // This test verifies that the server can be initialized and responds to initialize request

    let (_server, _client_stream) = create_test_server().await;

    // Create a mock MCP client
    // Send initialize request
    // Verify response contains correct capabilities
}

#[tokio::test]
#[ignore]
async fn test_list_tools() {
    // This test verifies that list_tools returns all registered tools

    // Expected tools: list_connections, connect, disconnect, get_connection_info,
    // execute_query, explain_query, preview_mutation, list_databases, list_schemas,
    // list_tables, describe_object, list_scripts, get_script, create_script,
    // update_script, delete_script, execute_script, request_execution,
    // list_pending_executions, get_pending_execution, approve_execution,
    // reject_execution, query_audit_logs, get_audit_entry, export_audit_logs
}

#[tokio::test]
#[ignore]
async fn test_list_connections_tool() {
    // This test verifies that the list_connections tool works correctly

    // Send call_tool request for list_connections
    // Verify response format
    // Check that connections list is returned
}

#[tokio::test]
#[ignore]
async fn test_execute_query_requires_connection() {
    // This test verifies that execute_query fails without connection_id

    // Send call_tool request for execute_query without connection_id
    // Verify error response
}

#[tokio::test]
#[ignore]
async fn test_governance_authorization() {
    // This test verifies that governance middleware enforces policies

    // Setup: Create server with restricted policies
    // Send call_tool request that should be denied
    // Verify authorization denied error
}

/// Helper to create a minimal ServerState for testing
#[allow(dead_code)]
fn create_test_server_state() -> dbflux_mcp_server::state::ServerState {
    use dbflux_mcp::{McpRuntime, TrustedClientDto, builtin_policies, builtin_roles};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let temp_dir = tempfile::tempdir().unwrap();
    let audit_path = temp_dir.path().join("test_audit.sqlite");
    let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path).unwrap();
    let mut runtime = McpRuntime::new(audit_service);

    // Register built-in roles and policies
    for role in builtin_roles() {
        let _ = runtime.upsert_role_mut(role);
    }

    for policy in builtin_policies() {
        let _ = runtime.upsert_policy_mut(policy);
    }

    // Register test trusted client
    let _ = runtime.upsert_trusted_client_mut(TrustedClientDto {
        id: "test-client".to_string(),
        name: "Test Client".to_string(),
        issuer: None,
        active: true,
    });

    runtime.drain_events();

    dbflux_mcp_server::state::ServerState {
        client_id: "test-client".to_string(),
        runtime: Arc::new(RwLock::new(runtime)),
        profile_manager: Arc::new(RwLock::new(dbflux_core::ProfileManager::new())),
        driver_registry: Arc::new(std::collections::HashMap::new()),
        connection_cache: Arc::new(RwLock::new(
            dbflux_mcp_server::connection_cache::ConnectionCache::new(),
        )),
        secret_manager: Arc::new(dbflux_core::SecretManager::new(Box::new(
            dbflux_core::NoopSecretStore,
        ))),
        mcp_enabled_by_default: true,
    }
}

// Note: These tests are currently ignored because they require:
// 1. Exposing ServerState::new() as public API
// 2. Exposing DbFluxServer::new() as public API
// 3. Refactoring run_mcp_server() to return the service for testing
//
// Alternative approach: Use end-to-end testing with actual stdio transport
// and a mock MCP client that sends JSON-RPC messages.
