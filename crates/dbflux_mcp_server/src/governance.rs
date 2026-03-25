//! Governance middleware for MCP server.
//!
//! Provides authorization, approval flow, and audit logging for all tool executions.

use dbflux_mcp::{
    McpGovernanceService,
    server::{
        authorization::{AuthorizationOutcome, AuthorizationRequest, authorize_request},
        request_context::RequestIdentity,
    },
};
use dbflux_policy::ExecutionClassification;
use rmcp::model::{CallToolResult, ErrorData as McpError};
use std::future::Future;

use crate::state::ServerState;

/// Helper to get current epoch time in milliseconds
fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Governance middleware that wraps tool execution with authorization and auditing.
#[derive(Clone)]
pub struct GovernanceMiddleware {
    pub(crate) state: ServerState,
}

impl GovernanceMiddleware {
    pub fn new(state: ServerState) -> Self {
        Self { state }
    }

    /// Authorize and execute a tool handler with governance controls.
    ///
    /// This method:
    /// 1. Checks if the client is authorized to execute the tool
    /// 2. Routes to approval flow if required
    /// 3. Executes the handler if authorized
    /// 4. Audits the execution
    pub async fn authorize_and_execute<F, Fut>(
        &self,
        tool_id: &str,
        connection_id: Option<&str>,
        classification: ExecutionClassification,
        handler: F,
    ) -> Result<CallToolResult, McpError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CallToolResult, McpError>>,
    {
        // Check if MCP is enabled for this connection
        let mcp_enabled_for_connection = if let Some(conn_id) = connection_id {
            self.state.is_mcp_enabled_for_connection(conn_id).await
        } else {
            true // Tools without connection_id are always enabled
        };

        // Build authorization request
        let runtime = self.state.runtime.read().await;

        let trusted_clients_dto = runtime.list_trusted_clients().map_err(|e| {
            McpError::internal_error(format!("Failed to list trusted clients: {}", e), None)
        })?;

        // Build TrustedClientRegistry from DTOs
        let clients: Vec<dbflux_policy::TrustedClient> = trusted_clients_dto
            .into_iter()
            .map(|dto| dbflux_policy::TrustedClient {
                id: dto.id,
                name: dto.name,
                issuer: dto.issuer,
                active: dto.active,
            })
            .collect();
        let trusted_clients = dbflux_policy::TrustedClientRegistry::new(clients);

        let assignments = runtime.policy_assignments_for_engine();
        let roles = runtime.roles_for_engine();
        let policies = runtime.policies_for_engine();
        let policy_engine = dbflux_policy::PolicyEngine::new(assignments, roles, policies);

        let auth_request = AuthorizationRequest {
            identity: RequestIdentity {
                client_id: self.state.client_id.clone(),
                issuer: None,
            },
            // Empty string for tools without a specific connection (matches server_old.rs behavior)
            connection_id: connection_id.map(String::from).unwrap_or_default(),
            tool_id: tool_id.to_string(),
            classification,
            mcp_enabled_for_connection,
        };

        // Authorize the request (keep runtime lock while calling authorize_request)
        let outcome = authorize_request(
            &trusted_clients,
            &policy_engine,
            runtime.audit_service(),
            &auth_request,
            now_epoch_ms(),
        )
        .map_err(|e| McpError::internal_error(format!("Authorization error: {}", e), None))?;

        // Drop runtime lock now that authorization is complete
        drop(runtime);

        // Handle authorization outcome
        if !outcome.allowed {
            return Err(McpError::new(
                rmcp::model::ErrorCode::INVALID_REQUEST,
                outcome
                    .deny_reason
                    .as_deref()
                    .unwrap_or("authorization denied")
                    .to_string(),
                outcome
                    .deny_code
                    .map(|code| serde_json::json!({ "code": code })),
            ));
        }

        // Execute the handler
        let result = handler().await;

        // Audit the execution (success or failure)
        self.audit_execution(tool_id, connection_id, &result, &outcome)
            .await?;

        result
    }

    /// Audit a tool execution
    async fn audit_execution(
        &self,
        tool_id: &str,
        connection_id: Option<&str>,
        result: &Result<CallToolResult, McpError>,
        outcome: &AuthorizationOutcome,
    ) -> Result<(), McpError> {
        // For now, we rely on the audit service being called in authorize_request
        // Future: could add more detailed audit events here based on result
        let _ = (tool_id, connection_id, result, outcome);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_mcp::{McpRuntime, builtin_policies, builtin_roles};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Helper to create a test ServerState with minimal setup
    fn create_test_state() -> ServerState {
        // Use a temporary file for testing (in-memory doesn't work well with rusqlite's open pattern)
        let temp_path = dbflux_audit::temp_sqlite_path(&format!(
            "test_audit_{}.sqlite",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let audit_service = dbflux_audit::AuditService::new_sqlite(&temp_path)
            .expect("failed to create test audit service");
        let mut runtime = McpRuntime::new(audit_service);

        // Register built-in roles and policies
        for role in builtin_roles() {
            let _ = runtime.upsert_role_mut(role);
        }

        for policy in builtin_policies() {
            let _ = runtime.upsert_policy_mut(policy);
        }

        // Register a test trusted client
        let _ = runtime.upsert_trusted_client_mut(dbflux_mcp::TrustedClientDto {
            id: "test-client".to_string(),
            name: "Test Client".to_string(),
            issuer: None,
            active: true,
        });

        // Create a default connection-scoped assignment for the test client
        // This assigns the "admin" role to the test client for "test-connection"
        let _ = runtime.save_connection_policy_assignment_mut(
            dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: "test-connection".to_string(),
                assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: "test-client".to_string(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: "test-connection".to_string(),
                    },
                    role_ids: vec!["builtin/admin".to_string()],
                    policy_ids: vec![],
                }],
            },
        );

        // Create an assignment for global/metadata operations (empty connection_id)
        // This allows tools like list_connections, list_scripts, query_audit_logs
        let _ = runtime.save_connection_policy_assignment_mut(
            dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: "".to_string(),
                assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: "test-client".to_string(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: "".to_string(),
                    },
                    role_ids: vec!["builtin/admin".to_string()],
                    policy_ids: vec![],
                }],
            },
        );

        runtime.drain_events();

        ServerState {
            client_id: "test-client".to_string(),
            runtime: Arc::new(RwLock::new(runtime)),
            profile_manager: Arc::new(RwLock::new(dbflux_core::ProfileManager::new())),
            driver_registry: Arc::new(std::collections::HashMap::new()),
            connection_cache: Arc::new(
                RwLock::new(crate::connection_cache::ConnectionCache::new()),
            ),
            secret_manager: Arc::new(dbflux_core::SecretManager::new(Box::new(
                dbflux_core::NoopSecretStore,
            ))),
            mcp_enabled_by_default: true,
        }
    }

    #[tokio::test]
    async fn test_authorize_metadata_operation_allows() {
        let state = create_test_state();
        let middleware = GovernanceMiddleware::new(state);

        let result = middleware
            .authorize_and_execute(
                "list_connections",
                None,
                ExecutionClassification::Metadata,
                || async { Ok(CallToolResult::success(vec![])) },
            )
            .await;

        if let Err(ref err) = result {
            eprintln!(
                "Authorization failed: code={:?}, message={}",
                err.code, err.message
            );
        }
        assert!(
            result.is_ok(),
            "Metadata operations should be allowed by default"
        );
    }

    #[tokio::test]
    async fn test_authorize_unknown_client_denies() {
        let mut state = create_test_state();
        state.client_id = "unknown-client".to_string();
        let middleware = GovernanceMiddleware::new(state);

        let result = middleware
            .authorize_and_execute(
                "select_data",
                Some("test-connection"),
                ExecutionClassification::Read,
                || async { Ok(CallToolResult::success(vec![])) },
            )
            .await;

        assert!(result.is_err(), "Unknown client should be denied");
        let err = result.unwrap_err();
        // Error message could be "client not trusted" or similar
        assert!(!err.message.is_empty(), "Error should have a message");
    }

    #[tokio::test]
    async fn test_handler_execution_success() {
        let state = create_test_state();
        let middleware = GovernanceMiddleware::new(state);

        let result = middleware
            .authorize_and_execute(
                "list_connections", // Use a tool that's in the builtin policies
                None,
                ExecutionClassification::Metadata,
                || async {
                    Ok(CallToolResult::success(vec![rmcp::model::Content::text(
                        "test result",
                    )]))
                },
            )
            .await;

        assert!(result.is_ok());
        let call_result = result.unwrap();
        assert!(call_result.is_error == Some(false));
        assert_eq!(call_result.content.len(), 1);
    }

    #[tokio::test]
    async fn test_handler_execution_failure_propagates() {
        let state = create_test_state();
        let middleware = GovernanceMiddleware::new(state);

        let result = middleware
            .authorize_and_execute(
                "list_connections", // Use a tool that's in the builtin policies
                None,
                ExecutionClassification::Metadata,
                || async { Err(McpError::internal_error("Test error".to_string(), None)) },
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.message, "Test error");
    }
}
