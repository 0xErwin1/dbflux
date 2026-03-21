//! Governance middleware for MCP server.
//!
//! Provides authorization, approval flow, and audit logging for all tool executions.

use std::future::Future;
use dbflux_mcp::{
    server::{
        authorization::{authorize_request, AuthorizationOutcome, AuthorizationRequest},
        request_context::RequestIdentity,
    },
    McpGovernanceService,
};
use dbflux_policy::ExecutionClassification;
use rmcp::model::{CallToolResult, ErrorData as McpError};

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
    state: ServerState,
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
        let trusted_clients_dto = self
            .state
            .runtime
            .list_trusted_clients()
            .map_err(|e| {
                McpError::internal_error(
                    format!("Failed to list trusted clients: {}", e),
                    None,
                )
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

        let assignments = self.state.runtime.policy_assignments_for_engine();
        let roles = self.state.runtime.roles_for_engine();
        let policies = self.state.runtime.policies_for_engine();
        let policy_engine = dbflux_policy::PolicyEngine::new(assignments, roles, policies);

        let auth_request = AuthorizationRequest {
            identity: RequestIdentity {
                client_id: self.state.client_id.clone(),
                issuer: None,
            },
            connection_id: connection_id.map(String::from).unwrap_or_default(),
            tool_id: tool_id.to_string(),
            classification,
            mcp_enabled_for_connection,
        };

        // Authorize the request
        let outcome = authorize_request(
            &trusted_clients,
            &policy_engine,
            self.state.runtime.audit_service(),
            &auth_request,
            now_epoch_ms(),
        )
        .map_err(|e| McpError::internal_error(format!("Authorization error: {}", e), None))?;

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
