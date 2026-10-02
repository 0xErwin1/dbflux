//! Governance middleware for MCP server.
//!
//! Provides authorization, approval flow, and audit logging for all tool executions.

use dbflux_core::LogErr;
use dbflux_core::observability::{
    AuditContext, EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity, actions,
    new_correlation_id,
};
use dbflux_mcp::{
    McpGovernanceService,
    server::{
        authorization::{APPROVAL_REQUIRED_CODE, AuthorizationOutcome, AuthorizationRequest},
        request_context::RequestIdentity,
    },
};
use dbflux_policy::ExecutionClassification;
use rmcp::model::ContentBlock;
use rmcp::model::{CallToolResult, ErrorData as McpError};
use std::future::Future;

use crate::state::ServerState;

/// Optional audit details that a tool handler can return alongside its result.
///
/// When `query` is `Some`, the SQL string is merged into the success `details_json`
/// under the `"query"` key. The audit sink then applies fingerprinting or raw
/// capture depending on the `AuditService::capture_query_text` setting.
/// `None` leaves `details_json` unchanged (only `content_count` is recorded).
#[derive(Default)]
pub struct AuditDetails {
    pub query: Option<String>,
}

tokio::task_local! {
    /// Arguments of the MCP tool call being served, set by
    /// `DbFluxServer::call_tool` for the duration of the call. A call that a
    /// policy sends to approval is queued and matched against approvals by
    /// these arguments.
    pub(crate) static TOOL_CALL_ARGUMENTS: serde_json::Value;
}

/// The arguments of the tool call being served, or an empty object when the
/// middleware runs outside `call_tool` (direct calls in tests).
fn current_tool_call_arguments() -> serde_json::Value {
    TOOL_CALL_ARGUMENTS
        .try_with(Clone::clone)
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()))
}

/// Helper to get current epoch time in milliseconds
fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Which schema metadata the current client may receive inside the error of
/// another tool, on one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HintPermissions {
    /// Table, view and collection names, as `list_tables` returns them.
    pub table_names: bool,

    /// Column names, as `describe_object` returns them.
    pub column_names: bool,

    /// Database names, as `list_databases` returns them.
    pub databases: bool,
}

/// Asks the policy engine whether the current client could run the metadata
/// tools on `connection_id` right now, without running them.
///
/// A tool counts only when the decision is a plain allow: a denied tool and one
/// that needs approval are both `false`, as is a policy that fails to
/// evaluate. Nothing is audited and nothing is queued, because no call is
/// made. The trusted-client and MCP-enabled checks are not repeated: the
/// caller is already inside an authorized call by the same client on the same
/// connection.
pub(crate) async fn hint_permissions(state: &ServerState, connection_id: &str) -> HintPermissions {
    let runtime = state.runtime.read().await;

    let policy_engine = dbflux_policy::PolicyEngine::new(
        runtime.policy_assignments_for_engine(),
        runtime.roles_for_engine(),
        runtime.policies_for_engine(),
    );

    drop(runtime);

    let allowed = |tool_id: &str| {
        let decision = policy_engine
            .evaluate(&dbflux_policy::PolicyEvaluationRequest {
                actor_id: state.client_id.clone(),
                connection_id: connection_id.to_string(),
                tool_id: tool_id.to_string(),
                classification: ExecutionClassification::Metadata,
            })
            .log_err_with("Failed to evaluate a metadata tool for a not-found hint");

        matches!(decision, Some(dbflux_policy::PolicyDecision::Allow))
    };

    HintPermissions {
        table_names: allowed("list_tables"),
        column_names: allowed("describe_object"),
        databases: allowed("list_databases"),
    }
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

    /// Authorize and execute a tool handler, capturing optional SQL for audit.
    ///
    /// The handler returns `(CallToolResult, AuditDetails)`. If `AuditDetails.query`
    /// is `Some`, it is merged into the success `details_json` under the `"query"` key.
    /// The audit sink applies fingerprinting or raw capture per its configuration.
    pub async fn authorize_and_execute_audited<F, Fut>(
        &self,
        tool_id: &str,
        connection_id: Option<&str>,
        classification: ExecutionClassification,
        handler: F,
    ) -> Result<CallToolResult, McpError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(CallToolResult, AuditDetails), McpError>>,
    {
        let mcp_enabled_for_connection = if let Some(conn_id) = connection_id {
            self.state.is_mcp_enabled_for_connection(conn_id).await
        } else {
            true
        };

        let mut runtime = self.state.runtime.write().await;

        let trusted_clients_dto = runtime.list_trusted_clients().map_err(|e| {
            McpError::internal_error(format!("Failed to list trusted clients: {}", e), None)
        })?;

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

        let correlation_id = new_correlation_id();

        let auth_request = AuthorizationRequest {
            identity: RequestIdentity {
                client_id: self.state.client_id.clone(),
                issuer: None,
            },
            connection_id: connection_id.map(String::from).unwrap_or_default(),
            tool_id: tool_id.to_string(),
            classification,
            mcp_enabled_for_connection,
            correlation_id: Some(correlation_id.clone()),
        };

        let outcome = runtime
            .authorize_with_approval_mut(
                &trusted_clients,
                &policy_engine,
                &auth_request,
                current_tool_call_arguments(),
                now_epoch_ms(),
            )
            .map_err(|e| McpError::internal_error(format!("Authorization error: {}", e), None))?;

        drop(runtime);

        if !outcome.allowed {
            return Err(authorization_error(&outcome, tool_id, classification));
        }

        let handler_result = handler().await;

        let (result, audit_details) = match handler_result {
            Ok((result, details)) => (Ok(result), details),
            Err(e) => (Err(e), AuditDetails::default()),
        };

        self.audit_execution_with_details(
            tool_id,
            connection_id,
            &result,
            &outcome,
            &audit_details,
        )
        .await?;

        result
    }

    /// Authorize and execute a tool handler with governance controls.
    ///
    /// This method:
    /// 1. Generates one `correlation_id` shared between authorization and execution events
    /// 2. Checks if the client is authorized to execute the tool
    /// 3. Routes to approval flow if required
    /// 4. Executes the handler if authorized
    /// 5. Audits the execution with the shared correlation_id
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
        self.authorize_and_execute_audited(tool_id, connection_id, classification, || async {
            handler().await.map(|r| (r, AuditDetails::default()))
        })
        .await
    }

    /// Audit a tool execution after authorization succeeds.
    ///
    /// Emits exactly one `mcp_tool_execute` or `mcp_tool_execute_failed` event
    /// with the same `correlation_id` as the authorization event.
    /// When `audit_details.query` is `Some`, the SQL is included in `details_json`
    /// so the audit sink can fingerprint or store it per the capture-query-text setting.
    async fn audit_execution_with_details(
        &self,
        tool_id: &str,
        connection_id: Option<&str>,
        result: &Result<CallToolResult, McpError>,
        outcome: &AuthorizationOutcome,
        audit_details: &AuditDetails,
    ) -> Result<(), McpError> {
        let runtime = self.state.runtime.read().await;
        let audit_service = runtime.audit_service();

        let ts_ms = now_epoch_ms();
        let correlation_id = outcome
            .correlation_id
            .clone()
            .unwrap_or_else(new_correlation_id);
        let origin = EventOrigin::mcp();
        let conn_id = connection_id.unwrap_or_default();

        match result {
            Ok(call_result) if call_result.is_error != Some(true) => {
                // Success
                let mut event = build_execution_event(
                    ts_ms,
                    EventSeverity::Info,
                    EventOutcome::Success,
                    tool_id,
                    outcome.actor_id.as_str(),
                );

                let ctx = AuditContext::new()
                    .with_origin(origin)
                    .with_correlation_id(correlation_id.as_str())
                    .with_connection_id(conn_id);
                ctx.apply_to(&mut event);

                let mut map = serde_json::Map::new();
                map.insert(
                    "content_count".to_string(),
                    serde_json::json!(call_result.content.len()),
                );
                if let Some(q) = &audit_details.query {
                    map.insert("query".to_string(), serde_json::Value::String(q.clone()));
                }
                event = event.with_details_json(serde_json::Value::Object(map).to_string());

                audit_service.record(event).map_err(|e| {
                    McpError::internal_error(format!("Execution audit error: {}", e), None)
                })?;
            }
            Ok(call_result) => {
                // Handler returned an error-structured result (is_error == true)
                let error_msg = extract_error_content(call_result);
                let mut event = build_execution_event(
                    ts_ms,
                    EventSeverity::Warn,
                    EventOutcome::Failure,
                    tool_id,
                    outcome.actor_id.as_str(),
                );

                let ctx = AuditContext::new()
                    .with_origin(origin)
                    .with_correlation_id(correlation_id.as_str())
                    .with_connection_id(conn_id);
                ctx.apply_to(&mut event);

                event = event.with_error("handler_error", &error_msg);

                audit_service.record(event).map_err(|e| {
                    McpError::internal_error(format!("Execution audit error: {}", e), None)
                })?;
            }
            Err(mcp_error) => {
                // Handler returned an error
                let mut event = build_execution_event(
                    ts_ms,
                    EventSeverity::Error,
                    EventOutcome::Failure,
                    tool_id,
                    outcome.actor_id.as_str(),
                );

                let ctx = AuditContext::new()
                    .with_origin(origin)
                    .with_correlation_id(correlation_id.as_str())
                    .with_connection_id(conn_id);
                ctx.apply_to(&mut event);

                event = event.with_error("handler_error", mcp_error.message.to_string());

                audit_service.record(event).map_err(|e| {
                    McpError::internal_error(format!("Execution audit error: {}", e), None)
                })?;
            }
        }

        Ok(())
    }
}

/// Builds the error returned for a call that authorization did not allow.
///
/// A call queued for approval carries its pending execution id and the exact
/// steps an agent must follow, so it can hand the decision to the user and
/// retry correctly without guessing.
fn authorization_error(
    outcome: &AuthorizationOutcome,
    tool_id: &str,
    classification: ExecutionClassification,
) -> McpError {
    let reason = outcome
        .deny_reason
        .as_deref()
        .unwrap_or("authorization denied");

    if outcome.deny_code == Some(APPROVAL_REQUIRED_CODE) {
        let pending_id = outcome.pending_execution_id.as_deref().unwrap_or("unknown");

        return McpError::new(
            rmcp::model::ErrorCode::INVALID_REQUEST,
            approval_required_message(tool_id, classification, pending_id),
            Some(serde_json::json!({
                "code": APPROVAL_REQUIRED_CODE,
                "status": "pending",
                "pending_id": outcome.pending_execution_id,
                "tool_id": tool_id,
                "next_action": "wait_for_human_approval_then_repeat_identical_call",
                "status_tool": "get_pending_execution",
            })),
        );
    }

    McpError::new(
        rmcp::model::ErrorCode::INVALID_REQUEST,
        reason.to_string(),
        outcome
            .deny_code
            .map(|code| serde_json::json!({ "code": code })),
    )
}

/// Instructions returned to the agent when a call is queued for approval.
pub fn approval_required_message(
    tool_id: &str,
    classification: ExecutionClassification,
    pending_id: &str,
) -> String {
    format!(
        "Approval required: the policy requires a person to approve '{tool_id}' calls of class \
         {classification:?}. This call has NOT run; it was queued as pending execution \
         {pending_id}. Next steps: (1) Tell the user that pending execution {pending_id} is \
         waiting for their approval in DBFlux (Workspace > Pending Approvals). (2) Wait for the \
         user. Do not call approve_execution or reject_execution; they are always denied to MCP \
         clients. (3) To check the status, call get_pending_execution with \
         {{\"pending_id\": \"{pending_id}\"}}: while its status is 'pending', it is still \
         waiting for a decision; status 'rejected' means the user rejected it, and its 'reason' \
         field carries what they wrote; once it is no longer found, it was approved or has \
         expired. (4) After the user approves it, repeat this exact call: the same tool \
         '{tool_id}' with identical arguments. It then runs once. If it was rejected or expired, \
         repeating it queues a new request instead of running."
    )
}

/// Returns the appropriate typed audit action for a tool execution.
///
/// Query and script executions emit canonical `QUERY_EXECUTE`/`SCRIPT_EXECUTE` events
/// instead of the generic `MCP_TOOL_EXECUTE` event.
fn execution_action(
    tool_id: &str,
    outcome: EventOutcome,
) -> dbflux_core::observability::AuditAction {
    if is_query_tool(tool_id) {
        match outcome {
            EventOutcome::Success => actions::QUERY_EXECUTE,
            EventOutcome::Failure => actions::QUERY_EXECUTE_FAILED,
            _ => actions::QUERY_EXECUTE,
        }
    } else if is_script_tool(tool_id) {
        match outcome {
            EventOutcome::Success => actions::SCRIPT_EXECUTE,
            EventOutcome::Failure => actions::SCRIPT_EXECUTE_FAILED,
            _ => actions::SCRIPT_EXECUTE,
        }
    } else {
        match outcome {
            EventOutcome::Success => actions::MCP_TOOL_EXECUTE,
            EventOutcome::Failure => actions::MCP_TOOL_EXECUTE_FAILED,
            _ => actions::MCP_TOOL_EXECUTE,
        }
    }
}

/// Returns true if the tool is a query tool (select_data, count_records, aggregate_data).
fn is_query_tool(tool_id: &str) -> bool {
    matches!(tool_id, "select_data" | "count_records" | "aggregate_data")
}

/// Returns true if the tool is a script tool (execute_script).
fn is_script_tool(tool_id: &str) -> bool {
    tool_id == "execute_script"
}

/// Builds a canonical MCP tool execution event (without context fields — apply those separately).
fn build_execution_event(
    ts_ms: i64,
    level: EventSeverity,
    outcome: EventOutcome,
    tool_id: &str,
    actor_id: &str,
) -> EventRecord {
    let action = execution_action(tool_id, outcome);

    let summary = if is_query_tool(tool_id) {
        format!("Query {}: tool={}", outcome.as_str(), tool_id)
    } else if is_script_tool(tool_id) {
        format!("Script {}: tool={}", outcome.as_str(), tool_id)
    } else {
        format!("MCP tool {}: tool={}", outcome.as_str(), tool_id)
    };

    EventRecord::new(ts_ms, level, EventCategory::Mcp, outcome)
        .with_typed_action(action)
        .with_summary(summary)
        .with_actor_id(actor_id)
        .with_object_ref("tool", tool_id)
}

/// Extracts the error content from a [`CallToolResult`] that has `is_error == true`.
///
/// `CallToolResult.content` is a `Vec<ContentBlock>`, the MCP 2025-11-25 unified
/// content union, so the variants are matched directly.
fn extract_error_content(result: &CallToolResult) -> String {
    let mut msgs = Vec::new();
    for content in &result.content {
        match content {
            ContentBlock::Text(text) => {
                msgs.push(text.text.clone());
            }
            ContentBlock::Image(img) => {
                msgs.push(format!("[image: {} bytes]", img.data.len()));
            }
            ContentBlock::Audio(_) => {
                msgs.push("[audio content]".to_string());
            }
            ContentBlock::Resource(res) => {
                match &res.resource {
                    rmcp::model::ResourceContents::TextResourceContents { uri, .. }
                    | rmcp::model::ResourceContents::BlobResourceContents { uri, .. } => {
                        msgs.push(format!("[resource: {}]", uri));
                    }
                    _ => {}
                };
            }
            ContentBlock::ResourceLink(link) => {
                msgs.push(format!("[resource_link: {}]", link.uri));
            }
            _ => {}
        }
    }
    msgs.join("\n")
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
        let temp_path = dbflux_audit::temp_sqlite_path("governance_test_audit.sqlite");
        let audit_service = dbflux_audit::AuditService::new_sqlite(&temp_path)
            .expect("failed to create test audit service");
        let mut runtime = McpRuntime::new(
            audit_service,
            Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
        );

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
            profile_manager: Arc::new(RwLock::new(dbflux_core::ProfileManager::new_in_memory())),
            auth_profile_manager: Arc::new(RwLock::new(dbflux_core::AuthProfileManager::default())),
            driver_registry: Arc::new(std::collections::HashMap::new()),
            auth_provider_registry: Arc::new(std::collections::HashMap::new()),
            driver_settings: Arc::new(std::collections::HashMap::new()),
            connection_cache: Arc::new(
                RwLock::new(crate::connection_cache::ConnectionCache::new()),
            ),
            connection_setup_lock: Arc::new(tokio::sync::Mutex::new(())),
            secret_manager: Arc::new(dbflux_core::SecretManager::new(Box::new(
                dbflux_core::NoopSecretStore,
            ))),
            mcp_enabled_by_default: true,
        }
    }

    #[test]
    fn approval_required_message_tells_the_agent_exactly_what_to_do() {
        let message = approval_required_message(
            "delete_records",
            ExecutionClassification::Destructive,
            "1234",
        );

        for expected in [
            "has NOT run",
            "pending execution 1234",
            "Pending Approvals",
            "Do not call approve_execution or reject_execution",
            "get_pending_execution",
            "\"pending_id\": \"1234\"",
            "repeat this exact call",
            "identical arguments",
            "'delete_records'",
            "Destructive",
        ] {
            assert!(
                message.contains(expected),
                "missing {expected:?} in {message}"
            );
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
            tracing::error!(
                code = ?err.code,
                message = %err.message,
                "Authorization failed"
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
                    Ok(CallToolResult::success(vec![
                        rmcp::model::ContentBlock::text("test result"),
                    ]))
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
