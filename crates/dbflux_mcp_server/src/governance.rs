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
#[expect(
    clippy::unwrap_used,
    reason = "a sane process clock is assumed to have passed UNIX_EPOCH; the standard \
        library guarantees no such thing, so a pre-epoch reading aborts here instead of \
        yielding a negative timestamp"
)]
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
    #[cfg(feature = "telemetry")]
    pub(crate) usage_client: std::sync::Arc<crate::telemetry_usage::McpToolUsageClient>,
}

impl GovernanceMiddleware {
    pub fn new(state: ServerState) -> Self {
        Self {
            state,
            #[cfg(feature = "telemetry")]
            usage_client: std::sync::Arc::new(crate::telemetry_usage::McpToolUsageClient::new()),
        }
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

        let read_only_connection = match connection_id {
            Some(conn_id) => self.state.is_read_only_connection(conn_id).await,
            None => false,
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
            read_only_connection,
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

        #[cfg(feature = "telemetry")]
        let evidence = crate::telemetry_usage::capture(tool_id);
        #[cfg(feature = "telemetry")]
        let capability = if evidence.is_some() {
            self.usage_client.begin().await
        } else {
            None
        };

        let operation = handler();
        #[cfg(feature = "telemetry")]
        let operation = crate::telemetry_usage::OPERATION.scope(evidence.clone(), async {
            let result = operation.await;
            if let Ok((result, _)) = &result {
                crate::telemetry_usage::observe_final(result);
            }
            result
        });
        let handler_result = operation.await;

        let (result, audit_details) = match handler_result {
            Ok((result, details)) => (Ok(result), details),
            Err(e) => (Err(e), AuditDetails::default()),
        };

        let audit_result = self
            .audit_execution_with_details(tool_id, connection_id, &result, &outcome, &audit_details)
            .await;

        #[cfg(feature = "telemetry")]
        if let Some(capability) = capability {
            let success = audit_result.is_ok()
                && result
                    .as_ref()
                    .is_ok_and(|result| result.is_error != Some(true))
                && evidence
                    .as_ref()
                    .is_some_and(|evidence| evidence.successful());
            // No detached cancellation cleanup: dropping this future abandons the
            // opaque capability to the foreground owner's bounded expiry policy.
            let _finish_ack = self.usage_client.finish(capability, success).await;
        }
        audit_result?;
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
pub(crate) mod tests {
    use super::*;
    use dbflux_mcp::{McpRuntime, builtin_policies, builtin_roles};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Helper to create a test ServerState with minimal setup
    pub(crate) fn create_test_state() -> ServerState {
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
            secret_provider_registry: Arc::new(std::collections::HashMap::new()),
            driver_settings: Arc::new(std::collections::HashMap::new()),
            connection_cache: Arc::new(
                RwLock::new(crate::connection_cache::ConnectionCache::new()),
            ),
            connection_setup_lock: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            detached_processes: Arc::new(crate::state::DetachedProcessRegistry::new()),
            secret_manager: Arc::new(dbflux_core::SecretManager::new(Box::new(
                dbflux_core::NoopSecretStore,
            ))),
            mcp_enabled_by_default: true,
            profile_store_path: None,
        }
    }

    #[cfg(feature = "telemetry")]
    struct MutationFixture {
        metadata: dbflux_core::DriverMetadata,
        query: dbflux_core::QueryResult,
        opaque: dbflux_core::CrudResult,
    }

    #[cfg(feature = "telemetry")]
    impl dbflux_core::Connection for MutationFixture {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }
        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::SQLite
        }
        fn ping(&self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }
        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
            Ok(dbflux_core::SchemaSnapshot::default())
        }
        fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
            dbflux_core::SchemaLoadingStrategy::SingleDatabase
        }
        fn execute(
            &self,
            _request: &dbflux_core::QueryRequest,
        ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
            Ok(self.query.clone())
        }
        fn plan_semantic_request(
            &self,
            _request: &dbflux_core::SemanticRequest,
        ) -> Result<dbflux_core::SemanticPlan, dbflux_core::DbError> {
            Ok(dbflux_core::SemanticPlan::single_query(
                dbflux_core::SemanticPlanKind::Query,
                dbflux_core::PlannedQuery::new(
                    dbflux_core::QueryLanguage::Sql,
                    "private mutation fixture",
                ),
            ))
        }
        fn insert_row(
            &self,
            _insert: &dbflux_core::RowInsert,
        ) -> Result<dbflux_core::CrudResult, dbflux_core::DbError> {
            // Model the source-verified SQL Server conversion, not a SQL Server runtime.
            Ok(match self.query.rows.first() {
                Some(row) => dbflux_core::CrudResult::success(row.clone()),
                None => dbflux_core::CrudResult::empty(),
            })
        }
        fn update_document(
            &self,
            _update: &dbflux_core::DocumentUpdate,
        ) -> Result<dbflux_core::CrudResult, dbflux_core::DbError> {
            Ok(self.opaque.clone())
        }
        fn delete_document(
            &self,
            _delete: &dbflux_core::DocumentDelete,
        ) -> Result<dbflux_core::CrudResult, dbflux_core::DbError> {
            Ok(self.opaque.clone())
        }
    }

    #[cfg(feature = "telemetry")]
    pub(crate) async fn with_usage_owner<F, Fut>(
        state: ServerState,
        call: F,
    ) -> (Fut::Output, Vec<bool>, [u16; 6], usize)
    where
        F: FnOnce(GovernanceMiddleware) -> Fut,
        Fut: Future,
    {
        use dbflux_core::observability::usage::{UsageController, UsageFeature};
        use dbflux_ipc::{
            AppControlRequest, AppControlResponse,
            framing::{recv_telemetry_msg, send_telemetry_msg},
            protocol::{IpcMessage, IpcResponse},
        };
        let mut middleware = GovernanceMiddleware::new(state);
        let mut owner_streams = std::collections::VecDeque::new();
        let mut client_streams = Vec::new();
        for _ in 0..16 {
            let (client, owner) = tokio::io::duplex(4096);
            client_streams.push(client);
            owner_streams.push_back(owner);
        }
        middleware.usage_client = Arc::new(crate::telemetry_usage::McpToolUsageClient::fixture(
            client_streams,
        ));
        let mut controller = UsageController::new();
        controller.set_enabled(true).expect("Basic");
        let source = controller.source();
        let (stop_send, mut stop_receive) = tokio::sync::oneshot::channel();
        let owner = async move {
            let mut tickets = std::collections::HashMap::new();
            let mut finishes = Vec::new();
            while let Some(mut stream) = owner_streams.pop_front() {
                let request: AppControlRequest = tokio::select! {
                    request = recv_telemetry_msg(&mut stream) => match request {
                        Ok(request) => request,
                        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(error) => panic!("private request: {error}"),
                    },
                    _ = &mut stop_receive => break,
                };
                assert_eq!(request.auth_token.as_deref(), Some("private-fixture-auth"));
                let body = match request.body {
                    IpcMessage::BeginMcpToolUsage { session_nonce } => {
                        let capability = uuid::Uuid::new_v4();
                        tickets.insert(
                            (session_nonce, capability),
                            source.ticket().expect("gate").expect("ticket"),
                        );
                        IpcResponse::McpToolUsageCapability {
                            capability: Some(capability),
                        }
                    }
                    IpcMessage::FinishMcpToolUsage {
                        session_nonce,
                        capability,
                        success,
                    } => {
                        finishes.push(success);
                        let ticket = tickets
                            .remove(&(session_nonce, capability))
                            .expect("one-shot");
                        if success {
                            assert!(ticket.record(UsageFeature::McpTool).expect("credit"));
                        }
                        IpcResponse::McpToolUsageFinished { accepted: true }
                    }
                    _ => panic!("unexpected private request"),
                };
                send_telemetry_msg(
                    &mut stream,
                    &AppControlResponse::ok(request.request_id, body),
                )
                .await
                .expect("reply");
            }
            (finishes, tickets.len())
        };
        let call = async move {
            let result = call(middleware).await;
            stop_send.send(()).expect("stop");
            result
        };
        let ((finishes, leases), result) = tokio::join!(owner, call);
        let mut counts = [0; 6];
        controller
            .commit(|snapshot| {
                counts = snapshot;
                Ok::<(), ()>(())
            })
            .expect("gate")
            .expect("commit");
        (result, finishes, counts, leases)
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_normal_dispatch_credits_fresh_completed_calls() {
        use dbflux_core::observability::usage::{UsageController, UsageFeature};
        use dbflux_ipc::{
            AppControlRequest, AppControlResponse,
            framing::{recv_telemetry_msg, send_telemetry_msg},
            protocol::{IpcMessage, IpcResponse},
        };
        use rmcp::{ServiceExt, model::CallToolRequestParams};
        use std::collections::{HashMap, VecDeque};

        let state = create_test_state();
        let runtime = state.runtime.clone();
        let mut server = crate::server::DbFluxServer::new(state);
        let mut client_streams = Vec::new();
        let mut owner_streams = VecDeque::new();
        for _ in 0..4 {
            let (client, owner) = tokio::io::duplex(4096);
            client_streams.push(client);
            owner_streams.push_back(owner);
        }
        server.governance.usage_client = Arc::new(
            crate::telemetry_usage::McpToolUsageClient::fixture(client_streams),
        );
        let mut owner = UsageController::new();
        owner.set_enabled(true).expect("Basic permission");
        let source = owner.source();
        let (stop_send, mut stop_receive) = tokio::sync::oneshot::channel();
        let owner_work = async {
            let mut tickets = HashMap::new();
            let mut steps = Vec::new();
            while let Some(mut stream) = owner_streams.pop_front() {
                let request: AppControlRequest = tokio::select! {
                    request = recv_telemetry_msg(&mut stream) => request.expect("private request"),
                    _ = &mut stop_receive => break,
                };
                assert_eq!(request.auth_token.as_deref(), Some("private-fixture-auth"));
                let body = match request.body {
                    IpcMessage::BeginMcpToolUsage { session_nonce } => {
                        steps.push("begin");
                        let events = runtime
                            .read()
                            .await
                            .audit_service()
                            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
                            .expect("authorization audit");
                        assert_eq!(
                            events
                                .iter()
                                .filter(|event| event.action.as_deref() == Some("mcp_authorize"))
                                .count(),
                            steps.len().div_ceil(2),
                            "authorization precedes Begin"
                        );
                        let capability = uuid::Uuid::new_v4();
                        tickets.insert(
                            (session_nonce, capability),
                            source.ticket().expect("gate").expect("ticket"),
                        );
                        IpcResponse::McpToolUsageCapability {
                            capability: Some(capability),
                        }
                    }
                    IpcMessage::FinishMcpToolUsage {
                        session_nonce,
                        capability,
                        success,
                    } => {
                        steps.push("finish");
                        assert!(success);
                        let events = runtime
                            .read()
                            .await
                            .audit_service()
                            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
                            .expect("audit");
                        assert_eq!(
                            events
                                .iter()
                                .filter(|event| event.action.as_deref() == Some("mcp_tool_execute"))
                                .count(),
                            steps.len() / 2,
                            "execution audit precedes finish"
                        );
                        let ticket = tickets
                            .remove(&(session_nonce, capability))
                            .expect("one-shot capability");
                        assert!(ticket.record(UsageFeature::McpTool).expect("credit"));
                        assert!(
                            !ticket
                                .clone()
                                .record(UsageFeature::McpTool)
                                .expect("replay")
                        );
                        IpcResponse::McpToolUsageFinished { accepted: true }
                    }
                    _ => panic!("unexpected private message"),
                };
                send_telemetry_msg(
                    &mut stream,
                    &AppControlResponse::ok(request.request_id, body),
                )
                .await
                .expect("private reply");
            }
            (steps, tickets.len())
        };
        let calls = async move {
            let (server_io, client_io) = tokio::io::duplex(65536);
            let (served, client) = tokio::try_join!(
                async {
                    server
                        .serve(server_io)
                        .await
                        .map_err(|error| error.to_string())
                },
                async {
                    rmcp::serve_client((), client_io)
                        .await
                        .map_err(|error| error.to_string())
                },
            )
            .expect("MCP handshake");
            for _ in 0..2 {
                let result = client
                    .peer()
                    .call_tool(CallToolRequestParams::new("list_connections"))
                    .await
                    .expect("normal dispatcher call");
                assert_eq!(result.is_error, Some(false));
            }
            stop_send.send(()).expect("stop owner");
            client.cancel().await.expect("close client");
            served.cancel().await.expect("close server");
        };
        let ((steps, leases), ()) = tokio::join!(owner_work, calls);
        let mut counts = [0; 6];
        owner
            .commit(|snapshot| {
                counts = snapshot;
                Ok::<(), ()>(())
            })
            .expect("gate")
            .expect("snapshot");
        assert_eq!(counts, [0, 0, 0, 0, 2, 0]);
        assert_eq!(steps, ["begin", "finish", "begin", "finish"]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    async fn dispatch_fixture(
        middleware: GovernanceMiddleware,
        request: rmcp::model::CallToolRequestParams,
    ) -> (CallToolResult, usize) {
        use rmcp::ServiceExt;
        let usage_client = middleware.usage_client.clone();
        let mut server = crate::server::DbFluxServer::new(middleware.state.clone());
        server.governance = middleware;
        let (server_io, client_io) = tokio::io::duplex(65536);
        let (served, client) = tokio::try_join!(
            async {
                server
                    .serve(server_io)
                    .await
                    .map_err(|error| error.to_string())
            },
            async {
                rmcp::serve_client((), client_io)
                    .await
                    .map_err(|error| error.to_string())
            },
        )
        .expect("normal dispatcher handshake");
        let result = client
            .peer()
            .call_tool(request)
            .await
            .expect("unchanged successful result");
        let begins = usage_client.acknowledged_begins();
        client.cancel().await.expect("close client");
        served.cancel().await.expect("close server");
        (result, begins)
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_normal_document_and_typed_mutation_contracts() {
        use crate::connection_cache::CachedConnection;
        use dbflux_core::{
            ConnectionProfile, CrudResult, DatabaseCategory, DbConfig, DriverMetadataBuilder,
            QueryLanguage, QueryResult,
        };
        use rmcp::model::CallToolRequestParams;
        let state = create_test_state();
        let profile = ConnectionProfile::new(
            "private mutation fixture",
            DbConfig::SQLite {
                path: std::path::PathBuf::from("unused-private-fake"),
                connection_id: None,
            },
        );
        let connection_id = profile.id.to_string();
        state.profile_manager.write().await.add(profile);
        state
            .runtime
            .write()
            .await
            .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: connection_id.clone(),
                assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: "test-client".into(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: connection_id.clone(),
                    },
                    role_ids: vec!["builtin/admin".into()],
                    policy_ids: Vec::new(),
                }],
            })
            .expect("permission");
        let mut policy = dbflux_mcp::builtin_policies()
            .into_iter()
            .find(|policy| policy.id == "builtin/admin")
            .expect("policy");
        policy
            .allowed_classes
            .extend(policy.approval_classes.drain(..));
        state
            .runtime
            .write()
            .await
            .upsert_policy_mut(policy)
            .expect("explicit mutation permission");
        for tool in ["update_records", "delete_records"] {
            let key = if tool == "update_records" {
                "updated"
            } else {
                "deleted"
            };
            let mut baseline = None;
            for (document, failure) in [(true, true), (true, false), (false, true), (false, false)]
            {
                let metadata = DriverMetadataBuilder::new(
                    "private-fixture",
                    "Private fixture",
                    if document {
                        DatabaseCategory::Document
                    } else {
                        DatabaseCategory::Relational
                    },
                    QueryLanguage::Sql,
                )
                .build();
                let mut query = QueryResult::empty();
                query.affected_rows = Some(0);
                let mut opaque = CrudResult::empty();
                if failure {
                    let failed = std::collections::HashMap::from([(
                        "script_failure".into(),
                        serde_json::json!(true),
                    )]);
                    query.metadata_extra = Some(failed.clone());
                    opaque.metadata_extra = Some(failed);
                }
                let connection = Arc::new(MutationFixture {
                    metadata,
                    query,
                    opaque,
                });
                state.connection_cache.write().await.insert(
                    connection_id.clone(),
                    Arc::new(CachedConnection::new(connection, None)),
                );
                let arguments = serde_json::json!({"connection_id": connection_id, "table": "items", "where": {"id": 1}, "set": {"value": "private"}});
                let request = CallToolRequestParams::new(tool)
                    .with_arguments(arguments.as_object().expect("arguments").clone());
                let ((result, begins), finishes, counts, leases) =
                    with_usage_owner(state.clone(), |middleware| {
                        dispatch_fixture(middleware, request)
                    })
                    .await;
                assert_eq!(result.is_error, Some(false));
                let bytes = serde_json::to_vec(&result).expect("response bytes");
                if let Some(baseline) = &baseline {
                    assert_eq!(&bytes, baseline);
                } else {
                    baseline = Some(bytes);
                }
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(
                        &result.content[0].as_text().expect("text").text
                    )
                    .expect("JSON"),
                    serde_json::json!({key: 0})
                );
                let success = !document && !failure;
                assert_eq!(counts, [0, 0, 0, 0, u16::from(success), 0]);
                assert_eq!(
                    begins, 1,
                    "reviewed tool captures permission, not opaque completion"
                );
                assert_eq!(finishes, [success]);
                assert_eq!(leases, 0);
            }
        }
        let events = state
            .runtime
            .read()
            .await
            .audit_service()
            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
            .expect("audit");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action.as_deref() == Some("mcp_tool_execute")
                    && event.outcome.as_deref() == Some("success"))
                .count(),
            8
        );
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_normal_insert_and_script_listing_are_ineligible() {
        use crate::connection_cache::CachedConnection;
        use dbflux_core::{ConnectionProfile, DbConfig, DbKind};
        use dbflux_test_support::fake_driver::FakeDriver;
        use rmcp::{ServiceExt, model::CallToolRequestParams};
        let state = create_test_state();
        let profile = ConnectionProfile::new(
            "private insert fixture",
            DbConfig::SQLite {
                path: std::path::PathBuf::from("unused-private-fake"),
                connection_id: None,
            },
        );
        let connection_id = profile.id.to_string();
        state.profile_manager.write().await.add(profile.clone());
        state
            .runtime
            .write()
            .await
            .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: connection_id.clone(),
                assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: "test-client".into(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: connection_id.clone(),
                    },
                    role_ids: vec!["builtin/admin".into()],
                    policy_ids: Vec::new(),
                }],
            })
            .expect("permission");
        let mut policy = dbflux_mcp::builtin_policies()
            .into_iter()
            .find(|policy| policy.id == "builtin/admin")
            .expect("policy");
        policy
            .allowed_classes
            .extend(policy.approval_classes.drain(..));
        state
            .runtime
            .write()
            .await
            .upsert_policy_mut(policy)
            .expect("explicit write permission");
        let driver = FakeDriver::new(DbKind::SQLite);
        let connection = driver
            .connect_arc(&profile)
            .expect("fake connection without I/O");
        state.connection_cache.write().await.insert(
            connection_id.clone(),
            Arc::new(CachedConnection::new(connection, None)),
        );
        for request in [
            CallToolRequestParams::new("insert_record").with_arguments(serde_json::json!({"connection_id": connection_id, "table": "items", "records": [{"id": 1}]}).as_object().expect("arguments").clone()),
            CallToolRequestParams::new("list_scripts"),
        ] {
            let listing = request.name == "list_scripts";
            let ((result, begins), finishes, counts, leases) = with_usage_owner(state.clone(), |middleware| async move {
                let usage_client = middleware.usage_client.clone();
                let mut server = crate::server::DbFluxServer::new(middleware.state.clone());
                server.governance = middleware;
                let (server_io, client_io) = tokio::io::duplex(65536);
                let (served, client) = tokio::try_join!(
                    async { server.serve(server_io).await.map_err(|error| error.to_string()) },
                    async { rmcp::serve_client((), client_io).await.map_err(|error| error.to_string()) },
                ).expect("normal dispatcher handshake");
                let result = client.peer().call_tool(request).await.expect("unchanged successful result");
                let begins = usage_client.acknowledged_begins();
                client.cancel().await.expect("close client");
                served.cancel().await.expect("close server");
                (result, begins)
            }).await;
            assert_eq!(result.is_error, Some(false));
            let value = serde_json::from_str::<serde_json::Value>(&result.content[0].as_text().expect("text").text).expect("JSON");
            // `list_scripts` reads the machine's real scripts directory, so the
            // assertion is about shape, not about an empty machine. The subject
            // of this test is that neither call earns telemetry credit.
            if listing {
                assert!(
                    value.is_array(),
                    "list_scripts answers an array, whatever the machine holds"
                );
            } else {
                assert_eq!(value, serde_json::json!({"inserted": 1, "records": []}));
            }
            assert_eq!(counts, [0; 6], "opaque contracts never earn credit");
            assert_eq!(begins, 0);
            assert!(finishes.is_empty());
            assert_eq!(leases, 0);
        }
        let mut baseline = None;
        for failure_position in [None, Some(0), Some(1)] {
            let mut query = dbflux_core::QueryResult::empty();
            query.rows.push(vec![dbflux_core::Value::Int(1)]);
            if let Some(position) = failure_position {
                let failure = std::collections::HashMap::from([(
                    "script_failure".into(),
                    serde_json::json!(true),
                )]);
                if position == 0 {
                    query.metadata_extra = Some(failure);
                } else {
                    let mut earlier = dbflux_core::QueryResult::empty();
                    earlier.metadata_extra = Some(failure);
                    query.push_additional_result(earlier);
                }
            }
            let connection = MutationFixture {
                metadata: dbflux_core::DriverMetadataBuilder::new(
                    "private-fixture",
                    "Private fixture",
                    dbflux_core::DatabaseCategory::Relational,
                    dbflux_core::QueryLanguage::Sql,
                )
                .build(),
                query,
                opaque: dbflux_core::CrudResult::empty(),
            };
            use dbflux_core::Connection;
            let converted = connection.insert_row(&dbflux_core::RowInsert::new(
                "items".into(),
                None,
                vec!["id".into()],
                vec![dbflux_core::Value::Int(1)],
            ));
            assert!(
                converted
                    .expect("modeled conversion")
                    .metadata_extra
                    .is_none(),
                "source evidence is lost before the opaque boundary"
            );
            state.connection_cache.write().await.insert(
                connection_id.clone(),
                Arc::new(CachedConnection::new(Arc::new(connection), None)),
            );
            let request = CallToolRequestParams::new("insert_record").with_arguments(serde_json::json!({"connection_id": connection_id, "table": "items", "records": [{"id": 1}]}).as_object().expect("arguments").clone());
            let ((result, begins), finishes, counts, leases) =
                with_usage_owner(state.clone(), |middleware| {
                    dispatch_fixture(middleware, request)
                })
                .await;
            assert_eq!(result.is_error, Some(false));
            let bytes = serde_json::to_vec(&result).expect("bytes");
            if let Some(baseline) = &baseline {
                assert_eq!(&bytes, baseline);
            } else {
                baseline = Some(bytes);
            }
            assert_eq!(counts, [0; 6]);
            assert_eq!(begins, 0);
            assert!(finishes.is_empty());
            assert_eq!(leases, 0);
        }
        let events = state
            .runtime
            .read()
            .await
            .audit_service()
            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
            .expect("audit");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action.as_deref() == Some("mcp_tool_execute")
                    && event.outcome.as_deref() == Some("success"))
                .count(),
            5
        );
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_mutation_primary_role_rejects_probe_and_opaque_results() {
        use crate::{
            helper::{serialize_mutation_result, serialize_query_result, to_json_content},
            telemetry_usage::scope_call,
        };
        let state = create_test_state();
        let mut policy = dbflux_mcp::builtin_policies()
            .into_iter()
            .find(|policy| policy.id == "builtin/admin")
            .expect("policy");
        policy
            .allowed_classes
            .extend(policy.approval_classes.drain(..));
        state
            .runtime
            .write()
            .await
            .upsert_policy_mut(policy)
            .expect("mutation permission");
        let ((), finishes, counts, leases) = with_usage_owner(state, |middleware| async move {
            for tool in ["update_records", "delete_records", "upsert_record"] {
                for primary in [false, true] {
                    scope_call(middleware.authorize_and_execute(
                        tool,
                        None,
                        ExecutionClassification::Write,
                        || async {
                            let result = dbflux_core::QueryResult::empty();
                            let _probe = serialize_query_result(&result);
                            if !primary {
                                middleware
                                    .authorize_and_execute(
                                        tool,
                                        None,
                                        ExecutionClassification::Write,
                                        || async {
                                            let key = match tool {
                                                "update_records" => "updated",
                                                "delete_records" => "deleted",
                                                _ => "upserted",
                                            };
                                            let value =
                                                serialize_mutation_result(&result, key, false);
                                            Ok(CallToolResult::success(vec![to_json_content(
                                                &value,
                                            )?]))
                                        },
                                    )
                                    .await?;
                            }
                            let key = match tool {
                                "update_records" => "updated",
                                "delete_records" => "deleted",
                                _ => "upserted",
                            };
                            let value = if primary {
                                serialize_mutation_result(&result, key, false)
                            } else {
                                serde_json::json!({key: 0})
                            };
                            Ok(CallToolResult::success(vec![to_json_content(&value)?]))
                        },
                    ))
                    .await
                    .expect("unchanged response");
                }
            }
        })
        .await;
        assert_eq!(
            counts,
            [0, 0, 0, 0, 3, 0],
            "only primary mutation observations qualify"
        );
        assert_eq!(finishes, [false, true, false, true, false, true]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_normal_count_dispatch_is_ineligible_even_when_successful() {
        use crate::connection_cache::CachedConnection;
        use dbflux_core::{ConnectionProfile, DbConfig, DbKind, QueryResult};
        use dbflux_test_support::fake_driver::FakeDriver;
        use rmcp::{ServiceExt, model::CallToolRequestParams};

        let state = create_test_state();
        let profile = ConnectionProfile::new(
            "private count fixture",
            DbConfig::SQLite {
                path: std::path::PathBuf::from("unused-private-fake"),
                connection_id: None,
            },
        );
        let connection_id = profile.id.to_string();
        state.profile_manager.write().await.add(profile.clone());
        state
            .runtime
            .write()
            .await
            .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: connection_id.clone(),
                assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: "test-client".into(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: connection_id.clone(),
                    },
                    role_ids: vec!["builtin/admin".into()],
                    policy_ids: Vec::new(),
                }],
            })
            .expect("count permission");
        for (failure_position, tool) in [
            (Some(0), "count_records"),
            (Some(1), "count_records"),
            (None, "count_records"),
            (None, "list_connections"),
        ] {
            let mut query = QueryResult::empty();
            if let Some(position) = failure_position {
                let mut failed = QueryResult::empty();
                failed.metadata_extra = Some(std::collections::HashMap::from([(
                    "script_failure".into(),
                    serde_json::json!(true),
                )]));
                if position == 0 {
                    query = failed;
                } else {
                    query.push_additional_result(failed);
                }
            }
            let driver = FakeDriver::new(DbKind::SQLite).with_default_result(query);
            let connection = driver
                .connect_arc(&profile)
                .expect("fake connection without I/O");
            state.connection_cache.write().await.insert(
                connection_id.clone(),
                Arc::new(CachedConnection::new(connection, None)),
            );
            let request = if tool == "count_records" {
                CallToolRequestParams::new(tool).with_arguments(
                    serde_json::json!({"connection_id": connection_id, "table": "items"})
                        .as_object()
                        .expect("arguments")
                        .clone(),
                )
            } else {
                CallToolRequestParams::new(tool)
            };
            let ((result, begins), finishes, counts, leases) =
                with_usage_owner(state.clone(), |middleware| async move {
                    let usage_client = middleware.usage_client.clone();
                    let mut server = crate::server::DbFluxServer::new(middleware.state.clone());
                    server.governance = middleware;
                    let (server_io, client_io) = tokio::io::duplex(65536);
                    let (served, client) = tokio::try_join!(
                        async {
                            server
                                .serve(server_io)
                                .await
                                .map_err(|error| error.to_string())
                        },
                        async {
                            rmcp::serve_client((), client_io)
                                .await
                                .map_err(|error| error.to_string())
                        },
                    )
                    .expect("normal dispatcher handshake");
                    let result = client
                        .peer()
                        .call_tool(request)
                        .await
                        .expect("unchanged successful result");
                    let begins = usage_client.acknowledged_begins();
                    client.cancel().await.expect("close client");
                    served.cancel().await.expect("close server");
                    (result, begins)
                })
                .await;
            assert_eq!(result.is_error, Some(false));
            let expected_credit = u16::from(tool == "list_connections");
            assert_eq!(
                counts,
                [0, 0, 0, 0, expected_credit, 0],
                "only proven operations earn credit"
            );
            assert_eq!(
                begins,
                usize::from(expected_credit),
                "opaque scalar contract is excluded before Begin"
            );
            assert_eq!(
                finishes,
                if expected_credit == 0 {
                    Vec::new()
                } else {
                    vec![true]
                }
            );
            assert_eq!(leases, 0);
            if tool == "count_records" {
                let text = result.content[0].as_text().expect("count text");
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&text.text).expect("count JSON"),
                    serde_json::json!({"count": 0})
                );
                assert_eq!(
                    driver.stats().executed_requests.len(),
                    1,
                    "default Core count_table executed once"
                );
            }
        }
        let events = state
            .runtime
            .read()
            .await
            .audit_service()
            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
            .expect("unchanged audit");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action.as_deref() == Some("query_execute")
                    && event.outcome.as_deref() == Some("success"))
                .count(),
            3
        );
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_authorize_begin_handler_audit_finish_order() {
        use crate::{helper::to_json_content, telemetry_usage::scope_call};
        let state = create_test_state();
        let runtime = state.runtime.clone();
        let ((), finishes, counts, leases) = with_usage_owner(state, |middleware| async move {
            let client = middleware.usage_client.clone();
            let result = scope_call(middleware.authorize_and_execute(
                "list_connections",
                None,
                ExecutionClassification::Metadata,
                || async {
                    assert_eq!(
                        client.acknowledged_begins(),
                        1,
                        "foreground Begin reply precedes handler"
                    );
                    let events = runtime
                        .read()
                        .await
                        .audit_service()
                        .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
                        .expect("audit");
                    assert!(
                        events
                            .iter()
                            .any(|event| event.action.as_deref() == Some("mcp_authorize"))
                    );
                    assert!(
                        !events
                            .iter()
                            .any(|event| event.action.as_deref() == Some("mcp_tool_execute")),
                        "execution audit follows handler"
                    );
                    Ok(CallToolResult::success(vec![to_json_content(
                        &serde_json::json!({"connections": []}),
                    )?]))
                },
            ))
            .await
            .expect("completed");
            assert_eq!(result.is_error, Some(false));
            let events = runtime
                .read()
                .await
                .audit_service()
                .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
                .expect("audit");
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.action.as_deref() == Some("mcp_tool_execute"))
                    .count(),
                1
            );
        })
        .await;
        assert_eq!(finishes, [true]);
        assert_eq!(counts, [0, 0, 0, 0, 1, 0]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_failures_unknown_scope_and_controls_never_credit() {
        use crate::{helper::to_json_content, telemetry_usage::scope_call};
        let (results, finishes, counts, leases) =
            with_usage_owner(create_test_state(), |middleware| async move {
                let error = scope_call(middleware.authorize_and_execute(
                    "list_connections",
                    None,
                    ExecutionClassification::Metadata,
                    || async { Err(McpError::internal_error("handler failed", None)) },
                ))
                .await;
                let shaped = scope_call(middleware.authorize_and_execute(
                    "list_connections",
                    None,
                    ExecutionClassification::Metadata,
                    || async {
                        Ok(CallToolResult::error(vec![to_json_content(
                            &serde_json::json!({"connections": []}),
                        )?]))
                    },
                ))
                .await;
                let unknown_shape = scope_call(middleware.authorize_and_execute(
                    "list_connections",
                    None,
                    ExecutionClassification::Metadata,
                    || async {
                        Ok(CallToolResult::success(vec![to_json_content(
                            &serde_json::json!({"new_shape": []}),
                        )?]))
                    },
                ))
                .await;
                let no_scope = middleware
                    .authorize_and_execute(
                        "list_connections",
                        None,
                        ExecutionClassification::Metadata,
                        || async {
                            Ok(CallToolResult::success(vec![to_json_content(
                                &serde_json::json!({"connections": []}),
                            )?]))
                        },
                    )
                    .await;
                for tool in [
                    "request_execution",
                    "list_pending_executions",
                    "get_pending_execution",
                    "approve_execution",
                    "reject_execution",
                    "help",
                    "future_tool",
                ] {
                    let result = scope_call(middleware.authorize_and_execute(
                        tool,
                        None,
                        ExecutionClassification::Metadata,
                        || async {
                            Ok(CallToolResult::success(vec![to_json_content(
                                &serde_json::json!({"connections": []}),
                            )?]))
                        },
                    ))
                    .await;
                    // Authorization may deny these controls; either disposition earns no credit.
                    drop(result);
                }
                (error, shaped, unknown_shape, no_scope)
            })
            .await;
        assert!(results.0.is_err());
        assert_eq!(results.1.expect("error-shaped Ok").is_error, Some(true));
        assert!(results.2.is_ok() && results.3.is_ok());
        assert_eq!(finishes, [false, false, false]);
        assert_eq!(counts, [0; 6]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_nested_and_unrelated_child_evidence_cannot_complete_parent() {
        use crate::{
            helper::{serialize_query_result, to_json_content},
            telemetry_usage::scope_call,
        };
        let (result, finishes, counts, leases) =
            with_usage_owner(create_test_state(), |middleware| async move {
                scope_call(middleware.authorize_and_execute(
                    "execute_script",
                    None,
                    ExecutionClassification::Read,
                    || async {
                        let nested = middleware
                            .authorize_and_execute(
                                "execute_script",
                                None,
                                ExecutionClassification::Read,
                                || async {
                                    let value =
                                        serialize_query_result(&dbflux_core::QueryResult::empty());
                                    Ok(CallToolResult::success(vec![to_json_content(&value)?]))
                                },
                            )
                            .await?;
                        assert_eq!(nested.is_error, Some(false));
                        // Tokio child tasks do not inherit the operation's task locals.
                        let child = tokio::spawn(async {
                            serialize_query_result(&dbflux_core::QueryResult::empty())
                        })
                        .await
                        .expect("owned child joined");
                        Ok(CallToolResult::success(vec![to_json_content(&child)?]))
                    },
                ))
                .await
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(finishes, [false]);
        assert_eq!(counts, [0; 6]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_audit_error_terminalizes_false_and_preserves_error() {
        use crate::{helper::to_json_content, telemetry_usage::scope_call};
        let state = create_test_state();
        let runtime = state.runtime.clone();
        let (result, finishes, counts, leases) = with_usage_owner(state, |middleware| async move {
            scope_call(middleware.authorize_and_execute("list_connections", None,
                ExecutionClassification::Metadata, || async {
                    let path = runtime.read().await.audit_service().sqlite_path().to_path_buf();
                    let connection = rusqlite::Connection::open(path).expect("private audit fixture");
                    connection.execute_batch("CREATE TRIGGER fail_execution BEFORE INSERT ON aud_audit_events WHEN NEW.action = 'mcp_tool_execute' BEGIN SELECT RAISE(FAIL, 'private audit failure'); END;").expect("failure fixture");
                    Ok(CallToolResult::success(vec![to_json_content(&serde_json::json!({"connections": []}))?]))
                })).await
        }).await;
        assert!(
            result
                .expect_err("audit failure is outward Err")
                .message
                .contains("Execution audit error")
        );
        assert_eq!(finishes, [false]);
        assert_eq!(counts, [0; 6]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_cancelled_handler_abandons_only_one_bounded_lease() {
        use crate::telemetry_usage::scope_call;
        let ((), finishes, counts, leases) =
            with_usage_owner(create_test_state(), |middleware| async move {
                let (started_send, mut started_receive) = tokio::sync::oneshot::channel();
                let mut call = Box::pin(scope_call(middleware.authorize_and_execute(
                    "list_connections",
                    None,
                    ExecutionClassification::Metadata,
                    || async {
                        started_send.send(()).expect("handler started after begin");
                        std::future::pending::<Result<CallToolResult, McpError>>().await
                    },
                )));
                tokio::select! {
                    _ = &mut call => panic!("blocked handler completed"),
                    result = &mut started_receive => result.expect("started"),
                }
                drop(call);
            })
            .await;
        assert!(finishes.is_empty());
        assert_eq!(counts, [0; 6]);
        assert_eq!(leases, 1);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_concurrent_typed_multisets_sticky_failure_and_truncation() {
        use crate::{
            helper::{serialize_query_result, to_json_content},
            telemetry_usage::scope_call,
        };
        let (results, finishes, counts, leases) =
            with_usage_owner(create_test_state(), |middleware| async move {
                let barrier = Arc::new(tokio::sync::Barrier::new(2));
                let run = |failure: bool| {
                    let middleware = middleware.clone();
                    let barrier = barrier.clone();
                    async move {
                        scope_call(middleware.authorize_and_execute(
                            "execute_script",
                            None,
                            ExecutionClassification::Read,
                            || async {
                                let mut result = dbflux_core::QueryResult::empty();
                                let mut extra = dbflux_core::QueryResult::empty();
                                extra.metadata_extra = Some(std::collections::HashMap::from([(
                                    if failure {
                                        "script_failure"
                                    } else {
                                        "rows_truncated"
                                    }
                                    .into(),
                                    serde_json::json!(true),
                                )]));
                                result.push_additional_result(extra);
                                let json = serialize_query_result(&result);
                                barrier.wait().await;
                                // A later successful value cannot erase an earlier failed set.
                                let _later =
                                    serialize_query_result(&dbflux_core::QueryResult::empty());
                                Ok(CallToolResult::success(vec![to_json_content(&json)?]))
                            },
                        ))
                        .await
                    }
                };
                tokio::join!(run(true), run(false))
            })
            .await;
        assert!(results.0.is_ok() && results.1.is_ok());
        assert_eq!(finishes.len(), 2);
        assert_eq!(finishes.iter().filter(|success| **success).count(), 1);
        assert_eq!(counts, [0, 0, 0, 0, 1, 0]);
        assert_eq!(leases, 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn telemetry_denied_and_approval_queue_zero_then_approved_retry_one() {
        use crate::{helper::to_json_content, telemetry_usage::scope_call};
        let state = create_test_state();
        let runtime = state.runtime.clone();
        let ((), finishes, counts, leases) = with_usage_owner(state, |middleware| async move {
            let mut denied = middleware.clone();
            denied.state.client_id = "untrusted".into();
            assert!(
                scope_call(denied.authorize_and_execute(
                    "list_connections",
                    None,
                    ExecutionClassification::Metadata,
                    || async { panic!("denied handler") }
                ))
                .await
                .is_err()
            );
            let invoke = || {
                middleware.authorize_and_execute(
                    "delete_records",
                    None,
                    ExecutionClassification::Destructive,
                    || async {
                        Ok(CallToolResult::success(vec![to_json_content(
                            &crate::helper::serialize_mutation_result(
                                &dbflux_core::QueryResult::empty(),
                                "deleted",
                                false,
                            ),
                        )?]))
                    },
                )
            };
            let queued = scope_call(invoke())
                .await
                .expect_err("admin destructive requires approval");
            let pending_id = queued.data.expect("approval data")["pending_id"]
                .as_str()
                .expect("id")
                .to_owned();
            runtime
                .write()
                .await
                .approve_pending_execution_with_origin_mut(
                    &pending_id,
                    "local",
                    EventOrigin::local(),
                )
                .expect("person approves");
            let completed = scope_call(invoke())
                .await
                .expect("approved target actually executes");
            assert_eq!(completed.is_error, Some(false));
        })
        .await;
        assert_eq!(finishes, [true]);
        assert_eq!(counts, [0, 0, 0, 0, 1, 0]);
        assert_eq!(leases, 0);
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

    /// Builds a test state holding one profile with the given read-only
    /// setting, on which the test client has the admin role.
    async fn state_with_profile(read_only_flag: bool) -> (ServerState, String) {
        let state = create_test_state();

        let mut profile = dbflux_core::ConnectionProfile::new(
            "read-only-test",
            dbflux_core::DbConfig::SQLite {
                path: ":memory:".into(),
                connection_id: None,
            },
        );
        profile.read_only_flag = read_only_flag;
        let connection_id = profile.id.to_string();

        state.profile_manager.write().await.add(profile);

        state
            .runtime
            .write()
            .await
            .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: connection_id.clone(),
                assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: "test-client".to_string(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: connection_id.clone(),
                    },
                    role_ids: vec!["builtin/admin".to_string()],
                    policy_ids: vec![],
                }],
            })
            .expect("assignment should save");

        (state, connection_id)
    }

    async fn authorize_on(
        state: ServerState,
        connection_id: &str,
        tool_id: &str,
        classification: ExecutionClassification,
    ) -> (Result<CallToolResult, McpError>, bool) {
        let handler_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let middleware = GovernanceMiddleware::new(state);

        let result = middleware
            .authorize_and_execute(tool_id, Some(connection_id), classification, {
                let handler_ran = handler_ran.clone();
                || async move {
                    handler_ran.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(CallToolResult::success(vec![]))
                }
            })
            .await;

        (
            result,
            handler_ran.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    #[tokio::test]
    async fn read_only_profile_denies_a_write_before_the_policy() {
        let (state, connection_id) = state_with_profile(true).await;

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "insert_record",
            ExecutionClassification::Write,
        )
        .await;

        let error = result.expect_err("a write on a read-only profile must be denied");
        assert_eq!(
            error.data,
            Some(serde_json::json!({
                "code": dbflux_mcp::server::authorization::READ_ONLY_CONNECTION_CODE
            }))
        );
        assert!(error.message.contains("read-only"), "{}", error.message);
        assert!(!handler_ran, "the handler must not run");
    }

    #[tokio::test]
    async fn read_only_profile_still_allows_a_read() {
        let (state, connection_id) = state_with_profile(true).await;

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "select_data",
            ExecutionClassification::Read,
        )
        .await;

        assert!(result.is_ok(), "a read must still run: {result:?}");
        assert!(handler_ran);
    }

    #[tokio::test]
    async fn writable_profile_leaves_a_write_to_the_policy() {
        let (state, connection_id) = state_with_profile(false).await;

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "insert_record",
            ExecutionClassification::Write,
        )
        .await;

        let error = result.expect_err("the admin role asks for approval of a write");
        let code = error
            .data
            .as_ref()
            .and_then(|data| data.get("code"))
            .and_then(|code| code.as_str());
        assert_eq!(code, Some(APPROVAL_REQUIRED_CODE));
        assert!(!handler_ran);
    }

    /// Builds a state whose profile was loaded with `loaded_flag` and is
    /// stored with `stored_flag`, as after the user toggles the flag in the
    /// GUI while the server runs.
    async fn state_with_stored_profile(
        loaded_flag: bool,
        stored_flag: bool,
    ) -> (ServerState, String, tempfile::TempDir) {
        let (mut state, connection_id) = state_with_profile(loaded_flag).await;

        let store_dir = tempfile::tempdir().expect("temp dir should be created");
        let db_path = store_dir.path().join("dbflux.db");
        let storage = dbflux_storage::StorageRuntime::for_path(db_path.clone())
            .expect("storage should initialize");

        let profile_uuid: uuid::Uuid = connection_id.parse().expect("profile id is a uuid");
        let mut stored =
            dbflux_storage::repositories::connection_profiles::ConnectionProfileDto::new(
                profile_uuid,
                "read-only-test".to_string(),
            );
        stored.read_only = stored_flag;
        storage
            .connection_profiles()
            .insert(&stored)
            .expect("profile should be stored");

        state.profile_store_path = Some(db_path);

        (state, connection_id, store_dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn profile_set_to_read_only_after_startup_denies_a_write() {
        let (state, connection_id, _store_dir) = state_with_stored_profile(false, true).await;

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "insert_record",
            ExecutionClassification::Write,
        )
        .await;

        let error = result.expect_err("a write on a profile now read-only must be denied");
        assert_eq!(
            error.data,
            Some(serde_json::json!({
                "code": dbflux_mcp::server::authorization::READ_ONLY_CONNECTION_CODE
            }))
        );
        assert!(!handler_ran);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn profile_made_writable_after_startup_leaves_a_write_to_the_policy() {
        let (state, connection_id, _store_dir) = state_with_stored_profile(true, false).await;

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "insert_record",
            ExecutionClassification::Write,
        )
        .await;

        let error = result.expect_err("the admin role asks for approval of a write");
        let code = error
            .data
            .as_ref()
            .and_then(|data| data.get("code"))
            .and_then(|code| code.as_str());
        assert_eq!(code, Some(APPROVAL_REQUIRED_CODE));
        assert!(!handler_ran);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unreadable_profile_store_denies_a_write_on_a_profile_writable_at_startup() {
        let (mut state, connection_id) = state_with_profile(false).await;
        let store_dir = tempfile::tempdir().expect("temp dir should be created");
        state.profile_store_path = Some(store_dir.path().join("missing").join("dbflux.db"));

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "insert_record",
            ExecutionClassification::Write,
        )
        .await;

        let error = result.expect_err("an unreadable store must not let a write through");
        assert_eq!(
            error.data,
            Some(serde_json::json!({
                "code": dbflux_mcp::server::authorization::READ_ONLY_CONNECTION_CODE
            }))
        );
        assert!(!handler_ran);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unreadable_profile_store_still_allows_a_read() {
        let (mut state, connection_id) = state_with_profile(false).await;
        let store_dir = tempfile::tempdir().expect("temp dir should be created");
        state.profile_store_path = Some(store_dir.path().join("missing").join("dbflux.db"));

        let (result, handler_ran) = authorize_on(
            state,
            &connection_id,
            "select_data",
            ExecutionClassification::Read,
        )
        .await;

        assert!(result.is_ok(), "{result:?}");
        assert!(handler_ran);
    }
}
