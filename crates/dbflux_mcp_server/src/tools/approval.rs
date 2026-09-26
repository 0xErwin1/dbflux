//! Approval tools for MCP server.
//!
//! Provides tools for managing execution approvals:
//! - `request_execution`: Request approval for a pending operation
//! - `list_pending_executions`: List all pending executions
//! - `get_pending_execution`: Get details of a specific pending execution
//! - `approve_execution` / `reject_execution`: always denied over MCP by
//!   authorization; a person resolves pending executions in DBFlux

use dbflux_approval::store::{ExecutionPlan, PendingExecution, PendingStatus};
use dbflux_policy::ExecutionClassification;
use rmcp::{
    ErrorData, handler::server::wrapper::Parameters, model::CallToolResult, schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{helper::to_json_content, server::DbFluxServer};

const DEFAULT_REJECTION_REASON: &str = "rejected by approver";

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RequestExecutionParams {
    #[schemars(description = "Tool ID to execute (e.g., 'delete_records', 'drop_table')")]
    pub tool_id: String,

    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Tool parameters as JSON object")]
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListPendingExecutionsParams {
    #[schemars(description = "Filter by actor ID (optional)")]
    pub actor_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetPendingExecutionParams {
    #[schemars(description = "Pending execution ID")]
    pub pending_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ApproveExecutionParams {
    #[schemars(description = "Pending execution ID")]
    pub pending_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RejectExecutionParams {
    #[schemars(description = "Pending execution ID")]
    pub pending_id: String,

    #[schemars(description = "Reason for rejection (optional)")]
    pub reason: Option<String>,
}

#[tool_router(router = approval_router, vis = "pub")]
impl DbFluxServer {
    #[tool(
        description = "Queue an operation for approval by a person in DBFlux and return its pending_id. Tell the user to approve it in DBFlux (Workspace > Pending Approvals), wait (MCP clients cannot approve), check it with get_pending_execution, and once approved call the target tool itself with exactly the same params to run it once"
    )]
    async fn request_execution(
        &self,
        Parameters(params): Parameters<RequestExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
        let client_id = state.client_id.clone();

        let classification = Self::request_execution_classification(&params.tool_id)
            .map_err(|error| ErrorData::invalid_params(error, None))?;

        // Extract connection_id for authorization
        let connection_id_ref = params.connection_id.clone();

        // Clone all values for the closure
        let connection_id = params.connection_id;
        let tool_id = params.tool_id;
        let payload = params.params;

        self.governance
            .authorize_and_execute(
                "request_execution",
                Some(&connection_id_ref),
                classification,
                move || async move {
                    let plan = ExecutionPlan {
                        connection_id,
                        actor_id: client_id,
                        tool_id,
                        classification,
                        payload,
                    };

                    let pending = {
                        let mut runtime = state.runtime.write().await;
                        runtime
                            .request_execution_mut(plan)
                            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?
                    };

                    let response = serde_json::json!({
                        "pending_id": pending.id,
                        "status": "pending",
                        "classification": format!("{:?}", classification),
                        "message": "Execution request created. Awaiting approval."
                    });

                    Ok(CallToolResult::success(vec![to_json_content(&response)?]))
                },
            )
            .await
    }

    #[tool(
        description = "List executions still waiting for a person to approve or reject them in DBFlux. An execution disappears from this list once it is approved, rejected or expired"
    )]
    async fn list_pending_executions(
        &self,
        Parameters(params): Parameters<ListPendingExecutionsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();

        self.governance
            .authorize_and_execute(
                "list_pending_executions",
                None,
                ExecutionClassification::Read,
                move || async move {
                    let pending_list = {
                        let runtime = state.runtime.read().await;
                        let approval_service = runtime.approval_service();
                        approval_service
                            .list_pending()
                            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?
                    };

                    // Filter by actor_id if provided
                    let filtered: Vec<_> = if let Some(actor_id) = params.actor_id {
                        pending_list
                            .into_iter()
                            .filter(|p| p.plan.actor_id == actor_id)
                            .collect()
                    } else {
                        pending_list
                    };

                    let response = serde_json::json!({
                        "pending_executions": filtered,
                        "count": filtered.len()
                    });

                    Ok(CallToolResult::success(vec![to_json_content(&response)?]))
                },
            )
            .await
    }

    #[tool(
        description = "Check a pending execution by pending_id. While it is returned with status 'pending', it is still waiting for a person to approve it in DBFlux. Status 'rejected' means a person rejected it; 'reason' carries what they wrote, if anything: do not repeat the call unchanged. Once it is no longer found it was approved or expired: repeat the identical original call (same tool, same arguments); an approved call then runs once, otherwise a new request is queued"
    )]
    async fn get_pending_execution(
        &self,
        Parameters(params): Parameters<GetPendingExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
        let pending_id = params
            .pending_id
            .parse::<Uuid>()
            .map_err(|_| ErrorData::invalid_params("Invalid pending_id format", None))?;

        self.governance
            .authorize_and_execute(
                "get_pending_execution",
                None,
                ExecutionClassification::Read,
                move || async move {
                    let execution = {
                        let runtime = state.runtime.read().await;
                        runtime
                            .approval_service()
                            .get_execution(pending_id)
                            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?
                    };

                    match execution.and_then(|execution| {
                        pending_execution_status_response(&execution, now_epoch_ms())
                    }) {
                        Some(response) => {
                            Ok(CallToolResult::success(vec![to_json_content(&response)?]))
                        }
                        None => Err(ErrorData::invalid_params(
                            format!("Pending execution not found: {}", pending_id),
                            None,
                        )),
                    }
                },
            )
            .await
    }

    #[tool(
        description = "Always denied over MCP: pending executions are approved by a person in DBFlux (Pending Approvals), never by an MCP client"
    )]
    async fn approve_execution(
        &self,
        Parameters(params): Parameters<ApproveExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
        let approver_actor_id = state.client_id.clone();
        let pending_id = params
            .pending_id
            .parse::<Uuid>()
            .map_err(|_| ErrorData::invalid_params("Invalid pending_id format", None))?;

        self.governance
            .authorize_and_execute(
                "approve_execution",
                None,
                ExecutionClassification::Admin,
                move || async move {
                    let replay_plan = {
                        let runtime = state.runtime.read().await;
                        runtime
                            .approval_service()
                            .list_pending()
                            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?
                            .into_iter()
                            .find(|pending| pending.id == pending_id)
                            .map(|pending| pending.plan)
                            .ok_or_else(|| {
                                ErrorData::invalid_params(
                                    format!("Pending execution not found: {}", pending_id),
                                    None,
                                )
                            })?
                    };

                    {
                        let mut runtime = state.runtime.write().await;
                        runtime
                            .approve_pending_execution_with_origin_mut(
                                &pending_id.to_string(),
                                &approver_actor_id,
                                dbflux_core::observability::EventOrigin::mcp(),
                            )
                            .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?
                    };

                    // Return the approved plan for the caller to execute
                    let tool_id = replay_plan.tool_id.clone();
                    let connection_id = replay_plan.connection_id.clone();
                    let payload = replay_plan.payload.clone();

                    let response = serde_json::json!({
                        "approved": true,
                        "pending_id": pending_id.to_string(),
                        "status": "approved",
                        "replay_plan": {
                            "tool_id": tool_id,
                            "connection_id": connection_id,
                            "params": payload
                        },
                        "message": format!(
                            "Execution approved. Call tool '{}' with the provided params to execute.",
                            replay_plan.tool_id
                        )
                    });

                    Ok(CallToolResult::success(vec![to_json_content(&response)?]))
                },
            )
            .await
    }

    #[tool(
        description = "Always denied over MCP: pending executions are rejected by a person in DBFlux (Pending Approvals), never by an MCP client"
    )]
    async fn reject_execution(
        &self,
        Parameters(params): Parameters<RejectExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
        let rejection_reason = effective_rejection_reason(params.reason.as_deref()).to_string();
        let pending_id = params
            .pending_id
            .parse::<Uuid>()
            .map_err(|_| ErrorData::invalid_params("Invalid pending_id format", None))?;

        self.governance
            .authorize_and_execute(
                "reject_execution",
                None,
                ExecutionClassification::Admin,
                move || async move {
                    {
                        let mut runtime = state.runtime.write().await;
                        runtime
                            .reject_pending_execution_with_origin_mut(
                                &pending_id.to_string(),
                                &state.client_id,
                                Some(rejection_reason.as_str()),
                                dbflux_core::observability::EventOrigin::mcp(),
                            )
                            .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?;
                    }

                    let response = serde_json::json!({
                        "rejected": true,
                        "pending_id": pending_id.to_string(),
                        "reason": rejection_reason
                    });

                    Ok(CallToolResult::success(vec![to_json_content(&response)?]))
                },
            )
            .await
    }

    fn request_execution_classification(tool_id: &str) -> Result<ExecutionClassification, String> {
        let target_classification = Self::classify_tool(tool_id)
            .ok_or_else(|| format!("Unsupported tool for approval request: {}", tool_id))?;

        Ok(ExecutionClassification::Admin.max(target_classification))
    }

    /// Classify a tool by its ID to determine its execution classification.
    fn classify_tool(tool_id: &str) -> Option<ExecutionClassification> {
        match tool_id {
            // Metadata operations
            "list_connections"
            | "connect"
            | "disconnect"
            | "list_databases"
            | "list_schemas"
            | "list_tables"
            | "list_collections"
            | "describe_object"
            | "get_connection_info"
            | "list_scripts" => Some(ExecutionClassification::Metadata),

            // Read operations
            "select_data" | "count_records" | "aggregate_data" | "explain_query"
            | "preview_mutation" | "get_script" | "query_audit_logs" | "get_audit_entry"
            | "export_audit_logs" => Some(ExecutionClassification::Read),

            // Write operations
            "insert_record" | "update_records" | "upsert_record" | "create_script"
            | "update_script" => Some(ExecutionClassification::Write),

            // Destructive operations
            "delete_records" | "truncate_table" => Some(ExecutionClassification::Destructive),

            // Admin operations
            "create_table" | "create_index" | "create_type" | "delete_script" => {
                Some(ExecutionClassification::Admin)
            }

            // Dynamic or strongly destructive operations default to the stricter class.
            "alter_table" | "execute_script" | "drop_table" | "drop_database" | "drop_index" => {
                Some(ExecutionClassification::AdminDestructive)
            }

            _ => None,
        }
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// What `get_pending_execution` returns for `execution`: the entry itself
/// while it waits for a decision, a rejection payload carrying the person's
/// reason once rejected, and `None` when it was approved, used or expired.
fn pending_execution_status_response(
    execution: &PendingExecution,
    now_ms: i64,
) -> Option<serde_json::Value> {
    match execution.status {
        PendingStatus::Pending if execution.expires_at.is_none_or(|expires| expires > now_ms) => {
            serde_json::to_value(execution).ok()
        }
        PendingStatus::Rejected => Some(serde_json::json!({
            "pending_id": execution.id.to_string(),
            "status": "rejected",
            "tool_id": execution.plan.tool_id,
            "connection_id": execution.plan.connection_id,
            "reason": execution.rejection_reason,
            "message": "A person rejected this execution in DBFlux. It will not run. \
                        Repeating the identical call queues a new request.",
        })),
        _ => None,
    }
}

fn effective_rejection_reason(reason: Option<&str>) -> &str {
    reason.unwrap_or(DEFAULT_REJECTION_REASON)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_REJECTION_REASON, DbFluxServer, effective_rejection_reason,
        pending_execution_status_response,
    };
    use dbflux_approval::store::{ExecutionPlan, PendingExecution, PendingStatus};
    use dbflux_policy::ExecutionClassification;

    #[test]
    fn classify_tool_keeps_stricter_requested_execution_levels() {
        assert_eq!(
            DbFluxServer::classify_tool("drop_table"),
            Some(ExecutionClassification::AdminDestructive)
        );
        assert_eq!(
            DbFluxServer::classify_tool("select_data"),
            Some(ExecutionClassification::Read)
        );
        assert_eq!(
            DbFluxServer::classify_tool("alter_table"),
            Some(ExecutionClassification::AdminDestructive)
        );
        assert_eq!(DbFluxServer::classify_tool("request_execution"), None);
    }

    #[test]
    fn request_execution_classification_is_at_least_admin() {
        assert_eq!(
            DbFluxServer::request_execution_classification("insert_record"),
            Ok(ExecutionClassification::Admin)
        );
        assert_eq!(
            DbFluxServer::request_execution_classification("drop_table"),
            Ok(ExecutionClassification::AdminDestructive)
        );
    }

    #[test]
    fn request_execution_rejects_unsupported_governance_tools() {
        assert!(DbFluxServer::request_execution_classification("request_execution").is_err());
        assert!(DbFluxServer::request_execution_classification("approve_execution").is_err());
        assert!(DbFluxServer::request_execution_classification("unknown_tool").is_err());
    }

    #[test]
    fn request_execution_promotes_read_only_tools_to_admin_review() {
        assert_eq!(
            DbFluxServer::request_execution_classification("select_data"),
            Ok(ExecutionClassification::Admin)
        );
    }

    #[test]
    fn reject_execution_uses_runtime_default_reason_when_missing() {
        assert_eq!(effective_rejection_reason(None), DEFAULT_REJECTION_REASON);
    }

    fn execution(status: PendingStatus, rejection_reason: Option<&str>) -> PendingExecution {
        PendingExecution {
            id: uuid::Uuid::new_v4(),
            status,
            plan: ExecutionPlan {
                connection_id: "conn-a".to_string(),
                actor_id: "agent-a".to_string(),
                tool_id: "delete_records".to_string(),
                classification: ExecutionClassification::Destructive,
                payload: serde_json::json!({ "table": "items" }),
            },
            created_at: 1_000,
            expires_at: Some(10_000),
            rejection_reason: rejection_reason.map(str::to_string),
        }
    }

    #[test]
    fn status_response_returns_the_rejection_reason_to_the_agent() {
        let rejected = execution(PendingStatus::Rejected, Some("wrong table"));

        let response = pending_execution_status_response(&rejected, 2_000)
            .expect("a rejected execution is reported");

        assert_eq!(response["status"], serde_json::json!("rejected"));
        assert_eq!(response["reason"], serde_json::json!("wrong table"));
        assert_eq!(
            response["pending_id"],
            serde_json::json!(rejected.id.to_string())
        );
    }

    #[test]
    fn status_response_keeps_pending_and_hides_resolved_or_expired_entries() {
        let pending = execution(PendingStatus::Pending, None);
        let response = pending_execution_status_response(&pending, 2_000)
            .expect("a pending execution is reported");
        assert_eq!(response["status"], serde_json::json!("pending"));

        assert!(pending_execution_status_response(&pending, 20_000).is_none());
        assert!(
            pending_execution_status_response(&execution(PendingStatus::Approved, None), 2_000)
                .is_none()
        );
        assert!(
            pending_execution_status_response(&execution(PendingStatus::Consumed, None), 2_000)
                .is_none()
        );
    }

    #[test]
    fn test_drop_index_classification_admin_destructive() {
        assert_eq!(
            DbFluxServer::classify_tool("drop_index"),
            Some(ExecutionClassification::AdminDestructive),
            "drop_index must be classified AdminDestructive, not Admin"
        );
        assert_eq!(
            DbFluxServer::classify_tool("create_index"),
            Some(ExecutionClassification::Admin),
            "create_index classification must remain Admin"
        );
    }

    #[test]
    fn test_drop_index_denied_by_admin_destructive_policy() {
        use dbflux_policy::{
            ConnectionPolicyAssignment, PolicyBindingScope, PolicyDecision, PolicyDecisionReason,
            PolicyEngine, PolicyEvaluationRequest, ToolPolicy,
        };

        let engine = PolicyEngine::new(
            vec![ConnectionPolicyAssignment {
                actor_id: "actor".to_string(),
                scope: PolicyBindingScope {
                    connection_id: "conn".to_string(),
                },
                role_ids: vec![],
                policy_ids: vec!["admin-only".to_string()],
            }],
            vec![],
            vec![ToolPolicy {
                id: "admin-only".to_string(),
                allowed_tools: vec!["drop_index".to_string()],
                allowed_classes: vec![ExecutionClassification::Admin],
                approval_classes: Vec::new(),
            }],
        );

        let classification = DbFluxServer::classify_tool("drop_index")
            .expect("drop_index must have a classification");

        let decision = engine
            .evaluate(&PolicyEvaluationRequest {
                actor_id: "actor".to_string(),
                connection_id: "conn".to_string(),
                tool_id: "drop_index".to_string(),
                classification,
            })
            .expect("evaluation must not error");

        assert_eq!(
            decision,
            PolicyDecision::Deny(PolicyDecisionReason::ClassificationDenied),
            "an Admin-only policy must not authorize drop_index when it is classified AdminDestructive"
        );
    }
}
