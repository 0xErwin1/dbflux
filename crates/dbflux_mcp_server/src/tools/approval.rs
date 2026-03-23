//! Approval tools for MCP server.
//!
//! Provides tools for managing execution approvals:
//! - `request_execution`: Request approval for a pending operation
//! - `list_pending_executions`: List all pending executions
//! - `get_pending_execution`: Get details of a specific pending execution
//! - `approve_execution`: Approve and execute a pending operation
//! - `reject_execution`: Reject a pending operation

use dbflux_approval::store::ExecutionPlan;
use dbflux_policy::ExecutionClassification;
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::server::DbFluxServer;

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

impl DbFluxServer {
    #[tool(description = "Request approval for a potentially destructive operation")]
    async fn request_execution(
        &self,
        Parameters(params): Parameters<RequestExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
        let client_id = state.client_id.clone();

        // Classify the operation based on the tool_id
        let classification = Self::classify_tool(&params.tool_id);

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
                ExecutionClassification::Write,
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
                        let approval_service = runtime.approval_service_mut();
                        approval_service.request_execution(&plan)
                    };

                    let response = serde_json::json!({
                        "pending_id": pending.id.to_string(),
                        "status": "pending",
                        "classification": format!("{:?}", classification),
                        "message": "Execution request created. Awaiting approval."
                    });

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&response).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "List pending executions awaiting approval")]
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
                        approval_service.list_pending()
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

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&response).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Get details of a specific pending execution")]
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
                    let pending = {
                        let runtime = state.runtime.read().await;
                        let approval_service = runtime.approval_service();
                        approval_service
                            .list_pending()
                            .into_iter()
                            .find(|p| p.id == pending_id)
                    };

                    match pending {
                        Some(pending) => Ok(CallToolResult::success(vec![Content::text(
                            serde_json::to_string_pretty(&pending).unwrap(),
                        )])),
                        None => Err(ErrorData::invalid_params(
                            format!("Pending execution not found: {}", pending_id),
                            None,
                        )),
                    }
                },
            )
            .await
    }

    #[tool(description = "Approve a pending operation (returns replay instructions)")]
    async fn approve_execution(
        &self,
        Parameters(params): Parameters<ApproveExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
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
                    // Approve the execution
                    let approved = {
                        let mut runtime = state.runtime.write().await;
                        let approval_service = runtime.approval_service_mut();
                        approval_service
                            .approve(pending_id)
                            .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?
                    };

                    // Return the approved plan for the caller to execute
                    let response = serde_json::json!({
                        "approved": true,
                        "pending_id": pending_id.to_string(),
                        "status": "approved",
                        "replay_plan": {
                            "tool_id": approved.replay_plan.tool_id,
                            "connection_id": approved.replay_plan.connection_id,
                            "params": approved.replay_plan.payload
                        },
                        "message": format!(
                            "Execution approved. Call tool '{}' with the provided params to execute.",
                            approved.replay_plan.tool_id
                        )
                    });

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&response).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Reject a pending execution")]
    async fn reject_execution(
        &self,
        Parameters(params): Parameters<RejectExecutionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
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
                    let _rejected = {
                        let mut runtime = state.runtime.write().await;
                        let approval_service = runtime.approval_service_mut();
                        approval_service
                            .reject(pending_id)
                            .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?
                    };

                    let response = serde_json::json!({
                        "rejected": true,
                        "pending_id": pending_id.to_string(),
                        "reason": params.reason.unwrap_or_else(|| "No reason provided".to_string())
                    });

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&response).unwrap(),
                    )]))
                },
            )
            .await
    }

    /// Classify a tool by its ID to determine its execution classification
    fn classify_tool(tool_id: &str) -> ExecutionClassification {
        match tool_id {
            // Metadata operations
            "list_connections" | "list_databases" | "list_schemas" | "list_tables"
            | "list_collections" | "describe_object" | "get_connection_info" => {
                ExecutionClassification::Metadata
            }

            // Read operations
            "select_data" | "count_records" | "aggregate_data" => ExecutionClassification::Read,

            // Write operations
            "insert_record" | "update_records" | "upsert_record" => ExecutionClassification::Write,

            // Destructive operations
            "delete_records" | "truncate_table" | "drop_table" | "drop_database"
            | "drop_index" => ExecutionClassification::Destructive,

            // Admin operations
            "create_table" | "alter_table" | "create_index" | "create_type" => {
                ExecutionClassification::Admin
            }

            // Default to Admin for unknown tools (safest classification)
            _ => ExecutionClassification::Admin,
        }
    }
}
