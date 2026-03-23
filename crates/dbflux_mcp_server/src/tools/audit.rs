//! Audit log tools for MCP server.
//!
//! Provides tools for querying and exporting audit logs:
//! - `query_audit_logs`: Query audit logs with filters
//! - `get_audit_entry`: Get a specific audit entry by ID
//! - `export_audit_logs`: Export audit logs in CSV or JSON format

use dbflux_audit::export::AuditExportFormat;
use dbflux_audit::query::AuditQueryFilter;
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool,
};
use serde::Deserialize;

use crate::{
    helper::IntoErrorData,
    server::DbFluxServer,
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryAuditLogsParams {
    #[schemars(description = "Filter by actor ID (optional)")]
    pub actor_id: Option<String>,

    #[schemars(description = "Filter by tool ID (optional)")]
    pub tool_id: Option<String>,

    #[schemars(description = "Filter by decision (e.g., 'allow', 'deny', 'approved', 'rejected') (optional)")]
    pub decision: Option<String>,

    #[schemars(description = "Filter by start date in ISO8601 format (e.g., '2024-03-20T10:00:00Z') (optional)")]
    pub start_date: Option<String>,

    #[schemars(description = "Filter by end date in ISO8601 format (e.g., '2024-03-20T23:59:59Z') (optional)")]
    pub end_date: Option<String>,

    #[schemars(description = "Maximum number of entries to return (optional)")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetAuditEntryParams {
    #[schemars(description = "Audit entry ID")]
    pub id: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExportAuditLogsParams {
    #[schemars(description = "Filter by actor ID (optional)")]
    pub actor_id: Option<String>,

    #[schemars(description = "Filter by tool ID (optional)")]
    pub tool_id: Option<String>,

    #[schemars(description = "Filter by decision (e.g., 'allow', 'deny', 'approved', 'rejected') (optional)")]
    pub decision: Option<String>,

    #[schemars(description = "Filter by start date in ISO8601 format (e.g., '2024-03-20T10:00:00Z') (optional)")]
    pub start_date: Option<String>,

    #[schemars(description = "Filter by end date in ISO8601 format (e.g., '2024-03-20T23:59:59Z') (optional)")]
    pub end_date: Option<String>,

    #[schemars(description = "Maximum number of entries to return (optional)")]
    pub limit: Option<usize>,

    #[schemars(description = "Export format: 'csv' or 'json'")]
    pub format: String,
}

fn parse_iso8601_to_epoch_ms(date_str: &str) -> Result<i64, String> {
    use chrono::DateTime;

    DateTime::parse_from_rfc3339(date_str)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| format!("Invalid ISO8601 date '{}': {}", date_str, e))
}

fn build_audit_filter(
    actor_id: Option<String>,
    tool_id: Option<String>,
    decision: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    limit: Option<usize>,
) -> Result<AuditQueryFilter, String> {
    let start_epoch_ms = if let Some(ref date) = start_date {
        Some(parse_iso8601_to_epoch_ms(date)?)
    } else {
        None
    };

    let end_epoch_ms = if let Some(ref date) = end_date {
        Some(parse_iso8601_to_epoch_ms(date)?)
    } else {
        None
    };

    Ok(AuditQueryFilter {
        actor_id,
        tool_id,
        decision,
        start_epoch_ms,
        end_epoch_ms,
        limit,
    })
}

impl DbFluxServer {
    #[tool(description = "Query audit logs with optional filters (actor, tool, decision, date range, limit)")]
    async fn query_audit_logs(
        &self,
        Parameters(params): Parameters<QueryAuditLogsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let actor_id = params.actor_id;
        let tool_id = params.tool_id;
        let decision = params.decision;
        let start_date = params.start_date;
        let end_date = params.end_date;
        let limit = params.limit;

        let state = self.state.clone();

        self.governance
            .authorize_and_execute(
                "query_audit_logs",
                None,
                ExecutionClassification::Read,
                move || async move {
                    let filter = build_audit_filter(
                        actor_id,
                        tool_id,
                        decision,
                        start_date,
                        end_date,
                        limit,
                    )
                    .map_err(|e| e.into_error_data())?;

                    let runtime = state.runtime.read().await;
                    let audit_service = runtime.audit_service();

                    let events = audit_service
                        .query(&filter)
                        .map_err(|e| format!("Failed to query audit logs: {}", e))
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&events).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Get a specific audit entry by ID")]
    async fn get_audit_entry(
        &self,
        Parameters(params): Parameters<GetAuditEntryParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let id = params.id;

        let state = self.state.clone();

        self.governance
            .authorize_and_execute(
                "get_audit_entry",
                None,
                ExecutionClassification::Read,
                move || async move {
                    let runtime = state.runtime.read().await;
                    let audit_service = runtime.audit_service();

                    let event = audit_service
                        .get(id)
                        .map_err(|e| format!("Failed to get audit entry: {}", e))
                        .map_err(|e| e.into_error_data())?;

                    match event {
                        Some(entry) => Ok(CallToolResult::success(vec![Content::text(
                            serde_json::to_string_pretty(&entry).unwrap(),
                        )])),
                        None => Err(format!("Audit entry with ID {} not found", id).into_error_data()),
                    }
                },
            )
            .await
    }

    #[tool(description = "Export audit logs in CSV or JSON format with optional filters")]
    async fn export_audit_logs(
        &self,
        Parameters(params): Parameters<ExportAuditLogsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let actor_id = params.actor_id;
        let tool_id = params.tool_id;
        let decision = params.decision;
        let start_date = params.start_date;
        let end_date = params.end_date;
        let limit = params.limit;
        let format = params.format;

        let state = self.state.clone();

        self.governance
            .authorize_and_execute(
                "export_audit_logs",
                None,
                ExecutionClassification::Admin,
                move || async move {
                    let filter = build_audit_filter(
                        actor_id,
                        tool_id,
                        decision,
                        start_date,
                        end_date,
                        limit,
                    )
                    .map_err(|e| e.into_error_data())?;

                    let export_format = match format.to_lowercase().as_str() {
                        "csv" => AuditExportFormat::Csv,
                        "json" => AuditExportFormat::Json,
                        _ => {
                            return Err(format!(
                                "Invalid export format '{}'. Must be 'csv' or 'json'",
                                format
                            )
                            .into_error_data())
                        }
                    };

                    let runtime = state.runtime.read().await;
                    let audit_service = runtime.audit_service();

                    let output = audit_service
                        .export(&filter, export_format)
                        .map_err(|e| format!("Failed to export audit logs: {}", e))
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(output)]))
                },
            )
            .await
    }
}
