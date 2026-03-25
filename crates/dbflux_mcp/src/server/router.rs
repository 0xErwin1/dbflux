use crate::tool_catalog::{validate_v1_tool, ToolCatalogError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteTarget {
    Discovery,
    Schema,
    Query,
    Scripts,
    Approval,
    Audit,
}

pub fn route_tool(tool_id: &str) -> Result<RouteTarget, ToolCatalogError> {
    validate_v1_tool(tool_id)?;

    let target = match tool_id {
        "list_connections" | "get_connection" | "get_connection_metadata" => RouteTarget::Discovery,
        "list_databases" | "list_schemas" | "list_tables" | "list_collections"
        | "describe_object" => RouteTarget::Schema,
        "select_data" | "explain_query" | "preview_mutation" => RouteTarget::Query,
        "list_scripts" | "get_script" | "create_script" | "update_script" | "delete_script"
        | "run_script" => RouteTarget::Scripts,
        "request_execution"
        | "list_pending_executions"
        | "get_pending_execution"
        | "approve_execution"
        | "reject_execution" => RouteTarget::Approval,
        "query_audit_logs" | "get_audit_entry" | "export_audit_logs" => RouteTarget::Audit,
        _ => {
            return Err(ToolCatalogError::UnknownTool {
                tool: tool_id.to_string(),
            });
        }
    };

    Ok(target)
}
