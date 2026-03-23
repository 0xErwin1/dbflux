use thiserror::Error;

pub const DEFERRED_TOOL_V1_ESTIMATE_QUERY_COST: &str = "estimate_query_cost";
pub const DEFERRED_TOOL_V1_GET_EXECUTION_STATUS: &str = "get_execution_status";

pub const DEFERRED_TOOL_IDS: &[&str] = &[
    DEFERRED_TOOL_V1_ESTIMATE_QUERY_COST,
    DEFERRED_TOOL_V1_GET_EXECUTION_STATUS,
];

pub const DEFERRED_TOOL_REJECTION_REASON: &str = "tool not available in v1";

pub const CANONICAL_V1_TOOLS: &[&str] = &[
    // Connection management
    "list_connections",
    "connect",
    "disconnect",
    "get_connection_info",
    // Schema introspection
    "list_databases",
    "list_schemas",
    "list_tables",
    "list_collections",
    "describe_object",
    // Read operations
    "select_data",
    "count_records",
    "aggregate_data",
    "explain_query",
    "preview_mutation",
    // Write operations
    "insert_record",
    "update_records",
    "upsert_record",
    // Destructive operations
    "delete_records",
    "truncate_table",
    "drop_table",
    "drop_database",
    // DDL operations
    "create_table",
    "alter_table",
    "create_index",
    "drop_index",
    "create_type",
    // Script management
    "list_scripts",
    "get_script",
    "create_script",
    "update_script",
    "delete_script",
    "execute_script",
    // Approval workflow
    "request_execution",
    "list_pending_executions",
    "get_pending_execution",
    "approve_execution",
    "reject_execution",
    // Audit logs
    "query_audit_logs",
    "get_audit_entry",
    "export_audit_logs",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ToolCatalogError {
    #[error("tool not available in v1: {tool}")]
    DeferredInV1 { tool: String },
    #[error("unknown tool: {tool}")]
    UnknownTool { tool: String },
}

pub fn is_canonical_v1_tool(tool_id: &str) -> bool {
    CANONICAL_V1_TOOLS
        .iter()
        .any(|candidate| candidate == &tool_id)
}

pub fn is_deferred_v1_tool(tool_id: &str) -> bool {
    DEFERRED_TOOL_IDS
        .iter()
        .any(|candidate| candidate == &tool_id)
}

pub fn validate_v1_tool(tool_id: &str) -> Result<(), ToolCatalogError> {
    if is_canonical_v1_tool(tool_id) {
        return Ok(());
    }

    if is_deferred_v1_tool(tool_id) {
        return Err(ToolCatalogError::DeferredInV1 {
            tool: tool_id.to_string(),
        });
    }

    Err(ToolCatalogError::UnknownTool {
        tool: tool_id.to_string(),
    })
}
