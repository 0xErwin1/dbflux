use crate::governance_service::{PolicyRoleDto, ToolPolicyDto};

/// ID prefix that marks built-in policies and roles.
/// Any item whose ID starts with this prefix is considered immutable.
pub const BUILTIN_ID_PREFIX: &str = "builtin/";

pub fn is_builtin(id: &str) -> bool {
    id.starts_with(BUILTIN_ID_PREFIX)
}

/// Returns the three built-in policies.
/// These are prepended to user-defined policies in all list operations.
pub fn builtin_policies() -> Vec<ToolPolicyDto> {
    vec![
        ToolPolicyDto {
            id: "builtin/read-only".to_string(),
            allowed_tools: vec![
                "list_connections".to_string(),
                "get_connection".to_string(),
                "get_connection_metadata".to_string(),
                "list_databases".to_string(),
                "list_schemas".to_string(),
                "list_tables".to_string(),
                "list_collections".to_string(),
                "describe_object".to_string(),
                "read_query".to_string(),
                "explain_query".to_string(),
                "list_scripts".to_string(),
                "get_script".to_string(),
                "query_audit_logs".to_string(),
                "get_audit_entry".to_string(),
            ],
            allowed_classes: vec!["metadata".to_string(), "read".to_string()],
        },
        ToolPolicyDto {
            id: "builtin/write".to_string(),
            allowed_tools: vec![
                "list_connections".to_string(),
                "get_connection".to_string(),
                "get_connection_metadata".to_string(),
                "list_databases".to_string(),
                "list_schemas".to_string(),
                "list_tables".to_string(),
                "list_collections".to_string(),
                "describe_object".to_string(),
                "read_query".to_string(),
                "explain_query".to_string(),
                "preview_mutation".to_string(),
                "list_scripts".to_string(),
                "get_script".to_string(),
                "create_script".to_string(),
                "update_script".to_string(),
                "run_script".to_string(),
                "request_execution".to_string(),
                "list_pending_executions".to_string(),
                "get_pending_execution".to_string(),
                "query_audit_logs".to_string(),
                "get_audit_entry".to_string(),
            ],
            allowed_classes: vec![
                "metadata".to_string(),
                "read".to_string(),
                "write".to_string(),
            ],
        },
        ToolPolicyDto {
            id: "builtin/admin".to_string(),
            allowed_tools: vec![
                "list_connections".to_string(),
                "get_connection".to_string(),
                "get_connection_metadata".to_string(),
                "list_databases".to_string(),
                "list_schemas".to_string(),
                "list_tables".to_string(),
                "list_collections".to_string(),
                "describe_object".to_string(),
                "read_query".to_string(),
                "explain_query".to_string(),
                "preview_mutation".to_string(),
                "list_scripts".to_string(),
                "get_script".to_string(),
                "create_script".to_string(),
                "update_script".to_string(),
                "delete_script".to_string(),
                "run_script".to_string(),
                "request_execution".to_string(),
                "list_pending_executions".to_string(),
                "get_pending_execution".to_string(),
                "approve_execution".to_string(),
                "reject_execution".to_string(),
                "query_audit_logs".to_string(),
                "get_audit_entry".to_string(),
                "export_audit_logs".to_string(),
            ],
            allowed_classes: vec![
                "metadata".to_string(),
                "read".to_string(),
                "write".to_string(),
                "destructive".to_string(),
                "admin".to_string(),
            ],
        },
    ]
}

/// Returns the three built-in roles, each mapped to its corresponding built-in policy.
/// Returns a human-readable display name for a built-in ID, or `None` if the ID is not a
/// known built-in. This is the single source of truth for built-in display names.
pub fn builtin_display_name(id: &str) -> Option<&'static str> {
    match id {
        "builtin/read-only" => Some("Read Only"),
        "builtin/write" => Some("Write"),
        "builtin/admin" => Some("Admin"),
        _ => None,
    }
}

pub fn builtin_roles() -> Vec<PolicyRoleDto> {
    vec![
        PolicyRoleDto {
            id: "builtin/read-only".to_string(),
            policy_ids: vec!["builtin/read-only".to_string()],
        },
        PolicyRoleDto {
            id: "builtin/write".to_string(),
            policy_ids: vec!["builtin/write".to_string()],
        },
        PolicyRoleDto {
            id: "builtin/admin".to_string(),
            policy_ids: vec!["builtin/admin".to_string()],
        },
    ]
}
