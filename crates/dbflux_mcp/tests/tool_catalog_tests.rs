use dbflux_mcp::{
    CANONICAL_V1_TOOLS, DEFERRED_TOOL_V1_ESTIMATE_QUERY_COST,
    DEFERRED_TOOL_V1_GET_EXECUTION_STATUS, ToolCatalogError, is_canonical_v1_tool,
    validate_v1_tool,
};

#[test]
fn canonical_surface_includes_expected_v1_tools() {
    for tool in [
        "list_connections",
        "describe_object",
        "read_query",
        "request_execution",
        "export_audit_logs",
    ] {
        assert!(
            is_canonical_v1_tool(tool),
            "expected canonical tool missing: {tool}"
        );
    }

    assert!(
        !CANONICAL_V1_TOOLS
            .iter()
            .any(|tool| tool == &DEFERRED_TOOL_V1_ESTIMATE_QUERY_COST)
    );
    assert!(
        !CANONICAL_V1_TOOLS
            .iter()
            .any(|tool| tool == &DEFERRED_TOOL_V1_GET_EXECUTION_STATUS)
    );
}

#[test]
fn deferred_tool_is_rejected_with_structured_error() {
    let result = validate_v1_tool(DEFERRED_TOOL_V1_ESTIMATE_QUERY_COST);

    assert_eq!(
        result,
        Err(ToolCatalogError::DeferredInV1 {
            tool: DEFERRED_TOOL_V1_ESTIMATE_QUERY_COST.to_string()
        })
    );
}

#[test]
fn unknown_tool_is_rejected() {
    let result = validate_v1_tool("describe_table");

    assert_eq!(
        result,
        Err(ToolCatalogError::UnknownTool {
            tool: "describe_table".to_string()
        })
    );
}
