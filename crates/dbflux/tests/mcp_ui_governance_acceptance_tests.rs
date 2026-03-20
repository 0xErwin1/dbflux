use std::fs;
use std::path::PathBuf;

fn read_workspace_file(relative_path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(root.join(relative_path)).expect("file should be readable")
}

#[test]
fn ui_contains_trusted_client_and_connection_policy_controls() {
    let mcp_settings = read_workspace_file("src/ui/windows/settings/mcp_section.rs");
    let connection_form = read_workspace_file("src/ui/windows/connection_manager/form.rs");
    let connection_tabs = read_workspace_file("src/ui/windows/connection_manager/render_tabs.rs");

    assert!(mcp_settings.contains("mcp-client-save"));
    assert!(mcp_settings.contains("mcp-client-toggle-active"));

    assert!(connection_form.contains("save_mcp_connection_policy_assignment"));
    assert!(connection_form.contains("ConnectionPolicyUpdated"));
    assert!(connection_tabs.contains("Scope/policy assignment preview"));
    assert!(connection_tabs.contains("Enable MCP for this connection"));
}

#[test]
fn ui_contains_approval_and_audit_controls_with_workspace_wiring() {
    let governance_view = read_workspace_file("src/ui/document/governance.rs");
    let workspace_actions = read_workspace_file("src/ui/views/workspace/actions.rs");
    let workspace_dispatch = read_workspace_file("src/ui/views/workspace/dispatch.rs");
    let workspace_mod = read_workspace_file("src/ui/views/workspace/mod.rs");

    assert!(governance_view.contains("mcp-approval-approve"));
    assert!(governance_view.contains("mcp-approval-reject"));
    assert!(governance_view.contains("mcp-audit-export-csv"));
    assert!(governance_view.contains("mcp-audit-export-json"));

    assert!(workspace_actions.contains("open_mcp_approvals"));
    assert!(workspace_actions.contains("open_mcp_audit"));
    assert!(workspace_dispatch.contains("Command::OpenMcpApprovals"));
    assert!(workspace_dispatch.contains("Command::OpenMcpAudit"));
    assert!(workspace_dispatch.contains("Command::RefreshMcpGovernance"));
    assert!(workspace_mod.contains("McpRuntimeEventRaised"));
}
