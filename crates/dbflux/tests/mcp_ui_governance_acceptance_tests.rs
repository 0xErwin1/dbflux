use std::fs;
use std::path::PathBuf;

fn read_workspace_file(relative_path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(root.join(relative_path)).expect("file should be readable")
}

#[test]
fn ui_contains_trusted_client_and_connection_policy_controls() {
    let mcp_settings = read_workspace_file("../dbflux_ui/src/ui/windows/settings/mcp_section.rs");
    let connection_form =
        read_workspace_file("../dbflux_ui/src/ui/windows/connection_manager/form.rs");
    let connection_tabs =
        read_workspace_file("../dbflux_ui/src/ui/windows/connection_manager/render_tabs.rs");

    assert!(mcp_settings.contains("mcp-client-save"));
    assert!(mcp_settings.contains("mcp-client-toggle-active"));

    assert!(connection_form.contains("save_mcp_connection_policy_assignment"));
    assert!(connection_form.contains("ConnectionPolicyUpdated"));
    assert!(connection_tabs.contains("Scope/policy assignment preview"));
    assert!(connection_tabs.contains("Enable MCP for this connection"));
}

#[test]
fn ui_contains_approval_and_audit_controls_with_workspace_wiring() {
    let governance_view = read_workspace_file("../dbflux_ui/src/ui/document/governance.rs");
    let workspace_actions = read_workspace_file("../dbflux_ui/src/ui/views/workspace/actions.rs");
    let workspace_dispatch = read_workspace_file("../dbflux_ui/src/ui/views/workspace/dispatch.rs");
    let workspace_mod = read_workspace_file("../dbflux_ui/src/ui/views/workspace/mod.rs");

    assert!(governance_view.contains("mcp-approval-approve"));
    assert!(governance_view.contains("mcp-approval-reject"));
    // Audit is routed through the unified AuditDocument, not a separate MCP audit view
    assert!(!governance_view.contains("mcp-audit-export-csv"));
    assert!(!governance_view.contains("mcp-audit-export-json"));

    assert!(workspace_actions.contains("open_mcp_approvals"));
    // No separate MCP audit command — unified into OpenAuditViewer
    assert!(!workspace_actions.contains("open_mcp_audit"));
    assert!(workspace_dispatch.contains("Command::OpenMcpApprovals"));
    assert!(!workspace_dispatch.contains("Command::OpenMcpAudit"));
    assert!(workspace_dispatch.contains("Command::RefreshMcpGovernance"));
    assert!(workspace_mod.contains("McpRuntimeEventRaised"));
}

#[test]
fn audit_workspace_actions_retarget_existing_document_and_close_governance_overlay() {
    let audit_document = read_workspace_file("../dbflux_ui/src/ui/document/audit/mod.rs");
    let workspace_actions = read_workspace_file("../dbflux_ui/src/ui/views/workspace/actions.rs");

    assert!(audit_document.contains("pub fn set_category_filter"));
    assert!(audit_document.contains("doc.pending_initial_load = false;"));

    // Unified audit viewer clears MCP filter when opened generically
    assert!(workspace_actions.contains("doc.set_category_filter(None, cx);"));
    assert!(workspace_actions.contains("self.active_governance_panel = None;"));
}

#[test]
fn approvals_view_surfaces_failures_instead_of_swallowing_them() {
    let governance_view = read_workspace_file("../dbflux_ui/src/ui/document/governance.rs");

    assert!(!governance_view.contains("let _ = state.approve_mcp_pending_execution"));
    assert!(!governance_view.contains("let _ = state.reject_mcp_pending_execution"));
    assert!(governance_view.contains("self.status_message = Some(error);"));
}
