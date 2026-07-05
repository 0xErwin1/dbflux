use super::*;

impl Workspace {
    pub(super) fn dispatch_audit(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::OpenAuditViewer => {
                self.open_audit_viewer(window, cx);
                Some(true)
            }
            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals => {
                self.open_mcp_approvals(window, cx);
                Some(true)
            }
            #[cfg(feature = "mcp")]
            Command::RefreshMcpGovernance => {
                self.refresh_mcp_governance(window, cx);
                Some(true)
            }
            _ => None,
        }
    }
}
