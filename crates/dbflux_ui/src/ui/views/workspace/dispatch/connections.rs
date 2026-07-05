use super::*;

impl Workspace {
    pub(super) fn dispatch_connections(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::OpenConnectionManager => {
                self.open_connection_manager(cx);
                Some(true)
            }
            Command::ExportConnections => {
                // Export is now per-connection: it is initiated from a
                // connection's three-dots menu, which carries the profile id.
                dbflux_ui_base::toast::Toast::info("Export a connection from its menu")
                    .body("Right-click a connection in the sidebar and choose Export.")
                    .push(cx);
                Some(true)
            }
            Command::Disconnect => {
                self.disconnect_active(window, cx);
                Some(true)
            }
            Command::RefreshSchema => {
                self.refresh_schema(window, cx);
                Some(true)
            }
            _ => None,
        }
    }
}
