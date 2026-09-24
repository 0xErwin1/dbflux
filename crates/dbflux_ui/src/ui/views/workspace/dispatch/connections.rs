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
                dbflux_ui_base::toast::Toast::info(dbflux_i18n::t!(
                    "connections.toast.export_from_menu"
                ))
                .body(dbflux_i18n::t!("connections.toast.export_from_menu_body"))
                .push(cx);
                Some(true)
            }
            Command::Disconnect => {
                self.disconnect_active(window, cx);
                Some(true)
            }
            Command::RefreshSchema => {
                self.refresh_document_or_schema(window, cx);
                Some(true)
            }
            _ => None,
        }
    }

    /// Refreshes the active document when the document area has focus and the
    /// document implements a refresh; otherwise reloads the active
    /// connection's schema.
    ///
    /// The sidebar and background-tasks panel keep the connection-schema
    /// refresh, so the sidebar's refresh key is unchanged.
    fn refresh_document_or_schema(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_target == FocusTarget::Document {
            let handled = self.tab_manager.update(cx, |manager, cx| {
                manager.dispatch_active(Command::RefreshSchema, window, cx)
            });

            if handled {
                return;
            }
        }

        self.refresh_schema(window, cx);
    }
}
