use super::*;

impl Workspace {
    pub(super) fn dispatch_charts_dashboards(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::OpenSavedChart => {
                // Build a palette populated only with saved chart items,
                // then open the command palette so the user can fuzzy-search them.
                let chart_items = self.build_saved_chart_palette_items(cx);
                if chart_items.is_empty() {
                    Toast::warning("No saved charts for the current profile")
                        .meta_right(now_hms())
                        .push(cx);
                } else {
                    // Prepend the chart items before any other items so the
                    // palette opens showing only charts (the user searched "open chart").
                    self.command_palette.update(cx, |palette, cx| {
                        palette.open_with_items(chart_items, window, cx);
                    });
                }
                Some(true)
            }

            Command::ImportDashboard => {
                // Only available when the active connection has DASHBOARD_IMPORT.
                let has_capability = self
                    .app_state
                    .read(cx)
                    .active_connection()
                    .map(|conn| {
                        conn.connection
                            .metadata()
                            .capabilities
                            .contains(dbflux_core::DriverCapabilities::DASHBOARD_IMPORT)
                    })
                    .unwrap_or(false);

                if has_capability {
                    self.modal_import_dashboard.update(cx, |modal, cx| {
                        modal.open(window, cx);
                    });
                } else {
                    Toast::warning("The active connection does not support dashboard import.")
                        .meta_right(now_hms())
                        .push(cx);
                }
                Some(true)
            }

            Command::NewDashboard => {
                self.create_dashboard_from_palette(window, cx);
                Some(true)
            }

            _ => None,
        }
    }
}
