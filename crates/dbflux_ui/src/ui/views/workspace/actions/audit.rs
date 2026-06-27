use super::*;

impl Workspace {
    /// Opens the global audit viewer as a document tab.
    pub(in crate::ui::views::workspace) fn open_audit_viewer(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::AuditDocument;
        use crate::ui::document::DocumentKey;

        self.active_governance_panel = None;

        // Check if an audit document is already open.
        let existing_id = self
            .tab_manager
            .read(cx)
            .find_by_key(&DocumentKey::Audit, cx);

        if let Some(id) = existing_id {
            // Reset the category filter and focus the existing audit tab.
            // Both operations are done in a single update to avoid multiple borrows.
            self.tab_manager.update(cx, |mgr, cx| {
                if let Some(tab) = mgr.documents().iter().find(|t| t.id() == id) {
                    let pane = tab.as_pane();
                    if let Some(f) = &pane.set_category_filter {
                        f(None, cx);
                    }
                }
                mgr.activate(id, cx);
            });

            self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
            Toast::info("Focusing existing audit viewer")
                .meta_right(now_hms())
                .push(cx);
            return;
        }

        // Create a new audit document. Pre-open the audit repo so failures can
        // be surfaced through report_error before entering cx.new.
        let audit_repo = match self.app_state.read(cx).storage_runtime().audit() {
            Ok(repo) => repo,
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Cannot open audit viewer: {e}"),
                    ),
                    cx,
                );
                return;
            }
        };
        let doc = cx.new(|cx| AuditDocument::new(audit_repo, self.app_state.clone(), window, cx));
        let pane = AuditDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
        Toast::info("Opened audit viewer")
            .meta_right(now_hms())
            .push(cx);
    }

    /// Opens (or focuses) the audit viewer pre-filtered by correlation id.
    ///
    /// When an audit tab is already open, the correlation filter is applied to
    /// the existing tab and it is brought to focus. When `correlation_id` is
    /// `None`, the audit viewer opens with the default user-error filter
    /// (`action = "user_error"`). When no tab is open and a specific id was
    /// provided, a new tab is created pre-filtered by that id.
    pub(in crate::ui::views::workspace) fn open_audit_viewer_with_correlation(
        &mut self,
        correlation_id: Option<uuid::Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::AuditDocument;
        use crate::ui::document::DocumentKey;

        let existing_id = self
            .tab_manager
            .read(cx)
            .find_by_key(&DocumentKey::Audit, cx);

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                if let Some(tab) = mgr.documents().iter().find(|t| t.id() == id) {
                    let pane = tab.as_pane();
                    if let Some(f) = &pane.set_correlation_filter {
                        f(correlation_id.map(|u| u.to_string()), cx);
                    }
                }
                mgr.activate(id, cx);
            });

            self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
            return;
        }

        match correlation_id {
            Some(id) => {
                let audit_repo = match self.app_state.read(cx).storage_runtime().audit() {
                    Ok(repo) => repo,
                    Err(e) => {
                        report_error(
                            UserFacingError::new(
                                ErrorKind::Storage,
                                format!("Cannot open audit viewer: {e}"),
                            ),
                            cx,
                        );
                        return;
                    }
                };
                let doc = cx.new(|cx| {
                    AuditDocument::new_with_correlation_id(
                        id,
                        audit_repo,
                        self.app_state.clone(),
                        window,
                        cx,
                    )
                });
                let pane = AuditDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
            None => {
                self.open_audit_viewer(window, cx);
                return;
            }
        }

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
    }

    #[cfg(feature = "mcp")]
    pub(in crate::ui::views::workspace) fn open_mcp_approvals(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mcp_approvals_view.update(cx, |view, cx| {
            view.refresh(cx);
        });

        self.active_governance_panel = Some(super::GovernancePanel::Approvals);
        Toast::info("Opened MCP approvals")
            .meta_right(now_hms())
            .push(cx);
    }

    #[cfg(feature = "mcp")]
    pub(in crate::ui::views::workspace) fn refresh_mcp_governance(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.persist_mcp_governance() {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Config,
                        format!("Failed to persist MCP governance: {e}"),
                    ),
                    cx,
                );
                return;
            }

            for event in state.drain_mcp_runtime_events() {
                cx.emit(crate::app::McpRuntimeEventRaised { event });
            }
        });

        Toast::info("MCP governance state persisted")
            .meta_right(now_hms())
            .push(cx);
    }
}
