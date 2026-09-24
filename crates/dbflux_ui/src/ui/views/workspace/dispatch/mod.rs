use super::*;

mod audit;
mod charts_dashboards;
mod connections;
mod documents;
mod navigation;
mod preflight;
mod query;
mod scripts;
mod settings;

use preflight::sidebar_tree_command_is_blocked_by_search_focus;

impl CommandDispatcher for Workspace {
    fn dispatch(&mut self, cmd: Command, window: &mut Window, cx: &mut Context<Self>) -> bool {
        // The active-query prompt is checked first: it can open over another
        // confirmation (quitting while one is visible) and is drawn on top.
        if matches!(cmd, Command::Execute | Command::Cancel)
            && self.modal_active_query.read(cx).is_visible()
        {
            self.modal_active_query.update(cx, |modal, cx| match cmd {
                Command::Execute => modal.confirm(cx),
                _ => modal.cancel(cx),
            });
            return true;
        }

        // A visible workspace confirmation captures the keyboard
        // (ContextId::ConfirmModal resolves only Enter/Escape), so it is
        // resolved before every other dispatch domain, including the sidebar
        // guards below. Without this, Enter fell through to
        // `dispatch_connections` and connected the profile the user was about
        // to delete, and Escape cleared the sidebar's pending delete without
        // closing the overlay.
        if matches!(cmd, Command::Execute | Command::Cancel)
            && route_confirm_modal_command(
                cmd,
                &self.modal_delete_connection,
                &self.modal_unsaved_changes,
                &self.modal_delete_dashboard,
                &self.modal_delete_saved_chart,
                cx,
            )
        {
            return true;
        }

        if self.sidebar.read(cx).has_child_picker_open() {
            match cmd {
                Command::SelectNext => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_next(cx));
                    return true;
                }
                Command::SelectPrev => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_prev(cx));
                    return true;
                }
                Command::SelectFirst => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_first(cx));
                    return true;
                }
                Command::SelectLast => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_last(cx));
                    return true;
                }
                Command::Execute => {
                    self.sidebar.update(cx, |s, cx| s.picker_execute(cx));
                    return true;
                }
                Command::FocusSearch => {
                    self.sidebar
                        .update(cx, |s, cx| s.picker_focus_search(window, cx));
                    return true;
                }
                Command::Cancel => {
                    if self.sidebar.read(cx).child_picker_filter_is_focused() {
                        // Pop focus back to the list so subsequent Cancel closes the modal.
                        self.sidebar
                            .update(cx, |s, cx| s.picker_focus_list(window, cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.close_child_picker(cx));
                    }
                    return true;
                }
                _ => return false,
            }
        }

        #[cfg(feature = "mcp")]
        if cmd == Command::Cancel && self.active_governance_panel.is_some() {
            self.close_governance_panel(window, cx);
            return true;
        }

        if self.focus_target == FocusTarget::Sidebar
            && self.sidebar.read(cx).search_input_is_focused(window, cx)
            && sidebar_tree_command_is_blocked_by_search_focus(cmd)
        {
            return false;
        }

        // When context menu is open, only allow menu-related commands
        if self.focus_target == FocusTarget::Sidebar
            && self.sidebar.read(cx).has_context_menu_open()
        {
            match cmd {
                Command::SelectNext
                | Command::SelectPrev
                | Command::SelectFirst
                | Command::SelectLast
                | Command::Execute
                | Command::ColumnLeft
                | Command::ColumnRight
                | Command::Cancel
                | Command::NewQueryTab => {}
                _ => return true,
            }
        }

        if cmd == Command::Cancel {
            return self.handle_cancel(window, cx);
        }

        if let Some(result) = self.dispatch_connections(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_settings(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_audit(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_scripts(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_charts_dashboards(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_query(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_documents(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_navigation(cmd, window, cx) {
            return result;
        }

        unreachable!("Command::{:?} not handled by any dispatch domain", cmd)
    }
}

impl Workspace {
    fn handle_cancel(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.command_palette.read(cx).is_visible() {
            self.command_palette.update(cx, |p, cx| p.hide(cx));
            self.set_focus(self.focus_target, window, cx);
            return true;
        }

        // Cancel delete confirmation modal
        if self.sidebar.read(cx).has_delete_modal() {
            self.sidebar.update(cx, |s, cx| s.cancel_modal_delete(cx));
            return true;
        }

        // Cancel pending delete (keyboard x)
        if self.sidebar.read(cx).has_pending_delete() {
            self.sidebar.update(cx, |s, cx| s.cancel_pending_delete(cx));
            return true;
        }

        if self.sidebar.read(cx).has_context_menu_open() {
            self.sidebar.update(cx, |s, cx| s.close_context_menu(cx));
            return true;
        }

        // Clear multi-selection in sidebar
        if self.sidebar.read(cx).has_multi_selection() {
            self.sidebar.update(cx, |s, cx| s.clear_selection(cx));
            return true;
        }

        // Route Cancel to active document (handles modals, edit modes, etc.).
        if self.tab_manager.update(cx, |mgr, cx| {
            mgr.dispatch_active(Command::Cancel, window, cx)
        }) {
            return true;
        }

        // Workspace-level inspector close fallback.
        // Only fires after the document has declined Cancel.
        if self.workspace_inspector.read(cx).is_open() {
            self.workspace_inspector.update(cx, |insp, cx| {
                insp.close(cx);
            });
            return true;
        }

        // Always focus workspace to blur any input and enable keyboard navigation
        self.focus_handle.focus(window, cx);
        true
    }
}

/// Routes `Command::Execute`/`Command::Cancel` to the visible workspace
/// confirmation modal, resolving and closing it through the same outcome
/// handler its buttons use. Returns `true` when a visible modal consumed
/// the command.
pub(crate) fn route_confirm_modal_command(
    cmd: Command,
    delete_connection: &Entity<crate::ui::overlays::modals::ModalDeleteConnection>,
    unsaved_changes: &Entity<crate::ui::overlays::modals::ModalUnsavedChanges>,
    delete_dashboard: &Entity<ModalDeleteDashboardConfirm>,
    delete_saved_chart: &Entity<ModalDeleteSavedChartConfirm>,
    cx: &mut App,
) -> bool {
    if delete_connection.read(cx).is_visible() {
        delete_connection.update(cx, |modal, cx| match cmd {
            Command::Execute => modal.confirm(cx),
            _ => modal.cancel(cx),
        });
        return true;
    }

    if unsaved_changes.read(cx).is_visible() {
        unsaved_changes.update(cx, |modal, cx| match cmd {
            Command::Execute => modal.confirm(cx),
            _ => modal.cancel(cx),
        });
        return true;
    }

    if delete_dashboard.read(cx).is_visible() {
        delete_dashboard.update(cx, |modal, cx| match cmd {
            Command::Execute => modal.confirm(cx),
            _ => modal.cancel(cx),
        });
        return true;
    }

    if delete_saved_chart.read(cx).is_visible() {
        delete_saved_chart.update(cx, |modal, cx| match cmd {
            Command::Execute => modal.confirm(cx),
            _ => modal.cancel(cx),
        });
        return true;
    }

    false
}

#[cfg(test)]
mod confirm_modal_routing_tests {
    // No `use super::*` here: combining the parent glob with `#[gpui::test]`
    // sends the gpui_macros expansion into unbounded recursion (an empty test
    // body crashes rustc). Explicit imports sidestep it.
    use super::route_confirm_modal_command;
    use crate::keymap::Command;
    use crate::ui::overlays::modals::{
        CloseAction, DeleteConnectionOutcome, DeleteConnectionRequest, DirtySummaryEntry,
        ModalDeleteConnection, ModalUnsavedChanges, UnsavedChangesOutcome, UnsavedChangesRequest,
    };
    use dbflux_core::document_id::DocumentId;
    use dbflux_ui_base::modals::{
        DeleteDashboardOutcome, DeleteDashboardRequest, DeleteSavedChartOutcome,
        DeleteSavedChartRequest, ModalDeleteDashboardConfirm, ModalDeleteSavedChartConfirm,
    };
    use gpui::AppContext;
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    /// Regression: with the `confirm_modal` keymap layer resolving
    /// Enter/Escape while a workspace confirmation is visible, both chords
    /// must resolve and close the modal through its outcome handlers —
    /// Enter must not fall through to `dispatch_connections` (it connected
    /// the profile the user was deleting), and Escape must not leave the
    /// overlay open after clearing the sidebar's pending delete.
    #[gpui::test]
    fn delete_connection_modal_resolves_and_closes_on_execute_and_cancel(
        cx: &mut gpui::TestAppContext,
    ) {
        let outcomes: Rc<RefCell<Vec<DeleteConnectionOutcome>>> = Rc::new(RefCell::new(Vec::new()));
        let modal = cx.update(|cx| {
            let modal = cx.new(ModalDeleteConnection::new);
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, event: &DeleteConnectionOutcome, _| {
                sink.borrow_mut().push(event.clone());
            })
            .detach();
            modal.update(cx, |modal, cx| {
                modal.open(
                    DeleteConnectionRequest {
                        connection_name: "prod".to_string(),
                        has_open_documents: false,
                    },
                    cx,
                );
            });
            modal
        });

        let handled = cx.update(|cx| {
            route_confirm_modal_command(
                Command::Execute,
                &modal,
                &cx.new(ModalUnsavedChanges::new),
                &cx.new(ModalDeleteDashboardConfirm::new),
                &cx.new(ModalDeleteSavedChartConfirm::new),
                cx,
            )
        });
        assert!(handled);
        assert_eq!(
            outcomes.borrow().as_slice(),
            vec![DeleteConnectionOutcome::Confirmed]
        );
        let visible = cx.update(|cx| modal.read(cx).is_visible());
        assert!(!visible);

        // Reopen and cancel: the outcome must be Cancelled and the modal must
        // be gone, not merely cleared of its pending delete.
        cx.update(|cx| {
            modal.update(cx, |modal, cx| {
                modal.open(
                    DeleteConnectionRequest {
                        connection_name: "prod".to_string(),
                        has_open_documents: false,
                    },
                    cx,
                );
            });
        });
        let handled = cx.update(|cx| {
            route_confirm_modal_command(
                Command::Cancel,
                &modal,
                &cx.new(ModalUnsavedChanges::new),
                &cx.new(ModalDeleteDashboardConfirm::new),
                &cx.new(ModalDeleteSavedChartConfirm::new),
                cx,
            )
        });
        assert!(handled);
        assert_eq!(
            outcomes.borrow().as_slice(),
            vec![
                DeleteConnectionOutcome::Confirmed,
                DeleteConnectionOutcome::Cancelled
            ]
        );
        let visible = cx.update(|cx| modal.read(cx).is_visible());
        assert!(!visible);
    }

    #[gpui::test]
    fn unsaved_changes_modal_saves_selected_on_execute(cx: &mut gpui::TestAppContext) {
        let entry_id = DocumentId::new();
        let outcomes: Rc<RefCell<Vec<UnsavedChangesOutcome>>> = Rc::new(RefCell::new(Vec::new()));
        let modal = cx.update(|cx| {
            let modal = cx.new(ModalUnsavedChanges::new);
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, event: &UnsavedChangesOutcome, _| {
                sink.borrow_mut().push(event.clone());
            })
            .detach();
            modal.update(cx, |modal, cx| {
                modal.open(
                    UnsavedChangesRequest {
                        entries: vec![DirtySummaryEntry {
                            id: entry_id,
                            name: "query.sql".to_string(),
                            summary: "1 statement".to_string(),
                            action: CloseAction::Save,
                        }],
                    },
                    cx,
                );
            });
            modal
        });

        let handled = cx.update(|cx| {
            route_confirm_modal_command(
                Command::Execute,
                &cx.new(ModalDeleteConnection::new),
                &modal,
                &cx.new(ModalDeleteDashboardConfirm::new),
                &cx.new(ModalDeleteSavedChartConfirm::new),
                cx,
            )
        });
        assert!(handled);
        let saved = match outcomes.borrow().first() {
            Some(UnsavedChangesOutcome::SaveSelected(ids)) => ids.clone(),
            other => panic!("Enter must emit SaveSelected with the checked entries, got {other:?}"),
        };
        assert_eq!(saved, vec![entry_id]);
        let visible = cx.update(|cx| modal.read(cx).is_visible());
        assert!(!visible);
    }

    #[gpui::test]
    fn dashboard_and_chart_delete_modals_resolve_on_execute_and_cancel(
        cx: &mut gpui::TestAppContext,
    ) {
        let dashboard_outcomes: Rc<RefCell<Vec<DeleteDashboardOutcome>>> =
            Rc::new(RefCell::new(Vec::new()));
        let chart_outcomes: Rc<RefCell<Vec<DeleteSavedChartOutcome>>> =
            Rc::new(RefCell::new(Vec::new()));
        let (dashboard, chart) = cx.update(|cx| {
            let dashboard = cx.new(ModalDeleteDashboardConfirm::new);
            let chart = cx.new(ModalDeleteSavedChartConfirm::new);
            let d_sink = dashboard_outcomes.clone();
            let c_sink = chart_outcomes.clone();
            cx.subscribe(&dashboard, move |_, event: &DeleteDashboardOutcome, _| {
                d_sink.borrow_mut().push(event.clone());
            })
            .detach();
            cx.subscribe(&chart, move |_, event: &DeleteSavedChartOutcome, _| {
                c_sink.borrow_mut().push(event.clone());
            })
            .detach();
            dashboard.update(cx, |modal, cx| {
                modal.open(
                    DeleteDashboardRequest {
                        dashboard_id: test_uuid(),
                        dashboard_name: "Ops".to_string(),
                    },
                    cx,
                );
            });
            chart.update(cx, |modal, cx| {
                modal.open(
                    DeleteSavedChartRequest {
                        chart_id: test_uuid(),
                        chart_name: "Latency".to_string(),
                        referencing_dashboards: Vec::new(),
                    },
                    cx,
                );
            });
            (dashboard, chart)
        });

        // The dashboard modal precedes the chart modal in precedence order:
        // Execute confirms the dashboard and leaves the chart untouched.
        let handled = cx.update(|cx| {
            route_confirm_modal_command(
                Command::Execute,
                &cx.new(ModalDeleteConnection::new),
                &cx.new(ModalUnsavedChanges::new),
                &dashboard,
                &chart,
                cx,
            )
        });
        assert!(handled);
        assert!(matches!(
            dashboard_outcomes.borrow().first(),
            Some(DeleteDashboardOutcome::Confirmed { dashboard_id }) if *dashboard_id == test_uuid()
        ));
        assert_eq!(chart_outcomes.borrow().len(), 0);
        let dashboard_visible = cx.update(|cx| dashboard.read(cx).is_visible());
        assert!(!dashboard_visible);
        let chart_visible = cx.update(|cx| chart.read(cx).is_visible());
        assert!(chart_visible);

        // Cancel now resolves the remaining chart modal.
        let handled = cx.update(|cx| {
            route_confirm_modal_command(
                Command::Cancel,
                &cx.new(ModalDeleteConnection::new),
                &cx.new(ModalUnsavedChanges::new),
                &dashboard,
                &chart,
                cx,
            )
        });
        assert!(handled);
        assert_eq!(
            chart_outcomes.borrow().as_slice(),
            vec![DeleteSavedChartOutcome::Cancelled]
        );
        let chart_visible = cx.update(|cx| chart.read(cx).is_visible());
        assert!(!chart_visible);
    }

    #[gpui::test]
    fn routing_returns_false_when_no_confirmation_is_visible(cx: &mut gpui::TestAppContext) {
        let handled = cx.update(|cx| {
            route_confirm_modal_command(
                Command::Execute,
                &cx.new(ModalDeleteConnection::new),
                &cx.new(ModalUnsavedChanges::new),
                &cx.new(ModalDeleteDashboardConfirm::new),
                &cx.new(ModalDeleteSavedChartConfirm::new),
                cx,
            )
        });
        assert!(!handled);
    }
}
