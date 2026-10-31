// Explicit imports, not `use super::*`: the parent glob together with
// `#[gpui::test]` sends the macro expansion into unbounded recursion.
use crate::keymap::{Command, CommandDispatcher};
use crate::ui::document::{CodeDocument, Tab, TabManagerEvent};
use crate::ui::overlays::command_palette::PaletteSelection;
use crate::ui::overlays::modals::{
    CloseAction, DirtySummaryEntry, UnsavedChangesOutcome, UnsavedChangesRequest,
};
use crate::ui::views::workspace::{FocusTarget, Workspace, WorkspaceInspectorEvent};
use dbflux_core::QueryLanguage;
use dbflux_core::document_id::DocumentId;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::modals::DeleteConfirmRequest;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

mod close_flush;

fn new_workspace(
    cx: &mut TestAppContext,
) -> (
    Entity<Workspace>,
    Entity<AppStateEntity>,
    &mut VisualTestContext,
) {
    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });
    new_workspace_with(cx, app_state)
}

/// Like [`new_workspace`], but with a caller-built app state — used by tests
/// that need a Save As override or other per-entity test seam installed
/// before any document is opened.
fn new_workspace_with(
    cx: &mut TestAppContext,
    app_state: Entity<AppStateEntity>,
) -> (
    Entity<Workspace>,
    Entity<AppStateEntity>,
    &mut VisualTestContext,
) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);

    let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
    let workspace_ref = holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
        workspace_ref.replace(Some(workspace.clone()));
        gpui_component::Root::new(workspace, window, cx)
    });

    let workspace = holder
        .borrow()
        .clone()
        .expect("workspace should be created");
    (workspace, app_state, window)
}

/// Opens an empty query tab and returns its document id.
fn open_code_tab(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
    app_state: &Entity<AppStateEntity>,
) -> DocumentId {
    let document = window.update(|window, cx| {
        cx.new(|cx| {
            CodeDocument::new_with_language(app_state.clone(), None, QueryLanguage::Sql, window, cx)
        })
    });
    let document_id = window.update(|_, cx| document.read(cx).id());

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            let pane = CodeDocument::into_pane(document.clone(), cx);
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
        });
    });

    document_id
}

#[gpui::test]
fn sidebar_dock_hover_waits_cancels_and_preserves_focus(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.toggle(cx));
            let start = std::time::Instant::now();
            workspace.sidebar_dock.update(cx, |dock, cx| {
                dock.pointer_enter(start, cx);
                dock.process_hover_deadline(start + std::time::Duration::from_millis(249), cx);
                assert!(dock.is_collapsed());
                dock.pointer_leave(cx);
                dock.process_hover_deadline(start + std::time::Duration::from_secs(1), cx);
                assert!(dock.is_collapsed());
                dock.pointer_enter(start, cx);
                dock.process_hover_deadline(start + std::time::Duration::from_millis(250), cx);
                assert!(!dock.is_collapsed());
            });
            assert_eq!(workspace.focus_target, FocusTarget::Document);
            workspace.set_focus(FocusTarget::Sidebar, window, cx);
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.pointer_leave(cx));
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::Document, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

#[gpui::test]
fn sidebar_dock_menu_closure_dismisses_after_pointer_and_focus_leave(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace.sidebar_dock.update(cx, |dock, cx| {
                dock.toggle(cx);
                let start = std::time::Instant::now();
                dock.pointer_enter(start, cx);
                dock.process_hover_deadline(start + std::time::Duration::from_millis(250), cx);
            });
            let id = dbflux_core::SchemaNodeId::Profile {
                profile_id: uuid::Uuid::new_v4(),
            }
            .to_string();
            workspace.sidebar.update(cx, |sidebar, cx| {
                sidebar.open_menu_for_item(&id, gpui::point(gpui::px(5.0), gpui::px(5.0)), cx);
                assert!(sidebar.has_transient_interaction());
            });
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.pointer_leave(cx));
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace
                .sidebar
                .update(cx, |sidebar, cx| sidebar.close_context_menu(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        assert!(workspace.read(cx).sidebar_dock.read(cx).is_collapsed());
    });
}

#[gpui::test]
fn sidebar_dock_toggle_on_transient_reveal_collapses_instead_of_expanding(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace.sidebar_dock.update(cx, |dock, cx| {
                dock.toggle(cx);
                dock.reveal_transiently(cx);
                assert!(!dock.is_collapsed());
                dock.toggle(cx);
                assert!(
                    dock.is_collapsed(),
                    "visible left-chevron must close the transient reveal"
                );
            });
            workspace.set_focus(FocusTarget::Document, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

#[gpui::test]
fn sidebar_dock_dispatched_focus_preserves_collapsed_preference(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.toggle(cx));
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            assert!(workspace.dispatch(Command::FocusSidebar, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::Document, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

#[gpui::test]
fn sidebar_dock_palette_focus_preserves_collapsed_preference(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.toggle(cx));
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.command_palette.update(cx, |_, cx| {
                cx.emit(PaletteSelection::FocusConnection {
                    profile_id: uuid::Uuid::new_v4(),
                });
            });
        });
    });
    window.run_until_parked();
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::Document, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

#[gpui::test]
fn sidebar_dock_focus_left_enters_explicitly_collapsed_sidebar(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.toggle(cx));
            assert!(workspace.dispatch(Command::FocusLeft, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::Document, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

#[gpui::test]
fn sidebar_dock_focus_cycle_enters_explicitly_collapsed_sidebar(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.toggle(cx));
            assert!(workspace.dispatch(Command::CycleFocusForward, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::BackgroundTasks, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            assert!(workspace.dispatch(Command::CycleFocusBackward, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

#[gpui::test]
fn sidebar_dock_transient_resize_survives_dismiss_and_reveal(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.sidebar_dock.update(cx, |dock, cx| {
                dock.toggle(cx);
                dock.reveal_transiently(cx);
                let resized = dock.current_width() + gpui::px(70.0);
                dock.begin_resize(gpui::px(270.0), cx);
                dock.handle_resize_move(gpui::px(340.0), cx);
                assert_eq!(dock.current_width(), resized);
                dock.finish_resize(cx);
                dock.dismiss_transient(cx);
                assert!(dock.is_collapsed());
                dock.reveal_transiently(cx);
                assert_eq!(dock.current_width(), resized);
            });
        });
    });
}

#[gpui::test]
fn sidebar_dock_window_exit_clears_transient_reveal(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace.sidebar_dock.update(cx, |dock, cx| {
                dock.toggle(cx);
                let start = std::time::Instant::now();
                dock.pointer_enter(start, cx);
                dock.process_hover_deadline(start + std::time::Duration::from_millis(250), cx);
                assert!(!dock.is_collapsed());
            });
        });
    });
    window.run_until_parked();
    window.update(|window, cx| {
        window.dispatch_event(
            gpui::PlatformInput::MouseExited(gpui::MouseExitEvent::default()),
            cx,
        );
        assert!(workspace.read(cx).sidebar_dock.read(cx).is_collapsed());
    });
}

#[gpui::test]
fn sidebar_dock_keyboard_reveal_preserves_explicit_collapse(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace
                .sidebar_dock
                .update(cx, |dock, cx| dock.toggle(cx));
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::Sidebar, window, cx);
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            workspace.set_focus(FocusTarget::Document, window, cx);
            assert!(workspace.sidebar_dock.read(cx).is_collapsed());
        });
    });
}

/// Regression: the document's close request only ends the tab because the
/// workspace subscribes to the tab manager. Removing that link would leave
/// every save-and-close tab open with no test failing.
#[gpui::test]
fn a_close_request_closes_the_tab_that_asked(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.tab_manager.update(cx, |_manager, cx| {
                cx.emit(TabManagerEvent::RequestClose { id: document_id });
            });
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let workspace = workspace.read(cx);
        assert!(
            workspace
                .tab_manager
                .read(cx)
                .document(document_id)
                .is_none(),
            "the workspace must close the tab whose document asked"
        );
    });
}

/// Regression: closing the last tab dropped the focused document and
/// left keyboard focus nowhere, so sidebar navigation and shortcuts stopped
/// responding until the user clicked.
#[gpui::test]
fn closing_the_last_tab_hands_focus_to_the_sidebar(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    close_tab(window, &workspace, document_id);

    window.update(|window, cx| {
        let workspace = workspace.read(cx);
        assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
        assert!(
            workspace.focus_handle.is_focused(window),
            "the sidebar must hold keyboard focus once no tab is left"
        );
    });
}

#[gpui::test]
fn closing_a_tab_with_others_left_keeps_focus_on_the_document(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    open_code_tab(window, &workspace, &app_state);
    let closed = open_code_tab(window, &workspace, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    close_tab(window, &workspace, closed);

    window.update(|_, cx| {
        assert_eq!(workspace.read(cx).focus_target, FocusTarget::Document);
    });
}

/// An overlay owns the keyboard without changing the focus target, so a
/// deferred close landing while it is open must not pull focus out of it.
/// The sidebar takes focus once the overlay closes.
#[gpui::test]
fn closing_the_last_tab_under_an_overlay_keeps_the_overlay_focused(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace.toggle_command_palette(window, cx);
        });
    });
    window.run_until_parked();

    close_tab(window, &workspace, document_id);

    window.update(|window, cx| {
        let workspace = workspace.read(cx);
        assert!(workspace.command_palette.read(cx).is_visible());
        assert!(
            !workspace.focus_handle.is_focused(window),
            "the open palette must keep keyboard focus"
        );
        assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
    });

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.toggle_command_palette(window, cx);
        });
    });
    window.run_until_parked();

    window.update(|window, cx| {
        assert!(
            workspace.read(cx).focus_handle.is_focused(window),
            "closing the palette must hand focus to the sidebar"
        );
    });
}

/// The SQL preview does not hand focus back when it closes, so the
/// workspace has to move it once no overlay is left.
#[gpui::test]
fn closing_the_sql_preview_after_the_last_tab_focuses_the_sidebar(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
            workspace.sql_preview_modal.update(cx, |modal, cx| {
                modal.open_query_preview(
                    QueryLanguage::Sql,
                    "SQL",
                    "SELECT 1".to_string(),
                    window,
                    cx,
                );
            });
        });
    });
    window.run_until_parked();

    close_tab(window, &workspace, document_id);

    window.update(|window, cx| {
        assert!(
            !workspace.read(cx).focus_handle.is_focused(window),
            "the open preview must keep keyboard focus"
        );
        workspace.update(cx, |workspace, cx| {
            workspace
                .sql_preview_modal
                .update(cx, |modal, cx| modal.close(cx));
        });
    });
    window.run_until_parked();

    window.update(|window, cx| {
        assert!(
            workspace.read(cx).focus_handle.is_focused(window),
            "closing the preview must hand focus to the sidebar"
        );
    });
}

/// A modal restores the handle that was focused when it opened, which
/// belongs to the closed document; the sidebar must win over it.
#[gpui::test]
fn closing_a_modal_after_the_last_tab_focuses_the_sidebar(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.modal_delete_dashboard.update(cx, |modal, cx| {
                modal.open(
                    DeleteConfirmRequest::Dashboard {
                        dashboard_id: uuid::Uuid::new_v4(),
                        dashboard_name: "Sales".to_string(),
                    },
                    cx,
                );
            });
        });
    });
    window.run_until_parked();

    close_tab(window, &workspace, document_id);

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace
                .modal_delete_dashboard
                .update(cx, |modal, cx| modal.cancel(cx));
        });
    });
    window.run_until_parked();

    window.update(|window, cx| {
        let workspace = workspace.read(cx);
        assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
        assert!(
            workspace.focus_handle.is_focused(window),
            "closing the modal must hand focus to the sidebar"
        );
    });
}

/// A tab can close while the user works elsewhere (a deferred close lands
/// after its save), so the last close must not pull focus away from an
/// area the user moved to.
#[gpui::test]
fn closing_the_last_tab_leaves_focus_outside_the_document_alone(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::BackgroundTasks, window, cx);
        });
    });
    window.run_until_parked();

    close_tab(window, &workspace, document_id);

    window.update(|_, cx| {
        assert_eq!(
            workspace.read(cx).focus_target,
            FocusTarget::BackgroundTasks
        );
    });
}

/// Regression: "Don't save" used to close every open tab, not just the
/// document the dialog was asking about.
#[gpui::test]
fn discarding_closes_only_the_listed_documents(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let discarded = open_code_tab(window, &workspace, &app_state);
    let kept = open_code_tab(window, &workspace, &app_state);

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.modal_unsaved_changes.update(cx, |_modal, cx| {
                cx.emit(UnsavedChangesOutcome::DiscardAll(vec![discarded]));
            });
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let manager = workspace.read(cx).tab_manager.read(cx);
        assert!(
            manager.document(discarded).is_none(),
            "the listed document must close"
        );
        assert!(
            manager.document(kept).is_some(),
            "every other tab keeps its changes"
        );
    });
}

/// Regression: the workspace confirmation owns the keyboard, so its
/// routing must run before the sidebar guards in `dispatch`. Resolving the
/// modal through `dispatch` is what proves the call site is wired, which
/// calling the router directly would not.
#[gpui::test]
fn dispatch_resolves_a_visible_unsaved_changes_modal(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let document_id = open_code_tab(window, &workspace, &app_state);

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.modal_unsaved_changes.update(cx, |modal, cx| {
                modal.open(
                    UnsavedChangesRequest {
                        entries: vec![DirtySummaryEntry {
                            id: document_id,
                            name: "query.sql".to_string(),
                            summary: "1 pending change".to_string(),
                            action: CloseAction::Save,
                        }],
                    },
                    cx,
                );
                assert!(modal.is_visible(), "the modal starts visible");
            });
        });
    });

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.dispatch(Command::Cancel, window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        assert!(
            !workspace
                .read(cx)
                .modal_unsaved_changes
                .read(cx)
                .is_visible(),
            "Escape must resolve the visible confirmation"
        );
        assert!(
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(document_id)
                .is_some(),
            "cancelling keeps the tab open"
        );
    });
}

// === Inspector rail follows the active tab ===

/// Mounts content into the workspace rail exactly as a document does: an
/// `OpenInspector` relayed by the tab manager.
fn open_rail(window: &mut VisualTestContext, workspace: &Entity<Workspace>) {
    window.update(|_, cx| {
        let content: gpui::AnyView = cx.new(|_| gpui::EmptyView).into();
        workspace.update(cx, |workspace, cx| {
            workspace.tab_manager.update(cx, |_manager, cx| {
                cx.emit(TabManagerEvent::OpenInspector {
                    title: "Row".into(),
                    content,
                    content_has_header: false,
                });
            });
        });
    });
    window.run_until_parked();

    assert!(
        rail_is_open(window, workspace),
        "the rail starts open with content"
    );
}

fn rail_is_open(window: &mut VisualTestContext, workspace: &Entity<Workspace>) -> bool {
    window.update(|_, cx| workspace.read(cx).workspace_inspector.read(cx).is_open())
}

fn activate_tab(window: &mut VisualTestContext, workspace: &Entity<Workspace>, id: DocumentId) {
    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace
                .tab_manager
                .update(cx, |manager, cx| manager.activate(id, cx));
        });
    });
    window.run_until_parked();
}

fn close_tab(window: &mut VisualTestContext, workspace: &Entity<Workspace>, id: DocumentId) {
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(
                workspace.close_tab(id, window, cx),
                "a clean tab closes immediately"
            );
        });
    });
    window.run_until_parked();
}

fn record_tab_events(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
) -> Rc<RefCell<Vec<TabManagerEvent>>> {
    let events: Rc<RefCell<Vec<TabManagerEvent>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = events.clone();

    window.update(|_, cx| {
        let manager = workspace.read(cx).tab_manager.clone();
        cx.subscribe(&manager, move |_, event: &TabManagerEvent, _| {
            sink.borrow_mut().push(event.clone());
        })
        .detach();
    });

    events
}

/// Switching to a tab that owns nothing in the rail hides it, instead of
/// leaving the previous tab's content on screen. The rail is hidden, not
/// closed: a `Closed` would make the tab forget its saved rail state.
#[gpui::test]
fn switching_to_a_tab_that_owns_nothing_hides_the_rail(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let owns_nothing = open_code_tab(window, &workspace, &app_state);
    let _owner = open_code_tab(window, &workspace, &app_state);
    open_rail(window, &workspace);

    let closed_events = Rc::new(Cell::new(0));
    let closed_sink = closed_events.clone();
    window.update(|_, cx| {
        let inspector = workspace.read(cx).workspace_inspector.clone();
        cx.subscribe(&inspector, move |_, event: &WorkspaceInspectorEvent, _| {
            if matches!(event, WorkspaceInspectorEvent::Closed) {
                closed_sink.set(closed_sink.get() + 1);
            }
        })
        .detach();
    });

    activate_tab(window, &workspace, owns_nothing);

    assert!(
        !rail_is_open(window, &workspace),
        "a tab that owns nothing must not show the previous tab's rail"
    );
    assert_eq!(
        closed_events.get(),
        0,
        "a tab switch hides the rail and must never close it"
    );
}

/// A tab that mounts its content again when it becomes active keeps the
/// rail open: the hide runs first, so the tab's own `OpenInspector` wins.
#[gpui::test]
fn a_tab_that_remounts_its_content_keeps_the_rail_open(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let owner = open_code_tab(window, &workspace, &app_state);
    let _other = open_code_tab(window, &workspace, &app_state);

    window.update(|_, cx| {
        let manager = workspace.read(cx).tab_manager.clone();
        cx.subscribe(&manager, move |manager, event: &TabManagerEvent, cx| {
            if matches!(event, TabManagerEvent::Activated(id) if *id == owner) {
                let content: gpui::AnyView = cx.new(|_| gpui::EmptyView).into();
                manager.update(cx, |_manager, cx| {
                    cx.emit(TabManagerEvent::OpenInspector {
                        title: "Row".into(),
                        content,
                        content_has_header: false,
                    });
                });
            }
        })
        .detach();
    });

    activate_tab(window, &workspace, owner);

    assert!(
        rail_is_open(window, &workspace),
        "the active tab's own content must re-open the rail after the hide"
    );
}

/// Closing the active tab runs the activation pass for the tab that takes
/// over, so a successor that owns nothing hides the rail.
#[gpui::test]
fn closing_the_active_tab_activates_the_tab_that_takes_over(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let successor = open_code_tab(window, &workspace, &app_state);
    let closed = open_code_tab(window, &workspace, &app_state);
    open_rail(window, &workspace);

    let events = record_tab_events(window, &workspace);
    close_tab(window, &workspace, closed);

    let recorded = events.borrow().clone();
    assert!(
        matches!(
            recorded.as_slice(),
            [TabManagerEvent::Closed(first), TabManagerEvent::Activated(second)]
                if *first == closed && *second == successor
        ),
        "closing the active tab must activate its successor, got {recorded:?}"
    );
    assert!(
        !rail_is_open(window, &workspace),
        "a successor that owns nothing must not show the closed tab's rail"
    );
}

/// With no tab left, nothing owns the rail any more.
#[gpui::test]
fn closing_the_last_tab_hides_the_rail(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let only = open_code_tab(window, &workspace, &app_state);
    open_rail(window, &workspace);

    close_tab(window, &workspace, only);

    let active_id = window.update(|_, cx| workspace.read(cx).tab_manager.read(cx).active_id());
    assert_eq!(active_id, None, "no tab is left");
    assert!(
        !rail_is_open(window, &workspace),
        "closing the last tab must hide the rail"
    );
}

#[cfg(feature = "mcp")]
fn open_approvals(window: &mut VisualTestContext, workspace: &Entity<Workspace>) {
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.dispatch(Command::OpenMcpApprovals, window, cx);
        });
    });
    window.run_until_parked();
}

fn tabs_of_kind(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
    kind: crate::ui::document::DocumentKind,
) -> Vec<DocumentId> {
    window.update(|_, cx| {
        workspace
            .read(cx)
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter(|tab| tab.kind() == kind)
            .map(|tab| tab.id())
            .collect()
    })
}

fn active_tab_id(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
) -> Option<DocumentId> {
    window.update(|_, cx| workspace.read(cx).tab_manager.read(cx).active_id())
}

/// The approvals open as a document tab, not as an overlay over the app.
#[cfg(feature = "mcp")]
#[gpui::test]
fn the_approvals_open_as_a_tab(cx: &mut TestAppContext) {
    use crate::ui::document::DocumentKind;

    let (workspace, _app_state, window) = new_workspace(cx);
    open_approvals(window, &workspace);

    let approvals = tabs_of_kind(window, &workspace, DocumentKind::McpApprovals);
    assert_eq!(approvals.len(), 1, "the approvals must open one tab");
    assert_eq!(
        active_tab_id(window, &workspace),
        approvals.first().copied(),
        "the approvals tab must become the active tab"
    );
    assert_eq!(
        window.update(|_, cx| workspace.read(cx).focus_target),
        FocusTarget::Document,
        "the approvals tab must take the keyboard"
    );
}

/// Opening the approvals again focuses the tab that is already open
/// instead of adding a second one.
#[cfg(feature = "mcp")]
#[gpui::test]
fn opening_the_approvals_again_focuses_the_existing_tab(cx: &mut TestAppContext) {
    use crate::ui::document::DocumentKind;

    let (workspace, app_state, window) = new_workspace(cx);
    open_approvals(window, &workspace);
    let other = open_code_tab(window, &workspace, &app_state);
    activate_tab(window, &workspace, other);

    open_approvals(window, &workspace);

    let approvals = tabs_of_kind(window, &workspace, DocumentKind::McpApprovals);
    assert_eq!(approvals.len(), 1, "a second open must not add a tab");
    assert_eq!(
        active_tab_id(window, &workspace),
        approvals.first().copied()
    );
}

/// The status bar's approvals chip opens the same tab.
#[cfg(feature = "mcp")]
#[gpui::test]
fn the_status_bar_chip_opens_the_approvals_tab(cx: &mut TestAppContext) {
    use crate::ui::document::DocumentKind;
    use crate::ui::views::status_bar::OpenApprovalsRequested;

    let (workspace, _app_state, window) = new_workspace(cx);
    let status_bar = window.update(|_, cx| workspace.read(cx).status_bar.clone());

    window.update(|_, cx| {
        status_bar.update(cx, |_, cx| cx.emit(OpenApprovalsRequested));
    });
    window.run_until_parked();

    assert_eq!(
        tabs_of_kind(window, &workspace, DocumentKind::McpApprovals).len(),
        1,
        "the status bar chip must open the approvals tab"
    );
}

/// Escape inside the approvals tab leaves the tab open: it is a document,
/// not an overlay that Cancel dismisses.
#[cfg(feature = "mcp")]
#[gpui::test]
fn escape_leaves_the_approvals_tab_open(cx: &mut TestAppContext) {
    use crate::ui::document::DocumentKind;

    let (workspace, _app_state, window) = new_workspace(cx);
    open_approvals(window, &workspace);

    window.simulate_keystrokes("escape");

    assert_eq!(
        tabs_of_kind(window, &workspace, DocumentKind::McpApprovals).len(),
        1
    );
}

/// The approvals keys run through the workspace like any key binding:
/// `a` approves the selected call instead of reaching `unreachable!`.
#[cfg(feature = "mcp")]
#[gpui::test]
fn the_approvals_keys_reach_the_approvals_tab(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    app_state.update(window, |state, _| {
        state
            .request_mcp_execution(
                "agent-a".to_string(),
                "conn-a".to_string(),
                "delete_records".to_string(),
                serde_json::from_value(serde_json::json!("destructive"))
                    .expect("destructive classification"),
                serde_json::json!({ "table": "items" }),
            )
            .expect("queue a pending execution");
    });
    open_approvals(window, &workspace);

    window.simulate_keystrokes("a");
    window.run_until_parked();

    let pending = window.update(|_, cx| {
        app_state
            .read(cx)
            .list_mcp_pending_executions()
            .expect("list pending executions")
    });
    assert!(pending.is_empty(), "`a` approves the selected call");
}

/// An open migrate wizard reports its own key context, and every command
/// its layer binds reaches it through the workspace's dispatch domains.
#[gpui::test]
fn the_migrate_wizard_keys_reach_the_wizard(cx: &mut TestAppContext) {
    use dbflux_core::TableRef;

    let (workspace, _app_state, window) = new_workspace(cx);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.open_migrate_wizard(
                uuid::Uuid::new_v4(),
                None,
                vec![TableRef::new("users")],
                window,
                cx,
            );
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    let context =
        window.update(|_, cx| workspace.update(cx, |workspace, cx| workspace.active_context(cx)));
    assert_eq!(context, crate::keymap::ContextId::MigrateWizard);

    for command in [
        Command::SelectNext,
        Command::SelectPrev,
        Command::ColumnLeft,
        Command::ColumnRight,
        Command::Execute,
        Command::ExpandCollapse,
        Command::MoveSelectedUp,
        Command::MoveSelectedDown,
        Command::NextPanelTab,
        Command::PrevPanelTab,
        Command::RunQuery,
        Command::Cancel,
    ] {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| workspace.dispatch(command, window, cx));
        });
    }
}

/// The Migrate action opens the wizard as a tab; repeating it for the same
/// selection focuses that tab, and a different selection gets its own.
#[gpui::test]
fn the_migrate_wizard_opens_as_a_tab_deduplicated_by_its_selection(cx: &mut TestAppContext) {
    use crate::ui::document::DocumentKind;
    use dbflux_core::TableRef;

    let (workspace, app_state, window) = new_workspace(cx);
    let profile_id = uuid::Uuid::new_v4();
    let users = vec![TableRef {
        schema: Some("public".to_string()),
        name: "users".to_string(),
    }];
    let orders = vec![TableRef {
        schema: Some("public".to_string()),
        name: "orders".to_string(),
    }];

    let open_migrate = |window: &mut VisualTestContext, tables: Vec<TableRef>| {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_migrate_wizard(profile_id, None, tables, window, cx);
            });
        });
        window.run_until_parked();
    };

    open_migrate(window, users.clone());
    let first = tabs_of_kind(window, &workspace, DocumentKind::MigrateWizard);
    assert_eq!(first.len(), 1, "the Migrate action must open one tab");

    let other = open_code_tab(window, &workspace, &app_state);
    activate_tab(window, &workspace, other);

    open_migrate(window, users);
    assert_eq!(
        tabs_of_kind(window, &workspace, DocumentKind::MigrateWizard),
        first,
        "the same selection must focus the existing wizard tab"
    );
    assert_eq!(active_tab_id(window, &workspace), first.first().copied());

    open_migrate(window, orders);
    assert_eq!(
        tabs_of_kind(window, &workspace, DocumentKind::MigrateWizard).len(),
        2,
        "a different selection must open its own wizard tab"
    );
}

/// The panel keys move keyboard focus between the workspace panels:
/// Tab and Shift+Tab cycle them, Ctrl+Shift+digit jumps to one.
#[gpui::test]
fn keyboard_moves_between_the_workspace_panels(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    let target =
        |window: &mut VisualTestContext| window.update(|_, cx| workspace.read(cx).focus_target);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx)
        })
    });
    window.run_until_parked();

    window.simulate_keystrokes("tab");
    assert_eq!(target(window), FocusTarget::Document);

    window.simulate_keystrokes("shift-tab");
    assert_eq!(target(window), FocusTarget::Sidebar);

    window.simulate_keystrokes("ctrl-shift-4");
    assert_eq!(target(window), FocusTarget::BackgroundTasks);

    window.simulate_keystrokes("shift-tab");
    assert_eq!(target(window), FocusTarget::Sidebar);

    window.simulate_keystrokes("ctrl-shift-1");
    assert_eq!(target(window), FocusTarget::Sidebar);
}

/// Ctrl+Shift+Page Up and Page Down move the active tab one place left
/// or right, as dragging it does, and stop at either end.
#[gpui::test]
fn ctrl_shift_page_keys_move_the_active_tab(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let first = open_code_tab(window, &workspace, &app_state);
    let second = open_code_tab(window, &workspace, &app_state);
    let order = |window: &mut VisualTestContext| -> Vec<DocumentId> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|tab| tab.id())
                .collect()
        })
    };
    activate_tab(window, &workspace, second);
    window.update(|window, _| window.activate_window());

    window.simulate_keystrokes("ctrl-shift-pageup");
    window.run_until_parked();
    assert_eq!(order(window), vec![second, first]);
    assert_eq!(active_tab_id(window, &workspace), Some(second));

    window.simulate_keystrokes("ctrl-shift-pageup");
    window.run_until_parked();
    assert_eq!(
        order(window),
        vec![second, first],
        "the first place stops it"
    );

    window.simulate_keystrokes("ctrl-shift-pagedown");
    window.run_until_parked();
    assert_eq!(order(window), vec![first, second]);
    assert_eq!(active_tab_id(window, &workspace), Some(second));
}

fn tasks_panel_rendered(window: &mut VisualTestContext) -> bool {
    window.run_until_parked();
    window.debug_bounds("tasks-panel").is_some()
}

fn tasks_expanded(window: &mut VisualTestContext, workspace: &Entity<Workspace>) -> bool {
    window.update(|_, cx| workspace.read(cx).tasks_state.is_expanded())
}

/// Collapsed, the background tasks render nothing under the documents:
/// no bar and no panel.
#[gpui::test]
fn collapsed_background_tasks_render_nothing(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);

    assert!(!tasks_expanded(window, &workspace));
    assert!(!tasks_panel_rendered(window));
    assert!(
        window
            .debug_bounds("panel-header-Background Tasks")
            .is_none()
    );
}

/// The status bar's tasks chip is the way into the collapsed panel.
#[gpui::test]
fn the_status_tasks_chip_expands_the_tasks_panel(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.run_until_parked();

    let chip = window
        .debug_bounds("tasks-toggle")
        .expect("the status bar must draw the tasks chip");
    window.simulate_click(chip.center(), gpui::Modifiers::none());

    assert!(tasks_expanded(window, &workspace));
    assert!(tasks_panel_rendered(window));
}

/// Focus Background Tasks expands the collapsed panel so focus lands on
/// something drawn.
#[gpui::test]
fn focusing_the_background_tasks_expands_their_panel(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    window.run_until_parked();

    window.simulate_keystrokes("ctrl-shift-4");

    window.update(|_, cx| {
        assert_eq!(
            workspace.read(cx).focus_target,
            FocusTarget::BackgroundTasks
        );
    });
    assert!(tasks_expanded(window, &workspace));
    assert!(tasks_panel_rendered(window));
}

/// Tab and Shift+Tab skip the background tasks while their panel is
/// collapsed, and stop on them once it is expanded.
#[gpui::test]
fn focus_cycling_skips_the_collapsed_background_tasks(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    let target =
        |window: &mut VisualTestContext| window.update(|_, cx| workspace.read(cx).focus_target);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx)
        })
    });
    window.run_until_parked();

    window.simulate_keystrokes("tab");
    assert_eq!(target(window), FocusTarget::Document);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(workspace.dispatch(Command::CycleFocusBackward, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
            assert!(workspace.dispatch(Command::CycleFocusBackward, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::Document);
        })
    });
    assert!(!tasks_expanded(window, &workspace));

    window.update(|_, cx| workspace.update(cx, |workspace, cx| workspace.toggle_tasks_panel(cx)));
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx);
            assert!(workspace.dispatch(Command::CycleFocusForward, window, cx));
            assert_eq!(workspace.focus_target, FocusTarget::BackgroundTasks);
        })
    });
}

/// Collapsing the panel while it holds focus returns focus to the
/// document instead of leaving it on an element that is no longer drawn.
#[gpui::test]
fn collapsing_the_focused_tasks_panel_returns_focus_to_the_document(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::BackgroundTasks, window, cx);
            workspace.toggle_tasks_panel(cx);
        })
    });

    assert!(!tasks_panel_rendered(window));
    window.update(|_, cx| {
        assert_eq!(workspace.read(cx).focus_target, FocusTarget::Document);
    });
}

/// Letters the sidebar binds (`q` switches its tab) are text while its
/// filter has focus; `/` is what puts focus there.
#[gpui::test]
fn the_sidebar_filter_keeps_the_letters_typed_into_it(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    let sidebar = window.update(|_, cx| workspace.read(cx).sidebar.clone());

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx)
        })
    });
    window.run_until_parked();

    let tab_before = window.update(|_, cx| sidebar.read(cx).active_tab());

    window.simulate_keystrokes("/");
    assert!(
        window.update(|window, cx| sidebar.read(cx).search_input_is_focused(window, cx)),
        "`/` focuses the sidebar filter"
    );

    window.simulate_keystrokes("q");
    assert_eq!(
        window.update(|_, cx| sidebar.read(cx).active_tab()),
        tab_before,
        "`q` typed in the filter does not switch the sidebar tab"
    );
    assert!(window.update(|window, cx| sidebar.read(cx).search_input_is_focused(window, cx)));
}

/// The palette opens from the keyboard and Escape closes it.
#[gpui::test]
fn the_command_palette_opens_and_closes_from_the_keyboard(cx: &mut TestAppContext) {
    let (workspace, _, window) = new_workspace(cx);
    let palette = window.update(|_, cx| workspace.read(cx).command_palette.clone());

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx)
        })
    });
    window.run_until_parked();

    #[cfg(target_os = "macos")]
    window.simulate_keystrokes("cmd-shift-p");
    #[cfg(not(target_os = "macos"))]
    window.simulate_keystrokes("ctrl-shift-p");
    assert!(window.update(|_, cx| palette.read(cx).is_visible()));

    window.simulate_keystrokes("escape");
    assert!(!window.update(|_, cx| palette.read(cx).is_visible()));
}

fn toast_count(window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .read(cx)
            .toast_count()
    })
}

fn last_toast_title(window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .read(cx)
            .last_toast_title()
    })
}

/// Opens a buckets table for a profile with no live connection, so its
/// refresh settles synchronously into the error state without any I/O.
/// Returns the document and a counter of its change notifications.
fn open_buckets_tab(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
    app_state: &Entity<AppStateEntity>,
) -> (
    Entity<crate::ui::document::BucketsTableDocument>,
    Rc<Cell<usize>>,
    gpui::Subscription,
) {
    let document = window.update(|window, cx| {
        cx.new(|cx| {
            crate::ui::document::BucketsTableDocument::new(
                uuid::Uuid::new_v4(),
                app_state.clone(),
                window,
                cx,
            )
        })
    });

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            let pane = crate::ui::document::BucketsTableDocument::into_pane(document.clone(), cx);
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
        });
    });

    let notifications = Rc::new(Cell::new(0));
    let subscription = window.update(|_, cx| {
        cx.observe(&document, {
            let notifications = notifications.clone();
            move |_, _| notifications.set(notifications.get() + 1)
        })
    });

    (document, notifications, subscription)
}

/// Root section events open the presentation their declaration names: the
/// containers table for a section, the object browser for an item. Without a
/// connection each opener answers with its own no-connection toast.
#[gpui::test]
fn root_section_events_route_by_declared_presentation(cx: &mut TestAppContext) {
    use crate::ui::labels::{NoActiveConnectionKind, documents_no_active_connection_message};
    use dbflux_ui_sidebar::SidebarEvent;

    let (workspace, _app_state, window) = new_workspace(cx);
    let sidebar = window.update(|_, cx| workspace.read(cx).sidebar.clone());
    let profile_id = uuid::Uuid::new_v4();

    window.update(|_, cx| {
        sidebar.update(cx, |_, cx| {
            cx.emit(SidebarEvent::OpenRootSection {
                profile_id,
                section_id: "buckets".to_string(),
                presentation: dbflux_core::RootSectionPresentation::ObjectContainerTable,
            });
        });
    });
    window.run_until_parked();
    assert_eq!(
        last_toast_title(window),
        Some(documents_no_active_connection_message(
            NoActiveConnectionKind::ObjectStorageAccount
        ))
    );

    window.update(|_, cx| {
        sidebar.update(cx, |_, cx| {
            cx.emit(SidebarEvent::OpenRootSectionItem {
                profile_id,
                section_id: "buckets".to_string(),
                item_id: "assets".to_string(),
                presentation: dbflux_core::RootItemPresentation::ObjectBrowser,
            });
        });
    });
    window.run_until_parked();
    assert_eq!(
        last_toast_title(window),
        Some(documents_no_active_connection_message(
            NoActiveConnectionKind::Bucket
        ))
    );
}

#[gpui::test]
fn refresh_schema_refreshes_the_focused_document_that_handles_it(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let (_document, notifications, _subscription) =
        open_buckets_tab(window, &workspace, &app_state);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    let toasts_before = toast_count(window);
    notifications.set(0);

    let handled = window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.dispatch(Command::RefreshSchema, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    assert!(
        notifications.get() > 0,
        "the buckets table must reload when it has document focus"
    );
    assert_eq!(
        toast_count(window),
        toasts_before,
        "the connection-schema refresh must not run when the document handled the refresh"
    );
}

#[gpui::test]
fn refresh_schema_falls_back_to_the_connection_schema_when_the_document_declines(
    cx: &mut TestAppContext,
) {
    let (workspace, app_state, window) = new_workspace(cx);
    open_code_tab(window, &workspace, &app_state);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    let toasts_before = toast_count(window);

    let handled = window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.dispatch(Command::RefreshSchema, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    assert_eq!(toast_count(window), toasts_before + 1);
    assert_eq!(
        last_toast_title(window),
        Some(crate::ui::labels::connections_no_active_connection_message()),
        "a query tab has no refresh, so the connection-schema refresh must run"
    );
}

#[gpui::test]
fn refresh_schema_from_the_sidebar_reloads_only_the_connection_schema(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let (_document, notifications, _subscription) =
        open_buckets_tab(window, &workspace, &app_state);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx);
        });
    });
    window.run_until_parked();

    let toasts_before = toast_count(window);
    notifications.set(0);

    let handled = window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.dispatch(Command::RefreshSchema, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    assert_eq!(
        notifications.get(),
        0,
        "the active document must not refresh while the sidebar has focus"
    );
    assert_eq!(toast_count(window), toasts_before + 1);
    assert_eq!(
        last_toast_title(window),
        Some(crate::ui::labels::connections_no_active_connection_message()),
    );
}
