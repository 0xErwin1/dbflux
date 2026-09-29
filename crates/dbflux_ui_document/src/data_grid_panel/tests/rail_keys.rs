//! The query builder rails driven from the keyboard, beside a grid hosted
//! as the workspace hosts it.

use super::*;
use crate::keyboard_test_support::init_keyboard_runtime;
use crate::query_builder::QueryBuilderPanel;
use dbflux_app::keymap::ContextId;
use dbflux_core::{Comparator, FilterNode};

/// A two-row table beside its inspector rail, under the app keymap, with
/// the table focused.
pub(super) fn host_table_grid_with_rail(
    cx: &mut TestAppContext,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);

    host_in_rail(cx, move |window, cx| {
        let source = DataSource::Table {
            profile_id: Uuid::nil(),
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "users"),
            pagination: Pagination::default(),
            order_by: Vec::new(),
            total_rows: Some(2),
        };
        cx.new(|cx| {
            let mut panel =
                DataGridPanel::new_internal(source, app_state, vec!["id".to_string()], window, cx);
            panel.set_result(keyed_result(&["1", "2"]), cx);
            panel
        })
    })
}

/// Hosts the grid `build` makes beside its inspector rail, as the workspace
/// hosts it, with keyboard focus on the grid (its table when it has one).
pub(crate) fn host_in_rail(
    cx: &mut TestAppContext,
    build: impl FnOnce(&mut gpui::Window, &mut gpui::App) -> gpui::Entity<DataGridPanel> + 'static,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    let slot: Rc<RefCell<Option<gpui::Entity<DataGridPanel>>>> = Rc::default();

    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let panel = build(window, cx);
            slot.replace(Some(panel.clone()));

            let host = cx.new(|cx| {
                let subscription = cx.subscribe(
                    &panel,
                    |host: &mut RailHost, _, event: &DataGridEvent, cx| match event {
                        DataGridEvent::OpenInspector { content, .. } => {
                            host.rail = Some(content.clone());
                            cx.notify();
                        }
                        DataGridEvent::CloseInspector => {
                            host.rail = None;
                            cx.notify();
                        }
                        _ => {}
                    },
                );

                RailHost {
                    panel: panel.clone(),
                    rail: None,
                    _subscription: subscription,
                }
            });

            Root::new(host, window, cx)
        }
    });
    window.run_until_parked();

    let panel = slot
        .borrow()
        .clone()
        .expect("the window builder stores the grid");
    window.update(|window, cx| {
        let grid = panel.read(cx);
        let focus_handle = grid
            .grid_table
            .table_state
            .as_ref()
            .map(|state| state.read(cx).focus_handle().clone())
            .unwrap_or_else(|| grid.focus_handle.clone());
        focus_handle.focus(window, cx);
    });
    window.run_until_parked();

    (panel, window)
}

pub(crate) fn keys(window: &mut VisualTestContext, keys: &str) {
    for key in keys.split(' ') {
        window.simulate_keystrokes(key);
        window.run_until_parked();
    }
}

pub(crate) fn context(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> ContextId {
    window.update(|_, cx| panel.read(cx).active_context(cx))
}

fn sql_builder(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> gpui::Entity<QueryBuilderPanel> {
    window.update(|_, cx| {
        panel
            .read(cx)
            .builder
            .builder_panel
            .clone()
            .expect("the builder is open")
    })
}

fn cursor_row(
    builder: &gpui::Entity<QueryBuilderPanel>,
    window: &mut VisualTestContext,
) -> Option<String> {
    window.update(|_, cx| {
        builder
            .read(cx)
            .rail
            .cursor_row()
            .map(|row| row.to_string())
    })
}

/// The single WHERE predicate of the builder's spec.
fn only_predicate(
    builder: &gpui::Entity<QueryBuilderPanel>,
    window: &mut VisualTestContext,
) -> Option<dbflux_core::Predicate> {
    window.update(|_, cx| match &builder.read(cx).current_spec.filter {
        Some(FilterNode::Group { children, .. }) => match children.as_slice() {
            [FilterNode::Predicate(predicate)] => Some(predicate.clone()),
            _ => None,
        },
        _ => None,
    })
}

/// Leaves a text field of the rail: Escape closes a completion list first,
/// then hands the keyboard back to the rail.
fn leave_field(builder: &gpui::Entity<QueryBuilderPanel>, window: &mut VisualTestContext) {
    for _ in 0..2 {
        let on_rail = window.update(|window, cx| {
            builder
                .read(cx)
                .focus_handle
                .as_ref()
                .is_some_and(|handle| handle.is_focused(window))
        });
        if on_rail {
            return;
        }
        keys(window, "escape");
    }
}

/// Ctrl+L enters the SQL builder; A adds a WHERE condition and the cursor
/// follows it; the column, operator and value are filled with Enter, the
/// operator list and typing; Ctrl+Enter runs it through the same path as the
/// Run button; X removes the condition; Escape leaves the rail.
#[gpui::test]
fn the_sql_builder_adds_fills_runs_and_removes_a_condition_by_keys(cx: &mut TestAppContext) {
    let (panel, window) = host_table_grid_with_rail(cx);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| panel.open_query_builder(window, cx));
    });
    window.run_until_parked();

    keys(window, "ctrl-l");
    assert_eq!(
        context(&panel, window),
        ContextId::QueryBuilder,
        "Ctrl+L moves the keyboard into the builder"
    );
    let builder = sql_builder(&panel, window);

    keys(window, "j");
    assert_eq!(cursor_row(&builder, window).as_deref(), Some("where-empty"));

    keys(window, "a");
    let predicate = only_predicate(&builder, window).expect("A adds a condition");
    let row = format!("where-predicate-{}", predicate.node_id);
    assert_eq!(
        cursor_row(&builder, window),
        Some(row.clone()),
        "the cursor moves onto the new condition"
    );

    keys(window, "enter");
    assert_eq!(
        context(&panel, window),
        ContextId::QueryBuilder,
        "a text field keeps the rail's context"
    );
    window.simulate_input("users.name");
    window.run_until_parked();
    leave_field(&builder, window);

    keys(window, "l enter j enter");
    keys(window, "l enter");
    window.simulate_input("bob");
    window.run_until_parked();
    leave_field(&builder, window);

    let predicate = only_predicate(&builder, window).expect("the condition stays");
    assert_eq!(predicate.column, "name", "the column was typed");
    assert_ne!(
        predicate.comparator,
        Comparator::Eq,
        "the operator was picked from its list"
    );
    assert_eq!(
        predicate.value,
        dbflux_core::PredicateValue::Single(dbflux_core::LiteralValue::Text("bob".to_string())),
        "the value was typed"
    );

    keys(window, "ctrl-enter");
    assert!(
        window.update(|_, cx| panel.read(cx).builder.filter_input_hidden),
        "Ctrl+Enter applies the builder's query like Run"
    );

    keys(window, "x");
    assert!(
        only_predicate(&builder, window).is_none(),
        "X removes the condition"
    );

    keys(window, "escape");
    assert_eq!(context(&panel, window), ContextId::Results);
}

/// M lists the cursor row's actions and the rail's own; its entries run
/// with the context-menu keys, and Escape closes it.
#[gpui::test]
fn m_opens_the_sql_builder_menu_and_runs_an_entry(cx: &mut TestAppContext) {
    let (panel, window) = host_table_grid_with_rail(cx);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| panel.open_query_builder(window, cx));
    });
    window.run_until_parked();
    keys(window, "ctrl-l j");
    let builder = sql_builder(&panel, window);

    keys(window, "m");
    assert_eq!(context(&panel, window), ContextId::ContextMenu);
    let labels = window.update(|_, cx| builder.read(cx).rail.menu_labels());
    assert_eq!(
        labels.first().map(|label| label.to_string()),
        Some(dbflux_i18n::t!("composites.rail.add")),
        "the cursor row's actions come first"
    );
    assert!(
        labels
            .iter()
            .any(|label| label.as_ref() == dbflux_i18n::t!("document.query_builder.status.run")),
        "the rail's Run is listed: {labels:?}"
    );

    keys(window, "escape");
    assert_eq!(context(&panel, window), ContextId::QueryBuilder);

    keys(window, "m enter");
    assert!(
        only_predicate(&builder, window).is_some(),
        "the first entry, Add, added a condition"
    );
    assert_eq!(context(&panel, window), ContextId::QueryBuilder);
}

/// Ctrl+Enter in DELETE mode asks for the same mutation run the Run button
/// asks for, so the grid's no-WHERE confirmation and mutation policy apply
/// to it unchanged.
#[gpui::test]
fn ctrl_enter_in_delete_mode_requests_the_run_button_mutation(cx: &mut TestAppContext) {
    use crate::query_builder::{BuilderEvent, BuilderMode};

    let (panel, window) = host_table_grid_with_rail(cx);
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| panel.open_query_builder(window, cx));
    });
    window.run_until_parked();
    keys(window, "ctrl-l");
    let builder = sql_builder(&panel, window);

    let requested: Rc<RefCell<Vec<dbflux_core::VisualMutationSpec>>> = Rc::default();
    let _subscription = window.update(|_, cx| {
        builder.update(cx, |builder, cx| {
            builder.switch_builder_mode(BuilderMode::Delete, cx)
        });
        let requested = requested.clone();
        cx.subscribe(&builder, move |_, event: &BuilderEvent, _| {
            if let BuilderEvent::MutationRunRequested { spec, .. } = event {
                requested.borrow_mut().push(spec.as_ref().clone());
            }
        })
    });
    window.run_until_parked();

    keys(window, "ctrl-enter");

    let requested = requested.borrow();
    assert_eq!(requested.len(), 1, "Ctrl+Enter requests one mutation run");
    assert!(
        matches!(requested[0].kind, dbflux_core::MutationKind::Delete),
        "the DELETE the Run button would request"
    );
    assert!(
        requested[0].filter.is_none(),
        "without WHERE, left to the grid's no-WHERE confirmation"
    );
}

/// With Vim mode on, the SQL preview takes Vim motions and stays unchanged:
/// `l` and `w` move the cursor, `x` deletes nothing.
#[gpui::test]
fn the_sql_preview_takes_vim_motions(cx: &mut TestAppContext) {
    cx.update(|cx| dbflux_components::vim::set_vim_enabled(cx, true));
    let (panel, window) = host_table_grid_with_rail(cx);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| panel.open_query_builder(window, cx));
    });
    window.run_until_parked();
    let builder = sql_builder(&panel, window);

    let preview = window.update(|_, cx| {
        builder
            .read(cx)
            .sql_preview_state
            .clone()
            .expect("the builder has a preview")
    });
    window.update(|window, cx| {
        preview.update(cx, |state, cx| {
            state.set_value("SELECT id FROM users", window, cx);
            state.set_selected_range(0..0, cx);
            state.focus(window, cx);
        });
    });
    window.run_until_parked();
    let before = window.update(|_, cx| preview.read(cx).value().to_string());

    keys(window, "l x");
    let (text, cursor) = window.update(|_, cx| {
        (
            preview.read(cx).value().to_string(),
            preview.read(cx).cursor(),
        )
    });
    assert_eq!(text, before, "the preview is read-only");
    assert_eq!(cursor, 1, "l moved the cursor");
}
