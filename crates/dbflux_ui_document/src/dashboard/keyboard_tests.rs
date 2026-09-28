//! Keyboard tests of the dashboard: the panel grid keys (X8 raw keys turned
//! into commands), opening a panel, moving and resizing, the Configure
//! popover and the pane actions.

use super::{DashboardDocument, DashboardMode, DashboardPanelSlot, PanelGridPos};
use crate::chart_document::ChartDocument;
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::chart::ChartKind;
use dbflux_components::common::time_range::view::TimeRangePanel;
use dbflux_components::saved_chart::SavedChartRefreshPolicy;
use dbflux_ui_base::{AppStateEntity, DashboardPanelDraft, DraftGridLayout};
use gpui::{App, AppContext as _, Entity, TestAppContext, VisualTestContext, Window};
use uuid::Uuid;

fn app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    })
}

fn pos(grid_row: u32, grid_column: u32, grid_width: u32, grid_height: u32) -> PanelGridPos {
    PanelGridPos {
        grid_row,
        grid_column,
        grid_width,
        grid_height,
    }
}

fn orphan(grid_pos: PanelGridPos) -> DashboardPanelSlot {
    DashboardPanelSlot::Orphan {
        saved_chart_id: Uuid::nil(),
        grid_pos,
    }
}

type SlotBuilder =
    Box<dyn FnOnce(&Entity<AppStateEntity>, &mut Window, &mut App) -> Vec<DashboardPanelSlot>>;

/// A dashboard with the slots `build` returns, hosted under the app keymap
/// with the keyboard on it.
fn dashboard_with_keyboard(
    cx: &mut TestAppContext,
    dashboard_id: Uuid,
    app_state: Entity<AppStateEntity>,
    build: SlotBuilder,
) -> (
    Entity<KeymapHost<DashboardDocument>>,
    Entity<DashboardDocument>,
    &mut VisualTestContext,
) {
    let (host, window) = host_document(
        cx,
        move |window, cx| {
            let slots = build(&app_state, window, cx);
            let shared_time_range = cx.new(|cx| TimeRangePanel::new("24h", Some(3), window, cx));

            cx.new(|cx| {
                DashboardDocument::new(
                    dashboard_id,
                    "Dashboard".to_string(),
                    slots,
                    shared_time_range,
                    None,
                    SavedChartRefreshPolicy::Off,
                    false,
                    app_state,
                    cx,
                )
            })
        },
        |dashboard, cx| dashboard.active_context(cx),
        DashboardDocument::dispatch_command,
    );
    let dashboard = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|window, cx| dashboard.update(cx, |dashboard, cx| dashboard.focus(window, cx)));
    window.run_until_parked();

    (host, dashboard, window)
}

fn selected(dashboard: &Entity<DashboardDocument>, window: &mut VisualTestContext) -> Option<u32> {
    window.update(|_, cx| dashboard.read(cx).focused_panel_index)
}

/// A divider over three panels: two side by side, one below.
fn divider_and_three_panels() -> SlotBuilder {
    Box::new(|_, _, _| {
        vec![
            DashboardPanelSlot::Divider {
                markdown: "# Section".to_string(),
                grid_pos: pos(0, 0, 12, 1),
            },
            orphan(pos(1, 0, 6, 2)),
            orphan(pos(1, 6, 6, 2)),
            orphan(pos(3, 0, 12, 2)),
        ]
    })
}

/// hjkl select panels on the grid, and Space folds the divider's section,
/// after which the folded panels are skipped.
#[gpui::test]
fn hjkl_select_panels_and_space_folds_a_section(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);
    let app_state = app_state(cx);
    let (host, dashboard, window) =
        dashboard_with_keyboard(cx, Uuid::nil(), app_state, divider_and_three_panels());

    assert_eq!(
        window.update(|_, cx| dashboard.read(cx).active_context(cx)),
        ContextId::Dashboard
    );
    assert_eq!(selected(&dashboard, window), Some(0));

    window.simulate_keystrokes("j");
    assert_eq!(
        selected(&dashboard, window),
        Some(1),
        "J goes to the next row"
    );

    window.simulate_keystrokes("l");
    assert_eq!(selected(&dashboard, window), Some(2), "L goes right");

    window.simulate_keystrokes("j");
    assert_eq!(selected(&dashboard, window), Some(3));

    window.simulate_keystrokes("k");
    assert_eq!(
        selected(&dashboard, window),
        Some(1),
        "K goes up to the panel nearest the column"
    );

    window.simulate_keystrokes("g space j");
    assert!(window.update(|_, cx| dashboard.read(cx).is_divider_collapsed(0)));
    assert_eq!(
        selected(&dashboard, window),
        Some(0),
        "the folded panels are skipped"
    );

    assert_eq!(
        window.update(|_, cx| host.read(cx).commands.clone()),
        vec![
            Command::SelectNext,
            Command::ColumnRight,
            Command::SelectNext,
            Command::SelectPrev,
            Command::SelectFirst,
            Command::ExpandCollapse,
            Command::SelectNext,
        ]
    );
}

/// In Edit mode Shift+J moves the selected panel a row down and
/// Alt+Shift+J makes it taller; a move onto another panel is refused.
#[gpui::test]
fn shift_and_alt_shift_move_and_resize_the_panel(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);
    let app_state = app_state(cx);

    let dashboard_id = app_state.update(cx, |state, _| {
        let profile = dbflux_core::ConnectionProfile::new(
            "test",
            dbflux_core::DbConfig::SQLite {
                path: std::path::PathBuf::from(":memory:"),
                connection_id: None,
            },
        );
        let profile_id = profile.id;
        state.add_profile_in_folder(profile, None);

        let id = state
            .dashboards
            .create_dashboard(
                "Keys".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .expect("dashboard created");
        let draft = |column| DashboardPanelDraft::Inspector {
            metric_id: "sessions".to_string(),
            layout: Some(DraftGridLayout {
                grid_row: 0,
                grid_column: column,
                grid_width: 6,
                grid_height: 2,
            }),
        };
        state
            .dashboards
            .append_panels(id, vec![draft(0), draft(6)])
            .expect("panels appended");
        id
    });

    let (_host, dashboard, window) = dashboard_with_keyboard(
        cx,
        dashboard_id,
        app_state,
        Box::new(|_, _, _| vec![orphan(pos(0, 0, 6, 2)), orphan(pos(0, 6, 6, 2))]),
    );
    let grid_pos = |window: &mut VisualTestContext, index: usize| {
        window.update(|_, cx| dashboard.read(cx).panel_slots()[index].grid_pos())
    };

    window.simulate_keystrokes("shift-j");
    assert_eq!(
        grid_pos(window, 0),
        pos(0, 0, 6, 2),
        "View mode moves nothing"
    );

    window.update(|_, cx| {
        dashboard.update(cx, |dashboard, cx| {
            dashboard.set_mode(DashboardMode::Edit, cx)
        })
    });

    window.simulate_keystrokes("shift-l");
    assert_eq!(
        grid_pos(window, 0),
        pos(0, 0, 6, 2),
        "a move onto the other panel is refused"
    );

    window.simulate_keystrokes("shift-j");
    assert_eq!(grid_pos(window, 0), pos(1, 0, 6, 2));

    window.simulate_keystrokes("alt-shift-j");
    assert_eq!(grid_pos(window, 0), pos(1, 0, 6, 3));
}

/// Enter opens a chart panel: the chart keys drive it (Alt+L switches its
/// type) until Escape brings the keyboard back to the dashboard.
#[gpui::test]
fn enter_opens_a_chart_panel_and_escape_leaves_it(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);
    let app_state = app_state(cx);

    let chart_slot: std::rc::Rc<std::cell::RefCell<Option<Entity<ChartDocument>>>> =
        Default::default();
    let (_host, dashboard, window) = dashboard_with_keyboard(
        cx,
        Uuid::nil(),
        app_state,
        Box::new({
            let chart_slot = chart_slot.clone();
            move |app_state, window, cx| {
                let chart = cx.new(|cx| {
                    let mut chart =
                        ChartDocument::new(None, String::new(), app_state.clone(), window, cx);
                    chart.set_embedded(true, cx);
                    chart
                });
                chart_slot.replace(Some(chart.clone()));

                vec![DashboardPanelSlot::Loaded {
                    panel: chart,
                    grid_pos: pos(0, 0, 12, 4),
                    title_override: None,
                }]
            }
        }),
    );
    let chart = chart_slot.borrow().clone().expect("chart panel built");
    let context = |window: &mut VisualTestContext| {
        window.update(|_, cx| dashboard.read(cx).active_context(cx))
    };

    window.simulate_keystrokes("enter");
    assert_eq!(context(window), ContextId::Chart);

    window.simulate_keystrokes("alt-l");
    assert_eq!(
        window.update(|_, cx| chart.read(cx).chart_kind(cx)),
        ChartKind::Bar
    );

    window.simulate_keystrokes("escape");
    assert_eq!(context(window), ContextId::Dashboard);
    assert_eq!(
        window.update(|_, cx| dashboard.read(cx).entered_panel()),
        None
    );
}

/// C opens the Configure popover of a chart panel; its chart keys switch
/// the type and Escape closes it.
#[gpui::test]
fn c_opens_the_configure_popover_driven_by_the_chart_keys(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);
    let app_state = app_state(cx);

    let chart_slot: std::rc::Rc<std::cell::RefCell<Option<Entity<ChartDocument>>>> =
        Default::default();
    let (_host, dashboard, window) = dashboard_with_keyboard(
        cx,
        Uuid::nil(),
        app_state,
        Box::new({
            let chart_slot = chart_slot.clone();
            move |app_state, window, cx| {
                let chart = cx.new(|cx| {
                    let mut chart =
                        ChartDocument::new(None, String::new(), app_state.clone(), window, cx);
                    chart.set_embedded(true, cx);
                    chart
                });
                chart_slot.replace(Some(chart.clone()));

                vec![DashboardPanelSlot::Loaded {
                    panel: chart,
                    grid_pos: pos(0, 0, 12, 4),
                    title_override: None,
                }]
            }
        }),
    );
    let chart = chart_slot.borrow().clone().expect("chart panel built");
    let popover = |window: &mut VisualTestContext| {
        window.update(|_, cx| dashboard.read(cx).pending_configure_panel_index())
    };

    window.simulate_keystrokes("c");
    assert_eq!(popover(window), Some(0));

    window.simulate_keystrokes("alt-l");
    assert_eq!(
        window.update(|_, cx| chart.read(cx).chart_kind(cx)),
        ChartKind::Bar,
        "Alt+L in the popover switches the panel's chart type"
    );

    window.simulate_keystrokes("escape");
    assert_eq!(popover(window), None);
}

/// The pane actions list the selected panel's actions and the toolbar.
#[gpui::test]
fn the_pane_actions_list_the_panel_and_the_toolbar(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);
    let app_state = app_state(cx);
    let (_host, dashboard, window) =
        dashboard_with_keyboard(cx, Uuid::nil(), app_state, divider_and_three_panels());

    window.simulate_keystrokes("j");
    let ids: Vec<String> = window.update(|_, cx| {
        dashboard
            .read(cx)
            .pane_actions(&dashboard, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    for id in [
        "dashboard-panel-remove",
        "dashboard-add-panel",
        "dashboard-refresh",
        "dashboard-auto-refresh",
        "dashboard-next-time-range",
        "dashboard-mode",
    ] {
        assert!(ids.iter().any(|entry| entry == id), "{id} in {ids:?}");
    }
}

#[test]
fn dashboard_pane_action_labels_resolve_in_every_locale() {
    for key in [
        "open_panel",
        "fold_section",
        "switch_to_view",
        "switch_to_edit",
    ] {
        let key = format!("document.dashboard.pane_actions.{key}");

        for locale in ["en", "es", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(&key, locale = locale);

            assert!(!value.is_empty(), "{key} resolved empty in {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} missing from {locale}"
            );
        }
    }
}
