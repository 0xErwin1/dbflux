//! Keyboard coverage of the data grid: its toolbar, footer, edit bar, menus
//! and side islands (see `crate::keyboard_coverage::DATA_GRID`).
//!
//! Reaching the grid and its islands from the keyboard is proven by
//! `ctrl_l_enters_the_value_panel_and_ctrl_h_returns`,
//! `ctrl_l_enters_the_row_inspector_and_escape_returns` and the toolbar
//! tests in `context_menu::toolbar` (`m`, then the Toolbar submenu).

use super::fk_navigation::{orders_result, orders_with_customer_fk};
use super::rail_keys::{host_in_rail, host_table_grid_with_rail};
use super::register_stub_connection;
use super::{DataGridPanel, DataSource, StubConnection};
use crate::keyboard_coverage::DATA_GRID;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_core::Pagination;
use dbflux_core::TableRef;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::sync::Arc;

fn keys(window: &mut VisualTestContext, keystrokes: &str) {
    for keystroke in keystrokes.split(' ') {
        window.simulate_keystrokes(keystroke);
        window.update(|window, _| window.refresh());
        window.run_until_parked();
    }
}

/// The ids of the Toolbar submenu of the table menu, as the grid lists them
/// right now.
fn toolbar_menu(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, cx| {
        panel
            .read(cx)
            .toolbar_actions(cx)
            .into_iter()
            .map(|action| action.id().to_string())
            .collect()
    })
}

fn assert_covered(
    panel: &Entity<DataGridPanel>,
    capture: &FrameCapture,
    window: &mut VisualTestContext,
) -> Vec<String> {
    let menu = toolbar_menu(panel, window);
    let frame = capture.frame(window);

    Coverage::new(DATA_GRID)
        .with_menu_entries(menu)
        .assert_covered(&frame)
}

#[gpui::test]
fn a_table_grid_and_its_menus_are_covered(cx: &mut TestAppContext) {
    let (panel, window) = host_table_grid_with_rail(cx);
    let capture = FrameCapture::observe(window);

    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id == "export-trigger"),
        "{checked:?}"
    );

    keys(window, "ctrl-e");
    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id.starts_with("export-save-")),
        "{checked:?}"
    );

    keys(window, "escape m k l");
    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id == "toolbar-action-export"),
        "{checked:?}"
    );
}

#[gpui::test]
fn the_side_islands_are_covered(cx: &mut TestAppContext) {
    let (panel, window) = host_table_grid_with_rail(cx);
    let capture = FrameCapture::observe(window);

    keys(window, "j v");
    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id == "value-panel-wrap"),
        "{checked:?}"
    );

    // A changed value enables Revert and Save.
    keys(window, "ctrl-l enter");
    window.simulate_input("x");
    keys(window, "escape");
    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id == "value-panel-save"),
        "{checked:?}"
    );

    keys(window, "ctrl-h v ctrl-space");
    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id == "row-inspector-copy"),
        "{checked:?}"
    );
}

#[gpui::test]
fn the_edit_bar_is_covered(cx: &mut TestAppContext) {
    let (panel, window) = host_table_grid_with_rail(cx);
    let capture = FrameCapture::observe(window);

    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("a table grid has a table");
        table_state.update(cx, |state, _| state.edit_buffer_mut().mark_for_delete(0));
    });

    let checked = assert_covered(&panel, &capture, window);
    assert!(checked.iter().any(|id| id == "save-btn"), "{checked:?}");
}

/// The REFERENCES entries of the row inspector are Toolbar entries of the
/// table menu.
#[gpui::test]
fn the_fk_references_are_covered(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(StubConnection));

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub connection is registered");
            connected.table_details.insert(
                (
                    "app".to_string(),
                    Some("public".to_string()),
                    "orders".to_string(),
                ),
                orders_with_customer_fk(),
            );
        });
    });

    let (panel, window) = host_in_rail(cx, {
        let app_state = app_state.clone();
        move |window, cx| {
            cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id,
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "orders"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(1),
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state,
                    vec!["id".to_string()],
                    window,
                    cx,
                );
                panel.set_result(orders_result(dbflux_core::Value::Int(2129)), cx);
                panel
            })
        }
    });

    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |state, cx| state.select_cell(CellCoord::new(0, 1), cx));
    });
    window.run_until_parked();

    let capture = FrameCapture::observe(window);

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let checked = assert_covered(&panel, &capture, window);
    assert!(
        checked.iter().any(|id| id == "row-inspector-reference-0"),
        "{checked:?}"
    );
}

/// The SQL builder rail, with a WHERE condition and its rail menu open.
/// `rail_keys::the_sql_builder_adds_fills_runs_and_removes_a_condition_by_keys`
/// proves Ctrl+L reaches it and `m_opens_the_sql_builder_menu_and_runs_an_entry`
/// that `m` opens its menu.
#[gpui::test]
fn the_sql_builder_rail_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::QUERY_BUILDER;

    let (panel, window) = host_table_grid_with_rail(cx);
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| panel.open_query_builder(window, cx));
    });
    window.run_until_parked();
    keys(window, "ctrl-l j a");

    let builder = window.update(|_, cx| {
        panel
            .read(cx)
            .builder
            .builder_panel
            .clone()
            .expect("the builder is open")
    });
    let rail_menu: Vec<String> = window.update(|_, cx| {
        use dbflux_components::composites::RailOwner as _;

        builder
            .read(cx)
            .rail_actions(cx)
            .iter()
            .map(|entry| entry.id.to_string())
            .collect()
    });

    let grid_menu = toolbar_menu(&panel, window);

    let capture = FrameCapture::observe(window);
    let coverage = || {
        Coverage::new(QUERY_BUILDER)
            .with_surface(DATA_GRID)
            .with_menu_entries(rail_menu.iter().cloned())
            .with_menu_entries(grid_menu.iter().cloned())
    };

    let checked = coverage().assert_covered(&capture.frame(window));
    assert!(
        checked.iter().any(|id| id.starts_with("qb-pred-rm")),
        "{checked:?}"
    );

    keys(window, "m");
    let checked = coverage().assert_covered(&capture.frame(window));
    assert!(
        checked.iter().any(|id| id == "rail-action-run"),
        "{checked:?}"
    );
}
