//! Keyboard coverage of the data grid: its toolbar, footer, edit bar, menus
//! and side islands (see `crate::keyboard_coverage::DATA_GRID`).
//!
//! Reaching the grid and its islands from the keyboard is proven by
//! `ctrl_l_enters_the_value_panel_and_ctrl_h_returns`,
//! `ctrl_l_enters_the_row_inspector_and_escape_returns` and the toolbar
//! tests in `context_menu::toolbar` (`m`, then the Toolbar submenu).

use super::DataGridPanel;
use super::rail_keys::host_table_grid_with_rail;
use crate::keyboard_coverage::DATA_GRID;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use gpui::{Entity, TestAppContext, VisualTestContext};

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

    keys(window, "v ctrl-space");
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
