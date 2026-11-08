use super::new_workspace;
use crate::ui::views::workspace::{FocusTarget, Workspace};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::app_state_entity::AppStateChanged;
use gpui::{
    Bounds, Entity, Modifiers, MouseButton, Pixels, Point, TestAppContext, VisualTestContext,
    point, px,
};

const DRAG: f32 = 40.0;

fn add_profiles(window: &mut VisualTestContext, app_state: &Entity<AppStateEntity>) {
    window.update(|_, cx| {
        app_state.update(cx, |state, cx| {
            for name in ["alpha", "beta", "gamma"] {
                state.add_profile_in_folder(
                    dbflux_core::ConnectionProfile::new(
                        name,
                        dbflux_core::DbConfig::SQLite {
                            path: std::path::PathBuf::from(":memory:"),
                            connection_id: None,
                        },
                    ),
                    None,
                );
            }
            cx.emit(AppStateChanged);
        });
    });
    window.run_until_parked();
    window.update(|window, _| window.refresh());
    window.run_until_parked();
}

fn dock_bounds(window: &mut VisualTestContext) -> Bounds<Pixels> {
    window
        .debug_bounds("sidebar-dock")
        .expect("the sidebar dock is rendered")
}

fn width(window: &mut VisualTestContext, workspace: &Entity<Workspace>) -> Pixels {
    window.update(|_, cx| workspace.read(cx).sidebar_dock.read(cx).current_width())
}

fn drag(window: &mut VisualTestContext, from: Point<Pixels>, by: f32) {
    window.simulate_mouse_down(from, MouseButton::Left, Modifiers::none());
    let to = point(from.x + px(by), from.y);
    window.simulate_mouse_move(to, MouseButton::Left, Modifiers::none());
    window.simulate_mouse_up(to, MouseButton::Left, Modifiers::none());
}

/// A drag that starts `offset` px right of the dock's right edge (negative:
/// inside the island) at a y with no tree row under it.
fn width_after_drag_from(cx: &mut TestAppContext, offset: f32) -> (Pixels, Pixels) {
    let (workspace, app_state, window) = new_workspace(cx);
    add_profiles(window, &app_state);

    let dock = dock_bounds(window);
    let before = width(window, &workspace);
    let start = point(
        dock.origin.x + dock.size.width + px(offset),
        dock.origin.y + px(500.0),
    );
    drag(window, start, DRAG);
    (before, width(window, &workspace))
}

#[gpui::test]
fn a_drag_from_the_desk_gutter_resizes_the_sidebar(cx: &mut TestAppContext) {
    let (before, after) = width_after_drag_from(cx, 4.0);
    assert_eq!(after, before + px(DRAG));
}

#[gpui::test]
fn a_drag_from_the_island_edge_band_resizes_the_sidebar(cx: &mut TestAppContext) {
    for offset in [-12.0, -8.0, -1.0] {
        let (before, after) = width_after_drag_from(cx, offset);
        assert_eq!(after, before + px(DRAG), "drag from {offset} px");
    }
}

#[gpui::test]
fn a_drag_from_the_document_island_does_not_resize_the_sidebar(cx: &mut TestAppContext) {
    let (before, after) = width_after_drag_from(cx, 8.0 + 4.0);
    assert_eq!(after, before);
}

#[gpui::test]
fn a_drag_from_a_tree_row_does_not_resize_the_sidebar(cx: &mut TestAppContext) {
    let (before, after) = width_after_drag_from(cx, -100.0);
    assert_eq!(after, before);
}

#[gpui::test]
fn a_click_on_a_tree_row_outside_the_band_still_reaches_the_row(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    add_profiles(window, &app_state);
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    let before = width(window, &workspace);
    let dock = dock_bounds(window);
    let row = point(px(200.0), dock.origin.y + px(100.0));
    window.simulate_mouse_down(row, MouseButton::Left, Modifiers::none());
    window.simulate_mouse_up(row, MouseButton::Left, Modifiers::none());
    window.run_until_parked();

    window.update(|_, cx| {
        let workspace = workspace.read(cx);
        assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
        assert!(!workspace.sidebar_dock.read(cx).is_resizing());
    });
    assert_eq!(width(window, &workspace), before);
}
