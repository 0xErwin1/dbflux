//! GPUI tests for the schema diagram's keyboard handling: key presses resolve
//! through the app keymap's SchemaViz and ContextMenu layers and reach the
//! diagram with the same effect the hand-parsed handler had. Kept out of
//! `mod.rs` for the same compile-time reason as `tests.rs`.

use super::{LoadStatus, SchemaVizDocument, SchemaVizMode, snap_to_lattice};
use dbflux_components::theme;
use dbflux_core::{ColumnInfo, TableInfo};
use dbflux_schema_viz::graph::SchemaGraph;
use dbflux_schema_viz::layout::LayoutFormat;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::{WORKSPACE_KEY_CONTEXT, init_keymap, root_key_context};
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
    Pixels, Point, Render, Styled as _, TestAppContext, VisualTestContext, Window, div, px,
};
use uuid::Uuid;

/// Stands in for the workspace root: it carries the key context the
/// workspace reports for the diagram, as it does in the app.
struct Host {
    document: Entity<SchemaVizDocument>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let context = self.document.read(cx).active_context();

        div()
            .size_full()
            .key_context(root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]))
            .child(self.document.clone())
    }
}

fn table(name: &str) -> TableInfo {
    TableInfo {
        name: name.to_owned(),
        schema: None,
        columns: Some(vec![ColumnInfo {
            name: "id".to_owned(),
            type_name: "integer".to_owned(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        }]),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
        storage_hints: None,
    }
}

/// Opens a diagram of two tables as the window's root view, with keyboard
/// focus on the diagram viewport.
fn focused_diagram(cx: &mut TestAppContext) -> (Entity<SchemaVizDocument>, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    cx.update(init_keymap);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    let (host, window) = cx.add_window_view(|window, cx| Host {
        document: cx.new(|cx| {
            SchemaVizDocument::new(
                Uuid::nil(),
                None,
                SchemaVizMode::Global,
                app_state,
                window,
                cx,
            )
        }),
    });
    window.run_until_parked();
    let document = window.update(|_, cx| host.read(cx).document.clone());

    // The test profile has no connection, so the tab's own load fails; the
    // graph is installed directly once that load has settled.
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.graph = Some(SchemaGraph::build(&[table("users"), table("orders")]));
            document.recompute_layout();
            document.load_status = LoadStatus::Ready;
            document.focus_handle.focus(window, cx);
            cx.notify();
        });
    });
    window.run_until_parked();

    (document, window)
}

fn pan_offset(
    window: &mut VisualTestContext,
    document: &Entity<SchemaVizDocument>,
) -> Point<Pixels> {
    window.update(|_, cx| document.read(cx).pan_offset)
}

fn zoom(window: &mut VisualTestContext, document: &Entity<SchemaVizDocument>) -> f32 {
    window.update(|_, cx| document.read(cx).zoom)
}

#[gpui::test]
fn pan_and_zoom_keys_move_the_camera(cx: &mut TestAppContext) {
    let (document, window) = focused_diagram(cx);
    let start = pan_offset(window, &document);

    window.simulate_keystrokes("h");
    assert_eq!(
        pan_offset(window, &document).x,
        start.x + px(50.0),
        "h pans left"
    );

    window.simulate_keystrokes("down");
    assert_eq!(
        pan_offset(window, &document).y,
        start.y - px(50.0),
        "Down pans down"
    );

    // Each press pans again, so a repeated key keeps moving the camera.
    window.simulate_keystrokes("l l");
    assert_eq!(
        pan_offset(window, &document).x,
        start.x - px(50.0),
        "l pans right"
    );

    let before_zoom = zoom(window, &document);
    window.simulate_keystrokes("=");
    assert_eq!(zoom(window, &document), before_zoom * 1.25, "= zooms in");

    window.simulate_keystrokes("shift-=");
    assert_eq!(
        zoom(window, &document),
        before_zoom * 1.25 * 1.25,
        "Shift+= zooms in"
    );

    window.simulate_keystrokes("-");
    assert_eq!(zoom(window, &document), before_zoom * 1.25, "- zooms out");
}

#[gpui::test]
fn selection_move_and_layout_keys_edit_the_diagram(cx: &mut TestAppContext) {
    let (document, window) = focused_diagram(cx);

    window.simulate_keystrokes("shift-l");
    let selected = window
        .update(|_, cx| document.read(cx).selected_node)
        .expect("Shift+L selects a table when none is selected");

    let origin = window.update(|_, cx| {
        let node = &document.read(cx).layout.as_ref().expect("layout").nodes[&selected];
        Point::new(node.x, node.y)
    });
    window.simulate_keystrokes("alt-l");
    let moved = window.update(|_, cx| {
        document
            .read(cx)
            .node_position_overrides
            .get(&selected)
            .copied()
    });
    assert_eq!(
        moved,
        Some(Point::new(
            snap_to_lattice(origin.x + 20.0),
            snap_to_lattice(origin.y)
        )),
        "Alt+L moves the selected table right",
    );

    window.simulate_keystrokes("s");
    assert_eq!(
        window.update(|_, cx| document.read(cx).layout_format),
        LayoutFormat::Snowflake,
        "s switches to the snowflake layout",
    );

    window.simulate_keystrokes("escape");
    assert_eq!(
        window.update(|_, cx| document.read(cx).selected_node),
        None,
        "Escape clears the selection",
    );
}

#[gpui::test]
fn context_menu_keys_navigate_and_close_the_menu(cx: &mut TestAppContext) {
    let (document, window) = focused_diagram(cx);
    let pan_before = pan_offset(window, &document);

    window.simulate_keystrokes("m");
    let selected_index = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            document
                .read(cx)
                .context_menu
                .as_ref()
                .map(|menu| menu.selected_index)
        })
    };
    assert_eq!(selected_index(window), Some(0), "m opens the context menu");

    window.simulate_keystrokes("j");
    assert_eq!(
        selected_index(window),
        Some(1),
        "j moves the menu highlight down"
    );

    window.simulate_keystrokes("escape");
    assert_eq!(selected_index(window), None, "Escape closes the menu");
    assert_eq!(
        pan_offset(window, &document),
        pan_before,
        "menu keys must not also pan the diagram",
    );
}

/// The toolbar's Reset, Fit, Arrange, Types and Indexes controls are also
/// entries of the `m` menu, so the keyboard reaches them.
#[gpui::test]
fn the_context_menu_lists_the_toolbar_view_controls(cx: &mut TestAppContext) {
    let (document, window) = focused_diagram(cx);

    window.simulate_keystrokes("= l");
    assert_ne!(zoom(window, &document), 1.0);

    // Zoom in, Zoom out, then Reset view.
    window.simulate_keystrokes("m j j enter");
    assert_eq!(zoom(window, &document), 1.0, "Reset view restores 100%");
    assert_eq!(
        pan_offset(window, &document),
        Point::default(),
        "Reset view recenters the camera"
    );

    let show_types = window.update(|_, cx| document.read(cx).show_types);
    // Zoom in, Zoom out, Reset view, Fit, Layout, Arrange, Copy as, then
    // Show column types (separators are skipped).
    window.simulate_keystrokes("m j j j j j j j enter");
    assert_eq!(
        window.update(|_, cx| document.read(cx).show_types),
        !show_types,
        "Show column types flips the Types toggle"
    );

    window.simulate_keystrokes("m j j j j j j j j enter");
    assert_eq!(
        window.update(|_, cx| document.read(cx).show_indexes),
        true,
        "Show indexes flips the Indexes toggle"
    );
}
