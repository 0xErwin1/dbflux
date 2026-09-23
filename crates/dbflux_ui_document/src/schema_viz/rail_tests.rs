//! GPUI tests for how a schema diagram tab hands its inspector to the shared
//! workspace rail. Kept out of `mod.rs` for the same compile-time reason as
//! `tests.rs`, with explicit imports instead of `use super::*`.

use super::{SchemaVizDocument, SchemaVizMode};
use crate::handle::DocumentEvent;
use dbflux_components::theme;
use dbflux_core::{ColumnInfo, TableInfo};
use dbflux_schema_viz::graph::SchemaGraph;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::Cell;
use std::rc::Rc;
use uuid::Uuid;

fn init_test_runtime(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    })
}

fn users_table() -> TableInfo {
    TableInfo {
        name: "users".to_owned(),
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

/// Builds a diagram tab and opens the schema inspector on its only table, the
/// way a double click or the context menu does.
///
/// The test profile has no connection, so the tab's own load fails; the graph
/// is installed directly once that load has settled.
fn diagram_with_open_inspector(
    window: &mut VisualTestContext,
    app_state: Entity<AppStateEntity>,
) -> Entity<SchemaVizDocument> {
    let document = window.update(|window, cx| {
        cx.new(|cx| {
            SchemaVizDocument::new(
                Uuid::nil(),
                None,
                SchemaVizMode::Global,
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let graph = SchemaGraph::build(&[users_table()]);
            let node_index = graph
                .nodes()
                .map(|(index, _)| index)
                .next()
                .expect("the graph holds the table");
            document.graph = Some(graph);

            document.open_schema_inspector(node_index, cx);
        });
    });
    window.run_until_parked();

    document
}

/// Counts the `OpenInspector` events `document` emits from now on.
fn count_inspector_opens(
    window: &mut VisualTestContext,
    document: &Entity<SchemaVizDocument>,
) -> Rc<Cell<usize>> {
    let opens = Rc::new(Cell::new(0));
    let sink = opens.clone();

    window.update(|_, cx| {
        cx.subscribe(document, move |_, event: &DocumentEvent, _| {
            if matches!(event, DocumentEvent::OpenInspector { .. }) {
                sink.set(sink.get() + 1);
            }
        })
        .detach();
    });

    opens
}

fn activate(window: &mut VisualTestContext, document: &Entity<SchemaVizDocument>) {
    window.update(|_, cx| {
        document.update(cx, |document, cx| document.set_active_tab(true, cx));
    });
    window.run_until_parked();
}

/// Returning to a diagram tab re-mounts the schema inspector it had open,
/// since the workspace hid the rail when the tab became active.
#[gpui::test]
fn activating_a_diagram_tab_remounts_its_open_inspector(cx: &mut TestAppContext) {
    let app_state = init_test_runtime(cx);
    let window = cx.add_empty_window();
    let document = diagram_with_open_inspector(window, app_state);
    let opens = count_inspector_opens(window, &document);

    activate(window, &document);

    assert_eq!(opens.get(), 1, "the open schema inspector must come back");
}

/// A rail the user dismissed stays closed when the tab becomes active again.
#[gpui::test]
fn activating_after_the_user_closed_the_rail_does_not_remount_it(cx: &mut TestAppContext) {
    let app_state = init_test_runtime(cx);
    let window = cx.add_empty_window();
    let document = diagram_with_open_inspector(window, app_state);

    window.update(|_, cx| {
        document.update(cx, |document, _cx| document.mark_inspector_closed());
    });
    let opens = count_inspector_opens(window, &document);

    activate(window, &document);

    assert_eq!(opens.get(), 0, "a dismissed rail must not come back");
}

/// A diagram tab that never opened its inspector owns nothing in the rail.
#[gpui::test]
fn activating_a_diagram_tab_without_an_inspector_mounts_nothing(cx: &mut TestAppContext) {
    let app_state = init_test_runtime(cx);
    let window = cx.add_empty_window();
    let document = window.update(|window, cx| {
        cx.new(|cx| {
            SchemaVizDocument::new(
                Uuid::nil(),
                None,
                SchemaVizMode::Global,
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();
    let opens = count_inspector_opens(window, &document);

    activate(window, &document);

    assert_eq!(opens.get(), 0, "a tab without an inspector mounts nothing");
}
