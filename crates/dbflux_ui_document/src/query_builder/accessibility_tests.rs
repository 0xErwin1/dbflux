//! Accessible names of the query builder's checkboxes (issue #807).
//!
//! Kept out of `columns.rs` for the same compile-time reason as the sibling
//! test modules, with explicit imports rather than `use super::*`.

use super::panel::tests::{make_panel, make_spec, test_source};
use super::sections::columns::render_columns;
use super::{ProjectionMode, ProjectionRow, QueryBuilderPanel};
use dbflux_components::accessibility::{FrameCapture, accessibility_node, accessible_name};
use dbflux_components::theme;
use gpui::{
    AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Role, Styled as _,
    TestAppContext, VisualTestContext, Window, div,
};

struct ColumnsHarness {
    panel: Entity<QueryBuilderPanel>,
}

impl Render for ColumnsHarness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let section = self
            .panel
            .update(cx, |panel, cx| render_columns(panel, cx).into_any_element());
        div().size_full().child(section)
    }
}

fn open_columns(panel: QueryBuilderPanel, cx: &mut TestAppContext) -> &mut VisualTestContext {
    cx.update(gpui_component::init);
    cx.update(theme::init);

    let panel = cx.update(|cx| cx.new(|_| panel));
    let (_, window) = cx.add_window_view(move |window, cx| {
        let harness = cx.new(|_| ColumnsHarness {
            panel: panel.clone(),
        });
        gpui_component::Root::new(harness, window, cx)
    });
    window.run_until_parked();
    window
}

/// A panel in All mode draws only the "All columns (*)" checkbox.
fn all_columns_panel() -> QueryBuilderPanel {
    make_panel(make_spec(test_source()))
}

/// A panel listing one picked column with the picker open draws a checkbox
/// for every source column.
fn explicit_columns_panel() -> QueryBuilderPanel {
    let mut panel = make_panel(make_spec(test_source()));
    panel.projection_mode = ProjectionMode::Explicit;
    panel.projection_rows = vec![ProjectionRow {
        source_alias: "users".to_string(),
        column: "email".to_string(),
        alias: None,
    }];
    panel.available_columns = vec!["id".to_string(), "email".to_string(), "name".to_string()];
    panel.column_picker_open = true;
    panel
}

/// S3, S7: the "All columns" checkbox is named by the catalogue text drawn
/// next to it, exactly as the label renders it.
#[gpui::test]
fn the_all_columns_checkbox_is_named_by_the_catalogue_text(cx: &mut TestAppContext) {
    let window = open_columns(all_columns_panel(), cx);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let node = accessibility_node(&frame, "qb-all-columns")
        .expect("the all-columns checkbox is drawn and accessible");
    assert_eq!(node.role(), Role::CheckBox);
    assert_eq!(
        accessible_name(&frame, "qb-all-columns"),
        Some(dbflux_i18n::t!("document.query_builder.columns.all_columns").to_string()),
    );
}

/// S3, S7: every column toggle in the picker is named by the column name it
/// toggles, the text drawn beside it.
#[gpui::test]
fn each_column_toggle_is_named_by_the_column_it_toggles(cx: &mut TestAppContext) {
    let columns = ["id", "email", "name"];
    let window = open_columns(explicit_columns_panel(), cx);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    for (index, column) in columns.iter().enumerate() {
        let id = format!("qb-col-toggle-{index}");
        let node = accessibility_node(&frame, &id)
            .unwrap_or_else(|| panic!("the {column} toggle is drawn and accessible"));
        assert_eq!(node.role(), Role::CheckBox);
        assert_eq!(
            accessible_name(&frame, &id),
            Some(column.to_string()),
            "the {column} toggle must be named by the column it toggles",
        );
    }
}
