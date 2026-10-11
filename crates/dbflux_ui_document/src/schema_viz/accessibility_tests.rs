//! Accessible names of the schema diagram's own controls.
//!
//! Kept out of `mod.rs` for the same compile-time reason as `tests.rs` and
//! `rail_tests.rs`, with explicit imports rather than `use super::*`.

use super::keymap_tests::focused_diagram;
use dbflux_components::accessibility::{FrameCapture, accessibility_node, accessible_name};
use gpui::{Entity, Role, TestAppContext, Toggled};

/// The Types and Indexes switches of the diagram toolbar are independent
/// toggles that draw their text beside a thumb, so each one is a switch named
/// by that text and carrying its own on/off state.
#[gpui::test]
fn the_toolbar_toggles_are_named_switches_with_their_state(cx: &mut TestAppContext) {
    let (document, window) = focused_diagram(cx);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let types = dbflux_i18n::t!("document.schema_viz.toolbar.types").to_string();
    let indexes = dbflux_i18n::t!("document.schema_viz.toolbar.indexes").to_string();

    assert_eq!(
        accessible_name(&frame, "schema-viz-show-types"),
        Some(types),
        "the Types toggle is named by the text beside it"
    );
    assert_eq!(
        accessible_name(&frame, "schema-viz-show-indexes"),
        Some(indexes)
    );

    let types_node = accessibility_node(&frame, "schema-viz-show-types")
        .expect("the Types toggle is an accessible node");
    assert_eq!(types_node.role(), Role::Switch);
    assert_eq!(types_node.toggled(), Some(Toggled::True));

    let indexes_node = accessibility_node(&frame, "schema-viz-show-indexes")
        .expect("the Indexes toggle is an accessible node");
    assert_eq!(indexes_node.role(), Role::Switch);
    assert_eq!(
        indexes_node.toggled(),
        Some(Toggled::False),
        "indexes start hidden"
    );

    turn_indexes_on(window, &document);
    let frame = capture.frame(window);
    let indexes_node = accessibility_node(&frame, "schema-viz-show-indexes")
        .expect("the Indexes toggle is an accessible node");

    assert_eq!(
        indexes_node.toggled(),
        Some(Toggled::True),
        "turning the group on is announced"
    );
}

fn turn_indexes_on(
    window: &mut gpui::VisualTestContext,
    document: &Entity<super::SchemaVizDocument>,
) {
    window.update(|_, cx| document.update(cx, |document, cx| document.set_show_indexes(true, cx)));
}
