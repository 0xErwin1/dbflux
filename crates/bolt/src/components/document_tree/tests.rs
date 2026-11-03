use std::sync::{Arc, Mutex};

use super::row::{
    DocumentTreeRow, DocumentTreeRowHost, DocumentTreeRowView, RowPaint, TreeRowMetrics,
    render_row, row_paint,
};
use super::{NodeId, ValueTypeClass, value_type_color};
use crate::accessibility::{FrameCapture, accessible_name};
use crate::primitives::{Badge, BadgeTone};
use crate::tokens::{ChromeColors, DocumentTreeMetrics, ValueTypeColors};
use gpui::{
    AccessibilityFrame, App, IntoElement as _, Modifiers, TestAppContext, VisualTestContext, hsla,
    point, px,
};
use gpui_component::ActiveTheme;

/// A neutral brand at the default palette and density, so the tests render
/// against the same theme engine the products install.
fn install_test_theme(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });
}

/// The paint decisions of one row, computed against the live theme. Returns
/// the decision and the theme it was computed against, so assertions compare
/// against the same values.
fn paint_of(
    cx: &mut TestAppContext,
    is_cursor: bool,
    is_current_match: bool,
    is_search_match: bool,
    value_class: ValueTypeClass,
) -> (RowPaint, gpui_component::Theme) {
    install_test_theme(cx);
    let paint = std::rc::Rc::new(std::cell::RefCell::new(None));
    let paint_for_assert = paint.clone();
    cx.update(move |cx| {
        let theme = cx.theme().clone();
        paint_for_assert.replace(Some((
            row_paint(
                is_cursor,
                is_current_match,
                is_search_match,
                value_class,
                &theme,
            ),
            theme,
        )));
    });
    paint
        .borrow()
        .clone()
        .expect("the paint decision must be computed")
}

/// The background precedence is the cursor, then the current search match,
/// then any search match, then nothing.
#[gpui::test]
fn the_row_background_precedes_cursor_then_current_match_then_match(cx: &mut TestAppContext) {
    let (cursor, theme, _) = {
        let (paint, theme) = paint_of(cx, true, false, false, ValueTypeClass::Text);
        (paint, theme, ())
    };
    assert_eq!(cursor.background, theme.selection, "the cursor row wins");
    assert_eq!(
        cursor.hover_background, theme.selection,
        "a hovered cursor row keeps its own background"
    );

    let (current, theme) = paint_of(cx, false, true, false, ValueTypeClass::Text);
    assert_eq!(current.background, theme.warning.opacity(0.4));
    assert_eq!(current.hover_background, theme.warning.opacity(0.5));

    let (matched, theme) = paint_of(cx, false, false, true, ValueTypeClass::Text);
    assert_eq!(matched.background, theme.warning.opacity(0.2));
    assert_eq!(matched.hover_background, theme.warning.opacity(0.3));

    let (ordinary, theme) = paint_of(cx, false, false, false, ValueTypeClass::Text);
    assert_eq!(ordinary.background, gpui::Hsla::transparent_black());
    assert_eq!(
        ordinary.hover_background,
        theme.secondary.opacity(0.5),
        "an ordinary row hovers with the secondary wash"
    );
}

/// The current match outranks an ordinary match on the same row.
#[gpui::test]
fn the_current_match_outranks_an_ordinary_match(cx: &mut TestAppContext) {
    let (both, theme) = paint_of(cx, false, true, true, ValueTypeClass::Text);
    assert_eq!(
        both.background,
        theme.warning.opacity(0.4),
        "the current match wash wins over the plain match wash"
    );
}

/// The value color per class: the JSON type colors for the classes no theme
/// role expresses, the theme's own roles for the rest. These are the values
/// the product's type-color table committed to before the painter moved.
#[gpui::test]
fn the_value_type_colors_pin_the_json_type_colors(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let theme = cx.update(|cx| cx.theme().clone());

    assert_eq!(
        value_type_color(ValueTypeClass::Boolean, &theme),
        hsla(280.0 / 360.0, 0.6, 0.6, 1.0), // guardrail-allow: pinning the moved constant
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Integer, &theme),
        hsla(120.0 / 360.0, 0.5, 0.5, 1.0), // guardrail-allow: pinning the moved constant
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Float, &theme),
        hsla(150.0 / 360.0, 0.5, 0.5, 1.0), // guardrail-allow: pinning the moved constant
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Text, &theme),
        hsla(30.0 / 360.0, 0.7, 0.6, 1.0), // guardrail-allow: pinning the moved constant
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Temporal, &theme),
        hsla(200.0 / 360.0, 0.6, 0.5, 1.0), // guardrail-allow: pinning the moved constant
    );

    assert_eq!(
        value_type_color(ValueTypeClass::Null, &theme),
        theme.muted_foreground,
        "null and json read the muted color"
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Json, &theme),
        theme.muted_foreground
    );
    assert_eq!(
        value_type_color(ValueTypeClass::ObjectId, &theme),
        ChromeColors::tint(&theme)
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Bytes, &theme),
        theme.warning
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Other, &theme),
        theme.foreground
    );
    assert_eq!(
        value_type_color(ValueTypeClass::Container, &theme),
        theme.muted_foreground,
        "documents and arrays read the muted color"
    );

    // The token constants and the painter's answers are the same table.
    assert_eq!(
        ValueTypeColors::TEXT,
        value_type_color(ValueTypeClass::Text, &theme)
    );
}

/// The row metrics follow the grid font size: the painter scales the row
/// height and the indent step with the grid font, so at the default grid
/// size they equal the token values unscaled.
#[gpui::test]
fn the_row_metrics_follow_the_grid_font_size(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let (metrics, row_height, indent, chevron_box) = cx.update(|cx| {
        (
            TreeRowMetrics::for_grid(cx),
            crate::fonts::grid_scaled(cx, DocumentTreeMetrics::ROW_HEIGHT),
            crate::fonts::grid_scaled(cx, DocumentTreeMetrics::INDENT),
            crate::fonts::grid_px(cx, crate::tokens::Heights::ICON_SM),
        )
    });

    assert_eq!(
        metrics.row_height, row_height,
        "the row height is grid-scaled"
    );
    assert_eq!(metrics.indent, indent, "the indent step is grid-scaled");
    assert_eq!(
        metrics.chevron_box, chevron_box,
        "the chevron box is the small icon size"
    );
}

/// The node id the painter keys rows and callbacks by: paths build down from
/// a document root and climb back up losslessly.
#[test]
fn node_ids_build_paths_down_and_climb_back_up() {
    let root = NodeId::root(2);
    assert_eq!(root.path, vec!["2".to_string()]);
    assert!(root.is_root());
    assert_eq!(root.depth(), 0);
    assert_eq!(root.doc_index(), Some(2));
    assert_eq!(root.parent(), None);

    let city = root.child("address").child("city");
    assert_eq!(
        city.path,
        vec!["2".to_string(), "address".to_string(), "city".to_string()]
    );
    assert!(!city.is_root());
    assert_eq!(city.depth(), 2);
    assert_eq!(
        city.doc_index(),
        Some(2),
        "doc_index reads the first path segment, whatever the depth"
    );

    let address = city.parent().expect("a nested node has a parent");
    assert_eq!(address.path, vec!["2".to_string(), "address".to_string()]);
    assert_eq!(address.parent().expect("the root's parent"), root);
}

// --- The rendered row -------------------------------------------------------

/// The id the painter gives a row's element: the same format the row builder
/// uses, so assertions address the drawn row.
fn row_node_id(id: &NodeId) -> String {
    format!("tree-row-{:?}", id.path)
}

fn tree_host() -> DocumentTreeRowHost {
    DocumentTreeRowHost {
        chevron_expanded: gpui_component::IconName::ChevronDown.into(),
        chevron_collapsed: gpui_component::IconName::ChevronRight.into(),
        on_row_click: Arc::new(|_, _, _, _| {}),
        on_row_context: Arc::new(|_, _, _, _| {}),
        on_chevron_click: Arc::new(|_, _, _, _| {}),
        on_value_click: Arc::new(|_, _, _, _| {}),
    }
}

/// A leaf row whose drawn text is the key `status`, the value `created` and
/// the type label `string`.
fn leaf_view() -> DocumentTreeRowView {
    DocumentTreeRowView {
        id: NodeId::root(7),
        depth: 1,
        key: "status".into(),
        value_text: "created".into(),
        value_class: ValueTypeClass::Text,
        type_label: "string",
        is_expandable: false,
        is_expanded: false,
        is_value_expanded: false,
        is_cursor: false,
        is_current_match: false,
        is_search_match: false,
        is_editing: false,
        inline_edit_input: None,
    }
}

/// Renders one row alone in a themed test window of a fixed width and
/// returns its accessibility frame and the window, so assertions read both
/// the announced tree and the laid-out bounds.
fn row_window(
    cx: &mut TestAppContext,
    build: fn(&mut App) -> gpui::AnyElement,
) -> (AccessibilityFrame, &mut VisualTestContext) {
    struct Surface(fn(&mut App) -> gpui::AnyElement);

    impl gpui::Render for Surface {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{InteractiveElement as _, ParentElement as _, Styled as _};

            gpui::div().size_full().child(
                gpui::div()
                    .debug_selector(|| "row-under-test".to_string())
                    .w(gpui::px(400.0))
                    .child((self.0)(cx)),
            )
        }
    }

    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |_, _| Surface(build));
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (frame, window)
}

fn plain_row(_cx: &mut App) -> gpui::AnyElement {
    let host = tree_host();
    render_row(leaf_view(), &host, _cx).into_any_element()
}

fn trailing_row(_cx: &mut App) -> gpui::AnyElement {
    let host = tree_host();
    render_row(
        DocumentTreeRow::new(leaf_view())
            .trailing(Badge::new("201", BadgeTone::Success).id("saved-example-status-7")),
        &host,
        _cx,
    )
    .into_any_element()
}

/// A row without a trailing element renders exactly its own text — the key,
/// the value and the type badge — and no status slot.
#[gpui::test]
fn a_row_without_a_trailing_element_renders_its_own_text_alone(cx: &mut TestAppContext) {
    let (frame, _window) = row_window(cx, plain_row);
    let view = leaf_view();

    let row = accessible_name(&frame, &row_node_id(&view.id)).expect("the row renders");
    assert!(row.contains("status"), "the key is drawn: {row}");
    assert!(row.contains("created"), "the value is drawn: {row}");
    assert!(row.contains("string"), "the type badge is drawn: {row}");
    assert!(
        frame
            .nodes()
            .all(|(_, node)| node.id() != "saved-example-status-7"),
        "a row without a trailing element draws no status slot"
    );
}

/// A row with a trailing element carries it under the id the caller
/// supplied, draws its label, and keeps drawing the row's own text; the slot
/// sits at the row's end, past the middle of the row.
#[gpui::test]
fn a_row_with_a_trailing_element_carries_it_under_the_callers_id(cx: &mut TestAppContext) {
    let (frame, window) = row_window(cx, trailing_row);
    let view = leaf_view();

    assert_eq!(
        accessible_name(&frame, "saved-example-status-7").as_deref(),
        Some("201"),
        "the trailing slot carries the caller's id and its label"
    );
    let row = accessible_name(&frame, &row_node_id(&view.id)).expect("the row renders");
    assert!(row.contains("201"), "the trailing element draws in the row");

    let row_bounds = window
        .debug_bounds("row-under-test")
        .expect("the row is laid out");
    let status = window
        .debug_bounds("saved-example-status-7")
        .expect("the trailing slot is laid out");
    assert!(
        status.right() <= row_bounds.right(),
        "the slot stays inside the row"
    );
    assert!(
        status.left() > row_bounds.center().x,
        "the slot sits at the row's end, right-aligned"
    );
}

/// One trailing row in a themed window whose row clicks are recorded, so a
/// test can tell whether a click reached the row's handler.
struct ClickRecordingSurface {
    row_clicks: Arc<Mutex<Vec<String>>>,
}

impl gpui::Render for ClickRecordingSurface {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        use gpui::{InteractiveElement as _, ParentElement as _, Styled as _};

        let mut host = tree_host();
        let row_clicks = self.row_clicks.clone();
        host.on_row_click = Arc::new(move |id, _, _, _| {
            row_clicks
                .lock()
                .expect("row click recorder lock")
                .push(format!("{id:?}"));
        });
        gpui::div().size_full().child(
            gpui::div()
                .debug_selector(|| "row-under-test".to_string())
                .w(gpui::px(400.0))
                .child(
                    render_row(
                        DocumentTreeRow::new(leaf_view()).trailing(
                            Badge::new("201", BadgeTone::Success).id("saved-example-status-7"),
                        ),
                        &host,
                        cx,
                    )
                    .into_any_element(),
                ),
        )
    }
}

/// A click on the trailing element stops at the slot: the row's handler
/// never hears it, while a click on the row itself still reaches it.
#[gpui::test]
fn a_click_on_the_trailing_element_does_not_reach_the_row(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let row_clicks = Arc::new(Mutex::new(Vec::new()));
    let (_view, window) = cx.add_window_view({
        let row_clicks = row_clicks.clone();
        move |_, _| ClickRecordingSurface { row_clicks }
    });
    window.run_until_parked();

    let status = window
        .debug_bounds("saved-example-status-7")
        .expect("the trailing slot is laid out");
    window.simulate_click(status.center(), Modifiers::default());
    assert!(
        row_clicks
            .lock()
            .expect("row click recorder lock")
            .is_empty(),
        "a click on the trailing element never reaches the row"
    );

    // The row's own surface still reports: a click in the leading indent,
    // clear of the value, the badge and the slot, reaches the row.
    let row = window
        .debug_bounds("row-under-test")
        .expect("the row is laid out");
    window.simulate_click(
        point(row.left() + px(2.0), row.center().y),
        Modifiers::default(),
    );
    assert_eq!(
        row_clicks.lock().expect("row click recorder lock").len(),
        1,
        "a click on the row itself reaches the row's handler"
    );
}
