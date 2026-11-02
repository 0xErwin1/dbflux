use super::row::{RowPaint, TreeRowMetrics, row_paint};
use super::{NodeId, ValueTypeClass, value_type_color};
use crate::tokens::{ChromeColors, DocumentTreeMetrics, ValueTypeColors};
use gpui::{TestAppContext, hsla};
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
