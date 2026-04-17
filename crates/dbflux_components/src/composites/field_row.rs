use gpui::prelude::*;
use gpui::{div, px, App, Pixels, SharedString};

use crate::primitives::{Label, Text};
use crate::tokens::Spacing;

/// Default label width for horizontal field rows.
const DEFAULT_LABEL_WIDTH: Pixels = px(140.0);

/// Render a standard form field row: label on the left, control on the right.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn field_row(label: impl Into<SharedString>, control: impl IntoElement, cx: &App) -> gpui::Div {
    field_row_inner(label, control, None, DEFAULT_LABEL_WIDTH, cx)
}

/// Render a form field row with a muted description below the control.
pub fn field_row_with_desc(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: impl Into<SharedString>,
    cx: &App,
) -> gpui::Div {
    field_row_inner(label, control, Some(desc.into()), DEFAULT_LABEL_WIDTH, cx)
}

/// Render a form field row with a custom label width.
///
/// Use this when the default 140px label width doesn't fit the content.
pub fn field_row_with_label_width(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    label_width: Pixels,
    cx: &App,
) -> gpui::Div {
    field_row_inner(label, control, None, label_width, cx)
}

/// Render a vertical form field row: label above the control.
///
/// Use this for narrow containers or when controls need full width.
pub fn field_row_vertical(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    field_row_vertical_inner(label, control, None, cx)
}

/// Render a vertical form field row with a muted description below the control.
pub fn field_row_vertical_with_desc(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: impl Into<SharedString>,
    cx: &App,
) -> gpui::Div {
    field_row_vertical_inner(label, control, Some(desc.into()), cx)
}

fn field_row_inner(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: Option<SharedString>,
    label_width: Pixels,
    _cx: &App,
) -> gpui::Div {
    let label_el = Label::new(label);

    let mut control_col = div().flex_1().child(control);

    if let Some(desc_text) = desc {
        control_col = control_col.child(div().mt(px(2.0)).child(Text::caption(desc_text)));
    }

    div()
        .flex()
        .items_start()
        .gap(Spacing::MD)
        .child(
            div()
                .w(label_width)
                .pt(px(6.0))
                .flex_shrink_0()
                .child(label_el),
        )
        .child(control_col)
}

fn field_row_vertical_inner(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: Option<SharedString>,
    _cx: &App,
) -> gpui::Div {
    let label_el = Label::new(label);

    let mut col = div()
        .flex_col()
        .gap(px(4.0))
        .child(label_el)
        .child(div().w_full().child(control));

    if let Some(desc_text) = desc {
        col = col.child(div().mt(px(2.0)).child(Text::caption(desc_text)));
    }

    div().flex().flex_col().child(col)
}
