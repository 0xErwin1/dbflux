use gpui::prelude::*;
use gpui::{App, SharedString, div, px};

use crate::primitives::{Label, Text};
use crate::tokens::Spacing;

/// Render a standard form field row: label on the left, control on the right.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn field_row(label: impl Into<SharedString>, control: impl IntoElement, cx: &App) -> gpui::Div {
    field_row_inner(label, control, None, cx)
}

/// Render a form field row with a muted description below the control.
pub fn field_row_with_desc(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: impl Into<SharedString>,
    cx: &App,
) -> gpui::Div {
    field_row_inner(label, control, Some(desc.into()), cx)
}

fn field_row_inner(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: Option<SharedString>,
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
                .w(px(140.0))
                .pt(px(6.0))
                .flex_shrink_0()
                .child(label_el),
        )
        .child(control_col)
}
