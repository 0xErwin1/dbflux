//! Layout regression tests for the `Label` primitive inside a fixed-width
//! label column, the shape used by form rows such as the connection manager's.
//!
//! A label longer than its column must wrap inside the column instead of
//! painting past it and over the input that follows.

use dbflux_components::primitives::Label;
use dbflux_components::theme;
use gpui::prelude::*;
use gpui::{Context, TestAppContext, Window, div, px};

const LABEL_COLUMN_WIDTH: gpui::Pixels = px(140.0);

struct LabelColumnHarness;

impl LabelColumnHarness {
    fn row(selector: &'static str, label: &'static str, required: bool) -> gpui::Div {
        div()
            .flex()
            .items_start()
            .w(px(400.0))
            .child(
                div()
                    .debug_selector(move || selector.to_string())
                    .w(LABEL_COLUMN_WIDTH)
                    .flex_shrink_0()
                    .child(Label::new(label).required(required)),
            )
            .child(div().flex_1().min_w_0().h(px(24.0)))
    }
}

impl Render for LabelColumnHarness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .child(Self::row("short-label-column", "Host", false))
            .child(Self::row(
                "long-label-column",
                "Default database (optional)",
                false,
            ))
            .child(Self::row(
                "long-required-label-column",
                "Sentinel Master Name",
                true,
            ))
    }
}

#[gpui::test]
fn long_labels_wrap_inside_their_column(cx: &mut TestAppContext) {
    cx.update(theme::init);

    let (_, window) = cx.add_window_view(|_, _| LabelColumnHarness);

    let short = window
        .debug_bounds("short-label-column")
        .expect("short label column should render");
    let long = window
        .debug_bounds("long-label-column")
        .expect("long label column should render");
    let long_required = window
        .debug_bounds("long-required-label-column")
        .expect("long required label column should render");

    assert_eq!(short.size.width, LABEL_COLUMN_WIDTH);
    assert_eq!(long.size.width, LABEL_COLUMN_WIDTH);

    assert!(
        long.size.height > short.size.height,
        "a label wider than its column must wrap onto another line: short {:?}, long {:?}",
        short.size,
        long.size
    );
    assert!(
        long_required.size.height > short.size.height,
        "a required label wider than its column must wrap onto another line: short {:?}, long {:?}",
        short.size,
        long_required.size
    );
}
