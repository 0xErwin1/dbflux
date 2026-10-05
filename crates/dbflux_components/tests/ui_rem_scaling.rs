//! Layout tests for interface lengths expressed in rems with `tokens::ui`.
//!
//! Every window renders under gpui-component's `Root`, which sets the rem size
//! from the theme font size, and the theme derives that size from the
//! interface font size. A length built with `ui(..)` must therefore keep its
//! design size at the default interface size and grow with the interface scale.

use dbflux_components::controls::Button;
use dbflux_components::fonts::{self, FontSettings};
use dbflux_components::icon::IconSource;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::theme;
use dbflux_components::tokens::{BASE_REM, ButtonMetrics, ui};
use gpui::prelude::*;
use gpui::{Bounds, Context, Pixels, TestAppContext, VisualTestContext, Window, div, px};

const ROW_HEIGHT: f32 = 30.0;
const ICON_SIZE: f32 = 14.0;
const TEXT_SIZE: f32 = 13.0;

/// Interface size that doubles the default 13 px.
const DOUBLE_UI_SIZE: f32 = 26.0;

struct ScaledHarness;

impl Render for ScaledHarness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .items_start()
            .child(
                div()
                    .debug_selector(|| "ui-row".to_string())
                    .w(ui(ROW_HEIGHT))
                    .h(ui(ROW_HEIGHT)),
            )
            .child(div().debug_selector(|| "ui-icon".to_string()).flex().child(
                Icon::new(IconSource::Svg("icons/scaling-probe.svg".into())).size(ui(ICON_SIZE)),
            ))
            .child(
                div()
                    .debug_selector(|| "ui-text".to_string())
                    .child(Text::body("Ag").font_size(ui(TEXT_SIZE))),
            )
            .child(
                div()
                    .debug_selector(|| "ui-button".to_string())
                    .flex()
                    .child(Button::new("scaling-probe-button", "Save")),
            )
    }
}

fn open_window(cx: &mut TestAppContext) -> &mut VisualTestContext {
    cx.update(theme::init);

    let (_, window) = cx.add_window_view(|window, cx| {
        let harness = cx.new(|_| ScaledHarness);
        gpui_component::Root::new(harness, window, cx)
    });
    window.run_until_parked();

    window
}

fn bounds(window: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
    window
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("{selector} should be laid out"))
}

fn set_ui_size(window: &mut VisualTestContext, ui_size: f32) {
    window.update(|_, cx| {
        fonts::set(
            cx,
            FontSettings {
                ui_size,
                ..FontSettings::default()
            },
        );
        theme::apply_fonts(cx);
        cx.refresh_windows();
    });
    window.run_until_parked();
}

#[gpui::test]
fn ui_lengths_keep_their_design_size_at_the_default_interface_size(cx: &mut TestAppContext) {
    let window = open_window(cx);

    let row = bounds(window, "ui-row");
    let icon = bounds(window, "ui-icon");

    assert_eq!(row.size.height, px(ROW_HEIGHT));
    assert_eq!(row.size.width, px(ROW_HEIGHT));
    assert_eq!(icon.size.height, px(ICON_SIZE));
    assert_eq!(
        bounds(window, "ui-button").size.height,
        ButtonMetrics::HEIGHT.to_pixels(px(BASE_REM))
    );
}

#[gpui::test]
fn ui_lengths_double_when_the_interface_size_doubles(cx: &mut TestAppContext) {
    let window = open_window(cx);
    let default_text = bounds(window, "ui-text");
    let default_button = bounds(window, "ui-button");

    set_ui_size(window, DOUBLE_UI_SIZE);

    let row = bounds(window, "ui-row");
    let icon = bounds(window, "ui-icon");
    let text = bounds(window, "ui-text");

    assert_eq!(row.size.height, px(ROW_HEIGHT * 2.0));
    assert_eq!(row.size.width, px(ROW_HEIGHT * 2.0));
    assert_eq!(icon.size.height, px(ICON_SIZE * 2.0));
    assert_eq!(
        bounds(window, "ui-button").size.height,
        default_button.size.height * 2.0
    );

    let text_ratio = f32::from(text.size.height) / f32::from(default_text.size.height);
    assert!(
        (text_ratio - 2.0).abs() < 0.1,
        "text line height should double: default {:?}, scaled {:?}",
        default_text.size,
        text.size
    );
}
