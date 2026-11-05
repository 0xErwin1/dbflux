use gpui::{Context, IntoElement, Render, TestAppContext, VisualTestContext, Window, div, px};

use super::{ValueField, value_color, value_font};
use crate::accessibility::{FrameCapture, accessible_name};
use crate::tokens::{BASE_REM, Fields};
use gpui_component::ActiveTheme as _;

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

/// A single value field in a 300 px row.
struct Harness {
    value: gpui::SharedString,
    mono: bool,
    with_id: bool,
}

impl Render for Harness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        let field = ValueField::new(self.value.clone()).mono(self.mono);
        let field = if self.with_id {
            field.id("proxy-ca-path")
        } else {
            field
        };

        div()
            .size_full()
            .child(div().flex().w(px(300.0)).child(field))
    }
}

fn open(
    cx: &mut TestAppContext,
    mono: bool,
    with_id: bool,
) -> (&mut VisualTestContext, gpui::AccessibilityFrame) {
    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |_, _| Harness {
        value: "/var/lib/proxy.pem".into(),
        mono,
        with_id,
    });
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (window, frame)
}

fn open_with(
    value: impl Into<gpui::SharedString>,
    cx: &mut TestAppContext,
) -> (&mut VisualTestContext, gpui::AccessibilityFrame) {
    install_test_theme(cx);
    let value = value.into();
    let (_, window) = cx.add_window_view(move |_, _| Harness {
        value,
        mono: false,
        with_id: true,
    });
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (window, frame)
}

/// The field draws at the height, inset and width of a text field, and the
/// value sits `Fields::PADDING_X` inside its left edge.
#[gpui::test]
fn a_value_field_draws_the_field_metrics_around_its_value(cx: &mut TestAppContext) {
    let (window, frame) = open(cx, false, true);
    let rem = px(BASE_REM);
    let field = window
        .debug_bounds("proxy-ca-path")
        .expect("the field is laid out");
    let value = window
        .debug_bounds("proxy-ca-path-value")
        .expect("the value is laid out");

    assert_eq!(field.size.height, Fields::HEIGHT.to_pixels(rem));
    assert_eq!(field.size.width, px(300.0), "the field fills its row");
    assert_eq!(
        value.left(),
        field.left() + Fields::PADDING_X,
        "the value sits at the field's inset"
    );
    assert_eq!(
        accessible_name(&frame, "proxy-ca-path-value").as_deref(),
        Some("/var/lib/proxy.pem")
    );
}

/// Without an id the field draws no accessibility node at all.
#[gpui::test]
fn a_value_field_without_an_id_draws_no_node(cx: &mut TestAppContext) {
    let (window, frame) = open(cx, false, false);

    assert!(
        frame
            .nodes()
            .all(|(_, node)| !node.id().starts_with("proxy-ca-path")),
        "an unnamed field carries no id"
    );
    assert!(window.debug_bounds("proxy-ca-path-value").is_none());
}

/// The value draws in the interface face unless the caller asks for the
/// editor face, and in the strong value color unless the caller sets one.
#[gpui::test]
fn a_value_field_draws_the_interface_face_in_the_value_color(cx: &mut TestAppContext) {
    install_test_theme(cx);

    cx.update(|cx| {
        assert_eq!(value_font(false, cx), crate::fonts::ui_family(cx));
        assert_eq!(value_font(true, cx), crate::fonts::editor_family(cx));

        let theme = cx.theme().clone();
        assert_eq!(
            value_color(&theme, None),
            crate::tokens::ChromeColors::strong(&theme)
        );
        assert_eq!(value_color(&theme, Some(theme.danger)), theme.danger);
    });
}

/// A value with a line break draws on one line: the break reads as a space,
/// so nothing draws past the field's height.
#[gpui::test]
fn a_value_field_draws_one_line_whatever_breaks_the_value(cx: &mut TestAppContext) {
    let (window, frame) = open_with("first\r\nsecond\nthird", cx);

    let field = window
        .debug_bounds("proxy-ca-path")
        .expect("the field is laid out");
    let value = window
        .debug_bounds("proxy-ca-path-value")
        .expect("the value is laid out");

    assert!(
        value.size.height <= field.size.height,
        "the value stays inside the field's height"
    );
    assert_eq!(
        accessible_name(&frame, "proxy-ca-path-value").as_deref(),
        Some("first second third"),
        "a break reads as one space"
    );
}
