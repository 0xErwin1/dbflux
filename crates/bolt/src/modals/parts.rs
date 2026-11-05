//! Body blocks shared by dialogs and wizards: the lead
//! sentence, the statement block, labelled fields and the framed list.

use std::ops::Range;

use gpui::prelude::*;
use gpui::{AnyElement, App, Div, SharedString, StyledText, div, relative};
use gpui_component::ActiveTheme;

use crate::controls::{one_line, value_field_base};
use crate::primitives::{Chamfer, highlight_styles};
use crate::tokens::{ChamferCut, ChromeColors, ModalMetrics};

/// The lead sentence of a dialog body: 13.5 px body text on a 1.6 line
/// height. Pass a [`StyledText`] or a row of spans for inline emphasis.
pub fn modal_lead(content: impl IntoElement, cx: &App) -> Div {
    div()
        .text_size(ModalMetrics::LEAD_FONT)
        .line_height(relative(ModalMetrics::LEAD_LINE_HEIGHT))
        .text_color(cx.theme().foreground)
        .child(content)
}

/// A statement block: 12.5 px strong mono on the ground behind a cut-8 line
/// frame, with `highlights` drawn in bold tint.
///
/// Which ranges are highlighted is the caller's knowledge: a product that
/// previews SQL passes its keywords, one that previews JSON passes its keys. The
/// style those ranges take is a design-system decision and stays here.
pub fn modal_code(code: impl Into<SharedString>, highlights: &[Range<usize>], cx: &App) -> Div {
    let theme = cx.theme();
    let code: SharedString = code.into();
    // Styled text needs sorted, non-overlapping runs, and the caller's ranges
    // are neither.
    let highlights =
        crate::syntax::overlay(Vec::new(), highlight_styles(highlights.iter().cloned(), cx));

    div()
        .relative()
        .w_full()
        .px(ModalMetrics::CODE_PADDING_X)
        .py(ModalMetrics::CODE_PADDING_Y)
        .font_family(crate::fonts::editor_family(cx))
        .text_size(ModalMetrics::CODE_FONT)
        .text_color(ChromeColors::strong(theme))
        .child(
            Chamfer::new(ChamferCut::INPUT)
                .fill(theme.background)
                .border(theme.border),
        )
        .child(StyledText::new(code).with_highlights(highlights))
}

/// A cut-8 line frame that clips its rows, for framed lists and tables.
pub fn modal_frame(cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .relative()
        .w_full()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(Chamfer::new(ChamferCut::INPUT).border(theme.border))
}

/// A control under its 12 px muted label.
pub fn modal_field(label: impl IntoElement, control: impl IntoElement, cx: &App) -> Div {
    div()
        .flex()
        .flex_col()
        .gap(ModalMetrics::FIELD_GAP)
        .child(
            div()
                .text_size(ModalMetrics::FIELD_LABEL_FONT)
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(control)
}

/// A wizard form row: the label in a 120 px column, the control and
/// its optional hint beside it.
pub fn modal_form_row(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    hint: Option<AnyElement>,
    cx: &App,
) -> Div {
    let theme = cx.theme();

    div()
        .flex()
        .items_start()
        .gap(ModalMetrics::FORM_ROW_GAP)
        .py(ModalMetrics::FORM_ROW_PADDING_Y)
        .child(
            div()
                .w(ModalMetrics::FORM_LABEL_WIDTH)
                .flex_shrink_0()
                .pt(ModalMetrics::FORM_LABEL_OFFSET)
                .text_color(theme.foreground)
                .child(label.into()),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .gap(ModalMetrics::FIELD_GAP)
                .child(control)
                .children(hint),
        )
}

/// A 12 px muted hint under a form control.
pub fn modal_hint(text: impl Into<SharedString>, cx: &App) -> AnyElement {
    div()
        .text_size(ModalMetrics::FIELD_LABEL_FONT)
        .text_color(cx.theme().muted_foreground)
        .child(text.into())
        .into_any_element()
}

/// A read-only value drawn like a text field: ground fill, cut 6, line
/// frame, mono text, strong when `value` is set and a muted `placeholder`
/// otherwise. `trailing` sits at the right end (a copy button).
pub fn modal_value_field(
    value: Option<SharedString>,
    placeholder: impl Into<SharedString>,
    trailing: Option<AnyElement>,
    cx: &App,
) -> Div {
    let theme = cx.theme();
    let has_value = value.is_some();

    value_field_base(cx)
        .font_family(crate::fonts::editor_family(cx))
        .text_color(if has_value {
            ChromeColors::strong(theme)
        } else {
            theme.muted_foreground
        })
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .child(value.map_or_else(|| placeholder.into(), |value| one_line(&value))),
        )
        .children(trailing)
}

/// Inline strong mono text, for a name quoted inside a sentence.
pub fn inline_code(text: impl Into<SharedString>, cx: &App) -> AnyElement {
    div()
        .font_family(crate::fonts::editor_family(cx))
        .text_color(ChromeColors::strong(cx.theme()))
        .child(text.into())
        .into_any_element()
}

#[cfg(test)]
mod tests {
    use gpui::{
        Context, InteractiveElement as _, IntoElement, Render, TestAppContext, VisualTestContext,
        Window, div, px,
    };

    use super::modal_value_field;
    use crate::accessibility::FrameCapture;
    use crate::selector::id_selector;
    use crate::tokens::{BASE_REM, Fields};

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

    struct Host {
        value: &'static str,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            use gpui::{ParentElement as _, Styled as _};

            let id = gpui::ElementId::Name("modal-value".into());

            div().size_full().child(
                div()
                    .debug_selector(id_selector(&id))
                    .id(id)
                    .flex()
                    .w(px(300.0))
                    .child(modal_value_field(Some(self.value.into()), "None", None, cx)),
            )
        }
    }

    fn row_height(value: &'static str, cx: &mut TestAppContext) -> gpui::Pixels {
        install_test_theme(cx);
        let (_, window): (_, &mut VisualTestContext) =
            cx.add_window_view(move |_, _| Host { value });
        let capture = FrameCapture::observe(window);
        let _frame = capture.frame(window);

        window
            .debug_bounds("modal-value")
            .expect("the value row is laid out")
            .size
            .height
    }

    /// A dialog's read-only value keeps the height of a text field, so it
    /// still sits in a form row the way it did before the field moved.
    #[gpui::test]
    fn a_modal_value_field_keeps_the_field_height(cx: &mut TestAppContext) {
        assert_eq!(
            row_height("https://example.test", cx),
            Fields::HEIGHT.to_pixels(px(BASE_REM))
        );
    }

    /// A dialog's read-only value is one line too: a break in it reads as a
    /// space instead of drawing a second row past the field.
    #[gpui::test]
    fn a_modal_value_field_draws_one_line_whatever_breaks_the_value(cx: &mut TestAppContext) {
        assert_eq!(
            row_height("first\r\nsecond\nthird", cx),
            Fields::HEIGHT.to_pixels(px(BASE_REM))
        );
    }
}
