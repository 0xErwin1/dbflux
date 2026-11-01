//! Body blocks shared by dialogs and wizards: the lead
//! sentence, the statement block, labelled fields and the framed list.

use std::ops::Range;

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Div, FontWeight, HighlightStyle, SharedString, StyledText, div, relative,
};
use gpui_component::ActiveTheme;

use crate::primitives::Chamfer;
use crate::tokens::{ChamferCut, ChromeColors, Fields, ModalMetrics};

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
    let tint = ChromeColors::tint(theme);

    let highlights: Vec<(Range<usize>, HighlightStyle)> = highlights
        .iter()
        .cloned()
        .map(|range| {
            (
                range,
                HighlightStyle {
                    color: Some(tint),
                    font_weight: Some(FontWeight::BOLD),
                    ..Default::default()
                },
            )
        })
        .collect();

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

    div()
        .relative()
        .flex()
        .items_center()
        .gap(Fields::GAP)
        .h(Fields::HEIGHT)
        .px(Fields::PADDING_X)
        .font_family(crate::fonts::editor_family(cx))
        .text_size(Fields::TEXT)
        .text_color(if has_value {
            ChromeColors::strong(theme)
        } else {
            theme.muted_foreground
        })
        .child(
            Chamfer::new(ChamferCut::CONTROL)
                .fill(theme.background)
                .border(theme.border),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .child(value.unwrap_or_else(|| placeholder.into())),
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
