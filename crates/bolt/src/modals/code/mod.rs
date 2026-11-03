//! `ModalCode` — the statement block of a dialog body, with an optional
//! line-number gutter for code a reader refers to by line.

use std::ops::Range;

use gpui::prelude::*;
use gpui::{App, ElementId, Role, SharedString, StyledText, Window, div};
use gpui_component::ActiveTheme;

use super::parts::highlight_styles;
use crate::primitives::Chamfer;
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColors, ModalMetrics};

/// A statement block under its own id: 12.5 px strong mono on the ground
/// behind a cut-8 line frame, with `highlights` drawn in bold tint, like
/// [`super::modal_code`].
///
/// With [`ModalCode::line_numbers`] each line draws on its own 20 px row
/// after a right-aligned number in the line-2 color, and carries the id
/// `<id>-line-<n>` (numbered from 1) named by its text, so a test or an
/// assistive reader addresses a line the way a reader quotes it. The lines
/// sit in the code column `<id>-lines`, which scrolls sideways when a line
/// is wider than the block; the gutter stays in place. The block
/// is a label node named by [`ModalCode::aria_label`], or by the code itself
/// when the caller names none.
///
/// ```ignore
/// ModalCode::new("export-code", code)
///     .highlights(keyword_ranges)
///     .line_numbers(true)
///     .aria_label(caption)
/// ```
#[derive(IntoElement)]
pub struct ModalCode {
    id: SharedString,
    code: SharedString,
    highlights: Vec<Range<usize>>,
    line_numbers: bool,
    aria_label: Option<SharedString>,
}

impl ModalCode {
    pub fn new(id: impl Into<SharedString>, code: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            code: code.into(),
            highlights: Vec::new(),
            line_numbers: false,
            aria_label: None,
        }
    }

    /// Byte ranges of the code drawn in bold tint. Which ranges matter is
    /// the caller's knowledge (keywords, keys, references); the style is
    /// the design system's. A range crossing a line break is drawn on every
    /// line it covers.
    pub fn highlights(mut self, highlights: impl IntoIterator<Item = Range<usize>>) -> Self {
        self.highlights = highlights.into_iter().collect();
        self
    }

    /// Draws the line-number gutter, one row per line.
    pub fn line_numbers(mut self, line_numbers: bool) -> Self {
        self.line_numbers = line_numbers;
        self
    }

    /// The name the block announces, in place of the code text.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }
}

impl RenderOnce for ModalCode {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let id = ElementId::Name(self.id.clone());
        let name = self.aria_label.unwrap_or_else(|| self.code.clone());

        let block = div()
            .debug_selector(id_selector(&id))
            .id(id)
            .role(Role::Label)
            .aria_label(name)
            .relative()
            .w_full()
            .py(ModalMetrics::CODE_PADDING_Y)
            .font_family(crate::fonts::editor_family(cx))
            .text_size(ModalMetrics::CODE_FONT)
            .text_color(ChromeColors::strong(theme))
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(theme.background)
                    .border(theme.border),
            );

        if !self.line_numbers {
            let highlights = highlight_styles(self.highlights, cx);
            return block
                .px(ModalMetrics::CODE_PADDING_X)
                .child(StyledText::new(self.code).with_highlights(highlights));
        }

        let gutter_color = crate::colors(cx).line_2;
        let lines = split_lines(&self.code, &self.highlights);
        let numbers = (1..=lines.len()).map(|number| {
            div()
                .h(ModalMetrics::CODE_LINE_HEIGHT)
                .flex()
                .items_center()
                .justify_end()
                .child(number.to_string())
        });
        let rows = lines.into_iter().enumerate().map(|(index, line)| {
            let text: SharedString = self.code[line.text].to_string().into();
            let row_id = ElementId::Name(format!("{}-line-{}", self.id, index + 1).into());

            div()
                .debug_selector(id_selector(&row_id))
                .id(row_id)
                .role(Role::Label)
                .aria_label(text.clone())
                .flex()
                .items_center()
                .h(ModalMetrics::CODE_LINE_HEIGHT)
                .whitespace_nowrap()
                .child(StyledText::new(text).with_highlights(highlight_styles(line.highlights, cx)))
        });
        let column_id = ElementId::Name(format!("{}-lines", self.id).into());

        // The gutter is its own column so a long line scrolls the code
        // sideways while the numbers stay put.
        block
            .flex()
            .overflow_hidden()
            .pr(ModalMetrics::CODE_PADDING_X)
            .child(
                div()
                    .w(ModalMetrics::CODE_GUTTER_WIDTH)
                    .flex_none()
                    .flex()
                    .flex_col()
                    .pr(ModalMetrics::CODE_GUTTER_PADDING_RIGHT)
                    .text_color(gutter_color)
                    .children(numbers),
            )
            .child(
                div()
                    .debug_selector(id_selector(&column_id))
                    .id(column_id)
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .overflow_x_scroll()
                    // A vertical wheel belongs to the dialog body.
                    .restrict_scroll_to_axis()
                    .child(div().flex_none().flex().flex_col().children(rows)),
            )
    }
}

/// One line of a numbered block: its text range in the code, without the
/// line break, and its highlights relative to the line start.
#[derive(Debug, PartialEq)]
pub(crate) struct CodeLine {
    pub text: Range<usize>,
    pub highlights: Vec<Range<usize>>,
}

/// The lines of `code` with each highlight clipped to the lines it covers.
/// A trailing line break closes the last line rather than opening an empty
/// one, and a `\r` before a break is not part of the line.
pub(crate) fn split_lines(code: &str, highlights: &[Range<usize>]) -> Vec<CodeLine> {
    // One sweep over the highlights in start order: a highlight joins
    // `active` when its line arrives and leaves once a line starts past it.
    let mut ordered: Vec<(usize, Range<usize>)> = highlights.iter().cloned().enumerate().collect();
    ordered.sort_by_key(|(_, range)| range.start);
    let mut pending = ordered.into_iter().peekable();
    let mut active: Vec<(usize, Range<usize>)> = Vec::new();
    let mut lines = Vec::new();
    let mut start = 0;

    for segment in code.split_inclusive('\n') {
        let content = segment
            .strip_suffix('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line))
            .unwrap_or(segment);
        let text = start..start + content.len();

        while let Some(highlight) = pending.next_if(|(_, range)| range.start < text.end) {
            active.push(highlight);
        }
        active.retain(|(_, range)| range.end > text.start);

        // Input order, as the caller gave the ranges.
        let mut covering: Vec<(usize, Range<usize>)> = active
            .iter()
            .map(|(position, range)| {
                (
                    *position,
                    range.start.max(text.start)..range.end.min(text.end),
                )
            })
            .filter(|(_, clipped)| clipped.start < clipped.end)
            .collect();
        covering.sort_unstable_by_key(|(position, _)| *position);
        let line_highlights = covering
            .into_iter()
            .map(|(_, clipped)| clipped.start - text.start..clipped.end - text.start)
            .collect();

        lines.push(CodeLine {
            text,
            highlights: line_highlights,
        });
        start += segment.len();
    }

    if lines.is_empty() {
        lines.push(CodeLine {
            text: 0..0,
            highlights: Vec::new(),
        });
    }

    lines
}

#[cfg(test)]
mod tests;
