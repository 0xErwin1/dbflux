//! `CodeView` — read-only code, with an optional line-number gutter.

use std::ops::Range;

use gpui::prelude::*;
use gpui::{
    App, ElementId, FontWeight, HighlightStyle, Role, SharedString, StyledText, Window, div,
};
use gpui_component::ActiveTheme;

use super::BadgeTone;
use crate::selector::id_selector;
use crate::tokens::CodeMetrics;

/// Read-only code under its own id, with the syntax highlighting seam of
/// [`crate::syntax`] and an optional line-number gutter.
///
/// With [`Self::line_numbers`] each line draws on its own row after a
/// right-aligned number in the line-2 color, and carries the id
/// `<id>-line-<n>` (numbered from 1) named by its text, so a test or an
/// assistive reader addresses a line the way a reader quotes it. The rows sit
/// in the code column `<id>-lines`, which scrolls sideways when a line is wider
/// than the view, and the gutter stays in place. A vertical wheel is left to
/// the host, whether or not the view scrolls.
///
/// The view draws no frame or padding of its own: the host places it in a
/// [`FieldWell`](crate::primitives::FieldWell) or any other container, and its
/// font family and color follow that container. Its text size does not: the
/// code draws at [`CodeMetrics::FONT`] so a line fits its
/// [`CodeMetrics::LINE_HEIGHT`] row at any host size. The view is a label node
/// named by [`Self::aria_label`], or by the code itself when the caller names
/// none.
///
/// ```ignore
/// FieldWell::new().id("document-text").child(
///     CodeView::new("document", text)
///         .line_numbers(true)
///         .language("json"),
/// )
/// ```
#[derive(IntoElement)]
pub struct CodeView {
    id: SharedString,
    code: SharedString,
    highlights: Vec<Range<usize>>,
    toned_highlights: Vec<(Range<usize>, BadgeTone)>,
    language: Option<SharedString>,
    line_numbers: bool,
    aria_label: Option<SharedString>,
}

impl CodeView {
    pub fn new(id: impl Into<SharedString>, code: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            code: code.into(),
            highlights: Vec::new(),
            toned_highlights: Vec::new(),
            language: None,
            line_numbers: false,
            aria_label: None,
        }
    }

    /// Byte ranges of the code drawn in bold tint, over the syntax runs. Which
    /// ranges matter is the caller's knowledge (keywords, keys, references);
    /// the style is the design system's. A range crossing a line break is drawn
    /// on every line it covers.
    pub fn highlights(mut self, highlights: impl IntoIterator<Item = Range<usize>>) -> Self {
        self.highlights = highlights.into_iter().collect();
        self
    }

    /// Byte ranges of the code drawn bold in `tone`'s text color (the tint for
    /// `Accent`, the warning color for `Warning`), such as references that
    /// resolve and references that do not. Each call adds to the toned ranges;
    /// they draw over the [`Self::highlights`] ranges where the two overlap.
    pub fn highlights_with_tone(
        mut self,
        highlights: impl IntoIterator<Item = Range<usize>>,
        tone: BadgeTone,
    ) -> Self {
        self.toned_highlights
            .extend(highlights.into_iter().map(|range| (range, tone)));
        self
    }

    /// Colors the code with the grammar registered under `language`. A highlight
    /// replaces the syntax style over its range; a language with no grammar
    /// leaves the code plain.
    pub fn language(mut self, language: impl Into<SharedString>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Draws the line-number gutter, one row per line.
    pub fn line_numbers(mut self, line_numbers: bool) -> Self {
        self.line_numbers = line_numbers;
        self
    }

    /// The name the view announces, in place of the code text.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// The styled runs of the whole code: the syntax runs with the highlights
    /// drawn over them.
    pub(crate) fn styled_runs(&self, cx: &App) -> Vec<(Range<usize>, HighlightStyle)> {
        let theme = cx.theme();
        let toned = self.toned_highlights.iter().map(|(range, tone)| {
            let style = HighlightStyle {
                color: Some(tone.text_color(theme)),
                font_weight: Some(FontWeight::BOLD),
                ..Default::default()
            };
            (range.clone(), style)
        });
        let mut highlights = highlight_styles(self.highlights.iter().cloned(), cx);
        highlights.extend(toned);
        // Styled text needs sorted, non-overlapping runs; the caller's ranges
        // are neither, and a toned range may cover a tint one. Normalizing
        // every time keeps an unordered pair off the wrong bytes.
        let highlights = crate::syntax::overlay(Vec::new(), highlights);
        match &self.language {
            Some(language) => crate::syntax::overlay(
                crate::syntax::highlight(language, &self.code, cx),
                highlights,
            ),
            None => highlights,
        }
    }
}

impl RenderOnce for CodeView {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let id = ElementId::Name(self.id.clone());
        let name = self.aria_label.clone().unwrap_or_else(|| self.code.clone());
        let runs = self.styled_runs(cx);

        let view = div()
            .debug_selector(id_selector(&id))
            .id(id)
            .role(Role::Label)
            .aria_label(name)
            .w_full()
            // The rows are `CodeMetrics::LINE_HEIGHT` tall, so the code draws
            // at the code font whatever size the host set around it.
            .text_size(CodeMetrics::FONT);

        if !self.line_numbers {
            return view.child(StyledText::new(self.code).with_highlights(runs));
        }

        let gutter_color = crate::colors(cx).line_2;
        let lines = split_runs(&self.code, &runs);
        let numbers = (1..=lines.len()).map(|number| {
            div()
                .h(CodeMetrics::LINE_HEIGHT)
                .flex()
                .items_center()
                .justify_end()
                .child(number.to_string())
        });
        let rows = lines
            .into_iter()
            .enumerate()
            .map(|(index, (line, line_runs))| {
                let text: SharedString = self.code[line].to_string().into();
                let row_id = ElementId::Name(format!("{}-line-{}", self.id, index + 1).into());

                div()
                    .debug_selector(id_selector(&row_id))
                    .id(row_id)
                    .role(Role::Label)
                    .aria_label(text.clone())
                    .flex()
                    .items_center()
                    .h(CodeMetrics::LINE_HEIGHT)
                    .whitespace_nowrap()
                    .child(StyledText::new(text).with_highlights(line_runs))
            });
        let column_id = ElementId::Name(format!("{}-lines", self.id).into());

        // The gutter is its own column so a long line scrolls the code
        // sideways while the numbers stay put.
        view.flex()
            .overflow_hidden()
            .child(
                div()
                    .w(CodeMetrics::GUTTER_WIDTH)
                    .flex_none()
                    .flex()
                    .flex_col()
                    .pr(CodeMetrics::GUTTER_PADDING_RIGHT)
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
                    // A vertical wheel belongs to the host's scroll area.
                    .restrict_scroll_to_axis()
                    .child(div().flex_none().flex().flex_col().children(rows)),
            )
    }
}

/// `ranges` paired with the highlight a statement block draws them in: bold
/// tint.
pub(crate) fn highlight_styles(
    ranges: impl IntoIterator<Item = Range<usize>>,
    cx: &App,
) -> Vec<(Range<usize>, HighlightStyle)> {
    let style = HighlightStyle {
        color: Some(crate::tokens::ChromeColors::tint(cx.theme())),
        font_weight: Some(FontWeight::BOLD),
        ..Default::default()
    };

    ranges.into_iter().map(|range| (range, style)).collect()
}

/// One line of a numbered block: its text range in the code, without the
/// line break, and its highlights relative to the line start.
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) struct CodeLine {
    pub text: Range<usize>,
    pub highlights: Vec<Range<usize>>,
}

/// The lines of `code` with each highlight clipped to the lines it covers.
#[cfg(test)]
pub(crate) fn split_lines(code: &str, highlights: &[Range<usize>]) -> Vec<CodeLine> {
    let runs: Vec<(Range<usize>, ())> =
        highlights.iter().map(|range| (range.clone(), ())).collect();

    split_runs(code, &runs)
        .into_iter()
        .map(|(text, runs)| CodeLine {
            text,
            highlights: runs.into_iter().map(|(range, ())| range).collect(),
        })
        .collect()
}

/// The lines of `code`, each as its text range in the code (without the line
/// break) and the runs clipped to it, relative to the line start and in the
/// order the caller gave them. A trailing line break closes the last line
/// rather than opening an empty one, and a `\r` before a break is not part of
/// the line.
pub(crate) fn split_runs<T: Clone>(
    code: &str,
    runs: &[(Range<usize>, T)],
) -> Vec<(Range<usize>, Vec<(Range<usize>, T)>)> {
    // One sweep over the runs in start order: a run joins `active` when its
    // line arrives and leaves once a line starts past it.
    let mut ordered: Vec<(usize, &(Range<usize>, T))> = runs.iter().enumerate().collect();
    ordered.sort_by_key(|(_, (range, _))| range.start);
    let mut pending = ordered.into_iter().peekable();
    let mut active: Vec<(usize, &(Range<usize>, T))> = Vec::new();
    let mut lines = Vec::new();
    let mut start = 0;

    for segment in code.split_inclusive('\n') {
        let content = segment
            .strip_suffix('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line))
            .unwrap_or(segment);
        let text = start..start + content.len();

        while let Some(run) = pending.next_if(|(_, (range, _))| range.start < text.end) {
            active.push(run);
        }
        active.retain(|(_, (range, _))| range.end > text.start);

        // Input order, as the caller gave the runs.
        let mut covering: Vec<(usize, Range<usize>, &T)> = active
            .iter()
            .map(|(position, (range, value))| {
                (
                    *position,
                    range.start.max(text.start)..range.end.min(text.end),
                    value,
                )
            })
            .filter(|(_, clipped, _)| clipped.start < clipped.end)
            .collect();
        covering.sort_unstable_by_key(|(position, _, _)| *position);
        let line_runs = covering
            .into_iter()
            .map(|(_, clipped, value)| {
                (
                    clipped.start - text.start..clipped.end - text.start,
                    value.clone(),
                )
            })
            .collect();

        lines.push((text, line_runs));
        start += segment.len();
    }

    if lines.is_empty() {
        lines.push((0..0, Vec::new()));
    }

    lines
}

#[cfg(test)]
mod tests;
