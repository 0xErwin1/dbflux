//! The body preview of the picker: the raw body with every placeholder drawn
//! as a neutral chip.
//!
//! The chip look is the approved one: a muted surface with a one-pixel border,
//! the delimiters (`${1:`, `}`, `$1`, `$0`) in the muted foreground and the
//! default text in the normal foreground. Nothing here uses the accent color.

use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::snippet_placeholder_spans;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::Theme;
use std::ops::Range;

/// What a run of the body is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreviewTone {
    /// Text outside any placeholder.
    Plain,
    /// `${1:`, `}`, `$1`, `$0`.
    Delimiter,
    /// The default text inside a placeholder.
    Default,
}

/// A byte range of the raw body and how to draw it. Runs cover the body in
/// order; the runs of one placeholder share its `chip` index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreviewRun {
    pub(super) range: Range<usize>,
    pub(super) tone: PreviewTone,
    pub(super) chip: Option<usize>,
}

pub(super) fn preview_runs(body: &str) -> Vec<PreviewRun> {
    let mut runs = Vec::new();
    let mut cursor = 0;

    for (chip, span) in snippet_placeholder_spans(body).into_iter().enumerate() {
        push_run(
            &mut runs,
            cursor..span.range.start,
            PreviewTone::Plain,
            None,
        );
        let chip = Some(chip);
        match span.default {
            Some(default) => {
                push_run(
                    &mut runs,
                    span.range.start..default.start,
                    PreviewTone::Delimiter,
                    chip,
                );
                push_run(&mut runs, default.clone(), PreviewTone::Default, chip);
                push_run(
                    &mut runs,
                    default.end..span.range.end,
                    PreviewTone::Delimiter,
                    chip,
                );
            }
            None => push_run(&mut runs, span.range.clone(), PreviewTone::Delimiter, chip),
        }
        cursor = span.range.end;
    }
    push_run(&mut runs, cursor..body.len(), PreviewTone::Plain, None);
    runs
}

fn push_run(
    runs: &mut Vec<PreviewRun>,
    range: Range<usize>,
    tone: PreviewTone,
    chip: Option<usize>,
) {
    if !range.is_empty() {
        runs.push(PreviewRun { range, tone, chip });
    }
}

/// Theme colors of the chips. None of them is the accent or primary color.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ChipPalette {
    pub(super) background: Hsla,
    pub(super) border: Hsla,
    pub(super) delimiter: Hsla,
    pub(super) text: Hsla,
}

impl ChipPalette {
    pub(super) fn from_theme(theme: &Theme) -> Self {
        Self {
            background: theme.secondary,
            border: theme.border,
            delimiter: theme.muted_foreground,
            text: theme.foreground,
        }
    }

    pub(super) fn color(&self, tone: PreviewTone) -> Hsla {
        match tone {
            PreviewTone::Delimiter => self.delimiter,
            PreviewTone::Plain | PreviewTone::Default => self.text,
        }
    }
}

enum Piece {
    Text(String, PreviewTone),
    Chip(Vec<(String, PreviewTone)>),
}

/// Splits the runs into lines of text pieces and chips.
fn preview_lines(body: &str, runs: &[PreviewRun]) -> Vec<Vec<Piece>> {
    let mut lines: Vec<Vec<Piece>> = Vec::new();
    let mut current: Vec<Piece> = Vec::new();
    let mut open_chip: Option<usize> = None;

    for run in runs {
        let text = body.get(run.range.clone()).unwrap_or_default();
        match run.chip {
            Some(chip) if open_chip == Some(chip) => {
                if let Some(Piece::Chip(parts)) = current.last_mut() {
                    parts.push((text.to_string(), run.tone));
                }
            }
            Some(chip) => {
                open_chip = Some(chip);
                current.push(Piece::Chip(vec![(text.to_string(), run.tone)]));
            }
            None => {
                open_chip = None;
                for (position, segment) in text.split('\n').enumerate() {
                    if position > 0 {
                        lines.push(std::mem::take(&mut current));
                    }
                    if !segment.is_empty() {
                        current.push(Piece::Text(segment.to_string(), run.tone));
                    }
                }
            }
        }
    }
    lines.push(current);
    lines
}

/// The preview element: one row per body line, chips inline.
pub(super) fn render_preview(body: &str, theme: &Theme) -> Div {
    let palette = ChipPalette::from_theme(theme);
    let runs = preview_runs(body);

    div()
        .flex()
        .flex_col()
        .children(preview_lines(body, &runs).into_iter().map(|pieces| {
            let empty = pieces.is_empty();
            div()
                .flex()
                .flex_wrap()
                .items_center()
                .when(empty, |row| row.child(Text::code(" ")))
                .children(pieces.into_iter().map(|piece| {
                    match piece {
                        Piece::Text(text, tone) => div()
                            .child(Text::code(text).text_color(palette.color(tone)))
                            .into_any_element(),
                        Piece::Chip(parts) => div()
                            .flex()
                            .items_center()
                            .px(Spacing::XS)
                            .rounded(px(2.0))
                            .bg(palette.background)
                            .border_1()
                            .border_color(palette.border)
                            .children(parts.into_iter().map(|(text, tone)| {
                                Text::code(text).text_color(palette.color(tone))
                            }))
                            .into_any_element(),
                    }
                }))
        }))
}
