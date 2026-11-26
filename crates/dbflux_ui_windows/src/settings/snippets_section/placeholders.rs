//! Placeholder chips in the body editor.
//!
//! Every placeholder of the raw body gets the approved neutral look: a filled
//! and framed range over the whole token (muted surface, border color), the
//! delimiters in the muted foreground and the default text in the normal
//! foreground. The input draws fills and frames per range, and text styles per
//! range, so the chip is drawn in full; nothing uses the accent color. The
//! frame is a rectangle (the input has no rounded corners).

use dbflux_core::snippet_placeholder_spans;
use gpui::{HighlightStyle, Hsla};
use gpui_component::Theme;
use gpui_component::input::{RangeDecoration, RangeDecorationStyle, TextDecoration};

/// Theme colors of the chips.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct PlaceholderColors {
    pub(super) background: Hsla,
    pub(super) border: Hsla,
    pub(super) delimiter: Hsla,
    pub(super) text: Hsla,
}

impl PlaceholderColors {
    pub(super) fn from_theme(theme: &Theme) -> Self {
        Self {
            background: theme.secondary,
            border: theme.border,
            delimiter: theme.muted_foreground,
            text: theme.foreground,
        }
    }
}

/// The decorations of one body.
#[derive(Debug, Default, PartialEq)]
pub(super) struct PlaceholderDecorations {
    /// A fill and a frame per placeholder, over the whole token.
    pub(super) chips: Vec<RangeDecoration>,
    /// Delimiter and default text colors.
    pub(super) text: Vec<TextDecoration>,
}

pub(super) fn placeholder_decorations(
    body: &str,
    colors: PlaceholderColors,
) -> PlaceholderDecorations {
    let mut decorations = PlaceholderDecorations::default();
    let color = |color: Hsla| HighlightStyle {
        color: Some(color),
        ..HighlightStyle::default()
    };

    for span in snippet_placeholder_spans(body) {
        decorations.chips.push(
            RangeDecoration::new(span.range.clone())
                .with_style(RangeDecorationStyle::Fill)
                .with_color(colors.background),
        );
        decorations
            .chips
            .push(RangeDecoration::new(span.range.clone()).with_color(colors.border));

        let (before, default, after) = match span.default {
            Some(default) => (
                span.range.start..default.start,
                default.clone(),
                default.end..span.range.end,
            ),
            None => (
                span.range.clone(),
                span.range.end..span.range.end,
                span.range.end..span.range.end,
            ),
        };
        for (range, tone) in [
            (before, colors.delimiter),
            (default, colors.text),
            (after, colors.delimiter),
        ] {
            if !range.is_empty() {
                decorations
                    .text
                    .push(TextDecoration::new(range, color(tone)));
            }
        }
    }
    decorations
}
