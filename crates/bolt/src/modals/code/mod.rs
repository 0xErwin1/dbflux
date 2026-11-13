//! `ModalCode` — the statement block of a dialog body, with an optional
//! line-number gutter for code a reader refers to by line.

use std::ops::Range;

#[cfg(test)]
use gpui::HighlightStyle;
use gpui::prelude::*;
use gpui::{App, SharedString, Window};
use gpui_component::ActiveTheme;

use crate::primitives::{BadgeTone, CodeView, FieldWell};
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
/// With [`ModalCode::language`] the code is colored by that language's
/// grammar (see [`crate::syntax`]), and the highlights draw over it.
///
/// The rows, the gutter and the highlighting are a [`CodeView`] in a
/// [`FieldWell`], so a block inside a dialog and the same code inside a page
/// are one implementation.
///
/// ```ignore
/// ModalCode::new("export-code", code)
///     .language("bash")
///     .highlights(reference_ranges)
///     .line_numbers(true)
///     .aria_label(caption)
/// ```
#[derive(IntoElement)]
pub struct ModalCode {
    view: CodeView,
    line_numbers: bool,
}

impl ModalCode {
    pub fn new(id: impl Into<SharedString>, code: impl Into<SharedString>) -> Self {
        Self {
            view: CodeView::new(id, code),
            line_numbers: false,
        }
    }

    /// Byte ranges of the code drawn in bold tint. Which ranges matter is
    /// the caller's knowledge (keywords, keys, references); the style is
    /// the design system's. A range crossing a line break is drawn on every
    /// line it covers.
    pub fn highlights(mut self, highlights: impl IntoIterator<Item = Range<usize>>) -> Self {
        self.view = self.view.highlights(highlights);
        self
    }

    /// Byte ranges of the code drawn bold in `tone`'s text color (the tint
    /// for `Accent`, the warning color for `Warning`), such as references
    /// that resolve and references that do not. Each call adds to the toned
    /// ranges; they draw over the [`ModalCode::highlights`] ranges where
    /// the two overlap.
    pub fn highlights_with_tone(
        mut self,
        highlights: impl IntoIterator<Item = Range<usize>>,
        tone: BadgeTone,
    ) -> Self {
        self.view = self.view.highlights_with_tone(highlights, tone);
        self
    }

    /// Colors the code with the grammar registered under `language`. A
    /// highlight replaces the syntax style over its range; a language with
    /// no grammar leaves the code plain.
    pub fn language(mut self, language: impl Into<SharedString>) -> Self {
        self.view = self.view.language(language);
        self
    }

    /// Draws the line-number gutter, one row per line.
    pub fn line_numbers(mut self, line_numbers: bool) -> Self {
        self.line_numbers = line_numbers;
        self.view = self.view.line_numbers(line_numbers);
        self
    }

    /// The name the block announces, in place of the code text.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.view = self.view.aria_label(label);
        self
    }

    /// The styled runs of the whole code: the syntax runs with the
    /// highlights drawn over them.
    #[cfg(test)]
    pub(crate) fn styled_runs(&self, cx: &App) -> Vec<(Range<usize>, HighlightStyle)> {
        self.view.styled_runs(cx)
    }
}

impl RenderOnce for ModalCode {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let block = FieldWell::new()
            .cut(ChamferCut::INPUT)
            .w_full()
            .py(ModalMetrics::CODE_PADDING_Y)
            .font_family(crate::fonts::editor_family(cx))
            .text_color(ChromeColors::strong(cx.theme()));

        // The gutter runs to the frame's left edge; without it the code keeps
        // the block's inset on both sides.
        let block = if self.line_numbers {
            block.pr(ModalMetrics::CODE_PADDING_X)
        } else {
            block.px(ModalMetrics::CODE_PADDING_X)
        };

        block.child(self.view)
    }
}

#[cfg(test)]
pub(crate) use crate::primitives::{CodeLine, split_lines, split_runs};

#[cfg(test)]
mod tests;
