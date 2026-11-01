//! `StatusChip` — a segment of a status bar.

use gpui::prelude::*;
use gpui::{App, Div, ElementId, FontWeight, Hsla, Stateful, div};
use gpui_component::ActiveTheme;

use super::island_fill;
use crate::primitives::Chamfer;
use crate::tokens::{ChamferCut, ShellMetrics};

/// A chip of a status bar: 26 px high, cut 6, 12 px padding and an 8 px gap
/// between its children, on the island fill.
///
/// A clickable chip takes the raised fill on hover and press. A toned chip
/// draws its text in the tone, semibold; with the wash it also sits on the
/// tone at `ShellMetrics::STATUS_CHIP_ALPHA` instead of the island fill (a
/// connection's environment, for example).
pub struct StatusChip {
    id: ElementId,
    clickable: bool,
    tone: Option<Hsla>,
    wash: bool,
}

impl StatusChip {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            clickable: false,
            tone: None,
            wash: false,
        }
    }

    pub fn clickable(mut self, clickable: bool) -> Self {
        self.clickable = clickable;
        self
    }

    /// Draws the chip's text in `tone`, semibold.
    pub fn tone(mut self, tone: Hsla) -> Self {
        self.tone = Some(tone);
        self
    }

    /// Fills the chip with a wash of its tone. Without a tone the chip keeps
    /// the island fill.
    pub fn wash(mut self, wash: bool) -> Self {
        self.wash = wash;
        self
    }

    /// The chip's resting fill.
    pub fn fill(&self, theme: &gpui_component::Theme) -> Hsla {
        match self.tone.filter(|_| self.wash) {
            Some(tone) => tone.opacity(ShellMetrics::STATUS_CHIP_ALPHA),
            None => island_fill(theme),
        }
    }

    /// The chip as a row the caller adds its icon and text to.
    pub fn build(self, cx: &App) -> Stateful<Div> {
        let theme = cx.theme();
        let mut shape = Chamfer::new(ChamferCut::CONTROL).fill(self.fill(theme));

        if self.clickable {
            shape = shape
                .fill_hover(theme.secondary)
                .fill_active(theme.secondary_hover)
                .interactive(ElementId::Name(format!("{}-chamfer", self.id).into()));
        }

        div()
            .debug_selector(crate::selector::id_selector(&self.id))
            .id(self.id)
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::STATUS_CHIP_GAP)
            .h(ShellMetrics::STATUS_CHIP_HEIGHT)
            .px(ShellMetrics::STATUS_CHIP_PADDING_X)
            .whitespace_nowrap()
            .when_some(self.tone, |chip, tone| {
                chip.text_color(tone).font_weight(FontWeight::SEMIBOLD)
            })
            .when(self.clickable, |chip| chip.cursor_pointer())
            .child(shape)
    }
}

#[cfg(test)]
mod tests;
