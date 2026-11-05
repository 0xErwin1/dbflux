//! `FieldWell` — the framed, filled area a read-only block sits in.

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Div, ElementId, ParentElement, Pixels, StyleRefinement, Styled, Window, div,
};
use gpui_component::ActiveTheme;

use super::Chamfer;
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColorSlot};

/// Render-free description of a field well.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FieldWellInspection {
    pub fill: ChromeColorSlot,
    pub border: ChromeColorSlot,
    pub cut: Pixels,
}

/// The frame of a field well: the ground fill cut
/// [`ChamferCut::CONTROL`](crate::tokens::ChamferCut::CONTROL) inside a line,
/// the frame a text field draws.
pub fn inspect_field_well() -> FieldWellInspection {
    FieldWellInspection {
        fill: ChromeColorSlot::Background,
        border: ChromeColorSlot::Border,
        cut: ChamferCut::CONTROL,
    }
}

/// The framed area a read-only block sits in: the ground fill cut 6 behind a
/// line, holding whatever the host puts in it — a
/// [`CodeView`](crate::primitives::CodeView), a rendered document, a preview.
///
/// It is a relative flex column: chain layout and sizing like on a `div` and
/// add content with `.child()`, which sits above the frame. Do not set `.bg()`
/// on it. [`Self::cut`] draws a different cut, for a caller whose frame must
/// match another block's.
///
/// ```ignore
/// FieldWell::new()
///     .id("document-text")
///     .flex_1()
///     .child(CodeView::new("document", text).line_numbers(true))
/// ```
#[derive(IntoElement)]
pub struct FieldWell {
    base: Div,
    id: Option<ElementId>,
    cut: Pixels,
    children: Vec<AnyElement>,
}

impl FieldWell {
    pub fn new() -> Self {
        Self {
            base: div().relative().flex().flex_col(),
            id: None,
            cut: inspect_field_well().cut,
            children: Vec::new(),
        }
    }

    /// Records `id` as the well's element and debug id.
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Draws `cut` instead of the field cut.
    pub fn cut(mut self, cut: Pixels) -> Self {
        self.cut = cut;
        self
    }
}

impl Default for FieldWell {
    fn default() -> Self {
        Self::new()
    }
}

impl Styled for FieldWell {
    fn style(&mut self) -> &mut StyleRefinement {
        self.base.style()
    }
}

impl ParentElement for FieldWell {
    fn extend(&mut self, elements: impl IntoIterator<Item = AnyElement>) {
        self.children.extend(elements);
    }
}

impl RenderOnce for FieldWell {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let inspection = inspect_field_well();
        let theme = cx.theme();
        let well = self
            .base
            .child(
                Chamfer::new(self.cut)
                    .fill(inspection.fill.resolve(theme))
                    .border(inspection.border.resolve(theme)),
            )
            .children(self.children);

        match self.id {
            Some(id) => well
                .debug_selector(id_selector(&id))
                .id(id)
                .into_any_element(),
            None => well.into_any_element(),
        }
    }
}

#[cfg(test)]
mod tests;
