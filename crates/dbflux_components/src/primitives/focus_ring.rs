use gpui::prelude::*;
use gpui::{App, Hsla, Pixels, div, px};
use gpui_component::ActiveTheme;

use crate::primitives::{Chamfer, ChamferFillKind, ChamferRing};

/// Outline the focus ring follows.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FocusShape {
    /// Square outline, for surfaces that are never cut: grid cells, rows,
    /// code editors.
    Rect,
    /// Chamfer outline with the given cut, ring inside the bounds: outlined,
    /// ghost and data controls (inputs, dropdowns, secondary buttons).
    Chamfer(Pixels),
    /// Chamfer outline with the given cut, ring 2 px outside the bounds:
    /// filled controls (primary and danger buttons).
    FilledChamfer(Pixels),
}

impl FocusShape {
    fn cut(self) -> Pixels {
        match self {
            Self::Rect => px(0.0),
            Self::Chamfer(cut) | Self::FilledChamfer(cut) => cut,
        }
    }

    fn fill_kind(self) -> ChamferFillKind {
        match self {
            Self::FilledChamfer(_) => ChamferFillKind::Filled,
            Self::Rect | Self::Chamfer(_) => ChamferFillKind::Surface,
        }
    }

    /// The ring stroked for this shape: `Borders::FOCUS_RING` (1.5 px) of
    /// `color`, inset or outside depending on the fill kind.
    pub fn ring(self, color: Hsla) -> ChamferRing {
        ChamferRing::focus_for(color, self.fill_kind())
    }
}

/// The app's one keyboard-focus style: a tint ring, 1.5 px, that traces
/// `shape` around `child` when `focused`.
///
/// Use it for controls whose focus is tracked by form navigation state rather
/// than by their own focus handle. Controls that own a focus handle
/// (`Button`, `Input`, `Dropdown`) draw this same ring themselves. `ring_color`
/// defaults to the theme tint. The ring is painted above `child` and does not
/// change its layout.
pub fn focus_ring(
    focused: bool,
    shape: FocusShape,
    ring_color: Option<Hsla>,
    child: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    let color = ring_color.unwrap_or(cx.theme().ring);

    div().relative().child(child).when(focused, |frame| {
        frame.child(Chamfer::new(shape.cut()).ring(shape.ring(color)))
    })
}

#[cfg(test)]
mod tests {
    use super::FocusShape;
    use crate::tokens::{Borders, ChamferCut};

    #[test]
    fn outlined_shapes_keep_the_ring_inside() {
        for shape in [FocusShape::Rect, FocusShape::Chamfer(ChamferCut::CONTROL)] {
            let ring = shape.ring(gpui::red());
            assert_eq!(ring.thickness, Borders::FOCUS_RING, "{shape:?}");
            assert_eq!(ring.offset, -Borders::FOCUS_RING, "{shape:?}");
        }
    }

    #[test]
    fn filled_shapes_move_the_ring_two_pixels_outside() {
        let ring = FocusShape::FilledChamfer(ChamferCut::CONTROL).ring(gpui::red());

        assert_eq!(ring.thickness, Borders::FOCUS_RING);
        assert_eq!(ring.offset, Borders::MEDIUM);
    }

    #[test]
    fn rect_shape_has_no_cut() {
        assert_eq!(FocusShape::Rect.cut(), gpui::px(0.0));
        assert_eq!(
            FocusShape::Chamfer(ChamferCut::INPUT).cut(),
            ChamferCut::INPUT
        );
    }
}
