//! `Island` — a chamfered pane floating on the desk (Isl* boards).
//!
//! Every pane of the main window (sidebar, document, right-side panels) and
//! of the Settings and Connection Manager windows is an island: the panel
//! fill cut 14 px at the top-left and bottom-right corners, a hairline along
//! the full outline, and an `IslandMetrics::GAP` of desk between islands.

use gpui::prelude::*;
use gpui::{AnyElement, App, Div, Hsla, Pixels, StyleRefinement, Window, div};
use gpui_component::ActiveTheme;

use crate::primitives::{Chamfer, ChamferRing};
use crate::tokens::{ChromeColorSlot, ChromeColors, IslandMetrics};

/// Render-free description of an island.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IslandInspection {
    pub fill: ChromeColorSlot,
    pub cut: Pixels,
    /// Thickness of the hairline along the outline; zero draws none.
    pub edge: Pixels,
}

pub fn inspect_island() -> IslandInspection {
    IslandInspection {
        fill: ChromeColorSlot::Popover,
        cut: IslandMetrics::CUT,
        edge: IslandMetrics::EDGE,
    }
}

/// The hairline of an island: `IslandMetrics::EDGE` thick, fully inside the
/// bounds, following the cuts. `None` when the edge token is zero.
pub fn island_edge_ring(theme: &gpui_component::Theme) -> Option<ChamferRing> {
    let edge = inspect_island().edge;

    (edge > Pixels::ZERO).then(|| ChamferRing {
        color: ChromeColors::island_edge(theme),
        thickness: edge,
        offset: -edge,
        focus_visible: false,
    })
}

/// A chamfered pane on the desk.
///
/// It lays out as a flex column that clips its overflow; chain layout and
/// sizing like on a `div` and add content with `.child()`. The fill is painted
/// behind the content, and the cut corners and the hairline are painted over
/// it in the desk color, so content that fills its rows (headers, selected
/// rows, toolbars) still reads as cut. Do not set `.bg()` on it.
#[derive(IntoElement)]
pub struct Island {
    base: Div,
    children: Vec<AnyElement>,
}

impl Island {
    pub fn new() -> Self {
        Self {
            base: div().relative().flex().flex_col().overflow_hidden(),
            children: Vec::new(),
        }
    }
}

impl Default for Island {
    fn default() -> Self {
        Self::new()
    }
}

impl Styled for Island {
    fn style(&mut self) -> &mut StyleRefinement {
        self.base.style()
    }
}

impl ParentElement for Island {
    fn extend(&mut self, elements: impl IntoIterator<Item = AnyElement>) {
        self.children.extend(elements);
    }
}

impl RenderOnce for Island {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let inspection = inspect_island();
        let fill = island_fill(theme);

        let mut mask = Chamfer::new(inspection.cut).outside_fill(ChromeColors::desk(theme));

        if let Some(ring) = island_edge_ring(theme) {
            mask = mask.ring(ring);
        }

        self.base
            .child(Chamfer::new(inspection.cut).fill(fill))
            .children(self.children)
            .child(mask)
    }
}

/// Frame for a side panel that a document draws inside its own island, such
/// as a settings rail or a preview. The desk shows through an
/// `IslandMetrics::GAP` gutter on the panel's left and top, so an `Island`
/// placed inside reads as a pane of its own next to the document. Size it
/// `IslandMetrics::GAP` wider than the panel.
pub fn docked_island_frame(theme: &gpui_component::Theme) -> Div {
    div()
        .flex()
        .flex_col()
        .pl(IslandMetrics::GAP)
        .pt(IslandMetrics::GAP)
        .bg(ChromeColors::desk(theme))
}

/// Fill of an island, per theme: the panel color.
pub fn island_fill(theme: &gpui_component::Theme) -> Hsla {
    inspect_island().fill.resolve(theme)
}

#[cfg(test)]
mod tests {
    use super::{inspect_island, island_edge_ring};
    use crate::tokens::{ChamferCut, ChromeColorSlot, ChromeColors, IslandMetrics};

    #[test]
    fn islands_use_the_panel_fill_and_the_card_cut() {
        let inspection = inspect_island();

        assert_eq!(inspection.fill, ChromeColorSlot::Popover);
        assert_eq!(inspection.cut, ChamferCut::CARD);
        assert_eq!(inspection.edge, IslandMetrics::EDGE);
    }

    #[test]
    fn the_desk_and_the_hairline_follow_the_theme_mode() {
        let mut theme = gpui_component::Theme {
            mode: gpui_component::ThemeMode::Dark,
            ..gpui_component::Theme::default()
        };

        let dark_desk = ChromeColors::desk(&theme);
        let dark_edge = ChromeColors::island_edge(&theme);

        theme.mode = gpui_component::ThemeMode::Light;
        let light_desk = ChromeColors::desk(&theme);
        let light_edge = ChromeColors::island_edge(&theme);

        assert_ne!(dark_desk, light_desk);
        assert!(dark_desk.l < light_desk.l);
        assert!((dark_edge.a - 0.05).abs() < f32::EPSILON);
        assert!((light_edge.a - 0.06).abs() < f32::EPSILON);
    }

    #[test]
    fn the_hairline_stays_inside_the_outline_and_never_waits_for_focus() {
        let theme = gpui_component::Theme::default();
        let ring = island_edge_ring(&theme).expect("islands draw a hairline");

        assert_eq!(ring.thickness, IslandMetrics::EDGE);
        assert_eq!(ring.offset, -IslandMetrics::EDGE);
        assert!(!ring.focus_visible);
        assert_eq!(ring.color, ChromeColors::island_edge(&theme));
    }
}
