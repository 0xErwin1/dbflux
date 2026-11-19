//! Where a flyout goes. Pure rect arithmetic, because a submenu flips
//! horizontally and the anchoring helper only snaps.

use crate::composites::menu_tree::model::MenuNode;
use gpui::{Bounds, Pixels, Point, Size, point, px};

/// The side a flyout opens on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlyoutSide {
    /// To the right of the parent panel, its left edge overlapping it.
    Right,
    /// To the left of the parent panel, its right edge overlapping it.
    Left,
}

impl FlyoutSide {
    pub fn flipped(self) -> Self {
        match self {
            Self::Right => Self::Left,
            Self::Left => Self::Right,
        }
    }
}

/// Where one flyout lands, and how tall it may be before it scrolls.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FlyoutPlacement {
    pub side: FlyoutSide,
    /// Top-left corner of the flyout in window space.
    pub origin: Point<Pixels>,
    /// Height the flyout may reach; taller content scrolls inside it.
    pub max_height: Pixels,
    /// Whether this flyout's content exceeds [`Self::max_height`].
    pub scrolls: bool,
}

/// What a flyout would like to be placed in.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FlyoutSpec {
    pub size: Size<Pixels>,
    pub viewport: Size<Pixels>,
    /// Gap kept between the flyout and the window edge.
    pub margin: Pixels,
    /// How far the flyout overlaps the parent panel, so the two frames meet
    /// rather than leaving a seam.
    pub overlap: Pixels,
}

/// Pixel heights of the rows of one level, for the height estimate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LevelMetrics {
    pub row_height: Pixels,
    pub detail_row_height: Pixels,
    pub header_height: Pixels,
    pub separator_height: Pixels,
    pub padding_y: Pixels,
}

/// The height a level occupies: its padding, its rows, and the header and
/// separator blocks between them.
///
/// The flyout is placed from an estimate rather than from a measurement,
/// because a menu has to be positioned in the same frame it first draws in.
pub fn level_height(nodes: &[MenuNode], metrics: &LevelMetrics) -> Pixels {
    let rows = nodes
        .iter()
        .fold(px(0.0), |total, node| total + row_height(node, metrics));

    rows + metrics.padding_y * 2.0
}

/// The height one row takes in a level: its block for a header or a separator,
/// the taller row when it carries a detail line, its row height otherwise.
pub(crate) fn row_height(node: &MenuNode, metrics: &LevelMetrics) -> Pixels {
    if node.item.is_separator {
        metrics.separator_height
    } else if node.item.is_header {
        metrics.header_height
    } else if node.item.detail.is_some() {
        metrics.detail_row_height
    } else {
        metrics.row_height
    }
}

/// Places a flyout beside the parent panel, top-aligned to the row that opened
/// it.
///
/// `parent` is the parent level's frame, `row_top` the top of the row the
/// flyout hangs off, and `vertical_inset` how far above that row the flyout
/// frame starts so its first row lines up with it.
pub fn place_flyout(
    parent: Bounds<Pixels>,
    row_top: Pixels,
    vertical_inset: Pixels,
    spec: FlyoutSpec,
) -> FlyoutPlacement {
    let max_height = (spec.viewport.height - spec.margin * 2.0).max(spec.margin);
    let scrolls = spec.size.height > max_height;

    let (side, unbounded_x) = horizontal_side(parent, spec);

    let right_limit = spec.viewport.width - spec.margin;
    let widest_left = (right_limit - spec.size.width).max(spec.margin);
    let x = unbounded_x.max(spec.margin).min(widest_left);

    FlyoutPlacement {
        side,
        origin: point(
            x,
            vertical_origin(row_top, vertical_inset, spec, max_height),
        ),
        max_height,
        scrolls,
    }
}

/// The side the flyout opens on, and the x it would take there.
fn horizontal_side(parent: Bounds<Pixels>, spec: FlyoutSpec) -> (FlyoutSide, Pixels) {
    let opens_at = parent.origin.x + parent.size.width - spec.overlap;
    let flips_at = parent.origin.x - spec.size.width + spec.overlap;
    let right_limit = spec.viewport.width - spec.margin;

    let fits_right = opens_at + spec.size.width <= right_limit;
    let fits_left = flips_at >= spec.margin;

    if fits_right {
        return (FlyoutSide::Right, opens_at);
    }

    // Neither side has room in a window narrower than the cascade: open on the
    // side of the window the parent leaves more space on.
    let parent_is_right_of_centre = parent.center().x > spec.viewport.width * 0.5;

    if fits_left || parent_is_right_of_centre {
        (FlyoutSide::Left, flips_at)
    } else {
        (FlyoutSide::Right, opens_at)
    }
}

/// The y that puts the flyout's first row level with the parent row, slid up
/// only as far as the window's bottom edge requires.
fn vertical_origin(
    row_top: Pixels,
    vertical_inset: Pixels,
    spec: FlyoutSpec,
    max_height: Pixels,
) -> Pixels {
    let aligned = row_top - vertical_inset;
    let bottom_limit = spec.viewport.height - spec.margin;
    let visible_height = spec.size.height.min(max_height);
    let overflow = (aligned + visible_height - bottom_limit).max(px(0.0));

    (aligned - overflow).max(spec.margin)
}
