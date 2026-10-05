use gpui::*;

use crate::tokens::Borders;

/// Per-node metadata needed to draw indent guides.
///
/// Used by callers that don't use `TreeNav` directly (e.g. the sidebar, which
/// uses `gpui_component::tree::TreeState` for virtual scrolling) but still want
/// the same gutter visuals.
#[derive(Debug, Clone)]
pub struct GutterInfo {
    pub depth: usize,
    pub is_last: bool,
    pub ancestors_continue: Vec<bool>,
}

/// Indent guide color: the palette line.
pub fn tree_line_color(theme: &gpui_component::Theme) -> Hsla {
    theme.border
}

/// Render the indent of one tree row with its guides (AppByzTable sidebar,
/// DSApp "Tree"): one 1 px vertical line per ancestor level, running the full
/// row height through the middle of that ancestor's chevron column.
///
/// `indent_px` is the width of one level and `row_height` the fixed row
/// height, in pixels or rems. Set `skip_level_zero` for trees whose depth-0
/// rows are category headers without a chevron column (e.g. Settings sidebar
/// groups).
pub fn render_gutter(
    depth: usize,
    indent_px: f32,
    row_height: impl Into<AbsoluteLength>,
    line_color: Hsla,
    skip_level_zero: bool,
) -> AnyElement {
    if depth == 0 {
        return div().w(px(0.0)).flex_shrink_0().into_any_element();
    }

    let first_level = usize::from(skip_level_zero);
    let row_height: AbsoluteLength = row_height.into();

    let guides = guide_offsets(depth, indent_px, first_level).map(|offset| {
        div()
            .absolute()
            .left(px(offset))
            .top_0()
            .bottom_0()
            .w(Borders::THIN)
            .bg(line_color)
            .into_any_element()
    });

    div()
        .w(px(depth as f32 * indent_px))
        .h(row_height)
        .relative()
        .flex_shrink_0()
        .children(guides)
        .into_any_element()
}

/// Horizontal offsets, from the start of the indent, of the guides of a row
/// at `depth`: one per level from `first_level`, centered in its column.
fn guide_offsets(depth: usize, indent_px: f32, first_level: usize) -> impl Iterator<Item = f32> {
    (first_level..depth).map(move |level| level as f32 * indent_px + indent_px / 2.0)
}

#[cfg(test)]
mod tests {
    use super::guide_offsets;

    #[test]
    fn every_ancestor_level_gets_a_centered_guide() {
        let offsets: Vec<f32> = guide_offsets(3, 14.0, 0).collect();

        assert_eq!(offsets, vec![7.0, 21.0, 35.0]);
    }

    #[test]
    fn skipping_level_zero_drops_the_first_guide() {
        let offsets: Vec<f32> = guide_offsets(2, 14.0, 1).collect();

        assert_eq!(offsets, vec![21.0]);
    }
}
