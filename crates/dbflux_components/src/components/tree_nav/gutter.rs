use gpui::*;

const LINE_WEIGHT: f32 = 1.0;

/// Per-node metadata needed to draw tree connector lines.
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

/// Derive the tree line color from the theme's muted foreground at reduced opacity.
pub fn tree_line_color(theme: &gpui_component::Theme) -> Hsla {
    let mut color = theme.muted_foreground;
    color.a = 0.35;
    color
}

/// Render tree connector lines for a single row.
///
/// The caller passes the three layout fields (`depth`, `is_last`,
/// `ancestors_continue`) that describe where this row sits in the tree.
///
/// `indent_px` controls horizontal spacing per level; `row_height` is the
/// fixed height of each row; `line_color` is the connector line color.
///
/// Set `skip_level_zero` to true for trees where depth-0 items are category
/// headers that have no gutter (e.g. Settings sidebar groups).
pub fn render_gutter(
    depth: usize,
    is_last: bool,
    ancestors_continue: &[bool],
    indent_px: f32,
    row_height: Pixels,
    line_color: Hsla,
    skip_level_zero: bool,
) -> AnyElement {
    if depth == 0 {
        return div().w(px(0.0)).flex_shrink_0().into_any_element();
    }

    let gutter_width = depth as f32 * indent_px;
    let center_y = f32::from(row_height) / 2.0;
    let min_ancestor_level: usize = if skip_level_zero { 1 } else { 0 };
    let connector_level = depth - 1;

    let mut lines: Vec<AnyElement> = Vec::new();

    for level in continuation_levels(depth, ancestors_continue, min_ancestor_level) {
        lines.push(
            div()
                .absolute()
                .left(px(level as f32 * indent_px + indent_px / 2.0))
                .top_0()
                .bottom_0()
                .w(px(LINE_WEIGHT))
                .bg(line_color)
                .into_any_element(),
        );
    }

    let connector_x = connector_level as f32 * indent_px + indent_px / 2.0;

    if is_last {
        lines.push(
            div()
                .absolute()
                .left(px(connector_x))
                .top_0()
                .h(px(center_y + LINE_WEIGHT))
                .w(px(LINE_WEIGHT))
                .bg(line_color)
                .into_any_element(),
        );
    } else {
        lines.push(
            div()
                .absolute()
                .left(px(connector_x))
                .top_0()
                .bottom_0()
                .w(px(LINE_WEIGHT))
                .bg(line_color)
                .into_any_element(),
        );
    }

    lines.push(
        div()
            .absolute()
            .left(px(connector_x))
            .top(px(center_y))
            .w(px(indent_px / 2.0))
            .h(px(LINE_WEIGHT))
            .bg(line_color)
            .into_any_element(),
    );

    div()
        .w(px(gutter_width))
        .h(row_height)
        .relative()
        .flex_shrink_0()
        .children(lines)
        .into_any_element()
}

/// Levels whose vertical continuation line must be drawn for a row at `depth`.
///
/// The line at level `k` sits under the chevron of the ancestor at depth `k`,
/// so it is the spine of that ancestor's children. It continues past this row
/// only when the ancestor at depth `k + 1` still has later siblings, which is
/// `ancestors_continue[k + 1]`. The row's own connector occupies level
/// `depth - 1` and is drawn separately.
fn continuation_levels(
    depth: usize,
    ancestors_continue: &[bool],
    min_level: usize,
) -> impl Iterator<Item = usize> + '_ {
    let connector_level = depth.saturating_sub(1);

    (min_level..connector_level)
        .filter(move |level| ancestors_continue.get(level + 1).copied().unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::continuation_levels;

    fn levels(depth: usize, ancestors: &[bool], min_level: usize) -> Vec<usize> {
        continuation_levels(depth, ancestors, min_level).collect()
    }

    #[test]
    fn spine_under_last_parent_stops_at_grandchildren() {
        // root(continues) > parent(last) > child: the column under root's
        // chevron is the spine of root's children, which ends at parent.
        assert_eq!(levels(2, &[true, false], 0), Vec::<usize>::new());
    }

    #[test]
    fn spine_under_continuing_parent_passes_through_grandchildren() {
        // root(last) > parent(continues) > child: root has no spine column,
        // parent's siblings keep the column under root's chevron alive.
        assert_eq!(levels(2, &[false, true], 0), vec![0]);
    }

    #[test]
    fn deeper_rows_map_each_column_to_the_next_ancestor() {
        // depth 4: columns 0..=2 follow ancestors at depth 1..=3.
        assert_eq!(levels(4, &[true, true, false, true], 0), vec![0, 2]);
    }

    #[test]
    fn own_connector_level_is_excluded() {
        assert_eq!(levels(1, &[true], 0), Vec::<usize>::new());
    }

    #[test]
    fn min_level_skips_column_zero() {
        assert_eq!(levels(3, &[false, true, true], 1), vec![1]);
    }
}
