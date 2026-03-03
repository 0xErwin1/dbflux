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

    for (level, continues) in ancestors_continue.iter().enumerate() {
        if *continues && level >= min_ancestor_level && level < connector_level {
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
