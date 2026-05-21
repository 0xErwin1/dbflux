use crate::ui::tokens::{Heights, Spacing};
use dbflux_components::primitives::Text;
use gpui::prelude::*;
use gpui::*;
use gpui_component::theme::Theme;

/// Toolbar bar that matches the DataGridPanel toolbar exactly:
/// `h(Heights::TOOLBAR)` / `bg(theme.secondary)` / `border_b_1`.
///
/// `flex_shrink_0` ensures that when toolbar items wrap onto multiple rows the
/// bar claims its full content-driven height before the sibling `flex_1`
/// content area gets the residual space.  Without it the parent `flex_col`
/// keeps the bar at `min_h(TOOLBAR)` and clipped wrapped rows are invisible.
///
/// `py(Spacing::XS)` adds breathing room between wrapped rows.
pub(crate) fn compact_top_bar(
    theme: &Theme,
    children: impl IntoIterator<Item = AnyElement>,
) -> Div {
    div()
        .flex()
        .flex_wrap()
        .flex_shrink_0()
        .items_center()
        .gap(Spacing::SM)
        .min_h(Heights::TOOLBAR)
        .py(Spacing::XS)
        .px(Spacing::SM)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary)
        .children(children)
}

/// Labeled control pair matching the `WHERE`/`LIMIT` style in DataGridPanel:
/// muted label text + control inline.
#[allow(dead_code)]
pub(crate) fn compact_labeled_control(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    _theme: &Theme,
) -> Div {
    div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .child(Text::caption(label.into()))
        .child(control)
}

/// Status/footer bar that matches the DataGridPanel status bar exactly:
/// `h(Heights::ROW_COMPACT)` / `bg(theme.tab_bar)` / `border_t_1`.
pub(crate) fn workspace_footer_bar(
    theme: &Theme,
    left: impl IntoElement,
    center: impl IntoElement,
    right: impl IntoElement,
) -> Div {
    div()
        .flex()
        .items_center()
        .justify_between()
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .border_t_1()
        .border_color(theme.border)
        .bg(theme.tab_bar)
        .child(div().flex().items_center().gap(Spacing::SM).child(left))
        .child(div().flex().items_center().gap(Spacing::SM).child(center))
        .child(div().flex().items_center().gap(Spacing::SM).child(right))
}
