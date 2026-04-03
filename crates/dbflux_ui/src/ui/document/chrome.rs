use crate::ui::tokens::{FontSizes, Heights, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::theme::Theme;

/// Toolbar bar that matches the DataGridPanel toolbar exactly:
/// `h(Heights::TOOLBAR)` / `bg(theme.secondary)` / `border_b_1`.
pub(crate) fn compact_top_bar(
    theme: &Theme,
    children: impl IntoIterator<Item = AnyElement>,
) -> Div {
    div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .h(Heights::TOOLBAR)
        .px(Spacing::SM)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary)
        .children(children)
}

/// Labeled control pair matching the `WHERE`/`LIMIT` style in DataGridPanel:
/// muted label text + control inline.
pub(crate) fn compact_labeled_control(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    theme: &Theme,
) -> Div {
    div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .child(
            div()
                .text_size(FontSizes::SM)
                .text_color(theme.muted_foreground)
                .child(label.into()),
        )
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
