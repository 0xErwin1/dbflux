use gpui::{Context, IntoElement, Window, div};

use super::panel::QueryBuilderPanel;

/// Top-level render function for `QueryBuilderPanel`.
///
/// Builds the panel UI from five sections: Columns, Filters, Joins, Sort,
/// and Limit/Offset, plus a header, SQL preview, and footer.
/// Each section delegates to its own render helper for clarity.
pub fn render_panel(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::prelude::*;

    let container = div().flex().flex_col().size_full();

    let container = match &panel.focus_handle {
        Some(handle) => container.track_focus(handle),
        None => container,
    };

    container
        .child(render_header(panel, window, cx))
        .child(render_columns_section(panel, window, cx))
        .child(render_filters_section(panel, window, cx))
        .child(render_joins_section(panel, window, cx))
        .child(render_sort_section(panel, window, cx))
        .child(render_limit_offset(panel, window, cx))
        .child(render_preview(panel, window, cx))
        .child(render_footer(panel, window, cx))
}

fn render_header(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    div().flex().flex_row().p_2().child(
        div().flex_1().child(
            div()
                .text_sm()
                .child(SharedString::from(panel.current_spec.source.table.clone())),
        ),
    )
}

fn render_columns_section(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::sections::columns::render_columns;
    use gpui::prelude::*;
    render_columns(panel, cx)
}

fn render_filters_section(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::sections::filters::render_filters;
    render_filters(panel, cx)
}

fn render_joins_section(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::sections::joins::render_joins;
    render_joins(panel, cx)
}

fn render_sort_section(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::sections::sort::render_sort;
    render_sort(panel, cx)
}

fn render_limit_offset(
    _panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    div()
        .flex()
        .flex_row()
        .p_2()
        .gap_2()
        .child(
            div()
                .flex_row()
                .gap_1()
                .child(div().text_sm().child(SharedString::from("Limit"))),
        )
        .child(
            div()
                .flex_row()
                .gap_1()
                .child(div().text_sm().child(SharedString::from("Offset"))),
        )
}

fn render_preview(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    div()
        .flex()
        .flex_col()
        .p_2()
        .child(div().text_sm().child(SharedString::from("SQL Preview")))
        .child(
            div()
                .font_family("monospace")
                .text_sm()
                .child(SharedString::from(panel.sql_preview.clone())),
        )
}

fn render_footer(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    let _is_runnable = panel.is_runnable();

    div()
        .flex()
        .flex_row()
        .p_2()
        .gap_2()
        .child(div().text_sm().child(SharedString::from("Run")))
        .child(div().text_sm().child(SharedString::from("Open in Editor")))
}
