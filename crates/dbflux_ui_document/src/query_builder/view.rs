use gpui::{Context, IntoElement, Window, div};

use super::panel::QueryBuilderPanel;

/// Top-level render function for `QueryBuilderPanel`.
///
/// Builds the panel UI from five sections: Columns, Filters, Joins, Sort,
/// and Limit/Offset, plus a header, SQL preview, and footer.
/// Each section delegates to its own render helper for clarity.
///
/// Also flushes pending state syncs here, while `Window` is available:
/// - `pending_preview_sync` → pushes the latest SQL text into the read-only editor
/// - `pending_join_rebuild` → creates InputState entities for newly added join rows
pub fn render_panel(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::prelude::*;

    if panel.pending_preview_sync {
        panel.pending_preview_sync = false;
        if let Some(state) = panel.sql_preview_state.clone() {
            let text = panel.sql_preview.clone();
            state.update(cx, |s, cx| {
                s.set_value(&text, window, cx);
            });
        }
    }

    if panel.pending_join_rebuild {
        panel.pending_join_rebuild = false;
        panel.rebuild_join_input_states(window, cx);
    }

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

    div().flex().flex_row().p_2().gap_2().child(
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
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Input;
    use gpui::SharedString;
    use gpui::prelude::*;

    let mut row = div().flex().flex_row().p_2().gap_2();

    if let Some(limit_state) = panel.limit_input_state.as_ref() {
        row = row.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(div().text_sm().child(SharedString::from("Limit")))
                .child(Input::new(limit_state).small().w_full()),
        );
    } else {
        row = row.child(div().text_sm().child(SharedString::from("Limit")));
    }

    if let Some(offset_state) = panel.offset_input_state.as_ref() {
        row = row.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(div().text_sm().child(SharedString::from("Offset")))
                .child(Input::new(offset_state).small().w_full()),
        );
    } else {
        row = row.child(div().text_sm().child(SharedString::from("Offset")));
    }

    let _ = cx;
    row
}

fn render_preview(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::ReadonlyTextView;
    use gpui::SharedString;
    use gpui::prelude::*;
    use gpui::px;

    div()
        .flex()
        .flex_col()
        .p_2()
        .child(div().text_sm().child(SharedString::from("SQL Preview")))
        .when_some(panel.sql_preview_state.as_ref(), |container, state| {
            container.child(ReadonlyTextView::new(state).w_full().h(px(120.0)))
        })
}

fn render_footer(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, ButtonVariant};
    use gpui::prelude::*;

    let is_runnable = panel.is_runnable();

    div()
        .flex()
        .flex_row()
        .p_2()
        .gap_2()
        .child(
            Button::new("qb-run", "Run")
                .primary()
                .disabled(!is_runnable)
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::RunRequested);
                })),
        )
        .child(
            Button::new("qb-open-editor", "Open in Editor")
                .variant(ButtonVariant::Ghost)
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::OpenInEditorRequested);
                })),
        )
}
