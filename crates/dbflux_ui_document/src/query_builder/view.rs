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

    if panel.pending_filter_input_sweep {
        panel.pending_filter_input_sweep = false;
        panel.sweep_stale_predicate_inputs();
    }

    ensure_predicate_inputs(panel, window, cx);

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

/// Walks the current filter tree and ensures every `Predicate` node has a
/// corresponding `Entity<InputState>` in `panel.predicate_input_states`.
///
/// This must run every render cycle (not only on mutations) so that predicates
/// loaded from a saved query also get their input state created on first render.
fn ensure_predicate_inputs(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) {
    use dbflux_core::FilterNode;

    let filter = panel.current_spec.filter.clone();
    if let Some(root) = filter {
        ensure_in_node(panel, &root, vec![], window, cx);
    }
}

fn ensure_in_node(
    panel: &mut QueryBuilderPanel,
    node: &dbflux_core::FilterNode,
    path: Vec<usize>,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) {
    use dbflux_core::FilterNode;

    match node {
        FilterNode::Predicate(pred) => {
            let current_value = match &pred.value {
                dbflux_core::PredicateValue::None => String::new(),
                dbflux_core::PredicateValue::Single(v) => literal_to_display_string(v),
                dbflux_core::PredicateValue::List(vs) => vs
                    .iter()
                    .map(literal_to_display_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            panel.ensure_predicate_input(pred.node_id, path, &current_value, window, cx);
        }
        FilterNode::Group { children, .. } => {
            for (i, child) in children.iter().enumerate() {
                let mut child_path = path.clone();
                child_path.push(i);
                ensure_in_node(panel, child, child_path, window, cx);
            }
        }
    }
}

fn literal_to_display_string(v: &dbflux_core::LiteralValue) -> String {
    use dbflux_core::LiteralValue;
    match v {
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Integer(n) => n.to_string(),
        LiteralValue::Float(f) => f.to_string(),
        LiteralValue::Bool(b) => b.to_string(),
        LiteralValue::Timestamp(t) => t.clone(),
        LiteralValue::Null => "NULL".to_string(),
    }
}

fn render_header(
    panel: &mut QueryBuilderPanel,
    _window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use gpui::SharedString;
    use gpui::prelude::*;

    div()
        .flex()
        .flex_row()
        .p_2()
        .gap_2()
        .items_center()
        .child(
            div()
                .flex_1()
                .text_sm()
                .child(SharedString::from(panel.current_spec.source.table.clone())),
        )
        .child(
            Button::new("qb-hdr-save", "Save")
                .ghost()
                .small()
                .on_click(cx.listener(|this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    let name = this
                        .loaded_id
                        .as_deref()
                        .unwrap_or("Untitled query")
                        .to_string();
                    cx.emit(BuilderEvent::SaveRequested { name });
                })),
        )
        .child(
            Button::new("qb-hdr-reset", "Reset")
                .ghost()
                .small()
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::ResetRequested);
                })),
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
