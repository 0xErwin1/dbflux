use dbflux_components::controls::{Button, ButtonVariant, Input, ReadonlyTextView};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::{AnyElement, Context, IntoElement, SharedString, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use gpui_component::theme::Theme;

use super::panel::QueryBuilderPanel;

/// Top-level render function for `QueryBuilderPanel`.
///
/// Renders a sticky header (source + Save/Reset), a scrollable middle pane
/// containing the section cards, and a sticky footer with Run / Open in
/// Editor. State syncs that need `Window` are flushed at the top.
pub fn render_panel(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
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

    let theme = cx.theme().clone();

    let container = div().flex().flex_col().size_full().bg(theme.background);

    let container = match &panel.focus_handle {
        Some(handle) => container.track_focus(handle),
        None => container,
    };

    container
        .child(render_header(panel, &theme, cx))
        .child(render_body(panel, &theme, cx))
        .child(render_footer(panel, &theme, cx))
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

fn render_header(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let source_table = panel.current_spec.source.table.clone();
    let source_schema = panel.current_spec.source.schema.clone();

    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(Spacing::SM)
        .px(Spacing::MD)
        .h(Heights::HEADER)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.background)
        .child(
            Icon::new(AppIcon::Table)
                .small()
                .color(theme.muted_foreground),
        )
        .child(Text::label(SharedString::from(source_table)).color(theme.foreground))
        .when_some(source_schema, |row, schema| {
            row.child(
                div()
                    .px(Spacing::XS)
                    .rounded(Radii::SM)
                    .bg(theme.secondary)
                    .child(Text::caption(SharedString::from(schema)).color(theme.muted_foreground)),
            )
        })
        .child(div().flex_1())
        .child(
            Button::new("qb-hdr-save", "Save")
                .icon(AppIcon::Save)
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
                .icon(AppIcon::RotateCcw)
                .ghost()
                .small()
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::ResetRequested);
                })),
        )
}

// ---------------------------------------------------------------------------
// Scrollable body with section cards
// ---------------------------------------------------------------------------

fn render_body(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::sections::{columns, filters, joins, sort};

    let columns_body = columns::render_columns(panel, cx).into_any_element();
    let filters_body = filters::render_filters(panel, cx).into_any_element();
    let joins_body = joins::render_joins(panel, cx).into_any_element();
    let sort_body = sort::render_sort(panel, cx).into_any_element();
    let limit_body = render_limit_offset_body(panel).into_any_element();
    let preview_body = render_preview_body(panel, theme).into_any_element();

    div()
        .flex_1()
        .min_h(px(0.0))
        .overflow_y_scrollbar()
        .child(section_card(
            "COLUMNS",
            AppIcon::Columns,
            theme,
            columns_body,
        ))
        .child(section_card(
            "FILTERS",
            AppIcon::ListFilter,
            theme,
            filters_body,
        ))
        .child(section_card("JOINS", AppIcon::Layers, theme, joins_body))
        .child(section_card("SORT", AppIcon::ArrowUpDown, theme, sort_body))
        .child(section_card(
            "LIMIT & OFFSET",
            AppIcon::Hash,
            theme,
            limit_body,
        ))
        .child(section_card(
            "SQL PREVIEW",
            AppIcon::Code,
            theme,
            preview_body,
        ))
}

/// Renders a section as a bordered card with an uppercase header bar and
/// a padded body. Used for every section in the builder panel so the
/// hierarchy stays consistent.
fn section_card(
    title: &'static str,
    icon: AppIcon,
    theme: &Theme,
    body: AnyElement,
) -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .border_b_1()
        .border_color(theme.border)
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap(Spacing::XS)
                .h(Heights::TOOLBAR)
                .px(Spacing::MD)
                .bg(theme.secondary)
                .child(Icon::new(icon).small().color(theme.muted_foreground))
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child(SharedString::from(title)),
                ),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .px(Spacing::MD)
                .py(Spacing::SM)
                .child(body),
        )
}

// ---------------------------------------------------------------------------
// Limit & Offset (small enough to keep inline)
// ---------------------------------------------------------------------------

fn render_limit_offset_body(panel: &mut QueryBuilderPanel) -> impl IntoElement {
    let row = div().flex().flex_row().gap(Spacing::MD).items_center();

    let row = if let Some(limit_state) = panel.limit_input_state.as_ref() {
        row.child(
            div()
                .flex_1()
                .flex()
                .flex_col()
                .gap(Spacing::XXS)
                .child(Text::caption(SharedString::from("Limit")))
                .child(Input::new(limit_state).small().w_full()),
        )
    } else {
        row.child(
            div()
                .flex_1()
                .child(Text::caption(SharedString::from("Limit"))),
        )
    };

    if let Some(offset_state) = panel.offset_input_state.as_ref() {
        row.child(
            div()
                .flex_1()
                .flex()
                .flex_col()
                .gap(Spacing::XXS)
                .child(Text::caption(SharedString::from("Offset")))
                .child(Input::new(offset_state).small().w_full()),
        )
    } else {
        row.child(
            div()
                .flex_1()
                .child(Text::caption(SharedString::from("Offset"))),
        )
    }
}

// ---------------------------------------------------------------------------
// SQL Preview
// ---------------------------------------------------------------------------

fn render_preview_body(panel: &mut QueryBuilderPanel, theme: &Theme) -> impl IntoElement {
    let line_count = panel.sql_preview.lines().count().max(1);
    let line_label = if line_count == 1 { "line" } else { "lines" };
    let status_text = format!("valid · {line_count} {line_label}");

    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap(Spacing::XS)
                .child(
                    Icon::new(AppIcon::CircleCheck)
                        .small()
                        .color(theme.muted_foreground),
                )
                .child(
                    Text::caption(SharedString::from(status_text)).color(theme.muted_foreground),
                ),
        )
        .when_some(panel.sql_preview_state.as_ref(), |container, state| {
            container.child(
                div()
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.border)
                    .child(ReadonlyTextView::new(state).w_full().h(px(140.0))),
            )
        })
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

fn render_footer(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let is_runnable = panel.is_runnable();

    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(Spacing::SM)
        .px(Spacing::MD)
        .h(Heights::HEADER)
        .border_t_1()
        .border_color(theme.border)
        .bg(theme.background)
        .child(
            Button::new("qb-run", "Run")
                .icon(AppIcon::Play)
                .primary()
                .small()
                .disabled(!is_runnable)
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::RunRequested);
                })),
        )
        .child(
            Button::new("qb-open-editor", "Open in Editor")
                .icon(AppIcon::ExternalLink)
                .variant(ButtonVariant::Ghost)
                .small()
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::OpenInEditorRequested);
                })),
        )
        .child(div().flex_1())
}

// ---------------------------------------------------------------------------
// Predicate input lifecycle
// ---------------------------------------------------------------------------

/// Walks the current filter tree and ensures every `Predicate` node has a
/// corresponding `Entity<InputState>` in `panel.predicate_input_states`.
///
/// Runs every render cycle so predicates loaded from a saved query also get
/// their input state created on first render.
fn ensure_predicate_inputs(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) {
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
            let column_ref = format!("{}.{}", pred.source_alias, pred.column);
            panel.ensure_predicate_input(pred.node_id, path.clone(), &current_value, window, cx);
            panel.ensure_predicate_column_input(
                pred.node_id,
                path.clone(),
                &column_ref,
                window,
                cx,
            );
            panel.ensure_predicate_comparator_dropdown(pred.node_id, path, pred.comparator, cx);
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
