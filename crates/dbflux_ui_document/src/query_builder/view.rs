use dbflux_components::controls::{Button, ButtonVariant, Input, ReadonlyTextView};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::{AnyElement, Context, IntoElement, SharedString, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use gpui_component::theme::Theme;

use crate::query_builder::mutation_state::BuilderMode;

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

    if panel.pending_group_by_rebuild {
        panel.pending_group_by_rebuild = false;
        panel.rebuild_group_by_input_states(window, cx);
    }

    if panel.pending_filter_input_sweep {
        panel.pending_filter_input_sweep = false;
        panel.sweep_stale_predicate_inputs();
    }

    if panel.pending_having_input_sweep {
        panel.pending_having_input_sweep = false;
        panel.sweep_stale_having_predicate_inputs();
    }

    ensure_predicate_inputs(panel, window, cx);
    ensure_having_predicate_inputs(panel, window, cx);
    ensure_join_condition_inputs(panel, window, cx);

    if panel.pending_join_condition_sweep {
        panel.pending_join_condition_sweep = false;
        panel.sweep_stale_join_condition_state();
    }

    if panel.pending_assign_rebuild {
        panel.pending_assign_rebuild = false;
        panel.rebuild_assign_inputs(window, cx);
    }

    panel.maybe_refresh_mutation_count(cx);

    let theme = cx.theme().clone();

    let show_mode_selector = panel.shows_mutation_selector(cx);

    let container = div().flex().flex_col().size_full().bg(theme.background);

    let container = match &panel.focus_handle {
        Some(handle) => container.track_focus(handle),
        None => container,
    };

    container
        .child(render_header(panel, &theme, cx))
        .when(show_mode_selector, |c| {
            c.child(render_mode_selector(panel, &theme, cx))
        })
        .child(render_body(panel, &theme, cx))
        .child(render_preview_pane(panel, &theme))
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
// Mode selector (SELECT / UPDATE / DELETE) — shown only for SQL connections
// ---------------------------------------------------------------------------

fn render_mode_selector(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use crate::query_builder::mutation_state::BuilderMode;

    let current_mode = panel
        .mutation_state
        .as_ref()
        .map(|s| s.mode)
        .unwrap_or(BuilderMode::Select);

    let modes: [(BuilderMode, &'static str); 3] = [
        (BuilderMode::Select, "SELECT"),
        (BuilderMode::Update, "UPDATE"),
        (BuilderMode::Delete, "DELETE"),
    ];

    let mut row = div()
        .flex()
        .flex_row()
        .items_center()
        .gap(Spacing::XS)
        .px(Spacing::MD)
        .py(Spacing::SM)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.background);

    for (mode, label) in modes {
        let is_active = mode == current_mode;
        let variant = if is_active {
            ButtonVariant::Primary
        } else {
            ButtonVariant::Default
        };
        row = row.child(
            Button::new(("qb-mode", mode as usize), label)
                .variant(variant)
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.switch_builder_mode(mode, cx);
                })),
        );
    }

    row
}

// ---------------------------------------------------------------------------
// Scrollable body with section cards
// ---------------------------------------------------------------------------

fn render_body(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::sections::{assignments, columns, execution, filters, group_by, joins, sort};

    let current_mode = panel
        .mutation_state
        .as_ref()
        .map(|s| s.mode)
        .unwrap_or(BuilderMode::Select);

    match current_mode {
        BuilderMode::Select => {
            let is_grouped = panel.is_grouped();

            let shows_joins = panel.shows_joins_section(cx);
            let shows_group_by = panel.shows_group_by_section(cx);
            let shows_having = panel.shows_having_section(cx);
            let shows_sort = panel.order_by_mode(cx) != dbflux_core::OrderByMode::None;

            let columns_body = if is_grouped {
                render_effective_select_preview(panel, theme).into_any_element()
            } else {
                columns::render_columns(panel, cx).into_any_element()
            };

            let filters_body = filters::render_filters(panel, cx).into_any_element();
            let joins_body = shows_joins.then(|| joins::render_joins(panel, cx).into_any_element());
            let group_by_body =
                shows_group_by.then(|| group_by::render_group_by(panel, cx).into_any_element());
            let sort_body = shows_sort.then(|| sort::render_sort(panel, cx).into_any_element());
            let limit_body = render_limit_offset_body(panel).into_any_element();

            let mut body = div()
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
                .when_some(joins_body, |body, joins_body| {
                    body.child(section_card("JOINS", AppIcon::Layers, theme, joins_body))
                })
                .when_some(group_by_body, |body, group_by_body| {
                    body.child(section_card(
                        "GROUP BY / AGGREGATES",
                        AppIcon::Layers,
                        theme,
                        group_by_body,
                    ))
                });

            if is_grouped && shows_having {
                let having_body = group_by::render_having(panel, cx).into_any_element();
                body = body.child(section_card(
                    "HAVING",
                    AppIcon::ListFilter,
                    theme,
                    having_body,
                ));
            }

            body.when_some(sort_body, |body, sort_body| {
                body.child(section_card("SORT", AppIcon::ArrowUpDown, theme, sort_body))
            })
            .child(section_card(
                "LIMIT & OFFSET",
                AppIcon::Hash,
                theme,
                limit_body,
            ))
            .into_any_element()
        }
        BuilderMode::Update => {
            let assignments_body = assignments::render_assignments(panel, cx).into_any_element();
            let filters_body = filters::render_filters(panel, cx).into_any_element();
            let execution_body = execution::render_execution(panel, cx).into_any_element();

            div()
                .flex_1()
                .min_h(px(0.0))
                .overflow_y_scrollbar()
                .child(section_card(
                    "SET",
                    AppIcon::Pencil,
                    theme,
                    assignments_body,
                ))
                .child(section_card(
                    "FILTERS (WHERE)",
                    AppIcon::ListFilter,
                    theme,
                    filters_body,
                ))
                .child(section_card(
                    "EXECUTION",
                    AppIcon::Play,
                    theme,
                    execution_body,
                ))
                .into_any_element()
        }
        BuilderMode::Delete => {
            let filters_body = filters::render_filters(panel, cx).into_any_element();
            let execution_body = execution::render_execution(panel, cx).into_any_element();

            div()
                .flex_1()
                .min_h(px(0.0))
                .overflow_y_scrollbar()
                .child(section_card(
                    "FILTERS (WHERE)",
                    AppIcon::ListFilter,
                    theme,
                    filters_body,
                ))
                .child(section_card(
                    "EXECUTION",
                    AppIcon::Play,
                    theme,
                    execution_body,
                ))
                .into_any_element()
        }
    }
}

/// Renders the SQL Preview as a fixed pane between the scrollable body and
/// the action footer, so it stays visible regardless of how many sections
/// the user has scrolled past.
fn render_preview_pane(panel: &mut QueryBuilderPanel, theme: &Theme) -> impl IntoElement {
    let body = render_preview_body(panel, theme).into_any_element();
    section_card("SQL PREVIEW", AppIcon::Code, theme, body)
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
// Effective SELECT preview (shown when grouped, replaces editable columns)
// ---------------------------------------------------------------------------

fn render_effective_select_preview(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
) -> impl IntoElement {
    use dbflux_core::AggFn;

    let mut container = div().flex().flex_col().gap(Spacing::XS).child(
        Text::caption(SharedString::from(
            "Grouped query — SELECT is managed automatically",
        ))
        .color(theme.muted_foreground),
    );

    for entry in &panel.current_spec.group_by {
        let label = format!("{}.{}", entry.source_alias, entry.column);
        container = container.child(div().text_sm().child(SharedString::from(label)));
    }

    for agg in &panel.current_spec.aggregates {
        let fn_name = match agg.function {
            AggFn::Count => "COUNT",
            AggFn::CountStar => "COUNT",
            AggFn::CountDistinct => "COUNT DISTINCT",
            AggFn::Sum => "SUM",
            AggFn::Avg => "AVG",
            AggFn::Min => "MIN",
            AggFn::Max => "MAX",
        };
        let col_part = if agg.function == AggFn::CountStar {
            "*".to_string()
        } else {
            match (&agg.source_alias, &agg.column) {
                (Some(sa), Some(col)) => format!("{}.{}", sa, col),
                (None, Some(col)) => col.clone(),
                _ => String::new(),
            }
        };
        let label = format!("{}({}) AS {}", fn_name, col_part, agg.alias);
        container = container.child(div().text_sm().child(SharedString::from(label)));
    }

    container
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
    let current_mode = panel
        .mutation_state
        .as_ref()
        .map(|s| s.mode)
        .unwrap_or(BuilderMode::Select);

    let is_mutation_mode = current_mode.is_mutation();

    let mutation_disabled = is_mutation_mode
        && panel
            .mutation_state
            .as_ref()
            .map(|s| s.is_update_with_no_assignments())
            .unwrap_or(true);

    let is_runnable = if is_mutation_mode {
        !mutation_disabled
    } else {
        panel.is_runnable()
    };

    let run_label = if is_mutation_mode {
        let has_filter = panel.current_spec.filter.is_some();
        if current_mode == BuilderMode::Update && has_filter {
            "Apply Update"
        } else {
            "Run"
        }
    } else {
        "Run"
    };

    let sort_error = panel.sort_validation_error.clone();
    let incomplete_count = panel.incomplete_aggregate_row_count;
    let is_grouped = panel.is_grouped();

    div()
        .flex()
        .flex_col()
        .border_t_1()
        .border_color(theme.border)
        .bg(theme.background)
        .when_some(sort_error, |d, error_msg| {
            d.child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .bg(theme.danger.opacity(0.08))
                    .border_b_1()
                    .border_color(theme.danger.opacity(0.3))
                    .child(
                        Icon::new(AppIcon::TriangleAlert)
                            .small()
                            .color(theme.danger),
                    )
                    .child(Text::caption(SharedString::from(error_msg)).color(theme.danger)),
            )
        })
        .when(is_grouped && incomplete_count > 0, |d| {
            let label = if incomplete_count == 1 {
                SharedString::from(
                    "1 aggregate row is incomplete and will be excluded from the query",
                )
            } else {
                SharedString::from(format!(
                    "{} aggregate rows are incomplete and will be excluded from the query",
                    incomplete_count
                ))
            };
            d.child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .bg(theme.warning.opacity(0.08))
                    .border_b_1()
                    .border_color(theme.warning.opacity(0.3))
                    .child(
                        Icon::new(AppIcon::TriangleAlert)
                            .small()
                            .color(theme.warning),
                    )
                    .child(Text::caption(label).color(theme.warning)),
            )
        })
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap(Spacing::SM)
                .px(Spacing::MD)
                .h(Heights::HEADER)
                .child(
                    Button::new("qb-run", run_label)
                        .icon(AppIcon::Play)
                        .primary()
                        .small()
                        .disabled(!is_runnable)
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            use crate::query_builder::events::BuilderEvent;
                            if is_mutation_mode {
                                if let Some(result) = this.build_mutation_spec_and_opts() {
                                    use crate::data_grid_panel::mutation_executor::CountState;
                                    let est_rows = this.mutation_state.as_ref().and_then(|s| {
                                        match &s.count_state {
                                            CountState::Done(n) => Some(*n),
                                            _ => None,
                                        }
                                    });
                                    cx.emit(BuilderEvent::MutationRunRequested {
                                        spec: Box::new(result.0),
                                        opts: Box::new(result.1),
                                        est_rows,
                                    });
                                }
                            } else {
                                cx.emit(BuilderEvent::RunRequested);
                            }
                        })),
                )
                .when(!is_mutation_mode, |row| {
                    row.child(
                        Button::new("qb-open-editor", "Open in Editor")
                            .icon(AppIcon::ExternalLink)
                            .variant(ButtonVariant::Ghost)
                            .small()
                            .on_click(cx.listener(|_this, _event, _window, cx| {
                                use crate::query_builder::events::BuilderEvent;
                                cx.emit(BuilderEvent::OpenInEditorRequested);
                            })),
                    )
                })
                .child(div().flex_1()),
        )
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
            let column_ref = if pred.column.is_empty() {
                String::new()
            } else {
                format!("{}.{}", pred.source_alias, pred.column)
            };
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

/// Walks the HAVING filter tree and ensures every `Predicate` node has a
/// corresponding `Entity<InputState>` in `panel.having_predicate_*` maps.
fn ensure_having_predicate_inputs(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) {
    let having = panel.current_spec.having.clone();
    if let Some(root) = having {
        ensure_in_having_node(panel, &root, vec![], window, cx);
    }
}

fn ensure_in_having_node(
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
            let column_ref = if pred.column.is_empty() {
                String::new()
            } else if pred.source_alias.is_empty() {
                pred.column.clone()
            } else {
                format!("{}.{}", pred.source_alias, pred.column)
            };
            panel.ensure_having_predicate_input(
                pred.node_id,
                path.clone(),
                &current_value,
                window,
                cx,
            );
            panel.ensure_having_predicate_column_input(
                pred.node_id,
                path.clone(),
                &column_ref,
                window,
                cx,
            );
            panel.ensure_having_predicate_comparator_dropdown(
                pred.node_id,
                path,
                pred.comparator,
                cx,
            );
        }
        FilterNode::Group { children, .. } => {
            for (i, child) in children.iter().enumerate() {
                let mut child_path = path.clone();
                child_path.push(i);
                ensure_in_having_node(panel, child, child_path, window, cx);
            }
        }
    }
}

/// Walks every join's condition tree and ensures inputs/dropdowns exist for
/// each `JoinPredicate` leaf, regardless of nesting depth.
fn ensure_join_condition_inputs(
    panel: &mut QueryBuilderPanel,
    window: &mut Window,
    cx: &mut Context<QueryBuilderPanel>,
) {
    use dbflux_core::{JoinFilterNode, JoinOn};

    fn collect(
        node: &JoinFilterNode,
        acc: &mut Vec<(u64, String, String, dbflux_core::Comparator)>,
    ) {
        match node {
            JoinFilterNode::Predicate(p) => {
                acc.push((p.node_id, p.left.clone(), p.right.clone(), p.op));
            }
            JoinFilterNode::Group { children, .. } => {
                for child in children {
                    collect(child, acc);
                }
            }
        }
    }

    let mut snapshot = Vec::new();
    for join in &panel.current_spec.joins {
        if let JoinOn::Conditions(root) = &join.on {
            collect(root, &mut snapshot);
        }
    }

    for (node_id, left, right, op) in snapshot {
        panel.ensure_join_condition_state(node_id, &left, &right, op, window, cx);
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
