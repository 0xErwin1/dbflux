use dbflux_components::controls::{Button, Input, ReadOnlyEditor, ReadonlyTextView};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, Chamfer, Icon, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{BuilderMetrics, ChamferCut, ChromeColors, Spacing};
use dbflux_components::vim::VimBinding;
use gpui::prelude::*;
use gpui::{AnyElement, Context, FontWeight, IntoElement, SharedString, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::theme::Theme;

use crate::query_builder::mutation_state::BuilderMode;
use dbflux_components::composites::{RailOwner, rail_scroll_area, render_rail_menu};

/// Keycap on the Run button: the builder runs on Cmd/Ctrl+Enter.
#[cfg(target_os = "macos")]
const RUN_SHORTCUT_HINT: &str = "Cmd \u{21b5}";
#[cfg(not(target_os = "macos"))]
const RUN_SHORTCUT_HINT: &str = "Ctrl \u{21b5}";

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

    let rail_active = panel
        .focus_handle
        .as_ref()
        .is_some_and(|handle| handle.contains_focused(window, cx));
    let rows = panel.rail_rows(cx);
    panel.rail_mark = panel.rail.mark(&rows, rail_active, cx);

    let theme = cx.theme().clone();

    let show_mode_selector = panel.shows_mutation_selector(cx);

    let container = div()
        .relative()
        .flex()
        .flex_col()
        .size_full()
        .bg(theme.popover);

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
        .child(
            div()
                .px(BuilderMetrics::RAIL_PADDING_X)
                .pb(BuilderMetrics::SECTION_GAP)
                .child(render_preview_pane(panel, &theme, cx)),
        )
        .child(render_footer(panel, &theme, cx))
        .children(render_rail_menu(&panel.rail, "qb-rail-menu", cx))
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

fn render_header(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let title = panel
        .loaded_id
        .clone()
        .unwrap_or_else(|| panel.current_spec.source.table.clone());
    let source_schema = panel.current_spec.source.schema.clone();

    div()
        .flex()
        .flex_row()
        .flex_shrink_0()
        .items_center()
        .gap(BuilderMetrics::HEADER_GAP)
        .px(BuilderMetrics::RAIL_PADDING_X)
        .h(BuilderMetrics::HEADER_HEIGHT)
        .border_b_1()
        .border_color(theme.border)
        .child(
            Icon::new(AppIcon::SquareFunction)
                .size(BuilderMetrics::HEADER_ICON)
                .color(ChromeColors::tint(theme)),
        )
        .child(
            div()
                .min_w_0()
                .truncate()
                .font_weight(FontWeight::BOLD)
                .text_color(ChromeColors::strong(theme))
                .child(SharedString::from(title)),
        )
        .when_some(source_schema, |row, schema| {
            row.child(Badge::new(schema, BadgeTone::Neutral))
        })
        .child(div().flex_1())
        .child(
            Button::new("qb-hdr-save", "")
                .icon(AppIcon::Save)
                .icon_only()
                .tooltip(dbflux_i18n::t!("document.query_builder.chrome.save"))
                .tab_stop(false)
                .on_click(cx.listener(|this, _event, _window, cx| this.request_save(cx))),
        )
        .child(
            Button::new("qb-hdr-reset", "")
                .icon(AppIcon::RotateCcw)
                .icon_only()
                .tooltip(dbflux_i18n::t!("document.query_builder.chrome.reset"))
                .tab_stop(false)
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::ResetRequested);
                })),
        )
        .child(
            Button::new("qb-hdr-close", "")
                .icon(AppIcon::CircleX)
                .icon_only()
                .tooltip(dbflux_i18n::t!("document.query_builder.chrome.close"))
                .tab_stop(false)
                .on_click(cx.listener(|_this, _event, _window, cx| {
                    use crate::query_builder::events::BuilderEvent;
                    cx.emit(BuilderEvent::CloseRequested);
                })),
        )
}

// ---------------------------------------------------------------------------
// Mode selector (SELECT / UPDATE / DELETE) — shown only for SQL connections
// ---------------------------------------------------------------------------

fn render_mode_selector(
    panel: &mut QueryBuilderPanel,
    _theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use crate::query_builder::mutation_state::BuilderMode;

    let current_mode = panel
        .mutation_state
        .as_ref()
        .map(|s| s.mode)
        .unwrap_or(BuilderMode::Select);

    let mode_id = |mode: BuilderMode| SharedString::from(format!("qb-mode-{}", mode as usize));

    let items = mode_selector_options()
        .into_iter()
        .map(|(mode, label)| SegmentedItem::new(mode_id(mode), label))
        .collect();

    let weak = cx.weak_entity();
    let control = SegmentedControl::new(items, mode_id(current_mode), move |id, _, cx| {
        let Some((mode, _)) = mode_selector_options()
            .into_iter()
            .find(|(mode, _)| mode_id(*mode) == *id)
        else {
            return;
        };

        if let Some(builder) = weak.upgrade() {
            builder.update(cx, |this, cx| this.switch_builder_mode(mode, cx));
        }
    });

    div()
        .flex()
        .flex_shrink_0()
        .px(BuilderMetrics::RAIL_PADDING_X)
        .py(BuilderMetrics::MODE_PADDING_Y)
        .child(control)
}

/// The mode-switch bar's (mode, label) options, translated through the
/// catalog.
///
/// A function rather than a `const` array because `dbflux_i18n::t!` is not
/// evaluable in a const context; every arm's translated value happens to
/// stay byte-identical between locales since these are SQL statement
/// names, not prose.
fn mode_selector_options() -> [(BuilderMode, String); 3] {
    [
        (
            BuilderMode::Select,
            crate::labels::builder_mode_label(BuilderMode::Select),
        ),
        (
            BuilderMode::Update,
            crate::labels::builder_mode_label(BuilderMode::Update),
        ),
        (
            BuilderMode::Delete,
            crate::labels::builder_mode_label(BuilderMode::Delete),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Scrollable body with section cards
// ---------------------------------------------------------------------------

fn render_body(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let sections = render_sections(panel, theme, cx);

    rail_scroll_area(
        &panel.rail,
        "qb-sections",
        div().flex().flex_col().child(sections),
    )
}

fn render_sections(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> gpui::AnyElement {
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
            let sort_body = sort::render_sort(panel, cx).into_any_element();
            let sort_title = if shows_sort {
                dbflux_i18n::t!("document.query_builder.section.sort_limit")
            } else {
                dbflux_i18n::t!("document.query_builder.section.limit_offset")
            };

            let columns_badge = columns_badge(panel);
            let filters_badge = filters_badge(panel.current_spec.filter.as_ref());

            let mut body = sections_container()
                .child(section_card_with_badge(
                    dbflux_i18n::t!("document.query_builder.section.columns"),
                    AppIcon::Columns,
                    columns_badge,
                    theme,
                    columns_body,
                ))
                .child(section_card_with_badge(
                    dbflux_i18n::t!("document.query_builder.section.filters"),
                    AppIcon::ListFilter,
                    filters_badge,
                    theme,
                    filters_body,
                ))
                .when_some(joins_body, |body, joins_body| {
                    body.child(section_card(
                        dbflux_i18n::t!("document.query_builder.section.joins"),
                        AppIcon::Layers,
                        theme,
                        joins_body,
                    ))
                })
                .when_some(group_by_body, |body, group_by_body| {
                    body.child(section_card(
                        dbflux_i18n::t!("document.query_builder.section.group_by_aggregates"),
                        AppIcon::Layers,
                        theme,
                        group_by_body,
                    ))
                });

            if is_grouped && shows_having {
                let having_body = group_by::render_having(panel, cx).into_any_element();
                body = body.child(section_card(
                    dbflux_i18n::t!("document.query_builder.section.having"),
                    AppIcon::ListFilter,
                    theme,
                    having_body,
                ));
            }

            body.child(section_card(
                sort_title,
                AppIcon::ArrowUpDown,
                theme,
                sort_body,
            ))
            .into_any_element()
        }
        BuilderMode::Update => {
            let assignments_body = assignments::render_assignments(panel, cx).into_any_element();
            let filters_body = filters::render_filters(panel, cx).into_any_element();
            let execution_body = execution::render_execution(panel, cx).into_any_element();

            sections_container()
                .child(section_card(
                    dbflux_i18n::t!("document.query_builder.section.set"),
                    AppIcon::Pencil,
                    theme,
                    assignments_body,
                ))
                .child(section_card(
                    dbflux_i18n::t!("document.query_builder.section.filters_where"),
                    AppIcon::ListFilter,
                    theme,
                    filters_body,
                ))
                .child(section_card(
                    dbflux_i18n::t!("document.query_builder.section.execution"),
                    AppIcon::Play,
                    theme,
                    execution_body,
                ))
                .into_any_element()
        }
        BuilderMode::Delete => {
            let filters_body = filters::render_filters(panel, cx).into_any_element();
            let execution_body = execution::render_execution(panel, cx).into_any_element();

            sections_container()
                .child(section_card(
                    dbflux_i18n::t!("document.query_builder.section.filters_where"),
                    AppIcon::ListFilter,
                    theme,
                    filters_body,
                ))
                .child(section_card(
                    dbflux_i18n::t!("document.query_builder.section.execution"),
                    AppIcon::Play,
                    theme,
                    execution_body,
                ))
                .into_any_element()
        }
    }
}

/// The column of section cards inside the scrolling area: 14 px side
/// padding, 10 px between cards.
fn sections_container() -> gpui::Div {
    div()
        .flex()
        .flex_col()
        .gap(BuilderMetrics::SECTION_GAP)
        .px(BuilderMetrics::RAIL_PADDING_X)
        .pb(BuilderMetrics::SECTION_GAP)
}

/// "N of M" over the Columns card while columns are picked one by one.
fn columns_badge(panel: &QueryBuilderPanel) -> Option<Badge> {
    use crate::query_builder::panel::ProjectionMode;

    if panel.projection_mode == ProjectionMode::All || panel.available_columns.is_empty() {
        return None;
    }

    Some(Badge::new(
        dbflux_i18n::t!(
            "document.query_builder.columns.selected_count",
            selected = panel.projection_rows.len(),
            total = panel.available_columns.len()
        ),
        BadgeTone::Neutral,
    ))
}

/// Number of predicates over the Filters card, when there is any.
fn filters_badge(filter: Option<&dbflux_core::FilterNode>) -> Option<Badge> {
    let count = filter.map(predicate_count).unwrap_or(0);

    (count > 0).then(|| Badge::new(count.to_string(), BadgeTone::Accent))
}

/// Predicates in a filter tree, groups excluded.
fn predicate_count(node: &dbflux_core::FilterNode) -> usize {
    match node {
        dbflux_core::FilterNode::Predicate(_) => 1,
        dbflux_core::FilterNode::Group { children, .. } => {
            children.iter().map(predicate_count).sum()
        }
    }
}

/// Renders the SQL Preview as a fixed card between the scrollable body and
/// the action footer, so it stays visible regardless of how many sections
/// the user has scrolled past.
fn render_preview_pane(
    panel: &mut QueryBuilderPanel,
    theme: &Theme,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let line_count = panel.sql_preview.lines().count().max(1);
    let status = div()
        .flex()
        .items_center()
        .gap(BuilderMetrics::STATUS_GAP)
        .text_size(BuilderMetrics::STATUS_FONT)
        .text_color(theme.success)
        .child(
            Icon::new(AppIcon::CircleCheck)
                .size(BuilderMetrics::STATUS_ICON)
                .color(theme.success),
        )
        .child(SharedString::from(crate::labels::valid_lines_label(
            line_count,
        )));

    let body = render_preview_body(panel, cx).into_any_element();

    section_card_with_trailing(
        dbflux_i18n::t!("document.query_builder.section.sql_preview"),
        AppIcon::Code,
        Some(status.into_any_element()),
        theme,
        body,
    )
}

/// A section card (P1Builder): the input cut on the ground with a line
/// border, 12 px padding and 10 px gap, headed by a tint icon and the
/// uppercase label.
fn section_card(
    title: impl Into<SharedString>,
    icon: AppIcon,
    theme: &Theme,
    body: AnyElement,
) -> impl IntoElement {
    section_card_with_trailing(title, icon, None, theme, body)
}

/// A section card with a badge at the right of its header.
fn section_card_with_badge(
    title: impl Into<SharedString>,
    icon: AppIcon,
    badge: Option<Badge>,
    theme: &Theme,
    body: AnyElement,
) -> impl IntoElement {
    section_card_with_trailing(
        title,
        icon,
        badge.map(IntoElement::into_any_element),
        theme,
        body,
    )
}

fn section_card_with_trailing(
    title: impl Into<SharedString>,
    icon: AppIcon,
    trailing: Option<AnyElement>,
    theme: &Theme,
    body: AnyElement,
) -> impl IntoElement {
    let title: SharedString = title.into();

    div()
        .relative()
        .flex()
        .flex_col()
        .flex_shrink_0()
        .gap(BuilderMetrics::CARD_GAP)
        .p(BuilderMetrics::CARD_PADDING)
        .child(
            Chamfer::new(ChamferCut::INPUT)
                .fill(theme.background)
                .border(theme.border),
        )
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap(BuilderMetrics::CARD_HEADER_GAP)
                .child(
                    Icon::new(icon)
                        .size(BuilderMetrics::CARD_ICON)
                        .color(ChromeColors::tint(theme)),
                )
                .child(Text::label(title).font_size(BuilderMetrics::CARD_LABEL_FONT))
                .child(div().flex_1())
                .children(trailing),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .gap(BuilderMetrics::CARD_GAP)
                .child(body),
        )
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
        Text::caption(dbflux_i18n::t!(
            "document.query_builder.group_by.managed_select_notice"
        ))
        .color(theme.muted_foreground),
    );

    for entry in &panel.current_spec.group_by {
        let label = format!("{}.{}", entry.source_alias, entry.column);
        container = container.child(div().text_sm().child(SharedString::from(label)));
    }

    for agg in &panel.current_spec.aggregates {
        let fn_name = match agg.function {
            AggFn::Count | AggFn::CountStar => {
                dbflux_i18n::t!("document.query_builder.aggregate.fn.count")
            }
            AggFn::CountDistinct => {
                dbflux_i18n::t!("document.query_builder.aggregate.fn.count_distinct")
            }
            AggFn::Sum => dbflux_i18n::t!("document.query_builder.aggregate.fn.sum"),
            AggFn::Avg => dbflux_i18n::t!("document.query_builder.aggregate.fn.avg"),
            AggFn::Min => dbflux_i18n::t!("document.query_builder.aggregate.fn.min"),
            AggFn::Max => dbflux_i18n::t!("document.query_builder.aggregate.fn.max"),
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

fn render_preview_body(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let container = match panel.sql_preview_vim.as_ref() {
        Some(vim) => {
            let input = vim.input_id();
            VimBinding::capture_run_command(
                VimBinding::wire(vim.leader_scope(div(), cx), input, cx),
                input,
                cx,
            )
        }
        None => div(),
    };
    let indicator = panel
        .sql_preview_vim
        .as_ref()
        .and_then(|vim| vim.render_indicator(cx));

    container
        .when_some(panel.sql_preview_state.as_ref(), |container, state| {
            container.child(
                ReadOnlyEditor::new(state)
                    .appearance(false)
                    .w_full()
                    .h(BuilderMetrics::PREVIEW_HEIGHT),
            )
        })
        .children(indicator)
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
            dbflux_i18n::t!("document.query_builder.status.apply_update")
        } else {
            dbflux_i18n::t!("document.query_builder.status.run")
        }
    } else {
        dbflux_i18n::t!("document.query_builder.status.run")
    };

    let sort_error = panel.sort_validation_error.clone();
    let incomplete_count = panel.incomplete_aggregate_row_count;
    let is_grouped = panel.is_grouped();

    div()
        .flex()
        .flex_col()
        .flex_shrink_0()
        .border_t_1()
        .border_color(theme.border)
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
            let label = SharedString::from(crate::labels::incomplete_aggregate_rows_label(
                incomplete_count,
            ));
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
                .gap(BuilderMetrics::FOOTER_GAP)
                .px(BuilderMetrics::RAIL_PADDING_X)
                .py(BuilderMetrics::FOOTER_PADDING_Y)
                .when(!is_mutation_mode, |row| {
                    row.child(
                        Button::new(
                            "qb-open-editor",
                            dbflux_i18n::t!("document.query_builder.status.open_in_editor"),
                        )
                        .icon(AppIcon::ExternalLink)
                        .on_click(cx.listener(
                            |_this, _event, _window, cx| {
                                use crate::query_builder::events::BuilderEvent;
                                cx.emit(BuilderEvent::OpenInEditorRequested);
                            },
                        )),
                    )
                })
                .child(div().flex_1())
                .child(
                    Button::new("qb-run", run_label)
                        .icon(AppIcon::Play)
                        .primary()
                        .kbd(RUN_SHORTCUT_HINT)
                        .disabled(!is_runnable)
                        .on_click(cx.listener(|this, _event, _window, cx| this.request_run(cx))),
                ),
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

#[cfg(test)]
mod tests {
    use super::mode_selector_options;
    use crate::query_builder::mutation_state::BuilderMode;

    #[test]
    fn mode_selector_options_returns_translated_labels_for_every_mode() {
        let options = mode_selector_options();

        assert_eq!(options.len(), 3);
        assert_eq!(options[0].0, BuilderMode::Select);
        assert_eq!(options[0].1, "SELECT");
        assert_eq!(options[1].0, BuilderMode::Update);
        assert_eq!(options[1].1, "UPDATE");
        assert_eq!(options[2].0, BuilderMode::Delete);
        assert_eq!(options[2].1, "DELETE");
    }

    #[test]
    fn query_builder_section_keys_resolve_in_both_locales() {
        let keys = [
            "document.query_builder.section.columns",
            "document.query_builder.section.filters",
            "document.query_builder.section.filters_where",
            "document.query_builder.section.sort_limit",
            "document.query_builder.section.execution",
            "document.query_builder.section.sql_preview",
            "document.query_builder.section.group_by_aggregates",
            "document.query_builder.group_by.managed_select_notice",
        ];

        for key in keys {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn query_builder_section_columns_differs_between_locales() {
        let en = dbflux_i18n::t!("document.query_builder.section.columns", locale = "en");
        let es = dbflux_i18n::t!("document.query_builder.section.columns", locale = "es");

        assert_eq!(en, "COLUMNS");
        assert_eq!(es, "COLUMNAS");
        assert_ne!(en, es);
    }

    #[test]
    fn query_builder_sql_clause_headers_keep_the_keyword_in_every_locale() {
        let headers = [
            ("document.query_builder.section.joins", "JOINS"),
            ("document.query_builder.section.having", "HAVING"),
            (
                "document.query_builder.section.limit_offset",
                "LIMIT & OFFSET",
            ),
            ("document.query_builder.section.set", "SET"),
        ];

        for (key, keyword) in headers {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                assert_eq!(
                    dbflux_i18n::t!(key, locale = locale),
                    keyword,
                    "{key} must keep the SQL keyword in {locale}"
                );
            }
        }

        for locale in ["en", "es", "ko", "zh_Hans"] {
            let group_by = dbflux_i18n::t!(
                "document.query_builder.section.group_by_aggregates",
                locale = locale
            );

            assert!(
                group_by.starts_with("GROUP BY / "),
                "GROUP BY must stay literal in {locale}, got {group_by:?}"
            );
        }

        assert_ne!(
            dbflux_i18n::t!(
                "document.query_builder.section.group_by_aggregates",
                locale = "en"
            ),
            dbflux_i18n::t!(
                "document.query_builder.section.group_by_aggregates",
                locale = "es"
            )
        );
    }

    #[test]
    fn query_builder_section_headers_are_not_literal_in_source() {
        let full_source = include_str!("view.rs");
        let source = full_source
            .split("#[cfg(test)]")
            .next()
            .expect("view.rs must contain the render code above the test module");

        for translated_header in [
            "\"JOINS\"",
            "\"GROUP BY / AGGREGATES\"",
            "\"HAVING\"",
            "\"LIMIT & OFFSET\"",
            "\"SET\"",
            "\"COLUMNS\"",
            "\"FILTERS\",",
            "\"FILTERS (WHERE)\"",
            "\"SORT\"",
            "\"EXECUTION\"",
            "\"SQL PREVIEW\"",
        ] {
            assert!(
                !source.contains(translated_header),
                "general header literal {translated_header:?} must be replaced with a t! call"
            );
        }
    }
}
