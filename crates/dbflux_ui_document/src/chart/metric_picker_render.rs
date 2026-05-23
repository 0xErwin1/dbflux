//! `MetricPickerView` — boundary struct for rendering the metric picker rail.
//!
//! This file contains the render helpers for `MetricPickerState`. Following the
//! `KeyValueView`/`LogStreamView` boundary-struct pattern: state lives on
//! `ChartShell` (a GPUI entity), render code lives here (a plain struct with
//! borrowed references), never a GPUI entity itself.
//!
//! Layout (three horizontal bands inside the 320px rail):
//!
//!   ┌──────────────────────────────────────────┐
//!   │ ← left column → │ ← metrics column ────→ │  (row 1, flex-1)
//!   │  filter input    │  filter input           │
//!   │  namespace rows  │  metric rows            │
//!   │  (scrollable)    │  load-more button       │
//!   │                  │  (scrollable)           │
//!   ├──────────────────┴─────────────────────────┤
//!   │ Dimensions  (filtered by selected metric)  │  (row 2)
//!   ├────────────────────────────────────────────┤
//!   │ Period  [dropdown]  Stat [dropdown]  Apply │  (row 3)
//!   └────────────────────────────────────────────┘

// All render helpers are wired in Phase 8 (entry point). Until then the
// compiler sees them as dead code; suppress to keep CI clean.
#![allow(dead_code)]

use super::metric_picker::{MetricPickerState, MetricsState, NamespaceState};
use super::shell::{ChartShell, ChartShellEvent};
use dbflux_app::MetricCatalogCache;
use dbflux_components::controls::{Button, Input};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{Heights, Spacing};
use dbflux_core::{DimensionFilter, MetricDescriptor, MetricNamespace};
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::{AnyElement, App, Context, Entity, SharedString, Window, div, px};
use gpui_component::ActiveTheme;
use std::sync::Arc;

/// Boundary struct for rendering the metric picker rail.
///
/// Holds borrowed references into the `ChartShell`'s state. `render` is
/// called from the rail render dispatch inside `shell.rs`/`data_grid_panel`.
///
/// Note: `ChartShell` state is accessed indirectly through `cx.listener`
/// closures inside the render helpers rather than via a direct mutable reference.
/// This avoids a split-borrow conflict when extracting `state` from the shell.
pub struct MetricPickerView<'a> {
    pub state: &'a mut MetricPickerState,
    pub app_state: &'a Entity<AppStateEntity>,
    pub cache: &'a Arc<MetricCatalogCache>,
}

impl<'a> MetricPickerView<'a> {
    /// Render the metric picker rail content.
    ///
    /// Layout (Phase 7):
    ///   ┌─────────────────────────────────────┐
    ///   │ Namespace column (left, ~160px)     │
    ///   │ Metrics column   (right, flex)      │
    ///   ├─────────────────────────────────────┤
    ///   │ Dimensions section                  │
    ///   ├─────────────────────────────────────┤
    ///   │ Config section (period, stat, apply)│
    ///   └─────────────────────────────────────┘
    pub fn render(
        &mut self,
        window: &mut Window,
        cx: &mut Context<ChartShell>,
    ) -> impl IntoElement {
        // Ensure filter inputs are created now that we have a Window.
        self.state.ensure_inputs_created(window, cx);

        // Kick namespace fetch if not started.
        let shell_weak = cx.weak_entity();
        let cache = self.cache.clone();
        self.state.ensure_namespaces_loaded(shell_weak, cache, cx);

        // Kick metrics fetch if a namespace is selected but not yet loaded.
        let shell_weak2 = cx.weak_entity();
        let cache2 = self.cache.clone();
        self.state.ensure_metrics_loaded(shell_weak2, cache2, cx);

        let theme = cx.theme().clone();

        // Render all sections — each call consumes the cx borrow in its closure
        // scope and converts to AnyElement before the next call.
        let namespace_col = render_namespace_column(self.state, self.cache, cx);
        let metrics_col = render_metrics_column(self.state, self.cache, cx);
        let dimensions_section = render_dimensions_section(self.state, cx);
        let config_footer = render_config_footer(self.state, cx);

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(theme.popover)
            // Top area: namespace + metrics side by side.
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_1()
                    .min_h_0()
                    .child(namespace_col)
                    .child(div().w(px(1.0)).flex_shrink_0().bg(theme.border))
                    .child(metrics_col),
            )
            // Divider.
            .child(div().h(px(1.0)).bg(theme.border))
            // Dimensions section.
            .child(dimensions_section)
            // Divider.
            .child(div().h(px(1.0)).bg(theme.border))
            // Config footer.
            .child(config_footer)
    }
}

// ---------------------------------------------------------------------------
// Namespace column (left, fixed ~160px)
// ---------------------------------------------------------------------------

fn render_namespace_column(
    state: &MetricPickerState,
    _cache: &Arc<MetricCatalogCache>,
    cx: &mut Context<ChartShell>,
) -> AnyElement {
    let theme = cx.theme().clone();
    // Pre-extract Hsla values (Copy) so closures in map() can capture them
    // without moving `theme` itself (which is not Copy).
    let c_border = theme.border;
    let c_secondary = theme.secondary;
    let c_accent = theme.accent;
    let c_popover = theme.popover;
    let c_foreground = theme.foreground;
    let c_muted_fg = theme.muted_foreground;

    let filter_text = state
        .namespace_filter
        .as_ref()
        .map(|ns| ns.read(cx).value().to_string().to_lowercase());

    let filter_element: Option<AnyElement> = state.namespace_filter.as_ref().map(|ns| {
        div()
            .h(Heights::INPUT)
            .border_b_1()
            .border_color(c_border)
            .child(Input::new(ns).small().cleanable(true))
            .into_any_element()
    });

    let list_body: AnyElement = match &state.namespace_state {
        NamespaceState::NotFetched | NamespaceState::Loading => div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .child(Text::muted("Loading…"))
            .into_any_element(),

        NamespaceState::Error(msg) => {
            let msg = msg.clone();
            div()
                .flex_1()
                .p(Spacing::SM)
                .child(Text::muted(format!("Error: {msg}")))
                .into_any_element()
        }

        NamespaceState::Loaded(namespaces) => {
            let filter = filter_text.unwrap_or_default();
            let selected = state.selected_namespace.clone();

            let rows: Vec<AnyElement> = namespaces
                .iter()
                .filter(|ns| filter.is_empty() || ns.to_lowercase().contains(&filter))
                .enumerate()
                .map(|(i, ns)| {
                    let is_selected = selected.as_deref() == Some(ns.as_str());
                    let ns_label: SharedString = SharedString::from(ns.clone());
                    let ns_for_click: MetricNamespace = ns.clone();

                    let row_bg = if is_selected { c_accent } else { c_popover };
                    let row_fg = if is_selected {
                        c_foreground
                    } else {
                        c_muted_fg
                    };

                    div()
                        .id(("ns-row", i))
                        .h(Heights::ROW_COMPACT)
                        .flex()
                        .items_center()
                        .px(Spacing::SM)
                        .bg(row_bg)
                        .cursor_pointer()
                        .hover(move |d| if !is_selected { d.bg(c_secondary) } else { d })
                        .on_click(cx.listener(move |shell, _, _, cx| {
                            if let Some(picker) = &mut shell.metric_picker {
                                picker.on_namespace_selected(ns_for_click.clone());
                            }
                            cx.notify();
                        }))
                        .child(
                            div()
                                .flex_1()
                                .overflow_hidden()
                                .text_ellipsis()
                                .text_color(row_fg)
                                .child(ns_label),
                        )
                        .into_any_element()
                })
                .collect();

            if rows.is_empty() {
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .p(Spacing::SM)
                    .child(Text::dim("No namespaces"))
                    .into_any_element()
            } else {
                div()
                    .id("mp-namespace-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .children(rows)
                    .into_any_element()
            }
        }
    };

    div()
        .w(px(160.0))
        .flex_shrink_0()
        .flex()
        .flex_col()
        .when_some(filter_element, |d, fe| d.child(fe))
        .child(list_body)
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Metrics column (right, flex-1)
// ---------------------------------------------------------------------------

fn render_metrics_column(
    state: &MetricPickerState,
    cache: &Arc<MetricCatalogCache>,
    cx: &mut Context<ChartShell>,
) -> AnyElement {
    let theme = cx.theme().clone();
    let c_border = theme.border;

    if state.selected_namespace.is_none() {
        return div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .p(Spacing::SM)
            .child(Text::muted("Select a namespace"))
            .into_any_element();
    }

    let filter_element: Option<AnyElement> = state.metrics_filter.as_ref().map(|mf| {
        div()
            .h(Heights::INPUT)
            .border_b_1()
            .border_color(c_border)
            .child(Input::new(mf).small().cleanable(true))
            .into_any_element()
    });

    let list_body: AnyElement = match &state.metrics_state {
        MetricsState::NotFetched | MetricsState::Loading => div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .child(Text::muted("Loading…"))
            .into_any_element(),

        MetricsState::Error(msg) => {
            let msg = msg.clone();
            div()
                .flex_1()
                .p(Spacing::SM)
                .child(Text::muted(format!("Error: {msg}")))
                .into_any_element()
        }

        MetricsState::Loaded {
            accumulated,
            fully_loaded: fl,
        } => render_metrics_list(state, cache, accumulated.clone(), *fl, false, cx),

        MetricsState::LoadingMore { accumulated } => {
            render_metrics_list(state, cache, accumulated.clone(), false, true, cx)
        }
    };

    div()
        .flex_1()
        .flex()
        .flex_col()
        .when_some(filter_element, |d, fe| d.child(fe))
        .child(list_body)
        .into_any_element()
}

/// Render the metric rows list with optional load-more footer.
fn render_metrics_list(
    state: &MetricPickerState,
    cache: &Arc<MetricCatalogCache>,
    accumulated: Arc<Vec<MetricDescriptor>>,
    fully_loaded: bool,
    is_loading_more: bool,
    cx: &mut Context<ChartShell>,
) -> AnyElement {
    let theme = cx.theme().clone();
    // Pre-extract Hsla (Copy) so map() closures can capture without moving theme.
    let c_secondary = theme.secondary;
    let c_accent = theme.accent;
    let c_popover = theme.popover;
    let c_foreground = theme.foreground;
    let c_muted_fg = theme.muted_foreground;

    let filter = state
        .metrics_filter
        .as_ref()
        .map(|mf| mf.read(cx).value().to_string().to_lowercase())
        .unwrap_or_default();

    let selected = state.selected_metric.clone();

    let rows: Vec<AnyElement> = accumulated
        .iter()
        .filter(|m| filter.is_empty() || m.metric_name.to_lowercase().contains(&filter))
        .enumerate()
        .map(|(i, m)| {
            let is_selected = selected
                .as_ref()
                .map(|s| s.metric_name == m.metric_name)
                .unwrap_or(false);
            let name: SharedString = SharedString::from(m.metric_name.clone());
            let metric_for_click = m.clone();

            let row_bg = if is_selected { c_accent } else { c_popover };
            let row_fg = if is_selected {
                c_foreground
            } else {
                c_muted_fg
            };

            div()
                .id(("metric-row", i))
                .h(Heights::ROW_COMPACT)
                .flex()
                .items_center()
                .px(Spacing::SM)
                .bg(row_bg)
                .cursor_pointer()
                .hover(move |d| if !is_selected { d.bg(c_secondary) } else { d })
                .on_click(cx.listener(move |shell, _, _, cx| {
                    if let Some(picker) = &mut shell.metric_picker {
                        picker.on_metric_selected(metric_for_click.clone());
                    }
                    cx.notify();
                }))
                .child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_color(row_fg)
                        .child(name),
                )
                .into_any_element()
        })
        .collect();

    let load_more_element: Option<AnyElement> = if !fully_loaded {
        let cache_for_click = cache.clone();
        let lm = if is_loading_more {
            div()
                .h(Heights::ROW_COMPACT)
                .flex()
                .items_center()
                .justify_center()
                .child(Text::muted("Loading more…"))
                .into_any_element()
        } else {
            div()
                .id("metric-load-more")
                .h(Heights::ROW_COMPACT)
                .flex()
                .items_center()
                .justify_center()
                .cursor_pointer()
                .hover(move |d| d.bg(c_secondary))
                .on_click(cx.listener(move |shell, _, _, cx| {
                    let weak = cx.weak_entity();
                    if let Some(picker) = &mut shell.metric_picker {
                        picker.load_more_metrics(weak, cache_for_click.clone(), cx);
                    }
                    cx.notify();
                }))
                .child(Text::caption("Load more…"))
                .into_any_element()
        };
        Some(lm)
    } else {
        None
    };

    if rows.is_empty() {
        return div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .p(Spacing::SM)
            .child(Text::dim(if filter.is_empty() {
                "No metrics"
            } else {
                "No match"
            }))
            .into_any_element();
    }

    div()
        .id("mp-metrics-scroll")
        .flex_1()
        .overflow_y_scroll()
        .children(rows)
        .when_some(load_more_element, |d, lm| d.child(lm))
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Dimensions section
// ---------------------------------------------------------------------------

fn render_dimensions_section(
    state: &MetricPickerState,
    cx: &mut Context<ChartShell>,
) -> AnyElement {
    let theme = cx.theme().clone();

    let selected_metric = match &state.selected_metric {
        Some(m) => m.clone(),
        None => {
            return div()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .child(Text::dim("No metric selected"))
                .into_any_element();
        }
    };

    let dims = selected_metric.dimensions.clone();
    let current_filter = &state.dimension_filter;

    let header = div()
        .h(Heights::ROW_COMPACT)
        .flex()
        .items_center()
        .px(Spacing::SM)
        .border_b_1()
        .border_color(theme.border)
        .child(
            div()
                .text_size(px(10.0))
                .text_color(theme.muted_foreground)
                .font_weight(gpui::FontWeight::BOLD)
                .child(SharedString::from("DIMENSIONS")),
        );

    // AggregateAll row — always shown at the top.
    // Convert immediately to AnyElement to release the `cx` borrow before
    // calling dim_radio_row a second time.
    let is_agg_selected = matches!(current_filter, DimensionFilter::AggregateAll);
    let agg_row: AnyElement = dim_radio_row(
        0,
        SharedString::from("Aggregate all"),
        is_agg_selected,
        &theme,
        cx,
        |shell, _, _, cx| {
            if let Some(picker) = &mut shell.metric_picker {
                picker.dimension_filter = DimensionFilter::AggregateAll;
            }
            cx.notify();
        },
    )
    .into_any_element();

    // Per-dimension-set row (one row per unique dimension combo in the descriptor).
    let dim_rows: Vec<AnyElement> = if dims.is_empty() {
        vec![]
    } else {
        let label = SharedString::from(
            dims.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
        let is_filter_selected = match current_filter {
            DimensionFilter::FilterTo(d) => d == &dims,
            _ => false,
        };
        let dims_for_click = dims.clone();
        let row: AnyElement = dim_radio_row(
            1,
            label,
            is_filter_selected,
            &theme,
            cx,
            move |shell, _, _, cx| {
                if let Some(picker) = &mut shell.metric_picker {
                    picker.dimension_filter = DimensionFilter::FilterTo(dims_for_click.clone());
                }
                cx.notify();
            },
        )
        .into_any_element();
        vec![row]
    };

    div()
        .flex()
        .flex_col()
        .child(header)
        .child(agg_row)
        .children(dim_rows)
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Config footer: period, statistic dropdowns + Apply button
// ---------------------------------------------------------------------------

fn render_config_footer(state: &MetricPickerState, cx: &mut Context<ChartShell>) -> AnyElement {
    let theme = cx.theme().clone();
    let can_apply = state.selected_namespace.is_some() && state.selected_metric.is_some();

    let period_dropdown = state.period_dropdown.clone();
    let statistic_dropdown = state.statistic_dropdown.clone();

    div()
        .h(Heights::TOOLBAR)
        .flex()
        .flex_row()
        .items_center()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .bg(theme.secondary)
        .child(period_dropdown)
        .child(statistic_dropdown)
        .child(div().flex_1()) // pushes Apply to the right
        .child(
            Button::new("metric-picker-apply", "Apply")
                .primary()
                .small()
                .disabled(!can_apply)
                .on_click(cx.listener(|shell, _, _, cx| {
                    if let Some(picker) = &shell.metric_picker {
                        if let Some(source) = picker.build_metric_source() {
                            cx.emit(ChartShellEvent::MetricPickerApplied(Box::new(source)));
                        }
                    }
                })),
        )
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Render a single radio-row for the dimensions selector.
fn dim_radio_row<F>(
    id_suffix: usize,
    label: SharedString,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
    cx: &mut Context<ChartShell>,
    on_click: F,
) -> impl IntoElement
where
    F: Fn(&mut ChartShell, &gpui::ClickEvent, &mut Window, &mut Context<ChartShell>)
        + Send
        + Sync
        + 'static,
{
    let row_bg = if is_selected {
        theme.accent
    } else {
        theme.popover
    };
    let dot_color = if is_selected {
        theme.primary
    } else {
        theme.border
    };
    let row_fg = theme.muted_foreground;

    div()
        .id(("dim-row", id_suffix))
        .h(Heights::ROW_COMPACT)
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .bg(row_bg)
        .cursor_pointer()
        .hover(move |d| {
            if !is_selected {
                d.bg(theme.secondary)
            } else {
                d
            }
        })
        .on_click(cx.listener(on_click))
        .child(
            div()
                .w(px(10.0))
                .h(px(10.0))
                .rounded_full()
                .border_1()
                .border_color(dot_color)
                .when(is_selected, |d| d.bg(dot_color)),
        )
        .child(
            div()
                .flex_1()
                .overflow_hidden()
                .text_ellipsis()
                .text_color(row_fg)
                .child(label),
        )
}

// ---------------------------------------------------------------------------
// Compile-time check: `MetricPickerView` must not be a GPUI entity.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// T-MP-BND-01: `MetricPickerView` must remain a plain boundary struct.
    ///
    /// Asserts that its type name does not start with "Entity" (which would
    /// indicate it was accidentally converted to a GPUI entity).
    #[test]
    fn metric_picker_view_is_not_a_gpui_entity() {
        let name = std::any::type_name::<MetricPickerView>();
        assert!(
            !name.starts_with("Entity"),
            "MetricPickerView must be a plain boundary struct, not a GPUI entity; got: {name}"
        );
    }
}
