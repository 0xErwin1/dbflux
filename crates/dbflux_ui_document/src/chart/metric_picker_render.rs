//! `MetricPickerView` — boundary struct for rendering the metric picker rail.
//!
//! This file contains the render helpers for `MetricPickerState`. Following the
//! `KeyValueView`/`LogStreamView` boundary-struct pattern: state lives on
//! `ChartShell` (a GPUI entity), render code lives here (a plain struct with
//! borrowed references), never a GPUI entity itself.
//!
//! Layout (dimensions + config):
//!
//!   ┌──────────────────────────────────────────┐
//!   │ Header: <Namespace> / <MetricName>       │
//!   ├──────────────────────────────────────────┤
//!   │ Dimensions  (loaded from cache)          │
//!   ├──────────────────────────────────────────┤
//!   │ Period  [dropdown]  Stat [dropdown] Apply │
//!   └──────────────────────────────────────────┘

use super::metric_picker::{DimensionsState, MetricPickerState};
use super::shell::{ChartShell, ChartShellEvent};
use dbflux_app::MetricCatalogCache;
use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{Heights, Spacing};
use dbflux_core::DimensionFilter;
use gpui::prelude::*;
use gpui::{AnyElement, Context, Entity, KeyDownEvent, SharedString, Window, div, px};
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
    pub cache: &'a Arc<MetricCatalogCache>,
}

impl<'a> MetricPickerView<'a> {
    /// Render the metric picker rail content.
    ///
    /// Layout:
    ///   ┌─────────────────────────────────────────┐
    ///   │ Header: Namespace / Metric name (pinned) │
    ///   ├─────────────────────────────────────────┤
    ///   │ Dimensions section (from cache)         │
    ///   ├─────────────────────────────────────────┤
    ///   │ Config section (period, stat, apply)    │
    ///   └─────────────────────────────────────────┘
    pub fn render(
        &mut self,
        window: &mut Window,
        cx: &mut Context<ChartShell>,
    ) -> impl IntoElement {
        // Kick dimensions fetch on the first render where the picker is
        // visible. Gated on `pending_dimensions_fetch` so we don't spawn
        // futures unconditionally from inside the render pass (documented
        // GPUI antipattern in CLAUDE.md). `ensure_dimensions_loaded` itself
        // remains idempotent — the flag just prevents redundant calls.
        if self.state.pending_dimensions_fetch {
            self.state.pending_dimensions_fetch = false;
            let shell_weak = cx.weak_entity();
            let cache = self.cache.clone();
            self.state.ensure_dimensions_loaded(shell_weak, cache, cx);
        }

        // Lazily build the Custom… text inputs the first time their dropdown
        // entry is selected (InputState::new requires a Window, which is only
        // available in render). We also wire a PressEnter subscription that
        // validates the value via the state's commit_custom_* helpers.
        ensure_custom_inputs(self.state, window, cx);

        let theme = cx.theme().clone();

        let header = render_metric_header(self.state, cx);
        let dimensions_section = render_dimensions_section(self.state, cx);
        let config_footer = render_config_footer(self.state, cx);
        let custom_row = render_custom_inputs_row(self.state, cx);

        let focus_handle = self.state.focus_handle.clone();

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(theme.popover)
            // Track focus so on_key_down receives keyboard events when the rail
            // is active. Clicking inside the picker focuses this handle.
            .track_focus(&focus_handle)
            // Cmd/Ctrl+Enter from anywhere in the picker triggers Apply.
            .on_key_down(cx.listener(|shell, event: &KeyDownEvent, _window, cx| {
                let ks = &event.keystroke;
                let is_apply = ks.key == "return"
                    && !ks.modifiers.shift
                    && !ks.modifiers.alt
                    && (ks.modifiers.platform || ks.modifiers.control);
                if is_apply && let Some(picker) = &mut shell.metric_picker {
                    // Flush any pending Custom… inputs so the user does not
                    // need to press Enter inside the input before Apply.
                    // If validation fails the inline error is shown and Apply
                    // is suppressed.
                    if !picker.flush_pending_custom_inputs(cx) {
                        cx.notify();
                        return;
                    }
                    let source = picker.build_metric_source();
                    cx.emit(ChartShellEvent::MetricPickerApplied(Box::new(source)));
                }
            }))
            // Header: pinned namespace + metric name.
            .child(header)
            // Divider.
            .child(div().h(px(1.0)).bg(theme.border))
            // Dimensions section.
            .child(dimensions_section)
            // Divider.
            .child(div().h(px(1.0)).bg(theme.border))
            // Config footer.
            .child(config_footer)
            // Inline custom-value row, only rendered when at least one of the
            // dropdowns is on "Custom…".
            .when_some(custom_row, |this, row| this.child(row))
    }
}

// ---------------------------------------------------------------------------
// Custom… text input lazy construction + render
// ---------------------------------------------------------------------------

/// Build the two Custom… `InputState` entities on first render and wire their
/// PressEnter subscriptions so committing the value runs the matching
/// validator on the picker state.
fn ensure_custom_inputs(
    state: &mut MetricPickerState,
    window: &mut Window,
    cx: &mut Context<ChartShell>,
) {
    if state.period_custom_input.is_none() {
        let input: Entity<InputState> =
            cx.new(|cx| InputState::new(window, cx).placeholder("Period (seconds)"));
        let sub = cx.subscribe(
            &input,
            |shell: &mut ChartShell, input_entity: Entity<InputState>, event: &InputEvent, cx| {
                if matches!(event, InputEvent::PressEnter { .. })
                    && let Some(picker) = &mut shell.metric_picker
                {
                    let raw = input_entity.read(cx).value().to_string();
                    picker.commit_custom_period(&raw);
                    cx.notify();
                }
            },
        );
        state.period_custom_input = Some(input);
        state.period_custom_sub = Some(sub);
    }

    if state.statistic_custom_input.is_none() {
        let input: Entity<InputState> =
            cx.new(|cx| InputState::new(window, cx).placeholder("Statistic (e.g. p99)"));
        let sub = cx.subscribe(
            &input,
            |shell: &mut ChartShell, input_entity: Entity<InputState>, event: &InputEvent, cx| {
                if matches!(event, InputEvent::PressEnter { .. })
                    && let Some(picker) = &mut shell.metric_picker
                {
                    let raw = input_entity.read(cx).value().to_string();
                    picker.commit_custom_statistic(&raw);
                    cx.notify();
                }
            },
        );
        state.statistic_custom_input = Some(input);
        state.statistic_custom_sub = Some(sub);
    }
}

/// Render an inline row beneath the config footer that shows whichever
/// Custom… text inputs are currently active. Returns `None` when neither
/// dropdown is on Custom….
fn render_custom_inputs_row(
    state: &MetricPickerState,
    cx: &mut Context<ChartShell>,
) -> Option<AnyElement> {
    if !state.period_custom_active && !state.statistic_custom_active {
        return None;
    }

    let theme = cx.theme().clone();

    let mut row = div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .py(Spacing::XS)
        .bg(theme.popover)
        .border_t_1()
        .border_color(theme.border);

    if state.period_custom_active
        && let Some(input) = state.period_custom_input.as_ref()
    {
        row = row.child(Input::new(input).placeholder("Period (seconds)"));
        if let Some(err) = state.period_custom_error.as_ref() {
            row = row.child(Text::muted(format!("Period: {err}")));
        }
    }

    if state.statistic_custom_active
        && let Some(input) = state.statistic_custom_input.as_ref()
    {
        row = row.child(Input::new(input).placeholder("Statistic (e.g. p99)"));
        if let Some(err) = state.statistic_custom_error.as_ref() {
            row = row.child(Text::muted(format!("Statistic: {err}")));
        }
    }

    Some(row.into_any_element())
}

// ---------------------------------------------------------------------------
// Header: pinned namespace + metric name
// ---------------------------------------------------------------------------

fn render_metric_header(state: &MetricPickerState, cx: &mut Context<ChartShell>) -> AnyElement {
    let theme = cx.theme().clone();
    let namespace: SharedString = SharedString::from(state.selected_namespace.clone());
    let metric_name: SharedString = SharedString::from(state.selected_metric_name.clone());

    div()
        .h(Heights::TOOLBAR)
        .flex()
        .flex_col()
        .justify_center()
        .px(Spacing::SM)
        .bg(theme.secondary)
        .child(
            div()
                .text_size(px(10.0))
                .text_color(theme.muted_foreground)
                .font_weight(gpui::FontWeight::BOLD)
                .child(namespace),
        )
        .child(
            div()
                .text_size(px(12.0))
                .text_color(theme.foreground)
                .overflow_hidden()
                .text_ellipsis()
                .child(metric_name),
        )
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

    let body: AnyElement = match &state.dimensions_state {
        DimensionsState::NotFetched | DimensionsState::Loading => div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .py(Spacing::SM)
            .child(Text::muted("Loading dimensions…"))
            .into_any_element(),

        DimensionsState::Error(msg) => {
            let msg = msg.clone();
            div()
                .flex()
                .flex_col()
                .p(Spacing::SM)
                .gap(Spacing::XS)
                .child(Text::muted(format!("Error: {msg}")))
                .child(
                    Button::new("metric-picker-dim-retry", "Retry")
                        .small()
                        .on_click(cx.listener(|shell, _, _, cx| {
                            if let Some(picker) = &mut shell.metric_picker {
                                picker.dimensions_state = DimensionsState::NotFetched;
                                picker.dimensions_task = None;
                                // Re-arm the fetch trigger so the next render
                                // re-issues ensure_dimensions_loaded.
                                picker.pending_dimensions_fetch = true;
                            }
                            cx.notify();
                        })),
                )
                .into_any_element()
        }

        DimensionsState::Loaded(combos) => {
            let current_filter = &state.dimension_filter;

            // AggregateAll row — always shown at the top.
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

            let dim_rows: Vec<AnyElement> = combos
                .iter()
                .enumerate()
                .map(|(i, combo)| {
                    let label = SharedString::from(
                        combo
                            .iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join(", "),
                    );
                    let is_filter_selected = match current_filter {
                        DimensionFilter::FilterTo(d) => d == combo,
                        _ => false,
                    };
                    let combo_for_click = combo.clone();
                    dim_radio_row(
                        i + 1,
                        label,
                        is_filter_selected,
                        &theme,
                        cx,
                        move |shell, _, _, cx| {
                            if let Some(picker) = &mut shell.metric_picker {
                                picker.dimension_filter =
                                    DimensionFilter::FilterTo(combo_for_click.clone());
                            }
                            cx.notify();
                        },
                    )
                    .into_any_element()
                })
                .collect();

            if combos.is_empty() {
                div()
                    .flex()
                    .flex_col()
                    .child(agg_row)
                    .child(
                        div()
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .child(Text::dim("No dimension combinations")),
                    )
                    .into_any_element()
            } else {
                div()
                    .flex()
                    .flex_col()
                    .child(agg_row)
                    .children(dim_rows)
                    .into_any_element()
            }
        }
    };

    div()
        .flex()
        .flex_col()
        .child(header)
        .child(body)
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Config footer: period, statistic dropdowns + Apply button
// ---------------------------------------------------------------------------

fn render_config_footer(state: &MetricPickerState, cx: &mut Context<ChartShell>) -> AnyElement {
    let theme = cx.theme().clone();

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
                .on_click(cx.listener(|shell, _, _, cx| {
                    if let Some(picker) = &mut shell.metric_picker {
                        // Flush any pending Custom… inputs so the click path
                        // commits typed values without requiring Enter.
                        if !picker.flush_pending_custom_inputs(cx) {
                            cx.notify();
                            return;
                        }
                        let source = picker.build_metric_source();
                        cx.emit(ChartShellEvent::MetricPickerApplied(Box::new(source)));
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
