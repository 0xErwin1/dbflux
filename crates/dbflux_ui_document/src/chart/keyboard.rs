//! Keyboard control of a chart, shared by the chart document, the chart view
//! of a result grid and a chart panel a dashboard has entered.
//!
//! The keys reach the host as keymap commands; the host hands the chart ones
//! to [`ChartShell::keyboard_command`]:
//!
//! - H and L move the highlighted point along the focused series, G and
//!   Shift+G jump to its first and last point, J and K move it to the
//!   neighboring series. The highlighted point stands in for the pointer, so
//!   the crosshair, the readout and the point inspector show it.
//! - Alt+H and Alt+L switch the chart kind.
//! - Space hides or shows the focused series, like its legend entry.
//! - While an axis picker is open (opened from the pane actions), J and K
//!   move its highlighted row, H and L switch to the neighboring picker,
//!   Enter picks the row, Space toggles a Y column and keeps the picker open,
//!   Escape closes it.
//!
//! Column roles come from `ColumnKind` only; nothing here knows a driver.

use super::shell::ChartShell;
use super::toolbar::CHART_KINDS;
use crate::pane::PaneAction;
use dbflux_app::keymap::Command;
use dbflux_components::chart::{AxisPickerOption, AxisPill, axis_picker_options};
use dbflux_components::common::time_range::state::TimeRange;
use dbflux_components::common::time_range::view::TimeRangePanel;
use dbflux_components::icons::AppIcon;
use dbflux_core::{ColumnMeta, DimensionFilter};
use gpui::{App, Context, Entity, Focusable as _, Window};

/// What a chart made of a keymap command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChartKeyOutcome {
    /// The command is not a chart key; the host handles it.
    Unhandled,
    /// The chart handled it.
    Handled,
    /// The chart handled it by changing the axis bindings, so the host must
    /// rebuild the chart view from its last result.
    BindingsChanged,
}

impl ChartKeyOutcome {
    pub(crate) fn handled(self) -> bool {
        self != ChartKeyOutcome::Unhandled
    }
}

/// The axis pickers in the order of their pills.
const AXIS_PILLS: [AxisPill; 4] = [AxisPill::X, AxisPill::Y, AxisPill::Group, AxisPill::Agg];

/// Number of time-range presets, Custom included (see
/// [`TimeRangePanel::time_range_for_index`]).
const TIME_RANGE_PRESET_COUNT: usize = 6;

impl ChartShell {
    /// Opens the picker of `pill` with the keyboard on the row the bindings
    /// hold now (the first row when none).
    pub(crate) fn open_axis_picker(
        &mut self,
        pill: AxisPill,
        columns: &[ColumnMeta],
        cx: &mut Context<Self>,
    ) {
        let bindings = self.active_bindings();

        self.axis_picker_cursor = axis_picker_options(pill, &bindings, columns)
            .iter()
            .position(|option| option.is_current(&bindings))
            .unwrap_or(0);
        self.axis_open_pill = Some(pill);
        cx.notify();
    }

    /// The row of the open axis picker the keyboard is on.
    pub(crate) fn axis_picker_cursor(&self) -> Option<usize> {
        self.axis_open_pill.map(|_| self.axis_picker_cursor)
    }

    /// Runs `cmd` as a chart key. `columns` are the columns of the result the
    /// chart draws, which the axis pickers list.
    pub(crate) fn keyboard_command(
        &mut self,
        cmd: Command,
        columns: &[ColumnMeta],
        cx: &mut Context<Self>,
    ) -> ChartKeyOutcome {
        if let Some(pill) = self.axis_open_pill {
            return self.axis_picker_command(pill, cmd, columns, cx);
        }

        match cmd {
            Command::NextPanelTab => self.step_chart_kind(1, cx),
            Command::PrevPanelTab => self.step_chart_kind(-1, cx),
            Command::ExpandCollapse => self.toggle_focused_series(cx),
            Command::ColumnLeft
            | Command::ColumnRight
            | Command::SelectFirst
            | Command::SelectLast
            | Command::SelectNext
            | Command::SelectPrev
            | Command::Cancel => self.point_command(cmd, cx),
            _ => ChartKeyOutcome::Unhandled,
        }
    }

    /// Moves the highlighted point or series.
    fn point_command(&mut self, cmd: Command, cx: &mut Context<Self>) -> ChartKeyOutcome {
        let Some(view) = self.chart_view.clone() else {
            return ChartKeyOutcome::Unhandled;
        };

        let handled = view.update(cx, |view, cx| match cmd {
            Command::ColumnLeft => view.step_keyboard_point(-1, cx),
            Command::ColumnRight => view.step_keyboard_point(1, cx),
            Command::SelectFirst => view.jump_keyboard_point(false, cx),
            Command::SelectLast => view.jump_keyboard_point(true, cx),
            Command::SelectNext => view.step_keyboard_series(1, cx),
            Command::SelectPrev => view.step_keyboard_series(-1, cx),
            Command::Cancel => view.clear_keyboard_point(cx),
            _ => false,
        });

        if !handled {
            return ChartKeyOutcome::Unhandled;
        }

        self.chart_focused_series_idx = view.read(cx).focused_series_idx();
        cx.notify();
        ChartKeyOutcome::Handled
    }

    fn step_chart_kind(&mut self, delta: isize, cx: &mut Context<Self>) -> ChartKeyOutcome {
        let current = CHART_KINDS
            .iter()
            .position(|kind| *kind == self.chart_kind())
            .unwrap_or(0);
        let next = (current as isize + delta).rem_euclid(CHART_KINDS.len() as isize) as usize;

        self.set_chart_kind(CHART_KINDS[next], cx);
        ChartKeyOutcome::Handled
    }

    fn toggle_focused_series(&mut self, cx: &mut Context<Self>) -> ChartKeyOutcome {
        let Some(view) = self.chart_view.clone() else {
            return ChartKeyOutcome::Unhandled;
        };

        let focused = view.read(cx).focused_series_idx();
        self.toggle_chart_series_hidden(focused, cx);
        ChartKeyOutcome::Handled
    }

    fn axis_picker_command(
        &mut self,
        pill: AxisPill,
        cmd: Command,
        columns: &[ColumnMeta],
        cx: &mut Context<Self>,
    ) -> ChartKeyOutcome {
        let bindings = self.active_bindings();
        let options = axis_picker_options(pill, &bindings, columns);
        let last = options.len().saturating_sub(1);

        let cursor = match cmd {
            Command::SelectNext if !options.is_empty() => {
                (self.axis_picker_cursor + 1) % options.len()
            }
            Command::SelectPrev if !options.is_empty() => {
                (self.axis_picker_cursor + last) % options.len()
            }
            Command::SelectFirst => 0,
            Command::SelectLast => last,
            Command::ColumnLeft | Command::ColumnRight => {
                let current = AXIS_PILLS.iter().position(|p| *p == pill).unwrap_or(0);
                let delta = if cmd == Command::ColumnLeft { -1 } else { 1 };
                let next = (current as isize + delta).rem_euclid(AXIS_PILLS.len() as isize);

                self.open_axis_picker(AXIS_PILLS[next as usize], columns, cx);
                return ChartKeyOutcome::Handled;
            }
            Command::Cancel => {
                self.close_axis_picker(cx);
                return ChartKeyOutcome::Handled;
            }
            Command::Execute | Command::ExpandCollapse => {
                let Some(option) = options.get(self.axis_picker_cursor).copied() else {
                    return ChartKeyOutcome::Handled;
                };

                self.apply_bindings(option.apply(&bindings), cx);

                // A Y column is one of several: Space leaves the picker open
                // on the same row so the next one can be toggled too.
                if matches!(option, AxisPickerOption::Y { .. }) && cmd == Command::ExpandCollapse {
                    self.axis_open_pill = Some(pill);
                }

                return ChartKeyOutcome::BindingsChanged;
            }
            _ => return ChartKeyOutcome::Unhandled,
        };

        self.axis_picker_cursor = cursor;
        cx.notify();
        ChartKeyOutcome::Handled
    }
}

/// Selects the time-range preset `delta` steps from the current one, Custom
/// included, wrapping at either end. Selecting Custom shows the custom range
/// row, which its Apply turns into a window.
pub(crate) fn step_time_range(panel: &Entity<TimeRangePanel>, delta: isize, cx: &mut App) {
    panel.update(cx, |panel, cx| {
        let current = (0..TIME_RANGE_PRESET_COUNT)
            .find(|index| TimeRangePanel::time_range_for_index(*index) == panel.selected_time_range)
            .unwrap_or(0);
        let next = (current as isize + delta).rem_euclid(TIME_RANGE_PRESET_COUNT as isize);

        panel.select_preset(next as usize, cx);
    });
}

/// The pane-action entries that pick a time range by key: next and previous
/// preset, and while Custom is selected the controls of the custom range row
/// and its Apply (`on_apply`).
pub(crate) fn time_range_pane_actions(
    panel: &Entity<TimeRangePanel>,
    id_prefix: &str,
    context: dbflux_app::keymap::ContextId,
    on_apply: impl Fn(&mut Window, &mut App) + 'static,
    cx: &App,
) -> Vec<PaneAction> {
    let mut actions = vec![
        PaneAction::command(
            format!("{id_prefix}-next-time-range"),
            dbflux_i18n::t!("document.chart.pane_actions.next_time_range"),
            Command::NextTimeRange,
            context,
        )
        .icon(AppIcon::Clock),
        PaneAction::command(
            format!("{id_prefix}-prev-time-range"),
            dbflux_i18n::t!("document.chart.pane_actions.prev_time_range"),
            Command::PrevTimeRange,
            context,
        )
        .icon(AppIcon::Clock),
    ];

    if panel.read(cx).selected_time_range != Some(TimeRange::Custom) {
        return actions;
    }

    let date_picker = panel.read(cx).custom_date_range_picker.clone();
    actions.push(PaneAction::callback(
        format!("{id_prefix}-custom-date-range"),
        dbflux_i18n::t!("document.chart.pane_actions.date_range"),
        move |window, cx| {
            let handle = date_picker.read(cx).focus_handle(cx);
            handle.focus(window, cx);
        },
    ));

    let dropdowns = {
        let panel = panel.read(cx);
        [
            (
                "start-hour",
                "document.chart.pane_actions.start_hour",
                panel.custom_start_hour_dropdown.clone(),
            ),
            (
                "start-minute",
                "document.chart.pane_actions.start_minute",
                panel.custom_start_minute_dropdown.clone(),
            ),
            (
                "end-hour",
                "document.chart.pane_actions.end_hour",
                panel.custom_end_hour_dropdown.clone(),
            ),
            (
                "end-minute",
                "document.chart.pane_actions.end_minute",
                panel.custom_end_minute_dropdown.clone(),
            ),
        ]
    };

    for (id, label_key, dropdown) in dropdowns {
        actions.push(PaneAction::callback(
            format!("{id_prefix}-custom-{id}"),
            dbflux_i18n::t!(label_key),
            move |window, cx| {
                dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
            },
        ));
    }

    actions.push(
        PaneAction::callback(
            format!("{id_prefix}-custom-apply"),
            dbflux_i18n::t!("document.chart.shell.custom_range.apply"),
            on_apply,
        )
        .icon(AppIcon::Check)
        .enabled(panel.read(cx).can_apply_custom_range(cx)),
    );

    actions
}

/// The pane-action entries of a chart shell: the chart kind, the axis
/// pickers (when there are columns to pick), the stats rail and, for a
/// metric chart, the metric picker rail and its controls.
pub(crate) fn chart_shell_pane_actions(
    shell: &Entity<ChartShell>,
    columns: &[ColumnMeta],
    context: dbflux_app::keymap::ContextId,
    cx: &App,
) -> Vec<PaneAction> {
    let mut actions = vec![
        PaneAction::command(
            "chart-next-kind",
            dbflux_i18n::t!("document.chart.pane_actions.next_kind"),
            Command::NextPanelTab,
            context,
        )
        .icon(AppIcon::ChartSpline),
        PaneAction::command(
            "chart-prev-kind",
            dbflux_i18n::t!("document.chart.pane_actions.prev_kind"),
            Command::PrevPanelTab,
            context,
        )
        .icon(AppIcon::ChartSpline),
    ];

    if !columns.is_empty() {
        for (pill, id, label_key) in [
            (
                AxisPill::X,
                "chart-axis-x",
                "document.chart.pane_actions.x_axis",
            ),
            (
                AxisPill::Y,
                "chart-axis-y",
                "document.chart.pane_actions.y_axis",
            ),
            (
                AxisPill::Group,
                "chart-axis-group",
                "document.chart.pane_actions.group_by",
            ),
            (
                AxisPill::Agg,
                "chart-axis-agg",
                "document.chart.pane_actions.aggregation",
            ),
        ] {
            let shell = shell.clone();
            let columns = columns.to_vec();

            actions.push(PaneAction::callback(
                id,
                dbflux_i18n::t!(label_key),
                move |_window, cx| {
                    shell.update(cx, |shell, cx| shell.open_axis_picker(pill, &columns, cx));
                },
            ));
        }
    }

    let stats_shell = shell.clone();
    actions.push(
        PaneAction::callback(
            "chart-stats",
            dbflux_i18n::t!("document.chart.toolbar.stats"),
            move |_window, cx| {
                stats_shell.update(cx, |shell, cx| shell.toggle_stats_rail(cx));
            },
        )
        .icon(AppIcon::Sigma),
    );

    if shell.read(cx).metric_picker.is_some() {
        actions.extend(metric_picker_pane_actions(shell));
    }

    actions
}

/// The metric picker rail and its controls: open or close the rail, step the
/// dimension filter, open the period and statistic lists, apply.
fn metric_picker_pane_actions(shell: &Entity<ChartShell>) -> Vec<PaneAction> {
    let mut actions = Vec::new();

    let rail_shell = shell.clone();
    actions.push(PaneAction::callback(
        "chart-metric-rail",
        dbflux_i18n::t!("document.chart.pane_actions.metric_rail"),
        move |_window, cx| {
            rail_shell.update(cx, |shell, cx| shell.toggle_metric_rail(cx));
        },
    ));

    for (id, label_key, delta) in [
        (
            "chart-metric-next-dimension",
            "document.chart.pane_actions.next_dimension",
            1,
        ),
        (
            "chart-metric-prev-dimension",
            "document.chart.pane_actions.prev_dimension",
            -1,
        ),
    ] {
        let shell = shell.clone();
        actions.push(PaneAction::callback(
            id,
            dbflux_i18n::t!(label_key),
            move |_window, cx| {
                shell.update(cx, |shell, cx| shell.step_metric_dimension(delta, cx));
            },
        ));
    }

    for (id, label_key, statistic) in [
        (
            "chart-metric-period",
            "document.chart.pane_actions.period",
            false,
        ),
        (
            "chart-metric-statistic",
            "document.chart.pane_actions.statistic",
            true,
        ),
    ] {
        let shell = shell.clone();
        actions.push(PaneAction::callback(
            id,
            dbflux_i18n::t!(label_key),
            move |window, cx| {
                let dropdown = shell.read(cx).metric_picker.as_ref().map(|picker| {
                    if statistic {
                        picker.statistic_dropdown.clone()
                    } else {
                        picker.period_dropdown.clone()
                    }
                });

                if let Some(dropdown) = dropdown {
                    dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
                }
            },
        ));
    }

    let apply_shell = shell.clone();
    actions.push(
        PaneAction::callback(
            "chart-metric-apply",
            dbflux_i18n::t!("document.chart.metric_picker.apply"),
            move |_window, cx| {
                apply_shell.update(cx, |shell, cx| shell.apply_metric_picker(cx));
            },
        )
        .icon(AppIcon::Check),
    );

    actions
}

impl ChartShell {
    /// Opens the stats rail, or closes it when it is the one open.
    pub(crate) fn toggle_stats_rail(&mut self, cx: &mut Context<Self>) {
        (self.chart_rail_open, self.chart_rail_tab) =
            if self.chart_rail_open && self.chart_rail_tab == super::ChartRailTab::Stats {
                (false, self.chart_rail_tab)
            } else {
                (true, super::ChartRailTab::Stats)
            };
        cx.notify();
    }

    /// Opens the metric picker rail, or closes it when it is the one open.
    fn toggle_metric_rail(&mut self, cx: &mut Context<Self>) {
        (self.chart_rail_open, self.chart_rail_tab) =
            if self.chart_rail_open && self.chart_rail_tab == super::ChartRailTab::Metric {
                (false, self.chart_rail_tab)
            } else {
                (true, super::ChartRailTab::Metric)
            };
        cx.notify();
    }

    /// Moves the metric picker's dimension filter `delta` rows through its
    /// list (aggregate all, then each loaded combination), wrapping.
    fn step_metric_dimension(&mut self, delta: isize, cx: &mut Context<Self>) {
        let Some(picker) = self.metric_picker.as_mut() else {
            return;
        };
        let super::metric_picker::DimensionsState::Loaded(combos) = &picker.dimensions_state else {
            return;
        };

        let rows: Vec<DimensionFilter> = std::iter::once(DimensionFilter::AggregateAll)
            .chain(combos.iter().cloned().map(DimensionFilter::FilterTo))
            .collect();
        let current = rows
            .iter()
            .position(|row| *row == picker.dimension_filter)
            .unwrap_or(0);
        let next = (current as isize + delta).rem_euclid(rows.len() as isize) as usize;

        picker.dimension_filter = rows[next].clone();
        cx.notify();
    }

    /// Applies the metric picker, as its Apply button does.
    fn apply_metric_picker(&mut self, cx: &mut Context<Self>) {
        let Some(picker) = self.metric_picker.as_mut() else {
            return;
        };

        if !picker.flush_pending_custom_inputs(cx) {
            cx.notify();
            return;
        }

        let source = picker.build_metric_source();
        cx.emit(super::ChartShellEvent::MetricPickerApplied(Box::new(
            source,
        )));
    }
}
