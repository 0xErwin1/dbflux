//! Pieces of a task row that sit around the row itself: the status glyph,
//! the reason sub-rows and the stage rows of a parent task.

use super::TasksPanel;
use crate::ui::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{ChromeColors, ShellMetrics, ui};
use dbflux_core::{TaskSnapshot, TaskStatus};
use gpui::prelude::FluentBuilder;
use gpui::*;

const STAGE_ROW_HEIGHT: Rems = ui(26.0);
const STAGE_INDENT: Rems = ui(57.0);
const STAGE_FONT: Rems = ui(12.0);
const WAITING_MARKER: Rems = ui(10.0);
const REASON_ICON: Rems = ui(13.0);
const WARNING_WASH_ALPHA: f32 = 0.07;

/// Status mark of a task: a spinner while it works, a hollow circle while it
/// has not started or never will, and a filled icon once it settled.
pub(super) fn status_glyph(status: &TaskStatus, theme: &gpui_component::Theme) -> AnyElement {
    match status {
        TaskStatus::Running | TaskStatus::Cancelling => Icon::new(AppIcon::Loader)
            .size(ShellMetrics::TASK_ICON)
            .color(ChromeColors::tint(theme))
            .into_any_element(),
        TaskStatus::Completed => Icon::new(AppIcon::CircleCheck)
            .size(ShellMetrics::TASK_ICON)
            .color(theme.success)
            .into_any_element(),
        TaskStatus::Failed(_) => Icon::new(AppIcon::CircleX)
            .size(ShellMetrics::TASK_ICON)
            .color(theme.danger)
            .into_any_element(),
        TaskStatus::Cancelled => Icon::new(AppIcon::CircleX)
            .size(ShellMetrics::TASK_ICON)
            .color(theme.muted_foreground)
            .into_any_element(),
        TaskStatus::Pending | TaskStatus::Skipped => div()
            .flex_shrink_0()
            .flex()
            .items_center()
            .justify_center()
            .w(ShellMetrics::TASK_ICON)
            .h(ShellMetrics::TASK_ICON)
            .child(
                div()
                    .w(WAITING_MARKER)
                    .h(WAITING_MARKER)
                    .rounded_full()
                    .border_1()
                    .border_color(theme.muted_foreground),
            )
            .into_any_element(),
    }
}

/// How a reason sub-row is washed.
#[derive(Clone, Copy)]
pub(super) enum ReasonFill {
    /// The raised fill: the task cannot be cancelled, or was cancelled.
    Raised,
    /// A warning wash: a cancel is waiting for the worker.
    Warning,
}

/// A sub-row under a task row that says why its cancel control is off or
/// what a cancel is waiting for.
pub(super) fn reason_row(
    id: impl Into<ElementId>,
    fill: ReasonFill,
    text: String,
    theme: &gpui_component::Theme,
) -> Stateful<Div> {
    let (background, icon_color) = match fill {
        ReasonFill::Raised => (theme.secondary, theme.muted_foreground),
        ReasonFill::Warning => (theme.warning.opacity(WARNING_WASH_ALPHA), theme.warning),
    };

    div()
        .id(id)
        .flex()
        .items_center()
        .gap(ShellMetrics::TASKS_GAP)
        .w_full()
        .pt(ShellMetrics::TASK_ERROR_PADDING_TOP)
        .pb(ShellMetrics::TASK_ERROR_PADDING_BOTTOM)
        .pl(ShellMetrics::TASK_ERROR_INDENT)
        .pr(ShellMetrics::TASKS_PADDING_X)
        .bg(background)
        .border_b_1()
        .border_color(theme.table_row_border)
        .text_size(ShellMetrics::TASK_ERROR_FONT)
        .text_color(theme.foreground)
        .child(Icon::new(AppIcon::Info).size(REASON_ICON).color(icon_color))
        .child(div().min_w_0().child(text))
}

/// What a stage says next to its elapsed time, when its status needs words.
fn stage_status_label(
    status: &TaskStatus,
    theme: &gpui_component::Theme,
) -> Option<(String, Hsla)> {
    match status {
        TaskStatus::Pending => Some((
            dbflux_i18n::t!("tasks_panel.stage_waiting"),
            theme.muted_foreground,
        )),
        TaskStatus::Skipped => Some((
            dbflux_i18n::t!("tasks_panel.stage_not_started"),
            theme.muted_foreground,
        )),
        TaskStatus::Cancelled => Some((
            dbflux_i18n::t!("tasks_panel.stage_cancelled"),
            theme.muted_foreground,
        )),
        TaskStatus::Cancelling => Some((
            dbflux_i18n::t!("tasks_panel.cancelling_badge"),
            theme.warning,
        )),
        TaskStatus::Failed(_) => Some((dbflux_i18n::t!("tasks_panel.stage_failed"), theme.danger)),
        TaskStatus::Running | TaskStatus::Completed => None,
    }
}

/// The reason line above the stages of a cancelled parent.
pub(super) fn cancelled_with_stages_text(stages: &[TaskSnapshot]) -> String {
    match stages
        .iter()
        .find(|stage| stage.status == TaskStatus::Cancelled)
    {
        Some(stopped) => dbflux_i18n::t!(
            "tasks_panel.cancelled_with_stages_stopped",
            stage = stopped.description.as_str()
        ),
        None => dbflux_i18n::t!("tasks_panel.cancelled_with_stages"),
    }
}

/// The stages of a parent task as indented rows on the raised fill.
pub(super) fn stage_rows(
    stages: &[TaskSnapshot],
    mono_family: SharedString,
    theme: &gpui_component::Theme,
) -> Div {
    let muted = theme.muted_foreground;

    div()
        .w_full()
        .flex()
        .flex_col()
        .bg(theme.secondary)
        .border_b_1()
        .border_color(theme.table_row_border)
        .children(stages.iter().map(|stage| {
            let label = stage_status_label(&stage.status, theme);
            let timed = !matches!(stage.status, TaskStatus::Pending | TaskStatus::Skipped);
            let name_color = if stage.status.is_in_flight() {
                ChromeColors::strong(theme)
            } else {
                theme.foreground
            };

            div()
                .id(SharedString::from(format!("task-stage-{}", stage.id)))
                .flex()
                .items_center()
                .gap(ShellMetrics::TASKS_GAP)
                .w_full()
                .h(STAGE_ROW_HEIGHT)
                .pl(STAGE_INDENT)
                .pr(ShellMetrics::TASKS_PADDING_X)
                .border_b_1()
                .border_color(theme.table_row_border)
                .text_size(STAGE_FONT)
                .child(status_glyph(&stage.status, theme))
                .child(
                    div()
                        .min_w_0()
                        .overflow_hidden()
                        .whitespace_nowrap()
                        .text_ellipsis()
                        .text_color(name_color)
                        .child(stage.description.clone()),
                )
                .when(timed, |row| {
                    row.child(
                        div()
                            .flex_shrink_0()
                            .font_family(mono_family.clone())
                            .text_size(ShellMetrics::TASK_META_FONT)
                            .text_color(muted)
                            .child(TasksPanel::format_elapsed(stage.elapsed_secs)),
                    )
                })
                .when_some(label, |row, (text, color)| {
                    row.child(
                        div()
                            .flex_shrink_0()
                            .text_size(ShellMetrics::TASK_META_FONT)
                            .text_color(color)
                            .child(text),
                    )
                })
        }))
}
