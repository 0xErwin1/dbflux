//! Pieces of a task row that sit around the row itself: the status glyph,
//! the reason sub-rows and the stage rows of a parent task.

use super::TasksPanel;
use crate::ui::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{ChromeColors, ShellMetrics, ui};
use dbflux_core::{TaskId, TaskManager, TaskSnapshot, TaskStatus};
use gpui::prelude::FluentBuilder;
use gpui::*;

const STAGE_ROW_HEIGHT: Rems = ui(26.0);
const STAGE_INDENT: Rems = ui(57.0);
const STAGE_NESTING_STEP: Rems = ui(16.0);
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

/// A stage of a parent task and how deep it sits below that parent.
#[derive(Clone)]
pub(super) struct StageEntry {
    pub(super) task: TaskSnapshot,
    pub(super) depth: usize,
}

/// Every descendant of `id`, depth-first in start order, so a stage of a
/// stage is listed right under its own parent.
pub(super) fn stage_tree(tasks: &TaskManager, id: TaskId) -> Vec<StageEntry> {
    let mut entries = Vec::new();
    collect_stages(tasks, id, 0, &mut entries);
    entries
}

fn collect_stages(tasks: &TaskManager, id: TaskId, depth: usize, entries: &mut Vec<StageEntry>) {
    for child in tasks.children_of(id) {
        let child_id = child.id;
        entries.push(StageEntry { task: child, depth });
        collect_stages(tasks, child_id, depth + 1, entries);
    }
}

/// The reason line above the stages of a cancelled parent.
///
/// It names the stopped stage only when the direct stages back the claim: the
/// first cancelled one is followed by stages and every one of those is
/// `Skipped`. Anything else reads the generic line.
pub(super) fn cancelled_with_stages_text(stages: &[StageEntry]) -> String {
    let direct: Vec<&TaskSnapshot> = stages
        .iter()
        .filter(|entry| entry.depth == 0)
        .map(|entry| &entry.task)
        .collect();

    let mut after_stopped = direct
        .iter()
        .skip_while(|stage| stage.status != TaskStatus::Cancelled);
    let stopped = after_stopped.next();
    let later: Vec<&&TaskSnapshot> = after_stopped.collect();
    let stopped = stopped.filter(|_| {
        !later.is_empty()
            && later
                .iter()
                .all(|stage| stage.status == TaskStatus::Skipped)
    });

    match stopped {
        Some(stopped) => dbflux_i18n::t!(
            "tasks_panel.cancelled_with_stages_stopped",
            stage = stopped.description.as_str()
        ),
        None => dbflux_i18n::t!("tasks_panel.cancelled_with_stages"),
    }
}

/// The stages of a parent task as indented rows on the raised fill.
pub(super) fn stage_rows(
    stages: &[StageEntry],
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
        .children(stages.iter().map(|entry| {
            let stage = &entry.task;
            let indent = STAGE_INDENT + STAGE_NESTING_STEP * entry.depth as f32;
            let label = stage_status_label(&stage.status, theme);
            let timed = !matches!(stage.status, TaskStatus::Pending | TaskStatus::Skipped);
            let name_color = if stage.status.is_in_flight() {
                ChromeColors::strong(theme)
            } else {
                theme.foreground
            };

            let row = div()
                .id(SharedString::from(format!("task-stage-{}", stage.id)))
                .flex()
                .items_center()
                .gap(ShellMetrics::TASKS_GAP)
                .w_full()
                .h(STAGE_ROW_HEIGHT)
                .pl(indent)
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
                });

            div()
                .w_full()
                .flex()
                .flex_col()
                .child(row)
                .children(stage_failure(stage, indent, mono_family.clone(), theme))
        }))
}

/// The failure message of a failed stage, and its output when it has any,
/// under the stage's row. The parent row is the only top-level row, so this
/// is the one place a stage's own diagnostics stay readable.
fn stage_failure(
    stage: &TaskSnapshot,
    indent: Rems,
    mono_family: SharedString,
    theme: &gpui_component::Theme,
) -> Option<Stateful<Div>> {
    let TaskStatus::Failed(error) = &stage.status else {
        return None;
    };

    let output = stage
        .details
        .as_deref()
        .filter(|details| !details.trim().is_empty());

    Some(
        div()
            .id(SharedString::from(format!("task-stage-error-{}", stage.id)))
            .w_full()
            .flex()
            .flex_col()
            .pt(ShellMetrics::TASK_ERROR_PADDING_TOP)
            .pb(ShellMetrics::TASK_ERROR_PADDING_BOTTOM)
            .pl(indent)
            .pr(ShellMetrics::TASKS_PADDING_X)
            .bg(theme.danger.opacity(ShellMetrics::TASK_ERROR_ALPHA))
            .border_b_1()
            .border_color(theme.table_row_border)
            .font_family(mono_family)
            .text_size(ShellMetrics::TASK_ERROR_FONT)
            .text_color(theme.danger)
            .children(
                TasksPanel::truncated_lines(error)
                    .into_iter()
                    .map(|line| div().child(line)),
            )
            .children(output.map(|details| {
                div().text_color(theme.muted_foreground).children(
                    TasksPanel::truncated_lines(details)
                        .into_iter()
                        .map(|line| div().child(line)),
                )
            })),
    )
}
