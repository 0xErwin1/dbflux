mod rows;

use self::rows::{
    ReasonFill, StageEntry, cancelled_with_stages_text, reason_row, stage_rows, stage_tree,
    status_glyph,
};
use crate::app::{AppStateChanged, AppStateEntity};
use crate::ui::icons::AppIcon;
use crate::ui::labels::{TaskHeadline, task_headline, task_profile_name};
use dbflux_components::controls::Button;
use dbflux_components::primitives::{Badge, BadgeTone, Icon, Text};
use dbflux_components::tokens::{ChromeColors, ShellMetrics};
use dbflux_core::{TaskId, TaskKind, TaskManager, TaskSnapshot, TaskStatus};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

/// The expanded panel's header was clicked: collapse the panel.
pub struct CollapseTasksPanel;

pub struct TasksPanel {
    app_state: Entity<AppStateEntity>,
    expanded_task_ids: HashSet<TaskId>,
    /// The task the keyboard points at. `None` until the keyboard moves,
    /// which points at the first row.
    selected_task_id: Option<TaskId>,
    /// Row of the selected task when it was chosen: the selection lands on
    /// the row now there once that task leaves the list.
    selected_index: usize,
    scroll_handle: ScrollHandle,
    /// The workspace's keyboard focus is on the tasks panel.
    focused: bool,
    _timer: Option<Task<()>>,
}

/// What the keyboard can do with the selected task, for the pane actions
/// menu of the panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedTaskActions {
    /// The task has output to show under its row.
    pub has_output: bool,
    /// That output is shown.
    pub output_shown: bool,
    pub cancellable: bool,
    /// The task is no longer running, so it can be dismissed.
    pub finished: bool,
}

impl EventEmitter<CollapseTasksPanel> for TasksPanel {}

impl TasksPanel {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&app_state, |_this, _, _: &AppStateChanged, cx| {
            cx.notify();
        })
        .detach();

        let timer = cx.spawn(async move |this, cx| {
            Self::timer_loop(this, cx).await;
        });

        Self {
            app_state,
            expanded_task_ids: HashSet::new(),
            selected_task_id: None,
            selected_index: 0,
            scroll_handle: ScrollHandle::new(),
            focused: false,
            _timer: Some(timer),
        }
    }

    /// The rows of the panel in order: running ones first, then the finished
    /// ones [`visible_finished_tasks`] keeps. Stages of a parent are not rows.
    fn visible_tasks(&self, cx: &App) -> Vec<TaskSnapshot> {
        top_level_tasks(self.app_state.read(cx).tasks())
    }

    /// Row of the selected task in `tasks`, or of the row that took its
    /// place when it left the list; `None` when the list is empty.
    fn selected_position(&self, tasks: &[TaskSnapshot]) -> Option<usize> {
        let last = tasks.len().checked_sub(1)?;

        self.selected_task_id
            .and_then(|task_id| tasks.iter().position(|task| task.id == task_id))
            .or(Some(self.selected_index.min(last)))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "`current` returned by `selected_position` is a row of `tasks` \
                  (an `iter().position()` hit, or `selected_index.min(last)`), \
                  and the four callers' closures map it into `0..=last` (`min`, \
                  `saturating_sub`, `0`, and `last` itself), so `index` stays \
                  below `tasks.len()`"
    )]
    fn move_selection(
        &mut self,
        target: impl FnOnce(usize, usize) -> usize,
        cx: &mut Context<Self>,
    ) {
        let tasks = self.visible_tasks(cx);
        let Some(current) = self.selected_position(&tasks) else {
            return;
        };

        let index = target(current, tasks.len() - 1);
        self.selected_index = index;
        self.selected_task_id = Some(tasks[index].id);
        self.scroll_handle.scroll_to_item(index);
        cx.notify();
    }

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        self.move_selection(|current, last| (current + 1).min(last), cx);
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        self.move_selection(|current, _| current.saturating_sub(1), cx);
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        self.move_selection(|_, _| 0, cx);
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        self.move_selection(|_, last| last, cx);
    }

    /// The task the keyboard points at.
    pub fn selected_task(&self, cx: &App) -> Option<TaskSnapshot> {
        let tasks = self.visible_tasks(cx);
        let index = self.selected_position(&tasks)?;

        tasks.into_iter().nth(index)
    }

    pub fn selected_actions(&self, cx: &App) -> Option<SelectedTaskActions> {
        let task = self.selected_task(cx)?;

        Some(SelectedTaskActions {
            has_output: has_output(&task),
            output_shown: self.expanded_task_ids.contains(&task.id),
            cancellable: task.is_cancellable,
            finished: task.status.is_terminal(),
        })
    }

    /// Shows or hides the output of the selected task, like a click on its
    /// row. Returns `false` when it has no output.
    pub fn toggle_selected_output(&mut self, cx: &mut Context<Self>) -> bool {
        match self.selected_task(cx) {
            Some(task) if has_output(&task) => {
                self.toggle_task_expanded(task.id, cx);
                true
            }
            _ => false,
        }
    }

    /// Cancels the selected task, like its cancel button. Returns `false`
    /// when it cannot be cancelled.
    pub fn cancel_selected(&mut self, cx: &mut Context<Self>) -> bool {
        match self.selected_task(cx) {
            Some(task) if task.is_cancellable => {
                self.cancel_task(task.id, task.kind, task.profile_id, cx);
                true
            }
            _ => false,
        }
    }

    /// Removes the selected task from the list once it has finished, like
    /// the dismiss button of a failed task. Returns `false` while it runs.
    pub fn dismiss_selected(&mut self, cx: &mut Context<Self>) -> bool {
        match self.selected_task(cx) {
            Some(task) if task.status.is_terminal() => {
                self.dismiss_task(task.id, cx);
                true
            }
            _ => false,
        }
    }

    /// Whether the list holds a task that is no longer running.
    pub fn has_finished_tasks(&self, cx: &App) -> bool {
        self.visible_tasks(cx)
            .iter()
            .any(|task| task.status.is_terminal())
    }

    pub fn set_focused(&mut self, focused: bool, cx: &mut Context<Self>) {
        if self.focused != focused {
            self.focused = focused;
            cx.notify();
        }
    }

    fn toggle_task_expanded(&mut self, task_id: TaskId, cx: &mut Context<Self>) {
        if !self.expanded_task_ids.insert(task_id) {
            self.expanded_task_ids.remove(&task_id);
        }

        cx.notify();
    }

    async fn timer_loop(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        let mut tick_count: u32 = 0;

        loop {
            cx.background_executor()
                .timer(Duration::from_millis(100))
                .await;

            tick_count = tick_count.wrapping_add(1);

            cx.update(|cx| {
                if let Some(entity) = this.upgrade() {
                    Self::report_cancel_dispatch_failures(&entity, cx);
                }
            });

            let should_notify = cx.update(|cx| {
                this.upgrade()
                    .map(|entity| {
                        entity
                            .read(cx)
                            .app_state
                            .read(cx)
                            .tasks()
                            .has_running_tasks()
                    })
                    .unwrap_or(false)
            });

            if should_notify {
                cx.update(|cx| {
                    if let Some(entity) = this.upgrade() {
                        entity.update(cx, |_, cx| cx.notify());
                    }
                });
            }

            if tick_count.is_multiple_of(300) {
                cx.update(|cx| {
                    if let Some(entity) = this.upgrade() {
                        entity.update(cx, |panel, cx| {
                            panel.app_state.update(cx, |state, _| {
                                state.tasks_mut().cleanup_completed(60);
                            });
                        });
                    }
                });
            }
        }
    }

    /// A driver cancel runs on its own thread, so a failure of the call
    /// itself reaches the user here, from where the failures are collected.
    fn report_cancel_dispatch_failures(entity: &Entity<Self>, cx: &mut App) {
        let failures = entity
            .read(cx)
            .app_state
            .read(cx)
            .take_cancel_dispatch_failures();

        for failure in failures {
            let message = dbflux_i18n::t!(
                "tasks_panel.query_cancel_rejected",
                driver = failure.driver,
                error = failure.error
            );
            report_error(UserFacingError::new(ErrorKind::Driver, message), cx);
        }
    }

    fn cancel_task(
        &mut self,
        task_id: TaskId,
        task_kind: TaskKind,
        profile_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        if task_kind == TaskKind::Connect
            && let Some(profile_id) = profile_id
        {
            self.app_state.update(cx, |state, cx| {
                state.cancel_running_connect_tasks_for_profile(profile_id);
                cx.emit(AppStateChanged);
            });
            return;
        }

        self.app_state.update(cx, |state, cx| {
            state.request_cancel_task(task_id);
            cx.emit(AppStateChanged);
        });
    }

    fn dismiss_task(&mut self, task_id: TaskId, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            for stage in stage_tree(state.tasks(), task_id) {
                state.tasks_mut().remove(stage.task.id);
            }
            state.tasks_mut().remove(task_id);
            cx.emit(AppStateChanged);
        });
    }

    /// Removes every task that is no longer running, failures included.
    pub fn clear_finished(&mut self, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            let finished: Vec<TaskId> = state
                .tasks()
                .recent_tasks(usize::MAX)
                .into_iter()
                .filter(|task| task.status.is_terminal())
                .map(|task| task.id)
                .collect();

            for task_id in finished {
                state.tasks_mut().remove(task_id);
            }

            cx.emit(AppStateChanged);
        });
    }

    fn format_elapsed(secs: f64) -> String {
        if secs < 1.0 {
            format!("{:.0}ms", secs * 1000.0)
        } else if secs < 60.0 {
            format!("{:.1}s", secs)
        } else {
            let mins = (secs / 60.0).floor() as u32;
            let remaining_secs = secs % 60.0;
            format!("{}m {:.0}s", mins, remaining_secs)
        }
    }

    /// Whole percent of a task's progress, clamped to 0..=100.
    fn progress_percent(progress: f32) -> u32 {
        (progress.clamp(0.0, 1.0) * 100.0).round() as u32
    }

    fn render_header(&self, counts: HeaderCounts, cx: &mut Context<Self>) -> Stateful<Div> {
        let theme = cx.theme();
        let title_color = if self.focused {
            ChromeColors::tint(theme)
        } else {
            ChromeColors::strong(theme)
        };

        div()
            .id("tasks-panel-header")
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::TASKS_GAP)
            .h(ShellMetrics::TASKS_HEADER_HEIGHT)
            .px(ShellMetrics::TASKS_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .cursor_pointer()
            .on_click(cx.listener(|_this, _, _, cx| {
                cx.emit(CollapseTasksPanel);
            }))
            .child(
                Icon::new(AppIcon::ChevronDown)
                    .size(ShellMetrics::TASK_CHEVRON)
                    .color(theme.muted_foreground),
            )
            .child(
                Icon::new(AppIcon::Loader)
                    .size(ShellMetrics::TASK_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                Text::body(dbflux_i18n::t!("tasks_panel.title"))
                    .font_weight(FontWeight::SEMIBOLD)
                    .color(title_color),
            )
            .when(counts.running > 0, |header| {
                header.child(Badge::new(
                    dbflux_i18n::t!("tasks_panel.running", count = counts.running),
                    BadgeTone::Accent,
                ))
            })
            .when(counts.cancelling > 0, |header| {
                header.child(
                    Badge::new(
                        dbflux_i18n::t!("tasks_panel.cancelling_count", count = counts.cancelling),
                        BadgeTone::Warning,
                    )
                    .id("tasks-cancelling-badge"),
                )
            })
            .when(counts.failed > 0, |header| {
                header.child(Badge::new(
                    dbflux_i18n::t!("tasks_panel.failed", count = counts.failed),
                    BadgeTone::Danger,
                ))
            })
            .child(div().flex_1())
            .child(
                Button::new(
                    "tasks-clear-finished",
                    dbflux_i18n::t!("tasks_panel.clear_finished"),
                )
                .icon(AppIcon::CircleX)
                .disabled(counts.finished == 0)
                .on_click(cx.listener(|this, _, _, cx| {
                    cx.stop_propagation();
                    this.clear_finished(cx);
                })),
            )
    }

    fn render_task_row(
        &mut self,
        view: &TaskRowView,
        index: usize,
        is_selected: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let theme = cx.theme();
        let task = &view.task;
        let task_id = task.id;
        let task_kind = task.kind;
        let task_profile_id = task.profile_id;
        let is_cancellable = task.is_cancellable;
        let is_running = !task.status.is_terminal();
        let is_cancelling = task.status == TaskStatus::Cancelling;
        let show_cancel = is_cancellable
            || (task.status.is_in_flight()
                && (is_cancelling || task.cancel_unavailable_reason.is_some()));
        let failure = match &task.status {
            TaskStatus::Failed(error) => Some(error.clone()),
            _ => None,
        };
        let details_text = task.details.clone();
        let has_details = has_output(task);
        let is_expanded = self.expanded_task_ids.contains(&task_id);
        let name_color = if is_running {
            ChromeColors::strong(theme)
        } else {
            theme.foreground
        };
        let progress = task.progress.filter(|_| is_running);
        let muted = theme.muted_foreground;
        let row_divider = theme.table_row_border;
        let track = theme.secondary;
        let fill = theme.primary;
        let danger = theme.danger;
        let selected_fill = ChromeColors::tint(theme).opacity(ShellMetrics::TASK_SELECTED_ALPHA);
        let reason_id = SharedString::from(format!("task-reason-{task_id}"));
        let reason = Self::reason_sub_row(view, reason_id, theme);
        let stages = (!view.stages.is_empty()).then(|| {
            stage_rows(
                &view.stages,
                dbflux_components::fonts::editor_family(cx),
                theme,
            )
        });

        div()
            .w_full()
            .flex()
            .flex_col()
            .child(
                div()
                    .id(SharedString::from(format!("task-row-{task_id}")))
                    .flex()
                    .items_center()
                    .gap(ShellMetrics::TASKS_GAP)
                    .w_full()
                    .h(ShellMetrics::TASK_ROW_HEIGHT)
                    .px(ShellMetrics::TASKS_PADDING_X)
                    .border_b_1()
                    .border_color(row_divider)
                    .text_size(ShellMetrics::TASK_FONT)
                    .when(is_selected, |row| row.bg(selected_fill))
                    .when(has_details, |row| row.cursor_pointer())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.selected_task_id = Some(task_id);
                        this.selected_index = index;

                        if has_details {
                            this.toggle_task_expanded(task_id, cx);
                        } else {
                            cx.notify();
                        }
                    }))
                    .child(div().flex_shrink_0().w(ShellMetrics::TASK_CHEVRON).when(
                        has_details,
                        |slot| {
                            slot.child(
                                Icon::new(if is_expanded {
                                    AppIcon::ChevronDown
                                } else {
                                    AppIcon::ChevronRight
                                })
                                .size(ShellMetrics::TASK_CHEVRON)
                                .color(muted),
                            )
                        },
                    ))
                    .child(status_glyph(&task.status, theme))
                    .child(Self::render_title(view, name_color, muted))
                    .child(
                        div()
                            .flex_shrink_0()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_size(ShellMetrics::TASK_META_FONT)
                            .text_color(muted)
                            .child(Self::format_elapsed(task.elapsed_secs)),
                    )
                    .when(is_cancelling, |row| {
                        row.child(
                            Badge::new(
                                dbflux_i18n::t!("tasks_panel.cancelling_badge"),
                                BadgeTone::Warning,
                            )
                            .id(SharedString::from(format!("task-cancelling-{task_id}"))),
                        )
                    })
                    .child(div().flex_1())
                    .when_some(progress, |row, progress| {
                        row.child(
                            div()
                                .flex_shrink_0()
                                .w(ShellMetrics::TASK_PROGRESS_WIDTH)
                                .h(ShellMetrics::TASK_PROGRESS_HEIGHT)
                                .bg(track)
                                .child(
                                    div()
                                        .h_full()
                                        .w(relative(progress.clamp(0.0, 1.0)))
                                        .bg(fill),
                                ),
                        )
                        .child(
                            div()
                                .flex_shrink_0()
                                .font_family(dbflux_components::fonts::editor_family(cx))
                                .text_size(ShellMetrics::TASK_META_FONT)
                                .text_color(muted)
                                .child(format!("{}%", Self::progress_percent(progress))),
                        )
                    })
                    .when(show_cancel, |row| {
                        let element_id = format!("cancel-task-{}", task_id);
                        row.child(
                            div().debug_selector(move || element_id).child(
                                Button::new(
                                    SharedString::from(format!("cancel-task-button-{task_id}")),
                                    dbflux_i18n::t!("tasks_panel.cancel"),
                                )
                                .icon(AppIcon::X)
                                .icon_size(ShellMetrics::TASK_CHEVRON)
                                .icon_only()
                                .disabled(!is_cancellable)
                                .on_click(cx.listener(
                                    move |this, _, _, cx| {
                                        cx.stop_propagation();
                                        this.cancel_task(task_id, task_kind, task_profile_id, cx);
                                    },
                                )),
                            ),
                        )
                    })
                    .when(failure.is_some(), |row| {
                        let element_id = format!("dismiss-task-{}", task_id);
                        row.child(
                            div().debug_selector(move || element_id).child(
                                Button::new(
                                    SharedString::from(format!("dismiss-task-button-{task_id}")),
                                    dbflux_i18n::t!("tasks_panel.dismiss"),
                                )
                                .ghost()
                                .icon(AppIcon::X)
                                .icon_size(ShellMetrics::TASK_CHEVRON)
                                .icon_only()
                                .on_click(cx.listener(
                                    move |this, _, _, cx| {
                                        cx.stop_propagation();
                                        this.dismiss_task(task_id, cx);
                                    },
                                )),
                            ),
                        )
                    }),
            )
            .children(reason)
            .when_some(failure, |el, error| {
                el.child(
                    div()
                        .pt(ShellMetrics::TASK_ERROR_PADDING_TOP)
                        .pb(ShellMetrics::TASK_ERROR_PADDING_BOTTOM)
                        .pl(ShellMetrics::TASK_ERROR_INDENT)
                        .pr(ShellMetrics::TASKS_PADDING_X)
                        .bg(danger.opacity(ShellMetrics::TASK_ERROR_ALPHA))
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(ShellMetrics::TASK_ERROR_FONT)
                        .text_color(danger)
                        .children(
                            Self::truncated_lines(&error)
                                .into_iter()
                                .map(|line| div().child(line)),
                        ),
                )
            })
            .children(stages)
            .when(has_details && is_expanded, |el| {
                let details = details_text.unwrap_or_default();

                el.child(
                    div()
                        .py(ShellMetrics::TASK_ERROR_PADDING_TOP)
                        .pl(ShellMetrics::TASK_ERROR_INDENT)
                        .pr(ShellMetrics::TASKS_PADDING_X)
                        .flex()
                        .flex_col()
                        .bg(track)
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(ShellMetrics::TASK_ERROR_FONT)
                        .text_color(muted)
                        .children(
                            Self::truncated_lines(&details)
                                .into_iter()
                                .map(|line| div().child(line)),
                        ),
                )
            })
    }

    /// "target · kind" with the target in semibold, or the description when
    /// the task has no target.
    fn render_title(view: &TaskRowView, name_color: Hsla, muted: Hsla) -> Div {
        let title = div()
            .flex()
            .min_w_0()
            .overflow_hidden()
            .whitespace_nowrap()
            .text_color(name_color);

        match &view.headline {
            Some(headline) => title
                .child(
                    div()
                        .flex_shrink_0()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child(headline.target.clone()),
                )
                .child(div().flex_shrink_0().text_color(muted).child(" \u{b7} "))
                .child(
                    div()
                        .min_w_0()
                        .overflow_hidden()
                        .text_ellipsis()
                        .child(headline.kind.clone()),
                ),
            None => title.child(
                div()
                    .min_w_0()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(view.task.description.clone()),
            ),
        }
    }

    /// The sub-row that explains a task whose cancel is waiting, refused, or
    /// settled together with its stages.
    fn reason_sub_row(
        view: &TaskRowView,
        id: SharedString,
        theme: &gpui_component::Theme,
    ) -> Option<Stateful<Div>> {
        let task = &view.task;

        match &task.status {
            TaskStatus::Cancelling => {
                let text = if task.kind == TaskKind::Query {
                    dbflux_i18n::t!("tasks_panel.cancelling_reason_query")
                } else {
                    dbflux_i18n::t!("tasks_panel.cancelling_reason")
                };

                Some(reason_row(id, ReasonFill::Warning, text, theme))
            }
            TaskStatus::Running => task
                .cancel_unavailable_reason
                .clone()
                .map(|reason| reason_row(id, ReasonFill::Raised, reason, theme)),
            TaskStatus::Cancelled if !view.stages.is_empty() => Some(reason_row(
                id,
                ReasonFill::Raised,
                cancelled_with_stages_text(&view.stages),
                theme,
            )),
            _ => None,
        }
    }

    /// The first 40 lines of a task's output, with a marker when more were cut.
    fn truncated_lines(text: &str) -> Vec<String> {
        let mut lines: Vec<String> = text.lines().map(str::to_string).collect();

        if lines.len() > 40 {
            lines.truncate(40);
            lines.push(dbflux_i18n::t!("tasks_panel.output_truncated"));
        }

        lines
    }
}

/// Whether a task has output the panel can show under its row.
fn has_output(task: &TaskSnapshot) -> bool {
    task.details
        .as_ref()
        .is_some_and(|details| !details.trim().is_empty())
}

/// The tasks the panel lists as rows: those in flight, then the finished
/// ones [`visible_finished_tasks`] keeps. A stage is shown under its parent.
fn top_level_tasks(tasks: &TaskManager) -> Vec<TaskSnapshot> {
    tasks
        .running_tasks()
        .into_iter()
        .filter(|task| task.parent_id.is_none())
        .chain(visible_finished_tasks(tasks.recent_tasks(usize::MAX)))
        .collect()
}

/// Counts the header badges show.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct HeaderCounts {
    running: usize,
    cancelling: usize,
    failed: usize,
    finished: usize,
}

impl HeaderCounts {
    fn of(tasks: &[TaskSnapshot]) -> Self {
        tasks
            .iter()
            .fold(Self::default(), |counts, task| match task.status {
                TaskStatus::Running => Self {
                    running: counts.running + 1,
                    ..counts
                },
                TaskStatus::Cancelling => Self {
                    cancelling: counts.cancelling + 1,
                    ..counts
                },
                TaskStatus::Failed(_) => Self {
                    failed: counts.failed + 1,
                    finished: counts.finished + 1,
                    ..counts
                },
                TaskStatus::Completed | TaskStatus::Cancelled | TaskStatus::Skipped => Self {
                    finished: counts.finished + 1,
                    ..counts
                },
                TaskStatus::Pending => counts,
            })
    }
}

/// A row of the panel: the task, how its title reads, and its stages.
struct TaskRowView {
    task: TaskSnapshot,
    headline: Option<TaskHeadline>,
    stages: Vec<StageEntry>,
}

/// Number of completed or cancelled tasks the panel lists under the running ones.
const RECENT_FINISHED_TASK_LIMIT: usize = 5;

/// Picks the finished tasks to list, newest first, from `recent_tasks`.
///
/// Every failed task is kept so its error stays readable until the user
/// dismisses it. Completed and cancelled tasks are capped at
/// [`RECENT_FINISHED_TASK_LIMIT`].
fn visible_finished_tasks(recent_tasks: Vec<TaskSnapshot>) -> Vec<TaskSnapshot> {
    let mut other_finished_count = 0;

    recent_tasks
        .into_iter()
        .filter(|task| task.parent_id.is_none())
        .filter(|task| match task.status {
            TaskStatus::Running | TaskStatus::Cancelling | TaskStatus::Pending => false,
            TaskStatus::Failed(_) => true,
            TaskStatus::Completed | TaskStatus::Cancelled | TaskStatus::Skipped => {
                other_finished_count += 1;
                other_finished_count <= RECENT_FINISHED_TASK_LIMIT
            }
        })
        .collect()
}

impl Render for TasksPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.app_state.read(cx);
        let all_tasks = top_level_tasks(state.tasks());
        let counts = HeaderCounts::of(&all_tasks);
        let views: Vec<TaskRowView> = all_tasks
            .iter()
            .map(|task| TaskRowView {
                headline: task_headline(task, task_profile_name(state.profiles(), task).as_deref()),
                stages: stage_tree(state.tasks(), task.id),
                task: task.clone(),
            })
            .collect();

        let visible_task_ids: HashSet<TaskId> = all_tasks.iter().map(|task| task.id).collect();
        self.expanded_task_ids
            .retain(|task_id| visible_task_ids.contains(task_id));

        let selected = self
            .focused
            .then(|| self.selected_position(&all_tasks))
            .flatten();

        let mut task_rows: Vec<Div> = Vec::new();
        for (index, view) in views.iter().enumerate() {
            task_rows.push(self.render_task_row(view, index, selected == Some(index), cx));
        }

        let header = self.render_header(counts, cx);
        let theme = cx.theme();

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(theme.background)
            .border_t_1()
            .border_color(theme.input)
            .child(header)
            .child(
                div()
                    .id("tasks-panel-rows")
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .track_scroll(&self.scroll_handle)
                    .when(all_tasks.is_empty(), |el| {
                        el.child(
                            div()
                                .flex()
                                .items_center()
                                .justify_center()
                                .py(ShellMetrics::TASKS_PADDING_X)
                                .child(Text::caption(dbflux_i18n::t!("tasks_panel.empty"))),
                        )
                    })
                    .children(task_rows),
            )
    }
}

#[cfg(test)]
mod tests;
