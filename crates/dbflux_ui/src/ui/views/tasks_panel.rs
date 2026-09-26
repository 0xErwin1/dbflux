use crate::app::{AppStateChanged, AppStateEntity};
use crate::ui::icons::AppIcon;
use dbflux_components::controls::Button;
use dbflux_components::primitives::{Badge, BadgeTone, Icon, Text};
use dbflux_components::tokens::{ChromeColors, ShellMetrics};
use dbflux_components::typography::AppFonts;
use dbflux_core::{TaskId, TaskKind, TaskSnapshot, TaskStatus};
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
    /// The workspace's keyboard focus is on the tasks panel.
    focused: bool,
    _timer: Option<Task<()>>,
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
            focused: false,
            _timer: Some(timer),
        }
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

    fn cancel_task(
        &mut self,
        task_id: TaskId,
        task_kind: TaskKind,
        profile_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        match task_kind {
            TaskKind::Query => {
                self.app_state.read(cx).cancel_query_for_task(task_id);
            }

            TaskKind::Connect => {
                if let Some(profile_id) = profile_id {
                    self.app_state.update(cx, |state, cx| {
                        state.cancel_running_connect_tasks_for_profile(profile_id);
                        cx.emit(AppStateChanged);
                    });
                    return;
                }
            }

            _ => {}
        }

        self.app_state.update(cx, |state, cx| {
            state.tasks_mut().cancel(task_id);
            cx.emit(AppStateChanged);
        });
    }

    fn dismiss_task(&mut self, task_id: TaskId, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.tasks_mut().remove(task_id);
            cx.emit(AppStateChanged);
        });
    }

    /// Removes every task that is no longer running, failures included.
    fn clear_finished(&mut self, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            let finished: Vec<TaskId> = state
                .tasks()
                .recent_tasks(usize::MAX)
                .into_iter()
                .filter(|task| task.status != TaskStatus::Running)
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

    fn status_icon(status: &TaskStatus, theme: &gpui_component::Theme) -> (AppIcon, Hsla) {
        match status {
            TaskStatus::Running => (AppIcon::Loader, ChromeColors::tint(theme)),
            TaskStatus::Completed => (AppIcon::CircleCheck, theme.success),
            TaskStatus::Failed(_) => (AppIcon::CircleX, theme.danger),
            TaskStatus::Cancelled => (AppIcon::CircleX, theme.muted_foreground),
        }
    }

    fn render_header(
        &self,
        running: usize,
        failed: usize,
        finished: usize,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
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
            .when(running > 0, |header| {
                header.child(Badge::new(
                    dbflux_i18n::t!("tasks_panel.running", count = running),
                    BadgeTone::Accent,
                ))
            })
            .when(failed > 0, |header| {
                header.child(Badge::new(
                    dbflux_i18n::t!("tasks_panel.failed", count = failed),
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
                .disabled(finished == 0)
                .on_click(cx.listener(|this, _, _, cx| {
                    cx.stop_propagation();
                    this.clear_finished(cx);
                })),
            )
    }

    fn render_task_row(&mut self, task: &TaskSnapshot, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let task_id = task.id;
        let task_kind = task.kind;
        let task_profile_id = task.profile_id;
        let is_cancellable = task.is_cancellable;
        let is_running = task.status == TaskStatus::Running;
        let failure = match &task.status {
            TaskStatus::Failed(error) => Some(error.clone()),
            _ => None,
        };
        let details_text = task.details.clone();
        let has_details = details_text
            .as_ref()
            .is_some_and(|details| !details.trim().is_empty());
        let is_expanded = self.expanded_task_ids.contains(&task_id);
        let (status_icon, status_color) = Self::status_icon(&task.status, theme);
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
                    .when(has_details, |row| {
                        row.cursor_pointer()
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.toggle_task_expanded(task_id, cx);
                            }))
                    })
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
                    .child(
                        Icon::new(status_icon)
                            .size(ShellMetrics::TASK_ICON)
                            .color(status_color),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .overflow_hidden()
                            .whitespace_nowrap()
                            .text_ellipsis()
                            .text_color(name_color)
                            .child(task.description.clone()),
                    )
                    .child(
                        div()
                            .flex_shrink_0()
                            .font_family(AppFonts::MONO)
                            .text_size(ShellMetrics::TASK_META_FONT)
                            .text_color(muted)
                            .child(Self::format_elapsed(task.elapsed_secs)),
                    )
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
                                .font_family(AppFonts::MONO)
                                .text_size(ShellMetrics::TASK_META_FONT)
                                .text_color(muted)
                                .child(format!("{}%", Self::progress_percent(progress))),
                        )
                    })
                    .when(is_cancellable, |row| {
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
            .when_some(failure, |el, error| {
                el.child(
                    div()
                        .pt(ShellMetrics::TASK_ERROR_PADDING_TOP)
                        .pb(ShellMetrics::TASK_ERROR_PADDING_BOTTOM)
                        .pl(ShellMetrics::TASK_ERROR_INDENT)
                        .pr(ShellMetrics::TASKS_PADDING_X)
                        .bg(danger.opacity(ShellMetrics::TASK_ERROR_ALPHA))
                        .font_family(AppFonts::MONO)
                        .text_size(ShellMetrics::TASK_ERROR_FONT)
                        .text_color(danger)
                        .children(
                            Self::truncated_lines(&error)
                                .into_iter()
                                .map(|line| div().child(line)),
                        ),
                )
            })
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
                        .font_family(AppFonts::MONO)
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
        .filter(|task| match task.status {
            TaskStatus::Running => false,
            TaskStatus::Failed(_) => true,
            TaskStatus::Completed | TaskStatus::Cancelled => {
                other_finished_count += 1;
                other_finished_count <= RECENT_FINISHED_TASK_LIMIT
            }
        })
        .collect()
}

impl Render for TasksPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.app_state.read(cx);

        let running_tasks = state.tasks().running_tasks();
        let finished_tasks = visible_finished_tasks(state.tasks().recent_tasks(usize::MAX));

        let running_count = running_tasks.len();
        let failed_count = finished_tasks
            .iter()
            .filter(|task| matches!(task.status, TaskStatus::Failed(_)))
            .count();
        let finished_count = finished_tasks.len();

        let all_tasks: Vec<TaskSnapshot> =
            running_tasks.into_iter().chain(finished_tasks).collect();
        let visible_task_ids: HashSet<TaskId> = all_tasks.iter().map(|task| task.id).collect();
        self.expanded_task_ids
            .retain(|task_id| visible_task_ids.contains(task_id));

        let mut task_rows: Vec<Div> = Vec::new();
        for task in &all_tasks {
            task_rows.push(self.render_task_row(task, cx));
        }

        let header = self.render_header(running_count, failed_count, finished_count, cx);
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
mod tests {
    use super::{RECENT_FINISHED_TASK_LIMIT, TasksPanel, visible_finished_tasks};
    use crate::app::{AppStateChanged, AppStateEntity};
    use dbflux_core::{TaskId, TaskKind, TaskManager, TaskStatus};
    use dbflux_storage::bootstrap::StorageRuntime;
    use gpui::{AppContext as _, Entity, Modifiers, TestAppContext, VisualTestContext};

    fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
        cx.update(dbflux_components::theme::init);

        cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("test storage runtime"),
                )
                .expect("test app state")
            })
        })
    }

    fn start_task(
        app_state: &Entity<AppStateEntity>,
        kind: TaskKind,
        cx: &mut VisualTestContext,
    ) -> TaskId {
        app_state.update(cx, |state, cx| {
            let (task_id, _token) = state.start_task(kind, kind.label());
            cx.emit(AppStateChanged);
            task_id
        })
    }

    fn selector(prefix: &str, task_id: TaskId) -> &'static str {
        format!("{prefix}-{task_id}").leak()
    }

    #[gpui::test]
    fn cancel_button_renders_only_for_cancellable_running_tasks(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let (_, window) = cx.add_window_view({
            let app_state = app_state.clone();
            move |window, cx| TasksPanel::new(app_state, window, cx)
        });

        let query_id = start_task(&app_state, TaskKind::Query, window);
        let key_value_ids = [TaskKind::KeyScan, TaskKind::KeyGet, TaskKind::KeyMutation]
            .map(|kind| start_task(&app_state, kind, window));
        window.run_until_parked();

        assert!(
            window
                .debug_bounds(selector("cancel-task", query_id))
                .is_some(),
            "a running query keeps its cancel button"
        );

        for task_id in key_value_ids {
            assert!(
                window
                    .debug_bounds(selector("cancel-task", task_id))
                    .is_none(),
                "a key-value task must not render a cancel button"
            );
        }
    }

    #[gpui::test]
    fn failed_task_survives_retention_and_is_removed_by_dismiss(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let (_, window) = cx.add_window_view({
            let app_state = app_state.clone();
            move |window, cx| TasksPanel::new(app_state, window, cx)
        });

        let failed_id = start_task(&app_state, TaskKind::KeyScan, window);
        let completed_id = start_task(&app_state, TaskKind::Query, window);
        app_state.update(window, |state, cx| {
            state.fail_task(failed_id, "connection reset");
            state.complete_task(completed_id);
            state.tasks_mut().cleanup_completed(0);
            cx.emit(AppStateChanged);
        });
        window.run_until_parked();

        assert!(
            app_state.read_with(window, |state, _| state.tasks().get(completed_id).is_none()),
            "a completed task is removed once past the retention window"
        );
        assert!(
            window
                .debug_bounds(selector("dismiss-task", completed_id))
                .is_none()
        );

        let dismiss_bounds = window
            .debug_bounds(selector("dismiss-task", failed_id))
            .expect("a failed task renders a dismiss button after the retention sweep");

        window.simulate_click(dismiss_bounds.center(), Modifiers::none());
        window.run_until_parked();

        assert!(
            app_state.read_with(window, |state, _| state.tasks().get(failed_id).is_none()),
            "dismiss removes the failed task"
        );
        assert!(
            window
                .debug_bounds(selector("dismiss-task", failed_id))
                .is_none()
        );
    }

    #[test]
    fn visible_finished_tasks_keeps_every_failure_and_caps_the_rest() {
        let mut manager = TaskManager::new();

        let failed_ids: Vec<TaskId> = (0..3)
            .map(|_| {
                let (id, _token) = manager.start(TaskKind::Export, "export");
                manager.fail(id, "disk full");
                id
            })
            .collect();

        for _ in 0..(RECENT_FINISHED_TASK_LIMIT + 3) {
            let (id, _token) = manager.start(TaskKind::Query, "query");
            manager.complete(id);
        }

        let (running_id, _token) = manager.start(TaskKind::Query, "running");

        let visible = visible_finished_tasks(manager.recent_tasks(usize::MAX));

        for failed_id in &failed_ids {
            assert!(visible.iter().any(|task| task.id == *failed_id));
        }
        assert!(visible.iter().all(|task| task.id != running_id));
        assert_eq!(
            visible
                .iter()
                .filter(|task| task.status == TaskStatus::Completed)
                .count(),
            RECENT_FINISHED_TASK_LIMIT
        );
    }
}
