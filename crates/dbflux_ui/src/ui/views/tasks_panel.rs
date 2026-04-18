use crate::app::{AppStateChanged, AppStateEntity};
use crate::ui::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_core::{TaskId, TaskKind, TaskSnapshot, TaskStatus};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

pub struct TasksPanel {
    app_state: Entity<AppStateEntity>,
    expanded_task_ids: HashSet<TaskId>,
    _timer: Option<Task<()>>,
}

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
            _timer: Some(timer),
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

            let should_notify = cx
                .update(|cx| {
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
                })
                .unwrap_or(false);

            if should_notify {
                cx.update(|cx| {
                    if let Some(entity) = this.upgrade() {
                        entity.update(cx, |_, cx| cx.notify());
                    }
                })
                .ok();
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
                })
                .ok();
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
                let task_target = self
                    .app_state
                    .read(cx)
                    .tasks()
                    .get(task_id)
                    .and_then(|task| task.target);

                if let Some(target) = task_target {
                    self.app_state.read(cx).cancel_query_for_target(&target);
                } else if let Some(profile_id) = profile_id {
                    let fallback_target = dbflux_core::TaskTarget {
                        profile_id,
                        database: None,
                    };

                    self.app_state
                        .read(cx)
                        .cancel_query_for_target(&fallback_target);
                }
            }

            TaskKind::KeyScan | TaskKind::KeyGet | TaskKind::KeyMutation => {
                // Soft cancel only — Redis/MongoDB drivers don't support driver-level cancel
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

    fn render_task_row(&mut self, task: &TaskSnapshot, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let task_id = task.id;
        let task_kind = task.kind;
        let task_profile_id = task.profile_id;
        let is_running = matches!(task.status, TaskStatus::Running);
        let details_text = task.details.clone().or_else(|| match &task.status {
            TaskStatus::Failed(error) => Some(error.clone()),
            _ => None,
        });
        let has_details = details_text
            .as_ref()
            .is_some_and(|details| !details.trim().is_empty());
        let is_expanded = self.expanded_task_ids.contains(&task_id);

        let status_icon = match &task.status {
            TaskStatus::Running => "⋯",
            TaskStatus::Completed => "✓",
            TaskStatus::Failed(_) => "✗",
            TaskStatus::Cancelled => "⊘",
        };

        let status_color = match &task.status {
            TaskStatus::Running => theme.accent,
            TaskStatus::Completed => theme.success,
            TaskStatus::Failed(_) => theme.danger,
            TaskStatus::Cancelled => theme.muted_foreground,
        };

        div()
            .w_full()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .w_full()
                    .px_3()
                    .py_1()
                    .hover(|s| s.bg(theme.secondary))
                    .when(has_details, |el| {
                        el.cursor_pointer().on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, _, cx| {
                                this.toggle_task_expanded(task_id, cx);
                            }),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .flex_1()
                            .overflow_hidden()
                            .when(has_details, |el| {
                                el.child(
                                    svg()
                                        .path(if is_expanded {
                                            AppIcon::ChevronDown.path()
                                        } else {
                                            AppIcon::ChevronRight.path()
                                        })
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                )
                            })
                            .child(Text::caption(status_icon.to_string()).text_color(status_color))
                            .child(
                                div()
                                    .flex_1()
                                    .text_ellipsis()
                                    .child(Text::body(task.description.clone())),
                            )
                            .child(Text::caption(format!(
                                "({})",
                                Self::format_elapsed(task.elapsed_secs)
                            ))),
                    )
                    .when(is_running, |el| {
                        let danger = theme.danger;
                        let danger_bg = theme.danger.opacity(0.1);
                        el.child(
                            div()
                                .id(SharedString::from(format!("cancel-task-{}", task_id)))
                                .flex()
                                .items_center()
                                .justify_center()
                                .size_5()
                                .rounded(px(2.0))
                                .cursor_pointer()
                                .text_color(danger)
                                .hover(move |s| s.bg(danger_bg))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.cancel_task(task_id, task_kind, task_profile_id, cx);
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::Power.path())
                                        .size_3()
                                        .text_color(danger),
                                ),
                        )
                    }),
            )
            .when(has_details && is_expanded, |el| {
                let mut lines: Vec<String> = details_text
                    .unwrap_or_default()
                    .lines()
                    .map(|line| line.to_string())
                    .collect();

                if lines.len() > 40 {
                    lines.truncate(40);
                    lines.push("... output truncated in panel".to_string());
                }

                el.child(
                    div()
                        .px_4()
                        .pb_2()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .bg(theme.secondary)
                        .children(lines.into_iter().map(Text::caption)),
                )
            })
    }
}

impl Render for TasksPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.app_state.read(cx);

        let running_tasks = state.tasks().running_tasks();
        let running_ids: HashSet<TaskId> = running_tasks.iter().map(|t| t.id).collect();

        let recent_tasks: Vec<TaskSnapshot> = state
            .tasks()
            .recent_tasks(10)
            .into_iter()
            .filter(|t| !running_ids.contains(&t.id))
            .take(5)
            .collect();

        let all_tasks: Vec<TaskSnapshot> = running_tasks.into_iter().chain(recent_tasks).collect();
        let visible_task_ids: HashSet<TaskId> = all_tasks.iter().map(|task| task.id).collect();
        self.expanded_task_ids
            .retain(|task_id| visible_task_ids.contains(task_id));

        let mut task_rows: Vec<Div> = Vec::new();
        for task in &all_tasks {
            task_rows.push(self.render_task_row(task, cx));
        }

        let theme = cx.theme();

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(theme.background)
            .when(all_tasks.is_empty(), |el: Div| {
                el.child(
                    div()
                        .flex()
                        .items_center()
                        .justify_center()
                        .py_4()
                        .child(Text::muted("No background tasks")),
                )
            })
            .children(task_rows)
    }
}
