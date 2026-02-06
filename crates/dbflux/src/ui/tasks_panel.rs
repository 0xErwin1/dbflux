use crate::app::{AppState, AppStateChanged};
use crate::ui::icons::AppIcon;
use dbflux_core::{TaskId, TaskKind, TaskSnapshot, TaskStatus};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::collections::HashSet;
use std::time::Duration;

pub struct TasksPanel {
    app_state: Entity<AppState>,
    _timer: Option<Task<()>>,
}

impl TasksPanel {
    pub fn new(app_state: Entity<AppState>, _window: &mut Window, cx: &mut Context<Self>) -> Self {
        cx.subscribe(&app_state, |_this, _, _: &AppStateChanged, cx| {
            cx.notify();
        })
        .detach();

        let timer = cx.spawn(async move |this, cx| {
            Self::timer_loop(this, cx).await;
        });

        Self {
            app_state,
            _timer: Some(timer),
        }
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
                        .map(|entity| entity.read(cx).app_state.read(cx).tasks().has_running_tasks())
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

    fn cancel_task(&mut self, task_id: TaskId, task_kind: TaskKind, cx: &mut Context<Self>) {
        if task_kind == TaskKind::Query
            && let Some(conn) = self
                .app_state
                .read(cx)
                .active_connection()
                .map(|c| c.connection.clone())
        {
            let cancel_handle = conn.cancel_handle();
            if let Err(e) = cancel_handle.cancel() {
                log::warn!("Failed to send cancel via handle: {}", e);
            }

            if let Err(e) = conn.cancel_active() {
                log::warn!("Failed to send cancel to database: {}", e);
            }
        }

        self.app_state.update(cx, |state, cx| {
            state.tasks_mut().cancel(task_id);
            cx.emit(AppStateChanged);
        });

        log::info!("Cancelled task from panel");
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

    fn render_task_row(&self, task: &TaskSnapshot, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let task_id = task.id;
        let task_kind = task.kind;
        let is_running = matches!(task.status, TaskStatus::Running);

        let status_icon = match &task.status {
            TaskStatus::Running => "⋯",
            TaskStatus::Completed => "✓",
            TaskStatus::Failed(_) => "✗",
            TaskStatus::Cancelled => "⊘",
        };

        let status_color = match &task.status {
            TaskStatus::Running => theme.accent,
            TaskStatus::Completed => gpui::rgb(0x22C55E).into(),
            TaskStatus::Failed(_) => gpui::rgb(0xDC2626).into(),
            TaskStatus::Cancelled => theme.muted_foreground,
        };

        div()
            .flex()
            .items_center()
            .justify_between()
            .w_full()
            .px_3()
            .py_1()
            .border_b_1()
            .border_color(theme.border)
            .hover(|s| s.bg(theme.secondary))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .flex_1()
                    .overflow_hidden()
                    .child(div().text_sm().text_color(status_color).child(status_icon))
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .text_ellipsis()
                            .child(task.description.clone()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("({})", Self::format_elapsed(task.elapsed_secs))),
                    ),
            )
            .when(is_running, |el| {
                el.child(
                    div()
                        .id(SharedString::from(format!("cancel-task-{}", task_id)))
                        .flex()
                        .items_center()
                        .justify_center()
                        .size_5()
                        .rounded(px(2.0))
                        .cursor_pointer()
                        .text_color(gpui::rgb(0xDC2626))
                        .hover(|s| s.bg(gpui::rgb(0xFEE2E2)))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.cancel_task(task_id, task_kind, cx);
                        }))
                        .child(
                            svg()
                                .path(AppIcon::Power.path())
                                .size_3()
                                .text_color(gpui::rgb(0xDC2626)),
                        ),
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
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("No background tasks"),
                )
            })
            .children(task_rows)
    }
}
