use crate::app::{AppState, AppStateChanged};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, IconName, IconNamed};
use std::time::Duration;

pub struct ToggleTasksPanel;

pub struct StatusBar {
    app_state: Entity<AppState>,
    _timer: Option<Task<()>>,
}

impl EventEmitter<ToggleTasksPanel> for StatusBar {}

impl StatusBar {
    pub fn new(app_state: Entity<AppState>, _window: &mut Window, cx: &mut Context<Self>) -> Self {
        cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.on_app_state_changed(cx);
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

    fn on_app_state_changed(&mut self, cx: &mut Context<Self>) {
        cx.notify();
    }

    async fn timer_loop(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        loop {
            cx.background_executor()
                .timer(Duration::from_millis(100))
                .await;

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
        }
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

    fn format_completed_task(task: &dbflux_core::TaskSnapshot) -> String {
        let status_icon = match &task.status {
            dbflux_core::TaskStatus::Completed => "✓",
            dbflux_core::TaskStatus::Failed(_) => "✗",
            dbflux_core::TaskStatus::Cancelled => "⊘",
            dbflux_core::TaskStatus::Running => "⋯",
        };

        format!(
            "{} {} ({})",
            status_icon,
            task.description,
            Self::format_elapsed(task.elapsed_secs)
        )
    }
}

impl Render for StatusBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let app_state = self.app_state.read(cx);

        let connection_info = app_state
            .active_connection()
            .map(|c| c.profile.name.clone())
            .unwrap_or_else(|| "No connection".to_string());

        let running_tasks = app_state.tasks().running_tasks();
        let running_count = running_tasks.len();
        let current_task = running_tasks.first();

        let last_completed = if current_task.is_none() {
            app_state.tasks().last_completed_task()
        } else {
            None
        };

        div()
            .flex()
            .items_center()
            .justify_between()
            .h(px(24.0))
            .px_2()
            .bg(cx.theme().tab_bar)
            .border_t_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(connection_info),
                    )
                    .when_some(current_task.cloned(), |this, task| {
                        this.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("|")
                                .child(task.description.clone())
                                .child(div().text_xs().text_color(cx.theme().accent).child(
                                    format!("({})", Self::format_elapsed(task.elapsed_secs)),
                                )),
                        )
                    })
                    .when_some(last_completed, |this, task| {
                        this.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("|")
                                .child(Self::format_completed_task(&task)),
                        )
                    }),
            )
            .child(
                div()
                    .id("tasks-toggle")
                    .flex()
                    .items_center()
                    .gap_1()
                    .px_2()
                    .cursor_pointer()
                    .rounded(px(4.0))
                    .hover(|s| s.bg(cx.theme().secondary))
                    .on_click(cx.listener(|_this, _, _, cx| {
                        cx.emit(ToggleTasksPanel);
                    }))
                    .when(running_count > 0, |this| {
                        this.child(
                            svg()
                                .path(IconName::Loader.path())
                                .size_3()
                                .text_color(cx.theme().accent),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child(format!("{} task(s)", running_count)),
                        )
                    })
                    .when(running_count == 0, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("Tasks"),
                        )
                    }),
            )
    }
}
