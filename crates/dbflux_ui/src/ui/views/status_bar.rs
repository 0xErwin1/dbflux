use crate::app::{AppStateChanged, AppStateEntity};
use crate::ui::theme::ghost_border_color;
use dbflux_components::primitives::{Icon, StatusDot, StatusDotVariant};
use dbflux_components::tokens::{Anim, ChromeColors, FontSizes};
use dbflux_components::typography::{MonoCaption, MonoMeta};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::time::Duration;

pub struct ToggleTasksPanel;

pub struct StatusBar {
    app_state: Entity<AppStateEntity>,
    /// Periodic notify task that drives the 100 ms busy-pulse animation.
    /// Present only while there are running tasks. Dropping it stops the loop.
    _pulse_task: Option<Task<()>>,
    /// Tracks which opacity phase the pulse dot is in (on / off).
    pulse_visible: bool,
    /// Legacy 100 ms notify loop — kept for the elapsed-time counter on running tasks.
    _timer: Option<Task<()>>,
}

impl EventEmitter<ToggleTasksPanel> for StatusBar {}

impl StatusBar {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.on_app_state_changed(cx);
        })
        .detach();

        let timer = cx.spawn(async move |this, cx| {
            Self::timer_loop(this, cx).await;
        });

        Self {
            app_state,
            _pulse_task: None,
            pulse_visible: true,
            _timer: Some(timer),
        }
    }

    fn on_app_state_changed(&mut self, cx: &mut Context<Self>) {
        let has_running = self.app_state.read(cx).tasks().has_running_tasks();

        if has_running && self._pulse_task.is_none() {
            // Spawn the pulse loop: toggles pulse_visible every PULSE_INTERVAL_MS.
            let task = cx.spawn(async move |this, cx| {
                Self::pulse_loop(this, cx).await;
            });
            self._pulse_task = Some(task);
        } else if !has_running {
            self._pulse_task = None;
            self.pulse_visible = true;
        }

        cx.notify();
    }

    async fn pulse_loop(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        loop {
            cx.background_executor()
                .timer(Duration::from_millis(Anim::PULSE_INTERVAL_MS))
                .await;

            let keep_running = cx
                .update(|cx| {
                    this.upgrade().map(|entity| {
                        entity.update(cx, |bar, cx| {
                            let running = bar.app_state.read(cx).tasks().has_running_tasks();

                            if running {
                                bar.pulse_visible = !bar.pulse_visible;
                                cx.notify();
                                true
                            } else {
                                bar.pulse_visible = true;
                                bar._pulse_task = None;
                                cx.notify();
                                false
                            }
                        })
                    })
                })
                .ok()
                .flatten()
                .unwrap_or(false);

            if !keep_running {
                break;
            }
        }
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

    fn single_line(text: &str) -> String {
        text.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
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
            Self::single_line(&task.description),
            Self::format_elapsed(task.elapsed_secs)
        )
    }

    fn metadata_text(text: impl Into<SharedString>) -> MonoMeta {
        MonoMeta::new(text)
    }

    fn status_text(text: impl Into<SharedString>) -> MonoCaption {
        MonoCaption::new(text).font_size(FontSizes::SM)
    }
}

impl Render for StatusBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let app_state = self.app_state.read(cx);

        let connection = app_state.active_connection();
        let connection_name = connection
            .map(|c| c.profile.name.clone())
            .unwrap_or_default();
        let is_connected = connection.is_some();

        let running_tasks = app_state.tasks().running_tasks();
        let running_count = running_tasks.len();
        let is_busy = running_count > 0;
        let current_task = running_tasks.first();

        let last_completed = if current_task.is_none() {
            app_state.tasks().last_completed_task()
        } else {
            None
        };

        // Pick the StatusDot variant.
        // While busy, alternate between Busy and Idle on each pulse tick to emulate
        // the CSS @keyframes pulse effect from the design bundle.
        let dot_variant = if is_busy {
            if self.pulse_visible {
                StatusDotVariant::Busy
            } else {
                StatusDotVariant::Idle
            }
        } else if is_connected {
            StatusDotVariant::Success
        } else {
            StatusDotVariant::Idle
        };

        let divider_color = ChromeColors::ghost_border();

        div()
            .flex()
            .items_center()
            .justify_between()
            .h(px(32.0))
            .bg(cx.theme().background)
            .border_t_1()
            .border_color(ghost_border_color())
            // Left section: connection indicator + task info
            .child(
                div()
                    .flex()
                    .flex_1()
                    .items_center()
                    .overflow_x_hidden()
                    .whitespace_nowrap()
                    // Segment: StatusDot + connection name
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(px(10.0))
                            .h(px(22.0))
                            .child(StatusDot::new(dot_variant))
                            .when(is_connected, |this| {
                                this.child(Self::metadata_text(connection_name))
                            })
                            .when(!is_connected, |this| {
                                this.child(Self::metadata_text("disconnected"))
                            }),
                    )
                    // Running task info — shown with a divider when a task is active
                    .when_some(current_task.cloned(), |this, task| {
                        let description = Self::single_line(&task.description);
                        this.child(Self::vertical_divider(divider_color)).child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(px(10.0))
                                .h(px(22.0))
                                .child(Self::status_text(description))
                                .child(
                                    Self::metadata_text(format!(
                                        "({})",
                                        Self::format_elapsed(task.elapsed_secs)
                                    ))
                                    .color(cx.theme().primary),
                                ),
                        )
                    })
                    .when_some(last_completed, |this, task| {
                        this.child(Self::vertical_divider(divider_color)).child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(px(10.0))
                                .h(px(22.0))
                                .child(Self::status_text(Self::format_completed_task(&task))),
                        )
                    }),
            )
            // Right section: tasks toggle
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .child(Self::vertical_divider(divider_color))
                    .child(
                        div()
                            .id("tasks-toggle")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(px(10.0))
                            .h(px(22.0))
                            .cursor_pointer()
                            .hover(|s| s.bg(cx.theme().secondary))
                            .on_click(cx.listener(|_this, _, _, cx| {
                                cx.emit(ToggleTasksPanel);
                            }))
                            .when(running_count > 0, |this| {
                                this.child(
                                    Icon::new(crate::ui::icons::AppIcon::Loader)
                                        .size(px(12.0))
                                        .primary(),
                                )
                                .child(Self::status_text(format!("{} running", running_count)))
                            })
                            .when(running_count == 0, |this| {
                                this.child(Self::status_text("Tasks"))
                            }),
                    ),
            )
    }
}

impl StatusBar {
    /// Renders a 1 px vertical ghost-border separator between status bar sections.
    fn vertical_divider(color: gpui::Hsla) -> impl IntoElement {
        div().w(px(1.0)).h(px(16.0)).bg(color).flex_shrink_0()
    }
}

#[cfg(test)]
mod tests {
    use super::StatusBar;
    use dbflux_components::tokens::FontSizes;
    use dbflux_components::typography::AppFonts;

    #[test]
    fn status_bar_metadata_uses_small_mono_meta_role() {
        let inspection = StatusBar::metadata_text("dbflux-postgres").inspect();

        assert_eq!(inspection.family, Some(AppFonts::MONO));
        assert_eq!(inspection.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(inspection.size_override, Some(FontSizes::SM));
        assert_eq!(inspection.weight_override, None);
        assert!(inspection.uses_muted_foreground_override);
        assert!(!inspection.has_custom_color_override);
    }

    #[test]
    fn status_bar_copy_keeps_mono_family_with_readable_small_size() {
        let running = StatusBar::status_text("2 running").inspect();
        let divider = StatusBar::status_text("|").inspect();

        for inspection in [running, divider] {
            assert_eq!(inspection.family, Some(AppFonts::MONO));
            assert_eq!(inspection.fallbacks, &[AppFonts::MONO_FALLBACK]);
            assert_eq!(inspection.size_override, Some(FontSizes::SM));
            assert_eq!(inspection.weight_override, None);
            assert!(inspection.uses_muted_foreground_override);
            assert!(!inspection.has_custom_color_override);
        }
    }
}
