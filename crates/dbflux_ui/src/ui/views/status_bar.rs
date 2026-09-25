use crate::app::{AppStateChanged, AppStateEntity};
use crate::ui::document::{TabManager, TabManagerEvent};
use crate::ui::icons::AppIcon;
use dbflux_components::primitives::{Chamfer, Icon, Status, StatusIndicator, Text};
use dbflux_components::tokens::{ChamferCut, ShellMetrics};
use dbflux_components::typography::AppFonts;
use dbflux_core::{TaskSnapshot, TaskStatus};
use dbflux_ui_document::StatusSegment;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::tooltip::Tooltip;
use std::time::Duration;

pub struct ToggleTasksPanel;

/// The status bar's approvals segment was clicked.
pub struct OpenApprovalsRequested;

pub struct StatusBar {
    app_state: Entity<AppStateEntity>,
    tab_manager: Entity<TabManager>,
    /// 100 ms notify loop that keeps the elapsed time of running tasks live.
    _timer: Option<Task<()>>,
}

impl EventEmitter<ToggleTasksPanel> for StatusBar {}
impl EventEmitter<OpenApprovalsRequested> for StatusBar {}

/// What the statement segment on the left of the status bar shows.
#[derive(Clone, Debug, PartialEq)]
enum StatementSummary {
    /// A task is running: its description and elapsed time.
    Running(String),
    /// The last task finished: its outcome, description and duration.
    Finished { status: TaskStatus, text: String },
    /// Nothing has run yet.
    Ready,
}

/// Counts shown by the background tasks segment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TaskCounts {
    running: usize,
    failed: usize,
}

impl StatusBar {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        tab_manager: Entity<TabManager>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&app_state, |_this, _, _: &AppStateChanged, cx| {
            cx.notify();
        })
        .detach();

        // The active tab's contributed status segments (DEC-23) can change on
        // activation or on document-internal updates; re-render generically
        // on any tab-manager event rather than tracking each document's
        // segment-affecting state individually.
        cx.subscribe(&tab_manager, |_this, _, _: &TabManagerEvent, cx| {
            cx.notify();
        })
        .detach();

        let timer = cx.spawn(async move |this, cx| {
            Self::timer_loop(this, cx).await;
        });

        Self {
            app_state,
            tab_manager,
            _timer: Some(timer),
        }
    }

    async fn timer_loop(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        loop {
            cx.background_executor()
                .timer(Duration::from_millis(100))
                .await;

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
        }
    }

    fn format_elapsed(secs: f64) -> String {
        if secs < 1.0 {
            format!("{:.0} ms", secs * 1000.0)
        } else if secs < 60.0 {
            format!("{:.1} s", secs)
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

    /// "description · duration", the form of the statement segment.
    fn statement_text(task: &TaskSnapshot) -> String {
        format!(
            "{} \u{b7} {}",
            Self::single_line(&task.description),
            Self::format_elapsed(task.elapsed_secs)
        )
    }

    fn statement_summary(
        running: Option<&TaskSnapshot>,
        last_finished: Option<&TaskSnapshot>,
    ) -> StatementSummary {
        if let Some(task) = running {
            return StatementSummary::Running(Self::statement_text(task));
        }

        match last_finished {
            Some(task) => StatementSummary::Finished {
                status: task.status.clone(),
                text: Self::statement_text(task),
            },
            None => StatementSummary::Ready,
        }
    }

    fn task_counts(tasks: &[TaskSnapshot]) -> TaskCounts {
        tasks
            .iter()
            .fold(TaskCounts::default(), |counts, task| match task.status {
                TaskStatus::Running => TaskCounts {
                    running: counts.running + 1,
                    ..counts
                },
                TaskStatus::Failed(_) => TaskCounts {
                    failed: counts.failed + 1,
                    ..counts
                },
                TaskStatus::Completed | TaskStatus::Cancelled => counts,
            })
    }

    fn metadata_text(text: impl Into<SharedString>) -> Text {
        Text::caption(text)
    }

    /// Statements and counters stay in the data face so their digits line up.
    fn readout_text(text: impl Into<SharedString>) -> Text {
        Text::code(text).font_size(ShellMetrics::STATUS_FONT)
    }

    /// Compact badge label and tooltip explaining why the active connection
    /// is read-only, differentiated by `ReadOnlyReason`.
    fn read_only_copy(reason: dbflux_core::ReadOnlyReason) -> (SharedString, SharedString) {
        use dbflux_core::ReadOnlyReason;

        match reason {
            ReadOnlyReason::ProfileSetting => (
                dbflux_i18n::t!("status_bar.read_only.profile.label").into(),
                dbflux_i18n::t!("status_bar.read_only.profile.tooltip").into(),
            ),
            ReadOnlyReason::ServerEnforced => (
                dbflux_i18n::t!("status_bar.read_only.server.label").into(),
                dbflux_i18n::t!("status_bar.read_only.server.tooltip").into(),
            ),
        }
    }

    /// A segment of the bar: full height, 12 px padding, 7 px gap.
    fn segment(id: impl Into<ElementId>) -> Stateful<Div> {
        div()
            .id(id)
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::STATUS_SEGMENT_GAP)
            .h_full()
            .px(ShellMetrics::STATUS_SEGMENT_PADDING_X)
            .whitespace_nowrap()
    }

    /// A segment on the right half, separated from the one before by a line.
    fn right_segment(id: impl Into<ElementId>, cx: &App) -> Stateful<Div> {
        Self::segment(id)
            .border_l_1()
            .border_color(cx.theme().border)
    }

    fn pending_approvals_count(&self, cx: &App) -> usize {
        #[cfg(feature = "mcp")]
        {
            match self.app_state.read(cx).list_mcp_pending_executions() {
                Ok(pending) => pending.len(),
                Err(error) => {
                    log::debug!("Failed to list pending MCP approvals: {error}");
                    0
                }
            }
        }

        #[cfg(not(feature = "mcp"))]
        {
            let _unused = cx;
            0
        }
    }

    fn render_connection(&self, cx: &App) -> AnyElement {
        let theme = cx.theme();
        let app_state = self.app_state.read(cx);

        let Some(connection) = app_state.active_connection() else {
            return div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .mx(ShellMetrics::STATUS_SEGMENT_PADDING_X)
                .text_color(theme.muted_foreground)
                .child(
                    StatusIndicator::new(Status::Idle)
                        .label(dbflux_i18n::t!("status_bar.no_connection")),
                )
                .into_any_element();
        };

        let metadata = connection.connection.metadata();
        let icon = AppIcon::for_driver(metadata.icon, metadata.category);
        let success = theme.success;

        div()
            .id("status-bar-connection")
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::STATUS_SEGMENT_GAP)
            .h(ShellMetrics::STATUS_CHIP_HEIGHT)
            .mx(ShellMetrics::STATUS_CHIP_MARGIN_X)
            .px(ShellMetrics::STATUS_CHIP_PADDING_X)
            .text_color(success)
            .font_weight(FontWeight::SEMIBOLD)
            .child(
                Chamfer::new(ChamferCut::KEYCAP)
                    .fill(success.opacity(ShellMetrics::STATUS_CHIP_ALPHA)),
            )
            .child(
                Icon::new(icon)
                    .size(ShellMetrics::STATUS_ICON)
                    .color(success),
            )
            .child(connection.profile.name.clone())
            .into_any_element()
    }

    fn render_statement(summary: StatementSummary, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let tint = dbflux_components::tokens::ChromeColors::tint(theme);

        let (icon, icon_color, text, framed) = match summary {
            StatementSummary::Running(text) => (AppIcon::Loader, tint, text, true),
            StatementSummary::Finished { status, text } => match status {
                TaskStatus::Failed(_) => (AppIcon::CircleX, theme.danger, text, true),
                TaskStatus::Cancelled => (AppIcon::CircleX, theme.muted_foreground, text, true),
                TaskStatus::Completed | TaskStatus::Running => {
                    (AppIcon::Check, theme.success, text, true)
                }
            },
            StatementSummary::Ready => (
                AppIcon::Check,
                theme.success,
                dbflux_i18n::t!("status_bar.ready"),
                false,
            ),
        };

        Self::segment("status-bar-statement")
            .flex_shrink(1.0)
            .min_w_0()
            .overflow_hidden()
            .when(framed, |segment| {
                segment.border_r_1().border_color(theme.border)
            })
            .child(
                Icon::new(icon)
                    .size(ShellMetrics::STATUS_ICON)
                    .color(icon_color),
            )
            .child(
                div()
                    .min_w_0()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(Self::readout_text(text)),
            )
    }

    fn render_task_segments(&self, counts: TaskCounts, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme();
        let tint = dbflux_components::tokens::ChromeColors::tint(theme);
        let danger = theme.danger;
        let muted = theme.muted_foreground;
        let hover = theme.list_hover;

        let mut segments: Vec<(SharedString, AppIcon, Hsla, String)> = Vec::new();

        if counts.running > 0 {
            segments.push((
                "tasks-toggle".into(),
                AppIcon::Loader,
                tint,
                crate::ui::labels::tasks_running_label(counts.running),
            ));
        }

        if counts.failed > 0 {
            segments.push((
                "tasks-failed".into(),
                AppIcon::CircleX,
                danger,
                dbflux_i18n::t!("status_bar.tasks_failed", count = counts.failed),
            ));
        }

        if segments.is_empty() {
            segments.push((
                "tasks-toggle".into(),
                AppIcon::Loader,
                muted,
                crate::ui::labels::background_tasks_label(0),
            ));
        }

        segments
            .into_iter()
            .map(|(id, icon, color, label)| {
                Self::right_segment(ElementId::Name(id.clone()), cx)
                    .debug_selector(move || id.to_string())
                    .cursor_pointer()
                    .hover(move |segment| segment.bg(hover))
                    .text_color(color)
                    .child(Icon::new(icon).size(ShellMetrics::STATUS_ICON).color(color))
                    .child(label)
                    .on_click(cx.listener(|_this, _, _, cx| {
                        cx.emit(ToggleTasksPanel);
                    }))
                    .into_any_element()
            })
            .collect()
    }
}

impl Render for StatusBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let app_state = self.app_state.read(cx);

        let read_only_copy = app_state
            .active_connection()
            .filter(|c| c.mutation_policy == dbflux_core::MutationPolicy::ReadOnly)
            .and_then(|c| c.read_only_reason)
            .map(Self::read_only_copy);

        let running_tasks = app_state.tasks().running_tasks();
        let last_finished = app_state.tasks().last_completed_task();
        let summary = Self::statement_summary(running_tasks.first(), last_finished.as_ref());
        let counts = Self::task_counts(&app_state.tasks().recent_tasks(usize::MAX));
        let unread = app_state.unread_error_count;
        let approvals = self.pending_approvals_count(cx);

        // Segments contributed by the active document (DEC-23) — e.g. engine
        // + region, bucket path, key count, cursor position. Empty for every
        // document that does not populate `PaneHandle::status_segments`.
        let status_segments: Vec<StatusSegment> = self
            .tab_manager
            .read(cx)
            .active_tab()
            .map(|tab| tab.status_segments(cx))
            .unwrap_or_default();

        let theme = cx.theme();
        let tint = dbflux_components::tokens::ChromeColors::tint(theme);
        let danger = theme.danger;
        let hover = theme.list_hover;

        let task_segments = self.render_task_segments(counts, cx);

        div()
            .id("status-bar")
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(ShellMetrics::STATUS_BAR_HEIGHT)
            .bg(cx.theme().background)
            .border_t_1()
            .border_color(cx.theme().border)
            .font_family(AppFonts::INTERFACE)
            .text_size(ShellMetrics::STATUS_FONT)
            .text_color(cx.theme().muted_foreground)
            .overflow_hidden()
            .child(self.render_connection(cx))
            .child(Self::render_statement(summary, cx))
            // Read-only indicator — shown only for the active connection,
            // differentiating whether the profile or the server enforced it.
            .when_some(read_only_copy, |this, (label, tooltip)| {
                this.child(
                    Self::segment("status-bar-read-only")
                        .child(Self::metadata_text(label))
                        .tooltip(move |window, cx| Tooltip::new(tooltip.clone()).build(window, cx)),
                )
            })
            .child(div().flex_1())
            .when(unread > 0, |this| {
                this.child(
                    Self::right_segment("error-badge", cx)
                        .cursor_pointer()
                        .hover(move |segment| segment.bg(hover))
                        .text_color(danger)
                        .child(
                            Icon::new(AppIcon::CircleAlert)
                                .size(ShellMetrics::STATUS_ICON)
                                .color(danger),
                        )
                        .child(Self::readout_text(unread.to_string()).color(danger))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.app_state.update(cx, |s, cx| {
                                s.clear_unread_errors(cx);
                                s.request_open_audit(None, cx);
                            });
                        })),
                )
            })
            .children(task_segments)
            .when(approvals > 0, |this| {
                this.child(
                    Self::right_segment("status-bar-approvals", cx)
                        .cursor_pointer()
                        .hover(move |segment| segment.bg(hover))
                        .text_color(tint)
                        .child(
                            Icon::new(AppIcon::Bell)
                                .size(ShellMetrics::STATUS_ICON)
                                .color(tint),
                        )
                        .child(crate::ui::labels::approvals_label(approvals))
                        .on_click(cx.listener(|_this, _, _, cx| {
                            cx.emit(OpenApprovalsRequested);
                        })),
                )
            })
            // Segments contributed by the active document, generically —
            // StatusBar never branches on document or driver type.
            .children(
                status_segments
                    .into_iter()
                    .enumerate()
                    .map(|(index, segment)| {
                        let mut el = Self::right_segment(
                            ElementId::Name(format!("status-segment-{index}").into()),
                            cx,
                        )
                        .child(segment.text);

                        if let Some(tooltip) = segment.tooltip {
                            el = el.tooltip(move |window, cx| {
                                Tooltip::new(tooltip.clone()).build(window, cx)
                            });
                        }

                        el
                    }),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::{StatementSummary, StatusBar, TaskCounts};
    use dbflux_components::primitives::TextVariant;
    use dbflux_components::tokens::ShellMetrics;
    use dbflux_components::typography::AppFonts;
    use dbflux_core::{TaskKind, TaskManager, TaskSnapshot, TaskStatus};

    #[test]
    fn status_bar_metadata_uses_small_interface_meta_role() {
        let inspection = StatusBar::metadata_text("dbflux-postgres").inspect();

        assert_eq!(inspection.family, AppFonts::INTERFACE);
        assert!(inspection.fallbacks.is_empty());
        assert_eq!(inspection.variant, TextVariant::Caption);
        assert_eq!(inspection.size_override, None);
        assert_eq!(inspection.weight_override, None);
        assert!(inspection.uses_role_default_color);
    }

    #[test]
    fn status_bar_readouts_use_the_mono_face_at_status_size() {
        let inspection = StatusBar::readout_text("SELECT 1 \u{b7} 12 ms").inspect();

        assert_eq!(inspection.family, AppFonts::MONO);
        assert_eq!(inspection.size_override, Some(ShellMetrics::STATUS_FONT));
    }

    fn task(status: TaskStatus, description: &str, elapsed_secs: f64) -> TaskSnapshot {
        let mut manager = TaskManager::new();
        let (id, _token) = manager.start(TaskKind::Query, description);
        let mut snapshot = manager.get(id).expect("started task");
        snapshot.status = status;
        snapshot.elapsed_secs = elapsed_secs;
        snapshot
    }

    #[test]
    fn statement_prefers_the_running_task_then_the_last_finished_one() {
        let running = task(TaskStatus::Running, "SELECT *\n  FROM tasks", 0.25);
        let finished = task(TaskStatus::Completed, "SELECT 1", 1.1);

        assert_eq!(
            StatusBar::statement_summary(Some(&running), Some(&finished)),
            StatementSummary::Running("SELECT * FROM tasks \u{b7} 250 ms".into())
        );
        assert_eq!(
            StatusBar::statement_summary(None, Some(&finished)),
            StatementSummary::Finished {
                status: TaskStatus::Completed,
                text: "SELECT 1 \u{b7} 1.1 s".into(),
            }
        );
        assert_eq!(
            StatusBar::statement_summary(None, None),
            StatementSummary::Ready
        );
    }

    #[test]
    fn task_counts_split_running_and_failed_tasks() {
        let tasks = [
            task(TaskStatus::Running, "a", 0.0),
            task(TaskStatus::Running, "b", 0.0),
            task(TaskStatus::Failed("refused".into()), "c", 0.0),
            task(TaskStatus::Completed, "d", 0.0),
            task(TaskStatus::Cancelled, "e", 0.0),
        ];

        assert_eq!(
            StatusBar::task_counts(&tasks),
            TaskCounts {
                running: 2,
                failed: 1,
            }
        );
    }

    #[test]
    fn read_only_copy_selects_profile_label_for_profile_setting() {
        let (label, tooltip) =
            StatusBar::read_only_copy(dbflux_core::ReadOnlyReason::ProfileSetting);

        assert_eq!(
            label,
            gpui::SharedString::from(dbflux_i18n::t!("status_bar.read_only.profile.label"))
        );
        assert_eq!(
            tooltip,
            gpui::SharedString::from(dbflux_i18n::t!("status_bar.read_only.profile.tooltip"))
        );
    }

    #[test]
    fn read_only_copy_selects_server_label_for_server_enforced() {
        let (label, tooltip) =
            StatusBar::read_only_copy(dbflux_core::ReadOnlyReason::ServerEnforced);

        assert_eq!(
            label,
            gpui::SharedString::from(dbflux_i18n::t!("status_bar.read_only.server.label"))
        );
        assert_eq!(
            tooltip,
            gpui::SharedString::from(dbflux_i18n::t!("status_bar.read_only.server.tooltip"))
        );
    }

    #[test]
    fn read_only_copy_keys_resolve_and_differ_between_reasons_and_locales() {
        let keys = [
            "status_bar.read_only.profile.label",
            "status_bar.read_only.profile.tooltip",
            "status_bar.read_only.server.label",
            "status_bar.read_only.server.tooltip",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }

        let profile_label_en = dbflux_i18n::t!("status_bar.read_only.profile.label", locale = "en");
        let server_label_en = dbflux_i18n::t!("status_bar.read_only.server.label", locale = "en");
        assert_ne!(
            profile_label_en, server_label_en,
            "profile and server labels must be distinguishable"
        );
    }
}
