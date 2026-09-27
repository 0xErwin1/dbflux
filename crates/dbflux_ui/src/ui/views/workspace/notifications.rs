//! The title-bar bell and the notifications center it opens
//! (IslNotifications, IslNotificationStates).
//!
//! The model lives in `dbflux_ui_base::notifications` on the app state; this
//! module feeds it the live sources (pending MCP approvals, the available
//! update, finished tasks) and turns its snapshot into the popover.

use std::cell::Cell;
use std::rc::Rc;

use super::*;
use dbflux_app::updates::{CheckedAgo, InstallSource};
use dbflux_components::composites::{
    BellUrgency, NotificationBell, NotificationFilterChip, NotificationGroupSection,
    NotificationIconTone, NotificationPopover, NotificationRow,
};
use dbflux_components::controls::Button;
use dbflux_components::primitives::BadgeTone;
use dbflux_components::tokens::{NotificationMetrics, ShellMetrics};
use dbflux_core::chrono::{DateTime, Utc};
use dbflux_ui_base::notifications::{
    ErrorNotification, LiveApproval, LiveUpdate, NotificationEntry, NotificationFilter,
    NotificationGroup, NotificationKey, NotificationSnapshot, NotificationSources,
    NotificationUrgency, TaskNotification,
};

/// Paint priority of the toast stack among the workspace's deferred layers.
pub(super) const TOAST_LAYER_PRIORITY: usize = 0;

/// Paint priority of the open notifications popover: above the toast stack,
/// so a toast in the top-right corner never covers the popover's header,
/// and above the deferred menus of documents (priority 1 and 2).
pub(super) const NOTIFICATIONS_LAYER_PRIORITY: usize = 3;

/// Open state of the notifications popover, owned by the workspace.
pub(super) struct NotificationsPopoverState {
    open: bool,
    filter: NotificationFilter,
    focus_handle: FocusHandle,
    /// Window bounds of the bell, recorded while painting the title bar, so
    /// the popover lines up under it.
    anchor: Rc<Cell<Option<Bounds<Pixels>>>>,
}

impl NotificationsPopoverState {
    pub(super) fn new(cx: &mut App) -> Self {
        Self {
            open: false,
            filter: NotificationFilter::All,
            focus_handle: cx.focus_handle(),
            anchor: Rc::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn is_open(&self) -> bool {
        self.open
    }
}

/// What an approval row shows about a pending MCP execution.
struct PendingApproval {
    id: String,
    tool_id: String,
    actor_id: String,
    connection_id: String,
    classification_label: String,
    classification_tone: BadgeTone,
    created_at_epoch_ms: i64,
}

/// "waiting 4 min", as the approvals document words it.
fn waiting_label(created_at_epoch_ms: i64, now_ms: i64) -> String {
    let minutes = (now_ms - created_at_epoch_ms).max(0) / 60_000;

    if minutes < 1 {
        dbflux_i18n::t!("document.governance.waiting.just_now")
    } else if minutes < 60 {
        dbflux_i18n::t!("document.governance.waiting.minutes", count = minutes)
    } else {
        dbflux_i18n::t!("document.governance.waiting.hours", count = minutes / 60)
    }
}

/// Bell badge for the model's urgency.
fn bell_urgency(urgency: NotificationUrgency) -> BellUrgency {
    match urgency {
        NotificationUrgency::None => BellUrgency::None,
        NotificationUrgency::Info => BellUrgency::Info,
        NotificationUrgency::Approval => BellUrgency::Approval,
        NotificationUrgency::Error => BellUrgency::Error,
    }
}

/// "12 min ago", in the wording of the update check's status line.
fn ago_label(at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    match CheckedAgo::between(at, now) {
        CheckedAgo::JustNow => dbflux_i18n::t!("updates.ago.just_now"),
        CheckedAgo::Minutes(count) => dbflux_i18n::t!("updates.ago.minutes", count = count),
        CheckedAgo::Hours(count) => dbflux_i18n::t!("updates.ago.hours", count = count),
        CheckedAgo::Days(count) => dbflux_i18n::t!("updates.ago.days", count = count),
    }
}

fn first_line(text: &str) -> &str {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
}

fn filter_label(filter: NotificationFilter) -> String {
    match filter {
        NotificationFilter::All => dbflux_i18n::t!("notifications.filter.all"),
        NotificationFilter::Approvals => dbflux_i18n::t!("notifications.filter.approvals"),
        NotificationFilter::Errors => dbflux_i18n::t!("notifications.filter.errors"),
        NotificationFilter::Updates => dbflux_i18n::t!("notifications.filter.updates"),
    }
}

fn group_label(group: NotificationGroup) -> String {
    match group {
        NotificationGroup::NeedsYou => dbflux_i18n::t!("notifications.group.needs_you"),
        NotificationGroup::Updates => dbflux_i18n::t!("notifications.group.updates"),
        NotificationGroup::Earlier => dbflux_i18n::t!("notifications.group.earlier"),
    }
}

fn task_icon(kind: dbflux_core::TaskKind) -> AppIcon {
    match kind {
        dbflux_core::TaskKind::Migrate => AppIcon::ArrowUpDown,
        dbflux_core::TaskKind::Export => AppIcon::FileDown,
        dbflux_core::TaskKind::Import => AppIcon::Download,
        _ => AppIcon::ScrollText,
    }
}

fn row_id(key: &NotificationKey) -> ElementId {
    ElementId::Name(format!("notification-row-{}", key.slug()).into())
}

fn action_id(action: &str, key: &NotificationKey) -> ElementId {
    ElementId::Name(format!("notification-{action}-{}", key.slug()).into())
}

impl Workspace {
    /// Records finished tasks whenever the app state changes, and redraws
    /// the bell.
    pub(super) fn subscribe_notifications(
        app_state: &Entity<AppStateEntity>,
        cx: &mut Context<Self>,
    ) {
        cx.subscribe(app_state, |this, _, _: &AppStateChanged, cx| {
            this.record_finished_tasks(cx);
            cx.notify();
        })
        .detach();
    }

    fn record_finished_tasks(&mut self, cx: &mut Context<Self>) {
        let tasks = self.app_state.read(cx).tasks().recent_tasks(usize::MAX);

        self.app_state.update(cx, |state, _| {
            state
                .notifications
                .record_finished_tasks(&tasks, Utc::now());
        });
    }

    /// MCP executions waiting for a decision; empty without MCP support or
    /// when the governance service cannot list them.
    #[cfg(feature = "mcp")]
    fn pending_approvals(&self, cx: &App) -> Vec<PendingApproval> {
        use crate::ui::document::McpApprovalsView;

        let pending = match self.app_state.read(cx).list_mcp_pending_executions() {
            Ok(pending) => pending,
            Err(error) => {
                log::debug!("Failed to list pending MCP approvals: {error}");
                return Vec::new();
            }
        };

        pending
            .into_iter()
            .map(|pending| PendingApproval {
                classification_label: McpApprovalsView::classification_display(
                    pending.classification,
                ),
                classification_tone: McpApprovalsView::classification_tone(pending.classification),
                id: pending.id,
                tool_id: pending.tool_id,
                actor_id: pending.actor_id,
                connection_id: pending.connection_id,
                created_at_epoch_ms: pending.created_at_epoch_ms,
            })
            .collect()
    }

    #[cfg(not(feature = "mcp"))]
    fn pending_approvals(&self, _cx: &App) -> Vec<PendingApproval> {
        Vec::new()
    }

    fn notification_sources(&self, cx: &App) -> NotificationSources {
        let approvals = self
            .pending_approvals(cx)
            .into_iter()
            .map(|pending| LiveApproval {
                created_at: DateTime::<Utc>::from_timestamp_millis(pending.created_at_epoch_ms)
                    .unwrap_or_else(Utc::now),
                id: pending.id,
            })
            .collect();

        let state = self.app_state.read(cx);
        let update = state.visible_update().map(|update| LiveUpdate {
            label: update.label.clone(),
            checked_at: state.update_check().checked_at.unwrap_or_else(Utc::now),
        });

        NotificationSources { approvals, update }
    }

    fn notifications_snapshot(&self, cx: &App) -> NotificationSnapshot {
        let sources = self.notification_sources(cx);
        self.app_state.read(cx).notifications.snapshot(&sources)
    }

    /// The bell at the right end of the title bar, with the badge of the
    /// most urgent unread notification. It records its bounds for the
    /// popover.
    pub(super) fn render_notification_bell(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let snapshot = self.notifications_snapshot(cx);
        let workspace = cx.entity().clone();
        let anchor = self.notifications.anchor.clone();

        let bell = NotificationBell::new(
            "notifications-bell",
            dbflux_i18n::t!("notifications.title"),
            snapshot.unread_count(),
        )
        .urgency(bell_urgency(snapshot.urgency()))
        .open(self.notifications.open)
        .on_click(move |_, window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.toggle_notifications(window, cx);
            });
        });

        div().relative().flex_shrink_0().child(bell).child(
            canvas(
                move |bounds, _, _| anchor.set(Some(bounds)),
                |_, _, _, _| {},
            )
            .absolute()
            .size_full(),
        )
    }

    pub(super) fn toggle_notifications(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.notifications.open {
            self.close_notifications(window, cx);
            return;
        }

        self.notifications.open = true;
        self.notifications.filter = NotificationFilter::All;
        self.notifications.focus_handle.focus(window, cx);
        cx.notify();
    }

    fn close_notifications(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.notifications.open {
            return;
        }

        self.notifications.open = false;
        self.set_focus(self.focus_target, window, cx);
        cx.notify();
    }

    fn mark_notification_read(&mut self, key: NotificationKey, cx: &mut Context<Self>) {
        self.app_state
            .update(cx, |state, _| state.notifications.mark_read(key));
        cx.notify();
    }

    /// Opens a row's target and marks it read: the approvals tab on that
    /// request, Audit filtered by the error, the release notes of the
    /// update, or the background tasks panel.
    fn open_notification(
        &mut self,
        key: NotificationKey,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match &key {
            NotificationKey::Approval(id) => self.review_approval(id.clone(), window, cx),
            NotificationKey::Error(correlation_id) => {
                self.view_error_in_audit(*correlation_id, window, cx)
            }
            NotificationKey::Update(_) => self.open_update_notes(window, cx),
            NotificationKey::Task(_) => {
                self.close_notifications(window, cx);
                self.set_focus(FocusTarget::BackgroundTasks, window, cx);
            }
        }

        self.mark_notification_read(key, cx);
    }

    fn review_approval(&mut self, pending_id: String, window: &mut Window, cx: &mut Context<Self>) {
        self.close_notifications(window, cx);

        #[cfg(feature = "mcp")]
        {
            self.app_state.update(cx, |state, _| {
                state.pending_approval_focus = Some(pending_id);
            });
            self.open_mcp_approvals(window, cx);
        }

        #[cfg(not(feature = "mcp"))]
        {
            let _unused = pending_id;
        }
    }

    /// The same path as the error toast's "View in Audit".
    fn view_error_in_audit(
        &mut self,
        correlation_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_notifications(window, cx);
        self.app_state.update(cx, |state, cx| {
            state.request_open_audit(Some(correlation_id), cx);
        });
    }

    fn open_update_notes(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let notes_url = self
            .app_state
            .read(cx)
            .visible_update()
            .map(|update| update.notes_url.clone());

        self.close_notifications(window, cx);

        if let Some(url) = notes_url {
            cx.open_url(&url);
        }
    }

    fn install_update(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let download_url = self
            .app_state
            .read(cx)
            .visible_update()
            .map(|update| update.download_url.clone());

        self.close_notifications(window, cx);

        if let Some(url) = download_url {
            cx.open_url(&url);
        }
    }

    fn update_notification_later(&mut self, key: NotificationKey, cx: &mut Context<Self>) {
        self.app_state
            .update(cx, |state, _| state.notifications.dismiss(key));
        cx.notify();
    }

    fn mark_all_notifications_read(&mut self, cx: &mut Context<Self>) {
        let sources = self.notification_sources(cx);
        self.app_state
            .update(cx, |state, _| state.notifications.mark_all_read(&sources));
        cx.notify();
    }

    fn clear_read_notifications(&mut self, cx: &mut Context<Self>) {
        let sources = self.notification_sources(cx);
        self.app_state
            .update(cx, |state, _| state.notifications.clear_read(&sources));
        cx.notify();
    }

    /// The popover and the transparent layer behind it that closes it on a
    /// click outside, or `None` while it is closed. It floats over the
    /// islands, right-aligned with the bell, 2 px under the title bar.
    pub(super) fn render_notifications_popover(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        if !self.notifications.open {
            return None;
        }

        let viewport = window.viewport_size();
        let right = self
            .notifications
            .anchor
            .get()
            .map(|bounds| (viewport.width - bounds.right()).max(px(0.0)))
            .unwrap_or(ShellMetrics::TITLE_BAR_PADDING_X);
        let top = ShellMetrics::TITLE_BAR_HEIGHT + NotificationMetrics::POPOVER_GAP_TOP;
        let max_height = viewport.height * NotificationMetrics::POPOVER_MAX_HEIGHT_FRACTION;

        let popover = self.build_notifications_popover(cx).max_height(max_height);

        let backdrop = div()
            .id("notifications-backdrop")
            .absolute()
            .inset_0()
            .occlude()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| this.close_notifications(window, cx)),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(|this, _, window, cx| this.close_notifications(window, cx)),
            );

        let panel = div()
            .id("notifications-popover-anchor")
            .absolute()
            .top(top)
            .right(right)
            .track_focus(&self.notifications.focus_handle)
            .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                if run_command(action) == Some(Command::Cancel) {
                    this.close_notifications(window, cx);
                } else {
                    cx.propagate();
                }
            }))
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                if event.keystroke.key == "escape" {
                    cx.stop_propagation();
                    this.close_notifications(window, cx);
                }
            }))
            .child(popover);

        Some(
            deferred(
                div()
                    .id("notifications-layer")
                    .absolute()
                    .inset_0()
                    .child(backdrop)
                    .child(panel),
            )
            .with_priority(NOTIFICATIONS_LAYER_PRIORITY)
            .into_any_element(),
        )
    }

    fn build_notifications_popover(&self, cx: &mut Context<Self>) -> NotificationPopover {
        let snapshot = self.notifications_snapshot(cx);
        let title = dbflux_i18n::t!("notifications.title");
        let close_label = dbflux_i18n::t!("notifications.close");

        let popover = NotificationPopover::new("notifications-popover", title).on_close(
            close_label,
            cx.listener(|this, _, window, cx| this.close_notifications(window, cx)),
        );

        if snapshot.is_empty() {
            return popover.empty(
                dbflux_i18n::t!("notifications.empty.title"),
                dbflux_i18n::t!("notifications.empty.message"),
            );
        }

        let unread = snapshot.unread_count();
        let has_read = snapshot.entries.iter().any(|entry| entry.read);
        let filter = self.notifications.filter;

        let mut popover = popover
            .unread_label(
                (unread > 0)
                    .then(|| dbflux_i18n::t!("notifications.unread", count = unread).into()),
            )
            .footer(
                dbflux_i18n::t!("notifications.footer.hint"),
                dbflux_i18n::t!("notifications.footer.clear_read"),
                has_read,
            )
            .on_clear_read(cx.listener(|this, _, _, cx| this.clear_read_notifications(cx)));

        if unread > 0 {
            popover = popover.mark_all_read(
                Button::new(
                    "notifications-mark-all-read",
                    dbflux_i18n::t!("notifications.mark_all_read"),
                )
                .icon(AppIcon::Check)
                .on_click(cx.listener(|this, _, _, cx| this.mark_all_notifications_read(cx))),
            );
        }

        for chip_filter in NotificationFilter::ALL {
            popover = popover.filter(
                NotificationFilterChip::new(
                    ElementId::Name(format!("notifications-filter-{}", chip_filter.slug()).into()),
                    filter_label(chip_filter),
                    snapshot.count(chip_filter),
                    chip_filter == filter,
                )
                .on_select(cx.listener(move |this, _, _, cx| {
                    this.notifications.filter = chip_filter;
                    cx.notify();
                })),
            );
        }

        let visible: Vec<&NotificationEntry> = snapshot.filtered(filter).collect();

        for group in NotificationGroup::ALL {
            let entries: Vec<&NotificationEntry> = visible
                .iter()
                .copied()
                .filter(|entry| entry.group == group)
                .collect();

            let count = (group != NotificationGroup::Earlier).then_some(entries.len());
            let mut section = NotificationGroupSection::new(group_label(group), count);

            for entry in entries {
                if let Some(row) = self.notification_row(entry, cx) {
                    section = section.row(row);
                }
            }

            popover = popover.group(section);
        }

        popover
    }

    fn notification_row(
        &self,
        entry: &NotificationEntry,
        cx: &mut Context<Self>,
    ) -> Option<NotificationRow> {
        let now = Utc::now();
        let key = entry.key.clone();

        let row = match &entry.key {
            NotificationKey::Approval(id) => self.approval_row(id, &key, now, cx)?,
            NotificationKey::Error(correlation_id) => {
                let error = self
                    .app_state
                    .read(cx)
                    .notifications
                    .error(*correlation_id)?
                    .clone();
                Self::error_row(&error, &key, now, cx)
            }
            NotificationKey::Update(_) => self.update_row(&key, now, cx)?,
            NotificationKey::Task(task_id) => {
                let task = self
                    .app_state
                    .read(cx)
                    .notifications
                    .task(*task_id)?
                    .clone();
                Self::task_row(&task, &key, now)
            }
        };

        let open_key = key.clone();
        Some(
            row.unread(!entry.read)
                .on_open(cx.listener(move |this, _, window, cx| {
                    this.open_notification(open_key.clone(), window, cx);
                })),
        )
    }

    fn approval_row(
        &self,
        pending_id: &str,
        key: &NotificationKey,
        now: DateTime<Utc>,
        cx: &mut Context<Self>,
    ) -> Option<NotificationRow> {
        let pending = self
            .pending_approvals(cx)
            .into_iter()
            .find(|pending| pending.id == pending_id)?;

        let connection = if pending.connection_id.is_empty() {
            "\u{2014}".to_string()
        } else {
            self.app_state
                .read(cx)
                .profiles()
                .iter()
                .find(|profile| profile.id.to_string() == pending.connection_id)
                .map(|profile| profile.name.clone())
                .unwrap_or_else(|| pending.connection_id.clone())
        };

        let waiting = waiting_label(pending.created_at_epoch_ms, now.timestamp_millis());

        let review_key = key.clone();
        Some(
            NotificationRow::new(
                row_id(key),
                AppIcon::Bot,
                NotificationIconTone::Accent,
                pending.tool_id.clone(),
            )
            .mono_title()
            .chip(pending.classification_label, pending.classification_tone)
            .meta(dbflux_i18n::t!(
                "notifications.approval.meta",
                connection = connection,
                client = pending.actor_id,
                waiting = waiting
            ))
            .action(
                Button::new(
                    action_id("review", key),
                    dbflux_i18n::t!("notifications.approval.review"),
                )
                .icon(AppIcon::ChevronRight)
                .on_click(cx.listener(move |this, _, window, cx| {
                    cx.stop_propagation();
                    this.open_notification(review_key.clone(), window, cx);
                })),
            ),
        )
    }

    fn error_row(
        error: &ErrorNotification,
        key: &NotificationKey,
        now: DateTime<Utc>,
        cx: &mut Context<Self>,
    ) -> NotificationRow {
        let ago = ago_label(error.reported_at, now);
        let meta = match error.cause.as_deref().map(first_line) {
            Some(cause) if !cause.is_empty() => format!("{cause} \u{b7} {ago}"),
            _ => ago,
        };

        let audit_key = key.clone();
        NotificationRow::new(
            row_id(key),
            AppIcon::TriangleAlert,
            NotificationIconTone::Danger,
            error.summary.clone(),
        )
        .meta(meta)
        .meta_code(error.short_correlation_id())
        .action(
            Button::new(
                action_id("view-in-audit", key),
                dbflux_i18n::t!("errors.action.view_in_audit"),
            )
            .icon(AppIcon::FingerprintPattern)
            .on_click(cx.listener(move |this, _, window, cx| {
                cx.stop_propagation();
                this.open_notification(audit_key.clone(), window, cx);
            })),
        )
    }

    fn update_row(
        &self,
        key: &NotificationKey,
        now: DateTime<Utc>,
        cx: &mut Context<Self>,
    ) -> Option<NotificationRow> {
        let state = self.app_state.read(cx);
        let update = state.visible_update()?.clone();
        let checked_at = state.update_check().checked_at;

        let channel =
            dbflux_ui_base::updates::channel_label(dbflux_core::ReleaseChannel::current());
        let current = dbflux_ui_base::updates::running_version_label();
        let meta = match checked_at {
            Some(checked_at) => dbflux_i18n::t!(
                "notifications.update.meta",
                channel = channel,
                current = current,
                ago = ago_label(checked_at, now)
            ),
            None => dbflux_i18n::t!(
                "notifications.update.meta_unchecked",
                channel = channel,
                current = current
            ),
        };

        let is_direct =
            dbflux_app::updates::install_source::current_install_source() == InstallSource::Direct;

        let mut row = NotificationRow::new(
            row_id(key),
            AppIcon::Download,
            NotificationIconTone::Success,
            dbflux_i18n::t!("updates.toast.title", version = update.label),
        )
        .meta(meta);

        if is_direct {
            let install_key = key.clone();
            row = row.action(
                Button::new(
                    action_id("install", key),
                    dbflux_i18n::t!("notifications.update.install"),
                )
                .primary()
                .icon(AppIcon::Download)
                .on_click(cx.listener(move |this, _, window, cx| {
                    cx.stop_propagation();
                    this.install_update(window, cx);
                    this.mark_notification_read(install_key.clone(), cx);
                })),
            );
        }

        let notes_key = key.clone();
        let later_key = key.clone();
        Some(
            row.action(
                Button::new(
                    action_id("whats-new", key),
                    dbflux_i18n::t!("notifications.update.whats_new"),
                )
                .icon(AppIcon::History)
                .on_click(cx.listener(move |this, _, window, cx| {
                    cx.stop_propagation();
                    this.open_notification(notes_key.clone(), window, cx);
                })),
            )
            .action(
                Button::new(
                    action_id("later", key),
                    dbflux_i18n::t!("notifications.update.later"),
                )
                .on_click(cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.update_notification_later(later_key.clone(), cx);
                })),
            ),
        )
    }

    fn task_row(
        task: &TaskNotification,
        key: &NotificationKey,
        now: DateTime<Utc>,
    ) -> NotificationRow {
        let ago = ago_label(task.finished_at, now);
        let meta = match task.details.as_deref().map(first_line) {
            Some(details) if !details.is_empty() => format!("{details} \u{b7} {ago}"),
            _ => ago,
        };

        NotificationRow::new(
            row_id(key),
            task_icon(task.kind),
            NotificationIconTone::Muted,
            dbflux_i18n::t!(
                "notifications.task.finished",
                task = first_line(&task.description)
            ),
        )
        .meta(meta)
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{bell_urgency, first_line};
    use crate::keymap::{Command, ContextId, FocusTarget};
    use crate::ui::document::DocumentIcon;
    use crate::ui::views::workspace::Workspace;
    use dbflux_components::composites::BellUrgency;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::notifications::NotificationUrgency;
    use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
    use gpui::{
        AccessibilityFrame, AppContext as _, Bounds, Entity, FrameObserver, Pixels, TestAppContext,
        VisualTestContext,
    };
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    struct Harness<'a> {
        workspace: Entity<Workspace>,
        app_state: Entity<AppStateEntity>,
        frames: Arc<FrameCapture>,
        window: &'a mut VisualTestContext,
    }

    fn open_workspace(cx: &mut TestAppContext) -> Harness<'_> {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let frames = Arc::new(FrameCapture::default());
        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::default();
        let (_, window) = cx.add_window_view({
            let holder = holder.clone();
            let app_state = app_state.clone();
            let frames = frames.clone();
            move |window, cx| {
                window.observe_frames(&frames);
                let workspace = cx.new(|cx| Workspace::new(app_state, window, cx));
                holder.replace(Some(workspace.clone()));
                gpui_component::Root::new(workspace, window, cx)
            }
        });
        let workspace = holder.borrow().clone().expect("workspace created");
        window.run_until_parked();

        Harness {
            workspace,
            app_state,
            frames,
            window,
        }
    }

    impl Harness<'_> {
        fn is_open(&mut self) -> bool {
            let workspace = self.workspace.clone();
            self.window
                .update(|_, cx| workspace.read(cx).notifications.is_open())
        }

        /// Bounds of the element whose id is `id` in the last drawn frame.
        fn bounds_of(&mut self, id: &str) -> Option<Bounds<Pixels>> {
            self.window.update(|window, _| window.refresh());
            self.window.run_until_parked();

            let frame = self
                .frames
                .0
                .lock()
                .expect("frame capture lock")
                .clone()
                .expect("the window rendered a frame");

            frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| node.bounds())
        }

        /// Draws a frame, which delivers the focus changes of the last
        /// key presses to the elements that listen for them.
        fn redraw(&mut self) {
            self.window.update(|window, _| window.refresh());
            self.window.run_until_parked();
        }

        fn is_rendered(&mut self, id: &str) -> bool {
            self.bounds_of(id).is_some()
        }

        fn click(&mut self, id: &str) {
            let bounds = self
                .bounds_of(id)
                .unwrap_or_else(|| panic!("{id} is not on screen"));
            self.window
                .simulate_click(bounds.center(), gpui::Modifiers::none());
            self.window.run_until_parked();
        }

        /// Reports an error through the user-facing error seam, which also
        /// pushes its toast into the top-right corner.
        fn report_error(&mut self, summary: &str) {
            let error = UserFacingError::new(ErrorKind::Storage, summary);
            self.window.update(|_, cx| report_error(error, cx));
            self.window.run_until_parked();
        }

        fn urgency(&mut self) -> NotificationUrgency {
            let workspace = self.workspace.clone();
            self.window
                .update(|_, cx| workspace.read(cx).notifications_snapshot(cx).urgency())
        }
    }

    #[gpui::test]
    fn the_bell_opens_the_popover_and_escape_closes_it(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        assert!(!harness.is_open());

        harness.click("notifications-bell");
        assert!(harness.is_open());
        assert!(harness.is_rendered("notifications-popover"));
        assert!(harness.is_rendered("notifications-empty"));

        harness.window.simulate_keystrokes("escape");
        harness.window.run_until_parked();
        assert!(!harness.is_open());
        assert!(!harness.is_rendered("notifications-popover"));
    }

    #[gpui::test]
    fn a_click_outside_closes_the_popover(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        harness.click("notifications-bell");
        assert!(harness.is_open());

        harness.click("notifications-backdrop");
        assert!(!harness.is_open());
    }

    #[gpui::test]
    fn opening_the_popover_marks_nothing_read(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        harness.report_error("Export failed");
        assert_eq!(harness.urgency(), NotificationUrgency::Error);

        harness.click("notifications-bell");
        harness.click("notifications-close");

        assert!(!harness.is_open());
        assert_eq!(harness.urgency(), NotificationUrgency::Error);
    }

    #[test]
    fn the_popover_layer_paints_after_the_toast_layer() {
        const { assert!(super::NOTIFICATIONS_LAYER_PRIORITY > super::TOAST_LAYER_PRIORITY) };
    }

    #[gpui::test]
    fn the_popover_covers_a_toast_it_overlaps(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        harness.report_error("Export failed");

        harness.click("notifications-bell");

        let toast = harness
            .bounds_of("toast-host")
            .expect("the error toast stays on screen while the popover is open");
        let popover = harness
            .bounds_of("notifications-popover")
            .expect("the popover is open");
        let close = harness
            .bounds_of("notifications-close")
            .expect("the popover header has its close button");
        assert!(
            toast.intersects(&popover) && toast.contains(&close.center()),
            "the test needs the toast over the popover's close button: toast {toast:?}, close {close:?}"
        );

        // The topmost hitbox under the close button receives the click; it
        // only reaches the popover when the popover paints above the toast.
        harness.click("notifications-close");

        assert!(!harness.is_open());
        assert!(harness.is_rendered("toast-host"));
    }

    #[gpui::test]
    fn mark_all_read_quiets_the_bell(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        harness.report_error("Export failed");

        harness.click("notifications-bell");
        harness.click("notifications-mark-all-read");

        assert_eq!(harness.urgency(), NotificationUrgency::None);
        assert!(harness.is_open(), "marking read keeps the popover open");
    }

    #[gpui::test]
    fn view_in_audit_opens_the_audit_tab(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        let error = UserFacingError::new(ErrorKind::Storage, "Export failed");
        let slug = format!("notification-view-in-audit-error-{}", error.correlation_id);
        harness.window.update(|_, cx| report_error(error, cx));
        harness.window.run_until_parked();

        harness.click("notifications-bell");
        harness.click(&slug);

        let workspace = harness.workspace.clone();
        let active_icon = harness.window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .active_tab()
                .map(|tab| tab.meta_snapshot(cx).icon)
        });
        assert_eq!(active_icon, Some(DocumentIcon::Audit));
        assert!(!harness.is_open());
        assert_eq!(harness.urgency(), NotificationUrgency::None);
    }

    /// The keys the default keymap gives `command` in the global layer, in
    /// GPUI keystroke syntax.
    fn global_keys(command: Command) -> String {
        let keymap = dbflux_ui_base::keymap::effective_keymap();
        let keys = keymap
            .keys_for_command(ContextId::Global, command)
            .unwrap_or_else(|| panic!("{command:?} has a default shortcut"));

        dbflux_ui_base::keymap::gpui_keystrokes(keys)
    }

    /// Typing in the sidebar search leaves letters to the field, and the
    /// global chords still run while it has focus.
    #[gpui::test]
    fn global_chords_run_while_the_sidebar_search_has_focus(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        let workspace = harness.workspace.clone();

        // Focus events reach the search field only in the active window.
        harness.window.update(|window, cx| {
            window.activate_window();
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Sidebar, window, cx)
            })
        });
        harness.window.run_until_parked();

        harness.window.simulate_keystrokes("/");
        harness.redraw();

        let search_focused = |harness: &mut Harness<'_>| {
            let workspace = harness.workspace.clone();
            harness.window.update(|_, cx| {
                workspace
                    .read(cx)
                    .sidebar
                    .read(cx)
                    .search_input_has_focus_state()
            })
        };
        assert!(search_focused(&mut harness), "`/` focuses the search");

        harness.window.simulate_keystrokes("j k");
        harness.redraw();
        assert!(search_focused(&mut harness), "letters stay with the field");

        harness
            .window
            .simulate_keystrokes(&global_keys(Command::ToggleCommandPalette));
        harness.window.run_until_parked();

        let palette_visible = harness
            .window
            .update(|_, cx| workspace.read(cx).command_palette.read(cx).is_visible());
        assert!(
            palette_visible,
            "the palette chord runs from the search field"
        );
    }

    #[cfg(feature = "mcp")]
    #[gpui::test]
    fn review_opens_the_approvals_tab_on_that_request(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        let app_state = harness.app_state.clone();

        let pending = harness.window.update(|_, cx| {
            app_state.update(cx, |state, _| {
                state
                    .request_mcp_execution(
                        "claude-desktop".to_string(),
                        String::new(),
                        "update_records".to_string(),
                        serde_json::from_value(serde_json::json!("write"))
                            .expect("write classification"),
                        serde_json::json!({ "table": "orders" }),
                    )
                    .expect("queue a pending execution")
            })
        });
        assert_eq!(harness.urgency(), NotificationUrgency::Approval);

        harness.click("notifications-bell");
        harness.click(&format!("notification-review-approval-{}", pending.id));

        let workspace = harness.workspace.clone();
        let (active_icon, request_left) = harness.window.update(|_, cx| {
            (
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .active_tab()
                    .map(|tab| tab.meta_snapshot(cx).icon),
                app_state.read(cx).pending_approval_focus.clone(),
            )
        });
        assert_eq!(active_icon, Some(DocumentIcon::McpApprovals));
        assert_eq!(
            request_left, None,
            "the approvals document consumed the request to select it"
        );
        assert!(!harness.is_open());
        assert_eq!(harness.urgency(), NotificationUrgency::None);
    }

    #[gpui::test]
    fn an_available_update_lives_in_the_bell_not_the_status_bar(cx: &mut TestAppContext) {
        let mut harness = open_workspace(cx);
        let app_state = harness.app_state.clone();

        harness.window.update(|_, cx| {
            app_state.update(cx, |state, cx| {
                state.set_update_check(dbflux_app::updates::UpdateCheckState {
                    outcome: Some(dbflux_app::updates::UpdateCheckOutcome::Available(
                        dbflux_app::updates::AvailableUpdate {
                            label: "99.0.0".to_string(),
                            download_url: "https://example.com/download".to_string(),
                            notes_url: "https://example.com/notes".to_string(),
                        },
                    )),
                    checked_at: Some(dbflux_core::chrono::Utc::now()),
                    in_progress: false,
                });
                cx.emit(dbflux_ui_base::AppStateChanged);
            });
        });
        harness.window.run_until_parked();

        assert_eq!(harness.urgency(), NotificationUrgency::Info);
        assert!(harness.is_rendered("status-bar"));
        assert!(harness.is_rendered("notifications-badge"));
        assert!(!harness.is_rendered("status-bar-update-available"));

        harness.click("notifications-bell");
        assert!(harness.is_rendered("notification-row-update-99.0.0"));
        harness.click("notification-later-update-99.0.0");

        assert_eq!(harness.urgency(), NotificationUrgency::None);
        assert!(!harness.is_rendered("notifications-badge"));
    }

    #[test]
    fn bell_badge_follows_the_model_urgency() {
        assert_eq!(bell_urgency(NotificationUrgency::None), BellUrgency::None);
        assert_eq!(bell_urgency(NotificationUrgency::Info), BellUrgency::Info);
        assert_eq!(
            bell_urgency(NotificationUrgency::Approval),
            BellUrgency::Approval
        );
        assert_eq!(bell_urgency(NotificationUrgency::Error), BellUrgency::Error);
    }

    #[test]
    fn first_line_skips_blank_lines() {
        assert_eq!(first_line("\n  No space left  \nmore"), "No space left");
        assert_eq!(first_line(""), "");
    }

    #[test]
    fn notification_copy_resolves_in_every_locale() {
        for key in [
            "notifications.title",
            "notifications.unread",
            "notifications.mark_all_read",
            "notifications.close",
            "notifications.filter.all",
            "notifications.filter.approvals",
            "notifications.filter.errors",
            "notifications.filter.updates",
            "notifications.group.needs_you",
            "notifications.group.updates",
            "notifications.group.earlier",
            "notifications.footer.hint",
            "notifications.footer.clear_read",
            "notifications.empty.title",
            "notifications.empty.message",
            "notifications.approval.meta",
            "notifications.approval.review",
            "notifications.update.meta",
            "notifications.update.meta_unchecked",
            "notifications.update.install",
            "notifications.update.whats_new",
            "notifications.update.later",
            "notifications.task.finished",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty for {locale}");
                assert_ne!(value, key, "{key} missing in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
            }
        }
    }
}
