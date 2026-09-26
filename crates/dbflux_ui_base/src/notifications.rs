//! Session-scoped model of the notifications center behind the title-bar bell.
//!
//! Four sources feed it: MCP executions waiting for approval, errors reported
//! through [`crate::user_error::report_error`], the newer release found by the
//! update check, and background tasks (export, import, migrate, dump
//! analysis) that finished. Approvals and the update are not stored: the
//! caller passes their current state as [`NotificationSources`] every time
//! it asks for a [`NotificationSnapshot`], so an approval disappears as soon
//! as it is decided. Errors and finished tasks are recorded here, because
//! their sources forget them.
//!
//! What the center adds is read state. Opening the popover marks nothing
//! read; opening a row, or "Mark all read", does. "Clear read" drops read
//! errors and tasks and hides read approvals and updates for the rest of the
//! session. Nothing is persisted.

use std::collections::HashSet;

use dbflux_core::chrono::{DateTime, Utc};
use dbflux_core::{TaskId, TaskKind, TaskSnapshot, TaskStatus};
use uuid::Uuid;

use crate::user_error::UserFacingError;

/// Errors kept at most; the oldest is dropped first. The audit log keeps
/// every one of them.
pub const MAX_RECORDED_ERRORS: usize = 200;

/// Finished tasks kept at most; the oldest is dropped first.
pub const MAX_RECORDED_TASKS: usize = 100;

/// Which source a notification comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NotificationKind {
    Approval,
    Error,
    Update,
    Task,
}

/// Identity of one notification, stable for as long as its source keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NotificationKey {
    /// A pending MCP execution, by its pending id.
    Approval(String),
    /// A reported error, by its correlation id.
    Error(Uuid),
    /// An available release, by the label the update check reports.
    Update(String),
    /// A finished background task.
    Task(TaskId),
}

impl NotificationKey {
    pub fn kind(&self) -> NotificationKind {
        match self {
            Self::Approval(_) => NotificationKind::Approval,
            Self::Error(_) => NotificationKind::Error,
            Self::Update(_) => NotificationKind::Update,
            Self::Task(_) => NotificationKind::Task,
        }
    }

    /// A string that names the notification in element ids
    /// (`notification-row-<slug>`): the kind and the source id, with
    /// whitespace replaced so an update label such as `nightly 1a2b3c` stays
    /// one token.
    pub fn slug(&self) -> String {
        let (prefix, id) = match self {
            Self::Approval(id) => ("approval", id.clone()),
            Self::Error(id) => ("error", id.to_string()),
            Self::Update(label) => ("update", label.clone()),
            Self::Task(id) => ("task", id.to_string()),
        };

        let id: String = id
            .chars()
            .map(|character| {
                if character.is_whitespace() {
                    '-'
                } else {
                    character
                }
            })
            .collect();

        format!("{prefix}-{id}")
    }
}

/// How urgent the most urgent unread notification is. It decides the color
/// of the bell's badge; errors outrank approvals, which outrank the rest.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum NotificationUrgency {
    /// Nothing unread: a muted bell without a badge.
    #[default]
    None,
    /// Only updates or finished tasks are unread: a neutral badge.
    Info,
    /// An approval waits: the accent badge.
    Approval,
    /// An error is unread: the danger badge.
    Error,
}

impl NotificationUrgency {
    fn of(kind: NotificationKind) -> Self {
        match kind {
            NotificationKind::Error => Self::Error,
            NotificationKind::Approval => Self::Approval,
            NotificationKind::Update | NotificationKind::Task => Self::Info,
        }
    }
}

/// The filter chips of the popover. `All` lists every notification; the
/// others list one source each, so finished tasks appear under `All` only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NotificationFilter {
    #[default]
    All,
    Approvals,
    Errors,
    Updates,
}

impl NotificationFilter {
    pub const ALL: [Self; 4] = [Self::All, Self::Approvals, Self::Errors, Self::Updates];

    pub fn matches(self, kind: NotificationKind) -> bool {
        match self {
            Self::All => true,
            Self::Approvals => kind == NotificationKind::Approval,
            Self::Errors => kind == NotificationKind::Error,
            Self::Updates => kind == NotificationKind::Update,
        }
    }

    /// Short lowercase name used in element ids (`notifications-filter-all`).
    pub fn slug(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Approvals => "approvals",
            Self::Errors => "errors",
            Self::Updates => "updates",
        }
    }
}

/// The section a notification is listed under, in display order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum NotificationGroup {
    /// Unread approvals and errors.
    NeedsYou,
    /// Unread updates and finished tasks.
    Updates,
    /// Everything already read.
    Earlier,
}

impl NotificationGroup {
    pub const ALL: [Self; 3] = [Self::NeedsYou, Self::Updates, Self::Earlier];
}

/// An error reported through the user-facing error seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorNotification {
    pub correlation_id: Uuid,
    pub summary: String,
    pub cause: Option<String>,
    pub reported_at: DateTime<Utc>,
}

impl ErrorNotification {
    pub fn from_user_error(error: &UserFacingError, reported_at: DateTime<Utc>) -> Self {
        Self {
            correlation_id: error.correlation_id,
            summary: error.summary.clone(),
            cause: error.cause.clone(),
            reported_at,
        }
    }

    /// The first eight characters of the correlation id, as the row shows it.
    pub fn short_correlation_id(&self) -> String {
        self.correlation_id.simple().to_string()[..8].to_string()
    }
}

/// A background task that completed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskNotification {
    pub task_id: TaskId,
    pub kind: TaskKind,
    pub description: String,
    pub details: Option<String>,
    pub finished_at: DateTime<Utc>,
}

/// Whether a finished task of this kind becomes a notification. Only the
/// long-running jobs a user walks away from do; queries, connects and schema
/// loads finish in front of the user. Failures are not listed as tasks:
/// those jobs report them through the error seam, which lists them as errors.
pub fn is_notified_task_kind(kind: TaskKind) -> bool {
    matches!(
        kind,
        TaskKind::Export | TaskKind::Import | TaskKind::Migrate | TaskKind::DumpAnalysis
    )
}

/// An MCP execution currently waiting for a decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveApproval {
    pub id: String,
    pub created_at: DateTime<Utc>,
}

/// The release the update check found and the user has not skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveUpdate {
    pub label: String,
    pub checked_at: DateTime<Utc>,
}

/// The current state of the sources the center does not store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotificationSources {
    pub approvals: Vec<LiveApproval>,
    pub update: Option<LiveUpdate>,
}

/// One listed notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationEntry {
    pub key: NotificationKey,
    pub read: bool,
    pub group: NotificationGroup,
    /// When the notification happened: an approval's request time, an
    /// error's report time, the update check's time, a task's finish time.
    pub at: DateTime<Utc>,
}

impl NotificationEntry {
    pub fn kind(&self) -> NotificationKind {
        self.key.kind()
    }
}

/// Every visible notification, in display order: grouped as
/// [`NotificationGroup::ALL`]; approvals by longest wait, then errors newest
/// first in "Needs you"; the update, then tasks newest first in "Updates";
/// newest first in "Earlier".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotificationSnapshot {
    pub entries: Vec<NotificationEntry>,
}

impl NotificationSnapshot {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn unread_count(&self) -> usize {
        self.entries.iter().filter(|entry| !entry.read).count()
    }

    /// The most urgent unread kind, which colors the bell's badge.
    pub fn urgency(&self) -> NotificationUrgency {
        self.entries
            .iter()
            .filter(|entry| !entry.read)
            .map(|entry| NotificationUrgency::of(entry.kind()))
            .max()
            .unwrap_or_default()
    }

    /// Notifications a filter chip lists, read ones included.
    pub fn count(&self, filter: NotificationFilter) -> usize {
        self.filtered(filter).count()
    }

    pub fn filtered(
        &self,
        filter: NotificationFilter,
    ) -> impl Iterator<Item = &NotificationEntry> + '_ {
        self.entries
            .iter()
            .filter(move |entry| filter.matches(entry.kind()))
    }
}

/// Recorded errors and finished tasks plus the read and hidden state of every
/// notification.
#[derive(Clone, Debug, Default)]
pub struct NotificationCenter {
    errors: Vec<ErrorNotification>,
    tasks: Vec<TaskNotification>,
    /// Tasks already turned into a notification, so a cleared one is not
    /// recorded again while the task manager still lists it.
    seen_tasks: HashSet<TaskId>,
    read: HashSet<NotificationKey>,
    /// Approvals and updates cleared or put off by the user.
    hidden: HashSet<NotificationKey>,
}

impl NotificationCenter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_error(&mut self, error: ErrorNotification) {
        self.errors.push(error);

        if self.errors.len() > MAX_RECORDED_ERRORS {
            let removed = self.errors.remove(0);
            self.read
                .remove(&NotificationKey::Error(removed.correlation_id));
        }
    }

    /// Records the tasks of `tasks` that completed and are of a notified
    /// kind, once each. Returns whether anything was recorded.
    pub fn record_finished_tasks(&mut self, tasks: &[TaskSnapshot], now: DateTime<Utc>) -> bool {
        let mut recorded = false;

        for task in tasks {
            if task.status != TaskStatus::Completed || !is_notified_task_kind(task.kind) {
                continue;
            }

            if !self.seen_tasks.insert(task.id) {
                continue;
            }

            self.tasks.push(TaskNotification {
                task_id: task.id,
                kind: task.kind,
                description: task.description.clone(),
                details: task.details.clone(),
                finished_at: now,
            });
            recorded = true;
        }

        while self.tasks.len() > MAX_RECORDED_TASKS {
            let removed = self.tasks.remove(0);
            self.read.remove(&NotificationKey::Task(removed.task_id));
        }

        recorded
    }

    pub fn error(&self, correlation_id: Uuid) -> Option<&ErrorNotification> {
        self.errors
            .iter()
            .find(|error| error.correlation_id == correlation_id)
    }

    pub fn task(&self, task_id: TaskId) -> Option<&TaskNotification> {
        self.tasks.iter().find(|task| task.task_id == task_id)
    }

    pub fn is_read(&self, key: &NotificationKey) -> bool {
        self.read.contains(key)
    }

    pub fn mark_read(&mut self, key: NotificationKey) {
        self.read.insert(key);
    }

    /// Marks every notification currently listed as read.
    pub fn mark_all_read(&mut self, sources: &NotificationSources) {
        let keys: Vec<NotificationKey> = self
            .snapshot(sources)
            .entries
            .into_iter()
            .map(|entry| entry.key)
            .collect();

        self.read.extend(keys);
    }

    /// Removes read errors and tasks, and hides read approvals and updates
    /// until the session ends.
    pub fn clear_read(&mut self, sources: &NotificationSources) {
        let read_keys: Vec<NotificationKey> = self
            .snapshot(sources)
            .entries
            .into_iter()
            .filter(|entry| entry.read)
            .map(|entry| entry.key)
            .collect();

        for key in read_keys {
            match &key {
                NotificationKey::Error(correlation_id) => {
                    self.errors
                        .retain(|error| error.correlation_id != *correlation_id);
                    self.read.remove(&key);
                }
                NotificationKey::Task(task_id) => {
                    self.tasks.retain(|task| task.task_id != *task_id);
                    self.read.remove(&key);
                }
                NotificationKey::Approval(_) | NotificationKey::Update(_) => {
                    self.hidden.insert(key);
                }
            }
        }
    }

    /// Hides one notification for the rest of the session ("Later" on an
    /// update). Recorded errors and tasks are removed instead.
    pub fn dismiss(&mut self, key: NotificationKey) {
        match &key {
            NotificationKey::Error(correlation_id) => {
                self.errors
                    .retain(|error| error.correlation_id != *correlation_id);
                self.read.remove(&key);
            }
            NotificationKey::Task(task_id) => {
                self.tasks.retain(|task| task.task_id != *task_id);
                self.read.remove(&key);
            }
            NotificationKey::Approval(_) | NotificationKey::Update(_) => {
                self.hidden.insert(key);
            }
        }
    }

    /// The notifications to list now, given the live sources.
    pub fn snapshot(&self, sources: &NotificationSources) -> NotificationSnapshot {
        let mut entries = Vec::new();

        let mut approvals: Vec<&LiveApproval> = sources.approvals.iter().collect();
        approvals.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        for approval in approvals {
            self.push_entry(
                &mut entries,
                NotificationKey::Approval(approval.id.clone()),
                approval.created_at,
            );
        }

        for error in self.errors.iter().rev() {
            self.push_entry(
                &mut entries,
                NotificationKey::Error(error.correlation_id),
                error.reported_at,
            );
        }

        if let Some(update) = &sources.update {
            self.push_entry(
                &mut entries,
                NotificationKey::Update(update.label.clone()),
                update.checked_at,
            );
        }

        for task in self.tasks.iter().rev() {
            self.push_entry(
                &mut entries,
                NotificationKey::Task(task.task_id),
                task.finished_at,
            );
        }

        // Stable sort: the source order above stays the order inside the
        // unread groups; read entries are then ordered newest first.
        entries.sort_by(|left, right| {
            left.group.cmp(&right.group).then_with(|| {
                if left.group == NotificationGroup::Earlier {
                    right.at.cmp(&left.at)
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        });

        NotificationSnapshot { entries }
    }

    fn push_entry(
        &self,
        entries: &mut Vec<NotificationEntry>,
        key: NotificationKey,
        at: DateTime<Utc>,
    ) {
        if self.hidden.contains(&key) {
            return;
        }

        let read = self.read.contains(&key);
        let group = match (read, key.kind()) {
            (true, _) => NotificationGroup::Earlier,
            (false, NotificationKind::Approval | NotificationKind::Error) => {
                NotificationGroup::NeedsYou
            }
            (false, NotificationKind::Update | NotificationKind::Task) => {
                NotificationGroup::Updates
            }
        };

        entries.push(NotificationEntry {
            key,
            read,
            group,
            at,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::TaskManager;
    use dbflux_core::chrono::TimeDelta;

    fn at(minutes_ago: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_800_000_000, 0).expect("valid timestamp")
            - TimeDelta::minutes(minutes_ago)
    }

    fn error(minutes_ago: i64) -> ErrorNotification {
        ErrorNotification {
            correlation_id: Uuid::now_v7(),
            summary: format!("failed {minutes_ago} min ago"),
            cause: None,
            reported_at: at(minutes_ago),
        }
    }

    fn approval(id: &str, minutes_ago: i64) -> LiveApproval {
        LiveApproval {
            id: id.to_string(),
            created_at: at(minutes_ago),
        }
    }

    fn update(label: &str) -> LiveUpdate {
        LiveUpdate {
            label: label.to_string(),
            checked_at: at(30),
        }
    }

    fn keys(snapshot: &NotificationSnapshot) -> Vec<NotificationKey> {
        snapshot
            .entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect()
    }

    fn finished_task(kind: TaskKind, status: TaskStatus) -> TaskSnapshot {
        let mut manager = TaskManager::new();
        let (id, _token) = manager.start(kind, "Migrate 4 tables");
        let mut snapshot = manager.get(id).expect("started task");
        snapshot.status = status;
        snapshot
    }

    #[test]
    fn nothing_listed_is_the_empty_state() {
        let center = NotificationCenter::new();
        let snapshot = center.snapshot(&NotificationSources::default());

        assert!(snapshot.is_empty());
        assert_eq!(snapshot.unread_count(), 0);
        assert_eq!(snapshot.urgency(), NotificationUrgency::None);
    }

    #[test]
    fn unread_count_covers_every_source() {
        let mut center = NotificationCenter::new();
        center.record_error(error(12));
        center.record_finished_tasks(
            &[finished_task(TaskKind::Migrate, TaskStatus::Completed)],
            at(5),
        );

        let sources = NotificationSources {
            approvals: vec![approval("a", 4), approval("b", 1)],
            update: Some(update("0.8.1")),
        };
        let snapshot = center.snapshot(&sources);

        assert_eq!(snapshot.unread_count(), 5);
        assert_eq!(snapshot.count(NotificationFilter::All), 5);
        assert_eq!(snapshot.count(NotificationFilter::Approvals), 2);
        assert_eq!(snapshot.count(NotificationFilter::Errors), 1);
        assert_eq!(snapshot.count(NotificationFilter::Updates), 1);
    }

    #[test]
    fn urgency_follows_the_most_urgent_unread_kind() {
        let mut center = NotificationCenter::new();
        let mut sources = NotificationSources {
            approvals: Vec::new(),
            update: Some(update("0.8.1")),
        };
        assert_eq!(
            center.snapshot(&sources).urgency(),
            NotificationUrgency::Info
        );

        sources.approvals.push(approval("a", 3));
        assert_eq!(
            center.snapshot(&sources).urgency(),
            NotificationUrgency::Approval
        );

        let reported = error(1);
        let correlation_id = reported.correlation_id;
        center.record_error(reported);
        assert_eq!(
            center.snapshot(&sources).urgency(),
            NotificationUrgency::Error
        );

        center.mark_read(NotificationKey::Error(correlation_id));
        assert_eq!(
            center.snapshot(&sources).urgency(),
            NotificationUrgency::Approval
        );
    }

    #[test]
    fn groups_order_needs_you_then_updates_then_earlier() {
        let mut center = NotificationCenter::new();
        let older_error = error(20);
        let newer_error = error(2);
        let (older_id, newer_id) = (older_error.correlation_id, newer_error.correlation_id);
        center.record_error(older_error);
        center.record_error(newer_error);

        let task = finished_task(TaskKind::Export, TaskStatus::Completed);
        let task_id = task.id;
        center.record_finished_tasks(&[task], at(60));
        center.mark_read(NotificationKey::Task(task_id));

        let sources = NotificationSources {
            approvals: vec![approval("recent", 1), approval("waiting", 4)],
            update: Some(update("0.8.1")),
        };
        let snapshot = center.snapshot(&sources);

        assert_eq!(
            keys(&snapshot),
            vec![
                NotificationKey::Approval("waiting".into()),
                NotificationKey::Approval("recent".into()),
                NotificationKey::Error(newer_id),
                NotificationKey::Error(older_id),
                NotificationKey::Update("0.8.1".into()),
                NotificationKey::Task(task_id),
            ]
        );

        let groups: Vec<NotificationGroup> =
            snapshot.entries.iter().map(|entry| entry.group).collect();
        assert_eq!(
            groups,
            vec![
                NotificationGroup::NeedsYou,
                NotificationGroup::NeedsYou,
                NotificationGroup::NeedsYou,
                NotificationGroup::NeedsYou,
                NotificationGroup::Updates,
                NotificationGroup::Earlier,
            ]
        );
    }

    #[test]
    fn read_entries_move_to_earlier_newest_first() {
        let mut center = NotificationCenter::new();
        let old = error(90);
        let recent = error(10);
        let (old_id, recent_id) = (old.correlation_id, recent.correlation_id);
        center.record_error(old);
        center.record_error(recent);
        center.mark_read(NotificationKey::Error(old_id));
        center.mark_read(NotificationKey::Error(recent_id));
        center.mark_read(NotificationKey::Approval("a".into()));

        let sources = NotificationSources {
            approvals: vec![approval("a", 40)],
            update: None,
        };
        let snapshot = center.snapshot(&sources);

        assert_eq!(
            keys(&snapshot),
            vec![
                NotificationKey::Error(recent_id),
                NotificationKey::Approval("a".into()),
                NotificationKey::Error(old_id),
            ]
        );
        assert!(
            snapshot
                .entries
                .iter()
                .all(|entry| entry.group == NotificationGroup::Earlier)
        );
        assert_eq!(snapshot.unread_count(), 0);
    }

    #[test]
    fn mark_all_read_reads_everything_listed() {
        let mut center = NotificationCenter::new();
        center.record_error(error(3));
        let sources = NotificationSources {
            approvals: vec![approval("a", 2)],
            update: Some(update("0.8.1")),
        };

        center.mark_all_read(&sources);
        let snapshot = center.snapshot(&sources);

        assert_eq!(snapshot.unread_count(), 0);
        assert_eq!(snapshot.entries.len(), 3);
        assert_eq!(snapshot.urgency(), NotificationUrgency::None);
    }

    #[test]
    fn clear_read_removes_only_read_entries() {
        let mut center = NotificationCenter::new();
        let read_error = error(8);
        let unread_error = error(2);
        let (read_id, unread_id) = (read_error.correlation_id, unread_error.correlation_id);
        center.record_error(read_error);
        center.record_error(unread_error);
        center.mark_read(NotificationKey::Error(read_id));
        center.mark_read(NotificationKey::Approval("decided-later".into()));

        let sources = NotificationSources {
            approvals: vec![approval("decided-later", 6), approval("fresh", 1)],
            update: None,
        };
        center.clear_read(&sources);
        let snapshot = center.snapshot(&sources);

        assert_eq!(
            keys(&snapshot),
            vec![
                NotificationKey::Approval("fresh".into()),
                NotificationKey::Error(unread_id),
            ]
        );
        assert!(center.error(read_id).is_none());
    }

    #[test]
    fn dismissing_an_update_hides_it_for_the_session() {
        let mut center = NotificationCenter::new();
        let sources = NotificationSources {
            approvals: Vec::new(),
            update: Some(update("0.8.1")),
        };

        center.dismiss(NotificationKey::Update("0.8.1".into()));

        assert!(center.snapshot(&sources).is_empty());

        let newer = NotificationSources {
            approvals: Vec::new(),
            update: Some(update("0.8.2")),
        };
        assert_eq!(center.snapshot(&newer).unread_count(), 1);
    }

    #[test]
    fn only_completed_long_running_tasks_are_recorded_once() {
        let mut center = NotificationCenter::new();
        let migrate = finished_task(TaskKind::Migrate, TaskStatus::Completed);
        let failed_export = finished_task(TaskKind::Export, TaskStatus::Failed("disk full".into()));
        let query = finished_task(TaskKind::Query, TaskStatus::Completed);
        let running_import = finished_task(TaskKind::Import, TaskStatus::Running);
        let tasks = [migrate.clone(), failed_export, query, running_import];

        assert!(center.record_finished_tasks(&tasks, at(0)));
        assert!(!center.record_finished_tasks(&tasks, at(0)));

        let snapshot = center.snapshot(&NotificationSources::default());
        assert_eq!(keys(&snapshot), vec![NotificationKey::Task(migrate.id)]);

        center.mark_read(NotificationKey::Task(migrate.id));
        center.clear_read(&NotificationSources::default());
        assert!(!center.record_finished_tasks(&tasks, at(0)));
        assert!(center.snapshot(&NotificationSources::default()).is_empty());
    }

    #[test]
    fn recorded_errors_are_capped() {
        let mut center = NotificationCenter::new();
        let first = error(500);
        let first_id = first.correlation_id;
        center.record_error(first);

        for minutes in 0..MAX_RECORDED_ERRORS as i64 {
            center.record_error(error(minutes));
        }

        assert!(center.error(first_id).is_none());
        assert_eq!(
            center
                .snapshot(&NotificationSources::default())
                .entries
                .len(),
            MAX_RECORDED_ERRORS
        );
    }

    #[test]
    fn slugs_are_single_tokens() {
        assert_eq!(
            NotificationKey::Update("nightly 1a2b3c".into()).slug(),
            "update-nightly-1a2b3c"
        );
        assert_eq!(NotificationKey::Approval("42".into()).slug(), "approval-42");
    }

    #[test]
    fn short_correlation_id_is_eight_hex_characters() {
        let notification = ErrorNotification {
            correlation_id: Uuid::parse_str("019f3c7a-0000-7000-8000-000000000000")
                .expect("valid uuid"),
            summary: "Export failed".into(),
            cause: None,
            reported_at: at(0),
        };

        assert_eq!(notification.short_correlation_id(), "019f3c7a");
    }
}
