use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use uuid::Uuid;

use crate::HookPhase;

const MAX_TASK_DETAILS_BYTES: usize = 4 * 1024 * 1024;
const TASK_DETAILS_TRUNCATED_NOTICE: &str = "\n[output truncated]\n";

pub type TaskId = Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTarget {
    pub profile_id: Uuid,
    pub database: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Query,
    Connect,
    Disconnect,
    Hook {
        phase: HookPhase,
    },
    SwitchDatabase,
    /// Lazy schema loading for a single database (MySQL/MariaDB).
    LoadSchema,
    SchemaRefresh,
    SchemaDrop,
    Export,
    Import,
    Migrate,
    KeyScan,
    KeyGet,
    KeyMutation,
    /// Offline analysis of a driver's native dump/export format (e.g. a Redis
    /// RDB file), run without a live connection.
    DumpAnalysis,
}

impl TaskKind {
    pub fn label(&self) -> &'static str {
        match self {
            TaskKind::Query => "Query",
            TaskKind::Connect => "Connect",
            TaskKind::Disconnect => "Disconnect",
            TaskKind::Hook { phase } => phase.label(),
            TaskKind::SwitchDatabase => "Switch Database",
            TaskKind::LoadSchema => "Load Schema",
            TaskKind::SchemaRefresh => "Schema Refresh",
            TaskKind::SchemaDrop => "Schema Drop",
            TaskKind::Export => "Export",
            TaskKind::Import => "Import",
            TaskKind::Migrate => "Migrate",
            TaskKind::KeyScan => "Key Scan",
            TaskKind::KeyGet => "Key Get",
            TaskKind::KeyMutation => "Key Mutation",
            TaskKind::DumpAnalysis => "Dump Analysis",
        }
    }

    /// Whether a user cancel request can stop the work behind a task of this kind.
    ///
    /// Key-value scans, reads, and mutations run as one blocking
    /// `KeyValueApi` call that takes no cancel token, and no key-value driver
    /// exposes a cancel path for them. Cancelling such a task could only mark
    /// it cancelled while the driver keeps working (and a mutation still
    /// commits), so these kinds report `false` and the UI hides their cancel
    /// control. Internal supersede logic may still cancel them to discard a
    /// stale result.
    pub fn supports_cancellation(&self) -> bool {
        !matches!(
            self,
            TaskKind::KeyScan | TaskKind::KeyGet | TaskKind::KeyMutation
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, TaskStatus::Running)
    }
}

#[derive(Clone)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Returns `Err(PipelineError::cancelled())` if cancellation was requested.
    ///
    /// Insert between pipeline stages to allow early exit.
    pub fn check_pipeline(&self) -> Result<(), crate::pipeline::PipelineError> {
        if self.is_cancelled() {
            Err(crate::pipeline::PipelineError::cancelled())
        } else {
            Ok(())
        }
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Task {
    pub id: TaskId,
    pub kind: TaskKind,
    pub description: String,
    pub status: TaskStatus,
    pub started_at: Instant,
    pub completed_at: Option<Instant>,
    pub progress: Option<f32>,
    pub details: Option<String>,
    pub profile_id: Option<Uuid>,
    pub target: Option<TaskTarget>,
    /// Full text of the query a `TaskKind::Query` task runs, when the
    /// description had to shorten it. Shared, so snapshots taken on every
    /// render do not copy a large script.
    pub query_text: Option<Arc<str>>,
    cancel_token: CancelToken,
}

impl Task {
    pub fn elapsed(&self) -> std::time::Duration {
        match self.completed_at {
            Some(completed) => completed.duration_since(self.started_at),
            None => self.started_at.elapsed(),
        }
    }

    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel_token
    }

    pub fn is_cancellable(&self) -> bool {
        self.status == TaskStatus::Running && self.kind.supports_cancellation()
    }
}

#[derive(Clone)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub kind: TaskKind,
    pub description: String,
    pub status: TaskStatus,
    pub elapsed_secs: f64,
    pub progress: Option<f32>,
    pub details: Option<String>,
    pub is_cancellable: bool,
    pub profile_id: Option<Uuid>,
    pub target: Option<TaskTarget>,
    pub query_text: Option<Arc<str>>,
}

impl From<&Task> for TaskSnapshot {
    fn from(task: &Task) -> Self {
        Self {
            id: task.id,
            kind: task.kind,
            description: task.description.clone(),
            status: task.status.clone(),
            elapsed_secs: task.elapsed().as_secs_f64(),
            progress: task.progress,
            details: task.details.clone(),
            is_cancellable: task.is_cancellable(),
            profile_id: task.profile_id,
            target: task.target.clone(),
            query_text: task.query_text.clone(),
        }
    }
}

#[derive(Default)]
pub struct TaskManager {
    tasks: HashMap<TaskId, Task>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub fn start(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
    ) -> (TaskId, CancelToken) {
        self.start_for_target(kind, description, None)
    }

    pub fn start_for_target(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        target: Option<TaskTarget>,
    ) -> (TaskId, CancelToken) {
        let id = TaskId::new_v4();
        let cancel_token = CancelToken::new();
        let profile_id = target.as_ref().map(|target| target.profile_id);

        let task = Task {
            id,
            kind,
            description: description.into(),
            status: TaskStatus::Running,
            started_at: Instant::now(),
            completed_at: None,
            progress: None,
            details: None,
            profile_id,
            target,
            query_text: None,
            cancel_token: cancel_token.clone(),
        };

        self.tasks.insert(id, task);
        (id, cancel_token)
    }

    /// Records the full query text of a task whose description is a
    /// shortened label, so prompts about the running query can show all of it.
    pub fn set_query_text(&mut self, id: TaskId, query_text: impl Into<Arc<str>>) {
        if let Some(task) = self.tasks.get_mut(&id) {
            task.query_text = Some(query_text.into());
        }
    }

    pub fn complete(&mut self, id: TaskId) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.status = TaskStatus::Completed;
            task.completed_at = Some(Instant::now());
        }
    }

    pub fn complete_with_details(&mut self, id: TaskId, details: impl Into<String>) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.status = TaskStatus::Completed;
            task.completed_at = Some(Instant::now());

            let details = details.into();
            task.details = if details.trim().is_empty() {
                None
            } else {
                Some(details)
            };
        }
    }

    pub fn append_details(&mut self, id: TaskId, chunk: impl AsRef<str>) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };

        if task.status.is_terminal() {
            return;
        }

        let chunk = chunk.as_ref();
        if chunk.is_empty() {
            return;
        }

        let details = task.details.get_or_insert_with(String::new);
        append_with_limit(
            details,
            chunk,
            MAX_TASK_DETAILS_BYTES,
            TASK_DETAILS_TRUNCATED_NOTICE,
        );
    }

    pub fn fail(&mut self, id: TaskId, error: impl Into<String>) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.status = TaskStatus::Failed(error.into());
            task.completed_at = Some(Instant::now());
        }
    }

    pub fn fail_with_details(
        &mut self,
        id: TaskId,
        error: impl Into<String>,
        details: impl Into<String>,
    ) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.status = TaskStatus::Failed(error.into());
            task.completed_at = Some(Instant::now());

            let details = details.into();
            task.details = if details.trim().is_empty() {
                None
            } else {
                Some(details)
            };
        }
    }

    pub fn cancel(&mut self, id: TaskId) -> bool {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.cancel_token.cancel();
            task.status = TaskStatus::Cancelled;
            task.completed_at = Some(Instant::now());
            return true;
        }
        false
    }

    /// Cancel all running tasks.
    ///
    /// Returns the number of tasks that were actually cancelled.
    pub fn cancel_all(&mut self) -> usize {
        let running_ids: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.status == TaskStatus::Running)
            .map(|(id, _)| *id)
            .collect();

        let mut cancelled_count = 0;
        for id in running_ids {
            if self.cancel(id) {
                cancelled_count += 1;
            }
        }
        cancelled_count
    }

    pub fn update_progress(&mut self, id: TaskId, progress: f32) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.progress = Some(progress.clamp(0.0, 1.0));
        }
    }

    pub fn get(&self, id: TaskId) -> Option<TaskSnapshot> {
        self.tasks.get(&id).map(TaskSnapshot::from)
    }

    pub fn running_tasks(&self) -> Vec<TaskSnapshot> {
        self.tasks
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .map(TaskSnapshot::from)
            .collect()
    }

    pub fn recent_tasks(&self, limit: usize) -> Vec<TaskSnapshot> {
        let mut tasks: Vec<_> = self.tasks.values().collect();
        tasks.sort_by_key(|t| std::cmp::Reverse(t.started_at));
        tasks
            .into_iter()
            .take(limit)
            .map(TaskSnapshot::from)
            .collect()
    }

    pub fn active_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .count()
    }

    /// Count running background tasks (non-interactive: Connect, LoadSchema,
    /// SchemaRefresh, SwitchDatabase, Disconnect). Query and Export tasks are
    /// excluded so manual "Run Query" is never throttled.
    pub fn background_task_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|t| {
                t.status == TaskStatus::Running
                    && matches!(
                        t.kind,
                        TaskKind::Connect
                            | TaskKind::Disconnect
                            | TaskKind::Hook { .. }
                            | TaskKind::SwitchDatabase
                            | TaskKind::LoadSchema
                            | TaskKind::SchemaRefresh
                            | TaskKind::SchemaDrop
                            | TaskKind::KeyScan
                    )
            })
            .count()
    }

    pub fn has_running_tasks(&self) -> bool {
        self.tasks.values().any(|t| t.status == TaskStatus::Running)
    }

    /// Removes completed and cancelled tasks that finished at least
    /// `max_age_secs` ago.
    ///
    /// Failed tasks are never removed here, so their error output stays
    /// available until the user dismisses them with [`TaskManager::remove`].
    pub fn cleanup_completed(&mut self, max_age_secs: u64) {
        let now = Instant::now();
        self.tasks.retain(|_, task| {
            if matches!(task.status, TaskStatus::Failed(_)) {
                return true;
            }

            if task.status.is_terminal()
                && let Some(completed) = task.completed_at
            {
                return now.duration_since(completed).as_secs() < max_age_secs;
            }
            true
        });
    }

    pub fn remove(&mut self, id: TaskId) {
        self.tasks.remove(&id);
    }

    pub fn current_status_message(&self) -> Option<String> {
        let running: Vec<_> = self
            .tasks
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .collect();

        match running.len() {
            0 => None,
            1 => Some(running[0].description.clone()),
            n => Some(format!("{} tasks running...", n)),
        }
    }

    pub fn last_completed_task(&self) -> Option<TaskSnapshot> {
        self.tasks
            .values()
            .filter(|t| t.status.is_terminal())
            .filter(|t| t.completed_at.is_some())
            .max_by_key(|t| t.completed_at)
            .map(TaskSnapshot::from)
    }
}

fn append_with_limit(target: &mut String, chunk: &str, max_bytes: usize, truncated_notice: &str) {
    if target.contains(truncated_notice) {
        return;
    }

    let remaining = max_bytes.saturating_sub(target.len());

    if remaining == 0 {
        return;
    }

    if chunk.len() <= remaining {
        target.push_str(chunk);
        return;
    }

    if remaining > truncated_notice.len() {
        target.push_str(&safe_prefix_by_bytes(
            chunk,
            remaining - truncated_notice.len(),
        ));
        target.push_str(truncated_notice);
    } else {
        target.push_str(&safe_prefix_by_bytes(truncated_notice, remaining));
    }
}

fn safe_prefix_by_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }

    let mut safe_end = 0;

    for (index, ch) in input.char_indices() {
        let next = index + ch.len_utf8();

        if next > max_bytes {
            break;
        }

        safe_end = next;
    }

    input[..safe_end].to_string()
}

// ---------------------------------------------------------------------------
// TaskSlot — single-occupancy slot that auto-cancels the previous occupant
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct TaskSlot {
    active: Option<(TaskId, CancelToken)>,
}

impl TaskSlot {
    pub fn new() -> Self {
        Self { active: None }
    }

    /// Store a new task pair, cancelling the previous token if present.
    /// Returns the old `TaskId` if one was replaced.
    pub fn start(&mut self, id: TaskId, cancel_token: CancelToken) -> Option<TaskId> {
        let previous = self.active.take().map(|(old_id, old_token)| {
            old_token.cancel();
            old_id
        });

        self.active = Some((id, cancel_token));
        previous
    }

    /// Cancel the current occupant and clear the slot.
    /// Returns the cancelled `TaskId` if the slot was occupied.
    pub fn cancel(&mut self) -> Option<TaskId> {
        self.active.take().map(|(id, token)| {
            token.cancel();
            id
        })
    }

    /// Take the current occupant without cancelling.
    pub fn take(&mut self) -> Option<(TaskId, CancelToken)> {
        self.active.take()
    }

    /// Take the current occupant only if its `TaskId` matches.
    ///
    /// Returns `None` if the slot is empty or holds a different task.
    /// This prevents stale callbacks from draining a newer task's entry.
    pub fn take_if(&mut self, expected: TaskId) -> Option<(TaskId, CancelToken)> {
        if self.active.as_ref().is_some_and(|(id, _)| *id == expected) {
            self.active.take()
        } else {
            None
        }
    }

    /// Returns `true` when the slot holds a non-cancelled task.
    pub fn is_active(&self) -> bool {
        self.active
            .as_ref()
            .is_some_and(|(_, token)| !token.is_cancelled())
    }

    pub fn active_token(&self) -> Option<&CancelToken> {
        self.active.as_ref().map(|(_, token)| token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_and_migrate_labels() {
        assert_eq!(TaskKind::Import.label(), "Import");
        assert_eq!(TaskKind::Migrate.label(), "Migrate");
    }

    #[test]
    fn every_task_kind_has_a_non_empty_label() {
        let kinds = [
            TaskKind::Query,
            TaskKind::Connect,
            TaskKind::Disconnect,
            TaskKind::SwitchDatabase,
            TaskKind::LoadSchema,
            TaskKind::SchemaRefresh,
            TaskKind::SchemaDrop,
            TaskKind::Export,
            TaskKind::Import,
            TaskKind::Migrate,
            TaskKind::KeyScan,
            TaskKind::KeyGet,
            TaskKind::KeyMutation,
            TaskKind::DumpAnalysis,
        ];

        for kind in kinds {
            assert!(!kind.label().is_empty());
        }
    }

    #[test]
    fn key_value_tasks_are_not_cancellable_while_running() {
        let mut manager = TaskManager::new();

        for kind in [TaskKind::KeyScan, TaskKind::KeyGet, TaskKind::KeyMutation] {
            let (id, _token) = manager.start(kind, kind.label());
            let snapshot = manager.get(id).expect("task exists");

            assert_eq!(snapshot.status, TaskStatus::Running);
            assert!(
                !snapshot.is_cancellable,
                "{} must not offer a cancel that cannot stop the driver call",
                kind.label()
            );
        }
    }

    #[test]
    fn running_query_task_is_cancellable_and_finished_one_is_not() {
        let mut manager = TaskManager::new();
        let (id, _token) = manager.start(TaskKind::Query, "SELECT 1");

        assert!(manager.get(id).expect("task exists").is_cancellable);

        manager.complete(id);

        assert!(!manager.get(id).expect("task exists").is_cancellable);
    }

    #[test]
    fn query_text_reaches_the_snapshot_and_leaves_the_description_alone() {
        let mut manager = TaskManager::new();
        let full_query =
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c;";
        let (id, _token) = manager.start(TaskKind::Query, "WITH RECURSIVE c(x) AS ...");

        assert!(manager.get(id).expect("task exists").query_text.is_none());

        manager.set_query_text(id, full_query);

        let snapshot = manager.get(id).expect("task exists");
        assert_eq!(snapshot.query_text.as_deref(), Some(full_query));
        assert_eq!(snapshot.description, "WITH RECURSIVE c(x) AS ...");
    }

    #[test]
    fn cleanup_keeps_failed_tasks_until_they_are_removed() {
        let mut manager = TaskManager::new();
        let (failed_id, _token) = manager.start(TaskKind::KeyScan, "SCAN 0");
        manager.fail(failed_id, "connection reset");

        manager.cleanup_completed(0);

        let failed = manager
            .get(failed_id)
            .expect("failed task survives cleanup");
        assert_eq!(
            failed.status,
            TaskStatus::Failed("connection reset".to_string())
        );

        manager.remove(failed_id);

        assert!(manager.get(failed_id).is_none());
    }

    #[test]
    fn cleanup_removes_completed_and_cancelled_tasks_past_the_retention_window() {
        let mut manager = TaskManager::new();
        let (completed_id, _token) = manager.start(TaskKind::Query, "SELECT 1");
        let (cancelled_id, _token) = manager.start(TaskKind::Export, "Export");
        let (running_id, _token) = manager.start(TaskKind::LoadSchema, "Load");
        manager.complete(completed_id);
        manager.cancel(cancelled_id);

        manager.cleanup_completed(60);

        assert!(manager.get(completed_id).is_some());
        assert!(manager.get(cancelled_id).is_some());

        manager.cleanup_completed(0);

        assert!(manager.get(completed_id).is_none());
        assert!(manager.get(cancelled_id).is_none());
        assert!(manager.get(running_id).is_some());
    }
}
