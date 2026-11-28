use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Instant;

use uuid::Uuid;

use crate::HookPhase;

mod dedup;
mod hierarchy;

pub use dedup::TaskBusy;

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
    /// A connection probe run from the connection form. Interactive like a
    /// query, so it does not count toward the background task cap.
    TestConnection,
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
            TaskKind::TestConnection => "Test Connection",
        }
    }

    /// Whether a user cancel request can stop the work behind a task of this kind.
    ///
    /// Key scans pass their token to `KeyValueApi::scan_keys_with_cancel`,
    /// which stops between server round trips. Key reads and mutations run as
    /// one blocking `KeyValueApi` call with no cancel path: cancelling one
    /// could only mark it cancelled while the driver keeps working (and a
    /// mutation still commits), so these kinds report `false` and the UI hides
    /// their cancel control. Internal supersede logic may still cancel them to
    /// discard a stale result.
    pub fn supports_cancellation(&self) -> bool {
        !matches!(self, TaskKind::KeyGet | TaskKind::KeyMutation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    /// Registered ahead of time (a pipeline stage that has not begun). Not
    /// terminal, but not in flight either: it is not counted as running and
    /// does nothing until `TaskManager::activate` moves it to `Running`.
    Pending,
    Running,
    /// A cancel was requested and the work has not confirmed that it
    /// stopped. Not terminal: the worker still decides the final outcome.
    Cancelling,
    Completed,
    Failed(String),
    Cancelled,
    /// A `Pending` task that never started because its parent was cancelled.
    /// Shown as "Not started".
    Skipped,
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(
            self,
            TaskStatus::Pending | TaskStatus::Running | TaskStatus::Cancelling
        )
    }

    /// Whether work is being done for the task: `Running` or `Cancelling`.
    /// `Pending` is not in flight.
    pub fn is_in_flight(&self) -> bool {
        matches!(self, TaskStatus::Running | TaskStatus::Cancelling)
    }
}

/// How a cancel request settles the task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CancelPolicy {
    /// The task becomes `Cancelled` the moment cancel is requested.
    #[default]
    Immediate,
    /// The task becomes `Cancelling` and turns `Cancelled` only when the
    /// worker confirms with `TaskManager::acknowledge_cancelled`, or when its
    /// cancel hook reports that the work stopped.
    WorkerAcknowledged,
}

/// What a task's cancel hook achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The work is already stopped.
    Stopped,
    /// A stop was asked for; the worker will confirm.
    Requested,
    /// The work cannot be stopped.
    NotSupported { reason: String },
    /// The stop could not be delivered this time. The task stays as it was,
    /// cancellable, so the user can try again.
    DeliveryFailed { reason: String },
}

/// Cancel action of a task, run by `TaskManager::request_cancel` while the
/// manager is borrowed: it must return without blocking.
pub type CancelHook = Arc<dyn Fn() -> CancelOutcome + Send + Sync>;

/// Result of asking a task to cancel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelRequestOutcome {
    /// The work stopped; the task is `Cancelled`.
    Cancelled,
    /// The request was delivered; the task is `Cancelling` until the worker
    /// confirms.
    Cancelling,
    /// An earlier request is still waiting for the worker.
    AlreadyCancelling,
    /// The task cannot be cancelled and kept its status.
    Refused { reason: String },
    /// The task is unknown or already finished.
    NotRunning,
}

/// Counts of how `TaskManager::cancel_all_detailed` settled each running task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CancelAllSummary {
    pub cancelled: usize,
    pub cancelling: usize,
    pub refused: usize,
}

/// Parameters of a task started with `TaskManager::start_handle`.
pub struct TaskRequest {
    kind: TaskKind,
    description: String,
    target: Option<TaskTarget>,
    policy: CancelPolicy,
    cancel_hook: Option<CancelHook>,
    parent: Option<TaskId>,
    pending: bool,
    supersede_key: Option<String>,
    target_label: Option<String>,
}

impl TaskRequest {
    pub fn new(kind: TaskKind, description: impl Into<String>) -> Self {
        Self {
            kind,
            description: description.into(),
            target: None,
            policy: CancelPolicy::default(),
            cancel_hook: None,
            parent: None,
            pending: false,
            supersede_key: None,
            target_label: None,
        }
    }

    pub fn target(mut self, target: TaskTarget) -> Self {
        self.target = Some(target);
        self
    }

    pub fn policy(mut self, policy: CancelPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Only consulted for `CancelPolicy::WorkerAcknowledged` tasks.
    pub fn cancel_hook(mut self, hook: CancelHook) -> Self {
        self.cancel_hook = Some(hook);
        self
    }

    /// Makes the task a child of `parent`: cancelling the parent cancels it,
    /// even through a parent that already finished.
    ///
    /// A child registered under a parent that is `Cancelling`, `Cancelled` or
    /// `Skipped` inherits that cancellation: a `Pending` child is created
    /// `Skipped` and can never be activated, and a `Running` child goes
    /// through `request_cancel` right away, so its status is the honest
    /// outcome of its own policy. Under a running parent nothing changes.
    pub fn parent(mut self, parent: TaskId) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Registers the task as `TaskStatus::Pending` until a worker calls
    /// `TaskManager::activate`.
    pub fn pending(mut self) -> Self {
        self.pending = true;
        self
    }

    /// Starting this task requests cancel on every non-terminal task that
    /// carries the same key. See `TaskManager::start_handle`.
    pub fn supersede_key(mut self, key: impl Into<String>) -> Self {
        self.supersede_key = Some(key.into());
        self
    }

    /// Name the panel shows for the task's target when it cannot be looked
    /// up, such as a connection that is being tested before it is saved.
    pub fn target_label(mut self, label: impl Into<String>) -> Self {
        self.target_label = Some(label.into());
        self
    }
}

/// Identity and cancel token of a started task. The `TaskManager` keeps
/// owning the task state.
#[derive(Clone)]
pub struct TaskHandle {
    id: TaskId,
    token: CancelToken,
    superseded: Vec<TaskId>,
}

impl TaskHandle {
    /// Older tasks this start asked to cancel because of a matching
    /// `supersede_key`. Each one's own status says whether it already stopped.
    pub fn superseded(&self) -> &[TaskId] {
        &self.superseded
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn token(&self) -> &CancelToken {
        &self.token
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<CancelTokenInner>,
}

struct CancelTokenInner {
    cancelled: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancelTokenInner {
                cancelled: AtomicBool::new(false),
                wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);

        let wakers = std::mem::take(
            &mut *self
                .inner
                .wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for waker in wakers {
            waker.wake();
        }
    }

    /// Resolves once cancellation is requested. Runtime-agnostic, so it can
    /// be raced against work running on the GPUI background executor.
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let token = self.clone();
        std::future::poll_fn(move |context| {
            if token.is_cancelled() {
                return Poll::Ready(());
            }

            {
                let mut wakers = token
                    .inner
                    .wakers
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !wakers.iter().any(|waker| waker.will_wake(context.waker())) {
                    wakers.push(context.waker().clone());
                }
            }

            // `cancel` may have run between the first check and registering
            // the waker; checking again keeps that wake-up from being lost.
            if token.is_cancelled() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
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
    pub parent_id: Option<TaskId>,
    /// Full text of the query a `TaskKind::Query` task runs, when the
    /// description had to shorten it. Shared, so snapshots taken on every
    /// render do not copy a large script.
    pub query_text: Option<Arc<str>>,
    /// Why the task cannot be cancelled, shown next to the disabled cancel
    /// control. Set by a point of no return or a hook reporting
    /// `CancelOutcome::NotSupported`.
    pub cancel_unavailable_reason: Option<String>,
    /// Display name of the target when it cannot be looked up by id.
    pub target_label: Option<String>,
    cancel_token: CancelToken,
    /// Cleared by `TaskManager::mark_uncancellable` for work that never polls
    /// its token, such as a write already sent to the server.
    accepts_cancel: bool,
    policy: CancelPolicy,
    cancel_hook: Option<CancelHook>,
    supersede_key: Option<String>,
    /// Start order, stable where two `Instant`s tie.
    sequence: u64,
    /// Set when a cancel request on a parent could not settle it because a
    /// child was still in flight: the parent turns `Cancelled` on its own
    /// once every child stopped.
    settles_with_children: bool,
    /// What a task that settles with its children reads once they stopped:
    /// `Skipped` for a stage that never started, `Cancelled` otherwise.
    settles_as: TaskStatus,
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
        self.status == TaskStatus::Running
            && self.accepts_cancel
            && self.cancel_unavailable_reason.is_none()
            && self.kind.supports_cancellation()
    }

    fn finish(&mut self, status: TaskStatus) {
        self.status = status;
        self.completed_at = Some(Instant::now());
        // The hook can own a connection; a finished task must not keep it
        // alive until the user dismisses the row.
        self.cancel_hook = None;
    }

    fn skip(&mut self) -> CancelRequestOutcome {
        self.cancel_token.cancel();
        self.finish(TaskStatus::Skipped);
        CancelRequestOutcome::Cancelled
    }

    fn apply_cancel_request(&mut self) -> CancelRequestOutcome {
        if let Some(reason) = &self.cancel_unavailable_reason {
            return CancelRequestOutcome::Refused {
                reason: reason.clone(),
            };
        }

        match self.policy {
            CancelPolicy::Immediate => {
                self.cancel_token.cancel();
                self.finish(TaskStatus::Cancelled);
                CancelRequestOutcome::Cancelled
            }
            CancelPolicy::WorkerAcknowledged => self.apply_acknowledged_cancel(),
        }
    }

    fn apply_acknowledged_cancel(&mut self) -> CancelRequestOutcome {
        let outcome = match &self.cancel_hook {
            Some(hook) => hook(),
            None => CancelOutcome::Requested,
        };

        match outcome {
            CancelOutcome::Stopped => {
                self.cancel_token.cancel();
                self.finish(TaskStatus::Cancelled);
                CancelRequestOutcome::Cancelled
            }
            CancelOutcome::Requested => {
                self.cancel_token.cancel();
                self.status = TaskStatus::Cancelling;
                CancelRequestOutcome::Cancelling
            }
            CancelOutcome::NotSupported { reason } => {
                self.cancel_unavailable_reason = Some(reason.clone());
                CancelRequestOutcome::Refused { reason }
            }
            CancelOutcome::DeliveryFailed { reason } => CancelRequestOutcome::Refused { reason },
        }
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
    pub cancel_unavailable_reason: Option<String>,
    pub target_label: Option<String>,
    pub profile_id: Option<Uuid>,
    pub target: Option<TaskTarget>,
    pub parent_id: Option<TaskId>,
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
            cancel_unavailable_reason: task.cancel_unavailable_reason.clone(),
            target_label: task.target_label.clone(),
            profile_id: task.profile_id,
            target: task.target.clone(),
            parent_id: task.parent_id,
            query_text: task.query_text.clone(),
        }
    }
}

#[derive(Default)]
pub struct TaskManager {
    tasks: HashMap<TaskId, Task>,
    next_sequence: u64,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            next_sequence: 0,
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
        let mut request = TaskRequest::new(kind, description);
        request.target = target;

        let handle = self.start_handle(request);
        (handle.id, handle.token)
    }

    /// Starts a task with an explicit cancel policy and optional cancel hook.
    ///
    /// When the request carries a `supersede_key`, every non-terminal task
    /// with the same key gets `request_cancel` first and is listed in
    /// [`TaskHandle::superseded`]. The new task starts whatever the outcome:
    /// an older task that cannot stop keeps its honest status.
    pub fn start_handle(&mut self, request: TaskRequest) -> TaskHandle {
        let parent_was_cancelled = self.has_cancelled_ancestor(request.parent);
        let superseded = if parent_was_cancelled {
            // A child that cannot start must not take down live work.
            Vec::new()
        } else {
            self.supersede_matching(request.supersede_key.as_deref())
        };
        // The supersede pass can itself cancel the parent (same key), so the
        // ancestry is read again once it ran.
        let parent_cancelled = parent_was_cancelled || self.has_cancelled_ancestor(request.parent);
        let id = TaskId::new_v4();
        let cancel_token = CancelToken::new();
        let profile_id = request.target.as_ref().map(|target| target.profile_id);
        let skipped = parent_cancelled && request.pending;

        let mut task = Task {
            id,
            kind: request.kind,
            description: request.description,
            status: if request.pending {
                TaskStatus::Pending
            } else {
                TaskStatus::Running
            },
            started_at: Instant::now(),
            completed_at: None,
            progress: None,
            details: None,
            profile_id,
            target: request.target,
            parent_id: request.parent,
            query_text: None,
            cancel_unavailable_reason: None,
            target_label: request.target_label,
            cancel_token: cancel_token.clone(),
            accepts_cancel: true,
            policy: request.policy,
            cancel_hook: request.cancel_hook,
            supersede_key: request.supersede_key,
            sequence: self.next_sequence,
            settles_with_children: false,
            settles_as: TaskStatus::Cancelled,
        };
        self.next_sequence += 1;

        if skipped {
            task.skip();
        }

        self.tasks.insert(id, task);
        if parent_cancelled && !skipped {
            self.request_cancel(id);
        }
        TaskHandle {
            id,
            token: cancel_token,
            superseded,
        }
    }

    /// Hides the cancel control of a task whose work does not observe its
    /// cancel token, so the UI never offers a cancel that cannot stop it.
    /// Internal supersede logic may still cancel the task to discard a stale
    /// result.
    pub fn mark_uncancellable(&mut self, id: TaskId) {
        if let Some(task) = self.tasks.get_mut(&id) {
            task.accepts_cancel = false;
        }
    }

    /// Declares that the work can no longer be stopped (for example a DDL
    /// statement already sent) and records why, so the UI can show the reason
    /// next to the disabled cancel control. Later cancel requests are refused
    /// and leave the task and its token untouched.
    pub fn mark_point_of_no_return(&mut self, id: TaskId, reason: impl Into<String>) {
        if let Some(task) = self.tasks.get_mut(&id)
            && !task.status.is_terminal()
        {
            task.accepts_cancel = false;
            task.cancel_unavailable_reason = Some(reason.into());
        }
    }

    /// Gate for the step after which a task can no longer be stopped: marks
    /// the point of no return and returns `true` only while no cancel request
    /// has reached the task. On `false` the caller must not start the step
    /// and acknowledges the cancel instead. The check and the mark happen in
    /// one `&mut self` call, so no cancel can interleave.
    pub fn enter_point_of_no_return(&mut self, id: TaskId, reason: impl Into<String>) -> bool {
        let open = self.tasks.get(&id).is_some_and(|task| {
            task.status == TaskStatus::Running && !task.cancel_token.is_cancelled()
        });

        if open {
            self.mark_point_of_no_return(id, reason);
        }

        open
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
            && !task.status.is_terminal()
        {
            task.finish(TaskStatus::Completed);
            self.settle_ancestors(id);
        }
    }

    pub fn complete_with_details(&mut self, id: TaskId, details: impl Into<String>) {
        if let Some(task) = self.tasks.get_mut(&id)
            && !task.status.is_terminal()
        {
            task.finish(TaskStatus::Completed);

            let details = details.into();
            task.details = if details.trim().is_empty() {
                None
            } else {
                Some(details)
            };
            self.settle_ancestors(id);
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
            && !task.status.is_terminal()
        {
            task.finish(TaskStatus::Failed(error.into()));
            self.settle_ancestors(id);
        }
    }

    pub fn fail_with_details(
        &mut self,
        id: TaskId,
        error: impl Into<String>,
        details: impl Into<String>,
    ) {
        if let Some(task) = self.tasks.get_mut(&id)
            && !task.status.is_terminal()
        {
            task.finish(TaskStatus::Failed(error.into()));

            let details = details.into();
            task.details = if details.trim().is_empty() {
                None
            } else {
                Some(details)
            };
            self.settle_ancestors(id);
        }
    }

    /// Asks a task to cancel and reports what actually happened; see
    /// [`CancelRequestOutcome`]. A task never becomes `Cancelled` here unless
    /// its policy is `Immediate` or its hook reports the work stopped.
    ///
    /// Cancelling a task cascades depth-first to its non-terminal children
    /// first; see the `hierarchy` module for how the parent settles.
    pub fn request_cancel(&mut self, id: TaskId) -> CancelRequestOutcome {
        let Some(task) = self.tasks.get(&id) else {
            return CancelRequestOutcome::NotRunning;
        };

        if task.status.is_terminal() {
            return CancelRequestOutcome::NotRunning;
        }

        self.cancel_subtree(id)
    }

    /// Cancels the children first, passing through terminal ones so their
    /// in-flight descendants are reached, then the task itself.
    fn cancel_subtree(&mut self, id: TaskId) -> CancelRequestOutcome {
        for child in self.child_ids(id) {
            self.cancel_subtree(child);
        }

        let Some(task) = self.tasks.get_mut(&id) else {
            return CancelRequestOutcome::NotRunning;
        };

        let was_pending = task.status == TaskStatus::Pending;
        let own = match task.status {
            TaskStatus::Pending => task.skip(),
            TaskStatus::Running => task.apply_cancel_request(),
            TaskStatus::Cancelling => CancelRequestOutcome::AlreadyCancelling,
            _ => CancelRequestOutcome::NotRunning,
        };

        self.reconcile_with_children(id, own, was_pending)
    }

    /// Legacy cancel: `true` when the request was accepted. Prefer
    /// [`TaskManager::request_cancel`], which says whether the work stopped.
    pub fn cancel(&mut self, id: TaskId) -> bool {
        matches!(
            self.request_cancel(id),
            CancelRequestOutcome::Cancelled | CancelRequestOutcome::Cancelling
        )
    }

    /// Worker confirmation that a `Cancelling` task really stopped. Returns
    /// `true` when the task is now `Cancelled`, and `false` without changing
    /// its status when it is in any other state.
    ///
    /// A parent whose descendants are still in flight stays `Cancelling`, but
    /// the confirmation is recorded: the last descendant to stop settles it,
    /// because its own worker already returned and will not call again.
    pub fn acknowledge_cancelled(&mut self, id: TaskId) -> bool {
        let has_in_flight_descendants = self.has_in_flight_descendants(id);

        match self.tasks.get_mut(&id) {
            Some(task) if task.status == TaskStatus::Cancelling => {
                if has_in_flight_descendants {
                    task.settles_with_children = true;
                    return false;
                }
                task.finish(TaskStatus::Cancelled);
                self.settle_ancestors(id);
                true
            }
            _ => false,
        }
    }

    /// Settles a task whose worker already finished and whose result is
    /// dropped (a stale session, a rejected install): the task ends
    /// `Cancelled` whatever its policy, because nothing is left running.
    /// Returns `false` and changes nothing for a task past its point of no
    /// return or one that already finished.
    pub fn settle_discarded(&mut self, id: TaskId) -> bool {
        match self.request_cancel(id) {
            CancelRequestOutcome::Cancelled => true,
            CancelRequestOutcome::Cancelling | CancelRequestOutcome::AlreadyCancelling => {
                self.acknowledge_cancelled(id)
            }
            CancelRequestOutcome::Refused { .. } | CancelRequestOutcome::NotRunning => false,
        }
    }

    /// Requests cancellation of every running task and reports how each
    /// request settled.
    pub fn cancel_all_detailed(&mut self) -> CancelAllSummary {
        let running_ids: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, task)| matches!(task.status, TaskStatus::Running | TaskStatus::Pending))
            .map(|(id, _)| *id)
            .collect();

        let mut summary = CancelAllSummary::default();
        for id in running_ids {
            match self.request_cancel(id) {
                CancelRequestOutcome::Cancelled => summary.cancelled += 1,
                CancelRequestOutcome::Cancelling => summary.cancelling += 1,
                CancelRequestOutcome::Refused { .. } => summary.refused += 1,
                CancelRequestOutcome::AlreadyCancelling | CancelRequestOutcome::NotRunning => {}
            }
        }
        summary
    }

    /// Cancel all running tasks.
    ///
    /// Returns the number of cancel requests that were accepted, whether the
    /// task stopped or is now waiting for its worker.
    pub fn cancel_all(&mut self) -> usize {
        let summary = self.cancel_all_detailed();
        summary.cancelled + summary.cancelling
    }

    pub fn update_progress(&mut self, id: TaskId, progress: f32) {
        if let Some(task) = self.tasks.get_mut(&id)
            && !task.status.is_terminal()
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
            .filter(|t| t.status.is_in_flight())
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
            .filter(|t| t.status.is_in_flight())
            .count()
    }

    /// Count running background tasks (non-interactive: Connect, LoadSchema,
    /// SchemaRefresh, SwitchDatabase, Disconnect). Query and Export tasks are
    /// excluded so manual "Run Query" is never throttled.
    pub fn background_task_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|t| {
                t.status.is_in_flight()
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
        self.tasks.values().any(|t| t.status.is_in_flight())
    }

    /// Removes completed and cancelled tasks that finished at least
    /// `max_age_secs` ago.
    ///
    /// Failed tasks are never removed here, so their error output stays
    /// available until the user dismisses them with [`TaskManager::remove`].
    /// A finished task is also kept while a descendant is in flight, so the
    /// running stage stays reachable under it.
    pub fn cleanup_completed(&mut self, max_age_secs: u64) {
        let now = Instant::now();
        let anchors: std::collections::HashSet<TaskId> = self
            .tasks
            .keys()
            .copied()
            .filter(|id| self.has_in_flight_descendants(*id))
            .collect();

        self.tasks.retain(|id, task| {
            if anchors.contains(id) || matches!(task.status, TaskStatus::Failed(_)) {
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
            .filter(|t| t.status.is_in_flight())
            .collect();

        match running.len() {
            0 => None,
            1 => running.first().map(|task| task.description.clone()),
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

    /// Identity of the current occupant, without touching its token.
    pub fn active_id(&self) -> Option<TaskId> {
        self.active.as_ref().map(|(id, _)| *id)
    }

    pub fn active_token(&self) -> Option<&CancelToken> {
        self.active.as_ref().map(|(_, token)| token)
    }
}

#[cfg(test)]
mod tests;
