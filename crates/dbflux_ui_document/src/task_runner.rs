use dbflux_core::{
    CancelHook, CancelOutcome, CancelPolicy, CancelRequestOutcome, CancelToken, DbError, TaskId,
    TaskKind, TaskRequest, TaskSlot, TaskTarget,
};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::*;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Poll, Waker};
use uuid::Uuid;

/// Wakes the foreground once the mutation worker passes its point of no
/// return, so the task can show the reason before anyone clicks cancel.
#[derive(Default)]
struct SentSignal {
    state: Mutex<SentSignalState>,
}

#[derive(Default)]
struct SentSignalState {
    fired: bool,
    closed: bool,
    waker: Option<Waker>,
}

impl SentSignal {
    fn lock(&self) -> std::sync::MutexGuard<'_, SentSignalState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn fire(&self) {
        let mut state = self.lock();
        state.fired = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Resolves to `true` when the point of no return was reached, `false`
    /// when the mutation was dropped without reaching it.
    async fn wait(&self) -> bool {
        std::future::poll_fn(|task| {
            let mut state = self.lock();
            if state.fired {
                Poll::Ready(true)
            } else if state.closed {
                Poll::Ready(false)
            } else {
                state.waker = Some(task.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

/// What the mutation's worker and its cancel hook agree on.
///
/// The cancel hook runs on the foreground and the executor on a background
/// thread, so the choice between "stopped before the statement" and "statement
/// already sent" is taken under one lock: whichever side comes second sees the
/// other's decision.
struct MutationGate {
    token: OnceLock<CancelToken>,
    sent: Mutex<bool>,
    signal: Arc<SentSignal>,
}

impl MutationGate {
    fn token_cancelled(&self) -> bool {
        self.token.get().is_some_and(CancelToken::is_cancelled)
    }

    fn request_cancel(&self) -> CancelOutcome {
        let sent = self.sent.lock().unwrap_or_else(PoisonError::into_inner);
        if *sent {
            return CancelOutcome::NotSupported {
                reason: mutation_point_of_no_return_reason(),
            };
        }

        if let Some(token) = self.token.get() {
            token.cancel();
        }
        CancelOutcome::Requested
    }

    fn enter_point_of_no_return(&self) -> bool {
        let mut sent = self.sent.lock().unwrap_or_else(PoisonError::into_inner);
        if self.token_cancelled() {
            return false;
        }

        *sent = true;
        self.signal.fire();
        true
    }
}

impl Drop for MutationGate {
    fn drop(&mut self) {
        self.signal.close();
    }
}

fn mutation_point_of_no_return_reason() -> String {
    dbflux_i18n::t!("tasks_panel.mutation_point_of_no_return")
}

/// Cancellation handle of a visual mutation run, shared by the task manager's
/// cancel path and the executor that polls it between steps.
///
/// It reads the task's own token: there is one token per task. The executor
/// calls [`Self::enter_point_of_no_return`] before it sends a statement that
/// cannot be undone.
#[derive(Clone)]
pub struct MutationCancelHandle {
    gate: Arc<MutationGate>,
}

#[cfg(test)]
impl Default for MutationCancelHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl MutationCancelHandle {
    fn unbound() -> Self {
        Self {
            gate: Arc::new(MutationGate {
                token: OnceLock::new(),
                sent: Mutex::new(false),
                signal: Arc::new(SentSignal::default()),
            }),
        }
    }

    /// Builds a handle that shares `token`, the task manager's cancel flag.
    fn from_token(token: CancelToken) -> Self {
        let handle = Self::unbound();
        handle.bind(token);
        handle
    }

    fn bind(&self, token: CancelToken) {
        self.gate.token.set(token).ok();
    }

    #[cfg(test)]
    pub fn new() -> Self {
        Self::from_token(CancelToken::new())
    }

    pub fn is_cancelled(&self) -> bool {
        self.gate.token_cancelled()
    }

    #[cfg(test)]
    pub fn cancel(&self) -> CancelOutcome {
        self.request_cancel()
    }

    /// What a cancel request achieves now: it stops the run, or is refused
    /// because the statement was already sent.
    pub fn request_cancel(&self) -> CancelOutcome {
        self.gate.request_cancel()
    }

    /// Marks the run as past its point of no return and returns `true`, unless
    /// a cancel already reached it, in which case nothing may be sent.
    pub fn enter_point_of_no_return(&self) -> bool {
        self.gate.enter_point_of_no_return()
    }

    fn cancel_hook(&self) -> CancelHook {
        let handle = self.clone();
        Arc::new(move || handle.request_cancel())
    }
}

/// Settles a query whose result is discarded because its token was
/// cancelled: `Cancelled` only when the driver reported a cancellation,
/// otherwise the statement's own outcome.
pub(crate) fn settle_task_after_cancel<T>(
    app_state: &Entity<AppStateEntity>,
    task_id: TaskId,
    result: &Result<T, DbError>,
    cx: &mut App,
) {
    app_state.update(cx, |state, cx| {
        match result {
            Err(DbError::Cancelled) => {
                state.acknowledge_task_cancelled(task_id);
            }
            Ok(_) => state.complete_task(task_id),
            Err(error) => state.fail_task(task_id, error.to_string()),
        }
        cx.emit(AppStateChanged);
    });
}

fn query_cancel_unsupported_reason(driver: &str) -> String {
    dbflux_i18n::t!("tasks_panel.query_cancel_unsupported", driver = driver)
}

pub struct DocumentTaskRunner {
    primary: TaskSlot,
    app_state: Entity<AppStateEntity>,
    profile_id: Option<Uuid>,
}

impl DocumentTaskRunner {
    pub fn new(app_state: Entity<AppStateEntity>) -> Self {
        Self {
            primary: TaskSlot::new(),
            app_state,
            profile_id: None,
        }
    }

    pub fn set_profile_id(&mut self, profile_id: Uuid) {
        self.profile_id = Some(profile_id);
    }

    pub fn clear_profile_id(&mut self) {
        self.profile_id = None;
    }

    fn default_target(&self) -> Option<TaskTarget> {
        self.profile_id.map(|profile_id| TaskTarget {
            profile_id,
            database: None,
        })
    }

    // -- Primary slot (reads — auto-cancel-previous) --

    pub fn start_primary(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        self.start_primary_for_target(kind, description, self.default_target(), cx)
    }

    pub fn start_primary_for_target(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        target: Option<TaskTarget>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        self.start_primary_with_request(TaskRequest::new(kind, description), target, cx)
    }

    /// Starts a primary query whose cancel goes through the driver.
    ///
    /// The task stays `Cancelling` until the worker calls
    /// [`Self::settle_after_cancel`], and a driver that cannot cancel keeps it
    /// `Running` with the reason. `cancels_connection` is `false` when the
    /// query runs on its own session, so cancelling must not interrupt the
    /// shared connection.
    pub fn start_primary_query_for_target(
        &mut self,
        description: impl Into<String>,
        target: TaskTarget,
        cancels_connection: bool,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        let hook = self.app_state.read(cx).query_cancel_hook(
            &target,
            cancels_connection,
            query_cancel_unsupported_reason,
        );

        self.start_primary_with_hook(TaskKind::Query, description, Some(target), hook, cx)
    }

    /// Starts a primary query on the runner's own connection, whose cancel
    /// goes through the driver like [`Self::start_primary_query_for_target`].
    /// Without a connection the task only requests the stop.
    pub fn start_primary_query(
        &mut self,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        match self.default_target() {
            Some(target) => self.start_primary_query_for_target(description, target, true, cx),
            None => self.start_primary_worker_acknowledged(TaskKind::Query, description, cx),
        }
    }

    /// Starts a primary task whose worker stops by itself when its token is
    /// cancelled (a script process, for example). It stays `Cancelling` until
    /// the worker calls [`Self::acknowledge_cancelled`] after it stopped.
    pub fn start_primary_worker_acknowledged(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        let request = TaskRequest::new(kind, description).policy(CancelPolicy::WorkerAcknowledged);

        self.start_primary_with_request(request, self.default_target(), cx)
    }

    fn start_primary_with_hook(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        target: Option<TaskTarget>,
        hook: CancelHook,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        let request = TaskRequest::new(kind, description)
            .policy(CancelPolicy::WorkerAcknowledged)
            .cancel_hook(hook);

        self.start_primary_with_request(request, target, cx)
    }

    fn start_primary_with_request(
        &mut self,
        request: TaskRequest,
        target: Option<TaskTarget>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        let request = match target {
            Some(target) => request.target(target),
            None => request,
        };

        let handle = self
            .app_state
            .update(cx, |state, _cx| state.start_task_handle(request));
        let (task_id, cancel_token) = (handle.id(), handle.token().clone());

        if let Some((old_id, old_token)) = self.primary.take() {
            let outcome = self.app_state.update(cx, |state, cx| {
                let outcome = state.request_cancel_task(old_id);
                cx.emit(AppStateChanged);
                outcome
            });

            // The older result is never wanted once a newer run took over,
            // even when its driver cannot stop the statement: the task keeps
            // running with its reason, and its worker drops the answer.
            if matches!(outcome, CancelRequestOutcome::Refused { .. }) {
                old_token.cancel();
            }
        }
        self.primary.start(task_id, cancel_token.clone());

        (task_id, cancel_token)
    }

    pub fn complete_primary(&mut self, task_id: TaskId, cx: &mut App) {
        self.primary.take_if(task_id);
        self.app_state.update(cx, |state, cx| {
            state.complete_task(task_id);
            cx.emit(AppStateChanged);
        });
    }

    pub fn fail_primary(&mut self, task_id: TaskId, error: impl Into<String>, cx: &mut App) {
        self.primary.take_if(task_id);
        self.app_state.update(cx, |state, cx| {
            state.fail_task(task_id, error);
            cx.emit(AppStateChanged);
        });
    }

    /// Asks the active primary task to cancel and reports whether the
    /// request was accepted. A task that cannot be stopped stays in the slot,
    /// running, with the reason on its snapshot.
    pub fn cancel_primary(&mut self, cx: &mut App) -> bool {
        self.request_cancel_primary(cx).is_some()
    }

    /// Like [`Self::cancel_primary`], returning how the task settled:
    /// `Cancelled` for a task without a driver hook (already final),
    /// `Cancelling` or `AlreadyCancelling` while the worker still has to
    /// confirm. `None` when nothing was cancelled.
    pub fn request_cancel_primary(&mut self, cx: &mut App) -> Option<CancelRequestOutcome> {
        let task_id = self.primary.active_id()?;

        let outcome = self.app_state.update(cx, |state, cx| {
            let outcome = state.request_cancel_task(task_id);
            cx.emit(AppStateChanged);
            outcome
        });

        match outcome {
            CancelRequestOutcome::Cancelled
            | CancelRequestOutcome::Cancelling
            | CancelRequestOutcome::AlreadyCancelling => {
                self.primary.take_if(task_id);
                Some(outcome)
            }
            CancelRequestOutcome::Refused { .. } => None,
            CancelRequestOutcome::NotRunning => {
                self.primary.take_if(task_id);
                None
            }
        }
    }

    /// Worker confirmation that a cancelled query really stopped.
    pub fn acknowledge_cancelled(&mut self, task_id: TaskId, cx: &mut App) -> bool {
        self.app_state.update(cx, |state, cx| {
            let acknowledged = state.acknowledge_task_cancelled(task_id);
            cx.emit(AppStateChanged);
            acknowledged
        })
    }

    /// Settles a query whose result the document discards because its token
    /// was cancelled; see [`settle_task_after_cancel`].
    pub fn settle_after_cancel<T>(
        &mut self,
        task_id: TaskId,
        result: &Result<T, DbError>,
        cx: &mut App,
    ) {
        self.primary.take_if(task_id);
        settle_task_after_cancel(&self.app_state, task_id, result, cx);
    }

    pub fn clear_primary(&mut self, task_id: TaskId) {
        let _ = self.primary.take_if(task_id);
    }

    pub fn is_primary_active(&self) -> bool {
        self.primary.is_active()
    }

    #[allow(dead_code)]
    pub fn primary_token(&self) -> Option<&CancelToken> {
        self.primary.active_token()
    }

    // -- Mutation slot (writes — independent, no auto-cancel) --

    /// Starts a write that runs as one driver call. Once sent it commits
    /// regardless of a cancel, so the task offers no cancel control.
    pub fn start_mutation(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, MutationCancelHandle) {
        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            let (task_id, cancel_token) =
                state.start_task_for_target(kind, description, self.default_target());
            state.tasks_mut().mark_uncancellable(task_id);
            (task_id, cancel_token)
        });

        (task_id, MutationCancelHandle::from_token(cancel_token))
    }

    /// Starts a write whose executor polls the returned handle between steps
    /// (chunks or statements), so a cancel stops the remaining work.
    ///
    /// The task stays `Cancelling` until the run calls
    /// [`Self::acknowledge_cancelled`] after its rollback. A run that reaches
    /// its point of no return through the handle shows the reason on the task
    /// and refuses further cancels.
    pub fn start_cancellable_mutation(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, MutationCancelHandle) {
        let handle = MutationCancelHandle::unbound();
        let request = TaskRequest::new(kind, description)
            .policy(CancelPolicy::WorkerAcknowledged)
            .cancel_hook(handle.cancel_hook());
        let request = match self.default_target() {
            Some(target) => request.target(target),
            None => request,
        };

        let task = self
            .app_state
            .update(cx, |state, _cx| state.start_task_handle(request));
        let task_id = task.id();
        handle.bind(task.token().clone());

        let signal = handle.gate.signal.clone();
        let app_state = self.app_state.clone();
        cx.spawn(async move |cx| {
            if !signal.wait().await {
                return;
            }

            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.mark_task_point_of_no_return(
                        task_id,
                        mutation_point_of_no_return_reason(),
                    );
                    cx.emit(AppStateChanged);
                });
            });
        })
        .detach();

        (task_id, handle)
    }

    pub fn complete_mutation(&mut self, task_id: TaskId, cx: &mut App) {
        self.app_state.update(cx, |state, cx| {
            state.complete_task(task_id);
            cx.emit(AppStateChanged);
        });
    }

    pub fn fail_mutation(&mut self, task_id: TaskId, error: impl Into<String>, cx: &mut App) {
        self.app_state.update(cx, |state, cx| {
            state.fail_task(task_id, error);
            cx.emit(AppStateChanged);
        });
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use gpui::*` which triggers macro recursion.
    use dbflux_core::CancelToken;

    use super::MutationCancelHandle;

    // T-21 — [RED] Tests for MutationCancelHandle (spec J-1, J-2, DR-15.1)

    #[test]
    fn j1_new_handle_is_not_cancelled() {
        // We test the standalone handle directly since start_mutation requires GPUI context.
        let handle = MutationCancelHandle::new();
        assert!(
            !handle.is_cancelled(),
            "new handle must start as not-cancelled"
        );
    }

    #[test]
    fn j2_cancel_flips_is_cancelled_to_true() {
        let handle = MutationCancelHandle::new();
        assert!(!handle.is_cancelled());
        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[test]
    fn mutation_cancel_handle_clone_shares_state() {
        let handle = MutationCancelHandle::new();
        let clone = handle.clone();
        handle.cancel();
        assert!(
            clone.is_cancelled(),
            "clone must see cancellation from original"
        );
    }

    // F-R2-2: DR-15.1 — cancellation via the task manager's CancelToken must propagate
    // to the MutationCancelHandle that the executor polls.
    //
    // `start_mutation` now wraps the token returned by `start_task_for_target` into the
    // handle via `from_token`. This test proves the shared-state invariant without
    // requiring a GPUI context: cancelling the original token must flip the handle.
    #[test]
    fn cancel_mutation_via_task_runner_flips_executor_handle() {
        let token = CancelToken::new();
        let handle = MutationCancelHandle::from_token(token.clone());

        assert!(!handle.is_cancelled(), "handle must start as not-cancelled");

        token.cancel();

        assert!(
            handle.is_cancelled(),
            "cancelling the CancelToken must flip is_cancelled() on the handle (DR-15.1)"
        );
    }

    fn runner_with_state(
        cx: &mut gpui::TestAppContext,
    ) -> (
        gpui::Entity<dbflux_ui_base::AppStateEntity>,
        super::DocumentTaskRunner,
    ) {
        use gpui::AppContext as _;

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                    .expect("test storage setup")
            })
        });
        let runner = super::DocumentTaskRunner::new(app_state.clone());

        (app_state, runner)
    }

    fn is_cancellable(
        app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
        task_id: dbflux_core::TaskId,
        cx: &mut gpui::TestAppContext,
    ) -> bool {
        cx.read(|cx| {
            app_state
                .read(cx)
                .tasks()
                .get(task_id)
                .expect("task exists")
                .is_cancellable
        })
    }

    #[gpui::test]
    fn a_sent_mutation_offers_no_cancel(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);

        let (task_id, _handle) =
            cx.update(|cx| runner.start_mutation(dbflux_core::TaskKind::Query, "Save row", cx));

        assert!(!is_cancellable(&app_state, task_id, cx));
    }

    #[gpui::test]
    fn a_chunked_mutation_that_polls_its_handle_stays_cancellable(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);

        let (task_id, _handle) = cx.update(|cx| {
            runner.start_cancellable_mutation(dbflux_core::TaskKind::Query, "Update rows", cx)
        });

        assert!(is_cancellable(&app_state, task_id, cx));
    }

    fn status_of(
        app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
        task_id: dbflux_core::TaskId,
        cx: &mut gpui::TestAppContext,
    ) -> dbflux_core::TaskStatus {
        cx.read(|cx| {
            app_state
                .read(cx)
                .tasks()
                .get(task_id)
                .expect("task exists")
                .status
        })
    }

    fn hook_returning(outcome: dbflux_core::CancelOutcome) -> dbflux_core::CancelHook {
        std::sync::Arc::new(move || outcome.clone())
    }

    #[gpui::test]
    fn cancel_primary_with_a_requested_hook_waits_for_the_worker(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, token) = cx.update(|cx| {
            runner.start_primary_with_hook(
                dbflux_core::TaskKind::Query,
                "select 1",
                None,
                hook_returning(dbflux_core::CancelOutcome::Requested),
                cx,
            )
        });

        assert!(cx.update(|cx| runner.cancel_primary(cx)));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );
        assert!(token.is_cancelled());

        assert!(cx.update(|cx| runner.acknowledge_cancelled(task_id, cx)));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelled
        );
    }

    #[gpui::test]
    fn cancel_primary_on_a_driver_without_cancel_keeps_the_query_running(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let reason = "Engine does not support cancelling a running command.".to_string();
        let (task_id, token) = cx.update(|cx| {
            runner.start_primary_with_hook(
                dbflux_core::TaskKind::Query,
                "select 1",
                None,
                hook_returning(dbflux_core::CancelOutcome::NotSupported {
                    reason: reason.clone(),
                }),
                cx,
            )
        });

        assert!(!cx.update(|cx| runner.cancel_primary(cx)));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Running
        );
        assert!(!token.is_cancelled(), "the worker must not see a cancel");
        assert!(runner.is_primary_active());
        let snapshot = cx.read(|cx| app_state.read(cx).tasks().get(task_id).expect("task"));
        assert_eq!(snapshot.cancel_unavailable_reason, Some(reason));
    }

    #[gpui::test]
    fn cancel_primary_still_cancels_an_immediate_task(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, _token) = cx.update(|cx| {
            runner.start_primary_for_target(dbflux_core::TaskKind::KeyScan, "scan", None, cx)
        });

        assert!(cx.update(|cx| runner.cancel_primary(cx)));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelled
        );
    }

    #[gpui::test]
    fn a_rerun_asks_the_older_acknowledged_query_to_cancel(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (older, _token) = cx.update(|cx| {
            runner.start_primary_with_hook(
                dbflux_core::TaskKind::Query,
                "older",
                None,
                hook_returning(dbflux_core::CancelOutcome::Requested),
                cx,
            )
        });
        let (newer, _token) = cx.update(|cx| {
            runner.start_primary_for_target(dbflux_core::TaskKind::Query, "newer", None, cx)
        });

        assert_eq!(
            status_of(&app_state, older, cx),
            dbflux_core::TaskStatus::Cancelling
        );
        assert_eq!(
            status_of(&app_state, newer, cx),
            dbflux_core::TaskStatus::Running
        );
    }

    #[gpui::test]
    fn a_rerun_makes_the_older_query_drop_its_answer_even_if_the_driver_cannot_cancel(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (older, older_token) = cx.update(|cx| {
            runner.start_primary_with_hook(
                dbflux_core::TaskKind::Query,
                "older",
                None,
                hook_returning(dbflux_core::CancelOutcome::NotSupported {
                    reason: "Engine does not support cancelling a running command.".to_string(),
                }),
                cx,
            )
        });
        cx.update(|cx| {
            runner.start_primary_for_target(dbflux_core::TaskKind::Query, "newer", None, cx)
        });

        assert!(
            older_token.is_cancelled(),
            "the worker discards the stale answer"
        );
        assert_eq!(
            status_of(&app_state, older, cx),
            dbflux_core::TaskStatus::Running,
            "the statement still runs on the server, and the task says so"
        );
    }

    #[gpui::test]
    fn settling_a_cancelled_query_reports_what_the_worker_saw(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);

        let mut cancelled_by =
            |cx: &mut gpui::TestAppContext, result: Result<(), dbflux_core::DbError>| {
                let (task_id, _token) = cx.update(|cx| {
                    runner.start_primary_with_hook(
                        dbflux_core::TaskKind::Query,
                        "select 1",
                        None,
                        hook_returning(dbflux_core::CancelOutcome::Requested),
                        cx,
                    )
                });
                cx.update(|cx| runner.cancel_primary(cx));
                cx.update(|cx| runner.settle_after_cancel(task_id, &result, cx));
                status_of(&app_state, task_id, cx)
            };

        assert_eq!(
            cancelled_by(cx, Err(dbflux_core::DbError::Cancelled)),
            dbflux_core::TaskStatus::Cancelled
        );
        assert_eq!(
            cancelled_by(cx, Ok(())),
            dbflux_core::TaskStatus::Completed,
            "a statement that finished anyway is not reported as cancelled"
        );
    }

    const POINT_OF_NO_RETURN_REASON: &str =
        "Changes are applied without a transaction and can no longer be cancelled.";

    #[gpui::test]
    fn a_direct_mutation_past_its_point_of_no_return_refuses_cancel_with_the_reason(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, handle) = cx.update(|cx| {
            runner.start_cancellable_mutation(dbflux_core::TaskKind::Query, "Update rows", cx)
        });

        assert!(
            handle.enter_point_of_no_return(),
            "nothing cancelled it yet"
        );
        cx.run_until_parked();

        let snapshot = cx.read(|cx| app_state.read(cx).tasks().get(task_id).expect("task"));
        assert!(!snapshot.is_cancellable, "the cancel control is disabled");
        assert_eq!(
            snapshot.cancel_unavailable_reason.as_deref(),
            Some(POINT_OF_NO_RETURN_REASON)
        );

        let outcome =
            cx.update(|cx| app_state.update(cx, |state, _| state.request_cancel_task(task_id)));
        assert_eq!(
            outcome,
            dbflux_core::CancelRequestOutcome::Refused {
                reason: POINT_OF_NO_RETURN_REASON.to_string()
            }
        );
        assert!(
            !handle.is_cancelled(),
            "a refused cancel must not reach the worker"
        );
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Running
        );

        cx.update(|cx| runner.complete_mutation(task_id, cx));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Completed
        );
    }

    #[gpui::test]
    fn a_cancel_that_reaches_a_direct_mutation_first_stops_it_before_any_statement(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, handle) = cx.update(|cx| {
            runner.start_cancellable_mutation(dbflux_core::TaskKind::Query, "Update rows", cx)
        });

        let outcome =
            cx.update(|cx| app_state.update(cx, |state, _| state.request_cancel_task(task_id)));

        assert_eq!(outcome, dbflux_core::CancelRequestOutcome::Cancelling);
        assert!(
            !handle.enter_point_of_no_return(),
            "the executor must send nothing once the cancel arrived"
        );
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );
    }

    #[gpui::test]
    fn a_transactional_mutation_is_cancelled_only_after_its_rollback(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, handle) = cx.update(|cx| {
            runner.start_cancellable_mutation(dbflux_core::TaskKind::Query, "Update rows", cx)
        });

        let outcome =
            cx.update(|cx| app_state.update(cx, |state, _| state.request_cancel_task(task_id)));

        assert_eq!(outcome, dbflux_core::CancelRequestOutcome::Cancelling);
        assert!(
            handle.is_cancelled(),
            "the executor sees the cancel between steps"
        );
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );

        assert!(cx.update(|cx| runner.acknowledge_cancelled(task_id, cx)));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelled
        );
    }

    #[gpui::test]
    fn a_cancel_that_arrives_after_the_commit_completes_the_mutation(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, _handle) = cx.update(|cx| {
            runner.start_cancellable_mutation(dbflux_core::TaskKind::Query, "Update rows", cx)
        });
        cx.update(|cx| app_state.update(cx, |state, _| state.request_cancel_task(task_id)));

        cx.update(|cx| runner.complete_mutation(task_id, cx));

        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Completed
        );
    }

    #[gpui::test]
    fn a_script_run_is_cancelled_only_after_its_runner_stopped(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, token) = cx.update(|cx| {
            runner.start_primary_worker_acknowledged(dbflux_core::TaskKind::Query, "Run Lua", cx)
        });

        assert!(cx.update(|cx| runner.cancel_primary(cx)));

        assert!(
            token.is_cancelled(),
            "the runner sees the cancel and kills the process"
        );
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );

        assert!(cx.update(|cx| runner.acknowledge_cancelled(task_id, cx)));
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelled
        );
    }

    #[gpui::test]
    fn a_query_without_a_connection_target_waits_for_the_worker_when_cancelled(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (task_id, _token) = cx.update(|cx| runner.start_primary_query("select 1", cx));

        assert!(cx.update(|cx| runner.cancel_primary(cx)));

        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );
    }

    #[test]
    fn a_cancel_racing_the_point_of_no_return_is_refused_by_the_hook_itself() {
        let handle = MutationCancelHandle::new();

        assert!(handle.enter_point_of_no_return());

        assert_eq!(
            handle.request_cancel(),
            dbflux_core::CancelOutcome::NotSupported {
                reason: POINT_OF_NO_RETURN_REASON.to_string()
            },
            "the foreground may not have marked the task yet"
        );
        assert!(!handle.is_cancelled());
    }

    // Inverse: cancelling the handle must also flip the shared token.
    #[test]
    fn cancel_on_handle_also_cancels_shared_token() {
        let token = CancelToken::new();
        let handle = MutationCancelHandle::from_token(token.clone());

        handle.cancel();

        assert!(
            token.is_cancelled(),
            "cancelling the handle must propagate to the shared CancelToken"
        );
    }
}
