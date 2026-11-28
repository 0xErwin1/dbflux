use dbflux_app::app_state::CancelMessages;
use dbflux_core::Connection;
use dbflux_core::{
    CancelHook, CancelPolicy, CancelRequestOutcome, CancelToken, DbError, TaskId, TaskKind,
    TaskRequest, TaskSlot, TaskTarget,
};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

/// A cloneable cancellation handle for a visual mutation execution.
///
/// Wraps the `CancelToken` returned by `start_task_for_target` so that the task
/// manager's cancel path (triggered by the task chip UI) and the executor's poll
/// point (`is_cancelled`) share the same underlying flag. This ensures that
/// `DocumentTaskRunner::cancel_mutation` actually stops the executor loop.
#[derive(Clone)]
pub struct MutationCancelHandle {
    token: CancelToken,
}

impl Default for MutationCancelHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl MutationCancelHandle {
    pub fn new() -> Self {
        Self {
            token: CancelToken::new(),
        }
    }

    /// Build a handle from an existing token (used by `start_mutation` to share
    /// the task manager's cancel flag with the executor loop).
    fn from_token(token: CancelToken) -> Self {
        Self { token }
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn cancel(&self) {
        self.token.cancel();
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

fn query_cancel_not_sent_reason(driver: &str, error: &str) -> String {
    dbflux_i18n::t!(
        "tasks_panel.query_cancel_not_sent",
        driver = driver,
        error = error
    )
}

fn query_cancel_messages() -> CancelMessages {
    CancelMessages {
        unsupported: query_cancel_unsupported_reason,
        not_sent: query_cancel_not_sent_reason,
    }
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
    /// `Running` with the reason. `connection` is the one the query executes
    /// on, so a cancel never reaches a connection that replaced it; it is
    /// `None` when the query runs on its own session, where cancelling must
    /// not interrupt the shared connection.
    pub fn start_primary_query_for_target(
        &mut self,
        description: impl Into<String>,
        target: TaskTarget,
        connection: Option<Arc<dyn Connection>>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        let hook = self
            .app_state
            .read(cx)
            .query_cancel_hook(connection, query_cancel_messages());

        self.start_primary_with_hook(TaskKind::Query, description, Some(target), hook, cx)
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

        if let Some((old_id, _)) = self.primary.take() {
            self.app_state.update(cx, |state, cx| {
                state.request_cancel_task(old_id);
                cx.emit(AppStateChanged);
            });
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

    /// Whether `task_id` is still the query the document waits for. A
    /// superseded query whose driver could not cancel it keeps running and
    /// finishes later; its rows must not replace the newer query's state.
    pub fn owns_primary(&self, task_id: TaskId) -> bool {
        self.primary.active_id() == Some(task_id)
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
    pub fn start_cancellable_mutation(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, MutationCancelHandle) {
        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            state.start_task_for_target(kind, description, self.default_target())
        });

        (task_id, MutationCancelHandle::from_token(cancel_token))
    }

    pub fn cancel_mutation(&mut self, task_id: TaskId, cx: &mut App) {
        self.app_state.update(cx, |state, cx| {
            state.request_cancel_task(task_id);
            cx.emit(AppStateChanged);
        });
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

    struct CancelProbe {
        metadata: dbflux_core::DriverMetadata,
        cancels: std::sync::mpsc::Sender<()>,
    }

    impl dbflux_core::Connection for CancelProbe {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn execute(
            &self,
            _: &dbflux_core::QueryRequest,
        ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
            Err(dbflux_core::DbError::NotSupported("probe".to_string()))
        }

        fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn cancel_active(&self) -> Result<(), dbflux_core::DbError> {
            self.cancels
                .send(())
                .map_err(|_| dbflux_core::DbError::Cancelled)
        }

        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
            Ok(dbflux_core::SchemaSnapshot::default())
        }

        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
            dbflux_core::SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
    }

    fn cancel_probe() -> (
        std::sync::Arc<dyn dbflux_core::Connection>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let (cancels, received) = std::sync::mpsc::channel();
        let metadata = dbflux_core::DriverMetadataBuilder::new(
            "probe",
            "Probe DB",
            dbflux_core::DatabaseCategory::Relational,
            dbflux_core::QueryLanguage::Sql,
        )
        .capabilities(dbflux_core::DriverCapabilities::QUERY_CANCELLATION)
        .build();

        (
            std::sync::Arc::new(CancelProbe { metadata, cancels }),
            received,
        )
    }

    #[gpui::test]
    fn a_query_cancel_reaches_the_connection_the_query_runs_on(cx: &mut gpui::TestAppContext) {
        let (app_state, mut runner) = runner_with_state(cx);
        let (executing, executing_cancels) = cancel_probe();
        let (replacement, replacement_cancels) = cancel_probe();
        let target = dbflux_core::TaskTarget {
            profile_id: uuid::Uuid::new_v4(),
            database: None,
        };
        let (task_id, _token) = cx.update(|cx| {
            runner.start_primary_query_for_target("select 1", target, Some(executing), cx)
        });

        assert!(cx.update(|cx| runner.cancel_primary(cx)));

        executing_cancels
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the executing connection was asked to cancel");
        assert!(
            replacement_cancels
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err()
        );
        drop(replacement);
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );
    }

    #[gpui::test]
    fn a_query_on_its_own_session_never_cancels_the_shared_connection(
        cx: &mut gpui::TestAppContext,
    ) {
        let (app_state, mut runner) = runner_with_state(cx);
        let target = dbflux_core::TaskTarget {
            profile_id: uuid::Uuid::new_v4(),
            database: None,
        };
        let (task_id, token) =
            cx.update(|cx| runner.start_primary_query_for_target("select 1", target, None, cx));

        assert!(cx.update(|cx| runner.cancel_primary(cx)));

        assert!(token.is_cancelled());
        assert_eq!(
            status_of(&app_state, task_id, cx),
            dbflux_core::TaskStatus::Cancelling
        );
    }

    #[gpui::test]
    fn only_the_newest_primary_query_owns_the_slot(cx: &mut gpui::TestAppContext) {
        let (_app_state, mut runner) = runner_with_state(cx);
        let (older, _token) = cx.update(|cx| {
            runner.start_primary_for_target(dbflux_core::TaskKind::Query, "older", None, cx)
        });
        assert!(runner.owns_primary(older));

        let (newer, _token) = cx.update(|cx| {
            runner.start_primary_for_target(dbflux_core::TaskKind::Query, "newer", None, cx)
        });

        assert!(!runner.owns_primary(older), "superseded");
        assert!(runner.owns_primary(newer));
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
