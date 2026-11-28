//! Telemetry usage-counting for the visual query builder, driven through the
//! panel's real run paths (SELECT runs, mutation runs, owner liveness) with a
//! scripted connection, so every credit lands through the ticket lifecycle the
//! production code uses.

use super::close_completion::mutation_run;
use super::*;
use crate::query_builder::{AssignmentRow, BuilderEvent, BuilderMode};
use dbflux_components::modals::MutationConfirmOutcome;
use std::collections::VecDeque;

/// Connection that answers a table browse with whatever `rows` holds when the
/// query runs, so a reload lands through the real request path.
struct StubBrowseConnection {
    rows: Arc<std::sync::Mutex<QueryResult>>,
}

impl dbflux_core::Connection for StubBrowseConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, DriverMetadata, Icon as CoreIcon, QueryLanguage,
            TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "stub-browse".to_string(),
            display_name: "Stub".to_string(),
            description: "test".to_string(),
            category: DatabaseCategory::Relational,
            transfer_family: TransferFamily::Sql,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "stub".to_string(),
            icon: CoreIcon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        })
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(&self, _: &dbflux_core::QueryRequest) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn browse_table(
        &self,
        _: &dbflux_core::TableBrowseRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        let rows = self
            .rows
            .lock()
            .map_err(|_| dbflux_core::DbError::NotSupported("poisoned".to_string()))?;
        Ok(rows.clone())
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

static THREADED_BUILDER_FIXTURE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

static BUILDER_FIXTURE_COMPLETIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

// Workers are numbered when the foreground starts the run, which is
// deterministic, so a gate can hold "the first run" whatever order the
// threads reach it in.
static BUILDER_FIXTURE_SPAWNS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    static BUILDER_WORKER_SEQUENCE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

static BUILDER_FIXTURE_WAKERS: std::sync::Mutex<Vec<std::task::Waker>> =
    std::sync::Mutex::new(Vec::new());

// Only gated tests leave the deterministic dispatcher: a synchronous driver
// cannot wait for foreground edits on the dispatcher's own thread.
pub(crate) fn next_builder_fixture_sequence() -> usize {
    BUILDER_FIXTURE_SPAWNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub(crate) async fn run_builder_fixture_worker<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> T {
    run_sequenced_builder_fixture_worker(next_builder_fixture_sequence(), operation).await
}

pub(crate) async fn run_sequenced_builder_fixture_worker<T: Send + 'static>(
    sequence: usize,
    operation: impl FnOnce() -> T + Send + 'static,
) -> T {
    if !THREADED_BUILDER_FIXTURE.load(std::sync::atomic::Ordering::SeqCst) {
        return operation();
    }
    let shared = Arc::new(std::sync::Mutex::new((None, None::<std::task::Waker>)));
    let worker = std::thread::spawn({
        let shared = shared.clone();
        move || {
            BUILDER_WORKER_SEQUENCE.with(|slot| slot.set(sequence));
            crate::task_runner::fixture_worker::mark_current_thread();
            let result = operation();
            let mut state = shared.lock().expect("fixture result");
            state.0 = Some(result);
            if let Some(waker) = state.1.take() {
                BUILDER_FIXTURE_WAKERS
                    .lock()
                    .expect("fixture wakes")
                    .push(waker);
            }
            BUILDER_FIXTURE_COMPLETIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let result = std::future::poll_fn(move |cx| {
        let mut state = shared.lock().expect("fixture result");
        if let Some(result) = state.0.take() {
            std::task::Poll::Ready(result)
        } else {
            state.1 = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    })
    .await;
    worker.join().expect("fixture worker");
    result
}

struct BuilderWorkerGate {
    prefix: &'static str,
    /// When set, only the worker numbered `held_sequence` waits, so a later
    /// statement can finish while the earlier one is still running.
    hold_first_only: std::sync::atomic::AtomicBool,
    held_sequence: std::sync::atomic::AtomicUsize,
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}

impl BuilderWorkerGate {
    fn new(prefix: &'static str) -> Arc<Self> {
        Arc::new(Self {
            prefix,
            hold_first_only: std::sync::atomic::AtomicBool::new(false),
            held_sequence: std::sync::atomic::AtomicUsize::new(0),
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: std::sync::atomic::AtomicBool::new(false),
            released: std::sync::Mutex::new(false),
            wake: std::sync::Condvar::new(),
        })
    }

    fn enter(&self, sql: &str) {
        if !self.armed.load(std::sync::atomic::Ordering::SeqCst)
            || !sql.trim_start().starts_with(self.prefix)
        {
            return;
        }
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let sequence = BUILDER_WORKER_SEQUENCE.with(|slot| slot.get());
        if self
            .hold_first_only
            .load(std::sync::atomic::Ordering::SeqCst)
            && sequence != self.held_sequence.load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let mut released = self.released.lock().expect("gate");
        while !*released {
            released = self.wake.wait(released).expect("gate");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("gate") = true;
        self.wake.notify_all();
    }
}

struct BuilderWorkerRelease(Arc<BuilderWorkerGate>);

impl Drop for BuilderWorkerRelease {
    fn drop(&mut self) {
        self.0.release();
        THREADED_BUILDER_FIXTURE.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

fn pump_builder_until(window: &mut VisualTestContext, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let is_ready = ready();
        let mut wakers =
            std::mem::take(&mut *BUILDER_FIXTURE_WAKERS.lock().expect("fixture wakes"));
        wakers.extend(crate::task_runner::fixture_worker::take_deferred());
        for waker in wakers {
            waker.wake();
        }
        window.run_until_parked();
        if is_ready {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "fixture timed out");
        std::thread::yield_now();
    }
}

struct UsageBuilderConnection {
    browse: StubBrowseConnection,
    executions: Arc<std::sync::atomic::AtomicUsize>,
    failure: bool,
    program: std::sync::Mutex<VecDeque<Result<QueryResult, dbflux_core::DbError>>>,
    commits: std::sync::atomic::AtomicUsize,
    gate: Option<Arc<BuilderWorkerGate>>,
    /// Declares `QUERY_CANCELLATION`, and answers a driver cancel by failing
    /// the statement with `DbError::Cancelled` the way a real driver does.
    cancellable: bool,
    cancel_requested: std::sync::atomic::AtomicBool,
}

impl UsageBuilderConnection {
    fn new(
        result: QueryResult,
        executions: Arc<std::sync::atomic::AtomicUsize>,
        failure: bool,
    ) -> Self {
        Self {
            browse: StubBrowseConnection {
                rows: Arc::new(std::sync::Mutex::new(result)),
            },
            executions,
            failure,
            program: Default::default(),
            commits: std::sync::atomic::AtomicUsize::new(0),
            gate: None,
            cancellable: false,
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn cancellable(mut self) -> Self {
        self.cancellable = true;
        self
    }
}

impl dbflux_core::Connection for UsageBuilderConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static CANCELLABLE: std::sync::OnceLock<dbflux_core::DriverMetadata> =
            std::sync::OnceLock::new();

        if !self.cancellable {
            return dbflux_core::Connection::metadata(&self.browse);
        }
        CANCELLABLE.get_or_init(|| {
            let mut metadata = dbflux_core::Connection::metadata(&self.browse).clone();
            metadata.capabilities = dbflux_core::DriverCapabilities::QUERY_CANCELLATION;
            metadata
        })
    }

    fn cancel_active(&self) -> Result<(), dbflux_core::DbError> {
        self.cancel_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        static GENERATOR: dbflux_core::SqlMutationGenerator =
            dbflux_core::SqlMutationGenerator::new(&dbflux_core::DefaultSqlDialect);
        Some(&GENERATOR)
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        request: &dbflux_core::QueryRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        self.executions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.enter(&request.sql);
        }
        if self
            .cancel_requested
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(dbflux_core::DbError::Cancelled);
        }
        let programmed = self
            .program
            .lock()
            .map_err(|_| dbflux_core::DbError::NotSupported("poisoned fixture".to_string()))?
            .pop_front();
        if let Some(result) = programmed {
            if result.is_ok() && request.sql.trim() == "COMMIT" {
                self.commits
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            return result;
        }
        if self.failure {
            return Err(dbflux_core::DbError::NotSupported(
                "fixture failure".to_string(),
            ));
        }
        dbflux_core::Connection::browse_table(
            &self.browse,
            &dbflux_core::TableBrowseRequest::new(TableRef::new("users")),
        )
    }

    fn browse_table(
        &self,
        request: &dbflux_core::TableBrowseRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        dbflux_core::Connection::browse_table(&self.browse, request)
    }

    fn count_table(&self, _: &dbflux_core::TableCountRequest) -> Result<u64, dbflux_core::DbError> {
        let result = self
            .browse
            .rows
            .lock()
            .map_err(|_| dbflux_core::DbError::NotSupported("poisoned fixture".to_string()))?;
        Ok(result.row_count() as u64)
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

fn usage_table_document(
    window: &mut VisualTestContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
    pk_columns: Vec<String>,
) -> (
    gpui::Entity<crate::data_document::DataDocument>,
    gpui::Entity<DataGridPanel>,
) {
    let document = window.update(|window, cx| {
        cx.new(|cx| {
            crate::data_document::DataDocument::new_for_table(
                profile_id,
                TableRef::with_schema("public", "users"),
                Some("app".to_string()),
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();
    let grid = window.update(|_, cx| document.read(cx).grid_for_usage_test());
    window.update(|_, cx| grid.update(cx, |grid, _| grid.pk_columns = pk_columns));
    (document, grid)
}

fn enable_builder_usage(app: &mut AppStateEntity) {
    app.initialize_telemetry(
        dbflux_app::telemetry::TelemetryConfig::from_values(None, None, false)
            .expect("no transport"),
        chrono::Utc::now(),
    )
    .expect("in-memory scheduler");
    let scheduler = app.telemetry_mut().expect("scheduler");
    scheduler
        .set_basic(true, chrono::Utc::now())
        .expect("Basic");
    scheduler
        .disclosure_dismissed(chrono::Utc::now())
        .expect("disclosure");
}

fn builder_usage_counts(app: &mut AppStateEntity) -> serde_json::Value {
    let scheduler = app.telemetry_mut().expect("scheduler");
    let _preparation = scheduler
        .prepare(chrono::Utc::now(), "en")
        .expect("prepare without I/O");
    serde_json::to_value(scheduler.state()).expect("state")["counters"].clone()
}

#[gpui::test]
fn builder_closed_grid_still_terminalizes_actual_select(cx: &mut TestAppContext) {
    for scenario in ["success", "failure", "cancel"] {
        let gate = BuilderWorkerGate::new("SELECT");
        let _release = BuilderWorkerRelease(gate.clone());
        let mut chartless = keyed_result(&[]);
        for column in &mut chartless.columns {
            column.kind = dbflux_core::ColumnKind::Unknown;
        }
        let mut connection = UsageBuilderConnection::new(
            chartless,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            scenario == "failure",
        );
        if scenario == "cancel" {
            connection = connection.cancellable();
        }
        connection.gate = Some(gate.clone());
        let connection = Arc::new(connection);
        let (app_state, profile_id) = register_stub_connection(cx, connection.clone());
        cx.update(|cx| app_state.update(cx, |app, _| enable_builder_usage(app)));
        let window = cx.add_empty_window();
        let (document, grid) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
        let builder = window.update(|window, cx| {
            grid.update(cx, |grid, cx| {
                grid.open_query_builder(window, cx);
                grid.builder.builder_panel.clone().expect("builder")
            })
        });
        window.run_until_parked();
        let weak_document = document.downgrade();
        let weak_grid = grid.downgrade();
        window.update(|_, cx| {
            assert!(
                grid.read(cx).chart.chart_shell.is_none(),
                "chartless fixture"
            )
        });
        BUILDER_FIXTURE_COMPLETIONS.store(0, std::sync::atomic::Ordering::SeqCst);
        THREADED_BUILDER_FIXTURE.store(true, std::sync::atomic::Ordering::SeqCst);
        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
        window.update(|window, cx| {
            grid.update(cx, |grid, cx| grid.process_pending_actions(window, cx))
        });
        pump_builder_until(window, || {
            gate.entered.load(std::sync::atomic::Ordering::SeqCst)
        });
        let task_id = window.update(|_, cx| {
            let running = app_state.read(cx).tasks().running_tasks();
            // The run executes twice and this fixture cannot cancel the
            // older SELECT, so it honestly stays in flight beside the
            // current one.
            assert_eq!(running.len(), 2, "superseded and current SELECT");
            let current: Vec<_> = running
                .iter()
                .filter(|task| grid.read(cx).runner.owns_primary(task.id))
                .collect();
            assert_eq!(current.len(), 1, "one current SELECT");
            current[0].id
        });
        drop(builder);
        drop(document);
        drop(grid);
        window.update(|_, _| {});
        window.run_until_parked();
        assert!(
            weak_document.upgrade().is_none(),
            "actual document destroyed"
        );
        assert!(
            weak_grid.upgrade().is_none(),
            "last grid reference released"
        );
        if scenario == "cancel" {
            window.update(|_, cx| {
                app_state.update(cx, |app, _| {
                    assert!(app.tasks_mut().cancel(task_id));
                })
            });
            // The cancel hook reaches the driver on its own thread: the
            // worker must not wake before the fixture has seen it.
            pump_builder_until(window, || {
                connection
                    .cancel_requested
                    .load(std::sync::atomic::Ordering::SeqCst)
            });
        }
        gate.release();
        pump_builder_until(window, || {
            BUILDER_FIXTURE_COMPLETIONS.load(std::sync::atomic::Ordering::SeqCst) == 2
        });
        window.run_until_parked();
        window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                let status = app.tasks().get(task_id).expect("tracked task").status;
                match scenario {
                    "success" => assert_eq!(status, dbflux_core::TaskStatus::Completed),
                    "failure" => assert!(
                        matches!(status, dbflux_core::TaskStatus::Failed(_)),
                        "{status:?}"
                    ),
                    _ => assert_eq!(status, dbflux_core::TaskStatus::Cancelled),
                }
                assert!(
                    app.tasks().running_tasks().is_empty(),
                    "the superseded SELECT settles too"
                );
                assert!(
                    builder_usage_counts(app)
                        .get("visual_query_builder")
                        .is_none()
                );
            })
        });
    }
}

#[gpui::test]
fn builder_superseded_select_never_replaces_the_newer_result(cx: &mut TestAppContext) {
    let gate = BuilderWorkerGate::new("SELECT");
    gate.hold_first_only
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let _release = BuilderWorkerRelease(gate.clone());
    let mut connection = UsageBuilderConnection::new(
        keyed_result(&[]),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        false,
    );
    connection.gate = Some(gate.clone());
    connection
        .program
        .lock()
        .expect("program")
        .push_back(Ok(keyed_result(&["newer"])));
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(connection));
    let window = cx.add_empty_window();
    let (_document, grid) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
    let builder = window.update(|window, cx| {
        grid.update(cx, |grid, cx| {
            grid.open_query_builder(window, cx);
            grid.builder.builder_panel.clone().expect("builder")
        })
    });
    window.run_until_parked();

    BUILDER_FIXTURE_COMPLETIONS.store(0, std::sync::atomic::Ordering::SeqCst);
    THREADED_BUILDER_FIXTURE.store(true, std::sync::atomic::Ordering::SeqCst);
    gate.held_sequence.store(
        BUILDER_FIXTURE_SPAWNS.load(std::sync::atomic::Ordering::SeqCst),
        std::sync::atomic::Ordering::SeqCst,
    );
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
    window
        .update(|window, cx| grid.update(cx, |grid, cx| grid.process_pending_actions(window, cx)));
    pump_builder_until(window, || {
        BUILDER_FIXTURE_COMPLETIONS.load(std::sync::atomic::Ordering::SeqCst) == 1
    });
    let rows_of =
        |window: &mut VisualTestContext| window.update(|_, cx| grid.read(cx).result.rows.len());
    assert_eq!(rows_of(window), 1, "the newer run is adopted first");

    gate.release();
    pump_builder_until(window, || {
        BUILDER_FIXTURE_COMPLETIONS.load(std::sync::atomic::Ordering::SeqCst) == 2
    });
    window.run_until_parked();

    assert_eq!(
        rows_of(window),
        1,
        "the older run finished last and must not replace the newer rows"
    );
    window.update(|_, cx| {
        assert!(
            app_state.read(cx).tasks().running_tasks().is_empty(),
            "the superseded run still settles its task"
        );
    });
}

#[gpui::test]
fn builder_actual_mutation_edits_invalidate_blocked_run(cx: &mut TestAppContext) {
    let mut incorrect_credits = Vec::new();
    for scenario in [
        "unchanged",
        "column",
        "value",
        "delete_mode",
        "select_mode",
        "reset",
        "clear_draft",
    ] {
        let gate = BuilderWorkerGate::new("UPDATE");
        let _release = BuilderWorkerRelease(gate.clone());
        let mut connection = UsageBuilderConnection::new(
            keyed_result(&[]),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            false,
        );
        connection.gate = Some(gate.clone());
        let (app_state, profile_id) = register_stub_connection(cx, Arc::new(connection));
        cx.update(|cx| app_state.update(cx, |app, _| enable_builder_usage(app)));
        let window = cx.add_empty_window();
        let (_document, grid) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
        let builder = window.update(|window, cx| {
            grid.update(cx, |grid, cx| {
                grid.open_query_builder(window, cx);
                grid.builder.builder_panel.clone().expect("builder")
            })
        });
        window.run_until_parked();
        window.update(|_, cx| {
            builder.update(cx, |builder, cx| {
                builder.switch_builder_mode(BuilderMode::Update, cx);
                let state = builder.mutation_state.as_mut().expect("Update");
                state.assignments.push(AssignmentRow {
                    assignment: dbflux_core::Assignment {
                        column: "name".to_string(),
                        value: dbflux_core::AssignmentValue::Literal(
                            dbflux_core::ScalarLiteral::Text("before".to_string()),
                        ),
                    },
                    raw_text: "before".to_string(),
                });
                state.exec_options.mode =
                    crate::data_grid_panel::mutation_executor::ExecutionMode::DirectAutocommit;
                builder.refresh_mutation_preview_pure();
            })
        });
        window.run_until_parked();
        BUILDER_FIXTURE_COMPLETIONS.store(0, std::sync::atomic::Ordering::SeqCst);
        THREADED_BUILDER_FIXTURE.store(true, std::sync::atomic::Ordering::SeqCst);
        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
        window.update(|window, cx| {
            grid.update(cx, |grid, cx| {
                assert!(
                    grid.pending_mutation_exec
                        .as_ref()
                        .expect("Run")
                        .usage
                        .is_some()
                );
                grid.handle_mutation_confirm_outcome(MutationConfirmOutcome::Confirmed, window, cx);
            })
        });
        pump_builder_until(window, || {
            gate.entered.load(std::sync::atomic::Ordering::SeqCst)
        });
        let (task_id, generation) = window.update(|_, cx| {
            let running = app_state.read(cx).tasks().running_tasks();
            assert_eq!(running.len(), 1, "mutation is still running");
            (running[0].id, grid.read(cx).builder.usage_generation)
        });
        window.update(|window, cx| match scenario {
            "column" => builder.update(cx, |builder, cx| {
                builder.set_assignment_column(0, "other".to_string(), cx)
            }),
            "value" => builder.update(cx, |builder, cx| {
                builder.set_assignment_raw_text(0, "after".to_string(), cx)
            }),
            "delete_mode" => builder.update(cx, |builder, cx| {
                builder.switch_builder_mode(BuilderMode::Delete, cx)
            }),
            "select_mode" => builder.update(cx, |builder, cx| {
                builder.switch_builder_mode(BuilderMode::Select, cx)
            }),
            "reset" => grid.update(cx, |grid, cx| assert!(grid.reset_builder_query(window, cx))),
            "clear_draft" => grid.update(cx, |grid, cx| grid.clear_builder_draft_spec(cx)),
            _ => {}
        });
        if matches!(scenario, "column" | "value" | "delete_mode" | "select_mode") {
            window.update(|_, cx| {
                assert_eq!(grid.read(cx).builder.usage_generation, generation);
            });
        }
        gate.release();
        pump_builder_until(window, || {
            BUILDER_FIXTURE_COMPLETIONS.load(std::sync::atomic::Ordering::SeqCst) == 1
        });
        window.run_until_parked();
        window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                assert!(
                    app.tasks().running_tasks().is_empty(),
                    "actual executor finished"
                );
                assert_eq!(
                    app.tasks().get(task_id).expect("mutation task").status,
                    dbflux_core::TaskStatus::Completed,
                    "actual executor success: {scenario}"
                );
                let credit = builder_usage_counts(app)["visual_query_builder"]
                    .as_u64()
                    .unwrap_or(0);
                if credit != u64::from(scenario == "unchanged") {
                    incorrect_credits.push((scenario, credit));
                }
            })
        });
    }
    assert!(
        incorrect_credits.is_empty(),
        "stale actual drafts credited: {incorrect_credits:?}"
    );
}

#[gpui::test]
fn builder_document_owner_liveness_with_retained_grid(cx: &mut TestAppContext) {
    for scenario in [
        "select_open",
        "select_inactive",
        "select_closed",
        "select_grouped_closed",
        "mutation_open",
        "mutation_inactive",
        "mutation_closed",
    ] {
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (app_state, profile_id) = register_stub_connection(
            cx,
            Arc::new(UsageBuilderConnection::new(
                keyed_result(&[]),
                executions.clone(),
                false,
            )),
        );
        cx.update(|cx| app_state.update(cx, |app, _| enable_builder_usage(app)));
        let window = cx.add_empty_window();
        let (document, grid) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
        let weak_document = document.downgrade();
        let weak_grid = grid.downgrade();
        let builder = window.update(|window, cx| {
            grid.update(cx, |grid, cx| {
                if scenario.contains("grouped") {
                    let mut spec = make_test_spec();
                    spec.projection = dbflux_core::Projection::Explicit(vec![]);
                    spec.aggregates = vec![dbflux_core::VisualAggregateSpec {
                        function: dbflux_core::AggFn::CountStar,
                        source_alias: None,
                        column: None,
                        alias: "count".to_string(),
                    }];
                    grid.builder.builder_draft_spec = Some(spec);
                }
                grid.open_query_builder(window, cx);
                grid.builder.builder_panel.clone().expect("builder")
            })
        });
        window.run_until_parked();
        let mutation = scenario.starts_with("mutation");
        if mutation {
            window.update(|_, cx| {
                builder.update(cx, |builder, cx| {
                    builder.switch_builder_mode(BuilderMode::Delete, cx);
                    builder
                        .mutation_state
                        .as_mut()
                        .expect("mutation state")
                        .exec_options
                        .mode =
                        crate::data_grid_panel::mutation_executor::ExecutionMode::DirectAutocommit;
                })
            });
            window.run_until_parked();
        }
        if scenario.ends_with("inactive") {
            window.update(|_, cx| {
                document.update(cx, |document, cx| document.set_active_tab(false, cx))
            });
        }
        let baseline = executions.load(std::sync::atomic::Ordering::SeqCst);
        window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
        window.update(|window, cx| {
            grid.update(cx, |grid, cx| {
                if mutation {
                    grid.handle_mutation_confirm_outcome(
                        MutationConfirmOutcome::Confirmed,
                        window,
                        cx,
                    );
                } else {
                    grid.process_pending_actions(window, cx);
                }
            })
        });
        let closed = scenario.ends_with("closed");
        let mut document = Some(document);
        if closed {
            drop(document.take());
            assert!(
                weak_document.upgrade().is_none(),
                "actual document dropped: {scenario}"
            );
            assert!(
                weak_grid.upgrade().is_some(),
                "grid deliberately retained: {scenario}"
            );
        }
        window.run_until_parked();
        assert_eq!(
            executions.load(std::sync::atomic::Ordering::SeqCst) - baseline,
            if scenario.contains("grouped") { 4 } else { 2 },
            "primary and probe workers completed: {scenario}"
        );
        window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                let counts = builder_usage_counts(app);
                assert_eq!(
                    counts["visual_query_builder"].as_u64().unwrap_or(0),
                    u64::from(!closed),
                    "{scenario}"
                );
            })
        });
        if !closed {
            assert!(
                weak_document.upgrade().is_some(),
                "open owner stays live: {scenario}"
            );
        }
    }
}

#[gpui::test]
fn builder_failed_terminal_outcome_rejects_actual_run_ticket(cx: &mut TestAppContext) {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (app_state, profile_id) = register_stub_connection(
        cx,
        Arc::new(UsageBuilderConnection::new(
            keyed_result(&[]),
            executions,
            false,
        )),
    );
    cx.update(|cx| app_state.update(cx, |app, _| enable_builder_usage(app)));
    let window = cx.add_empty_window();
    let (_document, grid) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
    let builder = window.update(|window, cx| {
        grid.update(cx, |grid, cx| {
            grid.open_query_builder(window, cx);
            grid.builder.builder_panel.clone().expect("builder")
        })
    });
    window.run_until_parked();
    window.update(|_, cx| {
        builder.update(cx, |builder, cx| {
            builder.switch_builder_mode(BuilderMode::Delete, cx);
        })
    });
    window.run_until_parked();
    window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
    window.update(|_, cx| {
        grid.update(cx, |grid, cx| {
            let pending = grid
                .pending_mutation_exec
                .take()
                .expect("actual Run captured");
            assert!(pending.usage.is_some(), "eligible operation ticket");
            let mut run = mutation_run(MutationIntent::Direct);
            run.usage = pending.usage;
            let failure = grid.finish_mutation(
                run,
                Ok(
                    crate::data_grid_panel::mutation_executor::MutationOutcome::Failed {
                        error: "terminal fixture failure".to_string(),
                    },
                ),
                cx,
            );
            assert!(failure.is_some());
            app_state.update(cx, |app, _| {
                assert!(
                    builder_usage_counts(app)
                        .get("visual_query_builder")
                        .is_none()
                );
            });
        })
    });
}

#[gpui::test]
fn builder_mutation_run_confirm_credits_zero_rows(cx: &mut TestAppContext) {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (app_state, profile_id) = register_stub_connection(
        cx,
        Arc::new(UsageBuilderConnection::new(
            keyed_result(&[]),
            executions.clone(),
            false,
        )),
    );
    cx.update(|cx| app_state.update(cx, |app, _| enable_builder_usage(app)));
    let window = cx.add_empty_window();
    let (_document, panel) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
    let builder = window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.open_query_builder(window, cx);
            panel.builder.builder_panel.clone().expect("builder")
        })
    });
    window.run_until_parked();
    window.update(|_, cx| {
        builder.update(cx, |builder, cx| {
            builder.switch_builder_mode(BuilderMode::Delete, cx);
            builder
                .mutation_state
                .as_mut()
                .expect("mutation state")
                .exec_options
                .mode = crate::data_grid_panel::mutation_executor::ExecutionMode::DirectAutocommit;
        })
    });
    window.run_until_parked();
    let baseline = executions.load(std::sync::atomic::Ordering::SeqCst);
    window.update(|_, cx| {
        app_state.update(cx, |app, _| {
            assert!(
                builder_usage_counts(app)
                    .get("visual_query_builder")
                    .is_none()
            );
        })
    });
    window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            assert!(panel.pending_mutation_exec.is_some());
            app_state.update(cx, |app, _| {
                assert!(
                    builder_usage_counts(app)
                        .get("visual_query_builder")
                        .is_none()
                );
            });
            panel.handle_mutation_confirm_outcome(MutationConfirmOutcome::Confirmed, window, cx);
        });
    });
    window.run_until_parked();
    assert_eq!(
        executions.load(std::sync::atomic::Ordering::SeqCst) - baseline,
        2,
        "one preview and one real mutation"
    );
    window.update(|_, cx| {
        app_state.update(cx, |app, _| {
            assert_eq!(builder_usage_counts(app)["visual_query_builder"], 1);
        })
    });
}

#[gpui::test]
fn builder_user_run_credits_successful_current_result(cx: &mut TestAppContext) {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (app_state, profile_id) = register_stub_connection(
        cx,
        Arc::new(UsageBuilderConnection::new(
            keyed_result(&[]),
            executions.clone(),
            false,
        )),
    );
    cx.update(|cx| app_state.update(cx, |app, _| enable_builder_usage(app)));
    let window = cx.add_empty_window();
    let (_document, panel) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
    let builder = window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.builder.builder_draft_spec = Some(make_test_spec());
            panel.open_query_builder(window, cx);
            panel.builder.builder_panel.clone().expect("builder")
        })
    });
    window.run_until_parked();
    window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            assert!(
                panel.pending.refresh,
                "actual event subscription queued refresh"
            );
            panel.process_pending_actions(window, cx);
        })
    });
    window.run_until_parked();
    assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 2);
    window.update(|_, cx| {
        assert!(matches!(panel.read(cx).refresh.state, GridState::Ready));
        assert_eq!(panel.read(cx).result.row_count(), 0);
        app_state.update(cx, |app, _| {
            let scheduler = app.telemetry_mut().expect("scheduler");
            let _preparation = scheduler
                .prepare(chrono::Utc::now(), "en")
                .expect("prepare without I/O");
            let counters =
                serde_json::to_value(scheduler.state()).expect("state")["counters"].clone();
            assert_eq!(counters["visual_query_builder"], 1);
            assert!(counters.get("query_run").is_none());
        });
    });
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.pending.refresh = true;
            panel.process_pending_actions(window, cx);
        })
    });
    window.run_until_parked();
    window.update(|_, cx| {
        app_state.update(cx, |app, _| {
            assert_eq!(builder_usage_counts(app)["visual_query_builder"], 1);
        })
    });
    window.update(|_, cx| builder.update(cx, |builder, cx| builder.request_run(cx)));
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| panel.process_pending_actions(window, cx))
    });
    window.run_until_parked();
    window.update(|_, cx| {
        app_state.update(cx, |app, _| {
            assert_eq!(builder_usage_counts(app)["visual_query_builder"], 2);
        })
    });
}

#[gpui::test]
fn builder_select_origin_owner_and_epoch_matrix(cx: &mut TestAppContext) {
    for scenario in [
        "rows",
        "inactive",
        "dedup",
        "automatic",
        "filter",
        "closed",
        "cancel",
        "superseded",
        "spec",
        "context",
        "page",
        "error",
        "partial",
        "off",
        "undisclosed",
        "late_basic",
        "reset",
        "replay",
        "grouped",
        "rejected",
        "unknown_owner",
    ] {
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut result = keyed_result(&["1", "2"]);
        if scenario == "partial" {
            result.metadata_extra = Some(std::collections::HashMap::from([(
                "script_failure".to_string(),
                serde_json::json!({"message": "fixture"}),
            )]));
        }
        let mut connection =
            UsageBuilderConnection::new(result, executions.clone(), scenario == "error");
        if scenario == "cancel" {
            connection = connection.cancellable();
        }
        let (app_state, profile_id) = register_stub_connection(cx, Arc::new(connection));
        cx.update(|cx| {
            app_state.update(cx, |app, _| {
                if scenario == "undisclosed" {
                    app.initialize_telemetry(
                        dbflux_app::telemetry::TelemetryConfig::from_values(None, None, false)
                            .expect("config"),
                        chrono::Utc::now(),
                    )
                    .expect("scheduler");
                    app.telemetry_mut()
                        .expect("scheduler")
                        .set_basic(true, chrono::Utc::now())
                        .expect("Basic without disclosure");
                } else {
                    enable_builder_usage(app);
                }
                let scheduler = app.telemetry_mut().expect("scheduler");
                if matches!(scenario, "off" | "late_basic") {
                    scheduler.set_basic(false, chrono::Utc::now()).expect("Off");
                }
            })
        });
        let window = cx.add_empty_window();
        let (document, panel) = usage_table_document(window, app_state.clone(), profile_id, vec![]);
        let weak = panel.downgrade();
        let weak_document = document.downgrade();
        window.update(|window, cx| {
            panel.update(cx, |panel, cx| {
                if scenario == "unknown_owner" {
                    panel.builder.usage_owner_is_alive = None;
                }
                let mut spec = make_test_spec();
                if scenario == "grouped" {
                    spec.projection = dbflux_core::Projection::Explicit(vec![]);
                    spec.aggregates = vec![dbflux_core::VisualAggregateSpec {
                        function: dbflux_core::AggFn::CountStar,
                        source_alias: None,
                        column: None,
                        alias: "count".to_string(),
                    }];
                } else if scenario == "rejected" {
                    spec.projection = dbflux_core::Projection::Explicit(vec![]);
                    spec.aggregates = vec![dbflux_core::VisualAggregateSpec {
                        function: dbflux_core::AggFn::Sum,
                        source_alias: None,
                        column: None,
                        alias: "sum".to_string(),
                    }];
                }
                panel.builder.builder_draft_spec = Some(spec);
                if scenario == "automatic" {
                    panel.apply_builder_draft_spec(make_test_spec(), cx);
                } else {
                    panel.handle_builder_event(&BuilderEvent::RunRequested, window, cx);
                }
                if scenario == "rejected" {
                    assert!(
                        panel.builder.visual_select.is_none(),
                        "generator rejected spec"
                    );
                }
                match scenario {
                    "inactive" => panel.is_active_tab = false,
                    "cancel" => {
                        panel.runner.cancel_primary(cx);
                    }
                    "spec" => {
                        let mut spec = make_test_spec();
                        spec.limit = Some(7);
                        panel.handle_builder_event(
                            &BuilderEvent::SpecChanged(Box::new(spec)),
                            window,
                            cx,
                        );
                    }
                    "context" => {
                        if let DataSource::Table { database, .. } = &mut panel.source {
                            *database = Some("other".to_string());
                        }
                    }
                    "page" => {
                        if let DataSource::Table { pagination, .. } = &mut panel.source {
                            *pagination = Pagination::Offset {
                                limit: 100,
                                offset: 100,
                            };
                        }
                    }
                    "filter" => panel.replace_filter_and_reload("id = 1", window, cx),
                    "late_basic" => app_state.update(cx, |app, _| {
                        app.telemetry_mut()
                            .expect("scheduler")
                            .set_basic(true, chrono::Utc::now())
                            .expect("Basic");
                    }),
                    "reset" => app_state.update(cx, |app, _| {
                        app.telemetry_mut()
                            .expect("scheduler")
                            .reset(chrono::Utc::now())
                            .expect("reset");
                    }),
                    "superseded" => {
                        panel.handle_builder_event(&BuilderEvent::RunRequested, window, cx)
                    }
                    _ => {}
                }
                if !matches!(scenario, "dedup" | "closed") {
                    panel.process_pending_actions(window, cx);
                }
            })
        });
        if scenario == "closed" {
            drop(document);
            window.run_until_parked();
            assert!(weak_document.upgrade().is_none(), "actual dropped document");
            assert!(weak.upgrade().is_some(), "grid retained outside document");
            assert_eq!(
                executions.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "worker completed"
            );
        } else {
            window.run_until_parked();
            if matches!(scenario, "dedup" | "replay") {
                window.update(|window, cx| {
                    panel.update(cx, |panel, cx| {
                        if scenario == "replay" {
                            panel.pending.refresh = true;
                        }
                        panel.process_pending_actions(window, cx);
                    })
                });
                window.run_until_parked();
            }
            if matches!(
                scenario,
                "rows" | "inactive" | "dedup" | "superseded" | "replay"
            ) {
                window.update(|_, cx| assert_eq!(panel.read(cx).result.row_count(), 2));
            }
        }
        let expected = if matches!(
            scenario,
            "rows" | "inactive" | "dedup" | "superseded" | "replay" | "grouped"
        ) {
            1
        } else {
            0
        };
        if scenario == "rejected" {
            assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
        if scenario == "grouped" {
            assert_eq!(
                executions.load(std::sync::atomic::Ordering::SeqCst),
                4,
                "two primary queries and two count probes credit one operation"
            );
        }
        window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                let counts = builder_usage_counts(app);
                assert_eq!(
                    counts["visual_query_builder"].as_u64().unwrap_or(0),
                    expected,
                    "{scenario}"
                );
                assert!(counts.get("query_run").is_none(), "{scenario}");
            })
        });
    }
}

#[gpui::test]
fn builder_mutation_origin_owner_and_outcome_matrix(cx: &mut TestAppContext) {
    for scenario in [
        "rows",
        "zero",
        "inactive",
        "error",
        "confirm_cancel",
        "cancel",
        "closed",
        "off",
        "late_basic",
        "reset",
        "context",
        "spec",
        "newer",
        "approval",
        "readonly",
        "commit_error",
        "partial_chunked",
        "undisclosed",
        "unknown_owner",
    ] {
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut result = keyed_result(&[]);
        result.affected_rows = Some(if scenario == "rows" { 3 } else { 0 });
        let connection = Arc::new(UsageBuilderConnection::new(
            result,
            executions.clone(),
            scenario == "error",
        ));
        if matches!(scenario, "commit_error" | "partial_chunked") {
            let empty = keyed_result(&[]);
            let mut written = empty.clone();
            written.affected_rows = Some(1);
            let sequence = if scenario == "partial_chunked" {
                let mut keys = keyed_result(&["1"]);
                keys.columns.truncate(1);
                for row in &mut keys.rows {
                    row.truncate(1);
                }
                let mut full_chunk = keys.clone();
                full_chunk.rows = (0..1_000)
                    .map(|index| vec![dbflux_core::Value::Text(index.to_string())])
                    .collect();
                vec![
                    Ok(empty.clone()),
                    Ok(full_chunk),
                    Ok(empty.clone()),
                    Ok(written),
                    Ok(empty.clone()),
                    Ok(keys),
                    Ok(empty.clone()),
                    Err(dbflux_core::DbError::NotSupported(
                        "programmed DML/commit failure".to_string(),
                    )),
                    Ok(empty),
                ]
            } else {
                vec![
                    Ok(empty.clone()),
                    Ok(empty.clone()),
                    Ok(written),
                    Err(dbflux_core::DbError::NotSupported(
                        "programmed DML/commit failure".to_string(),
                    )),
                    Ok(empty),
                ]
            };
            connection.program.lock().expect("program").extend(sequence);
        }
        let (app_state, profile_id) = register_stub_connection(cx, connection.clone());
        cx.update(|cx| {
            app_state.update(cx, |app, _| {
                if scenario == "undisclosed" {
                    app.initialize_telemetry(
                        dbflux_app::telemetry::TelemetryConfig::from_values(None, None, false)
                            .expect("config"),
                        chrono::Utc::now(),
                    )
                    .expect("scheduler");
                    app.telemetry_mut()
                        .expect("scheduler")
                        .set_basic(true, chrono::Utc::now())
                        .expect("Basic without disclosure");
                } else {
                    enable_builder_usage(app);
                }
                if matches!(scenario, "off" | "late_basic") {
                    app.telemetry_mut()
                        .expect("scheduler")
                        .set_basic(false, chrono::Utc::now())
                        .expect("Off");
                }
                let connected = app
                    .connections_mut()
                    .get_mut(&profile_id)
                    .expect("connection");
                connected.mutation_policy = match scenario {
                    "approval" => dbflux_core::MutationPolicy::ApprovalRequired,
                    "readonly" => dbflux_core::MutationPolicy::ReadOnly,
                    _ => dbflux_core::MutationPolicy::Allowed,
                };
            })
        });
        let window = cx.add_empty_window();
        let (document, panel) = usage_table_document(
            window,
            app_state.clone(),
            profile_id,
            vec!["id".to_string()],
        );
        let weak = panel.downgrade();
        let weak_document = document.downgrade();
        window.update(|window, cx| {
            panel.update(cx, |panel, cx| {
                if scenario == "unknown_owner" {
                    panel.builder.usage_owner_is_alive = None;
                }
                let mut opts =
                    crate::data_grid_panel::mutation_executor::MutationExecOptions::single_transaction();
                opts.chunk_size = 1_000;
                panel.open_query_builder(window, cx);
                panel
                    .builder
                    .builder_panel
                    .clone()
                    .expect("builder")
                    .update(cx, |builder, cx| {
                        builder.switch_builder_mode(BuilderMode::Delete, cx);
                    });
                opts.mode = match scenario {
                    "partial_chunked" => {
                        crate::data_grid_panel::mutation_executor::ExecutionMode::ChunkedTransaction
                    }
                    "commit_error" => {
                        crate::data_grid_panel::mutation_executor::ExecutionMode::SingleTransaction
                    }
                    _ => crate::data_grid_panel::mutation_executor::ExecutionMode::DirectAutocommit,
                };
                let event = BuilderEvent::MutationRunRequested {
                    spec: Box::new(dbflux_core::VisualMutationSpec {
                        from: TableRef::with_schema("public", "users"),
                        filter: None,
                        kind: dbflux_core::MutationKind::Delete,
                    }),
                    opts: Box::new(opts),
                    est_rows: Some(0),
                };
                panel.handle_builder_event(&event, window, cx);
                if matches!(scenario, "approval" | "readonly") {
                    assert!(panel.pending_mutation_exec.is_none());
                    if scenario == "approval" {
                        app_state.update(cx, |app, _| {
                            let queued = app.list_mcp_pending_executions().expect("pending");
                            assert_eq!(queued.len(), 1, "actual approval queue");
                            app.approve_mcp_pending_execution(&queued[0].id)
                                .expect("approve");
                        });
                    }
                } else {
                    assert!(panel.pending_mutation_exec.is_some());
                    if scenario == "late_basic" {
                        app_state.update(cx, |app, _| {
                            app.telemetry_mut()
                                .expect("scheduler")
                                .set_basic(true, chrono::Utc::now())
                                .expect("late Basic");
                        });
                    }
                    panel.handle_mutation_confirm_outcome(
                        if scenario == "confirm_cancel" {
                            MutationConfirmOutcome::Cancelled
                        } else {
                            MutationConfirmOutcome::Confirmed
                        },
                        window,
                        cx,
                    );
                    match scenario {
                        "inactive" => panel.is_active_tab = false,
                        "cancel" => app_state.update(cx, |app, _| {
                            app.tasks_mut().cancel_all();
                        }),
                        "reset" => app_state.update(cx, |app, _| {
                            app.telemetry_mut()
                                .expect("scheduler")
                                .reset(chrono::Utc::now())
                                .expect("reset");
                        }),
                        "context" => {
                            if let DataSource::Table { database, .. } = &mut panel.source {
                                *database = Some("other".to_string());
                            }
                        }
                        "spec" => panel.handle_builder_event(
                            &BuilderEvent::SpecChanged(Box::new(make_test_spec())),
                            window,
                            cx,
                        ),
                        "newer" => panel.handle_builder_event(&event, window, cx),
                        _ => {}
                    }
                }
            })
        });
        if scenario == "closed" {
            drop(document);
            window.run_until_parked();
            assert!(
                weak_document.upgrade().is_none(),
                "actual mutation owner dropped"
            );
            assert!(weak.upgrade().is_some(), "grid retained outside document");
            assert_eq!(
                executions.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "preview and completed worker"
            );
        } else {
            window.run_until_parked();
        }
        if matches!(scenario, "approval" | "readonly") {
            assert_eq!(
                executions.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "no preview or execution"
            );
        }
        if scenario == "partial_chunked" {
            assert_eq!(
                connection.commits.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "first chunk really committed"
            );
            assert!(
                connection.program.lock().expect("program").is_empty(),
                "failed second chunk rolled back"
            );
            assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 9);
        }
        let expected = if matches!(scenario, "rows" | "zero" | "inactive") {
            1
        } else {
            0
        };
        window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                let counts = builder_usage_counts(app);
                assert_eq!(
                    counts["visual_query_builder"].as_u64().unwrap_or(0),
                    expected,
                    "{scenario}"
                );
                assert!(counts.get("query_run").is_none(), "{scenario}");
            })
        });
    }
}
