use dbflux_core::{
    Connection, DbError, ExecutionSession, QueryRequest, QueryResult, TransactionControl,
    classify_sql_transaction_control,
};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};

pub(super) struct SessionExecution {
    pub(super) result: Result<QueryResult, DbError>,
    pub(super) isolated: bool,
}

enum SessionSlot {
    Empty,
    Ready {
        generation: u64,
        root: Weak<dyn Connection>,
        database: Option<String>,
        session: Arc<dyn ExecutionSession>,
    },
    Failed {
        generation: u64,
    },
    Closed {
        generation: u64,
    },
}

/// Background-owned, document-local state for an optional isolated execution session.
///
/// The slot mutex is intentionally held throughout open and execute. It serializes
/// interactive statements and is never acquired from the foreground thread.
pub(super) struct ExecutionSessionBinding {
    generation: AtomicU64,
    slot: Mutex<SessionSlot>,
}

impl ExecutionSessionBinding {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicU64::new(0),
            slot: Mutex::new(SessionSlot::Empty),
        })
    }

    /// Advances the context generation before a background caller schedules cleanup.
    pub(super) fn invalidate(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(super) fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(super) fn is_current_generation(&self, generation: u64) -> bool {
        self.current_generation() == generation
    }

    pub(super) fn execute(
        &self,
        root: Arc<dyn Connection>,
        database: Option<String>,
        request: &QueryRequest,
    ) -> SessionExecution {
        let generation = self.generation.load(Ordering::Acquire);
        let mut slot = match self.slot.lock() {
            Ok(slot) => slot,
            Err(_) => return failure("editor session state is unavailable", false),
        };

        let Some(factory) = root.execution_session_factory() else {
            return SessionExecution {
                result: root.execute(request),
                isolated: false,
            };
        };

        if matches!(
            classify_sql_transaction_control(&request.sql),
            TransactionControl::Unsupported
        ) {
            return failure(
                "unsupported or multiple transaction-control statements in the editor",
                true,
            );
        }

        let matches_context = matches!(
            &*slot,
            SessionSlot::Ready {
                generation: bound_generation,
                root: bound_root,
                database: bound_database,
                session,
            } if *bound_generation == generation
                && bound_database == &database
                && bound_root.upgrade().is_some_and(|bound| Arc::ptr_eq(&bound, &root))
                && !session.is_closed()
        );

        if matches!(
            &*slot,
            SessionSlot::Failed {
                generation: failed_generation
            } | SessionSlot::Closed {
                generation: failed_generation
            } if *failed_generation == generation
        ) {
            return failure("editor session is closed or previously failed", true);
        }

        if !matches_context {
            if let SessionSlot::Ready { session, .. } =
                std::mem::replace(&mut *slot, SessionSlot::Empty)
                && let Err(error) = session.close()
            {
                return SessionExecution {
                    result: Err(error),
                    isolated: true,
                };
            }

            let session = match factory.open() {
                Ok(session) => session,
                Err(error) => {
                    *slot = SessionSlot::Failed { generation };
                    return SessionExecution {
                        result: Err(error),
                        isolated: true,
                    };
                }
            };
            if generation != self.generation.load(Ordering::Acquire) {
                let cleanup = session.close();
                *slot = SessionSlot::Closed { generation };
                return SessionExecution {
                    result: combine_cleanup(
                        Err(DbError::query_failed(
                            "editor session became stale while opening",
                        )),
                        cleanup,
                        "editor session became stale while opening",
                    ),
                    isolated: true,
                };
            }
            *slot = SessionSlot::Ready {
                generation,
                root: Arc::downgrade(&root),
                database,
                session,
            };
        }

        let SessionSlot::Ready { session, .. } = &*slot else {
            return failure("editor session was not installed", true);
        };
        let result = session.connection().execute(request);

        if generation != self.generation.load(Ordering::Acquire) || session.is_closed() {
            let stale = match std::mem::replace(&mut *slot, SessionSlot::Closed { generation }) {
                SessionSlot::Ready { session, .. } => session,
                _ => return failure("editor session state changed unexpectedly", true),
            };
            let cleanup = stale.close();
            return SessionExecution {
                result: combine_cleanup(result, cleanup, "editor session became stale"),
                isolated: true,
            };
        }

        SessionExecution {
            result,
            isolated: true,
        }
    }

    /// Closes the session only after prior serialized execution has completed.
    #[allow(clippy::result_large_err)]
    pub(super) fn close_invalidated(&self, generation: u64) -> Result<(), DbError> {
        let mut slot = self
            .slot
            .lock()
            .map_err(|_| DbError::query_failed("editor session state is unavailable"))?;
        let stale = matches!(
            &*slot,
            SessionSlot::Ready {
                generation: bound_generation,
                ..
            } if *bound_generation < generation
        );
        if stale {
            if let SessionSlot::Ready { session, .. } =
                std::mem::replace(&mut *slot, SessionSlot::Empty)
            {
                session.close()?;
            }
        } else if matches!(
            &*slot,
            SessionSlot::Failed {
                generation: bound_generation
            } | SessionSlot::Closed {
                generation: bound_generation
            } if *bound_generation < generation
        ) {
            *slot = SessionSlot::Empty;
        }
        Ok(())
    }
}

fn failure(message: &str, isolated: bool) -> SessionExecution {
    SessionExecution {
        result: Err(DbError::query_failed(message)),
        isolated,
    }
}

#[allow(clippy::result_large_err)]
fn combine_cleanup(
    result: Result<QueryResult, DbError>,
    cleanup: Result<(), DbError>,
    stale_message: &str,
) -> Result<QueryResult, DbError> {
    match (result, cleanup) {
        (Ok(_), Ok(())) => Err(DbError::query_failed(stale_message)),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(DbError::query_failed(format!(
            "{primary}\nCleanup failed: {cleanup}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code::CodeDocument;
    use crate::tab_manager::TabManager;
    use dbflux_components::controls::DropdownItem;
    use dbflux_components::theme;
    use dbflux_core::{
        DatabaseCategory, DatabaseInfo, DbKind, DriverMetadataBuilder, ExecutionSessionFactory,
        QueryHandle, RelationalSchema, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
    };
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use dbflux_ui_base::{AppStateEntity, AppStateGlobal};
    use gpui::{AppContext, ParentElement};
    use gpui_component::Root;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct ExecuteGate {
        entered: AtomicBool,
    }

    struct FakeConnection {
        factory: Option<Arc<FakeFactory>>,
        queries: AtomicUsize,
        cancels: AtomicUsize,
        databases: Mutex<Vec<Option<String>>>,
        execute_gate: Option<Arc<ExecuteGate>>,
    }

    impl FakeConnection {
        fn root(factory: Arc<FakeFactory>) -> Arc<Self> {
            Arc::new(Self {
                factory: Some(factory),
                queries: AtomicUsize::new(0),
                cancels: AtomicUsize::new(0),
                databases: Mutex::new(Vec::new()),
                execute_gate: None,
            })
        }

        fn isolated() -> Arc<Self> {
            Self::isolated_with_gate(None)
        }

        fn isolated_with_gate(execute_gate: Option<Arc<ExecuteGate>>) -> Arc<Self> {
            Arc::new(Self {
                factory: None,
                queries: AtomicUsize::new(0),
                cancels: AtomicUsize::new(0),
                databases: Mutex::new(Vec::new()),
                execute_gate,
            })
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            static METADATA: std::sync::OnceLock<dbflux_core::DriverMetadata> =
                std::sync::OnceLock::new();
            METADATA.get_or_init(|| {
                DriverMetadataBuilder::new(
                    "execution-session-test",
                    "Execution Session Test",
                    DatabaseCategory::Relational,
                    dbflux_core::QueryLanguage::Sql,
                )
                .build()
            })
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
            if let Some(gate) = &self.execute_gate {
                gate.entered.store(true, Ordering::SeqCst);
            }
            self.queries.fetch_add(1, Ordering::SeqCst);
            self.databases
                .lock()
                .expect("test database collection")
                .push(request.database.clone());
            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn cancel_active(&self) -> Result<(), DbError> {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("not used".to_string()))
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }

        fn execution_session_factory(&self) -> Option<&dyn ExecutionSessionFactory> {
            self.factory
                .as_deref()
                .map(|factory| factory as &dyn ExecutionSessionFactory)
        }
    }

    struct FakeSession {
        connection: Arc<FakeConnection>,
        closed: AtomicBool,
        closes: AtomicUsize,
        fail_close: AtomicBool,
    }

    impl ExecutionSession for FakeSession {
        fn connection(&self) -> Arc<dyn Connection> {
            self.connection.clone()
        }

        fn close(&self) -> Result<(), DbError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.closed.store(true, Ordering::SeqCst);
            if self.fail_close.load(Ordering::SeqCst) {
                Err(DbError::query_failed("close failed"))
            } else {
                Ok(())
            }
        }

        fn finish_operation(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }
    }

    struct OpenGate {
        opened: AtomicBool,
        release: AtomicBool,
    }

    struct FakeFactory {
        session: Arc<FakeSession>,
        sessions: Mutex<Vec<Arc<FakeSession>>>,
        opens: AtomicUsize,
        fail_next_open: AtomicBool,
        open_gate: Option<Arc<OpenGate>>,
    }

    impl ExecutionSessionFactory for FakeFactory {
        fn open(&self) -> Result<Arc<dyn ExecutionSession>, DbError> {
            let open_number = self.opens.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.open_gate {
                gate.opened.store(true, Ordering::SeqCst);
                while !gate.release.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
            }
            if self.fail_next_open.swap(false, Ordering::SeqCst) {
                return Err(DbError::query_failed("open failed"));
            }
            let session = if open_number == 0 {
                self.session.clone()
            } else {
                Arc::new(FakeSession {
                    connection: FakeConnection::isolated(),
                    closed: AtomicBool::new(false),
                    closes: AtomicUsize::new(0),
                    fail_close: AtomicBool::new(false),
                })
            };
            self.sessions
                .lock()
                .expect("test session collection")
                .push(session.clone());
            Ok(session)
        }

        fn shutdown(&self) -> Result<(), DbError> {
            Ok(())
        }
    }

    struct MountedDocuments(Vec<gpui::Entity<crate::code::CodeDocument>>);

    impl gpui::Render for MountedDocuments {
        fn render(
            &mut self,
            _: &mut gpui::Window,
            _: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            gpui::div().children(self.0.clone())
        }
    }

    fn initialized_app_state(cx: &mut gpui::TestAppContext) -> gpui::Entity<AppStateEntity> {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("isolated storage runtime"),
                )
                .expect("test storage setup")
            })
        });
        cx.update(|cx| {
            cx.set_global(AppStateGlobal {
                entity: app_state.clone(),
            });
        });
        app_state
    }

    fn add_test_profile(
        cx: &mut gpui::TestAppContext,
        app_state: &gpui::Entity<AppStateEntity>,
        root: Arc<dyn Connection>,
    ) -> uuid::Uuid {
        let profile_id = uuid::Uuid::new_v4();
        cx.update(|cx| {
            app_state.update(cx, |app, _| {
                let mut profile = dbflux_core::ConnectionProfile::new(
                    "test",
                    dbflux_core::DbConfig::SQLite {
                        path: std::path::PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                );
                profile.id = profile_id;
                app.connections_mut().insert(
                    profile_id,
                    dbflux_core::ConnectedProfile {
                        profile,
                        connection: root,
                        schema: Some(SchemaSnapshot::relational(RelationalSchema {
                            databases: vec![
                                DatabaseInfo {
                                    name: "databaseA".to_string(),
                                    is_current: true,
                                },
                                DatabaseInfo {
                                    name: "databaseB".to_string(),
                                    is_current: false,
                                },
                            ],
                            current_database: Some("databaseA".to_string()),
                            schemas: Vec::new(),
                            tables: Vec::new(),
                            views: Vec::new(),
                        })),
                        mutation_policy: dbflux_core::MutationPolicy::default(),
                        read_only_reason: None,
                        database_schemas: Default::default(),
                        table_details: Default::default(),
                        collection_children: Default::default(),
                        schema_types: Default::default(),
                        schema_indexes: Default::default(),
                        schema_foreign_keys: Default::default(),
                        schema_routines: Default::default(),
                        dependents_cache: Default::default(),
                        active_database: None,
                        redis_key_cache: Default::default(),
                        database_connections: Default::default(),
                        proxy_tunnel: None,
                    },
                );
                app.set_active_connection(profile_id);
            });
        });
        profile_id
    }

    fn factory_backed_root() -> (Arc<dyn Connection>, Arc<FakeFactory>, Arc<FakeConnection>) {
        factory_backed_root_with_open_gate(None)
    }

    fn factory_backed_root_with_open_gate(
        open_gate: Option<Arc<OpenGate>>,
    ) -> (Arc<dyn Connection>, Arc<FakeFactory>, Arc<FakeConnection>) {
        let isolated = FakeConnection::isolated();
        let factory = Arc::new(FakeFactory {
            session: Arc::new(FakeSession {
                connection: isolated.clone(),
                closed: AtomicBool::new(false),
                closes: AtomicUsize::new(0),
                fail_close: AtomicBool::new(false),
            }),
            opens: AtomicUsize::new(0),
            sessions: Mutex::new(Vec::new()),
            fail_next_open: AtomicBool::new(false),
            open_gate,
        });
        (FakeConnection::root(factory.clone()), factory, isolated)
    }

    #[test]
    fn stale_open_is_closed_without_executing_or_installing() {
        let gate = Arc::new(OpenGate {
            opened: AtomicBool::new(false),
            release: AtomicBool::new(false),
        });
        let (root, factory, isolated) = factory_backed_root_with_open_gate(Some(gate.clone()));
        let binding = ExecutionSessionBinding::new();
        let task_binding = binding.clone();
        let task = std::thread::spawn(move || {
            task_binding.execute(root, None, &QueryRequest::new("SELECT 1"))
        });

        while !gate.opened.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        binding.invalidate();
        gate.release.store(true, Ordering::SeqCst);

        assert!(task.join().expect("background execution").result.is_err());
        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 0);
        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_first_open_shares_one_child_session() {
        let (root, factory, child) = factory_backed_root();
        let binding = ExecutionSessionBinding::new();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let request = QueryRequest::new("SELECT 1");

        std::thread::scope(|scope| {
            let request = &request;
            let tasks = (0..2)
                .map(|_| {
                    let binding = binding.clone();
                    let root = root.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        binding.execute(root, None, &request)
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            for task in tasks {
                assert!(task.join().expect("concurrent execution").result.is_ok());
            }
        });

        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(child.queries.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn failed_session_does_not_reopen_or_replay_the_next_statement() {
        let (root, factory, isolated) = factory_backed_root();
        factory.fail_next_open.store(true, Ordering::SeqCst);
        let binding = ExecutionSessionBinding::new();
        let request = QueryRequest::new("SELECT 1");

        assert!(
            binding
                .execute(root.clone(), None, &request)
                .result
                .is_err()
        );
        assert!(binding.execute(root, None, &request).result.is_err());

        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn one_binding_reuses_its_session_for_transaction_lifecycle() {
        let (root, factory, isolated) = factory_backed_root();
        let binding = ExecutionSessionBinding::new();

        for sql in [
            "BEGIN",
            "INSERT INTO users VALUES (1)",
            "COMMIT",
            "SELECT 1",
        ] {
            assert!(
                binding
                    .execute(root.clone(), None, &QueryRequest::new(sql))
                    .result
                    .is_ok()
            );
        }

        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn documents_keep_isolated_sessions_separate() {
        let (first_root, first_factory, first_isolated) = factory_backed_root();
        let (second_root, second_factory, second_isolated) = factory_backed_root();

        assert!(
            ExecutionSessionBinding::new()
                .execute(first_root, None, &QueryRequest::new("SELECT 1"))
                .result
                .is_ok()
        );
        assert!(
            ExecutionSessionBinding::new()
                .execute(second_root, None, &QueryRequest::new("SELECT 2"))
                .result
                .is_ok()
        );

        assert_eq!(first_factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(second_factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(first_isolated.queries.load(Ordering::SeqCst), 1);
        assert_eq!(second_isolated.queries.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unsupported_control_is_rejected_before_session_open() {
        let (root, factory, isolated) = factory_backed_root();
        let execution = ExecutionSessionBinding::new().execute(
            root,
            None,
            &QueryRequest::new("SAVEPOINT editor_savepoint"),
        );

        assert!(execution.result.is_err());
        assert_eq!(factory.opens.load(Ordering::SeqCst), 0);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn none_factory_preserves_root_execution() {
        let root = FakeConnection::isolated();
        let execution = ExecutionSessionBinding::new().execute(
            root.clone(),
            None,
            &QueryRequest::new("SELECT 1"),
        );

        assert!(execution.result.is_ok());
        assert!(!execution.isolated);
        assert_eq!(root.queries.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    fn real_run_query_reuses_session_after_result_processing(cx: &mut gpui::TestAppContext) {
        let app_state = initialized_app_state(cx);
        let (root, factory, isolated) = factory_backed_root();
        let profile_id = add_test_profile(cx, &app_state, root);
        let document = Rc::new(RefCell::new(None));
        let document_ref = document.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    Some(profile_id),
                    dbflux_core::QueryLanguage::Sql,
                    window,
                    cx,
                );
                document.set_content("SELECT 1", window, cx);
                document
            });
            document_ref.replace(Some(document.clone()));
            Root::new(document, window, cx)
        });
        let document = document.borrow().clone().expect("document created");
        window.run_until_parked();

        window.update(|window, cx| {
            document.update(cx, |document, cx| document.run_query(window, cx));
        });
        window.run_until_parked();
        window.run_until_parked();
        assert!(window.update(|_, cx| {
            let document = document.read(cx);
            document.execution.active_query_task.is_none()
                && document.execution_session_context.is_some()
        }));
        window.update(|window, cx| {
            document.update(cx, |document, cx| document.run_query(window, cx));
        });
        window.run_until_parked();
        window.run_until_parked();
        assert!(window.update(|_, cx| { document.read(cx).execution.active_query_task.is_none() }));

        assert_eq!(
            window.update(|_, cx| document.read(cx).execution.execution_history.len()),
            2
        );
        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 2);
        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 0);

        let (replacement_root, replacement_factory, replacement_isolated) = factory_backed_root();
        window.update(|_, cx| {
            app_state.update(cx, |app, cx| {
                app.connections_mut()
                    .get_mut(&profile_id)
                    .expect("connected test profile")
                    .connection = replacement_root;
                cx.emit(dbflux_ui_base::AppStateChanged);
            });
        });
        window.run_until_parked();
        window.update(|window, cx| {
            document.update(cx, |document, cx| document.run_query(window, cx));
        });
        window.run_until_parked();
        window.run_until_parked();

        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 1);
        assert_eq!(replacement_factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(replacement_isolated.queries.load(Ordering::SeqCst), 1);
    }

    enum CompletionInterruption {
        Cancel,
        ChangeDatabaseContext,
    }

    fn assert_real_isolated_query_completion(
        interruption: CompletionInterruption,
        cx: &mut gpui::TestAppContext,
    ) {
        let app_state = initialized_app_state(cx);
        let execute_gate = Arc::new(ExecuteGate {
            entered: AtomicBool::new(false),
        });
        let isolated = FakeConnection::isolated_with_gate(Some(execute_gate.clone()));
        let session = Arc::new(FakeSession {
            connection: isolated.clone(),
            closed: AtomicBool::new(false),
            closes: AtomicUsize::new(0),
            fail_close: AtomicBool::new(false),
        });
        let factory = Arc::new(FakeFactory {
            session: session.clone(),
            sessions: Mutex::new(Vec::new()),
            opens: AtomicUsize::new(0),
            fail_next_open: AtomicBool::new(false),
            open_gate: None,
        });
        let root = FakeConnection::root(factory.clone());
        let profile_id = add_test_profile(cx, &app_state, root.clone());
        let document = Rc::new(RefCell::new(None));
        let document_ref = document.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    Some(profile_id),
                    dbflux_core::QueryLanguage::Sql,
                    window,
                    cx,
                );
                document.set_content("SELECT 1", window, cx);
                document
            });
            document_ref.replace(Some(document.clone()));
            Root::new(document, window, cx)
        });
        let document = document.borrow().clone().expect("document created");
        window.run_until_parked();

        window.update(|window, cx| {
            document.update(cx, |document, cx| document.run_query(window, cx));
        });
        for _ in 0..128 {
            if window.update(|_, cx| document.read(cx).execution.active_query_task.is_some()) {
                break;
            }
            window.dispatcher.tick(false);
        }
        assert!(window.update(|_, cx| { document.read(cx).execution.active_query_task.is_some() }));
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 0);
        assert!(window.dispatcher.tick(true));
        assert!(execute_gate.entered.load(Ordering::SeqCst));
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 1);
        assert!(window.update(|_, cx| {
            document
                .read(cx)
                .execution
                .execution_history
                .last()
                .is_some_and(|record| record.result.is_none() && record.error.is_none())
        }));

        window.update(|_, cx| {
            document.update(cx, |document, cx| match interruption {
                CompletionInterruption::Cancel => document.cancel_query(cx),
                CompletionInterruption::ChangeDatabaseContext => document
                    .on_database_changed(&DropdownItem::with_value("databaseB", "databaseB"), cx),
            });
        });
        window.run_until_parked();

        assert_eq!(root.cancels.load(Ordering::SeqCst), 0);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 1);
        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(session.closes.load(Ordering::SeqCst), 1);
        assert!(window.update(|_, cx| {
            let document = document.read(cx);
            let record = document
                .execution
                .execution_history
                .last()
                .expect("query record");
            match interruption {
                CompletionInterruption::Cancel => {
                    record.result.is_some()
                        && record.error.is_none()
                        && document.execution.active_query_task.is_none()
                }
                CompletionInterruption::ChangeDatabaseContext => {
                    record.result.is_none()
                        && record.error.as_deref()
                            == Some("Execution result discarded after context changed")
                        && document.execution.active_query_task.is_none()
                }
            }
        }));
    }

    #[gpui::test]
    fn cancelling_real_isolated_query_preserves_result_and_closes_child_session(
        cx: &mut gpui::TestAppContext,
    ) {
        assert_real_isolated_query_completion(CompletionInterruption::Cancel, cx);
    }

    #[gpui::test]
    fn changing_database_before_real_isolated_result_publication_discards_stale_result(
        cx: &mut gpui::TestAppContext,
    ) {
        assert_real_isolated_query_completion(CompletionInterruption::ChangeDatabaseContext, cx);
    }

    #[gpui::test]
    fn real_run_query_rotates_session_after_database_context_change(cx: &mut gpui::TestAppContext) {
        let app_state = initialized_app_state(cx);
        let (root, factory, isolated) = factory_backed_root();
        let profile_id = add_test_profile(cx, &app_state, root);
        let document = Rc::new(RefCell::new(None));
        let document_ref = document.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    Some(profile_id),
                    dbflux_core::QueryLanguage::Sql,
                    window,
                    cx,
                );
                document.set_content("SELECT 1", window, cx);
                document
            });
            document_ref.replace(Some(document.clone()));
            Root::new(document, window, cx)
        });
        let document = document.borrow().clone().expect("document created");
        window.run_until_parked();

        window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document
                    .on_database_changed(&DropdownItem::with_value("databaseA", "databaseA"), cx);
                document.run_query(window, cx);
            });
        });
        window.run_until_parked();
        window.run_until_parked();

        window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document
                    .on_database_changed(&DropdownItem::with_value("databaseB", "databaseB"), cx);
                document.run_query(window, cx);
            });
        });
        window.run_until_parked();
        window.run_until_parked();

        assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 1);
        assert_eq!(isolated.queries.load(Ordering::SeqCst), 1);
        let replacement = factory.sessions.lock().expect("test session collection")[1].clone();
        assert_eq!(replacement.connection.queries.load(Ordering::SeqCst), 1);

        window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document
                    .on_database_changed(&DropdownItem::with_value("databaseB", "databaseB"), cx);
                document.run_query(window, cx);
            });
        });
        window.run_until_parked();
        window.run_until_parked();

        assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 1);
        assert_eq!(replacement.connection.queries.load(Ordering::SeqCst), 2);
        assert_eq!(
            isolated
                .databases
                .lock()
                .expect("test database collection")
                .as_slice(),
            &[Some("databaseA".to_string())]
        );
        assert_eq!(
            replacement
                .connection
                .databases
                .lock()
                .expect("test database collection")
                .as_slice(),
            &[Some("databaseB".to_string()), Some("databaseB".to_string())]
        );
    }

    #[gpui::test]
    fn two_mounted_documents_keep_sessions_independent_after_tab_close(
        cx: &mut gpui::TestAppContext,
    ) {
        #[derive(Debug, PartialEq)]
        enum CloseRoute {
            Close,
            CloseActive,
            CloseAll,
            CloseOthers,
            CloseToLeft,
            CloseToRight,
        }

        for route in [
            CloseRoute::Close,
            CloseRoute::CloseActive,
            CloseRoute::CloseAll,
            CloseRoute::CloseOthers,
            CloseRoute::CloseToLeft,
            CloseRoute::CloseToRight,
        ] {
            let app_state = initialized_app_state(cx);
            let (root, factory, _isolated) = factory_backed_root();
            let profile_id = add_test_profile(cx, &app_state, root);
            let documents = Rc::new(RefCell::new(Vec::new()));
            let documents_ref = documents.clone();
            let tabs = Rc::new(RefCell::new(None));
            let tabs_ref = tabs.clone();
            let (_, window) = cx.add_window_view(|window, cx| {
                let mut code_documents = Vec::new();
                for query in ["SELECT 1", "SELECT 2", "SELECT 3"] {
                    let document = cx.new(|cx| {
                        let mut document = CodeDocument::new_with_language(
                            app_state.clone(),
                            Some(profile_id),
                            dbflux_core::QueryLanguage::Sql,
                            window,
                            cx,
                        );
                        document.set_content(query, window, cx);
                        document
                    });
                    code_documents.push(document);
                }
                let tab_manager = cx.new(|_| TabManager::new());
                tab_manager.update(cx, |tabs, cx| {
                    for document in &code_documents {
                        tabs.open_pane(CodeDocument::into_pane(document.clone(), cx), cx);
                    }
                });
                documents_ref.replace(code_documents.clone());
                tabs_ref.replace(Some(tab_manager));
                Root::new(cx.new(|_| MountedDocuments(code_documents)), window, cx)
            });
            let documents = documents.borrow().clone();
            let tabs = tabs.borrow().clone().expect("tab manager created");
            window.run_until_parked();

            for document in &documents {
                window.update(|window, cx| {
                    document.update(cx, |document, cx| document.run_query(window, cx));
                });
                window.run_until_parked();
                window.run_until_parked();
            }
            let ids = window.update(|_, cx| {
                documents
                    .iter()
                    .map(|document| document.read(cx).id())
                    .collect::<Vec<_>>()
            });
            let victims = match route {
                CloseRoute::Close | CloseRoute::CloseActive => vec![1],
                CloseRoute::CloseAll => vec![0, 1, 2],
                CloseRoute::CloseOthers => vec![0, 2],
                CloseRoute::CloseToLeft => vec![0, 1],
                CloseRoute::CloseToRight => vec![1, 2],
            };

            if route == CloseRoute::CloseAll {
                drop(documents);
            }
            window.update(|_, cx| {
                tabs.update(cx, |tabs, cx| match route {
                    CloseRoute::Close => assert!(tabs.close(ids[1], cx)),
                    CloseRoute::CloseActive => {
                        tabs.activate(ids[1], cx);
                        tabs.close_active(cx);
                    }
                    CloseRoute::CloseAll => tabs.close_all(cx),
                    CloseRoute::CloseOthers => tabs.close_others(ids[1], cx),
                    CloseRoute::CloseToLeft => tabs.close_to_left(ids[2], cx),
                    CloseRoute::CloseToRight => tabs.close_to_right(ids[0], cx),
                });
            });
            window.run_until_parked();

            let sessions = factory.sessions.lock().expect("test session collection");
            assert_eq!(sessions.len(), 3, "{route:?}");
            assert!(
                !Arc::ptr_eq(&sessions[0].connection(), &sessions[1].connection()),
                "{route:?}"
            );
            for (index, session) in sessions.iter().enumerate() {
                assert_eq!(
                    session.connection.queries.load(Ordering::SeqCst),
                    1,
                    "{route:?}, document {index} query count"
                );
                assert_eq!(
                    session.closes.load(Ordering::SeqCst),
                    usize::from(victims.contains(&index)),
                    "{route:?}, document {index}"
                );
            }
            assert_eq!(factory.opens.load(Ordering::SeqCst), 3, "{route:?}");
            assert_eq!(
                sessions
                    .iter()
                    .map(|session| session.connection.queries.load(Ordering::SeqCst))
                    .sum::<usize>(),
                3,
                "{route:?}"
            );
        }
    }

    #[gpui::test]
    fn pane_close_reports_failed_cleanup_after_document_is_released(cx: &mut gpui::TestAppContext) {
        let app_state = initialized_app_state(cx);
        let (root, factory, _) = factory_backed_root();
        factory.session.fail_close.store(true, Ordering::SeqCst);
        let document = Rc::new(RefCell::new(None));
        let tab_manager = Rc::new(RefCell::new(None));
        let mounted = Rc::new(RefCell::new(None));
        let document_ref = document.clone();
        let tab_manager_ref = tab_manager.clone();
        let mounted_ref = mounted.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    dbflux_core::QueryLanguage::Sql,
                    window,
                    cx,
                )
            });
            let tabs = cx.new(|_| TabManager::new());
            tabs.update(cx, |tabs, cx| {
                tabs.open_pane(CodeDocument::into_pane(document.clone(), cx), cx);
            });
            let mounted_documents = cx.new(|_| MountedDocuments(vec![document.clone()]));
            document_ref.replace(Some(document));
            tab_manager_ref.replace(Some(tabs));
            mounted_ref.replace(Some(mounted_documents.clone()));
            Root::new(mounted_documents, window, cx)
        });

        let document_holder = document;
        let document = document_holder.borrow().clone().expect("document created");
        let weak_document = document.downgrade();
        let tabs = tab_manager.borrow().clone().expect("tab manager created");
        let mounted_documents = mounted.borrow().clone().expect("mounted documents created");
        let binding = document.update(window, |document, _| document.execution_session.clone());
        assert!(
            binding
                .execute(root, None, &QueryRequest::new("BEGIN"))
                .result
                .is_ok()
        );

        tabs.update(window, |tabs, cx| {
            assert!(tabs.close(tabs.active_id().expect("active code tab"), cx));
        });
        mounted_documents.update(window, |mounted, cx| {
            mounted.0.clear();
            cx.notify();
        });
        document_holder.replace(None);
        tab_manager.replace(None);
        drop(document);
        drop(tabs);
        drop(mounted_documents);
        window.run_until_parked();
        window.run_until_parked();

        assert!(weak_document.upgrade().is_none());
        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 1);
        assert_eq!(
            window.update(|_, cx| app_state.read(cx).unread_error_count),
            1
        );
    }

    #[gpui::test]
    fn pane_close_closes_a_real_code_document_session(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("isolated storage runtime"),
                )
                .expect("test storage setup")
            })
        });
        let document = Rc::new(RefCell::new(None));
        let tab_manager = Rc::new(RefCell::new(None));
        let document_ref = document.clone();
        let tab_manager_ref = tab_manager.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    dbflux_core::QueryLanguage::Sql,
                    window,
                    cx,
                )
            });
            let tabs = cx.new(|_| TabManager::new());
            tabs.update(cx, |tabs, cx| {
                tabs.open_pane(CodeDocument::into_pane(document.clone(), cx), cx);
            });
            document_ref.replace(Some(document.clone()));
            tab_manager_ref.replace(Some(tabs));
            Root::new(document, window, cx)
        });

        let document = document.borrow().clone().expect("document created");
        let tabs = tab_manager.borrow().clone().expect("tab manager created");
        let (root, factory, _) = factory_backed_root();
        let binding = document.update(window, |document, _| document.execution_session.clone());
        assert!(
            binding
                .execute(root, None, &QueryRequest::new("BEGIN"))
                .result
                .is_ok()
        );

        tabs.update(window, |tabs, cx| {
            let id = tabs.active_id().expect("active code tab");
            assert!(tabs.close(id, cx));
        });
        window.run_until_parked();

        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        assert_eq!(factory.session.closes.load(Ordering::SeqCst), 1);
        let _ = window;
    }
}
