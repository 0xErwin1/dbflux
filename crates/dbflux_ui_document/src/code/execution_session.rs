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
    pub(super) fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
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
    pub(super) fn close(&self) -> Result<(), DbError> {
        let mut slot = self
            .slot
            .lock()
            .map_err(|_| DbError::query_failed("editor session state is unavailable"))?;
        let current_generation = self.generation.load(Ordering::Acquire);
        match std::mem::replace(&mut *slot, SessionSlot::Empty) {
            SessionSlot::Ready {
                generation,
                session,
                ..
            } => {
                *slot = SessionSlot::Closed { generation };
                session.close()
            }
            SessionSlot::Empty => {
                *slot = SessionSlot::Closed {
                    generation: current_generation,
                };
                Ok(())
            }
            SessionSlot::Failed { generation } | SessionSlot::Closed { generation } => {
                *slot = SessionSlot::Closed { generation };
                Ok(())
            }
        }
    }
}

fn failure(message: &str, isolated: bool) -> SessionExecution {
    SessionExecution {
        result: Err(DbError::query_failed(message)),
        isolated,
    }
}

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
    use dbflux_core::{
        DbKind, ExecutionSessionFactory, QueryHandle, SchemaLoadingStrategy, SchemaSnapshot,
        SqlDialect,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct FakeConnection {
        factory: Option<Arc<FakeFactory>>,
        queries: AtomicUsize,
    }

    impl FakeConnection {
        fn root(factory: Arc<FakeFactory>) -> Arc<Self> {
            Arc::new(Self {
                factory: Some(factory),
                queries: AtomicUsize::new(0),
            })
        }

        fn isolated() -> Arc<Self> {
            Arc::new(Self {
                factory: None,
                queries: AtomicUsize::new(0),
            })
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            panic!("metadata is not used by execution-session tests")
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _request: &QueryRequest) -> Result<QueryResult, DbError> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
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
        connection: Arc<dyn Connection>,
        closed: AtomicBool,
        closes: AtomicUsize,
    }

    impl ExecutionSession for FakeSession {
        fn connection(&self) -> Arc<dyn Connection> {
            self.connection.clone()
        }

        fn close(&self) -> Result<(), DbError> {
            if !self.closed.swap(true, Ordering::SeqCst) {
                self.closes.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }

        fn finish_operation(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }
    }

    struct FakeFactory {
        session: Arc<FakeSession>,
        opens: AtomicUsize,
        fail_next_open: AtomicBool,
    }

    impl ExecutionSessionFactory for FakeFactory {
        fn open(&self) -> Result<Arc<dyn ExecutionSession>, DbError> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            if self.fail_next_open.swap(false, Ordering::SeqCst) {
                return Err(DbError::query_failed("open failed"));
            }
            Ok(self.session.clone())
        }

        fn shutdown(&self) -> Result<(), DbError> {
            Ok(())
        }
    }

    fn factory_backed_root() -> (Arc<dyn Connection>, Arc<FakeFactory>, Arc<FakeConnection>) {
        let isolated = FakeConnection::isolated();
        let factory = Arc::new(FakeFactory {
            session: Arc::new(FakeSession {
                connection: isolated.clone(),
                closed: AtomicBool::new(false),
                closes: AtomicUsize::new(0),
            }),
            opens: AtomicUsize::new(0),
            fail_next_open: AtomicBool::new(false),
        });
        (FakeConnection::root(factory.clone()), factory, isolated)
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
    fn pane_close_closes_a_real_code_document_session(cx: &mut gpui::TestAppContext) {
        use crate::code::CodeDocument;
        use crate::tab_manager::TabManager;
        use dbflux_components::theme;
        use dbflux_storage::bootstrap::StorageRuntime;
        use dbflux_ui_base::AppStateEntity;
        use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
        use gpui::AppContext;
        use gpui_component::Root;
        use std::cell::RefCell;
        use std::rc::Rc;

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
