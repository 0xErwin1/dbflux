use std::sync::Arc;

use super::{Connection, DbError, ExecutionSession};

/// Owns an optional isolated session for one blocking operation.
///
/// Construct and finish this scope only from a background execution context. Dropping a scope
/// deliberately performs no I/O; callers must invoke `finish` to observe cleanup failures.
pub struct ExecutionSessionScope {
    connection: Arc<dyn Connection>,
    session: Option<Arc<dyn ExecutionSession>>,
    finished: bool,
}

impl ExecutionSessionScope {
    /// Acquires one isolated session when the root connection provides a factory.
    pub fn new(root: Arc<dyn Connection>) -> Result<Self, DbError> {
        let Some(factory) = root.execution_session_factory() else {
            return Ok(Self {
                connection: root,
                session: None,
                finished: false,
            });
        };

        let session = factory.open()?;
        Ok(Self {
            connection: session.connection(),
            session: Some(session),
            finished: false,
        })
    }

    /// Returns the connection on which this operation must execute.
    pub fn connection(&self) -> Arc<dyn Connection> {
        self.connection.clone()
    }

    /// Finishes the session once and returns the operation result with any cleanup failure.
    pub fn finish<T>(&mut self, operation: Result<T, DbError>) -> Result<T, DbError> {
        if self.finished {
            return operation;
        }
        self.finished = true;

        let cleanup = match self.session.as_ref() {
            Some(session) => session.finish_operation(),
            None => Ok(()),
        };

        match (operation, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Ok(())) => Err(primary),
            (Err(primary), Err(cleanup)) => Err(compose_operation_error(primary, cleanup)),
        }
    }
}

fn compose_operation_error(primary: DbError, cleanup: DbError) -> DbError {
    let cleanup_detail = format!("Cleanup failed: {cleanup}");
    let append_detail = |mut formatted: super::FormattedError| {
        formatted.detail = Some(match formatted.detail {
            Some(existing) => format!("{existing}\n{cleanup_detail}"),
            None => cleanup_detail.clone(),
        });
        formatted
    };

    match primary {
        DbError::ConnectionFailed(formatted) => DbError::ConnectionFailed(append_detail(formatted)),
        DbError::QueryFailed(formatted) => DbError::QueryFailed(append_detail(formatted)),
        DbError::AuthFailed(formatted) => DbError::AuthFailed(append_detail(formatted)),
        DbError::ConstraintViolation(formatted) => {
            DbError::ConstraintViolation(append_detail(formatted))
        }
        DbError::SyntaxError(formatted) => DbError::SyntaxError(append_detail(formatted)),
        DbError::PermissionDenied(formatted) => DbError::PermissionDenied(append_detail(formatted)),
        DbError::ObjectNotFound(formatted) => DbError::ObjectNotFound(append_detail(formatted)),
        primary => DbError::QueryFailed(
            super::FormattedError::new(primary.to_string()).with_detail(cleanup_detail),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::ExecutionSessionFactory;
    use crate::{
        DbError, DbKind, DriverMetadata, FormattedError, QueryHandle, QueryRequest, QueryResult,
        SchemaLoadingStrategy, SchemaSnapshot,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct FakeConnection {
        factory: Option<Arc<FakeFactory>>,
        close_calls: AtomicUsize,
    }

    impl FakeConnection {
        fn without_factory() -> Arc<Self> {
            Arc::new(Self {
                factory: None,
                close_calls: AtomicUsize::new(0),
            })
        }

        fn with_factory(factory: Arc<FakeFactory>) -> Arc<Self> {
            Arc::new(Self {
                factory: Some(factory),
                close_calls: AtomicUsize::new(0),
            })
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            panic!("fake metadata is not used by execution-session tests")
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn execute(&self, _request: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported(
                "fake execution is not used".to_string(),
            ))
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("fake schema is not used".to_string()))
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn crate::SqlDialect {
            panic!("fake dialect is not used by execution-session tests")
        }

        fn execution_session_factory(&self) -> Option<&dyn ExecutionSessionFactory> {
            self.factory
                .as_deref()
                .map(|factory| factory as &dyn ExecutionSessionFactory)
        }
    }

    struct FakeSession {
        connection: Arc<dyn Connection>,
        close_calls: AtomicUsize,
        finish_calls: AtomicUsize,
        closed: AtomicBool,
        close_fails: bool,
        finish_fails: bool,
    }

    impl FakeSession {
        fn new(
            connection: Arc<dyn Connection>,
            close_fails: bool,
            finish_fails: bool,
        ) -> Arc<Self> {
            Arc::new(Self {
                connection,
                close_calls: AtomicUsize::new(0),
                finish_calls: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                close_fails,
                finish_fails,
            })
        }
    }

    impl ExecutionSession for FakeSession {
        fn connection(&self) -> Arc<dyn Connection> {
            self.connection.clone()
        }

        fn close(&self) -> Result<(), DbError> {
            if !self.closed.swap(true, Ordering::SeqCst) {
                self.close_calls.fetch_add(1, Ordering::SeqCst);
            }

            if self.close_fails {
                return Err(DbError::query_failed("close failed"));
            }

            Ok(())
        }

        fn finish_operation(&self) -> Result<(), DbError> {
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            if self.finish_fails {
                return Err(DbError::query_failed("finish failed"));
            }
            Ok(())
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }
    }

    struct FakeFactory {
        session: Mutex<Option<Arc<FakeSession>>>,
        open_calls: AtomicUsize,
        shutdown_calls: AtomicUsize,
        admission_closed: AtomicBool,
        open_fails: AtomicBool,
    }

    impl FakeFactory {
        fn new(session: Arc<FakeSession>) -> Arc<Self> {
            Arc::new(Self {
                session: Mutex::new(Some(session)),
                open_calls: AtomicUsize::new(0),
                shutdown_calls: AtomicUsize::new(0),
                admission_closed: AtomicBool::new(false),
                open_fails: AtomicBool::new(false),
            })
        }
    }

    impl ExecutionSessionFactory for FakeFactory {
        fn open(&self) -> Result<Arc<dyn ExecutionSession>, DbError> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            if self.admission_closed.load(Ordering::SeqCst) {
                return Err(DbError::query_failed("admission closed"));
            }
            if self.open_fails.load(Ordering::SeqCst) {
                return Err(DbError::query_failed("open failed"));
            }

            let session = self
                .session
                .lock()
                .expect("fake factory session mutex must not be poisoned")
                .clone()
                .expect("fake factory must have a session");
            Ok(session)
        }

        fn shutdown(&self) -> Result<(), DbError> {
            self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
            self.admission_closed.store(true, Ordering::SeqCst);
            if let Some(session) = self
                .session
                .lock()
                .expect("fake factory session mutex must not be poisoned")
                .as_ref()
            {
                session.close()?;
            }
            Ok(())
        }
    }

    fn factory_backed_root(
        close_fails: bool,
        finish_fails: bool,
    ) -> (Arc<FakeConnection>, Arc<FakeFactory>, Arc<FakeSession>) {
        let isolated = FakeConnection::without_factory();
        let session = FakeSession::new(isolated, close_fails, finish_fails);
        let factory = FakeFactory::new(session.clone());
        let root = FakeConnection::with_factory(factory.clone());
        (root, factory, session)
    }

    #[test]
    fn none_factory_returns_the_original_connection_without_cleanup() {
        let root = FakeConnection::without_factory();
        let root_connection: Arc<dyn Connection> = root.clone();
        let mut scope = ExecutionSessionScope::new(root_connection.clone()).unwrap();

        assert!(Arc::ptr_eq(&root_connection, &scope.connection()));
        assert!(scope.finish(Ok(())).is_ok());
        drop(scope);
        assert_eq!(root.close_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn factory_session_opens_once_and_finishes_once() {
        let (root, factory, session) = factory_backed_root(false, false);
        let mut scope = ExecutionSessionScope::new(root).unwrap();

        assert_eq!(factory.open_calls.load(Ordering::SeqCst), 1);
        assert!(scope.finish(Ok(())).is_ok());
        assert!(scope.finish(Ok(())).is_ok());
        assert_eq!(session.finish_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn factory_failure_does_not_create_or_cleanup_a_session() {
        let (root, factory, session) = factory_backed_root(false, false);
        factory.open_fails.store(true, Ordering::SeqCst);

        assert!(ExecutionSessionScope::new(root).is_err());
        assert_eq!(factory.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(session.finish_calls.load(Ordering::SeqCst), 0);
        assert_eq!(session.close_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn primary_failure_is_returned_unchanged_when_cleanup_succeeds() {
        let (root, _factory, _session) = factory_backed_root(false, false);
        let mut scope = ExecutionSessionScope::new(root).unwrap();

        let error = scope
            .finish::<()>(Err(DbError::query_failed("primary failed")))
            .unwrap_err();

        assert_eq!(error.to_string(), "primary failed");
    }

    #[test]
    fn cleanup_failure_is_returned_when_operation_succeeds() {
        let (root, _factory, _session) = factory_backed_root(false, true);
        let mut scope = ExecutionSessionScope::new(root).unwrap();

        let error = scope.finish(Ok(())).unwrap_err();

        assert_eq!(error.to_string(), "finish failed");
    }

    #[test]
    fn both_operation_and_cleanup_failures_are_preserved() {
        let (root, _factory, _session) = factory_backed_root(false, true);
        let mut scope = ExecutionSessionScope::new(root).unwrap();
        let primary = DbError::QueryFailed(
            FormattedError::new("primary failed")
                .with_detail("primary detail")
                .with_code("XX001"),
        );

        let error = scope.finish::<()>(Err(primary)).unwrap_err();
        let formatted = error
            .formatted()
            .expect("combined error must remain formatted");
        assert_eq!(formatted.message, "primary failed");
        assert_eq!(formatted.code.as_deref(), Some("XX001"));
        assert!(
            formatted
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("primary detail"))
        );
        assert!(
            formatted
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("finish failed"))
        );
    }

    #[test]
    fn close_is_shared_safe_and_does_not_retry_after_failure() {
        let connection = FakeConnection::without_factory();
        let session = FakeSession::new(connection, true, false);
        let clone = session.clone();

        assert!(session.close().is_err());
        assert!(clone.close().is_err());
        assert!(session.is_closed());
        assert_eq!(session.close_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_closes_retained_children_and_stops_admission() {
        let (root, factory, session) = factory_backed_root(false, false);
        let _retained_child: Arc<dyn ExecutionSession> = session.clone();
        let _scope = ExecutionSessionScope::new(root).unwrap();

        factory.shutdown().unwrap();

        assert!(session.is_closed());
        assert_eq!(session.close_calls.load(Ordering::SeqCst), 1);
        assert!(factory.open().is_err());
    }

    #[test]
    fn dropping_a_scope_performs_no_session_io() {
        let (root, _factory, session) = factory_backed_root(false, false);
        let scope = ExecutionSessionScope::new(root).unwrap();

        drop(scope);

        assert_eq!(session.finish_calls.load(Ordering::SeqCst), 0);
        assert_eq!(session.close_calls.load(Ordering::SeqCst), 0);
    }
}
