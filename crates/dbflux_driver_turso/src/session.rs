//! Isolated execution sessions: one Hrana stream per session.
//!
//! The editor keeps a session alive across runs so `BEGIN` … `COMMIT` works
//! interactively. Grid mutations and MCP calls open a session per operation
//! through `ExecutionSessionScope` and finish it when the operation ends.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use dbflux_core::{Connection, DbError, ExecutionSession, ExecutionSessionFactory};
use turso_serverless::Database;

use crate::connection::{TursoConnection, Worker};

pub(crate) struct TursoSessionFactory {
    database: Database,
    worker: Arc<Worker>,
    closed: AtomicBool,
    children: Mutex<Vec<Weak<TursoSession>>>,
}

impl TursoSessionFactory {
    pub(crate) fn new(database: Database, worker: Arc<Worker>) -> Self {
        Self {
            database,
            worker,
            closed: AtomicBool::new(false),
            children: Mutex::new(Vec::new()),
        }
    }

    fn register(&self, session: &Arc<TursoSession>) -> Result<(), DbError> {
        let mut children = self
            .children
            .lock()
            .map_err(|e| DbError::query_failed(format!("Session registry poisoned: {e}")))?;
        children.retain(|weak| weak.strong_count() > 0);
        children.push(Arc::downgrade(session));
        Ok(())
    }
}

impl ExecutionSessionFactory for TursoSessionFactory {
    fn open(&self) -> Result<Arc<dyn ExecutionSession>, DbError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DbError::connection_failed(
                "Turso connection is shut down; no new sessions can be opened",
            ));
        }

        let connection = TursoConnection::open_child(&self.database, self.worker.clone())?;
        let session = Arc::new(TursoSession {
            connection: Arc::new(connection),
            closed: AtomicBool::new(false),
        });
        self.register(&session)?;
        Ok(session)
    }

    fn shutdown(&self) -> Result<(), DbError> {
        self.closed.store(true, Ordering::SeqCst);

        let children: Vec<Arc<TursoSession>> = {
            let mut guard = self
                .children
                .lock()
                .map_err(|e| DbError::query_failed(format!("Session registry poisoned: {e}")))?;
            guard.drain(..).filter_map(|weak| weak.upgrade()).collect()
        };

        let mut first_error = None;
        for child in children {
            if let Err(error) = child.close() {
                log::warn!("Turso child session failed to close: {error}");
                first_error.get_or_insert(error);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

pub(crate) struct TursoSession {
    connection: Arc<TursoConnection>,
    closed: AtomicBool,
}

impl ExecutionSession for TursoSession {
    fn connection(&self) -> Arc<dyn Connection> {
        self.connection.clone()
    }

    fn close(&self) -> Result<(), DbError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // The server rolls back whatever is open when the stream closes; an
        // explicit rollback first keeps the outcome observable rather than
        // relying on stream expiry.
        let rollback = if self.connection.in_transaction() {
            self.connection.rollback()
        } else {
            Ok(())
        };
        let close = self.connection.close_stream();
        rollback.and(close)
    }

    /// Ends a single scoped operation. A transaction still open at this point
    /// is a bug in the caller (or an interactive `BEGIN` sent through a
    /// non-interactive surface); it is rolled back and reported instead of
    /// being silently committed or left dangling on the server.
    fn finish_operation(&self) -> Result<(), DbError> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        let leaked_transaction = self.connection.in_transaction();
        let close = self.close();

        if leaked_transaction {
            let mut message =
                "Operation finished with an open transaction; it was rolled back".to_string();
            if let Err(error) = &close {
                message.push_str(&format!(" (cleanup also failed: {error})"));
            }
            return Err(DbError::query_failed(message));
        }
        close
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}
