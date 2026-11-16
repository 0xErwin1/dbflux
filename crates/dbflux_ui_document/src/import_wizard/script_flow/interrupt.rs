//! Scopes "Cancel Run" to the statement it was pressed for.
//!
//! `Connection::cancel_active` has no statement scope: it stops whatever the
//! connection is doing when it arrives. The engine ends a transaction with its
//! own `ROLLBACK` on the same connection, so an interrupt that arrives late
//! could stop that rollback and leave the transaction open. The run therefore
//! goes through [`ScopedConnection`], which knows whether a statement of the
//! script or one of the engine's transaction statements is in flight, and
//! delivers an interrupt only to the former.

use std::sync::{Arc, Mutex, PoisonError};

use dbflux_core::{
    Connection, DbError, DbKind, DriverCapabilities, DriverMetadata, LanguageService, QueryHandle,
    QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TransactionVocab,
};

/// Where the run publishes the connection Cancel Run interrupts. It holds
/// the run's own connection while the run is in progress and nothing before
/// and after it, so an interrupt can never reach anything else.
pub(super) type InterruptSlot = Arc<Mutex<Option<Arc<ScopedConnection>>>>;

/// The connection to interrupt, if a run is in progress.
pub(super) fn current(slot: &InterruptSlot) -> Option<Arc<ScopedConnection>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

/// What the connection is running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    /// A statement of the script.
    Statement,
    /// The engine's `BEGIN`, `COMMIT` or `ROLLBACK`.
    Control,
}

/// What an interrupt did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Interrupted {
    /// The driver was asked to stop the statement.
    Requested,
    /// No statement of the script was running, so nothing was interrupted.
    NothingToStop,
}

/// The run's connection: the real one, plus what it is running.
pub(super) struct ScopedConnection {
    inner: Arc<dyn Connection>,
    phase: Mutex<Phase>,
}

impl ScopedConnection {
    pub(super) fn new(inner: Arc<dyn Connection>) -> Self {
        Self {
            inner,
            phase: Mutex::new(Phase::Idle),
        }
    }

    fn set_phase(&self, phase: Phase) {
        *self.phase.lock().unwrap_or_else(PoisonError::into_inner) = phase;
    }

    #[cfg(test)]
    pub(super) fn enter_for_test(&self, phase: Phase) {
        self.set_phase(phase);
    }

    /// Asks the driver to stop the statement in progress. The check and the
    /// request happen while holding the lock the next statement takes to
    /// start, so a transaction-end statement cannot begin in between and be
    /// the one that is stopped.
    pub(super) fn interrupt(&self) -> Result<Interrupted, Box<DbError>> {
        let phase = self.phase.lock().unwrap_or_else(PoisonError::into_inner);
        if *phase != Phase::Statement {
            return Ok(Interrupted::NothingToStop);
        }
        self.inner.cancel_active().map_err(Box::new)?;

        Ok(Interrupted::Requested)
    }

    fn classify(&self, sql: &str) -> Phase {
        let control = TransactionVocab::for_kind(self.inner.kind())
            .is_some_and(|vocab| [vocab.begin, vocab.commit, vocab.rollback].contains(&sql));

        if control {
            Phase::Control
        } else {
            Phase::Statement
        }
    }
}

impl Connection for ScopedConnection {
    fn metadata(&self) -> &DriverMetadata {
        self.inner.metadata()
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.inner.capabilities()
    }

    fn ping(&self) -> Result<(), DbError> {
        self.inner.ping()
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.set_phase(self.classify(&req.sql));
        let result = self.inner.execute(req);
        self.set_phase(Phase::Idle);

        result
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        self.inner.cancel(handle)
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.inner.cancel_active()
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        self.inner.schema()
    }

    fn kind(&self) -> DbKind {
        self.inner.kind()
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.inner.schema_loading_strategy()
    }

    fn dialect(&self) -> &dyn SqlDialect {
        self.inner.dialect()
    }

    fn language_service(&self) -> &dyn LanguageService {
        self.inner.language_service()
    }

    fn supports_transactional_ddl(&self) -> bool {
        self.inner.supports_transactional_ddl()
    }
}
