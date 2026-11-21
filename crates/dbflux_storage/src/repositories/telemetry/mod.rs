//! Transactional database-local telemetry state; never publish before commit.

use chrono::{DateTime, Utc};
use dbflux_telemetry::TelemetryState;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::borrow::Borrow;

use crate::bootstrap::OwnedConnection;

#[derive(Debug, thiserror::Error)]
pub enum TelemetryRepositoryError {
    #[error("telemetry storage unavailable")]
    Storage,
    #[error("invalid persisted telemetry state")]
    InvalidState,
    #[error("telemetry update rejected")]
    Update,
}

/// Generic over how the connection is held so that a repository owning a
/// plain, non-shared connection needs no `Arc` around a `!Sync` handle.
pub struct TelemetryRepository<C = OwnedConnection> {
    connection: C,
}

impl<C: Borrow<Connection>> TelemetryRepository<C> {
    pub fn new(connection: C) -> Self {
        Self { connection }
    }

    /// The committed state without taking a write reservation, so a caller can
    /// notice another process's change on an idle tick. `None` before the first
    /// write.
    pub fn load(&self) -> Result<Option<TelemetryState>, TelemetryRepositoryError> {
        let stored: Option<String> = self
            .connection
            .borrow()
            .query_row(
                "SELECT state_json FROM cfg_telemetry WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| TelemetryRepositoryError::Storage)?;
        let Some(json) = stored else {
            return Ok(None);
        };
        let state: TelemetryState =
            serde_json::from_str(&json).map_err(|_| TelemetryRepositoryError::InvalidState)?;
        state
            .validate()
            .map_err(|_| TelemetryRepositoryError::InvalidState)?;
        Ok(Some(state))
    }

    /// Load the latest state under a write reservation and return only after commit.
    /// No stale-cache upsert is exposed. Separate connections serialize in SQLite;
    /// contention returns a fixed error rather than publishing speculative state.
    pub fn update(
        &self,
        now: DateTime<Utc>,
        update: impl FnOnce(&mut TelemetryState) -> Result<(), TelemetryRepositoryError>,
    ) -> Result<TelemetryState, TelemetryRepositoryError> {
        let transaction =
            Transaction::new_unchecked(self.connection.borrow(), TransactionBehavior::Immediate)
                .map_err(|_| TelemetryRepositoryError::Storage)?;
        let stored: Option<(i64, String)> = transaction
            .query_row(
                "SELECT revision, state_json FROM cfg_telemetry WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| TelemetryRepositoryError::Storage)?;
        let (revision, mut state): (i64, TelemetryState) = match stored {
            Some((revision, json)) => (
                revision,
                serde_json::from_str(&json).map_err(|_| TelemetryRepositoryError::InvalidState)?,
            ),
            None => (0, TelemetryState::new(now)),
        };
        state
            .validate()
            .map_err(|_| TelemetryRepositoryError::InvalidState)?;
        update(&mut state)?;
        state
            .validate()
            .map_err(|_| TelemetryRepositoryError::InvalidState)?;
        let json =
            serde_json::to_string(&state).map_err(|_| TelemetryRepositoryError::InvalidState)?;
        let revision = revision
            .checked_add(1)
            .ok_or(TelemetryRepositoryError::InvalidState)?;
        transaction.execute(
            "INSERT INTO cfg_telemetry (id, revision, state_json) VALUES (1, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET revision = excluded.revision, state_json = excluded.state_json",
            params![revision, json],
        ).map_err(|_| TelemetryRepositoryError::Storage)?;
        transaction
            .commit()
            .map_err(|_| TelemetryRepositoryError::Storage)?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests;
