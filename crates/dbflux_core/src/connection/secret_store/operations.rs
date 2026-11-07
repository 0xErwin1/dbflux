//! The write operations of the secret store seam beyond
//! `SecretStoreSource::put_value`: rolling a version back, deleting and
//! restoring, rotating, editing tags and the description, and creating.
//!
//! Every request type here is provider-neutral and checks the limits no
//! store accepts a call past in its own `validate`, so the governed seam
//! refuses an invalid request before any call. A request that carries a
//! value (`SecretCreateRequest`) holds it as a `secrecy` type, so `Debug`
//! never renders it.

use crate::DbError;

/// A request to make an earlier version of a secret the current one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRollbackRequest {
    /// Full name of the secret, including any path.
    pub name: String,

    /// The version to make current, in the selector `get_value` accepts.
    pub version_id: String,
}

impl SecretRollbackRequest {
    pub fn new(name: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version_id: version_id.into(),
        }
    }

    /// Refuses a request that names no version.
    pub fn validate(&self) -> Result<(), DbError> {
        if self.version_id.trim().is_empty() {
            return Err(DbError::constraint_violation(format!(
                "Rolling back \"{}\" needs the version to roll back to.",
                self.name
            )));
        }

        Ok(())
    }
}

/// What a roll back moved: no value is rewritten, only which version holds
/// the current stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRollback {
    /// The version that is current after the roll back.
    pub current_version: String,

    /// The version that was current before it, which the store keeps as the
    /// previous one.
    pub previous_version: Option<String>,
}
