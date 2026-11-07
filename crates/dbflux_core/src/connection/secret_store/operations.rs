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

/// Shortest recovery window a scheduled deletion accepts, in days.
pub const MIN_RECOVERY_WINDOW_DAYS: u32 = 7;

/// Longest recovery window a scheduled deletion accepts, in days.
pub const MAX_RECOVERY_WINDOW_DAYS: u32 = 30;

/// Recovery window a deletion offers unless the user picks another.
pub const DEFAULT_RECOVERY_WINDOW_DAYS: u32 = 30;

/// A request to delete a secret.
///
/// The two ways a store deletes are two variants, so an irreversible
/// deletion cannot be built without the name typed back, and a recoverable
/// one cannot be turned into an irreversible one by a flag. A store with a
/// recovery window (`SecretStoreCapabilities::SOFT_DELETE`) takes only
/// `Scheduled`, and no store is ever asked to delete without recovery when
/// it has a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretDeleteRequest {
    /// Schedule the deletion with a recovery window, during which the secret
    /// can be restored.
    Scheduled {
        name: String,
        recovery_window_days: u32,
    },

    /// Delete with no way back. `confirmation` is the name as the user typed
    /// it, and the deletion is refused unless it equals `name` exactly.
    Irreversible { name: String, confirmation: String },
}

impl SecretDeleteRequest {
    pub fn scheduled(name: impl Into<String>, recovery_window_days: u32) -> Self {
        Self::Scheduled {
            name: name.into(),
            recovery_window_days,
        }
    }

    pub fn irreversible(name: impl Into<String>, confirmation: impl Into<String>) -> Self {
        Self::Irreversible {
            name: name.into(),
            confirmation: confirmation.into(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Scheduled { name, .. } | Self::Irreversible { name, .. } => name,
        }
    }

    /// Whether the deletion cannot be recovered.
    pub fn is_irreversible(&self) -> bool {
        matches!(self, Self::Irreversible { .. })
    }

    /// Whether the request carries the confirmation it needs: a scheduled
    /// deletion needs none, an irreversible one needs its exact name.
    pub fn confirmation_matches(&self) -> bool {
        match self {
            Self::Scheduled { .. } => true,
            Self::Irreversible { name, confirmation } => name == confirmation,
        }
    }

    /// Refuses a window outside the days the service accepts, and an
    /// irreversible deletion whose confirmation is not the exact name.
    pub fn validate(&self) -> Result<(), DbError> {
        match self {
            Self::Scheduled {
                name,
                recovery_window_days,
            } => {
                if !(MIN_RECOVERY_WINDOW_DAYS..=MAX_RECOVERY_WINDOW_DAYS)
                    .contains(recovery_window_days)
                {
                    return Err(DbError::constraint_violation(format!(
                        "The recovery window for \"{name}\" must be {MIN_RECOVERY_WINDOW_DAYS} to \
                         {MAX_RECOVERY_WINDOW_DAYS} days, not {recovery_window_days}."
                    )));
                }

                Ok(())
            }
            Self::Irreversible { name, .. } => {
                if !self.confirmation_matches() {
                    return Err(DbError::constraint_violation(format!(
                        "\"{name}\" cannot be deleted without recovery: type its exact name to confirm."
                    )));
                }

                Ok(())
            }
        }
    }
}

/// What a deletion did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretDeletion {
    /// When a scheduled deletion takes effect. `None` for a deletion that
    /// took effect at once.
    pub deletion_scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
}
