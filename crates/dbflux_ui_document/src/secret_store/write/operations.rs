//! The governed entry points of the write operations other than a value
//! put. Each one builds its [`SecretOperation`] attempt and hands it to the
//! shared governed path, so every operation is classified, gated, audited
//! once and scrubbed the same way a put is.

use dbflux_core::{
    SecretDeleteRequest, SecretDeletion, SecretRollback, SecretRollbackRequest,
    SecretRotationStart, SecretRotationUpdate, SecretStoreCapabilities,
};

use dbflux_core::DbError;

use super::{Attempt, SecretOperation, SecretWriteFailure, WriteContext, govern};

/// Rolls `request.name` back to `request.version_id` through the governed
/// path: classification `Write`, capabilities `WRITE` and `VERSION_LABELS`.
/// No value is rewritten.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy,
/// capability, or a request that names no version), the store's failure
/// (including rolling back to the version that is already current), or the
/// audit failure after a roll back that landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn roll_back_secret(
    context: &WriteContext<'_>,
    request: &SecretRollbackRequest,
) -> Result<SecretRollback, SecretWriteFailure> {
    let mut details = serde_json::Map::new();
    details.insert(
        "target_version".to_string(),
        serde_json::Value::String(request.version_id.clone()),
    );

    govern(
        context,
        Attempt {
            operation: SecretOperation::RollBack,
            name: &request.name,
            invalid: request
                .validate()
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: None,
            details,
            call: Box::new(|store| store.roll_back(request)),
            landed: Box::new(|rollback: &SecretRollback| {
                let mut fields = serde_json::Map::new();
                fields.insert(
                    "version".to_string(),
                    serde_json::Value::String(rollback.current_version.clone()),
                );
                if let Some(previous) = &rollback.previous_version {
                    fields.insert(
                        "previous_version".to_string(),
                        serde_json::Value::String(previous.clone()),
                    );
                }
                fields
            }),
        },
    )
}

/// Deletes `request.name` through the governed path. A scheduled deletion
/// is classified `Destructive` and needs `WRITE`, `DELETE` and
/// `SOFT_DELETE`; an irreversible one is classified `AdminDestructive`,
/// needs `WRITE` and `DELETE`, and is refused before any call when the
/// confirmation is not the exact name or when the store offers a recovery
/// window, because a secret that can be recovered is never deleted for good.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy,
/// capability, a window outside 7 to 30 days, a confirmation that is not the
/// exact name, an irreversible deletion on a store with a window), the
/// store's failure, or the audit failure after a deletion that landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn delete_secret(
    context: &WriteContext<'_>,
    request: &SecretDeleteRequest,
) -> Result<SecretDeletion, SecretWriteFailure> {
    let mut details = serde_json::Map::new();
    let (operation, invalid) = match request {
        SecretDeleteRequest::Scheduled {
            recovery_window_days,
            ..
        } => {
            details.insert(
                "recovery_window_days".to_string(),
                serde_json::Value::from(*recovery_window_days),
            );
            (
                SecretOperation::DeleteScheduled,
                request
                    .validate()
                    .err()
                    .map(|error| ("invalid_request", error)),
            )
        }
        SecretDeleteRequest::Irreversible { name, .. } => {
            details.insert("irreversible".to_string(), serde_json::Value::Bool(true));
            let store = context.store.descriptor();

            let invalid = if store.supports(SecretStoreCapabilities::SOFT_DELETE) {
                Some((
                    "recovery_window_available",
                    DbError::NotSupported(format!(
                        "Secret store \"{}\" ({}) deletes with a recovery window only; \
                         refused to delete \"{name}\" without recovery.",
                        store.label, store.id
                    )),
                ))
            } else {
                request
                    .validate()
                    .err()
                    .map(|error| ("confirmation_mismatch", error))
            };

            (SecretOperation::DeleteIrreversible, invalid)
        }
    };

    govern(
        context,
        Attempt {
            operation,
            name: request.name(),
            invalid,
            submitted: None,
            details,
            call: Box::new(|store| store.delete_secret(request)),
            landed: Box::new(|deletion: &SecretDeletion| {
                let mut fields = serde_json::Map::new();
                if let Some(when) = deletion.deletion_scheduled_for {
                    fields.insert(
                        "deletion_scheduled_for".to_string(),
                        serde_json::Value::String(when.to_rfc3339()),
                    );
                }
                fields
            }),
        },
    )
}

/// Restores `name` from a scheduled deletion through the governed path:
/// classification `Write`, capabilities `WRITE` and `SOFT_DELETE`.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy or
/// capability), the store's failure (including a secret that is not
/// scheduled for deletion), or the audit failure after a restore that
/// landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn restore_secret(context: &WriteContext<'_>, name: &str) -> Result<(), SecretWriteFailure> {
    govern(
        context,
        Attempt {
            operation: SecretOperation::Restore,
            name,
            invalid: None,
            submitted: None,
            details: serde_json::Map::new(),
            call: Box::new(|store| store.restore_secret(name)),
            landed: Box::new(|_: &()| serde_json::Map::new()),
        },
    )
}

/// Starts a rotation of `name` now through the governed path:
/// classification `Write`, capabilities `WRITE` and `ROTATION`.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy or
/// capability), the store's failure (including a secret whose rotation is
/// not enabled), or the audit failure after a rotation that started.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn rotate_secret_now(
    context: &WriteContext<'_>,
    name: &str,
) -> Result<SecretRotationStart, SecretWriteFailure> {
    govern(
        context,
        Attempt {
            operation: SecretOperation::RotateNow,
            name,
            invalid: None,
            submitted: None,
            details: serde_json::Map::new(),
            call: Box::new(|store| store.rotate_now(name)),
            landed: Box::new(|start: &SecretRotationStart| {
                let mut fields = serde_json::Map::new();
                if let Some(version) = &start.version {
                    fields.insert(
                        "version".to_string(),
                        serde_json::Value::String(version.clone()),
                    );
                }
                fields
            }),
        },
    )
}

/// Applies a rotation edit through the governed path: classification
/// `Write`, capabilities `WRITE` and `ROTATION`. The schedule, window and
/// function are validated before any call, and the audit event records the
/// settings it applied — none of them is a secret value.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy,
/// capability, or a schedule, window or function the service would
/// reject), the store's failure, or the audit failure after an edit that
/// landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn update_secret_rotation(
    context: &WriteContext<'_>,
    update: &SecretRotationUpdate,
) -> Result<(), SecretWriteFailure> {
    let mut details = serde_json::Map::new();
    details.insert(
        "enabled".to_string(),
        serde_json::Value::Bool(update.enabled),
    );
    for (key, value) in [
        ("schedule", &update.schedule),
        ("window", &update.window),
        ("function", &update.function),
    ] {
        if let Some(value) = value {
            details.insert(key.to_string(), serde_json::Value::String(value.clone()));
        }
    }

    govern(
        context,
        Attempt {
            operation: SecretOperation::EditRotation,
            name: &update.name,
            invalid: update
                .validate()
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: None,
            details,
            call: Box::new(|store| store.update_rotation(update)),
            landed: Box::new(|_: &()| serde_json::Map::new()),
        },
    )
}
