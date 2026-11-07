//! The governed entry points of the write operations other than a value
//! put. Each one builds its [`SecretOperation`] attempt and hands it to the
//! shared governed path, so every operation is classified, gated, audited
//! once and scrubbed the same way a put is.

use dbflux_core::SecretRollback;
use dbflux_core::SecretRollbackRequest;

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
