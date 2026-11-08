//! The governed entry points of the write operations other than a value
//! put. Each one builds its [`SecretOperation`] attempt and hands it to the
//! shared governed path, so every operation is classified, gated, audited
//! once and scrubbed the same way a put is.

use dbflux_core::{
    SecretDeleteRequest, SecretDeletion, SecretDescriptionChange, SecretDescriptionUpdate,
    SecretRollback, SecretRollbackRequest, SecretRotationChange, SecretRotationStart,
    SecretRotationUpdate, SecretStoreCapabilities, SecretTagsUpdate,
};

use dbflux_core::{DbError, SecretCreateRequest, SecretCreated, require_name};

use super::{Attempt, SecretOperation, SecretWriteFailure, Submitted, WriteContext, govern};

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
            submitted: Vec::new(),
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
            submitted: Vec::new(),
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
            invalid: require_name(name)
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: Vec::new(),
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
            invalid: require_name(name)
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: Vec::new(),
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
) -> Result<SecretRotationChange, SecretWriteFailure> {
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
            submitted: Vec::new(),
            details,
            call: Box::new(|store| store.update_rotation(update)),
            landed: Box::new(|change: &SecretRotationChange| {
                let mut fields = serde_json::Map::new();
                if let Some(version) = &change.unfinished_version {
                    fields.insert(
                        "unfinished_version".to_string(),
                        serde_json::Value::String(version.clone()),
                    );
                }
                fields
            }),
        },
    )
}

/// Applies a tag edit through the governed path: classification `Write`,
/// capabilities `WRITE` and `TAGS`. Keys and values are validated against
/// the services' limits before any call. The audit event records the keys
/// set and removed, never the values.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy,
/// capability, or a tag the service would reject), the store's failure, or
/// the audit failure after an edit that landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn update_secret_tags(
    context: &WriteContext<'_>,
    update: &SecretTagsUpdate,
) -> Result<(), SecretWriteFailure> {
    let mut details = serde_json::Map::new();
    details.insert(
        "set_keys".to_string(),
        serde_json::Value::from(
            update
                .set
                .iter()
                .map(|tag| tag.key.clone())
                .collect::<Vec<_>>(),
        ),
    );
    details.insert(
        "removed_keys".to_string(),
        serde_json::Value::from(update.remove.clone()),
    );

    govern(
        context,
        Attempt {
            operation: SecretOperation::EditTags,
            name: &update.name,
            invalid: update
                .validate()
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: update
                .set
                .iter()
                .map(|added| Submitted::Text(&added.value))
                .collect(),
            details,
            call: Box::new(|store| store.update_tags(update)),
            landed: Box::new(|_: &()| serde_json::Map::new()),
        },
    )
}

/// Changes a description through the governed path: classification
/// `Write`, capabilities `WRITE` and `DESCRIPTION`. A store that changes it
/// only by writing the value again creates a version, and the audit event
/// names it, so the extra version is never a surprise in the trail. The
/// description text is not recorded.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy or
/// capability), the store's failure (including a store that cannot keep the
/// value, type and key intact), or the audit failure after a change that
/// landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn update_secret_description(
    context: &WriteContext<'_>,
    update: &SecretDescriptionUpdate,
) -> Result<SecretDescriptionChange, SecretWriteFailure> {
    let mut details = serde_json::Map::new();
    details.insert(
        "cleared".to_string(),
        serde_json::Value::Bool(update.description.is_empty()),
    );

    govern(
        context,
        Attempt {
            operation: SecretOperation::EditDescription,
            name: &update.name,
            invalid: update
                .validate()
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: vec![Submitted::Text(&update.description)],
            details,
            call: Box::new(|store| store.update_description(update)),
            landed: Box::new(|change: &SecretDescriptionChange| {
                let mut fields = serde_json::Map::new();
                if let Some(version) = &change.version {
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

/// Creates a secret through the governed path: classification `Write`,
/// capabilities `WRITE` and `CREATE`. It never overwrites: the store refuses
/// a name that exists, and the failure says so. The initial value is
/// scrubbed from any failure that echoes it, in every form a value may be
/// echoed in, exactly like a value put; the audit event records the type,
/// tier, key and tag keys, and never the value.
///
/// # Errors
///
/// A [`SecretWriteFailure`]: a refusal before any store call (policy,
/// capability, or a request no store accepts), the store's failure
/// (including a name that is taken) scrubbed of the value, or the audit
/// failure after a creation that landed.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn create_secret(
    context: &WriteContext<'_>,
    request: &SecretCreateRequest,
) -> Result<SecretCreated, SecretWriteFailure> {
    let mut details = serde_json::Map::new();
    for (key, value) in [
        ("value_type", &request.value_type),
        ("tier", &request.tier),
        ("encryption_key", &request.encryption_key),
    ] {
        if let Some(value) = value {
            details.insert(key.to_string(), serde_json::Value::String(value.clone()));
        }
    }
    details.insert(
        "has_description".to_string(),
        serde_json::Value::Bool(request.description.is_some()),
    );
    details.insert(
        "tag_keys".to_string(),
        serde_json::Value::from(
            request
                .tags
                .iter()
                .map(|tag| tag.key.clone())
                .collect::<Vec<_>>(),
        ),
    );

    govern(
        context,
        Attempt {
            operation: SecretOperation::Create,
            name: &request.name,
            invalid: request
                .validate()
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: create_submissions(request),
            details,
            call: Box::new(|store| store.create_secret(request)),
            landed: Box::new(|created: &SecretCreated| {
                let mut fields = serde_json::Map::new();
                if let Some(version) = &created.version {
                    fields.insert(
                        "version".to_string(),
                        serde_json::Value::String(version.clone()),
                    );
                }
                if let Some(resource_id) = &created.resource_id {
                    fields.insert(
                        "resource_id".to_string(),
                        serde_json::Value::String(resource_id.clone()),
                    );
                }
                fields
            }),
        },
    )
}

/// Everything a create submits that an error must not echo: the value, the
/// description and the tag values.
fn create_submissions(request: &SecretCreateRequest) -> Vec<Submitted<'_>> {
    let mut submitted = vec![Submitted::Payload(&request.payload)];

    if let Some(description) = &request.description {
        submitted.push(Submitted::Text(description));
    }
    submitted.extend(
        request
            .tags
            .iter()
            .map(|added| Submitted::Text(&added.value)),
    );

    submitted
}
