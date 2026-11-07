//! The governed write seam for secret values.
//!
//! One entry point, [`put_secret_value`], performs a secret write the way
//! the app governs its own mutations: it classifies the operation, asks the
//! connection's resolved [`MutationPolicy`] for a decision, refuses a
//! read-only or approval-required connection — and a store that does not
//! advertise `SecretStoreCapabilities::WRITE` — before the store is called,
//! and records exactly one audit event per attempt — written, refused, or
//! failed — in the same category a value retrieval uses, so a write and a
//! retrieval of one secret read as one story in the audit document. No UI
//! draws on this module; the editor layer calls it.
//!
//! The policy decision reuses the composition the data grid uses:
//! `ConnectedProfile::mutation_policy`, resolved at connect time. The grid
//! defers an approval-required mutation through the MCP approval queue; no
//! approval surface exists for secret writes yet, so this seam refuses such
//! a write instead of silently allowing it — refusing is the conservative
//! direction, and the refusal is audited so the attempt is visible.
//!
//! The audit sink is fallible, and the outcomes where that matters differ.
//! When the sink refuses the event of a refused or failed write, the write
//! did not happen and the returned error already says so; the lost audit row
//! is logged. When the sink refuses the event of a write the store *accepted*,
//! the value cannot be un-written, so the seam returns an error that says the
//! write stands and its audit trail has a hole, in the spirit of the object
//! browser's bucket deletion (the operation stands, the audit failure is
//! reported separately): the caller must surface it instead of retrying —
//! a retry would write a second version and still not repair the audit.
//!
//! Values only travel as `secrecy` types here. Refusal messages are built
//! from the policy and the secret's name, never the value, and a failure is
//! scrubbed of the submitted value — every form a service may echo it in —
//! before it reaches the audit trail or the caller, the way the AWS drivers'
//! put paths already scrub their own errors.

use dbflux_core::observability::{
    EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::secrecy::zeroize::Zeroize;
use dbflux_core::{
    DbError, ExecutionClassification, FormattedError, MutationPolicy, SecretPayload,
    SecretPutRequest, SecretStoreCapabilities, SecretStoreSource, SecretValue, SecretVersionStage,
};
use uuid::Uuid;

use crate::labels::mutation_read_only_error;

/// Audit action of a governed write that the store accepted.
pub const SECRET_VALUE_WRITTEN_ACTION: &str = "secret_value_written";

/// Audit action of a governed write the policy refused.
pub const SECRET_VALUE_WRITE_REFUSED_ACTION: &str = "secret_value_write_refused";

/// Audit action of a governed write the store failed.
pub const SECRET_VALUE_WRITE_FAILED_ACTION: &str = "secret_value_write_failed";

/// The classification of a secret value write.
///
/// [`ExecutionClassification::Write`]: the write changes stored data, so it
/// is at least a write, but version history keeps earlier versions readable
/// and a soft-delete store schedules deletions with a recovery window, so it
/// does not reach `Destructive`. The MCP approval queue takes this value
/// when an approval surface for secret writes exists.
pub const SECRET_WRITE_CLASSIFICATION: ExecutionClassification = ExecutionClassification::Write;

/// Who and what a write was for, as the audit event records it. Mirrors
/// [`crate::secret_store::value::RetrievalTarget`] so both events share
/// their shape.
pub struct WriteTarget<'a> {
    pub profile_id: Uuid,
    pub driver_id: &'a str,
    pub store_id: &'a str,
    pub store_label: &'a str,
    pub name: &'a str,
}

/// How one governed write ended, as its audit event reports it.
pub enum SecretWriteOutcome<'a> {
    /// The store accepted the write. `stage` is the stage the request asked
    /// for, `Current` when it named none.
    Written {
        version: Option<&'a str>,
        stage: SecretVersionStage,
    },
    /// A gate refused the write before the store was called. `reason` is
    /// `read_only`, `approval_required` or `store_not_writable`.
    Refused { reason: &'static str },
    /// The store failed the write. `message` is the store's own message,
    /// already scrubbed of the submitted value.
    Failed { message: &'a str },
}

/// Performs one governed secret write against `store`.
///
/// Synchronous and blocking, like `SecretStoreSource::put_value`: call it
/// from a background executor, never on the UI thread. The caller passes the
/// connection's resolved mutation policy and read-only reason —
/// `ConnectedProfile::mutation_policy` and `read_only_reason` — and an audit
/// sink, normally the app's `AuditService` behind its `EventSink`
/// implementation. The store is called at most once, and only when the
/// policy allows the write and the store accepts writes.
///
/// # Errors
///
/// Returns the policy's refusal as `DbError::PermissionDenied` when the
/// connection is read-only or approval-required, the store-level refusal of
/// `SecretStoreDescriptor::ensure_can_write` when the store does not
/// advertise `WRITE`, or the store's own error when the write itself fails —
/// scrubbed of the submitted value in every form it may be echoed in. A
/// write the store accepted whose audit event the sink refused returns the
/// audit failure instead, with a message that says the write stands; see the
/// module documentation for the tradeoff.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
pub fn put_secret_value(
    store: &dyn SecretStoreSource,
    profile_id: Uuid,
    driver_id: &str,
    policy: MutationPolicy,
    read_only_reason: Option<dbflux_core::ReadOnlyReason>,
    audit: &dyn EventSink,
    request: &SecretPutRequest,
) -> Result<SecretValue, DbError> {
    let descriptor = store.descriptor();
    let target = WriteTarget {
        profile_id,
        driver_id,
        store_id: &descriptor.id,
        store_label: &descriptor.label,
        name: &request.name,
    };

    // The gates, most fundamental first: the connection's resolved policy,
    // then the store's own write capability — front-running the
    // `ensure_can_write` check every honest `put_value` begins with, in case
    // a driver forgot its gate. Every refusal is audited before the error is
    // returned.
    if let Some((reason, message)) = policy_refusal(policy, read_only_reason, &request.name) {
        record(
            audit,
            secret_write_event(&target, SecretWriteOutcome::Refused { reason }),
        );
        return Err(DbError::permission_denied(message));
    }

    if let Err(error) = descriptor.ensure_can_write(&request.name) {
        record(
            audit,
            secret_write_event(
                &target,
                SecretWriteOutcome::Refused {
                    reason: "store_not_writable",
                },
            ),
        );
        return Err(error);
    }

    match store.put_value(request) {
        Ok(value) => {
            let event = secret_write_event(
                &target,
                SecretWriteOutcome::Written {
                    version: value.version.as_deref(),
                    stage: request.stage.unwrap_or(SecretVersionStage::Current),
                },
            );

            match audit.record(event) {
                Ok(_) => Ok(value),
                Err(error) => {
                    log::warn!("[secret store] failed to record secret write audit event: {error}");
                    Err(DbError::query_failed(format!(
                        "The write to \"{}\" landed in {}, but its audit event could not be \
                         recorded ({}). The value cannot be un-written; do not retry the write \
                         to repair the audit trail.",
                        request.name, descriptor.label, error
                    )))
                }
            }
        }
        Err(error) => {
            // Drivers scrub their own put errors; a driver that fails to
            // must not defeat the seam, so the submitted value is scrubbed
            // again here — in every form a service may have echoed it —
            // before the message reaches audit or the caller.
            let error = scrub_submitted_value(error, &request.payload);
            let message = error.to_string();
            record(
                audit,
                secret_write_event(&target, SecretWriteOutcome::Failed { message: &message }),
            );
            Err(error)
        }
    }
}

/// The refusal the resolved mutation policy imposes on a secret value
/// write, as `(audit reason, refusal message)`, or `None` when the write may
/// proceed.
///
/// A read-only profile refuses with the same message the data grid and the
/// schema diff show for a refused mutation, so every refusal of a gated
/// connection speaks with one voice. An approval-required profile refuses
/// too: the MCP approval queue defers data-grid mutations, but no approval
/// surface exists for secret values yet, and silently allowing a gated write
/// would defeat the policy the connection was resolved with.
fn policy_refusal(
    policy: MutationPolicy,
    read_only_reason: Option<dbflux_core::ReadOnlyReason>,
    name: &str,
) -> Option<(&'static str, String)> {
    match policy {
        MutationPolicy::Allowed => None,
        MutationPolicy::ReadOnly => Some((
            "read_only",
            format!(
                "{} The write to \"{name}\" was refused.",
                mutation_read_only_error(read_only_reason)
            ),
        )),
        MutationPolicy::ApprovalRequired => Some((
            "approval_required",
            format!(
                "Writes on this connection require approval, and approving secret value \
                 writes is not supported yet. The write to \"{name}\" was refused."
            ),
        )),
    }
}

/// The audit event of one governed write.
///
/// It names the connection, the store (`object_type`) and the secret
/// (`object_id`), the version when the store accepted the write, and the
/// outcome. It never carries the value or anything derived from it, such as
/// its length.
///
/// The write is its first catch site of a failure, so a failed write mints
/// the correlation id here, the way a retrieval's failure carries the id its
/// catch site created. A staged write records its stage; a `Current` write
/// does not, because becoming the current version is the default meaning of
/// every accepted write, and a store without staging labels — such as
/// Parameter Store — has no stage to name.
pub fn secret_write_event(
    target: &WriteTarget<'_>,
    outcome: SecretWriteOutcome<'_>,
) -> EventRecord {
    let (severity, result, action, summary) = match &outcome {
        SecretWriteOutcome::Written { .. } => (
            EventSeverity::Info,
            EventOutcome::Success,
            SECRET_VALUE_WRITTEN_ACTION,
            format!(
                "Wrote a new version of {} to {}",
                target.name, target.store_label
            ),
        ),
        SecretWriteOutcome::Refused { .. } => (
            EventSeverity::Warn,
            EventOutcome::Failure,
            SECRET_VALUE_WRITE_REFUSED_ACTION,
            format!(
                "Refused the write to {} in {}",
                target.name, target.store_label
            ),
        ),
        SecretWriteOutcome::Failed { .. } => (
            EventSeverity::Error,
            EventOutcome::Failure,
            SECRET_VALUE_WRITE_FAILED_ACTION,
            format!(
                "Could not write the value of {} to {}",
                target.name, target.store_label
            ),
        ),
    };

    let mut event = EventRecord::new(
        dbflux_core::chrono::Utc::now().timestamp_millis(),
        severity,
        EventCategory::SecretStore,
        result,
    )
    .with_action(action)
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref(target.store_id, target.name)
    .with_connection_context(
        target.profile_id.to_string(),
        String::new(),
        target.driver_id,
    );

    let mut details = serde_json::Map::new();
    if let Ok(classification) = serde_json::to_value(SECRET_WRITE_CLASSIFICATION) {
        details.insert("classification".to_string(), classification);
    }

    match &outcome {
        SecretWriteOutcome::Written { version, stage } => {
            if let Some(version) = version {
                details.insert(
                    "version".to_string(),
                    serde_json::Value::String(version.to_string()),
                );
            }
            if *stage == SecretVersionStage::Pending {
                details.insert(
                    "stage".to_string(),
                    serde_json::Value::String("pending".to_string()),
                );
            }
        }
        SecretWriteOutcome::Refused { reason } => {
            details.insert(
                "reason".to_string(),
                serde_json::Value::String((*reason).to_string()),
            );
        }
        SecretWriteOutcome::Failed { message } => {
            event = event
                .with_error(SECRET_VALUE_WRITE_FAILED_ACTION, message.to_string())
                .with_correlation_id(Uuid::now_v7().to_string());
        }
    }

    event.with_details_json(serde_json::Value::Object(details).to_string())
}

/// Passes the event to the sink, logging — never failing the write — when
/// the sink refuses it. A lost event on a refused or failed write is logged
/// here because the returned error already describes the outcome; a lost
/// event on an accepted write is escalated to the caller by
/// [`put_secret_value`] instead.
fn record(audit: &dyn EventSink, event: EventRecord) {
    if let Err(error) = audit.record(event) {
        log::warn!("[secret store] failed to record secret write audit event: {error}");
    }
}

/// Marker a scrubbed value leaves behind, so the message still shows the
/// reader that something was removed.
const REDACTED: &str = "[redacted]";

/// Removes a submitted value from a put error before it reaches the caller
/// or the audit trail. A service message that echoes the submitted value
/// would print the very secret the write was carrying, so every field an
/// error renders — the message, its detail and hint, the code and every
/// location field — is scrubbed of every form the value may be echoed in:
/// the text value verbatim; a binary value as its lossy UTF-8 rendering,
/// the form a service echoing received bytes would produce, and its
/// standard base64, the form a blob formatter would produce. An empty form
/// scrubs nothing, so `replace` cannot mangle the message.
///
/// The needle is borrowed straight from the `secrecy` type for text, so no
/// second plaintext copy is created; the derived forms of a binary payload
/// are zeroized as soon as the scrub is done.
///
/// The AWS drivers scrub their own put errors the same way; the seam scrubs
/// again because a driver that forgets must not leak a value through a
/// failure path this seam owns.
fn scrub_submitted_value(error: DbError, payload: &SecretPayload) -> DbError {
    match payload {
        SecretPayload::Text(text) => {
            let needle = text.expose_secret();
            scrub_value_from_message(error, needle, None)
        }
        SecretPayload::Binary(bytes) => {
            let bytes = bytes.expose_secret();
            let mut lossy = String::from_utf8_lossy(bytes).into_owned();
            let mut base64 = super::value::encode_base64(bytes);
            let error = scrub_value_from_message(error, &lossy, Some(&base64));
            lossy.zeroize();
            base64.zeroize();
            error
        }
    }
}

/// Scrubs `needle` — and `derived`, the second form of a binary payload —
/// out of every field of the error that a rendered message reaches.
fn scrub_value_from_message(error: DbError, needle: &str, derived: Option<&str>) -> DbError {
    if needle.is_empty() {
        return error;
    }

    let scrub = |field: &mut String| {
        *field = field.replace(needle, REDACTED);

        // Skips a second form that is empty or already the first one: a
        // base64 rendering equal to the lossy text would only find what the
        // first pass already removed.
        if let Some(derived) = derived.filter(|derived| !derived.is_empty() && *derived != needle) {
            *field = field.replace(derived, REDACTED);
        }
    };
    let scrub_formatted = |formatted: &mut FormattedError| {
        scrub(&mut formatted.message);
        if let Some(detail) = &mut formatted.detail {
            scrub(detail);
        }
        if let Some(hint) = &mut formatted.hint {
            scrub(hint);
        }
        if let Some(code) = &mut formatted.code {
            scrub(code);
        }
        if let Some(location) = &mut formatted.location {
            if let Some(schema) = &mut location.schema {
                scrub(schema);
            }
            if let Some(table) = &mut location.table {
                scrub(table);
            }
            if let Some(column) = &mut location.column {
                scrub(column);
            }
            if let Some(constraint) = &mut location.constraint {
                scrub(constraint);
            }
        }
    };

    match error {
        DbError::ConnectionFailed(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::ConnectionFailed(formatted)
        }
        DbError::QueryFailed(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::QueryFailed(formatted)
        }
        DbError::AuthFailed(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::AuthFailed(formatted)
        }
        DbError::ConstraintViolation(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::ConstraintViolation(formatted)
        }
        DbError::SyntaxError(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::SyntaxError(formatted)
        }
        DbError::PermissionDenied(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::PermissionDenied(formatted)
        }
        DbError::ObjectNotFound(mut formatted) => {
            scrub_formatted(&mut formatted);
            DbError::ObjectNotFound(formatted)
        }
        DbError::NotSupported(mut message) => {
            scrub(&mut message);
            DbError::NotSupported(message)
        }
        DbError::InvalidProfile(mut message) => {
            scrub(&mut message);
            DbError::InvalidProfile(message)
        }
        DbError::ValueResolutionFailed(mut message) => {
            scrub(&mut message);
            DbError::ValueResolutionFailed(message)
        }
        DbError::Parse(mut message) => {
            scrub(&mut message);
            DbError::Parse(message)
        }
        DbError::Unsupported(mut message) => {
            scrub(&mut message);
            DbError::Unsupported(message)
        }
        // An `io::Error` may carry its message inside a custom error or a
        // formatted string; the message is rebuilt with the value removed
        // and the error's kind kept.
        DbError::IoError(error) => {
            let kind = error.kind();
            let mut message = error.to_string();
            scrub(&mut message);
            DbError::IoError(std::io::Error::new(kind, message))
        }
        // Message-less variants carry nothing to scrub. Every `DbError`
        // variant is named here on purpose: a new message-carrying variant
        // fails to compile until this scrub covers it, instead of leaking
        // through a wildcard until a reviewer notices.
        DbError::Timeout => DbError::Timeout,
        DbError::Cancelled => DbError::Cancelled,
    }
}
