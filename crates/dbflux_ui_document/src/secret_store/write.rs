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

/// Which of the seam's three failing outcomes a [`SecretWriteFailure`]
/// carries. The caller maps its report on this verdict, never on the error
/// variant: a refusal is a decision, a store failure is a transient error,
/// and a landed write whose audit row was lost is neither — it must not be
/// presented as something to retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretWriteFailureKind {
    /// A gate refused the write before the store was called: the
    /// connection's resolved mutation policy, or the store's own write
    /// capability.
    Refused,

    /// The store failed the write. The submitted value does not exist
    /// anywhere it did not before.
    StoreFailed,

    /// The store accepted the write, but the audit event could not be
    /// recorded. The write stands — the value cannot be un-written — and a
    /// retry would write a second version while still not repairing the
    /// audit; see the module documentation for the tradeoff.
    AuditLost,
}

/// Performs one governed secret write against `store`.
///
/// The failure side of one governed write.
///
/// The write's own error — a policy refusal, the store-level capability
/// refusal, or the store's scrubbed failure — travels next to the
/// correlation id of the failure audit event the seam recorded for it,
/// when one was. The caller reports with the same id, so its toast and the
/// audit row tell one story, the way a retrieval failure does.
#[derive(Debug)]
pub struct SecretWriteFailure {
    /// Why the write did not complete, or why the audit trail has a hole
    /// after a write the store accepted.
    pub error: DbError,

    /// The correlation id of the recorded failure event. Set for a store
    /// failure; a refusal, or an audit event the sink refused after a landed
    /// write, carries none, because no failure event was recorded.
    pub correlation_id: Option<Uuid>,

    /// Which failing outcome this is, as the seam classified it. The caller
    /// maps its report on this verdict, never on the error variant.
    pub kind: SecretWriteFailureKind,
}

impl std::fmt::Display for SecretWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

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
/// Returns [`SecretWriteFailure`] carrying the policy's refusal as
/// `DbError::PermissionDenied` when the connection is read-only or
/// approval-required, the store-level refusal of
/// `SecretStoreDescriptor::ensure_can_write` when the store does not
/// advertise `WRITE`, or the store's own error when the write itself fails —
/// scrubbed of the submitted value in every form it may be echoed in, and
/// paired with the correlation id of its recorded failure event. A write the
/// store accepted whose audit event the sink refused returns the audit
/// failure instead, with a message that says the write stands; see the
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
) -> Result<SecretValue, SecretWriteFailure> {
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
        return Err(SecretWriteFailure {
            error: DbError::permission_denied(message),
            correlation_id: None,
            kind: SecretWriteFailureKind::Refused,
        });
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
        return Err(SecretWriteFailure {
            error,
            correlation_id: None,
            kind: SecretWriteFailureKind::Refused,
        });
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
                    Err(SecretWriteFailure {
                        error: DbError::query_failed(format!(
                            "The write to \"{}\" landed in {}, but its audit event could not be \
                             recorded ({}). The value cannot be un-written; do not retry the write \
                             to repair the audit trail.",
                            request.name, descriptor.label, error
                        )),
                        correlation_id: None,
                        kind: SecretWriteFailureKind::AuditLost,
                    })
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
            let event =
                secret_write_event(&target, SecretWriteOutcome::Failed { message: &message });
            // The failure carries the id its audit event uses, so the
            // caller can point at the same row when it reports. The event
            // stores the id as text; parse it back to the type the caller
            // reports with.
            let correlation_id = event
                .correlation_id
                .as_deref()
                .and_then(|correlation_id| Uuid::parse_str(correlation_id).ok());
            record(audit, event);
            Err(SecretWriteFailure {
                error,
                correlation_id,
                kind: SecretWriteFailureKind::StoreFailed,
            })
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

/// The JSON-escaped form of a value: what a serializer writes for it inside
/// a JSON body, without the surrounding quotes — newlines as `\n`, quotes
/// as `\"`, backslashes as `\\`, tabs as `\t`, other control characters as
/// `\uXXXX`. A service or a client echoing a request body leaks the value
/// in this form too. Returns an empty string when the escape cannot be
/// produced or adds nothing over the verbatim value; an empty form scrubs
/// nothing.
fn json_escaped(value: &str) -> String {
    let Ok(quoted) = serde_json::to_string(value) else {
        return String::new();
    };

    if quoted.len() >= 2 && quoted.starts_with('"') && quoted.ends_with('"') {
        quoted[1..quoted.len() - 1].to_string()
    } else {
        String::new()
    }
}

/// Removes a submitted value from a put error before it reaches the caller
/// or the audit trail. A service message that echoes the submitted value
/// would print the very secret the write was carrying, so every field an
/// error renders — the message, its detail and hint, the code and every
/// location field — is scrubbed of every form the value may be echoed in:
/// the text value verbatim and the JSON-escaped form a serializer writes
/// inside a request body; a binary value as its lossy UTF-8 rendering, the
/// form a service echoing received bytes would produce, and its standard
/// base64, the form a blob formatter would produce, each also escaped. An
/// empty form scrubs nothing, so `replace` cannot mangle the message.
///
/// The verbatim text needle is borrowed straight from the `secrecy` type, so
/// no second plaintext copy is created; every form that had to be built is
/// zeroized as soon as the scrub is done.
///
/// The AWS drivers scrub their own put errors the same way; the seam scrubs
/// again because a driver that forgets must not leak a value through a
/// failure path this seam owns.
fn scrub_submitted_value(error: DbError, payload: &SecretPayload) -> DbError {
    match payload {
        SecretPayload::Text(text) => {
            let verbatim = text.expose_secret();
            let mut escaped = json_escaped(verbatim);
            let error = scrub_value_from_message(error, &[verbatim, &escaped]);
            escaped.zeroize();
            error
        }
        SecretPayload::Binary(bytes) => {
            let bytes = bytes.expose_secret();
            let mut lossy = String::from_utf8_lossy(bytes).into_owned();
            let mut base64 = super::value::encode_base64(bytes);
            let mut lossy_escaped = json_escaped(&lossy);
            let mut base64_escaped = json_escaped(&base64);
            let error = scrub_value_from_message(
                error,
                &[&lossy, &lossy_escaped, &base64, &base64_escaped],
            );
            for buffer in [
                &mut lossy,
                &mut base64,
                &mut lossy_escaped,
                &mut base64_escaped,
            ] {
                buffer.zeroize();
            }
            error
        }
    }
}

/// Scrubs each needle out of a message-carrying field, skipping a needle
/// that is empty — it would match everything `replace` sees — or equal to
/// the marker itself. A needle that equals an earlier one finds nothing
/// new and is harmless.
fn scrub_string_value(field: &mut String, needles: &[&str]) {
    for needle in needles {
        if needle.is_empty() || *needle == REDACTED {
            continue;
        }

        *field = field.replace(needle, REDACTED);
    }
}

/// Scrubs every field of a formatted error that a rendered message reaches:
/// the message, its detail and hint, the code and every location field.
fn scrub_formatted_error(formatted: &mut FormattedError, needles: &[&str]) {
    scrub_string_value(&mut formatted.message, needles);

    if let Some(detail) = &mut formatted.detail {
        scrub_string_value(detail, needles);
    }
    if let Some(hint) = &mut formatted.hint {
        scrub_string_value(hint, needles);
    }
    if let Some(code) = &mut formatted.code {
        scrub_string_value(code, needles);
    }
    if let Some(location) = &mut formatted.location {
        if let Some(schema) = &mut location.schema {
            scrub_string_value(schema, needles);
        }
        if let Some(table) = &mut location.table {
            scrub_string_value(table, needles);
        }
        if let Some(column) = &mut location.column {
            scrub_string_value(column, needles);
        }
        if let Some(constraint) = &mut location.constraint {
            scrub_string_value(constraint, needles);
        }
    }
}

/// Scrubs the submitted value, in every form it may be echoed in, out of
/// the error the store returned. Every `DbError` variant is named here on
/// purpose: a new message-carrying variant fails to compile until this
/// scrub covers it, instead of leaking through a wildcard until a reviewer
/// notices.
fn scrub_value_from_message(error: DbError, needles: &[&str]) -> DbError {
    match error {
        DbError::ConnectionFailed(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::ConnectionFailed(formatted)
        }
        DbError::QueryFailed(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::QueryFailed(formatted)
        }
        DbError::AuthFailed(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::AuthFailed(formatted)
        }
        DbError::ConstraintViolation(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::ConstraintViolation(formatted)
        }
        DbError::SyntaxError(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::SyntaxError(formatted)
        }
        DbError::PermissionDenied(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::PermissionDenied(formatted)
        }
        DbError::ObjectNotFound(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::ObjectNotFound(formatted)
        }
        DbError::NotSupported(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::NotSupported(message)
        }
        DbError::InvalidProfile(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::InvalidProfile(message)
        }
        DbError::ValueResolutionFailed(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::ValueResolutionFailed(message)
        }
        DbError::Parse(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::Parse(message)
        }
        DbError::Unsupported(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::Unsupported(message)
        }
        // An `io::Error` may carry its message inside a custom error or a
        // formatted string; the message is rebuilt with the value removed
        // and the error's kind kept.
        DbError::IoError(error) => {
            let kind = error.kind();
            let mut message = error.to_string();
            scrub_string_value(&mut message, needles);
            DbError::IoError(std::io::Error::new(kind, message))
        }
        // Message-less variants cannot carry any form of the value.
        DbError::Timeout => DbError::Timeout,
        DbError::Cancelled => DbError::Cancelled,
    }
}
