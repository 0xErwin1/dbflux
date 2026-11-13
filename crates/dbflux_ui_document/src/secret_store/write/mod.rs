//! The governed write seam for secret values.
//!
//! [`put_secret_value`] and the entry points of [`operations`] perform a
//! secret write the way the app governs its own mutations, all through one
//! governed path ([`govern`]) that differs per operation only by its
//! [`SecretOperation`] descriptor and the store call: it classifies the operation, asks the
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
use dbflux_core::{
    DbError, ExecutionClassification, MutationPolicy, SecretPayload, SecretPutRequest,
    SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource, SecretValue,
    SecretVersionStage, require_name, scrub_echoed_values, scrub_submitted_payload,
};
use uuid::Uuid;

use crate::labels::mutation_read_only_error;

mod operations;

pub use operations::{
    create_secret, delete_secret, restore_secret, roll_back_secret, rotate_secret_now,
    update_secret_description, update_secret_rotation, update_secret_tags,
};

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

/// One governed write operation: what the audit event calls it, how the
/// policy classifies it and which capabilities the store must advertise.
/// This descriptor is everything the governed path needs to know about an
/// operation besides the store call itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SecretOperation {
    /// Writing a new version of a value (`SecretStoreSource::put_value`).
    PutValue,
    /// Moving the current stage to an earlier version
    /// (`SecretStoreSource::roll_back`).
    RollBack,
    /// Deleting behind a recovery window, which the secret can be restored
    /// from (`SecretDeleteRequest::Scheduled`).
    DeleteScheduled,
    /// Deleting with no way back (`SecretDeleteRequest::Irreversible`).
    DeleteIrreversible,
    /// Cancelling a scheduled deletion (`SecretStoreSource::restore_secret`).
    Restore,
    /// Starting a rotation now (`SecretStoreSource::rotate_now`).
    RotateNow,
    /// Turning automatic rotation on or off
    /// (`SecretStoreSource::update_rotation`).
    EditRotation,
    /// Adding, changing and removing tags
    /// (`SecretStoreSource::update_tags`).
    EditTags,
    /// Changing the description
    /// (`SecretStoreSource::update_description`).
    EditDescription,
    /// Creating a secret (`SecretStoreSource::create_secret`).
    Create,
}

/// The three audit actions of one operation: the store accepted it, a gate
/// refused it before the store was called, or the store failed it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationActions {
    pub done: &'static str,
    pub refused: &'static str,
    pub failed: &'static str,
}

/// Everything that differs between operations, in one row each. The text
/// templates take `{name}`, `{store}` and, for `audit_lost`, `{error}`.
struct OperationSpec {
    classification: ExecutionClassification,
    actions: OperationActions,

    /// Every capability the store must advertise: `WRITE`, plus the ones
    /// the operation itself needs.
    required: SecretStoreCapabilities,

    /// The operation as a verb phrase, in capability refusals.
    verb: &'static str,

    /// The operation as a noun phrase naming the secret, in policy refusals.
    subject: &'static str,

    /// What the approval refusal says cannot be approved yet.
    approval_noun: &'static str,

    done_summary: &'static str,
    refused_summary: &'static str,
    failed_summary: &'static str,

    /// What landed, and why retrying it would not repair the audit trail.
    audit_lost: &'static str,
}

impl SecretOperation {
    const fn spec(self) -> OperationSpec {
        match self {
            Self::PutValue => Self::put_value_spec(),
            Self::RollBack => Self::roll_back_spec(),
            Self::DeleteScheduled => Self::delete_scheduled_spec(),
            Self::DeleteIrreversible => Self::delete_irreversible_spec(),
            Self::Restore => Self::restore_spec(),
            Self::RotateNow => Self::rotate_now_spec(),
            Self::EditRotation => Self::edit_rotation_spec(),
            Self::EditTags => Self::edit_tags_spec(),
            Self::EditDescription => Self::edit_description_spec(),
            Self::Create => Self::create_spec(),
        }
    }

    const fn put_value_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: SECRET_VALUE_WRITTEN_ACTION,
                refused: SECRET_VALUE_WRITE_REFUSED_ACTION,
                failed: SECRET_VALUE_WRITE_FAILED_ACTION,
            },
            required: write,
            verb: "write",
            subject: "write to \"{name}\"",
            approval_noun: "secret value writes",
            done_summary: "Wrote a new version of {name} to {store}",
            refused_summary: "Refused the write to {name} in {store}",
            failed_summary: "Could not write the value of {name} to {store}",
            audit_lost: "The write to \"{name}\" landed in {store}, but its audit event could \
                         not be recorded ({error}). The value cannot be un-written; do not \
                         retry the write to repair the audit trail.",
        }
    }

    const fn roll_back_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_rolled_back",
                refused: "secret_rollback_refused",
                failed: "secret_rollback_failed",
            },
            required: write.union(SecretStoreCapabilities::VERSION_LABELS),
            verb: "roll back",
            subject: "roll back of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Rolled back {name} in {store}",
            refused_summary: "Refused to roll back {name} in {store}",
            failed_summary: "Could not roll back {name} in {store}",
            audit_lost: "The roll back of \"{name}\" landed in {store}, but its audit event \
                         could not be recorded ({error}). Rolling forward again would move the \
                         labels a second time and still not repair the audit; do not retry the \
                         roll back.",
        }
    }

    /// A recoverable deletion is `Destructive`.
    const fn delete_scheduled_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Destructive,
            actions: OperationActions {
                done: "secret_delete_scheduled",
                refused: "secret_delete_refused",
                failed: "secret_delete_failed",
            },
            required: write
                .union(SecretStoreCapabilities::DELETE)
                .union(SecretStoreCapabilities::SOFT_DELETE),
            verb: "delete",
            subject: "deletion of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Scheduled the deletion of {name} in {store}",
            refused_summary: "Refused to delete {name} in {store}",
            failed_summary: "Could not delete {name} in {store}",
            audit_lost: "The deletion of \"{name}\" landed in {store} and is scheduled, but \
                         its audit event could not be recorded ({error}). Restore the secret \
                         to cancel it; do not retry the deletion to repair the audit trail.",
        }
    }

    /// A deletion with no way back is `AdminDestructive`, the class
    /// of dropping a table.
    const fn delete_irreversible_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::AdminDestructive,
            actions: OperationActions {
                done: "secret_deleted",
                refused: "secret_delete_refused",
                failed: "secret_delete_failed",
            },
            required: write.union(SecretStoreCapabilities::DELETE),
            verb: "delete",
            subject: "deletion of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Deleted {name} from {store}",
            refused_summary: "Refused to delete {name} in {store}",
            failed_summary: "Could not delete {name} in {store}",
            audit_lost: "The deletion of \"{name}\" landed in {store} and cannot be undone, \
                         but its audit event could not be recorded ({error}). Do not retry \
                         the deletion to repair the audit trail.",
        }
    }

    const fn restore_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_restored",
                refused: "secret_restore_refused",
                failed: "secret_restore_failed",
            },
            required: write.union(SecretStoreCapabilities::SOFT_DELETE),
            verb: "restore",
            subject: "restore of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Restored {name} in {store}",
            refused_summary: "Refused to restore {name} in {store}",
            failed_summary: "Could not restore {name} in {store}",
            audit_lost: "The restore of \"{name}\" landed in {store}, but its audit event \
                         could not be recorded ({error}). Do not retry the restore to repair \
                         the audit trail.",
        }
    }

    const fn rotate_now_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_rotation_started",
                refused: "secret_rotate_refused",
                failed: "secret_rotate_failed",
            },
            required: write.union(SecretStoreCapabilities::ROTATION),
            verb: "rotate",
            subject: "rotation of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Started a rotation of {name} in {store}",
            refused_summary: "Refused to rotate {name} in {store}",
            failed_summary: "Could not rotate {name} in {store}",
            audit_lost: "The rotation of \"{name}\" landed in {store} and has started, but its audit event \
                         could not be recorded ({error}). Do not start it again to repair the \
                         audit trail: that would create a second version.",
        }
    }

    const fn edit_rotation_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_rotation_updated",
                refused: "secret_rotation_update_refused",
                failed: "secret_rotation_update_failed",
            },
            required: write.union(SecretStoreCapabilities::ROTATION),
            verb: "edit the rotation of",
            subject: "rotation edit of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Updated the rotation of {name} in {store}",
            refused_summary: "Refused to update the rotation of {name} in {store}",
            failed_summary: "Could not update the rotation of {name} in {store}",
            audit_lost: "The rotation edit of \"{name}\" landed in {store}, but its audit \
                         event could not be recorded ({error}). Do not repeat the edit to \
                         repair the audit trail.",
        }
    }

    const fn edit_tags_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_tags_updated",
                refused: "secret_tags_update_refused",
                failed: "secret_tags_update_failed",
            },
            required: write.union(SecretStoreCapabilities::TAGS),
            verb: "edit the tags of",
            subject: "tag edit of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Updated the tags of {name} in {store}",
            refused_summary: "Refused to update the tags of {name} in {store}",
            failed_summary: "Could not update the tags of {name} in {store}",
            audit_lost: "The tag edit of \"{name}\" landed in {store}, but its audit event \
                         could not be recorded ({error}). Do not repeat the edit to repair \
                         the audit trail.",
        }
    }

    const fn edit_description_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_description_updated",
                refused: "secret_description_update_refused",
                failed: "secret_description_update_failed",
            },
            required: write.union(SecretStoreCapabilities::DESCRIPTION),
            verb: "edit the description of",
            subject: "description edit of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Updated the description of {name} in {store}",
            refused_summary: "Refused to update the description of {name} in {store}",
            failed_summary: "Could not update the description of {name} in {store}",
            audit_lost: "The description edit of \"{name}\" landed in {store}, but its audit \
                         event could not be recorded ({error}). Do not repeat the edit to \
                         repair the audit trail: a store that rewrites the value to change \
                         the description would create another version.",
        }
    }

    const fn create_spec() -> OperationSpec {
        let write = SecretStoreCapabilities::WRITE;

        OperationSpec {
            classification: ExecutionClassification::Write,
            actions: OperationActions {
                done: "secret_created",
                refused: "secret_create_refused",
                failed: "secret_create_failed",
            },
            required: write.union(SecretStoreCapabilities::CREATE),
            verb: "create",
            subject: "creation of \"{name}\"",
            approval_noun: "secret writes",
            done_summary: "Created {name} in {store}",
            refused_summary: "Refused to create {name} in {store}",
            failed_summary: "Could not create {name} in {store}",
            audit_lost: "The creation of \"{name}\" landed in {store}, but its audit event \
                         could not be recorded ({error}). The secret exists; do not retry the \
                         creation to repair the audit trail, because the name is taken now.",
        }
    }

    /// The classification the policy evaluates the operation under.
    pub const fn classification(self) -> ExecutionClassification {
        self.spec().classification
    }

    pub const fn actions(self) -> OperationActions {
        self.spec().actions
    }

    /// Whether the operation may change the value an open editor holds, or
    /// the version it was read from. Tags are the one that never does.
    pub const fn may_rewrite_value(self) -> bool {
        !matches!(self, Self::EditTags)
    }

    /// Every capability the store must advertise: `WRITE`, plus the ones
    /// the operation itself needs.
    pub const fn required_capabilities(self) -> SecretStoreCapabilities {
        self.spec().required
    }

    fn verb(self) -> &'static str {
        self.spec().verb
    }

    fn approval_noun(self) -> &'static str {
        self.spec().approval_noun
    }

    fn subject(self, name: &str) -> String {
        self.spec().subject.replace("{name}", name)
    }

    fn done_summary(self, target: &WriteTarget<'_>) -> String {
        fill(self.spec().done_summary, target)
    }

    fn refused_summary(self, target: &WriteTarget<'_>) -> String {
        fill(self.spec().refused_summary, target)
    }

    fn failed_summary(self, target: &WriteTarget<'_>) -> String {
        fill(self.spec().failed_summary, target)
    }

    fn audit_lost_message(
        self,
        name: &str,
        store_label: &str,
        error: &dyn std::fmt::Display,
    ) -> String {
        self.spec()
            .audit_lost
            .replace("{name}", name)
            .replace("{store}", store_label)
            .replace("{error}", &error.to_string())
    }
}

/// Fills `{name}` and `{store}` of a summary template from the target.
fn fill(template: &str, target: &WriteTarget<'_>) -> String {
    template
        .replace("{name}", target.name)
        .replace("{store}", target.store_label)
}

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
    /// connection's resolved mutation policy, the store's own capability,
    /// or a check of the request itself.
    Refused,

    /// The store failed the write. The submitted value does not exist
    /// anywhere it did not before.
    StoreFailed,

    /// The store failed in a way that does not say whether the write
    /// landed — a timeout, a dropped connection, or a response lost after
    /// the service committed. The change may exist; the caller must tell the
    /// user to check the secret before retrying, because a retry could write
    /// a second version.
    Indeterminate,

    /// The store accepted the write, but the audit event could not be
    /// recorded. The write stands — the value cannot be un-written — and a
    /// retry would write a second version while still not repairing the
    /// audit; see the module documentation for the tradeoff.
    AuditLost,
}

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

    /// The event of this refusal or failure could not be recorded, so no
    /// audit row exists to point at and the trail has a hole the caller must
    /// say so. Always `false` for [`SecretWriteFailureKind::AuditLost`],
    /// whose error already says it.
    pub audit_unrecorded: bool,
}

impl std::fmt::Display for SecretWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

/// Everything a governed write needs besides the request: the store, the
/// connection it belongs to, the connection's resolved mutation policy and
/// read-only reason — `ConnectedProfile::mutation_policy` and
/// `read_only_reason` — and an audit sink, normally the app's
/// `AuditService` behind its `EventSink` implementation.
pub struct WriteContext<'a> {
    pub store: &'a dyn SecretStoreSource,
    pub profile_id: Uuid,
    pub driver_id: &'a str,
    pub policy: MutationPolicy,
    pub read_only_reason: Option<dbflux_core::ReadOnlyReason>,
    pub audit: &'a dyn EventSink,
}

/// The store call of one attempt.
type StoreCall<'a, T> = Box<dyn FnOnce(&dyn SecretStoreSource) -> Result<T, DbError> + 'a>;

/// Builds the non-secret audit fields of an accepted outcome.
type Landed<'a, T> = Box<dyn Fn(&T) -> serde_json::Map<String, serde_json::Value> + 'a>;

/// One thing a call submits that an error must not echo back: the secret's
/// value, or request text — a tag value, a description — that may be as
/// sensitive as the value in the hands of whoever configured it.
enum Submitted<'a> {
    Payload(&'a SecretPayload),
    Text(&'a str),
}

/// One attempt of a governed operation: what it is, what it targets, the
/// request-level check that runs before any call, and the store call.
struct Attempt<'a, T> {
    operation: SecretOperation,
    name: &'a str,

    /// The refusal of a request the store should never see: the audit
    /// reason and the error the caller gets. `None` when the request is
    /// well formed.
    invalid: Option<(&'static str, DbError)>,

    /// What the call submits, so a failure that echoes any of it is
    /// scrubbed.
    submitted: Vec<Submitted<'a>>,

    /// Non-secret request fields recorded on every event of the attempt.
    details: serde_json::Map<String, serde_json::Value>,

    call: StoreCall<'a, T>,

    /// Non-secret fields of the accepted outcome, for its event.
    landed: Landed<'a, T>,
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
    let context = WriteContext {
        store,
        profile_id,
        driver_id,
        policy,
        read_only_reason,
        audit,
    };
    let stage = request.stage.unwrap_or(SecretVersionStage::Current);

    govern(
        &context,
        Attempt {
            operation: SecretOperation::PutValue,
            name: &request.name,
            invalid: require_name(&request.name)
                .err()
                .map(|error| ("invalid_request", error)),
            submitted: vec![Submitted::Payload(&request.payload)],
            details: serde_json::Map::new(),
            call: Box::new(|store| store.put_value(request)),
            landed: Box::new(move |value: &SecretValue| {
                written_details(value.version.as_deref(), stage)
            }),
        },
    )
}

/// The details of an accepted put: the version, and the stage only when it
/// is a staged write.
fn written_details(
    version: Option<&str>,
    stage: SecretVersionStage,
) -> serde_json::Map<String, serde_json::Value> {
    let mut details = serde_json::Map::new();

    if let Some(version) = version {
        details.insert(
            "version".to_string(),
            serde_json::Value::String(version.to_string()),
        );
    }
    if stage == SecretVersionStage::Pending {
        details.insert(
            "stage".to_string(),
            serde_json::Value::String("pending".to_string()),
        );
    }

    details
}

/// The one governed path every secret write takes. The gates, most
/// fundamental first: the connection's resolved policy, then the store's
/// own capabilities — front-running the check every honest store method
/// begins with, in case a driver forgot its gate — then the request's own
/// validity. Every refusal is audited before the error is returned.
#[allow(clippy::result_large_err)] // `DbError` is the crate's error type
fn govern<T>(context: &WriteContext<'_>, attempt: Attempt<'_, T>) -> Result<T, SecretWriteFailure> {
    let Attempt {
        operation,
        name,
        invalid,
        submitted,
        details,
        call,
        landed,
    } = attempt;
    let descriptor = context.store.descriptor();
    let target = WriteTarget {
        profile_id: context.profile_id,
        driver_id: context.driver_id,
        store_id: &descriptor.id,
        store_label: &descriptor.label,
        name,
    };
    let audited = AuditedAttempt {
        context,
        operation,
        target: &target,
        details: &details,
    };

    if let Some((reason, error)) = gate_refusal(context, descriptor, operation, name, invalid) {
        return Err(audited.refused(reason, error));
    }

    match call(context.store) {
        Ok(outcome) => audited.landed(outcome, landed),
        Err(error) => Err(audited.failed(error, &submitted)),
    }
}

/// The first gate that refuses the attempt, as `(audit reason, error)`.
fn gate_refusal(
    context: &WriteContext<'_>,
    descriptor: &SecretStoreDescriptor,
    operation: SecretOperation,
    name: &str,
    invalid: Option<(&'static str, DbError)>,
) -> Option<(&'static str, DbError)> {
    if let Some((reason, message)) =
        policy_refusal(context.policy, context.read_only_reason, operation, name)
    {
        return Some((reason, DbError::permission_denied(message)));
    }

    if let Err(error) =
        descriptor.ensure_can(operation.required_capabilities(), operation.verb(), name)
    {
        let reason = if descriptor.supports(SecretStoreCapabilities::WRITE) {
            "operation_not_supported"
        } else {
            "store_not_writable"
        };
        // A put keeps the store's own wording for a store that cannot
        // write at all.
        let error = if operation == SecretOperation::PutValue {
            descriptor.ensure_can_write(name).err().unwrap_or(error)
        } else {
            error
        };

        return Some((reason, error));
    }

    invalid
}

/// What one attempt records about itself: every outcome of [`govern`] ends
/// in exactly one audit event written through here.
struct AuditedAttempt<'a> {
    context: &'a WriteContext<'a>,
    operation: SecretOperation,
    target: &'a WriteTarget<'a>,
    details: &'a serde_json::Map<String, serde_json::Value>,
}

impl AuditedAttempt<'_> {
    /// A gate refused the attempt before the store was called.
    fn refused(&self, reason: &'static str, error: DbError) -> SecretWriteFailure {
        let recorded = record(
            self.context.audit,
            operation_event(
                self.operation,
                self.target,
                OperationOutcome::Refused { reason },
                self.details,
            ),
        );

        SecretWriteFailure {
            error,
            correlation_id: None,
            kind: SecretWriteFailureKind::Refused,
            audit_unrecorded: !recorded,
        }
    }

    /// The store accepted the attempt. When the sink refuses the event the
    /// write cannot be un-written, so the caller is told it stands.
    #[allow(clippy::result_large_err)] // `DbError` is the crate's error type
    fn landed<T>(&self, outcome: T, fields: Landed<'_, T>) -> Result<T, SecretWriteFailure> {
        let event = operation_event(
            self.operation,
            self.target,
            OperationOutcome::Done(fields(&outcome)),
            self.details,
        );

        match self.context.audit.record(event) {
            Ok(_) => Ok(outcome),
            Err(error) => {
                log::warn!("[secret store] failed to record secret write audit event: {error}");

                Err(SecretWriteFailure {
                    error: DbError::query_failed(self.operation.audit_lost_message(
                        self.target.name,
                        self.target.store_label,
                        &error,
                    )),
                    correlation_id: None,
                    kind: SecretWriteFailureKind::AuditLost,
                    audit_unrecorded: false,
                })
            }
        }
    }

    /// The store failed the attempt. Drivers scrub their own errors; a
    /// driver that fails to must not defeat the seam, so what the call
    /// submitted is scrubbed again here — in every form a service may have
    /// echoed it — before the message reaches audit or the caller.
    fn failed(&self, error: DbError, submitted: &[Submitted<'_>]) -> SecretWriteFailure {
        let may_have_landed = may_have_landed(&error);
        let error = scrub_submitted(error, submitted);
        let message = error.to_string();
        let event = operation_event(
            self.operation,
            self.target,
            OperationOutcome::Failed { message: &message },
            self.details,
        );
        // The failure carries the id its audit event uses, so the caller
        // can point at the same row when it reports. The event stores the
        // id as text; parse it back to the type the caller reports with.
        let correlation_id = event
            .correlation_id
            .as_deref()
            .and_then(|correlation_id| Uuid::parse_str(correlation_id).ok());

        let recorded = record(self.context.audit, event);

        // A row that was not recorded cannot be navigated to, so the id is
        // withheld.
        SecretWriteFailure {
            error,
            correlation_id: recorded.then_some(correlation_id).flatten(),
            kind: if may_have_landed {
                SecretWriteFailureKind::Indeterminate
            } else {
                SecretWriteFailureKind::StoreFailed
            },
            audit_unrecorded: !recorded,
        }
    }
}

/// Whether a store failure leaves it unknown that the write did not land: a
/// timeout, a dropped connection, or a service-less transport error the
/// driver marks retriable, as opposed to an answer from the service, which
/// says what it did.
fn may_have_landed(error: &DbError) -> bool {
    match error {
        DbError::Timeout | DbError::IoError(_) | DbError::ConnectionFailed(_) => true,
        other => other
            .formatted()
            .is_some_and(|formatted| formatted.retriable && formatted.code.is_none()),
    }
}

/// Scrubs everything the call submitted out of the error.
fn scrub_submitted(error: DbError, submitted: &[Submitted<'_>]) -> DbError {
    let texts: Vec<&str> = submitted
        .iter()
        .filter_map(|item| match item {
            Submitted::Text(text) => Some(*text),
            Submitted::Payload(_) => None,
        })
        .collect();

    let error = submitted.iter().fold(error, |error, item| match item {
        Submitted::Payload(payload) => scrub_submitted_payload(error, payload),
        Submitted::Text(_) => error,
    });

    scrub_echoed_values(error, &texts)
}

/// The refusal the resolved mutation policy imposes on a secret write, as
/// `(audit reason, refusal message)`, or `None` when the write may proceed.
///
/// A read-only profile refuses with the same message the data grid and the
/// schema diff show for a refused mutation, so every refusal of a gated
/// connection speaks with one voice. An approval-required profile refuses
/// too: the MCP approval queue defers data-grid mutations, but no approval
/// surface exists for secret writes yet, and silently allowing a gated write
/// would defeat the policy the connection was resolved with.
fn policy_refusal(
    policy: MutationPolicy,
    read_only_reason: Option<dbflux_core::ReadOnlyReason>,
    operation: SecretOperation,
    name: &str,
) -> Option<(&'static str, String)> {
    let subject = operation.subject(name);

    match policy {
        MutationPolicy::Allowed => None,
        MutationPolicy::ReadOnly => Some((
            "read_only",
            format!(
                "{} The {subject} was refused.",
                mutation_read_only_error(read_only_reason)
            ),
        )),
        MutationPolicy::ApprovalRequired => Some((
            "approval_required",
            format!(
                "Writes on this connection require approval, and approving {} \
                 is not supported yet. The {subject} was refused.",
                operation.approval_noun()
            ),
        )),
    }
}

/// How one governed operation ended, for its audit event.
enum OperationOutcome<'a> {
    /// The store accepted it; the fields are the outcome's own details.
    Done(serde_json::Map<String, serde_json::Value>),
    Refused {
        reason: &'static str,
    },
    Failed {
        message: &'a str,
    },
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
    let outcome = match outcome {
        SecretWriteOutcome::Written { version, stage } => {
            OperationOutcome::Done(written_details(version, stage))
        }
        SecretWriteOutcome::Refused { reason } => OperationOutcome::Refused { reason },
        SecretWriteOutcome::Failed { message } => OperationOutcome::Failed { message },
    };

    operation_event(
        SecretOperation::PutValue,
        target,
        outcome,
        &serde_json::Map::new(),
    )
}

/// The audit event of one governed operation, shaped like a value
/// retrieval's so both read as one story in the audit document. `request`
/// holds the non-secret fields of the request, recorded on every outcome.
fn operation_event(
    operation: SecretOperation,
    target: &WriteTarget<'_>,
    outcome: OperationOutcome<'_>,
    request: &serde_json::Map<String, serde_json::Value>,
) -> EventRecord {
    let actions = operation.actions();
    let (severity, result, action, summary) = match &outcome {
        OperationOutcome::Done(_) => (
            EventSeverity::Info,
            EventOutcome::Success,
            actions.done,
            operation.done_summary(target),
        ),
        OperationOutcome::Refused { .. } => (
            EventSeverity::Warn,
            EventOutcome::Failure,
            actions.refused,
            operation.refused_summary(target),
        ),
        OperationOutcome::Failed { .. } => (
            EventSeverity::Error,
            EventOutcome::Failure,
            actions.failed,
            operation.failed_summary(target),
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
    .with_object_ref(target.store_id, audit_object_name(target.name))
    .with_connection_context(
        target.profile_id.to_string(),
        String::new(),
        target.driver_id,
    );

    let mut details = request.clone();
    if let Ok(classification) = serde_json::to_value(operation.classification()) {
        details.insert("classification".to_string(), classification);
    }

    match outcome {
        OperationOutcome::Done(fields) => details.extend(fields),
        OperationOutcome::Refused { reason } => {
            details.insert(
                "reason".to_string(),
                serde_json::Value::String(reason.to_string()),
            );
        }
        OperationOutcome::Failed { message } => {
            event = event
                .with_error(actions.failed, message.to_string())
                .with_correlation_id(Uuid::now_v7().to_string());
        }
    }

    event.with_details_json(serde_json::Value::Object(details).to_string())
}

/// The name an audit event records for a target. The audit service refuses
/// an event without an object id, and a refusal of a blank name is exactly
/// the event that must still be recorded.
fn audit_object_name(name: &str) -> &str {
    if name.trim().is_empty() {
        "(blank name)"
    } else {
        name
    }
}

/// The failure of a write whose connection has no such store. The write never
/// reached the seam, so nothing recorded it: this records the attempt as a
/// failure of `operation` on the secret and the connection `target` names, and
/// hands back the failure carrying that event's correlation id, so the caller
/// reports the same row the audit trail holds.
pub(super) fn store_unavailable_failure(
    audit: &dyn EventSink,
    operation: SecretOperation,
    target: &WriteTarget<'_>,
    error: DbError,
) -> SecretWriteFailure {
    let message = error.to_string();
    let event = operation_event(
        operation,
        target,
        OperationOutcome::Failed { message: &message },
        &serde_json::Map::new(),
    );
    let correlation_id = event
        .correlation_id
        .as_deref()
        .and_then(|correlation_id| Uuid::parse_str(correlation_id).ok());
    let recorded = record(audit, event);

    SecretWriteFailure {
        error,
        correlation_id: recorded.then_some(correlation_id).flatten(),
        kind: SecretWriteFailureKind::StoreFailed,
        audit_unrecorded: !recorded,
    }
}

/// Passes the event to the sink, logging — never failing the write — when
/// the sink refuses it. A lost event on a refused or failed write is logged
/// here because the returned error already describes the outcome; a lost
/// event on an accepted write is escalated to the caller by [`govern`]
/// instead.
fn record(audit: &dyn EventSink, event: EventRecord) -> bool {
    match audit.record(event) {
        Ok(_) => true,
        Err(error) => {
            log::warn!("[secret store] failed to record secret write audit event: {error}");
            false
        }
    }
}
