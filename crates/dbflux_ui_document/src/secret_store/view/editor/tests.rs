use super::*;
use dbflux_core::DbError;
use dbflux_core::observability::EventSeverity;
use dbflux_ui_base::user_error::ErrorKind;

/// The audit-loss failure the seam builds for a landed write whose audit
/// event the sink refused says the write stands, and the editor reports it
/// as an audit-storage problem at warning severity — never as a store
/// failure a user might answer by retrying.
#[test]
fn an_audit_loss_after_a_landed_write_says_the_write_landed() {
    let failure = SecretWriteFailure {
        error: DbError::query_failed(
            "The write to \"prod/notify/webhook-token\" landed in Secrets Manager, but its \
             audit event could not be recorded (sink closed). The value cannot be un-written; \
             do not retry the write to repair the audit trail.",
        ),
        correlation_id: None,
        kind: SecretWriteFailureKind::AuditLost,
        audit_unrecorded: false,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert!(
        user_error.summary.contains("landed"),
        "{}",
        user_error.summary
    );
    assert!(!user_error.summary.contains("rotated webhook token 77b1"));
    assert_eq!(user_error.kind, ErrorKind::Storage);
    assert_eq!(user_error.severity, EventSeverity::Warn);
}

/// The correlation id a store failure carries replaces the fresh one the
/// user-facing error mints, so the toast points at the audit row.
#[test]
fn a_reported_write_failure_keeps_the_correlation_id_its_event_recorded() {
    let id = uuid::Uuid::now_v7();
    let failure = SecretWriteFailure {
        error: DbError::permission_denied("put was denied"),
        correlation_id: Some(id),
        kind: SecretWriteFailureKind::StoreFailed,
        audit_unrecorded: false,
    };

    assert_eq!(write_failure_to_user_facing(&failure).correlation_id, id);
}

/// A refusal — here the store's own write capability, the same verdict the
/// policy refusals carry — reads as a decision: the `User` kind at warning
/// severity.
#[test]
fn a_refused_write_reports_as_a_decision() {
    let failure = SecretWriteFailure {
        error: DbError::NotSupported(
            "Secret store \"Secrets Manager\" (secrets-manager) does not support writing \
             values; refused the write to \"prod/notify/webhook-token\"."
                .to_string(),
        ),
        correlation_id: None,
        kind: SecretWriteFailureKind::Refused,
        audit_unrecorded: false,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert_eq!(user_error.kind, ErrorKind::User);
    assert_eq!(user_error.severity, EventSeverity::Warn);
    assert!(user_error.summary.contains("does not support writing"));
}

/// A store failure reports through the driver-facing path even when its
/// error variant is one a refusal also uses: "the connection has no store
/// X" — reachable by disconnecting while the editor is open — is a lost
/// connection, not a gate's decision. This probe fails again if the
/// mapping ever keys on the variant instead of the seam's verdict.
#[test]
fn a_store_failure_reports_as_a_driver_error_even_when_its_variant_looks_like_a_refusal() {
    let failure = SecretWriteFailure {
        error: DbError::NotSupported("The connection has no store secrets-manager".to_string()),
        correlation_id: None,
        kind: SecretWriteFailureKind::StoreFailed,
        audit_unrecorded: false,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert_eq!(user_error.kind, ErrorKind::Driver);
    assert_eq!(user_error.severity, EventSeverity::Error);
}

/// A failure that may have landed, and one whose own audit event was lost,
/// each say so in the message the user reads.
#[test]
fn an_indeterminate_or_unaudited_failure_tells_the_user_what_to_check() {
    let failure = SecretWriteFailure {
        error: DbError::Timeout,
        correlation_id: None,
        kind: SecretWriteFailureKind::Indeterminate,
        audit_unrecorded: true,
    };

    let user_error = write_failure_to_user_facing(&failure);

    assert!(
        user_error.summary.contains("may have applied"),
        "{}",
        user_error.summary
    );
    assert!(
        user_error.summary.contains("audit trail"),
        "{}",
        user_error.summary
    );
    assert_eq!(user_error.severity, EventSeverity::Warn);
}
