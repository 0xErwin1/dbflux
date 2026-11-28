//! What the audit log records for a run whose task was cancelled.
//!
//! A cancel is a request: the statement can still finish or fail before the
//! driver observes it, and the log must say what the statement did.

use dbflux_core::observability::EventOutcome;
use dbflux_core::observability::actions::{self as audit_actions, AuditAction};
use dbflux_core::{DbError, QueryResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CancelledRunAudit {
    pub action: AuditAction,
    pub outcome: EventOutcome,
    pub summary: String,
    pub error: Option<String>,
}

/// `Cancelled` only when the worker itself reported `DbError::Cancelled`;
/// any other result is recorded as the statement's own outcome.
pub(super) fn audit_for_cancelled_run(
    result: &Result<QueryResult, DbError>,
    is_script: bool,
) -> CancelledRunAudit {
    match result {
        Err(DbError::Cancelled) => CancelledRunAudit {
            action: audit_actions::QUERY_CANCEL,
            outcome: EventOutcome::Cancelled,
            summary: if is_script {
                "Script cancelled"
            } else {
                "Query cancelled"
            }
            .to_string(),
            error: None,
        },
        Ok(query_result) => {
            let (count, label) = match query_result.affected_rows {
                Some(affected) => (affected, "affected"),
                None => (query_result.rows.len() as u64, "returned"),
            };

            CancelledRunAudit {
                action: if is_script {
                    audit_actions::SCRIPT_EXECUTE
                } else {
                    audit_actions::QUERY_EXECUTE
                },
                outcome: EventOutcome::Success,
                summary: format!("Query executed successfully: {count} rows {label}"),
                error: None,
            }
        }
        Err(error) => CancelledRunAudit {
            action: if is_script {
                audit_actions::SCRIPT_EXECUTE_FAILED
            } else {
                audit_actions::QUERY_EXECUTE_FAILED
            },
            outcome: EventOutcome::Failure,
            summary: format!("Query failed: {error}"),
            error: Some(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn rows(count: usize) -> QueryResult {
        QueryResult::table(Vec::new(), vec![Vec::new(); count], None, Duration::ZERO)
    }

    #[test]
    fn a_driver_cancellation_is_recorded_as_cancelled() {
        let audit = audit_for_cancelled_run(&Err(DbError::Cancelled), false);

        assert_eq!(audit.action, audit_actions::QUERY_CANCEL);
        assert_eq!(audit.outcome, EventOutcome::Cancelled);
        assert_eq!(audit.summary, "Query cancelled");
        assert_eq!(audit.error, None);
    }

    #[test]
    fn a_cancelled_script_keeps_its_own_summary() {
        let audit = audit_for_cancelled_run(&Err(DbError::Cancelled), true);

        assert_eq!(audit.action, audit_actions::QUERY_CANCEL);
        assert_eq!(audit.summary, "Script cancelled");
    }

    #[test]
    fn a_statement_that_finished_anyway_is_recorded_as_a_success() {
        let audit = audit_for_cancelled_run(&Ok(rows(3)), false);

        assert_eq!(audit.action, audit_actions::QUERY_EXECUTE);
        assert_eq!(audit.outcome, EventOutcome::Success);
        assert_eq!(
            audit.summary,
            "Query executed successfully: 3 rows returned"
        );
        assert_eq!(audit.error, None);
    }

    #[test]
    fn a_script_that_finished_anyway_is_recorded_as_a_script_success() {
        let audit = audit_for_cancelled_run(&Ok(rows(1)), true);

        assert_eq!(audit.action, audit_actions::SCRIPT_EXECUTE);
        assert_eq!(audit.outcome, EventOutcome::Success);
    }

    #[test]
    fn a_statement_that_failed_before_the_cancel_landed_is_recorded_as_a_failure() {
        let audit = audit_for_cancelled_run(&Err(DbError::NotSupported("boom".to_string())), false);
        let error = DbError::NotSupported("boom".to_string());

        assert_eq!(audit.action, audit_actions::QUERY_EXECUTE_FAILED);
        assert_eq!(audit.outcome, EventOutcome::Failure);
        assert_eq!(audit.summary, format!("Query failed: {error}"));
        assert_eq!(audit.error, Some(error.to_string()));
    }
}
