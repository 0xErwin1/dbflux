//! The audit trail of a script run: one `Query` event for the run and one for
//! each statement that failed, built the way the editor's query path builds
//! its events. A statement's text goes under the `query` key, which the audit
//! service replaces with a fingerprint unless the user turned query capture
//! on, and only its first line (the snippet) is ever carried, never the whole
//! statement.

use dbflux_audit::AuditService;
use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_transfer::sql_script::{
    Disposition, OnError, RunEnd, RunOptions, ScriptError, ScriptReport,
};
use uuid::Uuid;

use super::messages::is_unresolved;

/// The most failed statements recorded for one run, so a script that fails
/// everywhere cannot flood the log.
const MAX_FAILURE_EVENTS: usize = 50;

/// Where the run happened.
#[derive(Clone, Debug, Default)]
pub(super) struct AuditTarget {
    pub(super) connection_id: String,
    pub(super) database: String,
    pub(super) driver_id: String,
}

/// What the run was asked to do.
#[derive(Clone, Debug)]
pub(super) struct RunFacts {
    pub(super) file_name: String,
    pub(super) options: RunOptions,
    pub(super) statements: usize,
    pub(super) destructive_statements: usize,
}

/// The events of a finished run, the run's own first.
pub(super) fn run_events(
    target: &AuditTarget,
    facts: &RunFacts,
    result: &Result<ScriptReport, ScriptError>,
    now_ms: i64,
) -> Vec<EventRecord> {
    let correlation_id = Uuid::new_v4().to_string();
    let mut events = vec![run_event(target, facts, result, now_ms, &correlation_id)];

    if let Ok(report) = result {
        for failure in report.failures.iter().take(MAX_FAILURE_EVENTS) {
            let details = serde_json::json!({
                "script_file": facts.file_name,
                "statement_index": failure.index,
                "line": failure.line,
                "query": failure.snippet,
            });

            events.push(
                base_event(
                    target,
                    now_ms,
                    EventSeverity::Error,
                    EventOutcome::Failure,
                    &correlation_id,
                )
                .with_typed_action(audit_actions::QUERY_EXECUTE_FAILED)
                .with_summary(format!(
                    "Script statement {} failed (line {})",
                    failure.index, failure.line
                ))
                .with_error("script_statement", failure.error.clone())
                .with_details_json(details.to_string()),
            );
        }
    }

    events
}

fn run_event(
    target: &AuditTarget,
    facts: &RunFacts,
    result: &Result<ScriptReport, ScriptError>,
    now_ms: i64,
    correlation_id: &str,
) -> EventRecord {
    let (severity, outcome, action, summary, error) = classify(facts, result);
    let mut event = base_event(target, now_ms, severity, outcome, correlation_id)
        .with_typed_action(action)
        .with_summary(summary)
        .with_details_json(run_details(facts, result).to_string());

    if let Some(message) = error {
        event = event.with_error("script_run", message);
    }
    if let Ok(report) = result {
        event.duration_ms = Some(report.elapsed.as_millis() as i64);
    }

    event
}

type Classified = (
    EventSeverity,
    EventOutcome,
    dbflux_core::observability::AuditAction,
    String,
    Option<String>,
);

fn classify(facts: &RunFacts, result: &Result<ScriptReport, ScriptError>) -> Classified {
    let failed = |summary: String, error: String| {
        (
            EventSeverity::Error,
            EventOutcome::Failure,
            audit_actions::QUERY_EXECUTE_FAILED,
            summary,
            Some(error),
        )
    };

    match result {
        Err(error) => failed(
            format!("SQL script {} did not run", facts.file_name),
            error.to_string(),
        ),
        Ok(report) if is_unresolved(report) => failed(
            format!(
                "SQL script {} ended with an unresolved transaction",
                facts.file_name
            ),
            report.finish_error.clone().unwrap_or_default(),
        ),
        Ok(report) if report.end == RunEnd::Cancelled => (
            EventSeverity::Warn,
            EventOutcome::Cancelled,
            audit_actions::QUERY_CANCEL,
            format!(
                "SQL script {} cancelled after {} of {} statements",
                facts.file_name, report.ran, report.total
            ),
            None,
        ),
        Ok(report) if report.end == RunEnd::StoppedOnError || !report.failures.is_empty() => {
            let error = report
                .failures
                .first()
                .map(|failure| failure.error.clone())
                .or_else(|| report.finish_error.clone())
                .unwrap_or_default();

            failed(
                format!(
                    "SQL script {} failed: {} of {} statements ran, {} failed",
                    facts.file_name,
                    report.ran,
                    report.total,
                    report.failures.len()
                ),
                error,
            )
        }
        Ok(report) => (
            EventSeverity::Info,
            EventOutcome::Success,
            audit_actions::QUERY_EXECUTE,
            format!(
                "SQL script {} ran: {} statements",
                facts.file_name, report.ran
            ),
            None,
        ),
    }
}

fn run_details(facts: &RunFacts, result: &Result<ScriptReport, ScriptError>) -> serde_json::Value {
    let on_error = match facts.options.on_error {
        OnError::Stop => "stop",
        OnError::Continue => "continue",
    };
    let mut details = serde_json::json!({
        "script_file": facts.file_name,
        "statements": facts.statements,
        "destructive_statements": facts.destructive_statements,
        "on_error": on_error,
        "transaction": facts.options.transaction,
    });

    if let (Ok(report), Some(map)) = (result, details.as_object_mut()) {
        map.insert("ran".to_string(), report.ran.into());
        map.insert("failed".to_string(), report.failures.len().into());
        map.insert(
            "disposition".to_string(),
            format!("{:?}", report.disposition).into(),
        );
        map.insert(
            "script_transaction".to_string(),
            format!("{:?}", report.script_transaction).into(),
        );
        map.insert(
            "first_implicit_commit".to_string(),
            report.first_implicit_commit.into(),
        );
    }

    details
}

fn base_event(
    target: &AuditTarget,
    now_ms: i64,
    severity: EventSeverity,
    outcome: EventOutcome,
    correlation_id: &str,
) -> EventRecord {
    EventRecord::new(now_ms, severity, EventCategory::Query, outcome)
        .with_connection_context(
            target.connection_id.clone(),
            target.database.clone(),
            target.driver_id.clone(),
        )
        .with_origin(EventOrigin::local())
        .with_correlation_id(correlation_id.to_string())
}

/// Records the events; one that cannot be written is logged, never lost
/// silently and never allowed to hide the run's own result.
pub(super) fn record_events(service: &AuditService, events: Vec<EventRecord>) {
    for event in events {
        if let Err(error) = service.record(event) {
            log::warn!("Failed to record a SQL script audit event: {error}");
        }
    }
}
