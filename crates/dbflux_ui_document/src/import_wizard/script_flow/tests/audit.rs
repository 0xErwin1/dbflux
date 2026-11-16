use std::time::Duration;

use dbflux_transfer::sql_script::{
    Disposition, OnError, RunEnd, RunOptions, ScriptError, ScriptReport, StatementFailure,
};

use super::super::audit::{AuditTarget, RunFacts, run_events};

fn facts() -> RunFacts {
    RunFacts {
        file_name: "shop.sql".to_string(),
        options: RunOptions {
            on_error: OnError::Stop,
            transaction: true,
            read_only_profile: false,
        },
        statements: 10,
        destructive_statements: 2,
    }
}

fn target() -> AuditTarget {
    AuditTarget {
        connection_id: "conn-1".to_string(),
        database: "shop".to_string(),
        driver_id: "builtin:test".to_string(),
    }
}

fn report(end: RunEnd, disposition: Disposition) -> ScriptReport {
    ScriptReport {
        end,
        disposition,
        total: 10,
        ran: 10,
        failures: Vec::new(),
        stopped_at: None,
        transaction_used: true,
        ddl_not_transactional: false,
        finish_error: None,
        elapsed: Duration::from_millis(1_500),
    }
}

fn failure(index: usize) -> StatementFailure {
    StatementFailure {
        index,
        line: index + 1,
        snippet: format!("INSERT INTO secrets VALUES ({index})…"),
        error: "duplicate key".to_string(),
    }
}

#[test]
fn a_finished_run_records_one_query_event() {
    let events = run_events(
        &target(),
        &facts(),
        &Ok(report(RunEnd::Finished, Disposition::Committed)),
        1_000,
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "query_execute");
    assert_eq!(events[0].duration_ms, Some(1_500));
    assert_eq!(events[0].connection_id.as_deref(), Some("conn-1"));
    assert_eq!(events[0].database_name.as_deref(), Some("shop"));
    assert!(
        events[0]
            .details_json
            .as_deref()
            .unwrap_or_default()
            .contains("\"script_file\":\"shop.sql\"")
    );
}

#[test]
fn a_cancelled_run_records_a_cancel() {
    let cancelled = ScriptReport {
        stopped_at: Some(4),
        ran: 3,
        ..report(RunEnd::Cancelled, Disposition::RolledBack)
    };
    let events = run_events(&target(), &facts(), &Ok(cancelled), 1_000);

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "query_cancel");
}

#[test]
fn each_failed_statement_is_recorded_with_its_snippet_under_the_query_key() {
    let failed = ScriptReport {
        failures: vec![failure(2), failure(5)],
        ..report(RunEnd::Finished, Disposition::Kept)
    };
    let events = run_events(&target(), &facts(), &Ok(failed), 1_000);

    assert_eq!(events.len(), 3, "the run, then one per failure");
    assert_eq!(events[0].action, "query_execute_failed");
    assert_eq!(events[1].action, "query_execute_failed");

    let details: serde_json::Value =
        serde_json::from_str(events[1].details_json.as_deref().expect("details")).expect("json");
    assert_eq!(details["query"], "INSERT INTO secrets VALUES (2)…");
    assert_eq!(details["statement_index"], 2);
    assert!(
        events
            .iter()
            .all(|event| event.correlation_id == events[0].correlation_id),
        "one run, one correlation id"
    );
}

#[test]
fn a_run_that_did_not_start_records_the_failure() {
    let events = run_events(
        &target(),
        &facts(),
        &Err(ScriptError::TransactionUnavailable),
        1_000,
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "query_execute_failed");
    assert!(
        events[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("transaction")
    );
}

#[test]
fn no_more_than_fifty_failures_are_recorded() {
    let failed = ScriptReport {
        failures: (1..=80).map(failure).collect(),
        ..report(RunEnd::Finished, Disposition::Kept)
    };

    assert_eq!(
        run_events(&target(), &facts(), &Ok(failed), 1_000).len(),
        51
    );
}
