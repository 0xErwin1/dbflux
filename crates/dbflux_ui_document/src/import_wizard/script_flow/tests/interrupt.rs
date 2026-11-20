use std::sync::{Arc, Mutex};

use dbflux_core::{Connection, DbKind, QueryRequest, TransactionVocab};

use super::super::interrupt::{Interrupted, Phase, ScopedConnection};
use super::support::{FakeKind, ScriptFake};

fn vocab() -> TransactionVocab {
    TransactionVocab::for_kind(DbKind::Postgres).expect("vocabulary")
}

fn scoped(fake: &Arc<ScriptFake>) -> Arc<ScopedConnection> {
    Arc::new(ScopedConnection::new(fake.clone()))
}

#[test]
fn an_interrupt_with_no_statement_running_stops_nothing() {
    let fake = ScriptFake::new(FakeKind::SQL);
    let scope = scoped(&fake);

    assert_eq!(
        scope.interrupt().expect("interrupt"),
        Interrupted::NothingToStop
    );
    assert_eq!(fake.interrupts(), 0, "the connection was not touched");
}

#[test]
fn an_interrupt_during_a_statement_of_the_script_reaches_the_driver() {
    let fake = ScriptFake::new(FakeKind::SQL);
    let scope = scoped(&fake);
    let hook_scope = Arc::clone(&scope);
    let outcome: Arc<Mutex<Option<Interrupted>>> = Arc::default();
    let hook_outcome = Arc::clone(&outcome);
    fake.on_statement(move |_| {
        if let Ok(result) = hook_scope.interrupt() {
            *hook_outcome.lock().expect("outcome") = Some(result);
        }
    });

    scope
        .execute(&QueryRequest::new("INSERT INTO t VALUES (1)"))
        .expect("execute");

    assert_eq!(
        *outcome.lock().expect("outcome"),
        Some(Interrupted::Requested)
    );
    assert_eq!(fake.interrupts(), 1);
}

#[test]
fn an_interrupt_during_the_engines_rollback_is_not_delivered() {
    let fake = ScriptFake::new(FakeKind::SQL);
    let scope = scoped(&fake);
    let hook_scope = Arc::clone(&scope);
    fake.on_statement(move |_| {
        hook_scope.interrupt().ok();
    });

    for sql in [vocab().begin, vocab().commit, vocab().rollback] {
        scope.execute(&QueryRequest::new(sql)).expect("execute");
    }

    assert_eq!(
        fake.interrupts(),
        0,
        "a late interrupt must not stop the transaction end"
    );
}

#[test]
fn the_phase_returns_to_idle_after_each_statement() {
    let fake = ScriptFake::new(FakeKind::SQL);
    let scope = scoped(&fake);

    scope
        .execute(&QueryRequest::new("SELECT 1"))
        .expect("execute");

    assert_eq!(
        scope.interrupt().expect("interrupt"),
        Interrupted::NothingToStop
    );
    scope.enter_for_test(Phase::Control);
    assert_eq!(
        scope.interrupt().expect("interrupt"),
        Interrupted::NothingToStop
    );
    assert_eq!(fake.interrupts(), 0);
}

#[test]
fn a_driver_that_cannot_interrupt_reports_the_failure() {
    let fake = ScriptFake::new(FakeKind::SQL);
    fake.fail_interrupts();
    let scope = scoped(&fake);
    scope.enter_for_test(Phase::Statement);

    let error = scope.interrupt().expect_err("the driver refused");

    assert!(error.to_string().contains("no such query"), "{error}");
}
