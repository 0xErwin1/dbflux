//! A script runs on an isolated session when the driver offers one, so other
//! work on the profile's connection cannot reach its statements or its
//! transaction, and Cancel Run can only reach the script's own session.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use dbflux_core::CancelToken;
use dbflux_transfer::sql_script::{RunOptions, SqlScript};

use super::super::run::{SessionRun, run_in_session};
use super::support::{FakeKind, ScriptFake};
use dbflux_core::QueryLanguage;

fn script() -> SqlScript {
    SqlScript::from_text(
        "INSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\n",
        &QueryLanguage::Sql,
    )
    .expect("script")
}

fn run(root: Arc<ScriptFake>) -> SessionRun {
    run_in_session(
        root,
        &script(),
        &RunOptions::default(),
        &CancelToken::new(),
        &Arc::default(),
        |_| {},
    )
}

#[test]
fn the_script_runs_on_the_isolated_session_and_not_on_the_shared_connection() {
    let session = ScriptFake::new(FakeKind::SQL);
    let (root, probe) = ScriptFake::with_sessions(FakeKind::SQL, session.clone());

    let outcome = run(root.clone());

    assert!(matches!(outcome, SessionRun::Ran { .. }));
    assert!(
        root.executed().is_empty(),
        "the shared connection is untouched: {:?}",
        root.executed()
    );
    assert_eq!(session.executed().len(), 2);
    assert_eq!(probe.opened.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.finished.load(Ordering::SeqCst),
        1,
        "the session is released"
    );
}

#[test]
fn a_connection_without_sessions_runs_the_script_on_itself() {
    let root = ScriptFake::new(FakeKind::SQL);

    let outcome = run(root.clone());

    assert!(matches!(outcome, SessionRun::Ran { .. }));
    assert_eq!(root.executed().len(), 2);
}

#[test]
fn a_session_that_cannot_be_opened_means_nothing_ran() {
    let session = ScriptFake::new(FakeKind::SQL);
    let (root, probe) = ScriptFake::with_sessions(FakeKind::SQL, session.clone());
    probe.fail_open.store(true, Ordering::SeqCst);

    let outcome = run(root.clone());

    match outcome {
        SessionRun::OpenFailed(error) => assert!(error.to_string().contains("no more sessions")),
        SessionRun::Ran { .. } => panic!("the script must not run without its session"),
    }
    assert!(root.executed().is_empty() && session.executed().is_empty());
}

#[test]
fn a_session_that_cannot_be_released_is_reported_with_the_result() {
    let session = ScriptFake::new(FakeKind::SQL);
    let (root, probe) = ScriptFake::with_sessions(FakeKind::SQL, session);
    probe.fail_finish.store(true, Ordering::SeqCst);

    match run(root) {
        SessionRun::Ran {
            result,
            cleanup_error,
        } => {
            assert!(result.is_ok(), "the run itself succeeded");
            assert!(
                cleanup_error
                    .expect("cleanup error")
                    .to_string()
                    .contains("could not release the session")
            );
        }
        SessionRun::OpenFailed(error) => panic!("unexpected open failure: {error}"),
    }
}

#[test]
fn cancel_run_reaches_only_the_scripts_session() {
    let session = ScriptFake::new(FakeKind::SQL);
    let (root, _probe) = ScriptFake::with_sessions(FakeKind::SQL, session.clone());
    let slot: super::super::interrupt::InterruptSlot = Arc::default();
    let hook_slot = Arc::clone(&slot);
    session.on_statement(move |_| {
        if let Some(scope) = super::super::interrupt::current(&hook_slot) {
            scope.interrupt().ok();
        }
    });

    run_in_session(
        root.clone(),
        &script(),
        &RunOptions::default(),
        &CancelToken::new(),
        &slot,
        |_| {},
    );

    assert_eq!(
        root.interrupts(),
        0,
        "unrelated work on the shared connection is not stopped"
    );
    assert!(session.interrupts() >= 1, "the script's own statement is");
    assert!(
        super::super::interrupt::current(&slot).is_none(),
        "nothing can be interrupted once the run is over"
    );
}
