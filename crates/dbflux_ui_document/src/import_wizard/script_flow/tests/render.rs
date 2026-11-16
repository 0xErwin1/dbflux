use std::time::Duration;

use dbflux_transfer::sql_script::{
    Disposition, RunEnd, ScriptReport, ScriptTransaction, StatementFailure,
};
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, MODAL_CHROME};

use super::super::messages::{DoneView, done_view};
use super::support::{
    FakeKind, Opened, SCRIPT, Scratch, ScriptFake, WRITE_SCRIPT, open_wizard, pick,
};
use crate::import_wizard::WizardStep;
use crate::keyboard_coverage::IMPORT_WIZARD;

fn present(opened: &mut Opened<'_>, selector: &'static str) -> bool {
    opened.window.run_until_parked();
    opened.window.debug_bounds(selector).is_some()
}

fn show(opened: &mut Opened<'_>, step: WizardStep) {
    opened.wizard.update(opened.window, |this, cx| {
        this.step = step;
        cx.notify();
    });
    opened.window.run_until_parked();
}

fn show_done(opened: &mut Opened<'_>, view: DoneView) {
    opened.wizard.update(opened.window, |this, cx| {
        if let Some(flow) = this.script.as_mut() {
            flow.done = Some(view);
        }
        this.step = WizardStep::Done;
        cx.notify();
    });
    opened.window.run_until_parked();
}

fn report(end: RunEnd, disposition: Disposition) -> ScriptReport {
    ScriptReport {
        end,
        disposition,
        total: 10,
        ran: 10,
        failures: Vec::new(),
        stopped_at: None,
        transaction_used: disposition != Disposition::Kept,
        ddl_not_transactional: false,
        first_implicit_commit: None,
        script_transaction: ScriptTransaction::None,
        finish_error: None,
        elapsed: Duration::from_millis(900),
    }
}

fn failing(report: ScriptReport) -> ScriptReport {
    ScriptReport {
        ran: 3,
        stopped_at: Some(4),
        failures: vec![StatementFailure {
            index: 4,
            line: 4,
            snippet: "INSERT INTO t VALUES (1)".to_string(),
            error: "duplicate key".to_string(),
        }],
        ..report
    }
}

#[gpui::test]
fn configure_draws_the_summary_preview_and_options(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("render_configure");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    for selector in [
        "import-wizard-script-summary",
        "import-wizard-script-chips",
        "import-wizard-script-preview",
        "import-wizard-script-statement-1",
        "import-wizard-script-statement-5",
        "import-wizard-script-on-error",
        "import-wizard-script-transaction-row",
        "import-wizard-continue",
    ] {
        assert!(present(&mut opened, selector), "{selector}");
    }
}

#[gpui::test]
fn confirm_shows_the_danger_block_only_for_a_destructive_script(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("render_confirm_danger");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    show(&mut opened, WizardStep::Confirm);

    assert!(present(&mut opened, "import-wizard-script-confirm-danger"));
    assert!(present(&mut opened, "import-wizard-confirm-destructive"));
    assert!(!present(&mut opened, "import-wizard-run-script"));
    assert!(!present(&mut opened, "import-wizard-script-confirm-plain"));
    assert!(present(&mut opened, "import-wizard-cancel-confirm"));
    assert!(present(&mut opened, "import-wizard-cancel"));
}

#[gpui::test]
fn confirm_of_a_plain_script_lists_the_counts_and_offers_run_script(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("render_confirm_plain");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(
        &mut opened,
        scratch.write("shop.sql", WRITE_SCRIPT.as_bytes()),
    );
    show(&mut opened, WizardStep::Confirm);

    assert!(present(&mut opened, "import-wizard-script-confirm-plain"));
    assert!(present(&mut opened, "import-wizard-run-script"));
    assert!(!present(&mut opened, "import-wizard-script-confirm-danger"));
    assert!(!present(&mut opened, "import-wizard-confirm-destructive"));
}

#[gpui::test]
fn running_shows_the_progress_the_snippet_and_cancel_run(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("render_running");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    opened.wizard.update(opened.window, |this, cx| {
        *this.progress.lock().expect("progress") = (3, Some(5));
        if let Some(flow) = this.script.as_ref()
            && let Ok(mut snippet) = flow.snippet.lock()
        {
            *snippet = "UPDATE t SET id = 2 WHERE id = 1".to_string();
        }
        this.running = true;
        this.cancel_token = Some(dbflux_core::CancelToken::new());
        this.step = WizardStep::Running;
        cx.notify();
    });

    for selector in [
        "import-wizard-script-running",
        "import-wizard-script-snippet",
        "import-wizard-script-running-mode",
        "import-wizard-cancel-run",
        "import-wizard-footer-reason",
    ] {
        assert!(present(&mut opened, selector), "{selector}");
    }
}

#[gpui::test]
fn done_draws_each_outcome(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("render_done");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    let cases: Vec<(&'static str, DoneView, &'static [&'static str])> = vec![
        (
            "success",
            done_view(&report(RunEnd::Finished, Disposition::Committed), true),
            &[
                "import-wizard-script-headline",
                "import-wizard-script-outcome-0-completed",
            ],
        ),
        (
            "rolled back",
            done_view(
                &ScriptReport {
                    stopped_at: Some(5),
                    ..report(RunEnd::Cancelled, Disposition::RolledBack)
                },
                true,
            ),
            &[
                "import-wizard-script-outcome-0-cancelled",
                "import-wizard-script-next-step",
            ],
        ),
        (
            "kept",
            done_view(
                &ScriptReport {
                    stopped_at: Some(5),
                    ..report(RunEnd::Cancelled, Disposition::Kept)
                },
                false,
            ),
            &["import-wizard-script-outcome-0-cancelled"],
        ),
        (
            "stopped",
            done_view(
                &failing(report(RunEnd::StoppedOnError, Disposition::RolledBack)),
                true,
            ),
            &[
                "import-wizard-script-failed-statement",
                "import-wizard-script-error",
            ],
        ),
        (
            "continue with failures",
            done_view(&failing(report(RunEnd::Finished, Disposition::Kept)), false),
            &[
                "import-wizard-script-failure-0-failed",
                "import-wizard-script-next-step",
            ],
        ),
        (
            "unresolved",
            done_view(
                &ScriptReport {
                    finish_error: Some("Rollback failed: reset".to_string()),
                    ..report(RunEnd::StoppedOnError, Disposition::Unresolved)
                },
                true,
            ),
            &["import-wizard-script-done-block"],
        ),
    ];

    for (name, view, selectors) in cases {
        show_done(&mut opened, view);

        for selector in selectors {
            assert!(present(&mut opened, selector), "{name}: {selector}");
        }
        assert!(present(&mut opened, "import-wizard-close"), "{name}");
    }
}

/// Every step of the script flow, each with its own buttons: every clickable
/// element has a keyboard path in the import wizard's registry.
#[gpui::test]
fn the_script_steps_are_covered(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("render_coverage");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    let capture = FrameCapture::observe(opened.window);
    let mut checked = Vec::new();

    let mut check = |opened: &mut Opened<'_>| {
        checked.extend(
            Coverage::new(IMPORT_WIZARD)
                .with_surface(MODAL_CHROME)
                .assert_covered(&capture.frame(opened.window)),
        );
    };

    show(&mut opened, WizardStep::Configure);
    check(&mut opened);
    show(&mut opened, WizardStep::Confirm);
    check(&mut opened);

    let plain = Scratch::new("render_coverage_plain");
    pick(
        &mut opened,
        plain.write("plain.sql", WRITE_SCRIPT.as_bytes()),
    );
    show(&mut opened, WizardStep::Confirm);
    check(&mut opened);

    opened.wizard.update(opened.window, |this, cx| {
        this.running = true;
        this.cancel_token = Some(dbflux_core::CancelToken::new());
        cx.notify();
    });
    show(&mut opened, WizardStep::Running);
    check(&mut opened);
    show_done(
        &mut opened,
        done_view(&report(RunEnd::Finished, Disposition::Committed), true),
    );
    check(&mut opened);

    for expected in [
        "import-wizard-script-transaction",
        "import-wizard-run-script",
        "import-wizard-cancel-confirm",
        "import-wizard-cancel-run",
        "import-wizard-close",
    ] {
        assert!(
            checked.iter().any(|id| id == expected),
            "{expected} in {checked:?}"
        );
    }
    assert!(
        checked
            .iter()
            .any(|id| id.starts_with("segmented-import-script-on-error-")),
        "the On error segments are covered: {checked:?}"
    );
}
