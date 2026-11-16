use std::sync::{Arc, Mutex};

use dbflux_core::{CancelToken, DbKind, TransactionVocab};
use dbflux_transfer::sql_script::OnError;

use super::super::messages::DoneTone;
use super::support::{
    FakeKind, Opened, SCRIPT, Scratch, ScriptFake, WRITE_SCRIPT, open_wizard, pick, read,
};
use crate::import_wizard::WizardStep;

fn vocab() -> TransactionVocab {
    TransactionVocab::for_kind(DbKind::Postgres).expect("vocabulary")
}

/// A wizard on Confirm with `script` picked, ready to run.
fn on_confirm<'a>(
    cx: &'a mut gpui::TestAppContext,
    fake: Arc<ScriptFake>,
    scratch: &Scratch,
    script: &str,
) -> Opened<'a> {
    let mut opened = open_wizard(cx, fake, false);
    pick(&mut opened, scratch.write("shop.sql", script.as_bytes()));
    opened
        .wizard
        .update(opened.window, |this, cx| this.continue_script(cx));

    opened
}

fn run_to_the_end(opened: &mut Opened<'_>) {
    opened
        .wizard
        .update(opened.window, |this, cx| this.run_script_confirmed(cx));
    opened.window.run_until_parked();
}

#[gpui::test]
fn confirm_is_shown_for_every_script_and_the_run_starts_only_from_it(
    cx: &mut gpui::TestAppContext,
) {
    let scratch = Scratch::new("confirm");
    let fake = ScriptFake::new(FakeKind::SQL);
    let mut opened = open_wizard(cx, fake.clone(), false);
    pick(
        &mut opened,
        scratch.write("shop.sql", WRITE_SCRIPT.as_bytes()),
    );

    opened
        .wizard
        .update(opened.window, |this, cx| this.run_script_confirmed(cx));
    opened.window.run_until_parked();
    assert!(fake.executed().is_empty(), "nothing runs from Configure");

    opened
        .wizard
        .update(opened.window, |this, cx| this.continue_script(cx));
    read(&mut opened, |wizard| {
        let flow = wizard.script.as_ref().expect("script");

        assert!(matches!(wizard.step, WizardStep::Confirm));
        assert_eq!(
            flow.summary.destructive_count, 0,
            "not destructive, still confirmed"
        );
    });
    assert!(
        fake.executed().is_empty(),
        "Continue does not run it either"
    );
}

#[gpui::test]
fn back_from_confirm_returns_to_configure_with_the_choices(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("back");
    let mut opened = on_confirm(cx, ScriptFake::new(FakeKind::SQL), &scratch, SCRIPT);

    opened.wizard.update(opened.window, |this, cx| {
        this.step = WizardStep::Configure;
        cx.notify();
    });

    read(&mut opened, |wizard| {
        assert!(matches!(wizard.step, WizardStep::Configure));
        assert!(wizard.script.is_some());
    });
}

#[gpui::test]
fn a_committed_run_goes_begin_statements_commit_and_reports_success(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("commit");
    let fake = ScriptFake::new(FakeKind::SQL);
    let mut opened = on_confirm(cx, fake.clone(), &scratch, SCRIPT);

    opened
        .wizard
        .update(opened.window, |this, cx| this.run_script_confirmed(cx));
    read(&mut opened, |wizard| {
        assert!(matches!(wizard.step, WizardStep::Running));
        assert!(wizard.cancel_token.is_some());
        assert!(wizard.active_task_id.is_some());
    });
    opened.window.run_until_parked();

    let executed = fake.executed();
    assert_eq!(executed.first().map(String::as_str), Some(vocab().begin));
    assert_eq!(executed.last().map(String::as_str), Some(vocab().commit));
    assert_eq!(executed.len(), 7);
    read(&mut opened, |wizard| {
        let view = wizard
            .script
            .as_ref()
            .and_then(|flow| flow.done.as_ref())
            .expect("done");

        assert!(matches!(wizard.step, WizardStep::Done));
        assert!(!wizard.running);
        assert_eq!(view.tone, DoneTone::Success);
        assert!(
            view.headline
                .starts_with("Script finished: 5 statements ran in ")
        );
    });
}

#[gpui::test]
fn a_failed_statement_stops_the_run_and_rolls_back(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("stop");
    let fake = ScriptFake::new(FakeKind::SQL);
    fake.fail_on("UPDATE", "boom");
    let mut opened = on_confirm(cx, fake.clone(), &scratch, SCRIPT);

    run_to_the_end(&mut opened);

    assert_eq!(
        fake.executed().last().map(String::as_str),
        Some(vocab().rollback)
    );
    read(&mut opened, |wizard| {
        let view = wizard
            .script
            .as_ref()
            .and_then(|flow| flow.done.as_ref())
            .expect("done");

        assert_eq!(view.headline, "Script stopped at statement 3 of 5");
        assert_eq!(view.error_text.as_deref(), Some("Database error: boom"));
        assert!(
            view.outcome
                .as_deref()
                .unwrap_or_default()
                .starts_with("Rolled back")
        );
    });
}

#[gpui::test]
fn continue_after_errors_runs_everything_and_lists_the_failures(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("continue");
    let fake = ScriptFake::new(FakeKind::SQL);
    fake.fail_on("INSERT", "no insert");
    let mut opened = open_wizard(cx, fake.clone(), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    opened.wizard.update(opened.window, |this, cx| {
        this.set_script_on_error(OnError::Continue, cx)
    });
    opened
        .wizard
        .update(opened.window, |this, cx| this.continue_script(cx));

    run_to_the_end(&mut opened);

    assert_eq!(
        fake.executed().len(),
        5,
        "no transaction, every statement ran"
    );
    read(&mut opened, |wizard| {
        let view = wizard
            .script
            .as_ref()
            .and_then(|flow| flow.done.as_ref())
            .expect("done");

        assert_eq!(
            view.headline,
            "Script finished with errors: 4 of 5 statements ran, 1 failed"
        );
        assert_eq!(view.failures.len(), 1);
        assert_eq!(view.failures[0].label, "#2 · line 2");
    });
}

#[gpui::test]
fn cancel_run_trips_the_token_and_interrupts_the_statement(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("cancel_run");
    let fake = ScriptFake::new(FakeKind::SQL);
    let opened = on_confirm(cx, fake.clone(), &scratch, SCRIPT);
    let token = CancelToken::new();

    opened.wizard.update(opened.window, |this, cx| {
        this.running = true;
        this.step = WizardStep::Running;
        this.cancel_token = Some(token.clone());
        this.cancel_run(cx);
    });
    opened.window.run_until_parked();

    assert!(token.is_cancelled());
    assert_eq!(
        fake.interrupts(),
        1,
        "the driver is asked to stop the statement"
    );
}

#[gpui::test]
fn a_run_cancelled_in_the_middle_is_rolled_back_and_says_how_far_it_got(
    cx: &mut gpui::TestAppContext,
) {
    let scratch = Scratch::new("cancel_mid");
    let fake = ScriptFake::new(FakeKind::SQL);
    let slot: Arc<Mutex<Option<CancelToken>>> = Arc::default();
    let hook_slot = Arc::clone(&slot);
    fake.on_statement(move |sql| {
        if sql.starts_with("UPDATE")
            && let Ok(token) = hook_slot.lock()
            && let Some(token) = token.as_ref()
        {
            token.cancel();
        }
    });
    let mut opened = on_confirm(cx, fake.clone(), &scratch, SCRIPT);

    opened
        .wizard
        .update(opened.window, |this, cx| this.run_script_confirmed(cx));
    let token = super::support::cancel_token_of(&mut opened).expect("token");
    *slot.lock().expect("slot") = Some(token);
    opened.window.run_until_parked();

    assert_eq!(
        fake.executed().last().map(String::as_str),
        Some(vocab().rollback)
    );
    read(&mut opened, |wizard| {
        let view = wizard
            .script
            .as_ref()
            .and_then(|flow| flow.done.as_ref())
            .expect("done");

        assert_eq!(view.tone, DoneTone::Warning);
        assert_eq!(view.headline, "Script cancelled after 3 of 5 statements");
    });
}

#[gpui::test]
fn a_rollback_that_fails_is_an_unresolved_danger_block(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("unresolved");
    let fake = ScriptFake::new(FakeKind::SQL);
    fake.fail_on("UPDATE", "boom");
    fake.fail_on(vocab().rollback, "connection reset");
    let mut opened = on_confirm(cx, fake, &scratch, SCRIPT);

    run_to_the_end(&mut opened);

    read(&mut opened, |wizard| {
        let view = wizard
            .script
            .as_ref()
            .and_then(|flow| flow.done.as_ref())
            .expect("done");
        let block = view.block.as_ref().expect("danger block");

        assert!(
            block.message.contains("connection reset"),
            "{}",
            block.message
        );
        assert!(block.next_step.contains("state of the database"));
    });
}

#[gpui::test]
fn a_run_that_cannot_start_returns_to_configure_without_a_task(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("no_connection");
    let mut opened = on_confirm(cx, ScriptFake::new(FakeKind::SQL), &scratch, SCRIPT);

    opened.wizard.update(opened.window, |this, cx| {
        this.profile_id = Some(uuid::Uuid::new_v4());
        this.run_script_confirmed(cx);
    });

    read(&mut opened, |wizard| {
        assert!(matches!(wizard.step, WizardStep::Configure));
        assert!(wizard.cancel_token.is_none());
        assert!(!wizard.running);
    });
}
