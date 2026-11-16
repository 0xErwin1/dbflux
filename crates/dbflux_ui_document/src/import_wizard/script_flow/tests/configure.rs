use dbflux_transfer::sql_script::OnError;

use super::super::options::TransactionAvailability;
use super::support::{
    FakeKind, READ_ONLY_SCRIPT, SCRIPT, Scratch, ScriptFake, WRITE_SCRIPT, open_wizard, pick, read,
};
use crate::import_wizard::WizardStep;

#[gpui::test]
fn configure_summarizes_the_script_with_the_editors_classification(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("summary");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    read(&mut opened, |wizard| {
        let summary = &wizard.script.as_ref().expect("script").summary;

        assert_eq!(summary.statement_count, 5);
        assert_eq!(
            summary.preview.len(),
            5,
            "the preview lists the first statements"
        );
        assert_eq!(summary.preview[0].index, 1);
        assert!(
            summary.destructive_count >= 1,
            "DELETE without WHERE is destructive"
        );
        assert!(
            summary
                .preview
                .iter()
                .any(|statement| statement.danger.is_some())
        );
    });
}

#[gpui::test]
fn the_defaults_stop_at_the_first_error_in_one_transaction(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("defaults");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    read(&mut opened, |wizard| {
        let flow = wizard.script.as_ref().expect("script");

        assert_eq!(flow.options.on_error, OnError::Stop);
        assert_eq!(
            flow.options.availability(&flow.rules),
            TransactionAvailability::Available
        );
        assert!(flow.options.runs_in_transaction(&flow.rules));
    });
}

#[gpui::test]
fn continue_after_errors_turns_the_transaction_off_and_stop_restores_it(
    cx: &mut gpui::TestAppContext,
) {
    let scratch = Scratch::new("continue");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    opened.wizard.update(opened.window, |this, cx| {
        this.set_script_on_error(OnError::Continue, cx)
    });
    read(&mut opened, |wizard| {
        let flow = wizard.script.as_ref().expect("script");

        assert_eq!(
            flow.options.availability(&flow.rules),
            TransactionAvailability::OffForContinue
        );
        assert!(!flow.options.run_options(&flow.rules).transaction);
    });

    opened.wizard.update(opened.window, |this, cx| {
        this.set_script_on_error(OnError::Stop, cx)
    });
    read(&mut opened, |wizard| {
        let flow = wizard.script.as_ref().expect("script");

        assert!(
            flow.options.run_options(&flow.rules).transaction,
            "the user's choice is kept while Continue is on"
        );
    });
}

#[gpui::test]
fn a_connection_without_transactions_says_the_option_is_unavailable(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("no_tx");
    let kind = FakeKind {
        transactions: false,
        ..FakeKind::SQL
    };
    let mut opened = open_wizard(cx, ScriptFake::new(kind), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    read(&mut opened, |wizard| {
        let flow = wizard.script.as_ref().expect("script");

        assert_eq!(
            flow.options.availability(&flow.rules),
            TransactionAvailability::Unavailable
        );
        assert!(!flow.options.run_options(&flow.rules).transaction);
    });
    opened.window.update(|_, _| ());
    assert!(
        opened
            .window
            .debug_bounds("import-wizard-script-transaction-why")
            .is_some(),
        "the reason line takes the place of the checkbox"
    );
}

#[gpui::test]
fn a_connection_that_commits_ddl_on_its_own_says_so(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("ddl");
    let kind = FakeKind {
        transactional_ddl: false,
        ..FakeKind::SQL
    };
    let mut opened = open_wizard(cx, ScriptFake::new(kind), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));

    read(&mut opened, |wizard| {
        assert!(
            wizard
                .script
                .as_ref()
                .expect("script")
                .summary
                .ddl_commits_on_its_own
        );
    });
    assert!(
        opened
            .window
            .debug_bounds("import-wizard-script-ddl-commits")
            .is_some()
    );

    let transactional = Scratch::new("ddl_tx");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(
        &mut opened,
        transactional.write("shop.sql", SCRIPT.as_bytes()),
    );

    assert!(
        opened
            .window
            .debug_bounds("import-wizard-script-ddl-commits")
            .is_none()
    );
}

#[gpui::test]
fn a_read_only_profile_refuses_a_script_that_changes_data(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("read_only");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), true);
    pick(
        &mut opened,
        scratch.write("shop.sql", WRITE_SCRIPT.as_bytes()),
    );

    let block = read(&mut opened, |wizard| wizard.script_read_only_block()).expect("block");
    assert_eq!(
        block.message,
        "This connection is read-only, and 2 statements in the script change data or structure."
    );
    assert_eq!(
        block.next_step,
        "Open a read-write connection to run it. A script that only reads can run here."
    );
    assert_eq!(
        read(&mut opened, |wizard| wizard.script_footer_reason()),
        Some("Continue is off: the connection is read-only.".to_string())
    );

    opened
        .wizard
        .update(opened.window, |this, cx| this.continue_script(cx));
    read(&mut opened, |wizard| {
        assert!(
            matches!(wizard.step, WizardStep::Configure),
            "Continue does not advance"
        );
    });
}

#[gpui::test]
fn a_read_only_profile_runs_a_script_that_only_reads(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("read_only_ok");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), true);
    pick(
        &mut opened,
        scratch.write("look.sql", READ_ONLY_SCRIPT.as_bytes()),
    );

    assert!(read(&mut opened, |wizard| wizard.script_read_only_block()).is_none());
    opened
        .wizard
        .update(opened.window, |this, cx| this.continue_script(cx));
    read(&mut opened, |wizard| {
        assert!(matches!(wizard.step, WizardStep::Confirm));
    });
}

#[gpui::test]
fn a_script_with_its_own_commit_turns_the_transaction_off_and_says_why(
    cx: &mut gpui::TestAppContext,
) {
    let scratch = Scratch::new("own_tx");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(
        &mut opened,
        scratch.write(
            "own.sql",
            b"INSERT INTO t VALUES (1);\nCOMMIT;\nINSERT INTO t VALUES (2);\n",
        ),
    );

    read(&mut opened, |wizard| {
        let flow = wizard.script.as_ref().expect("script");

        assert_eq!(flow.rules.self_managed, Some(("COMMIT", 2)));
        assert_eq!(
            flow.options.availability(&flow.rules),
            TransactionAvailability::SelfManaged
        );
        assert!(!flow.options.run_options(&flow.rules).transaction);
        assert_eq!(
            wizard.configure_fields().len(),
            1,
            "no transaction stop on the cursor"
        );
    });
    assert!(
        opened
            .window
            .debug_bounds("import-wizard-script-transaction-why")
            .is_some()
    );
    assert_eq!(
        read(&mut opened, |wizard| wizard
            .script
            .as_ref()
            .map(super::super::render::transaction_reason)),
        Some(Some(
            "Off because this script manages its own transactions (COMMIT at line 2). It runs as written: each COMMIT in it saves the changes before it.".to_string()
        ))
    );
}

#[gpui::test]
fn confirm_says_a_self_managed_script_runs_as_written(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("own_tx_how");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(
        &mut opened,
        scratch.write(
            "own.sql",
            b"INSERT INTO t VALUES (1);\nBEGIN;\nINSERT INTO t VALUES (2);\n",
        ),
    );

    let how = read(&mut opened, |wizard| {
        wizard
            .script
            .as_ref()
            .map(super::super::render::how_it_runs)
    });
    assert_eq!(
        how.as_deref(),
        Some(
            "It runs as written: the script's own transaction statements decide what is saved, and the run stops at the first error."
        )
    );
    assert_eq!(
        read(&mut opened, |wizard| wizard
            .script
            .as_ref()
            .and_then(super::super::render::transaction_reason)),
        Some("Off because this script manages its own transactions (BEGIN at line 2). It runs as written: its own transaction statements decide what is saved.".to_string()),
        "a keyword that is not a commit gets the generic sentence"
    );
}
