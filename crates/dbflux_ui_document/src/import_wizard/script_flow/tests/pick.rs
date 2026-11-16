use dbflux_transfer::sql_script::MAX_SCRIPT_BYTES;

use super::support::{FakeKind, Opened, SCRIPT, Scratch, ScriptFake, open_wizard, pick, read};
use crate::import_wizard::{ImportWizard, WizardStep};

/// The Pick Source block after picking `path`, with the wizard still there.
fn error_after_picking(
    cx: &mut gpui::TestAppContext,
    kind: FakeKind,
    path: std::path::PathBuf,
) -> (String, String) {
    let mut opened = open_wizard(cx, ScriptFake::new(kind), false);
    pick(&mut opened, path);

    assert_stays_on_pick_source(&mut opened);
    read(&mut opened, |wizard| {
        let block = wizard.manifest_error.clone().expect("an error block");
        (block.message, block.next_step)
    })
}

fn assert_stays_on_pick_source(opened: &mut Opened<'_>) {
    read(opened, |wizard| {
        assert!(matches!(wizard.step, WizardStep::PickFolder));
        assert!(wizard.script.is_none(), "no script is kept after an error");
        assert!(!wizard.loading, "both buttons are enabled again");
    });
}

#[test]
fn the_dialog_offers_sql_only_when_the_query_language_is_sql() {
    let sql = ScriptFake::new(FakeKind::SQL);
    let mongo = ScriptFake::new(FakeKind {
        language_is_sql: false,
        ..FakeKind::SQL
    });

    assert!(ImportWizard::sql_scripts_offered(sql.as_ref()));
    assert!(!ImportWizard::sql_scripts_offered(mongo.as_ref()));
}

#[gpui::test]
fn a_sql_file_opens_the_script_flow_on_a_sql_connection(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("opens");
    let path = scratch.write("shop.sql", SCRIPT.as_bytes());
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);

    pick(&mut opened, path);

    read(&mut opened, |wizard| {
        assert!(matches!(wizard.step, WizardStep::Configure));
        let flow = wizard.script.as_ref().expect("script");
        assert_eq!(flow.summary.statement_count, 5);
        assert!(wizard.rows.is_empty(), "a script has no table plan");
        assert!(wizard.file_import.is_none());
        assert!(!wizard.loading);
        assert_eq!(wizard.dialog_title(), "Run SQL script");
    });
}

#[gpui::test]
fn the_not_sql_rejection_stays_as_a_backstop(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("not_sql");
    let path = scratch.write("shop.sql", SCRIPT.as_bytes());

    let (message, next_step) = error_after_picking(
        cx,
        FakeKind {
            language_is_sql: false,
            ..FakeKind::SQL
        },
        path,
    );

    assert_eq!(
        message,
        "Cannot run a SQL script on this connection: it uses the MongoDB query language, not SQL."
    );
    assert_eq!(
        next_step,
        "Connect to a SQL connection and open the import there, or choose a CSV, JSON or JSON Lines file to import data into this connection."
    );
}

#[gpui::test]
fn a_script_over_the_limit_is_refused_before_it_is_read(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("too_large");
    let path = scratch.sparse("huge.sql", MAX_SCRIPT_BYTES + 1);

    let (message, next_step) = error_after_picking(cx, FakeKind::SQL, path);

    assert_eq!(
        message,
        "This script is 100 MB, over the 100 MB limit for running a script here."
    );
    assert_eq!(
        next_step,
        "Run it with the database's own command-line client instead. Splitting it into smaller files also works, but each file runs on its own: a later file that fails cannot undo an earlier one."
    );
}

#[gpui::test]
fn a_script_that_is_not_utf8_says_where_and_how_to_fix_it(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("not_utf8");
    let path = scratch.write("latin1.sql", b"SELECT 'caf\xe9';");

    let (message, next_step) = error_after_picking(cx, FakeKind::SQL, path);

    assert_eq!(
        message,
        "Cannot read the script: it is not valid UTF-8 (invalid byte at position 12). SQL scripts are read as UTF-8 and have no encoding option."
    );
    assert_eq!(
        next_step,
        "Save the file as UTF-8 in your editor and choose it again."
    );
}

#[gpui::test]
fn an_unreadable_script_names_the_file_and_the_cause(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("unreadable");
    let path = scratch.write("gone.sql", b"SELECT 1;");
    std::fs::remove_file(&path).expect("remove");

    let (message, next_step) = error_after_picking(cx, FakeKind::SQL, path.clone());

    assert!(
        message.starts_with(&format!("Cannot read the file: {}: ", path.display())),
        "{message}"
    );
    assert_eq!(
        next_step,
        "Check that you can read the file, or choose another one."
    );
}

#[gpui::test]
fn a_script_with_no_statements_says_so(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("empty");
    let path = scratch.write("comments.sql", b"-- nothing here\n/* or here */\n");

    let (message, next_step) = error_after_picking(cx, FakeKind::SQL, path);

    assert_eq!(
        message,
        "The script has no statements: the file is empty or holds only comments."
    );
    assert_eq!(next_step, "Choose a file that has at least one statement.");
}

#[gpui::test]
fn a_file_the_wizard_cannot_import_lists_sql_on_a_sql_connection(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("unsupported");
    let path = scratch.write("notes.txt", b"hello");

    let (message, _) = error_after_picking(cx, FakeKind::SQL, path.clone());
    assert_eq!(
        message,
        "Only .csv, .json, .jsonl and .sql files can be imported."
    );

    let (message, _) = error_after_picking(
        cx,
        FakeKind {
            language_is_sql: false,
            ..FakeKind::SQL
        },
        path,
    );
    assert_eq!(
        message,
        "Only .csv, .json and .jsonl files can be imported."
    );
}

#[gpui::test]
fn a_script_with_a_client_directive_is_refused_with_localized_copy(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("delimiter");
    let path = scratch.write("proc.sql", b"SELECT 1;\nDELIMITER //\nSELECT 2//\n");

    let (message, next_step) = error_after_picking(cx, FakeKind::SQL, path);

    assert_eq!(
        message,
        "This script uses the client command DELIMITER (line 2), which only a database command-line client understands."
    );
    assert_eq!(
        next_step,
        "Run the script with the database's own command-line client, or remove the DELIMITER lines and end each statement with a semicolon."
    );
}
