//! The import wizard's chrome: the top stepper, the shared footer and the
//! reason a disabled Continue gives, on every step.

use std::cell::RefCell;
use std::rc::Rc;

use dbflux_core::SchemaLoadingStrategy;
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

use crate::import_wizard::{ImportWizard, WizardStep};
use crate::keyboard_test_support::init_keyboard_runtime;
use crate::migrate_wizard::source_target::shared_coordinator_tests::{
    PickerFakeConnection, connect_profile,
};

fn open_wizard(cx: &mut TestAppContext) -> (Entity<ImportWizard>, &mut VisualTestContext) {
    init_keyboard_runtime(cx);
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });
    let profile_id = Uuid::new_v4();
    connect_profile(
        &app_state,
        cx,
        profile_id,
        PickerFakeConnection::new(SchemaLoadingStrategy::SingleDatabase),
        None,
    );

    let slot: Rc<RefCell<Option<Entity<ImportWizard>>>> = Rc::default();
    let capture = Rc::clone(&slot);
    let (_root, window) = cx.add_window_view(move |window, cx| {
        let wizard = cx.new(|cx| ImportWizard::new(app_state, cx));
        wizard.update(cx, |this, cx| this.open(profile_id, None, window, cx));
        *capture.borrow_mut() = Some(wizard.clone());
        gpui_component::Root::new(wizard, window, cx)
    });
    window.run_until_parked();

    let wizard = slot.borrow().clone().expect("wizard");
    (wizard, window)
}

fn show_step(wizard: &Entity<ImportWizard>, window: &mut VisualTestContext, step: WizardStep) {
    wizard.update(window, |this, cx| {
        this.step = step;
        cx.notify();
    });
    window.run_until_parked();
}

fn present(window: &mut VisualTestContext, selector: &'static str) -> bool {
    window.debug_bounds(selector).is_some()
}

#[gpui::test]
fn every_step_has_the_top_stepper_and_the_shared_footer(cx: &mut TestAppContext) {
    let (wizard, window) = open_wizard(cx);

    for step in [
        WizardStep::PickFolder,
        WizardStep::Configure,
        WizardStep::Confirm,
        WizardStep::Running,
        WizardStep::Done,
    ] {
        show_step(&wizard, window, step);

        let stepper = window
            .debug_bounds("import-wizard-stepper")
            .unwrap_or_else(|| panic!("{step:?} must show the top stepper"));
        let footer = window
            .debug_bounds("import-wizard-footer")
            .unwrap_or_else(|| panic!("{step:?} must show the shared footer"));
        assert!(
            stepper.bottom() <= footer.top(),
            "{step:?}: the stepper {stepper:?} sits above the footer {footer:?}"
        );
    }
}

#[gpui::test]
fn the_footer_carries_each_steps_buttons(cx: &mut TestAppContext) {
    let (wizard, window) = open_wizard(cx);

    show_step(&wizard, window, WizardStep::PickFolder);
    assert!(present(window, "import-wizard-cancel"));
    assert!(!present(window, "import-wizard-continue"));

    show_step(&wizard, window, WizardStep::Configure);
    assert!(present(window, "import-wizard-cancel"));
    assert!(present(window, "import-wizard-continue"));

    show_step(&wizard, window, WizardStep::Confirm);
    assert!(present(window, "import-wizard-cancel-confirm"));
    assert!(present(window, "import-wizard-cancel"));
    assert!(present(window, "import-wizard-confirm-destructive"));

    show_step(&wizard, window, WizardStep::Running);
    assert!(present(window, "import-wizard-cancel-run"));

    show_step(&wizard, window, WizardStep::Done);
    assert!(present(window, "import-wizard-close"));
    assert!(!present(window, "import-wizard-cancel"));
}

/// A file that could not be read keeps Continue off, and the footer says so
/// until a read succeeds; reading a file switches both Pick Source buttons off
/// with its own reason.
#[gpui::test]
fn a_disabled_continue_says_why_in_the_footer(cx: &mut TestAppContext) {
    let directory = std::env::temp_dir().join(format!(
        "dbflux_import_chrome_reason_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("create scratch dir");
    let path = directory.join("people.csv");
    std::fs::write(&path, b"pr\xe9nom,id\nAda,1\n").expect("write file");
    let (wizard, window) = open_wizard(cx);

    show_step(&wizard, window, WizardStep::PickFolder);
    assert!(!present(window, "import-wizard-footer-reason"));

    wizard.update(window, |this, cx| {
        this.loading = true;
        cx.notify();
    });
    window.run_until_parked();
    assert!(
        present(window, "import-wizard-footer-reason"),
        "reading a file switches both buttons off, and the footer says so"
    );
    wizard.update(window, |this, cx| {
        this.loading = false;
        cx.notify();
    });

    wizard.update(window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    window.run_until_parked();
    assert!(
        present(window, "import-wizard-footer-reason"),
        "an unreadable header keeps Continue off and the footer says why"
    );
    assert!(
        present(window, "import-wizard-error"),
        "the read error is the danger block"
    );

    wizard.update(window, |this, cx| this.select_encoding("windows1252", cx));
    window.run_until_parked();
    assert!(!present(window, "import-wizard-footer-reason"));
    assert!(!present(window, "import-wizard-error"));
    std::fs::remove_dir_all(&directory).ok();
}

fn scratch_file(tag: &str, name: &str, bytes: &[u8]) -> (std::path::PathBuf, std::path::PathBuf) {
    let directory =
        std::env::temp_dir().join(format!("dbflux_import_chrome_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create scratch dir");
    let path = directory.join(name);
    std::fs::write(&path, bytes).expect("write file");
    (directory, path)
}

fn pick(wizard: &Entity<ImportWizard>, window: &mut VisualTestContext, path: std::path::PathBuf) {
    wizard.update(window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    window.run_until_parked();
}

/// A file with another extension stays on Pick Source with the danger block,
/// which names what to pick next.
#[gpui::test]
fn an_unsupported_file_type_shows_the_error_block_on_pick_source(cx: &mut TestAppContext) {
    let (directory, path) = scratch_file("unsupported", "notes.txt", b"hello");
    let (wizard, window) = open_wizard(cx);
    assert!(!present(window, "import-wizard-error"));

    pick(&wizard, window, path);

    assert!(present(window, "import-wizard-error"));
    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert_eq!(wizard.step, WizardStep::PickFolder);
        let block = wizard.manifest_error.as_ref().expect("the error");
        assert_eq!(
            block.next_step,
            "Choose a file with one of those extensions."
        );
    });
    std::fs::remove_dir_all(&directory).ok();
}

/// Broken JSON stays on Pick Source with the danger block and its next step;
/// picking another file clears it.
#[gpui::test]
fn a_broken_json_file_shows_the_error_block_until_another_file_is_picked(cx: &mut TestAppContext) {
    let (directory, broken) = scratch_file("broken", "people.json", b"[{\"id\": 1}, {oops");
    let good = directory.join("good.csv");
    std::fs::write(&good, b"id,name\n1,Ada\n").expect("write file");
    let (wizard, window) = open_wizard(cx);

    pick(&wizard, window, broken);

    assert!(present(window, "import-wizard-error"));
    window.update(|_, cx| {
        let block = wizard.read(cx).manifest_error.clone().expect("the error");
        assert_eq!(
            block.next_step,
            "Fix the JSON in the file and choose it again, or choose another file."
        );
    });

    pick(&wizard, window, good);
    assert!(!present(window, "import-wizard-error"));
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn json_files_say_they_have_no_options_and_csv_files_show_them(cx: &mut TestAppContext) {
    let (directory, json) = scratch_file("options", "events.jsonl", b"{\"id\": 1}\n");
    let csv = directory.join("people.csv");
    std::fs::write(&csv, b"id,name\n1,Ada\n").expect("write file");
    let (wizard, window) = open_wizard(cx);

    pick(&wizard, window, json);
    assert!(present(window, "import-wizard-json-no-options"));

    pick(&wizard, window, csv);
    assert!(!present(window, "import-wizard-json-no-options"));
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn xml_files_say_they_have_no_options(cx: &mut TestAppContext) {
    let (directory, xml) = scratch_file(
        "xml_options",
        "events.xml",
        b"<rows><row><field name=\"id\">1</field></row></rows>",
    );
    let (wizard, window) = open_wizard(cx);

    pick(&wizard, window, xml);
    assert!(present(window, "import-wizard-json-no-options"));
    std::fs::remove_dir_all(&directory).ok();
}

/// A connection that cannot truncate lists no Truncate mode, and the step
/// says why and what to choose instead.
#[gpui::test]
fn a_connection_that_cannot_truncate_says_why_truncate_is_not_listed(cx: &mut TestAppContext) {
    let (directory, path) = scratch_file("truncate", "people.csv", b"id,name\n1,Ada\n");
    let (wizard, window) = open_wizard(cx);
    pick(&wizard, window, path);

    wizard.update(window, |this, cx| {
        this.supports_truncate = false;
        cx.notify();
    });
    window.run_until_parked();
    assert!(present(window, "import-wizard-truncate-unavailable"));

    wizard.update(window, |this, cx| {
        this.supports_truncate = true;
        cx.notify();
    });
    window.run_until_parked();
    assert!(!present(window, "import-wizard-truncate-unavailable"));
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn a_finished_import_lists_one_row_per_table_and_a_failed_one_says_what_to_do(
    cx: &mut TestAppContext,
) {
    use crate::export_wizard::notes::{ResultKind, ResultRow};

    let (wizard, window) = open_wizard(cx);
    wizard.update(window, |this, cx| {
        this.step = WizardStep::Done;
        this.result_summary = Some("Imported".to_string());
        this.result_rows = vec![
            ResultRow {
                kind: ResultKind::Completed,
                text: "customers: completed (3 row(s))".to_string(),
            },
            ResultRow {
                kind: ResultKind::Failed,
                text: "orders: FAILED — constraint".to_string(),
            },
            ResultRow {
                kind: ResultKind::NotAttempted,
                text: "items: not attempted: the import stopped at the failed table".to_string(),
            },
        ];
        cx.notify();
    });
    window.run_until_parked();

    assert!(present(window, "import-wizard-result-0-completed"));
    assert!(present(window, "import-wizard-result-1-failed"));
    assert!(present(window, "import-wizard-result-2-not-attempted"));
    assert!(present(window, "import-wizard-closing-line"));

    wizard.update(window, |this, cx| {
        this.result_rows.truncate(1);
        cx.notify();
    });
    window.run_until_parked();
    assert!(!present(window, "import-wizard-closing-line"));
}

/// With the reason shown on Configure, Cancel and Continue stay inside the
/// dialog and to the right of the reason.
#[gpui::test]
fn a_footer_reason_leaves_the_buttons_inside_the_dialog(cx: &mut TestAppContext) {
    let (directory, path) = scratch_file("footer_reason", "people.csv", b"pr\xe9nom,id\nAda,1\n");
    let (wizard, window) = open_wizard(cx);
    pick(&wizard, window, path);

    let dialog = window.debug_bounds("modal-body").expect("the dialog");
    let reason = window
        .debug_bounds("import-wizard-footer-reason")
        .expect("the reason");
    for id in ["import-wizard-cancel", "import-wizard-continue"] {
        let button = window.debug_bounds(id).expect(id);
        assert!(
            button.right() <= dialog.right(),
            "{id} {button:?} must end inside the dialog {dialog:?}"
        );
        assert!(
            reason.right() <= button.left(),
            "the reason {reason:?} must end before {id} {button:?}"
        );
    }
    std::fs::remove_dir_all(&directory).ok();
}

/// The target table field shows the Enter keycap that applies the name.
#[gpui::test]
fn the_target_table_field_shows_the_enter_hint(cx: &mut TestAppContext) {
    let (directory, path) = scratch_file("enter_hint", "people.csv", b"id,name\n1,Ada\n");
    let (wizard, window) = open_wizard(cx);
    assert!(!present(window, "import-wizard-target-enter-hint"));

    pick(&wizard, window, path);

    assert!(present(window, "import-wizard-target-enter-hint"));
    std::fs::remove_dir_all(&directory).ok();
}

fn plan(
    name: &str,
    mode: dbflux_transfer::TableMappingMode,
) -> crate::import_wizard::TableImportConfig {
    let manifest_table = dbflux_transfer::manifest::ManifestTable {
        schema: None,
        name: name.to_string(),
        file: format!("{name}.csv"),
        format: "csv".to_string(),
        columns: vec![dbflux_core::TransferColumn {
            name: "id".to_string(),
            type_name: None,
            nullable: true,
            is_primary_key: false,
        }],
        row_count: 0,
        fk_order_index: 0,
        options: Default::default(),
    };
    let mut config =
        crate::import_wizard::TableImportConfig::new(&manifest_table, true, Vec::new());
    config.mapping_mode = mode;
    config
}

/// A destructive plan is confirmed through the danger block, which lists
/// each affected table with its mode; tables that are not touched are left
/// out.
#[gpui::test]
fn the_destructive_confirm_lists_each_table_with_its_mode(cx: &mut TestAppContext) {
    use dbflux_transfer::TableMappingMode;

    let (wizard, window) = open_wizard(cx);
    wizard.update(window, |this, cx| {
        this.supports_truncate = true;
        this.build_rows(
            vec![
                plan("customers", TableMappingMode::Recreate),
                plan("orders", TableMappingMode::Truncate),
                plan("items", TableMappingMode::Existing),
            ],
            cx,
        );
        this.step = WizardStep::Confirm;
        cx.notify();
    });
    window.run_until_parked();

    assert!(present(window, "import-wizard-confirm-block"));
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).destructive_lines(),
            vec![
                "customers — Recreate (drop + create)".to_string(),
                "orders — Truncate (empty + insert)".to_string(),
            ]
        );
    });
}
