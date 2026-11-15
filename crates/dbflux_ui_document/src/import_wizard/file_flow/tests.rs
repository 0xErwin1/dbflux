// Import only what the tests need: re-globbing the parent's `use gpui::*`
// next to `#[gpui::test]` blows rustc's macro recursion limit in this crate.
use std::path::PathBuf;

use dbflux_export::{CsvDelimiter, FormatOptions};
use dbflux_transfer::FileFormat;
use dbflux_ui_base::app_state_entity::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext, Entity};
use uuid::Uuid;

use super::{FileImportState, ImportJob};
use crate::import_wizard::{ImportWizard, WizardStep};

fn isolated_test_app_state(cx: &mut gpui::TestAppContext) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    })
}

fn init_test_runtime(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
}

fn file_state(format: FileFormat, error: Option<&str>) -> FileImportState {
    FileImportState {
        path: PathBuf::from("/data/people.csv"),
        format,
        options: FormatOptions::default(),
        error: error.map(str::to_string),
        generation: 0,
        continue_after_read: false,
    }
}

fn with_wizard<T: 'static>(
    cx: &mut gpui::TestAppContext,
    prepare: impl FnOnce(&mut ImportWizard) -> T + 'static,
    check: impl FnOnce(&mut gpui::VisualTestContext, T),
) {
    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);

    let (_root, window) = cx.add_window_view(|window, cx| {
        let wizard = cx.new(|cx| ImportWizard::new(app_state, cx));
        let prepared = wizard.update(cx, |this, cx| {
            this.open(Uuid::new_v4(), None, window, cx);
            prepare(this)
        });
        PREPARED.with(|slot| *slot.borrow_mut() = Some(Box::new(prepared)));
        gpui_component::Root::new(wizard, window, cx)
    });
    window.run_until_parked();
    let prepared = PREPARED
        .with(|slot| slot.borrow_mut().take())
        .and_then(|boxed| boxed.downcast::<T>().ok())
        .expect("prepared value");
    check(window, *prepared);
}

thread_local! {
    static PREPARED: std::cell::RefCell<Option<Box<dyn std::any::Any>>> =
        const { std::cell::RefCell::new(None) };
}

#[gpui::test]
fn the_first_step_offers_import_file_next_to_import_folder(cx: &mut gpui::TestAppContext) {
    with_wizard(
        cx,
        |_| {},
        |window, ()| assert!(window.debug_bounds("import-wizard-choose-file").is_some()),
    );
}

#[gpui::test]
fn a_csv_file_shows_the_null_text_field(cx: &mut gpui::TestAppContext) {
    with_wizard(
        cx,
        |this| {
            this.file_import = Some(file_state(FileFormat::Csv, None));
            this.step = WizardStep::Configure;
        },
        |window, ()| {
            assert!(
                window
                    .debug_bounds("import-wizard-file-null-text")
                    .is_some()
            )
        },
    );
}

#[gpui::test]
fn a_json_file_shows_no_options(cx: &mut gpui::TestAppContext) {
    with_wizard(
        cx,
        |this| {
            this.file_import = Some(file_state(FileFormat::JsonLines, None));
            this.step = WizardStep::Configure;
        },
        |window, ()| {
            assert!(
                window
                    .debug_bounds("import-wizard-file-null-text")
                    .is_none()
            )
        },
    );
}

#[gpui::test]
fn a_file_that_could_not_be_read_blocks_the_run(cx: &mut gpui::TestAppContext) {
    with_wizard(
        cx,
        |this| {
            this.file_import = Some(file_state(FileFormat::Csv, Some("no header row")));
            this.file_import_blocked()
        },
        |_, blocked| assert!(blocked),
    );
}

#[gpui::test]
fn the_run_reads_the_picked_file_else_the_picked_folder(cx: &mut gpui::TestAppContext) {
    with_wizard(
        cx,
        |this| {
            this.manifest_dir = Some(PathBuf::from("/exports/run-1"));
            let folder = this.import_job();

            let mut state = file_state(FileFormat::Csv, None);
            state.options.delimiter = CsvDelimiter::Semicolon;
            this.file_import = Some(state);
            (folder, this.import_job())
        },
        |_, (folder, file)| {
            assert!(
                matches!(&folder, Some(ImportJob::Folder(dir)) if dir == &PathBuf::from("/exports/run-1"))
            );
            let Some(ImportJob::File {
                path,
                format,
                options,
            }) = &file
            else {
                panic!("a picked file must be the job");
            };
            assert_eq!(path, &PathBuf::from("/data/people.csv"));
            assert_eq!(*format, FileFormat::Csv);
            assert_eq!(options.delimiter, CsvDelimiter::Semicolon);
        },
    );
}

#[gpui::test]
fn continuing_applies_a_target_table_typed_without_confirming_it(cx: &mut gpui::TestAppContext) {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::migrate_wizard::source_target::shared_coordinator_tests::{
        PickerFakeConnection, connect_profile,
    };
    use dbflux_core::SchemaLoadingStrategy;

    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let profile_id = Uuid::new_v4();
    connect_profile(
        &app_state,
        cx,
        profile_id,
        PickerFakeConnection::new(SchemaLoadingStrategy::SingleDatabase),
        None,
    );
    let directory = std::env::temp_dir().join(format!(
        "dbflux_import_wizard_typed_table_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("create scratch dir");
    let path = directory.join("people.csv");
    std::fs::write(&path, "id,name\n1,Ada\n").expect("write csv");

    let slot: Rc<RefCell<Option<Entity<ImportWizard>>>> = Rc::default();
    let capture = Rc::clone(&slot);
    let file_path = path.clone();
    let (_root, window) = cx.add_window_view(move |window, cx| {
        let wizard = cx.new(|cx| ImportWizard::new(app_state, cx));
        wizard.update(cx, |this, cx| {
            this.open(profile_id, None, window, cx);
            this.file_import = Some(FileImportState {
                path: file_path,
                format: FileFormat::Csv,
                options: FormatOptions::default(),
                error: None,
                generation: 0,
                continue_after_read: false,
            });
            let file = dbflux_transfer::import_file::ImportFile::inspect(
                &this.file_import.as_ref().expect("state").path,
                FileFormat::Csv,
                FormatOptions::default(),
            )
            .expect("inspect");
            this.build_rows(
                vec![crate::import_wizard::TableImportConfig::new(
                    &file.manifest_table(),
                    false,
                    Vec::new(),
                )],
                cx,
            );
            this.step = WizardStep::Configure;
            if let Some(inputs) = &this.file_inputs {
                inputs
                    .target_table
                    .update(cx, |input, cx| input.set_value("renamed", window, cx));
            }
        });
        *capture.borrow_mut() = Some(wizard.clone());
        gpui_component::Root::new(wizard, window, cx)
    });
    let wizard = slot.borrow().clone().expect("wizard");

    wizard.update(window, |this, cx| this.continue_from_configure(cx));
    window.run_until_parked();

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert_eq!(wizard.rows[0].config.target_table, "renamed");
        assert!(
            !matches!(wizard.step, WizardStep::Configure),
            "continuing runs the import once the typed table is applied"
        );
    });
    std::fs::remove_dir_all(&directory).ok();
}

/// A one-column table row for a file named `people.csv`.
fn people_row() -> crate::import_wizard::TableImportConfig {
    let manifest_table = dbflux_transfer::manifest::ManifestTable {
        schema: None,
        name: "people".to_string(),
        file: "people.csv".to_string(),
        format: "csv".to_string(),
        columns: vec![dbflux_core::TransferColumn {
            name: "id".to_string(),
            type_name: None,
            nullable: true,
            is_primary_key: false,
        }],
        row_count: 0,
        fk_order_index: 0,
        options: FormatOptions::default(),
    };
    crate::import_wizard::TableImportConfig::new(&manifest_table, false, Vec::new())
}

/// Every step of the wizard, with a CSV file picked so the file options show.
/// The wizard is a dialog: each control is a Tab stop inside it.
#[gpui::test]
fn the_wizard_steps_are_covered(cx: &mut gpui::TestAppContext) {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::keyboard_coverage::IMPORT_WIZARD;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, MODAL_CHROME};

    init_test_runtime(cx);
    cx.update(dbflux_ui_base::keymap::init_keymap);
    let app_state = isolated_test_app_state(cx);

    let slot: Rc<RefCell<Option<Entity<ImportWizard>>>> = Rc::default();
    let capture_slot = Rc::clone(&slot);
    let (_root, window) = cx.add_window_view(move |window, cx| {
        let wizard = cx.new(|cx| ImportWizard::new(app_state, cx));
        wizard.update(cx, |this, cx| {
            this.open(Uuid::new_v4(), None, window, cx);
            this.file_import = Some(file_state(FileFormat::Csv, None));
            this.build_rows(vec![people_row()], cx);
        });
        *capture_slot.borrow_mut() = Some(wizard.clone());
        gpui_component::Root::new(wizard, window, cx)
    });
    let wizard = slot.borrow().clone().expect("wizard");
    let capture = FrameCapture::observe(window);

    let mut checked = Vec::new();
    for step in [
        WizardStep::PickFolder,
        WizardStep::Configure,
        WizardStep::Confirm,
        WizardStep::Running,
        WizardStep::Done,
    ] {
        wizard.update(window, |this, cx| {
            this.step = step;
            cx.notify();
        });
        window.run_until_parked();
        checked.extend(
            Coverage::new(IMPORT_WIZARD)
                .with_surface(MODAL_CHROME)
                .assert_covered(&capture.frame(window)),
        );
    }

    for expected in [
        "import-wizard-choose-file",
        "import-wizard-file-apply-target",
        "import-wizard-continue",
        "import-wizard-close",
    ] {
        assert!(
            checked.iter().any(|id| id == expected),
            "{expected} in {checked:?}"
        );
    }
}

/// Opens a wizard on a fake connection and returns it with its window.
fn wizard_on_fake_connection(
    cx: &mut gpui::TestAppContext,
) -> (Entity<ImportWizard>, &mut gpui::VisualTestContext) {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::migrate_wizard::source_target::shared_coordinator_tests::{
        PickerFakeConnection, connect_profile,
    };
    use dbflux_core::SchemaLoadingStrategy;

    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);
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
    let wizard = slot.borrow().clone().expect("wizard");
    (wizard, window)
}

fn scratch_file(tag: &str, name: &str, bytes: &[u8]) -> (PathBuf, PathBuf) {
    let directory =
        std::env::temp_dir().join(format!("dbflux_import_wizard_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create scratch dir");
    let path = directory.join(name);
    std::fs::write(&path, bytes).expect("write file");
    (directory, path)
}

/// A Windows-1252 CSV with a non-ASCII header fails the first (UTF-8) read.
/// The error stays on the configure step with the file kept, so choosing
/// Windows-1252 reads the file again and decodes the header.
#[gpui::test]
fn a_windows_1252_header_can_be_fixed_by_choosing_the_encoding(cx: &mut gpui::TestAppContext) {
    let (directory, path) = scratch_file("cp1252", "people.csv", b"pr\xe9nom,id\nAda,1\n");
    let (wizard, window) = wizard_on_fake_connection(cx);

    wizard.update(window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(
            wizard.step == WizardStep::Configure,
            "the encoding is reachable"
        );
        assert!(
            wizard.file_import_blocked(),
            "the run waits for a good read"
        );
        let state = wizard.file_import.as_ref().expect("the file is kept");
        assert!(state.error.is_some(), "the read error is shown");
    });

    wizard.update(window, |this, cx| this.select_encoding("windows1252", cx));
    window.run_until_parked();

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(!wizard.file_import_blocked());
        let names: Vec<&str> = wizard.rows[0]
            .config
            .source_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, ["prénom", "id"]);
    });
    std::fs::remove_dir_all(&directory).ok();
}

fn table_info(columns: Option<Vec<dbflux_core::ColumnInfo>>) -> dbflux_core::TableInfo {
    let mut info =
        crate::migrate_wizard::source_target::shared_coordinator_tests::test_table(None, "people");
    info.columns = columns;
    info
}

fn snapshot_with(table: &str) -> dbflux_core::SchemaSnapshot {
    let mut schema = dbflux_core::RelationalSchema::default();
    schema.schemas.push(dbflux_core::DbSchemaInfo {
        name: "public".to_string(),
        tables: vec![
            crate::migrate_wizard::source_target::shared_coordinator_tests::test_table(
                Some("public"),
                table,
            ),
        ],
        views: Vec::new(),
        custom_types: None,
    });
    dbflux_core::SchemaSnapshot::relational(schema)
}

fn lookup_error(message: &str) -> dbflux_core::DbError {
    dbflux_core::DbError::query_failed(message)
}

#[test]
fn a_found_table_gives_its_columns_and_a_confirmed_missing_one_is_absent() {
    use dbflux_core::DbError;

    assert!(matches!(
        super::existing_table_columns(
            Ok(table_info(Some(vec![dbflux_core::ColumnInfo {
                name: "id".to_string(),
                type_name: "int".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            }]))),
            "people",
            || panic!("the listing is not consulted for a found table")
        ),
        Ok(Some(columns)) if columns.len() == 1
    ));
    assert!(matches!(
        super::existing_table_columns(
            Err(DbError::object_not_found("people")),
            "people",
            || panic!("the listing is not consulted for a confirmed miss")
        ),
        Ok(None)
    ));
}

/// A driver that flattens its errors fails the lookup with a generic error:
/// the schema listing decides whether the table is there.
#[test]
fn a_failed_lookup_of_a_table_the_listing_does_not_hold_is_absent() {
    let result = super::existing_table_columns(
        Err(lookup_error("relation does not exist")),
        "people",
        || Some(snapshot_with("orders")),
    );

    assert!(matches!(result, Ok(None)), "{result:?}");
}

#[test]
fn a_failed_lookup_of_a_table_the_listing_holds_keeps_the_original_error() {
    let failure =
        super::existing_table_columns(Err(lookup_error("permission glitch")), "people", || {
            Some(snapshot_with("People"))
        })
        .expect_err("the table exists, so the lookup failure is real");

    assert!(
        failure.to_string().contains("permission glitch"),
        "{failure}"
    );
}

#[test]
fn a_failed_lookup_with_a_failed_listing_keeps_the_original_error() {
    let failure = super::existing_table_columns(Err(lookup_error("link down")), "people", || None)
        .expect_err("nothing confirms the table is absent");

    let message = failure.to_string();
    assert!(message.contains("link down"), "{message}");
}

/// A read from a previous session finishing after the wizard was reopened is
/// ignored, even while the new session has a read in flight.
#[gpui::test]
fn a_read_from_the_previous_session_is_ignored(cx: &mut gpui::TestAppContext) {
    let (wizard, window) = wizard_on_fake_connection(cx);

    let stale = wizard.update(window, |this, _| {
        let stale = this.next_file_read_request();
        let mut state = file_state(FileFormat::Csv, None);
        state.generation = stale;
        this.file_import = Some(state);
        this.loading = true;
        stale
    });

    wizard.update_in(window, |this, window, cx| {
        let profile_id = this.profile_id.expect("profile");
        this.open(profile_id, None, window, cx);
        let mut state = file_state(FileFormat::Csv, None);
        state.generation = this.next_file_read_request();
        this.file_import = Some(state);
        this.loading = true;
    });

    wizard.update(window, |this, cx| {
        this.finish_file_read(
            stale,
            false,
            Ok(super::LoadedFile {
                config: people_row(),
            }),
            cx,
        );
    });

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(wizard.rows.is_empty(), "the stale read is not applied");
        assert!(wizard.loading, "the new session's read is still pending");
        assert!(wizard.step == WizardStep::PickFolder);
    });
}

/// A JSON file has no option that could fix a failed read, so the wizard
/// stays on the picker step, which has the way back, with the error shown and
/// the file forgotten.
#[gpui::test]
fn a_broken_json_file_stays_on_the_picker_step_with_its_error(cx: &mut gpui::TestAppContext) {
    let (directory, path) = scratch_file("broken_json", "people.json", b"[{\"id\": 1}, {oops");
    let (wizard, window) = wizard_on_fake_connection(cx);

    wizard.update(window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(wizard.step == WizardStep::PickFolder);
        assert!(wizard.file_import.is_none(), "the file state is cleared");
        assert!(wizard.manifest_error.is_some(), "the error is shown");
        assert!(!wizard.loading);
    });
    std::fs::remove_dir_all(&directory).ok();
}

fn databases_only_snapshot() -> dbflux_core::SchemaSnapshot {
    let mut schema = dbflux_core::RelationalSchema::default();
    schema.databases.push(dbflux_core::DatabaseInfo {
        name: "shop".to_string(),
        is_current: true,
    });
    dbflux_core::SchemaSnapshot::relational(schema)
}

fn database_schema(table: &str) -> dbflux_core::DbSchemaInfo {
    dbflux_core::DbSchemaInfo {
        name: "shop".to_string(),
        tables: vec![
            crate::migrate_wizard::source_target::shared_coordinator_tests::test_table(None, table),
        ],
        views: Vec::new(),
        custom_types: None,
    }
}

/// The lookup that fails with a generic error, decided from the listing the
/// production closure composes.
fn lookup_with_listing(
    first: Option<dbflux_core::SchemaSnapshot>,
    lazy: bool,
    for_database: Option<dbflux_core::DbSchemaInfo>,
) -> Result<Option<Vec<dbflux_core::TransferColumn>>, dbflux_transfer::TransferError> {
    super::existing_table_columns(Err(lookup_error("lock wait timeout")), "people", || {
        super::complete_listing(first, lazy, || for_database)
    })
}

#[test]
fn a_databases_only_snapshot_defers_to_the_target_databases_listing() {
    let absent = lookup_with_listing(
        Some(databases_only_snapshot()),
        false,
        Some(database_schema("orders")),
    );
    assert!(matches!(absent, Ok(None)), "{absent:?}");

    let present = lookup_with_listing(
        Some(databases_only_snapshot()),
        false,
        Some(database_schema("people")),
    )
    .expect_err("the table is listed, so the lookup failure is real");
    assert!(
        present.to_string().contains("lock wait timeout"),
        "{present}"
    );
}

#[test]
fn a_databases_only_snapshot_with_an_unreadable_database_listing_keeps_the_error() {
    let unreadable = lookup_with_listing(Some(databases_only_snapshot()), false, None);
    assert!(unreadable.is_err(), "{unreadable:?}");
}

/// A database listing that was read enumerates the database: when it holds
/// no tables at all, the table is absent (the first table of an empty
/// database on an RPC connection).
#[test]
fn an_empty_enumerated_database_listing_makes_the_table_absent() {
    let mut empty = database_schema("orders");
    empty.tables.clear();

    let absent = lookup_with_listing(Some(databases_only_snapshot()), false, Some(empty.clone()));
    assert!(matches!(absent, Ok(None)), "{absent:?}");

    let lazy = lookup_with_listing(None, true, Some(empty));
    assert!(lazy.is_err(), "no snapshot at all rules nothing out");
}

#[test]
fn a_lazily_loading_connection_defers_to_the_database_listing_even_with_tables() {
    let absent = lookup_with_listing(
        Some(snapshot_with("people")),
        true,
        Some(database_schema("orders")),
    );
    assert!(matches!(absent, Ok(None)), "{absent:?}");
}

#[test]
fn a_complete_snapshot_is_used_as_it_is() {
    let absent = super::existing_table_columns(Err(lookup_error("gone")), "people", || {
        super::complete_listing(Some(snapshot_with("orders")), false, || {
            panic!("a complete listing needs no second one")
        })
    });
    assert!(matches!(absent, Ok(None)), "{absent:?}");
}

/// PostgreSQL lists schemas with no tables for an empty database: the
/// database listing decides, and an unsupported one keeps the error (Redshift).
#[test]
fn a_snapshot_of_empty_schemas_defers_to_the_database_listing() {
    let mut schema = dbflux_core::RelationalSchema::default();
    schema.schemas.push(dbflux_core::DbSchemaInfo {
        name: "public".to_string(),
        tables: Vec::new(),
        views: Vec::new(),
        custom_types: None,
    });
    let snapshot = || Some(dbflux_core::SchemaSnapshot::relational(schema.clone()));

    let mut empty = database_schema("orders");
    empty.tables.clear();
    let absent = lookup_with_listing(snapshot(), false, Some(empty));
    assert!(matches!(absent, Ok(None)), "{absent:?}");

    let unsupported = lookup_with_listing(snapshot(), false, None);
    assert!(unsupported.is_err(), "{unsupported:?}");
}

/// SQL Server and Redshift answer a lookup of a missing table with no
/// columns instead of an error: the listing decides whether it is there.
#[test]
fn a_lookup_with_no_columns_is_decided_by_the_listing() {
    for columns in [None, Some(Vec::new())] {
        let absent =
            super::existing_table_columns(Ok(table_info(columns.clone())), "people", || {
                Some(snapshot_with("orders"))
            });
        assert!(matches!(absent, Ok(None)), "{absent:?}");

        let present =
            super::existing_table_columns(Ok(table_info(columns.clone())), "people", || {
                Some(snapshot_with("People"))
            });
        assert!(
            matches!(&present, Ok(Some(found)) if found.is_empty()),
            "{present:?}"
        );

        let undecided = super::existing_table_columns(Ok(table_info(columns)), "people", || None);
        assert!(
            matches!(&undecided, Ok(Some(found)) if found.is_empty()),
            "{undecided:?}"
        );
    }
}

#[test]
fn a_lookup_with_columns_does_not_consult_the_listing() {
    let column = dbflux_core::ColumnInfo {
        name: "id".to_string(),
        type_name: "int".to_string(),
        nullable: false,
        is_primary_key: true,
        default_value: None,
        enum_values: None,
    };
    let found = super::existing_table_columns(Ok(table_info(Some(vec![column]))), "people", || {
        panic!("the listing is not consulted when the lookup has columns")
    });
    assert!(
        matches!(&found, Ok(Some(columns)) if columns.len() == 1),
        "{found:?}"
    );
}
