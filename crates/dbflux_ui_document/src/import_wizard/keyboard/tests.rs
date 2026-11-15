//! Drives the import wizard's keyboard paths with real key presses under the
//! app keymap and checks the state the mouse controls change.

// Import only what the tests need: re-globbing `use gpui::*` next to
// `#[gpui::test]` blows rustc's macro recursion limit in this crate.
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use dbflux_core::SchemaLoadingStrategy;
use dbflux_core::TransferColumn;
use dbflux_export::{CsvDelimiter, FormatOptions, TextEncoding};
use dbflux_transfer::TableMappingMode;
use dbflux_transfer::manifest::ManifestTable;
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

use super::ConfigureField;
use crate::import_wizard::{ImportWizard, TableImportConfig, WizardStep};
use crate::keyboard_test_support::init_keyboard_runtime;
use crate::migrate_wizard::source_target::shared_coordinator_tests::{
    PickerFakeConnection, connect_profile,
};

fn isolated_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    })
}

/// An open wizard on a fake connection, under the app keymap.
fn open_wizard(cx: &mut TestAppContext) -> (Entity<ImportWizard>, &mut VisualTestContext) {
    init_keyboard_runtime(cx);
    let app_state = isolated_app_state(cx);
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

fn press(window: &mut VisualTestContext, keystrokes: &str) {
    for keystroke in keystrokes.split(' ') {
        window.simulate_keystrokes(keystroke);
        window.update(|window, _| window.refresh());
        window.run_until_parked();
    }
}

fn scratch_file(tag: &str, name: &str, bytes: &[u8]) -> (PathBuf, PathBuf) {
    let directory = std::env::temp_dir().join(format!(
        "dbflux_import_keyboard_{tag}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("create scratch dir");
    let path = directory.join(name);
    std::fs::write(&path, bytes).expect("write file");
    (directory, path)
}

/// Picks `path` as the import file, as the "Import file" picker does.
fn pick_file(wizard: &Entity<ImportWizard>, window: &mut VisualTestContext, path: PathBuf) {
    wizard.update(window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    window.run_until_parked();
}

fn source_names(wizard: &Entity<ImportWizard>, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, cx| {
        wizard.read(cx).rows[0]
            .config
            .source_columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    })
}

fn column(name: &str) -> TransferColumn {
    TransferColumn {
        name: name.to_string(),
        type_name: None,
        nullable: true,
        is_primary_key: false,
    }
}

/// A folder-style row: source columns `a`, `b` and one target column `x`.
fn rebind_row() -> TableImportConfig {
    let manifest_table = ManifestTable {
        schema: None,
        name: "people".to_string(),
        file: "people.csv".to_string(),
        format: "csv".to_string(),
        columns: vec![column("a"), column("b")],
        row_count: 0,
        fk_order_index: 0,
        options: FormatOptions::default(),
    };
    TableImportConfig::new(&manifest_table, true, vec![column("x")])
}

/// L and H pick the next and previous CSV delimiter and read the file again
/// with it, as choosing the segment does.
#[gpui::test]
fn l_and_h_cycle_the_csv_delimiter_and_read_the_file_again(cx: &mut TestAppContext) {
    let (directory, path) = scratch_file("delimiter", "people.csv", b"id;name\n1;Ada\n");
    let (wizard, window) = open_wizard(cx);
    pick_file(&wizard, window, path);

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert_eq!(wizard.step, WizardStep::Configure);
        assert_eq!(wizard.cursor_field(), Some(ConfigureField::Delimiter));
    });
    assert_eq!(source_names(&wizard, window), ["id;name"]);

    press(window, "l");
    window.update(|_, cx| {
        let options = &wizard.read(cx).file_import.as_ref().expect("file").options;
        assert_eq!(options.delimiter, CsvDelimiter::Semicolon);
    });
    assert_eq!(source_names(&wizard, window), ["id", "name"]);

    press(window, "h");
    window.update(|_, cx| {
        let options = &wizard.read(cx).file_import.as_ref().expect("file").options;
        assert_eq!(options.delimiter, CsvDelimiter::Comma);
    });
    assert_eq!(source_names(&wizard, window), ["id;name"]);

    press(window, "left");
    window.update(|_, cx| {
        let options = &wizard.read(cx).file_import.as_ref().expect("file").options;
        assert_eq!(options.delimiter, CsvDelimiter::Pipe, "wraps to the last");
    });
    std::fs::remove_dir_all(&directory).ok();
}

/// J moves the cursor to the encoding; Enter picks the next one, H the
/// previous one, and the file is read again with it.
#[gpui::test]
fn the_cursor_reaches_the_encoding_and_cycles_it(cx: &mut TestAppContext) {
    let (directory, path) = scratch_file("encoding", "people.csv", b"pr\xe9nom,id\nAda,1\n");
    let (wizard, window) = open_wizard(cx);
    pick_file(&wizard, window, path);

    window.update(|_, cx| {
        assert!(
            wizard.read(cx).file_import_blocked(),
            "the Windows-1252 header fails the UTF-8 read"
        );
    });

    press(window, "j");
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).cursor_field(),
            Some(ConfigureField::Encoding)
        );
    });

    press(window, "enter");
    window.update(|_, cx| {
        let options = &wizard.read(cx).file_import.as_ref().expect("file").options;
        assert_eq!(options.encoding, TextEncoding::Utf8Bom);
    });

    press(window, "h h");
    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        let options = &wizard.file_import.as_ref().expect("file").options;
        assert_eq!(options.encoding, TextEncoding::Windows1252);
        assert!(!wizard.file_import_blocked(), "the header decodes");
    });
    assert_eq!(source_names(&wizard, window), ["prénom", "id"]);
    std::fs::remove_dir_all(&directory).ok();
}

/// Enter on a row's mapping mode opens its dropdown; the dropdown's own keys
/// choose, and the row's mode changes as it does for a click.
#[gpui::test]
fn enter_opens_the_mapping_mode_dropdown_and_choosing_sets_the_mode(cx: &mut TestAppContext) {
    let (wizard, window) = open_wizard(cx);
    wizard.update(window, |this, cx| {
        this.build_rows(vec![rebind_row()], cx);
        this.step = WizardStep::Configure;
        cx.notify();
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert_eq!(wizard.cursor_field(), Some(ConfigureField::Mode(0)));
        assert_eq!(
            wizard.rows[0].config.mapping_mode,
            TableMappingMode::Existing
        );
    });

    press(window, "enter");
    window.update(|_, cx| {
        let dropdown = wizard.read(cx).rows[0].mapping_mode_dropdown.clone();
        assert!(dropdown.read(cx).is_open(), "Enter opens the dropdown");
    });

    // Existing is selected: the next item is Recreate.
    press(window, "j enter");
    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert_eq!(
            wizard.rows[0].config.mapping_mode,
            TableMappingMode::Recreate
        );
        assert!(!wizard.rows[0].mapping_mode_dropdown.read(cx).is_open());
    });
}

/// The target and source dropdowns open from the cursor and choose with
/// their own keys; applying the mapping rebinds the column as it does after
/// choosing with the mouse.
#[gpui::test]
fn the_cursor_opens_the_target_and_source_dropdowns(cx: &mut TestAppContext) {
    let (wizard, window) = open_wizard(cx);
    wizard.update(window, |this, cx| {
        this.build_rows(vec![rebind_row()], cx);
        this.step = WizardStep::Configure;
        cx.notify();
    });
    window.run_until_parked();

    press(window, "j");
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).cursor_field(),
            Some(ConfigureField::Target(0))
        );
    });
    press(window, "enter enter");
    window.update(|_, cx| {
        let dropdown = wizard.read(cx).rows[0].rebind_target_dropdown.clone();
        assert_eq!(dropdown.read(cx).selected_label().as_deref(), Some("x"));
    });

    press(window, "j");
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).cursor_field(),
            Some(ConfigureField::Source(0))
        );
    });
    // The source items are "(unset)", "a" and "b": two steps down is "b".
    press(window, "enter j j enter");
    window.update(|_, cx| {
        let dropdown = wizard.read(cx).rows[0].rebind_source_dropdown.clone();
        assert_eq!(dropdown.read(cx).selected_label().as_deref(), Some("b"));
    });

    wizard.update(window, |this, cx| this.apply_rebind(0, cx));
    window.update(|_, cx| {
        assert_eq!(wizard.read(cx).rows[0].config.bindings, vec![Some(1)]);
    });
}

/// K moves back up, stops at the first choice and at the last one, and a
/// step without choices keeps the keys to itself.
#[gpui::test]
fn the_cursor_stops_at_both_ends_and_other_steps_ignore_the_keys(cx: &mut TestAppContext) {
    let (wizard, window) = open_wizard(cx);

    press(window, "j");
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).field_cursor,
            0,
            "no choices on the first step"
        );
        assert!(wizard.read(cx).is_visible());
    });

    wizard.update(window, |this, cx| {
        this.build_rows(vec![rebind_row()], cx);
        this.step = WizardStep::Configure;
        cx.notify();
    });
    window.run_until_parked();

    press(window, "k");
    window.update(|_, cx| assert_eq!(wizard.read(cx).field_cursor, 0));
    press(window, "j j j j");
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).cursor_field(),
            Some(ConfigureField::Source(0))
        );
    });
    press(window, "k");
    window.update(|_, cx| {
        assert_eq!(
            wizard.read(cx).cursor_field(),
            Some(ConfigureField::Target(0))
        );
    });
}

/// Escape still closes the dialog (the dialog hands it to its owner now).
#[gpui::test]
fn escape_still_closes_the_dialog(cx: &mut TestAppContext) {
    let (wizard, window) = open_wizard(cx);

    press(window, "escape");

    window.update(|_, cx| assert!(!wizard.read(cx).is_visible()));
}
