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
