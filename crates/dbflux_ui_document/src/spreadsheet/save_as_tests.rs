use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dbflux_app::keymap::Command;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::{SaveTargetOutcome, SaveTargetProvider};
use gpui::{Entity, TestAppContext, VisualTestContext};

use super::document::SpreadsheetDocument;
use super::tests::{TestDirectory, fixture, open_local, toast_count};
use crate::keyboard_coverage::SPREADSHEET;

/// A picker that answers every Save As with `path` and counts how often it
/// was asked.
fn picker(path: PathBuf) -> (SaveTargetProvider, Arc<AtomicUsize>) {
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = asked.clone();

    let provider: SaveTargetProvider = Arc::new(move |request| {
        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.default_extension, "xlsx");

        gpui::Task::ready(SaveTargetOutcome::Selected {
            path: path.clone(),
            used_fallback: false,
        })
    });

    (provider, asked)
}

/// The paths a document asked to open after a Save As.
type OpenedPaths = Rc<RefCell<Vec<PathBuf>>>;

/// An open xls document, its window, the file's path, how often the picker
/// was asked, and the paths the document asked to open.
type OpenedXls<'a> = (
    Entity<SpreadsheetDocument>,
    &'a mut VisualTestContext,
    PathBuf,
    Arc<AtomicUsize>,
    OpenedPaths,
);

/// Opens the `in.xls` fixture with a picker that answers `target`, and
/// records the paths the document asks to open after a Save As.
fn open_xls<'a>(
    cx: &'a mut TestAppContext,
    directory: &TestDirectory,
    target: impl FnOnce(&PathBuf) -> PathBuf,
) -> OpenedXls<'a> {
    let path = directory.file("in.xls", &fixture("in.xls"));
    let (provider, asked) = picker(target(&path));
    let (document, window) = open_local(cx, path.clone());

    let opened = Rc::new(RefCell::new(Vec::new()));
    let recorded = opened.clone();

    window.update(|_, cx| {
        document.update(cx, |document, _| {
            document.set_save_target_override(Some(provider));
            document.set_on_saved_as(move |path, _cx| recorded.borrow_mut().push(path));
        })
    });

    (document, window, path, asked, opened)
}

fn prompt_message(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Option<String> {
    window.update(|_, cx| document.read(cx).save_as_prompt_message())
}

fn request_save_as(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) {
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::SaveFileAs, window, cx);
        })
    });
    window.run_until_parked();
}

fn confirm(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.confirm_save_as(cx)));
    window.run_until_parked();
}

#[gpui::test]
fn save_as_warns_before_writing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-as-warns");
    let (document, window, path, asked, opened) =
        open_xls(cx, &directory, |path| path.with_extension("xlsx"));

    assert_eq!(prompt_message(&document, window), None);

    request_save_as(&document, window);

    let message = prompt_message(&document, window).expect("the prompt is open");
    for lost in [
        "formulas",
        "formatting",
        "number formats",
        "merged cells",
        "column widths",
        "charts",
        "images",
        "macros",
        "every sheet",
        "in.xls is not changed",
    ] {
        assert!(message.contains(lost), "{message} names {lost}");
    }
    assert_eq!(asked.load(Ordering::SeqCst), 0, "no file is chosen yet");
    assert!(!path.with_extension("xlsx").exists());

    window.update(|_, cx| document.update(cx, |document, cx| document.dismiss_save_as_prompt(cx)));
    window.run_until_parked();

    assert_eq!(prompt_message(&document, window), None);
    assert_eq!(asked.load(Ordering::SeqCst), 0);
    assert!(!path.with_extension("xlsx").exists());
    assert!(opened.borrow().is_empty());
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn save_as_is_offered_for_xls_only_and_names_what_it_leaves_out(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-as-chart");
    let path = directory.file("items.xlsx", &super::tests::items_workbook());
    let (document, window) = open_local(cx, path);

    request_save_as(&document, window);

    assert_eq!(
        prompt_message(&document, window),
        None,
        "an xlsx is saved in place, not as a copy"
    );

    let message =
        crate::labels::spreadsheet_save_as_body("book.xls", &["Chart".to_string()], false);
    assert!(message.contains("Chart"), "{message}");

    let object = crate::labels::spreadsheet_save_as_body("book.xls", &[], true);
    assert!(object.contains("this computer"), "{object}");
}

#[gpui::test]
fn xls_is_never_replaced(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-as-source");
    let (document, window, path, asked, opened) = open_xls(cx, &directory, PathBuf::clone);
    let original = std::fs::read(&path).expect("the fixture was copied");

    request_save_as(&document, window);
    confirm(&document, window);

    assert_eq!(asked.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read(&path).expect("the source stays"), original);
    assert!(opened.borrow().is_empty(), "nothing new to open");
    assert_eq!(toast_count(window), 1, "the refusal is reported");

    let spelled_differently = path
        .parent()
        .expect("the file has a directory")
        .join(".")
        .join("in.xls");
    let (provider, _) = picker(spelled_differently);
    window.update(|_, cx| {
        document.update(cx, |document, _| {
            document.set_save_target_override(Some(provider))
        })
    });

    request_save_as(&document, window);
    confirm(&document, window);

    assert_eq!(std::fs::read(&path).expect("the source stays"), original);
    assert!(opened.borrow().is_empty());
    assert_eq!(toast_count(window), 2);
}

#[gpui::test]
fn save_as_writes_the_values_next_to_the_source(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-as-writes");
    let (document, window, path, asked, opened) =
        open_xls(cx, &directory, |path| path.with_extension("xlsx"));
    let original = std::fs::read(&path).expect("the fixture was copied");
    let target = path.with_extension("xlsx");

    request_save_as(&document, window);
    confirm(&document, window);

    assert_eq!(asked.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read(&path).expect("the source stays"), original);
    let resolved = std::fs::canonicalize(&target).expect("the new file resolves");
    assert_eq!(opened.borrow().as_slice(), std::slice::from_ref(&resolved));

    let mut written = dbflux_spreadsheet::open(dbflux_byte_source::MemorySource::new(
        std::fs::read(&target).expect("the new file exists"),
    ))
    .expect("the new file is a workbook");
    assert_eq!(
        written.format(),
        dbflux_spreadsheet::SpreadsheetFormat::Xlsx
    );
    let names: Vec<&str> = written
        .sheets()
        .iter()
        .map(|sheet| sheet.name.as_str())
        .collect();
    assert_eq!(names, ["Data", "Other"]);
    assert_eq!(
        written
            .read_sheet(0)
            .expect("the first sheet reads")
            .cell(0, 0)
            .map(|cell| cell.value.clone()),
        Some(dbflux_spreadsheet::CellValue::Text("Name".into()))
    );

    let leftovers: Vec<String> = std::fs::read_dir(path.parent().expect("a directory"))
        .expect("the directory lists")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with('.'))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[cfg(unix)]
#[gpui::test]
fn save_as_writes_into_the_directory_it_checked(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-as-alias");
    let (document, window, path, _, opened) = open_xls(cx, &directory, |path| {
        let source_directory = path.parent().expect("the file has a directory");
        let alias = source_directory.join("alias");
        std::os::unix::fs::symlink(source_directory, &alias).expect("the alias is created");

        alias.join("out.xlsx")
    });

    request_save_as(&document, window);
    confirm(&document, window);

    let resolved = std::fs::canonicalize(path.parent().expect("a directory"))
        .expect("the directory resolves")
        .join("out.xlsx");
    assert_eq!(
        opened.borrow().as_slice(),
        std::slice::from_ref(&resolved),
        "the file is written through the directory resolved before the check"
    );
    assert!(resolved.exists());
}

#[gpui::test]
fn save_as_is_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-as-coverage");
    let (document, window, _path, _asked, _opened) =
        open_xls(cx, &directory, |path| path.with_extension("xlsx"));

    let capture = FrameCapture::observe(window);
    let checked: Vec<String> = Coverage::new(SPREADSHEET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect();
    assert!(
        checked.iter().any(|id| id == "spreadsheet-save-as"),
        "{checked:?}"
    );

    let actions: Vec<String> = window.update(|_, cx| {
        document
            .read(cx)
            .pane_actions(&document)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    assert_eq!(actions, ["spreadsheet-save-as"]);

    request_save_as(&document, window);

    let capture = FrameCapture::observe(window);
    let checked: Vec<String> = Coverage::new(SPREADSHEET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect();
    for control in ["spreadsheet-save-as-confirm", "spreadsheet-save-as-cancel"] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }

    window.simulate_keystrokes("escape");
    window.run_until_parked();

    assert_eq!(prompt_message(&document, window), None);
}
