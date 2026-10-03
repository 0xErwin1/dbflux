//! Quitting with a delimited tab open: what the quit check reports for each
//! state of the document, the save the shutdown flush runs, and the save and
//! the discard of a quit the user confirmed.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use gpui::{Entity, TestAppContext, VisualTestContext};

use super::columns::LoadingRest;
use super::document::DelimitedDocument;
use super::document_tests::{CITIES, WINDOWS_1252_NAMES, connect_profile, open, toast_count};
use super::editing::SHUTDOWN_SAVE_MAX_BYTES;
use super::editing_tests::{
    asked_to_close, is_dirty, local_file, read, record_events, save_results, type_into_cell,
};
use super::tests::{BUCKET, FakeConnection, KEY, TestDirectory};
use super::text_view::DelimitedView;
use super::text_view_tests::{replace_in_text, show};
use crate::pane::{PaneHandle, QuitDisposition};

fn open_local(
    cx: &mut TestAppContext,
    path: std::path::PathBuf,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    super::document_tests::open_local(cx, path)
}

fn pane(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> PaneHandle {
    window.update(|_, cx| DelimitedDocument::into_pane(document.clone(), cx))
}

fn disposition(pane: &PaneHandle, window: &mut VisualTestContext) -> QuitDisposition {
    window.update(|_, cx| pane.quit_disposition(cx))
}

/// Polls the shutdown flush the way the shutdown loop does, until it reports
/// nothing outstanding, and returns how many polls reported a write running.
fn flush_until_idle(pane: &PaneHandle, window: &mut VisualTestContext) -> usize {
    for outstanding_polls in 0..50 {
        let outstanding = window.update(|_, cx| pane.flush_for_shutdown(cx));
        window.run_until_parked();

        if !outstanding {
            return outstanding_polls;
        }
    }

    panic!("the shutdown flush never finished");
}

const EDITED: &[u8] = b"name,city\nAna,Cusco\nBo,Quito\n";

#[gpui::test]
fn a_clean_tab_is_clean_for_the_quit_and_the_flush_writes_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-clean");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    assert_eq!(disposition(&pane, window), QuitDisposition::Clean);
    assert_eq!(flush_until_idle(&pane, window), 0);
    assert_eq!(read(&path), CITIES);
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn a_dirty_local_tab_that_saves_safely_is_saved_by_the_shutdown_flush(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-safe");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    assert_eq!(disposition(&pane, window), QuitDisposition::SavedOnQuit);

    flush_until_idle(&pane, window);

    assert_eq!(read(&path), EDITED, "only the edited record changes");
    assert!(!is_dirty(&document, window));
    assert_eq!(save_results(&events), [true]);
    assert!(!asked_to_close(&events), "a quit never closes the tab");
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn the_shutdown_flush_saves_once_however_often_it_is_polled(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-once");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let first_poll = window.update(|_, cx| pane.flush_for_shutdown(cx));
    let second_poll = window.update(|_, cx| pane.flush_for_shutdown(cx));
    window.run_until_parked();

    assert!(first_poll, "the save runs after the first poll");
    assert!(second_poll, "the second poll waits for the same save");
    assert_eq!(flush_until_idle(&pane, window), 0);
    assert_eq!(save_results(&events), [true]);
    assert_eq!(read(&path), EDITED);
}

#[gpui::test]
fn a_dirty_local_tab_whose_file_changed_elsewhere_needs_a_decision(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-changed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    std::fs::write(&path, b"name,city\nAna,Lima\nBo,Quito\nCy,Oslo\n")
        .expect("the test file is writable");

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
    assert_eq!(toast_count(window), 0, "the check reports nothing");
}

#[gpui::test]
fn a_dirty_local_tab_whose_file_is_gone_needs_a_decision(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-gone");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    std::fs::remove_file(&path).expect("the test file is removable");

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
}

#[gpui::test]
fn raw_text_that_does_not_parse_needs_a_decision(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-unparsed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Bo,Quito", "Bo,\"Quito");

    assert!(is_dirty(&document, window));
    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
    assert_eq!(toast_count(window), 0, "the check reports nothing");
}

#[gpui::test]
fn raw_text_that_parses_is_saved_by_the_shutdown_flush(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-parsed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Ana,Lima", "Ana,Cusco");

    assert_eq!(disposition(&pane, window), QuitDisposition::SavedOnQuit);

    flush_until_idle(&pane, window);

    assert_eq!(read(&path), EDITED);
}

#[gpui::test]
fn an_edit_the_writer_refuses_needs_a_decision(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-unencodable");
    let path = local_file(&directory, "names.csv", WINDOWS_1252_NAMES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "東京");

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
}

/// A save is refused while the rest of the file is loaded, so the check asks
/// instead of leaving the flush a save it would refuse.
#[gpui::test]
fn a_dirty_tab_whose_rest_is_loading_needs_a_decision(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-loading-rest");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    // The state of a load of the rest that is still reading.
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let loaded = document.loaded_mut().expect("the file is loaded");
            loaded.loading_rest = Some(LoadingRest {
                cancel: Arc::new(AtomicBool::new(false)),
            });
            cx.notify();
        })
    });

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
}

/// The save refuses a read-only file, so the quit asks instead of leaving the
/// shutdown a save that fails while the application goes away. The check
/// reads the permission bits the save reads, so it holds for any user.
#[cfg(unix)]
#[gpui::test]
fn a_read_only_file_needs_a_decision(cx: &mut TestAppContext) {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("quit-read-only");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))
        .expect("the permissions apply");

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
    assert_eq!(toast_count(window), 0, "the check reports nothing");
}

/// The save stages its bytes next to the file, so a directory the user
/// cannot write fails it; the check asks instead, and leaves no file behind.
#[cfg(unix)]
#[gpui::test]
fn a_file_in_a_directory_that_cannot_be_written_needs_a_decision(cx: &mut TestAppContext) {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("quit-read-only-directory");
    let path = local_file(&directory, "cities.csv", CITIES);
    let parent = path
        .parent()
        .expect("the file has a directory")
        .to_path_buf();

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555))
        .expect("the permissions apply");

    let probe = parent.join("probe");
    let writable = std::fs::File::create(&probe).is_ok();
    std::fs::remove_file(&probe).ok();

    let observed = disposition(&pane, window);
    let entries: Vec<_> = std::fs::read_dir(&parent)
        .expect("the directory lists")
        .filter_map(|entry| entry.ok().map(|entry| entry.file_name()))
        .collect();

    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755))
        .expect("the permissions are restored");

    assert!(
        !writable,
        "this test needs a user that a 0555 directory refuses: it cannot run as root"
    );
    assert_eq!(observed, QuitDisposition::NeedsDecision);
    assert_eq!(entries, ["cities.csv"], "the check leaves no file behind");
}

/// Without a prompt (a terminal signal) the flush still tries the save, and a
/// failure is reported once and not retried.
#[cfg(unix)]
#[gpui::test]
fn a_failing_shutdown_save_without_a_prompt_is_reported_once(cx: &mut TestAppContext) {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("quit-read-only-flush");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))
        .expect("the permissions apply");

    flush_until_idle(&pane, window);
    flush_until_idle(&pane, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        super::document_tests::last_toast_title(window).as_deref(),
        Some("Could not save cities.csv")
    );
    assert_eq!(save_results(&events), [false]);
    assert_eq!(read(&path), CITIES);
    assert!(is_dirty(&document, window));
}

/// A CSV of `bytes` bytes or a little more: a header and numbered records.
pub(super) fn csv_of_at_least(bytes: usize) -> Vec<u8> {
    let mut csv = b"id,city\n".to_vec();
    let mut record = 0usize;

    while csv.len() < bytes {
        csv.extend_from_slice(format!("{record:09},Lima\n").as_bytes());
        record += 1;
    }

    csv
}

/// A file too large to rewrite within the shutdown's flush budget asks
/// instead.
#[gpui::test]
fn a_file_over_the_shutdown_save_limit_needs_a_decision(cx: &mut TestAppContext) {
    let limit = usize::try_from(SHUTDOWN_SAVE_MAX_BYTES).expect("the limit fits");

    let directory = TestDirectory::new("quit-large");
    let path = local_file(&directory, "large.csv", &csv_of_at_least(limit + 1));

    let (document, window) = open_local(cx, path);
    let pane = pane(&document, window);
    type_into_cell(&document, window, 0, 1, "Cusco");

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
}

#[gpui::test]
fn a_file_at_the_shutdown_save_limit_is_saved_on_quit(cx: &mut TestAppContext) {
    let limit = usize::try_from(SHUTDOWN_SAVE_MAX_BYTES).expect("the limit fits");

    let directory = TestDirectory::new("quit-at-limit");
    let mut bytes = csv_of_at_least(limit - 64);
    bytes.truncate(bytes.len().min(limit));
    while !bytes.ends_with(b"\n") {
        bytes.pop();
    }
    let path = local_file(&directory, "limit.csv", &bytes);

    let (document, window) = open_local(cx, path);
    let pane = pane(&document, window);
    type_into_cell(&document, window, 0, 1, "Cusco");

    assert_eq!(disposition(&pane, window), QuitDisposition::SavedOnQuit);
}

/// Nothing displays the file after a shutdown save, so the save does not read
/// it again: the reader the file was opened with stays.
#[gpui::test]
fn a_shutdown_save_does_not_read_the_file_again(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-no-reread");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let reader_epoch = |window: &mut VisualTestContext| {
        window.update(|_, cx| document.read(cx).loaded().map(|loaded| loaded.reader_epoch))
    };
    let before = reader_epoch(window);

    flush_until_idle(&pane, window);

    assert_eq!(read(&path), EDITED);
    assert_eq!(reader_epoch(window), before, "the file is not read again");
    assert!(
        !is_dirty(&document, window),
        "the saved changes are not pending"
    );
}

/// Without a prompt (a terminal signal) the flush still tries a save that the
/// check would have asked about, and its refusal is reported once.
#[gpui::test]
fn a_refused_shutdown_save_of_text_that_does_not_parse_is_reported_once(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit-unparsed-flush");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Bo,Quito", "Bo,\"Quito");

    flush_until_idle(&pane, window);
    flush_until_idle(&pane, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(read(&path), CITIES);
}

fn open_object(
    cx: &mut TestAppContext,
    connection: Arc<FakeConnection>,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    open(cx, move |cx| {
        DelimitedDocument::open_object(
            app_state,
            profile_id,
            connection,
            BUCKET.to_string(),
            KEY.to_string(),
            cx,
        )
    })
}

#[gpui::test]
fn a_dirty_object_always_needs_a_decision_and_the_flush_leaves_it(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (document, window) = open_object(cx, connection.clone());
    let pane = pane(&document, window);

    assert_eq!(disposition(&pane, window), QuitDisposition::Clean);

    type_into_cell(&document, window, 0, 1, "Cusco");

    assert_eq!(disposition(&pane, window), QuitDisposition::NeedsDecision);
    assert_eq!(flush_until_idle(&pane, window), 0);
    assert_eq!(connection.store.bytes(), CITIES, "nothing is uploaded");
}

/// A terminal signal quits without asking, so a dirty object is dropped; the
/// flush records that once, with the object's key, instead of dropping it
/// without a trace.
#[gpui::test]
fn a_dirty_object_dropped_by_the_shutdown_flush_is_recorded_once(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (document, window) = open_object(cx, connection.clone());
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    flush_until_idle(&pane, window);
    flush_until_idle(&pane, window);

    let recorded = window.update(|_, cx| document.read(cx).dropped_at_shutdown.clone());
    assert_eq!(recorded, [format!("{BUCKET}/{KEY}")]);
}

#[gpui::test]
fn a_clean_object_is_not_recorded_as_dropped(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (document, window) = open_object(cx, connection.clone());
    let pane = pane(&document, window);

    flush_until_idle(&pane, window);

    let recorded = window.update(|_, cx| document.read(cx).dropped_at_shutdown.clone());
    assert!(recorded.is_empty());
}

#[gpui::test]
fn the_save_of_a_confirmed_quit_saves_without_closing_the_tab(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (document, window) = open_object(cx, connection.clone());
    let (events, _subscription) = record_events(&document, window);
    let pane = pane(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let started = window.update(|window, cx| pane.save_for_quit(window, cx));
    window.run_until_parked();

    assert!(started);
    assert_eq!(connection.store.bytes(), EDITED);
    assert_eq!(save_results(&events), [true]);
    assert!(!asked_to_close(&events));
}

#[gpui::test]
fn the_discard_of_a_confirmed_quit_drops_the_edits_and_the_flush_writes_nothing(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("quit-discard");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let pane = pane(&document, window);

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Bo,Quito", "Bo,\"Quito");

    window.update(|_, cx| pane.discard_for_quit(cx));
    window.run_until_parked();

    assert!(!is_dirty(&document, window));
    assert_eq!(flush_until_idle(&pane, window), 0);
    assert_eq!(read(&path), CITIES);
    assert_eq!(toast_count(window), 0);
}
