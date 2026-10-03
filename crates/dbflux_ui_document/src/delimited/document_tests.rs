use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;

use dbflux_app::keymap::Command;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_delimited::{
    Dialect, Encoding, Page, ReaderOptions, Record, RecordCount, SampleCoverage,
};
use dbflux_test_support::fake_driver::FakeDriver;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

use dbflux_ui_base::keyboard_coverage::KeyboardPath;
use dbflux_ui_base::user_error::ErrorKind;

use super::document::{
    DelimitedDocument, DelimitedWarning, PAGE_SIZE, READER_OPTIONS, SAMPLE_BYTES, extension_hint,
    open_error_to_user_facing, open_first_page, read_first_page, sample_coverage,
    settled_record_count,
};
use super::source::{DelimitedLocation, open_source};
use super::tests::{BUCKET, FakeConnection, KEY, TestDirectory};
use crate::dedup::{DelimitedFileKey, DocumentKey};
use crate::keyboard_coverage::DELIMITED;
use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
use crate::types::{DocumentKind, DocumentState};

const CITIES: &[u8] = b"name,city\nAna,Lima\nBo,Quito\n";

/// Two records to a page, and a fetch window shorter than a page, so every
/// further page is read from the source and not from bytes already fetched.
const SMALL_PAGES: ReaderOptions = ReaderOptions {
    page_size: NonZeroUsize::new(2).unwrap(),
    window_size: NonZeroU64::new(16).unwrap(),
};

/// A CSV file with a header and `record_total` records whose first field is
/// the record's index.
fn numbered_csv(record_total: usize) -> Vec<u8> {
    let mut bytes = b"id,city\n".to_vec();

    for record in 0..record_total {
        bytes.extend_from_slice(format!("{record},Lima\n").as_bytes());
    }

    bytes
}

/// Hosts the document `build` returns in a window, gives it the keyboard and
/// runs the open flow to its end.
fn open(
    cx: &mut TestAppContext,
    build: impl FnOnce(&mut gpui::Context<DelimitedDocument>) -> DelimitedDocument + 'static,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    init_keyboard_runtime(cx);

    let (host, window) = host_document(
        cx,
        move |_window, cx| cx.new(build),
        |document, _cx| document.active_context(),
        |document, command, window, cx| document.dispatch_command(command, window, cx),
    );

    let document = window.update(|_, cx| host.read(cx).document.clone());
    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();

    (document, window)
}

fn open_local(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    open(cx, move |cx| DelimitedDocument::open_local(path, cx))
}

fn open_local_in_small_pages(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    open(cx, move |cx| {
        DelimitedDocument::open_local_with(path, SMALL_PAGES, cx)
    })
}

fn open_object_in_small_pages(
    cx: &mut TestAppContext,
    app_state: Entity<AppStateEntity>,
    profile_id: uuid::Uuid,
    connection: Arc<FakeConnection>,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    open(cx, move |cx| {
        DelimitedDocument::open_object_with(
            app_state,
            profile_id,
            connection,
            BUCKET.to_string(),
            KEY.to_string(),
            SMALL_PAGES,
            cx,
        )
    })
}

/// An app state in which `connection` is the live connection of one
/// profile. Returns the state and the profile id.
fn connect_profile(
    cx: &mut TestAppContext,
    connection: Arc<FakeConnection>,
) -> (Entity<AppStateEntity>, uuid::Uuid) {
    let driver = FakeDriver::new(dbflux_core::DbKind::SQLite);
    let (app_state, profile_id) =
        crate::keyboard_test_support::connected_app_state(cx, &driver, "reports");

    replace_connection(cx, &app_state, profile_id, connection);

    (app_state, profile_id)
}

fn replace_connection(
    cx: &mut TestAppContext,
    app_state: &Entity<AppStateEntity>,
    profile_id: uuid::Uuid,
    connection: Arc<FakeConnection>,
) {
    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            let connected = state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the profile is connected");

            connected.connection = connection;
        });
    });
}

fn load_more(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.load_more(cx)));
    window.run_until_parked();
}

fn has_more_records(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| document.read(cx).has_more_records())
}

fn is_loading_more(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| document.read(cx).is_loading_more())
}

/// The text of the first cell of every row.
fn first_column(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    window.update(|_, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table");

        table_state
            .read(cx)
            .model()
            .rows
            .iter()
            .map(|row| row.cells[0].edit_text())
            .collect()
    })
}

fn last_toast_title(window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_title())
}

/// The ids of the clickable elements the coverage check found in the frame.
fn covered_ids(window: &mut VisualTestContext) -> Vec<String> {
    let capture = FrameCapture::observe(window);

    Coverage::new(DELIMITED)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect()
}

fn column_titles(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    window.update(|_, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table");

        table_state
            .read(cx)
            .model()
            .columns
            .iter()
            .map(|column| column.title.to_string())
            .collect()
    })
}

fn row_count(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table");

        table_state.read(cx).model().rows.len()
    })
}

fn status_items(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    window.update(|_, cx| {
        document
            .read(cx)
            .status_items()
            .iter()
            .map(|item| item.to_string())
            .collect()
    })
}

fn warnings(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<DelimitedWarning> {
    window.update(|_, cx| document.read(cx).warnings().to_vec())
}

fn state(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> DocumentState {
    window.update(|_, cx| document.read(cx).state())
}

fn toast_count(window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).toast_count())
}

#[gpui::test]
fn a_local_csv_opens_with_its_columns_rows_and_status(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-csv");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert_eq!(row_count(&document, window), 2);
    assert_eq!(
        status_items(&document, window),
        ["Delimiter: Comma", "Encoding: UTF-8", "2 records"]
    );
    assert!(warnings(&document, window).is_empty());
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn the_document_is_loading_until_the_first_page_arrives(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);

    let directory = TestDirectory::new("document-loading");
    let (path, _) = directory.file("cities.csv", CITIES);

    let document = cx.update(|cx| cx.new(|cx| DelimitedDocument::open_local(path, cx)));

    let before = cx.update(|cx| document.read(cx).state());
    assert_eq!(before, DocumentState::Loading);
    assert!(cx.update(|cx| document.read(cx).table_state().is_none()));

    cx.run_until_parked();

    let after = cx.update(|cx| document.read(cx).state());
    assert_eq!(after, DocumentState::Clean);
}

#[gpui::test]
fn a_headerless_tsv_gets_positional_column_names(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-tsv");
    let (path, _) = directory.file("cities.tsv", b"1\tLima\n2\tQuito\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);
    assert_eq!(row_count(&document, window), 2);
    assert_eq!(status_items(&document, window)[0], "Delimiter: Tab");
}

#[gpui::test]
fn a_file_longer_than_one_page_shows_no_total(cx: &mut TestAppContext) {
    let record_total = PAGE_SIZE.get() * 2 + 7;

    let mut bytes = b"id,city\n".to_vec();
    for record in 0..record_total {
        bytes.extend_from_slice(format!("{record},Lima\n").as_bytes());
    }

    let directory = TestDirectory::new("document-partial");
    let (path, _) = directory.file("many.csv", &bytes);

    let (document, window) = open_local(cx, path);

    assert_eq!(row_count(&document, window), PAGE_SIZE.get());
    assert!(matches!(
        window.update(|_, cx| document.read(cx).record_count()),
        Some(RecordCount::IndexedSoFar(_))
    ));
    assert_eq!(
        status_items(&document, window)[2],
        format!("{} records loaded, more in the file", PAGE_SIZE.get())
    );
}

#[gpui::test]
fn a_missing_file_ends_in_the_error_state_and_reports_one_error(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-missing");
    let (path, _) = directory.file("present.csv", CITIES);
    let missing = path.with_file_name("absent.csv");

    let (document, window) = open_local(cx, missing);

    assert_eq!(state(&document, window), DocumentState::Error);
    assert!(window.update(|_, cx| document.read(cx).table_state().is_none()));
    assert!(window.update(|_, cx| document.read(cx).failure().is_some()));
    assert_eq!(toast_count(window), 1);

    let title = window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_title());
    assert_eq!(title.as_deref(), Some("Could not open absent.csv"));
}

/// Pipe-delimited Shift_JIS is a dialect the reader refuses: a pipe byte can
/// be the second byte of a Shift_JIS character.
///
/// The dialect comes from detection, so this test holds only while the
/// statistical encoding detector reads this text as Shift_JIS.
/// `a_refused_dialect_is_reported_as_a_user_error` covers the refusal with a
/// dialect it states itself.
#[gpui::test]
fn a_dialect_the_reader_refuses_ends_in_the_error_state(cx: &mut TestAppContext) {
    let shift_jis = Encoding::for_label(b"shift_jis").expect("a known encoding label");

    let text = "名前|都市|備考\n".to_string()
        + &"太郎さんは東京に住んでいます|東京都|これは日本語のテキストです\n".repeat(20);
    let (bytes, _, unmappable) = shift_jis.encode(&text);
    assert!(!unmappable);

    let directory = TestDirectory::new("document-refused");
    let (path, _) = directory.file("names.txt", &bytes);

    let (document, window) = open_local(cx, path);

    assert_eq!(state(&document, window), DocumentState::Error);
    assert_eq!(toast_count(window), 1);

    let failure = window.update(|_, cx| document.read(cx).failure().map(str::to_string));
    assert!(
        failure
            .as_deref()
            .is_some_and(|failure| failure.contains("Shift_JIS")),
        "{failure:?}"
    );
}

/// The byte-order mark settles UTF-8, so the stray byte is malformed text
/// rather than a hint of another encoding.
#[gpui::test]
fn malformed_bytes_open_with_a_warning_and_no_error(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-malformed");
    let (path, _) = directory.file("cities.csv", b"\xEF\xBB\xBFname,city\nAn\xFFa,Lima\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(row_count(&document, window), 1);
    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::MalformedText]
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).warning_items().to_vec()),
        [
            "Some bytes are not valid UTF-8 text and are shown as \u{FFFD}. The detected encoding is probably wrong."
        ]
    );
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn an_object_without_an_identity_warns_that_it_cannot_be_saved(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    connection.store.omit_identity();

    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) = open(cx, move |cx| {
        DelimitedDocument::open_object(
            app_state,
            profile_id,
            connection,
            BUCKET.to_string(),
            KEY.to_string(),
            cx,
        )
    });

    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::CannotSaveInPlace]
    );

    window.update(|_, cx| {
        let document = document.read(cx);

        assert_eq!(document.title(), "cities.csv");
        assert_eq!(document.connection_id(), Some(profile_id));
    });
}

#[gpui::test]
fn an_object_with_an_identity_opens_without_warnings(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) = open(cx, move |cx| {
        DelimitedDocument::open_object(
            app_state,
            profile_id,
            connection,
            BUCKET.to_string(),
            KEY.to_string(),
            cx,
        )
    });

    assert_eq!(row_count(&document, window), 2);
    assert!(warnings(&document, window).is_empty());
}

#[gpui::test]
fn the_pane_reports_the_file_name_and_no_unsaved_changes(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-pane");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    window.update(|window, cx| {
        let pane = DelimitedDocument::into_pane(document.clone(), cx);
        let meta = pane.meta_snapshot(cx);

        assert_eq!(pane.tab_title(cx), "cities.csv");
        assert_eq!(pane.kind(), DocumentKind::Delimited);
        assert_eq!(meta.title, "cities.csv");
        assert_eq!(meta.state, DocumentState::Clean);
        assert_eq!(meta.connection_id, None);
        assert!(pane.can_close(cx));
        assert_eq!(pane.change_summary(cx), None);
        assert!(
            !pane.save_for_close(window, cx),
            "there is nothing a save could write"
        );
    });
}

#[gpui::test]
fn the_pane_matches_only_the_key_of_its_own_file(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-dedup");
    let (path, _) = directory.file("cities.csv", CITIES);
    let other_path = path.with_file_name("other.csv");

    let connection = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (local, window) = open_local(cx, path.clone());

    let object = window.update(|_, cx| {
        cx.new(|cx| {
            DelimitedDocument::open_object(
                app_state,
                profile_id,
                connection,
                BUCKET.to_string(),
                KEY.to_string(),
                cx,
            )
        })
    });
    window.run_until_parked();

    let object_key = |profile_id, key: &str| {
        DocumentKey::Delimited(DelimitedFileKey::Object {
            profile_id,
            bucket: BUCKET.to_string(),
            key: key.to_string(),
        })
    };

    window.update(|_, cx| {
        let local_pane = DelimitedDocument::into_pane(local.clone(), cx);
        let object_pane = DelimitedDocument::into_pane(object.clone(), cx);

        let same_path = DocumentKey::Delimited(DelimitedFileKey::Local { path: path.clone() });
        let different_path = DocumentKey::Delimited(DelimitedFileKey::Local { path: other_path });

        assert!(local_pane.matches_dedup_key(&same_path, cx));
        assert!(!local_pane.matches_dedup_key(&different_path, cx));
        assert!(!local_pane.matches_dedup_key(&DocumentKey::File { path: path.clone() }, cx));
        assert!(!local_pane.matches_dedup_key(&object_key(profile_id, KEY), cx));

        assert!(object_pane.matches_dedup_key(&object_key(profile_id, KEY), cx));
        assert!(!object_pane.matches_dedup_key(&object_key(profile_id, "2026/other.csv"), cx));
        assert!(!object_pane.matches_dedup_key(&object_key(uuid::Uuid::new_v4(), KEY), cx));
        assert!(!object_pane.matches_dedup_key(&same_path, cx));
    });
}

#[gpui::test]
fn the_loaded_table_is_read_only_and_holds_the_keyboard(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-focus");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    window.update(|window, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .read(cx);

        assert!(!table_state.is_editable());
        assert!(table_state.focus_handle().is_focused(window));
    });
}

#[gpui::test]
fn an_empty_file_opens_with_no_rows(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-empty");
    let (path, _) = directory.file("empty.csv", b"");

    let (document, window) = open_local(cx, path);

    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(row_count(&document, window), 0);
    assert_eq!(status_items(&document, window)[2], "0 records");
    assert_eq!(toast_count(window), 0);
}

/// One column gives the delimiter vote nothing to count, so the extension
/// decides, in any letter case.
#[gpui::test]
fn an_upper_case_extension_still_hints_at_the_delimiter(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-upper-case");
    let (path, _) = directory.file("CITIES.TSV", b"city\nLima\nQuito\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(status_items(&document, window)[0], "Delimiter: Tab");
}

/// The object answers `head_object` and refuses every range read, so the
/// failure comes from the first read through the source.
#[test]
fn a_failed_object_range_read_keeps_the_driver_error() {
    let connection = FakeConnection::with_object(CITIES);
    connection
        .store
        .fail_reads_with("AccessDenied: no read permission");

    let location = DelimitedLocation::Object {
        connection,
        bucket: BUCKET.to_string(),
        key: KEY.to_string(),
    };

    let Err(error) = open_first_page(&location, Some("csv"), READER_OPTIONS) else {
        panic!("a refused range read cannot open the file");
    };
    let reported = open_error_to_user_facing(&error, "Could not open cities.csv".to_string());

    assert_eq!(reported.kind, ErrorKind::Driver);
    assert_eq!(reported.summary, "Could not open cities.csv");
    assert!(
        reported
            .cause
            .as_deref()
            .is_some_and(|cause| cause.contains("AccessDenied: no read permission")),
        "{:?}",
        reported.cause
    );
}

#[test]
fn a_refused_dialect_is_reported_as_a_user_error() {
    let directory = TestDirectory::new("document-refused-dialect");
    let (_, location) = directory.file("cities.txt", b"name|city\nAna|Lima\n");

    let dialect = Dialect {
        delimiter: b'|',
        quote: Some(b'"'),
        has_header: true,
        encoding: Encoding::for_label(b"shift_jis").expect("a known encoding label"),
    };

    let (source, version) = open_source(&location).expect("the test file opens");

    let Err(error) = read_first_page(source, version, dialect, READER_OPTIONS) else {
        panic!("the reader refuses a pipe delimiter in Shift_JIS");
    };
    let reported = open_error_to_user_facing(&error, "Could not open cities.txt".to_string());

    assert_eq!(reported.kind, ErrorKind::User);
    assert!(
        reported
            .cause
            .as_deref()
            .is_some_and(|cause| cause.contains("Shift_JIS")),
        "{:?}",
        reported.cause
    );
}

#[test]
fn a_local_file_that_cannot_be_read_is_reported_as_a_storage_error() {
    let directory = TestDirectory::new("document-storage-kind");
    let (path, _) = directory.file("present.csv", CITIES);

    let location = DelimitedLocation::Local {
        path: path.with_file_name("absent.csv"),
    };

    let Err(error) = open_first_page(&location, Some("csv"), READER_OPTIONS) else {
        panic!("a missing file cannot be opened");
    };
    let reported = open_error_to_user_facing(&error, "Could not open absent.csv".to_string());

    assert_eq!(reported.kind, ErrorKind::Storage);
}

#[test]
fn a_sample_that_reaches_the_end_of_the_file_is_the_whole_file() {
    let sample_bytes = usize::try_from(SAMPLE_BYTES).expect("the sample size fits in memory");

    assert_eq!(sample_coverage(0, 0), SampleCoverage::WholeFile);
    assert_eq!(
        sample_coverage(sample_bytes - 1, SAMPLE_BYTES - 1),
        SampleCoverage::WholeFile
    );
    assert_eq!(
        sample_coverage(sample_bytes, SAMPLE_BYTES),
        SampleCoverage::WholeFile
    );
    assert_eq!(
        sample_coverage(sample_bytes, SAMPLE_BYTES + 1),
        SampleCoverage::Prefix
    );
}

/// A file of exactly the sample size ends without a line break. Its last
/// record is counted because the sample is the whole file, which makes tab
/// the delimiter more records agree on. One byte longer, the sample is a
/// prefix, that record is dropped as cut off, and comma and tab tie, which
/// goes to comma.
#[test]
fn a_file_of_exactly_the_sample_size_is_detected_as_a_whole_file() {
    let sample_bytes = usize::try_from(SAMPLE_BYTES).expect("the sample size fits in memory");

    let mut bytes = b"a,b\tc\na,b\tc\na,b\na\tb\n".to_vec();
    let padding = sample_bytes - bytes.len() - "e\t".len();
    bytes.extend_from_slice(b"e\t");
    bytes.extend(std::iter::repeat_n(b'x', padding));
    assert_eq!(bytes.len(), sample_bytes);

    let directory = TestDirectory::new("document-exact-sample");
    let (_, location) = directory.file("exact.txt", &bytes);

    let Ok(opened) = open_first_page(&location, None, READER_OPTIONS) else {
        panic!("the file opens");
    };
    assert_eq!(opened.dialect.delimiter, b'\t');

    bytes.push(b'x');
    let (_, longer) = directory.file("longer.txt", &bytes);

    let Ok(opened) = open_first_page(&longer, None, READER_OPTIONS) else {
        panic!("the file opens");
    };
    assert_eq!(opened.dialect.delimiter, b',');
}

#[test]
fn the_extension_hint_is_the_text_after_the_last_dot() {
    assert_eq!(extension_hint("CITIES.CSV"), Some("CSV"));
    assert_eq!(extension_hint("archive.tar.csv"), Some("csv"));
    assert_eq!(extension_hint("cities"), None);
    assert_eq!(extension_hint(".csv"), None);
}

/// A header click has no action here, so the registry must not name a
/// command for it.
#[test]
fn the_registry_lists_the_column_headers_as_mouse_only() {
    let header = DELIMITED
        .entries
        .iter()
        .find(|(pattern, _)| *pattern == "header-col-*")
        .map(|(_, path)| *path);

    assert!(
        matches!(header, Some(KeyboardPath::MouseOnly(reason)) if !reason.starts_with("gap:")),
        "{header:?}"
    );
}

/// The keyboard is on the loading notice when the first page arrives, and
/// the table takes it over.
#[gpui::test]
fn the_table_takes_the_keyboard_from_the_loading_notice(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);

    let directory = TestDirectory::new("document-focus-handover");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            let document = cx.new(|cx| DelimitedDocument::open_local(path, cx));

            document.update(cx, |document, cx| {
                assert_eq!(document.state(), DocumentState::Loading);
                document.focus(window, cx);
            });

            document
        },
        |document, _cx| document.active_context(),
        |document, command, window, cx| document.dispatch_command(command, window, cx),
    );
    window.run_until_parked();

    window.update(|window, cx| {
        let document = host.read(cx).document.read(cx);
        let table_state = document
            .table_state()
            .expect("a loaded document has a table")
            .read(cx);

        assert!(table_state.focus_handle().is_focused(window));
    });
}

/// The rows stay in file order: a header click leaves no sort indicator.
#[gpui::test]
fn a_header_sort_request_leaves_the_table_unsorted(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-sort");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let table_state = window.update(|_, cx| {
        document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .clone()
    });

    window.update(|_, cx| table_state.update(cx, |state, cx| state.cycle_sort(0, cx)));
    window.run_until_parked();

    assert!(window.update(|_, cx| table_state.read(cx).sort().is_none()));
    assert_eq!(row_count(&document, window), 2);
}

#[gpui::test]
fn the_delimited_document_is_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-coverage");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (_document, window) = open_local(cx, path);

    let capture = FrameCapture::observe(window);
    let checked = Coverage::new(DELIMITED).assert_covered(&capture.frame(window));

    assert!(
        checked.iter().any(|id| id.starts_with("cell-")),
        "{checked:?}"
    );
    assert!(
        checked.iter().any(|id| id.starts_with("header-col-")),
        "{checked:?}"
    );
}

#[gpui::test]
fn a_file_of_several_pages_loads_page_by_page(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-pages");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    assert_eq!(first_column(&document, window), ["0", "1"]);
    assert_eq!(
        status_items(&document, window)[2],
        "2 records loaded, more in the file"
    );
    assert!(matches!(
        window.update(|_, cx| document.read(cx).record_count()),
        Some(RecordCount::IndexedSoFar(_))
    ));
    assert!(has_more_records(&document, window));

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3"]);
    assert_eq!(
        status_items(&document, window)[2],
        "4 records loaded, more in the file"
    );
    assert!(has_more_records(&document, window));

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3", "4"]);
    assert_eq!(status_items(&document, window)[2], "5 records");
    assert_eq!(
        window.update(|_, cx| document.read(cx).record_count()),
        Some(RecordCount::Total(5))
    );
    assert!(!has_more_records(&document, window));
    assert_eq!(toast_count(window), 0);

    load_more(&document, window);

    assert_eq!(row_count(&document, window), 5);
    assert_eq!(toast_count(window), 0);
}

/// The last page is full, and the total is still settled by the read that
/// returns it: no further, empty, page has to be asked for.
#[gpui::test]
fn a_file_of_an_exact_number_of_pages_settles_its_total_with_the_last_page(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("document-exact-pages");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(4));

    let (document, window) = open_local_in_small_pages(cx, path);

    assert!(has_more_records(&document, window));

    load_more(&document, window);

    assert_eq!(row_count(&document, window), 4);
    assert_eq!(status_items(&document, window)[2], "4 records");
    assert_eq!(
        window.update(|_, cx| document.read(cx).record_count()),
        Some(RecordCount::Total(4))
    );
    assert!(!has_more_records(&document, window));
    assert!(
        !covered_ids(window)
            .iter()
            .any(|id| id == "delimited-load-more")
    );
}

#[gpui::test]
fn a_file_of_exactly_one_page_offers_no_load_more(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-one-page");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(2));

    let (document, window) = open_local_in_small_pages(cx, path);

    assert_eq!(status_items(&document, window)[2], "2 records");
    assert!(!has_more_records(&document, window));
}

#[gpui::test]
fn a_file_shorter_than_a_page_offers_no_load_more(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-short");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    assert!(!has_more_records(&document, window));
    assert!(
        !covered_ids(window)
            .iter()
            .any(|id| id == "delimited-load-more")
    );
}

#[gpui::test]
fn a_second_request_during_a_load_is_ignored(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-one-load");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(7));

    let (document, window) = open_local_in_small_pages(cx, path);

    assert!(!is_loading_more(&document, window));

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.load_more(cx);
            assert!(document.is_loading_more());

            document.load_more(cx);
        });
    });
    window.run_until_parked();

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3"]);
    assert!(!is_loading_more(&document, window));
    assert!(has_more_records(&document, window));
}

#[gpui::test]
fn a_failed_page_read_reports_one_error_keeps_the_rows_and_can_be_retried(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(&numbered_csv(5));
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    assert_eq!(row_count(&document, window), 2);

    connection
        .store
        .fail_reads_with("SlowDown: reduce the rate");
    load_more(&document, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not load more of cities.csv")
    );
    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(first_column(&document, window), ["0", "1"]);
    assert_eq!(
        status_items(&document, window)[2],
        "2 records loaded, more in the file"
    );
    assert!(has_more_records(&document, window));
    assert!(!is_loading_more(&document, window));

    connection.store.stop_failing_reads();
    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3"]);
    assert_eq!(toast_count(window), 1);
}

#[gpui::test]
fn an_object_whose_profile_is_disconnected_reports_one_error_and_reads_nothing(
    cx: &mut TestAppContext,
) {
    let connection = FakeConnection::with_object(&numbered_csv(5));
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state.clone(), profile_id, connection.clone());

    window.update(|_, cx| {
        app_state.update(cx, |state, _| {
            state.connections_mut().remove(&profile_id);
        });
    });
    let reads_before = connection.store.range_reads();

    load_more(&document, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not load more of cities.csv")
    );
    assert_eq!(connection.store.range_reads(), reads_before);
    assert_eq!(first_column(&document, window), ["0", "1"]);
    assert!(has_more_records(&document, window));
    assert!(!is_loading_more(&document, window));
}

/// The connection the document opened with is dead after a reconnect, so
/// the next page is read through the profile's new one.
#[gpui::test]
fn a_reconnected_profile_reads_the_next_page_through_its_new_connection(cx: &mut TestAppContext) {
    let bytes = numbered_csv(5);

    let first = FakeConnection::with_object(&bytes);
    let (app_state, profile_id) = connect_profile(cx, first.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state.clone(), profile_id, first.clone());

    let second = FakeConnection::with_object(&bytes);
    replace_connection(window, &app_state, profile_id, second.clone());
    first.store.fail_reads_with("the connection is closed");

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3"]);
    assert_eq!(toast_count(window), 0);
    assert!(second.store.range_reads() > 0);
}

/// The read fails after the document is gone, and nobody is told: the
/// failure belongs to a tab that no longer exists.
#[gpui::test]
fn a_document_closed_during_a_load_reports_nothing(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);

    let connection = FakeConnection::with_object(&numbered_csv(5));
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let document = cx.update(|cx| {
        cx.new(|cx| {
            DelimitedDocument::open_object_with(
                app_state,
                profile_id,
                connection.clone(),
                BUCKET.to_string(),
                KEY.to_string(),
                SMALL_PAGES,
                cx,
            )
        })
    });
    cx.run_until_parked();

    connection
        .store
        .fail_reads_with("SlowDown: reduce the rate");

    cx.update(|cx| {
        document.update(cx, |document, cx| {
            document.load_more(cx);
            assert!(document.is_loading_more());
        });
    });

    drop(document);
    cx.update(|_| {});
    cx.run_until_parked();

    let toasts = cx.update(|cx| cx.global::<ToastGlobal>().host.read(cx).toast_count());
    assert_eq!(toasts, 0);
}

/// The byte-order mark settles UTF-8. The first page is clean, and the
/// stray bytes of the pages after it add the warning once.
#[gpui::test]
fn malformed_bytes_in_a_later_page_add_the_warning_once(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-malformed-later");
    let (path, _) = directory.file(
        "cities.csv",
        b"\xEF\xBB\xBFid,city\n0,Lima\n1,Quito\n2,Li\xFFma\n3,Cusco\n4,Qui\xFFto\n",
    );

    let (document, window) = open_local_in_small_pages(cx, path);

    assert!(warnings(&document, window).is_empty());

    load_more(&document, window);

    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::MalformedText]
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).warning_items().len()),
        1
    );

    load_more(&document, window);

    assert_eq!(row_count(&document, window), 5);
    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::MalformedText]
    );
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn the_selected_cell_survives_a_page_load(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-selection");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    let table_state = window.update(|_, cx| {
        document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .clone()
    });
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| state.select_cell(CellCoord::new(1, 1), cx));
    });

    load_more(&document, window);

    window.update(|window, cx| {
        let state = table_state.read(cx);

        assert_eq!(state.model().rows.len(), 4);
        assert_eq!(state.selection().active, Some(CellCoord::new(1, 1)));
        assert!(state.focus_handle().is_focused(window));
    });
}

/// `]` is the key of the next page in the results context, and the document
/// answers it by loading one.
#[gpui::test]
fn the_next_page_key_loads_the_next_page(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-next-page-key");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    window.simulate_keystrokes("]");
    window.run_until_parked();

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3"]);
}

#[gpui::test]
fn the_next_page_command_is_handled_only_by_a_loaded_document(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-next-page-command");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));
    let missing = path.with_file_name("absent.csv");

    let (document, window) = open_local_in_small_pages(cx, path);

    let handled = window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::ResultsNextPage, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    assert_eq!(row_count(&document, window), 4);

    let other = window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::ResultsPrevPage, window, cx)
        })
    });
    assert!(!other);

    let failed = window.update(|_, cx| cx.new(|cx| DelimitedDocument::open_local(missing, cx)));
    window.run_until_parked();

    let handled = window.update(|window, cx| {
        failed.update(cx, |document, cx| {
            document.dispatch_command(Command::ResultsNextPage, window, cx)
        })
    });
    assert!(!handled);
}

#[gpui::test]
fn the_load_more_control_is_covered_while_the_file_has_more(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-load-more-coverage");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (_document, window) = open_local_in_small_pages(cx, path);

    let checked = covered_ids(window);

    assert!(
        checked.iter().any(|id| id == "delimited-load-more"),
        "{checked:?}"
    );
}

#[test]
fn the_registry_reaches_load_more_through_the_next_page_command() {
    let load_more = DELIMITED
        .entries
        .iter()
        .find(|(pattern, _)| *pattern == "delimited-load-more")
        .map(|(_, path)| *path);

    assert!(
        matches!(
            load_more,
            Some(KeyboardPath::Command(Command::ResultsNextPage))
        ),
        "{load_more:?}"
    );
}

#[test]
fn a_page_shorter_than_the_page_size_settles_the_total() {
    let page_size = NonZeroUsize::new(2).expect("a page size above zero");

    let page_of = |first_record: u64, record_total: u64| Page {
        first_record,
        records: (0..record_total)
            .map(|record| Record {
                byte_range: record * 7..(record + 1) * 7,
                fields: vec![record.to_string(), "Lima".to_string()],
                had_replacements: false,
            })
            .collect(),
    };

    let full = page_of(4, 2);
    let short = page_of(4, 1);
    let empty = page_of(4, 0);

    assert_eq!(
        settled_record_count(&full, RecordCount::IndexedSoFar(6), page_size),
        RecordCount::IndexedSoFar(6)
    );
    assert_eq!(
        settled_record_count(&short, RecordCount::IndexedSoFar(5), page_size),
        RecordCount::Total(5)
    );
    assert_eq!(
        settled_record_count(&empty, RecordCount::IndexedSoFar(4), page_size),
        RecordCount::Total(4)
    );
    assert_eq!(
        settled_record_count(&short, RecordCount::Total(9), page_size),
        RecordCount::Total(9)
    );
}

/// Every string of the document resolves in English and is translated, not
/// copied, in the other catalogs.
#[test]
fn the_document_strings_resolve_in_every_locale() {
    for key in [
        "document.delimited.loading",
        "document.delimited.error.open_failed",
        "document.delimited.warning.malformed_text",
        "document.delimited.warning.cannot_save_in_place",
        "document.delimited.status.delimiter",
        "document.delimited.status.encoding",
        "document.delimited.status.records.all.one",
        "document.delimited.status.records.all.many",
        "document.delimited.status.records.of_total",
        "document.delimited.status.records.partial",
        "document.delimited.delimiter.comma",
        "document.delimited.delimiter.tab",
        "document.delimited.delimiter.semicolon",
        "document.delimited.delimiter.pipe",
        "document.delimited.footer.load_more",
        "document.delimited.footer.loading_more",
        "document.delimited.error.load_more_failed",
    ] {
        let english = dbflux_i18n::t!(key, locale = "en");

        assert!(!english.is_empty());
        assert_ne!(english, format!("en.{key}"));

        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let translated = dbflux_i18n::t!(key, locale = locale);

            assert_ne!(
                translated,
                format!("{locale}.{key}"),
                "{locale} lacks {key}"
            );
            assert_ne!(translated, english, "{locale} copies English for {key}");
        }
    }
}
