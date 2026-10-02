use std::path::PathBuf;

use dbflux_delimited::{Dialect, Encoding, RecordCount, SampleCoverage};
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

use dbflux_ui_base::keyboard_coverage::KeyboardPath;
use dbflux_ui_base::user_error::ErrorKind;

use super::document::{
    DelimitedDocument, DelimitedWarning, PAGE_SIZE, SAMPLE_BYTES, extension_hint,
    open_error_to_user_facing, open_first_page, read_first_page, sample_coverage,
};
use super::source::{DelimitedLocation, open_source};
use super::tests::{BUCKET, FakeConnection, KEY, TestDirectory};
use crate::dedup::{DelimitedFileKey, DocumentKey};
use crate::keyboard_coverage::DELIMITED;
use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
use crate::types::{DocumentKind, DocumentState};

const CITIES: &[u8] = b"name,city\nAna,Lima\nBo,Quito\n";

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

    let profile_id = uuid::Uuid::new_v4();

    let (document, window) = open(cx, move |cx| {
        DelimitedDocument::open_object(
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

    let (document, window) = open(cx, move |cx| {
        DelimitedDocument::open_object(
            uuid::Uuid::new_v4(),
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
    let profile_id = uuid::Uuid::new_v4();

    let (local, window) = open_local(cx, path.clone());

    let connection = FakeConnection::with_object(CITIES);
    let object = window.update(|_, cx| {
        cx.new(|cx| {
            DelimitedDocument::open_object(
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

    let Err(error) = open_first_page(&location, Some("csv")) else {
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

    let Err(error) = read_first_page(source, version, dialect) else {
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

    let Err(error) = open_first_page(&location, Some("csv")) else {
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

    let Ok(opened) = open_first_page(&location, None) else {
        panic!("the file opens");
    };
    assert_eq!(opened.dialect.delimiter, b'\t');

    bytes.push(b'x');
    let (_, longer) = directory.file("longer.txt", &bytes);

    let Ok(opened) = open_first_page(&longer, None) else {
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
