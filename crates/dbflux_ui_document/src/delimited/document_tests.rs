use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;

use dbflux_app::keymap::Command;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_delimited::{
    Dialect, DialectOverrides, Encoding, Page, PagedReader, ReadError, ReaderOptions, Record,
    RecordCount, SampleCoverage,
};
use dbflux_test_support::fake_driver::FakeDriver;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

use dbflux_ui_base::keyboard_coverage::KeyboardPath;
use dbflux_ui_base::user_error::ErrorKind;

use super::document::{
    DelimitedDocument, DelimitedWarning, OpenError, PAGE_SIZE, READER_OPTIONS, SAMPLE_BYTES,
    extension_hint, open_error_to_user_facing, open_first_page, read_first_page, readable_dialect,
    refused_dialect_error, sample_coverage, settled_record_count,
};
use super::source::{DelimitedLocation, open_source};
use super::tests::{BUCKET, FakeConnection, KEY, TestDirectory};
use super::toolbar::encoding_choices;
use crate::dedup::{DelimitedFileKey, DocumentKey};
use crate::keyboard_coverage::DELIMITED;
use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
use crate::pane::PaneActionRun;
use crate::types::{DocumentKind, DocumentState};

pub(super) const CITIES: &[u8] = b"name,city\nAna,Lima\nBo,Quito\n";

/// Two records to a page, and a fetch window shorter than a page, so every
/// further page is read from the source and not from bytes already fetched.
pub(super) const SMALL_PAGES: ReaderOptions = ReaderOptions {
    page_size: NonZeroUsize::new(2).unwrap(),
    window_size: NonZeroU64::new(16).unwrap(),
};

/// A CSV file with a header and `record_total` records whose first field is
/// the record's index.
pub(super) fn numbered_csv(record_total: usize) -> Vec<u8> {
    let mut bytes = b"id,city\n".to_vec();

    for record in 0..record_total {
        bytes.extend_from_slice(format!("{record},Lima\n").as_bytes());
    }

    bytes
}

/// Hosts the document `build` returns in a window, gives it the keyboard and
/// runs the open flow to its end.
pub(super) fn open(
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

pub(super) fn open_local(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    open(cx, move |cx| DelimitedDocument::open_local(path, cx))
}

pub(super) fn open_local_in_small_pages(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<DelimitedDocument>, &mut VisualTestContext) {
    open(cx, move |cx| {
        DelimitedDocument::open_local_with(path, SMALL_PAGES, cx)
    })
}

pub(super) fn open_object_in_small_pages(
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
pub(super) fn connect_profile(
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

pub(super) fn load_more(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.load_more(cx)));
    window.run_until_parked();
}

pub(super) fn has_more_records(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> bool {
    window.update(|_, cx| document.read(cx).has_more_records())
}

fn is_loading_more(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| document.read(cx).is_loading_more())
}

/// The text of the first cell of every row.
pub(super) fn first_column(
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

pub(super) fn last_toast_title(window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_title())
}

/// The ids of the clickable elements the coverage check found in the frame.
pub(super) fn covered_ids(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    let menu = pane_action_ids(document, window);
    let capture = FrameCapture::observe(window);

    Coverage::new(DELIMITED)
        .with_menu_entries(menu)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect()
}

/// The ids of the entries of the pane actions menu, as the workspace lists
/// them.
pub(super) fn pane_action_ids(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    window.update(|_, cx| {
        document
            .read(cx)
            .pane_actions(document)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    })
}

pub(super) fn column_titles(
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

pub(super) fn row_count(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> usize {
    window.update(|_, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table");

        table_state.read(cx).model().rows.len()
    })
}

pub(super) fn status_items(
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

pub(super) fn warnings(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<DelimitedWarning> {
    window.update(|_, cx| document.read(cx).warnings().to_vec())
}

pub(super) fn state(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> DocumentState {
    window.update(|_, cx| document.read(cx).state())
}

pub(super) fn toast_count(window: &mut VisualTestContext) -> usize {
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

/// Pipe-delimited Shift_JIS text, which detection reads as such and the
/// reader refuses: a pipe byte can be the second byte of a Shift_JIS
/// character.
fn shift_jis_with_pipes() -> Vec<u8> {
    let shift_jis = Encoding::for_label(b"shift_jis").expect("a known encoding label");

    let text = "名前|都市|備考\n".to_string()
        + &"太郎さんは東京に住んでいます|東京都|これは日本語のテキストです\n".repeat(20);
    let (bytes, _, unmappable) = shift_jis.encode(&text);
    assert!(!unmappable);

    bytes.into_owned()
}

/// The detected dialect is refused, so the file opens with the first
/// delimiter the reader accepts as an override, and says so above the table
/// instead of failing.
#[gpui::test]
fn a_refused_detected_dialect_opens_under_a_delimiter_the_reader_accepts(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-refused");
    let (path, _) = directory.file("names.txt", &shift_jis_with_pipes());

    let (document, window) = open_local(cx, path);

    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(toast_count(window), 0);

    let (detected, in_effect, overrides) = window.update(|_, cx| {
        let document = document.read(cx);

        (
            document.detected_dialect().expect("a loaded document"),
            document.dialect().expect("a loaded document"),
            document.dialect_overrides(),
        )
    });

    assert_eq!(detected.delimiter, b'|');
    assert_eq!(detected.encoding.name(), "Shift_JIS");
    assert_eq!(
        in_effect,
        Dialect {
            delimiter: b',',
            ..detected
        }
    );
    assert_eq!(
        overrides,
        DialectOverrides {
            delimiter: Some(b','),
            ..DialectOverrides::default()
        }
    );
    assert!(!window.update(|_, cx| document.read(cx).is_rereading()));

    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::DetectedDelimiterUnreadable]
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).warning_items().to_vec()),
        [
            "The detected delimiter Pipe cannot be read in Shift_JIS text, so the file is read with Comma and its columns are probably wrong. Choose another encoding or delimiter."
        ]
    );
    assert_eq!(status_items(&document, window)[0], "Delimiter: Comma");
}

/// Reset asks for the detected dialect, which is the refused one: it is
/// reported like any refused override and the file stays as it is.
#[gpui::test]
fn reset_on_a_refused_detected_dialect_reports_the_refusal(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-refused-reset");
    let (path, _) = directory.file("names.txt", &shift_jis_with_pipes());

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| document.update(cx, |document, cx| document.reset_dialect(cx)));
    window.run_until_parked();

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read names.txt")
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).dialect().map(|dialect| dialect.delimiter)),
        Some(b',')
    );
    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::DetectedDelimiterUnreadable]
    );
}

/// In another encoding the detected delimiter is readable, so choosing one
/// puts the pipe back and the warning goes.
#[gpui::test]
fn the_unreadable_delimiter_warning_goes_once_the_file_is_read_another_way(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("document-refused-recovered");
    let (path, _) = directory.file("names.txt", &shift_jis_with_pipes());

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.set_dialect_overrides(
                DialectOverrides {
                    encoding: Some(Encoding::for_label(b"windows-1252").expect("a known label")),
                    ..DialectOverrides::default()
                },
                cx,
            );
        });
    });
    window.run_until_parked();

    assert_eq!(toast_count(window), 0);
    assert_eq!(
        window.update(|_, cx| document.read(cx).dialect().map(|dialect| dialect.delimiter)),
        Some(b'|')
    );
    assert_eq!(column_titles(&document, window).len(), 3);
    assert!(!warnings(&document, window).contains(&DelimitedWarning::DetectedDelimiterUnreadable));
}

/// Switching the header flag keeps the fallback delimiter, so the file is
/// still read with the comma and the warning stays.
#[gpui::test]
fn the_unreadable_delimiter_warning_stays_when_only_the_header_flag_changes(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("document-refused-header");
    let (path, _) = directory.file("names.txt", &shift_jis_with_pipes());

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| document.update(cx, |document, cx| document.toggle_header(cx)));
    window.run_until_parked();

    let (in_effect, detected) = window.update(|_, cx| {
        let document = document.read(cx);

        (
            document.dialect().expect("the file is loaded"),
            document.detected_dialect().expect("the file is loaded"),
        )
    });

    assert_eq!(toast_count(window), 0);
    assert_eq!(in_effect.delimiter, b',');
    assert_ne!(in_effect.has_header, detected.has_header);
    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::DetectedDelimiterUnreadable]
    );
}

/// Detection never reports ISO-2022-JP, in which the reader refuses every
/// delimiter. The dialect is stated here to reach the case with no
/// delimiter to fall back to, which keeps the refusal.
#[test]
fn a_dialect_with_no_readable_delimiter_keeps_its_refusal() {
    let unreadable = Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: true,
        encoding: Encoding::for_label(b"iso-2022-jp").expect("a known encoding label"),
    };

    assert!(matches!(
        readable_dialect(unreadable, READER_OPTIONS),
        Err(ReadError::UnsupportedDialect { .. })
    ));

    let pipes_in_shift_jis = Dialect {
        delimiter: b'|',
        encoding: Encoding::for_label(b"shift_jis").expect("a known encoding label"),
        ..unreadable
    };

    assert_eq!(
        readable_dialect(pipes_in_shift_jis, READER_OPTIONS).ok(),
        Some(Dialect {
            delimiter: b',',
            ..pipes_in_shift_jis
        })
    );

    let readable = Dialect {
        encoding: Encoding::for_label(b"utf-8").expect("a known encoding label"),
        ..unreadable
    };

    assert_eq!(
        readable_dialect(readable, READER_OPTIONS).ok(),
        Some(readable)
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
            pane.save_for_close(window, cx),
            "a file without edits closes without a write"
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

/// A local file is recorded in the workspace session by its path, so it is
/// reopened at startup. An object needs its profile's live connection and is
/// not recorded, as the object editor's tabs are not.
#[gpui::test]
fn the_session_records_a_local_file_by_its_path_and_no_object(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-session");
    let (path, _) = directory.file("cities.csv", CITIES);

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

    window.update(|_, cx| {
        let local_pane = DelimitedDocument::into_pane(local.clone(), cx);
        let object_pane = DelimitedDocument::into_pane(object.clone(), cx);

        let snapshot = local_pane
            .session_tab_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot(cx))
            .expect("a local file takes part in the session");

        assert_eq!(snapshot.kind, "Delimited");
        assert_eq!(snapshot.id, local.read(cx).id());
        assert_eq!(snapshot.title, "cities.csv");
        assert_eq!(snapshot.file_path.as_deref(), Some(path.as_path()));
        assert_eq!(snapshot.scratch_path, None);
        assert_eq!(snapshot.shadow_path, None);
        assert_eq!(snapshot.exec_ctx.connection_id, None);

        let object_snapshot = object_pane
            .session_tab_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot(cx));
        assert!(object_snapshot.is_none());
    });
}

#[gpui::test]
fn the_loaded_table_is_editable_by_position_and_holds_the_keyboard(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("document-focus");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    window.update(|window, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .read(cx);

        assert!(table_state.is_editable());
        assert!(table_state.is_positional_editing());
        assert!(table_state.is_insertable());
        assert!(table_state.pk_columns().is_empty());
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

#[test]
fn only_a_csv_or_tsv_extension_in_any_letter_case_opens_as_delimited() {
    use crate::delimited::is_delimited_path;
    use std::path::Path;

    assert!(is_delimited_path(Path::new("a.csv")));
    assert!(is_delimited_path(Path::new("B.TSV")));
    assert!(is_delimited_path(Path::new("/home/ana/Reports.Csv")));
    assert!(is_delimited_path(Path::new("2026/q1/cities.tsv")));

    assert!(!is_delimited_path(Path::new("notes.txt")));
    assert!(!is_delimited_path(Path::new("cities.csv.gz")));
    assert!(!is_delimited_path(Path::new("csv")));
    assert!(!is_delimited_path(Path::new(".csv")));
    assert!(!is_delimited_path(Path::new("reports/csv/")));
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

    let (document, window) = open_local(cx, path);

    let checked = covered_ids(&document, window);

    assert!(
        checked.iter().any(|id| id.starts_with("cell-")),
        "{checked:?}"
    );
    assert!(
        checked.iter().any(|id| id.starts_with("header-col-")),
        "{checked:?}"
    );

    for control in [
        "delimited-delimiter.dropdown-trigger",
        "delimited-quote.dropdown-trigger",
        "delimited-encoding.dropdown-trigger",
        "delimited-header",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }

    // The reset takes no click until there is an override to drop.
    assert!(
        !checked.iter().any(|id| id == "delimited-dialect-reset"),
        "{checked:?}"
    );

    window.update(|_, cx| {
        document.update(cx, |document, cx| document.override_has_header(false, cx));
    });
    window.run_until_parked();

    let checked = covered_ids(&document, window);

    assert!(
        checked.iter().any(|id| id == "delimited-dialect-reset"),
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
        !covered_ids(&document, window)
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
        !covered_ids(&document, window)
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

    let (document, window) = open_local_in_small_pages(cx, path);

    let checked = covered_ids(&document, window);

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
        "document.delimited.error.reread_failed",
        "document.delimited.error.refused.unsupported",
        "document.delimited.error.refused.line_break",
        "document.delimited.error.refused.quote_equals_delimiter",
        "document.delimited.quote.double",
        "document.delimited.quote.single",
        "document.delimited.quote.none",
        "document.delimited.toolbar.delimiter",
        "document.delimited.toolbar.quote",
        "document.delimited.toolbar.encoding",
        "document.delimited.toolbar.header",
        "document.delimited.toolbar.reset",
        "document.delimited.toolbar.detected",
        "document.delimited.warning.malformed_text_chosen",
        "document.delimited.warning.detected_delimiter_unreadable",
        "document.delimited.footer.rereading",
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

// -- The dialect toolbar ------------------------------------------------------

/// One record of each kind detection needs to settle on windows-1252: the
/// bytes above 0x7F are not valid UTF-8.
pub(super) const WINDOWS_1252_NAMES: &[u8] = b"name;city\nJos\xE9;M\xE1laga\nMar\xEDa;C\xF3rdoba\n";

pub(super) fn encoding(label: &str) -> &'static Encoding {
    Encoding::for_label(label.as_bytes()).expect("a known encoding label")
}

pub(super) fn set_overrides(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    overrides: DialectOverrides,
) {
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.set_dialect_overrides(overrides, cx)
        });
    });
    window.run_until_parked();
}

pub(super) fn dialect(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Dialect {
    window.update(|_, cx| {
        document
            .read(cx)
            .dialect()
            .expect("a loaded document has a dialect")
    })
}

fn overrides(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> DialectOverrides {
    window.update(|_, cx| document.read(cx).dialect_overrides())
}

/// What the delimiter, quote and encoding selects and the header checkbox
/// show, in that order.
fn control_labels(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> [String; 4] {
    window.update(|_, cx| {
        let document = document.read(cx);
        let controls = document
            .dialect_controls()
            .expect("a loaded document has dialect controls");

        let selected = |dropdown: &Entity<dbflux_components::controls::Dropdown>| {
            dropdown
                .read(cx)
                .selected_label()
                .map(|label| label.to_string())
                .unwrap_or_default()
        };

        [
            selected(&controls.delimiter),
            selected(&controls.quote),
            selected(&controls.encoding),
            document.header_label(),
        ]
    })
}

fn header_is_checked(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| {
        document
            .read(cx)
            .requested_dialect()
            .expect("a loaded document has a dialect")
            .has_header
    })
}

fn is_rereading(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| document.read(cx).is_rereading())
}

/// Runs the entry `id` of the pane actions menu, as choosing it does.
pub(super) fn run_pane_action(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    id: &str,
) {
    let action = window
        .update(|_, cx| document.read(cx).pane_actions(document))
        .into_iter()
        .find(|action| action.id.as_ref() == id)
        .unwrap_or_else(|| panic!("the pane actions list {id}"));

    assert!(action.enabled, "{id} is enabled");

    let PaneActionRun::Callback(run) = action.run else {
        panic!("{id} runs a document callback");
    };

    window.update(|window, cx| run(window, cx));
    window.run_until_parked();
}

#[gpui::test]
fn every_control_shows_the_detected_value_after_opening(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-detected");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    assert_eq!(
        control_labels(&document, window),
        [
            "Comma (detected)",
            "Double quote (detected)",
            "UTF-8 (detected)",
            "Header row (detected)"
        ]
    );
    assert!(header_is_checked(&document, window));
    assert_eq!(overrides(&document, window), DialectOverrides::default());
    assert_eq!(
        window.update(|_, cx| document.read(cx).detected_dialect()),
        Some(dialect(&document, window))
    );
}

#[gpui::test]
fn overriding_the_delimiter_reads_the_file_again_with_other_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-delimiter");
    let (path, _) = directory.file("cities.csv", b"name,city;zone\nAna,Lima;south\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(column_titles(&document, window), ["name", "city;zone"]);

    set_overrides(
        &document,
        window,
        DialectOverrides {
            delimiter: Some(b';'),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(column_titles(&document, window), ["name,city", "zone"]);
    assert_eq!(first_column(&document, window), ["Ana,Lima"]);
    assert_eq!(dialect(&document, window).delimiter, b';');
    assert_eq!(status_items(&document, window)[0], "Delimiter: Semicolon");
    assert_eq!(control_labels(&document, window)[0], "Semicolon");
    assert!(!is_rereading(&document, window));
    assert_eq!(toast_count(window), 0);

    let detected = window.update(|_, cx| document.read(cx).detected_dialect());
    assert_eq!(detected.map(|dialect| dialect.delimiter), Some(b','));
}

#[gpui::test]
fn overriding_the_header_moves_the_first_record_out_of_the_names_and_back(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-header-off");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);
    assert_eq!(first_column(&document, window), ["name", "Ana", "Bo"]);
    assert_eq!(status_items(&document, window)[2], "3 records");
    assert!(!header_is_checked(&document, window));
    assert_eq!(control_labels(&document, window)[3], "Header row");

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(true),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert_eq!(first_column(&document, window), ["Ana", "Bo"]);
    assert_eq!(status_items(&document, window)[2], "2 records");
    assert_eq!(
        overrides(&document, window),
        DialectOverrides::default(),
        "the detected value is not an override"
    );
}

#[gpui::test]
fn overriding_the_header_moves_the_first_record_into_the_names(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-header-on");
    let (path, _) = directory.file("cities.csv", b"1,Lima\n2,Quito\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);
    assert_eq!(first_column(&document, window), ["1", "2"]);
    assert_eq!(
        control_labels(&document, window)[3],
        "Header row (detected)"
    );
    assert!(!header_is_checked(&document, window));

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(true),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(column_titles(&document, window), ["1", "Lima"]);
    assert_eq!(first_column(&document, window), ["2"]);
    assert!(header_is_checked(&document, window));
}

#[gpui::test]
fn overriding_the_encoding_decodes_the_text_again(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-encoding");
    let (path, _) = directory.file("names.csv", WINDOWS_1252_NAMES);

    let (document, window) = open_local(cx, path);

    assert_eq!(
        dialect(&document, window).encoding,
        encoding("windows-1252")
    );
    assert_eq!(first_column(&document, window), ["José", "María"]);
    assert!(warnings(&document, window).is_empty());

    set_overrides(
        &document,
        window,
        DialectOverrides {
            encoding: Some(encoding("utf-8")),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(
        first_column(&document, window),
        ["Jos\u{FFFD}", "Mar\u{FFFD}a"]
    );
    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::MalformedText]
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).warning_items().to_vec()),
        [
            "Some bytes are not valid UTF-8 text and are shown as \u{FFFD}. The chosen encoding is probably wrong."
        ]
    );
    assert_eq!(status_items(&document, window)[1], "Encoding: UTF-8");
    assert_eq!(control_labels(&document, window)[2], "UTF-8");

    set_overrides(
        &document,
        window,
        DialectOverrides {
            encoding: Some(encoding("windows-1252")),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(first_column(&document, window), ["José", "María"]);
    assert!(warnings(&document, window).is_empty());
    assert_eq!(
        control_labels(&document, window)[2],
        "windows-1252 (detected)"
    );
}

#[gpui::test]
fn without_a_quote_a_quoted_field_shows_its_quote_characters(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-quote");
    let (path, _) = directory.file("cities.csv", b"name,city\n\"Ana\",Lima\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(first_column(&document, window), ["Ana"]);

    set_overrides(
        &document,
        window,
        DialectOverrides {
            quote: Some(None),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(first_column(&document, window), ["\"Ana\""]);
    assert_eq!(dialect(&document, window).quote, None);
    assert_eq!(control_labels(&document, window)[1], "No quoting");
}

/// The second override is applied on top of detection together with the
/// first, and the reset drops both.
#[gpui::test]
fn reset_returns_to_the_detected_dialect_after_two_overrides(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-reset");
    let (path, _) = directory.file("cities.csv", b"name,city;zone\nAna,Lima;south\n");

    let (document, window) = open_local(cx, path);
    let detected = dialect(&document, window);
    let detected_labels = control_labels(&document, window);

    window.update(|_, cx| {
        document.update(cx, |document, cx| document.override_delimiter(b';', cx));
    });
    window.run_until_parked();
    window.update(|_, cx| {
        document.update(cx, |document, cx| document.override_has_header(false, cx));
    });
    window.run_until_parked();

    assert_eq!(
        overrides(&document, window),
        DialectOverrides {
            delimiter: Some(b';'),
            has_header: Some(false),
            ..DialectOverrides::default()
        }
    );
    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);
    assert_eq!(first_column(&document, window), ["name,city", "Ana,Lima"]);

    window.update(|_, cx| document.update(cx, |document, cx| document.reset_dialect(cx)));
    window.run_until_parked();

    assert_eq!(dialect(&document, window), detected);
    assert_eq!(overrides(&document, window), DialectOverrides::default());
    assert_eq!(column_titles(&document, window), ["name", "city;zone"]);
    assert_eq!(first_column(&document, window), ["Ana"]);
    assert_eq!(control_labels(&document, window), detected_labels);
}

#[gpui::test]
fn an_override_after_several_pages_returns_to_the_first_page(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-pages");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    load_more(&document, window);
    load_more(&document, window);

    assert_eq!(row_count(&document, window), 5);
    assert!(!has_more_records(&document, window));

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(first_column(&document, window), ["id", "0"]);
    assert_eq!(
        status_items(&document, window)[2],
        "2 records loaded, more in the file"
    );
    assert!(matches!(
        window.update(|_, cx| document.read(cx).record_count()),
        Some(RecordCount::IndexedSoFar(_))
    ));
    assert!(has_more_records(&document, window));
    assert!(!is_loading_more(&document, window));

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["id", "0", "1", "2"]);
}

/// A pipe byte can be the second byte of a Shift_JIS character, a line
/// break always ends a record, and a quote equal to the delimiter cannot be
/// told apart from it.
#[gpui::test]
fn each_refused_dialect_reports_one_error_and_changes_nothing(cx: &mut TestAppContext) {
    let refused = [
        DialectOverrides {
            delimiter: Some(b'|'),
            encoding: Some(encoding("shift_jis")),
            ..DialectOverrides::default()
        },
        DialectOverrides {
            delimiter: Some(b'\n'),
            ..DialectOverrides::default()
        },
        DialectOverrides {
            quote: Some(Some(b',')),
            ..DialectOverrides::default()
        },
    ];

    let directory = TestDirectory::new("toolbar-refused");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let detected = dialect(&document, window);
    let labels = control_labels(&document, window);

    for (index, overrides_to_refuse) in refused.into_iter().enumerate() {
        // The refusal is known before anything is read: it is reported and
        // no reread starts.
        window.update(|_, cx| {
            document.update(cx, |document, cx| {
                document.set_dialect_overrides(overrides_to_refuse, cx);
                assert!(!document.is_rereading(), "{overrides_to_refuse:?}");
            });
        });
        assert_eq!(toast_count(window), index + 1, "{overrides_to_refuse:?}");

        window.run_until_parked();

        assert_eq!(toast_count(window), index + 1, "{overrides_to_refuse:?}");
        assert_eq!(
            last_toast_title(window).as_deref(),
            Some("Could not re-read cities.csv")
        );
        assert_eq!(state(&document, window), DocumentState::Clean);
        assert_eq!(dialect(&document, window), detected);
        assert_eq!(overrides(&document, window), DialectOverrides::default());
        assert_eq!(control_labels(&document, window), labels);
        assert_eq!(column_titles(&document, window), ["name", "city"]);
        assert_eq!(first_column(&document, window), ["Ana", "Bo"]);
        assert!(!is_rereading(&document, window));
        assert!(!is_loading_more(&document, window));
    }
}

#[test]
fn a_refused_dialect_is_a_user_error_that_says_what_to_change() {
    let refusal_of = |dialect: Dialect| {
        let opened = PagedReader::open(
            dbflux_delimited::MemorySource::new(Vec::new()),
            dialect,
            READER_OPTIONS,
        );

        let Err(refusal) = opened else {
            panic!("the reader refuses the dialect");
        };

        refusal
    };

    let comma = Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: true,
        encoding: encoding("utf-8"),
    };

    let unsupported = refusal_of(Dialect {
        delimiter: b'|',
        encoding: encoding("shift_jis"),
        ..comma
    });
    let line_break = refusal_of(Dialect {
        delimiter: b'\n',
        ..comma
    });
    let same_byte = refusal_of(Dialect {
        quote: Some(b','),
        ..comma
    });

    assert!(matches!(unsupported, ReadError::UnsupportedDialect { .. }));
    assert!(matches!(line_break, ReadError::LineBreakInDialect { .. }));
    assert!(matches!(same_byte, ReadError::QuoteEqualsDelimiter { .. }));

    let causes = [
        (
            unsupported,
            "Shift_JIS text cannot be read with Pipe as its delimiter or quote. Choose another delimiter, quote or encoding.",
        ),
        (
            line_break,
            "A line break cannot be the delimiter or the quote. Choose another character.",
        ),
        (
            same_byte,
            "The quote and the delimiter are both Comma. Choose a different character for one of them.",
        ),
    ];

    for (error, cause) in causes {
        let reported = refused_dialect_error("cities.csv", &error);

        assert_eq!(reported.kind, ErrorKind::User);
        assert_eq!(reported.summary, "Could not re-read cities.csv");
        assert_eq!(reported.cause.as_deref(), Some(cause));
    }
}

/// The end state of an override asked for during a page read, whichever of
/// the two ends first. `a_page_read_before_a_reread_was_applied_is_dropped`
/// pins the order in which the page comes back last.
#[gpui::test]
fn an_override_during_a_page_load_ends_on_the_first_page_of_the_new_reading(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("toolbar-during-load");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(7));

    let (document, window) = open_local_in_small_pages(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.load_more(cx);
            assert!(document.is_loading_more());

            document.override_has_header(false, cx);
            assert!(document.is_rereading());
        });
    });
    window.run_until_parked();

    assert_eq!(first_column(&document, window), ["id", "0"]);
    assert!(!is_loading_more(&document, window));
    assert!(!is_rereading(&document, window));
    assert!(has_more_records(&document, window));
    assert_eq!(toast_count(window), 0);

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["id", "0", "1", "2"]);
}

/// The reread is applied while the page read is still running, so the page
/// that comes back belongs to the reader the reread replaced.
#[gpui::test]
fn a_page_read_before_a_reread_was_applied_is_dropped(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-stale-page");
    let (path, location) = directory.file("numbers.csv", &numbered_csv(7));

    let (document, window) = open_local_in_small_pages(cx, path);

    let headerless = Dialect {
        has_header: false,
        ..dialect(&document, window)
    };
    let (source, version) = open_source(&location).expect("the test file opens");
    let Ok(reread) = read_first_page(source, version, headerless, SMALL_PAGES) else {
        panic!("the file reads without a header");
    };

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.load_more(cx);
            assert!(document.is_loading_more());

            let generation = document.reread_generation();
            document.apply_reread_outcome(generation, Ok(reread), cx);
            assert!(!document.is_loading_more());
        });
    });
    window.run_until_parked();

    assert_eq!(first_column(&document, window), ["id", "0"]);
    assert!(!is_loading_more(&document, window));

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["id", "0", "1", "2"]);
}

/// Two overrides one after the other end with both applied: the second is
/// asked for on top of the first. `a_stale_reread_does_not_overwrite_a_newer_one`
/// pins what happens to the result of the first.
#[gpui::test]
fn two_overrides_in_a_row_end_with_both_applied(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-latest");
    let (path, _) = directory.file("cities.csv", b"name,city;zone\nAna,Lima;south\n");

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.override_delimiter(b';', cx);
            document.override_has_header(false, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);
    assert_eq!(first_column(&document, window), ["name,city", "Ana,Lima"]);
    assert!(!is_rereading(&document, window));
    assert_eq!(toast_count(window), 0);
}

/// The result of a reread that a later override replaced arrives and is
/// ignored: the table keeps what it shows until the later reread arrives.
#[gpui::test]
fn a_stale_reread_does_not_overwrite_a_newer_one(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-stale-reread");
    let (path, location) = directory.file("cities.csv", b"name,city;zone\nAna,Lima;south\n");

    let (document, window) = open_local(cx, path);

    let stale_dialect = Dialect {
        delimiter: b';',
        ..dialect(&document, window)
    };
    let (source, version) = open_source(&location).expect("the test file opens");
    let Ok(stale) = read_first_page(source, version, stale_dialect, READER_OPTIONS) else {
        panic!("the file reads with a semicolon");
    };

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let stale_generation = document.reread_generation();

            document.override_has_header(false, cx);
            assert_ne!(document.reread_generation(), stale_generation);

            document.apply_reread_outcome(stale_generation, Ok(stale), cx);
        });
    });

    assert_eq!(column_titles(&document, window), ["name", "city;zone"]);
    assert_eq!(dialect(&document, window).delimiter, b',');
    assert!(is_rereading(&document, window));

    window.run_until_parked();

    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);
    assert_eq!(dialect(&document, window).delimiter, b',');
    assert!(!dialect(&document, window).has_header);
}

/// A failure that is not a refusal: the object cannot be read again. The
/// override is given up and the controls return to the dialect shown.
#[gpui::test]
fn a_failed_reread_reports_one_error_and_keeps_the_previous_dialect(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    let labels = control_labels(&document, window);

    connection
        .store
        .fail_reads_with("SlowDown: reduce the rate");

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read cities.csv")
    );
    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert_eq!(overrides(&document, window), DialectOverrides::default());
    assert_eq!(control_labels(&document, window), labels);
    assert!(!is_rereading(&document, window));

    connection.store.stop_failing_reads();

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(first_column(&document, window), ["name", "Ana"]);
    assert_eq!(toast_count(window), 1);
}

/// The connection the document opened with is dead after a reconnect, so
/// the file is read again through the profile's new one.
#[gpui::test]
fn a_reconnected_profile_rereads_through_its_new_connection(cx: &mut TestAppContext) {
    let first = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, first.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state.clone(), profile_id, first.clone());

    let second = FakeConnection::with_object(CITIES);
    replace_connection(window, &app_state, profile_id, second.clone());
    first.store.fail_reads_with("the connection is closed");

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(first_column(&document, window), ["name", "Ana"]);
    assert_eq!(toast_count(window), 0);
    assert!(second.store.range_reads() > 0);
}

#[test]
fn the_encoding_list_holds_the_common_encodings_and_the_detected_one() {
    let names = |detected: &'static Encoding| -> Vec<&'static str> {
        encoding_choices(detected)
            .into_iter()
            .map(Encoding::name)
            .collect()
    };

    // `encoding_rs` reads the label ISO-8859-1 as windows-1252, so the two
    // are one entry.
    let common = ["UTF-8", "UTF-16LE", "UTF-16BE", "windows-1252", "Shift_JIS"];

    assert_eq!(names(encoding("utf-8")), common);
    assert_eq!(names(encoding("iso-8859-1")), common);

    let with_detected = names(encoding("euc-kr"));
    assert_eq!(with_detected[..common.len()], common);
    assert_eq!(with_detected[common.len()..], ["EUC-KR"]);
}

/// Detection reads this Korean text as EUC-KR, which is not one of the
/// common encodings, so the select lists it after them and shows it.
#[gpui::test]
fn a_detected_encoding_outside_the_common_ones_is_listed_and_selected(cx: &mut TestAppContext) {
    let text = "이름,도시\n".to_string() + &"김철수는 서울에 삽니다,서울특별시\n".repeat(20);
    let (bytes, _, unmappable) = encoding("euc-kr").encode(&text);
    assert!(!unmappable);

    let directory = TestDirectory::new("toolbar-uncommon-encoding");
    let (path, _) = directory.file("names.csv", &bytes);

    let (document, window) = open_local(cx, path);

    assert_eq!(dialect(&document, window).encoding, encoding("euc-kr"));
    assert_eq!(control_labels(&document, window)[2], "EUC-KR (detected)");
}

#[gpui::test]
fn the_quote_and_encoding_entries_of_the_pane_actions_open_their_selects(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-keyboard-open");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let open_selects = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            let controls = document
                .read(cx)
                .dialect_controls()
                .expect("a loaded document has dialect controls");

            [
                controls.delimiter.read(cx).is_open(),
                controls.quote.read(cx).is_open(),
                controls.encoding.read(cx).is_open(),
            ]
        })
    };

    assert_eq!(open_selects(window), [false, false, false]);

    run_pane_action(&document, window, "delimited-quote");
    assert_eq!(open_selects(window), [false, true, false]);

    window.simulate_keystrokes("down enter");
    window.run_until_parked();

    assert_eq!(dialect(&document, window).quote, Some(b'\''));

    run_pane_action(&document, window, "delimited-encoding");
    assert_eq!(open_selects(window), [false, false, true]);
}

/// The pane actions menu is the keyboard path of the toolbar. Its delimiter
/// entry opens the select with the keyboard on it, where the arrow moves and
/// Enter confirms.
#[gpui::test]
fn the_delimiter_select_is_driven_from_the_keyboard(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-keyboard-select");
    let (path, _) = directory.file("cities.csv", b"name,city;zone\nAna,Lima;south\n");

    let (document, window) = open_local(cx, path);

    run_pane_action(&document, window, "delimited-delimiter");

    let is_open = window.update(|_, cx| {
        document
            .read(cx)
            .dialect_controls()
            .expect("a loaded document has dialect controls")
            .delimiter
            .read(cx)
            .is_open()
    });
    assert!(is_open, "the entry opens the delimiter list");

    window.simulate_keystrokes("down down enter");
    window.run_until_parked();

    assert_eq!(dialect(&document, window).delimiter, b';');
    assert_eq!(column_titles(&document, window), ["name,city", "zone"]);
}

#[gpui::test]
fn the_header_and_reset_entries_of_the_pane_actions_run_their_controls(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-keyboard-header");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let reset_is_enabled = |window: &mut VisualTestContext| {
        window
            .update(|_, cx| document.read(cx).pane_actions(&document))
            .into_iter()
            .find(|action| action.id.as_ref() == "delimited-dialect-reset")
            .map(|action| action.enabled)
    };

    assert_eq!(reset_is_enabled(window), Some(false));

    run_pane_action(&document, window, "delimited-header");

    assert_eq!(first_column(&document, window), ["name", "Ana", "Bo"]);
    assert_eq!(reset_is_enabled(window), Some(true));

    run_pane_action(&document, window, "delimited-dialect-reset");

    assert_eq!(first_column(&document, window), ["Ana", "Bo"]);
    assert_eq!(overrides(&document, window), DialectOverrides::default());
}

#[gpui::test]
fn the_pane_handle_lists_the_dialect_and_edit_actions(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-pane-handle");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let ids: Vec<String> = window.update(|_, cx| {
        DelimitedDocument::into_pane(document.clone(), cx)
            .pane_actions(cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    assert_eq!(
        ids,
        [
            "delimited-delimiter",
            "delimited-quote",
            "delimited-header",
            "delimited-encoding",
            "delimited-dialect-reset",
            "delimited-insert-above",
            "delimited-add-column",
            "delimited-rename-column",
            "delimited-discard",
            "delimited-reload"
        ]
    );
}

/// The select moves to the item that was chosen before the document hears
/// of it. A choice the reader refuses is put back.
#[gpui::test]
fn the_refused_selection_of_a_select_returns_to_the_value_in_effect(cx: &mut TestAppContext) {
    let text = "名前,都市,備考\n".to_string()
        + &"太郎さんは東京に住んでいます,東京都,これは日本語のテキストです\n".repeat(20);
    let (bytes, _, unmappable) = encoding("shift_jis").encode(&text);
    assert!(!unmappable);

    let directory = TestDirectory::new("toolbar-refused-selection");
    let (path, _) = directory.file("names.csv", &bytes);

    let (document, window) = open_local(cx, path);

    assert_eq!(dialect(&document, window).encoding, encoding("shift_jis"));
    assert_eq!(control_labels(&document, window)[0], "Comma (detected)");

    run_pane_action(&document, window, "delimited-delimiter");
    window.simulate_keystrokes("down down down enter");
    window.run_until_parked();

    assert_eq!(toast_count(window), 1);
    assert_eq!(control_labels(&document, window)[0], "Comma (detected)");
    assert_eq!(dialect(&document, window).delimiter, b',');
    assert_eq!(overrides(&document, window), DialectOverrides::default());
}

#[gpui::test]
fn an_override_of_a_disconnected_profile_reports_one_error_and_reads_nothing(
    cx: &mut TestAppContext,
) {
    let connection = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state.clone(), profile_id, connection.clone());

    window.update(|_, cx| {
        app_state.update(cx, |state, _| {
            state.connections_mut().remove(&profile_id);
        });
    });
    let reads_before = connection.store.range_reads();
    let labels = control_labels(&document, window);

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read cities.csv")
    );
    assert_eq!(connection.store.range_reads(), reads_before);
    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert_eq!(control_labels(&document, window), labels);
    assert!(!is_rereading(&document, window));
}

// -- A reread in progress ------------------------------------------------------

fn headerless() -> DialectOverrides {
    DialectOverrides {
        has_header: Some(false),
        ..DialectOverrides::default()
    }
}

/// While the file is read again the footer says so, and the next page takes
/// no click and is not read: its records would belong to the reader the
/// reread replaces.
#[gpui::test]
fn load_more_is_off_while_the_file_is_read_again(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(&numbered_csv(7));
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    assert!(
        covered_ids(&document, window)
            .iter()
            .any(|id| id == "delimited-load-more")
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).progress_item()),
        None
    );

    let reads_before = connection.store.range_reads();

    assert!(window.update(|_, cx| document.read(cx).can_load_more()));

    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.set_dialect_overrides(headerless(), cx);
            assert!(document.is_rereading());
            assert!(!document.can_load_more());
            assert_eq!(
                document.progress_item().as_deref(),
                Some("Reading the file again\u{2026}")
            );

            document.load_more(cx);
            assert!(!document.is_loading_more());

            assert!(document.dispatch_command(Command::ResultsNextPage, window, cx));
            assert!(!document.is_loading_more());
        });
    });

    assert_eq!(connection.store.range_reads(), reads_before);

    window.run_until_parked();

    assert_eq!(first_column(&document, window), ["id", "0"]);
    assert_eq!(
        window.update(|_, cx| document.read(cx).progress_item()),
        None
    );
    assert!(window.update(|_, cx| document.read(cx).can_load_more()));
    assert!(
        covered_ids(&document, window)
            .iter()
            .any(|id| id == "delimited-load-more")
    );
}

/// Asking for the overrides that are in effect while a different reread is
/// running gives that reread up: its task is dropped before it reads.
#[gpui::test]
fn asking_for_the_dialect_in_effect_cancels_a_running_reread_without_a_read(
    cx: &mut TestAppContext,
) {
    let connection = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    let reads_before = connection.store.range_reads();

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.set_dialect_overrides(headerless(), cx);
            assert!(document.is_rereading());

            document.reset_dialect(cx);
            assert!(!document.is_rereading());
        });
    });
    window.run_until_parked();

    assert_eq!(connection.store.range_reads(), reads_before);
    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert_eq!(overrides(&document, window), DialectOverrides::default());
    assert_eq!(toast_count(window), 0);
}

/// A reread a later override replaced is dropped before it reads, so quick
/// overrides of an object cost one reading and not one each.
#[gpui::test]
fn a_replaced_reread_is_cancelled_before_it_reads(cx: &mut TestAppContext) {
    let one = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, one.clone());
    let (single, window) = open_object_in_small_pages(cx, app_state, profile_id, one.clone());

    let before = one.store.range_reads();
    set_overrides(&single, window, headerless());
    let reads_of_one_reread = one.store.range_reads() - before;
    assert!(reads_of_one_reread > 0);

    let many = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, many.clone());
    let (document, window) = open_object_in_small_pages(cx, app_state, profile_id, many.clone());

    let before = many.store.range_reads();

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.override_delimiter(b';', cx);
            document.override_quote(None, cx);
            document.override_has_header(false, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(many.store.range_reads() - before, reads_of_one_reread);
    assert_eq!(dialect(&document, window).delimiter, b';');
    assert!(!dialect(&document, window).has_header);
}

#[gpui::test]
fn a_stale_failed_reread_reports_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("toolbar-stale-failed-reread");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let stale_generation = document.reread_generation();

            document.override_has_header(false, cx);

            document.apply_reread_outcome(
                stale_generation,
                Err(OpenError::Read(ReadError::LineBreakInDialect {
                    byte: b'\n',
                })),
                cx,
            );

            assert!(
                document.is_rereading(),
                "the newer reread is still asked for"
            );
        });
    });

    assert_eq!(toast_count(window), 0);

    window.run_until_parked();

    assert_eq!(toast_count(window), 0);
    assert_eq!(first_column(&document, window), ["name", "Ana", "Bo"]);
}

/// The page read fails after a reread replaced its reader, and nobody is
/// told: the failure belongs to a reading that is no longer shown.
#[gpui::test]
fn a_stale_failed_page_read_reports_nothing(cx: &mut TestAppContext) {
    let bytes = numbered_csv(7);
    let connection = FakeConnection::with_object(&bytes);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    let directory = TestDirectory::new("toolbar-stale-failed-page");
    let (_, location) = directory.file("numbers.csv", &bytes);

    let headerless = Dialect {
        has_header: false,
        ..dialect(&document, window)
    };
    let (source, version) = open_source(&location).expect("the test file opens");
    let Ok(reread) = read_first_page(source, version, headerless, SMALL_PAGES) else {
        panic!("the file reads without a header");
    };

    connection
        .store
        .fail_reads_with("SlowDown: reduce the rate");

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.load_more(cx);
            assert!(document.is_loading_more());

            let generation = document.reread_generation();
            document.apply_reread_outcome(generation, Ok(reread), cx);
        });
    });
    window.run_until_parked();

    assert_eq!(toast_count(window), 0);
    assert_eq!(first_column(&document, window), ["id", "0"]);
    assert!(!is_loading_more(&document, window));
}

/// The override that was in effect is not a detected value, and it is the
/// one the controls return to.
#[gpui::test]
fn a_failed_reread_returns_the_controls_to_the_override_in_effect(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    set_overrides(&document, window, headerless());

    let labels = control_labels(&document, window);
    assert_eq!(labels[3], "Header row");
    assert!(!header_is_checked(&document, window));

    connection
        .store
        .fail_reads_with("SlowDown: reduce the rate");

    window.update(|_, cx| {
        document.update(cx, |document, cx| document.override_delimiter(b';', cx));
    });
    assert_eq!(control_labels(&document, window)[0], "Semicolon");

    window.run_until_parked();

    assert_eq!(toast_count(window), 1);
    assert_eq!(overrides(&document, window), headerless());
    assert_eq!(control_labels(&document, window), labels);
    assert!(!header_is_checked(&document, window));
    assert_eq!(first_column(&document, window), ["name", "Ana"]);
}
