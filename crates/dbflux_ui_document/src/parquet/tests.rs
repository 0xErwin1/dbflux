use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dbflux_app::keymap::Command;
use dbflux_components::components::data_table::DataTableState;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_core::DEFAULT_PROJECTION_MAX_COLUMNS;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use parquet::arrow::ArrowWriter;

use super::document::ParquetDocument;
use crate::keyboard_coverage::PARQUET;
use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
use crate::types::{DocumentKind, DocumentState};

/// A directory removed when the test ends.
struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("dbflux-parquet-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("the test directory must be creatable");

        Self { path }
    }

    fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.path.join(name);
        std::fs::write(&path, bytes).expect("the test file must be writable");
        path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).ok();
    }
}

fn write_parquet(schema: Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).expect("the writer must open");

    for batch in batches {
        writer.write(batch).expect("the batch must be written");
    }

    writer.close().expect("the writer must close");
    buffer
}

/// A file of `rows` rows with an integer id, a name, a binary payload and a
/// list of tags, so the binary and nested displays can be checked.
fn rows_file(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("name {row}")),
    ));
    let payloads: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        (0..rows).map(|_| vec![0xde_u8, 0xad, 0xbe, 0xef]),
    ));

    let mut tags = ListBuilder::new(StringBuilder::new());

    for _ in 0..rows {
        tags.values().append_value("red");
        tags.values().append_value("blue");
        tags.append(true);
    }

    let tags: ArrayRef = Arc::new(tags.finish());

    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names, payloads, tags])
        .expect("the batch must build");

    write_parquet(schema, &[batch])
}

/// A file of `columns` integer columns named `c0`, `c1`, ... of `rows` rows.
fn wide_file(columns: usize, rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(
        (0..columns)
            .map(|column| Field::new(format!("c{column}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));

    let arrays: Vec<ArrayRef> = (0..columns)
        .map(|_| Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef)
        .collect();

    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("the batch must build");

    write_parquet(schema, &[batch])
}

/// A file with a schema and no row groups.
fn empty_file() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    write_parquet(schema, &[])
}

fn open_local(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<ParquetDocument>, &mut VisualTestContext) {
    init_keyboard_runtime(cx);

    let (host, window) = host_document(
        cx,
        move |_window, cx| cx.new(|cx| ParquetDocument::open_local(path, cx)),
        |document, _cx| document.active_context(),
        |document, command, window, cx| document.dispatch_command(command, window, cx),
    );

    let document = window.update(|_, cx| host.read(cx).document.clone());
    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();

    (document, window)
}

fn table_state(
    document: &Entity<ParquetDocument>,
    window: &mut VisualTestContext,
) -> Entity<DataTableState> {
    window.update(|_, cx| {
        document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .clone()
    })
}

fn titles(document: &Entity<ParquetDocument>, window: &mut VisualTestContext) -> Vec<String> {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state
            .read(cx)
            .model()
            .columns
            .iter()
            .map(|column| column.title.to_string())
            .collect()
    })
}

fn row_count(document: &Entity<ParquetDocument>, window: &mut VisualTestContext) -> usize {
    let table_state = table_state(document, window);

    window.update(|_, cx| table_state.read(cx).model().row_count())
}

fn display(
    document: &Entity<ParquetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    col: usize,
) -> String {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state
            .read(cx)
            .model()
            .cell(row, col)
            .expect("the cell exists")
            .display_text()
            .to_string()
    })
}

fn toast_count(window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).toast_count())
}

fn load_more(document: &Entity<ParquetDocument>, window: &mut VisualTestContext) {
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::ResultsNextPage, window, cx);
        })
    });
    window.run_until_parked();
}

#[gpui::test]
fn opens_first_500_rows_read_only(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("first-window");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (document, window) = open_local(cx, path);

    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Clean
    );
    assert_eq!(titles(&document, window), ["id", "name", "payload", "tags"]);
    assert_eq!(row_count(&document, window), 500);
    assert_eq!(
        window.update(|_, cx| document.read(cx).row_counts()),
        Some((500, 1200))
    );

    assert_eq!(display(&document, window, 7, 0), "7");
    assert_eq!(display(&document, window, 7, 1), "name 7");

    let payload = display(&document, window, 0, 2);
    assert!(payload.starts_with("0xdeadbeef"), "{payload}");

    let tags = display(&document, window, 0, 3);
    assert!(tags.starts_with('['), "{tags}");
    assert!(
        tags.contains("\"red\"") && tags.contains("\"blue\""),
        "{tags}"
    );

    let table_state = table_state(&document, window);

    let (editable, insertable) = window.update(|_, cx| {
        let state = table_state.read(cx);
        (state.is_editable(), state.is_insertable())
    });
    assert!(!editable && !insertable);

    let started = window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            state.start_editing(CellCoord::new(0, 1), window, cx)
        })
    });
    assert!(!started, "a Parquet cell must not open an editor");

    let annotation = window.update(|_, cx| table_state.read(cx).header_annotation(0).cloned());
    let annotation = annotation.expect("every shown column has its footer facts");
    assert!(annotation.leading.contains('×'), "{}", annotation.leading);
    assert_eq!(
        annotation.trailing.as_ref().map(|text| text.to_string()),
        Some("0% null".to_string())
    );

    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn wide_file_opens_with_default_subset(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("wide");
    let path = directory.file("wide.parquet", &wide_file(60, 10));

    let (document, window) = open_local(cx, path);

    let shown = window.update(|_, cx| document.read(cx).shown_columns().to_vec());
    assert_eq!(shown.len(), DEFAULT_PROJECTION_MAX_COLUMNS);
    assert_eq!(
        shown,
        (0..DEFAULT_PROJECTION_MAX_COLUMNS).collect::<Vec<_>>()
    );

    let expected: Vec<String> = (0..DEFAULT_PROJECTION_MAX_COLUMNS)
        .map(|column| format!("c{column}"))
        .collect();
    assert_eq!(titles(&document, window), expected);

    let status = window.update(|_, cx| document.read(cx).status_items());
    assert!(
        status
            .iter()
            .any(|item| item.contains("32") && item.contains("60")),
        "{status:?}"
    );
}

#[gpui::test]
fn load_more_appends_the_next_window(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("load-more");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (document, window) = open_local(cx, path);

    load_more(&document, window);

    assert_eq!(row_count(&document, window), 1000);
    assert_eq!(display(&document, window, 999, 0), "999");
    assert!(window.update(|_, cx| document.read(cx).has_more_rows()));

    load_more(&document, window);

    assert_eq!(row_count(&document, window), 1200);
    assert_eq!(display(&document, window, 1199, 1), "name 1199");
    assert!(!window.update(|_, cx| document.read(cx).has_more_rows()));
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn load_more_after_file_changed_reports_source_changed(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("changed");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (document, window) = open_local(cx, path.clone());

    std::fs::write(&path, rows_file(1300)).expect("the file must be rewritable");

    load_more(&document, window);

    assert_eq!(
        row_count(&document, window),
        500,
        "no row of the new file is shown"
    );
    assert_eq!(toast_count(window), 1);
    assert!(window.update(|_, cx| document.read(cx).source_changed()));
    assert!(!window.update(|_, cx| document.read(cx).can_load_more()));

    let last_toast =
        window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_title());
    assert!(
        last_toast
            .as_deref()
            .is_some_and(|title| title.contains("rows.parquet")),
        "{last_toast:?}"
    );

    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::RefreshSchema, window, cx);
        })
    });
    window.run_until_parked();

    assert!(!window.update(|_, cx| document.read(cx).source_changed()));
    assert_eq!(
        window.update(|_, cx| document.read(cx).row_counts()),
        Some((500, 1300))
    );
}

#[gpui::test]
fn sort_click_is_cleared(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("sort");
    let path = directory.file("rows.parquet", &rows_file(20));

    let (document, window) = open_local(cx, path);
    let table_state = table_state(&document, window);

    window.update(|_, cx| table_state.update(cx, |state, cx| state.cycle_sort(0, cx)));
    window.run_until_parked();

    assert!(window.update(|_, cx| table_state.read(cx).sort().is_none()));
    assert_eq!(display(&document, window, 0, 0), "0");
}

#[gpui::test]
fn a_file_that_is_not_parquet_shows_the_typed_error(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("not-parquet");
    let path = directory.file("cities.parquet", b"name,city\nAna,Lima\n");

    let (document, window) = open_local(cx, path);

    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Error
    );

    let failure = window.update(|_, cx| document.read(cx).failure().map(str::to_string));
    let failure = failure.expect("the open failed");
    assert!(
        failure.starts_with(&dbflux_i18n::t!("document.parquet.error.not_parquet")),
        "{failure}"
    );
    assert_eq!(toast_count(window), 1);
}

#[gpui::test]
fn an_empty_file_shows_the_empty_state(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("empty");
    let path = directory.file("empty.parquet", &empty_file());

    let (document, window) = open_local(cx, path);

    assert!(window.update(|_, cx| document.read(cx).is_empty_file()));
    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Clean
    );
    assert!(window.debug_bounds("parquet-empty").is_some());
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn the_pane_is_a_read_only_parquet_tab(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("pane");
    let path = directory.file("rows.parquet", &rows_file(3));

    let (document, window) = open_local(cx, path);

    let pane = window.update(|_, cx| ParquetDocument::into_pane(document.clone(), cx));

    assert_eq!(pane.kind(), DocumentKind::Parquet);
    window.update(|_, cx| assert_eq!(pane.tab_title(cx), "rows.parquet"));

    assert!(pane.session_tab_snapshot.is_none());
    assert!(pane.commit_pending_input.is_none());
    assert!(pane.save_for_close.is_none());
    assert!(pane.quit_disposition.is_none());
}

#[gpui::test]
fn the_parquet_document_is_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("coverage");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (_document, window) = open_local(cx, path);

    let capture = FrameCapture::observe(window);
    let checked: Vec<String> = Coverage::new(PARQUET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    for prefix in ["cell-", "header-col-"] {
        assert!(
            checked.iter().any(|id| id.starts_with(prefix)),
            "{checked:?}"
        );
    }

    for control in ["parquet-load-more", "parquet-reload"] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
}
