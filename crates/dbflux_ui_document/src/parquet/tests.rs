use std::cell::Cell;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dbflux_app::keymap::Command;
use dbflux_byte_source::{ByteSource, MemorySource, SourceError};
use dbflux_components::components::column_profile_view::ColumnProfileView;
use dbflux_components::components::column_projection::ColumnProjectionPicker;
use dbflux_components::components::data_table::DataTableState;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_core::{
    ColumnProjection, DEFAULT_PAGE_ROWS, DEFAULT_PROJECTION_MAX_COLUMNS, EstimateScope, PartUnit,
};
use dbflux_parquet::RowWindow;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use parquet::arrow::ArrowWriter;

use super::document::{ParquetDocument, ParquetView};
use super::page_model::header_annotation;
use crate::keyboard_coverage::PARQUET;
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
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
    let (_host, document, window) = open_local_hosted(cx, path);

    (document, window)
}

/// Opens the file at `path` and also returns the host, which records every
/// keymap command that reached it.
fn open_local_hosted(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (
    Entity<KeymapHost<ParquetDocument>>,
    Entity<ParquetDocument>,
    &mut VisualTestContext,
) {
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

    (host, document, window)
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

fn picker(
    document: &Entity<ParquetDocument>,
    window: &mut VisualTestContext,
) -> Entity<ColumnProjectionPicker> {
    window.update(|_, cx| {
        document
            .read(cx)
            .projection_picker()
            .expect("a loaded document has a column picker")
            .clone()
    })
}

fn profile_view(
    document: &Entity<ParquetDocument>,
    window: &mut VisualTestContext,
) -> Entity<ColumnProfileView> {
    window.update(|_, cx| {
        document
            .read(cx)
            .column_profile_view()
            .expect("a loaded document has a Columns view")
            .clone()
    })
}

fn apply_projection(
    document: &Entity<ParquetDocument>,
    window: &mut VisualTestContext,
    total: usize,
    columns: &[usize],
) {
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.apply_projection(ColumnProjection::from_indices(total, columns), cx);
        })
    });
    window.run_until_parked();
}

fn shown_columns(document: &Entity<ParquetDocument>, window: &mut VisualTestContext) -> Vec<usize> {
    window.update(|_, cx| document.read(cx).shown_columns().to_vec())
}

/// The projections the document, the picker and the Columns view hold, in
/// that order.
fn projections(
    document: &Entity<ParquetDocument>,
    window: &mut VisualTestContext,
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    let picker = picker(document, window);
    let profile_view = profile_view(document, window);

    window.update(|_, cx| {
        (
            document
                .read(cx)
                .applied_projection()
                .map(ColumnProjection::selected_indices)
                .unwrap_or_default(),
            picker.read(cx).applied().selected_indices(),
            profile_view.read(cx).applied().selected_indices(),
        )
    })
}

/// A source over bytes in memory that counts the bytes it hands out.
struct CountingSource {
    inner: MemorySource,
    read_bytes: Cell<u64>,
}

impl CountingSource {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            inner: MemorySource::new(bytes),
            read_bytes: Cell::new(0),
        }
    }

    fn take_read_bytes(&self) -> u64 {
        self.read_bytes.replace(0)
    }
}

impl ByteSource for CountingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.inner.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let bytes = self.inner.read_range(range)?;
        self.read_bytes
            .set(self.read_bytes.get() + bytes.len() as u64);

        Ok(bytes)
    }
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
fn wide_file_opens_with_default_subset_and_picker_shows_it(cx: &mut TestAppContext) {
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

    let picker = picker(&document, window);
    let (label, picked) = window.update(|_, cx| {
        let picker = picker.read(cx);
        (
            picker.trigger_label().to_string(),
            picker.applied().selected_indices(),
        )
    });
    assert_eq!(label, "32 of 60 columns");
    assert_eq!(picked, shown);

    let profile_view = profile_view(&document, window);
    let profiled = window.update(|_, cx| profile_view.read(cx).applied().selected_indices());
    assert_eq!(profiled, shown);
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
fn apply_projection_rereads_from_row_zero_with_selected_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("apply-projection");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (document, window) = open_local(cx, path);

    load_more(&document, window);
    assert_eq!(row_count(&document, window), 1000);

    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(700, 3), cx)
        })
    });

    apply_projection(&document, window, 4, &[1, 3]);

    assert_eq!(titles(&document, window), ["name", "tags"]);
    assert_eq!(shown_columns(&document, window), [1, 3]);
    assert_eq!(row_count(&document, window), 500);
    assert_eq!(
        window.update(|_, cx| document.read(cx).row_counts()),
        Some((500, 1200))
    );
    assert_eq!(display(&document, window, 0, 0), "name 0");
    assert_eq!(display(&document, window, 499, 0), "name 499");

    let table_state = self::table_state(&document, window);
    assert!(
        window.update(|_, cx| table_state.read(cx).selection().is_empty()),
        "the cursor of the old columns is dropped"
    );

    load_more(&document, window);
    assert_eq!(row_count(&document, window), 1000);
    assert_eq!(display(&document, window, 999, 0), "name 999");
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn columns_tab_toggle_updates_the_data_projection(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-tab");
    let path = directory.file("rows.parquet", &rows_file(30));

    let (document, window) = open_local(cx, path);

    window.simulate_keystrokes("t");
    window.run_until_parked();
    assert_eq!(
        window.update(|_, cx| document.read(cx).view()),
        ParquetView::Columns
    );

    // The cursor starts on `id`, the first column: Space drops it.
    window.simulate_keystrokes("space");
    window.run_until_parked();

    assert_eq!(shown_columns(&document, window), [1, 2, 3]);
    assert_eq!(titles(&document, window), ["name", "payload", "tags"]);
    assert_eq!(
        projections(&document, window),
        (vec![1, 2, 3], vec![1, 2, 3], vec![1, 2, 3])
    );

    window.simulate_keystrokes("t");
    window.run_until_parked();
    assert_eq!(
        window.update(|_, cx| document.read(cx).view()),
        ParquetView::Data
    );
}

#[gpui::test]
fn picker_and_columns_view_show_the_same_projection(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("same-projection");
    let path = directory.file("rows.parquet", &rows_file(30));

    let (document, window) = open_local(cx, path);
    assert_eq!(
        projections(&document, window),
        (vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![0, 1, 2, 3])
    );

    // `f` opens the picker with the keyboard on its list: drop `id`, apply.
    let picker = picker(&document, window);

    window.simulate_keystrokes("f");
    window.run_until_parked();
    assert!(window.update(|_, cx| picker.read(cx).is_open()));

    window.simulate_keystrokes("space enter");
    window.run_until_parked();

    assert_eq!(
        projections(&document, window),
        (vec![1, 2, 3], vec![1, 2, 3], vec![1, 2, 3])
    );
    assert_eq!(titles(&document, window), ["name", "payload", "tags"]);

    let profile_view = profile_view(&document, window);
    window.update(|_, cx| profile_view.update(cx, |view, cx| view.toggle_column(0, cx)));
    window.run_until_parked();

    assert_eq!(
        projections(&document, window),
        (vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![0, 1, 2, 3])
    );
    assert_eq!(titles(&document, window), ["id", "name", "payload", "tags"]);

    let label = window.update(|_, cx| picker.read(cx).trigger_label().to_string());
    assert_eq!(label, "4 of 4 columns");
}

#[gpui::test]
fn load_more_started_before_a_projection_change_is_dropped(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("dropped-window");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (document, window) = open_local(cx, path);

    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::ResultsNextPage, window, cx);
            assert!(document.is_loading_more());

            document.apply_projection(ColumnProjection::from_indices(4, &[0, 1]), cx);
        })
    });
    window.run_until_parked();

    assert_eq!(titles(&document, window), ["id", "name"]);
    assert_eq!(
        row_count(&document, window),
        500,
        "the window of the old columns is not appended"
    );
    assert_eq!(display(&document, window, 499, 1), "name 499");
    assert!(!window.update(|_, cx| document.read(cx).is_loading_more()));
    assert_eq!(toast_count(window), 0);

    load_more(&document, window);
    assert_eq!(row_count(&document, window), 1000);
    assert_eq!(display(&document, window, 999, 1), "name 999");
}

#[gpui::test]
fn a_projection_that_cannot_be_read_goes_back_to_the_shown_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("projection-changed");
    let path = directory.file("rows.parquet", &rows_file(1200));

    let (document, window) = open_local(cx, path.clone());

    std::fs::write(&path, rows_file(1300)).expect("the file must be rewritable");

    apply_projection(&document, window, 4, &[1]);

    assert_eq!(toast_count(window), 1);
    assert!(window.update(|_, cx| document.read(cx).source_changed()));
    assert_eq!(titles(&document, window), ["id", "name", "payload", "tags"]);
    assert_eq!(
        projections(&document, window),
        (vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![0, 1, 2, 3])
    );
}

#[gpui::test]
fn estimate_bar_matches_the_bytes_the_next_page_reads(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("estimate");
    let bytes = rows_file(1200);
    let path = directory.file("rows.parquet", &bytes);

    let (document, window) = open_local(cx, path);

    let source = CountingSource::new(bytes);
    let file = dbflux_parquet::open(&source).expect("the file opens");

    for columns in [vec![0, 1, 2, 3], vec![1]] {
        apply_projection(&document, window, 4, &columns);

        let (estimate, summary) = window.update(|_, cx| {
            let document = document.read(cx);
            (
                document.next_read_estimate().cloned(),
                document.estimate_bar().map(|bar| bar.summary()),
            )
        });
        let estimate = estimate.expect("a file with more rows has a next page");

        source.take_read_bytes();
        dbflux_parquet::read_window(
            &file,
            &source,
            RowWindow::new(DEFAULT_PAGE_ROWS, DEFAULT_PAGE_ROWS),
            &columns,
        )
        .expect("the next page reads");

        assert_eq!(estimate.total_bytes, source.take_read_bytes());
        assert_eq!(
            estimate
                .per_column
                .iter()
                .map(|(column, _)| *column)
                .collect::<Vec<_>>(),
            columns
        );
        assert_eq!(
            estimate.scope,
            EstimateScope::Parts {
                touched: 1,
                total: 1,
                unit: PartUnit::RowGroups,
            }
        );

        let summary = summary.expect("the estimate is shown");
        assert!(summary.ends_with("1 of 1 row groups"), "{summary}");
    }

    load_more(&document, window);
    load_more(&document, window);

    let (estimate, bar) = window.update(|_, cx| {
        let document = document.read(cx);
        (
            document.next_read_estimate().cloned(),
            document.estimate_bar().is_some(),
        )
    });
    assert_eq!(estimate, None, "a fully loaded file has no next page");
    assert!(!bar);
}

#[gpui::test]
fn header_annotations_follow_the_projection(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("annotations");
    let path = directory.file("rows.parquet", &rows_file(40));

    let (document, window) = open_local(cx, path);

    apply_projection(&document, window, 4, &[2, 3]);

    let profile = window.update(|_, cx| {
        document
            .read(cx)
            .profile()
            .expect("a loaded document has a profile")
            .clone()
    });
    let table_state = table_state(&document, window);

    let annotations = window.update(|_, cx| {
        let state = table_state.read(cx);
        (0..3)
            .map(|column| state.header_annotation(column).cloned())
            .collect::<Vec<_>>()
    });

    assert_eq!(
        annotations,
        vec![
            Some(header_annotation(&profile.columns[2], 40)),
            Some(header_annotation(&profile.columns[3], 40)),
            None,
        ]
    );
}

#[gpui::test]
fn s_in_the_picker_list_does_not_save_a_query(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("picker-keys");
    let path = directory.file("rows.parquet", &rows_file(30));

    let (host, document, window) = open_local_hosted(cx, path);
    let picker = picker(&document, window);

    window.simulate_keystrokes("f space");
    window.run_until_parked();

    window.update(|_, cx| host.update(cx, |host, _cx| host.commands.clear()));
    window.simulate_keystrokes("s ctrl-n");
    window.run_until_parked();

    let (open, draft, applied) = window.update(|_, cx| {
        let picker = picker.read(cx);
        (
            picker.is_open(),
            picker.draft().map(ColumnProjection::selected_indices),
            picker.applied().selected_indices(),
        )
    });
    assert!(open, "the keys leave the picker open");
    assert_eq!(draft, Some(vec![1, 2, 3]), "the draft is unchanged");
    assert_eq!(applied, vec![0, 1, 2, 3]);

    // `s` stops in the list. Ctrl+N reaches the workspace, as it does from
    // every pane, where it opens a query tab; the document takes no part.
    let commands = window.update(|_, cx| host.read(cx).commands.clone());
    assert!(!commands.contains(&Command::SaveQuery), "{commands:?}");
    assert!(commands.contains(&Command::NewQueryTab), "{commands:?}");

    // The rows of the Columns view stop `s` the same way.
    window.simulate_keystrokes("escape t");
    window.run_until_parked();
    window.update(|_, cx| host.update(cx, |host, _cx| host.commands.clear()));

    window.simulate_keystrokes("s");
    window.run_until_parked();

    let commands = window.update(|_, cx| host.read(cx).commands.clone());
    assert!(commands.is_empty(), "{commands:?}");
    assert_eq!(shown_columns(&document, window), [0, 1, 2, 3]);
    assert_eq!(toast_count(window), 0);
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

    for control in [
        "parquet-load-more",
        "parquet-reload",
        "column-projection-trigger",
        "segmented-parquet-view-data",
        "segmented-parquet-view-columns",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }

    // A changed draft enables Apply.
    window.simulate_keystrokes("f space");
    window.run_until_parked();

    let checked = covered_ids(window);
    for control in [
        "column-projection-row-0",
        "column-projection-select-all",
        "column-projection-select-none",
        "column-projection-apply",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }

    window.simulate_keystrokes("escape t");
    window.run_until_parked();

    let checked = covered_ids(window);
    for control in ["column-profile-eye-0", "column-profile-sort"] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
}

#[gpui::test]
fn columns_tab_keys_sort_and_filter(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-keys");
    let path = directory.file("rows.parquet", &rows_file(30));

    let (document, window) = open_local(cx, path);
    let profile_view = profile_view(&document, window);

    window.simulate_keystrokes("t shift-t");
    window.run_until_parked();

    let sort = window.update(|_, cx| profile_view.read(cx).sort());
    assert_eq!(
        sort,
        dbflux_components::components::column_profile_view::ColumnSort::Size
    );

    window.simulate_keystrokes("/ n a m e");
    window.run_until_parked();

    let visible = window.update(|_, cx| profile_view.read(cx).visible_columns().to_vec());
    assert_eq!(visible, vec![1]);
}

fn covered_ids(window: &mut VisualTestContext) -> Vec<String> {
    let capture = FrameCapture::observe(window);

    Coverage::new(PARQUET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect()
}
