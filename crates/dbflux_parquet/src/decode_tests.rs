#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use dbflux_byte_source::MemorySource;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::decode::read_window_with_budget;
use crate::test_support::CountingSource;
use crate::{
    ParquetError, ParquetFile, RowWindow, WindowRows, open, read_window, window_byte_ranges,
};

const ROWS: i64 = 10_000;
const ROWS_PER_ROW_GROUP: i64 = 2_500;
const ROWS_PER_PAGE: usize = 100;

const ID: usize = 0;
const NAME: usize = 1;
const SCORE: usize = 2;

fn flat_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, false),
    ]))
}

/// Rows `start..end` of the flat fixture: `id` is the row number, `name` is
/// `name <id>` and `score` is half the id.
fn flat_batch(start: i64, end: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(start..end));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (start..end).map(|row| format!("name {row}")),
    ));
    let scores: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (start..end).map(|row| row as f64 / 2.0),
    ));

    RecordBatch::try_new(flat_schema(), vec![ids, names, scores]).unwrap()
}

/// Several row groups with many small pages per column chunk. Dictionary
/// encoding is off so a chunk's size is spread over its data pages instead of
/// sitting in one dictionary page every window has to read.
fn indexed_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP as usize))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_dictionary_enabled(false)
        .build()
}

/// Same layout as [`indexed_properties`], without the page index. Page-level
/// statistics would force the offset index back on, so they stay at chunk
/// level.
fn unindexed_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP as usize))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_offset_index_disabled(true)
        .build()
}

fn write_parquet(
    schema: SchemaRef,
    batches: &[RecordBatch],
    properties: WriterProperties,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(properties)).unwrap();

    for batch in batches {
        writer.write(batch).unwrap();
    }

    writer.close().unwrap();

    buffer
}

/// The flat fixture, written one row group per batch.
fn flat_file(properties: WriterProperties) -> Vec<u8> {
    let batches: Vec<RecordBatch> = (0..ROWS / ROWS_PER_ROW_GROUP)
        .map(|group| flat_batch(group * ROWS_PER_ROW_GROUP, (group + 1) * ROWS_PER_ROW_GROUP))
        .collect();

    write_parquet(flat_schema(), &batches, properties)
}

/// Opens `bytes` and returns the file with a source whose request log only
/// holds what is read after the footer.
fn open_counting(bytes: Vec<u8>) -> (ParquetFile, CountingSource<MemorySource>) {
    let source = CountingSource::new(MemorySource::new(bytes));
    let file = open(&source).unwrap();

    source.clear_requests();

    (file, source)
}

fn int64_column(rows: &WindowRows, name: &str) -> Vec<i64> {
    rows.batches()
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn chunk_range(file: &ParquetFile, row_group: usize, column: usize) -> Range<u64> {
    let (start, length) = file
        .metadata()
        .row_group(row_group)
        .column(column)
        .byte_range();

    start..start + length
}

fn contains(outer: &Range<u64>, inner: &Range<u64>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

fn overlaps(left: &Range<u64>, right: &Range<u64>) -> bool {
    left.start < right.end && right.start < left.end
}

fn field_names(schema: &SchemaRef) -> Vec<&str> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect()
}

#[test]
fn a_window_returns_exactly_the_requested_rows_and_columns() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    let rows = read_window(&file, &source, RowWindow::new(1_234, 500), &[ID, SCORE]).unwrap();

    assert_eq!(rows.window(), RowWindow::new(1_234, 500));
    assert_eq!(field_names(rows.schema()), vec!["id", "score"]);

    for batch in rows.batches() {
        assert_eq!(field_names(&batch.schema()), vec!["id", "score"]);
    }

    assert_eq!(
        int64_column(&rows, "id"),
        (1_234..1_734).collect::<Vec<_>>()
    );

    let scores: Vec<f64> = rows
        .batches()
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("score")
                .unwrap()
                .as_primitive::<Float64Type>()
                .values()
                .to_vec()
        })
        .collect();
    let expected: Vec<f64> = (1_234..1_734).map(|row| row as f64 / 2.0).collect();
    assert_eq!(scores, expected);
}

#[test]
fn a_window_spanning_two_row_groups_is_stitched_in_order() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    let rows = read_window(&file, &source, RowWindow::new(2_400, 300), &[ID, NAME]).unwrap();

    assert_eq!(
        int64_column(&rows, "id"),
        (2_400..2_700).collect::<Vec<_>>()
    );

    let names: Vec<String> = rows
        .batches()
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("name")
                .unwrap()
                .as_string::<i32>()
                .iter()
                .map(|name| name.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    let expected: Vec<String> = (2_400..2_700).map(|row| format!("name {row}")).collect();
    assert_eq!(names, expected);

    // The window ends inside row group 1, so nothing of row groups 2 and 3 is read.
    let later_chunks = [
        chunk_range(&file, 2, ID),
        chunk_range(&file, 2, NAME),
        chunk_range(&file, 3, ID),
        chunk_range(&file, 3, NAME),
    ];
    for request in source.requests() {
        assert!(
            later_chunks.iter().all(|chunk| !overlaps(chunk, &request)),
            "{request:?} reads a row group outside the window"
        );
    }
}

#[test]
fn a_window_in_the_middle_reads_only_the_pages_it_needs() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    let window = RowWindow::new(6_000, 500);
    let planned = window_byte_ranges(&file, window, &[NAME]).unwrap();

    let rows = read_window(&file, &source, window, &[NAME]).unwrap();
    assert_eq!(rows.window(), window);

    let chunk = chunk_range(&file, 2, NAME);
    let chunk_length = chunk.end - chunk.start;
    let requests = source.requests();
    let bytes_read = source.bytes_read();

    eprintln!(
        "middle window: {} requests, {bytes_read} bytes read of a {chunk_length}-byte chunk: {requests:?}",
        requests.len()
    );

    assert_eq!(requests, planned);
    assert!(requests.iter().all(|request| contains(&chunk, request)));
    assert!(
        bytes_read * 3 < chunk_length,
        "{bytes_read} bytes read of a {chunk_length}-byte chunk"
    );
}

#[test]
fn window_at_row_9000_reads_only_selected_chunks() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    let rows = read_window(&file, &source, RowWindow::new(9_000, 500), &[SCORE]).unwrap();
    assert_eq!(rows.window(), RowWindow::new(9_000, 500));
    assert_eq!(field_names(rows.schema()), vec!["score"]);

    let requests = source.requests();
    assert!(!requests.is_empty());

    let selected_chunk = chunk_range(&file, 3, SCORE);
    let unselected_chunks: Vec<Range<u64>> = (0..file.row_group_count())
        .flat_map(|row_group| {
            [
                chunk_range(&file, row_group, ID),
                chunk_range(&file, row_group, NAME),
            ]
        })
        .collect();

    for request in &requests {
        assert!(contains(&selected_chunk, request), "{request:?}");
        assert!(
            unselected_chunks
                .iter()
                .all(|chunk| !overlaps(chunk, request)),
            "{request:?} reads an unselected column"
        );
    }

    eprintln!(
        "row 9000: {} requests, {} bytes read of a {}-byte chunk",
        requests.len(),
        source.bytes_read(),
        selected_chunk.end - selected_chunk.start
    );
}

#[test]
fn a_window_past_the_end_returns_no_rows() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    for start in [ROWS as u64, 50_000, u64::MAX] {
        let rows = read_window(&file, &source, RowWindow::new(start, 500), &[ID]).unwrap();

        assert_eq!(rows.window(), RowWindow::new(start, 0));
        assert!(rows.batches().is_empty());
        assert_eq!(field_names(rows.schema()), vec!["id"]);
    }

    assert!(source.requests().is_empty());
}

#[test]
fn a_window_running_past_the_end_is_clamped() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    let rows = read_window(&file, &source, RowWindow::new(9_800, 500), &[ID]).unwrap();

    assert_eq!(rows.window(), RowWindow::new(9_800, 200));
    assert_eq!(
        int64_column(&rows, "id"),
        (9_800..10_000).collect::<Vec<_>>()
    );

    let rows = read_window(&file, &source, RowWindow::new(9_990, u64::MAX), &[ID]).unwrap();

    assert_eq!(rows.window(), RowWindow::new(9_990, 10));
    assert_eq!(
        int64_column(&rows, "id"),
        (9_990..10_000).collect::<Vec<_>>()
    );
}

#[test]
fn window_without_offset_index_over_budget_is_refused() {
    let (file, source) = open_counting(flat_file(unindexed_properties()));
    assert!(!file.has_offset_index());

    let name_chunk = chunk_range(&file, 0, NAME);
    let name_chunk_length = name_chunk.end - name_chunk.start;

    let refused = read_window_with_budget(&file, &source, RowWindow::new(100, 10), &[NAME], 1_024);

    match refused {
        Err(ParquetError::UnindexedChunkTooLarge {
            column,
            size,
            limit,
        }) => {
            assert_eq!(column, "name");
            assert_eq!(size, name_chunk_length);
            assert_eq!(limit, 1_024);
        }
        other => panic!("expected UnindexedChunkTooLarge, got {other:?}"),
    }

    assert!(
        source.requests().is_empty(),
        "nothing is read before refusing"
    );

    // Under the real budget the same window reads the whole chunk.
    let rows = read_window(&file, &source, RowWindow::new(100, 10), &[NAME]).unwrap();
    let names: Vec<&str> = rows.batches()[0]
        .column(0)
        .as_string::<i32>()
        .iter()
        .map(|name| name.unwrap())
        .collect();
    assert_eq!(
        names,
        (100..110)
            .map(|row| format!("name {row}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(source.requests(), vec![name_chunk]);
}

#[test]
fn a_nested_column_is_read_whole_by_its_root_index() {
    let point_fields = Fields::from(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("point", DataType::Struct(point_fields.clone()), false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let rows = 0..1_000_i64;
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.clone()));
    let point: ArrayRef = Arc::new(StructArray::new(
        point_fields,
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.clone().map(|row| row * 10),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.clone().map(|row| format!("p{row}")),
            )),
        ],
        None,
    ));
    let tags: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.clone().map(|row| format!("t{row}")),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, point, tags]).unwrap();

    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(250))
        .set_data_page_row_count_limit(50)
        .set_write_batch_size(50)
        .build();
    let (file, source) = open_counting(write_parquet(schema, &[batch], properties));

    let rows = read_window(&file, &source, RowWindow::new(300, 10), &[1]).unwrap();

    assert_eq!(field_names(rows.schema()), vec!["point"]);
    assert_eq!(rows.batches().len(), 1);

    let points = rows.batches()[0].column(0).as_struct();
    assert_eq!(points.num_columns(), 2);
    assert_eq!(
        points
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec(),
        (300..310).map(|row| row * 10).collect::<Vec<_>>()
    );
    let labels: Vec<&str> = points
        .column(1)
        .as_string::<i32>()
        .iter()
        .map(|label| label.unwrap())
        .collect();
    assert_eq!(
        labels,
        (300..310).map(|row| format!("p{row}")).collect::<Vec<_>>()
    );

    // Leaves 1 and 2 are `point.x` and `point.label`; `id` and `tag` stay unread.
    let point_chunks = [chunk_range(&file, 1, 1), chunk_range(&file, 1, 2)];
    for request in source.requests() {
        assert!(
            point_chunks.iter().any(|chunk| contains(chunk, &request)),
            "{request:?} is outside the `point` chunks"
        );
    }
}

#[test]
fn an_empty_or_out_of_range_projection_is_an_error() {
    let (file, source) = open_counting(flat_file(indexed_properties()));

    assert!(matches!(
        read_window(&file, &source, RowWindow::new(0, 10), &[]),
        Err(ParquetError::NoColumnsSelected)
    ));

    assert!(matches!(
        read_window(&file, &source, RowWindow::new(0, 10), &[ID, 7]),
        Err(ParquetError::ColumnOutOfRange {
            index: 7,
            column_count: 3
        })
    ));

    assert!(matches!(
        window_byte_ranges(&file, RowWindow::new(0, 10), &[3]),
        Err(ParquetError::ColumnOutOfRange { index: 3, .. })
    ));

    assert!(source.requests().is_empty());
}

#[test]
fn a_file_with_zero_row_groups_reads_no_rows() {
    let (file, source) = open_counting(write_parquet(flat_schema(), &[], indexed_properties()));
    assert_eq!(file.row_group_count(), 0);

    let rows = read_window(&file, &source, RowWindow::new(0, 500), &[ID, SCORE]).unwrap();

    assert_eq!(rows.window(), RowWindow::new(0, 0));
    assert!(rows.batches().is_empty());
    assert_eq!(field_names(rows.schema()), vec!["id", "score"]);
    assert!(source.requests().is_empty());
}
