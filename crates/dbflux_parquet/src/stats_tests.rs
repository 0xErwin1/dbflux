#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use dbflux_byte_source::MemorySource;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use crate::test_support::CountingSource;
use crate::{
    ColumnStatistics, ParquetFile, RowWindow, cells_of, column_statistics, file_statistics, open,
    read_estimate, read_window, whole_file_read_estimate,
};

const ROWS: i64 = 10_000;
const ROWS_PER_ROW_GROUP: i64 = 2_500;
const ROWS_PER_PAGE: usize = 100;

fn flat_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, false),
    ]))
}

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

fn layout_properties() -> parquet::file::properties::WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP as usize))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_dictionary_enabled(false)
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

fn flat_file(properties: WriterProperties) -> Vec<u8> {
    let batches: Vec<RecordBatch> = (0..ROWS / ROWS_PER_ROW_GROUP)
        .map(|group| flat_batch(group * ROWS_PER_ROW_GROUP, (group + 1) * ROWS_PER_ROW_GROUP))
        .collect();

    write_parquet(flat_schema(), &batches, properties)
}

fn open_bytes(bytes: Vec<u8>) -> ParquetFile {
    open(&MemorySource::new(bytes)).unwrap()
}

/// One batch per row group, so each batch lands in its own row group.
fn single_column_file(field: Field, row_groups: Vec<ArrayRef>) -> ParquetFile {
    let schema = Arc::new(Schema::new(vec![field]));
    let batches: Vec<RecordBatch> = row_groups
        .into_iter()
        .map(|column| RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap())
        .collect();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();

    for batch in &batches {
        writer.write(batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();

    open_bytes(buffer)
}

fn statistics_named<'a>(statistics: &'a [ColumnStatistics], name: &str) -> &'a ColumnStatistics {
    statistics
        .iter()
        .find(|column| &*column.name == name)
        .unwrap()
}

fn footer_compressed_bytes(file: &ParquetFile, leaf: usize) -> u64 {
    file.metadata()
        .row_groups()
        .iter()
        .map(|row_group| row_group.column(leaf).compressed_size() as u64)
        .sum()
}

#[test]
fn estimate_equals_sum_of_selected_chunk_sizes() {
    for properties in [
        layout_properties().build(),
        layout_properties()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_offset_index_disabled(true)
            .build(),
    ] {
        let file = open_bytes(flat_file(properties));

        let estimate = whole_file_read_estimate(&file, &[2, 0]).unwrap();

        let id_bytes = footer_compressed_bytes(&file, 0);
        let score_bytes = footer_compressed_bytes(&file, 2);

        assert_eq!(estimate.per_column, vec![(0, id_bytes), (2, score_bytes)]);
        assert_eq!(estimate.total_bytes, id_bytes + score_bytes);
        assert_eq!(estimate.row_groups_touched, 4);
        assert_eq!(estimate.row_group_count, 4);
    }
}

#[test]
fn window_estimate_equals_the_bytes_read() {
    let source = CountingSource::new(MemorySource::new(flat_file(layout_properties().build())));
    let file = open(&source).unwrap();
    source.clear_requests();

    let window = RowWindow::new(3_100, 500);
    let columns = [0, 1];

    let estimate = read_estimate(&file, window, &columns).unwrap();
    read_window(&file, &source, window, &columns).unwrap();

    assert!(estimate.total_bytes > 0);
    assert_eq!(estimate.total_bytes, source.bytes_read());
    assert!(estimate.total_bytes < footer_compressed_bytes(&file, 0));

    let per_column_total: u64 = estimate.per_column.iter().map(|(_, bytes)| bytes).sum();
    assert_eq!(per_column_total, estimate.total_bytes);
    assert_eq!(
        estimate
            .per_column
            .iter()
            .map(|(column, _)| *column)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn estimate_counts_row_groups_touched() {
    let file = open_bytes(flat_file(layout_properties().build()));

    let spanning = read_estimate(&file, RowWindow::new(2_400, 200), &[0]).unwrap();
    assert_eq!(spanning.row_groups_touched, 2);
    assert_eq!(spanning.row_group_count, 4);

    let inside = read_estimate(&file, RowWindow::new(5_100, 100), &[0]).unwrap();
    assert_eq!(inside.row_groups_touched, 1);

    let past_the_end = read_estimate(&file, RowWindow::new(20_000, 100), &[0]).unwrap();
    assert_eq!(past_the_end.row_groups_touched, 0);
    assert_eq!(past_the_end.row_group_count, 4);
    assert_eq!(past_the_end.total_bytes, 0);
    assert_eq!(past_the_end.per_column, vec![(0, 0)]);
}

#[test]
fn missing_null_count_is_none_not_zero() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("counted", DataType::Int64, true),
        Field::new("uncounted", DataType::Int64, true),
    ]));

    let values = || -> ArrayRef {
        Arc::new(Int64Array::from_iter(
            (0..1_000).map(|row| (row % 10 != 0).then_some(row)),
        ))
    };

    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values(), values()]).unwrap();

    let properties = WriterProperties::builder()
        .set_column_statistics_enabled(ColumnPath::from("uncounted"), EnabledStatistics::None)
        .build();

    let file = open_bytes(write_parquet(schema, &[batch], properties));
    let statistics = column_statistics(&file).unwrap();

    let counted = statistics_named(&statistics, "counted");
    assert_eq!(counted.null_count, Some(100));
    assert!(counted.range.is_some());

    let uncounted = statistics_named(&statistics, "uncounted");
    assert_eq!(uncounted.null_count, None);
    assert_eq!(uncounted.range, None);
    assert_eq!(uncounted.value_count, 1_000);
}

#[test]
fn min_max_carries_exact_flag() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("long", DataType::Utf8, false),
        Field::new("short", DataType::Utf8, false),
    ]));

    let long_values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..100).map(|row| format!("{}{row:03}", "x".repeat(40))),
    ));
    let short_values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..100).map(|row| format!("v{row:03}")),
    ));

    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![long_values, short_values]).unwrap();

    let properties = WriterProperties::builder()
        .set_statistics_truncate_length(Some(8))
        .build();

    let file = open_bytes(write_parquet(schema, &[batch], properties));
    let statistics = column_statistics(&file).unwrap();

    let long = statistics_named(&statistics, "long").range.clone().unwrap();
    assert!(!long.exact);
    assert!(long.min.starts_with("xxxxxxx"));
    assert!(long.min.len() <= 8);

    let short = statistics_named(&statistics, "short")
        .range
        .clone()
        .unwrap();
    assert!(short.exact);
    assert_eq!(&*short.min, "v000");
    assert_eq!(&*short.max, "v099");
}

#[test]
fn timestamp_range_uses_the_cell_display_rule() {
    let field = Field::new(
        "at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    );

    let later: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![1_700_000_500_000_001, 1_700_000_900_123_456])
            .with_timezone("UTC"),
    );
    let earlier: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![1_600_000_000_000_007, 1_650_000_000_000_000])
            .with_timezone("UTC"),
    );

    let bytes = {
        let schema = Arc::new(Schema::new(vec![field]));
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None).unwrap();

        for column in [later, earlier] {
            writer
                .write(&RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap())
                .unwrap();
            writer.flush().unwrap();
        }

        writer.close().unwrap();
        buffer
    };

    let source = MemorySource::new(bytes);
    let file = open(&source).unwrap();
    assert_eq!(file.row_group_count(), 2);

    let range = column_statistics(&file).unwrap()[0].range.clone().unwrap();

    let rows = read_window(&file, &source, RowWindow::new(0, 4), &[0]).unwrap();
    let page = cells_of(&rows).unwrap();

    assert_eq!(range.min, page.rows[2][0].display);
    assert_eq!(range.max, page.rows[1][0].display);
    assert_eq!(&*range.min, "2020-09-13 12:26:40.000007Z");
    assert!(range.exact);
}

#[test]
fn decimal_range_is_scaled() {
    let decimals = |values: Vec<i128>| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(10, 2)
                .unwrap(),
        )
    };

    let file = single_column_file(
        Field::new("amount", DataType::Decimal128(10, 2), false),
        vec![decimals(vec![12_345, 700]), decimals(vec![-50, 9])],
    );
    assert_eq!(file.row_group_count(), 2);

    let statistics = column_statistics(&file).unwrap();
    let amount = &statistics[0];

    assert_eq!(&*amount.type_name, "DECIMAL(10,2)");

    let range = amount.range.clone().unwrap();
    assert_eq!(&*range.min, "-0.50");
    assert_eq!(&*range.max, "123.45");
}

#[test]
fn nested_root_has_no_range_and_sums_leaf_sizes() {
    let struct_fields = Fields::from(vec![
        Field::new("count", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("flat", DataType::Int64, false),
        Field::new("detail", DataType::Struct(struct_fields.clone()), true),
    ]));

    let batch_of = |start: i64| -> RecordBatch {
        let mut counts = Int64Builder::new();
        let mut labels = StringBuilder::new();

        for row in start..start + 500 {
            counts.append_value(row);
            labels.append_value(format!("label {row}"));
        }

        let detail = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(counts.finish()), Arc::new(labels.finish())],
            None,
        );

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(start..start + 500)),
                Arc::new(detail),
            ],
        )
        .unwrap()
    };

    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(500))
        .build();

    let file = open_bytes(write_parquet(
        Arc::clone(&schema),
        &[batch_of(0), batch_of(500)],
        properties,
    ));
    assert_eq!(file.row_group_count(), 2);

    let statistics = column_statistics(&file).unwrap();
    assert_eq!(statistics.len(), 2);

    let detail = statistics_named(&statistics, "detail");

    let leaf_sum = |leaf: usize, size: fn(&parquet::file::metadata::ColumnChunkMetaData) -> i64| {
        file.metadata()
            .row_groups()
            .iter()
            .map(|row_group| size(row_group.column(leaf)) as u64)
            .sum::<u64>()
    };

    assert_eq!(
        detail.compressed_bytes,
        leaf_sum(1, |column| column.compressed_size())
            + leaf_sum(2, |column| column.compressed_size())
    );
    assert_eq!(
        detail.uncompressed_bytes,
        leaf_sum(1, |column| column.uncompressed_size())
            + leaf_sum(2, |column| column.uncompressed_size())
    );
    assert_eq!(detail.value_count, 2_000);
    assert_eq!(detail.range, None);
    assert_eq!(detail.null_count, None);
    assert!(detail.type_name.starts_with("STRUCT"));

    let flat = statistics_named(&statistics, "flat");
    assert_eq!(
        flat.compressed_bytes,
        leaf_sum(0, |column| column.compressed_size())
    );
    assert_eq!(flat.value_count, 1_000);
    assert_eq!(flat.null_count, Some(0));

    let range = flat.range.clone().unwrap();
    assert_eq!((&*range.min, &*range.max), ("0", "999"));
    assert_eq!(flat.codec.as_deref(), Some("UNCOMPRESSED"));
}

#[test]
fn zero_row_groups_gives_empty_statistics() {
    let file = open_bytes(write_parquet(
        flat_schema(),
        &[],
        WriterProperties::builder().build(),
    ));
    assert_eq!(file.row_group_count(), 0);

    let statistics = column_statistics(&file).unwrap();
    assert_eq!(
        statistics
            .iter()
            .map(|column| &*column.name)
            .collect::<Vec<_>>(),
        vec!["id", "name", "score"]
    );

    for column in &statistics {
        assert_eq!(column.codec, None);
        assert_eq!(column.compressed_bytes, 0);
        assert_eq!(column.uncompressed_bytes, 0);
        assert_eq!(column.value_count, 0);
        assert_eq!(column.null_count, None);
        assert_eq!(column.range, None);
        assert!(!column.has_dictionary);
        assert!(!column.has_page_index);
    }

    let totals = file_statistics(&file).unwrap();
    assert_eq!(totals.row_count, 0);
    assert_eq!(totals.row_group_count, 0);
    assert_eq!(totals.compressed_bytes, 0);
    assert!(totals.created_by.is_some());

    let estimate = whole_file_read_estimate(&file, &[0]).unwrap();
    assert_eq!(estimate.total_bytes, 0);
    assert_eq!(estimate.row_group_count, 0);
}
