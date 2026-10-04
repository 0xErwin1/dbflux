#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::builder::{
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::types::{ArrowPrimitiveType, Decimal256Type, Float16Type, Int32Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, DurationMicrosecondArray, FixedSizeBinaryArray, Float16Array, Float32Array,
    Float64Array, Int8Array, Int32Array, Int64Array, IntervalYearMonthArray, LargeStringArray,
    RecordBatch, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt64Array,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use dbflux_byte_source::MemorySource;
use parquet::arrow::ArrowWriter;
use parquet::basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType};
use parquet::data_type::{
    ByteArray, ByteArrayType, FixedLenByteArray, FixedLenByteArrayType, Int96, Int96Type,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::Type;

use crate::{
    Cell, CellKind, CellPage, ColumnKind, NESTED_DISPLAY_CHARS, RowWindow, cells_of, open,
    read_window,
};

/// Writes `columns` with the Arrow writer, reads every row back through
/// [`read_window`] and converts it.
fn arrow_page(fields: Vec<Field>, columns: Vec<ArrayRef>) -> CellPage {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    page_of(buffer)
}

fn single_arrow_column(field: Field, column: ArrayRef) -> CellPage {
    arrow_page(vec![field], vec![column])
}

/// Writes one optional column with the low-level writer, for the Parquet
/// types the Arrow writer never produces (INT96, legacy INTERVAL, UUID, JSON,
/// GEOMETRY).
fn low_level_column(
    column_type: Type,
    write: impl FnOnce(&mut SerializedColumnWriter<'_>),
) -> CellPage {
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(column_type)])
            .build()
            .unwrap(),
    );

    let mut buffer = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut buffer,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();

    let mut row_group = writer.next_row_group().unwrap();
    let mut column = row_group.next_column().unwrap().unwrap();
    write(&mut column);
    column.close().unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();

    page_of(buffer)
}

fn page_of(bytes: Vec<u8>) -> CellPage {
    let source = MemorySource::new(bytes);
    let file = open(&source).unwrap();
    let all_columns: Vec<usize> = (0..file.schema().fields().len()).collect();

    let rows = read_window(
        &file,
        &source,
        RowWindow::new(0, file.row_count()),
        &all_columns,
    )
    .unwrap();

    cells_of(&rows).unwrap()
}

fn column(page: &CellPage, index: usize) -> Vec<Cell> {
    page.rows.iter().map(|row| row[index].clone()).collect()
}

fn displays(page: &CellPage, index: usize) -> Vec<String> {
    column(page, index)
        .iter()
        .map(|cell| cell.display.to_string())
        .collect()
}

fn optional_primitive(
    name: &str,
    physical: PhysicalType,
) -> parquet::schema::types::PrimitiveTypeBuilder<'_> {
    Type::primitive_type_builder(name, physical).with_repetition(Repetition::OPTIONAL)
}

struct ScalarCase {
    name: &'static str,
    data_type: DataType,
    array: ArrayRef,
    type_name: &'static str,
    column_kind: ColumnKind,
    cells: Vec<(CellKind, &'static str)>,
}

fn scalar_cases() -> Vec<ScalarCase> {
    vec![
        ScalarCase {
            name: "int8",
            data_type: DataType::Int8,
            array: Arc::new(Int8Array::from(vec![Some(-3), None])),
            type_name: "INT8",
            column_kind: ColumnKind::Integer,
            cells: vec![(CellKind::Integer(-3), "-3"), (CellKind::Null, "NULL")],
        },
        ScalarCase {
            name: "int64",
            data_type: DataType::Int64,
            array: Arc::new(Int64Array::from(vec![i64::MIN, 7])),
            type_name: "INT64",
            column_kind: ColumnKind::Integer,
            cells: vec![
                (CellKind::Integer(i64::MIN), "-9223372036854775808"),
                (CellKind::Integer(7), "7"),
            ],
        },
        ScalarCase {
            name: "uint8",
            data_type: DataType::UInt8,
            array: Arc::new(UInt8Array::from(vec![200, 0])),
            type_name: "UINT8",
            column_kind: ColumnKind::Integer,
            cells: vec![(CellKind::Integer(200), "200"), (CellKind::Integer(0), "0")],
        },
        ScalarCase {
            name: "float32",
            data_type: DataType::Float32,
            array: Arc::new(Float32Array::from(vec![0.1, 2.0])),
            type_name: "FLOAT",
            column_kind: ColumnKind::Float,
            cells: vec![
                (CellKind::Float(f64::from(0.1_f32)), "0.1"),
                (CellKind::Float(2.0), "2.0"),
            ],
        },
        ScalarCase {
            name: "float64",
            data_type: DataType::Float64,
            array: Arc::new(Float64Array::from(vec![1.25, f64::NAN])),
            type_name: "DOUBLE",
            column_kind: ColumnKind::Float,
            cells: vec![
                (CellKind::Float(1.25), "1.25"),
                (CellKind::Float(f64::NAN), "NaN"),
            ],
        },
        ScalarCase {
            name: "float16",
            data_type: DataType::Float16,
            array: Arc::new(Float16Array::from(vec![
                <Float16Type as ArrowPrimitiveType>::Native::from_f32(1.5),
                <Float16Type as ArrowPrimitiveType>::Native::from_f32(-0.25),
            ])),
            type_name: "FLOAT16",
            column_kind: ColumnKind::Float,
            cells: vec![
                (CellKind::Float(1.5), "1.5"),
                (CellKind::Float(-0.25), "-0.25"),
            ],
        },
        ScalarCase {
            name: "boolean",
            data_type: DataType::Boolean,
            array: Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            type_name: "BOOLEAN",
            column_kind: ColumnKind::Unknown,
            cells: vec![
                (CellKind::Bool(true), "true"),
                (CellKind::Bool(false), "false"),
            ],
        },
        ScalarCase {
            name: "utf8",
            data_type: DataType::Utf8,
            array: Arc::new(StringArray::from(vec!["hello", ""])),
            type_name: "STRING",
            column_kind: ColumnKind::Text,
            cells: vec![(CellKind::Text, "hello"), (CellKind::Text, "")],
        },
        ScalarCase {
            name: "large_utf8",
            data_type: DataType::LargeUtf8,
            array: Arc::new(LargeStringArray::from(vec!["large", "text"])),
            type_name: "STRING",
            column_kind: ColumnKind::Text,
            cells: vec![(CellKind::Text, "large"), (CellKind::Text, "text")],
        },
        ScalarCase {
            name: "utf8_view",
            data_type: DataType::Utf8View,
            array: Arc::new(StringViewArray::from(vec![
                "a view longer than twelve bytes",
                "v",
            ])),
            type_name: "STRING",
            column_kind: ColumnKind::Text,
            cells: vec![
                (CellKind::Text, "a view longer than twelve bytes"),
                (CellKind::Text, "v"),
            ],
        },
        ScalarCase {
            name: "dictionary",
            data_type: DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            array: Arc::new(
                vec![Some("red"), None]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            type_name: "STRING",
            column_kind: ColumnKind::Text,
            cells: vec![(CellKind::Text, "red"), (CellKind::Null, "NULL")],
        },
        ScalarCase {
            name: "all_null",
            data_type: DataType::Int32,
            array: Arc::new(Int32Array::from(vec![None::<i32>, None])),
            type_name: "INT32",
            column_kind: ColumnKind::Integer,
            cells: vec![(CellKind::Null, "NULL"), (CellKind::Null, "NULL")],
        },
        ScalarCase {
            name: "date32",
            data_type: DataType::Date32,
            array: Arc::new(Date32Array::from(vec![19_000, -1])),
            type_name: "DATE",
            column_kind: ColumnKind::Timestamp,
            cells: vec![
                (CellKind::Text, "2022-01-08"),
                (CellKind::Text, "1969-12-31"),
            ],
        },
        ScalarCase {
            name: "time_seconds",
            data_type: DataType::Time32(TimeUnit::Second),
            array: Arc::new(Time32SecondArray::from(vec![3_723, 0])),
            type_name: "TIME(SECONDS)",
            column_kind: ColumnKind::Text,
            cells: vec![(CellKind::Text, "01:02:03"), (CellKind::Text, "00:00:00")],
        },
        ScalarCase {
            name: "time_millis",
            data_type: DataType::Time32(TimeUnit::Millisecond),
            array: Arc::new(Time32MillisecondArray::from(vec![3_723_456, 3_723_000])),
            type_name: "TIME(MILLIS)",
            column_kind: ColumnKind::Text,
            cells: vec![
                (CellKind::Text, "01:02:03.456"),
                (CellKind::Text, "01:02:03.000"),
            ],
        },
        ScalarCase {
            name: "time_nanos",
            data_type: DataType::Time64(TimeUnit::Nanosecond),
            array: Arc::new(Time64NanosecondArray::from(vec![3_723_456_789_012])),
            type_name: "TIME(NANOS)",
            column_kind: ColumnKind::Text,
            cells: vec![(CellKind::Text, "01:02:03.456789012")],
        },
        ScalarCase {
            name: "duration",
            data_type: DataType::Duration(TimeUnit::Microsecond),
            array: Arc::new(DurationMicrosecondArray::from(vec![1_500, -2])),
            type_name: "DURATION(MICROS)",
            column_kind: ColumnKind::Integer,
            cells: vec![
                (CellKind::Integer(1_500), "1500"),
                (CellKind::Integer(-2), "-2"),
            ],
        },
        ScalarCase {
            name: "interval_year_month",
            data_type: DataType::Interval(IntervalUnit::YearMonth),
            array: Arc::new(IntervalYearMonthArray::from(vec![14, -1])),
            type_name: "INTERVAL (days and milliseconds not read)",
            column_kind: ColumnKind::Text,
            cells: vec![(CellKind::Text, "14 months"), (CellKind::Text, "-1 months")],
        },
    ]
}

#[test]
fn scalar_types_follow_their_display_rule() {
    for case in scalar_cases() {
        let page = single_arrow_column(Field::new(case.name, case.data_type, true), case.array);

        let column_display = &page.columns[0];
        assert_eq!(&*column_display.name, case.name, "{}", case.name);
        assert_eq!(&*column_display.type_name, case.type_name, "{}", case.name);
        assert_eq!(column_display.kind, case.column_kind, "{}", case.name);

        let cells = column(&page, 0);
        assert_eq!(cells.len(), case.cells.len(), "{}", case.name);

        for (cell, (kind, display)) in cells.iter().zip(&case.cells) {
            match (&cell.kind, kind) {
                (CellKind::Float(actual), CellKind::Float(expected)) if expected.is_nan() => {
                    assert!(actual.is_nan(), "{}", case.name)
                }
                _ => assert_eq!(&cell.kind, kind, "{}", case.name),
            }

            assert_eq!(&*cell.display, *display, "{}", case.name);
        }
    }
}

#[test]
fn cells_are_row_major_across_columns() {
    let page = arrow_page(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    );

    let rows: Vec<Vec<String>> = page
        .rows
        .iter()
        .map(|row| row.iter().map(|cell| cell.display.to_string()).collect())
        .collect();

    assert_eq!(rows, vec![vec!["1", "a"], vec!["2", "NULL"]]);
}

#[test]
fn uint64_above_i64_max_is_text() {
    let page = single_arrow_column(
        Field::new("count", DataType::UInt64, true),
        Arc::new(UInt64Array::from(vec![
            u64::MAX,
            i64::MAX as u64,
            i64::MAX as u64 + 1,
        ])),
    );

    let cells = column(&page, 0);

    assert_eq!(cells[0].kind, CellKind::Text);
    assert_eq!(&*cells[0].display, "18446744073709551615");
    assert_eq!(cells[1].kind, CellKind::Integer(i64::MAX));
    assert_eq!(&*cells[1].display, "9223372036854775807");
    assert_eq!(cells[2].kind, CellKind::Text);
    assert_eq!(&*cells[2].display, "9223372036854775808");
    assert_eq!(&*page.columns[0].type_name, "UINT64");
    assert_eq!(page.columns[0].kind, ColumnKind::Integer);
}

#[test]
fn decimal_displays_exactly() {
    let page = arrow_page(
        vec![
            Field::new("small", DataType::Decimal128(9, 2), true),
            Field::new("wide", DataType::Decimal128(38, 10), true),
            Field::new("huge", DataType::Decimal256(60, 5), true),
        ],
        vec![
            Arc::new(
                Decimal128Array::from(vec![12_345, -5])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![12_345_678_901_234_567_890_123_456_789_i128, 1])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![
                    <Decimal256Type as ArrowPrimitiveType>::Native::from_i128(-1_234_567),
                    <Decimal256Type as ArrowPrimitiveType>::Native::from_i128(0),
                ])
                .with_precision_and_scale(60, 5)
                .unwrap(),
            ),
        ],
    );

    assert_eq!(displays(&page, 0), vec!["123.45", "-0.05"]);
    assert_eq!(
        displays(&page, 1),
        vec!["1234567890123456789.0123456789", "0.0000000001"]
    );
    assert_eq!(displays(&page, 2), vec!["-12.34567", "0.00000"]);

    for (index, type_name) in ["DECIMAL(9,2)", "DECIMAL(38,10)", "DECIMAL(60,5)"]
        .into_iter()
        .enumerate()
    {
        assert_eq!(&*page.columns[index].type_name, type_name);
        assert_eq!(page.columns[index].kind, ColumnKind::Text);
        assert!(
            column(&page, index)
                .iter()
                .all(|cell| cell.kind == CellKind::Text)
        );
    }
}

#[test]
fn utc_timestamp_marks_offset() {
    let page = arrow_page(
        vec![
            Field::new(
                "micros",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "millis",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                true,
            ),
            Field::new(
                "nanos",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ],
        vec![
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_700_000_000_123_456, -1])
                    .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![1_700_000_000_000, 0]).with_timezone("+00:00"),
            ),
            Arc::new(
                TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789, 5])
                    .with_timezone("UTC"),
            ),
        ],
    );

    assert_eq!(
        displays(&page, 0),
        vec!["2023-11-14 22:13:20.123456Z", "1969-12-31 23:59:59.999999Z"]
    );
    assert_eq!(
        displays(&page, 1),
        vec!["2023-11-14 22:13:20.000Z", "1970-01-01 00:00:00.000Z"]
    );
    assert_eq!(
        displays(&page, 2),
        vec![
            "2023-11-14 22:13:20.123456789Z",
            "1970-01-01 00:00:00.000000005Z"
        ]
    );

    assert_eq!(&*page.columns[0].type_name, "TIMESTAMP(MICROS, UTC)");
    assert_eq!(&*page.columns[1].type_name, "TIMESTAMP(MILLIS, UTC)");
    assert_eq!(&*page.columns[2].type_name, "TIMESTAMP(NANOS, UTC)");
    assert!(
        page.columns
            .iter()
            .all(|column| column.kind == ColumnKind::Timestamp)
    );
}

#[test]
fn naive_timestamp_is_shown_as_written() {
    let page = arrow_page(
        vec![
            Field::new(
                "millis",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("seconds", DataType::Timestamp(TimeUnit::Second, None), true),
        ],
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1_700_000_000_123])),
            Arc::new(TimestampSecondArray::from(vec![1_700_000_000])),
        ],
    );

    assert_eq!(displays(&page, 0), vec!["2023-11-14 22:13:20.123"]);
    assert_eq!(displays(&page, 1), vec!["2023-11-14 22:13:20"]);
    assert_eq!(&*page.columns[0].type_name, "TIMESTAMP(MILLIS)");
    assert_eq!(page.columns[0].kind, ColumnKind::Timestamp);
    assert_eq!(&*page.columns[1].type_name, "TIMESTAMP(SECONDS)");
    assert_eq!(column(&page, 0)[0].kind, CellKind::Text);
}

#[test]
fn named_zone_shown_in_utc_with_zone_name() {
    let page = single_arrow_column(
        Field::new(
            "berlin",
            DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Berlin".into())),
            true,
        ),
        Arc::new(
            TimestampMicrosecondArray::from(vec![1_700_000_000_123_456])
                .with_timezone("Europe/Berlin"),
        ),
    );

    assert_eq!(
        displays(&page, 0),
        vec!["2023-11-14 22:13:20.123456Z [Europe/Berlin]"]
    );
    assert_eq!(&*page.columns[0].type_name, "TIMESTAMP(MICROS, UTC)");
}

#[test]
fn int96_labelled_naive() {
    let nanos_of_day: u64 = 80_000_123_456_789;

    let page = low_level_column(
        optional_primitive("legacy_ts", PhysicalType::INT96)
            .build()
            .unwrap(),
        |column| {
            let mut value = Int96::new();
            value.set_data(
                (nanos_of_day & 0xFFFF_FFFF) as u32,
                (nanos_of_day >> 32) as u32,
                2_460_263,
            );

            column
                .typed::<Int96Type>()
                .write_batch(&[value], Some(&[1, 0]), None)
                .unwrap();
        },
    );

    assert_eq!(
        displays(&page, 0),
        vec!["2023-11-14 22:13:20.123456789", "NULL"]
    );
    assert_eq!(&*page.columns[0].type_name, "INT96");
    assert_eq!(page.columns[0].kind, ColumnKind::Timestamp);
}

#[test]
fn legacy_interval_names_the_months_it_cannot_read() {
    let page = low_level_column(
        optional_primitive("legacy_interval", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(12)
            .with_converted_type(ConvertedType::INTERVAL)
            .build()
            .unwrap(),
        |column| {
            column
                .typed::<FixedLenByteArrayType>()
                .write_batch(
                    &[FixedLenByteArray::from(vec![
                        1_u8, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
                    ])],
                    Some(&[1]),
                    None,
                )
                .unwrap();
        },
    );

    assert_eq!(displays(&page, 0), vec!["2 days 3 ms"]);
    assert_eq!(&*page.columns[0].type_name, "INTERVAL (months not read)");
    assert_eq!(page.columns[0].kind, ColumnKind::Text);
}

#[test]
fn binary_hex_preview() {
    let forty_bytes: Vec<u8> = (0..40).collect();

    let page = arrow_page(
        vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("fixed", DataType::FixedSizeBinary(4), true),
        ],
        vec![
            Arc::new(BinaryArray::from(vec![
                b"\x00\x01\x02".as_ref(),
                forty_bytes.as_slice(),
                b"".as_ref(),
            ])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    vec![
                        vec![0xde_u8, 0xad, 0xbe, 0xef],
                        vec![1, 2, 3, 4],
                        vec![0, 0, 0, 1],
                    ]
                    .into_iter(),
                )
                .unwrap(),
            ),
        ],
    );

    assert_eq!(
        displays(&page, 0),
        vec![
            "0x000102 (3 bytes)",
            "0x000102030405060708090a0b0c0d0e0f… (40 bytes)",
            "0x (0 bytes)",
        ]
    );
    assert_eq!(
        column(&page, 0)
            .iter()
            .map(|cell| cell.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            CellKind::Binary { length: 3 },
            CellKind::Binary { length: 40 },
            CellKind::Binary { length: 0 },
        ]
    );
    assert_eq!(displays(&page, 1)[0], "0xdeadbeef (4 bytes)");
    assert_eq!(&*page.columns[0].type_name, "BYTE_ARRAY");
    assert_eq!(&*page.columns[1].type_name, "FIXED_LEN_BYTE_ARRAY(4)");
    assert_eq!(page.columns[0].kind, ColumnKind::Unknown);
}

#[test]
fn uuid_is_canonical_text() {
    let page = low_level_column(
        optional_primitive("id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(16)
            .with_logical_type(Some(LogicalType::Uuid))
            .build()
            .unwrap(),
        |column| {
            column
                .typed::<FixedLenByteArrayType>()
                .write_batch(
                    &[FixedLenByteArray::from((0_u8..16).collect::<Vec<_>>())],
                    Some(&[1]),
                    None,
                )
                .unwrap();
        },
    );

    assert_eq!(
        displays(&page, 0),
        vec!["00010203-0405-0607-0809-0a0b0c0d0e0f"]
    );
    assert_eq!(column(&page, 0)[0].kind, CellKind::Text);
    assert_eq!(&*page.columns[0].type_name, "UUID");
    assert_eq!(page.columns[0].kind, ColumnKind::Text);
}

#[test]
fn json_logical_type_is_shown_as_written() {
    let page = low_level_column(
        optional_primitive("document", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::Json))
            .build()
            .unwrap(),
        |column| {
            column
                .typed::<ByteArrayType>()
                .write_batch(&[ByteArray::from(r#"{"a": [1, 2]}"#)], Some(&[1]), None)
                .unwrap();
        },
    );

    assert_eq!(displays(&page, 0), vec![r#"{"a": [1, 2]}"#]);
    assert_eq!(&*page.columns[0].type_name, "JSON");
    assert_eq!(page.columns[0].kind, ColumnKind::Text);
}

#[test]
fn unsupported_type_is_named_not_panicking() {
    let page = low_level_column(
        optional_primitive("shape", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::geometry(None)))
            .build()
            .unwrap(),
        |column| {
            column
                .typed::<ByteArrayType>()
                .write_batch(&[ByteArray::from(vec![1_u8, 2, 3])], Some(&[1, 0]), None)
                .unwrap();
        },
    );

    assert_eq!(displays(&page, 0), vec!["<unsupported: GEOMETRY>", "NULL"]);
    assert_eq!(column(&page, 0)[0].kind, CellKind::Text);
    assert_eq!(&*page.columns[0].type_name, "GEOMETRY (unsupported)");
    assert_eq!(page.columns[0].kind, ColumnKind::Unknown);
}

#[test]
fn an_empty_window_still_describes_its_columns() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let source = MemorySource::new(buffer);
    let file = open(&source).unwrap();
    let rows = read_window(&file, &source, RowWindow::new(10, 5), &[0]).unwrap();

    let page = cells_of(&rows).unwrap();

    assert!(page.rows.is_empty());
    assert_eq!(&*page.columns[0].name, "id");
    assert_eq!(&*page.columns[0].type_name, "INT64");
}

#[test]
fn values_outside_the_calendar_show_their_raw_value() {
    let page = arrow_page(
        vec![
            Field::new(
                "far",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("late", DataType::Time32(TimeUnit::Millisecond), true),
        ],
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![i64::MAX])),
            Arc::new(Time32MillisecondArray::from(vec![86_400_000])),
        ],
    );

    assert_eq!(
        displays(&page, 0),
        vec!["<out of range: 9223372036854775807 MILLIS>"]
    );
    assert_eq!(displays(&page, 1), vec!["<out of range: 86400000 MILLIS>"]);
    assert_eq!(column(&page, 0)[0].kind, CellKind::Text);
}

#[test]
fn nested_is_truncated_json() {
    let mut lists = ListBuilder::new(Int32Builder::new());
    lists.values().append_slice(&[1, 2, 3]);
    lists.append(true);
    lists.append(true);
    lists.append(false);
    lists.values().append_value(4);
    lists.values().append_null();
    lists.append(true);
    lists.values().append_slice(&(0..500).collect::<Vec<i32>>());
    lists.append(true);

    let structs = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![
                Some(7),
                None,
                Some(1),
                Some(2),
                Some(3),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("x \"y\"\n"),
                Some("z"),
                None,
                Some(""),
                Some("w"),
            ])) as ArrayRef,
        ),
    ]);

    let lists = lists.finish();
    let page = arrow_page(
        vec![
            Field::new("numbers", lists.data_type().clone(), true),
            Field::new("pair", structs.data_type().clone(), true),
        ],
        vec![Arc::new(lists), Arc::new(structs)],
    );

    let numbers = displays(&page, 0);
    assert_eq!(numbers[0], "[1,2,3]");
    assert_eq!(numbers[1], "[]");
    assert_eq!(numbers[2], "NULL");
    assert_eq!(numbers[3], "[4,null]");

    let long = &numbers[4];
    assert_eq!(long.chars().count(), NESTED_DISPLAY_CHARS + 1);
    assert!(long.starts_with("[0,1,2,3,"));
    assert!(long.ends_with('…'));

    assert_eq!(
        displays(&page, 1),
        vec![
            r#"{"a":7,"b":"x \"y\"\n"}"#,
            r#"{"a":null,"b":"z"}"#,
            r#"{"a":1,"b":null}"#,
            r#"{"a":2,"b":""}"#,
            r#"{"a":3,"b":"w"}"#,
        ]
    );

    assert_eq!(column(&page, 0)[0].kind, CellKind::Nested);
    assert_eq!(column(&page, 0)[2].kind, CellKind::Null);
    assert_eq!(&*page.columns[0].type_name, "LIST<INT32>");
    assert_eq!(&*page.columns[1].type_name, "STRUCT<a: INT32, b: STRING>");
    assert_eq!(page.columns[0].kind, ColumnKind::Unknown);
    assert_eq!(page.columns[1].kind, ColumnKind::Unknown);
}

#[test]
fn map_with_integer_keys_renders_pairs() {
    let mut integer_keys = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
    integer_keys.keys().append_value(1);
    integer_keys.values().append_value("one");
    integer_keys.keys().append_value(2);
    integer_keys.values().append_null();
    integer_keys.append(true).unwrap();

    let mut string_keys = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    string_keys.keys().append_value("k");
    string_keys.values().append_value(1);
    string_keys.keys().append_value("q\"");
    string_keys.values().append_value(2);
    string_keys.append(true).unwrap();

    let integer_keys = integer_keys.finish();
    let string_keys = string_keys.finish();

    let page = arrow_page(
        vec![
            Field::new("by_number", integer_keys.data_type().clone(), true),
            Field::new("by_name", string_keys.data_type().clone(), true),
        ],
        vec![Arc::new(integer_keys), Arc::new(string_keys)],
    );

    assert_eq!(displays(&page, 0), vec![r#"[[1,"one"],[2,null]]"#]);
    assert_eq!(displays(&page, 1), vec![r#"{"k":1,"q\"":2}"#]);
    assert_eq!(&*page.columns[0].type_name, "MAP<INT32, STRING>");
    assert_eq!(&*page.columns[1].type_name, "MAP<STRING, INT64>");
    assert_eq!(column(&page, 0)[0].kind, CellKind::Nested);
}

#[test]
fn nested_timestamp_follows_scalar_rule() {
    let mut timestamps = ListBuilder::new(TimestampMicrosecondBuilder::new().with_timezone("UTC"));
    timestamps.values().append_value(1_700_000_000_123_456);
    timestamps.append(true);

    let mut floats = ListBuilder::new(Float64Builder::new());
    floats.values().append_slice(&[2.0, f64::NAN, 0.5]);
    floats.append(true);

    let details = StructArray::from(vec![
        (
            Arc::new(Field::new("price", DataType::Decimal128(9, 2), true)),
            Arc::new(
                Decimal128Array::from(vec![12_345])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ) as ArrayRef,
        ),
        (
            Arc::new(Field::new("blob", DataType::Binary, true)),
            Arc::new(BinaryArray::from(vec![b"\x0a\x1b".as_ref()])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("big", DataType::UInt64, true)),
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
        ),
    ]);

    let timestamps = timestamps.finish();
    let floats = floats.finish();

    let page = arrow_page(
        vec![
            Field::new("moments", timestamps.data_type().clone(), true),
            Field::new("ratios", floats.data_type().clone(), true),
            Field::new("details", details.data_type().clone(), true),
        ],
        vec![Arc::new(timestamps), Arc::new(floats), Arc::new(details)],
    );

    assert_eq!(
        displays(&page, 0),
        vec![r#"["2023-11-14 22:13:20.123456Z"]"#]
    );
    assert_eq!(displays(&page, 1), vec![r#"[2.0,"NaN",0.5]"#]);
    assert_eq!(
        displays(&page, 2),
        vec![r#"{"price":"123.45","blob":"0x0a1b (2 bytes)","big":18446744073709551615}"#]
    );
    assert_eq!(&*page.columns[0].type_name, "LIST<TIMESTAMP(MICROS, UTC)>");
    assert_eq!(
        &*page.columns[2].type_name,
        "STRUCT<price: DECIMAL(9,2), blob: BYTE_ARRAY, big: UINT64>"
    );
}

#[test]
fn nested_uuid_uses_the_parquet_logical_type() {
    let identifier = optional_primitive("id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_length(16)
        .with_logical_type(Some(LogicalType::Uuid))
        .build()
        .unwrap();

    let owner = Type::group_type_builder("owner")
        .with_repetition(Repetition::OPTIONAL)
        .with_fields(vec![Arc::new(identifier)])
        .build()
        .unwrap();

    let page = low_level_column(owner, |column| {
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &[FixedLenByteArray::from((0_u8..16).collect::<Vec<_>>())],
                Some(&[2, 1, 0]),
                None,
            )
            .unwrap();
    });

    assert_eq!(
        displays(&page, 0),
        vec![
            r#"{"id":"00010203-0405-0607-0809-0a0b0c0d0e0f"}"#,
            r#"{"id":null}"#,
            "NULL"
        ]
    );
    assert_eq!(&*page.columns[0].type_name, "STRUCT<id: UUID>");
}

#[test]
fn variant_group_is_named_unsupported() {
    let leaf = |name: &str| {
        Arc::new(
            Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        )
    };

    let variant = Type::group_type_builder("payload")
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(Some(LogicalType::variant(None)))
        .with_fields(vec![leaf("metadata"), leaf("value")])
        .build()
        .unwrap();

    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(variant)])
            .build()
            .unwrap(),
    );

    let mut buffer = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut buffer,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();

    let mut row_group = writer.next_row_group().unwrap();

    for bytes in [vec![1_u8, 0, 0], vec![0_u8]] {
        let mut column = row_group.next_column().unwrap().unwrap();
        column
            .typed::<ByteArrayType>()
            .write_batch(&[ByteArray::from(bytes)], Some(&[1, 0]), None)
            .unwrap();
        column.close().unwrap();
    }

    row_group.close().unwrap();
    writer.close().unwrap();

    let page = page_of(buffer);

    assert_eq!(displays(&page, 0), vec!["<unsupported: VARIANT>", "NULL"]);
    assert_eq!(&*page.columns[0].type_name, "VARIANT (unsupported)");
    assert_eq!(page.columns[0].kind, ColumnKind::Unknown);
}
