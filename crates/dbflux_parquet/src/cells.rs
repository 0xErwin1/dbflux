//! Turns decoded Arrow batches into display-ready cells.
//!
//! Every display string is built here, once, so a view only copies it. The
//! cell and column kinds are neutral: they carry no UI type, and the UI maps
//! them onto its own table model.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, Date32Type, Date64Type, Decimal32Type,
    Decimal64Type, Decimal128Type, Decimal256Type, DecimalType, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, PrimitiveArray};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use parquet::basic::{
    ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, TimestampType, Type as PhysicalType,
};
use parquet::schema::types::{Type, TypePtr};

use crate::nested::{is_nested, nested_cell, nested_type_name};
use crate::{ParquetError, WindowRows};

/// How many leading bytes of a binary value are shown as hex.
pub const BINARY_PREVIEW_BYTES: usize = 16;

/// How many characters of a nested value's JSON are shown.
pub const NESTED_DISPLAY_CHARS: usize = 256;

const NULL_DISPLAY: &str = "NULL";

/// One value, with the text a table shows for it.
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    pub kind: CellKind,
    pub display: Arc<str>,
}

/// What a cell holds, as far as a table needs to know to align, sort or copy
/// it. The value itself is always in [`Cell::display`]; the numeric and
/// boolean variants repeat it in native form.
#[derive(Debug, Clone, PartialEq)]
pub enum CellKind {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    /// Strings, and every value whose exact form is its text: decimals,
    /// UInt64 values above `i64::MAX`, dates, times, timestamps, intervals,
    /// UUIDs and unsupported values.
    Text,
    /// A hex preview of the first [`BINARY_PREVIEW_BYTES`] bytes.
    Binary {
        length: usize,
    },
    /// A list, struct or map, shown as JSON text.
    Nested,
}

/// The coarse kind of a column, matching the kinds charts and alignment use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    /// Timestamps and dates.
    Timestamp,
    Float,
    Integer,
    Text,
    /// Booleans, binary, nested and unsupported columns.
    Unknown,
}

/// A column header: the field name and its Parquet type as a reader of the
/// file would name it, such as `TIMESTAMP(MICROS, UTC)` or `DECIMAL(18,2)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDisplay {
    pub name: Arc<str>,
    pub type_name: Arc<str>,
    pub kind: ColumnKind,
}

/// The cells of a window: one [`ColumnDisplay`] per selected column and, in
/// row-major order, one row of cells per row of the window.
#[derive(Debug, Clone, PartialEq)]
pub struct CellPage {
    pub columns: Vec<ColumnDisplay>,
    pub rows: Vec<Vec<Cell>>,
}

/// Converts every value of `rows` into a [`Cell`].
///
/// Display rules:
/// - integers are integer cells, except a UInt64 above `i64::MAX`, which is a
///   text cell with its exact digits;
/// - floats (Float16 included) are float cells; an integral value below
///   `1e15` in magnitude shows one decimal (`2.0`), as the data table shows
///   driver floats, and any other value the shortest text of its own width;
/// - decimals are text with the scale applied (`123.45`), never rounded;
/// - dates are `YYYY-MM-DD`, times `HH:MM:SS` with as many fraction digits as
///   the unit holds (3, 6 or 9, zeros kept), timestamps both, separated by a
///   space;
/// - a UTC-adjusted timestamp is shown in UTC and ends in `Z`; one tagged with
///   a named zone or a non-zero offset is shown in UTC too, followed by the
///   zone in brackets (`2024-05-01 12:00:00.000000Z [Europe/Berlin]`); a naive
///   timestamp and every INT96 value are shown as written, without a zone;
/// - binary values show the hex of their first [`BINARY_PREVIEW_BYTES`] bytes
///   and their length (`0x0a1b… (40 bytes)`), and a 16-byte UUID its
///   canonical hyphenated form;
/// - lists, structs and maps are JSON, cut after [`NESTED_DISPLAY_CHARS`]
///   characters, with every leaf following the rules above;
/// - VARIANT, GEOMETRY, GEOGRAPHY and any type without a rule are shown as
///   `<unsupported: TYPE>`.
pub fn cells_of(rows: &WindowRows) -> Result<CellPage, ParquetError> {
    let fields = rows.schema().fields();
    let parquet_fields = rows.parquet_fields();

    if fields.len() != parquet_fields.len() {
        return Err(ParquetError::malformed(format!(
            "the window has {} Arrow fields but {} Parquet fields",
            fields.len(),
            parquet_fields.len()
        )));
    }

    let plans: Vec<ColumnPlan> = fields
        .iter()
        .zip(parquet_fields)
        .map(|(field, parquet_field)| ColumnPlan::new(field, parquet_field))
        .collect();

    let columns = plans.iter().map(|plan| plan.display.clone()).collect();

    let mut page_rows: Vec<Vec<Cell>> = Vec::new();

    for batch in rows.batches() {
        if batch.num_columns() != plans.len() {
            return Err(ParquetError::malformed(format!(
                "a decoded batch has {} columns instead of {}",
                batch.num_columns(),
                plans.len()
            )));
        }

        let first_row = page_rows.len();
        page_rows.extend((0..batch.num_rows()).map(|_| Vec::with_capacity(plans.len())));

        for (plan, array) in plans.iter().zip(batch.columns()) {
            let batch_rows = page_rows.iter_mut().skip(first_row);

            for (row, row_cells) in batch_rows.enumerate() {
                row_cells.push(plan.cell(array.as_ref(), row)?);
            }
        }
    }

    Ok(CellPage {
        columns,
        rows: page_rows,
    })
}

/// What the Parquet type of a column adds to its Arrow type.
#[derive(Debug, Clone, Default)]
pub(crate) struct LeafHints {
    /// A UUID logical type: the Arrow type is a plain 16-byte binary.
    uuid: bool,
    /// An INT96 column: the Arrow type is a nanosecond timestamp, possibly
    /// tagged with a zone the INT96 value itself never had.
    pub(crate) int96: bool,
    /// A logical type this crate does not decode, by its Parquet name.
    pub(crate) unsupported: Option<&'static str>,
}

struct ColumnPlan {
    display: ColumnDisplay,
    parquet_type: TypePtr,
    hints: LeafHints,
    nested: bool,
}

impl ColumnPlan {
    fn new(field: &Field, parquet_field: &TypePtr) -> Self {
        let hints = leaf_hints(parquet_field);
        let data_type = field.data_type();

        let display = ColumnDisplay {
            name: field.name().as_str().into(),
            type_name: column_type_name(parquet_field, data_type),
            kind: column_kind(data_type, &hints),
        };

        Self {
            display,
            parquet_type: TypePtr::clone(parquet_field),
            nested: is_nested(data_type) && hints.unsupported.is_none(),
            hints,
        }
    }

    fn cell(&self, array: &dyn Array, row: usize) -> Result<Cell, ParquetError> {
        if !self.nested {
            return scalar_cell(array, row, &self.hints);
        }

        if array.is_null(row) {
            return Ok(null_cell());
        }

        nested_cell(array, row, Some(&self.parquet_type))
    }
}

/// A nested type spells out every field, so a wide STRUCT or MAP type name can
/// run as long as a nested value and has no more room in a header than a value
/// has in a cell; it is cut at the same limit.
pub(crate) fn column_type_name(parquet_type: &Type, data_type: &DataType) -> Arc<str> {
    truncate_chars(type_name(parquet_type, data_type), NESTED_DISPLAY_CHARS).into()
}

/// `text` cut after `limit` characters and marked with an ellipsis.
fn truncate_chars(text: String, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((cut, _)) => {
            let mut truncated = text;
            truncated.truncate(cut);
            truncated.push('…');
            truncated
        }
        None => text,
    }
}

pub(crate) fn leaf_hints(parquet_type: &Type) -> LeafHints {
    let basic_info = parquet_type.get_basic_info();
    let logical_type = basic_info.logical_type_ref();

    let int96 = matches!(
        parquet_type,
        Type::PrimitiveType {
            physical_type: PhysicalType::INT96,
            ..
        }
    );

    LeafHints {
        uuid: matches!(logical_type, Some(LogicalType::Uuid)),
        int96,
        unsupported: logical_type.and_then(unsupported_logical_type),
    }
}

fn unsupported_logical_type(logical_type: &LogicalType) -> Option<&'static str> {
    match logical_type {
        LogicalType::Variant(_) => Some("VARIANT"),
        LogicalType::Geometry(_) => Some("GEOMETRY"),
        LogicalType::Geography(_) => Some("GEOGRAPHY"),
        _ => None,
    }
}

fn column_kind(data_type: &DataType, hints: &LeafHints) -> ColumnKind {
    if hints.unsupported.is_some() {
        return ColumnKind::Unknown;
    }

    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Duration(_) => ColumnKind::Integer,

        DataType::Float16 | DataType::Float32 | DataType::Float64 => ColumnKind::Float,

        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Interval(_) => ColumnKind::Text,

        DataType::FixedSizeBinary(16) if hints.uuid => ColumnKind::Text,

        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => ColumnKind::Timestamp,

        DataType::Dictionary(_, value_type) => column_kind(value_type, hints),

        _ => ColumnKind::Unknown,
    }
}

/// The Parquet type of a column as a reader of the file would name it. INT96
/// and legacy INTERVAL columns say what the Arrow reader makes of them.
pub(crate) fn type_name(parquet_type: &Type, data_type: &DataType) -> String {
    let Type::PrimitiveType {
        basic_info,
        physical_type,
        type_length,
        scale,
        precision,
    } = parquet_type
    else {
        return nested_type_name(Some(parquet_type), data_type);
    };

    if *physical_type == PhysicalType::INT96 {
        return "INT96".to_string();
    }

    if let Some(name) = basic_info.logical_type_ref().and_then(logical_type_name) {
        return name;
    }

    match basic_info.converted_type() {
        // Units Parquet has no annotation for are stored as plain integers,
        // and only the embedded Arrow schema says what they hold.
        ConvertedType::NONE => match data_type {
            DataType::Duration(unit) => format!("DURATION({})", arrow_unit_name(*unit)),
            DataType::Timestamp(unit, None) => format!("TIMESTAMP({})", arrow_unit_name(*unit)),
            DataType::Timestamp(unit, Some(_)) => {
                format!("TIMESTAMP({}, UTC)", arrow_unit_name(*unit))
            }
            DataType::Time32(unit) => format!("TIME({})", arrow_unit_name(*unit)),
            _ => physical_type_name(*physical_type, *type_length),
        },

        ConvertedType::INTERVAL => match data_type {
            DataType::Interval(IntervalUnit::DayTime) => "INTERVAL (months not read)".to_string(),
            DataType::Interval(IntervalUnit::YearMonth) => {
                "INTERVAL (days and milliseconds not read)".to_string()
            }
            _ => "INTERVAL".to_string(),
        },

        ConvertedType::DECIMAL => format!("DECIMAL({precision},{scale})"),
        ConvertedType::UTF8 => "STRING".to_string(),
        converted_type => converted_type.to_string(),
    }
}

fn logical_type_name(logical_type: &LogicalType) -> Option<String> {
    let name = match logical_type {
        LogicalType::Integer(integer) => {
            let sign = if integer.is_signed { "" } else { "U" };
            format!("{sign}INT{}", integer.bit_width)
        }
        LogicalType::Decimal(decimal) => {
            format!("DECIMAL({},{})", decimal.precision, decimal.scale)
        }
        LogicalType::String => "STRING".to_string(),
        LogicalType::Enum => "ENUM".to_string(),
        LogicalType::Json => "JSON".to_string(),
        LogicalType::Bson => "BSON".to_string(),
        LogicalType::Uuid => "UUID".to_string(),
        LogicalType::Float16 => "FLOAT16".to_string(),
        LogicalType::Date => "DATE".to_string(),
        LogicalType::Unknown => "UNKNOWN".to_string(),
        LogicalType::Time(time) => temporal_type_name("TIME", time),
        LogicalType::Timestamp(timestamp) => temporal_type_name("TIMESTAMP", timestamp),
        other => match unsupported_logical_type(other) {
            Some(name) => format!("{name} (unsupported)"),
            None => return None,
        },
    };

    Some(name)
}

fn temporal_type_name(prefix: &str, temporal: &TimestampType) -> String {
    let unit = match temporal.unit {
        ParquetTimeUnit::MILLIS => "MILLIS",
        ParquetTimeUnit::MICROS => "MICROS",
        ParquetTimeUnit::NANOS => "NANOS",
    };

    if temporal.is_adjusted_to_u_t_c {
        format!("{prefix}({unit}, UTC)")
    } else {
        format!("{prefix}({unit})")
    }
}

fn physical_type_name(physical_type: PhysicalType, type_length: i32) -> String {
    match physical_type {
        PhysicalType::BOOLEAN => "BOOLEAN".to_string(),
        PhysicalType::INT32 => "INT32".to_string(),
        PhysicalType::INT64 => "INT64".to_string(),
        PhysicalType::INT96 => "INT96".to_string(),
        PhysicalType::FLOAT => "FLOAT".to_string(),
        PhysicalType::DOUBLE => "DOUBLE".to_string(),
        PhysicalType::BYTE_ARRAY => "BYTE_ARRAY".to_string(),
        PhysicalType::FIXED_LEN_BYTE_ARRAY => format!("FIXED_LEN_BYTE_ARRAY({type_length})"),
    }
}

fn arrow_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "SECONDS",
        TimeUnit::Millisecond => "MILLIS",
        TimeUnit::Microsecond => "MICROS",
        TimeUnit::Nanosecond => "NANOS",
    }
}

/// The cell for `row` of `array`, a scalar column or a dictionary of one.
pub(crate) fn scalar_cell(
    array: &dyn Array,
    row: usize,
    hints: &LeafHints,
) -> Result<Cell, ParquetError> {
    let data_type = array.data_type();

    if array.is_null(row) || *data_type == DataType::Null {
        return Ok(null_cell());
    }

    if let Some(name) = hints.unsupported {
        return Ok(text_cell(format!("<unsupported: {name}>")));
    }

    let cell = match data_type {
        DataType::Boolean => {
            let value = array
                .as_boolean_opt()
                .ok_or_else(|| mismatch(data_type))?
                .value(row);

            Cell {
                kind: CellKind::Bool(value),
                display: if value { "true" } else { "false" }.into(),
            }
        }

        DataType::Int8 => integer_cell::<Int8Type>(array, row)?,
        DataType::Int16 => integer_cell::<Int16Type>(array, row)?,
        DataType::Int32 => integer_cell::<Int32Type>(array, row)?,
        DataType::Int64 => integer_cell::<Int64Type>(array, row)?,
        DataType::UInt8 => integer_cell::<UInt8Type>(array, row)?,
        DataType::UInt16 => integer_cell::<UInt16Type>(array, row)?,
        DataType::UInt32 => integer_cell::<UInt32Type>(array, row)?,
        DataType::UInt64 => unsigned_64_cell(primitive::<UInt64Type>(array)?.value(row)),

        DataType::Duration(TimeUnit::Second) => integer_cell::<DurationSecondType>(array, row)?,
        DataType::Duration(TimeUnit::Millisecond) => {
            integer_cell::<DurationMillisecondType>(array, row)?
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            integer_cell::<DurationMicrosecondType>(array, row)?
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            integer_cell::<DurationNanosecondType>(array, row)?
        }

        DataType::Float16 => {
            let value = primitive::<Float16Type>(array)?.value(row).to_f32();
            float_cell(f64::from(value), float_32_display(value))
        }
        DataType::Float32 => {
            let value = primitive::<Float32Type>(array)?.value(row);
            float_cell(f64::from(value), float_32_display(value))
        }
        DataType::Float64 => {
            let value = primitive::<Float64Type>(array)?.value(row);
            float_cell(value, float_64_display(value))
        }

        DataType::Decimal32(..) => decimal_cell::<Decimal32Type>(array, row)?,
        DataType::Decimal64(..) => decimal_cell::<Decimal64Type>(array, row)?,
        DataType::Decimal128(..) => decimal_cell::<Decimal128Type>(array, row)?,
        DataType::Decimal256(..) => decimal_cell::<Decimal256Type>(array, row)?,

        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => byte_cell(array, row, hints)?,

        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(..)
        | DataType::Interval(_) => temporal_cell(array, row, hints)?,

        DataType::Dictionary(..) => {
            let dictionary = array
                .as_any_dictionary_opt()
                .ok_or_else(|| mismatch(data_type))?;
            let values = dictionary.values();
            let key = dictionary_key(dictionary.keys(), row)?;

            if key >= values.len() {
                return Err(ParquetError::malformed(format!(
                    "a dictionary key ({key}) points past its {} values",
                    values.len()
                )));
            }

            scalar_cell(values.as_ref(), key, hints)?
        }

        other => text_cell(format!("<unsupported: {other}>")),
    };

    Ok(cell)
}

/// The cell of a string or binary value.
fn byte_cell(array: &dyn Array, row: usize, hints: &LeafHints) -> Result<Cell, ParquetError> {
    let data_type = array.data_type();

    let cell = match data_type {
        DataType::Utf8 => text_cell(string_at::<i32>(array, row)?),
        DataType::LargeUtf8 => text_cell(string_at::<i64>(array, row)?),
        DataType::Utf8View => text_cell(
            array
                .as_string_view_opt()
                .ok_or_else(|| mismatch(data_type))?
                .value(row),
        ),

        DataType::Binary => binary_cell(binary_at::<i32>(array, row)?, hints),
        DataType::LargeBinary => binary_cell(binary_at::<i64>(array, row)?, hints),
        DataType::BinaryView => binary_cell(
            array
                .as_binary_view_opt()
                .ok_or_else(|| mismatch(data_type))?
                .value(row),
            hints,
        ),
        DataType::FixedSizeBinary(_) => binary_cell(
            array
                .as_fixed_size_binary_opt()
                .ok_or_else(|| mismatch(data_type))?
                .value(row),
            hints,
        ),

        other => text_cell(format!("<unsupported: {other}>")),
    };

    Ok(cell)
}

/// The cell of a date, time, timestamp or interval value.
fn temporal_cell(array: &dyn Array, row: usize, hints: &LeafHints) -> Result<Cell, ParquetError> {
    let data_type = array.data_type();

    let cell = match data_type {
        DataType::Date32 => {
            let days = primitive::<Date32Type>(array)?.value(row);
            text_cell(date_text(i64::from(days)))
        }
        DataType::Date64 => {
            let millis = primitive::<Date64Type>(array)?.value(row);
            text_cell(date_text(millis.div_euclid(MILLIS_PER_DAY)))
        }

        DataType::Time32(TimeUnit::Second) => {
            let value = primitive::<Time32SecondType>(array)?.value(row);
            text_cell(time_of_day_text(i64::from(value), TimeUnit::Second))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let value = primitive::<Time32MillisecondType>(array)?.value(row);
            text_cell(time_of_day_text(i64::from(value), TimeUnit::Millisecond))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let value = primitive::<Time64MicrosecondType>(array)?.value(row);
            text_cell(time_of_day_text(value, TimeUnit::Microsecond))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let value = primitive::<Time64NanosecondType>(array)?.value(row);
            text_cell(time_of_day_text(value, TimeUnit::Nanosecond))
        }

        DataType::Timestamp(unit, zone) => {
            let value = timestamp_value(array, row, *unit)?;
            let zone = if hints.int96 { None } else { zone.as_deref() };

            text_cell(timestamp_text(value, *unit, zone))
        }

        DataType::Interval(IntervalUnit::YearMonth) => {
            let months = primitive::<IntervalYearMonthType>(array)?.value(row);
            text_cell(format!("{months} months"))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let interval = primitive::<IntervalDayTimeType>(array)?.value(row);
            text_cell(format!(
                "{} days {} ms",
                interval.days, interval.milliseconds
            ))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let interval = primitive::<IntervalMonthDayNanoType>(array)?.value(row);
            text_cell(format!(
                "{} months {} days {} ns",
                interval.months, interval.days, interval.nanoseconds
            ))
        }

        other => text_cell(format!("<unsupported: {other}>")),
    };

    Ok(cell)
}

const MILLIS_PER_DAY: i64 = 86_400_000;
const SECONDS_PER_DAY: i64 = 86_400;

fn null_cell() -> Cell {
    Cell {
        kind: CellKind::Null,
        display: NULL_DISPLAY.into(),
    }
}

fn text_cell(text: impl Into<Arc<str>>) -> Cell {
    Cell {
        kind: CellKind::Text,
        display: text.into(),
    }
}

fn mismatch(data_type: &DataType) -> ParquetError {
    ParquetError::malformed(format!(
        "a decoded column does not hold the {data_type} values its schema declares"
    ))
}

fn primitive<T: ArrowPrimitiveType>(array: &dyn Array) -> Result<&PrimitiveArray<T>, ParquetError> {
    array
        .as_primitive_opt::<T>()
        .ok_or_else(|| mismatch(array.data_type()))
}

fn integer_cell<T>(array: &dyn Array, row: usize) -> Result<Cell, ParquetError>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    let value: i64 = primitive::<T>(array)?.value(row).into();

    Ok(Cell {
        kind: CellKind::Integer(value),
        display: value.to_string().into(),
    })
}

fn unsigned_64_cell(value: u64) -> Cell {
    match i64::try_from(value) {
        Ok(signed) => Cell {
            kind: CellKind::Integer(signed),
            display: signed.to_string().into(),
        },
        Err(_) => text_cell(value.to_string()),
    }
}

fn float_cell(value: f64, display: String) -> Cell {
    Cell {
        kind: CellKind::Float(value),
        display: display.into(),
    }
}

/// Integral values keep one decimal so a float column never reads as
/// integers; the same rule the data table applies to driver floats.
fn float_64_display(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

/// Formats at the value's own width, so a FLOAT `0.1` reads `0.1` rather than
/// the `0.10000000149011612` its exact double would print.
fn float_32_display(value: f32) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn decimal_cell<T: DecimalType>(array: &dyn Array, row: usize) -> Result<Cell, ParquetError> {
    Ok(text_cell(primitive::<T>(array)?.value_as_string(row)))
}

fn string_at<O: arrow_array::OffsetSizeTrait>(
    array: &dyn Array,
    row: usize,
) -> Result<&str, ParquetError> {
    Ok(array
        .as_string_opt::<O>()
        .ok_or_else(|| mismatch(array.data_type()))?
        .value(row))
}

fn binary_at<O: arrow_array::OffsetSizeTrait>(
    array: &dyn Array,
    row: usize,
) -> Result<&[u8], ParquetError> {
    Ok(array
        .as_binary_opt::<O>()
        .ok_or_else(|| mismatch(array.data_type()))?
        .value(row))
}

fn binary_cell(bytes: &[u8], hints: &LeafHints) -> Cell {
    if hints.uuid
        && let Ok(uuid) = uuid::Uuid::from_slice(bytes)
    {
        return text_cell(uuid.hyphenated().to_string());
    }

    Cell {
        kind: CellKind::Binary {
            length: bytes.len(),
        },
        display: hex_preview(bytes).into(),
    }
}

/// `0x` and the hex of the first [`BINARY_PREVIEW_BYTES`] bytes, an ellipsis
/// when there are more, then the full length.
fn hex_preview(bytes: &[u8]) -> String {
    let shown = bytes.get(..BINARY_PREVIEW_BYTES).unwrap_or(bytes);

    let mut text = String::with_capacity(2 + shown.len() * 2 + 20);
    text.push_str("0x");

    for byte in shown {
        for nibble in [byte >> 4, byte & 0x0f] {
            if let Some(digit) = char::from_digit(u32::from(nibble), 16) {
                text.push(digit);
            }
        }
    }

    if bytes.len() > shown.len() {
        text.push('…');
    }

    let unit = if bytes.len() == 1 { "byte" } else { "bytes" };
    text.push_str(&format!(" ({} {unit})", bytes.len()));

    text
}

fn dictionary_key(keys: &dyn Array, row: usize) -> Result<usize, ParquetError> {
    match keys.data_type() {
        DataType::Int8 => key_at::<Int8Type>(keys, row),
        DataType::Int16 => key_at::<Int16Type>(keys, row),
        DataType::Int32 => key_at::<Int32Type>(keys, row),
        DataType::Int64 => key_at::<Int64Type>(keys, row),
        DataType::UInt8 => key_at::<UInt8Type>(keys, row),
        DataType::UInt16 => key_at::<UInt16Type>(keys, row),
        DataType::UInt32 => key_at::<UInt32Type>(keys, row),
        DataType::UInt64 => key_at::<UInt64Type>(keys, row),
        other => Err(ParquetError::malformed(format!(
            "a dictionary has {other} keys, which are not integers"
        ))),
    }
}

fn key_at<T>(keys: &dyn Array, row: usize) -> Result<usize, ParquetError>
where
    T: ArrowDictionaryKeyType,
    usize: TryFrom<T::Native>,
{
    let key = primitive::<T>(keys)?.value(row);

    usize::try_from(key)
        .map_err(|_| ParquetError::malformed("a dictionary key is negative or too large"))
}

fn units_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn fraction_digits(unit: TimeUnit) -> usize {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// `.` and the sub-second part with every digit the unit holds, zeros kept,
/// so values of one column line up; nothing for whole-second units.
fn fraction_suffix(fraction: i64, unit: TimeUnit) -> String {
    match fraction_digits(unit) {
        0 => String::new(),
        digits => format!(".{fraction:0digits$}"),
    }
}

fn out_of_range_text(value: i64, unit: TimeUnit) -> String {
    format!("<out of range: {value} {}>", arrow_unit_name(unit))
}

fn date_text(days: i64) -> String {
    match days
        .checked_mul(SECONDS_PER_DAY)
        .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
    {
        Some(date_time) => date_time.format("%Y-%m-%d").to_string(),
        None => format!("<out of range: {days} days>"),
    }
}

fn time_of_day_text(value: i64, unit: TimeUnit) -> String {
    let per_second = units_per_second(unit);

    if value < 0 || value / per_second >= SECONDS_PER_DAY {
        return out_of_range_text(value, unit);
    }

    let seconds = value / per_second;

    format!(
        "{:02}:{:02}:{:02}{}",
        seconds / 3_600,
        seconds % 3_600 / 60,
        seconds % 60,
        fraction_suffix(value % per_second, unit)
    )
}

fn timestamp_value(array: &dyn Array, row: usize, unit: TimeUnit) -> Result<i64, ParquetError> {
    Ok(match unit {
        TimeUnit::Second => primitive::<TimestampSecondType>(array)?.value(row),
        TimeUnit::Millisecond => primitive::<TimestampMillisecondType>(array)?.value(row),
        TimeUnit::Microsecond => primitive::<TimestampMicrosecondType>(array)?.value(row),
        TimeUnit::Nanosecond => primitive::<TimestampNanosecondType>(array)?.value(row),
    })
}

/// A timestamp of `unit`s since the epoch. Without a zone it is shown as
/// written; with one it is an instant, shown in UTC and marked `Z`, followed
/// by the zone in brackets unless the zone is UTC itself. Named zones are not
/// converted to local time, because that needs a time-zone database.
fn timestamp_text(value: i64, unit: TimeUnit, zone: Option<&str>) -> String {
    let per_second = units_per_second(unit);
    let seconds = value.div_euclid(per_second);
    let fraction = value.rem_euclid(per_second);

    let Some(date_time) = chrono::DateTime::from_timestamp(seconds, 0) else {
        return out_of_range_text(value, unit);
    };

    let mut text = format!(
        "{}{}",
        date_time.format("%Y-%m-%d %H:%M:%S"),
        fraction_suffix(fraction, unit)
    );

    if let Some(zone) = zone {
        text.push('Z');

        if !is_utc_zone(zone) {
            text.push_str(&format!(" [{zone}]"));
        }
    }

    text
}

fn is_utc_zone(zone: &str) -> bool {
    matches!(
        zone,
        "UTC" | "utc" | "Z" | "Etc/UTC" | "+00:00" | "-00:00" | "+0000" | "+00"
    )
}
