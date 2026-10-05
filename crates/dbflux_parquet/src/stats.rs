//! Per-column statistics read from the footer and page index alone; no data
//! page is read.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrowNativeTypeOp, downcast_primitive_array};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::basic::SortOrder;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::schema::types::Type;

use crate::cells::{CellKind, column_type_name, leaf_hints, scalar_cell};
use crate::decode::leaf_roots;
use crate::{ParquetError, ParquetFile};

/// The codec reported for a field whose chunks use more than one codec.
pub const MIXED_CODECS: &str = "MIXED";

/// What the footer says about one top-level field, summed over every leaf
/// column chunk of the field in every row group.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub name: Arc<str>,
    /// The same type name a column header shows for the field.
    pub type_name: Arc<str>,
    /// The codec of the field's chunks, such as `SNAPPY`; [`MIXED_CODECS`]
    /// when its leaves or row groups use more than one, and `None` when the
    /// file has no row groups.
    pub codec: Option<Arc<str>>,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    /// The values the footer counts for the field's leaf chunks, nulls
    /// included. For a primitive field it is its row count; for a nested
    /// field it is the sum over its leaves, where every list element, empty
    /// list and null at any level is one value.
    pub value_count: u64,
    /// The nulls of a primitive field. `None` when any row group has no null
    /// count, when the file has no row groups, and for every nested field,
    /// whose leaf null counts do not say how many of its rows are null.
    pub null_count: Option<u64>,
    /// The distinct values of a primitive field, known only when the file has
    /// one row group whose chunk records it: counts of separate row groups
    /// cannot be added.
    pub distinct_count: Option<u64>,
    /// The smallest and largest value of a primitive field; see
    /// [`column_statistics`] for when it is `None`.
    pub range: Option<ValueRange>,
    /// Whether any chunk of the field starts with a dictionary page.
    pub has_dictionary: bool,
    /// Whether every chunk of the field has both a column index and an
    /// offset index. False for a file with no row groups.
    pub has_page_index: bool,
}

/// The smallest and largest value of a column, shown the way a cell shows
/// the same value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueRange {
    pub min: Arc<str>,
    pub max: Arc<str>,
    /// False when the writer stored a bound that is not a value of the
    /// column, such as a string cut to the statistics length limit.
    pub exact: bool,
}

/// The totals of a whole file, from its footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStatistics {
    pub row_count: u64,
    pub row_group_count: usize,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    /// The writer the footer names, such as `parquet-rs version 59.3.0`.
    pub created_by: Option<Arc<str>>,
}

/// One entry per top-level field of `file`, in schema order, built from the
/// footer and page index.
///
/// A range is given only for a primitive field whose type has a defined sort
/// order, whose values are not binary and not INT96, and whose every row
/// group stores a min and a max. A row group the footer shows to hold only
/// nulls has no min or max and is skipped instead. The bounds are the values
/// of the row groups holding the overall min and max, and are exact only when
/// both of those row groups mark them exact.
pub fn column_statistics(file: &ParquetFile) -> Result<Vec<ColumnStatistics>, ParquetError> {
    let metadata = file.metadata();
    let schema_descriptor = metadata.file_metadata().schema_descr();
    let root_fields = schema_descriptor.root_schema().get_fields();
    let roots = leaf_roots(schema_descriptor);

    let column_count = file.schema().fields().len().min(root_fields.len());

    let mut statistics = Vec::with_capacity(column_count);

    for (root, (field, parquet_root)) in file.schema().fields().iter().zip(root_fields).enumerate()
    {
        let leaves: Vec<usize> = roots
            .iter()
            .enumerate()
            .filter(|(_, leaf_root)| **leaf_root == root)
            .map(|(leaf, _)| leaf)
            .collect();

        let chunks = leaf_chunks(metadata, &leaves)?;

        let primitive_leaf = match (parquet_root.as_ref(), leaves.as_slice()) {
            (Type::PrimitiveType { .. }, [leaf]) => Some(*leaf),
            _ => None,
        };

        let (null_count, distinct_count, range) = match primitive_leaf {
            Some(leaf) => (
                primitive_null_count(&chunks),
                single_distinct_count(&chunks),
                value_range(file, leaf, field.as_ref(), parquet_root)?,
            ),
            None => (None, None, None),
        };

        statistics.push(ColumnStatistics {
            name: field.name().as_str().into(),
            type_name: column_type_name(parquet_root, field.data_type()),
            codec: codec_name(&chunks),
            compressed_bytes: sum_sizes(&chunks, ColumnChunkMetaData::compressed_size)?,
            uncompressed_bytes: sum_sizes(&chunks, ColumnChunkMetaData::uncompressed_size)?,
            value_count: sum_sizes(&chunks, ColumnChunkMetaData::num_values)?,
            null_count,
            distinct_count,
            range,
            has_dictionary: chunks
                .iter()
                .any(|chunk| chunk.dictionary_page_offset().is_some()),
            has_page_index: has_page_index(metadata, &leaves),
        });
    }

    Ok(statistics)
}

/// The totals of `file`, summed over every column chunk in the footer.
pub fn file_statistics(file: &ParquetFile) -> Result<FileStatistics, ParquetError> {
    let metadata = file.metadata();

    let chunks: Vec<&ColumnChunkMetaData> = metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .collect();

    Ok(FileStatistics {
        row_count: file.row_count(),
        row_group_count: file.row_group_count(),
        compressed_bytes: sum_sizes(&chunks, ColumnChunkMetaData::compressed_size)?,
        uncompressed_bytes: sum_sizes(&chunks, ColumnChunkMetaData::uncompressed_size)?,
        created_by: metadata.file_metadata().created_by().map(Arc::from),
    })
}

/// The chunks of `leaves` in every row group, row group by row group.
fn leaf_chunks<'a>(
    metadata: &'a ParquetMetaData,
    leaves: &[usize],
) -> Result<Vec<&'a ColumnChunkMetaData>, ParquetError> {
    let mut chunks = Vec::with_capacity(metadata.num_row_groups() * leaves.len());

    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        for &leaf in leaves {
            let chunk = row_group.columns().get(leaf).ok_or_else(|| {
                ParquetError::malformed(format!(
                    "row group {row_group_index} has no chunk for column {leaf}"
                ))
            })?;

            chunks.push(chunk);
        }
    }

    Ok(chunks)
}

fn sum_sizes(
    chunks: &[&ColumnChunkMetaData],
    size: fn(&ColumnChunkMetaData) -> i64,
) -> Result<u64, ParquetError> {
    chunks.iter().try_fold(0u64, |total, chunk| {
        let value = size(chunk);

        u64::try_from(value)
            .ok()
            .and_then(|value| total.checked_add(value))
            .ok_or_else(|| {
                ParquetError::malformed(format!(
                    "column `{}` declares an invalid size or count ({value})",
                    chunk.column_path()
                ))
            })
    })
}

fn codec_name(chunks: &[&ColumnChunkMetaData]) -> Option<Arc<str>> {
    let first = chunks.first()?.compression_codec();

    if chunks
        .iter()
        .all(|chunk| chunk.compression_codec() == first)
    {
        Some(first.to_string().into())
    } else {
        Some(MIXED_CODECS.into())
    }
}

fn primitive_null_count(chunks: &[&ColumnChunkMetaData]) -> Option<u64> {
    if chunks.is_empty() {
        return None;
    }

    chunks.iter().try_fold(0u64, |total, chunk| {
        let nulls = chunk.statistics()?.null_count_opt()?;

        total.checked_add(nulls)
    })
}

fn single_distinct_count(chunks: &[&ColumnChunkMetaData]) -> Option<u64> {
    match chunks {
        [chunk] => chunk.statistics()?.distinct_count_opt(),
        _ => None,
    }
}

fn has_page_index(metadata: &ParquetMetaData, leaves: &[usize]) -> bool {
    let (Some(column_index), Some(offset_index)) =
        (metadata.column_index(), metadata.offset_index())
    else {
        return false;
    };

    metadata.num_row_groups() > 0
        && (0..metadata.num_row_groups()).all(|row_group| {
            leaves.iter().all(|&leaf| {
                let has_column_index = column_index
                    .get(row_group)
                    .and_then(|columns| columns.get(leaf))
                    .is_some_and(|index| !matches!(index, ColumnIndexMetaData::NONE));

                let has_offset_index = offset_index
                    .get(row_group)
                    .and_then(|columns| columns.get(leaf))
                    .is_some();

                has_column_index && has_offset_index
            })
        })
}

/// The range of the primitive field at `leaf`, from each row group's min and
/// max decoded to the field's Arrow type.
fn value_range(
    file: &ParquetFile,
    leaf: usize,
    field: &arrow_schema::Field,
    parquet_root: &Type,
) -> Result<Option<ValueRange>, ParquetError> {
    let metadata = file.metadata();
    let schema_descriptor = metadata.file_metadata().schema_descr();
    let row_groups = metadata.row_groups();
    let hints = leaf_hints(parquet_root);

    if row_groups.is_empty() || hints.int96 || hints.unsupported.is_some() {
        return Ok(None);
    }

    if schema_descriptor.column(leaf).sort_order() == SortOrder::UNDEFINED {
        return Ok(None);
    }

    let converter = StatisticsConverter::from_column_index(leaf, field, schema_descriptor)?;

    let mins = converter.row_group_mins(row_groups)?;
    let maxes = converter.row_group_maxes(row_groups)?;
    let min_exact = converter.row_group_is_min_value_exact(row_groups)?;
    let max_exact = converter.row_group_is_max_value_exact(row_groups)?;

    let count = row_groups.len();

    if mins.len() != count
        || maxes.len() != count
        || min_exact.len() != count
        || max_exact.len() != count
    {
        return Ok(None);
    }

    let mut min_row_group: Option<usize> = None;
    let mut max_row_group: Option<usize> = None;

    for (index, row_group) in row_groups.iter().enumerate() {
        if mins.is_null(index) || maxes.is_null(index) {
            let only_nulls = row_group.columns().get(leaf).is_some_and(|chunk| {
                let null_count = chunk.statistics().and_then(|stats| stats.null_count_opt());

                null_count.is_some() && null_count == u64::try_from(row_group.num_rows()).ok()
            });

            if only_nulls {
                continue;
            }

            return Ok(None);
        }

        let Some(new_min) = extends_bound(mins.as_ref(), index, min_row_group, Ordering::Less)
        else {
            return Ok(None);
        };

        let Some(new_max) = extends_bound(maxes.as_ref(), index, max_row_group, Ordering::Greater)
        else {
            return Ok(None);
        };

        if new_min {
            min_row_group = Some(index);
        }

        if new_max {
            max_row_group = Some(index);
        }
    }

    let (Some(min_row_group), Some(max_row_group)) = (min_row_group, max_row_group) else {
        return Ok(None);
    };

    let min = scalar_cell(mins.as_ref(), min_row_group, &hints)?;
    let max = scalar_cell(maxes.as_ref(), max_row_group, &hints)?;

    let displayable = |kind: &CellKind| !matches!(kind, CellKind::Null | CellKind::Binary { .. });

    if !displayable(&min.kind) || !displayable(&max.kind) {
        return Ok(None);
    }

    let exact = min_exact.is_valid(min_row_group)
        && min_exact.value(min_row_group)
        && max_exact.is_valid(max_row_group)
        && max_exact.value(max_row_group);

    Ok(Some(ValueRange {
        min: min.display,
        max: max.display,
        exact,
    }))
}

/// Whether entry `index` of `bounds` goes past the bound held so far, in the
/// direction of `outward`; `None` when the type cannot be ordered.
fn extends_bound(
    bounds: &dyn Array,
    index: usize,
    current: Option<usize>,
    outward: Ordering,
) -> Option<bool> {
    match current {
        Some(current) => Some(compare_entries(bounds, index, current)? == outward),
        None => Some(true),
    }
}

/// Orders two entries of a statistics array, or `None` for a type this crate
/// does not order. Strings order by their bytes, as Parquet does.
fn compare_entries(array: &dyn Array, left: usize, right: usize) -> Option<Ordering> {
    downcast_primitive_array!(
        array => Some(array.value(left).compare(array.value(right))),

        DataType::Boolean => {
            let array = array.as_boolean_opt()?;
            Some(array.value(left).cmp(&array.value(right)))
        }

        DataType::Utf8 => {
            let array = array.as_string_opt::<i32>()?;
            Some(array.value(left).cmp(array.value(right)))
        }

        DataType::LargeUtf8 => {
            let array = array.as_string_opt::<i64>()?;
            Some(array.value(left).cmp(array.value(right)))
        }

        DataType::Utf8View => {
            let array = array.as_string_view_opt()?;
            Some(array.value(left).cmp(array.value(right)))
        }

        _ => None
    )
}
