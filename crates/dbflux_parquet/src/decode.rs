use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use dbflux_byte_source::ByteSource;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::schema::types::{SchemaDescriptor, TypePtr};

use crate::footer::read_exact;
use crate::window::{RowGroupSlice, row_group_slices};
use crate::{ParquetError, ParquetFile, RowWindow};

/// Largest column chunk read whole for a row window. Without an offset index
/// the pages of a window cannot be located, so any window reads every chunk it
/// touches in full; this keeps one page from costing an unbounded download.
pub const UNINDEXED_CHUNK_BUDGET: u64 = 64 * 1024 * 1024;

/// The decoded rows of a [`RowWindow`].
#[derive(Debug, Clone)]
pub struct WindowRows {
    window: RowWindow,
    schema: SchemaRef,
    /// The Parquet types of the selected top-level fields, in the same order
    /// as [`Self::schema`]. The Arrow types alone cannot tell INT96 from an
    /// INT64 timestamp, nor a UUID from any 16-byte value.
    parquet_fields: Vec<TypePtr>,
    batches: Vec<RecordBatch>,
}

impl WindowRows {
    /// The window that was read, clamped to the rows the file holds: its
    /// count is the number of rows in [`Self::batches`].
    pub fn window(&self) -> RowWindow {
        self.window
    }

    /// The selected top-level fields, in file schema order. Holds even when
    /// the window has no rows.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The rows, in file order, split into batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    pub(crate) fn parquet_fields(&self) -> &[TypePtr] {
        &self.parquet_fields
    }
}

/// Decodes `window` for the top-level fields at `columns`, indices into
/// [`ParquetFile::schema`].
///
/// Columns come back in file schema order, each once, whatever the order of
/// `columns`; a nested field is read whole. When a row group has an offset
/// index only the pages holding the window's rows are read; otherwise each
/// selected column chunk is read whole, and a chunk larger than
/// [`UNINDEXED_CHUNK_BUDGET`] is refused before anything is read. A window
/// that starts past the last row returns no rows; one that runs past it is
/// clamped.
pub fn read_window<S: ByteSource + ?Sized>(
    file: &ParquetFile,
    source: &S,
    window: RowWindow,
    columns: &[usize],
) -> Result<WindowRows, ParquetError> {
    read_window_with_budget(file, source, window, columns, UNINDEXED_CHUNK_BUDGET)
}

/// The byte ranges [`read_window`] would request for the same arguments, in
/// request order, without reading them. Over-budget chunks are listed, not
/// refused.
pub fn window_byte_ranges(
    file: &ParquetFile,
    window: RowWindow,
    columns: &[usize],
) -> Result<Vec<Range<u64>>, ParquetError> {
    let plan = plan_window(file, window, columns)?;

    Ok(plan
        .row_groups
        .into_iter()
        .flat_map(|row_group| coalesce(row_group.ranges()))
        .collect())
}

/// What [`read_window`] would read for the same arguments, row group by row
/// group, with each range attributed to the top-level column it belongs to.
pub(crate) struct WindowReadPlan {
    /// The selected top-level columns, in file schema order, each once.
    pub(crate) columns: Vec<usize>,
    /// One entry per row group the window touches.
    pub(crate) row_groups: Vec<Vec<RootRange>>,
}

/// A range the decoder asks for, before coalescing, and the top-level column
/// whose leaf chunk holds it.
pub(crate) struct RootRange {
    pub(crate) root: usize,
    pub(crate) range: Range<u64>,
}

pub(crate) fn window_read_plan(
    file: &ParquetFile,
    window: RowWindow,
    columns: &[usize],
) -> Result<WindowReadPlan, ParquetError> {
    let plan = plan_window(file, window, columns)?;

    Ok(WindowReadPlan {
        columns: plan.columns,
        row_groups: plan
            .row_groups
            .into_iter()
            .map(|row_group| row_group.ranges)
            .collect(),
    })
}

/// Merges ranges the way [`read_window`] does before it reads them.
pub(crate) fn coalesced_ranges(ranges: &[RootRange]) -> Vec<Range<u64>> {
    coalesce(ranges.iter().map(|range| range.range.clone()).collect())
}

/// The top-level field of every leaf column, by leaf index.
pub(crate) fn leaf_roots(schema_descriptor: &SchemaDescriptor) -> Vec<usize> {
    (0..schema_descriptor.num_columns())
        .map(|leaf| schema_descriptor.get_column_root_idx(leaf))
        .collect()
}

pub(crate) fn read_window_with_budget<S: ByteSource + ?Sized>(
    file: &ParquetFile,
    source: &S,
    window: RowWindow,
    columns: &[usize],
    unindexed_chunk_budget: u64,
) -> Result<WindowRows, ParquetError> {
    let plan = plan_window(file, window, columns)?;

    check_unindexed_budget(file.metadata(), &plan, unindexed_chunk_budget)?;

    if plan.row_groups.is_empty() {
        return Ok(WindowRows {
            window: plan.window,
            schema: plan.schema,
            parquet_fields: plan.parquet_fields,
            batches: Vec::new(),
        });
    }

    let file_length = source.byte_length()?;

    let reader_metadata =
        ArrowReaderMetadata::try_new(Arc::clone(file.metadata()), ArrowReaderOptions::new())?;

    let mut batches = Vec::new();

    for row_group in plan.row_groups {
        let decoder = ParquetPushDecoderBuilder::new_with_metadata(reader_metadata.clone())
            .with_row_groups(vec![row_group.slice.row_group])
            .with_projection(plan.mask.clone())
            .with_row_selection(row_group.slice.selection)
            .build()?;

        decode_row_group(decoder, source, file_length, &mut batches)?;
    }

    Ok(WindowRows {
        window: plan.window,
        schema: plan.schema,
        parquet_fields: plan.parquet_fields,
        batches,
    })
}

/// Drives one row group's decoder to the end, answering each request for
/// bytes from `source` and appending the decoded batches.
fn decode_row_group<S: ByteSource + ?Sized>(
    mut decoder: ParquetPushDecoder,
    source: &S,
    file_length: u64,
    batches: &mut Vec<RecordBatch>,
) -> Result<(), ParquetError> {
    loop {
        match decoder.try_decode()? {
            DecodeResult::NeedsData(ranges) => {
                if ranges.is_empty() {
                    return Err(ParquetError::malformed(
                        "the decoder asked for data without naming any range",
                    ));
                }

                let reads = coalesce(ranges);
                let mut buffers = Vec::with_capacity(reads.len());

                for range in &reads {
                    check_data_range(range, file_length)?;
                    buffers.push(Bytes::from(read_exact(source, range.clone())?));
                }

                decoder.push_ranges(reads, buffers)?;
            }

            DecodeResult::Data(batch) => batches.push(batch),

            DecodeResult::Finished => return Ok(()),
        }
    }
}

struct WindowPlan {
    window: RowWindow,
    columns: Vec<usize>,
    schema: SchemaRef,
    parquet_fields: Vec<TypePtr>,
    mask: ProjectionMask,
    row_groups: Vec<PlannedRowGroup>,
}

struct PlannedRowGroup {
    slice: RowGroupSlice,
    /// The ranges the decoder asks for, before coalescing.
    ranges: Vec<RootRange>,
    has_offset_index: bool,
}

impl PlannedRowGroup {
    fn ranges(&self) -> Vec<Range<u64>> {
        self.ranges
            .iter()
            .map(|range| range.range.clone())
            .collect()
    }
}

/// Validates `columns` and works out, without reading, which row groups the
/// window touches and which bytes each needs.
///
/// Every chunk range and offset-index entry the decoder will use is checked
/// here, because the decoder panics on a negative chunk offset and on an
/// offset index that does not cover a row group.
fn plan_window(
    file: &ParquetFile,
    window: RowWindow,
    columns: &[usize],
) -> Result<WindowPlan, ParquetError> {
    let metadata = file.metadata();
    let schema_descriptor = metadata.file_metadata().schema_descr();

    if columns.is_empty() {
        return Err(ParquetError::NoColumnsSelected);
    }

    let column_count = file
        .schema()
        .fields()
        .len()
        .min(schema_descriptor.root_schema().get_fields().len());

    if let Some(&index) = columns.iter().find(|&&index| index >= column_count) {
        return Err(ParquetError::ColumnOutOfRange {
            index,
            column_count,
        });
    }

    let mut ordered_columns = columns.to_vec();
    ordered_columns.sort_unstable();
    ordered_columns.dedup();

    let schema = Arc::new(
        file.schema()
            .project(&ordered_columns)
            .map_err(|error| ParquetError::malformed(error.to_string()))?,
    );

    let root_fields = schema_descriptor.root_schema().get_fields();

    let parquet_fields = ordered_columns
        .iter()
        .map(|&index| root_fields.get(index).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            ParquetError::malformed("the Parquet schema has fewer fields than the Arrow schema")
        })?;

    let mask = ProjectionMask::roots(schema_descriptor, ordered_columns.iter().copied());

    let (window, slices) = row_group_slices(metadata, window)?;
    let roots = leaf_roots(schema_descriptor);

    let mut row_groups = Vec::with_capacity(slices.len());

    for slice in slices {
        let offset_index = row_group_offset_index(metadata, slice.row_group)?;
        let row_group_metadata = metadata.row_group(slice.row_group);
        let mut ranges = Vec::new();

        for (leaf, column) in row_group_metadata.columns().iter().enumerate() {
            if !mask.leaf_included(leaf) {
                continue;
            }

            let chunk = chunk_range(column)?;

            let root = *roots.get(leaf).ok_or_else(|| {
                ParquetError::malformed(format!(
                    "row group {} has more column chunks than the schema has columns",
                    slice.row_group
                ))
            })?;

            let mut leaf_ranges = Vec::new();

            match offset_index.and_then(|index| index.get(leaf)) {
                Some(column_offsets) => {
                    let page_locations = column_offsets.page_locations();

                    if let Some(first_page) = page_locations.first() {
                        let first_page_offset = u64::try_from(first_page.offset).map_err(|_| {
                            ParquetError::malformed(format!(
                                "the offset index of `{}` points at a negative offset",
                                column.column_path()
                            ))
                        })?;

                        if first_page_offset != chunk.start {
                            leaf_ranges.push(chunk.start..first_page_offset);
                        }
                    }

                    leaf_ranges.extend(slice.selection.scan_ranges(page_locations));
                }

                None => leaf_ranges.push(chunk),
            }

            ranges.extend(
                leaf_ranges
                    .into_iter()
                    .map(|range| RootRange { root, range }),
            );
        }

        row_groups.push(PlannedRowGroup {
            slice,
            ranges,
            has_offset_index: offset_index.is_some(),
        });
    }

    Ok(WindowPlan {
        window,
        columns: ordered_columns,
        schema,
        parquet_fields,
        mask,
        row_groups,
    })
}

/// The offset index of `row_group`, as the decoder looks it up: absent when
/// the file has none at all, and refused when the file has one that does not
/// cover this row group.
fn row_group_offset_index(
    metadata: &ParquetMetaData,
    row_group: usize,
) -> Result<Option<&[OffsetIndexMetaData]>, ParquetError> {
    let Some(offset_index) = metadata.offset_index().filter(|index| !index.is_empty()) else {
        return Ok(None);
    };

    let column_count = metadata.row_group(row_group).num_columns();

    match offset_index.get(row_group) {
        Some(columns) if columns.len() == column_count => Ok(Some(columns.as_slice())),
        _ => Err(ParquetError::malformed(format!(
            "the offset index does not cover the {column_count} columns of row group {row_group}"
        ))),
    }
}

/// The bytes of a column chunk, from its dictionary page or first data page,
/// refusing the negative values the decoder would panic on.
fn chunk_range(column: &ColumnChunkMetaData) -> Result<Range<u64>, ParquetError> {
    let start = column
        .dictionary_page_offset()
        .unwrap_or_else(|| column.data_page_offset());

    let malformed = || {
        ParquetError::malformed(format!(
            "column `{}` declares an invalid chunk position ({start}, {} bytes)",
            column.column_path(),
            column.compressed_size()
        ))
    };

    let start = u64::try_from(start).map_err(|_| malformed())?;
    let length = u64::try_from(column.compressed_size()).map_err(|_| malformed())?;
    let end = start.checked_add(length).ok_or_else(malformed)?;

    Ok(start..end)
}

/// Refuses the window when a row group without an offset index would read a
/// selected chunk larger than `budget`, before any byte is read.
fn check_unindexed_budget(
    metadata: &ParquetMetaData,
    plan: &WindowPlan,
    budget: u64,
) -> Result<(), ParquetError> {
    for row_group in &plan.row_groups {
        if row_group.has_offset_index {
            continue;
        }

        let columns = metadata.row_group(row_group.slice.row_group).columns();

        for (leaf, column) in columns.iter().enumerate() {
            if !plan.mask.leaf_included(leaf) {
                continue;
            }

            let chunk = chunk_range(column)?;
            let size = chunk.end - chunk.start;

            if size > budget {
                return Err(ParquetError::UnindexedChunkTooLarge {
                    column: column.column_path().string(),
                    size,
                    limit: budget,
                });
            }
        }
    }

    Ok(())
}

/// Merges ranges that touch or overlap into one read. The decoder accepts a
/// pushed range that contains the ranges it asked for, so consecutive pages
/// and adjacent chunks cost one request instead of one each.
fn coalesce(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|range| (range.start, range.end));

    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());

    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }

    merged
}

/// Refuses a data range that points outside the file before it reaches the
/// source.
fn check_data_range(range: &Range<u64>, file_length: u64) -> Result<(), ParquetError> {
    if range.start > range.end || range.end > file_length {
        return Err(ParquetError::malformed(format!(
            "the file metadata points at bytes {}..{}, outside the {file_length}-byte file",
            range.start, range.end
        )));
    }

    Ok(())
}
