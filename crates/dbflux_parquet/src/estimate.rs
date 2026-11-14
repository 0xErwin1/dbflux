use crate::decode::{coalesced_ranges, window_read_plan};
use crate::{ParquetError, ParquetFile, RowWindow};

/// The bytes a read of some top-level columns costs, worked out from the
/// footer and page index without reading any data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEstimate {
    /// Every byte the read requests.
    pub total_bytes: u64,
    /// The bytes of each selected top-level column, in file schema order,
    /// each column once and listed even when it costs nothing.
    pub per_column: Vec<(usize, u64)>,
    /// How many row groups the read touches.
    pub row_groups_touched: usize,
    /// How many row groups the file has.
    pub row_group_count: usize,
}

/// The bytes [`crate::read_window`] reads for the same arguments.
///
/// Built from [`crate::window_byte_ranges`]'s plan, so [`ReadEstimate::total_bytes`]
/// is the sum of the ranges `read_window` requests. A selected chunk over the
/// unindexed-chunk budget is counted rather than refused, as
/// `window_byte_ranges` does.
pub fn read_estimate(
    file: &ParquetFile,
    window: RowWindow,
    columns: &[usize],
) -> Result<ReadEstimate, ParquetError> {
    let plan = window_read_plan(file, window, columns)?;

    let mut per_column: Vec<(usize, u64)> =
        plan.columns.iter().map(|&column| (column, 0)).collect();

    let mut total_bytes: u64 = 0;

    for ranges in &plan.row_groups {
        for read in coalesced_ranges(ranges) {
            total_bytes = total_bytes.saturating_add(read.end - read.start);
        }

        for range in ranges {
            if let Some((_, bytes)) = per_column
                .iter_mut()
                .find(|(column, _)| *column == range.root)
            {
                *bytes = bytes.saturating_add(range.range.end - range.range.start);
            }
        }
    }

    Ok(ReadEstimate {
        total_bytes,
        per_column,
        row_groups_touched: plan.row_groups.len(),
        row_group_count: file.row_group_count(),
    })
}

/// The bytes of reading every row of `columns`: the sum of their column chunk
/// sizes in the footer.
pub fn whole_file_read_estimate(
    file: &ParquetFile,
    columns: &[usize],
) -> Result<ReadEstimate, ParquetError> {
    read_estimate(file, RowWindow::new(0, file.row_count()), columns)
}
