use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;

use crate::ParquetError;

/// A run of consecutive rows of a file, counted from the first row of its
/// first row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowWindow {
    pub start: u64,
    pub count: u64,
}

impl RowWindow {
    pub fn new(start: u64, count: u64) -> Self {
        Self { start, count }
    }

    /// The row just past the window, saturating instead of overflowing.
    pub fn end(&self) -> u64 {
        self.start.saturating_add(self.count)
    }
}

/// The rows a window selects inside one row group.
#[derive(Debug, Clone)]
pub(crate) struct RowGroupSlice {
    pub(crate) row_group: usize,
    /// Covers every row of the row group: skips before and after the window.
    pub(crate) selection: RowSelection,
}

/// The row groups a window touches, in file order, each with the selection of
/// its rows that fall inside the window, plus the window clamped to the rows
/// the file holds. Row groups outside the window are left out.
pub(crate) fn row_group_slices(
    metadata: &ParquetMetaData,
    window: RowWindow,
) -> Result<(RowWindow, Vec<RowGroupSlice>), ParquetError> {
    let window_end = window.end();

    let mut slices = Vec::new();
    let mut selected_rows: u64 = 0;
    let mut row_group_start: u64 = 0;

    for (row_group, row_group_metadata) in metadata.row_groups().iter().enumerate() {
        if row_group_start >= window_end {
            break;
        }

        let row_count = u64::try_from(row_group_metadata.num_rows()).map_err(|_| {
            ParquetError::malformed(format!(
                "row group {row_group} declares a negative row count ({})",
                row_group_metadata.num_rows()
            ))
        })?;

        let row_group_end = row_group_start.checked_add(row_count).ok_or_else(|| {
            ParquetError::malformed("the row groups declare more rows than fit in 64 bits")
        })?;

        let first = window.start.max(row_group_start);
        let last = window_end.min(row_group_end);

        if first < last {
            let selection = RowSelection::from(vec![
                RowSelector::skip(to_usize(first - row_group_start)?),
                RowSelector::select(to_usize(last - first)?),
                RowSelector::skip(to_usize(row_group_end - last)?),
            ]);

            slices.push(RowGroupSlice {
                row_group,
                selection,
            });

            selected_rows += last - first;
        }

        row_group_start = row_group_end;
    }

    Ok((RowWindow::new(window.start, selected_rows), slices))
}

fn to_usize(rows: u64) -> Result<usize, ParquetError> {
    usize::try_from(rows).map_err(|_| {
        ParquetError::malformed(format!(
            "a row group of {rows} rows is too large for this platform"
        ))
    })
}
