//! Reads one sheet of a workbook (xlsx, xlsm, xls or ods) as import rows,
//! through [`dbflux_spreadsheet`], which recognizes the format from the
//! file's content.
//!
//! The first row of the sheet is the header. Calamine has no row stream, so
//! [`SheetRows`] holds the whole sheet's decoded cells while it is read; that
//! is bounded by [`dbflux_spreadsheet::MAX_GRID_CELLS`] (10 million cells,
//! counting the empty cells that pad the grid to A1), and a larger sheet is
//! rejected rather than read. Rows are converted to [`Value`]s one chunk at a
//! time.

use std::path::Path;
use std::sync::Arc;

use dbflux_byte_source::FileSource as ByteFile;
use dbflux_core::Value;
use dbflux_core::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dbflux_spreadsheet::{
    CellFormula, CellValue, SheetGrid, SheetInfo, SheetKind, SpreadsheetError, SpreadsheetFormat,
    Workbook, open,
};

use crate::pipeline::TransferError;

mod issue;

pub use issue::{DuplicateHeader, InspectError, SheetIssue};

type FileWorkbook = Workbook<Arc<ByteFile>>;

/// The worksheets of the workbook, in workbook order.
pub(crate) fn list_worksheets(path: &Path) -> Result<Vec<SheetInfo>, TransferError> {
    let workbook = open_workbook(path)?;

    Ok(workbook
        .sheets()
        .iter()
        .filter(|sheet| sheet.kind == SheetKind::Worksheet)
        .cloned()
        .collect())
}

fn open_workbook(path: &Path) -> Result<FileWorkbook, TransferError> {
    let source = ByteFile::open(path)
        .map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))?;

    open(Arc::new(source)).map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))
}

/// One sheet's rows below its header row.
pub(crate) struct SheetRows {
    grid: SheetGrid,
    next_row: usize,
    sheet_name: String,
    headers: Vec<String>,
    label: String,
    /// One past the last row that holds a value, a formula or text; the
    /// grid can run further to cover merged ranges and tables.
    end_row: usize,
    /// Whether day zero (1899-12-31) stands for a time of day, which holds
    /// for the Excel formats and not for ods.
    day_zero_is_time: bool,
}

impl SheetRows {
    /// Opens the sheet named `sheet`, or the first visible worksheet when
    /// `None`, and reads its header row.
    pub(crate) fn open(path: &Path, sheet: Option<&str>) -> Result<Self, TransferError> {
        Self::read(path, sheet).map_err(|error| error.into_transfer_error(path))
    }

    /// Like [`Self::open`], and also checks what an import would stumble on
    /// later: a blank or repeated column name, and the first error cell.
    pub(crate) fn open_checked(path: &Path, sheet: Option<&str>) -> Result<Self, InspectError> {
        let rows = Self::read(path, sheet)?;

        // An error value in row 1 would be taken for a column name, so the
        // header row is checked for them before its names are.
        rows.check_cells(0..1)?;
        rows.check_header()?;
        rows.check_cells(1..rows.end_row)?;

        Ok(rows)
    }

    fn read(path: &Path, sheet: Option<&str>) -> Result<Self, InspectError> {
        let label = path.display().to_string();
        let mut workbook = open_workbook(path)?;
        let (index, sheet_name) = choose_sheet(&workbook, sheet, &label)?;

        let grid = match workbook.read_sheet(index) {
            Ok(grid) => grid,
            Err(SpreadsheetError::SheetTooLarge {
                rows,
                columns,
                limit,
            }) => {
                return Err(SheetIssue::TooLarge {
                    sheet: sheet_name,
                    cells: rows.saturating_mul(columns),
                    limit,
                }
                .into());
            }
            Err(error) => {
                return Err(TransferError::Source(format!("{label}: {error}")).into());
            }
        };

        let end_row = data_end_row(&grid);
        if grid.row_count() == 0 || grid.column_count() == 0 || end_row == 0 {
            return Err(SheetIssue::Empty { sheet: sheet_name }.into());
        }

        let day_zero_is_time = workbook.format() != SpreadsheetFormat::Ods;

        let headers = grid
            .row(0)
            .unwrap_or_default()
            .iter()
            .map(|cell| cell.display.to_string())
            .collect();

        Ok(Self {
            grid,
            next_row: 1,
            sheet_name,
            headers,
            label,
            end_row,
            day_zero_is_time,
        })
    }

    fn check_header(&self) -> Result<(), SheetIssue> {
        let mut blank_columns = Vec::new();
        let mut named: Vec<DuplicateHeader> = Vec::new();

        for (column, name) in self.headers.iter().enumerate() {
            if name.trim().is_empty() {
                blank_columns.push(column);
            } else if let Some(entry) = named.iter_mut().find(|entry| entry.name == *name) {
                entry.columns.push(column);
            } else {
                named.push(DuplicateHeader {
                    name: name.clone(),
                    columns: vec![column],
                });
            }
        }
        named.retain(|entry| entry.columns.len() > 1);

        if blank_columns.is_empty() && named.is_empty() {
            return Ok(());
        }

        Err(SheetIssue::InvalidHeader {
            sheet: self.sheet_name.clone(),
            blank_columns,
            duplicate_names: named,
        })
    }

    /// The first cell in `rows` that holds an error value.
    fn check_cells(&self, rows: std::ops::Range<usize>) -> Result<(), SheetIssue> {
        for row_index in rows {
            let Some(cells) = self.grid.row(row_index) else {
                break;
            };
            let found = cells
                .iter()
                .enumerate()
                .find_map(|(column, cell)| match &cell.value {
                    CellValue::Error(code) => Some((column, code.code())),
                    _ => None,
                });

            if let Some((column, code)) = found {
                return Err(SheetIssue::ErrorCell {
                    sheet: self.sheet_name.clone(),
                    row: row_index + 1,
                    column,
                    column_name: self.headers.get(column).cloned().unwrap_or_default(),
                    code,
                });
            }
        }

        Ok(())
    }

    /// The header row's text, one name per column of the sheet.
    pub(crate) fn headers(&self) -> &[String] {
        &self.headers
    }

    /// Converts up to `limit` more rows; the sheet's columns are returned
    /// in sheet order.
    pub(crate) fn next_rows(&mut self, limit: usize) -> Result<Vec<Vec<Value>>, TransferError> {
        let mut rows = Vec::new();

        while rows.len() < limit {
            let row_index = self.next_row;
            if row_index >= self.end_row {
                break;
            }
            let Some(cells) = self.grid.row(row_index) else {
                break;
            };
            self.next_row += 1;

            let mut values = Vec::with_capacity(cells.len());
            for (column, cell) in cells.iter().enumerate() {
                values.push(cell_value(&cell.value, self.day_zero_is_time).map_err(|code| {
                    TransferError::Source(format!(
                        "{}: sheet '{}', row {}, column '{}': the cell holds the error value {code}",
                        self.label,
                        self.sheet_name,
                        row_index + 1,
                        self.headers.get(column).map_or("?", String::as_str),
                    ))
                })?);
            }
            rows.push(values);
        }

        Ok(rows)
    }
}

/// One past the last row holding a non-empty cell or a formula, so trailing
/// rows that only pad the grid are not imported; empty rows between data
/// rows are kept.
fn data_end_row(grid: &SheetGrid) -> usize {
    (0..grid.row_count())
        .rev()
        .find(|&row| {
            grid.row(row).is_some_and(|cells| {
                cells
                    .iter()
                    .any(|cell| cell.value != CellValue::Empty || cell.formula != CellFormula::None)
            })
        })
        .map_or(0, |row| row + 1)
}

fn choose_sheet(
    workbook: &FileWorkbook,
    sheet: Option<&str>,
    label: &str,
) -> Result<(usize, String), TransferError> {
    let worksheets = || {
        workbook
            .sheets()
            .iter()
            .enumerate()
            .filter(|(_, info)| info.kind == SheetKind::Worksheet)
    };

    let chosen = match sheet {
        Some(name) => worksheets().find(|(_, info)| info.name == name),
        None => worksheets().find(|(_, info)| info.visible),
    };

    match (chosen, sheet) {
        (Some((index, info)), _) => Ok((index, info.name.clone())),
        (None, Some(name)) => {
            let available: Vec<String> = worksheets()
                .map(|(_, info)| format!("'{}'", info.name))
                .collect();
            Err(TransferError::Source(format!(
                "{label}: the workbook has no sheet named '{name}'; it has {}",
                available.join(", ")
            )))
        }
        (None, None) => Err(TransferError::Source(format!(
            "{label}: the workbook has no visible worksheet to import"
        ))),
    }
}

/// Maps a cell to a value, or to the code of the error it holds. Whole
/// numbers inside the `i64` range become integers, so a count does not
/// arrive as `3.0`; a date at midnight is a date and any other is a UTC
/// timestamp. Text stays text and is decoded by the target column's type.
fn cell_value(value: &CellValue, day_zero_is_time: bool) -> Result<Value, &'static str> {
    Ok(match value {
        CellValue::Empty => Value::Null,
        CellValue::Number(number) => number_value(*number),
        CellValue::Bool(flag) => Value::Bool(*flag),
        CellValue::Text(text) => Value::Text(text.to_string()),
        CellValue::Date(date) => date_value(*date, day_zero_is_time),
        CellValue::Error(code) => return Err(code.code()),
    })
}

/// `i64::MIN` as a float is exact; `i64::MAX` is not, so the upper bound is
/// the first float past it.
const I64_LOWER: f64 = -9_223_372_036_854_775_808.0;
const I64_UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;

fn number_value(number: f64) -> Value {
    if number.is_finite()
        && number.fract() == 0.0
        && (I64_LOWER..I64_UPPER_EXCLUSIVE).contains(&number)
    {
        // In range and integral, so the conversion is exact.
        return Value::Int(number as i64);
    }

    Value::Float(number)
}

/// Excel counts a time of day as a fraction of day zero, which the reader
/// reports as 1899-12-31 (no real date falls on it, since the 1900 system
/// starts at 1900-01-01). A workbook on the 1904 system has no such marker,
/// so a time of day there arrives as a timestamp on 1904-01-01.
fn date_value(date: NaiveDateTime, day_zero_is_time: bool) -> Value {
    if day_zero_is_time && Some(date.date()) == NaiveDate::from_ymd_opt(1899, 12, 31) {
        return Value::Time(date.time());
    }

    if date.time() == NaiveTime::MIN {
        return Value::Date(date.date());
    }

    Value::DateTime(date.and_utc())
}

#[cfg(test)]
mod tests;
