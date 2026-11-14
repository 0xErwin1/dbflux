//! Writes the values of a workbook into a new xlsx package.
//!
//! This is how an xls workbook, which DBFlux cannot write, becomes a file it
//! can edit. Only values travel: a formula becomes the value it shows, and
//! formatting, number formats other than dates, merged cells, column widths,
//! charts, images and macros are not written.

use std::io::Write;

use chrono::{Datelike, NaiveDateTime, NaiveTime, Timelike};
use dbflux_byte_source::ByteSource;
use rust_xlsxwriter::{ExcelDateTime, Format, Worksheet, XlsxError};

use crate::error::ValuesWriteError;
use crate::grid::{CellValue, SheetGrid};
use crate::workbook::{SheetKind, Workbook};
use crate::xlsx_patch::cell::cell_name;

const DATE_FORMAT: &str = "yyyy-mm-dd";
const DATE_TIME_FORMAT: &str = "yyyy-mm-dd hh:mm:ss";

/// Writes every worksheet of `workbook`, in workbook order and under its
/// own name, into a new xlsx package written to `sink`.
///
/// - Numbers, booleans and text keep their type; text is always written as
///   text, so a value such as `=A1` does not become a formula.
/// - A date is written as a date with the `yyyy-mm-dd` format, or
///   `yyyy-mm-dd hh:mm:ss` when it has a time of day.
/// - An error value is written as its text, such as `#DIV/0!`.
/// - Empty cells are not written. A hidden worksheet stays hidden.
/// - Chart sheets and other sheets without cells are not written.
///
/// The sheets are read one at a time with [`Workbook::read_sheet`] and each
/// grid is dropped once its values are copied, but the writer keeps every
/// value it was given until the package is written. Reads and writes block.
pub fn write_values_xlsx<S: ByteSource, W: Write + Send>(
    workbook: &mut Workbook<S>,
    sink: W,
) -> Result<(), ValuesWriteError> {
    let sheets = workbook.sheets().to_vec();
    let mut package = rust_xlsxwriter::Workbook::new();
    let date_format = Format::new().set_num_format(DATE_FORMAT);
    let date_time_format = Format::new().set_num_format(DATE_TIME_FORMAT);
    let mut active_chosen = false;

    for (index, sheet) in sheets.iter().enumerate() {
        if sheet.kind != SheetKind::Worksheet {
            continue;
        }

        let grid = workbook
            .read_sheet(index)
            .map_err(|source| ValuesWriteError::Read {
                sheet: sheet.name.clone(),
                source,
            })?;

        let worksheet = package.add_worksheet();
        worksheet
            .set_name(sheet.name.as_str())
            .map_err(|error| ValuesWriteError::Sheet {
                sheet: sheet.name.clone(),
                message: error.to_string(),
            })?;

        // The first visible worksheet opens first. rust_xlsxwriter would
        // otherwise make the first worksheet active, which unhides it.
        if !sheet.visible {
            worksheet.set_hidden(true);
        } else if !active_chosen {
            worksheet.set_active(true);
            active_chosen = true;
        }

        write_grid(
            worksheet,
            &sheet.name,
            &grid,
            &date_format,
            &date_time_format,
        )?;
    }

    package
        .save_to_writer(sink)
        .map_err(|error| ValuesWriteError::Write {
            message: error.to_string(),
        })
}

fn write_grid(
    worksheet: &mut Worksheet,
    sheet: &str,
    grid: &SheetGrid,
    date_format: &Format,
    date_time_format: &Format,
) -> Result<(), ValuesWriteError> {
    for row in 0..grid.row_count() {
        let Some(cells) = grid.row(row) else {
            continue;
        };

        for (column, cell) in cells.iter().enumerate() {
            if cell.value == CellValue::Empty {
                continue;
            }

            let cell_error = |message: String| ValuesWriteError::Cell {
                sheet: sheet.to_string(),
                cell: cell_name(row, column),
                message,
            };

            let (Ok(row_number), Ok(column_number)) = (u32::try_from(row), u16::try_from(column))
            else {
                return Err(cell_error(XlsxError::RowColumnLimitError.to_string()));
            };

            write_value(
                worksheet,
                row_number,
                column_number,
                &cell.value,
                date_format,
                date_time_format,
            )
            .map_err(|error| cell_error(error.to_string()))?;
        }
    }

    Ok(())
}

fn write_value(
    worksheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: &CellValue,
    date_format: &Format,
    date_time_format: &Format,
) -> Result<(), XlsxError> {
    match value {
        CellValue::Empty => Ok(()),

        CellValue::Number(number) => worksheet.write_number(row, column, *number).map(drop),

        CellValue::Bool(flag) => worksheet.write_boolean(row, column, *flag).map(drop),

        CellValue::Text(text) => worksheet.write_string(row, column, &**text).map(drop),

        CellValue::Date(date) => {
            let format = if date.time() == NaiveTime::MIN {
                date_format
            } else {
                date_time_format
            };

            worksheet
                .write_datetime_with_format(row, column, excel_date(date)?, format)
                .map(drop)
        }

        CellValue::Error(code) => worksheet.write_string(row, column, code.code()).map(drop),
    }
}

/// Converts a date to rust_xlsxwriter's, refusing a year it cannot store.
fn excel_date(date: &NaiveDateTime) -> Result<ExcelDateTime, XlsxError> {
    let year = u16::try_from(date.year()).map_err(|_| {
        XlsxError::DateTimeRangeError(format!("the year {} is out of range", date.year()))
    })?;

    // Both fit their types: a month is 1 to 12 and a day 1 to 31.
    let month = date.month() as u8;
    let day = date.day() as u8;
    let seconds = f64::from(date.second()) + f64::from(date.nanosecond()) / 1_000_000_000.0;

    ExcelDateTime::from_ymd(year, month, day)?.and_hms(
        date.hour() as u16,
        date.minute() as u8,
        seconds,
    )
}
