use std::sync::Arc;

use calamine::{CellErrorType, Data, ExcelDateTime, Range};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::error::SpreadsheetError;

/// The most cells [`crate::Workbook::read_sheet`] lays out for one sheet,
/// counting the empty cells that pad the grid to A1.
///
/// One stray value far from the data (say at `XFD1048576`) would otherwise
/// ask for billions of cells.
pub const MAX_GRID_CELLS: usize = 10_000_000;

/// One sheet's cells, laid out so that grid `(row, column)` is the sheet's
/// `(row, column)` counted from A1, both zero-based.
///
/// The grid ends at the last row and column that hold a value or a formula,
/// or for xlsx and xlsm at [`SheetGrid::append_row`] when that is further
/// down: merged ranges and tables can reach past the last value, and the
/// grid is padded with empty rows to cover them.
#[derive(Debug, Clone, PartialEq)]
pub struct SheetGrid {
    row_count: usize,
    column_count: usize,
    cells: Vec<SheetCell>,
    append_row: Option<usize>,
}

impl SheetGrid {
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn column_count(&self) -> usize {
        self.column_count
    }

    /// The zero-based row where appended rows go: one past the last row that
    /// holds a cell, a merged range or a table, so an appended row never
    /// lands inside one of them. `Some` for xlsx and xlsm worksheets, where
    /// [`crate::patch_xlsx`] writes appended rows, unless the worksheet's
    /// part could not be scanned for it, and `None` for the other formats.
    pub fn append_row(&self) -> Option<usize> {
        self.append_row
    }

    /// Records where appended rows go, padding the grid with empty rows up
    /// to that row when it is past the last row read.
    pub(crate) fn with_append_row(mut self, append_row: usize) -> Result<Self, SpreadsheetError> {
        if append_row > self.row_count {
            let cell_count = append_row.saturating_mul(self.column_count);
            if cell_count > MAX_GRID_CELLS {
                return Err(SpreadsheetError::SheetTooLarge {
                    rows: append_row,
                    columns: self.column_count,
                    limit: MAX_GRID_CELLS,
                });
            }

            let empty = SheetCell {
                value: CellValue::Empty,
                display: Arc::from(""),
                formula: CellFormula::None,
            };
            self.cells.resize(cell_count, empty);
            self.row_count = append_row;
        }

        self.append_row = Some(append_row);

        Ok(self)
    }

    /// Returns the cell at a zero-based position, or `None` outside the grid.
    pub fn cell(&self, row: usize, column: usize) -> Option<&SheetCell> {
        if column >= self.column_count {
            return None;
        }

        self.row(row).and_then(|cells| cells.get(column))
    }

    /// Returns the cells of a zero-based row, or `None` outside the grid.
    pub fn row(&self, row: usize) -> Option<&[SheetCell]> {
        if row >= self.row_count {
            return None;
        }

        let start = row.checked_mul(self.column_count)?;
        let end = start.checked_add(self.column_count)?;

        self.cells.get(start..end)
    }
}

/// One cell of a [`SheetGrid`].
#[derive(Debug, Clone, PartialEq)]
pub struct SheetCell {
    /// The value the file stores; for a formula, its cached result.
    pub value: CellValue,
    /// The text the grid shows for [`SheetCell::value`].
    pub display: Arc<str>,
    pub formula: CellFormula,
}

/// A cell value as the file stores it.
///
/// An xlsx or xls duration (a number formatted as elapsed time) stays a
/// [`CellValue::Number`] of days and displays as `hours:minutes:seconds`.
/// An ods duration stays [`CellValue::Text`] in its ISO 8601 form, and a date
/// that has no calendar day (Excel's 1900-02-29) stays a number.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    Empty,
    Number(f64),
    Bool(bool),
    Text(Arc<str>),
    Date(NaiveDateTime),
    Error(CellErrorCode),
}

/// The formula behind a cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellFormula {
    /// The cell holds no formula.
    None,
    /// The formula as the file writes it: `=A1*2` for xlsx and xlsm, and
    /// OpenFormula text such as `of:=[.A1]*2` for ods.
    Text(Arc<str>),
    /// The format does not let DBFlux read formula text, so whether the cell
    /// holds a formula is unknown. Every non-empty xls cell reports this.
    Unavailable,
}

/// A spreadsheet error value, such as the result of dividing by zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellErrorCode {
    DivisionByZero,
    NotAvailable,
    Name,
    Null,
    Number,
    Reference,
    Value,
    GettingData,
}

impl CellErrorCode {
    /// Returns the code the spreadsheet shows, such as `#DIV/0!`.
    pub fn code(self) -> &'static str {
        match self {
            Self::DivisionByZero => "#DIV/0!",
            Self::NotAvailable => "#N/A",
            Self::Name => "#NAME?",
            Self::Null => "#NULL!",
            Self::Number => "#NUM!",
            Self::Reference => "#REF!",
            Self::Value => "#VALUE!",
            Self::GettingData => "#DATA!",
        }
    }
}

impl From<&CellErrorType> for CellErrorCode {
    fn from(error: &CellErrorType) -> Self {
        match error {
            CellErrorType::Div0 => Self::DivisionByZero,
            CellErrorType::NA => Self::NotAvailable,
            CellErrorType::Name => Self::Name,
            CellErrorType::Null => Self::Null,
            CellErrorType::Num => Self::Number,
            CellErrorType::Ref => Self::Reference,
            CellErrorType::Value => Self::Value,
            CellErrorType::GettingData => Self::GettingData,
        }
    }
}

/// How the formula text of a sheet is reported.
pub(crate) enum FormulaSource {
    /// Formula text that needs a leading `=` (calamine drops it for xlsx).
    PrefixEquals(Range<String>),
    /// Formula text kept as the file writes it.
    Verbatim(Range<String>),
    /// No formula text can be trusted; every non-empty cell is unavailable.
    Unavailable,
}

impl FormulaSource {
    fn range(&self) -> Option<&Range<String>> {
        match self {
            Self::PrefixEquals(range) | Self::Verbatim(range) => Some(range),
            Self::Unavailable => None,
        }
    }

    fn formula_at(&self, position: (u32, u32), value: &CellValue) -> CellFormula {
        let text = self
            .range()
            .and_then(|range| range.get_value(position))
            .filter(|text| !text.is_empty());

        match (self, text) {
            (Self::Unavailable, _) if *value != CellValue::Empty => CellFormula::Unavailable,
            (Self::PrefixEquals(_), Some(text)) => CellFormula::Text(format!("={text}").into()),
            (Self::Verbatim(_), Some(text)) => CellFormula::Text(text.as_str().into()),
            _ => CellFormula::None,
        }
    }
}

/// Lays out calamine's value and formula ranges as a grid padded to A1.
///
/// Both ranges are dropped when this returns, so only the grid stays
/// resident.
pub(crate) fn build_grid(
    values: Range<Data>,
    formulas: FormulaSource,
) -> Result<SheetGrid, SpreadsheetError> {
    let ends = [values.end(), formulas.range().and_then(Range::end)];
    let (row_count, column_count) =
        ends.into_iter()
            .flatten()
            .fold((0usize, 0usize), |(rows, columns), (row, column)| {
                (
                    rows.max(index(row).saturating_add(1)),
                    columns.max(index(column).saturating_add(1)),
                )
            });

    let cell_count = row_count.saturating_mul(column_count);
    if cell_count > MAX_GRID_CELLS {
        return Err(SpreadsheetError::SheetTooLarge {
            rows: row_count,
            columns: column_count,
            limit: MAX_GRID_CELLS,
        });
    }

    let empty_display: Arc<str> = Arc::from("");
    let mut cells = Vec::with_capacity(cell_count);

    for row in 0..row_count {
        for column in 0..column_count {
            let position = (position(row), position(column));

            let (value, display) = match values.get_value(position) {
                Some(data) => convert(data, &empty_display),
                None => (CellValue::Empty, empty_display.clone()),
            };
            let formula = formulas.formula_at(position, &value);

            cells.push(SheetCell {
                value,
                display,
                formula,
            });
        }
    }

    Ok(SheetGrid {
        row_count,
        column_count,
        cells,
        append_row: None,
    })
}

fn index(position: u32) -> usize {
    usize::try_from(position).unwrap_or(usize::MAX)
}

/// Converts a grid index back to calamine's `u32`; the grid never exceeds the
/// `u32` positions it was sized from.
fn position(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

fn convert(data: &Data, empty_display: &Arc<str>) -> (CellValue, Arc<str>) {
    match data {
        Data::Empty => (CellValue::Empty, empty_display.clone()),
        Data::Int(number) => (CellValue::Number(*number as f64), number.to_string().into()),
        Data::Float(number) => (CellValue::Number(*number), display_number(*number).into()),
        Data::Bool(flag) => {
            let display = if *flag { "TRUE" } else { "FALSE" };
            (CellValue::Bool(*flag), display.into())
        }
        Data::String(text) | Data::DurationIso(text) => {
            let text: Arc<str> = text.as_str().into();
            (CellValue::Text(text.clone()), text)
        }
        Data::DateTime(excel_date) => convert_excel_date(excel_date),
        Data::DateTimeIso(text) => match parse_iso_date(text) {
            Some(date) => (CellValue::Date(date), display_date(date).into()),
            None => {
                let text: Arc<str> = text.as_str().into();
                (CellValue::Text(text.clone()), text)
            }
        },
        Data::Error(error) => {
            let code = CellErrorCode::from(error);
            (CellValue::Error(code), code.code().into())
        }
    }
}

fn convert_excel_date(excel_date: &ExcelDateTime) -> (CellValue, Arc<str>) {
    let serial = excel_date.as_f64();

    if excel_date.is_duration() {
        return (CellValue::Number(serial), display_duration(serial).into());
    }

    let (year, month, day, hour, minute, second, millisecond) = excel_date.to_ymd_hms_milli();
    let date = NaiveDate::from_ymd_opt(i32::from(year), u32::from(month), u32::from(day))
        .zip(NaiveTime::from_hms_milli_opt(
            u32::from(hour),
            u32::from(minute),
            u32::from(second),
            u32::from(millisecond),
        ))
        .map(|(date, time)| date.and_time(time));

    match date {
        Some(date) => (CellValue::Date(date), display_date(date).into()),
        None => (CellValue::Number(serial), display_number(serial).into()),
    }
}

/// Parses an ods `office:date-value`, which is a date or a date and time.
fn parse_iso_date(text: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .ok()
                .map(|date| date.and_time(NaiveTime::MIN))
        })
}

/// Shows a date as `2024-01-31`, adding the time of day only when it is not
/// midnight and the milliseconds only when there are some.
fn display_date(date: NaiveDateTime) -> String {
    if date.time() == NaiveTime::MIN {
        return date.format("%Y-%m-%d").to_string();
    }

    if date.nanosecond() == 0 {
        return date.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    date.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

/// Shows a number the way a spreadsheet does: rounded to 15 significant
/// digits, which drops binary noise such as `0.30000000000000004`, with no
/// trailing zeros and no exponent.
pub(crate) fn display_number(number: f64) -> String {
    if number == 0.0 {
        return "0".to_string();
    }

    if !number.is_finite() {
        return number.to_string();
    }

    let rounded = format!("{number:.14e}").parse::<f64>().unwrap_or(number);

    rounded.to_string()
}

/// Shows a duration of `days` as elapsed `hours:minutes:seconds`, the way a
/// `[h]:mm:ss` number format does.
pub(crate) fn display_duration(days: f64) -> String {
    let total_seconds = (days * 86_400.0).round();
    let sign = if total_seconds < 0.0 { "-" } else { "" };

    // Saturates for durations beyond `i64` seconds, which no sheet holds.
    let total_seconds = total_seconds.abs() as i64;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;

    format!("{sign}{hours}:{minutes:02}:{seconds:02}")
}
