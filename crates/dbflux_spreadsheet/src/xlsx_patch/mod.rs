//! Writes cell edits into an xlsx or xlsm package without rebuilding it.
//!
//! [`patch_xlsx`] rewrites only the worksheet parts that hold an edit and
//! copies every other zip entry as it is stored: the same compressed bytes,
//! checksum, compression method and order. Styles, shared strings, charts,
//! images, comments, tables and VBA projects therefore survive unchanged.

mod cell;
mod package;
mod sheet;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::rc::Rc;

use chrono::NaiveDateTime;
use dbflux_byte_source::{ByteSource, ByteSourceReader};
use zip::ZipWriter;

use crate::error::SheetWriteError;
use cell::{
    MAX_COLUMNS, MAX_ROWS, MAX_TEXT_LENGTH, cell_name, date_serial, escape_text, format_number,
    is_xml_character,
};
use package::{
    Package, WORKBOOK_PART, WorkbookPart, parse_relationships, parse_workbook, relationships_part,
    resolve_target,
};
use sheet::{EncodedCell, SheetPatch, patch_sheet};

/// The new content of one cell.
#[derive(Debug, Clone, PartialEq)]
pub enum CellEdit {
    /// Empties the cell. A cell with a style keeps it, so its formatting stays.
    Clear,
    /// Text, stored inline in the cell so the shared string table is not
    /// rewritten.
    Text(String),
    Number(f64),
    Bool(bool),
    /// A date and time, stored as a serial number in the workbook's date
    /// system. The cell keeps its own number format, so it shows as a date
    /// only when that format is a date format.
    Date(NaiveDateTime),
    /// Formula text such as `SUM(A1:A3)`. One leading `=`, as typed into a
    /// cell, is dropped, because the file stores formulas without it. It is
    /// written without a cached result.
    Formula(String),
}

/// Cell edits for one or more sheets of a workbook.
///
/// Sheets are addressed by their index in workbook order (the order of
/// [`crate::Workbook::sheets`]), and cells by a zero-based `(row, column)`
/// as in [`crate::SheetGrid`]. Setting a cell twice keeps the last edit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct XlsxEdits {
    sheets: BTreeMap<usize, BTreeMap<(usize, usize), CellEdit>>,
}

impl XlsxEdits {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, sheet: usize, row: usize, column: usize, edit: CellEdit) {
        self.sheets
            .entry(sheet)
            .or_default()
            .insert((row, column), edit);
    }

    pub fn is_empty(&self) -> bool {
        self.sheets.values().all(BTreeMap::is_empty)
    }
}

/// Writes `source` with `edits` applied to `sink` as a new xlsx or xlsm
/// package.
///
/// Every edit is checked before anything is written, so a refused edit
/// leaves the sink untouched. An edit is refused when it:
///
/// - holds text longer than 32,767 characters, or a character XML 1.0
///   cannot carry;
/// - stores NaN or an infinity, or a date the workbook's date system cannot
///   store;
/// - replaces the cell that holds a shared formula's text (the cells that
///   reuse it may be edited);
/// - falls inside the range of an array or data-table formula.
///
/// An edited cell keeps its style and drops its old value, formula and
/// value type. Other parts of the package, including `sharedStrings.xml`,
/// stay byte-identical. Reads block, so a caller with a slow source runs this
/// off its UI thread.
pub fn patch_xlsx<S: ByteSource>(
    source: S,
    edits: &XlsxEdits,
    sink: impl Write + Seek,
) -> Result<(), SheetWriteError> {
    let mut package = Package::open(ByteSourceReader::new(source))?;

    let workbook = parse_workbook(&package.require_part(WORKBOOK_PART)?)?;
    let relationships =
        parse_relationships(&package.require_part(&relationships_part(WORKBOOK_PART))?)?;

    let mut patched_entries = HashMap::new();

    for (&index, cells) in &edits.sheets {
        if cells.is_empty() {
            continue;
        }

        let (name, part) = sheet_part(&workbook, &relationships, index)?;
        let encoded = encode_cells(name, cells, workbook.date_1904)?;

        let entry_name = package.entry_name(&part).ok_or_else(|| {
            SheetWriteError::malformed(format!("the package has no `{part}` part for `{name}`"))
        })?;
        let xml = package.require_part(&entry_name)?;

        let patched = patch_sheet(
            &xml,
            &SheetPatch {
                sheet_name: name,
                cells: &encoded,
            },
        )?;

        patched_entries.insert(entry_name, patched);
    }

    write_package(&mut package, &patched_entries, sink)
}

/// Returns the name and part name of the worksheet at `index`.
fn sheet_part<'a>(
    workbook: &'a WorkbookPart,
    relationships: &[package::Relationship],
    index: usize,
) -> Result<(&'a str, String), SheetWriteError> {
    let sheet = workbook
        .sheets
        .get(index)
        .ok_or(SheetWriteError::SheetOutOfRange {
            index,
            sheet_count: workbook.sheets.len(),
        })?;

    let relationship = relationships
        .iter()
        .find(|relationship| relationship.id == sheet.relationship_id)
        .ok_or_else(|| {
            SheetWriteError::malformed(format!(
                "sheet `{}` points at relationship `{}`, which the workbook does not have",
                sheet.name, sheet.relationship_id
            ))
        })?;

    if !relationship.is_kind("worksheet") || relationship.external {
        return Err(SheetWriteError::NotAWorksheet {
            sheet: sheet.name.clone(),
        });
    }

    Ok((
        sheet.name.as_str(),
        resolve_target(WORKBOOK_PART, &relationship.target),
    ))
}

/// Checks every edit of one sheet and turns it into the XML text it writes.
fn encode_cells(
    sheet: &str,
    cells: &BTreeMap<(usize, usize), CellEdit>,
    date_1904: bool,
) -> Result<BTreeMap<(usize, usize), EncodedCell>, SheetWriteError> {
    cells
        .iter()
        .map(|(&(row, column), edit)| {
            if row >= MAX_ROWS || column >= MAX_COLUMNS {
                return Err(SheetWriteError::CellOutOfRange {
                    sheet: sheet.to_string(),
                    row,
                    column,
                });
            }

            let encoded = encode_cell(sheet, row, column, edit, date_1904)?;
            Ok(((row, column), encoded))
        })
        .collect()
}

fn encode_cell(
    sheet: &str,
    row: usize,
    column: usize,
    edit: &CellEdit,
    date_1904: bool,
) -> Result<EncodedCell, SheetWriteError> {
    let cell = || cell_name(row, column);

    let check_characters =
        |text: &str| match text.chars().find(|&character| !is_xml_character(character)) {
            Some(character) => Err(SheetWriteError::InvalidCharacter {
                sheet: sheet.to_string(),
                cell: cell(),
                character,
            }),
            None => Ok(()),
        };

    let number = |value: f64| {
        if value.is_finite() {
            return Ok(EncodedCell::Number(format_number(value)));
        }

        Err(SheetWriteError::NonFiniteNumber {
            sheet: sheet.to_string(),
            cell: cell(),
            value,
        })
    };

    match edit {
        CellEdit::Clear => Ok(EncodedCell::Clear),
        CellEdit::Bool(flag) => Ok(EncodedCell::Bool(*flag)),
        CellEdit::Number(value) => number(*value),
        CellEdit::Date(date) => match date_serial(*date, date_1904) {
            Some(serial) => number(serial),
            None => Err(SheetWriteError::DateOutOfRange {
                sheet: sheet.to_string(),
                cell: cell(),
                date: *date,
            }),
        },
        CellEdit::Text(text) => {
            let length = text.encode_utf16().count();
            if length > MAX_TEXT_LENGTH {
                return Err(SheetWriteError::TextTooLong {
                    sheet: sheet.to_string(),
                    cell: cell(),
                    length,
                });
            }

            check_characters(text)?;
            Ok(EncodedCell::Text(escape_text(text, true)))
        }
        CellEdit::Formula(formula) => {
            let formula = formula.strip_prefix('=').unwrap_or(formula);

            check_characters(formula)?;
            Ok(EncodedCell::Formula(escape_text(formula, false)))
        }
    }
}

/// Writes every entry of the package in its original order, the patched ones
/// with their new bytes and the others copied without recompressing.
fn write_package<R: Read + Seek>(
    package: &mut Package<R>,
    patched_entries: &HashMap<String, Vec<u8>>,
    sink: impl Write + Seek,
) -> Result<(), SheetWriteError> {
    let sink = TrackedSink::new(sink);
    let sink_failed = sink.failed.clone();
    let mut writer = ZipWriter::new(sink);

    // A zip I/O error can come from either side of a raw copy; the sink
    // records its own failures so the two are reported apart.
    let classify = |error: zip::result::ZipError| {
        if sink_failed.get() {
            SheetWriteError::from_write(error)
        } else {
            SheetWriteError::from_read(error)
        }
    };

    let archive = package.archive_mut();

    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .map_err(SheetWriteError::from_read)?;

        match patched_entries.get(entry.name()) {
            Some(bytes) => {
                let name = entry.name().to_string();
                let options = entry.options();
                drop(entry);

                writer.start_file(name, options).map_err(&classify)?;
                writer
                    .write_all(bytes)
                    .map_err(|error| classify(zip::result::ZipError::Io(error)))?;
            }
            None => writer.raw_copy_file(entry).map_err(&classify)?,
        }
    }

    if !archive.comment().is_empty() {
        writer
            .set_raw_comment(archive.comment().into())
            .map_err(&classify)?;
    }

    writer.finish().map_err(&classify)?;

    Ok(())
}

/// A sink that remembers whether one of its own writes or seeks failed.
struct TrackedSink<W> {
    inner: W,
    failed: Rc<Cell<bool>>,
}

impl<W> TrackedSink<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            failed: Rc::new(Cell::new(false)),
        }
    }

    fn track<T>(&self, result: std::io::Result<T>) -> std::io::Result<T> {
        if result.is_err() {
            self.failed.set(true);
        }
        result
    }
}

impl<W: Write> Write for TrackedSink<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let result = self.inner.write(buffer);
        self.track(result)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self.inner.flush();
        self.track(result)
    }
}

impl<W: Seek> Seek for TrackedSink<W> {
    fn seek(&mut self, target: SeekFrom) -> std::io::Result<u64> {
        let result = self.inner.seek(target);
        self.track(result)
    }
}

#[cfg(test)]
mod tests;
