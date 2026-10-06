//! Writes cell edits into an xlsx or xlsm package without rebuilding it.
//!
//! [`patch_xlsx`] rewrites only the worksheet parts that hold an edit, plus
//! the few workbook parts that ask for a recalculation, and copies every
//! other zip entry as it is stored: the same compressed bytes, checksum,
//! compression method and order. Styles, shared strings, charts, images,
//! comments, tables and VBA projects therefore survive unchanged.

pub(crate) mod cell;
pub(crate) mod package;
mod sheet;
mod workbook;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::rc::Rc;

use dbflux_byte_source::{ByteSource, ByteSourceReader, SourceError};
use zip::ZipWriter;

use crate::edits::{CellEdit, SheetEdits};
use crate::error::SheetWriteError;
use cell::{
    MAX_COLUMNS, MAX_ROWS, MAX_TEXT_LENGTH, cell_name, date_serial, escape_text, format_number,
    is_xml_character,
};
use package::{
    Package, Relationship, WORKBOOK_PART, WorkbookPart, parse_relationships, parse_workbook,
    relationships_part, resolve_target,
};
use sheet::{EncodedCell, SheetPatch, patch_sheet};
use workbook::{remove_content_type_override, remove_relationships, request_full_calculation};

const CONTENT_TYPES_PART: &str = "[Content_Types].xml";

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
/// value type. A new cell at or below the sheet's append row (see
/// [`crate::SheetGrid::append_row`]) takes the style of the nearest cell
/// above it in its column; merged ranges and tables are never grown.
///
/// When any sheet changes, the workbook asks for a full recalculation on
/// load (`fullCalcOnLoad`) and loses its calculation chain, whose cell list
/// the edits may have made wrong. Other parts of the package, including
/// `sharedStrings.xml`, stay byte-identical. Reads block, so a caller with a
/// slow source runs this off its UI thread.
pub fn patch_xlsx<S: ByteSource>(
    source: S,
    edits: &SheetEdits,
    sink: impl Write + Seek,
) -> Result<(), SheetWriteError> {
    let mut package = Package::open(ByteSourceReader::new(source))?;

    let workbook_xml = package.require_part(WORKBOOK_PART)?;
    let workbook = parse_workbook(&workbook_xml)?;
    let relationships_xml = package.require_part(&relationships_part(WORKBOOK_PART))?;
    let relationships = parse_relationships(&relationships_xml)?;

    let mut patched_entries = HashMap::new();
    let mut removed_entries = HashSet::new();

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
        let append_row = workbook::append_row(&mut package, &part, &xml)?;

        let patched = patch_sheet(
            &xml,
            &SheetPatch {
                sheet_name: name,
                cells: &encoded,
                append_row,
            },
        )?;

        patched_entries.insert(entry_name, patched);
    }

    if !patched_entries.is_empty() {
        let workbook_parts = WorkbookParts {
            workbook_xml: &workbook_xml,
            relationships_xml: &relationships_xml,
            relationships: &relationships,
        };
        request_recalculation(
            &mut package,
            &workbook_parts,
            &mut patched_entries,
            &mut removed_entries,
        )?;
    }

    write_package(&mut package, &patched_entries, &removed_entries, sink)
}

/// Returns the zero-based row where rows appended to the worksheet at
/// `index` go, or `None` when that sheet is not a worksheet. See
/// [`workbook::append_row`].
pub(crate) fn sheet_append_row<S: ByteSource>(
    source: &S,
    index: usize,
) -> Result<Option<usize>, SheetWriteError> {
    let mut package = Package::open(ByteSourceReader::new(BorrowedSource(source)))?;

    let workbook = parse_workbook(&package.require_part(WORKBOOK_PART)?)?;
    let relationships =
        parse_relationships(&package.require_part(&relationships_part(WORKBOOK_PART))?)?;

    let part = match sheet_part(&workbook, &relationships, index) {
        Ok((_, part)) => part,
        Err(SheetWriteError::NotAWorksheet { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };

    let xml = package.require_part(&part)?;

    workbook::append_row(&mut package, &part, &xml).map(Some)
}

/// The workbook parts [`patch_xlsx`] has already read.
struct WorkbookParts<'a> {
    workbook_xml: &'a [u8],
    relationships_xml: &'a [u8],
    relationships: &'a [Relationship],
}

/// Asks for a full recalculation on load and drops the calculation chain,
/// with its relationship and its content-type override.
fn request_recalculation<R: Read + Seek>(
    package: &mut Package<R>,
    parts: &WorkbookParts<'_>,
    patched_entries: &mut HashMap<String, Vec<u8>>,
    removed_entries: &mut HashSet<String>,
) -> Result<(), SheetWriteError> {
    let entry_name =
        |package: &Package<R>, part: &str| package.entry_name(part).unwrap_or(part.to_string());

    if let Some(workbook_xml) = request_full_calculation(parts.workbook_xml)? {
        patched_entries.insert(entry_name(package, WORKBOOK_PART), workbook_xml);
    }

    let calc_chains: Vec<&Relationship> = parts
        .relationships
        .iter()
        .filter(|relationship| relationship.is_kind("calcChain") && !relationship.external)
        .collect();

    if calc_chains.is_empty() {
        return Ok(());
    }

    let ids: Vec<&str> = calc_chains
        .iter()
        .map(|relationship| relationship.id.as_str())
        .collect();
    let relationships_part = relationships_part(WORKBOOK_PART);
    patched_entries.insert(
        entry_name(package, &relationships_part),
        remove_relationships(parts.relationships_xml, &ids)?,
    );

    let original_content_types = package.read_part(CONTENT_TYPES_PART)?;
    let mut content_types = original_content_types.clone();

    for relationship in calc_chains {
        let part = resolve_target(WORKBOOK_PART, &relationship.target);

        if let Some(name) = package.entry_name(&part) {
            removed_entries.insert(name);
        }

        if let Some(xml) = &content_types {
            content_types = Some(remove_content_type_override(xml, &part)?);
        }
    }

    if let Some(xml) = content_types
        && original_content_types.as_deref() != Some(xml.as_slice())
    {
        patched_entries.insert(entry_name(package, CONTENT_TYPES_PART), xml);
    }

    Ok(())
}

/// Reads through a borrowed source, so a [`crate::Workbook`] can scan its
/// package without giving up or cloning the source it keeps.
pub(crate) struct BorrowedSource<'a, S>(pub(crate) &'a S);

impl<S: ByteSource> ByteSource for BorrowedSource<'_, S> {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.0.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        self.0.read_range(range)
    }
}

/// Returns the name and part name of the worksheet at `index`.
fn sheet_part<'a>(
    workbook: &'a WorkbookPart,
    relationships: &[Relationship],
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

/// Writes the entries of the package in their original order, leaving out
/// the removed ones, writing the patched ones with their new bytes and
/// copying the others without recompressing.
pub(crate) fn write_package<R: Read + Seek>(
    package: &mut Package<R>,
    patched_entries: &HashMap<String, Vec<u8>>,
    removed_entries: &HashSet<String>,
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

        if removed_entries.contains(entry.name()) {
            continue;
        }

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
