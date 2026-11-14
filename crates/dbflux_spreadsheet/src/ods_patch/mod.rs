//! Writes cell edits into an ods package without rebuilding it.
//!
//! [`patch_ods`] rewrites only `content.xml` and copies every other zip
//! entry as it is stored: the same compressed bytes, checksum, compression
//! method and order. The `mimetype` entry therefore stays first and stored,
//! and styles, settings, metadata, the manifest, charts and pictures survive
//! unchanged.

mod content;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Seek, Write};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use dbflux_byte_source::{ByteSource, ByteSourceReader};

use crate::edits::{CellEdit, SheetEdits};
use crate::error::SheetWriteError;
use crate::grid::{display_date, display_number};
use crate::xlsx_patch::cell::{
    MAX_COLUMNS, MAX_ROWS, MAX_TEXT_LENGTH, cell_name, format_number, is_xml_character,
};
use crate::xlsx_patch::package::Package;
use crate::xlsx_patch::{BorrowedSource, write_package};
use content::{EncodedCell, append_row, patch_content};

const CONTENT_PART: &str = "content.xml";
const MIMETYPE_PART: &str = "mimetype";
const ODS_MIMETYPE: &[u8] = b"application/vnd.oasis.opendocument.spreadsheet";

/// Writes `source` with `edits` applied to `sink` as a new ods package.
///
/// Every edit is checked before anything is written, so a refused edit
/// leaves the sink untouched. An edit is refused when it:
///
/// - targets a covered cell, one inside a merged range other than its
///   first cell;
/// - falls inside the range a matrix (array) formula fills;
/// - holds text longer than 32,767 UTF-16 units;
/// - holds a character XML 1.0 cannot carry, or a formula with one;
/// - stores NaN or an infinity, or a date outside the years 1 to 9999;
/// - lies past row 1,048,576 or column 16,384, LibreOffice's last cell.
///
/// Repeated rows and cells (`table:number-rows-repeated`,
/// `table:number-columns-repeated`) are split around an edited cell, so only
/// that cell changes and the parts around it keep their attributes. An
/// edited cell keeps its style, validation, merge, notes and anchored
/// drawings, and loses its old value, text and formula. Rows past the
/// sheet's last row element are added after it.
///
/// Every formula cell of the workbook loses its cached result, because
/// LibreOffice shows cached results instead of computing them when it opens
/// an ods file, and an edit can make any of them stale. LibreOffice computes
/// them on load; other readers, calamine included, see those cells without
/// a value until a spreadsheet application saves the file again.
///
/// Reads block, so a caller with a slow source runs this off its UI thread.
pub fn patch_ods<S: ByteSource>(
    source: S,
    edits: &SheetEdits,
    sink: impl Write + Seek,
) -> Result<(), SheetWriteError> {
    let mut package = Package::open(ByteSourceReader::new(source))?;

    let mimetype = package.read_part(MIMETYPE_PART)?;
    if mimetype.as_deref().map(<[u8]>::trim_ascii) != Some(ODS_MIMETYPE) {
        return Err(SheetWriteError::malformed(
            "the package is not an ods spreadsheet",
        ));
    }

    let mut patched_entries = HashMap::new();

    if !edits.is_empty() {
        let xml = package.require_part(CONTENT_PART)?;
        patched_entries.insert(
            CONTENT_PART.to_string(),
            patch_content(&xml, &edits.sheets)?,
        );
    }

    write_package(&mut package, &patched_entries, &HashSet::new(), sink)
}

/// Returns the zero-based row where rows appended to the sheet at `index`
/// go, or `None` when the workbook has no such sheet. See
/// [`content::append_row`].
pub(crate) fn sheet_append_row<S: ByteSource>(
    source: &S,
    index: usize,
) -> Result<Option<usize>, SheetWriteError> {
    let mut package = Package::open(ByteSourceReader::new(BorrowedSource(source)))?;
    let xml = package.require_part(CONTENT_PART)?;

    append_row(&xml, index)
}

/// Checks every edit of one sheet and turns it into the XML it writes.
fn encode_cells(
    sheet: &str,
    cells: &BTreeMap<(usize, usize), CellEdit>,
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

            let encoded = encode_cell(sheet, row, column, edit)?;
            Ok(((row, column), encoded))
        })
        .collect()
}

fn encode_cell(
    sheet: &str,
    row: usize,
    column: usize,
    edit: &CellEdit,
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

    let encoded = |attributes: String, body: String| EncodedCell { attributes, body };

    match edit {
        CellEdit::Clear => Ok(encoded(String::new(), String::new())),
        CellEdit::Number(value) => {
            if !value.is_finite() {
                return Err(SheetWriteError::NonFiniteNumber {
                    sheet: sheet.to_string(),
                    cell: cell(),
                    value: *value,
                });
            }

            Ok(encoded(
                format!(
                    " office:value-type=\"float\" office:value=\"{}\"",
                    format_number(*value)
                ),
                paragraphs(&display_number(*value)),
            ))
        }
        CellEdit::Bool(flag) => Ok(encoded(
            format!(" office:value-type=\"boolean\" office:boolean-value=\"{flag}\""),
            paragraphs(if *flag { "TRUE" } else { "FALSE" }),
        )),
        CellEdit::Date(date) => match iso_date(*date) {
            Some(value) => Ok(encoded(
                format!(" office:value-type=\"date\" office:date-value=\"{value}\""),
                paragraphs(&display_date(*date)),
            )),
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
            Ok(encoded(
                " office:value-type=\"string\"".to_string(),
                paragraphs(text),
            ))
        }
        CellEdit::Formula(formula) => {
            check_characters(formula)?;
            Ok(encoded(
                format!(
                    " table:formula=\"{}\"",
                    escape_attribute(&formula_attribute(formula))
                ),
                String::new(),
            ))
        }
    }
}

/// Writes a date as an `xsd:dateTime`, or an `xsd:date` at midnight.
/// Returns `None` outside the years 1 to 9999, which the four-digit form
/// spreadsheet applications read cannot hold.
fn iso_date(date: NaiveDateTime) -> Option<String> {
    let first = NaiveDate::from_ymd_opt(1, 1, 1)?;
    let last = NaiveDate::from_ymd_opt(9999, 12, 31)?;
    if !(first..=last).contains(&date.date()) {
        return None;
    }

    if date.time() == NaiveTime::MIN {
        return Some(date.format("%Y-%m-%d").to_string());
    }

    if date.nanosecond() == 0 {
        return Some(date.format("%Y-%m-%dT%H:%M:%S").to_string());
    }

    Some(date.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
}

/// Returns the `table:formula` value for an edit's formula text: kept as it
/// is when it starts with a namespace prefix such as `of:=`, and prefixed
/// with `of:=` (OpenFormula) otherwise.
fn formula_attribute(formula: &str) -> String {
    if has_namespace_prefix(formula) {
        return formula.to_string();
    }

    format!("of:={}", formula.strip_prefix('=').unwrap_or(formula))
}

fn has_namespace_prefix(formula: &str) -> bool {
    let Some((prefix, _)) = formula.split_once(":=") else {
        return false;
    };

    prefix
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && prefix
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_-.".contains(character))
}

/// Writes text as `<text:p>` paragraphs, one per line.
///
/// ODF collapses runs of spaces and drops spaces at the ends of a
/// paragraph, so those spaces are written as `<text:s/>`, and tabs as
/// `<text:tab/>`.
fn paragraphs(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut output = String::new();

    for line in normalized.split('\n') {
        if line.is_empty() {
            output.push_str("<text:p/>");
            continue;
        }

        output.push_str("<text:p>");
        output.push_str(&paragraph_content(line));
        output.push_str("</text:p>");
    }

    output
}

fn paragraph_content(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut characters = line.char_indices().peekable();

    while let Some((offset, character)) = characters.next() {
        match character {
            ' ' => {
                let mut run = 1usize;
                while characters.next_if(|&(_, next)| next == ' ').is_some() {
                    run += 1;
                }

                let at_start = offset == 0;
                let at_end = characters.peek().is_none();

                if at_start || at_end {
                    output.push_str(&spaces(run));
                } else {
                    output.push(' ');
                    if run > 1 {
                        output.push_str(&spaces(run - 1));
                    }
                }
            }
            '\t' => output.push_str("<text:tab/>"),
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            other => output.push(other),
        }
    }

    output
}

fn spaces(count: usize) -> String {
    if count == 1 {
        return "<text:s/>".to_string();
    }

    format!("<text:s text:c=\"{count}\"/>")
}

/// Escapes text for a double-quoted attribute value. Tabs and line breaks
/// are written as character references, which attribute normalization would
/// otherwise turn into spaces.
fn escape_attribute(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());

    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\t' => escaped.push_str("&#9;"),
            '\n' => escaped.push_str("&#10;"),
            '\r' => escaped.push_str("&#13;"),
            other => escaped.push(other),
        }
    }

    escaped
}

#[cfg(test)]
mod tests;
