//! Rewrites one worksheet part by splicing edited cells into its original
//! bytes, so every byte outside the edited cells and rows stays as it was.

use std::collections::btree_map;
use std::collections::{BTreeMap, HashMap};
use std::iter::Peekable;
use std::ops::Range;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;

use crate::error::{FormulaRangeKind, SheetWriteError};
use crate::xlsx_patch::cell::{CellRange, cell_name, parse_cell_reference};
use crate::xlsx_patch::package::{attribute, reader_position, rebuild_tag, xml_reader};

/// An edit, already validated and escaped for the sheet XML.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EncodedCell {
    Clear,
    Number(String),
    Bool(bool),
    /// Escaped text, written as an inline string.
    Text(String),
    /// Escaped formula text without the leading `=`.
    Formula(String),
}

pub(crate) struct SheetPatch<'a> {
    pub(crate) sheet_name: &'a str,
    pub(crate) cells: &'a BTreeMap<(usize, usize), EncodedCell>,
    /// The first row past the sheet's data; see [`super::workbook::append_row`]. A new
    /// cell at or below it takes the style of the nearest cell above it.
    pub(crate) append_row: usize,
}

/// Returns the worksheet XML with the edits of `patch` applied.
pub(crate) fn patch_sheet(xml: &[u8], patch: &SheetPatch<'_>) -> Result<Vec<u8>, SheetWriteError> {
    let mut rewriter = SheetRewriter {
        input: xml,
        output: Vec::with_capacity(xml.len().saturating_add(1024)),
        copied: 0,
        prefix: String::new(),
        sheet_name: patch.sheet_name,
        pending: patch.cells.iter().peekable(),
        written_range: written_range(patch.cells),
        append_row: patch.append_row,
        tracks_column_styles: patch.cells.keys().any(|&(row, _)| row >= patch.append_row),
        formula_ranges: Vec::new(),
        column_styles: HashMap::new(),
    };

    rewriter.rewrite()?;

    Ok(rewriter.output)
}

/// The formula facts of an existing cell the patch needs.
#[derive(Default)]
struct FormulaInfo {
    formula_type: Option<String>,
    range: Option<String>,
}

/// An existing cell that an edit replaces.
struct ExistingCell {
    /// The input bytes from the cell's start tag to its end tag.
    span: Range<usize>,
    style: Option<String>,
    phonetic: Option<String>,
    formula: FormulaInfo,
}

/// Returns the smallest range holding every cell that gets a value.
fn written_range(cells: &BTreeMap<(usize, usize), EncodedCell>) -> Option<CellRange> {
    cells
        .iter()
        .filter(|(_, edit)| **edit != EncodedCell::Clear)
        .map(|(&(row, column), _)| CellRange::single(row, column))
        .reduce(|range, cell| range.union(&cell))
}

struct SheetRewriter<'a> {
    input: &'a [u8],
    output: Vec<u8>,
    /// How many input bytes are already copied to or dropped from the output.
    copied: usize,
    /// The namespace prefix of `<sheetData>`, with its colon, used for every
    /// element the patch writes.
    prefix: String,
    sheet_name: &'a str,
    pending: Peekable<btree_map::Iter<'a, (usize, usize), EncodedCell>>,
    /// The smallest range that holds every cell the patch writes a value to.
    written_range: Option<CellRange>,
    append_row: usize,
    /// Whether an edit lands at or below the append row, which is the only
    /// case that needs [`SheetRewriter::column_styles`].
    tracks_column_styles: bool,
    /// Array and data-table formula ranges seen so far. A range's formula is
    /// in its top-left cell, which comes before every other cell of the range.
    formula_ranges: Vec<(CellRange, FormulaRangeKind)>,
    /// The raw style of the last cell seen in each column, which is the
    /// nearest cell above the row being written.
    column_styles: HashMap<usize, Option<String>>,
}

impl SheetRewriter<'_> {
    fn rewrite(&mut self) -> Result<(), SheetWriteError> {
        let input = self.input;
        let mut reader = xml_reader(input)?;
        let mut found_sheet_data = false;

        loop {
            let start = reader_position(&reader)?;
            let event = reader.read_event()?;
            let end = reader_position(&reader)?;

            match event {
                Event::Start(element) if element.local_name().as_ref() == b"sheetData" => {
                    found_sheet_data = true;
                    self.prefix = prefix_of(element.name());
                    self.rewrite_sheet_data(&mut reader)?;
                }
                Event::Empty(element) if element.local_name().as_ref() == b"sheetData" => {
                    found_sheet_data = true;
                    self.prefix = prefix_of(element.name());

                    if self.pending.peek().is_some() {
                        self.replace(start, end);
                        let tag = format!("<{}sheetData>", self.prefix);
                        self.push(&tag);
                        self.insert_rows_before(usize::MAX)?;
                        let tag = format!("</{}sheetData>", self.prefix);
                        self.push(&tag);
                    }
                }
                Event::Start(element) | Event::Empty(element)
                    if element.local_name().as_ref() == b"dimension" =>
                {
                    self.update_dimension(&element, &reader, start, end)?;
                }
                Event::Eof => break,
                _ => {}
            }
        }

        if !found_sheet_data {
            return Err(SheetWriteError::malformed(format!(
                "sheet `{}` has no sheetData element",
                self.sheet_name
            )));
        }

        self.copy_to(input.len());

        Ok(())
    }

    fn rewrite_sheet_data(&mut self, reader: &mut Reader<&[u8]>) -> Result<(), SheetWriteError> {
        let mut previous_row: Option<usize> = None;

        loop {
            let start = reader_position(reader)?;
            let event = reader.read_event()?;
            let end = reader_position(reader)?;

            match event {
                Event::Start(element) if element.local_name().as_ref() == b"row" => {
                    let row = row_index(&element, reader, previous_row)?;
                    previous_row = Some(row);

                    self.copy_to(start);
                    self.insert_rows_before(row)?;

                    let has_edits = self.next_row() == Some(row);
                    if has_edits {
                        let tag = rebuild_tag(&element, None, &[b"spans"], false)?;
                        self.replace(start, end);
                        self.push(&tag);
                    }

                    self.walk_row(reader, row, has_edits)?;
                }
                Event::Empty(element) if element.local_name().as_ref() == b"row" => {
                    let row = row_index(&element, reader, previous_row)?;
                    previous_row = Some(row);

                    self.copy_to(start);
                    self.insert_rows_before(row)?;

                    if self.row_has_content(row) {
                        let start_tag = rebuild_tag(&element, None, &[b"spans"], false)?;
                        self.replace(start, end);
                        self.push(&start_tag);
                        self.insert_cells_before(row, usize::MAX)?;
                        let end_tag = format!("</{}row>", self.prefix);
                        self.push(&end_tag);
                    } else {
                        self.skip_row_edits(row);
                    }
                }
                Event::End(element) if element.local_name().as_ref() == b"sheetData" => {
                    self.copy_to(start);
                    self.insert_rows_before(usize::MAX)?;
                    return Ok(());
                }
                Event::Eof => {
                    return Err(SheetWriteError::malformed(
                        "the sheetData element is not closed",
                    ));
                }
                _ => {}
            }
        }
    }

    /// Walks the cells of one row, replacing and inserting its edited cells
    /// when `has_edits`, and collecting formula ranges either way.
    fn walk_row(
        &mut self,
        reader: &mut Reader<&[u8]>,
        row: usize,
        has_edits: bool,
    ) -> Result<(), SheetWriteError> {
        let mut previous_column: Option<usize> = None;
        // A removed cell followed by a cell without an `r` attribute would
        // shift that cell left, so the removed cell stays as an empty one.
        let mut removed_cell: Option<String> = None;

        loop {
            let start = reader_position(reader)?;
            let event = reader.read_event()?;
            let end = reader_position(reader)?;

            let (element, is_empty) = match event {
                Event::Start(element) if element.local_name().as_ref() == b"c" => (element, false),
                Event::Empty(element) if element.local_name().as_ref() == b"c" => (element, true),
                Event::End(element) if element.local_name().as_ref() == b"row" => {
                    if has_edits {
                        self.copy_to(start);
                        self.insert_cells_before(row, usize::MAX)?;
                    }
                    return Ok(());
                }
                Event::Eof => {
                    return Err(SheetWriteError::malformed("a row element is not closed"));
                }
                _ => continue,
            };

            let reference = attribute(&element, b"r", reader.decoder())?;
            let column = match reference.as_deref() {
                Some(text) => parse_cell_reference(text)
                    .map(|(_, column)| column)
                    .ok_or_else(|| {
                        SheetWriteError::malformed(format!("invalid cell reference `{text}`"))
                    })?,
                None => previous_column.map_or(0, |column| column.saturating_add(1)),
            };
            previous_column = Some(column);

            if let Some(placeholder) = removed_cell.take()
                && reference.is_none()
            {
                self.copy_to(start);
                self.push(&placeholder);
            }

            let (formula, cell_end) = if is_empty {
                (FormulaInfo::default(), end)
            } else {
                read_cell_body(reader)?
            };
            self.record_formula_range(&formula)?;

            if self.tracks_column_styles {
                let style = raw_attribute(&element, b"s")?;
                self.column_styles.insert(column, style);
            }

            if !has_edits {
                continue;
            }

            self.copy_to(start);
            self.insert_cells_before(row, column)?;

            if let Some(edit) = self.take_edit(row, column) {
                let existing = ExistingCell {
                    span: start..cell_end,
                    style: raw_attribute(&element, b"s")?,
                    phonetic: raw_attribute(&element, b"ph")?,
                    formula,
                };
                removed_cell = self.replace_cell(row, column, &edit, existing)?;
            }
        }
    }

    /// Replaces an existing cell with its edit, refusing an edit to a cell
    /// that holds a shared formula's text or lies in a formula range.
    ///
    /// Returns the empty cell to write instead when the edit removed the
    /// cell, for the caller to put back if a later cell depends on its place.
    fn replace_cell(
        &mut self,
        row: usize,
        column: usize,
        edit: &EncodedCell,
        existing: ExistingCell,
    ) -> Result<Option<String>, SheetWriteError> {
        self.check_formula_ranges(row, column)?;

        if existing.formula.formula_type.as_deref() == Some("shared")
            && let Some(range) = existing.formula.range
        {
            return Err(SheetWriteError::SharedFormulaMaster {
                sheet: self.sheet_name.to_string(),
                cell: cell_name(row, column),
                range,
            });
        }

        self.replace(existing.span.start, existing.span.end);

        if *edit == EncodedCell::Clear && existing.style.is_none() {
            let placeholder = format!("<{}c r=\"{}\"/>", self.prefix, cell_name(row, column));
            return Ok(Some(placeholder));
        }

        let cell = self.cell_xml(
            row,
            column,
            edit,
            existing.style.as_deref(),
            existing.phonetic.as_deref(),
        );
        self.push(&cell);

        Ok(None)
    }

    /// Writes new rows for every pending edit in a row before `limit`.
    fn insert_rows_before(&mut self, limit: usize) -> Result<(), SheetWriteError> {
        while let Some(row) = self.next_row().filter(|row| *row < limit) {
            if !self.row_has_content(row) {
                self.skip_row_edits(row);
                continue;
            }

            let start_tag = format!("<{}row r=\"{}\">", self.prefix, row.saturating_add(1));
            self.push(&start_tag);
            self.insert_cells_before(row, usize::MAX)?;
            let end_tag = format!("</{}row>", self.prefix);
            self.push(&end_tag);
        }

        Ok(())
    }

    /// Writes new cells for the pending edits of `row` before `limit`. An
    /// edit that clears a cell the sheet does not have writes nothing.
    fn insert_cells_before(&mut self, row: usize, limit: usize) -> Result<(), SheetWriteError> {
        while let Some(&(&(edit_row, column), edit)) = self.pending.peek() {
            if edit_row != row || column >= limit {
                break;
            }

            self.pending.next();

            if *edit == EncodedCell::Clear {
                continue;
            }

            self.check_formula_ranges(row, column)?;

            let style = if row >= self.append_row {
                self.column_styles.get(&column).cloned().flatten()
            } else {
                None
            };
            let cell = self.cell_xml(row, column, edit, style.as_deref(), None);
            self.push(&cell);
        }

        Ok(())
    }

    fn next_row(&mut self) -> Option<usize> {
        self.pending.peek().map(|&(&(row, _), _)| row)
    }

    /// Whether any pending edit of `row` writes a value.
    fn row_has_content(&self, row: usize) -> bool {
        self.pending
            .clone()
            .take_while(|&(&(edit_row, _), _)| edit_row == row)
            .any(|(_, edit)| *edit != EncodedCell::Clear)
    }

    fn skip_row_edits(&mut self, row: usize) {
        while self.next_row() == Some(row) {
            self.pending.next();
        }
    }

    fn take_edit(&mut self, row: usize, column: usize) -> Option<EncodedCell> {
        let &(&position, _) = self.pending.peek()?;
        if position != (row, column) {
            return None;
        }

        self.pending.next().map(|(_, edit)| edit.clone())
    }

    fn record_formula_range(&mut self, formula: &FormulaInfo) -> Result<(), SheetWriteError> {
        let kind = match formula.formula_type.as_deref() {
            Some("array") => FormulaRangeKind::Array,
            Some("dataTable") => FormulaRangeKind::DataTable,
            _ => return Ok(()),
        };

        let Some(text) = formula.range.as_deref() else {
            return Ok(());
        };
        let range = CellRange::parse(text)
            .ok_or_else(|| SheetWriteError::malformed(format!("invalid formula range `{text}`")))?;

        self.formula_ranges.push((range, kind));

        Ok(())
    }

    fn check_formula_ranges(&self, row: usize, column: usize) -> Result<(), SheetWriteError> {
        match self
            .formula_ranges
            .iter()
            .find(|(range, _)| range.contains(row, column))
        {
            Some((range, kind)) => Err(SheetWriteError::InsideFormulaRange {
                sheet: self.sheet_name.to_string(),
                cell: cell_name(row, column),
                range: range.name(),
                kind: *kind,
            }),
            None => Ok(()),
        }
    }

    fn cell_xml(
        &self,
        row: usize,
        column: usize,
        edit: &EncodedCell,
        style: Option<&str>,
        phonetic: Option<&str>,
    ) -> String {
        let prefix = &self.prefix;
        let mut cell = format!("<{prefix}c r=\"{}\"", cell_name(row, column));

        if let Some(style) = style {
            cell.push_str(&format!(" s=\"{style}\""));
        }
        if let Some(phonetic) = phonetic {
            cell.push_str(&format!(" ph=\"{phonetic}\""));
        }

        let body = match edit {
            EncodedCell::Clear => {
                cell.push_str("/>");
                return cell;
            }
            EncodedCell::Number(number) => format!("<{prefix}v>{number}</{prefix}v>"),
            EncodedCell::Bool(flag) => {
                cell.push_str(" t=\"b\"");
                format!("<{prefix}v>{}</{prefix}v>", u8::from(*flag))
            }
            EncodedCell::Text(text) => {
                cell.push_str(" t=\"inlineStr\"");
                format!(
                    "<{prefix}is><{prefix}t xml:space=\"preserve\">{text}</{prefix}t></{prefix}is>"
                )
            }
            EncodedCell::Formula(formula) => format!("<{prefix}f>{formula}</{prefix}f>"),
        };

        cell.push('>');
        cell.push_str(&body);
        cell.push_str(&format!("</{prefix}c>"));
        cell
    }

    /// Grows `<dimension ref>` to hold the written cells. The element is
    /// optional, so a sheet without one does not get one.
    fn update_dimension(
        &mut self,
        element: &BytesStart<'_>,
        reader: &Reader<&[u8]>,
        start: usize,
        end: usize,
    ) -> Result<(), SheetWriteError> {
        let Some(written) = self.written_range else {
            return Ok(());
        };
        let Some(current) = attribute(element, b"ref", reader.decoder())?
            .as_deref()
            .and_then(CellRange::parse)
        else {
            return Ok(());
        };

        let grown = current.union(&written);
        if grown == current {
            return Ok(());
        }

        let self_closing = self
            .input
            .get(start..end)
            .is_some_and(|tag| tag.ends_with(b"/>"));
        let tag = rebuild_tag(element, Some(("ref", &grown.name())), &[], self_closing)?;
        self.replace(start, end);
        self.push(&tag);

        Ok(())
    }

    fn copy_to(&mut self, offset: usize) {
        if offset <= self.copied {
            return;
        }

        if let Some(bytes) = self.input.get(self.copied..offset) {
            self.output.extend_from_slice(bytes);
        }
        self.copied = offset;
    }

    /// Copies the input up to `start` and drops the input up to `end`.
    fn replace(&mut self, start: usize, end: usize) {
        self.copy_to(start);
        self.copied = self.copied.max(end);
    }

    fn push(&mut self, text: &str) {
        self.output.extend_from_slice(text.as_bytes());
    }
}

/// Reads the children of a `<c>` element up to its end tag, returning its
/// formula facts and the offset just past the end tag.
fn read_cell_body(reader: &mut Reader<&[u8]>) -> Result<(FormulaInfo, usize), SheetWriteError> {
    let mut formula = FormulaInfo::default();
    let mut depth = 0usize;

    loop {
        match reader.read_event()? {
            Event::Start(element) => {
                if depth == 0 && element.local_name().as_ref() == b"f" {
                    formula = formula_info(&element, reader)?;
                }
                depth += 1;
            }
            Event::Empty(element) if depth == 0 && element.local_name().as_ref() == b"f" => {
                formula = formula_info(&element, reader)?;
            }
            Event::End(_) => {
                if depth == 0 {
                    return Ok((formula, reader_position(reader)?));
                }
                depth -= 1;
            }
            Event::Eof => return Err(SheetWriteError::malformed("a cell element is not closed")),
            _ => {}
        }
    }
}

fn formula_info(
    element: &BytesStart<'_>,
    reader: &Reader<&[u8]>,
) -> Result<FormulaInfo, SheetWriteError> {
    Ok(FormulaInfo {
        formula_type: attribute(element, b"t", reader.decoder())?,
        range: attribute(element, b"ref", reader.decoder())?,
    })
}

/// Returns a row's zero-based index from its `r` attribute, or the row after
/// the previous one when the attribute is missing.
fn row_index(
    element: &BytesStart<'_>,
    reader: &Reader<&[u8]>,
    previous_row: Option<usize>,
) -> Result<usize, SheetWriteError> {
    match attribute(element, b"r", reader.decoder())? {
        Some(text) => text
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|row| row.checked_sub(1))
            .ok_or_else(|| SheetWriteError::malformed(format!("invalid row number `{text}`"))),
        None => Ok(previous_row.map_or(0, |row| row.saturating_add(1))),
    }
}

/// Returns an attribute's value exactly as the file writes it, for copying
/// it into a rewritten element.
fn raw_attribute(element: &BytesStart<'_>, key: &[u8]) -> Result<Option<String>, SheetWriteError> {
    for entry in element.attributes() {
        let entry = entry?;
        if entry.key.as_ref() == key {
            return Ok(Some(
                String::from_utf8_lossy(&entry.value).replace('"', "&quot;"),
            ));
        }
    }

    Ok(None)
}

fn prefix_of(name: QName<'_>) -> String {
    match name.prefix() {
        Some(prefix) => format!("{}:", String::from_utf8_lossy(prefix.as_ref())),
        None => String::new(),
    }
}
