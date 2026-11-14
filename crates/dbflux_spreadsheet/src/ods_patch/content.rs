//! Rewrites the sheets of an ods `content.xml` by splicing edited cells and
//! rows into its original bytes, and finds where appended rows go.
//!
//! Elements are matched by their qualified names (`table:table-row`), the
//! prefixes every ods producer binds, as calamine does when it reads them.

use std::collections::BTreeMap;
use std::ops::Range;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

use crate::edits::CellEdit;
use crate::error::{FormulaRangeKind, SheetWriteError};
use crate::xlsx_patch::cell::{CellRange, cell_name};
use crate::xlsx_patch::package::{attribute, reader_position, rebuild_tag, splice, xml_reader};

use super::encode_cells;

const TABLE: &[u8] = b"table:table";
const ROW: &[u8] = b"table:table-row";
const CELL: &[u8] = b"table:table-cell";
const COVERED_CELL: &[u8] = b"table:covered-table-cell";

const ROWS_REPEATED: &[u8] = b"table:number-rows-repeated";
const COLUMNS_REPEATED: &[u8] = b"table:number-columns-repeated";
const ROWS_SPANNED: &[u8] = b"table:number-rows-spanned";
const MATRIX_ROWS_SPANNED: &[u8] = b"table:number-matrix-rows-spanned";
const MATRIX_COLUMNS_SPANNED: &[u8] = b"table:number-matrix-columns-spanned";
const FORMULA: &[u8] = b"table:formula";
const VALUE_TYPE: &[u8] = b"office:value-type";

/// The attributes that hold a cell's value, or a formula's cached result.
const VALUE_ATTRIBUTES: [&[u8]; 8] = [
    VALUE_TYPE,
    b"office:value",
    b"office:date-value",
    b"office:time-value",
    b"office:boolean-value",
    b"office:string-value",
    b"office:currency",
    b"calcext:value-type",
];

/// The attributes an edited cell drops before it takes its new value.
const REPLACED_ATTRIBUTES: [&[u8]; 10] = [
    VALUE_TYPE,
    b"office:value",
    b"office:date-value",
    b"office:time-value",
    b"office:boolean-value",
    b"office:string-value",
    b"office:currency",
    b"calcext:value-type",
    FORMULA,
    COLUMNS_REPEATED,
];

/// An edit, already validated and escaped for `content.xml`: the attributes
/// it adds to the cell's start tag, each with its leading space, and the
/// paragraphs it writes as the cell's text.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EncodedCell {
    pub(crate) attributes: String,
    pub(crate) body: String,
}

impl EncodedCell {
    fn is_clear(&self) -> bool {
        self.attributes.is_empty() && self.body.is_empty()
    }
}

/// Returns `content.xml` with the edits applied, keyed by sheet index.
///
/// Every formula cell of every sheet loses its cached result, because
/// LibreOffice shows the cached result of an ods formula instead of
/// computing it on load, and an edit can make any of them stale.
pub(crate) fn patch_content(
    xml: &[u8],
    edits: &BTreeMap<usize, BTreeMap<(usize, usize), CellEdit>>,
) -> Result<Vec<u8>, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut replacements: Vec<(Range<usize>, Vec<u8>)> = Vec::new();
    let mut table_count = 0usize;

    loop {
        match reader.read_event()? {
            Event::Start(element) if element.name().as_ref() == TABLE => {
                let name =
                    attribute(&element, b"table:name", reader.decoder())?.unwrap_or_default();
                let cells = match edits.get(&table_count) {
                    Some(cells) => encode_cells(&name, cells)?,
                    None => BTreeMap::new(),
                };

                let mut rewriter = TableRewriter {
                    input: xml,
                    sheet_name: &name,
                    cells: &cells,
                    replacements: &mut replacements,
                    matrix_ranges: Vec::new(),
                };
                rewriter.rewrite(&mut reader)?;

                table_count += 1;
            }
            Event::Empty(element) if element.name().as_ref() == TABLE => {
                if edits
                    .get(&table_count)
                    .is_some_and(|cells| !cells.is_empty())
                {
                    return Err(SheetWriteError::malformed(format!(
                        "sheet {table_count} has no rows to edit"
                    )));
                }
                table_count += 1;
            }
            Event::Eof => break,
            _ => {}
        }
    }

    if let Some((&index, _)) = edits
        .iter()
        .find(|&(&index, cells)| index >= table_count && !cells.is_empty())
    {
        return Err(SheetWriteError::SheetOutOfRange {
            index,
            sheet_count: table_count,
        });
    }

    Ok(splice(xml, &replacements))
}

/// Returns the zero-based row where rows appended to the sheet at `index`
/// go, or `None` when the document has no such sheet.
///
/// That is one past the last row that holds a non-empty cell (a value, a
/// formula, or any child element such as text, a note or an anchored
/// picture), a covered cell, or the end of a merged range. Rows of empty
/// cells after it, including the filler row LibreOffice writes down to the
/// last row of the sheet, do not count.
pub(crate) fn append_row(xml: &[u8], index: usize) -> Result<Option<usize>, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut table_count = 0usize;

    loop {
        match reader.read_event()? {
            Event::Start(element) if element.name().as_ref() == TABLE => {
                if table_count == index {
                    return last_used_row(&mut reader).map(Some);
                }

                reader.read_to_end(element.name())?;
                table_count += 1;
            }
            Event::Empty(element) if element.name().as_ref() == TABLE => {
                if table_count == index {
                    return Ok(Some(0));
                }
                table_count += 1;
            }
            Event::Eof => return Ok(None),
            _ => {}
        }
    }
}

fn last_used_row(reader: &mut Reader<&[u8]>) -> Result<usize, SheetWriteError> {
    let mut append_row = 0usize;

    walk_rows(reader, |row, first_row| {
        let last_row = first_row.saturating_add(row.repeat).saturating_sub(1);

        for cell in &row.cells {
            if cell.has_content || cell.covered {
                append_row = append_row.max(last_row.saturating_add(1));
            }

            if cell.rows_spanned > 1 {
                append_row = append_row.max(last_row.saturating_add(cell.rows_spanned));
            }
        }

        Ok(())
    })?;

    Ok(append_row)
}

/// Where a table's rows end, as [`walk_rows`] reports it.
struct TableEnd {
    /// The row after the last row element.
    next_row: usize,
    /// The offset just past the last row element, or the start of the
    /// table's end tag when it has no rows; new rows go there.
    insert_at: usize,
}

/// Calls `visit` with each row element of the table whose start tag was
/// just read, and the zero-based index of the first row it covers, up to
/// the table's end tag.
fn walk_rows<'a>(
    reader: &mut Reader<&'a [u8]>,
    mut visit: impl FnMut(&ParsedRow<'a>, usize) -> Result<(), SheetWriteError>,
) -> Result<TableEnd, SheetWriteError> {
    let mut next_row = 0usize;
    let mut last_row_end = None;

    loop {
        let start = reader_position(reader)?;
        let event = reader.read_event()?;
        let end = reader_position(reader)?;

        let (element, self_closing) = match event {
            Event::Start(element) if element.name().as_ref() == ROW => (element, false),
            Event::Empty(element) if element.name().as_ref() == ROW => (element, true),
            Event::End(element) if element.name().as_ref() == TABLE => {
                return Ok(TableEnd {
                    next_row,
                    insert_at: last_row_end.unwrap_or(start),
                });
            }
            Event::Eof => return Err(SheetWriteError::malformed("a table element is not closed")),
            _ => continue,
        };

        let row = parse_row(reader, element, start..end, self_closing)?;
        visit(&row, next_row)?;

        next_row = next_row.saturating_add(row.repeat);
        last_row_end = Some(row.span.end);
    }
}

/// A `<table:table-row>` element and the cells it holds.
struct ParsedRow<'a> {
    element: BytesStart<'a>,
    span: Range<usize>,
    start_tag: Range<usize>,
    /// The end tag; empty, at the end of the start tag, for `<table:table-row/>`.
    end_tag: Range<usize>,
    repeat: usize,
    cells: Vec<ParsedCell<'a>>,
}

/// A `<table:table-cell>` or `<table:covered-table-cell>` element.
struct ParsedCell<'a> {
    element: BytesStart<'a>,
    span: Range<usize>,
    /// The bytes between the start and end tags; `None` for a self-closing
    /// cell.
    inner: Option<Range<usize>>,
    covered: bool,
    repeat: usize,
    rows_spanned: usize,
    /// The rows and columns of the matrix formula whose result fills a range
    /// from this cell.
    matrix: Option<(usize, usize)>,
    children: Vec<Child>,
    has_content: bool,
    /// Whether the cell holds a formula with a cached result to drop.
    has_cached_result: bool,
}

/// A child element of a cell.
struct Child {
    span: Range<usize>,
    /// Text content (`text:p` and its siblings), which an edit replaces.
    /// Other children, such as notes and anchored drawings, stay.
    is_text: bool,
}

fn parse_row<'a>(
    reader: &mut Reader<&'a [u8]>,
    element: BytesStart<'a>,
    start_tag: Range<usize>,
    self_closing: bool,
) -> Result<ParsedRow<'a>, SheetWriteError> {
    let repeat = count_attribute(&element, ROWS_REPEATED, reader)?;
    let mut cells = Vec::new();

    if self_closing {
        return Ok(ParsedRow {
            element,
            span: start_tag.clone(),
            end_tag: start_tag.end..start_tag.end,
            start_tag,
            repeat,
            cells,
        });
    }

    loop {
        let start = reader_position(reader)?;
        let event = reader.read_event()?;
        let end = reader_position(reader)?;

        match event {
            Event::Start(cell) if is_cell(&cell) => {
                cells.push(parse_cell(reader, cell, start..end, false)?);
            }
            Event::Empty(cell) if is_cell(&cell) => {
                cells.push(parse_cell(reader, cell, start..end, true)?);
            }
            Event::Start(other) => {
                reader.read_to_end(other.name())?;
            }
            Event::End(end_element) if end_element.name().as_ref() == ROW => {
                return Ok(ParsedRow {
                    element,
                    span: start_tag.start..end,
                    start_tag,
                    end_tag: start..end,
                    repeat,
                    cells,
                });
            }
            Event::Eof => return Err(SheetWriteError::malformed("a table row is not closed")),
            _ => {}
        }
    }
}

fn is_cell(element: &BytesStart<'_>) -> bool {
    matches!(element.name().as_ref(), CELL | COVERED_CELL)
}

fn parse_cell<'a>(
    reader: &mut Reader<&'a [u8]>,
    element: BytesStart<'a>,
    start_tag: Range<usize>,
    self_closing: bool,
) -> Result<ParsedCell<'a>, SheetWriteError> {
    let covered = element.name().as_ref() == COVERED_CELL;
    let repeat = count_attribute(&element, COLUMNS_REPEATED, reader)?;
    let rows_spanned = count_attribute(&element, ROWS_SPANNED, reader)?;

    let matrix = if has_any_attribute(&element, &[MATRIX_ROWS_SPANNED, MATRIX_COLUMNS_SPANNED])? {
        Some((
            count_attribute(&element, MATRIX_ROWS_SPANNED, reader)?,
            count_attribute(&element, MATRIX_COLUMNS_SPANNED, reader)?,
        ))
    } else {
        None
    };

    let mut children = Vec::new();
    let mut inner = None;
    let mut span_end = start_tag.end;

    if !self_closing {
        loop {
            let start = reader_position(reader)?;
            let event = reader.read_event()?;
            let end = reader_position(reader)?;

            match event {
                Event::Start(child) => {
                    reader.read_to_end(child.name())?;
                    children.push(Child {
                        span: start..reader_position(reader)?,
                        is_text: is_text_element(&child),
                    });
                }
                Event::Empty(child) => children.push(Child {
                    span: start..end,
                    is_text: is_text_element(&child),
                }),
                Event::End(_) => {
                    inner = Some(start_tag.end..start);
                    span_end = end;
                    break;
                }
                Event::Eof => return Err(SheetWriteError::malformed("a table cell is not closed")),
                _ => {}
            }
        }
    }

    let has_formula = has_any_attribute(&element, &[FORMULA])?;
    let has_value = has_any_attribute(&element, &VALUE_ATTRIBUTES)?;
    let has_content =
        has_formula || has_any_attribute(&element, &[VALUE_TYPE])? || !children.is_empty();
    let has_cached_result =
        has_formula && (has_value || children.iter().any(|child| child.is_text));

    Ok(ParsedCell {
        span: start_tag.start..span_end,
        element,
        inner,
        covered,
        repeat,
        rows_spanned,
        matrix,
        children,
        has_content,
        has_cached_result,
    })
}

fn is_text_element(element: &BytesStart<'_>) -> bool {
    element.name().as_ref().starts_with(b"text:")
}

struct TableRewriter<'a, 'r> {
    input: &'a [u8],
    sheet_name: &'r str,
    cells: &'r BTreeMap<(usize, usize), EncodedCell>,
    replacements: &'r mut Vec<(Range<usize>, Vec<u8>)>,
    /// The ranges filled by matrix formulas seen so far. A matrix formula is
    /// in the range's top-left cell, which comes before every other cell of
    /// the range.
    matrix_ranges: Vec<CellRange>,
}

impl<'a> TableRewriter<'a, '_> {
    fn rewrite(&mut self, reader: &mut Reader<&'a [u8]>) -> Result<(), SheetWriteError> {
        let end = walk_rows(reader, |row, first_row| self.rewrite_row(row, first_row))?;

        self.insert_rows(end.next_row, end.insert_at);

        Ok(())
    }

    /// Splits a row element around its edited rows, writes their edits, and
    /// drops the cached results of its formula cells.
    fn rewrite_row(
        &mut self,
        row: &ParsedRow<'_>,
        first_row: usize,
    ) -> Result<(), SheetWriteError> {
        let end_row = first_row.saturating_add(row.repeat);
        self.record_matrix_ranges(row, first_row);

        let mut edited_rows: Vec<usize> = self
            .cells
            .range((first_row, 0)..(end_row, 0))
            .map(|(&(edit_row, _), _)| edit_row)
            .collect();
        edited_rows.dedup();

        if edited_rows.is_empty() {
            for cell in row.cells.iter().filter(|cell| cell.has_cached_result) {
                let copy = self.unchanged_cell(cell, cell.repeat)?;
                self.replacements.push((cell.span.clone(), copy));
            }
            return Ok(());
        }

        let mut output = Vec::new();
        let mut next_row = first_row;

        for edited_row in edited_rows {
            if edited_row > next_row {
                output.extend(self.row_copy(row, edited_row - next_row, None)?);
            }
            output.extend(self.row_copy(row, 1, Some(edited_row))?);
            next_row = edited_row + 1;
        }

        if next_row < end_row {
            output.extend(self.row_copy(row, end_row - next_row, None)?);
        }

        self.replacements.push((row.span.clone(), output));

        Ok(())
    }

    /// Writes `row` covering `count` rows, with the edits of `edited_row`
    /// applied when it is given.
    fn row_copy(
        &self,
        row: &ParsedRow<'_>,
        count: usize,
        edited_row: Option<usize>,
    ) -> Result<Vec<u8>, SheetWriteError> {
        let mut output = repeated_tag(&row.element, ROWS_REPEATED, count, false)?.into_bytes();

        let mut copied = row.start_tag.end;
        let mut column = 0usize;

        for cell in &row.cells {
            output.extend_from_slice(self.slice(copied..cell.span.start));
            copied = cell.span.end;

            let end_column = column.saturating_add(cell.repeat);
            let edits: Vec<(usize, &EncodedCell)> = match edited_row {
                Some(edited_row) => self
                    .cells
                    .range((edited_row, column)..(edited_row, end_column))
                    .map(|(&(_, edit_column), edit)| (edit_column, edit))
                    .collect(),
                None => Vec::new(),
            };

            if edits.is_empty() {
                output.extend(self.unchanged_cell(cell, cell.repeat)?);
                column = end_column;
                continue;
            }

            let edited_row = edited_row.unwrap_or_default();
            let mut next_column = column;

            for (edit_column, edit) in edits {
                if cell.covered {
                    return Err(SheetWriteError::CoveredCell {
                        sheet: self.sheet_name.to_string(),
                        cell: cell_name(edited_row, edit_column),
                    });
                }
                self.check_matrix_ranges(edited_row, edit_column)?;

                if edit_column > next_column {
                    output.extend(self.unchanged_cell(cell, edit_column - next_column)?);
                }
                output.extend(self.edited_cell(cell, edit)?);
                next_column = edit_column + 1;
            }

            if next_column < end_column {
                output.extend(self.unchanged_cell(cell, end_column - next_column)?);
            }

            column = end_column;
        }

        output.extend_from_slice(self.slice(copied..row.end_tag.start.max(copied)));

        if let Some(edited_row) = edited_row {
            self.check_new_cells(edited_row, column)?;
            output.extend(new_cells(self.cells, edited_row, column).into_bytes());
        }

        if row.end_tag.is_empty() {
            output.extend_from_slice(b"</table:table-row>");
        } else {
            output.extend_from_slice(self.slice(row.end_tag.clone()));
        }

        Ok(output)
    }

    /// Writes a cell that keeps its content, covering `count` columns,
    /// without its cached formula result.
    fn unchanged_cell(
        &self,
        cell: &ParsedCell<'_>,
        count: usize,
    ) -> Result<Vec<u8>, SheetWriteError> {
        if count == cell.repeat && !cell.has_cached_result {
            return Ok(self.slice(cell.span.clone()).to_vec());
        }

        if !cell.has_cached_result {
            let self_closing = cell.inner.is_none();
            let mut output =
                repeated_tag(&cell.element, COLUMNS_REPEATED, count, self_closing)?.into_bytes();

            if let Some(inner) = &cell.inner {
                output.extend_from_slice(self.slice(inner.clone()));
                output.extend(end_tag(&cell.element).into_bytes());
            }
            return Ok(output);
        }

        let mut remove: Vec<&[u8]> = VALUE_ATTRIBUTES.to_vec();
        let repeat_value = count.to_string();
        let set = if count == 1 {
            remove.push(COLUMNS_REPEATED);
            None
        } else {
            Some(("table:number-columns-repeated", repeat_value.as_str()))
        };

        let body = self.kept_children(cell);
        self.cell_with_body(cell, set, &remove, "", &body)
    }

    /// Writes the cell an edit replaces: it keeps its other attributes, such
    /// as its style, validation and merge, and its notes and drawings, and
    /// takes the edit's value and text.
    fn edited_cell(
        &self,
        cell: &ParsedCell<'_>,
        edit: &EncodedCell,
    ) -> Result<Vec<u8>, SheetWriteError> {
        let mut body = self.kept_children(cell);
        body.extend_from_slice(edit.body.as_bytes());

        self.cell_with_body(cell, None, &REPLACED_ATTRIBUTES, &edit.attributes, &body)
    }

    fn cell_with_body(
        &self,
        cell: &ParsedCell<'_>,
        set: Option<(&str, &str)>,
        remove: &[&[u8]],
        attributes: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, SheetWriteError> {
        let mut tag = rebuild_tag(&cell.element, set, remove, false)?;
        tag.pop();
        tag.push_str(attributes);

        if body.is_empty() {
            tag.push_str("/>");
            return Ok(tag.into_bytes());
        }

        tag.push('>');
        let mut output = tag.into_bytes();
        output.extend_from_slice(body);
        output.extend(end_tag(&cell.element).into_bytes());

        Ok(output)
    }

    fn kept_children(&self, cell: &ParsedCell<'_>) -> Vec<u8> {
        cell.children
            .iter()
            .filter(|child| !child.is_text)
            .flat_map(|child| self.slice(child.span.clone()).iter().copied())
            .collect()
    }

    /// Writes new rows for the edits past the table's last row element.
    fn insert_rows(&mut self, next_row: usize, insert_at: usize) {
        let mut output = String::new();
        let mut row = next_row;

        let mut edited_rows: Vec<usize> = self
            .cells
            .range((next_row, 0)..)
            .filter(|(_, edit)| !edit.is_clear())
            .map(|(&(edit_row, _), _)| edit_row)
            .collect();
        edited_rows.dedup();

        for edited_row in edited_rows {
            if edited_row > row {
                output.push_str(&repeated_row_of_empty_cells(edited_row - row));
            }

            output.push_str("<table:table-row>");
            output.push_str(&new_cells(self.cells, edited_row, 0));
            output.push_str("</table:table-row>");

            row = edited_row + 1;
        }

        if !output.is_empty() {
            self.replacements
                .push((insert_at..insert_at, output.into_bytes()));
        }
    }

    fn record_matrix_ranges(&mut self, row: &ParsedRow<'_>, first_row: usize) {
        let mut column = 0usize;

        for cell in &row.cells {
            if let Some((rows, columns)) = cell.matrix {
                self.matrix_ranges.push(CellRange {
                    first_row,
                    first_column: column,
                    last_row: first_row.saturating_add(rows.saturating_sub(1)),
                    last_column: column.saturating_add(columns.saturating_sub(1)),
                });
            }
            column = column.saturating_add(cell.repeat);
        }
    }

    fn check_matrix_ranges(&self, row: usize, column: usize) -> Result<(), SheetWriteError> {
        match self
            .matrix_ranges
            .iter()
            .find(|range| range.contains(row, column))
        {
            Some(range) => Err(SheetWriteError::InsideFormulaRange {
                sheet: self.sheet_name.to_string(),
                cell: cell_name(row, column),
                range: range.name(),
                kind: FormulaRangeKind::Array,
            }),
            None => Ok(()),
        }
    }

    fn check_new_cells(&self, row: usize, first_column: usize) -> Result<(), SheetWriteError> {
        for (&(_, column), _) in self.cells.range((row, first_column)..(row, usize::MAX)) {
            self.check_matrix_ranges(row, column)?;
        }
        Ok(())
    }

    fn slice(&self, range: Range<usize>) -> &'a [u8] {
        self.input.get(range).unwrap_or_default()
    }
}

/// Writes new cells for the edits of `row` at or past `first_column`, with
/// empty cells filling the columns between them.
fn new_cells(
    cells: &BTreeMap<(usize, usize), EncodedCell>,
    row: usize,
    first_column: usize,
) -> String {
    let mut output = String::new();
    let mut column = first_column;

    for (&(_, edit_column), edit) in cells.range((row, first_column)..(row, usize::MAX)) {
        if edit.is_clear() {
            continue;
        }

        if edit_column > column {
            output.push_str(&repeated_empty_cell(edit_column - column));
        }

        output.push_str("<table:table-cell");
        output.push_str(&edit.attributes);
        if edit.body.is_empty() {
            output.push_str("/>");
        } else {
            output.push('>');
            output.push_str(&edit.body);
            output.push_str("</table:table-cell>");
        }

        column = edit_column + 1;
    }

    output
}

fn repeated_empty_cell(count: usize) -> String {
    if count == 1 {
        return "<table:table-cell/>".to_string();
    }

    format!("<table:table-cell table:number-columns-repeated=\"{count}\"/>")
}

fn repeated_row_of_empty_cells(count: usize) -> String {
    if count == 1 {
        return "<table:table-row><table:table-cell/></table:table-row>".to_string();
    }

    format!(
        "<table:table-row table:number-rows-repeated=\"{count}\"><table:table-cell/></table:table-row>"
    )
}

/// Rebuilds a start tag so that it covers `count` rows or columns, through
/// the `key` repeat attribute that is dropped when `count` is one.
fn repeated_tag(
    element: &BytesStart<'_>,
    key: &[u8],
    count: usize,
    self_closing: bool,
) -> Result<String, SheetWriteError> {
    if count == 1 {
        return rebuild_tag(element, None, &[key], self_closing);
    }

    let value = count.to_string();
    let key = String::from_utf8_lossy(key);
    rebuild_tag(element, Some((&key, &value)), &[], self_closing)
}

fn end_tag(element: &BytesStart<'_>) -> String {
    format!("</{}>", String::from_utf8_lossy(element.name().as_ref()))
}

/// Reads a repeat or span count, which is one when the attribute is absent.
fn count_attribute(
    element: &BytesStart<'_>,
    key: &[u8],
    reader: &Reader<&[u8]>,
) -> Result<usize, SheetWriteError> {
    let Some(text) = attribute(element, key, reader.decoder())? else {
        return Ok(1);
    };

    match text.trim().parse::<usize>() {
        Ok(count) if count > 0 => Ok(count),
        _ => Err(SheetWriteError::malformed(format!(
            "invalid `{}` count `{text}`",
            String::from_utf8_lossy(key)
        ))),
    }
}

fn has_any_attribute(element: &BytesStart<'_>, keys: &[&[u8]]) -> Result<bool, SheetWriteError> {
    for entry in element.attributes() {
        if keys.contains(&entry?.key.as_ref()) {
            return Ok(true);
        }
    }

    Ok(false)
}
