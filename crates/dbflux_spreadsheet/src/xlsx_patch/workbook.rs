//! Package-level changes that come with patched sheets, and the row where
//! appended rows go.

use std::io::{Read, Seek};

use quick_xml::events::Event;

use crate::error::SheetWriteError;
use crate::xlsx_patch::cell::{CellRange, parse_cell_reference};
use crate::xlsx_patch::package::{
    Package, attribute, is_true, parse_relationships, reader_position, rebuild_tag,
    relationships_part, remove_elements, resolve_target, splice, xml_reader,
};

/// The elements `CT_Workbook` (ECMA-376 Part 1, 18.2.27) allows after
/// `calcPr`; a new `calcPr` goes before the first of them.
const AFTER_CALC_PROPERTIES: [&[u8]; 9] = [
    b"oleSize",
    b"customWorkbookViews",
    b"pivotCaches",
    b"smartTagPr",
    b"smartTagTypes",
    b"webPublishing",
    b"fileRecoveryPr",
    b"webPublishObjects",
    b"extLst",
];

/// Sets `fullCalcOnLoad="1"` on the workbook's `calcPr`, adding the element
/// in schema order when the workbook has none, so the spreadsheet
/// application recalculates the formulas whose cached values an edit made
/// stale. Returns `None` when the workbook already asks for it.
pub(crate) fn request_full_calculation(xml: &[u8]) -> Result<Option<Vec<u8>>, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut depth = 0usize;
    let mut root_prefix = String::new();

    loop {
        let start = reader_position(&reader)?;
        let event = reader.read_event()?;
        let end = reader_position(&reader)?;

        let (element, self_closing) = match event {
            Event::Start(element) => {
                depth += 1;
                (element, false)
            }
            Event::Empty(element) => (element, true),
            Event::End(_) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let calc_properties = new_calc_properties(&root_prefix);
                    return Ok(Some(splice(xml, &[(start..start, calc_properties)])));
                }
                continue;
            }
            Event::Eof => {
                return Err(SheetWriteError::malformed(
                    "the workbook part has no workbook element",
                ));
            }
            _ => continue,
        };

        // A start tag has already raised the depth to its children's level.
        let element_depth = if self_closing { depth + 1 } else { depth };
        let local_name = element.local_name();

        if element_depth == 1 {
            if let Some(prefix) = element.name().prefix() {
                root_prefix = format!("{}:", String::from_utf8_lossy(prefix.as_ref()));
            }
            continue;
        }

        if element_depth != 2 {
            continue;
        }

        if local_name.as_ref() == b"calcPr" {
            let current = attribute(&element, b"fullCalcOnLoad", reader.decoder())?;
            if current.as_deref().is_some_and(is_true) {
                return Ok(None);
            }

            let tag = rebuild_tag(&element, Some(("fullCalcOnLoad", "1")), &[], self_closing)?;
            return Ok(Some(splice(xml, &[(start..end, tag)])));
        }

        if AFTER_CALC_PROPERTIES.contains(&local_name.as_ref()) {
            let calc_properties = new_calc_properties(&root_prefix);
            return Ok(Some(splice(xml, &[(start..start, calc_properties)])));
        }
    }
}

fn new_calc_properties(prefix: &str) -> String {
    format!("<{prefix}calcPr fullCalcOnLoad=\"1\"/>")
}

/// Removes the relationships with the given ids from a `.rels` part.
pub(crate) fn remove_relationships(xml: &[u8], ids: &[&str]) -> Result<Vec<u8>, SheetWriteError> {
    remove_elements(xml, b"Relationship", |element, decoder| {
        Ok(attribute(element, b"Id", decoder)?.is_some_and(|id| ids.contains(&id.as_str())))
    })
}

/// Removes the `Override` of `part` from `[Content_Types].xml`.
pub(crate) fn remove_content_type_override(
    xml: &[u8],
    part: &str,
) -> Result<Vec<u8>, SheetWriteError> {
    let part_name = format!("/{part}");

    remove_elements(xml, b"Override", |element, decoder| {
        Ok(attribute(element, b"PartName", decoder)?
            .is_some_and(|name| name.eq_ignore_ascii_case(&part_name)))
    })
}

/// Returns the zero-based row where appended rows go: one past the last row
/// that holds a cell, a merged range or a table, so an appended row never
/// lands inside a merged range or a table and never grows one.
pub(crate) fn append_row<R: Read + Seek>(
    package: &mut Package<R>,
    sheet_part: &str,
    sheet_xml: &[u8],
) -> Result<usize, SheetWriteError> {
    let layout = scan_sheet(sheet_xml)?;
    let mut last_row = layout.last_row;

    if !layout.table_relationship_ids.is_empty() {
        let relationships_xml = package.require_part(&relationships_part(sheet_part))?;
        let relationships = parse_relationships(&relationships_xml)?;

        for id in &layout.table_relationship_ids {
            let relationship = relationships
                .iter()
                .find(|relationship| &relationship.id == id)
                .ok_or_else(|| {
                    SheetWriteError::malformed(format!(
                        "a table part points at relationship `{id}`, which the sheet does not have"
                    ))
                })?;

            let table_part = resolve_target(sheet_part, &relationship.target);
            let table_xml = package.require_part(&table_part)?;
            last_row = last_row.max(table_last_row(&table_xml)?);
        }
    }

    Ok(last_row.map_or(0, |row| row.saturating_add(1)))
}

struct SheetLayout {
    /// The last row that holds a cell or a merged range.
    last_row: Option<usize>,
    table_relationship_ids: Vec<String>,
}

fn scan_sheet(xml: &[u8]) -> Result<SheetLayout, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut layout = SheetLayout {
        last_row: None,
        table_relationship_ids: Vec::new(),
    };
    let mut current_row: Option<usize> = None;
    let mut in_sheet_data = false;

    loop {
        let element = match reader.read_event()? {
            Event::Start(element) => {
                if element.local_name().as_ref() == b"sheetData" {
                    in_sheet_data = true;
                }
                element
            }
            Event::Empty(element) => element,
            Event::End(element) => {
                if element.local_name().as_ref() == b"sheetData" {
                    in_sheet_data = false;
                }
                continue;
            }
            Event::Eof => break,
            _ => continue,
        };

        match element.local_name().as_ref() {
            b"row" if in_sheet_data => {
                let next = current_row.map_or(0, |row| row.saturating_add(1));
                current_row = match attribute(&element, b"r", reader.decoder())? {
                    Some(text) => text
                        .trim()
                        .parse::<usize>()
                        .ok()
                        .and_then(|row| row.checked_sub(1)),
                    None => Some(next),
                };
            }
            b"c" if in_sheet_data => {
                // A cell's own reference places it, as calamine reads it,
                // even inside a row whose position is only inferred.
                let row = attribute(&element, b"r", reader.decoder())?
                    .as_deref()
                    .and_then(parse_cell_reference)
                    .map(|(row, _)| row)
                    .or(current_row);

                layout.last_row = layout.last_row.max(row);
            }
            b"mergeCell" => {
                let range = attribute(&element, b"ref", reader.decoder())?
                    .as_deref()
                    .and_then(CellRange::parse);
                layout.last_row = layout.last_row.max(range.map(|range| range.last_row));
            }
            b"tablePart" => {
                for entry in element.attributes() {
                    let entry = entry?;
                    if entry.key.prefix().is_some() && entry.key.local_name().as_ref() == b"id" {
                        layout
                            .table_relationship_ids
                            .push(String::from_utf8_lossy(&entry.value).into_owned());
                    }
                }
            }
            _ => {}
        }
    }

    Ok(layout)
}

fn table_last_row(xml: &[u8]) -> Result<Option<usize>, SheetWriteError> {
    let mut reader = xml_reader(xml)?;

    loop {
        match reader.read_event()? {
            Event::Start(element) | Event::Empty(element)
                if element.local_name().as_ref() == b"table" =>
            {
                let range = attribute(&element, b"ref", reader.decoder())?
                    .as_deref()
                    .and_then(CellRange::parse);
                return Ok(range.map(|range| range.last_row));
            }
            Event::Eof => return Ok(None),
            _ => {}
        }
    }
}
