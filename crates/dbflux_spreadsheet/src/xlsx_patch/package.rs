//! The parts of an xlsx package the patch reads: the workbook, the
//! relationships that locate its sheets, and the zip entries behind them.

use std::io::{Read, Seek};
use std::ops::Range;

use quick_xml::encoding::Decoder;
use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, XmlVersion};
use zip::ZipArchive;

use crate::error::SheetWriteError;

pub(crate) const WORKBOOK_PART: &str = "xl/workbook.xml";

const UTF16_BYTE_ORDER_MARKS: [&[u8]; 2] = [b"\xFF\xFE", b"\xFE\xFF"];

/// The zip archive of an xlsx package.
pub(crate) struct Package<R> {
    archive: ZipArchive<R>,
}

impl<R: Read + Seek> Package<R> {
    pub(crate) fn open(reader: R) -> Result<Self, SheetWriteError> {
        let archive = ZipArchive::new(reader).map_err(SheetWriteError::from_read)?;

        Ok(Self { archive })
    }

    pub(crate) fn archive_mut(&mut self) -> &mut ZipArchive<R> {
        &mut self.archive
    }

    /// Returns the zip entry that stores a part. Part names compare without
    /// regard to ASCII case, so an exact match is tried first and a
    /// case-insensitive one after it.
    pub(crate) fn entry_name(&self, part: &str) -> Option<String> {
        if self.archive.index_for_name(part).is_some() {
            return Some(part.to_string());
        }

        self.archive
            .file_names()
            .find(|name| name.eq_ignore_ascii_case(part))
            .map(str::to_string)
    }

    /// Reads a part, or returns `None` when the package does not have it.
    pub(crate) fn read_part(&mut self, part: &str) -> Result<Option<Vec<u8>>, SheetWriteError> {
        let Some(entry_name) = self.entry_name(part) else {
            return Ok(None);
        };

        let mut entry = self
            .archive
            .by_name(&entry_name)
            .map_err(SheetWriteError::from_read)?;
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .map_err(SheetWriteError::from_read_io)?;

        Ok(Some(bytes))
    }

    pub(crate) fn require_part(&mut self, part: &str) -> Result<Vec<u8>, SheetWriteError> {
        self.read_part(part)?
            .ok_or_else(|| SheetWriteError::malformed(format!("the package has no `{part}` part")))
    }
}

/// What the patch needs from `xl/workbook.xml`.
pub(crate) struct WorkbookPart {
    pub(crate) sheets: Vec<SheetEntry>,
    pub(crate) date_1904: bool,
}

pub(crate) struct SheetEntry {
    pub(crate) name: String,
    pub(crate) relationship_id: String,
}

pub(crate) fn parse_workbook(xml: &[u8]) -> Result<WorkbookPart, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut sheets = Vec::new();
    let mut date_1904 = false;

    loop {
        match reader.read_event()? {
            Event::Start(element) | Event::Empty(element) => match element.local_name().as_ref() {
                b"workbookPr" => {
                    date_1904 = attribute(&element, b"date1904", reader.decoder())?
                        .is_some_and(|value| is_true(&value));
                }
                b"sheet" => sheets.push(parse_sheet_entry(&element, reader.decoder())?),
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(WorkbookPart { sheets, date_1904 })
}

fn parse_sheet_entry(
    element: &BytesStart<'_>,
    decoder: Decoder,
) -> Result<SheetEntry, SheetWriteError> {
    let name = attribute(element, b"name", decoder)?
        .ok_or_else(|| SheetWriteError::malformed("a workbook sheet has no name"))?;

    // The relationship id is `r:id`, but the prefix bound to the
    // relationships namespace is the producer's choice.
    let mut relationship_id = None;
    for entry in element.attributes() {
        let entry = entry?;
        if entry.key.prefix().is_some() && entry.key.local_name().as_ref() == b"id" {
            relationship_id = Some(
                entry
                    .decoded_and_normalized_value(XmlVersion::Implicit1_0, decoder)?
                    .into_owned(),
            );
        }
    }

    let relationship_id = relationship_id.ok_or_else(|| {
        SheetWriteError::malformed(format!("sheet `{name}` has no relationship id"))
    })?;

    Ok(SheetEntry {
        name,
        relationship_id,
    })
}

/// One `<Relationship>` of a `.rels` part.
pub(crate) struct Relationship {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) target: String,
    pub(crate) external: bool,
}

impl Relationship {
    /// Whether the relationship type ends with `/name`, which holds for both
    /// the transitional and the strict namespace of a type.
    pub(crate) fn is_kind(&self, name: &str) -> bool {
        self.kind
            .rsplit_once('/')
            .is_some_and(|(_, kind)| kind == name)
    }
}

pub(crate) fn parse_relationships(xml: &[u8]) -> Result<Vec<Relationship>, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut relationships = Vec::new();

    loop {
        match reader.read_event()? {
            Event::Start(element) | Event::Empty(element)
                if element.local_name().as_ref() == b"Relationship" =>
            {
                let decoder = reader.decoder();
                let required = |key: &[u8]| {
                    attribute(&element, key, decoder)?.ok_or_else(|| {
                        SheetWriteError::malformed(format!(
                            "a relationship has no `{}` attribute",
                            String::from_utf8_lossy(key)
                        ))
                    })
                };

                relationships.push(Relationship {
                    id: required(b"Id")?,
                    kind: required(b"Type")?,
                    target: required(b"Target")?,
                    external: attribute(&element, b"TargetMode", decoder)?
                        .is_some_and(|mode| mode == "External"),
                });
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(relationships)
}

/// Returns the `.rels` part that holds the relationships of `part`, such as
/// `xl/_rels/workbook.xml.rels` for `xl/workbook.xml`.
pub(crate) fn relationships_part(part: &str) -> String {
    match part.rsplit_once('/') {
        Some((directory, file)) => format!("{directory}/_rels/{file}.rels"),
        None => format!("_rels/{part}.rels"),
    }
}

/// Resolves a relationship target to a part name. A target that starts with
/// `/` is relative to the package root; any other is relative to the folder
/// of the part that owns the relationship.
pub(crate) fn resolve_target(source_part: &str, target: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();

    let relative = match target.strip_prefix('/') {
        Some(absolute) => absolute,
        None => {
            if let Some((directory, _)) = source_part.rsplit_once('/') {
                segments.extend(directory.split('/'));
            }
            target
        }
    };

    for segment in relative.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }

    segments.join("/")
}

/// Opens a reader over a part, refusing the UTF-16 encodings whose bytes the
/// patch could not splice UTF-8 text into.
pub(crate) fn xml_reader(xml: &[u8]) -> Result<Reader<&[u8]>, SheetWriteError> {
    if UTF16_BYTE_ORDER_MARKS
        .iter()
        .any(|mark| xml.starts_with(mark))
    {
        return Err(SheetWriteError::malformed(
            "the package has a UTF-16 part, which DBFlux does not patch",
        ));
    }

    Ok(Reader::from_reader(xml))
}

/// Returns an attribute's value, decoded and with entities resolved.
pub(crate) fn attribute(
    element: &BytesStart<'_>,
    key: &[u8],
    decoder: Decoder,
) -> Result<Option<String>, SheetWriteError> {
    for entry in element.attributes() {
        let entry = entry?;
        if entry.key.as_ref() == key {
            let value = entry.decoded_and_normalized_value(XmlVersion::Implicit1_0, decoder)?;
            return Ok(Some(value.into_owned()));
        }
    }

    Ok(None)
}

/// Reads an `xsd:boolean`, which is `1` or `true` when set.
pub(crate) fn is_true(value: &str) -> bool {
    matches!(value.trim(), "1" | "true")
}

/// Rebuilds a start or empty tag with one attribute set and others dropped,
/// keeping every other attribute's raw text and order.
pub(crate) fn rebuild_tag(
    element: &BytesStart<'_>,
    set: Option<(&str, &str)>,
    remove: &[&[u8]],
    self_closing: bool,
) -> Result<String, SheetWriteError> {
    let mut tag = format!("<{}", String::from_utf8_lossy(element.name().as_ref()));
    let mut set = set;

    for entry in element.attributes() {
        let entry = entry?;
        let key = entry.key.as_ref();

        if remove.contains(&key) {
            continue;
        }

        let value = match set {
            Some((set_key, set_value)) if set_key.as_bytes() == key => {
                set = None;
                set_value.to_string()
            }
            _ => String::from_utf8_lossy(&entry.value).replace('"', "&quot;"),
        };

        tag.push_str(&format!(" {}=\"{value}\"", String::from_utf8_lossy(key)));
    }

    if let Some((key, value)) = set {
        tag.push_str(&format!(" {key}=\"{value}\""));
    }

    tag.push_str(if self_closing { "/>" } else { ">" });

    Ok(tag)
}

/// Replaces byte ranges of `input`, given in increasing order and not
/// overlapping, with new text.
pub(crate) fn splice<T: AsRef<[u8]>>(input: &[u8], replacements: &[(Range<usize>, T)]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut copied = 0;

    for (range, text) in replacements {
        if let Some(bytes) = input.get(copied..range.start) {
            output.extend_from_slice(bytes);
        }
        output.extend_from_slice(text.as_ref());
        copied = copied.max(range.end);
    }

    if let Some(bytes) = input.get(copied..) {
        output.extend_from_slice(bytes);
    }

    output
}

/// Removes every `local_name` element that `matches` accepts, with its
/// content when it has any.
pub(crate) fn remove_elements(
    xml: &[u8],
    local_name: &[u8],
    matches: impl Fn(&BytesStart<'_>, Decoder) -> Result<bool, SheetWriteError>,
) -> Result<Vec<u8>, SheetWriteError> {
    let mut reader = xml_reader(xml)?;
    let mut removals = Vec::new();

    loop {
        let start = reader_position(&reader)?;

        match reader.read_event()? {
            Event::Empty(element)
                if element.local_name().as_ref() == local_name
                    && matches(&element, reader.decoder())? =>
            {
                removals.push((start..reader_position(&reader)?, String::new()));
            }
            Event::Start(element)
                if element.local_name().as_ref() == local_name
                    && matches(&element, reader.decoder())? =>
            {
                reader.read_to_end(element.name())?;
                removals.push((start..reader_position(&reader)?, String::new()));
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(splice(xml, &removals))
}

pub(crate) fn reader_position(reader: &Reader<&[u8]>) -> Result<usize, SheetWriteError> {
    usize::try_from(reader.buffer_position())
        .map_err(|_| SheetWriteError::malformed("a part is larger than memory can address"))
}
