//! Streaming reader for the XML shape DBFlux exports:
//!
//! ```xml
//! <?xml version="1.0" encoding="UTF-8"?>
//! <rows>
//!   <row><field name="id">1</field><field name="note" null="true"/></row>
//! </rows>
//! ```
//!
//! [`XmlRows`] yields one `<row>` at a time, so memory stays bounded by the
//! largest row rather than the file. Any other root or element is rejected
//! with an error that states the expected shape, and a malformed document
//! fails at the point it is read, wherever that is in the file.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use quick_xml::Reader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesStart, Event};

use crate::pipeline::TransferError;

const EXPECTED_SHAPE: &str =
    "expected <rows><row><field name=\"...\">text</field></row></rows>, the shape DBFlux exports";

/// One `<row>`: its fields in document order. `None` is a NULL
/// (`null="true"`); `Some("")` is a real empty string.
pub(crate) type XmlRow = Vec<(String, Option<String>)>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    /// Before the root element.
    Prolog,
    /// Inside `<rows>`.
    InRows,
    /// After `</rows>` (or an empty root).
    Done,
}

/// What one event contributes, owned so the reader's buffer is free again.
enum Token {
    Start(Element),
    Empty(Element),
    End(String),
    Text(String),
    Eof,
    /// A declaration, comment, processing instruction or doctype.
    Skip,
}

struct Element {
    name: String,
    field_name: Option<String>,
    null: bool,
}

pub(crate) struct XmlRows {
    reader: Reader<BufReader<File>>,
    buffer: Vec<u8>,
    state: State,
    label: String,
}

impl XmlRows {
    pub(crate) fn open(path: &Path) -> Result<Self, TransferError> {
        let file = File::open(path)
            .map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))?;

        Ok(Self {
            reader: Reader::from_reader(BufReader::new(file)),
            buffer: Vec::new(),
            state: State::Prolog,
            label: path.display().to_string(),
        })
    }

    /// The next row, or `None` after the closing `</rows>`.
    pub(crate) fn next_row(&mut self) -> Result<Option<XmlRow>, TransferError> {
        if self.state == State::Prolog {
            self.read_prolog()?;
        }

        while self.state == State::InRows {
            match self.next_token()? {
                Token::Skip => {}
                Token::Text(text) if text.trim().is_empty() => {}
                Token::Start(element) if element.name == "row" => {
                    return self.read_row().map(Some);
                }
                Token::Empty(element) if element.name == "row" => return Ok(Some(Vec::new())),
                Token::End(name) if name == "rows" => self.finish()?,
                Token::Start(element) | Token::Empty(element) => {
                    return Err(
                        self.shape_error(&format!("found <{}> inside <rows>", element.name))
                    );
                }
                Token::Text(_) => return Err(self.shape_error("unexpected text inside <rows>")),
                Token::End(name) => {
                    return Err(self.shape_error(&format!("unexpected </{name}>")));
                }
                Token::Eof => return Err(self.error("the document ends before </rows>")),
            }
        }

        Ok(None)
    }

    fn read_prolog(&mut self) -> Result<(), TransferError> {
        loop {
            match self.next_token()? {
                Token::Skip => {}
                Token::Text(text) if text.trim().is_empty() => {}
                Token::Start(element) if element.name == "rows" => {
                    self.state = State::InRows;
                    return Ok(());
                }
                Token::Empty(element) if element.name == "rows" => return self.finish(),
                Token::Start(element) | Token::Empty(element) => {
                    return Err(
                        self.shape_error(&format!("the root element is <{}>", element.name))
                    );
                }
                Token::Text(_) => {
                    return Err(self.shape_error("unexpected text before the root element"));
                }
                Token::End(name) => return Err(self.shape_error(&format!("unexpected </{name}>"))),
                Token::Eof => return Err(self.shape_error("the file has no root element")),
            }
        }
    }

    /// Reads to the end of the document, which may hold nothing but
    /// whitespace, comments and processing instructions after `</rows>`.
    fn finish(&mut self) -> Result<(), TransferError> {
        self.state = State::Done;

        loop {
            match self.next_token()? {
                Token::Eof => return Ok(()),
                Token::Skip => {}
                Token::Text(text) if text.trim().is_empty() => {}
                _ => return Err(self.error("unexpected content after </rows>")),
            }
        }
    }

    fn read_row(&mut self) -> Result<XmlRow, TransferError> {
        let mut fields = XmlRow::new();

        loop {
            match self.next_token()? {
                Token::Skip => {}
                Token::Text(text) if text.trim().is_empty() => {}
                Token::Start(element) if element.name == "field" => {
                    let name = self.field_name(element.field_name)?;
                    let text = self.read_field_text()?;
                    fields.push((name, (!element.null).then_some(text)));
                }
                Token::Empty(element) if element.name == "field" => {
                    let name = self.field_name(element.field_name)?;
                    fields.push((name, (!element.null).then(String::new)));
                }
                Token::End(name) if name == "row" => return Ok(fields),
                Token::Start(element) | Token::Empty(element) => {
                    return Err(self.shape_error(&format!("found <{}> inside <row>", element.name)));
                }
                Token::Text(_) => return Err(self.shape_error("unexpected text inside <row>")),
                Token::End(name) => return Err(self.shape_error(&format!("unexpected </{name}>"))),
                Token::Eof => return Err(self.error("the document ends inside a <row>")),
            }
        }
    }

    fn read_field_text(&mut self) -> Result<String, TransferError> {
        let mut text = String::new();

        loop {
            match self.next_token()? {
                Token::Skip => {}
                Token::Text(part) => text.push_str(&part),
                Token::End(name) if name == "field" => return Ok(text),
                Token::Start(element) | Token::Empty(element) => {
                    return Err(
                        self.shape_error(&format!("found <{}> inside <field>", element.name))
                    );
                }
                Token::End(name) => return Err(self.shape_error(&format!("unexpected </{name}>"))),
                Token::Eof => return Err(self.error("the document ends inside a <field>")),
            }
        }
    }

    fn field_name(&self, name: Option<String>) -> Result<String, TransferError> {
        name.ok_or_else(|| self.shape_error("a <field> without a name attribute"))
    }

    fn next_token(&mut self) -> Result<Token, TransferError> {
        self.buffer.clear();
        let event = match self.reader.read_event_into(&mut self.buffer) {
            Ok(event) => event,
            Err(error) => {
                return Err(TransferError::Source(format!(
                    "{}: invalid XML at byte {}: {error}",
                    self.label,
                    self.reader.error_position()
                )));
            }
        };

        let token = match event {
            Event::Start(start) => element(&start).map(Token::Start),
            Event::Empty(start) => element(&start).map(Token::Empty),
            Event::End(end) => Ok(Token::End(
                String::from_utf8_lossy(end.name().as_ref()).into_owned(),
            )),
            Event::Text(text) => text
                .xml_content()
                .map(|content| Token::Text(content.into_owned()))
                .map_err(|error| error.to_string()),
            Event::CData(data) => data
                .xml_content()
                .map(|content| Token::Text(content.into_owned()))
                .map_err(|error| error.to_string()),
            Event::GeneralRef(reference) => match reference.resolve_char_ref() {
                Ok(Some(character)) => Ok(Token::Text(character.to_string())),
                Ok(None) => reference
                    .decode()
                    .map_err(|error| error.to_string())
                    .and_then(|name| {
                        resolve_predefined_entity(&name)
                            .map(|text| Token::Text(text.to_string()))
                            .ok_or_else(|| format!("unknown entity &{name};"))
                    }),
                Err(error) => Err(error.to_string()),
            },
            Event::Eof => Ok(Token::Eof),
            Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_) => {
                Ok(Token::Skip)
            }
        };

        token.map_err(|message| self.error(&message))
    }

    fn error(&self, message: &str) -> TransferError {
        TransferError::Source(format!(
            "{}: invalid XML at byte {}: {message}",
            self.label,
            self.reader.buffer_position()
        ))
    }

    fn shape_error(&self, reason: &str) -> TransferError {
        TransferError::Source(format!("{}: {reason}; {EXPECTED_SHAPE}", self.label))
    }
}

fn element(start: &BytesStart<'_>) -> Result<Element, String> {
    let mut field_name = None;
    let mut null = false;

    for attribute in start.attributes() {
        let attribute = attribute.map_err(|error| error.to_string())?;
        let value = attribute
            .unescape_value()
            .map_err(|error| error.to_string())?;
        match attribute.key.as_ref() {
            b"name" => field_name = Some(value.into_owned()),
            b"null" => null = value == "true",
            _ => {}
        }
    }

    Ok(Element {
        name: String::from_utf8_lossy(start.name().as_ref()).into_owned(),
        field_name,
        null,
    })
}

/// The field names of the first `limit` rows, one list per row in document
/// order.
pub(crate) fn sample_field_names(
    path: &Path,
    limit: usize,
) -> Result<Vec<Vec<String>>, TransferError> {
    let mut rows = XmlRows::open(path)?;
    let mut names = Vec::new();

    while names.len() < limit {
        match rows.next_row()? {
            Some(row) => names.push(row.into_iter().map(|(name, _)| name).collect()),
            None => break,
        }
    }

    Ok(names)
}
