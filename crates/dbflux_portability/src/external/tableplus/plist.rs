/// Minimal reader for Apple's XML property-list format, covering the value
/// types TablePlus writes. `real`, `date` and `data` are kept as their raw
/// text because no connection field this importer reads uses them.
use std::collections::HashMap;

use quick_xml::Reader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::Event;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PlistValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Array(Vec<PlistValue>),
    Dictionary(HashMap<String, PlistValue>),
}

/// Parses an XML plist document into its root value.
pub(super) fn parse(text: &str) -> Result<PlistValue, String> {
    let mut parser = Parser {
        reader: Reader::from_str(text),
    };

    loop {
        match parser.next_significant()? {
            Token::Start(name) if name == "plist" => break,
            Token::Start(name) | Token::Empty(name) => {
                return Err(format!("expected a <plist> root element, found <{name}>"));
            }
            Token::Eof => return Err("no <plist> root element".to_string()),
            Token::End(_) | Token::Text(_) => {}
        }
    }

    let root = parser.value()?;

    match parser.next_significant()? {
        Token::End(name) if name == "plist" => {}
        Token::Eof => return Err("unterminated <plist>".to_string()),
        _ => return Err("expected </plist> after the root value".to_string()),
    }

    match parser.next_significant()? {
        Token::Eof => Ok(root),
        _ => Err("unexpected content after </plist>".to_string()),
    }
}

enum Token {
    Start(String),
    Empty(String),
    End(String),
    Text(String),
    Eof,
}

struct Parser<'a> {
    reader: Reader<&'a [u8]>,
}

impl Parser<'_> {
    fn value(&mut self) -> Result<PlistValue, String> {
        match self.next_significant()? {
            Token::Start(name) => self.value_from_start(&name),
            Token::Empty(name) => empty_value(&name),
            Token::End(name) => Err(format!("expected a value, found </{name}>")),
            Token::Text(_) => Err("expected a value, found text".to_string()),
            Token::Eof => Err("unexpected end of document".to_string()),
        }
    }

    fn value_from_start(&mut self, name: &str) -> Result<PlistValue, String> {
        match name {
            "string" | "real" | "date" | "data" => Ok(PlistValue::String(self.text_until(name)?)),
            "integer" => {
                let text = self.text_until(name)?;
                text.trim()
                    .parse::<i64>()
                    .map(PlistValue::Integer)
                    .map_err(|_| format!("invalid integer '{text}'"))
            }
            "true" | "false" => {
                self.text_until(name)?;
                Ok(PlistValue::Boolean(name == "true"))
            }
            "array" => self.array(),
            "dict" => self.dictionary(),
            other => Err(format!("unsupported element <{other}>")),
        }
    }

    fn array(&mut self) -> Result<PlistValue, String> {
        let mut items = Vec::new();

        loop {
            match self.next_significant()? {
                Token::End(name) if name == "array" => return Ok(PlistValue::Array(items)),
                Token::Start(name) => items.push(self.value_from_start(&name)?),
                Token::Empty(name) => items.push(empty_value(&name)?),
                Token::End(name) => return Err(format!("unexpected </{name}> inside <array>")),
                Token::Text(_) => return Err("unexpected text inside <array>".to_string()),
                Token::Eof => return Err("unterminated <array>".to_string()),
            }
        }
    }

    fn dictionary(&mut self) -> Result<PlistValue, String> {
        let mut entries = HashMap::new();

        loop {
            match self.next_significant()? {
                Token::End(name) if name == "dict" => return Ok(PlistValue::Dictionary(entries)),
                Token::Start(name) if name == "key" => {
                    let key = self.text_until("key")?;
                    let value = self.value()?;
                    entries.insert(key, value);
                }
                Token::Empty(name) if name == "key" => {
                    let value = self.value()?;
                    entries.insert(String::new(), value);
                }
                Token::Start(name) | Token::Empty(name) => {
                    return Err(format!("expected <key> inside <dict>, found <{name}>"));
                }
                Token::End(name) => return Err(format!("unexpected </{name}> inside <dict>")),
                Token::Text(_) => return Err("unexpected text inside <dict>".to_string()),
                Token::Eof => return Err("unterminated <dict>".to_string()),
            }
        }
    }

    /// Collects text up to the closing tag of `name`, joining the separate
    /// text and entity-reference events quick-xml emits for one run of text.
    fn text_until(&mut self, name: &str) -> Result<String, String> {
        let mut text = String::new();

        loop {
            match self.next_token()? {
                Token::Text(chunk) => text.push_str(&chunk),
                Token::End(end) if end == name => return Ok(text),
                Token::Start(other) | Token::Empty(other) => {
                    return Err(format!("unexpected <{other}> inside <{name}>"));
                }
                Token::End(other) => return Err(format!("unexpected </{other}> inside <{name}>")),
                Token::Eof => return Err(format!("unterminated <{name}>")),
            }
        }
    }

    fn next_significant(&mut self) -> Result<Token, String> {
        loop {
            match self.next_token()? {
                Token::Text(text) if text.trim().is_empty() => {}
                token => return Ok(token),
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, String> {
        loop {
            let event = self.reader.read_event().map_err(|error| {
                format!(
                    "invalid XML at byte {}: {error}",
                    self.reader.error_position()
                )
            })?;

            let token = match event {
                Event::Start(start) => Token::Start(element_name(start.name().as_ref())),
                Event::Empty(start) => Token::Empty(element_name(start.name().as_ref())),
                Event::End(end) => Token::End(element_name(end.name().as_ref())),
                Event::Text(text) => Token::Text(
                    text.xml_content()
                        .map_err(|error| error.to_string())?
                        .into_owned(),
                ),
                Event::CData(data) => Token::Text(
                    data.xml_content()
                        .map_err(|error| error.to_string())?
                        .into_owned(),
                ),
                Event::GeneralRef(reference) => match reference.resolve_char_ref() {
                    Ok(Some(character)) => Token::Text(character.to_string()),
                    Ok(None) => {
                        let name = reference.decode().map_err(|error| error.to_string())?;
                        let resolved = resolve_predefined_entity(&name)
                            .ok_or_else(|| format!("unknown entity &{name};"))?;
                        Token::Text(resolved.to_string())
                    }
                    Err(error) => return Err(error.to_string()),
                },
                Event::Eof => Token::Eof,
                Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_) => continue,
            };

            return Ok(token);
        }
    }
}

fn element_name(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).into_owned()
}

fn empty_value(name: &str) -> Result<PlistValue, String> {
    match name {
        "string" | "real" | "date" | "data" => Ok(PlistValue::String(String::new())),
        "true" => Ok(PlistValue::Boolean(true)),
        "false" => Ok(PlistValue::Boolean(false)),
        "array" => Ok(PlistValue::Array(Vec::new())),
        "dict" => Ok(PlistValue::Dictionary(HashMap::new())),
        other => Err(format!("unsupported empty element <{other}/>")),
    }
}
