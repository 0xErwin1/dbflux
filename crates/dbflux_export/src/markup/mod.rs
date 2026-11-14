//! Streaming writers for document formats meant to be read by people or other
//! tools rather than imported back: HTML, Markdown and XML.
//!
//! Each writer takes the column header once, then rows incrementally, so a
//! whole table streams through in bounded memory like CSV does. Cell text is
//! the same rendering CSV uses, escaped for the target format.

use crate::ExportError;
use crate::csv::value_to_csv_field;
use dbflux_core::{ColumnMeta, Value};
use std::io::Write;

/// Writes one HTML document holding a single table. Cells keep their line
/// breaks and repeated spaces (`white-space: pre-wrap`), so a multiline value
/// reads as stored.
pub struct HtmlStreamWriter<W: Write> {
    writer: W,
    title: String,
}

impl<W: Write> HtmlStreamWriter<W> {
    pub fn new(writer: W, title: impl Into<String>) -> Self {
        Self {
            writer,
            title: title.into(),
        }
    }

    pub fn write_header(&mut self, columns: &[ColumnMeta]) -> Result<(), ExportError> {
        write!(
            self.writer,
            "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>{}</title>\n<style>th, td {{ white-space: pre-wrap; }}</style>\n</head>\n<body>\n<table>\n<thead>\n<tr>",
            escape_markup(&self.title)
        )?;
        for column in columns {
            write!(self.writer, "<th>{}</th>", escape_markup(&column.name))?;
        }
        self.writer.write_all(b"</tr>\n</thead>\n<tbody>\n")?;
        Ok(())
    }

    pub fn write_row(&mut self, row: &[Value]) -> Result<(), ExportError> {
        self.writer.write_all(b"<tr>")?;
        for value in row {
            write!(
                self.writer,
                "<td>{}</td>",
                escape_markup(&value_to_csv_field(value))
            )?;
        }
        self.writer.write_all(b"</tr>\n")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        self.writer
            .write_all(b"</tbody>\n</table>\n</body>\n</html>\n")?;
        self.writer.flush()?;
        Ok(())
    }
}

/// Writes one GitHub Flavored Markdown table.
pub struct MarkdownStreamWriter<W: Write> {
    writer: W,
}

impl<W: Write> MarkdownStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write_header(&mut self, columns: &[ColumnMeta]) -> Result<(), ExportError> {
        let names: Vec<String> = columns
            .iter()
            .map(|column| escape_markdown_cell(&column.name))
            .collect();
        self.write_line(&names)?;
        let separators = vec!["---".to_string(); columns.len()];
        self.write_line(&separators)
    }

    pub fn write_row(&mut self, row: &[Value]) -> Result<(), ExportError> {
        let cells: Vec<String> = row
            .iter()
            .map(|value| escape_markdown_cell(&value_to_csv_field(value)))
            .collect();
        self.write_line(&cells)
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        self.writer.flush()?;
        Ok(())
    }

    fn write_line(&mut self, cells: &[String]) -> Result<(), ExportError> {
        writeln!(self.writer, "| {} |", cells.join(" | "))?;
        Ok(())
    }
}

/// Writes one XML document: a `<rows>` element holding a `<row>` per row and
/// a `<field name="...">` per column. Column names go in an attribute because
/// they need not be valid XML element names; a NULL is `null="true"`, so it
/// stays distinct from an empty string.
pub struct XmlStreamWriter<W: Write> {
    writer: W,
    column_names: Vec<String>,
}

impl<W: Write> XmlStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            column_names: Vec::new(),
        }
    }

    pub fn write_header(&mut self, columns: &[ColumnMeta]) -> Result<(), ExportError> {
        self.column_names = columns
            .iter()
            .map(|column| escape_xml(&column.name))
            .collect();
        self.writer
            .write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n")?;
        Ok(())
    }

    pub fn write_row(&mut self, row: &[Value]) -> Result<(), ExportError> {
        self.writer.write_all(b"<row>")?;
        for (name, value) in self.column_names.iter().zip(row) {
            if matches!(value, Value::Null) {
                write!(self.writer, "<field name=\"{name}\" null=\"true\"/>")?;
            } else {
                write!(
                    self.writer,
                    "<field name=\"{name}\">{}</field>",
                    escape_xml(&value_to_csv_field(value))
                )?;
            }
        }
        self.writer.write_all(b"</row>\n")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        self.writer.write_all(b"</rows>\n")?;
        self.writer.flush()?;
        Ok(())
    }
}

/// Escapes text for HTML content and attribute values.
pub(crate) fn escape_markup(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        push_markup_char(&mut escaped, character);
    }
    escaped
}

/// Escapes text for XML content and attribute values. Tab, line feed and
/// carriage return are written as character references: a parser normalizes
/// them to spaces inside an attribute and folds a literal carriage return in
/// text, so only a reference keeps the value as it was.
fn escape_xml(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\t' => escaped.push_str("&#9;"),
            '\n' => escaped.push_str("&#10;"),
            '\r' => escaped.push_str("&#13;"),
            character => push_markup_char(&mut escaped, character),
        }
    }
    escaped
}

/// Appends one character escaped for HTML or XML. A character XML 1.0 cannot
/// carry at all, such as most ASCII control characters, becomes U+FFFD,
/// because no escape makes it legal.
fn push_markup_char(escaped: &mut String, character: char) {
    match character {
        '&' => escaped.push_str("&amp;"),
        '<' => escaped.push_str("&lt;"),
        '>' => escaped.push_str("&gt;"),
        '"' => escaped.push_str("&quot;"),
        '\'' => escaped.push_str("&#39;"),
        character if is_xml_char(character) => escaped.push(character),
        _ => escaped.push(char::REPLACEMENT_CHARACTER),
    }
}

fn is_xml_char(character: char) -> bool {
    matches!(
        character,
        '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..
    )
}

/// Keeps a value inside one Markdown table cell and shows it as stored: a
/// pipe would end the cell, a line break would end the row, HTML and entities
/// would be rendered, and emphasis, code and link characters would format
/// the text. Line breaks become `<br>` only after everything else is escaped.
fn escape_markdown_cell(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '\\' | '|' | '*' | '_' | '`' | '~' | '[' | ']' => {
                escaped.push('\\');
                escaped.push(character);
            }
            '\r' => {
                if characters.peek() == Some(&'\n') {
                    characters.next();
                }
                escaped.push_str("<br>");
            }
            '\n' => escaped.push_str("<br>"),
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests;
