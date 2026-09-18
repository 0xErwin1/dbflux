//! Format detection, pretty-printing, and validation for single cell values.
//!
//! The value panel shows one cell at a time and lets the user read it as JSON,
//! XML, or plain text. Only the text transformations live here — the panel
//! that renders them is domain code in `dbflux_ui_document`.
//!
//! JSON delegates to [`crate::components::json_editor_view`] so the value
//! panel and the cell-editor modal agree on what valid JSON is. XML goes
//! through `quick-xml`, which re-emits the parsed event stream with indentation
//! rather than reformatting the text by hand — comments, CDATA, and attribute
//! values containing `>` survive the round trip.

use super::json_editor_view;

/// How the value panel interprets and formats a cell's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueFormat {
    Json,
    Xml,
    Text,
}

impl ValueFormat {
    /// Every format, in the order the panel's selector shows them.
    pub const ALL: [ValueFormat; 3] = [ValueFormat::Json, ValueFormat::Xml, ValueFormat::Text];

    /// Language id for `InputState::code_editor`, which drives syntax highlighting.
    pub fn editor_language(self) -> &'static str {
        match self {
            ValueFormat::Json => "json",
            ValueFormat::Xml => "xml",
            ValueFormat::Text => "text",
        }
    }

    /// Short label for the format selector. Not translated: these are format
    /// names, identical in every locale.
    pub fn label(self) -> &'static str {
        match self {
            ValueFormat::Json => "JSON",
            ValueFormat::Xml => "XML",
            ValueFormat::Text => "Text",
        }
    }

    /// Whether this format can pretty-print and compact.
    pub fn is_structured(self) -> bool {
        !matches!(self, ValueFormat::Text)
    }
}

/// Guess a format from the value itself.
///
/// Only a value that actually parses is reported as structured, so a text
/// column that happens to start with `{` does not open in JSON mode and
/// immediately show a parse error.
pub fn detect_format(value: &str) -> ValueFormat {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return ValueFormat::Text;
    }

    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<serde_json::Value>(trimmed).is_ok()
    {
        return ValueFormat::Json;
    }

    if trimmed.starts_with('<') && parse_xml(trimmed, XmlLayout::Indented).is_ok() {
        return ValueFormat::Xml;
    }

    ValueFormat::Text
}

/// Pretty-print `value`. Text is returned unchanged; malformed input reports
/// the parser's own message so the panel can show it verbatim.
pub fn format_value(value: &str, format: ValueFormat) -> Result<String, String> {
    match format {
        ValueFormat::Json => json_editor_view::format_json(value)
            .ok_or_else(|| json_parse_error(value).unwrap_or_default()),
        ValueFormat::Xml => parse_xml(value, XmlLayout::Indented),
        ValueFormat::Text => Ok(value.to_string()),
    }
}

/// Collapse `value` onto as few lines as the format allows.
pub fn compact_value(value: &str, format: ValueFormat) -> Result<String, String> {
    match format {
        ValueFormat::Json => json_editor_view::compact_json(value)
            .ok_or_else(|| json_parse_error(value).unwrap_or_default()),
        ValueFormat::Xml => parse_xml(value, XmlLayout::Compact),
        ValueFormat::Text => Ok(value.to_string()),
    }
}

/// Check that `value` is well-formed for `format`. An empty value is always
/// accepted: clearing a cell is how the user writes an empty string.
pub fn validate_value(value: &str, format: ValueFormat) -> Result<(), String> {
    if value.trim().is_empty() {
        return Ok(());
    }

    match format {
        ValueFormat::Json => json_editor_view::validate_json(value, true),
        ValueFormat::Xml => parse_xml(value, XmlLayout::Compact).map(|_| ()),
        ValueFormat::Text => Ok(()),
    }
}

fn json_parse_error(value: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(value)
        .err()
        .map(|error| error.to_string().replace('\n', " "))
}

#[derive(Clone, Copy)]
enum XmlLayout {
    Indented,
    Compact,
}

/// Re-emit the XML event stream with the requested layout.
///
/// Doubles as the XML validator: a document that cannot be read event by event
/// to EOF is not well-formed, and the reader's error carries the position.
fn parse_xml(value: &str, layout: XmlLayout) -> Result<String, String> {
    use quick_xml::events::Event;
    use quick_xml::{Reader, Writer};

    let mut reader = Reader::from_str(value);
    reader.config_mut().trim_text(true);

    let mut writer = match layout {
        XmlLayout::Indented => Writer::new_with_indent(Vec::new(), b' ', 2),
        XmlLayout::Compact => Writer::new(Vec::new()),
    };

    let mut saw_element = false;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(event) => {
                if matches!(event, Event::Start(_) | Event::Empty(_)) {
                    saw_element = true;
                }
                writer
                    .write_event(event)
                    .map_err(|error| error.to_string().replace('\n', " "))?;
            }
            Err(error) => return Err(error.to_string().replace('\n', " ")),
        }
    }

    // A reader fed plain text yields one Text event and no elements. That is
    // not an XML document, and silently echoing it back would let the panel
    // claim any string is valid XML.
    if !saw_element {
        return Err(dbflux_i18n::t!("components.value_panel.error.not_xml"));
    }

    String::from_utf8(writer.into_inner()).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_recognises_json_objects_and_arrays() {
        assert_eq!(detect_format(r#"{"a":1}"#), ValueFormat::Json);
        assert_eq!(detect_format("[1, 2, 3]"), ValueFormat::Json);
    }

    #[test]
    fn detect_falls_back_to_text_for_json_shaped_but_invalid_input() {
        // A text column starting with `{` must not open in JSON mode and
        // greet the user with a parse error.
        assert_eq!(detect_format("{not json"), ValueFormat::Text);
    }

    #[test]
    fn detect_recognises_xml() {
        assert_eq!(detect_format("<root><a>1</a></root>"), ValueFormat::Xml);
    }

    #[test]
    fn detect_treats_empty_as_text() {
        assert_eq!(detect_format("   "), ValueFormat::Text);
    }

    #[test]
    fn format_json_indents_and_compact_restores_one_line() {
        let compact = r#"{"a":1,"b":[2,3]}"#;

        let pretty = format_value(compact, ValueFormat::Json).expect("valid json");
        assert!(pretty.contains('\n'), "pretty json must span lines");

        let round_tripped = compact_value(&pretty, ValueFormat::Json).expect("valid json");
        assert_eq!(round_tripped, compact);
    }

    #[test]
    fn format_xml_indents_nested_elements() {
        let pretty =
            format_value("<root><a>1</a></root>", ValueFormat::Xml).expect("well-formed xml");
        assert!(pretty.contains('\n'), "pretty xml must span lines");
        assert!(pretty.contains("<a>1</a>"));
    }

    #[test]
    fn format_xml_preserves_attributes_containing_angle_brackets() {
        let source = r#"<root note="a &gt; b"><child/></root>"#;
        let pretty = format_value(source, ValueFormat::Xml).expect("well-formed xml");
        assert!(
            pretty.contains("a &gt; b"),
            "attribute must survive: {pretty}"
        );
    }

    #[test]
    fn malformed_xml_is_rejected_with_the_parser_message() {
        let error = format_value("<root><a></root>", ValueFormat::Xml).unwrap_err();
        assert!(!error.is_empty());
        assert!(!error.contains('\n'), "errors are shown on one line");
    }

    #[test]
    fn plain_text_is_not_accepted_as_xml() {
        assert!(validate_value("just a sentence", ValueFormat::Xml).is_err());
    }

    #[test]
    fn text_format_never_rewrites_or_rejects() {
        let value = "{ not json <not xml";
        assert_eq!(
            format_value(value, ValueFormat::Text).expect("text always formats"),
            value
        );
        assert!(validate_value(value, ValueFormat::Text).is_ok());
    }

    #[test]
    fn empty_values_validate_in_every_format() {
        for format in ValueFormat::ALL {
            assert!(
                validate_value("", format).is_ok(),
                "clearing a cell must be allowed in {:?}",
                format
            );
        }
    }
}
