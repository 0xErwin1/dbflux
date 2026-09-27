//! Typed operands: which editor a condition shows for its operator and field
//! type, and how the text typed into it reads as a [`DocumentValue`].

use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use dbflux_core::{DocumentFieldType, DocumentOperator, DocumentQueryCodec, DocumentValue};

/// How the connection's driver writes the values the builder types, taken
/// from its codec. The default has no object identifier literal and no
/// text that would run as another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ValueSyntax {
    object_id_wrapper: Option<(&'static str, &'static str)>,
    object_id_hex_digits: Option<usize>,
}

impl ValueSyntax {
    pub fn of(codec: &dyn DocumentQueryCodec) -> Self {
        Self {
            object_id_wrapper: codec.object_id_wrapper(),
            object_id_hex_digits: codec.object_id_hex_digits(),
        }
    }

    /// Text shown before and after an object identifier input.
    pub fn object_id_affixes(&self) -> Option<(String, String)> {
        self.object_id_wrapper
            .map(|(open, close)| (format!("{open}\""), format!("\"{close}")))
    }

    /// `hex` as written where a value of any type is accepted.
    fn format_object_id(&self, hex: &str) -> String {
        match self.object_id_wrapper {
            Some((open, close)) => format!("{open}\"{hex}\"{close}"),
            None => hex.to_string(),
        }
    }

    /// Whether `text` starts like a wrapped object identifier.
    fn opens_object_id(&self, text: &str) -> bool {
        self.object_id_wrapper
            .is_some_and(|(open, _)| text.starts_with(open))
    }

    /// The text inside the wrapper, or `text` when it is not wrapped.
    fn unwrap_object_id<'a>(&self, text: &'a str) -> &'a str {
        self.object_id_wrapper
            .and_then(|(open, close)| text.strip_prefix(open)?.strip_suffix(close))
            .map(str::trim)
            .unwrap_or(text)
    }

    fn is_object_id(&self, hex: &str) -> bool {
        let digits = hex.chars().all(|character| character.is_ascii_hexdigit());
        let length = match self.object_id_hex_digits {
            Some(expected) => hex.len() == expected,
            None => !hex.is_empty(),
        };

        digits && length
    }

    /// Whether `text` would not survive as a string: the driver would run
    /// it as an object identifier.
    fn text_changes_type(&self, text: &str) -> bool {
        self.object_id_hex_digits.is_some() && self.is_object_id(text)
    }
}

/// The type a single value is read as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarKind {
    Text,
    /// Integers and decimals: an integer literal reads as an integer.
    Number,
    Date,
    ObjectId,
    Bool,
    /// No known field type: the text decides (`true`, `42`, `"42"`, `Ada`).
    Auto,
}

impl ScalarKind {
    /// Kind of a field from its sampled types, most common first.
    pub fn for_types(types: &[DocumentFieldType]) -> Self {
        match types.first() {
            Some(DocumentFieldType::String) => ScalarKind::Text,
            Some(DocumentFieldType::Integer | DocumentFieldType::Decimal) => ScalarKind::Number,
            Some(DocumentFieldType::Date) => ScalarKind::Date,
            Some(DocumentFieldType::ObjectId) => ScalarKind::ObjectId,
            Some(DocumentFieldType::Bool) => ScalarKind::Bool,
            Some(DocumentFieldType::Array | DocumentFieldType::Object) | None => ScalarKind::Auto,
        }
    }

    /// Kind that keeps `value` the same type when its text is edited.
    pub fn of_value(value: &DocumentValue) -> Self {
        match value {
            DocumentValue::String(_) | DocumentValue::Regex { .. } => ScalarKind::Text,
            DocumentValue::Integer(_) | DocumentValue::Decimal(_) => ScalarKind::Number,
            DocumentValue::Date(_) => ScalarKind::Date,
            DocumentValue::ObjectId(_) => ScalarKind::ObjectId,
            DocumentValue::Bool(_) => ScalarKind::Bool,
            DocumentValue::List(items) => items.first().map_or(ScalarKind::Auto, Self::of_value),
            DocumentValue::Nested(_) | DocumentValue::Null => ScalarKind::Auto,
        }
    }
}

/// The control a condition shows for its operand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueEditor {
    /// One value typed as text.
    Scalar(ScalarKind),
    /// A true / false switch.
    Toggle,
    /// A list of values, one chip each.
    Chips(ScalarKind),
    /// A regular expression, `/pattern/flags` or a bare pattern.
    Pattern,
    /// A non-negative element count.
    Count,
    /// Conditions an array element must meet.
    Nested,
}

/// Editor for `operator` on a field read as `kind`.
pub fn value_editor(operator: DocumentOperator, kind: ScalarKind) -> ValueEditor {
    match operator {
        DocumentOperator::Exists => ValueEditor::Toggle,
        DocumentOperator::ElemMatch => ValueEditor::Nested,
        DocumentOperator::In | DocumentOperator::Nin | DocumentOperator::All => {
            ValueEditor::Chips(kind)
        }
        DocumentOperator::Regex => ValueEditor::Pattern,
        DocumentOperator::Size => ValueEditor::Count,
        DocumentOperator::Eq | DocumentOperator::Ne if kind == ScalarKind::Bool => {
            ValueEditor::Toggle
        }
        _ => ValueEditor::Scalar(kind),
    }
}

/// Operators offered for a field sampled with `types`: the type's own list
/// for one type, the union for several, every operator for a path the
/// sample never saw.
pub fn operator_choices(types: &[DocumentFieldType]) -> Vec<DocumentOperator> {
    match types {
        [] => DocumentOperator::ALL.to_vec(),
        [single] => single.operators().to_vec(),
        mixed => DocumentFieldType::operators_for(mixed),
    }
}

/// Why typed text is not a value of the expected kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueProblem {
    Empty,
    NotANumber,
    NotACount,
    NotADate,
    NotAnObjectId,
    NotABool,
    /// Text the driver would run as an object identifier, so it has to be
    /// entered as one.
    LooksLikeObjectId,
}

/// Reads `text` as a single value of `kind`.
pub fn parse_scalar(
    kind: ScalarKind,
    text: &str,
    syntax: ValueSyntax,
) -> Result<DocumentValue, ValueProblem> {
    match kind {
        ScalarKind::Text => parse_text(text, syntax),
        ScalarKind::Number => parse_number(text),
        ScalarKind::Date => parse_date(text).map(DocumentValue::Date),
        ScalarKind::ObjectId => parse_object_id(text, syntax),
        ScalarKind::Bool => match text.trim() {
            "true" => Ok(DocumentValue::Bool(true)),
            "false" => Ok(DocumentValue::Bool(false)),
            "" => Err(ValueProblem::Empty),
            _ => Err(ValueProblem::NotABool),
        },
        ScalarKind::Auto => parse_auto(text, syntax),
    }
}

/// Reads `text` as an element count.
pub fn parse_count(text: &str) -> Result<DocumentValue, ValueProblem> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ValueProblem::Empty);
    }

    trimmed
        .parse::<u32>()
        .map(|count| DocumentValue::Integer(i64::from(count)))
        .map_err(|_| ValueProblem::NotACount)
}

/// Reads `/pattern/flags`, or a bare pattern without flags.
pub fn parse_pattern(text: &str) -> Result<DocumentValue, ValueProblem> {
    if text.is_empty() {
        return Err(ValueProblem::Empty);
    }

    if let Some(body) = text.strip_prefix('/')
        && let Some((pattern, flags)) = body.rsplit_once('/')
        && flags.chars().all(|flag| flag.is_ascii_alphabetic())
    {
        return Ok(DocumentValue::Regex {
            pattern: pattern.to_string(),
            options: flags.to_string(),
        });
    }

    Ok(DocumentValue::Regex {
        pattern: text.to_string(),
        options: String::new(),
    })
}

/// Text that reads back as `value` through [`parse_scalar`] with `kind`, or
/// through [`parse_pattern`] for a regular expression.
pub fn format_value(value: &DocumentValue, kind: ScalarKind, syntax: ValueSyntax) -> String {
    match value {
        DocumentValue::String(text) => {
            let plain_reads_back = match kind {
                ScalarKind::Text => !is_quoted(text) && !text.is_empty(),
                _ => parse_auto(text, syntax).as_ref() == Ok(value),
            };
            if plain_reads_back {
                text.clone()
            } else {
                format!("\"{text}\"")
            }
        }
        DocumentValue::Integer(number) => number.to_string(),
        DocumentValue::Decimal(number) => number.to_string(),
        DocumentValue::Bool(flag) => flag.to_string(),
        DocumentValue::Null => "null".to_string(),
        DocumentValue::Date(date) => format_date(date),
        DocumentValue::ObjectId(hex) => match kind {
            ScalarKind::ObjectId => hex.clone(),
            _ => syntax.format_object_id(hex),
        },
        DocumentValue::Regex { pattern, options } => {
            if options.is_empty() && !pattern.starts_with('/') {
                pattern.clone()
            } else {
                format!("/{pattern}/{options}")
            }
        }
        DocumentValue::List(items) => {
            let parts: Vec<String> = items
                .iter()
                .map(|item| format_value(item, kind, syntax))
                .collect();
            format!("[{}]", parts.join(", "))
        }
        DocumentValue::Nested(_) => String::new(),
    }
}

/// A UTC timestamp as the date input shows it, dropping zero seconds.
pub fn format_date(date: &DateTime<Utc>) -> String {
    if date.nanosecond() != 0 {
        date.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    } else if date.second() != 0 {
        date.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        date.format("%Y-%m-%d %H:%M").to_string()
    }
}

fn is_quoted(text: &str) -> bool {
    text.len() >= 2 && text.starts_with('"') && text.ends_with('"')
}

fn unquote(text: &str) -> Option<&str> {
    unwrap_between(text, '"')
}

/// `text` without one `quote` at each end, when it has both.
fn unwrap_between(text: &str, quote: char) -> Option<&str> {
    text.strip_prefix(quote)
        .and_then(|inner| inner.strip_suffix(quote))
}

fn parse_text(text: &str, syntax: ValueSyntax) -> Result<DocumentValue, ValueProblem> {
    if text.is_empty() {
        return Err(ValueProblem::Empty);
    }

    let content = unquote(text).unwrap_or(text);
    if syntax.text_changes_type(content) {
        return Err(ValueProblem::LooksLikeObjectId);
    }

    Ok(DocumentValue::String(content.to_string()))
}

fn parse_number(text: &str) -> Result<DocumentValue, ValueProblem> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ValueProblem::Empty);
    }

    if let Ok(integer) = trimmed.parse::<i64>() {
        return Ok(DocumentValue::Integer(integer));
    }

    match trimmed.parse::<f64>() {
        Ok(decimal) if decimal.is_finite() => Ok(DocumentValue::Decimal(decimal)),
        _ => Err(ValueProblem::NotANumber),
    }
}

/// Reads a date in UTC. Accepts RFC 3339 and `YYYY-MM-DD[ HH:MM[:SS[.fff]]]`,
/// with an optional trailing `UTC`.
fn parse_date(text: &str) -> Result<DateTime<Utc>, ValueProblem> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ValueProblem::Empty);
    }

    if let Ok(date) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(date.with_timezone(&Utc));
    }

    let local = trimmed.strip_suffix("UTC").unwrap_or(trimmed).trim_end();

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(date) = NaiveDateTime::parse_from_str(local, format) {
            return Ok(date.and_utc());
        }
    }

    NaiveDate::parse_from_str(local, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|date| date.and_utc())
        .ok_or(ValueProblem::NotADate)
}

/// Reads an object identifier's hexadecimal digits, bare, quoted or inside
/// the driver's wrapper.
fn parse_object_id(text: &str, syntax: ValueSyntax) -> Result<DocumentValue, ValueProblem> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ValueProblem::Empty);
    }

    let inner = syntax.unwrap_object_id(trimmed);
    let hex = unquote(inner)
        .or_else(|| unwrap_between(inner, '\''))
        .unwrap_or(inner);

    if syntax.is_object_id(hex) {
        Ok(DocumentValue::ObjectId(hex.to_ascii_lowercase()))
    } else {
        Err(ValueProblem::NotAnObjectId)
    }
}

fn parse_auto(text: &str, syntax: ValueSyntax) -> Result<DocumentValue, ValueProblem> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ValueProblem::Empty);
    }

    if let Some(content) = unquote(trimmed) {
        return parse_text(content, syntax).or_else(|problem| match problem {
            ValueProblem::Empty => Ok(DocumentValue::String(String::new())),
            other => Err(other),
        });
    }

    match trimmed {
        "true" => return Ok(DocumentValue::Bool(true)),
        "false" => return Ok(DocumentValue::Bool(false)),
        "null" => return Ok(DocumentValue::Null),
        _ => {}
    }

    if syntax.opens_object_id(trimmed) {
        return parse_object_id(trimmed, syntax);
    }

    if let Ok(number) = parse_number(trimmed) {
        return Ok(number);
    }

    parse_text(trimmed, syntax)
}

/// Byte ranges of the `$operator` tokens in preview text, for highlighting.
pub fn operator_ranges(text: &str) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut characters = text.char_indices().peekable();

    while let Some((start, character)) = characters.next() {
        if character != '$' {
            continue;
        }

        let mut end = start + character.len_utf8();
        while let Some(&(index, next)) = characters.peek() {
            if next.is_ascii_alphanumeric() || next == '_' {
                end = index + next.len_utf8();
                characters.next();
            } else {
                break;
            }
        }

        if end > start + 1 {
            ranges.push(start..end);
        }
    }

    ranges
}
