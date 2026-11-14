//! Cell references and the XML text DBFlux writes for an edited value.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

/// The rows and columns of an xlsx sheet, `A1` to `XFD1048576`.
pub(crate) const MAX_ROWS: usize = 1_048_576;
pub(crate) const MAX_COLUMNS: usize = 16_384;

/// The most UTF-16 units a cell's text holds.
pub(crate) const MAX_TEXT_LENGTH: usize = 32_767;

const SECONDS_PER_DAY: f64 = 86_400.0;

/// Returns the A1 name of a zero-based position, such as `B3` for `(2, 1)`.
pub(crate) fn cell_name(row: usize, column: usize) -> String {
    format!("{}{}", column_name(column), row.saturating_add(1))
}

/// Returns the letters of a zero-based column, such as `AA` for 26.
pub(crate) fn column_name(column: usize) -> String {
    let mut letters = Vec::new();
    let mut remaining = column.saturating_add(1);

    while remaining > 0 {
        let offset = (remaining - 1) % 26;
        letters.push(char::from(b'A' + offset as u8));
        remaining = (remaining - 1) / 26;
    }

    letters.iter().rev().collect()
}

/// Parses an A1 reference such as `B3` or `$B$3` into a zero-based
/// `(row, column)`.
pub(crate) fn parse_cell_reference(text: &str) -> Option<(usize, usize)> {
    let text = text.trim().replace('$', "");
    let split = text.find(|character: char| character.is_ascii_digit())?;
    let (letters, digits) = text.split_at(split);

    if letters.is_empty() || !letters.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }

    let column = letters.bytes().try_fold(0usize, |column, letter| {
        let value = usize::from(letter.to_ascii_uppercase() - b'A') + 1;
        column.checked_mul(26)?.checked_add(value)
    })?;
    let row = digits.parse::<usize>().ok()?;

    Some((row.checked_sub(1)?, column.checked_sub(1)?))
}

/// A rectangle of cells, zero-based and inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CellRange {
    pub(crate) first_row: usize,
    pub(crate) first_column: usize,
    pub(crate) last_row: usize,
    pub(crate) last_column: usize,
}

impl CellRange {
    /// Parses `A1:C4`, or a single reference such as `B2`.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (first, last) = text.split_once(':').unwrap_or((text, text));
        let (first_row, first_column) = parse_cell_reference(first)?;
        let (last_row, last_column) = parse_cell_reference(last)?;

        Some(Self {
            first_row: first_row.min(last_row),
            first_column: first_column.min(last_column),
            last_row: first_row.max(last_row),
            last_column: first_column.max(last_column),
        })
    }

    pub(crate) fn single(row: usize, column: usize) -> Self {
        Self {
            first_row: row,
            first_column: column,
            last_row: row,
            last_column: column,
        }
    }

    pub(crate) fn union(&self, other: &Self) -> Self {
        Self {
            first_row: self.first_row.min(other.first_row),
            first_column: self.first_column.min(other.first_column),
            last_row: self.last_row.max(other.last_row),
            last_column: self.last_column.max(other.last_column),
        }
    }

    pub(crate) fn contains(&self, row: usize, column: usize) -> bool {
        (self.first_row..=self.last_row).contains(&row)
            && (self.first_column..=self.last_column).contains(&column)
    }

    pub(crate) fn name(&self) -> String {
        let first = cell_name(self.first_row, self.first_column);

        if self.first_row == self.last_row && self.first_column == self.last_column {
            return first;
        }

        format!("{first}:{}", cell_name(self.last_row, self.last_column))
    }
}

/// Writes a number in the shortest form that reads back as the same `f64`,
/// using an exponent only for magnitudes a plain decimal would spell out
/// with many zeros.
pub(crate) fn format_number(number: f64) -> String {
    if number == 0.0 {
        return "0".to_string();
    }

    if (1e-5..1e16).contains(&number.abs()) {
        return format!("{number}");
    }

    format!("{number:e}")
}

/// Converts a date to the serial number a cell stores: days since the
/// workbook's epoch, with the time of day as the fraction.
///
/// The 1900 system counts 1900-02-29, a day that never existed, so serials
/// from 1900-03-01 on are one higher than the day count. Returns `None` for
/// dates the date system cannot store.
pub(crate) fn date_serial(date: NaiveDateTime, date_1904: bool) -> Option<f64> {
    let last_storable = NaiveDate::from_ymd_opt(9999, 12, 31)?;
    if date.date() > last_storable {
        return None;
    }

    let (epoch, first_storable, leap_bug_from) = if date_1904 {
        let epoch = NaiveDate::from_ymd_opt(1904, 1, 1)?;
        (epoch, epoch, None)
    } else {
        (
            NaiveDate::from_ymd_opt(1899, 12, 31)?,
            NaiveDate::from_ymd_opt(1900, 1, 1)?,
            NaiveDate::from_ymd_opt(1900, 3, 1),
        )
    };

    if date.date() < first_storable {
        return None;
    }

    let elapsed = date.signed_duration_since(epoch.and_time(NaiveTime::MIN));
    let whole_seconds = elapsed.num_seconds() as f64;
    let fraction = f64::from(elapsed.subsec_nanos()) / 1e9;
    let mut serial = (whole_seconds + fraction) / SECONDS_PER_DAY;

    if leap_bug_from.is_some_and(|first_shifted| date.date() >= first_shifted) {
        serial += 1.0;
    }

    Some(serial)
}

/// Whether XML 1.0 can hold the character in element content.
pub(crate) fn is_xml_character(character: char) -> bool {
    matches!(
        character,
        '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..
    )
}

/// Escapes text for element content.
///
/// A carriage return is written as a character reference, because a parser
/// turns a literal one into a line feed. With `excel_escapes`, an underscore
/// that starts an `_xHHHH_` sequence is written as `_x005F_`, because
/// spreadsheet readers decode `_xHHHH_` in cell text as the character
/// `U+HHHH`.
pub(crate) fn escape_text(text: &str, excel_escapes: bool) -> String {
    let mut escaped = String::with_capacity(text.len());

    for (offset, character) in text.char_indices() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '\r' => escaped.push_str("&#13;"),
            '_' if excel_escapes && starts_excel_escape(text.get(offset..).unwrap_or("")) => {
                escaped.push_str("_x005F_");
            }
            other => escaped.push(other),
        }
    }

    escaped
}

fn starts_excel_escape(text: &str) -> bool {
    let bytes = text.as_bytes();

    bytes.get(1) == Some(&b'x')
        && bytes
            .get(2..6)
            .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
        && bytes.get(6) == Some(&b'_')
}
