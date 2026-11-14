//! Per-format export options: CSV delimiter and encoding, NULL text and the
//! date format. Every default reproduces the output the exporters wrote before
//! these options existed, byte for byte.

use dbflux_core::Value;
use serde::{Deserialize, Serialize};

/// Field separator for CSV output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CsvDelimiter {
    #[default]
    Comma,
    Semicolon,
    Tab,
    Pipe,
}

impl CsvDelimiter {
    pub const ALL: [CsvDelimiter; 4] = [Self::Comma, Self::Semicolon, Self::Tab, Self::Pipe];

    pub fn byte(self) -> u8 {
        match self {
            Self::Comma => b',',
            Self::Semicolon => b';',
            Self::Tab => b'\t',
            Self::Pipe => b'|',
        }
    }
}

/// Text encoding of a CSV file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextEncoding {
    #[default]
    Utf8,
    /// UTF-8 preceded by the `EF BB BF` byte order mark.
    Utf8Bom,
    /// UTF-16 little endian preceded by the `FF FE` byte order mark.
    Utf16Le,
    Windows1252,
}

impl TextEncoding {
    pub const ALL: [TextEncoding; 4] =
        [Self::Utf8, Self::Utf8Bom, Self::Utf16Le, Self::Windows1252];
}

/// How date, time and timestamp values are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateFormat {
    /// RFC 3339 timestamps, `YYYY-MM-DD` dates and `HH:MM:SS[.f]` times.
    #[default]
    Rfc3339,
    /// `YYYY-MM-DD HH:MM:SS` timestamps (UTC), `YYYY-MM-DD` dates and
    /// `HH:MM:SS` times.
    DateTime,
    /// Integer seconds since the Unix epoch. A date is its midnight (UTC) and
    /// a time is its seconds since midnight.
    Epoch,
}

impl DateFormat {
    pub const ALL: [DateFormat; 3] = [Self::Rfc3339, Self::DateTime, Self::Epoch];

    /// Renders a date, time or timestamp value, or `None` for any other value.
    /// RFC 3339 renders as it always has.
    pub(crate) fn render(self, value: &Value) -> Option<String> {
        let rendered = match (self, value) {
            (Self::Rfc3339, Value::DateTime(timestamp)) => timestamp.to_rfc3339(),
            (Self::Rfc3339, Value::Date(date)) | (Self::DateTime, Value::Date(date)) => {
                date.format("%Y-%m-%d").to_string()
            }
            (Self::Rfc3339, Value::Time(time)) => time.format("%H:%M:%S%.f").to_string(),
            (Self::DateTime, Value::DateTime(timestamp)) => {
                timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
            }
            (Self::DateTime, Value::Time(time)) => time.format("%H:%M:%S").to_string(),
            (Self::Epoch, _) => {
                return self.epoch_seconds(value).map(|seconds| seconds.to_string());
            }
            _ => return None,
        };
        Some(rendered)
    }

    /// The value as epoch seconds, for the `Epoch` format only.
    pub(crate) fn epoch_seconds(self, value: &Value) -> Option<i64> {
        use dbflux_core::chrono::Timelike;

        if self != Self::Epoch {
            return None;
        }
        match value {
            Value::DateTime(timestamp) => Some(timestamp.timestamp()),
            Value::Date(date) => Some(
                date.and_time(dbflux_core::chrono::NaiveTime::MIN)
                    .and_utc()
                    .timestamp(),
            ),
            Value::Time(time) => Some(i64::from(time.num_seconds_from_midnight())),
            _ => None,
        }
    }
}

/// Everything an export can vary per file. A format ignores the options it
/// does not use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct FormatOptions {
    pub delimiter: CsvDelimiter,
    pub encoding: TextEncoding,
    /// Text written for NULL in CSV, HTML and Markdown. Empty by default.
    pub null_text: String,
    pub date_format: DateFormat,
    /// Rows per `INSERT` statement in a SQL export. The driver's own limit
    /// caps it when it sets one. Omitted from a manifest at its default, so a
    /// manifest records only what shaped the file.
    #[serde(skip_serializing_if = "is_default_sql_batch_size")]
    pub sql_batch_size: u32,
}

fn is_default_sql_batch_size(size: &u32) -> bool {
    *size == DEFAULT_SQL_BATCH_SIZE
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            delimiter: CsvDelimiter::default(),
            encoding: TextEncoding::default(),
            null_text: String::new(),
            date_format: DateFormat::default(),
            sql_batch_size: DEFAULT_SQL_BATCH_SIZE,
        }
    }
}

/// Rows per `INSERT` statement unless the export chooses another size.
pub const DEFAULT_SQL_BATCH_SIZE: u32 = 500;

impl FormatOptions {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod tests;
