//! Pure helpers for the format options of the export wizard: which options a
//! format shows, and the segment id and label of each choice. No GPUI.

use dbflux_export::{CsvDelimiter, DateFormat, TextEncoding};
use dbflux_transfer::FileFormat;

/// The option rows the Format phase shows for one file format. An option the
/// format does not use is hidden, so the dialog never offers a choice that
/// has no effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VisibleOptions {
    pub(super) delimiter_and_encoding: bool,
    pub(super) null_text: bool,
    pub(super) date_format: bool,
}

pub(super) fn visible_options(format: FileFormat) -> VisibleOptions {
    VisibleOptions {
        delimiter_and_encoding: format.uses_delimiter_and_encoding(),
        null_text: format.uses_null_text(),
        date_format: true,
    }
}

pub(super) fn delimiter_segment_id(delimiter: CsvDelimiter) -> &'static str {
    match delimiter {
        CsvDelimiter::Comma => "comma",
        CsvDelimiter::Semicolon => "semicolon",
        CsvDelimiter::Tab => "tab",
        CsvDelimiter::Pipe => "pipe",
    }
}

pub(super) fn delimiter_from_segment_id(segment_id: &str) -> Option<CsvDelimiter> {
    CsvDelimiter::ALL
        .into_iter()
        .find(|delimiter| delimiter_segment_id(*delimiter) == segment_id)
}

pub(super) fn delimiter_label(delimiter: CsvDelimiter) -> String {
    match delimiter {
        CsvDelimiter::Comma => {
            dbflux_i18n::t!("document.export_wizard.format_options.delimiter.comma")
        }
        CsvDelimiter::Semicolon => {
            dbflux_i18n::t!("document.export_wizard.format_options.delimiter.semicolon")
        }
        CsvDelimiter::Tab => dbflux_i18n::t!("document.export_wizard.format_options.delimiter.tab"),
        CsvDelimiter::Pipe => {
            dbflux_i18n::t!("document.export_wizard.format_options.delimiter.pipe")
        }
    }
}

pub(super) fn encoding_segment_id(encoding: TextEncoding) -> &'static str {
    match encoding {
        TextEncoding::Utf8 => "utf8",
        TextEncoding::Utf8Bom => "utf8-bom",
        TextEncoding::Utf16Le => "utf16le",
        TextEncoding::Windows1252 => "windows1252",
    }
}

pub(super) fn encoding_from_segment_id(segment_id: &str) -> Option<TextEncoding> {
    TextEncoding::ALL
        .into_iter()
        .find(|encoding| encoding_segment_id(*encoding) == segment_id)
}

pub(super) fn encoding_label(encoding: TextEncoding) -> String {
    match encoding {
        TextEncoding::Utf8 => {
            dbflux_i18n::t!("document.export_wizard.format_options.encoding.utf8")
        }
        TextEncoding::Utf8Bom => {
            dbflux_i18n::t!("document.export_wizard.format_options.encoding.utf8_bom")
        }
        TextEncoding::Utf16Le => {
            dbflux_i18n::t!("document.export_wizard.format_options.encoding.utf16le")
        }
        TextEncoding::Windows1252 => {
            dbflux_i18n::t!("document.export_wizard.format_options.encoding.windows1252")
        }
    }
}

pub(super) fn date_format_segment_id(date_format: DateFormat) -> &'static str {
    match date_format {
        DateFormat::Rfc3339 => "rfc3339",
        DateFormat::DateTime => "date-time",
        DateFormat::Epoch => "epoch",
    }
}

pub(super) fn date_format_from_segment_id(segment_id: &str) -> Option<DateFormat> {
    DateFormat::ALL
        .into_iter()
        .find(|date_format| date_format_segment_id(*date_format) == segment_id)
}

pub(super) fn date_format_label(date_format: DateFormat) -> String {
    match date_format {
        DateFormat::Rfc3339 => {
            dbflux_i18n::t!("document.export_wizard.format_options.date_format.rfc3339")
        }
        DateFormat::DateTime => {
            dbflux_i18n::t!("document.export_wizard.format_options.date_format.date_time")
        }
        DateFormat::Epoch => {
            dbflux_i18n::t!("document.export_wizard.format_options.date_format.epoch")
        }
    }
}

#[cfg(test)]
mod tests;
