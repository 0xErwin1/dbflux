//! Pure helpers for the "Import file" entry: which formats the picker offers,
//! which options each one shows, and the segment id and label of each choice.
//! No GPUI.

use std::path::Path;

use dbflux_export::{CsvDelimiter, TextEncoding};
use dbflux_transfer::FileFormat;
use dbflux_transfer::import_file::format_for_path;

/// The option rows the wizard shows for one importable format. An option a
/// format does not use is hidden, so the dialog never offers a choice that has
/// no effect: CSV has all of them, JSON and JSON Lines none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VisibleFileOptions {
    pub(super) delimiter_and_encoding: bool,
    pub(super) null_text: bool,
}

pub(super) fn visible_file_options(format: FileFormat) -> VisibleFileOptions {
    VisibleFileOptions {
        delimiter_and_encoding: format.uses_delimiter_and_encoding(),
        null_text: format.is_importable() && format.uses_null_text(),
    }
}

/// File extensions the picker accepts: the importable formats only, so HTML,
/// Markdown, XML and SQL files are not offered.
pub(super) fn picker_extensions() -> Vec<&'static str> {
    FileFormat::WITH_SQL
        .into_iter()
        .filter(|format| format.is_importable())
        .map(FileFormat::extension)
        .collect()
}

/// The importable format of a picked file, from its extension.
pub(super) fn detect_format(path: &Path) -> Option<FileFormat> {
    format_for_path(path)
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

/// Labelled like the export wizard's options, so both dialogs read alike.
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

#[cfg(test)]
mod tests;
