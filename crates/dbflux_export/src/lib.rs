mod binary;
mod csv;
mod json;
mod text;

use dbflux_core::{QueryResult, QueryResultShape};
use std::io::Write;
use thiserror::Error;

pub use binary::{BinaryExportMode, BinaryExporter};
pub use csv::CsvExporter;
pub use json::JsonExporter;
pub use text::TextExporter;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] ::csv::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Export failed: {0}")]
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    JsonPretty,
    JsonCompact,
    Text,
    Binary,
    Hex,
    Base64,
}

impl ExportFormat {
    pub fn name(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::JsonPretty => "JSON (pretty)",
            Self::JsonCompact => "JSON (compact)",
            Self::Text => "Text",
            Self::Binary => "Binary",
            Self::Hex => "Hex",
            Self::Base64 => "Base64",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonPretty | Self::JsonCompact => "json",
            Self::Text => "txt",
            Self::Binary => "bin",
            Self::Hex => "hex",
            Self::Base64 => "b64",
        }
    }
}

pub fn available_formats(shape: &QueryResultShape) -> &'static [ExportFormat] {
    match shape {
        QueryResultShape::Table => &[
            ExportFormat::Csv,
            ExportFormat::JsonPretty,
            ExportFormat::JsonCompact,
        ],
        QueryResultShape::Json => &[
            ExportFormat::JsonPretty,
            ExportFormat::JsonCompact,
            ExportFormat::Csv,
        ],
        QueryResultShape::Text => &[ExportFormat::Text, ExportFormat::JsonPretty],
        QueryResultShape::Binary => &[
            ExportFormat::Binary,
            ExportFormat::Hex,
            ExportFormat::Base64,
        ],
    }
}

pub fn export(
    result: &QueryResult,
    format: ExportFormat,
    writer: &mut dyn Write,
) -> Result<(), ExportError> {
    match format {
        ExportFormat::Csv => CsvExporter.export(result, writer),
        ExportFormat::JsonPretty => JsonExporter { pretty: true }.export(result, writer),
        ExportFormat::JsonCompact => JsonExporter { pretty: false }.export(result, writer),
        ExportFormat::Text => TextExporter.export(result, writer),
        ExportFormat::Binary => BinaryExporter {
            mode: BinaryExportMode::Raw,
        }
        .export(result, writer),
        ExportFormat::Hex => BinaryExporter {
            mode: BinaryExportMode::Hex,
        }
        .export(result, writer),
        ExportFormat::Base64 => BinaryExporter {
            mode: BinaryExportMode::Base64,
        }
        .export(result, writer),
    }
}
