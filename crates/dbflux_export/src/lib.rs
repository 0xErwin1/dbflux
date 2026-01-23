mod csv;

use dbflux_core::QueryResult;
use std::io::Write;
use thiserror::Error;

pub use csv::CsvExporter;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] ::csv::Error),

    #[error("Export failed: {0}")]
    Failed(String),
}

pub trait Exporter: Send + Sync {
    fn name(&self) -> &'static str;

    fn extension(&self) -> &'static str;

    fn export(&self, result: &QueryResult, writer: &mut dyn Write) -> Result<(), ExportError>;
}

pub fn available_exporters() -> &'static [&'static dyn Exporter] {
    &[&CsvExporter]
}
