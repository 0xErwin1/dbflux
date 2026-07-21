//! Table -> File `RowSink`: streams rows into one `schema.table.<ext>` file
//! per table via `dbflux_export`'s streaming writers. CSV reuses the exact
//! value-formatting single-shot export uses, so CSV output is byte-identical.
//! JSON is written as NDJSON (one compact object per line, see
//! `dbflux_export::JsonStreamWriter`) so `file_source::FileSource` can import
//! it back in bounded memory instead of buffering the whole file.

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use dbflux_core::{ColumnKind, ColumnMeta, TransferColumn};
use dbflux_export::{CsvStreamWriter, JsonStreamWriter};

use crate::pipeline::TransferReport;
use crate::pipeline::{RowChunk, RowSink, TableMappingMode, TransferError, TransferOutcome};

/// File format written by a [`FileSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Json,
}

impl FileFormat {
    /// All formats the export UI can offer, in display order.
    pub const ALL: [FileFormat; 2] = [Self::Csv, Self::Json];

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
        }
    }

    /// Human-readable label for format pickers.
    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
        }
    }

    /// Inverse of [`Self::extension`] — resolves the format a manifest table
    /// entry's `format` field (e.g. `"csv"`) refers to, for Import.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

enum StreamWriter {
    Csv(Box<CsvStreamWriter<BufWriter<File>>>),
    Json(JsonStreamWriter<BufWriter<File>>),
}

/// Writes one table's rows to `<dir>/<schema>.<table>.<ext>`.
///
/// `begin()`'s `mode: TableMappingMode` parameter is part of the shared
/// `RowSink` trait but has no meaning for a file target — a file is always
/// (re)created fresh, so it is ignored here.
pub struct FileSink {
    dir: PathBuf,
    schema: Option<String>,
    table: String,
    format: FileFormat,
    columns: Vec<ColumnMeta>,
    writer: Option<StreamWriter>,
    rows_written: u64,
}

impl FileSink {
    pub fn new(
        dir: impl Into<PathBuf>,
        schema: Option<String>,
        table: impl Into<String>,
        format: FileFormat,
    ) -> Self {
        Self {
            dir: dir.into(),
            schema,
            table: table.into(),
            format,
            columns: Vec::new(),
            writer: None,
            rows_written: 0,
        }
    }

    /// The `schema.table.ext` file name this sink writes, matching the name
    /// recorded in the export folder's `manifest.json`.
    pub fn file_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}.{}", self.table, self.format.extension()),
            None => format!("{}.{}", self.table, self.format.extension()),
        }
    }

    fn path(&self) -> PathBuf {
        self.dir.join(self.file_name())
    }
}

fn to_column_meta(col: &TransferColumn) -> ColumnMeta {
    ColumnMeta {
        name: col.name.clone(),
        type_name: col.type_name.clone().unwrap_or_default(),
        kind: ColumnKind::Unknown,
        nullable: col.nullable,
        is_primary_key: col.is_primary_key,
    }
}

impl RowSink for FileSink {
    fn begin(
        &mut self,
        columns: &[TransferColumn],
        _mode: TableMappingMode,
    ) -> Result<(), TransferError> {
        self.columns = columns.iter().map(to_column_meta).collect();

        let path = self.path();
        let file = File::create(&path)
            .map_err(|e| TransferError::Sink(format!("{}: {e}", path.display())))?;
        let writer = BufWriter::new(file);

        let mut stream = match self.format {
            FileFormat::Csv => StreamWriter::Csv(Box::new(CsvStreamWriter::new(writer))),
            FileFormat::Json => StreamWriter::Json(JsonStreamWriter::new(writer)),
        };

        match &mut stream {
            StreamWriter::Csv(w) => w
                .write_header(&self.columns)
                .map_err(|e| TransferError::Sink(e.to_string()))?,
            StreamWriter::Json(w) => w
                .write_header(&self.columns)
                .map_err(|e| TransferError::Sink(e.to_string()))?,
        }

        self.writer = Some(stream);
        Ok(())
    }

    fn write_chunk(&mut self, chunk: &RowChunk) -> Result<u64, TransferError> {
        let Some(writer) = self.writer.as_mut() else {
            return Err(TransferError::Sink(
                "write_chunk called before begin".to_string(),
            ));
        };

        for row in &chunk.0 {
            match writer {
                StreamWriter::Csv(w) => w
                    .write_row(row)
                    .map_err(|e| TransferError::Sink(e.to_string()))?,
                StreamWriter::Json(w) => w
                    .write_row(&self.columns, row)
                    .map_err(|e| TransferError::Sink(e.to_string()))?,
            }
        }

        let written = chunk.0.len() as u64;
        self.rows_written += written;
        Ok(written)
    }

    fn finish(&mut self) -> Result<TransferReport, TransferError> {
        if let Some(writer) = self.writer.take() {
            match writer {
                StreamWriter::Csv(w) => {
                    w.finish().map_err(|e| TransferError::Sink(e.to_string()))?
                }
                StreamWriter::Json(w) => {
                    w.finish().map_err(|e| TransferError::Sink(e.to_string()))?
                }
            }
        }

        let mut report = TransferReport::new(TransferOutcome::Completed);
        report.rows_transferred = self.rows_written;
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::Value;

    fn column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_file_sink_test_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn writes_csv_file_named_schema_table_and_reports_row_count() {
        let dir = temp_dir("csv");
        let columns = vec![column("id"), column("name")];
        let mut sink = FileSink::new(&dir, Some("public".to_string()), "users", FileFormat::Csv);

        sink.begin(&columns, TableMappingMode::Existing).unwrap();
        let written = sink
            .write_chunk(&RowChunk(vec![
                vec![Value::Int(1), Value::Text("Alice".to_string())],
                vec![Value::Int(2), Value::Text("Bob".to_string())],
            ]))
            .unwrap();
        assert_eq!(written, 2);

        let report = sink.finish().unwrap();
        assert_eq!(report.rows_transferred, 2);
        assert_eq!(report.outcome, TransferOutcome::Completed);

        let path = dir.join("public.users.csv");
        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "id,name\n1,Alice\n2,Bob\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writes_json_file_and_reports_row_count() {
        let dir = temp_dir("json");
        let columns = vec![column("id")];
        let mut sink = FileSink::new(&dir, None, "widgets", FileFormat::Json);

        sink.begin(&columns, TableMappingMode::Existing).unwrap();
        sink.write_chunk(&RowChunk(vec![vec![Value::Int(7)]]))
            .unwrap();
        let report = sink.finish().unwrap();

        assert_eq!(report.rows_transferred, 1);

        let path = dir.join("widgets.json");
        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "{\"id\":7}\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_name_omits_schema_when_none() {
        let sink = FileSink::new("/tmp/whatever", None, "orders", FileFormat::Csv);
        assert_eq!(sink.file_name(), "orders.csv");
    }

    #[test]
    fn from_extension_round_trips_with_extension() {
        assert_eq!(FileFormat::from_extension("csv"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_extension("json"), Some(FileFormat::Json));
        assert_eq!(FileFormat::from_extension("xml"), None);
    }

    #[test]
    fn all_lists_every_format_with_a_label() {
        assert_eq!(FileFormat::ALL, [FileFormat::Csv, FileFormat::Json]);
        assert_eq!(FileFormat::Csv.label(), "CSV");
        assert_eq!(FileFormat::Json.label(), "JSON");
    }

    #[test]
    fn file_name_includes_schema_when_present() {
        let sink = FileSink::new(
            "/tmp/whatever",
            Some("public".to_string()),
            "orders",
            FileFormat::Json,
        );
        assert_eq!(sink.file_name(), "public.orders.json");
    }

    #[test]
    fn empty_table_still_writes_header_only_file() {
        let dir = temp_dir("empty");
        let columns = vec![column("id")];
        let mut sink = FileSink::new(&dir, None, "empty_table", FileFormat::Csv);

        sink.begin(&columns, TableMappingMode::Existing).unwrap();
        let report = sink.finish().unwrap();

        assert_eq!(report.rows_transferred, 0);
        let contents = std::fs::read_to_string(dir.join("empty_table.csv")).unwrap();
        assert_eq!(contents, "id\n");

        std::fs::remove_dir_all(&dir).ok();
    }
}
