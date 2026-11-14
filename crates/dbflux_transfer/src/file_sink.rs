//! Table -> File `RowSink`: streams rows into one `schema.table.<ext>` file
//! per table via `dbflux_export`'s streaming writers. CSV reuses the exact
//! value-formatting single-shot export uses, so CSV output is byte-identical.
//! JSON is one array with an object per line and JSON Lines is NDJSON (see
//! `dbflux_export::JsonArrayStreamWriter` and `JsonLinesStreamWriter`); both
//! are read back by `file_source::FileSource` in bounded memory.

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use dbflux_core::{ColumnKind, ColumnMeta, TransferColumn, Value};
use dbflux_export::{
    CsvStreamWriter, FormatOptions, HtmlStreamWriter, JsonArrayStreamWriter, JsonLinesStreamWriter,
    MarkdownStreamWriter, XmlStreamWriter,
};

use crate::pipeline::TransferReport;
use crate::pipeline::{RowChunk, RowSink, TableMappingMode, TransferError, TransferOutcome};

/// File format written by a [`FileSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    /// One JSON array. Export folders written before JSON Lines existed
    /// record NDJSON under this format, so the reader accepts both.
    Json,
    JsonLines,
    Html,
    Markdown,
    Xml,
}

impl FileFormat {
    /// All formats the export UI can offer, in display order.
    pub const ALL: [FileFormat; 6] = [
        Self::Csv,
        Self::Json,
        Self::JsonLines,
        Self::Html,
        Self::Markdown,
        Self::Xml,
    ];

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::JsonLines => "jsonl",
            Self::Html => "html",
            Self::Markdown => "md",
            Self::Xml => "xml",
        }
    }

    /// Human-readable label for format pickers.
    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
            Self::JsonLines => "JSON Lines",
            Self::Html => "HTML",
            Self::Markdown => "Markdown",
            Self::Xml => "XML",
        }
    }

    /// Inverse of [`Self::extension`] — resolves the format a manifest table
    /// entry's `format` field (e.g. `"csv"`) refers to, for Import.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            "jsonl" => Some(Self::JsonLines),
            "html" => Some(Self::Html),
            "md" => Some(Self::Markdown),
            "xml" => Some(Self::Xml),
            _ => None,
        }
    }

    /// Whether the CSV delimiter and encoding options apply.
    pub fn uses_delimiter_and_encoding(self) -> bool {
        matches!(self, Self::Csv)
    }

    /// Whether the NULL text option applies. JSON writes `null` and XML marks
    /// NULL with an attribute, so neither has a text to choose.
    pub fn uses_null_text(self) -> bool {
        matches!(self, Self::Csv | Self::Html | Self::Markdown)
    }

    /// `options` with every option this format does not use reset to its
    /// default, so a manifest records only what shaped the file. The date
    /// format applies to every format.
    pub fn applicable_options(self, options: &FormatOptions) -> FormatOptions {
        let defaults = FormatOptions::default();
        let mut applicable = options.clone();
        if !self.uses_delimiter_and_encoding() {
            applicable.delimiter = defaults.delimiter;
            applicable.encoding = defaults.encoding;
        }
        if !self.uses_null_text() {
            applicable.null_text = defaults.null_text;
        }
        applicable
    }

    /// Whether Import can read this format back. HTML and Markdown are for
    /// people to read, and XML import is not implemented yet.
    pub fn is_importable(self) -> bool {
        matches!(self, Self::Csv | Self::Json | Self::JsonLines)
    }
}

enum StreamWriter {
    Csv(Box<CsvStreamWriter<BufWriter<File>>>),
    Json(JsonArrayStreamWriter<BufWriter<File>>),
    JsonLines(JsonLinesStreamWriter<BufWriter<File>>),
    Html(HtmlStreamWriter<BufWriter<File>>),
    Markdown(MarkdownStreamWriter<BufWriter<File>>),
    Xml(XmlStreamWriter<BufWriter<File>>),
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
    /// Set by [`FileSink::to_file`]; overrides the `schema.table.ext` name.
    target_path: Option<PathBuf>,
    columns: Vec<ColumnMeta>,
    writer: Option<StreamWriter>,
    rows_written: u64,
    /// JSON rows that are one `Value::Document` are written as that
    /// document. CSV ignores it: its header is fixed.
    whole_documents: bool,
    options: FormatOptions,
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
            target_path: None,
            columns: Vec::new(),
            writer: None,
            rows_written: 0,
            whole_documents: false,
            options: FormatOptions::default(),
        }
    }

    /// Writes with the CSV delimiter and encoding, NULL text and date format
    /// the options choose; a format ignores the options it does not use.
    pub fn with_options(mut self, options: FormatOptions) -> Self {
        self.options = options;
        self
    }

    /// Writes to exactly `path`, for a single-file export whose name the
    /// user chose.
    pub fn to_file(path: impl Into<PathBuf>, format: FileFormat) -> Self {
        let path = path.into();
        let table = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        let dir = path.parent().map(PathBuf::from).unwrap_or_default();

        Self {
            target_path: Some(path),
            ..Self::new(dir, None, table, format)
        }
    }

    /// Writes each JSON row that is a single document with the document's
    /// own fields, so a field the first page lacked still reaches the file.
    pub fn writing_whole_documents(mut self) -> Self {
        self.whole_documents = true;
        self
    }

    /// The `schema.table.ext` file name this sink writes, matching the name
    /// recorded in the export folder's `manifest.json`.
    pub fn file_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}.{}", self.table, self.format.extension()),
            None => format!("{}.{}", self.table, self.format.extension()),
        }
    }

    /// Document title for formats that carry one: `schema.table`.
    fn title(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.table),
            None => self.table.clone(),
        }
    }

    fn path(&self) -> PathBuf {
        if let Some(path) = &self.target_path {
            return path.clone();
        }

        self.dir.join(self.file_name())
    }
}

/// A whole-document row as the columns and values of its own fields, so a
/// JSON line carries every field the document has.
fn whole_document(enabled: bool, row: &[Value]) -> Option<(Vec<ColumnMeta>, Vec<Value>)> {
    if !enabled {
        return None;
    }
    let [Value::Document(fields)] = row else {
        return None;
    };

    let columns = fields
        .keys()
        .map(|name| ColumnMeta {
            name: name.clone(),
            type_name: String::new(),
            kind: ColumnKind::Unknown,
            nullable: true,
            is_primary_key: false,
        })
        .collect();
    Some((columns, fields.values().cloned().collect()))
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
            FileFormat::Csv => StreamWriter::Csv(Box::new(CsvStreamWriter::with_options(
                writer,
                self.options.clone(),
            ))),
            FileFormat::Json => StreamWriter::Json(
                JsonArrayStreamWriter::new(writer).with_date_format(self.options.date_format),
            ),
            FileFormat::JsonLines => StreamWriter::JsonLines(
                JsonLinesStreamWriter::new(writer).with_date_format(self.options.date_format),
            ),
            FileFormat::Html => StreamWriter::Html(
                HtmlStreamWriter::new(writer, self.title()).with_options(self.options.clone()),
            ),
            FileFormat::Markdown => StreamWriter::Markdown(
                MarkdownStreamWriter::new(writer).with_options(self.options.clone()),
            ),
            FileFormat::Xml => {
                StreamWriter::Xml(XmlStreamWriter::new(writer).with_options(self.options.clone()))
            }
        };

        match &mut stream {
            StreamWriter::Csv(w) => w.write_header(&self.columns),
            StreamWriter::Json(w) => w.write_header(&self.columns),
            StreamWriter::JsonLines(w) => w.write_header(&self.columns),
            StreamWriter::Html(w) => w.write_header(&self.columns),
            StreamWriter::Markdown(w) => w.write_header(&self.columns),
            StreamWriter::Xml(w) => w.write_header(&self.columns),
        }
        .map_err(|e| TransferError::Sink(e.to_string()))?;

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
                StreamWriter::Json(w) => match whole_document(self.whole_documents, row) {
                    Some((columns, values)) => w.write_row(&columns, &values),
                    None => w.write_row(&self.columns, row),
                }
                .map_err(|e| TransferError::Sink(e.to_string()))?,
                StreamWriter::JsonLines(w) => match whole_document(self.whole_documents, row) {
                    Some((columns, values)) => w.write_row(&columns, &values),
                    None => w.write_row(&self.columns, row),
                }
                .map_err(|e| TransferError::Sink(e.to_string()))?,
                StreamWriter::Html(w) => w
                    .write_row(row)
                    .map_err(|e| TransferError::Sink(e.to_string()))?,
                StreamWriter::Markdown(w) => w
                    .write_row(row)
                    .map_err(|e| TransferError::Sink(e.to_string()))?,
                StreamWriter::Xml(w) => w
                    .write_row(row)
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
                StreamWriter::Csv(w) => w.finish(),
                StreamWriter::Json(w) => w.finish(),
                StreamWriter::JsonLines(w) => w.finish(),
                StreamWriter::Html(w) => w.finish(),
                StreamWriter::Markdown(w) => w.finish(),
                StreamWriter::Xml(w) => w.finish(),
            }
            .map_err(|e| TransferError::Sink(e.to_string()))?;
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
    use dbflux_export::FormatOptions;

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
        assert_eq!(contents, "[\n{\"id\":7}\n]\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writes_json_lines_file_with_jsonl_extension() {
        let dir = temp_dir("jsonl");
        let columns = vec![column("id")];
        let mut sink = FileSink::new(&dir, None, "widgets", FileFormat::JsonLines);

        sink.begin(&columns, TableMappingMode::Existing).unwrap();
        sink.write_chunk(&RowChunk(vec![vec![Value::Int(7)], vec![Value::Int(8)]]))
            .unwrap();
        let report = sink.finish().unwrap();

        assert_eq!(report.rows_transferred, 2);
        let contents = std::fs::read_to_string(dir.join("widgets.jsonl")).unwrap();
        assert_eq!(contents, "{\"id\":7}\n{\"id\":8}\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_array_writes_whole_documents_with_their_own_fields() {
        let dir = temp_dir("json_documents");
        let columns = vec![column("doc")];
        let mut sink =
            FileSink::new(&dir, None, "docs", FileFormat::Json).writing_whole_documents();
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("a".to_string(), Value::Int(1));
        let document = Value::Document(fields);

        sink.begin(&columns, TableMappingMode::Existing).unwrap();
        sink.write_chunk(&RowChunk(vec![vec![document]])).unwrap();
        sink.finish().unwrap();

        let contents = std::fs::read_to_string(dir.join("docs.json")).unwrap();
        assert_eq!(contents, "[\n{\"a\":1}\n]\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writes_markup_files_named_schema_table_with_their_extension() {
        let dir = temp_dir("markup");
        let columns = vec![column("id")];

        for (format, expected) in [
            (FileFormat::Html, "<title>public.users</title>"),
            (FileFormat::Markdown, "| id |\n| --- |\n| 7 |\n"),
            (FileFormat::Xml, "<row><field name=\"id\">7</field></row>\n"),
        ] {
            let mut sink = FileSink::new(&dir, Some("public".to_string()), "users", format);
            sink.begin(&columns, TableMappingMode::Existing).unwrap();
            sink.write_chunk(&RowChunk(vec![vec![Value::Int(7)]]))
                .unwrap();
            let report = sink.finish().unwrap();
            assert_eq!(report.rows_transferred, 1);

            let path = dir.join(format!("public.users.{}", format.extension()));
            let contents = std::fs::read_to_string(&path).unwrap();
            assert!(contents.contains(expected), "{format:?}: {contents}");
        }

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
        assert_eq!(
            FileFormat::from_extension("jsonl"),
            Some(FileFormat::JsonLines)
        );
        assert_eq!(FileFormat::from_extension("xml"), Some(FileFormat::Xml));
        assert_eq!(FileFormat::from_extension("yaml"), None);
        for format in FileFormat::ALL {
            assert_eq!(FileFormat::from_extension(format.extension()), Some(format));
        }
    }

    #[test]
    fn all_lists_every_format_with_a_label() {
        assert_eq!(
            FileFormat::ALL,
            [
                FileFormat::Csv,
                FileFormat::Json,
                FileFormat::JsonLines,
                FileFormat::Html,
                FileFormat::Markdown,
                FileFormat::Xml
            ]
        );
        assert_eq!(FileFormat::Html.label(), "HTML");
        assert_eq!(FileFormat::Markdown.label(), "Markdown");
        assert_eq!(FileFormat::Xml.label(), "XML");
        assert_eq!(FileFormat::Csv.label(), "CSV");
        assert_eq!(FileFormat::Json.label(), "JSON");
        assert_eq!(FileFormat::JsonLines.label(), "JSON Lines");
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

    #[test]
    fn whole_document_rows_keep_each_document_s_own_fields() {
        let dir = temp_dir("whole_documents");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("events.jsonl");
        let mut sink = FileSink::to_file(&path, FileFormat::JsonLines).writing_whole_documents();

        sink.begin(&[column("_document")], TableMappingMode::Existing)
            .expect("begin");
        let first = Value::Document([("_id".to_string(), Value::Int(1))].into_iter().collect());
        let later = Value::Document(
            [
                ("_id".to_string(), Value::Int(2)),
                (
                    "email".to_string(),
                    Value::Text("b@example.com".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        );
        sink.write_chunk(&RowChunk(vec![vec![first], vec![later]]))
            .expect("write");
        sink.finish().expect("finish");

        let text = std::fs::read_to_string(&path).expect("read back");
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("one object per line"))
            .collect();
        assert_eq!(
            lines,
            vec![
                serde_json::json!({"_id": 1}),
                serde_json::json!({"_id": 2, "email": "b@example.com"}),
            ]
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    fn write_with_options(
        label: &str,
        format: FileFormat,
        options: FormatOptions,
        rows: Vec<Vec<Value>>,
    ) -> Result<Vec<u8>, TransferError> {
        let dir = temp_dir(label);
        let columns = vec![column("id"), column("note")];
        let mut sink = FileSink::new(&dir, None, "t", format).with_options(options);
        sink.begin(&columns, TableMappingMode::Existing)?;
        sink.write_chunk(&RowChunk(rows))?;
        sink.finish()?;
        let bytes = std::fs::read(dir.join(format!("t.{}", format.extension())))
            .map_err(|e| TransferError::Sink(e.to_string()))?;
        std::fs::remove_dir_all(&dir).ok();
        Ok(bytes)
    }

    fn note_rows() -> Vec<Vec<Value>> {
        vec![
            vec![Value::Int(1), Value::Text("café".to_string())],
            vec![Value::Int(2), Value::Null],
        ]
    }

    #[test]
    fn default_options_write_the_same_bytes_for_every_format() {
        for format in FileFormat::ALL {
            let plain =
                write_with_options("defaults_a", format, FormatOptions::default(), note_rows())
                    .unwrap();

            let dir = temp_dir("defaults_b");
            let mut sink = FileSink::new(&dir, None, "t", format);
            sink.begin(&[column("id"), column("note")], TableMappingMode::Existing)
                .unwrap();
            sink.write_chunk(&RowChunk(note_rows())).unwrap();
            sink.finish().unwrap();
            let without = std::fs::read(dir.join(format!("t.{}", format.extension()))).unwrap();
            std::fs::remove_dir_all(&dir).ok();

            assert_eq!(plain, without, "{format:?}");
        }
    }

    #[test]
    fn csv_sink_applies_delimiter_encoding_and_null_text() {
        let options = FormatOptions {
            delimiter: dbflux_export::CsvDelimiter::Semicolon,
            encoding: dbflux_export::TextEncoding::Windows1252,
            null_text: "\\N".to_string(),
            date_format: dbflux_export::DateFormat::default(),
        };
        let bytes =
            write_with_options("csv_options", FileFormat::Csv, options, note_rows()).unwrap();
        assert_eq!(bytes, b"id;note\n1;caf\xE9\n2;\\N\n");
    }

    #[test]
    fn csv_sink_reports_a_character_the_encoding_cannot_represent() {
        let options = FormatOptions {
            encoding: dbflux_export::TextEncoding::Windows1252,
            ..FormatOptions::default()
        };
        let rows = vec![vec![Value::Int(1), Value::Text("ε".to_string())]];
        let error =
            write_with_options("csv_unmappable", FileFormat::Csv, options, rows).unwrap_err();
        assert!(error.to_string().contains("U+03B5"), "{error}");
    }

    #[test]
    fn markup_sinks_apply_null_text_and_xml_and_json_do_not() {
        let options = FormatOptions {
            null_text: "NULL".to_string(),
            ..FormatOptions::default()
        };
        let html = write_with_options("null_html", FileFormat::Html, options.clone(), note_rows())
            .unwrap();
        assert!(String::from_utf8(html).unwrap().contains("<td>NULL</td>"));
        let markdown = write_with_options(
            "null_md",
            FileFormat::Markdown,
            options.clone(),
            note_rows(),
        )
        .unwrap();
        assert!(
            String::from_utf8(markdown)
                .unwrap()
                .contains("| 2 | NULL |")
        );

        let xml = String::from_utf8(
            write_with_options("null_xml", FileFormat::Xml, options.clone(), note_rows()).unwrap(),
        )
        .unwrap();
        assert!(
            xml.contains("null=\"true\"") && !xml.contains("NULL"),
            "{xml}"
        );
        for format in [FileFormat::Json, FileFormat::JsonLines] {
            let json = String::from_utf8(
                write_with_options("null_json", format, options.clone(), note_rows()).unwrap(),
            )
            .unwrap();
            assert!(
                json.contains("\"note\":null") && !json.contains("NULL"),
                "{json}"
            );
        }
    }

    #[test]
    fn options_apply_only_to_the_formats_that_use_them() {
        assert!(FileFormat::Csv.uses_delimiter_and_encoding());
        for format in [
            FileFormat::Json,
            FileFormat::JsonLines,
            FileFormat::Html,
            FileFormat::Markdown,
            FileFormat::Xml,
        ] {
            assert!(!format.uses_delimiter_and_encoding(), "{format:?}");
        }
        for format in [FileFormat::Csv, FileFormat::Html, FileFormat::Markdown] {
            assert!(format.uses_null_text(), "{format:?}");
        }
        for format in [FileFormat::Json, FileFormat::JsonLines, FileFormat::Xml] {
            assert!(!format.uses_null_text(), "{format:?}");
        }

        let all = FormatOptions {
            delimiter: dbflux_export::CsvDelimiter::Pipe,
            encoding: dbflux_export::TextEncoding::Utf16Le,
            null_text: "x".to_string(),
            date_format: dbflux_export::DateFormat::Epoch,
        };
        let json = FileFormat::Json.applicable_options(&all);
        assert_eq!(json.date_format, dbflux_export::DateFormat::Epoch);
        assert_eq!(
            FormatOptions {
                date_format: dbflux_export::DateFormat::default(),
                ..json
            },
            FormatOptions::default()
        );
        assert_eq!(FileFormat::Csv.applicable_options(&all), all);
    }

    fn pinned_columns() -> Vec<TransferColumn> {
        ["id", "note", "at", "day", "tm"]
            .into_iter()
            .map(column)
            .collect()
    }

    fn pinned_rows() -> Vec<Vec<Value>> {
        use dbflux_core::chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
        vec![
            vec![
                Value::Int(1),
                Value::Text("café <&>".to_string()),
                Value::DateTime(Utc.with_ymd_and_hms(2026, 7, 7, 10, 30, 15).unwrap()),
                Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap()),
                Value::Time(NaiveTime::from_hms_milli_opt(10, 30, 15, 250).unwrap()),
            ],
            vec![
                Value::Int(2),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ]
    }

    /// The expected strings are the bytes the writers produced before the
    /// format options existed.
    #[test]
    fn default_options_write_the_pinned_bytes_for_every_format() {
        let expected = [
            (
                FileFormat::Csv,
                "id,note,at,day,tm\n1,café <&>,2026-07-07T10:30:15+00:00,2026-07-07,10:30:15.250\n2,,,,\n",
            ),
            (
                FileFormat::Html,
                "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>t</title>\n<style>th, td { white-space: pre-wrap; }</style>\n</head>\n<body>\n<table>\n<thead>\n<tr><th>id</th><th>note</th><th>at</th><th>day</th><th>tm</th></tr>\n</thead>\n<tbody>\n<tr><td>1</td><td>café &lt;&amp;&gt;</td><td>2026-07-07T10:30:15+00:00</td><td>2026-07-07</td><td>10:30:15.250</td></tr>\n<tr><td>2</td><td></td><td></td><td></td><td></td></tr>\n</tbody>\n</table>\n</body>\n</html>\n",
            ),
            (
                FileFormat::Markdown,
                "| id | note | at | day | tm |\n| --- | --- | --- | --- | --- |\n| 1 | café &lt;&amp;&gt; | 2026-07-07T10:30:15+00:00 | 2026-07-07 | 10:30:15.250 |\n| 2 |  |  |  |  |\n",
            ),
            (
                FileFormat::Xml,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n<row><field name=\"id\">1</field><field name=\"note\">café &lt;&amp;&gt;</field><field name=\"at\">2026-07-07T10:30:15+00:00</field><field name=\"day\">2026-07-07</field><field name=\"tm\">10:30:15.250</field></row>\n<row><field name=\"id\">2</field><field name=\"note\" null=\"true\"/><field name=\"at\" null=\"true\"/><field name=\"day\" null=\"true\"/><field name=\"tm\" null=\"true\"/></row>\n</rows>\n",
            ),
        ];

        for (format, text) in expected {
            let dir = temp_dir("pinned");
            let mut sink = FileSink::new(&dir, None, "t", format);
            sink.begin(&pinned_columns(), TableMappingMode::Existing)
                .unwrap();
            sink.write_chunk(&RowChunk(pinned_rows())).unwrap();
            sink.finish().unwrap();
            let bytes = std::fs::read(dir.join(format!("t.{}", format.extension()))).unwrap();
            std::fs::remove_dir_all(&dir).ok();
            assert_eq!(String::from_utf8(bytes).unwrap(), text, "{format:?}");
        }
    }

    /// JSON object key order depends on whether serde_json's `preserve_order`
    /// is enabled by another crate in the build, so the lines are compared as
    /// values, not as bytes.
    #[test]
    fn default_options_write_the_pinned_json_lines() {
        let dir = temp_dir("pinned_jsonl");
        let mut sink = FileSink::new(&dir, None, "t", FileFormat::JsonLines);
        sink.begin(&pinned_columns(), TableMappingMode::Existing)
            .unwrap();
        sink.write_chunk(&RowChunk(pinned_rows())).unwrap();
        sink.finish().unwrap();
        let text = std::fs::read_to_string(dir.join("t.jsonl")).unwrap();
        std::fs::remove_dir_all(&dir).ok();

        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines,
            vec![
                serde_json::json!({"id": 1, "note": "café <&>", "at": {"$date": "2026-07-07T10:30:15+00:00"}, "day": "2026-07-07", "tm": "10:30:15.250"}),
                serde_json::json!({"id": 2, "note": null, "at": null, "day": null, "tm": null}),
            ]
        );
        assert!(text.ends_with("}\n") && text.matches('\n').count() == 2);
        assert!(text.contains("\"at\":{\"$date\":\"2026-07-07T10:30:15+00:00\"}"));
    }

    fn null_text_options(null_text: &str) -> FormatOptions {
        FormatOptions {
            null_text: null_text.to_string(),
            ..FormatOptions::default()
        }
    }

    #[test]
    fn a_value_equal_to_the_null_text_fails_the_export_in_every_text_format() {
        for format in [FileFormat::Csv, FileFormat::Html, FileFormat::Markdown] {
            let text = write_with_options(
                "marker_text",
                format,
                null_text_options("NULL"),
                vec![vec![Value::Int(1), Value::Text("NULL".to_string())]],
            )
            .unwrap_err()
            .to_string();
            assert!(
                text.contains(
                    "value 'NULL' in column 'note' equals the NULL text; choose a different NULL text"
                ),
                "{format:?}: {text}"
            );

            let number = write_with_options(
                "marker_number",
                format,
                null_text_options("0"),
                vec![vec![Value::Int(0), Value::Text("x".to_string())]],
            )
            .unwrap_err()
            .to_string();
            assert!(
                number.contains(
                    "value '0' in column 'id' equals the NULL text; choose a different NULL text"
                ),
                "{format:?}: {number}"
            );
        }
    }

    #[test]
    fn values_that_differ_from_the_null_text_still_export() {
        let rows = vec![vec![Value::Int(1), Value::Text("NULLS".to_string())]];
        let bytes = write_with_options(
            "marker_ok",
            FileFormat::Csv,
            null_text_options("NULL"),
            rows,
        )
        .unwrap();
        assert_eq!(bytes, b"id,note\n1,NULLS\n");
    }

    #[test]
    fn an_empty_null_text_keeps_null_and_empty_string_as_before() {
        let rows = vec![
            vec![Value::Int(1), Value::Text(String::new())],
            vec![Value::Int(2), Value::Null],
        ];
        let bytes = write_with_options(
            "marker_empty",
            FileFormat::Csv,
            FormatOptions::default(),
            rows,
        )
        .unwrap();
        assert_eq!(bytes, b"id,note\n1,\n2,\n");
    }

    #[test]
    fn an_empty_string_is_not_the_null_text_when_another_text_is_chosen() {
        let rows = vec![vec![Value::Int(1), Value::Text(String::new())]];
        let bytes = write_with_options(
            "marker_distinct",
            FileFormat::Csv,
            null_text_options("\\N"),
            rows,
        )
        .unwrap();
        assert_eq!(bytes, b"id,note\n1,\n");
    }
}
