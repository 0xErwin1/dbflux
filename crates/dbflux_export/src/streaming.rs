//! Per-row streaming writers for CSV and JSON.
//!
//! [`crate::export`] formats an entire [`dbflux_core::QueryResult`] in one call, which
//! requires every row to be resident in memory at once. The data-transfer engine
//! streams rows in bounded-memory chunks, so it needs a writer that accepts a
//! header once and then rows incrementally. `CsvStreamWriter` shares the exact
//! value-formatting helpers used by [`crate::CsvExporter`], so CSV single-shot
//! and streaming output are byte-identical for the same input. `JsonLinesStreamWriter`
//! deliberately diverges from [`crate::JsonExporter`]'s single top-level JSON
//! array: it emits NDJSON (one object per line) so the reading side can parse
//! incrementally instead of buffering the whole file — see its own docs.

use crate::ExportError;
use crate::csv::value_to_csv_field;
use crate::json::row_to_json_object;
use dbflux_core::{ColumnMeta, Value};
use std::io::Write;

/// Incrementally writes a CSV document: one `write_header` call followed by
/// any number of `write_row` calls, then `finish` to flush the underlying writer.
pub struct CsvStreamWriter<W: Write> {
    inner: ::csv::Writer<W>,
}

impl<W: Write> CsvStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: ::csv::Writer::from_writer(writer),
        }
    }

    pub fn write_header(&mut self, columns: &[ColumnMeta]) -> Result<(), ExportError> {
        let headers: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        self.inner.write_record(&headers)?;
        Ok(())
    }

    pub fn write_row(&mut self, row: &[Value]) -> Result<(), ExportError> {
        for value in row {
            let field = value_to_csv_field(value);
            self.inner.write_field(&field)?;
        }
        self.inner.write_record(None::<&[u8]>)?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        self.inner.flush()?;
        Ok(())
    }
}

/// Incrementally writes newline-delimited JSON (NDJSON): `write_row` appends
/// one compact JSON object per line. `write_header` is a no-op — NDJSON has
/// no enclosing array to open, and the column shape is carried by each
/// object's own keys — kept only so `JsonLinesStreamWriter` shares a call shape
/// with [`CsvStreamWriter`].
///
/// NDJSON, unlike a single top-level JSON array, lets the reading side
/// (`dbflux_transfer::file_source::FileSource`) parse one value at a time
/// with `serde_json::Deserializer::into_iter`, so import never has to hold
/// the whole file in memory.
///
/// Always emits compact JSON. Reproducing `serde_json::to_writer_pretty`'s
/// indentation incrementally would require re-indenting each already-rendered
/// object as it is appended, which this streaming writer does not attempt.
pub struct JsonLinesStreamWriter<W: Write> {
    writer: W,
}

impl<W: Write> JsonLinesStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write_header(&mut self, _columns: &[ColumnMeta]) -> Result<(), ExportError> {
        Ok(())
    }

    pub fn write_row(&mut self, columns: &[ColumnMeta], row: &[Value]) -> Result<(), ExportError> {
        let object = row_to_json_object(columns, row);
        serde_json::to_writer(&mut self.writer, &object)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        self.writer.flush()?;
        Ok(())
    }
}

/// Incrementally writes one JSON array: `write_header` opens it, each
/// `write_row` appends one compact object on its own line, and `finish`
/// closes it, so the file is a single valid JSON document that still never
/// needs the whole result in memory.
pub struct JsonArrayStreamWriter<W: Write> {
    writer: W,
    rows_written: u64,
}

impl<W: Write> JsonArrayStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            rows_written: 0,
        }
    }

    pub fn write_header(&mut self, _columns: &[ColumnMeta]) -> Result<(), ExportError> {
        self.writer.write_all(b"[\n")?;
        Ok(())
    }

    pub fn write_row(&mut self, columns: &[ColumnMeta], row: &[Value]) -> Result<(), ExportError> {
        if self.rows_written > 0 {
            self.writer.write_all(b",\n")?;
        }
        let object = row_to_json_object(columns, row);
        serde_json::to_writer(&mut self.writer, &object)?;
        self.rows_written += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        if self.rows_written > 0 {
            self.writer.write_all(b"\n")?;
        }
        self.writer.write_all(b"]\n")?;
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CsvExporter;
    use dbflux_core::{ColumnKind, QueryResult};
    use std::time::Duration;

    fn columns() -> Vec<ColumnMeta> {
        vec![
            ColumnMeta {
                name: "id".to_string(),
                type_name: "int".to_string(),
                kind: ColumnKind::Integer,
                nullable: false,
                is_primary_key: true,
            },
            ColumnMeta {
                name: "name".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
        ]
    }

    fn rows() -> Vec<Vec<Value>> {
        vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
            vec![Value::Int(3), Value::Null],
        ]
    }

    #[test]
    fn csv_streaming_matches_single_shot_export() {
        let cols = columns();
        let all_rows = rows();

        let result = QueryResult::table(
            cols.clone(),
            all_rows.clone(),
            None,
            Duration::from_millis(1),
        );
        let mut single_shot = Vec::new();
        CsvExporter.export(&result, &mut single_shot).unwrap();

        let mut streamed = Vec::new();
        let mut writer = CsvStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.write_row(&all_rows[0]).unwrap();
        writer.write_row(&all_rows[1]).unwrap();
        writer.write_row(&all_rows[2]).unwrap();
        writer.finish().unwrap();

        assert_eq!(streamed, single_shot);
    }

    /// JD-W1 regression: `JsonLinesStreamWriter` must emit NDJSON (one compact
    /// object per line, no enclosing array/commas) instead of mirroring
    /// `JsonExporter`'s single top-level array — the reading side depends on
    /// this shape to parse one value at a time without buffering the file.
    #[test]
    fn json_streaming_emits_one_compact_object_per_line() {
        let cols = columns();
        let all_rows = rows();

        let mut streamed = Vec::new();
        let mut writer = JsonLinesStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.write_row(&cols, &all_rows[0]).unwrap();
        writer.write_row(&cols, &all_rows[1]).unwrap();
        writer.write_row(&cols, &all_rows[2]).unwrap();
        writer.finish().unwrap();

        let text = String::from_utf8(streamed).expect("NDJSON output must be valid UTF-8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "one line per row, no enclosing array");
        assert_eq!(lines[0], r#"{"id":1,"name":"Alice"}"#);
        assert_eq!(lines[1], r#"{"id":2,"name":"Bob"}"#);
        assert_eq!(lines[2], r#"{"id":3,"name":null}"#);
    }

    #[test]
    fn json_streaming_header_only_writes_nothing() {
        let cols = columns();

        let mut streamed = Vec::new();
        let mut writer = JsonLinesStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.finish().unwrap();

        assert!(
            streamed.is_empty(),
            "NDJSON has no enclosing array, so a header-only file must be empty"
        );
    }

    #[test]
    fn json_array_streaming_writes_one_valid_array_with_one_object_per_line() {
        let cols = columns();
        let all_rows = rows();

        let mut streamed = Vec::new();
        let mut writer = JsonArrayStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.write_row(&cols, &all_rows[0]).unwrap();
        writer.write_row(&cols, &all_rows[1]).unwrap();
        writer.write_row(&cols, &all_rows[2]).unwrap();
        writer.finish().unwrap();

        let text = String::from_utf8(streamed).expect("JSON output must be valid UTF-8");
        assert_eq!(
            text,
            "[\n{\"id\":1,\"name\":\"Alice\"},\n{\"id\":2,\"name\":\"Bob\"},\n{\"id\":3,\"name\":null}\n]\n"
        );
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed.as_array().map(Vec::len), Some(3));
    }

    #[test]
    fn json_array_streaming_header_only_writes_an_empty_array() {
        let cols = columns();

        let mut streamed = Vec::new();
        let mut writer = JsonArrayStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.finish().unwrap();

        assert_eq!(String::from_utf8(streamed).unwrap(), "[\n]\n");
    }

    #[test]
    fn csv_streaming_header_only_matches_empty_export() {
        let cols = columns();
        let result = QueryResult::table(cols.clone(), Vec::new(), None, Duration::from_millis(1));
        let mut single_shot = Vec::new();
        CsvExporter.export(&result, &mut single_shot).unwrap();

        let mut streamed = Vec::new();
        let mut writer = CsvStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.finish().unwrap();

        assert_eq!(streamed, single_shot);
    }
}
