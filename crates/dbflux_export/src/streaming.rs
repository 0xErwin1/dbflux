//! Per-row streaming writers for CSV and JSON.
//!
//! [`crate::export`] formats an entire [`dbflux_core::QueryResult`] in one call, which
//! requires every row to be resident in memory at once. The data-transfer engine
//! streams rows in bounded-memory chunks, so it needs a writer that accepts a
//! header once and then rows incrementally. These types share the exact
//! value-formatting helpers used by [`crate::CsvExporter`] and [`crate::JsonExporter`]
//! so single-shot and streaming output are byte-identical for the same input.

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

/// Incrementally writes a JSON array of row objects: `write_header` opens the
/// array, `write_row` appends one object per call, and `finish` closes it.
///
/// Always emits compact JSON. Reproducing `serde_json::to_writer_pretty`'s
/// indentation incrementally would require re-indenting each already-rendered
/// object as it is appended, which this streaming writer does not attempt.
pub struct JsonStreamWriter<W: Write> {
    writer: W,
    wrote_any: bool,
}

impl<W: Write> JsonStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            wrote_any: false,
        }
    }

    pub fn write_header(&mut self, _columns: &[ColumnMeta]) -> Result<(), ExportError> {
        self.writer.write_all(b"[")?;
        Ok(())
    }

    pub fn write_row(&mut self, columns: &[ColumnMeta], row: &[Value]) -> Result<(), ExportError> {
        if self.wrote_any {
            self.writer.write_all(b",")?;
        }

        let object = row_to_json_object(columns, row);
        serde_json::to_writer(&mut self.writer, &object)?;

        self.wrote_any = true;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), ExportError> {
        self.writer.write_all(b"]")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CsvExporter, JsonExporter};
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

        let result = QueryResult::table(cols.clone(), all_rows.clone(), None, Duration::from_millis(1));
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

    #[test]
    fn json_streaming_matches_single_shot_export() {
        let cols = columns();
        let all_rows = rows();

        let result = QueryResult::table(cols.clone(), all_rows.clone(), None, Duration::from_millis(1));
        let mut single_shot = Vec::new();
        JsonExporter { pretty: false }
            .export(&result, &mut single_shot)
            .unwrap();

        let mut streamed = Vec::new();
        let mut writer = JsonStreamWriter::new(&mut streamed);
        writer.write_header(&cols).unwrap();
        writer.write_row(&cols, &all_rows[0]).unwrap();
        writer.write_row(&cols, &all_rows[1]).unwrap();
        writer.write_row(&cols, &all_rows[2]).unwrap();
        writer.finish().unwrap();

        assert_eq!(streamed, single_shot);
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
