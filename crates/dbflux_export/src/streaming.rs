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
use crate::csv::value_to_field;
use crate::encoding::EncodingWriter;
use crate::json::row_to_json_object;
use crate::options::{DateFormat, FormatOptions};
use dbflux_core::{ColumnMeta, Value};
use std::io::Write;

/// Incrementally writes a CSV document: one `write_header` call followed by
/// any number of `write_row` calls, then `finish` to flush the underlying writer.
pub struct CsvStreamWriter<W: Write> {
    inner: ::csv::Writer<EncodingWriter<W>>,
    options: FormatOptions,
}

impl<W: Write> CsvStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_options(writer, FormatOptions::default())
    }

    /// Writes with the options' delimiter, encoding, NULL text and date
    /// format. A character the encoding cannot represent fails the write.
    pub fn with_options(writer: W, options: FormatOptions) -> Self {
        let inner = ::csv::WriterBuilder::new()
            .delimiter(options.delimiter.byte())
            .from_writer(EncodingWriter::new(writer, options.encoding));
        Self { inner, options }
    }

    pub fn write_header(&mut self, columns: &[ColumnMeta]) -> Result<(), ExportError> {
        let headers: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        self.inner.write_record(&headers)?;
        Ok(())
    }

    pub fn write_row(&mut self, row: &[Value]) -> Result<(), ExportError> {
        for value in row {
            let field = value_to_field(value, &self.options);
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
    date_format: DateFormat,
}

impl<W: Write> JsonLinesStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            date_format: DateFormat::default(),
        }
    }

    pub fn with_date_format(mut self, date_format: DateFormat) -> Self {
        self.date_format = date_format;
        self
    }

    pub fn write_header(&mut self, _columns: &[ColumnMeta]) -> Result<(), ExportError> {
        Ok(())
    }

    pub fn write_row(&mut self, columns: &[ColumnMeta], row: &[Value]) -> Result<(), ExportError> {
        let object = row_to_json_object(columns, row, self.date_format);
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
    date_format: DateFormat,
}

impl<W: Write> JsonArrayStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            rows_written: 0,
            date_format: DateFormat::default(),
        }
    }

    pub fn with_date_format(mut self, date_format: DateFormat) -> Self {
        self.date_format = date_format;
        self
    }

    pub fn write_header(&mut self, _columns: &[ColumnMeta]) -> Result<(), ExportError> {
        self.writer.write_all(b"[\n")?;
        Ok(())
    }

    pub fn write_row(&mut self, columns: &[ColumnMeta], row: &[Value]) -> Result<(), ExportError> {
        if self.rows_written > 0 {
            self.writer.write_all(b",\n")?;
        }
        let object = row_to_json_object(columns, row, self.date_format);
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
    use crate::{CsvDelimiter, DateFormat, FormatOptions, TextEncoding};
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

    fn csv_with(options: FormatOptions, all_rows: &[Vec<Value>]) -> Result<Vec<u8>, ExportError> {
        let mut streamed = Vec::new();
        let mut writer = CsvStreamWriter::with_options(&mut streamed, options);
        writer.write_header(&columns())?;
        for row in all_rows {
            writer.write_row(row)?;
        }
        writer.finish()?;
        Ok(streamed)
    }

    fn csv_options(delimiter: CsvDelimiter, encoding: TextEncoding) -> FormatOptions {
        FormatOptions {
            delimiter,
            encoding,
            ..FormatOptions::default()
        }
    }

    #[test]
    fn csv_default_options_are_byte_identical_to_new() {
        let mut plain = Vec::new();
        let mut writer = CsvStreamWriter::new(&mut plain);
        writer.write_header(&columns()).unwrap();
        for row in rows() {
            writer.write_row(&row).unwrap();
        }
        writer.finish().unwrap();

        assert_eq!(csv_with(FormatOptions::default(), &rows()).unwrap(), plain);
        assert_eq!(
            plain,
            b"id,name
1,Alice
2,Bob
3,
"
        );
    }

    #[test]
    fn csv_uses_each_delimiter_and_quotes_fields_containing_it() {
        let data = vec![vec![Value::Int(1), Value::Text("a;b|c\td".to_string())]];
        for (delimiter, expected) in [
            (CsvDelimiter::Comma, "id,name\n1,a;b|c\td\n"),
            (CsvDelimiter::Semicolon, "id;name\n1;\"a;b|c\td\"\n"),
            (CsvDelimiter::Tab, "id\tname\n1\t\"a;b|c\td\"\n"),
            (CsvDelimiter::Pipe, "id|name\n1|\"a;b|c\td\"\n"),
        ] {
            let output = csv_with(csv_options(delimiter, TextEncoding::Utf8), &data).unwrap();
            assert_eq!(
                String::from_utf8(output).unwrap(),
                expected,
                "{delimiter:?}"
            );
        }
    }

    #[test]
    fn csv_encodings_write_their_bytes() {
        let data = vec![vec![Value::Int(1), Value::Text("é".to_string())]];

        let utf8_bom = csv_with(
            csv_options(CsvDelimiter::Comma, TextEncoding::Utf8Bom),
            &data,
        )
        .unwrap();
        assert_eq!(&utf8_bom[..3], &[0xEF, 0xBB, 0xBF]);
        assert_eq!(&utf8_bom[3..], "id,name\n1,é\n".as_bytes());

        let utf16 = csv_with(
            csv_options(CsvDelimiter::Comma, TextEncoding::Utf16Le),
            &data,
        )
        .unwrap();
        assert_eq!(&utf16[..2], &[0xFF, 0xFE]);
        let units: Vec<u16> = utf16[2..]
            .chunks(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
            .collect();
        assert_eq!(String::from_utf16(&units).unwrap(), "id,name\n1,é\n");

        let latin = csv_with(
            csv_options(CsvDelimiter::Comma, TextEncoding::Windows1252),
            &data,
        )
        .unwrap();
        assert_eq!(latin, b"id,name\n1,\xE9\n");
    }

    #[test]
    fn csv_windows_1252_fails_clearly_on_an_unencodable_character() {
        let data = vec![vec![Value::Int(1), Value::Text("ε".to_string())]];
        let error = csv_with(
            csv_options(CsvDelimiter::Comma, TextEncoding::Windows1252),
            &data,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("U+03B5"), "{error}");
        assert!(error.contains("Windows-1252"), "{error}");
    }

    #[test]
    fn csv_writes_the_configured_null_text_but_keeps_empty_strings_empty() {
        let options = FormatOptions {
            null_text: "\\N".to_string(),
            ..FormatOptions::default()
        };
        let data = vec![
            vec![Value::Null, Value::Text(String::new())],
            vec![Value::Int(1), Value::Null],
        ];
        let output = csv_with(options, &data).unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "id,name\n\\N,\n1,\\N\n");
    }

    fn date_columns() -> Vec<ColumnMeta> {
        vec![ColumnMeta {
            name: "at".to_string(),
            type_name: "timestamptz".to_string(),
            kind: ColumnKind::Timestamp,
            nullable: true,
            is_primary_key: false,
        }]
    }

    fn date_row() -> Vec<Value> {
        use dbflux_core::chrono::{NaiveDate, TimeZone, Utc};
        vec![Value::DateTime(
            Utc.with_ymd_and_hms(2026, 7, 7, 10, 30, 15).unwrap(),
        )]
        .into_iter()
        .chain([Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap())])
        .collect()
    }

    #[test]
    fn csv_applies_each_date_preset() {
        let columns = vec![date_columns().remove(0), date_columns().remove(0)];
        for (date_format, expected) in [
            (
                DateFormat::Rfc3339,
                "at,at\n2026-07-07T10:30:15+00:00,2026-07-07\n",
            ),
            (
                DateFormat::DateTime,
                "at,at\n2026-07-07 10:30:15,2026-07-07\n",
            ),
            (DateFormat::Epoch, "at,at\n1783420215,1783382400\n"),
        ] {
            let mut streamed = Vec::new();
            let options = FormatOptions {
                date_format,
                ..FormatOptions::default()
            };
            let mut writer = CsvStreamWriter::with_options(&mut streamed, options);
            writer.write_header(&columns).unwrap();
            writer.write_row(&date_row()).unwrap();
            writer.finish().unwrap();
            assert_eq!(
                String::from_utf8(streamed).unwrap(),
                expected,
                "{date_format:?}"
            );
        }
    }

    #[test]
    fn json_writers_apply_each_date_preset_and_epoch_is_a_number() {
        let mut columns = date_columns();
        columns.push(ColumnMeta {
            name: "day".to_string(),
            type_name: "date".to_string(),
            ..date_columns().remove(0)
        });
        for (date_format, expected) in [
            (
                DateFormat::Rfc3339,
                "{\"at\":{\"$date\":\"2026-07-07T10:30:15+00:00\"},\"day\":\"2026-07-07\"}",
            ),
            (
                DateFormat::DateTime,
                "{\"at\":\"2026-07-07 10:30:15\",\"day\":\"2026-07-07\"}",
            ),
            (DateFormat::Epoch, "{\"at\":1783420215,\"day\":1783382400}"),
        ] {
            let mut streamed = Vec::new();
            let mut writer =
                JsonLinesStreamWriter::new(&mut streamed).with_date_format(date_format);
            writer.write_header(&columns).unwrap();
            writer.write_row(&columns, &date_row()).unwrap();
            writer.finish().unwrap();
            assert_eq!(
                String::from_utf8(streamed).unwrap().trim_end(),
                expected,
                "{date_format:?}"
            );
        }

        let single = vec![date_columns().remove(0)];
        let row = vec![date_row().remove(0)];
        let mut streamed = Vec::new();
        let mut writer =
            JsonArrayStreamWriter::new(&mut streamed).with_date_format(DateFormat::Epoch);
        writer.write_header(&single).unwrap();
        writer.write_row(&single, &row).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(streamed).unwrap(),
            "[\n{\"at\":1783420215}\n]\n"
        );

        let mut streamed = Vec::new();
        let mut writer =
            JsonLinesStreamWriter::new(&mut streamed).with_date_format(DateFormat::DateTime);
        writer.write_header(&single).unwrap();
        writer.write_row(&single, &row).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(streamed).unwrap(),
            "{\"at\":\"2026-07-07 10:30:15\"}\n"
        );
    }

    #[test]
    fn json_output_ignores_null_text() {
        let mut streamed = Vec::new();
        let mut writer = JsonLinesStreamWriter::new(&mut streamed);
        writer.write_header(&columns()).unwrap();
        writer.write_row(&columns(), &rows()[2]).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(streamed).unwrap(),
            "{\"id\":3,\"name\":null}\n"
        );
    }
}
