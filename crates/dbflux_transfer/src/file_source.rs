//! File -> Row `RowSource`: reads one previously-exported `schema.table.<ext>`
//! file back into row chunks for Import, recovering typed `Value`s via
//! `value_codec` guided by the manifest's per-column `type_name`.
//!
//! Every format is read incrementally in bounded-memory chunks: CSV via
//! `csv::Reader`, JSON Lines via `serde_json::Deserializer::into_iter`, and a
//! JSON array one element at a time (see [`JsonArrayStream`]). A `json` file
//! whose first token is not `[` is read as NDJSON, because export folders
//! written before JSON Lines existed stored NDJSON under that format. No path
//! ever holds the whole file's rows resident in memory at once.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use dbflux_core::{CancelToken, TransferColumn, Value};

use crate::file_sink::FileFormat;
use crate::pipeline::{RowChunk, RowSource, TransferError};
use crate::value_codec::{value_from_csv_field, value_from_json};

type JsonValueStream = serde_json::StreamDeserializer<
    'static,
    serde_json::de::IoRead<BufReader<File>>,
    serde_json::Value,
>;

enum SourceReader {
    Csv(Box<csv::Reader<BufReader<File>>>),
    Json(Box<JsonValueStream>),
    JsonArray(Box<JsonArrayStream>),
}

/// Yields the elements of one top-level JSON array without parsing the
/// whole array, so a large JSON export imports in bounded memory.
struct JsonArrayStream {
    reader: BufReader<File>,
    position: ArrayPosition,
}

/// Where the reader stands inside the array, so separators are enforced:
/// exactly one comma between elements, none before the first or after the
/// last.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ArrayPosition {
    /// After `[`: an element or `]`.
    Start,
    /// After an element: `,` or `]`.
    AfterElement,
    /// After `,`: an element.
    AfterComma,
    /// After `]` and the whitespace that may follow it.
    Finished,
}

impl JsonArrayStream {
    fn new(reader: BufReader<File>) -> Self {
        Self {
            reader,
            position: ArrayPosition::Start,
        }
    }

    fn next_value(&mut self) -> Result<Option<serde_json::Value>, TransferError> {
        loop {
            if self.position == ArrayPosition::Finished {
                return Ok(None);
            }
            let next = next_non_whitespace(&mut self.reader)?;
            match (self.position, next) {
                (_, None) => return Err(invalid_json("the array is not closed")),
                (ArrayPosition::AfterElement, Some(b',')) => {
                    self.reader.consume(1);
                    self.position = ArrayPosition::AfterComma;
                }
                (ArrayPosition::Start | ArrayPosition::AfterElement, Some(b']')) => {
                    self.reader.consume(1);
                    self.expect_end_of_file()?;
                    self.position = ArrayPosition::Finished;
                }
                (ArrayPosition::Start | ArrayPosition::AfterComma, Some(b'{')) => {
                    let value = self.read_element()?;
                    self.position = ArrayPosition::AfterElement;
                    return Ok(Some(value));
                }
                (ArrayPosition::Start | ArrayPosition::AfterComma, Some(b',' | b']')) => {
                    return Err(invalid_json("expected an object, found a separator"));
                }
                (ArrayPosition::Start | ArrayPosition::AfterComma, Some(_)) => {
                    return Err(invalid_json("each array element must be an object"));
                }
                (_, Some(_)) => {
                    return Err(invalid_json("expected `,` or `]` after an array element"));
                }
            }
        }
    }

    /// Reads one object. An object ends at its `}`, so the deserializer
    /// reads nothing past it and the next separator stays in the reader.
    fn read_element(&mut self) -> Result<serde_json::Value, TransferError> {
        let mut deserializer = serde_json::Deserializer::from_reader(&mut self.reader);
        serde::Deserialize::deserialize(&mut deserializer)
            .map_err(|e| TransferError::Source(format!("invalid JSON: {e}")))
    }

    fn expect_end_of_file(&mut self) -> Result<(), TransferError> {
        match next_non_whitespace(&mut self.reader)? {
            None => Ok(()),
            Some(_) => Err(invalid_json("unexpected content after the array")),
        }
    }
}

fn invalid_json(reason: &str) -> TransferError {
    TransferError::Source(format!("invalid JSON: {reason}"))
}

/// Skips JSON whitespace (space, tab, line feed, carriage return), returning
/// the next byte without consuming it.
fn next_non_whitespace(reader: &mut BufReader<File>) -> Result<Option<u8>, TransferError> {
    loop {
        let buffer = reader
            .fill_buf()
            .map_err(|e| TransferError::Source(e.to_string()))?;
        let Some(&byte) = buffer.first() else {
            return Ok(None);
        };
        if matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
            reader.consume(1);
        } else {
            return Ok(Some(byte));
        }
    }
}

/// Consumes a UTF-8 byte order mark at the start of the file, the only place
/// one may appear.
fn skip_byte_order_mark(reader: &mut BufReader<File>) -> Result<(), TransferError> {
    let buffer = reader
        .fill_buf()
        .map_err(|e| TransferError::Source(e.to_string()))?;
    if buffer.starts_with(b"\xEF\xBB\xBF") {
        reader.consume(3);
    }
    Ok(())
}

fn open_json(file: File) -> Result<SourceReader, TransferError> {
    let mut reader = BufReader::new(file);
    skip_byte_order_mark(&mut reader)?;
    if next_non_whitespace(&mut reader)? == Some(b'[') {
        reader.consume(1);
        return Ok(SourceReader::JsonArray(Box::new(JsonArrayStream::new(
            reader,
        ))));
    }
    Ok(open_json_lines(reader))
}

fn open_json_lines(reader: BufReader<File>) -> SourceReader {
    let stream = serde_json::Deserializer::from_reader(reader).into_iter::<serde_json::Value>();
    SourceReader::Json(Box::new(stream))
}

pub struct FileSource {
    reader: SourceReader,
    columns: Vec<TransferColumn>,
    segment_size: usize,
    estimated_total: Option<u64>,
}

impl FileSource {
    pub fn open(
        path: &Path,
        format: FileFormat,
        columns: Vec<TransferColumn>,
        segment_size: u32,
        estimated_total: Option<u64>,
    ) -> Result<Self, TransferError> {
        let file = File::open(path)
            .map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))?;

        let reader = match format {
            FileFormat::Csv => {
                let csv_reader = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .from_reader(BufReader::new(file));
                SourceReader::Csv(Box::new(csv_reader))
            }
            FileFormat::Json => open_json(file)?,
            FileFormat::JsonLines => open_json_lines(BufReader::new(file)),
            FileFormat::Html | FileFormat::Markdown | FileFormat::Xml => {
                return Err(TransferError::Source(format!(
                    "{}: {} files cannot be imported",
                    path.display(),
                    format.label()
                )));
            }
        };

        Ok(Self {
            reader,
            columns,
            segment_size: segment_size.max(1) as usize,
            estimated_total,
        })
    }
}

fn read_csv_chunk(
    reader: &mut csv::Reader<BufReader<File>>,
    columns: &[TransferColumn],
    segment_size: usize,
) -> Result<Vec<Vec<Value>>, TransferError> {
    let mut rows = Vec::new();

    for record in reader.records().take(segment_size) {
        let record = record.map_err(|e| TransferError::Source(e.to_string()))?;

        let row = columns
            .iter()
            .zip(record.iter())
            .map(|(col, field)| value_from_csv_field(field, col.type_name.as_deref()))
            .collect();

        rows.push(row);
    }

    Ok(rows)
}

/// Pulls up to `segment_size` values from the NDJSON stream, converting each
/// to a row. Stops early (without error) once the stream is exhausted; a
/// malformed value anywhere in the file surfaces as a `TransferError` instead
/// of silently truncating the import.
fn read_json_chunk(
    values: &mut JsonValueStream,
    columns: &[TransferColumn],
    segment_size: usize,
) -> Result<Vec<Vec<Value>>, TransferError> {
    let mut rows = Vec::with_capacity(segment_size);

    for _ in 0..segment_size {
        match values.next() {
            Some(Ok(value)) => rows.push(json_row_to_values(&value, columns)),
            Some(Err(e)) => return Err(TransferError::Source(format!("invalid NDJSON: {e}"))),
            None => break,
        }
    }

    Ok(rows)
}

fn read_json_array_chunk(
    array: &mut JsonArrayStream,
    columns: &[TransferColumn],
    segment_size: usize,
) -> Result<Vec<Vec<Value>>, TransferError> {
    let mut rows = Vec::with_capacity(segment_size);

    while rows.len() < segment_size {
        match array.next_value()? {
            Some(value) => rows.push(json_row_to_values(&value, columns)),
            None => break,
        }
    }

    Ok(rows)
}

fn json_row_to_values(value: &serde_json::Value, columns: &[TransferColumn]) -> Vec<Value> {
    let serde_json::Value::Object(map) = value else {
        return columns.iter().map(|_| Value::Null).collect();
    };

    columns
        .iter()
        .map(|col| {
            map.get(&col.name)
                .map(|v| value_from_json(v, col.type_name.as_deref()))
                .unwrap_or(Value::Null)
        })
        .collect()
}

impl RowSource for FileSource {
    fn columns(&self) -> &[TransferColumn] {
        &self.columns
    }

    fn next_chunk(&mut self, cancel: &CancelToken) -> Result<Option<RowChunk>, TransferError> {
        if cancel.is_cancelled() {
            return Ok(None);
        }

        let rows = match &mut self.reader {
            SourceReader::Csv(reader) => read_csv_chunk(reader, &self.columns, self.segment_size)?,
            SourceReader::Json(values) => {
                read_json_chunk(values, &self.columns, self.segment_size)?
            }
            SourceReader::JsonArray(array) => {
                read_json_array_chunk(array, &self.columns, self.segment_size)?
            }
        };

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RowChunk(rows)))
        }
    }

    fn estimated_total(&self) -> Option<u64> {
        self.estimated_total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some(type_name.to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_file_source_test_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn open_rejects_formats_import_cannot_read() {
        let dir = temp_dir("not_importable");
        let path = dir.join("t.html");
        std::fs::write(&path, "<table></table>").unwrap();

        for format in [FileFormat::Html, FileFormat::Markdown, FileFormat::Xml] {
            assert!(!format.is_importable());
            let Err(TransferError::Source(message)) =
                FileSource::open(&path, format, vec![column("id", "int4")], 10, None)
            else {
                panic!("{format:?} must not open for import");
            };
            assert!(
                message.ends_with(&format!("{} files cannot be imported", format.label())),
                "{message}"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reads_csv_rows_in_chunks_and_decodes_typed_values() {
        let dir = temp_dir("csv_chunks");
        let path = dir.join("public.users.csv");
        std::fs::write(
            &path,
            "id,active,name\n1,true,Alice\n2,false,Bob\n3,true,Cara\n",
        )
        .unwrap();

        let columns = vec![
            column("id", "int4"),
            column("active", "bool"),
            column("name", "text"),
        ];
        let mut source =
            FileSource::open(&path, FileFormat::Csv, columns, 2, Some(3)).expect("open csv");
        let cancel = CancelToken::new();

        let chunk1 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(
            chunk1.0,
            vec![
                vec![
                    Value::Int(1),
                    Value::Bool(true),
                    Value::Text("Alice".to_string())
                ],
                vec![
                    Value::Int(2),
                    Value::Bool(false),
                    Value::Text("Bob".to_string())
                ],
            ]
        );

        let chunk2 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(
            chunk2.0,
            vec![vec![
                Value::Int(3),
                Value::Bool(true),
                Value::Text("Cara".to_string())
            ]]
        );

        assert!(source.next_chunk(&cancel).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn header_only_csv_file_yields_no_chunks() {
        let dir = temp_dir("csv_empty");
        let path = dir.join("empty.csv");
        std::fs::write(&path, "id\n").unwrap();

        let mut source = FileSource::open(
            &path,
            FileFormat::Csv,
            vec![column("id", "int4")],
            10,
            Some(0),
        )
        .expect("open csv");
        let cancel = CancelToken::new();

        assert!(source.next_chunk(&cancel).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_csv_field_decodes_as_null_not_empty_string() {
        let dir = temp_dir("csv_null");
        let path = dir.join("t.csv");
        std::fs::write(&path, "id,email\n1,\n").unwrap();

        let columns = vec![column("id", "int4"), column("email", "text")];
        let mut source =
            FileSource::open(&path, FileFormat::Csv, columns, 10, Some(1)).expect("open csv");
        let cancel = CancelToken::new();

        let chunk = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(chunk.0, vec![vec![Value::Int(1), Value::Null]]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// JD-W1 regression: JSON import reads NDJSON (one object per line), not
    /// a single top-level array — pre-fix, `FileSource::open` parsed the
    /// whole file into a `Vec<serde_json::Value>` and would reject this
    /// bracket-less NDJSON input outright.
    #[test]
    fn reads_ndjson_rows_in_chunks_and_decodes_typed_values() {
        let dir = temp_dir("json_chunks");
        let path = dir.join("public.widgets.json");
        std::fs::write(
            &path,
            "{\"id\":1,\"price\":\"9.99\"}\n{\"id\":2,\"price\":\"5.00\"}\n",
        )
        .unwrap();

        let columns = vec![column("id", "int4"), column("price", "numeric")];
        let mut source =
            FileSource::open(&path, FileFormat::Json, columns, 1, Some(2)).expect("open json");
        let cancel = CancelToken::new();

        let chunk1 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(
            chunk1.0,
            vec![vec![Value::Int(1), Value::Decimal("9.99".to_string())]]
        );

        let chunk2 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(
            chunk2.0,
            vec![vec![Value::Int(2), Value::Decimal("5.00".to_string())]]
        );

        assert!(source.next_chunk(&cancel).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_source_reads_a_json_array_in_chunks() {
        let dir = temp_dir("json_array_chunks");
        let path = dir.join("public.widgets.json");
        std::fs::write(
            &path,
            " \n[\n{\"id\":1,\"price\":\"9.99\"},\n{\"id\":2,\"price\":\"5.00\"}\n]\n",
        )
        .unwrap();

        let columns = vec![column("id", "int4"), column("price", "numeric")];
        let mut source =
            FileSource::open(&path, FileFormat::Json, columns, 1, Some(2)).expect("open json");
        let cancel = CancelToken::new();

        let chunk1 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(
            chunk1.0,
            vec![vec![Value::Int(1), Value::Decimal("9.99".to_string())]]
        );
        let chunk2 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(
            chunk2.0,
            vec![vec![Value::Int(2), Value::Decimal("5.00".to_string())]]
        );
        assert!(source.next_chunk(&cancel).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_source_skips_a_byte_order_mark_at_the_start() {
        let dir = temp_dir("json_array_bom");
        let path = dir.join("t.json");
        std::fs::write(&path, "\u{feff}[{\"id\":1}]").unwrap();

        let mut source =
            FileSource::open(&path, FileFormat::Json, vec![column("id", "int4")], 5, None)
                .expect("open json");

        let chunk = source.next_chunk(&CancelToken::new()).unwrap().unwrap();
        assert_eq!(chunk.0, vec![vec![Value::Int(1)]]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_source_reads_an_empty_json_array_as_no_rows() {
        let dir = temp_dir("json_array_empty");
        let path = dir.join("t.json");
        std::fs::write(&path, "[\n]\n").unwrap();

        let mut source =
            FileSource::open(&path, FileFormat::Json, vec![column("id", "int4")], 5, None)
                .expect("open json");

        assert!(source.next_chunk(&CancelToken::new()).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_source_reports_an_unterminated_array() {
        let dir = temp_dir("json_array_unterminated");
        let path = dir.join("t.json");
        std::fs::write(&path, "[{\"id\":1},").unwrap();

        let mut source =
            FileSource::open(&path, FileFormat::Json, vec![column("id", "int4")], 5, None)
                .expect("open json");

        let result = source.next_chunk(&CancelToken::new());
        assert!(
            result.is_err(),
            "a truncated array must not import silently"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_source_rejects_malformed_arrays() {
        let dir = temp_dir("json_array_malformed");
        let path = dir.join("t.json");
        let cases = [
            (
                "[{\"id\":1}{\"id\":2}]",
                "expected `,` or `]` after an array element",
            ),
            ("[,{\"id\":1}]", "expected an object, found a separator"),
            ("[{\"id\":1},]", "expected an object, found a separator"),
            (
                "[{\"id\":1},,{\"id\":2}]",
                "expected an object, found a separator",
            ),
            (
                "[{\"id\":1}][{\"id\":2}]",
                "unexpected content after the array",
            ),
            (
                "[{\"id\":1}] trailing",
                "unexpected content after the array",
            ),
            ("[1]", "each array element must be an object"),
            (
                "[{\"id\":1},\u{c}{\"id\":2}]",
                "each array element must be an object",
            ),
            (
                "[{\"id\":1},\u{feff}{\"id\":2}]",
                "each array element must be an object",
            ),
            ("[]\u{feff}", "unexpected content after the array"),
            ("[]\u{c}", "unexpected content after the array"),
        ];

        for (contents, reason) in cases {
            std::fs::write(&path, contents).unwrap();
            let mut source = FileSource::open(
                &path,
                FileFormat::Json,
                vec![column("id", "int4")],
                10,
                None,
            )
            .expect("open json");
            match source.next_chunk(&CancelToken::new()) {
                Err(TransferError::Source(message)) => {
                    assert_eq!(message, format!("invalid JSON: {reason}"), "{contents}")
                }
                other => panic!("{contents} must be rejected, got {other:?}"),
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_lines_source_reads_one_object_per_line() {
        let dir = temp_dir("jsonl_chunks");
        let path = dir.join("t.jsonl");
        std::fs::write(&path, "{\"id\":1}\n{\"id\":2}\n").unwrap();

        let mut source = FileSource::open(
            &path,
            FileFormat::JsonLines,
            vec![column("id", "int4")],
            5,
            None,
        )
        .expect("open jsonl");

        let chunk = source.next_chunk(&CancelToken::new()).unwrap().unwrap();
        assert_eq!(chunk.0, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// JD-W1 regression: proves the JSON path is truly incremental rather
    /// than parsing the whole file up front — a malformed value near the end
    /// of the file must not prevent the valid rows before it from being
    /// yielded as chunks. A whole-file `Vec<Value>` parse (the pre-fix
    /// behavior) would fail in `open()` before any chunk was ever produced.
    #[test]
    fn ndjson_source_streams_valid_rows_before_a_later_malformed_value_errors() {
        let dir = temp_dir("json_streaming_proof");
        let path = dir.join("t.json");
        std::fs::write(&path, "{\"id\":1}\n{\"id\":2}\nnot-json\n").unwrap();

        let columns = vec![column("id", "int4")];
        let mut source =
            FileSource::open(&path, FileFormat::Json, columns, 1, None).expect("open json");
        let cancel = CancelToken::new();

        let chunk1 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(chunk1.0, vec![vec![Value::Int(1)]]);

        let chunk2 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(chunk2.0, vec![vec![Value::Int(2)]]);

        let result = source.next_chunk(&cancel);
        assert!(
            result.is_err(),
            "a malformed trailing value must surface as an error, not be silently skipped"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_ndjson_file_yields_no_chunks() {
        let dir = temp_dir("json_empty");
        let path = dir.join("empty.json");
        std::fs::write(&path, "").unwrap();

        let mut source = FileSource::open(
            &path,
            FileFormat::Json,
            vec![column("id", "int4")],
            10,
            Some(0),
        )
        .expect("open json");
        let cancel = CancelToken::new();

        assert!(source.next_chunk(&cancel).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_fails_when_the_file_is_missing() {
        let dir = temp_dir("missing");
        let path = dir.join("does_not_exist.csv");

        let result = FileSource::open(&path, FileFormat::Csv, vec![column("id", "int4")], 10, None);

        assert!(result.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cancelled_token_yields_no_chunks_even_with_data_remaining() {
        let dir = temp_dir("cancelled");
        let path = dir.join("t.csv");
        std::fs::write(&path, "id\n1\n2\n").unwrap();

        let mut source = FileSource::open(
            &path,
            FileFormat::Csv,
            vec![column("id", "int4")],
            10,
            Some(2),
        )
        .expect("open csv");
        let cancel = CancelToken::new();
        cancel.cancel();

        assert!(source.next_chunk(&cancel).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn estimated_total_returns_the_constructor_value() {
        let dir = temp_dir("estimated_total");
        let path = dir.join("t.csv");
        std::fs::write(&path, "id\n1\n").unwrap();

        let source = FileSource::open(
            &path,
            FileFormat::Csv,
            vec![column("id", "int4")],
            10,
            Some(1),
        )
        .expect("open csv");

        assert_eq!(source.estimated_total(), Some(1));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// JD-W1 regression: a JSON export -> import round trip via
    /// `FileSink`/`FileSource` preserves every row, proving the NDJSON write
    /// and streaming read sides agree on the wire format end to end.
    #[test]
    fn json_export_then_import_round_trip_preserves_all_rows() {
        use crate::file_sink::FileSink;
        use crate::pipeline::{RowChunk, RowSink, TableMappingMode};

        let dir = temp_dir("json_round_trip");
        let columns = vec![column("id", "int4"), column("name", "text")];

        let mut sink = FileSink::new(
            &dir,
            Some("public".to_string()),
            "widgets",
            FileFormat::Json,
        );
        sink.begin(&columns, TableMappingMode::Existing).unwrap();
        sink.write_chunk(&RowChunk(vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ]))
        .unwrap();
        sink.write_chunk(&RowChunk(vec![vec![Value::Int(3), Value::Null]]))
            .unwrap();
        sink.finish().unwrap();

        let path = dir.join("public.widgets.json");
        let mut source =
            FileSource::open(&path, FileFormat::Json, columns, 2, Some(3)).expect("open json");
        let cancel = CancelToken::new();

        let mut all_rows = Vec::new();
        while let Some(chunk) = source.next_chunk(&cancel).unwrap() {
            all_rows.extend(chunk.0);
        }

        assert_eq!(
            all_rows,
            vec![
                vec![Value::Int(1), Value::Text("Alice".to_string())],
                vec![Value::Int(2), Value::Text("Bob".to_string())],
                vec![Value::Int(3), Value::Null],
            ]
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
