//! File -> Row `RowSource`: reads one previously-exported `schema.table.<ext>`
//! file back into row chunks for Import, recovering typed `Value`s via
//! `value_codec` guided by the manifest's per-column `type_name`.
//!
//! Both CSV and JSON are read incrementally in bounded-memory chunks: CSV via
//! `csv::Reader`, JSON via `serde_json::Deserializer::into_iter`, which
//! parses one whitespace/newline-delimited top-level value at a time from the
//! NDJSON `dbflux_export::JsonStreamWriter` writes (see `file_sink`). Neither
//! path ever holds the whole file's rows resident in memory at once.

use std::fs::File;
use std::io::BufReader;
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
            FileFormat::Json => {
                let stream = serde_json::Deserializer::from_reader(BufReader::new(file))
                    .into_iter::<serde_json::Value>();
                SourceReader::Json(Box::new(stream))
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
