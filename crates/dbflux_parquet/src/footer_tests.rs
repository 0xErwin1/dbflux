#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use dbflux_byte_source::{ByteSource, MemorySource, SourceError};
use parquet::arrow::ArrowWriter;
use parquet::basic::CompressionCodec;
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
};
use parquet::file::properties::WriterProperties;

use crate::test_support::CountingSource;
use crate::{MAX_FOOTER_BYTES, ParquetError, TAIL_READ_BYTES, open};

/// Returns one byte less than asked for on every read.
struct ShortSource(MemorySource);

impl ByteSource for ShortSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.0.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let mut bytes = self.0.read_range(range)?;
        bytes.pop();

        Ok(bytes)
    }
}

/// Fails every read.
struct FailingSource;

impl ByteSource for FailingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(1024)
    }

    fn read_range(&self, _range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        Err(SourceError::new("the object store is unreachable"))
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn batch(rows: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("name {row}")),
    ));

    RecordBatch::try_new(schema(), vec![ids, names]).unwrap()
}

fn write_parquet(batches: &[RecordBatch], properties: Option<WriterProperties>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema(), properties).unwrap();

    for batch in batches {
        writer.write(batch).unwrap();
    }

    writer.close().unwrap();

    buffer
}

fn rows_per_row_group(rows: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows))
        .build()
}

/// The offset where the footer metadata of `bytes` starts, from the metadata
/// length in its last eight bytes.
fn metadata_start(bytes: &[u8]) -> usize {
    let length_bytes: [u8; 4] = bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap();
    let metadata_length = u32::from_le_bytes(length_bytes) as usize;

    bytes.len() - 8 - metadata_length
}

/// Replaces the footer of `bytes` with `metadata`, keeping the column data and
/// the page index where they are.
fn replace_footer(bytes: &[u8], metadata: &ParquetMetaData) -> Vec<u8> {
    let mut rewritten = bytes[..metadata_start(bytes)].to_vec();

    ParquetMetaDataWriter::new(&mut rewritten, metadata)
        .finish()
        .unwrap();

    rewritten
}

fn read_metadata(bytes: &[u8]) -> ParquetMetaData {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&Bytes::from(bytes.to_vec()))
        .unwrap()
}

/// A footer that claims `metadata_length` bytes of metadata after `body`.
fn footer_bytes(body: &[u8], metadata_length: u32, magic: &[u8; 4]) -> Vec<u8> {
    let mut bytes = b"PAR1".to_vec();
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&metadata_length.to_le_bytes());
    bytes.extend_from_slice(magic);

    bytes
}

#[test]
fn open_reads_schema_and_row_count_in_one_tail_request() {
    let bytes = write_parquet(&[batch(3)], None);
    let length = bytes.len() as u64;
    assert!(length < TAIL_READ_BYTES);

    let source = CountingSource::new(MemorySource::new(bytes));
    let file = open(&source).unwrap();

    // The tail read covers the whole small file, and with it the footer and
    // the page index that the writer puts just before the footer.
    assert_eq!(source.requests(), vec![0..length]);

    assert_eq!(file.row_count(), 3);
    assert_eq!(file.row_group_count(), 1);

    let names: Vec<&str> = file
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, vec!["id", "name"]);
    assert_eq!(file.schema().field(0).data_type(), &DataType::Int64);
}

#[test]
fn open_loads_the_offset_index_of_every_row_group() {
    let bytes = write_parquet(&[batch(6)], Some(rows_per_row_group(2)));

    let file = open(&MemorySource::new(bytes)).unwrap();

    assert_eq!(file.row_count(), 6);
    assert_eq!(file.row_group_count(), 3);
    assert!(file.has_offset_index());

    let offset_index = file.metadata().offset_index().unwrap();
    assert_eq!(offset_index.len(), 3);

    for row_group in offset_index {
        assert_eq!(row_group.len(), 2);

        for column in row_group {
            assert!(!column.page_locations().is_empty());
        }
    }

    assert!(file.metadata().column_index().is_some());
}

#[test]
fn a_footer_larger_than_the_tail_takes_two_more_requests() {
    // One batch per row group: `ArrowWriter::write` recurses once for every
    // row group it splits a batch into, which overflows a test thread's stack.
    let batches: Vec<RecordBatch> = (0..2_000).map(|_| batch(1)).collect();
    let bytes = write_parquet(&batches, Some(rows_per_row_group(1)));
    let length = bytes.len() as u64;
    let footer_start = metadata_start(&bytes) as u64;
    assert!(length - footer_start > TAIL_READ_BYTES);

    let source = CountingSource::new(MemorySource::new(bytes));
    let file = open(&source).unwrap();

    let requests = source.requests();
    assert_eq!(requests.len(), 3, "{requests:?}");

    // The tail, then the whole metadata (the decoder asks for it in one range
    // even though the tail already holds its end), then the page index that
    // lies before the metadata.
    assert_eq!(requests[0], length - TAIL_READ_BYTES..length);
    assert_eq!(requests[1], footer_start..length - 8);
    assert!(requests[2].end <= footer_start);

    assert_eq!(file.row_group_count(), 2_000);
    assert!(file.has_offset_index());
}

#[test]
fn zero_row_groups_opens_empty_without_panic() {
    let bytes = write_parquet(&[], None);

    let file = open(&MemorySource::new(bytes)).unwrap();

    assert_eq!(file.row_count(), 0);
    assert_eq!(file.row_group_count(), 0);
    assert!(file.has_offset_index());
    assert_eq!(file.schema().fields().len(), 2);
}

#[test]
fn a_file_without_the_parquet_magic_is_refused() {
    let text = MemorySource::new(b"id,name\n1,Ada\n2,Grace\n".to_vec());
    assert!(matches!(open(&text), Err(ParquetError::NotParquet { .. })));

    let tiny = MemorySource::new(b"PAR1".to_vec());
    assert!(matches!(open(&tiny), Err(ParquetError::NotParquet { .. })));
}

#[test]
fn footer_over_cap_is_refused() {
    let claimed = u32::try_from(MAX_FOOTER_BYTES + 1).unwrap();
    let source = MemorySource::new(footer_bytes(&[0; 16], claimed, b"PAR1"));

    match open(&source) {
        Err(ParquetError::FooterTooLarge { length, limit }) => {
            assert_eq!(length, MAX_FOOTER_BYTES + 1);
            assert_eq!(limit, MAX_FOOTER_BYTES);
        }
        other => panic!("expected FooterTooLarge, got {other:?}"),
    }
}

#[test]
fn a_footer_longer_than_the_file_is_malformed() {
    let source = MemorySource::new(footer_bytes(&[0; 16], 1_000, b"PAR1"));

    assert!(matches!(open(&source), Err(ParquetError::Malformed { .. })));
}

#[test]
fn an_encrypted_footer_is_refused() {
    let source = MemorySource::new(footer_bytes(&[0; 16], 8, b"PARE"));

    assert!(matches!(open(&source), Err(ParquetError::Encrypted)));
}

/// The writer cannot produce Brotli pages without the `brotli` feature, so the
/// fixture rewrites the footer of an uncompressed file to declare Brotli. Only
/// the footer is read when the file opens, so the pages never need to decode.
#[test]
fn a_brotli_column_names_the_codec() {
    let bytes = write_parquet(&[batch(3)], None);
    let metadata = read_metadata(&bytes);

    let row_groups = metadata
        .row_groups()
        .iter()
        .map(|row_group| {
            let columns = row_group
                .columns()
                .iter()
                .map(|column| {
                    let codec = if column.column_path().string() == "name" {
                        CompressionCodec::BROTLI
                    } else {
                        column.compression_codec()
                    };

                    column
                        .clone()
                        .into_builder()
                        .set_compression_codec(codec)
                        .build()
                        .unwrap()
                })
                .collect();

            row_group
                .clone()
                .into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();

    let brotli = ParquetMetaData::new(metadata.file_metadata().clone(), row_groups);
    let source = MemorySource::new(replace_footer(&bytes, &brotli));

    match open(&source) {
        Err(ParquetError::UnsupportedCodec { codec, column }) => {
            assert_eq!(codec, "BROTLI");
            assert_eq!(column, "name");
        }
        other => panic!("expected UnsupportedCodec, got {other:?}"),
    }
}

#[test]
fn a_short_read_from_the_source_is_reported() {
    let bytes = write_parquet(&[batch(3)], None);
    let length = bytes.len() as u64;

    match open(&ShortSource(MemorySource::new(bytes))) {
        Err(ParquetError::ShortRead {
            offset,
            expected,
            returned,
        }) => {
            assert_eq!(offset, 0);
            assert_eq!(expected, length);
            assert_eq!(returned, length - 1);
        }
        other => panic!("expected ShortRead, got {other:?}"),
    }
}

#[test]
fn a_source_failure_keeps_its_message() {
    match open(&FailingSource) {
        Err(ParquetError::Source(error)) => {
            assert_eq!(error.to_string(), "the object store is unreachable");
        }
        other => panic!("expected Source, got {other:?}"),
    }
}
