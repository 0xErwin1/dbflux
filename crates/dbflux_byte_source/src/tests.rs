#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::cell::Cell;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::ops::Range;

use super::{
    ByteSource, ByteSourceReader, FileSource, MemorySource, READ_AHEAD_BLOCK, SourceError,
};

#[test]
fn source_error_exposes_the_callers_error() {
    let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");

    let error = SourceError::from(io_error);
    assert_eq!(error.to_string(), "access denied");

    let inner = error.into_inner();
    let io_error = inner.downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
}

#[test]
fn memory_source_clamps_a_read_to_its_length() {
    let source = MemorySource::new(b"0123456789".to_vec());

    assert_eq!(source.byte_length().unwrap(), 10);
    assert_eq!(source.read_range(2..5).unwrap(), b"234");
    assert_eq!(source.read_range(8..50).unwrap(), b"89");
    assert_eq!(source.read_range(10..12).unwrap(), b"");
    assert_eq!(source.read_range(40..50).unwrap(), b"");
    assert_eq!(source.bytes(), b"0123456789");
}

#[test]
fn file_source_reads_positioned_ranges() {
    let path = std::env::temp_dir().join(format!(
        "dbflux_byte_source_file_source_{}.csv",
        std::process::id()
    ));
    std::fs::write(&path, b"id,name\n1,ana\n2,luis\n").unwrap();

    let source = FileSource::open(&path).unwrap();

    let length = source.byte_length().unwrap();
    let middle = source.read_range(8..14).unwrap();
    let tail = source.read_range(14..500).unwrap();
    let past_end = source.read_range(21..30).unwrap();

    std::fs::remove_file(&path).unwrap();

    assert_eq!(length, 21);
    assert_eq!(middle, b"1,ana\n");
    assert_eq!(tail, b"2,luis\n");
    assert_eq!(past_end, b"");
}

#[test]
fn file_source_open_error_keeps_the_io_message() {
    let missing = std::env::temp_dir().join("dbflux_byte_source_missing/none.csv");

    let error = FileSource::open(&missing).expect_err("opening a missing file must fail");

    let expected = std::fs::File::open(&missing).unwrap_err().to_string();
    assert_eq!(error.to_string(), expected);
}

/// Bytes whose value is their offset modulo 251, so a misplaced read shows.
fn patterned_bytes(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

struct CountingSource {
    inner: MemorySource,
    requests: Cell<usize>,
}

impl ByteSource for CountingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.inner.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        self.requests.set(self.requests.get() + 1);
        self.inner.read_range(range)
    }
}

struct ShortSource;

impl ByteSource for ShortSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(100)
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let wanted = usize::try_from(range.end.min(100).saturating_sub(range.start)).unwrap_or(0);
        Ok(vec![0; wanted / 2])
    }
}

struct FailingSource;

impl ByteSource for FailingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Err(SourceError::new("bucket is gone"))
    }

    fn read_range(&self, _range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        Err(SourceError::new("bucket is gone"))
    }
}

#[test]
fn reader_seeks_across_block_boundaries() {
    let bytes = patterned_bytes(READ_AHEAD_BLOCK * 3);
    let mut reader = ByteSourceReader::new(MemorySource::new(bytes.clone()));

    let start = READ_AHEAD_BLOCK - 100;
    reader.seek(SeekFrom::Start(start as u64)).unwrap();

    let mut buffer = vec![0; 200];
    reader.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, bytes.get(start..start + 200).unwrap());

    let position = reader.seek(SeekFrom::Current(-150)).unwrap();
    assert_eq!(position, (start + 50) as u64);

    let mut buffer = vec![0; READ_AHEAD_BLOCK + 10];
    reader.read_exact(&mut buffer).unwrap();
    assert_eq!(
        buffer,
        bytes
            .get(start + 50..start + 60 + READ_AHEAD_BLOCK)
            .unwrap()
    );

    let position = reader.seek(SeekFrom::End(-5)).unwrap();
    assert_eq!(position, (bytes.len() - 5) as u64);

    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).unwrap();
    assert_eq!(tail, bytes.get(bytes.len() - 5..).unwrap());
}

#[test]
fn reader_past_end_is_unexpected_eof() {
    let mut reader = ByteSourceReader::new(ShortSource);

    let mut buffer = [0; 10];
    let error = reader.read(&mut buffer).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
}

#[test]
fn reader_reads_zero_at_end_and_rejects_negative_seek() {
    let mut reader = ByteSourceReader::new(MemorySource::new(b"0123456789".to_vec()));

    let mut buffer = [0; 4];

    reader.seek(SeekFrom::End(0)).unwrap();
    assert_eq!(reader.read(&mut buffer).unwrap(), 0);

    reader.seek(SeekFrom::Start(40)).unwrap();
    assert_eq!(reader.read(&mut buffer).unwrap(), 0);

    let error = reader.seek(SeekFrom::Current(-41)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    let error = reader.seek(SeekFrom::End(-11)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    assert_eq!(reader.stream_position().unwrap(), 40);
}

#[test]
fn reader_keeps_the_source_error_message() {
    let mut reader = ByteSourceReader::new(FailingSource);

    let mut buffer = [0; 4];
    let error = reader.read(&mut buffer).unwrap_err();

    assert_eq!(error.to_string(), "bucket is gone");
}

#[test]
fn sequential_small_reads_use_one_request_per_block() {
    let bytes = patterned_bytes(READ_AHEAD_BLOCK * 3);
    let source = CountingSource {
        inner: MemorySource::new(bytes.clone()),
        requests: Cell::new(0),
    };
    let mut reader = ByteSourceReader::new(source);

    let mut collected = Vec::new();
    let mut buffer = [0; 16];

    loop {
        let read = reader.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        collected.extend_from_slice(buffer.get(..read).unwrap());
    }

    assert_eq!(collected, bytes);
    assert_eq!(reader.into_inner().requests.get(), 3);
}
