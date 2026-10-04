#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{ByteSource, FileSource, MemorySource, SourceError};

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
