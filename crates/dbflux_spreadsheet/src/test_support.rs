#![allow(clippy::panic, clippy::unwrap_used)]

use std::io::{Cursor, Read};
use std::path::PathBuf;

use dbflux_byte_source::MemorySource;
use rust_xlsxwriter::XlsxError;

use crate::{SpreadsheetError, Workbook, open};

/// Returns the bytes of a checked-in file under `tests/fixtures/`.
pub(crate) fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name);

    match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) => panic!("cannot read fixture {}: {error}", path.display()),
    }
}

/// Returns the path of a checked-in file under `tests/fixtures/`.
pub(crate) fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

/// Builds an xlsx (or xlsm, when `build` adds a VBA project) in memory.
pub(crate) fn xlsx_bytes(
    build: impl FnOnce(&mut rust_xlsxwriter::Workbook) -> Result<(), XlsxError>,
) -> Vec<u8> {
    let mut workbook = rust_xlsxwriter::Workbook::new();

    if let Err(error) = build(&mut workbook) {
        panic!("cannot build the test workbook: {error}");
    }

    match workbook.save_to_buffer() {
        Ok(bytes) => bytes,
        Err(error) => panic!("cannot save the test workbook: {error}"),
    }
}

pub(crate) fn open_bytes(bytes: Vec<u8>) -> Result<Workbook<MemorySource>, SpreadsheetError> {
    open(MemorySource::new(bytes))
}

/// Asserts that every entry but `changed` is stored exactly as before, in
/// the same order.
pub(crate) fn assert_entries_unchanged(original: &[u8], patched: &[u8], changed: &[&str]) {
    let mut before_archive = zip::ZipArchive::new(Cursor::new(original)).unwrap();
    let mut after_archive = zip::ZipArchive::new(Cursor::new(patched)).unwrap();
    assert_eq!(before_archive.len(), after_archive.len());

    for index in 0..before_archive.len() {
        let mut before = before_archive.by_index_raw(index).unwrap();
        let mut after = after_archive.by_index_raw(index).unwrap();
        assert_eq!(before.name(), after.name(), "entry order");

        if changed.contains(&before.name()) {
            continue;
        }

        assert_eq!(
            before.compression(),
            after.compression(),
            "{}",
            before.name()
        );
        assert_eq!(before.crc32(), after.crc32(), "{}", before.name());

        let mut before_raw = Vec::new();
        let mut after_raw = Vec::new();
        before.read_to_end(&mut before_raw).unwrap();
        after.read_to_end(&mut after_raw).unwrap();
        assert_eq!(before_raw, after_raw, "{}", before.name());
    }
}
