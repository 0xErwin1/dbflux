#![allow(clippy::panic)]

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
