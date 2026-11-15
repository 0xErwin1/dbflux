//! Single-file import: one loose CSV, JSON or JSON Lines file becomes a
//! one-table import, with no `manifest.json` on disk.
//!
//! [`ImportFile::inspect`] reads the columns off the file (the CSV header row,
//! or the keys of the first JSON records) and [`ImportFile::manifest_table`]
//! describes it as the in-memory equivalent of a one-table manifest.
//! [`run_file_import`] then runs the same pipeline as a folder import
//! ([`crate::import`]), so the column mapping, the table mapping modes, the
//! per-table transaction and the cancel and progress behavior are shared.
//!
//! A loose file has no source type names. Values read for an existing table
//! decode by that table's column types; for a new table they stay as the text
//! or JSON scalars the file holds.

use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, TransferColumn};
use dbflux_export::{FormatOptions, decode_to_utf8};

use crate::file_sink::FileFormat;
use crate::file_source::{FileSource, sample_object_keys};
use crate::import::{ImportOptions, ImportOutcome, ImportTablePlan, TableFile, run_plans};
use crate::manifest::ManifestTable;
use crate::pipeline::{RowSource, TransferError};

/// How many JSON records are read to discover the columns.
pub const DISCOVERY_SAMPLE_ROWS: usize = 100;

/// The importable format a path's extension names, case-insensitively. HTML,
/// Markdown, XML and SQL files are not importable and give `None`.
pub fn format_for_path(path: &Path) -> Option<FileFormat> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();

    FileFormat::from_extension(&extension).filter(|format| format.is_importable())
}

/// A loose file inspected for import: its format, the options it is read
/// with, and the columns found in it.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportFile {
    path: PathBuf,
    format: FileFormat,
    options: FormatOptions,
    columns: Vec<TransferColumn>,
}

impl ImportFile {
    /// Reads the columns of the file at `path`.
    pub fn inspect(
        path: &Path,
        format: FileFormat,
        options: FormatOptions,
    ) -> Result<Self, TransferError> {
        if !format.is_importable() {
            return Err(TransferError::Source(format!(
                "{}: {} files cannot be imported",
                path.display(),
                format.label()
            )));
        }

        let column_names = match format {
            FileFormat::Csv => csv_header(path, &options)?,
            _ => json_keys(path, format)?,
        };
        let columns = column_names
            .into_iter()
            .map(|name| TransferColumn {
                name,
                type_name: None,
                nullable: true,
                is_primary_key: false,
            })
            .collect();

        Ok(Self {
            path: path.to_path_buf(),
            format,
            options,
            columns,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn format(&self) -> FileFormat {
        self.format
    }

    pub fn options(&self) -> &FormatOptions {
        &self.options
    }

    pub fn columns(&self) -> &[TransferColumn] {
        &self.columns
    }

    /// The default target table name: the file name without its extension.
    pub fn table_name(&self) -> String {
        self.path.file_stem().map_or_else(
            || "import".to_string(),
            |stem| stem.to_string_lossy().into_owned(),
        )
    }

    /// The one-table manifest entry this file stands for, kept in memory.
    pub fn manifest_table(&self) -> ManifestTable {
        ManifestTable {
            schema: None,
            name: self.table_name(),
            file: self
                .path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            format: self.format.extension().to_string(),
            columns: self.columns.clone(),
            row_count: 0,
            fk_order_index: 0,
            options: self.options.clone(),
        }
    }
}

/// Imports `file` into the table `plan` names. The file is read through once
/// first, and a malformed row or record fails the call with nothing written. `plan.source_table` must be
/// [`ImportFile::table_name`]. `on_progress` receives `(rows_done,
/// estimated_total)`; a loose file has no row count, so the estimate is
/// always `None`.
pub fn run_file_import(
    connection: &Arc<dyn Connection>,
    file: &ImportFile,
    plan: &ImportTablePlan,
    options: &ImportOptions,
    cancel: &CancelToken,
    mut on_progress: impl FnMut(u64, Option<u64>),
) -> Result<ImportOutcome, TransferError> {
    check_readable(file, options.segment_size, cancel)?;

    run_plans(
        connection,
        std::slice::from_ref(plan),
        options,
        cancel,
        |_index, rows_done, estimated_total| on_progress(rows_done, estimated_total),
        |_plan| {
            Some(Ok(TableFile {
                path: file.path.clone(),
                format: file.format,
                columns: file.columns.clone(),
                estimated_total: None,
                options: file.options.clone(),
                loose: true,
            }))
        },
        |plan| format!("no file to import into table '{}'", plan.target_table),
    )
}

/// Reads the whole file once without writing anything, so a malformed row or
/// record rejects the import before the target is touched. Not every target
/// wraps an import in a transaction, and a failure found halfway through would
/// leave the rows written so far.
fn check_readable(
    file: &ImportFile,
    segment_size: u32,
    cancel: &CancelToken,
) -> Result<(), TransferError> {
    let mut source = FileSource::open_with(
        &file.path,
        file.format,
        file.columns.clone(),
        segment_size,
        None,
        file.options.clone(),
    )?;

    while source.next_chunk(cancel)?.is_some() {}

    Ok(())
}

/// The CSV header row, read the way the import reads the file: with the
/// chosen delimiter and encoding.
fn csv_header(path: &Path, options: &FormatOptions) -> Result<Vec<String>, TransferError> {
    let file =
        File::open(path).map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(options.delimiter.byte())
        .from_reader(decode_to_utf8(BufReader::new(file), options.encoding));

    let names: Vec<String> = reader
        .headers()
        .map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))?
        .iter()
        .map(str::to_string)
        .collect();

    if names.is_empty() {
        return Err(TransferError::Source(format!(
            "{}: the file has no header row",
            path.display()
        )));
    }

    let mut seen = HashSet::new();
    for (index, name) in names.iter().enumerate() {
        if name.trim().is_empty() {
            return Err(TransferError::Source(format!(
                "{}: header column {} has no name",
                path.display(),
                index + 1
            )));
        }
        if !seen.insert(name.as_str()) {
            return Err(TransferError::Source(format!(
                "{}: header column '{name}' appears more than once",
                path.display()
            )));
        }
    }

    Ok(names)
}

/// The union of the keys of the first sampled JSON records, in the order
/// they are first seen.
fn json_keys(path: &Path, format: FileFormat) -> Result<Vec<String>, TransferError> {
    let records = sample_object_keys(path, format, DISCOVERY_SAMPLE_ROWS)?;

    if records.is_empty() {
        return Err(TransferError::Source(format!(
            "{}: the file has no records",
            path.display()
        )));
    }

    let mut seen = HashSet::new();
    let names: Vec<String> = records
        .into_iter()
        .flatten()
        .filter(|key| seen.insert(key.clone()))
        .collect();

    if names.is_empty() {
        return Err(TransferError::Source(format!(
            "{}: the records have no keys to use as columns",
            path.display()
        )));
    }

    Ok(names)
}

#[cfg(test)]
mod tests;
