//! Single-file import: one loose CSV, JSON, JSON Lines, XML or spreadsheet
//! file becomes a one-table import, with no `manifest.json` on disk.
//!
//! [`ImportFile::inspect`] reads the columns off the file (the CSV header row,
//! the spreadsheet's first row, or the keys of the first JSON records and the
//! field names of the first XML rows) and [`ImportFile::manifest_table`]
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
use crate::file_source::{check_file_readable, check_no_late_fields, sample_object_keys};
use crate::import::{ImportOptions, ImportOutcome, ImportTablePlan, TableFile, run_plans};
use crate::manifest::ManifestTable;
use crate::pipeline::TransferError;
use crate::spreadsheet_source::{SheetRows, list_worksheets};
use crate::xml_source::sample_field_names;

/// How many JSON records are read to discover the columns.
pub const DISCOVERY_SAMPLE_ROWS: usize = 100;

/// The importable format a path's extension names, case-insensitively. HTML,
/// Markdown and SQL files are not importable and give `None`.
pub fn format_for_path(path: &Path) -> Option<FileFormat> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();

    FileFormat::from_extension(&extension).filter(|format| format.is_importable())
}

/// One worksheet of a workbook, as offered for choosing the sheet to import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSheet {
    pub name: String,
    /// `false` for hidden and very hidden sheets.
    pub visible: bool,
}

/// The worksheets of the workbook at `path`, in workbook order (chart
/// sheets hold no cells and are left out).
pub fn list_sheets(path: &Path) -> Result<Vec<ImportSheet>, TransferError> {
    Ok(list_worksheets(path)?
        .into_iter()
        .map(|sheet| ImportSheet {
            name: sheet.name,
            visible: sheet.visible,
        })
        .collect())
}

/// A loose file inspected for import: its format, the options it is read
/// with, and the columns found in it.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportFile {
    path: PathBuf,
    format: FileFormat,
    options: FormatOptions,
    columns: Vec<TransferColumn>,
    sheet: Option<String>,
}

impl ImportFile {
    /// Reads the columns of the file at `path`.
    pub fn inspect(
        path: &Path,
        format: FileFormat,
        options: FormatOptions,
    ) -> Result<Self, TransferError> {
        Self::inspect_sheet(path, format, options, None)
    }

    /// Like [`Self::inspect`], reading the spreadsheet sheet named `sheet`
    /// (the first worksheet when `None`). Other formats ignore `sheet`.
    pub fn inspect_sheet(
        path: &Path,
        format: FileFormat,
        options: FormatOptions,
        sheet: Option<&str>,
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
            FileFormat::Spreadsheet => spreadsheet_header(path, sheet)?,
            FileFormat::Xml => xml_keys(path)?,
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
            sheet: sheet.map(str::to_string),
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

    /// The spreadsheet sheet chosen at inspection, `None` for the first
    /// worksheet and for every other format.
    pub fn sheet(&self) -> Option<&str> {
        self.sheet.as_deref()
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
                sheet: file.sheet.clone(),
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
    check_file_readable(
        &file.path,
        file.format,
        &file.columns,
        &file.options,
        file.sheet(),
        segment_size,
        cancel,
    )?;

    check_no_late_fields(&file.path, file.format, &file.columns, cancel)
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

    check_header_names(path, &names)?;

    Ok(names)
}

/// Rejects a header with no columns, a blank name or a repeated name, the
/// same rules for a CSV header row and a spreadsheet's first row.
fn check_header_names(path: &Path, names: &[String]) -> Result<(), TransferError> {
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

    Ok(())
}

/// The first row of the sheet, which must be a valid header.
fn spreadsheet_header(path: &Path, sheet: Option<&str>) -> Result<Vec<String>, TransferError> {
    let rows = SheetRows::open(path, sheet)?;
    let names = rows.headers().to_vec();

    check_header_names(path, &names)?;

    Ok(names)
}

/// The union of the field names of the first sampled XML rows, in the order
/// they are first seen.
fn xml_keys(path: &Path) -> Result<Vec<String>, TransferError> {
    let rows = sample_field_names(path, DISCOVERY_SAMPLE_ROWS)?;

    if rows.is_empty() {
        return Err(TransferError::Source(format!(
            "{}: the file has no records",
            path.display()
        )));
    }

    unique_keys(path, rows)
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

    unique_keys(path, records)
}

fn unique_keys(path: &Path, records: Vec<Vec<String>>) -> Result<Vec<String>, TransferError> {
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
