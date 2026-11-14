//! SQL export: batched, re-runnable `INSERT` statements rendered by the source
//! connection's own [`QueryGenerator::generate_bulk_insert`], so the dialect,
//! quoting and literals are the driver's, not a second implementation here.

use std::io::Write;

use dbflux_core::{Connection, DriverCapabilities, QueryGenerator, TransferColumn, Value};

use crate::pipeline::TransferError;
use crate::table_sink::TableSink;

/// Whether `connection` can write SQL exports: it declares
/// [`DriverCapabilities::BULK_INSERT`] and its generator renders an `INSERT`.
///
/// The capability alone is not enough: it is a driver-level flag, while the
/// generator is what actually produces the text, and a generator may still
/// answer `None` (the engine's per-row fallback). Probing the generator with a
/// one-row sample is pure and cheap, and is the same call the export makes, so
/// the format is offered exactly when the export can succeed.
pub fn supports_sql_export(connection: &dyn Connection) -> bool {
    if !connection.supports(DriverCapabilities::BULK_INSERT) {
        return false;
    }

    connection
        .query_generator()
        .is_some_and(generator_renders_inserts)
}

fn generator_renders_inserts(generator: &dyn QueryGenerator) -> bool {
    let sample = [Value::Null];
    let rows: [&[Value]; 1] = [&sample];

    matches!(
        generator.generate_bulk_insert(None, "probe", &["probe".to_string()], &[None], &rows),
        Ok(Some(_))
    )
}

/// Rows per `INSERT` for a run: the requested size (at least one), capped at
/// the driver's `max_bulk_insert_rows` when it sets one.
pub(crate) fn effective_batch_size(requested: u32, driver_cap: Option<usize>) -> usize {
    let requested = usize::try_from(requested).unwrap_or(usize::MAX).max(1);

    match driver_cap {
        Some(cap) => requested.min(cap.max(1)),
        None => requested,
    }
}

/// Driver cap on rows per bulk insert, from the connection's metadata.
pub(crate) fn driver_batch_cap(connection: &dyn Connection) -> Option<usize> {
    TableSink::max_bulk_insert_rows(connection.metadata())
}

/// Buffers rows and writes one `INSERT ... ;` statement per full batch.
pub(crate) struct SqlInsertWriter<W: Write> {
    out: W,
    schema: Option<String>,
    table: String,
    column_names: Vec<String>,
    column_types: Vec<Option<String>>,
    batch_size: usize,
    pending: Vec<Vec<Value>>,
}

impl<W: Write> SqlInsertWriter<W> {
    pub(crate) fn new(
        out: W,
        schema: Option<String>,
        table: String,
        columns: &[TransferColumn],
        batch_size: usize,
    ) -> Self {
        Self {
            out,
            schema,
            table,
            column_names: columns.iter().map(|column| column.name.clone()).collect(),
            column_types: columns
                .iter()
                .map(|column| column.type_name.clone())
                .collect(),
            batch_size: batch_size.max(1),
            pending: Vec::new(),
        }
    }

    pub(crate) fn write_row(
        &mut self,
        generator: &dyn QueryGenerator,
        row: &[Value],
    ) -> Result<(), TransferError> {
        self.pending.push(row.to_vec());

        if self.pending.len() >= self.batch_size {
            self.flush_batch(generator)?;
        }

        Ok(())
    }

    /// Writes the last partial batch. A table with no rows writes nothing, so
    /// the file is empty: there is nothing to re-run.
    pub(crate) fn finish(mut self, generator: &dyn QueryGenerator) -> Result<(), TransferError> {
        self.flush_batch(generator)?;
        self.out
            .flush()
            .map_err(|error| TransferError::Sink(error.to_string()))
    }

    fn flush_batch(&mut self, generator: &dyn QueryGenerator) -> Result<(), TransferError> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let rows: Vec<&[Value]> = self.pending.iter().map(Vec::as_slice).collect();
        let query = generator
            .generate_bulk_insert(
                self.schema.as_deref(),
                &self.table,
                &self.column_names,
                &self.column_types,
                &rows,
            )
            .map_err(|error| TransferError::Sink(error.to_string()))?
            .ok_or_else(|| {
                TransferError::Sink(format!(
                    "the driver cannot generate INSERT statements for '{}'",
                    self.table
                ))
            })?;

        writeln!(self.out, "{};", query.text)
            .map_err(|error| TransferError::Sink(error.to_string()))?;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
