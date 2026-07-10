//! Core Source -> Map -> Sink pipeline shared by Export, Import, and Migration.

use dbflux_core::{CancelToken, TransferColumn, Value};

/// A bounded-memory batch of source rows, all shaped like `RowSource::columns()`.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChunk(pub Vec<Vec<Value>>);

/// Errors raised anywhere in the Source -> Map -> Sink pipeline.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum TransferError {
    #[error("source error: {0}")]
    Source(String),
    #[error("sink error: {0}")]
    Sink(String),
}

/// How [`run_transfer`]'s pipeline terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferOutcome {
    Completed,
    Cancelled,
}

/// Summary of a completed (or cancelled) transfer.
#[derive(Debug, Clone, PartialEq)]
pub struct TransferReport {
    pub rows_transferred: u64,
    pub outcome: TransferOutcome,
    pub warnings: Vec<String>,
}

/// Per-table result classification for run-level outcomes
/// (`import::ImportOutcome`, `migration::MigrationRunOutcome`) that itemize
/// every planned table rather than discarding earlier progress the moment
/// one table fails (R4-002/B-007).
#[derive(Debug, Clone, PartialEq)]
pub enum TableTransferStatus {
    /// The table's row-load phase ran to completion (including zero rows).
    Completed { rows: u64 },
    /// The table's mapping mode was `Skip` — never written to, by design.
    Skipped,
    /// The table's DDL or row-load phase failed; `error` is the formatted
    /// error that stopped it. Every table after this one in load order is
    /// `NotStarted`.
    Failed { error: String },
    /// The table was never reached — an earlier table failed, or the run
    /// was cancelled before this table's turn.
    NotStarted,
}

impl TransferReport {
    pub fn new(outcome: TransferOutcome) -> Self {
        Self {
            rows_transferred: 0,
            outcome,
            warnings: Vec::new(),
        }
    }
}

/// How a [`RowSink`] should handle its target table before loading rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMappingMode {
    /// Create the target table from the source schema, then insert.
    Create,
    /// The target table already exists with a compatible shape; insert only.
    Existing,
    /// Drop and recreate the target table, then insert. Destructive — the
    /// caller (wizard/orchestration layer) must confirm before reaching here.
    Recreate,
    /// Leave an existing target table untouched and report it skipped.
    Skip,
    /// Empty the existing target table (`TRUNCATE`, gated on
    /// `DriverCapabilities::TRUNCATE_TABLE`), then insert. Destructive like
    /// `Recreate` — the caller must confirm before reaching here — but keeps
    /// the table's existing DDL (indexes, constraints) instead of dropping it.
    Truncate,
}

/// Pulls rows from a source (a table, a file, ...) in bounded-memory chunks.
pub trait RowSource: Send {
    /// The source's column shape, in the order rows are yielded.
    fn columns(&self) -> &[TransferColumn];

    /// Returns the next chunk, or `Ok(None)` once the source is exhausted.
    fn next_chunk(&mut self, cancel: &CancelToken) -> Result<Option<RowChunk>, TransferError>;

    /// Best-effort total row count, when cheaply knowable, for progress reporting.
    fn estimated_total(&self) -> Option<u64>;
}

/// Projects one source row into the shape a [`RowSink`] expects.
pub trait ColumnMap: Send {
    fn project(&self, src: &[Value]) -> Vec<Value>;

    /// The target column shape every `project`ed row is shaped into, in the
    /// exact order `project` emits values. This is what the sink must be
    /// `begin()`-ed with — NOT the source's column shape — so the sink's
    /// INSERT column list stays aligned with the projected values regardless
    /// of how the source and target column order/arity differ.
    fn target_columns(&self) -> &[TransferColumn];

    /// Non-blocking warnings accumulated while resolving the mapping (e.g. an
    /// unmatched source column). Folded into the final `TransferReport` once,
    /// not per row.
    fn warnings(&self) -> &[String] {
        &[]
    }
}

/// Writes rows to a target (a table, a file, ...).
pub trait RowSink: Send {
    fn begin(
        &mut self,
        columns: &[TransferColumn],
        mode: TableMappingMode,
    ) -> Result<(), TransferError>;

    /// Writes one chunk, returning the number of rows actually written.
    fn write_chunk(&mut self, chunk: &RowChunk) -> Result<u64, TransferError>;

    /// Finalizes the sink and returns its summary. Called exactly once,
    /// whether the pipeline completed or was cancelled.
    fn finish(&mut self) -> Result<TransferReport, TransferError>;
}

/// Runs one Source -> Map -> Sink pipeline to completion or cancellation.
///
/// Cancellation is checked before requesting each chunk: a chunk already
/// in flight is always written before the pipeline stops, and `sink.finish()`
/// is always called exactly once. `on_progress` is invoked with
/// `(rows_written_so_far, estimated_total)` after each written chunk.
pub fn run_transfer(
    source: &mut dyn RowSource,
    column_map: &dyn ColumnMap,
    sink: &mut dyn RowSink,
    mapping_mode: TableMappingMode,
    cancel: &CancelToken,
    on_progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<TransferReport, TransferError> {
    sink.begin(column_map.target_columns(), mapping_mode)?;

    let estimated_total = source.estimated_total();
    let mut rows_done: u64 = 0;

    loop {
        if cancel.is_cancelled() {
            let mut report = sink.finish()?;
            report.outcome = TransferOutcome::Cancelled;
            report
                .warnings
                .extend(column_map.warnings().iter().cloned());
            return Ok(report);
        }

        let Some(chunk) = source.next_chunk(cancel)? else {
            break;
        };

        let mapped_rows: Vec<Vec<Value>> =
            chunk.0.iter().map(|row| column_map.project(row)).collect();

        let written = sink.write_chunk(&RowChunk(mapped_rows))?;
        rows_done += written;
        on_progress(rows_done, estimated_total);
    }

    let mut report = sink.finish()?;
    report
        .warnings
        .extend(column_map.warnings().iter().cloned());
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    /// Identity map whose target shape is simply the source's own columns —
    /// used by tests that don't care about column mapping, only pipeline
    /// control flow (chunking, cancellation).
    struct IdentityMap {
        columns: Vec<TransferColumn>,
    }

    impl ColumnMap for IdentityMap {
        fn project(&self, src: &[Value]) -> Vec<Value> {
            src.to_vec()
        }

        fn target_columns(&self) -> &[TransferColumn] {
            &self.columns
        }
    }

    /// Reorders a two-column `[b, a]` source row into `[a, b]` target order —
    /// a minimal stand-in for `AutoColumnMap` when a source and target
    /// disagree on physical column order.
    struct ReorderingMap {
        target_columns: Vec<TransferColumn>,
    }

    impl ColumnMap for ReorderingMap {
        fn project(&self, src: &[Value]) -> Vec<Value> {
            vec![src[1].clone(), src[0].clone()]
        }

        fn target_columns(&self) -> &[TransferColumn] {
            &self.target_columns
        }
    }

    /// Fake source that yields fixed-size chunks from an in-memory `Vec<Vec<Value>>`.
    struct VecSource {
        columns: Vec<TransferColumn>,
        rows: Vec<Vec<Value>>,
        chunk_size: usize,
        cursor: usize,
        cancel_after_chunk: Option<usize>,
        chunks_yielded: usize,
    }

    impl VecSource {
        fn new(columns: Vec<TransferColumn>, rows: Vec<Vec<Value>>, chunk_size: usize) -> Self {
            Self {
                columns,
                rows,
                chunk_size,
                cursor: 0,
                cancel_after_chunk: None,
                chunks_yielded: 0,
            }
        }

        fn cancel_after_chunk(mut self, n: usize) -> Self {
            self.cancel_after_chunk = Some(n);
            self
        }
    }

    impl RowSource for VecSource {
        fn columns(&self) -> &[TransferColumn] {
            &self.columns
        }

        fn next_chunk(&mut self, cancel: &CancelToken) -> Result<Option<RowChunk>, TransferError> {
            if self.cursor >= self.rows.len() {
                return Ok(None);
            }

            let end = (self.cursor + self.chunk_size).min(self.rows.len());
            let chunk = self.rows[self.cursor..end].to_vec();
            self.cursor = end;
            self.chunks_yielded += 1;

            if let Some(after) = self.cancel_after_chunk
                && self.chunks_yielded == after
            {
                cancel.cancel();
            }

            Ok(Some(RowChunk(chunk)))
        }

        fn estimated_total(&self) -> Option<u64> {
            Some(self.rows.len() as u64)
        }
    }

    /// Fake sink recording every chunk it was asked to write, plus the
    /// column names it was `begin()`-ed with (so tests can prove those match
    /// the column map's target shape, not the source's).
    struct VecSink {
        received_chunks: Vec<Vec<Vec<Value>>>,
        began: bool,
        finished: bool,
        began_columns: Vec<String>,
    }

    impl VecSink {
        fn new() -> Self {
            Self {
                received_chunks: Vec::new(),
                began: false,
                finished: false,
                began_columns: Vec::new(),
            }
        }
    }

    impl RowSink for VecSink {
        fn begin(
            &mut self,
            columns: &[TransferColumn],
            _mode: TableMappingMode,
        ) -> Result<(), TransferError> {
            self.began = true;
            self.began_columns = columns.iter().map(|c| c.name.clone()).collect();
            Ok(())
        }

        fn write_chunk(&mut self, chunk: &RowChunk) -> Result<u64, TransferError> {
            let count = chunk.0.len() as u64;
            self.received_chunks.push(chunk.0.clone());
            Ok(count)
        }

        fn finish(&mut self) -> Result<TransferReport, TransferError> {
            self.finished = true;
            let total_rows: u64 = self.received_chunks.iter().map(|c| c.len() as u64).sum();
            let mut report = TransferReport::new(TransferOutcome::Completed);
            report.rows_transferred = total_rows;
            Ok(report)
        }
    }

    fn row(value: i64) -> Vec<Value> {
        vec![Value::Int(value)]
    }

    #[test]
    fn full_row_count_transferred_with_identity_map_and_chunk_boundaries_respected() {
        let columns = vec![column("id")];
        let rows: Vec<Vec<Value>> = (0..7).map(row).collect();
        let mut source = VecSource::new(columns.clone(), rows, 3);
        let identity_map = IdentityMap { columns };
        let mut sink = VecSink::new();
        let cancel = CancelToken::new();
        let mut progress_calls: Vec<(u64, Option<u64>)> = Vec::new();

        let report = run_transfer(
            &mut source,
            &identity_map,
            &mut sink,
            TableMappingMode::Create,
            &cancel,
            &mut |done, total| progress_calls.push((done, total)),
        )
        .unwrap();

        assert!(sink.began);
        assert!(sink.finished);
        assert_eq!(report.rows_transferred, 7);
        assert_eq!(report.outcome, TransferOutcome::Completed);

        // 7 rows at chunk_size=3 -> chunks of [3, 3, 1].
        assert_eq!(
            sink.received_chunks
                .iter()
                .map(|c| c.len())
                .collect::<Vec<_>>(),
            vec![3, 3, 1]
        );
        assert_eq!(
            progress_calls,
            vec![(3, Some(7)), (6, Some(7)), (7, Some(7))]
        );
    }

    #[test]
    fn cancel_mid_transfer_stops_before_next_chunk() {
        let columns = vec![column("id")];
        let rows: Vec<Vec<Value>> = (0..3).map(row).collect();
        // 3 rows, chunk_size=1 -> 3 chunks total; cancel fires once chunk 1 is produced.
        let mut source = VecSource::new(columns.clone(), rows, 1).cancel_after_chunk(1);
        let identity_map = IdentityMap { columns };
        let mut sink = VecSink::new();
        let cancel = CancelToken::new();

        let report = run_transfer(
            &mut source,
            &identity_map,
            &mut sink,
            TableMappingMode::Create,
            &cancel,
            &mut |_, _| {},
        )
        .unwrap();

        assert_eq!(report.outcome, TransferOutcome::Cancelled);
        assert_eq!(
            sink.received_chunks.len(),
            1,
            "only chunk 1 must be written"
        );
        assert!(sink.finished, "finish() must still be called on cancel");
    }

    #[test]
    fn empty_source_completes_with_zero_rows_and_still_begins_and_finishes() {
        let columns = vec![column("id")];
        let mut source = VecSource::new(columns.clone(), Vec::new(), 10);
        let identity_map = IdentityMap { columns };
        let mut sink = VecSink::new();
        let cancel = CancelToken::new();

        let report = run_transfer(
            &mut source,
            &identity_map,
            &mut sink,
            TableMappingMode::Create,
            &cancel,
            &mut |_, _| {},
        )
        .unwrap();

        assert!(sink.began);
        assert!(sink.finished);
        assert_eq!(report.rows_transferred, 0);
        assert_eq!(report.outcome, TransferOutcome::Completed);
    }

    /// JD-C1 regression: when the column map's target shape differs in
    /// order/arity from the source, `run_transfer` must `begin()` the sink
    /// with the TARGET column list (matching what `project()` emits), not the
    /// source's own column list — otherwise the sink's INSERT column list
    /// names the wrong columns for the projected values.
    #[test]
    fn sink_begin_receives_target_columns_not_source_columns_when_order_differs() {
        let source_columns = vec![column("b"), column("a")];
        let target_columns = vec![column("a"), column("b")];
        // Source row is [b_value, a_value] per source column order.
        let rows = vec![vec![Value::Int(20), Value::Int(10)]];
        let mut source = VecSource::new(source_columns, rows, 10);
        let column_map = ReorderingMap {
            target_columns: target_columns.clone(),
        };
        let mut sink = VecSink::new();
        let cancel = CancelToken::new();

        run_transfer(
            &mut source,
            &column_map,
            &mut sink,
            TableMappingMode::Existing,
            &cancel,
            &mut |_, _| {},
        )
        .unwrap();

        assert_eq!(
            sink.began_columns,
            vec!["a".to_string(), "b".to_string()],
            "the sink must be begin()-ed with the TARGET column order, not the source's"
        );
        assert_eq!(
            sink.received_chunks[0][0],
            vec![Value::Int(10), Value::Int(20)],
            "values must already be projected into target order"
        );
    }
}
