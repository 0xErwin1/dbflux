//! Table -> Table `RowSink`: the bulk-insert engine path used by Import and
//! Migration. Prefers a driver's native multi-row `INSERT` (gated by
//! `DriverCapabilities::BULK_INSERT`), falling back to per-row `insert_row`
//! when the capability or generator is unavailable.
//!
//! When the target advertises `DriverCapabilities::TRANSACTIONS`, the row
//! INSERT phase (and only that phase) runs inside one transaction per table:
//! `begin()` issues any DDL (`CREATE`/`DROP`/`TRUNCATE`) first, autocommitted
//! as its own statement, then opens the transaction; `finish()` commits it;
//! a failed `write_chunk` rolls it back. DDL must stay outside the
//! transaction because several dialects (MySQL among them) implicitly commit
//! on DDL — wrapping it would silently defeat the rollback. Targets without
//! the capability keep the pre-existing autocommit behavior untouched.

use std::sync::Arc;

use dbflux_core::{
    ColumnAssignment, Connection, CreateTableSpec, DriverCapabilities, DriverMetadata, LogErr,
    QueryGenerator, QueryRequest, RowInsert, TransactionVocab, TransferColumn, Value,
};

use crate::pipeline::TransferReport;
use crate::pipeline::{RowChunk, RowSink, TableMappingMode, TransferError, TransferOutcome};

/// Writes rows into `schema.table` on `connection`, handling the target
/// table according to the `TableMappingMode` passed to `begin()`.
pub struct TableSink {
    connection: Arc<dyn Connection>,
    schema: Option<String>,
    table: String,
    column_names: Vec<String>,
    /// Runs parallel to `column_names` — each target column's driver-reported
    /// type, threaded into typed literal formatting (e.g. PostgreSQL array
    /// columns) instead of being dropped on the floor.
    column_types: Vec<Option<String>>,
    skipped: bool,
    rows_written: u64,
    warnings: Vec<String>,
    /// `Some` for the lifetime of an open per-table transaction: set by
    /// `begin()` when the target supports `DriverCapabilities::TRANSACTIONS`,
    /// consumed by `finish()` (COMMIT) or by a failed `write_chunk` (ROLLBACK).
    transaction_vocab: Option<TransactionVocab>,
}

impl TableSink {
    pub fn new(
        connection: Arc<dyn Connection>,
        schema: Option<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            connection,
            schema,
            table: table.into(),
            column_names: Vec::new(),
            column_types: Vec::new(),
            skipped: false,
            rows_written: 0,
            warnings: Vec::new(),
            transaction_vocab: None,
        }
    }

    fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.table),
            None => self.table.clone(),
        }
    }

    fn create_table(&self, columns: &[TransferColumn]) -> Result<(), TransferError> {
        let Some(generator) = self.connection.query_generator() else {
            return Err(TransferError::Sink(format!(
                "driver does not support CREATE TABLE for '{}'",
                self.qualified_name()
            )));
        };

        let spec = CreateTableSpec {
            schema: self.schema.clone(),
            table: self.table.clone(),
            columns: columns.to_vec(),
            if_not_exists: false,
        };

        match generator.generate_create_table(&spec) {
            Ok(Some(query)) => self
                .connection
                .execute(&QueryRequest::new(query.text))
                .map(|_| ())
                .map_err(|e| TransferError::Sink(e.to_string())),
            Ok(None) => Err(TransferError::Sink(format!(
                "driver does not support CREATE TABLE for '{}'",
                self.qualified_name()
            ))),
            Err(e) => Err(TransferError::Sink(e.to_string())),
        }
    }

    fn drop_table_if_exists(&self) -> Result<(), TransferError> {
        let qualified = self
            .connection
            .dialect()
            .qualified_table(self.schema.as_deref(), &self.table);

        self.connection
            .execute(&QueryRequest::new(format!(
                "DROP TABLE IF EXISTS {qualified}"
            )))
            .map(|_| ())
            .map_err(|e| TransferError::Sink(e.to_string()))
    }

    /// Empties the target table before loading, gated on
    /// `DriverCapabilities::TRUNCATE_TABLE` — some dialects (SQLite) have no
    /// `TRUNCATE` statement at all, so the wizard must not offer this mode
    /// unless the target actually supports it (mirrors the `DISABLE_FK_CHECKS`
    /// missing-capability pattern: unavailable, not a runtime surprise).
    fn truncate_table(&self) -> Result<(), TransferError> {
        if !self.connection.supports(DriverCapabilities::TRUNCATE_TABLE) {
            return Err(TransferError::Sink(format!(
                "driver does not support TRUNCATE TABLE for '{}'",
                self.qualified_name()
            )));
        }

        let qualified = self
            .connection
            .dialect()
            .qualified_table(self.schema.as_deref(), &self.table);

        self.connection
            .execute(&QueryRequest::new(format!("TRUNCATE TABLE {qualified}")))
            .map(|_| ())
            .map_err(|e| TransferError::Sink(e.to_string()))
    }

    /// `DriverLimits::max_bulk_insert_rows` interpreted as "0 = unlimited" —
    /// treating it as a literal zero-row cap would silently bulk-insert
    /// nothing.
    fn max_bulk_insert_rows(metadata: &DriverMetadata) -> Option<usize> {
        let cap = metadata
            .limits
            .as_ref()
            .map(|limits| limits.max_bulk_insert_rows)
            .unwrap_or(0);

        (cap != 0).then_some(cap as usize)
    }

    #[allow(clippy::too_many_arguments)]
    fn write_rows_bulk(
        connection: &Arc<dyn Connection>,
        generator: &dyn QueryGenerator,
        schema: Option<&str>,
        table: &str,
        column_names: &[String],
        column_types: &[Option<String>],
        chunk: &RowChunk,
    ) -> Result<u64, TransferError> {
        let cap = Self::max_bulk_insert_rows(connection.metadata());
        let batch_size = cap.unwrap_or_else(|| chunk.0.len().max(1)).max(1);
        let mut written = 0u64;

        for batch in chunk.0.chunks(batch_size) {
            let row_refs: Vec<&[Value]> = batch.iter().map(Vec::as_slice).collect();

            match generator.generate_bulk_insert(
                schema,
                table,
                column_names,
                column_types,
                &row_refs,
            ) {
                Ok(Some(query)) => {
                    connection
                        .execute(&QueryRequest::new(query.text))
                        .map_err(|e| TransferError::Sink(e.to_string()))?;
                    written += batch.len() as u64;
                }
                Ok(None) => {
                    written += Self::write_rows_per_row(
                        connection,
                        schema,
                        table,
                        column_names,
                        column_types,
                        batch,
                    )?;
                }
                Err(e) => return Err(TransferError::Sink(e.to_string())),
            }
        }

        Ok(written)
    }

    fn write_rows_per_row(
        connection: &Arc<dyn Connection>,
        schema: Option<&str>,
        table: &str,
        column_names: &[String],
        column_types: &[Option<String>],
        rows: &[Vec<Value>],
    ) -> Result<u64, TransferError> {
        let mut written = 0u64;

        for row in rows {
            let assignments: Vec<ColumnAssignment> = column_names
                .iter()
                .zip(row.iter())
                .enumerate()
                .map(|(index, (name, value))| ColumnAssignment {
                    name: name.clone(),
                    value: value.clone(),
                    type_name: column_types.get(index).cloned().flatten(),
                })
                .collect();

            let insert = RowInsert::with_typed_assignments(
                table.to_string(),
                schema.map(str::to_string),
                assignments,
            );

            connection
                .insert_row(&insert)
                .map_err(|e| TransferError::Sink(e.to_string()))?;
            written += 1;
        }

        Ok(written)
    }

    /// Opens a transaction for the upcoming row-INSERT phase when the target
    /// supports it. Must run AFTER any DDL `begin()` already issued — DDL
    /// autocommits on several dialects, so opening the transaction first
    /// would either fail or silently commit the DDL mid-transaction.
    fn begin_transaction_if_supported(&mut self) -> Result<(), TransferError> {
        if !self.connection.supports(DriverCapabilities::TRANSACTIONS) {
            return Ok(());
        }

        let Some(vocab) = TransactionVocab::for_kind(self.connection.kind()) else {
            return Ok(());
        };

        self.connection
            .execute(&QueryRequest::new(vocab.begin))
            .map_err(|e| {
                TransferError::Sink(format!("BEGIN failed for '{}': {e}", self.qualified_name()))
            })?;

        self.transaction_vocab = Some(vocab);
        Ok(())
    }

    /// Rolls back the open per-table transaction, if any, after a
    /// `write_chunk` failure. A failed ROLLBACK is logged rather than
    /// propagated — it must never replace or hide the original insert error
    /// the caller is already returning.
    fn rollback_on_error(&mut self) {
        let Some(vocab) = self.transaction_vocab.take() else {
            return;
        };

        self.connection
            .execute(&QueryRequest::new(vocab.rollback))
            .log_err();
    }
}

impl RowSink for TableSink {
    fn begin(
        &mut self,
        columns: &[TransferColumn],
        mode: TableMappingMode,
    ) -> Result<(), TransferError> {
        self.column_names = columns.iter().map(|c| c.name.clone()).collect();
        self.column_types = columns.iter().map(|c| c.type_name.clone()).collect();

        match mode {
            TableMappingMode::Skip => {
                self.skipped = true;
                self.warnings.push(format!(
                    "table '{}' skipped (mapping mode Skip)",
                    self.qualified_name()
                ));
                return Ok(());
            }
            TableMappingMode::Existing => {}
            TableMappingMode::Create => {
                self.create_table(columns)?;
            }
            TableMappingMode::Recreate => {
                self.drop_table_if_exists()?;
                self.create_table(columns)?;
            }
            TableMappingMode::Truncate => {
                self.truncate_table()?;
            }
        }

        self.begin_transaction_if_supported()
    }

    fn write_chunk(&mut self, chunk: &RowChunk) -> Result<u64, TransferError> {
        if self.skipped {
            return Ok(0);
        }

        let connection = Arc::clone(&self.connection);

        let result = if connection.supports(DriverCapabilities::BULK_INSERT)
            && let Some(generator) = connection.query_generator()
        {
            Self::write_rows_bulk(
                &connection,
                generator,
                self.schema.as_deref(),
                &self.table,
                &self.column_names,
                &self.column_types,
                chunk,
            )
        } else {
            Self::write_rows_per_row(
                &connection,
                self.schema.as_deref(),
                &self.table,
                &self.column_names,
                &self.column_types,
                &chunk.0,
            )
        };

        match result {
            Ok(written) => {
                self.rows_written += written;
                Ok(written)
            }
            Err(e) => {
                self.rollback_on_error();
                Err(e)
            }
        }
    }

    fn finish(&mut self) -> Result<TransferReport, TransferError> {
        if let Some(vocab) = self.transaction_vocab.take() {
            self.connection
                .execute(&QueryRequest::new(vocab.commit))
                .map_err(|e| {
                    TransferError::Sink(format!(
                        "COMMIT failed for '{}': {e}",
                        self.qualified_name()
                    ))
                })?;
        }

        let mut report = TransferReport::new(TransferOutcome::Completed);
        report.rows_transferred = self.rows_written;
        report.warnings = std::mem::take(&mut self.warnings);
        Ok(report)
    }
}

impl Drop for TableSink {
    /// Closes a transaction left open when the sink is dropped without
    /// `finish()` or `rollback_on_error()` ever running — e.g. the
    /// pipeline's SOURCE (not the sink) fails mid-transfer and the `?`
    /// propagates before `write_chunk`/`finish` get a chance to close it.
    /// Both normal exit paths already `.take()` `transaction_vocab`, so this
    /// only fires on that leaked-transaction path. A failed rollback is
    /// logged rather than propagated — `Drop` must never panic.
    fn drop(&mut self) {
        if let Some(vocab) = self.transaction_vocab.take() {
            self.connection
                .execute(&QueryRequest::new(vocab.rollback))
                .log_err();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        DbError, DbKind, DefaultSqlDialect, DriverCapabilities, DriverLimits, GeneratedQuery,
        GeneratorError, MutationCategory, QueryLanguage, QueryResult, SchemaLoadingStrategy,
        SchemaSnapshot, SqlDialect,
    };
    use std::sync::Mutex;

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    fn column(name: &str, is_pk: bool) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: !is_pk,
            is_primary_key: is_pk,
        }
    }

    /// Query generator that always produces a bulk INSERT, recording the row
    /// counts it was asked to batch (one entry per `generate_bulk_insert`
    /// call) so tests can assert chunking behavior.
    struct RecordingGenerator {
        batch_sizes: Mutex<Vec<usize>>,
        bulk_insert_returns_none: bool,
        /// Every `column_types` slice this generator was asked to bulk-insert
        /// with, one entry per `generate_bulk_insert` call.
        recorded_column_types: Mutex<Vec<Vec<Option<String>>>>,
    }

    impl RecordingGenerator {
        fn new(bulk_insert_returns_none: bool) -> Self {
            Self {
                batch_sizes: Mutex::new(Vec::new()),
                bulk_insert_returns_none,
                recorded_column_types: Mutex::new(Vec::new()),
            }
        }
    }

    impl QueryGenerator for RecordingGenerator {
        fn supported_categories(&self) -> &'static [MutationCategory] {
            &[MutationCategory::Sql]
        }

        fn generate_mutation(
            &self,
            _mutation: &dbflux_core::MutationRequest,
        ) -> Option<GeneratedQuery> {
            None
        }

        fn generate_bulk_insert(
            &self,
            _schema: Option<&str>,
            _table: &str,
            _columns: &[String],
            column_types: &[Option<String>],
            rows: &[&[Value]],
        ) -> Result<Option<GeneratedQuery>, GeneratorError> {
            self.batch_sizes.lock().unwrap().push(rows.len());
            self.recorded_column_types
                .lock()
                .unwrap()
                .push(column_types.to_vec());

            if self.bulk_insert_returns_none {
                return Ok(None);
            }

            Ok(Some(GeneratedQuery {
                language: QueryLanguage::Sql,
                text: format!("INSERT INTO t VALUES (...) -- {} rows", rows.len()),
            }))
        }

        fn generate_create_table(
            &self,
            spec: &CreateTableSpec,
        ) -> Result<Option<GeneratedQuery>, GeneratorError> {
            Ok(Some(GeneratedQuery {
                language: QueryLanguage::Sql,
                text: format!("CREATE TABLE {} (...)", spec.table),
            }))
        }
    }

    struct FakeConnection {
        capabilities: DriverCapabilities,
        generator: Option<RecordingGenerator>,
        executed_sql: Mutex<Vec<String>>,
        inserted_rows: Mutex<Vec<Vec<Value>>>,
        /// Every `insert_row` call's assignment type names, in column order —
        /// proves the per-row fallback threads `type_name` through instead of
        /// discarding it.
        inserted_type_names: Mutex<Vec<Vec<Option<String>>>>,
        metadata: DriverMetadata,
        /// When `true`, `insert_row` fails every call instead of recording the
        /// row — a stand-in for a mid-load insert error, used to prove
        /// `TableSink` rolls back its open transaction instead of leaving
        /// partial rows committed.
        insert_row_should_fail: bool,
    }

    impl FakeConnection {
        fn new(
            capabilities: DriverCapabilities,
            limits: Option<DriverLimits>,
            generator: Option<RecordingGenerator>,
        ) -> Self {
            let mut builder = dbflux_core::DriverMetadataBuilder::new(
                "fake",
                "Fake",
                dbflux_core::DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(capabilities);

            if let Some(limits) = limits {
                builder = builder.limits(limits);
            }

            Self {
                capabilities,
                generator,
                executed_sql: Mutex::new(Vec::new()),
                inserted_rows: Mutex::new(Vec::new()),
                inserted_type_names: Mutex::new(Vec::new()),
                metadata: builder.build(),
                insert_row_should_fail: false,
            }
        }

        fn with_insert_row_failing(mut self) -> Self {
            self.insert_row_should_fail = true;
            self
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn capabilities(&self) -> DriverCapabilities {
            self.capabilities
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.executed_sql.lock().unwrap().push(req.sql.clone());
            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("stub".to_string()))
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &DIALECT
        }

        fn query_generator(&self) -> Option<&dyn QueryGenerator> {
            self.generator.as_ref().map(|g| g as &dyn QueryGenerator)
        }

        fn insert_row(&self, insert: &RowInsert) -> Result<dbflux_core::CrudResult, DbError> {
            if self.insert_row_should_fail {
                return Err(DbError::NotSupported(
                    "insert_row forced failure".to_string(),
                ));
            }

            let values: Vec<Value> = insert.assignments.iter().map(|a| a.value.clone()).collect();
            let type_names: Vec<Option<String>> = insert
                .assignments
                .iter()
                .map(|a| a.type_name.clone())
                .collect();
            self.inserted_rows.lock().unwrap().push(values);
            self.inserted_type_names.lock().unwrap().push(type_names);
            Ok(dbflux_core::CrudResult::new(1, None))
        }
    }

    fn rows(n: i64) -> Vec<Vec<Value>> {
        (0..n).map(|i| vec![Value::Int(i)]).collect()
    }

    #[test]
    fn bulk_insert_used_when_capability_and_generator_present() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let written = sink.write_chunk(&RowChunk(rows(3))).unwrap();
        assert_eq!(written, 3);

        let generator = connection.generator.as_ref().unwrap();
        assert_eq!(*generator.batch_sizes.lock().unwrap(), vec![3]);
        assert!(connection.inserted_rows.lock().unwrap().is_empty());
    }

    /// JD-C2 regression: `begin()`'s per-column `type_name` must reach the
    /// bulk-insert generator call, not be dropped on the floor — otherwise a
    /// typed literal (e.g. PostgreSQL `text[]`) falls back to an untyped one.
    #[test]
    fn begin_threads_column_type_names_into_the_bulk_insert_call() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        let mut tags_column = column("tags", false);
        tags_column.type_name = Some("text[]".to_string());
        sink.begin(
            &[column("id", true), tags_column],
            TableMappingMode::Existing,
        )
        .unwrap();
        sink.write_chunk(&RowChunk(vec![vec![
            Value::Int(1),
            Value::Array(vec![Value::Text("a".to_string())]),
        ]]))
        .unwrap();

        let generator = connection.generator.as_ref().unwrap();
        assert_eq!(
            generator.recorded_column_types.lock().unwrap()[0],
            vec![Some("text".to_string()), Some("text[]".to_string())],
            "column type names must reach generate_bulk_insert, not be dropped"
        );
    }

    /// JD-C2 regression (per-row fallback): when the bulk path is
    /// unavailable, `type_name` must still reach `insert_row` via
    /// `RowInsert::with_typed_assignments`, not the untyped `RowInsert::new`.
    #[test]
    fn falls_back_to_per_row_insert_threading_column_type_names_into_typed_assignments() {
        let connection = Arc::new(FakeConnection::new(DriverCapabilities::empty(), None, None));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        let mut tags_column = column("tags", false);
        tags_column.type_name = Some("text[]".to_string());
        sink.begin(
            &[column("id", true), tags_column],
            TableMappingMode::Existing,
        )
        .unwrap();
        sink.write_chunk(&RowChunk(vec![vec![
            Value::Int(1),
            Value::Array(vec![Value::Text("a".to_string())]),
        ]]))
        .unwrap();

        assert_eq!(
            connection.inserted_type_names.lock().unwrap()[0],
            vec![Some("text".to_string()), Some("text[]".to_string())],
            "column type names must reach insert_row via typed assignments, not be dropped"
        );
    }

    #[test]
    fn zero_max_bulk_insert_rows_means_unlimited_not_a_literal_cap() {
        let generator = RecordingGenerator::new(false);
        let limits = DriverLimits {
            max_bulk_insert_rows: 0,
            ..default_limits()
        };
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            Some(limits),
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let written = sink.write_chunk(&RowChunk(rows(500))).unwrap();
        assert_eq!(written, 500);

        // A cap of 0 must never chunk rows into per-row calls — one bulk
        // statement covering all 500 rows.
        let generator = connection.generator.as_ref().unwrap();
        assert_eq!(*generator.batch_sizes.lock().unwrap(), vec![500]);
    }

    #[test]
    fn nonzero_cap_chunks_bulk_insert_batches_to_the_cap() {
        let generator = RecordingGenerator::new(false);
        let limits = DriverLimits {
            max_bulk_insert_rows: 1000,
            ..default_limits()
        };
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            Some(limits),
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let written = sink.write_chunk(&RowChunk(rows(2500))).unwrap();
        assert_eq!(written, 2500);

        let generator = connection.generator.as_ref().unwrap();
        assert_eq!(
            *generator.batch_sizes.lock().unwrap(),
            vec![1000, 1000, 500]
        );
    }

    #[test]
    fn falls_back_to_per_row_insert_when_capability_bit_absent() {
        let connection = Arc::new(FakeConnection::new(DriverCapabilities::empty(), None, None));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let written = sink.write_chunk(&RowChunk(rows(3))).unwrap();
        assert_eq!(written, 3);
        assert_eq!(connection.inserted_rows.lock().unwrap().len(), 3);
    }

    #[test]
    fn falls_back_to_per_row_insert_when_generator_returns_none() {
        let generator = RecordingGenerator::new(true);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let written = sink.write_chunk(&RowChunk(rows(2))).unwrap();
        assert_eq!(written, 2);
        assert_eq!(connection.inserted_rows.lock().unwrap().len(), 2);
    }

    #[test]
    fn create_mode_issues_create_table_before_inserts() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, Some("public".to_string()), "t");

        sink.begin(&[column("id", true)], TableMappingMode::Create)
            .unwrap();

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 1);
        assert!(executed[0].starts_with("CREATE TABLE"));
    }

    #[test]
    fn create_mode_without_generator_support_errors() {
        let connection = Arc::new(FakeConnection::new(DriverCapabilities::empty(), None, None));
        let conn: Arc<dyn Connection> = connection;
        let mut sink = TableSink::new(conn, None, "t");

        let result = sink.begin(&[column("id", true)], TableMappingMode::Create);
        assert!(result.is_err());
    }

    #[test]
    fn recreate_mode_drops_then_creates() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Recreate)
            .unwrap();

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 2);
        assert!(executed[0].starts_with("DROP TABLE IF EXISTS"));
        assert!(executed[1].starts_with("CREATE TABLE"));
    }

    #[test]
    fn existing_mode_issues_no_ddl() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        assert!(connection.executed_sql.lock().unwrap().is_empty());
    }

    #[test]
    fn truncate_mode_empties_the_table_before_insert_when_capability_present() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT | DriverCapabilities::TRUNCATE_TABLE,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, Some("public".to_string()), "t");

        sink.begin(&[column("id", true)], TableMappingMode::Truncate)
            .unwrap();

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 1);
        assert!(executed[0].starts_with("TRUNCATE TABLE"));
    }

    #[test]
    fn truncate_mode_errors_when_capability_bit_absent() {
        let connection = Arc::new(FakeConnection::new(DriverCapabilities::empty(), None, None));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        let result = sink.begin(&[column("id", true)], TableMappingMode::Truncate);
        assert!(result.is_err());
        assert!(connection.executed_sql.lock().unwrap().is_empty());
    }

    #[test]
    fn skip_mode_no_ops_writes_and_reports_a_warning() {
        let connection = Arc::new(FakeConnection::new(DriverCapabilities::empty(), None, None));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Skip)
            .unwrap();
        let written = sink.write_chunk(&RowChunk(rows(5))).unwrap();
        assert_eq!(written, 0);
        assert!(connection.executed_sql.lock().unwrap().is_empty());
        assert!(connection.inserted_rows.lock().unwrap().is_empty());

        let report = sink.finish().unwrap();
        assert_eq!(report.rows_transferred, 0);
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("skipped"));
    }

    /// R4-002/B-007 regression: DDL must autocommit BEFORE the transaction
    /// opens — wrapping DDL in the same transaction would silently break
    /// atomicity on dialects (MySQL) that implicitly commit on DDL.
    #[test]
    fn create_mode_issues_ddl_before_opening_the_transaction_when_target_supports_transactions() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT | DriverCapabilities::TRANSACTIONS,
            None,
            Some(generator),
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Create)
            .unwrap();

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 2);
        assert!(
            executed[0].starts_with("CREATE TABLE"),
            "DDL must run first: {:?}",
            *executed
        );
        assert!(
            executed[1].starts_with("BEGIN"),
            "BEGIN must follow DDL, not precede it: {:?}",
            *executed
        );
    }

    /// R4-002/B-007 regression: a successful table load commits the open
    /// per-table transaction exactly once, in `finish()`.
    #[test]
    fn successful_table_load_commits_the_open_transaction_when_target_supports_transactions() {
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::TRANSACTIONS,
            None,
            None,
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        sink.write_chunk(&RowChunk(rows(2))).unwrap();
        let report = sink.finish().unwrap();

        assert_eq!(report.rows_transferred, 2);
        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 2);
        assert!(executed[0].starts_with("BEGIN"));
        assert!(executed[1].starts_with("COMMIT"));
    }

    /// R4-002/B-007 regression: a failed `write_chunk` rolls back the open
    /// transaction instead of leaving whatever rows were inserted so far
    /// committed to the table.
    #[test]
    fn failed_write_chunk_rolls_back_the_open_transaction_when_target_supports_transactions() {
        let connection = Arc::new(
            FakeConnection::new(DriverCapabilities::TRANSACTIONS, None, None)
                .with_insert_row_failing(),
        );
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let result = sink.write_chunk(&RowChunk(rows(2)));

        assert!(result.is_err(), "the forced insert failure must propagate");
        assert!(
            connection.inserted_rows.lock().unwrap().is_empty(),
            "no row must be recorded as inserted once the failure rolls back"
        );
        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(
            executed.len(),
            2,
            "BEGIN then ROLLBACK, no COMMIT: {:?}",
            *executed
        );
        assert!(executed[0].starts_with("BEGIN"));
        assert!(executed[1].starts_with("ROLLBACK"));
    }

    /// R4-002/B-007 regression (Recreate/destructive): the DROP/CREATE DDL
    /// autocommits and is NOT rolled back, but a load failure still rolls
    /// back the (re)created table's inserted rows, leaving it empty rather
    /// than half-populated.
    #[test]
    fn recreate_mode_failure_mid_load_rolls_back_rows_but_leaves_ddl_in_place() {
        let generator = RecordingGenerator::new(false);
        let connection = Arc::new(
            FakeConnection::new(DriverCapabilities::TRANSACTIONS, None, Some(generator))
                .with_insert_row_failing(),
        );
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Recreate)
            .unwrap();
        let result = sink.write_chunk(&RowChunk(rows(2)));

        assert!(result.is_err());
        assert!(connection.inserted_rows.lock().unwrap().is_empty());

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 4, "{:?}", *executed);
        assert!(executed[0].starts_with("DROP TABLE IF EXISTS"));
        assert!(executed[1].starts_with("CREATE TABLE"));
        assert!(executed[2].starts_with("BEGIN"));
        assert!(
            executed[3].starts_with("ROLLBACK"),
            "the data ROLLBACK must not touch the already-autocommitted DDL: {:?}",
            *executed
        );
    }

    /// JDB-001 regression: dropping a `TableSink` whose transaction was
    /// opened but never closed via `finish()`/`rollback_on_error()` (the
    /// leaked-transaction case: the pipeline stopped before either ran)
    /// must still roll back the open transaction instead of leaving it open
    /// on the shared, long-lived connection.
    #[test]
    fn dropping_the_sink_with_an_open_transaction_rolls_it_back() {
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::TRANSACTIONS,
            None,
            None,
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        drop(sink);

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(executed.len(), 2, "BEGIN then ROLLBACK: {:?}", *executed);
        assert!(executed[0].starts_with("BEGIN"));
        assert!(
            executed[1].starts_with("ROLLBACK"),
            "dropping the sink must roll back a still-open transaction: {:?}",
            *executed
        );
    }

    /// JDB-001 regression guard: `Drop` must not double-close a transaction
    /// that `finish()` already committed.
    #[test]
    fn dropping_the_sink_after_a_successful_commit_does_not_rollback_again() {
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::TRANSACTIONS,
            None,
            None,
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        sink.write_chunk(&RowChunk(rows(2))).unwrap();
        sink.finish().unwrap();
        drop(sink);

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(
            executed.len(),
            2,
            "BEGIN then COMMIT only; Drop must not rollback after a successful commit: {:?}",
            *executed
        );
        assert!(executed[0].starts_with("BEGIN"));
        assert!(executed[1].starts_with("COMMIT"));
    }

    /// JDB-001 regression guard: `Drop` must not double-close a transaction
    /// that a failed `write_chunk` already rolled back.
    #[test]
    fn dropping_the_sink_after_a_failed_write_chunk_does_not_rollback_again() {
        let connection = Arc::new(
            FakeConnection::new(DriverCapabilities::TRANSACTIONS, None, None)
                .with_insert_row_failing(),
        );
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        let result = sink.write_chunk(&RowChunk(rows(2)));
        assert!(result.is_err(), "the forced insert failure must propagate");
        drop(sink);

        let executed = connection.executed_sql.lock().unwrap();
        assert_eq!(
            executed.len(),
            2,
            "BEGIN then ROLLBACK only; Drop must not rollback again after write_chunk already did: {:?}",
            *executed
        );
        assert!(executed[0].starts_with("BEGIN"));
        assert!(executed[1].starts_with("ROLLBACK"));
    }

    /// JDB-001 end-to-end regression: when the SOURCE errors mid-transfer
    /// (not the sink), `run_transfer`'s `?` drops the `TableSink` before
    /// `write_chunk`'s failure path or `finish()` ever run. The sink's
    /// `Drop` must still close the transaction opened in `begin()`, so no
    /// partial load is left dangling open on the shared connection.
    #[test]
    fn source_error_mid_transfer_still_rolls_back_the_sinks_open_transaction() {
        use crate::pipeline::{ColumnMap, RowSource, run_transfer};
        use dbflux_core::CancelToken;

        struct FailingSecondChunkSource {
            columns: Vec<TransferColumn>,
            calls: usize,
        }

        impl RowSource for FailingSecondChunkSource {
            fn columns(&self) -> &[TransferColumn] {
                &self.columns
            }

            fn next_chunk(
                &mut self,
                _cancel: &CancelToken,
            ) -> Result<Option<RowChunk>, TransferError> {
                self.calls += 1;
                if self.calls == 1 {
                    Ok(Some(RowChunk(rows(2))))
                } else {
                    Err(TransferError::Source(
                        "simulated source read failure".to_string(),
                    ))
                }
            }

            fn estimated_total(&self) -> Option<u64> {
                None
            }
        }

        struct IdentityColumnMap {
            columns: Vec<TransferColumn>,
        }

        impl ColumnMap for IdentityColumnMap {
            fn project(&self, src: &[Value]) -> Vec<Value> {
                src.to_vec()
            }

            fn target_columns(&self) -> &[TransferColumn] {
                &self.columns
            }
        }

        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::TRANSACTIONS,
            None,
            None,
        ));
        let conn: Arc<dyn Connection> = connection.clone();
        let columns = vec![column("id", true)];
        let mut source = FailingSecondChunkSource {
            columns: columns.clone(),
            calls: 0,
        };
        let column_map = IdentityColumnMap {
            columns: columns.clone(),
        };
        let mut sink = TableSink::new(conn, None, "t");
        let cancel = CancelToken::new();

        let result = run_transfer(
            &mut source,
            &column_map,
            &mut sink,
            TableMappingMode::Existing,
            &cancel,
            &mut |_, _| {},
        );
        assert!(
            result.is_err(),
            "the simulated source read failure must propagate"
        );
        drop(sink);

        let executed = connection.executed_sql.lock().unwrap();
        assert!(
            executed[0].starts_with("BEGIN"),
            "begin() must still open the transaction: {:?}",
            *executed
        );
        assert!(
            executed.iter().any(|s| s.starts_with("ROLLBACK")),
            "the sink's Drop must roll back the transaction the source-error path left open: {:?}",
            *executed
        );
        assert!(
            !executed.iter().any(|s| s.starts_with("COMMIT")),
            "chunk 1's insert must never be committed once the source fails on chunk 2: {:?}",
            *executed
        );
    }

    /// R4-002/B-007 regression: without the `TRANSACTIONS` capability, no
    /// BEGIN/COMMIT/ROLLBACK is ever attempted — the pre-existing autocommit
    /// behavior is preserved unchanged.
    #[test]
    fn falls_back_to_autocommit_without_attempting_any_transaction_when_capability_absent() {
        let connection = Arc::new(FakeConnection::new(DriverCapabilities::empty(), None, None));
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        sink.begin(&[column("id", true)], TableMappingMode::Existing)
            .unwrap();
        sink.write_chunk(&RowChunk(rows(2))).unwrap();
        let report = sink.finish().unwrap();

        assert_eq!(report.rows_transferred, 2);
        assert!(
            connection.executed_sql.lock().unwrap().is_empty(),
            "no BEGIN/COMMIT/ROLLBACK must be attempted without the TRANSACTIONS capability"
        );
    }

    /// Connection wired with the REAL SQL-backed generator (not a fake),
    /// so `begin(..., Create)` exercises the actual `build_create_table`
    /// DDL-safety validation (B-005/SEC-W1) end to end.
    struct RealGeneratorConnection {
        executed_sql: Mutex<Vec<String>>,
        metadata: DriverMetadata,
        generator: dbflux_core::SqlMutationGenerator,
    }

    impl RealGeneratorConnection {
        fn new() -> Self {
            let metadata = dbflux_core::DriverMetadataBuilder::new(
                "fake",
                "Fake",
                dbflux_core::DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(DriverCapabilities::empty())
            .build();

            Self {
                executed_sql: Mutex::new(Vec::new()),
                metadata,
                generator: dbflux_core::SqlMutationGenerator::new(&DIALECT),
            }
        }
    }

    impl Connection for RealGeneratorConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.executed_sql.lock().unwrap().push(req.sql.clone());
            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("stub".to_string()))
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &DIALECT
        }

        fn query_generator(&self) -> Option<&dyn QueryGenerator> {
            Some(&self.generator)
        }
    }

    /// B-005/SEC-W1 regression: `Create` mode must not execute DDL built
    /// from a crafted `type_name` — `begin()` must error out before
    /// `execute()` is ever called.
    #[test]
    fn create_mode_rejects_a_ddl_injection_type_name_without_touching_the_connection() {
        let connection = Arc::new(RealGeneratorConnection::new());
        let conn: Arc<dyn Connection> = connection.clone();
        let mut sink = TableSink::new(conn, None, "t");

        let malicious_column = TransferColumn {
            name: "id".to_string(),
            type_name: Some("TEXT); DROP TABLE users; --".to_string()),
            nullable: true,
            is_primary_key: false,
        };

        let result = sink.begin(&[malicious_column], TableMappingMode::Create);

        assert!(
            result.is_err(),
            "a crafted type_name must reject Create mode, not build DDL from it"
        );
        assert!(
            connection.executed_sql.lock().unwrap().is_empty(),
            "no DDL must ever reach execute() for a rejected type_name"
        );
    }

    fn default_limits() -> DriverLimits {
        DriverLimits {
            max_query_length: 0,
            max_parameters: 0,
            max_result_rows: 0,
            max_connections: 0,
            max_nested_subqueries: 16,
            max_identifier_length: 63,
            max_columns: 0,
            max_indexes_per_table: 0,
            max_bulk_insert_rows: 0,
        }
    }
}
