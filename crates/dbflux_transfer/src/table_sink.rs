//! Table -> Table `RowSink`: the bulk-insert engine path used by Import and
//! Migration. Prefers a driver's native multi-row `INSERT` (gated by
//! `DriverCapabilities::BULK_INSERT`), falling back to per-row `insert_row`
//! when the capability or generator is unavailable.

use std::sync::Arc;

use dbflux_core::{
    Connection, CreateTableSpec, DriverCapabilities, DriverMetadata, QueryGenerator, QueryRequest,
    RowInsert, TransferColumn, Value,
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
    skipped: bool,
    rows_written: u64,
    warnings: Vec<String>,
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
            skipped: false,
            rows_written: 0,
            warnings: Vec::new(),
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

    fn write_rows_bulk(
        connection: &Arc<dyn Connection>,
        generator: &dyn QueryGenerator,
        schema: Option<&str>,
        table: &str,
        column_names: &[String],
        chunk: &RowChunk,
    ) -> Result<u64, TransferError> {
        let cap = Self::max_bulk_insert_rows(connection.metadata());
        let batch_size = cap.unwrap_or_else(|| chunk.0.len().max(1)).max(1);
        let mut written = 0u64;

        for batch in chunk.0.chunks(batch_size) {
            let row_refs: Vec<&[Value]> = batch.iter().map(Vec::as_slice).collect();

            match generator.generate_bulk_insert(schema, table, column_names, &row_refs) {
                Ok(Some(query)) => {
                    connection
                        .execute(&QueryRequest::new(query.text))
                        .map_err(|e| TransferError::Sink(e.to_string()))?;
                    written += batch.len() as u64;
                }
                Ok(None) => {
                    written +=
                        Self::write_rows_per_row(connection, schema, table, column_names, batch)?;
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
        rows: &[Vec<Value>],
    ) -> Result<u64, TransferError> {
        let mut written = 0u64;

        for row in rows {
            let insert = RowInsert::new(
                table.to_string(),
                schema.map(str::to_string),
                column_names.to_vec(),
                row.clone(),
            );

            connection
                .insert_row(&insert)
                .map_err(|e| TransferError::Sink(e.to_string()))?;
            written += 1;
        }

        Ok(written)
    }
}

impl RowSink for TableSink {
    fn begin(
        &mut self,
        columns: &[TransferColumn],
        mode: TableMappingMode,
    ) -> Result<(), TransferError> {
        self.column_names = columns.iter().map(|c| c.name.clone()).collect();

        match mode {
            TableMappingMode::Skip => {
                self.skipped = true;
                self.warnings.push(format!(
                    "table '{}' skipped (mapping mode Skip)",
                    self.qualified_name()
                ));
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

        Ok(())
    }

    fn write_chunk(&mut self, chunk: &RowChunk) -> Result<u64, TransferError> {
        if self.skipped {
            return Ok(0);
        }

        let connection = Arc::clone(&self.connection);

        let written = if connection.supports(DriverCapabilities::BULK_INSERT)
            && let Some(generator) = connection.query_generator()
        {
            Self::write_rows_bulk(
                &connection,
                generator,
                self.schema.as_deref(),
                &self.table,
                &self.column_names,
                chunk,
            )?
        } else {
            Self::write_rows_per_row(
                &connection,
                self.schema.as_deref(),
                &self.table,
                &self.column_names,
                &chunk.0,
            )?
        };

        self.rows_written += written;
        Ok(written)
    }

    fn finish(&mut self) -> Result<TransferReport, TransferError> {
        let mut report = TransferReport::new(TransferOutcome::Completed);
        report.rows_transferred = self.rows_written;
        report.warnings = std::mem::take(&mut self.warnings);
        Ok(report)
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
            rows: &[&[Value]],
        ) -> Result<Option<GeneratedQuery>, GeneratorError> {
            self.batch_sizes.lock().unwrap().push(rows.len());

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
        metadata: DriverMetadata,
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
                metadata: builder.build(),
            }
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
            let values: Vec<Value> = insert.assignments.iter().map(|a| a.value.clone()).collect();
            self.inserted_rows.lock().unwrap().push(values);
            Ok(dbflux_core::CrudResult::new(1, None))
        }
    }

    fn rows(n: i64) -> Vec<Vec<Value>> {
        (0..n).map(|i| vec![Value::Int(i)]).collect()
    }

    #[test]
    fn bulk_insert_used_when_capability_and_generator_present() {
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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

    #[test]
    fn zero_max_bulk_insert_rows_means_unlimited_not_a_literal_cap() {
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: true,
        };
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
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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
        let generator = RecordingGenerator {
            batch_sizes: Mutex::new(Vec::new()),
            bulk_insert_returns_none: false,
        };
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
