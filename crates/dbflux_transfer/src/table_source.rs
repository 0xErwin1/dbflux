//! Table -> Row `RowSource`: reads one table in bounded-memory chunks via
//! `Connection::execute`, driving pagination itself since the trait is a
//! synchronous pull API (`limit`/`offset` on `QueryRequest`), not a cursor.
//!
//! Query text is built with literal values (via `SqlDialect::value_to_literal_typed`),
//! not bound parameters — the same strategy `SqlQueryBuilder::build_bulk_insert`
//! uses, which sidesteps per-dialect placeholder-style differences entirely.

use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, QueryRequest, TransferColumn, Value};

use crate::pipeline::{RowChunk, RowSource, TransferError};

/// Reads rows from `schema.table` in chunks of at most `segment_size`.
///
/// Uses keyset pagination (`WHERE pk > $last ORDER BY pk`) when the table has
/// exactly one primary-key column — stable and bounded regardless of
/// intervening writes. Falls back to `LIMIT`/`OFFSET` otherwise, ordering by
/// every selected column so pagination stays deterministic.
pub struct TableSource {
    connection: Arc<dyn Connection>,
    schema: Option<String>,
    table: String,
    columns: Vec<TransferColumn>,
    segment_size: u32,
    estimated_total: Option<u64>,
    keyset_pk: Option<String>,
    last_key_value: Option<Value>,
    offset: u32,
    exhausted: bool,
}

impl TableSource {
    /// `estimated_total` is a caller-supplied hint (skips the `COUNT(*)`
    /// query below when already cheaply known). When `None`, a `SELECT
    /// COUNT(*)` is issued once at construction so progress reporting has a
    /// real denominator instead of running indeterminate; a failed count
    /// query (e.g. insufficient privileges) falls back to `None` rather than
    /// failing the whole transfer — an unknown total is not a fatal error.
    pub fn new(
        connection: Arc<dyn Connection>,
        schema: Option<String>,
        table: impl Into<String>,
        columns: Vec<TransferColumn>,
        segment_size: u32,
        estimated_total: Option<u64>,
    ) -> Self {
        let table = table.into();
        let keyset_pk = single_primary_key_column(&columns);
        let estimated_total =
            estimated_total.or_else(|| count_rows(&connection, schema.as_deref(), &table));

        Self {
            connection,
            schema,
            table,
            columns,
            segment_size: segment_size.max(1),
            estimated_total,
            keyset_pk,
            last_key_value: None,
            offset: 0,
            exhausted: false,
        }
    }

    fn build_sql(&self) -> String {
        let dialect = self.connection.dialect();
        let table_ref = dialect.qualified_table(self.schema.as_deref(), &self.table);
        let column_list = self
            .columns
            .iter()
            .map(|c| dialect.quote_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        match &self.keyset_pk {
            Some(pk_col) => {
                let quoted_pk = dialect.quote_identifier(pk_col);

                let where_clause = match &self.last_key_value {
                    Some(value) => {
                        let type_name = self
                            .columns
                            .iter()
                            .find(|c| &c.name == pk_col)
                            .and_then(|c| c.type_name.as_deref());
                        let literal = dialect.value_to_literal_typed(value, type_name);
                        format!("WHERE {quoted_pk} > {literal} ")
                    }
                    None => String::new(),
                };

                format!(
                    "SELECT {column_list} FROM {table_ref} {where_clause}ORDER BY {quoted_pk} {}",
                    dialect.limit_clause(self.segment_size)
                )
            }
            None => {
                let order_by = self
                    .columns
                    .iter()
                    .map(|c| dialect.quote_identifier(&c.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                format!(
                    "SELECT {column_list} FROM {table_ref} ORDER BY {order_by} {}",
                    dialect.limit_offset_clause(self.segment_size, self.offset as u64)
                )
            }
        }
    }
}

/// Best-effort `SELECT COUNT(*)` against `schema.table`, gracefully
/// swallowing any failure (missing privileges, driver quirks, an
/// intentionally unreachable table in tests) into `None`.
fn count_rows(connection: &Arc<dyn Connection>, schema: Option<&str>, table: &str) -> Option<u64> {
    let dialect = connection.dialect();
    let qualified = dialect.qualified_table(schema, table);
    let sql = format!("SELECT COUNT(*) FROM {qualified}");

    let result = connection.execute(&QueryRequest::new(sql)).ok()?;
    let value = result.rows.first()?.first()?;

    match value {
        Value::Int(count) => u64::try_from(*count).ok(),
        Value::Decimal(text) => text.parse().ok(),
        Value::Float(count) if *count >= 0.0 => Some(*count as u64),
        _ => None,
    }
}

fn single_primary_key_column(columns: &[TransferColumn]) -> Option<String> {
    let mut pk_columns = columns.iter().filter(|c| c.is_primary_key);
    let first = pk_columns.next()?;

    if pk_columns.next().is_some() {
        return None;
    }

    Some(first.name.clone())
}

impl RowSource for TableSource {
    fn columns(&self) -> &[TransferColumn] {
        &self.columns
    }

    fn next_chunk(&mut self, cancel: &CancelToken) -> Result<Option<RowChunk>, TransferError> {
        if self.exhausted || cancel.is_cancelled() {
            return Ok(None);
        }

        let sql = self.build_sql();
        let result = self
            .connection
            .execute(&QueryRequest::new(sql))
            .map_err(|e| TransferError::Source(e.to_string()))?;

        if result.rows.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        let row_count = result.rows.len();
        if (row_count as u32) < self.segment_size {
            self.exhausted = true;
        }

        match &self.keyset_pk {
            Some(pk_col) => {
                let pk_index = self.columns.iter().position(|c| &c.name == pk_col);
                if let Some(pk_index) = pk_index
                    && let Some(last_row) = result.rows.last()
                {
                    self.last_key_value = last_row.get(pk_index).cloned();
                }
            }
            None => {
                self.offset += row_count as u32;
            }
        }

        Ok(Some(RowChunk(result.rows)))
    }

    fn estimated_total(&self) -> Option<u64> {
        self.estimated_total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        DbError, DbKind, DefaultSqlDialect, DriverMetadata, QueryResult, SchemaLoadingStrategy,
        SchemaSnapshot, SqlDialect,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    /// Fake connection that captures every SQL string it was asked to execute
    /// and returns canned responses in call order.
    struct FakeConnection {
        captured_sql: Mutex<Vec<String>>,
        responses: Mutex<VecDeque<Vec<Vec<Value>>>>,
        always_fail: bool,
    }

    impl FakeConnection {
        fn new(responses: Vec<Vec<Vec<Value>>>) -> Self {
            Self {
                captured_sql: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into()),
                always_fail: false,
            }
        }

        fn always_failing() -> Self {
            Self {
                captured_sql: Mutex::new(Vec::new()),
                responses: Mutex::new(VecDeque::new()),
                always_fail: true,
            }
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            unimplemented!("FakeConnection::metadata not needed for TableSource tests")
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.captured_sql.lock().unwrap().push(req.sql.clone());

            if self.always_fail {
                return Err(DbError::NotSupported("count query failed".to_string()));
            }

            let rows = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default();
            Ok(QueryResult::table(
                Vec::new(),
                rows,
                None,
                std::time::Duration::ZERO,
            ))
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
    }

    fn pk_column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("integer".to_string()),
            nullable: false,
            is_primary_key: true,
        }
    }

    fn column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn keyset_pagination_advances_last_value_and_never_repeats_boundary_row() {
        let responses = vec![
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            vec![vec![Value::Int(3)], vec![Value::Int(4)]],
            vec![vec![Value::Int(5)]],
        ];
        let fake = Arc::new(FakeConnection::new(responses));
        let connection: Arc<dyn Connection> = fake.clone();
        let columns = vec![pk_column("id")];
        let mut source = TableSource::new(
            connection,
            Some("public".to_string()),
            "users",
            columns,
            2,
            Some(5),
        );
        let cancel = CancelToken::new();

        let chunk1 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(chunk1.0, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

        let chunk2 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(chunk2.0, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);

        let chunk3 = source.next_chunk(&cancel).unwrap().unwrap();
        assert_eq!(chunk3.0, vec![vec![Value::Int(5)]]);

        // Partial (< segment_size) page marks exhausted; a 4th call must not
        // even hit the connection.
        assert!(source.next_chunk(&cancel).unwrap().is_none());

        let captured = fake.captured_sql.lock().unwrap();
        assert_eq!(
            captured.len(),
            3,
            "must not query a 4th time: {:?}",
            captured
        );
        assert!(
            !captured[0].contains("WHERE"),
            "first page must have no WHERE clause: {}",
            captured[0]
        );
        assert!(
            captured[1].contains("WHERE \"id\" > 2 "),
            "second page must page past the last row of page 1: {}",
            captured[1]
        );
        assert!(
            captured[2].contains("WHERE \"id\" > 4 "),
            "third page must page past the last row of page 2: {}",
            captured[2]
        );
        for sql in captured.iter() {
            assert!(sql.contains("ORDER BY \"id\""));
            assert!(sql.contains("LIMIT 2"));
        }
    }

    #[test]
    fn keyset_pagination_terminates_on_a_trailing_empty_page() {
        let responses = vec![
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            vec![vec![Value::Int(3)], vec![Value::Int(4)]],
            vec![],
        ];
        let fake = Arc::new(FakeConnection::new(responses));
        let connection: Arc<dyn Connection> = fake;
        let columns = vec![pk_column("id")];
        // Some(4): this test's concern is pagination termination, not the
        // auto-count query — an explicit estimate skips it.
        let mut source = TableSource::new(connection, None, "widgets", columns, 2, Some(4));
        let cancel = CancelToken::new();

        assert!(source.next_chunk(&cancel).unwrap().is_some());
        assert!(source.next_chunk(&cancel).unwrap().is_some());
        assert!(source.next_chunk(&cancel).unwrap().is_none());
    }

    #[test]
    fn no_single_column_pk_falls_back_to_limit_offset_and_advances_offset() {
        let responses = vec![
            vec![
                vec![Value::Text("a".to_string())],
                vec![Value::Text("b".to_string())],
            ],
            vec![vec![Value::Text("c".to_string())]],
        ];
        let fake = Arc::new(FakeConnection::new(responses));
        let connection: Arc<dyn Connection> = fake.clone();
        let columns = vec![column("name")];
        // Some(3): bypasses the auto-count query, isolating this test to the
        // offset-fallback pagination it actually exercises.
        let mut source = TableSource::new(connection, None, "logs", columns, 2, Some(3));
        let cancel = CancelToken::new();

        source.next_chunk(&cancel).unwrap();
        source.next_chunk(&cancel).unwrap();

        let captured = fake.captured_sql.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert!(
            captured[0].contains("LIMIT 2") && !captured[0].to_uppercase().contains("OFFSET"),
            "first page has no OFFSET: {}",
            captured[0]
        );
        assert!(
            captured[1].contains("LIMIT 2 OFFSET 2"),
            "second page offsets past the first page: {}",
            captured[1]
        );
        assert!(captured[0].contains("ORDER BY \"name\""));
    }

    #[test]
    fn composite_primary_key_falls_back_to_limit_offset() {
        let fake = Arc::new(FakeConnection::new(vec![vec![]]));
        let connection: Arc<dyn Connection> = fake.clone();
        let mut pk_a = pk_column("a");
        pk_a.is_primary_key = true;
        let mut pk_b = pk_column("b");
        pk_b.is_primary_key = true;
        let columns = vec![pk_a, pk_b];
        // Some(0): bypasses the auto-count query so `captured[0]` below is
        // the actual paginated SELECT, not the count.
        let mut source = TableSource::new(connection, None, "composite", columns, 10, Some(0));
        let cancel = CancelToken::new();

        source.next_chunk(&cancel).unwrap();

        let captured = fake.captured_sql.lock().unwrap();
        assert!(
            captured[0].contains("LIMIT 10") && !captured[0].contains("WHERE"),
            "composite PK must use the offset fallback, not keyset: {}",
            captured[0]
        );
    }

    #[test]
    fn estimated_total_uses_the_provided_value_without_querying() {
        let fake = Arc::new(FakeConnection::new(vec![vec![vec![Value::Int(999)]]]));
        let connection: Arc<dyn Connection> = fake.clone();

        let source = TableSource::new(connection, None, "t", vec![column("a")], 10, Some(123));

        assert_eq!(source.estimated_total(), Some(123));
        assert!(
            fake.captured_sql.lock().unwrap().is_empty(),
            "an explicit estimate must not trigger a COUNT(*) query"
        );
    }

    #[test]
    fn estimated_total_queries_count_when_not_provided() {
        let fake = Arc::new(FakeConnection::new(vec![vec![vec![Value::Int(42)]]]));
        let connection: Arc<dyn Connection> = fake.clone();

        let source = TableSource::new(connection, None, "t", vec![column("a")], 10, None);

        assert_eq!(source.estimated_total(), Some(42));
        let captured = fake.captured_sql.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].contains("COUNT(*)"));
    }

    #[test]
    fn estimated_total_falls_back_to_none_when_count_query_fails() {
        let connection: Arc<dyn Connection> = Arc::new(FakeConnection::always_failing());

        let source = TableSource::new(connection, None, "t", vec![column("a")], 10, None);

        assert_eq!(
            source.estimated_total(),
            None,
            "a failed COUNT(*) must fall back to indeterminate progress, not error the transfer"
        );
    }

    #[test]
    fn estimated_total_falls_back_to_none_when_count_query_returns_no_rows() {
        let connection: Arc<dyn Connection> = Arc::new(FakeConnection::new(vec![vec![]]));

        let source = TableSource::new(connection, None, "t", vec![column("a")], 10, None);

        assert_eq!(source.estimated_total(), None);
    }
}
