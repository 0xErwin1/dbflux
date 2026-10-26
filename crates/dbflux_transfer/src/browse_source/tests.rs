use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_core::{
    CancelToken, CollectionBrowseRequest, CollectionRef, ColumnKind, ColumnMeta, Connection,
    DbError, DbKind, DefaultSqlDialect, DriverMetadata, OrderByColumn, Pagination, Projection,
    QueryGenerator, QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SortEntry,
    SourceTable, SqlDialect, SqlMutationGenerator, TableBrowseRequest, TableRef, Value,
    VisualQuerySpec,
};

use super::{AlignByName, BrowseSource, PageShape, ReplayableRead};
use crate::column_map::AutoColumnMap;
use crate::file_sink::{FileFormat, FileSink};
use crate::pipeline::{RowSource, TableMappingMode, TransferOutcome, run_transfer};

static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&DIALECT);

/// Serves `total_rows` rows of `(id)` honouring the offset and limit of a
/// table browse, a collection browse or a generated SELECT, and records
/// every request it was asked.
struct PagedConnection {
    total_rows: u64,
    requests: Mutex<Vec<TableBrowseRequest>>,
    collection_requests: Mutex<Vec<CollectionBrowseRequest>>,
    executed: Mutex<Vec<QueryRequest>>,
}

impl PagedConnection {
    fn new(total_rows: u64) -> Self {
        Self {
            total_rows,
            requests: Mutex::new(Vec::new()),
            collection_requests: Mutex::new(Vec::new()),
            executed: Mutex::new(Vec::new()),
        }
    }

    fn rows(&self, offset: u64, limit: u64) -> QueryResult {
        let start = offset.min(self.total_rows);
        let end = (start + limit).min(self.total_rows);
        let rows = (start..end).map(|id| vec![Value::Int(id as i64)]).collect();

        QueryResult::table(vec![id_column()], rows, None, Duration::ZERO)
    }

    fn collection_requests(&self) -> Vec<CollectionBrowseRequest> {
        self.collection_requests
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }

    fn executed_sql(&self) -> Vec<String> {
        self.executed
            .lock()
            .map(|requests| requests.iter().map(|request| request.sql.clone()).collect())
            .unwrap_or_default()
    }

    fn requests(&self) -> Vec<TableBrowseRequest> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }
}

fn id_column() -> ColumnMeta {
    ColumnMeta {
        name: "id".to_string(),
        type_name: "integer".to_string(),
        kind: ColumnKind::Integer,
        nullable: false,
        is_primary_key: true,
    }
}

/// Reads `LIMIT n` and `OFFSET m` out of generated SQL.
fn sql_number_after(sql: &str, keyword: &str) -> Option<u64> {
    let upper = sql.to_uppercase();
    let start = upper.find(keyword)? + keyword.len();
    upper[start..]
        .split_whitespace()
        .next()
        .and_then(|number| number.parse().ok())
}

impl Connection for PagedConnection {
    fn metadata(&self) -> &DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, Icon, QueryLanguage, TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "paged".to_string(),
            display_name: "Paged".to_string(),
            description: "test".to_string(),
            category: DatabaseCategory::Relational,
            transfer_family: TransferFamily::Sql,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "paged".to_string(),
            icon: Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        })
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        Some(&GENERATOR)
    }

    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        if let Ok(mut requests) = self.collection_requests.lock() {
            requests.push(request.clone());
        }

        Ok(self.rows(
            request.pagination.offset(),
            u64::from(request.pagination.limit()),
        ))
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
        if let Ok(mut executed) = self.executed.lock() {
            executed.push(request.clone());
        }

        let limit = sql_number_after(&request.sql, "LIMIT").unwrap_or(self.total_rows);
        let offset = sql_number_after(&request.sql, "OFFSET").unwrap_or(0);
        Ok(self.rows(offset, limit))
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        if let Ok(mut requests) = self.requests.lock() {
            requests.push(request.clone());
        }

        Ok(self.rows(
            request.pagination.offset(),
            u64::from(request.pagination.limit()),
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

fn filtered_request() -> TableBrowseRequest {
    TableBrowseRequest::new(TableRef::with_schema("public", "users"))
        .with_pagination(Pagination::Offset {
            limit: 100,
            offset: 300,
        })
        .with_order_by(vec![OrderByColumn::desc("id")])
        .with_filter("id > 0".to_string())
}

fn table_read() -> ReplayableRead {
    ReplayableRead::Table(filtered_request())
}

fn drain(source: &mut BrowseSource) -> Vec<Value> {
    let cancel = CancelToken::new();
    let mut ids = Vec::new();
    while let Some(chunk) = source.next_chunk(&cancel).expect("reads a page") {
        ids.extend(chunk.0.into_iter().map(|mut row| row.remove(0)));
    }
    ids
}

fn users_spec(limit: Option<u64>, offset: u64) -> VisualQuerySpec {
    VisualQuerySpec {
        source: SourceTable {
            schema: None,
            table: "users".to_string(),
            alias: "users".to_string(),
        },
        projection: Projection::All,
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        having: None,
        sort: Vec::<SortEntry>::new(),
        limit,
        offset,
    }
}

#[test]
fn collection_read_pages_from_the_skip_with_the_request_filter_and_stops_on_a_short_page() {
    let connection = Arc::new(PagedConnection::new(25));
    let request = CollectionBrowseRequest::new(CollectionRef::new("app", "events"))
        .with_pagination(Pagination::Offset {
            limit: 50,
            offset: 150,
        })
        .with_filter(serde_json::json!({ "kind": "click" }))
        .with_sort(serde_json::json!({ "_id": 1 }));
    let read = ReplayableRead::Collection { request, skip: 3 };

    let mut source = BrowseSource::open(connection.clone(), read, 10, None).expect("opens");

    assert_eq!(
        drain(&mut source),
        (3..25).map(Value::Int).collect::<Vec<_>>()
    );
    let requests = connection.collection_requests();
    let offsets: Vec<u64> = requests.iter().map(|r| r.pagination.offset()).collect();
    assert_eq!(offsets, vec![3, 13, 23], "a short page ends the read");
    for request in &requests {
        assert_eq!(request.pagination.limit(), 10);
        assert_eq!(request.filter, Some(serde_json::json!({ "kind": "click" })));
        assert_eq!(request.sort, Some(serde_json::json!({ "_id": 1 })));
    }
}

#[test]
fn visual_query_read_pages_the_spec_until_a_short_page() {
    let connection = Arc::new(PagedConnection::new(25));
    let read = ReplayableRead::VisualQuery {
        spec: users_spec(None, 0),
        database: None,
    };

    let mut source = BrowseSource::open(connection.clone(), read, 10, None).expect("opens");

    assert_eq!(
        drain(&mut source),
        (0..25).map(Value::Int).collect::<Vec<_>>()
    );
    let executed = connection.executed_sql();
    assert_eq!(
        executed.len(),
        3,
        "a short page ends the read: {executed:?}"
    );
    let pages: Vec<(Option<u64>, Option<u64>)> = executed
        .iter()
        .map(|sql| {
            (
                sql_number_after(sql, "LIMIT"),
                sql_number_after(sql, "OFFSET"),
            )
        })
        .collect();
    assert_eq!(pages[0].0, Some(10));
    assert_eq!(pages[1], (Some(10), Some(10)));
    assert_eq!(pages[2], (Some(10), Some(20)));
}

#[test]
fn visual_query_read_keeps_the_spec_limit_and_offset() {
    let connection = Arc::new(PagedConnection::new(100));
    let read = ReplayableRead::VisualQuery {
        spec: users_spec(Some(23), 5),
        database: None,
    };

    let mut source = BrowseSource::open(connection.clone(), read, 10, None).expect("opens");

    assert_eq!(
        drain(&mut source),
        (5..28).map(Value::Int).collect::<Vec<_>>()
    );
    let pages: Vec<(Option<u64>, Option<u64>)> = connection
        .executed_sql()
        .iter()
        .map(|sql| {
            (
                sql_number_after(sql, "LIMIT"),
                sql_number_after(sql, "OFFSET"),
            )
        })
        .collect();
    assert_eq!(
        pages,
        vec![
            (Some(10), Some(5)),
            (Some(10), Some(15)),
            (Some(3), Some(25))
        ],
        "the last page asks only for what the spec limit leaves"
    );
}

#[test]
fn align_by_name_keeps_the_first_page_columns_and_reports_the_new_ones() {
    let column = |name: &str| ColumnMeta {
        name: name.to_string(),
        type_name: "text".to_string(),
        kind: ColumnKind::Text,
        nullable: true,
        is_primary_key: false,
    };
    let first = QueryResult::json(
        vec![column("_id"), column("name")],
        vec![vec![Value::Int(1), Value::Text("a".into())]],
        Duration::ZERO,
    );
    let later = QueryResult::json(
        vec![column("_id"), column("email"), column("name")],
        vec![vec![
            Value::Int(2),
            Value::Text("b@x".into()),
            Value::Text("b".into()),
        ]],
        Duration::ZERO,
    );
    let without_name = QueryResult::json(
        vec![column("_id")],
        vec![vec![Value::Int(3)]],
        Duration::ZERO,
    );

    let mut shape = AlignByName::default();
    let names: Vec<String> = shape
        .columns(&first)
        .into_iter()
        .map(|column| column.name)
        .collect();
    assert_eq!(names, vec!["_id", "name"]);
    assert_eq!(
        shape.rows(first),
        vec![vec![Value::Int(1), Value::Text("a".into())]]
    );
    assert_eq!(
        shape.rows(later),
        vec![vec![Value::Int(2), Value::Text("b".into())]]
    );
    assert_eq!(
        shape.rows(without_name),
        vec![vec![Value::Int(3), Value::Null]]
    );
    assert_eq!(shape.dropped_columns(), vec!["email".to_string()]);
}

#[test]
fn browse_source_pages_from_the_first_row_with_the_request_filter_and_sort() {
    let connection = Arc::new(PagedConnection::new(25));
    let mut source =
        BrowseSource::open(connection.clone(), table_read(), 10, Some(25)).expect("opens");

    let cancel = CancelToken::new();
    let mut ids = Vec::new();
    while let Some(chunk) = source.next_chunk(&cancel).expect("reads a page") {
        for row in chunk.0 {
            ids.push(row[0].clone());
        }
    }

    assert_eq!(ids, (0..25).map(Value::Int).collect::<Vec<_>>());
    assert_eq!(source.columns()[0].name, "id");
    assert_eq!(source.estimated_total(), Some(25));

    let requests = connection.requests();
    let offsets: Vec<u64> = requests
        .iter()
        .map(|request| request.pagination.offset())
        .collect();
    assert_eq!(offsets, vec![0, 10, 20], "a short page ends the read");
    for request in &requests {
        assert_eq!(request.pagination.limit(), 10);
        assert_eq!(request.filter.as_deref(), Some("id > 0"));
        assert_eq!(request.order_by, vec![OrderByColumn::desc("id")]);
    }
}

#[test]
fn browse_source_stops_reading_once_cancelled() {
    let connection = Arc::new(PagedConnection::new(25));
    let mut source = BrowseSource::open(connection.clone(), table_read(), 10, None).expect("opens");

    let cancel = CancelToken::new();
    assert!(source.next_chunk(&cancel).expect("first page").is_some());
    cancel.cancel();
    assert!(source.next_chunk(&cancel).expect("cancelled").is_none());
    assert_eq!(connection.requests().len(), 1);
}

#[test]
fn browse_source_streams_every_page_into_one_file() {
    let dir = std::env::temp_dir().join(format!(
        "dbflux_transfer_browse_source_test_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("users.csv");

    let connection = Arc::new(PagedConnection::new(23));
    let mut source = BrowseSource::open(connection, table_read(), 10, None).expect("opens");
    let columns = source.columns().to_vec();
    let column_map = AutoColumnMap::new(&columns, &columns);
    let mut sink = FileSink::to_file(&path, FileFormat::Csv);

    let report = run_transfer(
        &mut source,
        &column_map,
        &mut sink,
        TableMappingMode::Existing,
        &CancelToken::new(),
        &mut |_, _| {},
    )
    .expect("transfer runs");

    assert_eq!(report.outcome, TransferOutcome::Completed);
    assert_eq!(report.rows_transferred, 23);

    let contents = std::fs::read_to_string(&path).expect("file written");
    let expected: String = std::iter::once("id".to_string())
        .chain((0..23).map(|id| id.to_string()))
        .map(|line| format!("{line}\n"))
        .collect();
    assert_eq!(contents, expected);

    std::fs::remove_dir_all(&dir).ok();
}
