use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_core::{
    CancelToken, ColumnKind, ColumnMeta, Connection, DbError, DbKind, DefaultSqlDialect,
    DriverMetadata, OrderByColumn, Pagination, QueryRequest, QueryResult, SchemaLoadingStrategy,
    SchemaSnapshot, SqlDialect, TableBrowseRequest, TableRef, Value,
};

use super::BrowseSource;
use crate::column_map::AutoColumnMap;
use crate::file_sink::{FileFormat, FileSink};
use crate::pipeline::{RowSource, TableMappingMode, TransferOutcome, run_transfer};

static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

/// Serves `total_rows` rows of `(id)` honouring the request's offset and
/// limit, and records every request it was asked.
struct PagedConnection {
    total_rows: u64,
    requests: Mutex<Vec<TableBrowseRequest>>,
}

impl PagedConnection {
    fn new(total_rows: u64) -> Self {
        Self {
            total_rows,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<TableBrowseRequest> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }
}

impl Connection for PagedConnection {
    #[expect(
        clippy::unimplemented,
        reason = "test mock: BrowseSource never reads connection metadata"
    )]
    fn metadata(&self) -> &DriverMetadata {
        unimplemented!("PagedConnection::metadata not needed for BrowseSource tests")
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _request: &QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported("browse only".to_string()))
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        if let Ok(mut requests) = self.requests.lock() {
            requests.push(request.clone());
        }

        let start = request.pagination.offset().min(self.total_rows);
        let end = (start + u64::from(request.pagination.limit())).min(self.total_rows);
        let rows = (start..end).map(|id| vec![Value::Int(id as i64)]).collect();

        Ok(QueryResult::table(
            vec![ColumnMeta {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                kind: ColumnKind::Integer,
                nullable: false,
                is_primary_key: true,
            }],
            rows,
            None,
            Duration::ZERO,
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

#[test]
fn browse_source_pages_from_the_first_row_with_the_request_filter_and_sort() {
    let connection = Arc::new(PagedConnection::new(25));
    let mut source =
        BrowseSource::open(connection.clone(), filtered_request(), 10, Some(25)).expect("opens");

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
    let mut source =
        BrowseSource::open(connection.clone(), filtered_request(), 10, None).expect("opens");

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
    let mut source = BrowseSource::open(connection, filtered_request(), 10, None).expect("opens");
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
