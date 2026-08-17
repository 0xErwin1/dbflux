use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use dbflux_core::{
    ColumnMeta, Connection, ConnectionExt, DatabaseInfo, DbError, DbKind, DbSchemaInfo,
    DocumentConnection, DriverMetadata, KeyValueConnection, QueryCancelHandle, QueryGenerator,
    QueryHandle, QueryRequest, QueryResult, RelationalConnection, RelationalSchema, Row,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo, Value,
};
use serde::Deserialize;

use crate::dialect::CLICKHOUSE_DIALECT;
use crate::driver::{METADATA, READ_ONLY_GENERATOR};
use crate::error_formatter::ClickHouseErrorFormatter;
use crate::http::{ClickHouseHttpClient, HttpResponse};
use crate::introspection;
use crate::types::{
    clickhouse_type_is_nullable, clickhouse_type_to_column_kind, json_to_value,
    parse_clickhouse_type,
};

pub struct ClickHouseConnection {
    client: ClickHouseHttpClient,
    active_database: RwLock<String>,
}

impl ClickHouseConnection {
    pub(crate) fn new(client: ClickHouseHttpClient, database: String) -> Self {
        Self {
            client,
            active_database: RwLock::new(database),
        }
    }

    pub(crate) fn validate_connection(&self) -> Result<(), DbError> {
        self.client
            .execute("SELECT 1", None, None, None, None)
            .map(|_| ())
            .map_err(|error| ClickHouseErrorFormatter::into_connection_error(&error))
    }

    pub(crate) fn execute_sql(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout: Option<Duration>,
        row_limit: Option<u32>,
        row_offset: Option<u32>,
    ) -> Result<QueryResult, DbError> {
        let started = Instant::now();
        let response = self
            .client
            .execute(sql, database, timeout, row_limit, row_offset)
            .map_err(|error| {
                ClickHouseErrorFormatter::format_http_error(&error).into_query_error()
            })?;
        parse_response(response, started.elapsed())
    }

    fn current_database(&self) -> Result<String, DbError> {
        self.active_database
            .read()
            .map(|database| database.clone())
            .map_err(|error| {
                DbError::QueryFailed(format!("ClickHouse database lock failed: {error}").into())
            })
    }
}

impl Connection for ClickHouseConnection {
    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.execute_sql("SELECT 1", None, None, Some(1), None)
            .map(|_| ())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
        if !request.params.is_empty() {
            return Err(DbError::NotSupported(
                "ClickHouse HTTP queries do not support QueryRequest parameters".to_string(),
            ));
        }
        let active_database = self.current_database()?;
        let database = request.database.as_deref().unwrap_or(&active_database);
        self.execute_sql(
            &request.sql,
            Some(database),
            request.statement_timeout,
            request.limit,
            request.offset,
        )
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "ClickHouse HTTP query cancellation is not supported".to_string(),
        ))
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(dbflux_core::NoopCancelHandle)
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let databases = introspection::list_databases(self)?;
        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases,
            current_database: Some(self.current_database()?),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        introspection::list_databases(self)
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        introspection::schema_for_database(self, database)
    }

    fn table_details(
        &self,
        database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        introspection::table_details(self, database, table)
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        let Some(database) = database else {
            return Ok(());
        };
        let mut active = self.active_database.write().map_err(|error| {
            DbError::QueryFailed(format!("ClickHouse database lock failed: {error}").into())
        })?;
        *active = database.to_string();
        Ok(())
    }

    fn active_database(&self) -> Option<String> {
        self.active_database
            .read()
            .ok()
            .map(|database| database.clone())
    }

    fn kind(&self) -> DbKind {
        DbKind::ClickHouse
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &CLICKHOUSE_DIALECT
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        Some(&READ_ONLY_GENERATOR)
    }
}

impl RelationalConnection for ClickHouseConnection {}

impl ConnectionExt for ClickHouseConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        Some(self)
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        None
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
    }
}

#[derive(Deserialize)]
struct CompactResponse {
    #[serde(default)]
    meta: Vec<CompactColumn>,
    #[serde(default)]
    data: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct CompactColumn {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

fn parse_response(
    response: HttpResponse,
    execution_time: Duration,
) -> Result<QueryResult, DbError> {
    if response.body.iter().all(u8::is_ascii_whitespace) {
        let affected_rows = response
            .headers
            .get("X-ClickHouse-Summary")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| serde_json::from_str::<HashMap<String, String>>(value).ok())
            .and_then(|summary| {
                summary
                    .get("written_rows")
                    .and_then(|value| value.parse().ok())
            });
        return Ok(QueryResult::table(
            Vec::new(),
            Vec::new(),
            affected_rows,
            execution_time,
        ));
    }

    let compact: CompactResponse = serde_json::from_slice(&response.body).map_err(|error| {
        DbError::QueryFailed(
            format!("Invalid JSONCompact response from ClickHouse: {error}").into(),
        )
    })?;
    let parsed_types = compact
        .meta
        .iter()
        .map(|column| parse_clickhouse_type(&column.type_name))
        .collect::<Vec<_>>();
    let columns = compact
        .meta
        .iter()
        .zip(&parsed_types)
        .map(|(column, data_type)| ColumnMeta {
            name: column.name.clone(),
            type_name: column.type_name.clone(),
            kind: clickhouse_type_to_column_kind(data_type),
            nullable: clickhouse_type_is_nullable(data_type),
            is_primary_key: false,
        })
        .collect::<Vec<_>>();
    let rows = compact
        .data
        .iter()
        .map(|row| {
            if row.len() != parsed_types.len() {
                return Err(DbError::QueryFailed(
                    format!(
                        "Invalid JSONCompact row width from ClickHouse: expected {}, received {}",
                        parsed_types.len(),
                        row.len()
                    )
                    .into(),
                ));
            }
            let values = parsed_types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    row.get(index)
                        .map(|value| json_to_value(value, data_type))
                        .unwrap_or(Value::Null)
                })
                .collect::<Row>();
            Ok(values)
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    Ok(QueryResult::table(columns, rows, None, execution_time))
}

#[cfg(test)]
mod tests {
    use super::parse_response;
    use crate::http::HttpResponse;
    use dbflux_core::{ColumnKind, Value};
    use reqwest::header::HeaderMap;
    use std::time::Duration;

    #[test]
    fn parses_json_compact_metadata_and_values() {
        let response = HttpResponse {
            body: br#"{"meta":[{"name":"id","type":"UInt64"},{"name":"ts","type":"DateTime64(3, 'UTC')"}],"data":[["18446744073709551615","2026-08-17T12:00:00.000Z"]],"rows":1}"#.to_vec(),
            headers: HeaderMap::new(),
        };
        let result = parse_response(response, Duration::ZERO).expect("valid response");
        assert_eq!(result.columns[0].kind, ColumnKind::Integer);
        assert_eq!(result.columns[1].kind, ColumnKind::Timestamp);
        assert_eq!(
            result.rows[0][0],
            Value::Decimal("18446744073709551615".to_string())
        );
        assert_eq!(result.affected_rows, None);
    }
}
