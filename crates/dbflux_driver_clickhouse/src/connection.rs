use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use dbflux_core::{
    ColumnMeta, Connection, ConnectionExt, DatabaseInfo, DbError, DbKind, DbSchemaInfo,
    DocumentConnection, DriverMetadata, KeyValueConnection, QueryCancelHandle, QueryGenerator,
    QueryHandle, QueryRequest, QueryResult, RelationalConnection, RelationalSchema, Row,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo, Value, WritePrivilege,
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

    /// Runs [`READONLY_SETTING_QUERY`], collapsing a query error or an
    /// unparseable value to `None` so the caller falls back to the grants
    /// probe.
    fn probe_readonly_setting(&self) -> Option<u8> {
        let result = self
            .execute_sql(READONLY_SETTING_QUERY, None, None, Some(1), None)
            .ok()?;
        let row = result.rows.first()?;
        value_text(row.first()?)?.trim().parse::<u8>().ok()
    }

    /// Runs [`GRANTS_QUERY`] and parses the result into [`ClickHouseGrantRow`]s.
    ///
    /// Returns an empty list on query failure, which [`classify_clickhouse_grants`]
    /// treats the same as "no resolvable grants" — i.e. [`WritePrivilege::Unknown`].
    fn probe_grants(&self) -> Vec<ClickHouseGrantRow> {
        let Ok(result) = self.execute_sql(GRANTS_QUERY, None, None, None, None) else {
            return Vec::new();
        };

        result
            .rows
            .iter()
            .filter_map(|row| {
                let access_type = value_text(row.first()?)?.to_string();
                let is_partial_revoke = matches!(row.get(1), Some(Value::Int(1)));
                Some(ClickHouseGrantRow {
                    access_type,
                    is_partial_revoke,
                })
            })
            .collect()
    }
}

fn value_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) | Value::Json(value) | Value::Decimal(value) => Some(value),
        _ => None,
    }
}

/// Combines direct-user grants and current-session-role grants into a single
/// round trip. `system.current_roles` lists the roles active in the current
/// session, so no second query is needed to know which role names to expand.
///
/// This does not walk a role's own nested roles: a write grant reachable only
/// through a role-of-a-role is not resolved here and such a configuration
/// falls through to [`WritePrivilege::Unknown`], which is a safe default.
const GRANTS_QUERY: &str = "\
    SELECT access_type, is_partial_revoke \
    FROM system.grants \
    WHERE user_name = currentUser() \
       OR role_name IN (SELECT role_name FROM system.current_roles)";

/// The `readonly` server setting for the current session: `0` allows writes,
/// `1` allows only read-only queries, `2` allows read-only queries plus
/// changing settings. Either `1` or `2` overrides any grant-derived verdict.
const READONLY_SETTING_QUERY: &str = "SELECT getSetting('readonly')";

/// One row from [`GRANTS_QUERY`]: an access type and whether it is a partial
/// revoke rather than a grant.
struct ClickHouseGrantRow {
    access_type: String,
    is_partial_revoke: bool,
}

const CLICKHOUSE_WRITE_ACCESS_TYPES: [&str; 4] = ["INSERT", "ALTER UPDATE", "ALTER DELETE", "ALL"];
const CLICKHOUSE_READ_ACCESS_TYPES: [&str; 3] = ["SELECT", "SHOW", "dictGet"];

/// Verdict for a single [`ClickHouseGrantRow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrantRowVerdict {
    /// Grants at least one write-capable access type (`INSERT`,
    /// `ALTER UPDATE`, `ALTER DELETE`, or `ALL`).
    Write,
    /// Grants only read-class access (`SELECT`, `SHOW`, `dictGet`).
    ReadOnly,
    /// A partial revoke, or an access type this probe does not recognize.
    Unrecognized,
}

/// Classifies a single grants row.
///
/// A partial revoke never grants anything, so it is treated the same as an
/// unrecognized access type: it contributes no evidence either way.
fn classify_clickhouse_grant_row(row: &ClickHouseGrantRow) -> GrantRowVerdict {
    if row.is_partial_revoke {
        return GrantRowVerdict::Unrecognized;
    }

    let access_type = row.access_type.trim();
    if CLICKHOUSE_WRITE_ACCESS_TYPES.contains(&access_type) {
        GrantRowVerdict::Write
    } else if CLICKHOUSE_READ_ACCESS_TYPES.contains(&access_type) {
        GrantRowVerdict::ReadOnly
    } else {
        GrantRowVerdict::Unrecognized
    }
}

/// Overall verdict from the full set of [`GRANTS_QUERY`] rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClickHouseGrantsVerdict {
    Writable,
    ReadOnly,
    Unknown,
}

/// Classifies the full grants result for the connected user.
///
/// Any row granting write-capable access makes the whole result `Writable`.
/// Otherwise, any recognized read-class grant makes it `ReadOnly`. When no
/// row parses to a recognized access type — including an empty result from a
/// query failure — the result is `Unknown`.
fn classify_clickhouse_grants(grants: &[ClickHouseGrantRow]) -> ClickHouseGrantsVerdict {
    let mut saw_read_grant = false;

    for row in grants {
        match classify_clickhouse_grant_row(row) {
            GrantRowVerdict::Write => return ClickHouseGrantsVerdict::Writable,
            GrantRowVerdict::ReadOnly => saw_read_grant = true,
            GrantRowVerdict::Unrecognized => {}
        }
    }

    if saw_read_grant {
        ClickHouseGrantsVerdict::ReadOnly
    } else {
        ClickHouseGrantsVerdict::Unknown
    }
}

/// Resolves `WritePrivilege` from the `readonly` server setting and the
/// connected user's (plus active session roles') grants.
///
/// The `readonly` setting takes precedence: a session forced into read-only
/// mode by server or session configuration overrides any grant.
fn resolve_clickhouse_write_privilege(
    readonly_setting: Option<u8>,
    grants: &[ClickHouseGrantRow],
) -> WritePrivilege {
    if matches!(readonly_setting, Some(1) | Some(2)) {
        return WritePrivilege::ReadOnly;
    }

    match classify_clickhouse_grants(grants) {
        ClickHouseGrantsVerdict::Writable => WritePrivilege::Writable,
        ClickHouseGrantsVerdict::ReadOnly => WritePrivilege::ReadOnly,
        ClickHouseGrantsVerdict::Unknown => WritePrivilege::Unknown,
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

    fn probe_write_privilege(&self) -> WritePrivilege {
        let readonly_setting = self.probe_readonly_setting();
        let grants = self.probe_grants();
        resolve_clickhouse_write_privilege(readonly_setting, &grants)
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
    use super::{
        ClickHouseGrantRow, classify_clickhouse_grant_row, classify_clickhouse_grants,
        parse_response, resolve_clickhouse_write_privilege,
    };
    use crate::http::HttpResponse;
    use dbflux_core::{ColumnKind, Value, WritePrivilege};
    use reqwest::header::HeaderMap;
    use std::time::Duration;

    fn grant(access_type: &str) -> ClickHouseGrantRow {
        ClickHouseGrantRow {
            access_type: access_type.to_string(),
            is_partial_revoke: false,
        }
    }

    fn revoke(access_type: &str) -> ClickHouseGrantRow {
        ClickHouseGrantRow {
            access_type: access_type.to_string(),
            is_partial_revoke: true,
        }
    }

    #[test]
    fn classify_clickhouse_grant_row_insert_is_write() {
        let verdict = classify_clickhouse_grant_row(&grant("INSERT"));
        assert_eq!(verdict, super::GrantRowVerdict::Write);
    }

    #[test]
    fn classify_clickhouse_grant_row_all_is_write() {
        let verdict = classify_clickhouse_grant_row(&grant("ALL"));
        assert_eq!(verdict, super::GrantRowVerdict::Write);
    }

    #[test]
    fn classify_clickhouse_grant_row_select_is_read_only() {
        let verdict = classify_clickhouse_grant_row(&grant("SELECT"));
        assert_eq!(verdict, super::GrantRowVerdict::ReadOnly);
    }

    #[test]
    fn classify_clickhouse_grant_row_unrecognized_access_type() {
        let verdict = classify_clickhouse_grant_row(&grant("CREATE TABLE"));
        assert_eq!(verdict, super::GrantRowVerdict::Unrecognized);
    }

    #[test]
    fn classify_clickhouse_grant_row_partial_revoke_is_unrecognized() {
        let verdict = classify_clickhouse_grant_row(&revoke("INSERT"));
        assert_eq!(verdict, super::GrantRowVerdict::Unrecognized);
    }

    #[test]
    fn classify_clickhouse_grants_any_write_row_is_writable() {
        let grants = vec![grant("SELECT"), grant("INSERT")];
        assert_eq!(
            classify_clickhouse_grants(&grants),
            super::ClickHouseGrantsVerdict::Writable
        );
    }

    #[test]
    fn classify_clickhouse_grants_only_read_rows_is_read_only() {
        let grants = vec![grant("SELECT"), grant("SHOW")];
        assert_eq!(
            classify_clickhouse_grants(&grants),
            super::ClickHouseGrantsVerdict::ReadOnly
        );
    }

    #[test]
    fn classify_clickhouse_grants_only_unrecognized_rows_is_unknown() {
        let grants = vec![grant("CREATE TABLE")];
        assert_eq!(
            classify_clickhouse_grants(&grants),
            super::ClickHouseGrantsVerdict::Unknown
        );
    }

    #[test]
    fn classify_clickhouse_grants_empty_is_unknown() {
        assert_eq!(
            classify_clickhouse_grants(&[]),
            super::ClickHouseGrantsVerdict::Unknown
        );
    }

    #[test]
    fn resolve_clickhouse_write_privilege_readonly_one_wins_over_write_grant() {
        let grants = vec![grant("INSERT")];
        assert_eq!(
            resolve_clickhouse_write_privilege(Some(1), &grants),
            WritePrivilege::ReadOnly
        );
    }

    #[test]
    fn resolve_clickhouse_write_privilege_readonly_two_wins_over_write_grant() {
        let grants = vec![grant("INSERT")];
        assert_eq!(
            resolve_clickhouse_write_privilege(Some(2), &grants),
            WritePrivilege::ReadOnly
        );
    }

    #[test]
    fn resolve_clickhouse_write_privilege_readonly_zero_falls_back_to_grants() {
        let grants = vec![grant("INSERT")];
        assert_eq!(
            resolve_clickhouse_write_privilege(Some(0), &grants),
            WritePrivilege::Writable
        );
    }

    #[test]
    fn resolve_clickhouse_write_privilege_missing_setting_falls_back_to_grants() {
        let grants = vec![grant("SELECT")];
        assert_eq!(
            resolve_clickhouse_write_privilege(None, &grants),
            WritePrivilege::ReadOnly
        );
    }

    #[test]
    fn resolve_clickhouse_write_privilege_no_grants_is_unknown() {
        assert_eq!(
            resolve_clickhouse_write_privilege(Some(0), &[]),
            WritePrivilege::Unknown
        );
    }

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
