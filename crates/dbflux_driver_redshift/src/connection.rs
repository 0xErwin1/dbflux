use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_core::{
    ColumnMeta, Connection, ConnectionExt, DbError, DbKind, DocumentConnection, DriverMetadata,
    ExecutionClassification, KeyValueConnection, QueryCancelHandle, QueryHandle, QueryLanguage,
    QueryRequest, QueryResult, RelationalConnection, Row, SchemaLoadingStrategy, SchemaSnapshot,
    SqlDialect, Value, classify_query_for_language,
};
use dbflux_ssh::SshTunnel;
use native_tls::TlsConnector;
use postgres::types::{FromSql, Type};
use postgres::{CancelToken, Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use uuid::Uuid;

use crate::dialect::REDSHIFT_DIALECT;
use crate::driver::METADATA;
use crate::error_formatter::{format_redshift_connection_error, format_redshift_query_error};
use crate::types::redshift_oid_to_kind;

/// Parameters for a direct (host/port) Redshift connection.
pub(crate) struct RedshiftConnectParams<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub password: &'a str,
    pub database: &'a str,
    /// libpq-style sslmode identifier (e.g. `"require"`, `"verify-ca"`).
    pub ssl_mode: &'a str,
}

/// Opens a Redshift connection using the same libpq `sslmode` semantics as
/// PostgreSQL: `disable` skips TLS entirely, `allow`/`prefer` attempt TLS and
/// fall back to plaintext, `require` mandates TLS without certificate
/// validation, and `verify-ca`/`verify-full` mandate TLS with certificate
/// validation.
pub(crate) fn connect_redshift(params: &RedshiftConnectParams) -> Result<Client, DbError> {
    let conn_string = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=30",
        params.host, params.port, params.user, params.password, params.database
    );

    match params.ssl_mode {
        "disable" => Client::connect(&conn_string, NoTls)
            .map_err(|e| format_redshift_connection_error(&e, params.host, params.port)),

        "verify-ca" | "verify-full" => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(false)
                .build()
                .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {e}").into()))?;

            Client::connect(&conn_string, MakeTlsConnector::new(connector))
                .map_err(|e| format_redshift_connection_error(&e, params.host, params.port))
        }

        "require" => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {e}").into()))?;

            Client::connect(&conn_string, MakeTlsConnector::new(connector))
                .map_err(|e| format_redshift_connection_error(&e, params.host, params.port))
        }

        // "allow" | "prefer" and any unrecognized mode: try TLS first, fall
        // back to plaintext when the handshake itself fails.
        _ => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {e}").into()))?;

            match Client::connect(&conn_string, MakeTlsConnector::new(connector)) {
                Ok(client) => Ok(client),
                Err(_) => Client::connect(&conn_string, NoTls)
                    .map_err(|e| format_redshift_connection_error(&e, params.host, params.port)),
            }
        }
    }
}

/// Classifies `sql` and rejects anything that is not a read/metadata
/// statement.
///
/// This is the authoritative read-only enforcement point: `DriverCapabilities`
/// already omits every write/DDL flag, but the grid's inline-edit gating keys
/// off `MutationPolicy` (a profile-level setting), not driver capabilities. A
/// caller could otherwise still route a raw INSERT/UPDATE/DELETE/DDL statement
/// through `execute()`, so this check runs before any statement reaches the
/// wire.
fn ensure_read_only(sql: &str) -> Result<(), DbError> {
    match classify_query_for_language(&QueryLanguage::Sql, sql) {
        ExecutionClassification::Read | ExecutionClassification::Metadata => Ok(()),
        _ => Err(DbError::NotSupported(
            "Amazon Redshift connections are read-only in DBFlux; only SELECT/EXPLAIN/SHOW statements are supported".to_string(),
        )),
    }
}

pub struct RedshiftConnection {
    pub(crate) client: Arc<Mutex<Client>>,
    #[allow(dead_code)]
    pub(crate) ssh_tunnel: Option<SshTunnel>,
    pub(crate) cancel_token: CancelToken,
    pub(crate) active_query: RwLock<Option<Uuid>>,
    pub(crate) cancelled: Arc<AtomicBool>,
}

struct RedshiftCancelHandle {
    cancel_token: CancelToken,
    cancelled: Arc<AtomicBool>,
}

impl QueryCancelHandle for RedshiftCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel Redshift query: {e}");
            DbError::QueryFailed(format!("Failed to cancel query: {e}").into())
        })?;

        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

struct ActiveQueryGuard<'a> {
    active_query: &'a RwLock<Option<Uuid>>,
}

impl<'a> ActiveQueryGuard<'a> {
    fn activate(active_query: &'a RwLock<Option<Uuid>>, query_id: Uuid) -> Result<Self, DbError> {
        let mut active = active_query
            .write()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;
        *active = Some(query_id);
        drop(active);

        Ok(Self { active_query })
    }
}

impl Drop for ActiveQueryGuard<'_> {
    fn drop(&mut self) {
        match self.active_query.write() {
            Ok(mut active) => *active = None,
            Err(error) => {
                log::warn!("[CLEANUP] Failed to clear active Redshift query state: {error}");
            }
        }
    }
}

impl Connection for RedshiftConnection {
    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;
        client
            .simple_query("SELECT 1")
            .map_err(|e| format_redshift_query_error(&e))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        ensure_read_only(&req.sql)?;

        self.cancelled.store(false, Ordering::SeqCst);

        let start = Instant::now();
        let query_id = Uuid::new_v4();
        let _active_query_guard = ActiveQueryGuard::activate(&self.active_query, query_id)?;

        let mut client = match self.client.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let stmt = client.prepare(&req.sql).map_err(|e| {
            if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
                DbError::Cancelled
            } else {
                format_redshift_query_error(&e)
            }
        })?;

        let columns: Vec<ColumnMeta> = stmt
            .columns()
            .iter()
            .map(|col| ColumnMeta {
                name: col.name().to_string(),
                type_name: col.type_().name().to_string(),
                kind: redshift_oid_to_kind(col.type_().oid()),
                nullable: true,
                is_primary_key: false,
            })
            .collect();

        let rows = client.query(&stmt, &[]).map_err(|e| {
            if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
                DbError::Cancelled
            } else {
                format_redshift_query_error(&e)
            }
        })?;

        drop(client);

        let result_rows: Vec<Row> = rows
            .iter()
            .take(req.limit.unwrap_or(u32::MAX) as usize)
            .map(|row| {
                (0..columns.len())
                    .map(|i| redshift_value_to_value(row, i))
                    .collect()
            })
            .collect();

        Ok(QueryResult::table(
            columns,
            result_rows,
            None,
            start.elapsed(),
        ))
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;

        if *active != Some(handle.id) {
            return Err(DbError::QueryFailed(
                "No matching active query to cancel".to_string().into(),
            ));
        }

        drop(active);

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel Redshift query: {e}");
            DbError::QueryFailed(format!("Failed to cancel query: {e}").into())
        })
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;

        if active.is_none() {
            return Ok(());
        }

        drop(active);

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel Redshift query: {e}");
            DbError::QueryFailed(format!("Failed to cancel query: {e}").into())
        })
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(RedshiftCancelHandle {
            cancel_token: self.cancel_token.clone(),
            cancelled: self.cancelled.clone(),
        })
    }

    /// Schema/table/view/column introspection lands with the metadata
    /// introspection layer; this crate currently only supports connecting and
    /// running read-only queries.
    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Err(DbError::NotSupported(
            "Redshift schema introspection is not yet implemented".to_string(),
        ))
    }

    fn kind(&self) -> DbKind {
        DbKind::Redshift
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::ConnectionPerDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &REDSHIFT_DIALECT
    }
}

impl RelationalConnection for RedshiftConnection {}

impl ConnectionExt for RedshiftConnection {
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

/// Wrapper that decodes any column as raw UTF-8 text.
///
/// The `postgres` crate's `FromSql<String>` only accepts the handful of OIDs
/// it recognises as textual, so Redshift's extended types (`SUPER`,
/// `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`, `HLLSKETCH`) and any other
/// unrecognised type fail that check silently. This wrapper accepts every
/// type and reads the wire bytes as UTF-8, giving `redshift_value_to_value` a
/// defensive fallback instead of a decode panic.
struct RedshiftText(String);

impl<'a> FromSql<'a> for RedshiftText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RedshiftText(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// Decodes a single column of a `postgres::Row` into a core `Value`.
///
/// Known scalar types decode through their native Rust representation.
/// Anything else (enums, domains, Redshift's extended types) falls back to a
/// raw-text decode via [`RedshiftText`]; a fully undecodable value becomes
/// `Value::Unsupported` rather than panicking.
///
/// `idx` is always in-bounds: callers derive it from `0..columns.len()` where
/// `columns` was itself built from the same row's column list.
#[allow(clippy::indexing_slicing)]
fn redshift_value_to_value(row: &postgres::Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_();
    let type_name = col_type.name();

    match type_name {
        "bool" => row
            .try_get::<_, Option<bool>>(idx)
            .map(|value| value.map(Value::Bool).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "int2" => row
            .try_get::<_, Option<i16>>(idx)
            .map(|value| value.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "int4" => row
            .try_get::<_, Option<i32>>(idx)
            .map(|value| value.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "int8" => row
            .try_get::<_, Option<i64>>(idx)
            .map(|value| value.map(Value::Int).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "float4" => row
            .try_get::<_, Option<f32>>(idx)
            .map(|value| {
                value
                    .map(|float| Value::Float(float as f64))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "float8" | "numeric" => row
            .try_get::<_, Option<f64>>(idx)
            .map(|value| value.map(Value::Float).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "text" | "varchar" | "bpchar" | "name" => row
            .try_get::<_, Option<String>>(idx)
            .map(|value| value.map(Value::Text).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "date" => row
            .try_get::<_, Option<NaiveDate>>(idx)
            .map(|value| value.map(Value::Date).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "time" => row
            .try_get::<_, Option<NaiveTime>>(idx)
            .map(|value| value.map(Value::Time).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "timestamp" => row
            .try_get::<_, Option<NaiveDateTime>>(idx)
            .map(|value| {
                value
                    .map(|timestamp| {
                        Value::DateTime(DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc))
                    })
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "timestamptz" => row
            .try_get::<_, Option<DateTime<Utc>>>(idx)
            .map(|value| value.map(Value::DateTime).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "inet" => row
            .try_get::<_, Option<IpAddr>>(idx)
            .map(|value| {
                value
                    .map(|ip| Value::Text(ip.to_string()))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "bytea" => row
            .try_get::<_, Option<Vec<u8>>>(idx)
            .map(|value| value.map(Value::Bytes).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        _ => match row.try_get::<_, Option<RedshiftText>>(idx) {
            Ok(Some(RedshiftText(text))) => Value::Text(text),
            Ok(None) => Value::Null,
            Err(error) => {
                let column_name = row.columns()[idx].name();
                log::info!(
                    "Unsupported Redshift type '{type_name}' (kind: {:?}) for column '{column_name}': {error}",
                    col_type.kind()
                );
                Value::Unsupported(type_name.to_string())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_read_only;
    use dbflux_core::DbError;

    #[test]
    fn select_is_allowed() {
        assert!(ensure_read_only("SELECT * FROM users").is_ok());
        assert!(ensure_read_only("select id from orders where id = 1").is_ok());
    }

    #[test]
    fn metadata_statements_are_allowed() {
        assert!(ensure_read_only("EXPLAIN SELECT 1").is_ok());
        assert!(ensure_read_only("SHOW search_path").is_ok());
        assert!(ensure_read_only("").is_ok());
    }

    #[test]
    fn insert_is_rejected() {
        let result = ensure_read_only("INSERT INTO users (name) VALUES ('a')");
        assert!(matches!(result, Err(DbError::NotSupported(_))));
    }

    #[test]
    fn update_is_rejected() {
        let result = ensure_read_only("UPDATE users SET name = 'a' WHERE id = 1");
        assert!(matches!(result, Err(DbError::NotSupported(_))));
    }

    #[test]
    fn delete_is_rejected() {
        let result = ensure_read_only("DELETE FROM users WHERE id = 1");
        assert!(matches!(result, Err(DbError::NotSupported(_))));
    }

    #[test]
    fn ddl_statements_are_rejected() {
        for sql in [
            "CREATE TABLE t (id int)",
            "DROP TABLE users",
            "TRUNCATE TABLE users",
            "ALTER TABLE users ADD COLUMN x int",
        ] {
            let result = ensure_read_only(sql);
            assert!(
                matches!(result, Err(DbError::NotSupported(_))),
                "expected {sql:?} to be rejected"
            );
        }
    }

    #[test]
    fn rejection_message_never_dumps_raw_debug_output() {
        let Err(DbError::NotSupported(message)) = ensure_read_only("DELETE FROM users") else {
            panic!("expected DbError::NotSupported");
        };

        assert!(!message.contains("ExecutionClassification"));
        assert!(message.contains("read-only"));
    }
}
