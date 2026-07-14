use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_core::{
    ColumnMeta, Connection, ConnectionExt, DatabaseInfo, DbError, DbKind, DocumentConnection,
    DriverMetadata, ExecutionClassification, KeyValueConnection, QueryCancelHandle, QueryHandle,
    QueryLanguage, QueryRequest, QueryResult, RelationalConnection, RelationalSchema, Row,
    SchemaFeatures, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo, Value,
    classify_query_for_language,
};
use dbflux_ssh::SshTunnel;
use native_tls::TlsConnector;
use postgres::config::SslMode;
use postgres::types::{FromSql, Type};
use postgres::{CancelToken, Client, Config, NoTls};
use postgres_native_tls::MakeTlsConnector;
use uuid::Uuid;

use crate::dialect::REDSHIFT_DIALECT;
use crate::driver::METADATA;
use crate::error_formatter::{format_redshift_connection_error, format_redshift_query_error};
use crate::introspection::{get_current_database, get_databases, get_schemas, get_table_details};
use crate::types::{decode_defensive_fallback, decode_numeric_fallback, redshift_oid_to_kind};

/// Default connect timeout applied to every direct/tunnel connection, and to
/// URI connections that do not carry their own `connect_timeout`.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Unified libpq `sslmode` representation, parsed once and shared by both the
/// direct/form connect path and the URI connect path.
///
/// Redshift follows PostgreSQL's `sslmode` semantics. The six libpq values
/// collapse into four connection behaviors: `disable` skips TLS entirely;
/// `allow`/`prefer` attempt TLS and fall back to plaintext; `require` mandates
/// TLS without certificate validation; and `verify-ca`/`verify-full` mandate
/// TLS with certificate validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedshiftSslMode {
    Disable,
    Prefer,
    Require,
    Verify,
}

impl RedshiftSslMode {
    /// Parses a libpq `sslmode` value. Unknown values (and the empty string)
    /// fall back to `prefer`, matching libpq's own default posture.
    pub(crate) fn parse(value: &str) -> RedshiftSslMode {
        match value.to_ascii_lowercase().as_str() {
            "disable" => RedshiftSslMode::Disable,
            "require" => RedshiftSslMode::Require,
            "verify-ca" | "verify-full" => RedshiftSslMode::Verify,
            // "allow" | "prefer" and any unrecognized value.
            _ => RedshiftSslMode::Prefer,
        }
    }

    /// Maps to the protocol-level `postgres::Config` ssl mode. `Verify` mandates
    /// TLS at the protocol level (`Require`) and defers certificate validation
    /// to the native-tls connector built in [`connect_with_ssl_mode`].
    pub(crate) fn config_ssl_mode(self) -> SslMode {
        match self {
            RedshiftSslMode::Disable => SslMode::Disable,
            RedshiftSslMode::Prefer => SslMode::Prefer,
            RedshiftSslMode::Require | RedshiftSslMode::Verify => SslMode::Require,
        }
    }
}

/// Builds a fully-escaped `postgres::Config` for a direct/tunnel connection.
///
/// Every field is set through the typed builder rather than concatenated into a
/// libpq key=value string, so a password (or any other field) containing a
/// space, a quote, a backslash, or a substring such as `sslmode=disable` is
/// carried as an opaque value and can neither break parsing nor inject or
/// downgrade another connection parameter.
fn build_base_config(params: &RedshiftConnectParams, ssl_mode: RedshiftSslMode) -> Config {
    let mut config = Config::new();

    config
        .host(params.host)
        .port(params.port)
        .user(params.user)
        .password(params.password)
        .dbname(params.database)
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .ssl_mode(ssl_mode.config_ssl_mode());

    config
}

/// Opens a Redshift connection for a fully-built [`Config`] using the native-tls
/// connector policy for `ssl_mode`.
///
/// Shared by the direct/form path and the URI path so the per-mode certificate
/// policy lives in exactly one place: `disable` connects without TLS;
/// `require`/`prefer` accept invalid certificates; `verify-ca`/`verify-full`
/// validate them; and `prefer` (like libpq) retries in plaintext when the TLS
/// attempt fails. `map_err` lets each caller attach its own connection-error
/// context (host/port for the direct path, sanitized URI for the URI path).
pub(crate) fn connect_with_ssl_mode(
    config: &Config,
    ssl_mode: RedshiftSslMode,
    map_err: impl Fn(&postgres::Error) -> DbError,
) -> Result<Client, DbError> {
    match ssl_mode {
        RedshiftSslMode::Disable => config.connect(NoTls).map_err(|e| map_err(&e)),

        RedshiftSslMode::Verify => {
            let connector = build_tls_connector(false)?;
            config
                .connect(MakeTlsConnector::new(connector))
                .map_err(|e| map_err(&e))
        }

        RedshiftSslMode::Require => {
            let connector = build_tls_connector(true)?;
            config
                .connect(MakeTlsConnector::new(connector))
                .map_err(|e| map_err(&e))
        }

        RedshiftSslMode::Prefer => {
            let connector = build_tls_connector(true)?;
            match config.connect(MakeTlsConnector::new(connector)) {
                Ok(client) => Ok(client),
                Err(_) => config.connect(NoTls).map_err(|e| map_err(&e)),
            }
        }
    }
}

fn build_tls_connector(accept_invalid_certs: bool) -> Result<TlsConnector, DbError> {
    TlsConnector::builder()
        .danger_accept_invalid_certs(accept_invalid_certs)
        .build()
        .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {e}").into()))
}

/// Opens a Redshift connection using the same libpq `sslmode` semantics as
/// PostgreSQL (see [`RedshiftSslMode`]).
pub(crate) fn connect_redshift(params: &RedshiftConnectParams) -> Result<Client, DbError> {
    let ssl_mode = RedshiftSslMode::parse(params.ssl_mode);
    let config = build_base_config(params, ssl_mode);

    connect_with_ssl_mode(&config, ssl_mode, |e| {
        format_redshift_connection_error(e, params.host, params.port)
    })
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
///
/// Multi-statement input is rejected outright: this driver runs a single
/// read-only statement through `client.prepare()`, so a multi-statement buffer
/// would otherwise reach the wire as a raw protocol error. The shared
/// classifier's `SELECT`/`WITH` path already rejects multi-statement input, but
/// its `EXPLAIN`/`SHOW`/`DESC` path maps straight to `Metadata` with no such
/// check — so a buffer like `EXPLAIN SELECT 1; DROP TABLE t` would otherwise
/// slip past. This guard runs first and covers every leading keyword. The
/// comment/quote-aware tokenizer counts top-level `;`-separated statements so a
/// `;` inside a string literal, quoted identifier, or comment — and a single
/// optional trailing `;` — do not trip the check.
///
/// `SELECT ... INTO` gets a dedicated check on top of the shared classifier:
/// `classify_query_for_language` only inspects the leading keyword, so it
/// treats `SELECT ... INTO newtable FROM t` — and its CTE-prefixed variant
/// `WITH c AS (...) SELECT ... INTO newtable FROM t`, whose leading keyword
/// is `WITH` — as an ordinary read. On Redshift (and PostgreSQL) that form is
/// CTAS — it creates a table — so it must be rejected here, before any
/// statement reaches the wire, rather than relying on the shared classifier
/// used by every other SQL driver (some of which legitimately allow
/// `SELECT ... INTO @var`).
fn ensure_read_only(sql: &str) -> Result<(), DbError> {
    if has_multiple_statements(sql) {
        return Err(DbError::NotSupported(
            "Redshift driver runs a single read-only statement at a time; multiple statements are not supported".to_string(),
        ));
    }

    if is_select_into(sql) {
        return Err(DbError::NotSupported(
            "Amazon Redshift connections are read-only in DBFlux; SELECT ... INTO creates a table and is not supported".to_string(),
        ));
    }

    match classify_query_for_language(&QueryLanguage::Sql, sql) {
        ExecutionClassification::Read | ExecutionClassification::Metadata => Ok(()),
        _ => Err(DbError::NotSupported(
            "Amazon Redshift connections are read-only in DBFlux; only SELECT/EXPLAIN/SHOW statements are supported".to_string(),
        )),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SqlScanState {
    Normal,
    LineComment,
    BlockComment,
    SingleQuote,
    DoubleQuote,
}

/// Returns `true` when `sql` is a `SELECT` or `WITH`-led statement containing
/// a top-level `INTO` keyword (the `SELECT ... INTO newtable FROM ...` CTAS
/// form, including its CTE-prefixed variant `WITH c AS (...) SELECT ... INTO
/// newtable FROM ...`).
///
/// `WITH` is included alongside `SELECT` because a CTE-prefixed CTAS still
/// has `WITH` as its leading keyword, and `INTO` is not valid syntax inside a
/// CTE's own parenthesized body — so a top-level `INTO` found anywhere after
/// a `WITH`-led statement always belongs to the outer `SELECT ... INTO`.
///
/// Comments and quoted string/identifier contents are stripped before
/// tokenizing, so `INTO` appearing inside a comment, a string literal, or a
/// quoted identifier is never mistaken for the keyword, and `INTO` is only
/// matched as a whole word (not a substring of an identifier like
/// `point_into`).
fn is_select_into(sql: &str) -> bool {
    let scanned = strip_comments_and_quoted_content(sql);

    let mut words = scanned
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|word| !word.is_empty());

    let Some(leading_keyword) = words.next() else {
        return false;
    };

    if !leading_keyword.eq_ignore_ascii_case("select")
        && !leading_keyword.eq_ignore_ascii_case("with")
    {
        return false;
    }

    words.any(|word| word.eq_ignore_ascii_case("into"))
}

/// Returns `true` when `sql` contains more than one non-empty top-level
/// statement.
///
/// Comments and quoted string/identifier contents are stripped first so a `;`
/// inside a literal (`SELECT ';'`), a quoted identifier, or a comment is not
/// counted as a statement separator. A single optional trailing `;` leaves one
/// non-empty segment and is therefore allowed.
fn has_multiple_statements(sql: &str) -> bool {
    let scanned = strip_comments_and_quoted_content(sql);

    scanned
        .split(';')
        .filter(|segment| !segment.trim().is_empty())
        .count()
        > 1
}

/// Removes comments and the contents of single/double-quoted regions from
/// `sql`, replacing each with whitespace so word-boundary tokenization
/// cannot reconstruct a keyword from inside a literal. Doubled quote escapes
/// (`''`, `""`) are honored so an escaped quote does not end the literal
/// early.
///
/// `index` is always in-bounds: the loop condition below checks it against
/// `chars.len()` before every indexed read.
#[allow(clippy::indexing_slicing)]
fn strip_comments_and_quoted_content(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut state = SqlScanState::Normal;
    let mut index = 0;

    while index < chars.len() {
        let current = chars[index];
        let next = chars.get(index + 1).copied();

        match state {
            SqlScanState::Normal => {
                if current == '-' && next == Some('-') {
                    state = SqlScanState::LineComment;
                    index += 2;
                    continue;
                }

                if current == '/' && next == Some('*') {
                    state = SqlScanState::BlockComment;
                    index += 2;
                    continue;
                }

                if current == '\'' {
                    state = SqlScanState::SingleQuote;
                    result.push(' ');
                    index += 1;
                    continue;
                }

                if current == '"' {
                    state = SqlScanState::DoubleQuote;
                    result.push(' ');
                    index += 1;
                    continue;
                }

                result.push(current);
                index += 1;
            }

            SqlScanState::LineComment => {
                if current == '\n' {
                    result.push('\n');
                    state = SqlScanState::Normal;
                }
                index += 1;
            }

            SqlScanState::BlockComment => {
                if current == '*' && next == Some('/') {
                    state = SqlScanState::Normal;
                    index += 2;
                } else {
                    index += 1;
                }
            }

            SqlScanState::SingleQuote => {
                if current == '\'' {
                    if next == Some('\'') {
                        index += 2;
                        continue;
                    }
                    state = SqlScanState::Normal;
                }
                index += 1;
            }

            SqlScanState::DoubleQuote => {
                if current == '"' {
                    if next == Some('"') {
                        index += 2;
                        continue;
                    }
                    state = SqlScanState::Normal;
                }
                index += 1;
            }
        }
    }

    result
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

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;

        let databases = get_databases(&mut client)?;
        let current_database = get_current_database(&mut client)?;
        let schemas = get_schemas(&mut client)?;

        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases,
            current_database,
            schemas,
            tables: Vec::new(),
            views: Vec::new(),
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;

        get_databases(&mut client)
    }

    fn table_details(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        let schema_name = schema.unwrap_or("public");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {e}").into()))?;

        get_table_details(&mut client, schema_name, table)
    }

    /// Redshift accepts (but does not enforce) PK/FK/UNIQUE constraints, so
    /// this driver populates them from the catalog like PostgreSQL — the
    /// `storage_hints` "Constraints advisory" entry is what tells the UI they
    /// are informational only. Redshift has no true indexes and no CHECK
    /// constraints, so those feature flags stay unset.
    fn schema_features(&self) -> SchemaFeatures {
        SchemaFeatures::FOREIGN_KEYS | SchemaFeatures::UNIQUE_CONSTRAINTS
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

/// Wrapper that captures a column's raw wire bytes unconditionally.
///
/// The `postgres` crate's `FromSql<String>` only accepts the handful of OIDs
/// it recognises as textual, so Redshift's extended types (`SUPER`,
/// `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`, `HLLSKETCH`) and any other
/// unrecognised type fail that check silently. This wrapper accepts every
/// type and copies the wire bytes verbatim — it never fails — so the actual
/// decode/fallback decision lives in the pure, unit-tested
/// [`decode_defensive_fallback`].
struct RedshiftRawBytes(Vec<u8>);

impl<'a> FromSql<'a> for RedshiftRawBytes {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RedshiftRawBytes(raw.to_vec()))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// Decodes column `idx` as `T`, falling back to the defensive text decode
/// (see [`decode_defensive_fallback`]) when the typed decode itself fails.
///
/// A failed `try_get` means the value is present but this Rust type could
/// not decode it (e.g. a type/format mismatch) — that must not be conflated
/// with a genuine SQL NULL (`Ok(None)`), which is the only case that yields
/// `Value::Null`.
fn decode_typed_or_fallback<T>(
    row: &postgres::Row,
    idx: usize,
    oid: u32,
    type_name: &str,
    to_value: impl FnOnce(T) -> Value,
) -> Value
where
    T: for<'a> FromSql<'a>,
{
    match row.try_get::<_, Option<T>>(idx) {
        Ok(Some(value)) => to_value(value),
        Ok(None) => Value::Null,
        Err(_) => decode_via_raw_bytes(row, idx, oid, type_name, decode_defensive_fallback),
    }
}

/// Captures column `idx`'s raw wire bytes via [`RedshiftRawBytes`] (which
/// accepts every type) and hands them to `decode` for the final `Value`.
/// Used both as the fallback path for typed decode failures and directly for
/// types this driver never attempts to decode through a native Rust type.
fn decode_via_raw_bytes(
    row: &postgres::Row,
    idx: usize,
    oid: u32,
    type_name: &str,
    decode: impl FnOnce(u32, &str, Option<&[u8]>) -> Value,
) -> Value {
    match row.try_get::<_, Option<RedshiftRawBytes>>(idx) {
        Ok(raw) => decode(
            oid,
            type_name,
            raw.as_ref().map(|RedshiftRawBytes(bytes)| bytes.as_slice()),
        ),
        Err(error) => {
            log::info!(
                "Unsupported Redshift type '{type_name}' (oid {oid}) at column index {idx}: {error}"
            );
            Value::Unsupported(type_name.to_string())
        }
    }
}

/// Decodes a single column of a `postgres::Row` into a core `Value`.
///
/// Known scalar types decode through their native Rust representation.
/// Anything else (enums, domains, Redshift's extended types) falls back to
/// [`decode_defensive_fallback`], which degrades to `Value::Unsupported`
/// instead of panicking. `NUMERIC`/`DECIMAL` gets its own decoder,
/// [`decode_numeric_fallback`], because `f64: FromSql` never accepts that
/// OID: it decodes the binary `NUMERIC` wire format directly into an exact
/// `Value::Decimal` string, which is required to avoid silently corrupting or
/// dropping every decimal value.
///
/// `idx` is always in-bounds: callers derive it from `0..columns.len()` where
/// `columns` was itself built from the same row's column list.
#[allow(clippy::indexing_slicing)]
fn redshift_value_to_value(row: &postgres::Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_();
    let type_name = col_type.name();
    let oid = col_type.oid();

    match type_name {
        "bool" => decode_typed_or_fallback::<bool>(row, idx, oid, type_name, Value::Bool),

        "int2" => {
            decode_typed_or_fallback::<i16>(row, idx, oid, type_name, |v| Value::Int(v as i64))
        }

        "int4" => {
            decode_typed_or_fallback::<i32>(row, idx, oid, type_name, |v| Value::Int(v as i64))
        }

        "int8" => decode_typed_or_fallback::<i64>(row, idx, oid, type_name, Value::Int),

        "float4" => {
            decode_typed_or_fallback::<f32>(row, idx, oid, type_name, |v| Value::Float(v as f64))
        }

        "float8" => decode_typed_or_fallback::<f64>(row, idx, oid, type_name, Value::Float),

        "numeric" => decode_via_raw_bytes(row, idx, oid, type_name, decode_numeric_fallback),

        "text" | "varchar" | "bpchar" | "name" => {
            decode_typed_or_fallback::<String>(row, idx, oid, type_name, Value::Text)
        }

        "date" => decode_typed_or_fallback::<NaiveDate>(row, idx, oid, type_name, Value::Date),

        "time" => decode_typed_or_fallback::<NaiveTime>(row, idx, oid, type_name, Value::Time),

        "timestamp" => {
            decode_typed_or_fallback::<NaiveDateTime>(row, idx, oid, type_name, |timestamp| {
                Value::DateTime(DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc))
            })
        }

        "timestamptz" => {
            decode_typed_or_fallback::<DateTime<Utc>>(row, idx, oid, type_name, Value::DateTime)
        }

        "inet" => decode_typed_or_fallback::<IpAddr>(row, idx, oid, type_name, |ip| {
            Value::Text(ip.to_string())
        }),

        "bytea" => decode_typed_or_fallback::<Vec<u8>>(row, idx, oid, type_name, Value::Bytes),

        _ => decode_via_raw_bytes(row, idx, oid, type_name, decode_defensive_fallback),
    }
}

#[cfg(test)]
mod tests {
    use super::{RedshiftConnectParams, RedshiftSslMode, build_base_config, ensure_read_only};
    use dbflux_core::DbError;
    use postgres::config::SslMode;
    use std::time::Duration;

    #[test]
    fn ssl_mode_parse_maps_every_libpq_value() {
        assert_eq!(RedshiftSslMode::parse("disable"), RedshiftSslMode::Disable);
        assert_eq!(RedshiftSslMode::parse("allow"), RedshiftSslMode::Prefer);
        assert_eq!(RedshiftSslMode::parse("prefer"), RedshiftSslMode::Prefer);
        assert_eq!(RedshiftSslMode::parse("require"), RedshiftSslMode::Require);
        assert_eq!(RedshiftSslMode::parse("verify-ca"), RedshiftSslMode::Verify);
        assert_eq!(
            RedshiftSslMode::parse("verify-full"),
            RedshiftSslMode::Verify
        );

        // Case-insensitive, and unknown/empty fall back to prefer.
        assert_eq!(RedshiftSslMode::parse("REQUIRE"), RedshiftSslMode::Require);
        assert_eq!(RedshiftSslMode::parse("bogus"), RedshiftSslMode::Prefer);
        assert_eq!(RedshiftSslMode::parse(""), RedshiftSslMode::Prefer);
    }

    #[test]
    fn special_characters_in_password_are_carried_opaquely() {
        // A password containing a space, a quote, a backslash, and a substring
        // that looks like another connection parameter would break a raw
        // `key=value` string; the builder must carry it verbatim.
        let password = "pa ss'wo\\rd sslmode=disable";

        let params = RedshiftConnectParams {
            host: "cluster.example.com",
            port: 5439,
            user: "awsuser",
            password,
            database: "dev",
            ssl_mode: "verify-full",
        };
        let ssl_mode = RedshiftSslMode::parse(params.ssl_mode);
        let config = build_base_config(&params, ssl_mode);

        assert_eq!(config.get_password(), Some(password.as_bytes()));

        // The `sslmode=disable` substring inside the password must not downgrade
        // the negotiated ssl mode: verify-full mandates TLS at the protocol
        // level (`Require`).
        assert_eq!(config.get_ssl_mode(), SslMode::Require);
        assert_eq!(config.get_dbname(), Some("dev"));
        assert_eq!(config.get_user(), Some("awsuser"));
    }

    #[test]
    fn build_base_config_applies_the_default_connect_timeout() {
        let params = RedshiftConnectParams {
            host: "cluster.example.com",
            port: 5439,
            user: "awsuser",
            password: "secret",
            database: "dev",
            ssl_mode: "require",
        };
        let config = build_base_config(&params, RedshiftSslMode::parse(params.ssl_mode));

        assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(30)));
    }

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

    #[test]
    fn select_into_is_rejected() {
        let result = ensure_read_only("SELECT a INTO t FROM x");
        assert!(matches!(result, Err(DbError::NotSupported(_))));
    }

    #[test]
    fn select_into_is_rejected_regardless_of_case_and_whitespace() {
        for sql in [
            "select a into t from x",
            "SELECT a\nINTO\tt FROM x",
            "  SELECT a INTO t FROM x  ",
        ] {
            let result = ensure_read_only(sql);
            assert!(
                matches!(result, Err(DbError::NotSupported(_))),
                "expected {sql:?} to be rejected"
            );
        }
    }

    #[test]
    fn select_into_as_column_alias_or_literal_is_not_a_false_positive() {
        for sql in [
            "SELECT a AS into_col FROM x",
            "SELECT 'into' FROM x",
            "SELECT /* into */ 1",
            "SELECT point_into FROM x",
        ] {
            assert!(
                ensure_read_only(sql).is_ok(),
                "expected {sql:?} to be allowed"
            );
        }
    }

    #[test]
    fn plain_select_and_cte_are_still_allowed() {
        assert!(ensure_read_only("SELECT 1").is_ok());
        assert!(ensure_read_only("WITH c AS (SELECT 1) SELECT * FROM c").is_ok());
    }

    #[test]
    fn cte_prefixed_select_into_is_rejected() {
        let result = ensure_read_only("WITH c AS (SELECT 1) SELECT a INTO t FROM c");
        assert!(matches!(result, Err(DbError::NotSupported(_))));
    }

    #[test]
    fn cte_without_into_is_allowed() {
        for sql in [
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "WITH c AS (SELECT 1) SELECT a AS into_col FROM c",
        ] {
            assert!(
                ensure_read_only(sql).is_ok(),
                "expected {sql:?} to be allowed"
            );
        }
    }

    #[test]
    fn multiple_statements_are_rejected() {
        // `EXPLAIN`/`SHOW`-led buffers are the load-bearing cases: the shared
        // classifier maps their leading keyword straight to `Metadata` without
        // a multi-statement check, so without this guard the trailing DROP/
        // DELETE would reach the wire. The `SELECT`-led cases are also rejected
        // (belt-and-suspenders with the classifier's own check).
        for sql in [
            "SELECT 1; DELETE FROM t",
            "SELECT 1; SELECT 2",
            "EXPLAIN SELECT 1; DROP TABLE t",
            "SHOW search_path; DELETE FROM t",
        ] {
            let Err(DbError::NotSupported(message)) = ensure_read_only(sql) else {
                panic!("expected {sql:?} to be rejected as NotSupported");
            };
            assert!(
                message.contains("multiple statements"),
                "expected {sql:?} to be rejected by the multi-statement guard, got: {message}"
            );
        }
    }

    #[test]
    fn single_statement_with_trailing_semicolon_is_allowed() {
        for sql in [
            "SELECT 1;",
            "SELECT * FROM users ;",
            "WITH c AS (SELECT 1) SELECT * FROM c;",
        ] {
            assert!(
                ensure_read_only(sql).is_ok(),
                "expected {sql:?} to be allowed"
            );
        }
    }

    #[test]
    fn semicolon_inside_string_or_comment_is_not_a_statement_separator() {
        for sql in ["SELECT ';'", "SELECT 1 -- a; b", "SELECT 1 /* a; b */"] {
            assert!(
                ensure_read_only(sql).is_ok(),
                "expected {sql:?} to be allowed"
            );
        }
    }
}
