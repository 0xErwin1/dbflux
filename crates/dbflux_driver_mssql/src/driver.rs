use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_core::QueryGenerator;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    ColumnInfo, ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionExt, ConnectionProfile,
    ConstraintInfo, ConstraintKind, CrudResult, CustomTypeInfo, CustomTypeKind, DatabaseCategory,
    DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DdlCapabilities,
    DeploymentClass, DescribeRequest, DocumentConnection, DriverCapabilities, DriverFormDef,
    DriverLimits, DriverMetadata, ExplainRequest, ForeignKeyBuilder, ForeignKeyInfo, FormValues,
    FormattedError, Icon, IndexData, IndexInfo, IsolationLevel, KeyValueConnection,
    MutationCapabilities, OrderByColumn, PaginationStyle, PlaceholderStyle, QueryCancelHandle,
    QueryCapabilities, QueryErrorFormatter, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    RecordIdentity, RelationalConnection, RelationalSchema, Row, RowDelete, RowInsert, RowPatch,
    SQLSERVER_FORM, SchemaFeatures, SchemaForeignKeyBuilder, SchemaForeignKeyInfo,
    SchemaIndexBuilder, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SortDirection,
    SqlDialect, SqlMutationGenerator, SshTunnelConfig, SyntaxInfo, TableBrowseRequest,
    TableCountRequest, TableInfo, TransactionCapabilities, Value, ViewInfo, WhereOperator,
    generate_delete_template, generate_drop_table, generate_insert_template, generate_select_star,
    generate_truncate, generate_update_template, render_semantic_filter_sql, sanitize_uri,
};
use dbflux_ssh::SshTunnel;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, SqlBrowser};
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type TiberiusClient = Client<Compat<TcpStream>>;

/// SQL Server driver metadata.
pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "mssql".into(),
    display_name: "SQL Server".into(),
    description: "Microsoft SQL Server relational database".into(),
    category: DatabaseCategory::Relational,
    deployment_class: Some(DeploymentClass::SelfHosted),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::RELATIONAL_BASE.bits()
            | DriverCapabilities::SCHEMAS.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::FOREIGN_KEYS.bits()
            | DriverCapabilities::CHECK_CONSTRAINTS.bits()
            | DriverCapabilities::UNIQUE_CONSTRAINTS.bits()
            | DriverCapabilities::TRANSACTIONAL_DDL.bits(),
    ),
    default_port: Some(1433),
    uri_scheme: "sqlserver".into(),
    icon: Icon::Database,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::QuestionMark,
        supports_schemas: true,
        default_schema: Some("dbo".to_string()),
        case_sensitive_identifiers: false,
    }),
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Offset],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::Like,
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: false,
        max_query_parameters: 2100,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: false,
        supports_returning: true,
        supports_batch: true,
        supports_bulk_update: true,
        supports_bulk_delete: true,
        max_insert_values: 1000,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: true,
        supports_drop_database: true,
        supports_create_table: true,
        supports_drop_table: true,
        supports_alter_table: true,
        supports_create_index: true,
        supports_drop_index: true,
        supports_create_view: true,
        supports_drop_view: true,
        supports_create_trigger: true,
        supports_drop_trigger: true,
        transactional_ddl: true,
        supports_add_column: true,
        supports_drop_column: true,
        supports_rename_column: false,
        supports_alter_column: true,
        supports_add_constraint: true,
        supports_drop_constraint: true,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
            IsolationLevel::Snapshot,
        ],
        default_isolation_level: Some(IsolationLevel::ReadCommitted),
        supports_savepoints: true,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: Some(DriverLimits {
        max_query_length: 0,
        max_parameters: 2100,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 32,
        max_identifier_length: 128,
        max_columns: 1024,
        max_indexes_per_table: 999,
    }),
    ssl_modes: Some(&[
        dbflux_core::SslModeOption {
            id: "off",
            label: "off",
        },
        dbflux_core::SslModeOption {
            id: "on",
            label: "on (accept self-signed)",
        },
        dbflux_core::SslModeOption {
            id: "required",
            label: "required (validate cert)",
        },
    ]),
    ssl_cert_fields: Some(dbflux_core::SslCertFields {
        root_cert: true,
        client_cert: false,
    }),
    classification_override: None,
});

// =============================================================================
// SQL Server SQL dialect
// =============================================================================

pub struct MssqlDialect;

static MSSQL_DIALECT: MssqlDialect = MssqlDialect;

impl SqlDialect for MssqlDialect {
    fn quote_identifier(&self, name: &str) -> String {
        // SQL Server uses [bracket] quoting. Any closing bracket inside the
        // identifier must be doubled to be safe.
        let escaped = name.replace(']', "]]");
        format!("[{}]", escaped)
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        value_to_mssql_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }

    fn supports_returning(&self) -> bool {
        // SQL Server returns affected rows via the OUTPUT clause rather than
        // RETURNING. The driver's CRUD methods build the OUTPUT clause
        // directly; SqlQueryBuilder's RETURNING path is bypassed.
        true
    }

    fn build_upsert_statement(
        &self,
        _schema: Option<&str>,
        _table: &str,
        _columns: &[String],
        _values: &[Value],
        _conflict_columns: &[String],
        _update_assignments: &[(String, Value)],
    ) -> Option<String> {
        // SQL Server uses MERGE for upsert; not generated here to avoid producing
        // an incorrect template. Use UPDATE/INSERT separately.
        None
    }
}

fn value_to_mssql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            // SQL Server BIT uses 0/1.
            if *b { "1" } else { "0" }.to_string()
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                "NULL".to_string()
            } else {
                f.to_string()
            }
        }
        Value::Text(s) => format!("N'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("0x{}", hex)
        }
        Value::Json(s) => format!("N'{}'", s.replace('\'', "''")),
        Value::Decimal(s) => s.clone(),
        Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S%.f")),
        Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        Value::Time(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
        Value::Array(_) | Value::Document(_) | Value::ObjectId(_) => {
            let json = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            format!("N'{}'", json.replace('\'', "''"))
        }
        Value::Unsupported(_) => "NULL".to_string(),
    }
}

// =============================================================================
// MssqlDriver
// =============================================================================

pub struct MssqlDriver;

impl MssqlDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MssqlDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for MssqlDriver {
    fn kind(&self) -> DbKind {
        DbKind::SqlServer
    }

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:mssql".into()
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_mssql_config(&profile.config)?;

        let password = password.map(|value| value.expose_secret());
        let ssh_secret = ssh_secret.map(|value| value.expose_secret());

        if config.use_uri {
            return self.connect_with_uri(config.uri.as_deref().unwrap_or(""), password);
        }

        if let Some(tunnel_config) = &config.ssh_tunnel {
            self.connect_via_ssh_tunnel(tunnel_config, ssh_secret, &config, password)
        } else {
            self.connect_direct(&config, password)
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &SQLSERVER_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let use_uri = values.get("use_uri").map(|s| s == "true").unwrap_or(false);
        let uri = values.get("uri").filter(|s| !s.is_empty()).cloned();

        if use_uri {
            if uri.is_none() {
                return Err(DbError::InvalidProfile(
                    "Connection URI is required when using URI mode".to_string(),
                ));
            }

            return Ok(DbConfig::SqlServer {
                use_uri: true,
                uri,
                host: String::new(),
                port: 1433,
                user: String::new(),
                database: None,
                instance: None,
                ssl_mode: Some("on".to_string()),
                trust_server_certificate: true,
                ssl_root_cert_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            });
        }

        let host = values
            .get("host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Host is required".to_string()))?
            .clone();

        let port: u16 = values
            .get("port")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Port is required".to_string()))?
            .parse()
            .map_err(|_| DbError::InvalidProfile("Invalid port number".to_string()))?;

        let user = values
            .get("user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("User is required".to_string()))?
            .clone();

        let database = values.get("database").filter(|s| !s.is_empty()).cloned();
        let instance = values.get("instance").filter(|s| !s.is_empty()).cloned();

        // SSL Mode is the single user-facing knob. The trust-cert flag is
        // derived from it so the UI doesn't need a second checkbox:
        //   off      -> no encryption (trust is irrelevant)
        //   on       -> encrypted, accept self-signed certs (dev/corporate)
        //   required -> encrypted, validate cert chain (production CA cert)
        let ssl_mode_value = values
            .get("ssl_mode")
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| "on".to_string());
        let trust_server_certificate = !matches!(ssl_mode_value.as_str(), "required");

        Ok(DbConfig::SqlServer {
            use_uri: false,
            uri: None,
            host,
            port,
            user,
            database,
            instance,
            ssl_mode: Some(ssl_mode_value),
            trust_server_certificate,
            ssl_root_cert_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::SqlServer {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            instance,
            ..
        } = config
        {
            values.insert(
                "use_uri".to_string(),
                if *use_uri { "true" } else { "" }.to_string(),
            );
            values.insert("uri".to_string(), uri.clone().unwrap_or_default());
            values.insert("host".to_string(), host.clone());
            values.insert("port".to_string(), port.to_string());
            values.insert("user".to_string(), user.clone());
            values.insert("database".to_string(), database.clone().unwrap_or_default());
            values.insert("instance".to_string(), instance.clone().unwrap_or_default());
        }

        values
    }

    fn build_uri(&self, values: &FormValues, password: &str) -> Option<String> {
        let host = values.get("host").map(|s| s.as_str()).unwrap_or("");
        let port = values.get("port").map(|s| s.as_str()).unwrap_or("1433");
        let user = values.get("user").map(|s| s.as_str()).unwrap_or("");
        let database = values.get("database").map(|s| s.as_str()).unwrap_or("");
        let instance = values.get("instance").map(|s| s.as_str()).unwrap_or("");

        let credentials = if !user.is_empty() {
            if !password.is_empty() {
                format!(
                    "{}:{}@",
                    urlencoding::encode(user),
                    urlencoding::encode(password)
                )
            } else {
                format!("{}@", urlencoding::encode(user))
            }
        } else {
            String::new()
        };

        let db_part = if !database.is_empty() {
            format!("/{}", database)
        } else {
            String::new()
        };

        let query_part = if !instance.is_empty() {
            // Database segment is required before a query string; emit an
            // empty one if the user did not pick a default database.
            let separator = if db_part.is_empty() { "/" } else { "" };
            format!("{}?instance={}", separator, urlencoding::encode(instance))
        } else {
            String::new()
        };

        Some(format!(
            "sqlserver://{}{}:{}{}{}",
            credentials, host, port, db_part, query_part
        ))
    }

    fn parse_uri(&self, uri: &str) -> Option<FormValues> {
        let stripped = uri
            .strip_prefix("sqlserver://")
            .or_else(|| uri.strip_prefix("mssql://"))?;

        let mut values = HashMap::new();
        let (credentials, host_part) = if let Some(at_pos) = stripped.rfind('@') {
            (&stripped[..at_pos], &stripped[at_pos + 1..])
        } else {
            ("", stripped)
        };

        if !credentials.is_empty() {
            if let Some(colon) = credentials.find(':') {
                let user = urlencoding::decode(&credentials[..colon])
                    .unwrap_or_default()
                    .into_owned();
                values.insert("user".to_string(), user);
                let password = urlencoding::decode(&credentials[colon + 1..])
                    .unwrap_or_default()
                    .into_owned();
                if !password.is_empty() {
                    values.insert("password".to_string(), password);
                }
            } else {
                let user = urlencoding::decode(credentials)
                    .unwrap_or_default()
                    .into_owned();
                values.insert("user".to_string(), user);
            }
        }

        let (host_port, after_host) = if let Some(slash) = host_part.find('/') {
            (&host_part[..slash], &host_part[slash + 1..])
        } else {
            (host_part, "")
        };

        let (database, query_string) = match after_host.split_once('?') {
            Some((db, q)) => (db, q),
            None => (after_host, ""),
        };
        values.insert("database".to_string(), database.to_string());

        // Split host_port into host[\instance][:port]. SSMS-style
        // `host\instance` plus an optional `:port` suffix.
        let (host_and_instance, port_str) = match host_port.rfind(':') {
            Some(colon) => (&host_port[..colon], &host_port[colon + 1..]),
            None => (host_port, "1433"),
        };
        let (host, instance) = match host_and_instance.find('\\') {
            Some(bs) => (&host_and_instance[..bs], &host_and_instance[bs + 1..]),
            None => (host_and_instance, ""),
        };
        values.insert("host".to_string(), host.to_string());
        values.insert("port".to_string(), port_str.to_string());
        if !instance.is_empty() {
            values.insert("instance".to_string(), instance.to_string());
        }

        // ?instance=… overrides the backslash form if both are present.
        for pair in query_string.split('&').filter(|p| !p.is_empty()) {
            if let Some((key, value)) = pair.split_once('=')
                && key.eq_ignore_ascii_case("instance")
                && !value.is_empty()
            {
                let decoded = urlencoding::decode(value)
                    .map(|cow| cow.into_owned())
                    .unwrap_or_else(|_| value.to_string());
                values.insert("instance".to_string(), decoded);
            }
        }

        Some(values)
    }

    fn with_database(&self, config: &DbConfig, database: &str) -> Option<DbConfig> {
        match config {
            DbConfig::SqlServer {
                use_uri,
                uri,
                host,
                port,
                user,
                instance,
                ssl_mode,
                trust_server_certificate,
                ssl_root_cert_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => Some(DbConfig::SqlServer {
                use_uri: *use_uri,
                uri: uri.clone(),
                host: host.clone(),
                port: *port,
                user: user.clone(),
                database: Some(database.to_string()),
                instance: instance.clone(),
                ssl_mode: ssl_mode.clone(),
                trust_server_certificate: *trust_server_certificate,
                ssl_root_cert_path: ssl_root_cert_path.clone(),
                ssh_tunnel: ssh_tunnel.clone(),
                ssh_tunnel_profile_id: *ssh_tunnel_profile_id,
            }),
            _ => None,
        }
    }
}

struct ExtractedMssqlConfig {
    use_uri: bool,
    uri: Option<String>,
    host: String,
    port: u16,
    user: String,
    database: Option<String>,
    instance: Option<String>,
    ssl_mode: String,
    trust_server_certificate: bool,
    ssh_tunnel: Option<SshTunnelConfig>,
}

fn extract_mssql_config(config: &DbConfig) -> Result<ExtractedMssqlConfig, DbError> {
    match config {
        DbConfig::SqlServer {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            instance,
            ssl_mode,
            trust_server_certificate,
            ssh_tunnel,
            ..
        } => Ok(ExtractedMssqlConfig {
            use_uri: *use_uri,
            uri: uri.clone(),
            host: host.clone(),
            port: *port,
            user: user.clone(),
            database: database.clone(),
            instance: instance.clone(),
            ssl_mode: ssl_mode.clone().unwrap_or_else(|| "on".to_string()),
            trust_server_certificate: *trust_server_certificate,
            ssh_tunnel: ssh_tunnel.clone(),
        }),
        _ => Err(DbError::InvalidProfile(
            "Expected SQL Server configuration".to_string(),
        )),
    }
}

fn encryption_for(ssl_mode: &str) -> EncryptionLevel {
    match ssl_mode {
        "off" | "disabled" => EncryptionLevel::Off,
        "required" => EncryptionLevel::Required,
        // "on" or unknown
        _ => EncryptionLevel::On,
    }
}

fn build_tiberius_config(params: &MssqlConnectParams) -> Config {
    let mut config = Config::new();
    config.host(params.host);

    if let Some(instance) = params.instance {
        // Leave port unset so tiberius defaults to 1434 for the SQL Browser
        // SSRP lookup; Browser then returns the instance's actual TCP port.
        // Calling `config.port(...)` here would direct the Browser query at
        // the wrong UDP port and produce a connection reset (os error 10054).
        config.instance_name(instance);
    } else {
        config.port(params.port);
    }

    if let Some(database) = params.database
        && !database.is_empty()
    {
        config.database(database);
    }

    config.authentication(AuthMethod::sql_server(params.user, params.password));
    config.encryption(encryption_for(params.ssl_mode));

    if params.trust_server_certificate {
        config.trust_cert();
    }

    config
}

struct MssqlConnectParams<'a> {
    host: &'a str,
    port: u16,
    user: &'a str,
    password: &'a str,
    database: Option<&'a str>,
    instance: Option<&'a str>,
    ssl_mode: &'a str,
    trust_server_certificate: bool,
}

fn build_runtime() -> Result<Runtime, DbError> {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            DbError::ConnectionFailed(format!("Failed to start tokio runtime: {}", e).into())
        })
}

/// Short, non-reversible fingerprint of a password for diagnostic logs.
///
/// Uses SHA-256 with a fixed seed string and returns the leading 16 hex chars
/// of the digest. The point is to let users compare two values for equality
/// (e.g. "did the password reaching tiberius differ from what I typed?")
/// without ever exposing the password itself.
fn password_fingerprint(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"dbflux-mssql-password-fingerprint:v1");
    hasher.update(password.as_bytes());
    let digest = hasher.finalize();
    // SHA-256 always produces a 32-byte digest, so `get(..8)` is infallible —
    // the `unwrap_or_default` is just to keep clippy's indexing_slicing lint
    // happy without an `expect`.
    hex::encode(digest.get(..8).unwrap_or_default())
}

async fn establish_tiberius(config: Config) -> Result<TiberiusClient, tiberius::error::Error> {
    // `connect_named` falls back to a direct `host:port` connect when no
    // instance is set, and performs a SQL Browser (UDP 1434) lookup when
    // one is. Using it unconditionally keeps the two paths in one place.
    let tcp = TcpStream::connect_named(&config).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

/// Fetch the server-side session id (`@@SPID`) for the open client.
///
/// `@@SPID` returns a SQL `smallint`, so we widen to `i32` for the rest of
/// the driver. Returns `0` on any decoding failure rather than propagating
/// — a missing SPID just means cancel becomes a no-op, which is fine.
async fn capture_spid(client: &mut TiberiusClient) -> Result<i32, tiberius::error::Error> {
    let stream = client.simple_query("SELECT @@SPID").await?;
    let row = stream.into_row().await?;
    Ok(row
        .and_then(|r| r.get::<i16, _>(0))
        .map(|s| s as i32)
        .unwrap_or(0))
}

impl MssqlDriver {
    fn connect_with_uri(
        &self,
        base_uri: &str,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let uri = inject_password_into_mssql_uri(base_uri, password);
        log::info!("Connecting to SQL Server via URI {}", sanitize_uri(&uri));
        if let Some(pw) = password {
            log::debug!(
                "URI auth payload — password_chars: {}, password_bytes: {}, password_fingerprint: {}",
                pw.chars().count(),
                pw.len(),
                password_fingerprint(pw),
            );
        }

        // Prefer our own URL parser when the URI looks like one
        // (`sqlserver://` / `mssql://`). Tiberius's ADO parser is too
        // permissive: it accepts our URL as a single key=value pair
        // (key=`sqlserver://sa:pw@host/?instance`, value=`NAME`) and
        // returns a Config with empty host/auth that then dials a
        // default `localhost:1433`. ADO/JDBC parsers stay available for
        // genuine `Server=…;` / `jdbc:sqlserver://…` connection strings.
        let is_mssql_url = uri.starts_with("sqlserver://") || uri.starts_with("mssql://");
        let config = if is_mssql_url {
            parse_mssql_url(&uri).map_err(|e| format_mssql_uri_error(&e, base_uri))?
        } else {
            Config::from_ado_string(&uri)
                .or_else(|_| Config::from_jdbc_string(&uri))
                .map_err(|e| format_mssql_uri_error(&e, base_uri))?
        };

        let reconnect_config = config.clone();
        let runtime = build_runtime()?;

        let (client, spid) = runtime
            .block_on(async move {
                let mut client = establish_tiberius(config).await?;
                let spid = capture_spid(&mut client).await?;
                Ok::<_, tiberius::error::Error>((client, spid))
            })
            .map_err(|e| format_mssql_uri_error(&e, base_uri))?;

        log::info!(
            "[CONNECT] SQL Server connection established via URI (spid: {})",
            spid
        );

        Ok(Box::new(MssqlConnection {
            inner: Arc::new(Mutex::new(MssqlConnectionInner {
                client: Some(client),
                runtime,
            })),
            current_database: Mutex::new(None),
            ssh_tunnel: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            spid: Arc::new(AtomicI32::new(spid)),
            reconnect_config: Arc::new(reconnect_config),
            poisoned: Arc::new(AtomicBool::new(false)),
        }))
    }

    fn connect_direct(
        &self,
        config: &ExtractedMssqlConfig,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        log::info!(
            "Connecting directly to SQL Server at {}:{} as {} (database: {:?}, instance: {:?}, ssl_mode: {})",
            config.host,
            config.port,
            config.user,
            config.database,
            config.instance,
            config.ssl_mode
        );

        let resolved_password = password.unwrap_or("");
        log::debug!(
            "Auth payload — user_len: {}, password_chars: {}, password_bytes: {}, password_fingerprint: {}",
            config.user.chars().count(),
            resolved_password.chars().count(),
            resolved_password.len(),
            password_fingerprint(resolved_password),
        );

        let tiberius_config = build_tiberius_config(&MssqlConnectParams {
            host: &config.host,
            port: config.port,
            user: &config.user,
            password: resolved_password,
            database: config.database.as_deref(),
            instance: config.instance.as_deref(),
            ssl_mode: &config.ssl_mode,
            trust_server_certificate: config.trust_server_certificate,
        });

        let established = establish_mssql_session(tiberius_config, &config.host, config.port)?;

        log::info!(
            "Successfully connected to {}:{} (spid: {})",
            config.host,
            config.port,
            established.spid
        );

        Ok(Box::new(build_mssql_connection(
            established,
            config.database.clone(),
            None,
        )))
    }

    fn connect_via_ssh_tunnel(
        &self,
        tunnel_config: &SshTunnelConfig,
        ssh_secret: Option<&str>,
        config: &ExtractedMssqlConfig,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let total_start = Instant::now();

        log::info!(
            "[CONNECT] Starting SSH tunnel connection: {}@{}:{} -> {}:{}",
            tunnel_config.user,
            tunnel_config.host,
            tunnel_config.port,
            config.host,
            config.port
        );

        let ssh_session = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        let tunnel = SshTunnel::start(ssh_session, config.host.clone(), config.port)?;
        let local_port = tunnel.local_port();

        log::info!(
            "[SSH] Tunnel ready on 127.0.0.1:{} in {:.2}ms",
            local_port,
            total_start.elapsed().as_secs_f64() * 1000.0
        );

        let tiberius_config = build_tiberius_config(&MssqlConnectParams {
            host: "127.0.0.1",
            port: local_port,
            user: &config.user,
            password: password.unwrap_or(""),
            database: config.database.as_deref(),
            // The SSH tunnel forwards to a TCP port, so we cannot use a named
            // instance lookup via the SQL Browser service.
            instance: None,
            ssl_mode: &config.ssl_mode,
            trust_server_certificate: config.trust_server_certificate,
        });

        // The reconnect config points at the same 127.0.0.1:<local_port> that
        // the SSH tunnel owns. As long as the `SshTunnel` value lives on
        // `MssqlConnection`, the tunnel stays open and a fresh connection
        // (for KILL or for reconnect) can reuse it.
        let established = establish_mssql_session(tiberius_config, &config.host, config.port)?;

        log::info!(
            "[CONNECT] Total connection time: {:.2}ms ({}:{} via SSH {}, spid: {})",
            total_start.elapsed().as_secs_f64() * 1000.0,
            config.host,
            config.port,
            tunnel_config.host,
            established.spid
        );

        Ok(Box::new(build_mssql_connection(
            established,
            config.database.clone(),
            Some(tunnel),
        )))
    }
}

/// Bundle of pieces produced by `establish_mssql_session`.
struct EstablishedMssqlSession {
    client: TiberiusClient,
    runtime: Runtime,
    spid: i32,
    reconnect_config: tiberius::Config,
}

/// Build a tokio runtime, open the tiberius client, and capture `@@SPID`.
///
/// Shared by `connect_direct` and `connect_via_ssh_tunnel` so the
/// runtime/client/spid plumbing lives in one place. `host`/`port` are only
/// used to format a meaningful error if the dial fails.
fn establish_mssql_session(
    tiberius_config: Config,
    host: &str,
    port: u16,
) -> Result<EstablishedMssqlSession, DbError> {
    let reconnect_config = tiberius_config.clone();
    let runtime = build_runtime()?;
    let (client, spid) = runtime
        .block_on(async move {
            let mut client = establish_tiberius(tiberius_config).await?;
            let spid = capture_spid(&mut client).await?;
            Ok::<_, tiberius::error::Error>((client, spid))
        })
        .map_err(|e| format_mssql_connect_error(&e, host, port))?;
    Ok(EstablishedMssqlSession {
        client,
        runtime,
        spid,
        reconnect_config,
    })
}

fn build_mssql_connection(
    session: EstablishedMssqlSession,
    current_database: Option<String>,
    ssh_tunnel: Option<SshTunnel>,
) -> MssqlConnection {
    MssqlConnection {
        inner: Arc::new(Mutex::new(MssqlConnectionInner {
            client: Some(session.client),
            runtime: session.runtime,
        })),
        current_database: Mutex::new(current_database),
        ssh_tunnel,
        cancelled: Arc::new(AtomicBool::new(false)),
        spid: Arc::new(AtomicI32::new(session.spid)),
        reconnect_config: Arc::new(session.reconnect_config),
        poisoned: Arc::new(AtomicBool::new(false)),
    }
}

// =============================================================================
// MssqlConnection
// =============================================================================

struct MssqlConnectionInner {
    client: Option<TiberiusClient>,
    runtime: Runtime,
}

pub struct MssqlConnection {
    inner: Arc<Mutex<MssqlConnectionInner>>,
    current_database: Mutex<Option<String>>,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
    cancelled: Arc<AtomicBool>,

    // Cancellation state. `spid` is the server-side session id captured at
    // connect time (and refreshed after each reconnect). `reconnect_config`
    // is a cloneable tiberius Config we can use to (a) open a side-channel
    // connection to send `KILL <spid>` and (b) rebuild the primary client
    // after the session is killed. `poisoned` is set true once KILL has been
    // sent so `cleanup_after_cancel` knows it needs to reconnect.
    spid: Arc<AtomicI32>,
    reconnect_config: Arc<tiberius::Config>,
    poisoned: Arc<AtomicBool>,
}

struct MssqlCancelHandle {
    cancelled: Arc<AtomicBool>,
    spid: Arc<AtomicI32>,
    reconnect_config: Arc<tiberius::Config>,
    poisoned: Arc<AtomicBool>,
}

impl QueryCancelHandle for MssqlCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        // Mark cancellation requested first so `execute()` can translate the
        // server's "session was killed" error to `DbError::Cancelled` even
        // if the KILL itself races with the query completing normally.
        self.cancelled.store(true, Ordering::SeqCst);

        let spid = self.spid.load(Ordering::SeqCst);
        if spid == 0 {
            // No session id captured yet — nothing meaningful to kill. Mark
            // the connection poisoned so the next `execute()` runs
            // `cleanup_after_cancel`, which clears both flags. Without this
            // the `cancelled` flag stays set forever and every subsequent
            // query short-circuits to `DbError::Cancelled`.
            self.poisoned.store(true, Ordering::SeqCst);
            return Ok(());
        }

        // Use a fresh single-threaded runtime for the kill round-trip. We
        // must never share the runtime that's currently driving the
        // in-flight query, since blocking on it would deadlock.
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| DbError::query_failed(format!("Failed to start KILL runtime: {}", e)))?;

        let config = (*self.reconnect_config).clone();
        let result: Result<(), tiberius::error::Error> = rt.block_on(async move {
            let tcp = TcpStream::connect_named(&config).await?;
            tcp.set_nodelay(true)?;
            let mut killer = Client::connect(config, tcp.compat_write()).await?;
            killer
                .simple_query(format!("KILL {}", spid))
                .await?
                .into_results()
                .await?;
            Ok(())
        });

        // Mark as poisoned regardless of whether KILL succeeded: if it failed
        // the primary connection may still be in an unknown state and
        // forcing a reconnect on next use is safer than reusing it.
        self.poisoned.store(true, Ordering::SeqCst);

        match result {
            Ok(()) => {
                log::info!("[CANCEL] KILL {} sent successfully", spid);
                Ok(())
            }
            Err(err) => {
                log::error!("[CANCEL] KILL {} failed: {}", spid, err);
                Err(format_mssql_query_error(&err))
            }
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl MssqlConnection {
    fn with_client<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Runtime, &mut TiberiusClient) -> Result<R, DbError>,
    {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poison_err) => poison_err.into_inner(),
        };

        let inner = &mut *guard;
        let client = inner.client.as_mut().ok_or_else(|| {
            DbError::ConnectionFailed("SQL Server connection has been closed".to_string().into())
        })?;

        f(&inner.runtime, client)
    }

    fn execute_simple(&self, sql: &str) -> Result<QueryResult, DbError> {
        let start = Instant::now();
        let sql_owned = sql.to_string();

        // Multi-statement batches in SQL Server can produce multiple result
        // sets (e.g. `SELECT 1; SELECT 2;` or a stored procedure with
        // several `SELECT`s). We return the LAST non-empty set as the
        // primary `QueryResult` (preserving the historical "last statement
        // wins" UX) and attach every earlier non-empty set to
        // `additional_results` in batch order, so callers that want the
        // full batch can walk it via `QueryResult::iter_result_sets()`.
        // Pure preparation batches (`SET LOCK_TIMEOUT 5000`) produce no
        // result sets and surface as an empty primary, which is what
        // callers already expect.
        let collected = self.with_client(|runtime, client| {
            runtime.block_on(async move {
                let stream = client
                    .simple_query(sql_owned)
                    .await
                    .map_err(|e| format_mssql_query_error(&e))?;

                let result_sets = stream
                    .into_results()
                    .await
                    .map_err(|e| format_mssql_query_error(&e))?;

                Ok::<_, DbError>(convert_result_sets(result_sets))
            })
        })?;

        let total_time = start.elapsed();
        let result = build_multi_result(collected, total_time);

        log::debug!(
            "[QUERY] Completed in {:.2}ms, primary={}r/{}c, additional_sets={}",
            total_time.as_secs_f64() * 1000.0,
            result.rows.len(),
            result.columns.len(),
            result.additional_results.len()
        );

        Ok(result)
    }
}

/// Convert tiberius's raw result sets into `(columns, rows)` pairs, dropping
/// any empty sets so the multi-set splitter sees only meaningful output.
fn convert_result_sets(result_sets: Vec<Vec<tiberius::Row>>) -> Vec<(Vec<ColumnMeta>, Vec<Row>)> {
    result_sets
        .into_iter()
        .filter(|set| !set.is_empty())
        .map(convert_single_result_set)
        .collect()
}

fn convert_single_result_set(set: Vec<tiberius::Row>) -> (Vec<ColumnMeta>, Vec<Row>) {
    let columns = set
        .first()
        .map(|row| row.columns().iter().map(tiberius_column_to_meta).collect())
        .unwrap_or_default();

    let rows: Vec<Row> = set
        .iter()
        .map(|row| {
            (0..row.columns().len())
                .map(|idx| tiberius_value_to_value(row, idx))
                .collect::<Row>()
        })
        .collect();

    (columns, rows)
}

fn tiberius_column_to_meta(column: &tiberius::Column) -> ColumnMeta {
    ColumnMeta {
        name: column.name().to_string(),
        type_name: format!("{:?}", column.column_type()),
        kind: tiberius_column_to_kind(column.column_type()),
        nullable: true,
        is_primary_key: false,
    }
}

/// Splits a list of non-empty result sets into a primary `QueryResult` plus
/// additional sets, mirroring the multi-result-set contract on
/// `QueryResult::additional_results`.
///
/// The LAST non-empty set becomes the primary (this preserves the historical
/// "last statement wins" UX for `SELECT 1; SELECT 2;` style batches), and
/// every earlier non-empty set is attached to `additional_results` in batch
/// order. Empty input yields an empty primary.
///
/// Factored out of `execute_simple` so it can be unit-tested without a live
/// SQL Server.
fn build_multi_result(
    mut collected: Vec<(Vec<ColumnMeta>, Vec<Row>)>,
    total_time: std::time::Duration,
) -> QueryResult {
    let Some((primary_columns, primary_rows)) = collected.pop() else {
        return QueryResult::table(Vec::new(), Vec::new(), None, total_time);
    };

    let mut result = QueryResult::table(primary_columns, primary_rows, None, total_time);
    for (cols, rows) in collected {
        // Each additional set shares the same total batch duration;
        // tiberius doesn't expose per-statement timing.
        result.push_additional_result(QueryResult::table(cols, rows, None, total_time));
    }
    result
}

fn tiberius_value_to_value(row: &tiberius::Row, idx: usize) -> Value {
    use tiberius::ColumnData;

    // tiberius rows expose values as `ColumnData<'_>` via `row.cells()` /
    // `row.try_get`. We extract column data to detect typed nulls and avoid
    // panicking on type mismatches.
    let column = match row.columns().get(idx) {
        Some(col) => col,
        None => return Value::Null,
    };

    let cell: Option<&ColumnData<'static>> = row.cells().nth(idx).map(|(_, data)| data);
    let data = match cell {
        Some(d) => d,
        None => return Value::Null,
    };

    match data {
        ColumnData::U8(v) => v.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I16(v) => v.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I32(v) => v.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I64(v) => v.map(Value::Int).unwrap_or(Value::Null),
        ColumnData::F32(v) => v.map(|v| Value::Float(v as f64)).unwrap_or(Value::Null),
        ColumnData::F64(v) => v.map(Value::Float).unwrap_or(Value::Null),
        ColumnData::Bit(v) => v.map(Value::Bool).unwrap_or(Value::Null),
        ColumnData::String(v) => v
            .as_ref()
            .map(|s| Value::Text(s.to_string()))
            .unwrap_or(Value::Null),
        ColumnData::Guid(v) => v
            .as_ref()
            .map(|g| Value::Text(g.to_string()))
            .unwrap_or(Value::Null),
        ColumnData::Binary(v) => v
            .as_ref()
            .map(|b| Value::Bytes(b.to_vec()))
            .unwrap_or(Value::Null),
        ColumnData::Numeric(v) => v
            .as_ref()
            .map(|n| Value::Decimal(n.to_string()))
            .unwrap_or(Value::Null),
        ColumnData::Xml(v) => v
            .as_ref()
            .map(|x| Value::Text(x.to_string()))
            .unwrap_or(Value::Null),
        ColumnData::DateTime(_)
        | ColumnData::SmallDateTime(_)
        | ColumnData::Time(_)
        | ColumnData::Date(_)
        | ColumnData::DateTime2(_)
        | ColumnData::DateTimeOffset(_) => extract_temporal(row, idx, column.column_type()),
    }
}

/// Maps a tiberius `ColumnType` to the cross-driver `ColumnKind` the chart
/// engine and other consumers rely on.
///
/// Variants whose semantic category isn't representable (`Guid`, `Xml`, `Udt`,
/// `BigVarBin`/`BigBinary`/`Image`, `SSVariant`, `Null`) map to `Unknown` so
/// chart auto-detect excludes them rather than treating them as text.
fn tiberius_column_to_kind(column_type: tiberius::ColumnType) -> dbflux_core::ColumnKind {
    use dbflux_core::ColumnKind;
    use tiberius::ColumnType;

    match column_type {
        ColumnType::Bit | ColumnType::Bitn => ColumnKind::Integer,
        ColumnType::Int1
        | ColumnType::Int2
        | ColumnType::Int4
        | ColumnType::Int8
        | ColumnType::Intn => ColumnKind::Integer,
        ColumnType::Float4
        | ColumnType::Float8
        | ColumnType::Floatn
        | ColumnType::Money
        | ColumnType::Money4
        | ColumnType::Decimaln
        | ColumnType::Numericn => ColumnKind::Float,
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2
        | ColumnType::Daten
        | ColumnType::Timen
        | ColumnType::DatetimeOffsetn => ColumnKind::Timestamp,
        ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Text
        | ColumnType::NText => ColumnKind::Text,
        ColumnType::Null
        | ColumnType::Guid
        | ColumnType::Xml
        | ColumnType::Udt
        | ColumnType::BigVarBin
        | ColumnType::BigBinary
        | ColumnType::Image
        | ColumnType::SSVariant => ColumnKind::Unknown,
    }
}

fn extract_temporal(row: &tiberius::Row, idx: usize, _column_type: tiberius::ColumnType) -> Value {
    // tiberius exposes typed accessors for chrono types; we try them in order
    // of decreasing precision and fall back to a string representation.
    if let Ok(Some(v)) = row.try_get::<chrono::DateTime<Utc>, _>(idx) {
        return Value::DateTime(v);
    }

    if let Ok(Some(v)) = row.try_get::<NaiveDateTime, _>(idx) {
        return Value::DateTime(DateTime::<Utc>::from_naive_utc_and_offset(v, Utc));
    }

    if let Ok(Some(v)) = row.try_get::<NaiveDate, _>(idx) {
        return Value::Date(v);
    }

    if let Ok(Some(v)) = row.try_get::<NaiveTime, _>(idx) {
        return Value::Time(v);
    }

    Value::Null
}

impl Connection for MssqlConnection {
    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn language_service(&self) -> &dyn dbflux_core::LanguageService {
        // T-SQL has constructs that the shared tree-sitter-sequel parser
        // doesn't recognise (TOP, OUTPUT INSERTED, MERGE, CROSS APPLY,
        // WITH (NOLOCK), OFFSET ROWS FETCH NEXT, etc.). Returning the
        // T-SQL-aware service suppresses the noisy parse diagnostics
        // while keeping dangerous-query detection.
        &dbflux_core::TSqlLanguageService
    }

    fn ping(&self) -> Result<(), DbError> {
        self.execute_simple("SELECT 1").map(|_| ())
    }

    fn close(&mut self) -> Result<(), DbError> {
        if let Ok(mut guard) = self.inner.lock() {
            guard.client = None;
        }
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        // Do not blanket-reset `cancelled` here — that would race with a
        // `cancel()` that fires between two executions and silently drop the
        // signal. Instead, recover the connection if a previous cancel left
        // it poisoned (which is what `cleanup_after_cancel` is for) before
        // dispatching the new query.
        if self.poisoned.load(Ordering::SeqCst) {
            self.cleanup_after_cancel()?;
        }

        // Support explicit database override per query, mirroring postgres/mysql.
        if let Some(database) = req.database.as_deref() {
            self.set_active_database(Some(database))?;
        }

        // Slice by char boundary, not byte index: SQL with multi-byte UTF-8
        // (CJK identifiers, `N'…'` literals with non-ASCII, accented comments)
        // would panic on `&req.sql[..80]` when 80 falls mid-codepoint.
        let sql_preview = match req.sql.char_indices().nth(80) {
            Some((idx, _)) => format!("{}...", &req.sql[..idx]),
            None => req.sql.clone(),
        };
        log::debug!("[QUERY] Executing: {}", sql_preview.replace('\n', " "));

        match self.execute_simple(&req.sql) {
            Ok(result) => {
                if self.cancelled.load(Ordering::SeqCst) {
                    Err(DbError::Cancelled)
                } else {
                    Ok(result)
                }
            }
            Err(err) => {
                // After KILL, the server raises a session-terminated error
                // (596 / 233 / 6005) on the next read from the killed
                // connection. Map any such error to `Cancelled` when the user
                // actually requested cancellation, so the UI surfaces the
                // expected outcome instead of a confusing protocol error.
                if self.cancelled.load(Ordering::SeqCst) || is_kill_error(&err) {
                    Err(DbError::Cancelled)
                } else {
                    Err(err)
                }
            }
        }
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        self.cancel_handle().cancel()
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancel_handle().cancel()
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(MssqlCancelHandle {
            cancelled: self.cancelled.clone(),
            spid: self.spid.clone(),
            reconnect_config: self.reconnect_config.clone(),
            poisoned: self.poisoned.clone(),
        })
    }

    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        // Only reconnect if a previous `cancel()` poisoned the connection.
        // For ordinary errors (no KILL involved) the existing client is still
        // usable and this is a no-op.
        if !self.poisoned.load(Ordering::SeqCst) {
            self.cancelled.store(false, Ordering::SeqCst);
            return Ok(());
        }

        log::info!("[CLEANUP] Reconnecting SQL Server connection after KILL");

        let config = (*self.reconnect_config).clone();
        let active_database = self
            .current_database
            .lock()
            .ok()
            .and_then(|guard| guard.clone());

        let new_client_and_spid: Result<(TiberiusClient, i32), tiberius::error::Error> = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(poison_err) => poison_err.into_inner(),
            };

            // Drop the old client first so the underlying socket closes
            // promptly; tiberius does not always tear down on KILL alone.
            guard.client = None;

            guard.runtime.block_on(async move {
                let mut client = establish_tiberius(config).await?;
                let spid = capture_spid(&mut client).await?;
                Ok((client, spid))
            })
        };

        let (new_client, new_spid) = new_client_and_spid.map_err(|e| {
            log::error!("[CLEANUP] Reconnect failed: {}", e);
            // Address fields aren't worth reconstructing here — the message
            // from tiberius is already host-aware via Config::get_addr().
            format_mssql_connect_error(&e, "?", 0)
        })?;

        {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(poison_err) => poison_err.into_inner(),
            };
            guard.client = Some(new_client);
        }

        self.spid.store(new_spid, Ordering::SeqCst);

        // The new session starts in whatever the login defaults to. If the
        // user was previously on a specific database, restore that selection
        // before clearing the poisoned flag — otherwise a failing `USE [db]`
        // would leave the connection alive on the login's default database
        // while callers (with `poisoned == false`) assume recovery succeeded
        // on the requested one. With the clear deferred, a USE failure leaves
        // `poisoned == true`, so the next `execute()` triggers another
        // recovery attempt instead of silently running on the wrong DB.
        if let Some(db) = active_database {
            let escaped = db.replace(']', "]]");
            self.execute_simple(&format!("USE [{}]", escaped))?;
        }

        self.poisoned.store(false, Ordering::SeqCst);
        self.cancelled.store(false, Ordering::SeqCst);

        log::info!("[CLEANUP] Reconnect complete, new SPID = {}", new_spid);
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let total_start = Instant::now();
        log::info!("[SCHEMA] Starting schema fetch");

        let databases = self.list_databases()?;
        let current_database = self
            .current_database
            .lock()
            .ok()
            .and_then(|guard| guard.clone());

        log::info!(
            "[SCHEMA] Fetched {} databases in {:.2}ms",
            databases.len(),
            total_start.elapsed().as_secs_f64() * 1000.0
        );

        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases,
            current_database,
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        // Hide built-in system databases.
        let sql = "SELECT name FROM sys.databases \
                   WHERE name NOT IN ('master','tempdb','model','msdb') \
                   ORDER BY name";

        let result = self.execute_simple(sql)?;
        let current = self
            .current_database
            .lock()
            .ok()
            .and_then(|guard| guard.clone());

        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                row.into_iter().next().and_then(|v| match v {
                    Value::Text(s) => Some(s),
                    _ => None,
                })
            })
            .map(|name| DatabaseInfo {
                is_current: current.as_ref() == Some(&name),
                name,
            })
            .collect())
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        log::info!("[SCHEMA] Fetching schema for database: {}", database);

        let escaped_db = database.replace(']', "]]");
        let qualified = format!("[{}]", escaped_db);

        // Shallow table list scoped to the requested database.
        let tables_sql = format!(
            "SELECT s.name AS schema_name, t.name AS table_name \
             FROM {qualified}.sys.tables t \
             JOIN {qualified}.sys.schemas s ON s.schema_id = t.schema_id \
             WHERE t.is_ms_shipped = 0 \
             ORDER BY s.name, t.name",
            qualified = qualified
        );

        let table_rows = self.execute_simple(&tables_sql)?;
        let mut tables = Vec::new();

        for row in table_rows.rows {
            let mut iter = row.into_iter();
            let schema_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let table_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };

            tables.push(TableInfo {
                name: table_name,
                schema: Some(schema_name),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: dbflux_core::CollectionPresentation::DataGrid,
                child_items: None,
            });
        }

        let views_sql = format!(
            "SELECT s.name AS schema_name, v.name AS view_name \
             FROM {qualified}.sys.views v \
             JOIN {qualified}.sys.schemas s ON s.schema_id = v.schema_id \
             WHERE v.is_ms_shipped = 0 \
             ORDER BY s.name, v.name",
            qualified = qualified
        );

        let view_rows = self.execute_simple(&views_sql)?;
        let mut views = Vec::new();

        for row in view_rows.rows {
            let mut iter = row.into_iter();
            let schema_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let view_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };

            views.push(ViewInfo {
                name: view_name,
                schema: Some(schema_name),
            });
        }

        Ok(DbSchemaInfo {
            name: database.to_string(),
            tables,
            views,
            custom_types: None,
        })
    }

    fn table_details(
        &self,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        let schema_name = schema.unwrap_or("dbo");

        log::info!(
            "[SCHEMA] Fetching details for table: {}.{}.{}",
            database,
            schema_name,
            table
        );

        let columns = self.fetch_columns(database, schema_name, table)?;
        let indexes = self.fetch_indexes(database, schema_name, table)?;
        let foreign_keys = self.fetch_foreign_keys(database, schema_name, table)?;
        let constraints = self.fetch_constraints(database, schema_name, table)?;

        Ok(TableInfo {
            name: table.to_string(),
            schema: Some(schema_name.to_string()),
            columns: Some(columns),
            indexes: Some(IndexData::Relational(indexes)),
            foreign_keys: Some(foreign_keys),
            constraints: Some(constraints),
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
        })
    }

    fn view_details(
        &self,
        database: &str,
        schema: Option<&str>,
        view: &str,
    ) -> Result<ViewInfo, DbError> {
        let schema_name = schema.unwrap_or("dbo");
        log::info!(
            "[SCHEMA] Fetching details for view: {}.{}.{}",
            database,
            schema_name,
            view
        );

        // We only return the basic name/schema today; the `ViewInfo` shape
        // doesn't carry a definition field. The query here also asserts
        // the view actually exists in the requested database, which is the
        // useful side-effect for error reporting.
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema_name.replace('\'', "''");
        let escaped_view = view.replace('\'', "''");

        let sql = format!(
            "SELECT 1 FROM {qualified_db}.sys.views v \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = v.schema_id \
             WHERE s.name = '{escaped_schema}' AND v.name = '{escaped_view}'",
        );
        self.execute_simple(&sql)?;

        Ok(ViewInfo {
            name: view.to_string(),
            schema: Some(schema_name.to_string()),
        })
    }

    fn schema_features(&self) -> SchemaFeatures {
        SchemaFeatures::FOREIGN_KEYS
            | SchemaFeatures::CHECK_CONSTRAINTS
            | SchemaFeatures::UNIQUE_CONSTRAINTS
            | SchemaFeatures::CUSTOM_TYPES
    }

    fn schema_types(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<CustomTypeInfo>, DbError> {
        let schema_name = schema.unwrap_or("dbo");
        self.fetch_custom_types(database, schema_name)
    }

    fn schema_indexes(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let schema_name = schema.unwrap_or("dbo");
        self.fetch_schema_indexes(database, schema_name)
    }

    fn schema_foreign_keys(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let schema_name = schema.unwrap_or("dbo");
        self.fetch_schema_foreign_keys(database, schema_name)
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        let mut current = self
            .current_database
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        if current.as_deref() == database {
            return Ok(());
        }

        if let Some(db) = database {
            let escaped = db.replace(']', "]]");
            let sql = format!("USE [{}]", escaped);
            self.execute_simple(&sql)?;
        }

        *current = database.map(|s| s.to_string());
        Ok(())
    }

    fn active_database(&self) -> Option<String> {
        self.current_database
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    fn kind(&self) -> DbKind {
        DbKind::SqlServer
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    /// SQL Server-specific browse implementation.
    ///
    /// Overridden because the default impl in `dbflux_core` (a) emits the
    /// MySQL-style `LIMIT n OFFSET m` syntax, which SQL Server does not accept,
    /// and (b) forwards the table's schema (e.g. `dbo`) as the *database* via
    /// `with_database`, which would make the driver issue `USE [dbo]` — a 911
    /// "database does not exist" error, since `dbo` is a schema. We embed the
    /// schema in the `FROM` clause and leave the active database alone.
    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        let table = request.table.quoted_with(&MSSQL_DIALECT);
        let mut sql = format!("SELECT * FROM {}", table);

        if let Some(filter) = request.semantic_filter.as_ref() {
            let where_clause = render_semantic_filter_sql(filter, &MSSQL_DIALECT)?;
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause);
        } else if let Some(filter) = request.filter.as_ref() {
            let trimmed = filter.trim();
            if !trimmed.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(trimmed);
            }
        }

        // `OFFSET ... FETCH NEXT` requires `ORDER BY`; fall back to `ORDER BY 1`
        // when the caller didn't supply one so paginated browsing keeps working.
        if request.order_by.is_empty() {
            sql.push_str(" ORDER BY 1");
        } else {
            sql.push_str(" ORDER BY ");
            let parts: Vec<String> = request
                .order_by
                .iter()
                .map(|col| {
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {}", col.column.quoted_with(&MSSQL_DIALECT), dir)
                })
                .collect();
            sql.push_str(&parts.join(", "));
        }

        sql.push_str(&format!(
            " OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            request.pagination.offset(),
            request.pagination.limit()
        ));

        self.execute(&QueryRequest::new(sql))
    }

    /// SQL Server-specific count implementation.
    ///
    /// The default impl is mostly correct, but we override it for symmetry with
    /// `browse_table` and to make explicit that the schema is part of the
    /// `FROM` clause — never the active database.
    fn count_table(&self, request: &TableCountRequest) -> Result<u64, DbError> {
        let table = request.table.quoted_with(&MSSQL_DIALECT);
        let mut sql = format!("SELECT COUNT(*) FROM {}", table);

        if let Some(filter) = request.semantic_filter.as_ref() {
            let where_clause = render_semantic_filter_sql(filter, &MSSQL_DIALECT)?;
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause);
        } else if let Some(filter) = request.filter.as_ref() {
            let trimmed = filter.trim();
            if !trimmed.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(trimmed);
            }
        }

        let result = self.execute(&QueryRequest::new(sql))?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| match v {
                Value::Int(n) => Some(*n as u64),
                _ => None,
            })
            .unwrap_or(0))
    }

    fn fetch_row_by_pk(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
        pk_column: &str,
        pk_value: &Value,
    ) -> Result<Option<std::collections::HashMap<String, Value>>, DbError> {
        let pk_literal = MSSQL_DIALECT.value_to_literal(pk_value);
        let sql = format!(
            "SELECT TOP 1 * FROM {}.{} WHERE {} = {}",
            MSSQL_DIALECT.quote_identifier(schema),
            MSSQL_DIALECT.quote_identifier(table),
            MSSQL_DIALECT.quote_identifier(pk_column),
            pk_literal,
        );

        let result = self.execute(&QueryRequest::new(sql))?;
        let columns = result.columns;
        let Some(row) = result.rows.into_iter().next() else {
            return Ok(None);
        };

        let map = columns
            .into_iter()
            .zip(row)
            .map(|(col, val)| (col.name, val))
            .collect();

        Ok(Some(map))
    }

    fn referenced_tables(&self, query: &str) -> Option<Vec<dbflux_core::QueryTableRef>> {
        Some(dbflux_core::extract_referenced_tables(query))
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(generate_select_star(&MSSQL_DIALECT, table, 100)),
            "insert" => Ok(generate_insert_template(&MSSQL_DIALECT, table)),
            "update" => Ok(generate_update_template(&MSSQL_DIALECT, table)),
            "delete" => Ok(generate_delete_template(&MSSQL_DIALECT, table)),
            "truncate" => Ok(generate_truncate(&MSSQL_DIALECT, table)),
            "drop_table" => Ok(generate_drop_table(&MSSQL_DIALECT, table)),
            _ => Err(DbError::NotSupported(format!(
                "Code generator '{}' not supported",
                generator_id
            ))),
        }
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        if !patch.identity.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot update row: invalid row identity (missing primary key)"
                    .to_string()
                    .into(),
            ));
        }

        if !patch.has_changes() {
            return Err(DbError::QueryFailed(
                "No changes to save".to_string().into(),
            ));
        }

        let sql = build_update_with_output(patch)?;
        log::debug!("[UPDATE] Executing: {}", sql);

        let result = self.execute_simple(&sql)?;
        Ok(result_first_row_to_crud(result))
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot insert row: no columns specified".to_string().into(),
            ));
        }

        let sql = build_insert_with_output(insert)?;
        log::debug!("[INSERT] Executing: {}", sql);

        let result = self.execute_simple(&sql)?;
        Ok(result_first_row_to_crud(result))
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot delete row: invalid row identity (missing primary key)"
                    .to_string()
                    .into(),
            ));
        }

        let sql = build_delete_with_output(delete)?;
        log::debug!("[DELETE] Executing: {}", sql);

        let result = self.execute_simple(&sql)?;
        Ok(result_first_row_to_crud(result))
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        let schema = request.table.schema.as_deref().unwrap_or("dbo");
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = request.table.name.replace('\'', "''");

        let sql = format!(
            "SELECT \
                c.name AS column_name, \
                t.name AS data_type, \
                CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable, \
                CAST(dc.definition AS NVARCHAR(MAX)) AS column_default, \
                c.max_length AS character_maximum_length \
             FROM sys.columns c \
             JOIN sys.tables tbl ON tbl.object_id = c.object_id \
             JOIN sys.schemas s ON s.schema_id = tbl.schema_id \
             JOIN sys.types t ON t.user_type_id = c.user_type_id \
             LEFT JOIN sys.default_constraints dc \
               ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \
             WHERE s.name = '{}' AND tbl.name = '{}' \
             ORDER BY c.column_id",
            escaped_schema, escaped_table
        );

        self.execute(&QueryRequest::new(sql))
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        // SQL Server requires SET SHOWPLAN_XML to be the only statement in
        // its batch, so we run three separate batches: turn it on, run the
        // query (which compiles but does not execute, returning the plan
        // as a single nvarchar(max) row), then turn it off. We unset
        // SHOWPLAN even if the middle batch fails so the session state
        // does not leak.
        let query = match &request.query {
            Some(q) => q.clone(),
            None => format!(
                "SELECT * FROM {} ORDER BY 1 OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY",
                request.table.quoted_with(self.dialect())
            ),
        };

        self.execute_simple("SET SHOWPLAN_XML ON")?;
        let plan_result = self.execute_simple(&query);
        // Best-effort cleanup; if this fails the session is broken and the
        // next query will fail loudly.
        let _ = self.execute_simple("SET SHOWPLAN_XML OFF");
        plan_result
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &MSSQL_DIALECT
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&MSSQL_DIALECT);
        Some(&GENERATOR)
    }

    fn build_select_sql(
        &self,
        table: &str,
        columns: &[String],
        _filter: Option<&Value>,
        order_by: &[OrderByColumn],
        limit: u32,
        offset: u32,
    ) -> String {
        let quoted_table = MSSQL_DIALECT.quote_identifier(table);
        let cols = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| MSSQL_DIALECT.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!("SELECT {} FROM {}", cols, quoted_table);

        let order_by_clause = if order_by.is_empty() {
            // OFFSET requires ORDER BY in SQL Server; default to ordering by 1 to keep paginated
            // browses functional when the caller hasn't supplied an explicit order.
            "ORDER BY 1".to_string()
        } else {
            let parts: Vec<String> = order_by
                .iter()
                .map(|col| {
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {}", col.column.quoted_with(&MSSQL_DIALECT), dir)
                })
                .collect();
            format!("ORDER BY {}", parts.join(", "))
        };

        sql.push(' ');
        sql.push_str(&order_by_clause);
        sql.push_str(&format!(
            " OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            offset, limit
        ));
        sql
    }

    fn build_count_sql(&self, table: &str, _filter: Option<&Value>) -> String {
        let quoted_table = MSSQL_DIALECT.quote_identifier(table);
        format!("SELECT COUNT(*) FROM {}", quoted_table)
    }

    fn build_truncate_sql(&self, table: &str) -> String {
        let quoted_table = MSSQL_DIALECT.quote_identifier(table);
        format!("TRUNCATE TABLE {}", quoted_table)
    }

    fn build_drop_index_sql(
        &self,
        index_name: &str,
        table_name: Option<&str>,
        if_exists: bool,
    ) -> String {
        let quoted_index = MSSQL_DIALECT.quote_identifier(index_name);
        let table = table_name
            .map(|t| MSSQL_DIALECT.quote_identifier(t))
            .unwrap_or_else(|| "[table_name]".to_string());

        if if_exists {
            format!("DROP INDEX IF EXISTS {} ON {}", quoted_index, table)
        } else {
            format!("DROP INDEX {} ON {}", quoted_index, table)
        }
    }

    fn version_query(&self) -> &'static str {
        "SELECT @@VERSION"
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }
}

impl MssqlConnection {
    fn fetch_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = table.replace('\'', "''");

        let sql = format!(
            "SELECT \
                c.name AS column_name, \
                t.name AS type_name, \
                c.is_nullable AS nullable, \
                CAST(dc.definition AS NVARCHAR(MAX)) AS column_default, \
                CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_pk \
             FROM {qualified_db}.sys.columns c \
             JOIN {qualified_db}.sys.tables tbl ON tbl.object_id = c.object_id \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = tbl.schema_id \
             JOIN {qualified_db}.sys.types t ON t.user_type_id = c.user_type_id \
             LEFT JOIN {qualified_db}.sys.default_constraints dc \
               ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \
             LEFT JOIN {qualified_db}.sys.indexes pk \
               ON pk.object_id = c.object_id AND pk.is_primary_key = 1 \
             LEFT JOIN {qualified_db}.sys.index_columns ic \
               ON ic.object_id = pk.object_id AND ic.index_id = pk.index_id \
               AND ic.column_id = c.column_id \
             WHERE s.name = '{escaped_schema}' AND tbl.name = '{escaped_table}' \
             ORDER BY c.column_id",
            qualified_db = qualified_db,
            escaped_schema = escaped_schema,
            escaped_table = escaped_table
        );

        let result = self.execute_simple(&sql)?;
        let mut columns = Vec::new();

        for row in result.rows {
            let mut iter = row.into_iter();

            let name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let type_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => String::new(),
            };
            let nullable = value_is_truthy(iter.next());
            let default_value = match iter.next() {
                Some(Value::Text(s)) => Some(s),
                _ => None,
            };
            let is_primary_key = value_is_truthy(iter.next());

            columns.push(ColumnInfo {
                name,
                type_name,
                nullable,
                default_value,
                is_primary_key,
                enum_values: None,
            });
        }

        Ok(columns)
    }

    fn fetch_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = table.replace('\'', "''");

        let sql = format!(
            "SELECT \
                i.name AS index_name, \
                c.name AS column_name, \
                i.is_unique, \
                i.is_primary_key, \
                ic.key_ordinal \
             FROM {qualified_db}.sys.indexes i \
             JOIN {qualified_db}.sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
             JOIN {qualified_db}.sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
             JOIN {qualified_db}.sys.tables tbl ON tbl.object_id = i.object_id \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = tbl.schema_id \
             WHERE s.name = '{escaped_schema}' AND tbl.name = '{escaped_table}' \
               AND i.name IS NOT NULL \
             ORDER BY i.name, ic.key_ordinal",
            qualified_db = qualified_db,
            escaped_schema = escaped_schema,
            escaped_table = escaped_table
        );

        let result = self.execute_simple(&sql)?;

        let mut grouped: indexmap_for_indexes::IndexMap<String, IndexInfo> =
            indexmap_for_indexes::IndexMap::new();

        for row in result.rows {
            let mut iter = row.into_iter();

            let index_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let column_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let is_unique = value_is_truthy(iter.next());
            let is_primary = value_is_truthy(iter.next());
            // key_ordinal is consumed only for ORDER BY purposes
            let _ = iter.next();

            let entry = grouped
                .entry(index_name.clone())
                .or_insert_with(|| IndexInfo {
                    name: index_name,
                    columns: Vec::new(),
                    is_unique,
                    is_primary,
                });
            entry.columns.push(column_name);
        }

        Ok(grouped.into_iter().map(|(_, v)| v).collect())
    }

    fn fetch_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = table.replace('\'', "''");

        let sql = format!(
            "SELECT \
                fk.name AS constraint_name, \
                pc.name AS column_name, \
                rs.name AS referenced_schema, \
                rt.name AS referenced_table, \
                rc.name AS referenced_column, \
                fk.delete_referential_action_desc, \
                fk.update_referential_action_desc \
             FROM {qualified_db}.sys.foreign_keys fk \
             JOIN {qualified_db}.sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id \
             JOIN {qualified_db}.sys.tables pt ON pt.object_id = fk.parent_object_id \
             JOIN {qualified_db}.sys.schemas ps ON ps.schema_id = pt.schema_id \
             JOIN {qualified_db}.sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id \
             JOIN {qualified_db}.sys.tables rt ON rt.object_id = fk.referenced_object_id \
             JOIN {qualified_db}.sys.schemas rs ON rs.schema_id = rt.schema_id \
             JOIN {qualified_db}.sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id \
             WHERE ps.name = '{escaped_schema}' AND pt.name = '{escaped_table}' \
             ORDER BY fk.name, fkc.constraint_column_id",
            qualified_db = qualified_db,
            escaped_schema = escaped_schema,
            escaped_table = escaped_table
        );

        let result = self.execute_simple(&sql)?;

        let mut builder = ForeignKeyBuilder::new();

        for row in result.rows {
            let mut iter = row.into_iter();

            let name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let column = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let referenced_schema = match iter.next() {
                Some(Value::Text(s)) => Some(s),
                _ => None,
            };
            let referenced_table = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let referenced_column = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let on_delete = match iter.next() {
                Some(Value::Text(s)) if s != "NO_ACTION" => Some(normalize_fk_action(&s)),
                _ => None,
            };
            let on_update = match iter.next() {
                Some(Value::Text(s)) if s != "NO_ACTION" => Some(normalize_fk_action(&s)),
                _ => None,
            };

            builder.add_column(
                name,
                column,
                referenced_schema,
                referenced_table,
                referenced_column,
                on_delete,
                on_update,
            );
        }

        Ok(builder.build())
    }

    fn fetch_constraints(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = table.replace('\'', "''");

        let mut grouped: indexmap_for_indexes::IndexMap<String, ConstraintInfo> =
            indexmap_for_indexes::IndexMap::new();

        self.collect_check_constraints(
            &qualified_db,
            &escaped_schema,
            &escaped_table,
            &mut grouped,
        )?;
        self.collect_unique_constraints(
            &qualified_db,
            &escaped_schema,
            &escaped_table,
            &mut grouped,
        )?;

        Ok(grouped.into_iter().map(|(_, v)| v).collect())
    }

    fn collect_check_constraints(
        &self,
        qualified_db: &str,
        escaped_schema: &str,
        escaped_table: &str,
        grouped: &mut indexmap_for_indexes::IndexMap<String, ConstraintInfo>,
    ) -> Result<(), DbError> {
        let sql = format!(
            "SELECT \
                cc.name AS constraint_name, \
                CAST(cc.definition AS NVARCHAR(MAX)) AS check_clause, \
                c.name AS column_name \
             FROM {qualified_db}.sys.check_constraints cc \
             JOIN {qualified_db}.sys.tables tbl ON tbl.object_id = cc.parent_object_id \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = tbl.schema_id \
             LEFT JOIN {qualified_db}.sys.columns c \
               ON c.object_id = cc.parent_object_id AND c.column_id = cc.parent_column_id \
             WHERE s.name = '{escaped_schema}' AND tbl.name = '{escaped_table}' \
             ORDER BY cc.name"
        );

        let rows = self.execute_simple(&sql)?;
        for row in rows.rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let check_clause = match iter.next() {
                Some(Value::Text(s)) => Some(s),
                _ => None,
            };
            let column = match iter.next() {
                Some(Value::Text(s)) => Some(s),
                _ => None,
            };

            let entry = grouped
                .entry(name.clone())
                .or_insert_with(|| ConstraintInfo {
                    name,
                    kind: ConstraintKind::Check,
                    columns: Vec::new(),
                    check_clause,
                });
            if let Some(column) = column
                && !entry.columns.contains(&column)
            {
                entry.columns.push(column);
            }
        }
        Ok(())
    }

    /// UNIQUE constraints — surfaced as unique indexes that are not the
    /// table's primary key. `sys.indexes.is_unique_constraint` flags those
    /// backing an explicit UNIQUE constraint definition.
    fn collect_unique_constraints(
        &self,
        qualified_db: &str,
        escaped_schema: &str,
        escaped_table: &str,
        grouped: &mut indexmap_for_indexes::IndexMap<String, ConstraintInfo>,
    ) -> Result<(), DbError> {
        let sql = format!(
            "SELECT \
                i.name AS index_name, \
                c.name AS column_name, \
                ic.key_ordinal \
             FROM {qualified_db}.sys.indexes i \
             JOIN {qualified_db}.sys.index_columns ic \
               ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
             JOIN {qualified_db}.sys.columns c \
               ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
             JOIN {qualified_db}.sys.tables tbl ON tbl.object_id = i.object_id \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = tbl.schema_id \
             WHERE s.name = '{escaped_schema}' AND tbl.name = '{escaped_table}' \
               AND i.is_unique_constraint = 1 \
             ORDER BY i.name, ic.key_ordinal"
        );

        let rows = self.execute_simple(&sql)?;
        for row in rows.rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let column = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let _ordinal = iter.next();

            let entry = grouped
                .entry(name.clone())
                .or_insert_with(|| ConstraintInfo {
                    name,
                    kind: ConstraintKind::Unique,
                    columns: Vec::new(),
                    check_clause: None,
                });
            entry.columns.push(column);
        }
        Ok(())
    }

    fn fetch_custom_types(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<CustomTypeInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");

        // User-defined alias types map most cleanly to DBFlux's Domain kind
        // (a wrapper around a base type). CLR/table-typed types are
        // exposed as Composite for now.
        let sql = format!(
            "SELECT \
                t.name AS type_name, \
                bt.name AS base_type_name, \
                t.is_table_type \
             FROM {qualified_db}.sys.types t \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = t.schema_id \
             LEFT JOIN {qualified_db}.sys.types bt \
               ON bt.user_type_id = t.system_type_id AND bt.is_user_defined = 0 \
             WHERE t.is_user_defined = 1 AND s.name = '{escaped_schema}' \
             ORDER BY t.name"
        );

        let rows = self.execute_simple(&sql)?;
        let mut types = Vec::new();

        for row in rows.rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let base_type = match iter.next() {
                Some(Value::Text(s)) => Some(s),
                _ => None,
            };
            let is_table_type = value_is_truthy(iter.next());

            let kind = if is_table_type {
                CustomTypeKind::Composite
            } else {
                CustomTypeKind::Domain
            };

            types.push(CustomTypeInfo {
                name,
                schema: Some(schema.to_string()),
                kind,
                enum_values: None,
                base_type,
            });
        }

        Ok(types)
    }

    fn fetch_schema_indexes(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");

        let sql = format!(
            "SELECT \
                tbl.name AS table_name, \
                i.name AS index_name, \
                c.name AS column_name, \
                i.is_unique, \
                i.is_primary_key, \
                ic.key_ordinal \
             FROM {qualified_db}.sys.indexes i \
             JOIN {qualified_db}.sys.index_columns ic \
               ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
             JOIN {qualified_db}.sys.columns c \
               ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
             JOIN {qualified_db}.sys.tables tbl ON tbl.object_id = i.object_id \
             JOIN {qualified_db}.sys.schemas s ON s.schema_id = tbl.schema_id \
             WHERE s.name = '{escaped_schema}' AND i.name IS NOT NULL \
             ORDER BY tbl.name, i.name, ic.key_ordinal"
        );

        let rows = self.execute_simple(&sql)?;
        let mut builder = SchemaIndexBuilder::new();

        for row in rows.rows {
            let mut iter = row.into_iter();
            let table_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let index_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let column = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let is_unique = value_is_truthy(iter.next());
            let is_primary = value_is_truthy(iter.next());
            let _ordinal = iter.next();

            builder.add_column(table_name.clone(), index_name.clone(), column, is_unique);
            if is_primary {
                builder.set_primary(&table_name, &index_name);
            }
        }

        Ok(builder.build_sorted())
    }

    fn fetch_schema_foreign_keys(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let escaped_db = database.replace(']', "]]");
        let qualified_db = format!("[{}]", escaped_db);
        let escaped_schema = schema.replace('\'', "''");

        let sql = format!(
            "SELECT \
                pt.name AS table_name, \
                fk.name AS constraint_name, \
                pc.name AS column_name, \
                rs.name AS referenced_schema, \
                rt.name AS referenced_table, \
                rc.name AS referenced_column, \
                fk.delete_referential_action_desc, \
                fk.update_referential_action_desc \
             FROM {qualified_db}.sys.foreign_keys fk \
             JOIN {qualified_db}.sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id \
             JOIN {qualified_db}.sys.tables pt ON pt.object_id = fk.parent_object_id \
             JOIN {qualified_db}.sys.schemas ps ON ps.schema_id = pt.schema_id \
             JOIN {qualified_db}.sys.columns pc \
               ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id \
             JOIN {qualified_db}.sys.tables rt ON rt.object_id = fk.referenced_object_id \
             JOIN {qualified_db}.sys.schemas rs ON rs.schema_id = rt.schema_id \
             JOIN {qualified_db}.sys.columns rc \
               ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id \
             WHERE ps.name = '{escaped_schema}' \
             ORDER BY pt.name, fk.name, fkc.constraint_column_id"
        );

        let rows = self.execute_simple(&sql)?;
        let mut builder = SchemaForeignKeyBuilder::new();

        for row in rows.rows {
            let mut iter = row.into_iter();
            let table_name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let name = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let column = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let referenced_schema = match iter.next() {
                Some(Value::Text(s)) => Some(s),
                _ => None,
            };
            let referenced_table = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let referenced_column = match iter.next() {
                Some(Value::Text(s)) => s,
                _ => continue,
            };
            let on_delete = match iter.next() {
                Some(Value::Text(s)) if s != "NO_ACTION" => Some(normalize_fk_action(&s)),
                _ => None,
            };
            let on_update = match iter.next() {
                Some(Value::Text(s)) if s != "NO_ACTION" => Some(normalize_fk_action(&s)),
                _ => None,
            };

            builder.add_column(
                table_name,
                name,
                column,
                referenced_schema,
                referenced_table,
                referenced_column,
                on_update,
                on_delete,
            );
        }

        Ok(builder.build_sorted())
    }
}

// Local indexmap usage so we don't depend on dbflux_core's indexmap re-export.
mod indexmap_for_indexes {
    pub(super) use indexmap::IndexMap;
}

fn normalize_fk_action(action: &str) -> String {
    // Convert tiberius/sys.foreign_keys descriptors ("NO_ACTION", "CASCADE",
    // "SET_NULL", "SET_DEFAULT") into the canonical SQL clause form.
    action.replace('_', " ")
}

fn value_is_truthy(value: Option<Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => b,
        Some(Value::Int(i)) => i != 0,
        _ => false,
    }
}

/// Returns `true` when a server error looks like it was raised because the
/// session was killed (rather than being a real query failure).
///
/// SQL Server emits one of:
///   - 596: Cannot continue the execution because the session is in the kill state.
///   - 233 / 10054 family: connection-reset variants after the socket closes.
///   - 6005: SHUTDOWN is in progress (rare, but also fires for some KILL paths).
///
/// These codes are stable across modern SQL Server versions. The check is
/// only used to translate post-KILL errors into `DbError::Cancelled`, so
/// false negatives just result in a slightly less specific error message;
/// false positives would mask real errors, which is why the list is small.
fn is_kill_error(err: &DbError) -> bool {
    matches!(err,
        DbError::QueryFailed(fe)
        | DbError::ConstraintViolation(fe)
        | DbError::SyntaxError(fe)
        | DbError::AuthFailed(fe)
        | DbError::PermissionDenied(fe)
        | DbError::ObjectNotFound(fe)
        | DbError::ConnectionFailed(fe)
        if matches!(fe.code.as_deref(), Some("596") | Some("233") | Some("6005"))
    )
}

// ---------------------------------------------------------------------------
// OUTPUT-clause SQL builders
// ---------------------------------------------------------------------------
//
// SQL Server places the OUTPUT clause between the target and the
// VALUES/SET/WHERE clauses, not at the end like Postgres `RETURNING`.
// We build the SQL directly rather than threading a new dialect hook
// through `SqlQueryBuilder`, keeping the change local to this driver.

fn build_insert_with_output(insert: &RowInsert) -> Result<String, DbError> {
    let table = MSSQL_DIALECT.qualified_table(insert.schema.as_deref(), &insert.table);
    let columns = insert
        .columns
        .iter()
        .map(|c| MSSQL_DIALECT.quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let values = insert
        .values
        .iter()
        .map(|v| MSSQL_DIALECT.value_to_literal(v))
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!(
        "INSERT INTO {} ({}) OUTPUT INSERTED.* VALUES ({})",
        table, columns, values
    ))
}

fn build_update_with_output(patch: &RowPatch) -> Result<String, DbError> {
    let table = MSSQL_DIALECT.qualified_table(patch.schema.as_deref(), &patch.table);
    let set_clause = patch
        .changes
        .iter()
        .map(|(col, value)| {
            format!(
                "{} = {}",
                MSSQL_DIALECT.quote_identifier(col),
                MSSQL_DIALECT.value_to_literal(value)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = identity_where_clause(&patch.identity)?;

    Ok(format!(
        "UPDATE {} SET {} OUTPUT INSERTED.* WHERE {}",
        table, set_clause, where_clause
    ))
}

fn build_delete_with_output(delete: &RowDelete) -> Result<String, DbError> {
    let table = MSSQL_DIALECT.qualified_table(delete.schema.as_deref(), &delete.table);
    let where_clause = identity_where_clause(&delete.identity)?;

    Ok(format!(
        "DELETE FROM {} OUTPUT DELETED.* WHERE {}",
        table, where_clause
    ))
}

fn identity_where_clause(identity: &RecordIdentity) -> Result<String, DbError> {
    match identity {
        RecordIdentity::Composite { columns, values } => {
            if columns.is_empty() || columns.len() != values.len() {
                return Err(DbError::QueryFailed(
                    "Row identity has no columns".to_string().into(),
                ));
            }
            Ok(columns
                .iter()
                .zip(values.iter())
                .map(|(col, value)| {
                    format!(
                        "{} = {}",
                        MSSQL_DIALECT.quote_identifier(col),
                        MSSQL_DIALECT.value_to_literal(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "))
        }
        RecordIdentity::ObjectId(_) | RecordIdentity::Key(_) => Err(DbError::NotSupported(
            "SQL Server row mutations require a composite primary-key identity".to_string(),
        )),
    }
}

fn result_first_row_to_crud(result: QueryResult) -> CrudResult {
    // The OUTPUT-clause result is parsed through `execute_simple`, which
    // populates `ColumnMeta::kind` from the tiberius column type. The
    // column metadata travels with the QueryResult; CrudResult itself
    // only carries the row (matching postgres/mysql).
    match result.rows.into_iter().next() {
        Some(row) => CrudResult::success(row),
        None => CrudResult::empty(),
    }
}

impl RelationalConnection for MssqlConnection {}

impl ConnectionExt for MssqlConnection {
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

// =============================================================================
// URI helpers
// =============================================================================

fn inject_password_into_mssql_uri(base_uri: &str, password: Option<&str>) -> String {
    let password = match password {
        Some(p) if !p.is_empty() => p,
        _ => return base_uri.to_string(),
    };

    let prefixes = ["sqlserver://", "mssql://"];
    for prefix in prefixes {
        if let Some(rest) = base_uri.strip_prefix(prefix) {
            if let Some(at_pos) = rest.rfind('@') {
                let user_pass = &rest[..at_pos];
                let after_at = &rest[at_pos..];

                if user_pass.contains(':') {
                    return base_uri.to_string();
                }

                return format!(
                    "{}{}:{}{}",
                    prefix,
                    user_pass,
                    urlencoding::encode(password),
                    after_at
                );
            }

            return base_uri.to_string();
        }
    }

    base_uri.to_string()
}

/// Parse a `sqlserver://user:pass@host:port/db?...` URI into a tiberius Config.
struct ParsedMssqlUri<'a> {
    credentials: &'a str,
    host: &'a str,
    instance_from_host: Option<String>,
    explicit_port: Option<u16>,
    database: &'a str,
    params: &'a str,
}

struct UriQueryParams {
    encryption: EncryptionLevel,
    trust_cert_override: Option<bool>,
    instance_from_query: Option<String>,
}

fn split_mssql_uri(uri: &str) -> Result<ParsedMssqlUri<'_>, tiberius::error::Error> {
    let stripped = uri
        .strip_prefix("sqlserver://")
        .or_else(|| uri.strip_prefix("mssql://"))
        .ok_or_else(|| {
            tiberius::error::Error::Conversion("unsupported SQL Server URI scheme".into())
        })?;

    let (credentials, host_part) = match stripped.rfind('@') {
        Some(at_pos) => (&stripped[..at_pos], &stripped[at_pos + 1..]),
        None => ("", stripped),
    };

    let (host_port, query_part) = match host_part.find('/') {
        Some(slash) => (&host_part[..slash], &host_part[slash + 1..]),
        None => (host_part, ""),
    };

    let (database, params) = match query_part.find('?') {
        Some(qmark) => (&query_part[..qmark], &query_part[qmark + 1..]),
        None => (query_part, ""),
    };

    let (host_and_instance, explicit_port) = match host_port.rfind(':') {
        Some(colon) => {
            let port: u16 = host_port[colon + 1..].parse().map_err(|_| {
                tiberius::error::Error::Conversion("invalid SQL Server URI port".into())
            })?;
            (&host_port[..colon], Some(port))
        }
        None => (host_port, None),
    };

    // SSMS-style `host\instance`. The trailing `?instance=…` may also set
    // it and wins if both are supplied (resolved by the caller).
    let (host, instance_from_host) = match host_and_instance.find('\\') {
        Some(bs) => (
            &host_and_instance[..bs],
            Some(host_and_instance[bs + 1..].to_string()),
        ),
        None => (host_and_instance, None),
    };

    Ok(ParsedMssqlUri {
        credentials,
        host,
        instance_from_host,
        explicit_port,
        database,
        params,
    })
}

fn parse_uri_query_params(params: &str) -> UriQueryParams {
    // SSL Mode is the single user-facing knob; trust_cert is derived from
    // it unless the caller explicitly overrides via `?trust=` in the URI.
    //
    //   encrypt=off       -> Off,      trust irrelevant
    //   encrypt=on        -> On,       trust=true  (accept self-signed)
    //   encrypt=required  -> Required, trust=false (validate cert)
    let mut out = UriQueryParams {
        encryption: EncryptionLevel::On,
        trust_cert_override: None,
        instance_from_query: None,
    };

    for pair in params.split('&').filter(|p| !p.is_empty()) {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        match key.to_ascii_lowercase().as_str() {
            "encrypt" => {
                out.encryption = match value.to_ascii_lowercase().as_str() {
                    "true" | "yes" | "on" | "1" => EncryptionLevel::On,
                    "false" | "no" | "off" | "0" => EncryptionLevel::Off,
                    "strict" | "required" => EncryptionLevel::Required,
                    _ => EncryptionLevel::On,
                };
            }
            "trustservercertificate" | "trust_server_certificate" | "trust" => {
                out.trust_cert_override = Some(matches!(
                    value.to_ascii_lowercase().as_str(),
                    "true" | "1" | "yes"
                ));
            }
            "instance" | "instancename" | "instance_name" if !value.is_empty() => {
                let decoded = urlencoding::decode(value)
                    .map(|cow| cow.into_owned())
                    .unwrap_or_else(|_| value.to_string());
                out.instance_from_query = Some(decoded);
            }
            _ => {}
        }
    }

    out
}

fn apply_uri_credentials(config: &mut Config, credentials: &str) {
    if credentials.is_empty() {
        return;
    }
    let (user_part, password_part) = match credentials.find(':') {
        Some(colon) => (&credentials[..colon], &credentials[colon + 1..]),
        None => (credentials, ""),
    };
    let user = urlencoding::decode(user_part)
        .map(|cow| cow.into_owned())
        .unwrap_or_default();
    let password = urlencoding::decode(password_part)
        .map(|cow| cow.into_owned())
        .unwrap_or_default();
    config.authentication(AuthMethod::sql_server(user, password));
}

fn parse_mssql_url(uri: &str) -> Result<Config, tiberius::error::Error> {
    let parsed = split_mssql_uri(uri)?;
    let query = parse_uri_query_params(parsed.params);

    // `?instance=` wins over the SSMS-style `host\instance` form.
    let instance = query
        .instance_from_query
        .or(parsed.instance_from_host)
        .filter(|s| !s.is_empty());

    let mut config = Config::new();
    config.host(parsed.host);

    if !parsed.database.is_empty() {
        let decoded = urlencoding::decode(parsed.database)
            .map(|cow| cow.into_owned())
            .unwrap_or_else(|_| parsed.database.to_string());
        config.database(decoded);
    }

    apply_uri_credentials(&mut config, parsed.credentials);

    match (instance, parsed.explicit_port) {
        (Some(name), _) => {
            // Named instance: leave port unset so tiberius hits 1434 for
            // the Browser query and then dials whatever port Browser
            // returns. Any user-supplied port in the URI is ignored in
            // this mode — that matches SSMS/ADO behavior.
            config.instance_name(name);
        }
        (None, Some(port)) => {
            config.port(port);
        }
        (None, None) => {
            config.port(1433);
        }
    }

    config.encryption(query.encryption);
    let trust_cert = query
        .trust_cert_override
        .unwrap_or(!matches!(query.encryption, EncryptionLevel::Required));
    if trust_cert {
        config.trust_cert();
    }

    Ok(config)
}

// =============================================================================
// Error formatting
// =============================================================================

pub struct MssqlErrorFormatter;

/// Semantic class of an MSSQL error code, used to pick a `DbError` variant.
///
/// SQL Server reports errors as numeric `code()` values rather than ANSI
/// SQLSTATEs, so the shared `FormattedError::into_query_error()` classifier
/// (which inspects SQLSTATE prefixes) cannot route them. We classify here
/// and construct the `DbError` variant directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MssqlErrorClass {
    Auth,
    Permission,
    NotFound,
    Constraint,
    Syntax,
}

fn classify_mssql_code(code: u32) -> Option<MssqlErrorClass> {
    match code {
        // Login / auth
        4060 | 18450 | 18452 | 18456 | 18486 | 18487 | 18488 => Some(MssqlErrorClass::Auth),

        // Permission / access
        229 | 230 | 262 | 297 | 916 => Some(MssqlErrorClass::Permission),

        // Object not found
        // 208: Invalid object name
        // 207: Invalid column name
        // 2812: Could not find stored procedure
        // 4902: Cannot find object (ALTER)
        208 | 207 | 2812 | 4902 => Some(MssqlErrorClass::NotFound),

        // Constraint violations
        // 547: FK violation / CHECK constraint conflict
        // 2627: Unique constraint
        // 2601: Unique index
        // 515: Cannot insert NULL into column (NOT NULL violation)
        // 8152: String or binary data would be truncated
        // 245: Conversion failed (often constraint-shaped failure)
        547 | 2627 | 2601 | 515 | 8152 | 245 => Some(MssqlErrorClass::Constraint),

        // Syntax / batch parsing
        // 102: Incorrect syntax near
        // 156: Incorrect syntax near the keyword
        // 8180: Statement(s) could not be prepared
        102 | 156 | 8180 => Some(MssqlErrorClass::Syntax),

        _ => None,
    }
}

/// Extract structured location info (table, constraint) from common SQL Server
/// constraint-violation messages.
///
/// Messages look like:
///   "Violation of PRIMARY KEY constraint 'PK_users'. Cannot insert
///    duplicate key in object 'dbo.users'. The duplicate key value is (1)."
///   "The INSERT statement conflicted with the FOREIGN KEY constraint
///    'FK_orders_user'. The conflict occurred in database 'app',
///    table 'dbo.users', column 'id'."
fn extract_location_from_message(message: &str) -> dbflux_core::ErrorLocation {
    let mut location = dbflux_core::ErrorLocation::new();

    if let Some(name) = find_quoted_after(message, "constraint") {
        location = location.with_constraint(name);
    }

    if let Some(qualified) =
        find_quoted_after(message, "object").or_else(|| find_quoted_after(message, "table"))
    {
        if let Some((schema, table)) = qualified.split_once('.') {
            location = location
                .with_schema(schema.trim_matches(|c| c == '[' || c == ']'))
                .with_table(table.trim_matches(|c| c == '[' || c == ']'));
        } else {
            location = location.with_table(qualified);
        }
    }

    if let Some(column) = find_quoted_after(message, "column") {
        location = location.with_column(column);
    }

    location
}

fn find_quoted_after(haystack: &str, marker: &str) -> Option<String> {
    let lower = haystack.to_ascii_lowercase();
    let marker_lower = marker.to_ascii_lowercase();
    let mut search_from = 0;

    while let Some(idx) = lower[search_from..].find(&marker_lower) {
        let abs = search_from + idx + marker_lower.len();
        if let Some(rest) = haystack.get(abs..)
            && let Some(start) = rest.find('\'')
        {
            let after_quote = &rest[start + 1..];
            if let Some(end) = after_quote.find('\'') {
                return Some(after_quote[..end].to_string());
            }
        }
        search_from = abs;
    }
    None
}

impl MssqlErrorFormatter {
    fn classify_tiberius_error(err: &tiberius::error::Error) -> FormattedError {
        match err {
            tiberius::error::Error::Server(token) => {
                let message = token.message().to_string();
                let mut fe =
                    FormattedError::new(message.clone()).with_code(token.code().to_string());

                let state = token.state();
                if state != 0 {
                    fe = fe.with_detail(format!("State {}, line {}", state, token.line()));
                }

                let location = extract_location_from_message(&message);
                if !location.is_empty() {
                    fe = fe.with_location(location);
                }

                fe
            }
            tiberius::error::Error::Protocol(msg) => {
                FormattedError::new(format!("Protocol error: {}", msg))
            }
            tiberius::error::Error::Encoding(msg) => {
                FormattedError::new(format!("Encoding error: {}", msg))
            }
            tiberius::error::Error::Conversion(msg) => {
                FormattedError::new(format!("Conversion error: {}", msg))
            }
            tiberius::error::Error::Utf8 => FormattedError::new("Invalid UTF-8 in response"),
            tiberius::error::Error::Utf16 => FormattedError::new("Invalid UTF-16 in response"),
            tiberius::error::Error::ParseInt(e) => {
                FormattedError::new(format!("Integer parse error: {}", e))
            }
            tiberius::error::Error::Io { kind, message } => {
                FormattedError::new(format!("I/O {}: {}", kind, message))
            }
            other => FormattedError::new(other.to_string()),
        }
    }

    fn route_query_error(fe: FormattedError) -> DbError {
        if let Some(class) = fe
            .code
            .as_deref()
            .and_then(|c| c.parse::<u32>().ok())
            .and_then(classify_mssql_code)
        {
            return match class {
                MssqlErrorClass::Auth => DbError::AuthFailed(fe),
                MssqlErrorClass::Permission => DbError::PermissionDenied(fe),
                MssqlErrorClass::NotFound => DbError::ObjectNotFound(fe),
                MssqlErrorClass::Constraint => DbError::ConstraintViolation(fe),
                MssqlErrorClass::Syntax => DbError::SyntaxError(fe),
            };
        }

        // Fall back to the shared SQLSTATE classifier (which will just return
        // QueryFailed for MSSQL codes that don't match a SQLSTATE prefix).
        fe.into_query_error()
    }
}

impl QueryErrorFormatter for MssqlErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        if let Some(err) = error.downcast_ref::<tiberius::error::Error>() {
            Self::classify_tiberius_error(err)
        } else {
            FormattedError::new(error.to_string())
        }
    }
}

impl ConnectionErrorFormatter for MssqlErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        let source = error.to_string();
        let message = if source.contains("Login failed") || source.contains("18456") {
            "Authentication failed. Check your username and password.".to_string()
        } else if source.contains("Cannot open database") {
            format!("Database does not exist or login lacks access: {}", source)
        } else if source.contains("Connection refused") {
            format!(
                "Could not connect to {}:{}. The server may be unreachable, behind a firewall, or requires an SSH tunnel.",
                host, port
            )
        } else if source.contains("Name or service not known")
            || source.contains("nodename nor servname")
            || source.contains("failed to lookup address")
        {
            format!("Could not resolve hostname: {}", host)
        } else {
            format!("Connection error: {}", source)
        };

        FormattedError::new(message)
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        FormattedError::new(format!(
            "Connection error with URI {}: {}",
            sanitized_uri, error
        ))
    }
}

static MSSQL_ERROR_FORMATTER: MssqlErrorFormatter = MssqlErrorFormatter;

fn format_mssql_connect_error(e: &tiberius::error::Error, host: &str, port: u16) -> DbError {
    // Tiberius bubbles up login failures as `Server` token errors, not as
    // a separate `Auth` variant. We classify by the numeric code first so
    // 18456 etc. surface as `AuthFailed`, then fall back to the host-aware
    // connection error message for everything else.
    let server_fe = match e {
        tiberius::error::Error::Server(_) => Some(MssqlErrorFormatter::classify_tiberius_error(e)),
        _ => None,
    };

    if let Some(fe) = server_fe
        && let Some(code) = fe.code.as_deref().and_then(|c| c.parse::<u32>().ok())
        && matches!(classify_mssql_code(code), Some(MssqlErrorClass::Auth))
    {
        log::error!("SQL Server login failed: {}", fe.message);
        return DbError::AuthFailed(fe);
    }

    let formatted = MSSQL_ERROR_FORMATTER.format_connection_error(e, host, port);
    log::error!("SQL Server connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

fn format_mssql_query_error(e: &tiberius::error::Error) -> DbError {
    let formatted = MSSQL_ERROR_FORMATTER.format_query_error(e);
    let message = formatted.to_display_string();
    log::error!("SQL Server query failed: {}", message);
    MssqlErrorFormatter::route_query_error(formatted)
}

fn format_mssql_uri_error(e: &tiberius::error::Error, uri: &str) -> DbError {
    let sanitized = sanitize_uri(uri);
    let formatted = MSSQL_ERROR_FORMATTER.format_uri_error(e, &sanitized);
    log::error!("SQL Server URI connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_brackets_and_escapes() {
        assert_eq!(MSSQL_DIALECT.quote_identifier("users"), "[users]");
        assert_eq!(MSSQL_DIALECT.quote_identifier("ev]il"), "[ev]]il]");
    }

    #[test]
    fn bool_literal_uses_bit_zero_one() {
        assert_eq!(MSSQL_DIALECT.value_to_literal(&Value::Bool(true)), "1");
        assert_eq!(MSSQL_DIALECT.value_to_literal(&Value::Bool(false)), "0");
    }

    #[test]
    fn text_literal_is_n_prefixed_and_escaped() {
        assert_eq!(
            MSSQL_DIALECT.value_to_literal(&Value::Text("d'argent".into())),
            "N'd''argent'"
        );
    }

    #[test]
    fn bytes_literal_uses_uppercase_hex() {
        assert_eq!(
            MSSQL_DIALECT.value_to_literal(&Value::Bytes(vec![0x01, 0xAB, 0xCD])),
            "0x01ABCD"
        );
    }

    #[test]
    fn insert_with_output_emits_output_inserted_star() {
        let insert = RowInsert::new(
            "users".into(),
            Some("dbo".into()),
            vec!["name".into(), "age".into()],
            vec![Value::Text("alice".into()), Value::Int(30)],
        );
        let sql = build_insert_with_output(&insert).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO [dbo].[users] ([name], [age]) OUTPUT INSERTED.* VALUES (N'alice', 30)"
        );
    }

    #[test]
    fn update_with_output_emits_output_inserted_star() {
        let patch = RowPatch::new(
            RecordIdentity::composite(vec!["id".into()], vec![Value::Int(7)]),
            "users".into(),
            Some("dbo".into()),
            vec![("name".into(), Value::Text("bob".into()))],
        );
        let sql = build_update_with_output(&patch).unwrap();
        assert_eq!(
            sql,
            "UPDATE [dbo].[users] SET [name] = N'bob' OUTPUT INSERTED.* WHERE [id] = 7"
        );
    }

    #[test]
    fn delete_with_output_emits_output_deleted_star() {
        let delete = RowDelete {
            identity: RecordIdentity::composite(vec!["id".into()], vec![Value::Int(7)]),
            table: "users".into(),
            schema: Some("dbo".into()),
        };
        let sql = build_delete_with_output(&delete).unwrap();
        assert_eq!(
            sql,
            "DELETE FROM [dbo].[users] OUTPUT DELETED.* WHERE [id] = 7"
        );
    }

    #[test]
    fn identity_where_clause_composite_uses_and() {
        let identity = RecordIdentity::composite(
            vec!["tenant_id".into(), "user_id".into()],
            vec![Value::Int(1), Value::Int(42)],
        );
        assert_eq!(
            identity_where_clause(&identity).unwrap(),
            "[tenant_id] = 1 AND [user_id] = 42"
        );
    }

    #[test]
    fn identity_where_clause_rejects_non_composite() {
        assert!(identity_where_clause(&RecordIdentity::ObjectId("x".into())).is_err());
        assert!(identity_where_clause(&RecordIdentity::Key("x".into())).is_err());
    }

    #[test]
    fn extract_location_finds_constraint_and_table() {
        let msg = "Violation of PRIMARY KEY constraint 'PK_users'. \
                   Cannot insert duplicate key in object 'dbo.users'. \
                   The duplicate key value is (1).";
        let loc = extract_location_from_message(msg);
        assert_eq!(loc.constraint.as_deref(), Some("PK_users"));
        assert_eq!(loc.schema.as_deref(), Some("dbo"));
        assert_eq!(loc.table.as_deref(), Some("users"));
    }

    #[test]
    fn extract_location_finds_fk_constraint_and_referenced_table_column() {
        let msg = "The INSERT statement conflicted with the FOREIGN KEY \
                   constraint 'FK_orders_user'. The conflict occurred in \
                   database 'app', table 'dbo.users', column 'id'.";
        let loc = extract_location_from_message(msg);
        assert_eq!(loc.constraint.as_deref(), Some("FK_orders_user"));
        assert_eq!(loc.schema.as_deref(), Some("dbo"));
        assert_eq!(loc.table.as_deref(), Some("users"));
        assert_eq!(loc.column.as_deref(), Some("id"));
    }

    #[test]
    fn classify_mssql_code_routes_known_codes() {
        assert_eq!(classify_mssql_code(18456), Some(MssqlErrorClass::Auth));
        assert_eq!(classify_mssql_code(547), Some(MssqlErrorClass::Constraint));
        assert_eq!(classify_mssql_code(2627), Some(MssqlErrorClass::Constraint));
        assert_eq!(classify_mssql_code(208), Some(MssqlErrorClass::NotFound));
        assert_eq!(classify_mssql_code(229), Some(MssqlErrorClass::Permission));
        assert_eq!(classify_mssql_code(102), Some(MssqlErrorClass::Syntax));
        assert_eq!(classify_mssql_code(99999), None);
    }

    #[test]
    fn inject_password_skips_when_already_present() {
        let input = "sqlserver://sa:already@host:1433/db";
        assert_eq!(
            inject_password_into_mssql_uri(input, Some("override")),
            input
        );
    }

    #[test]
    fn inject_password_adds_when_missing() {
        let input = "sqlserver://sa@host:1433/db";
        assert_eq!(
            inject_password_into_mssql_uri(input, Some("p@ss")),
            "sqlserver://sa:p%40ss@host:1433/db"
        );
    }

    fn form_values(pairs: &[(&str, &str)]) -> FormValues {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn build_uri_appends_instance_as_query_param() {
        let driver = MssqlDriver;
        let values = form_values(&[
            ("host", "localhost"),
            ("port", "1433"),
            ("user", "sa"),
            ("database", ""),
            ("instance", "MSSQLSERVER2019"),
        ]);
        let uri = driver.build_uri(&values, "pw").expect("uri");
        assert_eq!(
            uri,
            "sqlserver://sa:pw@localhost:1433/?instance=MSSQLSERVER2019"
        );
    }

    #[test]
    fn build_uri_appends_instance_after_database() {
        let driver = MssqlDriver;
        let values = form_values(&[
            ("host", "localhost"),
            ("port", "1433"),
            ("user", "sa"),
            ("database", "tempdb"),
            ("instance", "SQLEXPRESS"),
        ]);
        let uri = driver.build_uri(&values, "pw").expect("uri");
        assert_eq!(
            uri,
            "sqlserver://sa:pw@localhost:1433/tempdb?instance=SQLEXPRESS"
        );
    }

    #[test]
    fn build_uri_omits_instance_when_empty() {
        let driver = MssqlDriver;
        let values = form_values(&[
            ("host", "localhost"),
            ("port", "1433"),
            ("user", "sa"),
            ("database", "tempdb"),
            ("instance", ""),
        ]);
        let uri = driver.build_uri(&values, "pw").expect("uri");
        assert_eq!(uri, "sqlserver://sa:pw@localhost:1433/tempdb");
    }

    #[test]
    fn parse_uri_extracts_instance_from_backslash_form() {
        let driver = MssqlDriver;
        let parsed = driver
            .parse_uri("sqlserver://sa:pw@localhost\\MSSQLSERVER2019:1433")
            .expect("parsed");
        assert_eq!(parsed.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(parsed.get("port").map(String::as_str), Some("1433"));
        assert_eq!(
            parsed.get("instance").map(String::as_str),
            Some("MSSQLSERVER2019")
        );
    }

    #[test]
    fn parse_uri_extracts_instance_from_query_param() {
        let driver = MssqlDriver;
        let parsed = driver
            .parse_uri("sqlserver://sa:pw@localhost:1433/?instance=SQLEXPRESS")
            .expect("parsed");
        assert_eq!(parsed.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(
            parsed.get("instance").map(String::as_str),
            Some("SQLEXPRESS")
        );
    }

    #[test]
    fn parse_uri_query_param_overrides_backslash_instance() {
        let driver = MssqlDriver;
        let parsed = driver
            .parse_uri("sqlserver://sa:pw@localhost\\IGNORED:1433/?instance=WINS")
            .expect("parsed");
        assert_eq!(parsed.get("instance").map(String::as_str), Some("WINS"));
    }

    #[test]
    fn parse_uri_decodes_password_with_percent_encoded_chars() {
        // ws123456%21 -> ws123456!  ; without this, switching URI -> form
        // leaves the password field empty/stale and the user saves a wrong
        // credential (server returns 18456 / State 8).
        let driver = MssqlDriver;
        let parsed = driver
            .parse_uri("sqlserver://sa:ws123456%21@localhost:1433/")
            .expect("parsed");
        assert_eq!(parsed.get("user").map(String::as_str), Some("sa"));
        assert_eq!(
            parsed.get("password").map(String::as_str),
            Some("ws123456!")
        );
    }

    #[test]
    fn parse_uri_omits_password_when_credentials_have_no_colon() {
        let driver = MssqlDriver;
        let parsed = driver
            .parse_uri("sqlserver://sa@localhost:1433/")
            .expect("parsed");
        assert_eq!(parsed.get("user").map(String::as_str), Some("sa"));
        assert!(!parsed.contains_key("password"));
    }

    fn build_form_values_for_ssl(ssl_mode: Option<&str>) -> FormValues {
        let mut v = form_values(&[("host", "localhost"), ("port", "1433"), ("user", "sa")]);
        if let Some(mode) = ssl_mode {
            v.insert("ssl_mode".to_string(), mode.to_string());
        }
        v
    }

    fn extract_ssl(values: FormValues) -> (String, bool) {
        let driver = MssqlDriver;
        match driver.build_config(&values).expect("config") {
            DbConfig::SqlServer {
                ssl_mode,
                trust_server_certificate,
                ..
            } => (ssl_mode.unwrap_or_default(), trust_server_certificate),
            _ => panic!("expected SqlServer config"),
        }
    }

    #[test]
    fn form_ssl_mode_off_disables_encryption() {
        let (mode, trust) = extract_ssl(build_form_values_for_ssl(Some("off")));
        assert_eq!(mode, "off");
        // trust is irrelevant when encryption is off, but stays `true` so
        // the value never represents "strict" silently.
        assert!(trust);
    }

    #[test]
    fn form_ssl_mode_on_trusts_self_signed() {
        let (mode, trust) = extract_ssl(build_form_values_for_ssl(Some("on")));
        assert_eq!(mode, "on");
        assert!(trust);
    }

    #[test]
    fn form_ssl_mode_required_validates_cert_chain() {
        let (mode, trust) = extract_ssl(build_form_values_for_ssl(Some("required")));
        assert_eq!(mode, "required");
        assert!(!trust);
    }

    #[test]
    fn form_ssl_mode_defaults_to_on_when_unset() {
        let (mode, trust) = extract_ssl(build_form_values_for_ssl(None));
        assert_eq!(mode, "on");
        assert!(trust);
    }

    #[test]
    fn build_then_parse_uri_round_trips_instance() {
        let driver = MssqlDriver;
        let original = form_values(&[
            ("host", "localhost"),
            ("port", "1433"),
            ("user", "sa"),
            ("database", "appdb"),
            ("instance", "MSSQLSERVER2019"),
        ]);
        let uri = driver.build_uri(&original, "pw").expect("uri");
        let parsed = driver.parse_uri(&uri).expect("parsed");
        assert_eq!(parsed.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(parsed.get("port").map(String::as_str), Some("1433"));
        assert_eq!(parsed.get("database").map(String::as_str), Some("appdb"));
        assert_eq!(
            parsed.get("instance").map(String::as_str),
            Some("MSSQLSERVER2019")
        );
    }

    #[test]
    fn parse_mssql_url_accepts_backslash_instance() {
        // We can only assert that parsing succeeds; tiberius `Config` doesn't
        // expose its internal instance/host fields publicly. A successful
        // parse means tiberius will perform SQL Browser instance lookup.
        let result = parse_mssql_url("sqlserver://sa:pw@localhost\\MSSQLSERVER2019:1433");
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    #[test]
    fn parse_mssql_url_accepts_instance_query_param() {
        let result = parse_mssql_url("sqlserver://sa:pw@localhost:1433/?instance=SQLEXPRESS");
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    fn fe_with_code(code: &str) -> FormattedError {
        FormattedError::new("session terminated").with_code(code)
    }

    #[test]
    fn is_kill_error_matches_known_kill_codes() {
        assert!(is_kill_error(&DbError::QueryFailed(fe_with_code("596"))));
        assert!(is_kill_error(&DbError::QueryFailed(fe_with_code("233"))));
        assert!(is_kill_error(&DbError::QueryFailed(fe_with_code("6005"))));
        // Same codes are also recognised when classified as connection errors,
        // since the post-KILL read sometimes comes back as a transport failure.
        assert!(is_kill_error(&DbError::ConnectionFailed(fe_with_code(
            "596"
        ))));
    }

    #[test]
    fn is_kill_error_ignores_unrelated_codes() {
        assert!(!is_kill_error(&DbError::QueryFailed(fe_with_code("547"))));
        assert!(!is_kill_error(&DbError::ConstraintViolation(fe_with_code(
            "2627"
        ))));
        assert!(!is_kill_error(&DbError::SyntaxError(fe_with_code("102"))));
    }

    #[test]
    fn is_kill_error_ignores_errors_without_codes() {
        assert!(!is_kill_error(&DbError::QueryFailed(FormattedError::new(
            "no code"
        ))));
        assert!(!is_kill_error(&DbError::Cancelled));
        assert!(!is_kill_error(&DbError::NotSupported("nope".to_string())));
    }

    fn col(name: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "text".to_string(),
            kind: dbflux_core::ColumnKind::Unknown,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn one_row_set(label: &str) -> (Vec<ColumnMeta>, Vec<Row>) {
        (vec![col(label)], vec![vec![Value::Text(label.to_string())]])
    }

    #[test]
    fn build_multi_result_empty_input_yields_empty_primary() {
        let result = build_multi_result(Vec::new(), std::time::Duration::ZERO);
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert!(!result.has_additional_results());
        assert_eq!(result.result_set_count(), 1);
    }

    #[test]
    fn build_multi_result_single_set_has_no_extras() {
        let result = build_multi_result(vec![one_row_set("first")], std::time::Duration::ZERO);
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert!(!result.has_additional_results());
        assert_eq!(result.result_set_count(), 1);
    }

    #[test]
    fn build_multi_result_multiple_sets_split_last_as_primary_rest_as_extras() {
        let result = build_multi_result(
            vec![one_row_set("a"), one_row_set("b"), one_row_set("c")],
            std::time::Duration::ZERO,
        );

        // Last set becomes the primary (preserves "last statement wins" UX).
        assert_eq!(result.columns[0].name, "c");
        assert_eq!(result.rows[0][0], Value::Text("c".to_string()));

        // Earlier sets land in additional_results in batch order.
        assert_eq!(result.result_set_count(), 3);
        assert_eq!(result.additional_results.len(), 2);
        assert_eq!(result.additional_results[0].columns[0].name, "a");
        assert_eq!(result.additional_results[1].columns[0].name, "b");

        // iter_result_sets yields primary first, then the earlier sets in
        // batch order.
        let names: Vec<String> = result
            .iter_result_sets()
            .map(|r| r.columns[0].name.clone())
            .collect();
        assert_eq!(
            names,
            vec!["c".to_string(), "a".to_string(), "b".to_string()]
        );
    }
}
