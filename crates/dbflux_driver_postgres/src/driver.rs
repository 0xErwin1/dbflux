use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use dbflux_core::{
    generate_create_table, generate_delete_template, generate_drop_table, generate_insert_template,
    generate_select_star, generate_truncate, generate_update_template, sanitize_uri,
    AddEnumValueRequest, AddForeignKeyRequest, CodeGenCapabilities, CodeGenScope, CodeGenerator,
    CodeGeneratorInfo, ColumnInfo, ColumnMeta, Connection, ConnectionErrorFormatter,
    ConnectionProfile, ConstraintInfo, ConstraintKind, CreateIndexRequest, CreateTypeRequest,
    CrudResult, CustomTypeInfo, CustomTypeKind, DatabaseCategory, DatabaseInfo, DbConfig, DbDriver,
    DbError, DbKind, DbSchemaInfo, DescribeRequest, DriverCapabilities, DriverFormDef,
    DriverMetadata, DropForeignKeyRequest, DropIndexRequest, DropTypeRequest, ErrorLocation,
    ExplainRequest, ForeignKeyBuilder, ForeignKeyInfo, FormValues, FormattedError, Icon, IndexData,
    IndexInfo, PlaceholderStyle, QueryCancelHandle, QueryErrorFormatter, QueryGenerator,
    QueryHandle, QueryLanguage, QueryRequest, QueryResult, ReindexRequest, RelationalSchema, Row,
    RowDelete, RowInsert, RowPatch, SchemaFeatures, SchemaForeignKeyBuilder, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, SqlMutationGenerator,
    SqlQueryBuilder, SshTunnelConfig, SslMode, TableInfo, TypeDefinition, Value, ViewInfo,
    POSTGRES_FORM,
};
use dbflux_ssh::SshTunnel;
use native_tls::TlsConnector;
use postgres::{CancelToken as PgCancelToken, Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use uuid::Uuid;

/// PostgreSQL driver metadata.
pub static METADATA: DriverMetadata = DriverMetadata {
    id: "postgres",
    display_name: "PostgreSQL",
    description: "Advanced open-source relational database",
    category: DatabaseCategory::Relational,
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
            | DriverCapabilities::CUSTOM_TYPES.bits()
            | DriverCapabilities::TRIGGERS.bits()
            | DriverCapabilities::STORED_PROCEDURES.bits()
            | DriverCapabilities::SEQUENCES.bits()
            | DriverCapabilities::RETURNING.bits(),
    ),
    default_port: Some(5432),
    uri_scheme: "postgresql",
    icon: Icon::Postgres,
};

/// PostgreSQL SQL dialect implementation.
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn quote_identifier(&self, name: &str) -> String {
        pg_quote_ident(name)
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        pg_qualified_name(schema, table)
    }

    fn value_to_literal(&self, value: &Value) -> String {
        value_to_pg_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        pg_escape_string(s)
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::DollarNumber
    }

    fn supports_returning(&self) -> bool {
        true
    }
}

static POSTGRES_DIALECT: PostgresDialect = PostgresDialect;

// =============================================================================
// PostgreSQL Code Generator
// =============================================================================

pub struct PostgresCodeGenerator;

static POSTGRES_CODE_GENERATOR: PostgresCodeGenerator = PostgresCodeGenerator;

impl PostgresCodeGenerator {
    fn quote(&self, name: &str) -> String {
        POSTGRES_DIALECT.quote_identifier(name)
    }

    fn qualified(&self, schema: Option<&str>, name: &str) -> String {
        POSTGRES_DIALECT.qualified_table(schema, name)
    }
}

impl CodeGenerator for PostgresCodeGenerator {
    fn capabilities(&self) -> CodeGenCapabilities {
        CodeGenCapabilities::POSTGRES_FULL
    }

    fn generate_create_index(&self, req: &CreateIndexRequest) -> Option<String> {
        let unique = if req.unique { "UNIQUE " } else { "" };
        let table = self.qualified(req.schema_name, req.table_name);
        let cols = req
            .columns
            .iter()
            .map(|c| self.quote(c))
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "CREATE {}INDEX {} ON {} ({});",
            unique,
            self.quote(req.index_name),
            table,
            cols
        ))
    }

    fn generate_drop_index(&self, req: &DropIndexRequest) -> Option<String> {
        let index = self.qualified(req.schema_name, req.index_name);
        Some(format!("DROP INDEX {};", index))
    }

    fn generate_reindex(&self, req: &ReindexRequest) -> Option<String> {
        let index = self.qualified(req.schema_name, req.index_name);
        Some(format!("REINDEX INDEX {};", index))
    }

    fn generate_add_foreign_key(&self, req: &AddForeignKeyRequest) -> Option<String> {
        let table = self.qualified(req.schema_name, req.table_name);
        let ref_table = self.qualified(req.ref_schema, req.ref_table);
        let cols = req
            .columns
            .iter()
            .map(|c| self.quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        let ref_cols = req
            .ref_columns
            .iter()
            .map(|c| self.quote(c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "ALTER TABLE {}\n    ADD CONSTRAINT {}\n    FOREIGN KEY ({})\n    REFERENCES {} ({})",
            table,
            self.quote(req.constraint_name),
            cols,
            ref_table,
            ref_cols
        );

        if let Some(on_delete) = req.on_delete {
            sql.push_str(&format!("\n    ON DELETE {}", on_delete));
        }
        if let Some(on_update) = req.on_update {
            sql.push_str(&format!("\n    ON UPDATE {}", on_update));
        }
        sql.push(';');

        Some(sql)
    }

    fn generate_drop_foreign_key(&self, req: &DropForeignKeyRequest) -> Option<String> {
        let table = self.qualified(req.schema_name, req.table_name);
        Some(format!(
            "ALTER TABLE {} DROP CONSTRAINT {};",
            table,
            self.quote(req.constraint_name)
        ))
    }

    fn generate_create_type(&self, req: &CreateTypeRequest) -> Option<String> {
        let type_name = self.qualified(req.schema_name, req.type_name);

        match &req.definition {
            TypeDefinition::Enum { values } => {
                let vals = if values.is_empty() {
                    "'value1', 'value2'".to_string()
                } else {
                    values
                        .iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                Some(format!("CREATE TYPE {} AS ENUM ({});", type_name, vals))
            }

            TypeDefinition::Domain { base_type } => {
                Some(format!("CREATE DOMAIN {} AS {};", type_name, base_type))
            }

            TypeDefinition::Composite => Some(format!(
                "CREATE TYPE {} AS (\n    field1 type1,\n    field2 type2\n);",
                type_name
            )),
        }
    }

    fn generate_drop_type(&self, req: &DropTypeRequest) -> Option<String> {
        let type_name = self.qualified(req.schema_name, req.type_name);
        Some(format!("DROP TYPE {};", type_name))
    }

    fn generate_add_enum_value(&self, req: &AddEnumValueRequest) -> Option<String> {
        let type_name = self.qualified(req.schema_name, req.type_name);
        Some(format!(
            "ALTER TYPE {} ADD VALUE '{}';",
            type_name, req.new_value
        ))
    }
}

// =============================================================================

pub struct PostgresDriver;

impl PostgresDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PostgresDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for PostgresDriver {
    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn metadata(&self) -> &'static DriverMetadata {
        &METADATA
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_postgres_config(&profile.config)?;

        if config.use_uri {
            return self.connect_with_uri(config.uri.as_deref().unwrap_or(""), password);
        }

        if let Some(tunnel_config) = &config.ssh_tunnel {
            self.connect_via_ssh_tunnel(
                tunnel_config,
                ssh_secret,
                &config.host,
                config.port,
                &config.user,
                &config.database,
                password,
                config.ssl_mode,
            )
        } else {
            self.connect_direct(
                &config.host,
                config.port,
                &config.user,
                &config.database,
                password,
                config.ssl_mode,
            )
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }

    fn form_definition(&self) -> &'static DriverFormDef {
        &POSTGRES_FORM
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

            return Ok(DbConfig::Postgres {
                use_uri: true,
                uri,
                host: String::new(),
                port: 5432,
                user: String::new(),
                database: String::new(),
                ssl_mode: SslMode::Prefer,
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

        let database = values
            .get("database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Database is required".to_string()))?
            .clone();

        Ok(DbConfig::Postgres {
            use_uri: false,
            uri: None,
            host,
            port,
            user,
            database,
            ssl_mode: SslMode::Prefer,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::Postgres {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
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
            values.insert("database".to_string(), database.clone());
        }

        values
    }

    fn build_uri(&self, values: &FormValues, password: &str) -> Option<String> {
        let host = values.get("host").map(|s| s.as_str()).unwrap_or("");
        let port = values.get("port").map(|s| s.as_str()).unwrap_or("5432");
        let user = values.get("user").map(|s| s.as_str()).unwrap_or("");
        let database = values.get("database").map(|s| s.as_str()).unwrap_or("");

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

        Some(format!(
            "postgresql://{}{}:{}/{}",
            credentials, host, port, database
        ))
    }

    fn parse_uri(&self, uri: &str) -> Option<FormValues> {
        let stripped = uri
            .strip_prefix("postgresql://")
            .or_else(|| uri.strip_prefix("postgres://"))?;

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
            } else {
                let user = urlencoding::decode(credentials)
                    .unwrap_or_default()
                    .into_owned();
                values.insert("user".to_string(), user);
            }
        }

        let (host_port, database) = if let Some(slash) = host_part.find('/') {
            (&host_part[..slash], &host_part[slash + 1..])
        } else {
            (host_part, "")
        };

        let database = database.split('?').next().unwrap_or(database);
        values.insert("database".to_string(), database.to_string());

        if let Some(colon) = host_port.rfind(':') {
            values.insert("host".to_string(), host_port[..colon].to_string());
            values.insert("port".to_string(), host_port[colon + 1..].to_string());
        } else {
            values.insert("host".to_string(), host_port.to_string());
            values.insert("port".to_string(), "5432".to_string());
        }

        Some(values)
    }
}

struct ExtractedPostgresConfig {
    use_uri: bool,
    uri: Option<String>,
    host: String,
    port: u16,
    user: String,
    database: String,
    ssl_mode: SslMode,
    ssh_tunnel: Option<SshTunnelConfig>,
}

fn extract_postgres_config(config: &DbConfig) -> Result<ExtractedPostgresConfig, DbError> {
    match config {
        DbConfig::Postgres {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            ssl_mode,
            ssh_tunnel,
            ..
        } => Ok(ExtractedPostgresConfig {
            use_uri: *use_uri,
            uri: uri.clone(),
            host: host.clone(),
            port: *port,
            user: user.clone(),
            database: database.clone(),
            ssl_mode: *ssl_mode,
            ssh_tunnel: ssh_tunnel.clone(),
        }),
        _ => Err(DbError::InvalidProfile(
            "Expected PostgreSQL configuration".to_string(),
        )),
    }
}

struct PostgresConnectParams<'a> {
    host: &'a str,
    port: u16,
    user: &'a str,
    password: &'a str,
    database: &'a str,
    ssl_mode: SslMode,
}

fn connect_postgres(params: &PostgresConnectParams) -> Result<Client, DbError> {
    let conn_string = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=30",
        params.host, params.port, params.user, params.password, params.database
    );

    match params.ssl_mode {
        SslMode::Disable => Client::connect(&conn_string, NoTls)
            .map_err(|e| format_pg_error(&e, params.host, params.port)),

        SslMode::Prefer | SslMode::Require => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(params.ssl_mode == SslMode::Prefer)
                .build()
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into())
                })?;

            let tls = MakeTlsConnector::new(connector);

            match Client::connect(&conn_string, tls) {
                Ok(client) => Ok(client),
                Err(_) if params.ssl_mode == SslMode::Prefer => {
                    Client::connect(&conn_string, NoTls)
                        .map_err(|e| format_pg_error(&e, params.host, params.port))
                }
                Err(e) => Err(format_pg_error(&e, params.host, params.port)),
            }
        }
    }
}

impl PostgresDriver {
    fn connect_with_uri(
        &self,
        base_uri: &str,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let uri = inject_password_into_pg_uri(base_uri, password);

        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into()))?;

        let tls = MakeTlsConnector::new(connector);

        let client = match Client::connect(&uri, tls) {
            Ok(c) => c,
            Err(_) => {
                Client::connect(&uri, NoTls).map_err(|e| format_pg_uri_error(&e, base_uri))?
            }
        };

        let cancel_token = client.cancel_token();
        log::info!("[CONNECT] PostgreSQL connection established via URI");

        Ok(Box::new(PostgresConnection {
            client: Mutex::new(client),
            ssh_tunnel: None,
            cancel_token,
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    fn connect_direct(
        &self,
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        ssl_mode: SslMode,
    ) -> Result<Box<dyn Connection>, DbError> {
        log::info!(
            "Connecting directly to PostgreSQL at {}:{} as {} (database: {})",
            host,
            port,
            user,
            database
        );

        let client = connect_postgres(&PostgresConnectParams {
            host,
            port,
            user,
            password: password.unwrap_or(""),
            database,
            ssl_mode,
        })?;

        let cancel_token = client.cancel_token();
        log::info!("Successfully connected to {}:{}", host, port);

        Ok(Box::new(PostgresConnection {
            client: Mutex::new(client),
            ssh_tunnel: None,
            cancel_token,
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_via_ssh_tunnel(
        &self,
        tunnel_config: &SshTunnelConfig,
        ssh_secret: Option<&str>,
        db_host: &str,
        db_port: u16,
        db_user: &str,
        database: &str,
        db_password: Option<&str>,
        ssl_mode: SslMode,
    ) -> Result<Box<dyn Connection>, DbError> {
        let total_start = Instant::now();

        log::info!(
            "[CONNECT] Starting SSH tunnel connection: {}@{}:{} -> {}:{}",
            tunnel_config.user,
            tunnel_config.host,
            tunnel_config.port,
            db_host,
            db_port
        );

        let phase_start = Instant::now();
        let ssh_session = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        log::info!(
            "[CONNECT] SSH session phase completed in {:.2}ms",
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        log::info!("[SSH] Setting up tunnel to {}:{}", db_host, db_port);
        let phase_start = Instant::now();

        let tunnel = SshTunnel::start(ssh_session, db_host.to_string(), db_port)?;
        let local_port = tunnel.local_port();

        log::info!(
            "[SSH] Tunnel ready on 127.0.0.1:{} in {:.2}ms",
            local_port,
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        log::info!("[DB] Connecting to PostgreSQL via tunnel");
        let phase_start = Instant::now();

        let client = connect_postgres(&PostgresConnectParams {
            host: "127.0.0.1",
            port: local_port,
            user: db_user,
            password: db_password.unwrap_or(""),
            database,
            ssl_mode,
        })?;

        let cancel_token = client.cancel_token();

        log::info!(
            "[DB] PostgreSQL connection established in {:.2}ms",
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        log::info!(
            "[CONNECT] Total connection time: {:.2}ms ({}:{} via SSH {})",
            total_start.elapsed().as_secs_f64() * 1000.0,
            db_host,
            db_port,
            tunnel_config.host
        );

        Ok(Box::new(PostgresConnection {
            client: Mutex::new(client),
            ssh_tunnel: Some(tunnel),
            cancel_token,
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }
}

pub struct PostgresConnection {
    client: Mutex<Client>,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
    cancel_token: PgCancelToken,
    active_query: RwLock<Option<Uuid>>,
    cancelled: Arc<AtomicBool>,
}

struct PostgresCancelHandle {
    cancel_token: PgCancelToken,
    cancelled: Arc<AtomicBool>,
}

impl QueryCancelHandle for PostgresCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel query: {}", e);
            DbError::QueryFailed(format!("Failed to cancel query: {}", e).into())
        })?;

        log::info!("[CANCEL] PostgreSQL cancel request sent");
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

const POSTGRES_CODE_GENERATORS: &[CodeGeneratorInfo] = &[
    CodeGeneratorInfo {
        id: "select_star",
        label: "SELECT *",
        scope: CodeGenScope::TableOrView,
        order: 0,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "insert",
        label: "INSERT INTO",
        scope: CodeGenScope::Table,
        order: 5,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "update",
        label: "UPDATE",
        scope: CodeGenScope::Table,
        order: 6,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "delete",
        label: "DELETE",
        scope: CodeGenScope::Table,
        order: 7,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "create_table",
        label: "CREATE TABLE",
        scope: CodeGenScope::Table,
        order: 10,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "truncate",
        label: "TRUNCATE",
        scope: CodeGenScope::Table,
        order: 20,
        destructive: true,
    },
    CodeGeneratorInfo {
        id: "drop_table",
        label: "DROP TABLE",
        scope: CodeGenScope::Table,
        order: 21,
        destructive: true,
    },
];

impl Connection for PostgresConnection {
    fn metadata(&self) -> &'static DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;
        client
            .simple_query("SELECT 1")
            .map_err(|e| format_pg_query_error(&e))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.cancelled.store(false, Ordering::SeqCst);

        let start = Instant::now();
        let query_id = Uuid::new_v4();

        {
            let mut active = self
                .active_query
                .write()
                .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;
            *active = Some(query_id);
        }

        let sql_preview = if req.sql.len() > 80 {
            format!("{}...", &req.sql[..80])
        } else {
            req.sql.clone()
        };
        log::debug!(
            "[QUERY] Executing (id={}): {}",
            query_id,
            sql_preview.replace('\n', " ")
        );

        let (columns, rows) = {
            let mut client = match self.client.lock() {
                Ok(guard) => guard,
                Err(poison_err) => {
                    log::warn!("[CLEANUP] Recovering from poisoned mutex during cleanup");
                    poison_err.into_inner()
                }
            };

            // Prepare the statement first to get column metadata
            let stmt = client.prepare(&req.sql).map_err(|e| {
                if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
                    log::info!("[QUERY] Query {} was cancelled during prepare", query_id);
                    DbError::Cancelled
                } else {
                    format_pg_query_error(&e)
                }
            })?;

            // Extract column metadata from the prepared statement
            let columns: Vec<ColumnMeta> = stmt
                .columns()
                .iter()
                .map(|col| ColumnMeta {
                    name: col.name().to_string(),
                    type_name: col.type_().name().to_string(),
                    nullable: true,
                })
                .collect();

            // Execute the prepared statement
            let rows = client.query(&stmt, &[]).map_err(|e| {
                if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
                    log::info!("[QUERY] Query {} was cancelled", query_id);
                    DbError::Cancelled
                } else {
                    format_pg_query_error(&e)
                }
            })?;

            (columns, rows)
        };

        {
            let mut active = self
                .active_query
                .write()
                .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;
            *active = None;
        }

        let query_time = start.elapsed();

        let result_rows: Vec<Row> = rows
            .iter()
            .take(req.limit.unwrap_or(u32::MAX) as usize)
            .map(|row| {
                (0..columns.len())
                    .map(|i| postgres_value_to_value(row, i))
                    .collect()
            })
            .collect();

        let total_time = start.elapsed();
        log::debug!(
            "[QUERY] Completed in {:.2}ms (query: {:.2}ms, parse: {:.2}ms), {} rows, {} cols",
            total_time.as_secs_f64() * 1000.0,
            query_time.as_secs_f64() * 1000.0,
            (total_time - query_time).as_secs_f64() * 1000.0,
            result_rows.len(),
            columns.len()
        );

        Ok(QueryResult::table(columns, result_rows, None, total_time))
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        if *active != Some(handle.id) {
            return Err(DbError::QueryFailed(
                "No matching active query to cancel".to_string().into(),
            ));
        }

        drop(active);

        log::info!("[CANCEL] Sending cancel request for query {}", handle.id);

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel query: {}", e);
            DbError::QueryFailed(format!("Failed to cancel query: {}", e).into())
        })?;

        log::info!("[CANCEL] Cancel request sent successfully");
        Ok(())
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        let query_id = match *active {
            Some(id) => id,
            None => {
                log::debug!("[CANCEL] No active query to cancel");
                return Ok(());
            }
        };

        drop(active);

        log::info!(
            "[CANCEL] Sending cancel request for active query {}",
            query_id
        );

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel query: {}", e);
            DbError::QueryFailed(format!("Failed to cancel query: {}", e).into())
        })?;

        log::info!("[CANCEL] Cancel request sent successfully");
        Ok(())
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(PostgresCancelHandle {
            cancel_token: self.cancel_token.clone(),
            cancelled: self.cancelled.clone(),
        })
    }

    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        if !self.cancelled.load(Ordering::SeqCst) {
            return Ok(());
        }

        log::info!("[CLEANUP] Running ROLLBACK after cancelled query");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        if let Err(e) = client.simple_query("ROLLBACK") {
            log::warn!(
                "[CLEANUP] ROLLBACK failed (may not have been in transaction): {}",
                e
            );
        }

        self.cancelled.store(false, Ordering::SeqCst);

        log::info!("[CLEANUP] Connection cleanup complete");
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let total_start = Instant::now();
        log::info!("[SCHEMA] Starting schema fetch");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        let phase_start = Instant::now();
        let databases = get_databases(&mut client)?;
        log::info!(
            "[SCHEMA] Fetched {} databases in {:.2}ms",
            databases.len(),
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        let phase_start = Instant::now();
        let current_database = get_current_database(&mut client)?;
        log::info!(
            "[SCHEMA] Fetched current database in {:.2}ms",
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        let phase_start = Instant::now();
        let schemas = get_schemas(&mut client)?;
        let table_count: usize = schemas.iter().map(|s| s.tables.len()).sum();
        let view_count: usize = schemas.iter().map(|s| s.views.len()).sum();
        log::info!(
            "[SCHEMA] Fetched {} schemas ({} tables, {} views) in {:.2}ms",
            schemas.len(),
            table_count,
            view_count,
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        log::info!(
            "[SCHEMA] Total schema fetch time: {:.2}ms",
            total_start.elapsed().as_secs_f64() * 1000.0
        );

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
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        get_databases(&mut client)
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::ConnectionPerDatabase
    }

    fn table_details(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        let schema_name = schema.unwrap_or("public");
        log::info!(
            "[SCHEMA] Fetching details for table: {}.{}",
            schema_name,
            table
        );

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        let columns = get_columns(&mut client, schema_name, table)?;
        let indexes = get_indexes(&mut client, schema_name, table)?;
        let foreign_keys = get_foreign_keys(&mut client, schema_name, table)?;
        let constraints = get_constraints(&mut client, schema_name, table)?;

        log::info!(
            "[SCHEMA] Table {}.{}: {} columns, {} indexes, {} FKs, {} constraints",
            schema_name,
            table,
            columns.len(),
            indexes.len(),
            foreign_keys.len(),
            constraints.len()
        );

        Ok(TableInfo {
            name: table.to_string(),
            schema: Some(schema_name.to_string()),
            columns: Some(columns),
            indexes: Some(IndexData::Relational(indexes)),
            foreign_keys: Some(foreign_keys),
            constraints: Some(constraints),
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
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<CustomTypeInfo>, DbError> {
        let schema_name = schema.unwrap_or("public");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        get_custom_types(&mut client, schema_name)
    }

    fn schema_indexes(
        &self,
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let schema_name = schema.unwrap_or("public");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        get_schema_indexes(&mut client, schema_name)
    }

    fn schema_foreign_keys(
        &self,
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let schema_name = schema.unwrap_or("public");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        get_schema_foreign_keys(&mut client, schema_name)
    }

    fn code_generators(&self) -> &'static [CodeGeneratorInfo] {
        POSTGRES_CODE_GENERATORS
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(generate_select_star(&POSTGRES_DIALECT, table, 100)),
            "insert" => Ok(generate_insert_template(&POSTGRES_DIALECT, table)),
            "update" => Ok(generate_update_template(&POSTGRES_DIALECT, table)),
            "delete" => Ok(generate_delete_template(&POSTGRES_DIALECT, table)),
            "create_table" => Ok(generate_create_table(&POSTGRES_DIALECT, table)),
            "truncate" => Ok(generate_truncate(&POSTGRES_DIALECT, table)),
            "drop_table" => Ok(generate_drop_table(&POSTGRES_DIALECT, table)),
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

        let builder = SqlQueryBuilder::new(&POSTGRES_DIALECT);
        let sql = builder.build_update(patch, true).ok_or_else(|| {
            DbError::QueryFailed("Failed to build UPDATE query".to_string().into())
        })?;

        log::debug!("[UPDATE] Executing: {}", sql);

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        let rows = client
            .query(&sql, &[])
            .map_err(|e| format_pg_query_error(&e))?;

        if rows.is_empty() {
            return Ok(CrudResult::empty());
        }

        let row = &rows[0];
        let returning_row: Row = (0..row.columns().len())
            .map(|i| postgres_value_to_value(row, i))
            .collect();

        Ok(CrudResult::success(returning_row))
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot insert row: no columns specified".to_string().into(),
            ));
        }

        let builder = SqlQueryBuilder::new(&POSTGRES_DIALECT);
        let sql = builder.build_insert(insert, true).ok_or_else(|| {
            DbError::QueryFailed("Failed to build INSERT query".to_string().into())
        })?;

        log::debug!("[INSERT] Executing: {}", sql);

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        let rows = client
            .query(&sql, &[])
            .map_err(|e| format_pg_query_error(&e))?;

        if rows.is_empty() {
            return Ok(CrudResult::empty());
        }

        let row = &rows[0];
        let returning_row: Row = (0..row.columns().len())
            .map(|i| postgres_value_to_value(row, i))
            .collect();

        Ok(CrudResult::success(returning_row))
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot delete row: invalid row identity (missing primary key)"
                    .to_string()
                    .into(),
            ));
        }

        let builder = SqlQueryBuilder::new(&POSTGRES_DIALECT);
        let sql = builder.build_delete(delete, true).ok_or_else(|| {
            DbError::QueryFailed("Failed to build DELETE query".to_string().into())
        })?;

        log::debug!("[DELETE] Executing: {}", sql);

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        let rows = client
            .query(&sql, &[])
            .map_err(|e| format_pg_query_error(&e))?;

        if rows.is_empty() {
            return Ok(CrudResult::empty());
        }

        let row = &rows[0];
        let returning_row: Row = (0..row.columns().len())
            .map(|i| postgres_value_to_value(row, i))
            .collect();

        Ok(CrudResult::success(returning_row))
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        let query = match &request.query {
            Some(q) => q.clone(),
            None => format!(
                "SELECT * FROM {} LIMIT 100",
                request.table.quoted_with(self.dialect())
            ),
        };

        let sql = format!("EXPLAIN (FORMAT JSON, ANALYZE) {}", query);
        self.execute(&QueryRequest::new(sql))
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        let schema = request.table.schema.as_deref().unwrap_or("public");
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = request.table.name.replace('\'', "''");

        let sql = format!(
            "SELECT column_name, data_type, is_nullable, column_default, \
             character_maximum_length \
             FROM information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}' \
             ORDER BY ordinal_position",
            escaped_schema, escaped_table
        );

        self.execute(&QueryRequest::new(sql))
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &POSTGRES_DIALECT
    }

    fn code_generator(&self) -> &dyn CodeGenerator {
        &POSTGRES_CODE_GENERATOR
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&POSTGRES_DIALECT);
        Some(&GENERATOR)
    }
}

fn get_databases(client: &mut Client) -> Result<Vec<DatabaseInfo>, DbError> {
    let current = get_current_database(client)?;

    let rows = client
        .query(
            r#"
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
            "#,
            &[],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let is_current = current.as_ref() == Some(&name);
            DatabaseInfo { name, is_current }
        })
        .collect())
}

fn get_current_database(client: &mut Client) -> Result<Option<String>, DbError> {
    let rows = client
        .query("SELECT current_database()", &[])
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows.first().map(|row| row.get(0)))
}

fn get_schemas(client: &mut Client) -> Result<Vec<DbSchemaInfo>, DbError> {
    let phase_start = Instant::now();
    let schema_rows = client
        .query(
            r#"
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
            "#,
            &[],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    log::info!(
        "[SCHEMA] Found {} schemas in {:.2}ms",
        schema_rows.len(),
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    let mut schemas = Vec::new();

    for row in schema_rows {
        let schema_name: String = row.get(0);
        let schema_start = Instant::now();

        let tables = get_tables_for_schema(client, &schema_name)?;
        let views = get_views_for_schema(client, &schema_name)?;

        log::info!(
            "[SCHEMA] Schema '{}': {} tables, {} views in {:.2}ms",
            schema_name,
            tables.len(),
            views.len(),
            schema_start.elapsed().as_secs_f64() * 1000.0
        );

        schemas.push(DbSchemaInfo {
            name: schema_name,
            tables,
            views,
            custom_types: None,
        });
    }

    Ok(schemas)
}

fn get_tables_for_schema(client: &mut Client, schema: &str) -> Result<Vec<TableInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema = $1
            ORDER BY table_name
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let tables = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            TableInfo {
                name,
                schema: Some(schema.to_string()),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
            }
        })
        .collect();

    Ok(tables)
}

fn get_views_for_schema(client: &mut Client, schema: &str) -> Result<Vec<ViewInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = $1
            ORDER BY table_name
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| ViewInfo {
            name: row.get(0),
            schema: Some(schema.to_string()),
        })
        .collect())
}

#[allow(dead_code)]
fn get_columns(client: &mut Client, schema: &str, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES' as nullable,
                c.column_default,
                COALESCE(
                    (SELECT true FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu
                       ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                     WHERE tc.constraint_type = 'PRIMARY KEY'
                       AND tc.table_schema = c.table_schema
                       AND tc.table_name = c.table_name
                       AND kcu.column_name = c.column_name),
                    false
                ) as is_pk
            FROM information_schema.columns c
            WHERE c.table_schema = $1 AND c.table_name = $2
            ORDER BY c.ordinal_position
            "#,
            &[&schema, &table],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| ColumnInfo {
            name: row.get(0),
            type_name: row.get(1),
            nullable: row.get(2),
            default_value: row.get(3),
            is_primary_key: row.get(4),
        })
        .collect())
}

#[allow(dead_code)]
fn get_all_columns_for_schema(
    client: &mut Client,
    schema: &str,
) -> Result<HashMap<String, Vec<ColumnInfo>>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                c.table_name,
                c.column_name,
                CASE
                    WHEN c.data_type = 'character varying' THEN
                        'varchar' || COALESCE('(' || c.character_maximum_length || ')', '')
                    WHEN c.data_type = 'character' THEN
                        'char' || COALESCE('(' || c.character_maximum_length || ')', '')
                    WHEN c.data_type = 'numeric' AND c.numeric_precision IS NOT NULL THEN
                        'numeric(' || c.numeric_precision ||
                        COALESCE(',' || c.numeric_scale, '') || ')'
                    WHEN c.data_type = 'bit' AND c.character_maximum_length IS NOT NULL THEN
                        'bit(' || c.character_maximum_length || ')'
                    WHEN c.data_type = 'bit varying' AND c.character_maximum_length IS NOT NULL THEN
                        'varbit(' || c.character_maximum_length || ')'
                    WHEN c.data_type = 'time without time zone' AND c.datetime_precision IS NOT NULL
                         AND c.datetime_precision != 6 THEN
                        'time(' || c.datetime_precision || ')'
                    WHEN c.data_type = 'time with time zone' AND c.datetime_precision IS NOT NULL
                         AND c.datetime_precision != 6 THEN
                        'timetz(' || c.datetime_precision || ')'
                    WHEN c.data_type = 'timestamp without time zone' AND c.datetime_precision IS NOT NULL
                         AND c.datetime_precision != 6 THEN
                        'timestamp(' || c.datetime_precision || ')'
                    WHEN c.data_type = 'timestamp with time zone' AND c.datetime_precision IS NOT NULL
                         AND c.datetime_precision != 6 THEN
                        'timestamptz(' || c.datetime_precision || ')'
                    WHEN c.data_type = 'interval' AND c.datetime_precision IS NOT NULL
                         AND c.datetime_precision != 6 THEN
                        'interval(' || c.datetime_precision || ')'
                    WHEN c.data_type = 'ARRAY' THEN
                        c.udt_name
                    ELSE c.data_type
                END as type_name,
                c.is_nullable = 'YES' as nullable,
                c.column_default,
                COALESCE(
                    (SELECT true FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu
                       ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                     WHERE tc.constraint_type = 'PRIMARY KEY'
                       AND tc.table_schema = c.table_schema
                       AND tc.table_name = c.table_name
                       AND kcu.column_name = c.column_name),
                    false
                ) as is_pk
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON c.table_schema = t.table_schema AND c.table_name = t.table_name
            WHERE c.table_schema = $1 AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name, c.ordinal_position
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut result: HashMap<String, Vec<ColumnInfo>> = HashMap::new();

    for row in rows {
        let table_name: String = row.get(0);
        let column = ColumnInfo {
            name: row.get(1),
            type_name: row.get(2),
            nullable: row.get(3),
            default_value: row.get(4),
            is_primary_key: row.get(5),
        };
        result.entry(table_name).or_default().push(column);
    }

    Ok(result)
}

#[allow(dead_code)]
fn get_all_indexes_for_schema(
    client: &mut Client,
    schema: &str,
) -> Result<HashMap<String, Vec<IndexInfo>>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                t.relname as table_name,
                i.relname as index_name,
                array_agg(a.attname ORDER BY k.n) as columns,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON true
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE n.nspname = $1
            GROUP BY t.relname, i.relname, ix.indisunique, ix.indisprimary
            ORDER BY t.relname, i.relname
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut result: HashMap<String, Vec<IndexInfo>> = HashMap::new();

    for row in rows {
        let table_name: String = row.get(0);
        let columns: Vec<String> = row.get(2);
        let index = IndexInfo {
            name: row.get(1),
            columns,
            is_unique: row.get(3),
            is_primary: row.get(4),
        };
        result.entry(table_name).or_default().push(index);
    }

    Ok(result)
}

#[allow(dead_code)]
fn get_indexes(client: &mut Client, schema: &str, table: &str) -> Result<Vec<IndexInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                i.relname as index_name,
                array_agg(a.attname ORDER BY k.n) as columns,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON true
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE n.nspname = $1 AND t.relname = $2
            GROUP BY i.relname, ix.indisunique, ix.indisprimary
            ORDER BY i.relname
            "#,
            &[&schema, &table],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| {
            let columns: Vec<String> = row.get(1);
            IndexInfo {
                name: row.get(0),
                columns,
                is_unique: row.get(2),
                is_primary: row.get(3),
            }
        })
        .collect())
}

fn get_foreign_keys(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ForeignKeyInfo>, DbError> {
    // Use a simpler query that avoids complex array_agg issues
    // Query each FK constraint individually with its columns
    // Cast sql_identifier to text to avoid deserialization issues
    let rows = client
        .query(
            r#"
            SELECT
                kcu.constraint_name::text,
                kcu.column_name::text,
                ccu.table_schema::text as referenced_schema,
                ccu.table_name::text as referenced_table,
                ccu.column_name::text as referenced_column,
                rc.delete_rule::text,
                rc.update_rule::text
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema = tc.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON kcu.constraint_name = ccu.constraint_name
                AND kcu.constraint_schema = ccu.constraint_schema
            JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND kcu.table_schema = $1
                AND kcu.table_name = $2
            ORDER BY kcu.constraint_name, kcu.ordinal_position
            "#,
            &[&schema, &table],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut builder = ForeignKeyBuilder::new();

    for row in &rows {
        let name: String = row.get(0);
        let column: String = row.get(1);
        let referenced_schema: Option<String> = row.get(2);
        let referenced_table: String = row.get(3);
        let referenced_column: String = row.get(4);
        let on_delete: Option<String> =
            row.get::<_, Option<String>>(5).filter(|s| s != "NO ACTION");
        let on_update: Option<String> =
            row.get::<_, Option<String>>(6).filter(|s| s != "NO ACTION");

        builder.add_column(
            name,
            column,
            referenced_schema,
            referenced_table,
            referenced_column,
            on_update,
            on_delete,
        );
    }

    let fks = builder.build_sorted();

    log::debug!(
        "[SCHEMA] get_foreign_keys for {}.{}: {} FKs found",
        schema,
        table,
        fks.len()
    );

    Ok(fks)
}

fn get_constraints(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ConstraintInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                COALESCE(
                    array_agg(kcu.column_name ORDER BY kcu.ordinal_position)
                    FILTER (WHERE kcu.column_name IS NOT NULL),
                    ARRAY[]::text[]
                ) as columns,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.table_schema = $1
                AND tc.table_name = $2
                AND tc.constraint_type IN ('CHECK', 'UNIQUE')
            GROUP BY tc.constraint_name, tc.constraint_type, cc.check_clause
            ORDER BY tc.constraint_type, tc.constraint_name
            "#,
            &[&schema, &table],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let name: String = row.try_get(0).ok()?;
            let constraint_type: String = row.try_get(1).ok()?;
            let columns: Vec<String> = row.try_get(2).ok().unwrap_or_default();
            let check_clause: Option<String> = row.try_get(3).ok().flatten();

            let kind = match constraint_type.as_str() {
                "CHECK" => ConstraintKind::Check,
                "UNIQUE" => ConstraintKind::Unique,
                _ => return None,
            };

            Some(ConstraintInfo {
                name,
                kind,
                columns,
                check_clause,
            })
        })
        .collect())
}

fn get_custom_types(client: &mut Client, schema: &str) -> Result<Vec<CustomTypeInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                t.typname as name,
                n.nspname as schema,
                CASE
                    WHEN t.typtype = 'e' THEN 'enum'
                    WHEN t.typtype = 'd' THEN 'domain'
                    WHEN t.typtype = 'c' THEN 'composite'
                    ELSE 'other'
                END as kind,
                CASE
                    WHEN t.typtype = 'e' THEN (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_enum e WHERE e.enumtypid = t.oid
                    )
                    ELSE NULL
                END as enum_values,
                CASE
                    WHEN t.typtype = 'd' THEN (
                        SELECT bt.typname FROM pg_type bt WHERE bt.oid = t.typbasetype
                    )
                    ELSE NULL
                END as base_type
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = $1
                AND t.typtype IN ('e', 'd', 'c')
                AND NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    WHERE c.reltype = t.oid AND c.relkind = 'r'
                )
            ORDER BY t.typtype, t.typname
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let name: String = row.get(0);
            let schema: String = row.get(1);
            let kind_str: String = row.get(2);
            let enum_values: Option<Vec<String>> = row.get(3);
            let base_type: Option<String> = row.get(4);

            let kind = match kind_str.as_str() {
                "enum" => CustomTypeKind::Enum,
                "domain" => CustomTypeKind::Domain,
                "composite" => CustomTypeKind::Composite,
                _ => return None,
            };

            Some(CustomTypeInfo {
                name,
                schema: Some(schema),
                kind,
                enum_values,
                base_type,
            })
        })
        .collect())
}

/// Convert a Value to a safe PostgreSQL literal string.
///
/// Uses dollar quoting for strings to avoid SQL injection.
fn value_to_pg_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() {
                "'NaN'::float8".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'::float8".to_string()
                } else {
                    "'-Infinity'::float8".to_string()
                }
            } else {
                format!("{}::float8", f)
            }
        }
        Value::Decimal(s) => format!("'{}'::numeric", pg_escape_string(s)),
        Value::Text(s) => pg_quote_string(s),
        Value::Json(s) => format!("{}::jsonb", pg_quote_string(s)),
        Value::Bytes(b) => format!("'\\x{}'::bytea", hex::encode(b)),
        Value::DateTime(dt) => format!("'{}'::timestamptz", dt.to_rfc3339()),
        Value::Date(d) => format!("'{}'::date", d.format("%Y-%m-%d")),
        Value::Time(t) => format!("'{}'::time", t.format("%H:%M:%S%.f")),
        Value::ObjectId(id) => pg_quote_string(id),
        Value::Array(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
            format!("{}::jsonb", pg_quote_string(&json))
        }
        Value::Document(doc) => {
            let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
            format!("{}::jsonb", pg_quote_string(&json))
        }
    }
}

/// Escape a string for use inside a PostgreSQL single-quoted literal.
fn pg_escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Quote a string as a PostgreSQL literal using dollar quoting.
fn pg_quote_string(s: &str) -> String {
    if !s.contains("$$") {
        return format!("$${}$$", s);
    }

    for i in 0..100 {
        let tag = format!("$tag{}$", i);
        if !s.contains(&tag) {
            return format!("{}{}{}", tag, s, tag);
        }
    }

    format!("'{}'", pg_escape_string(s))
}

fn postgres_value_to_value(row: &postgres::Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_();

    match col_type.name() {
        "bool" => row
            .try_get::<_, bool>(idx)
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "int2" => row
            .try_get::<_, i16>(idx)
            .map(|v| Value::Int(v as i64))
            .unwrap_or(Value::Null),
        "int4" => row
            .try_get::<_, i32>(idx)
            .map(|v| Value::Int(v as i64))
            .unwrap_or(Value::Null),
        "int8" => row
            .try_get::<_, i64>(idx)
            .map(Value::Int)
            .unwrap_or(Value::Null),
        "float4" => row
            .try_get::<_, f32>(idx)
            .map(|v| Value::Float(v as f64))
            .unwrap_or(Value::Null),
        "float8" | "numeric" => row
            .try_get::<_, f64>(idx)
            .map(Value::Float)
            .unwrap_or(Value::Null),
        "bytea" => row
            .try_get::<_, Vec<u8>>(idx)
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        _ => row
            .try_get::<_, String>(idx)
            .map(Value::Text)
            .unwrap_or(Value::Null),
    }
}

pub struct PostgresErrorFormatter;

impl PostgresErrorFormatter {
    fn format_postgres_error(e: &postgres::Error) -> FormattedError {
        if let Some(db_err) = e.as_db_error() {
            let mut formatted = FormattedError::new(db_err.message());

            if let Some(detail) = db_err.detail() {
                formatted = formatted.with_detail(detail);
            }

            if let Some(hint) = db_err.hint() {
                formatted = formatted.with_hint(hint);
            }

            formatted = formatted.with_code(db_err.code().code());

            let has_location = db_err.table().is_some()
                || db_err.column().is_some()
                || db_err.constraint().is_some()
                || db_err.schema().is_some();

            if has_location {
                let mut location = ErrorLocation::new();

                if let Some(schema) = db_err.schema() {
                    location = location.with_schema(schema);
                }
                if let Some(table) = db_err.table() {
                    location = location.with_table(table);
                }
                if let Some(column) = db_err.column() {
                    location = location.with_column(column);
                }
                if let Some(constraint) = db_err.constraint() {
                    location = location.with_constraint(constraint);
                }

                formatted = formatted.with_location(location);
            }

            formatted
        } else {
            FormattedError::new(e.to_string())
        }
    }

    fn format_connection_message(source: &str, host: &str, port: u16) -> String {
        if source.contains("timed out") {
            format!(
                "Connection to {}:{} timed out. Check that the host is reachable and the port is open.",
                host, port
            )
        } else if source.contains("Connection refused") {
            format!(
                "Connection refused at {}:{}. Verify PostgreSQL is running and accepting connections.",
                host, port
            )
        } else if source.contains("password authentication failed") {
            "Authentication failed. Check your username and password.".to_string()
        } else if source.contains("does not exist") {
            format!("Database or user does not exist: {}", source)
        } else if source.contains("no pg_hba.conf entry") {
            format!(
                "Server rejected connection from this host. Check pg_hba.conf on {}.",
                host
            )
        } else if source.contains("error connecting to server")
            || source.contains("could not connect")
        {
            format!(
                "Could not connect to {}:{}. The server may be unreachable, behind a firewall, or requires SSH tunnel.",
                host, port
            )
        } else if source.contains("Name or service not known")
            || source.contains("nodename nor servname")
        {
            format!("Could not resolve hostname: {}", host)
        } else {
            format!("Connection error: {}", source)
        }
    }
}

impl QueryErrorFormatter for PostgresErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        if let Some(pg_err) = error.downcast_ref::<postgres::Error>() {
            Self::format_postgres_error(pg_err)
        } else {
            FormattedError::new(error.to_string())
        }
    }
}

impl ConnectionErrorFormatter for PostgresErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        let source = error.to_string();
        let message = Self::format_connection_message(&source, host, port);
        FormattedError::new(message)
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        let source = error.to_string();

        let message = if source.contains("password authentication failed") {
            "Authentication failed. Check your username and password in the URI.".to_string()
        } else if source.contains("does not exist") {
            format!("Database or user does not exist: {}", source)
        } else if source.contains("invalid connection string") {
            format!("Invalid connection URI format: {}", sanitized_uri)
        } else {
            format!("Connection error with URI {}: {}", sanitized_uri, source)
        };

        FormattedError::new(message)
    }
}

static POSTGRES_ERROR_FORMATTER: PostgresErrorFormatter = PostgresErrorFormatter;

fn format_pg_error(e: &postgres::Error, host: &str, port: u16) -> DbError {
    let formatted = POSTGRES_ERROR_FORMATTER.format_connection_error(e, host, port);
    log::error!("PostgreSQL connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

fn format_pg_query_error(e: &postgres::Error) -> DbError {
    let formatted = PostgresErrorFormatter::format_postgres_error(e);
    let message = formatted.to_display_string();
    log::error!("PostgreSQL query failed: {}", message);
    formatted.into_query_error()
}

fn format_pg_uri_error(e: &postgres::Error, uri: &str) -> DbError {
    let sanitized = sanitize_uri(uri);
    let formatted = POSTGRES_ERROR_FORMATTER.format_uri_error(e, &sanitized);
    log::error!("PostgreSQL URI connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

fn inject_password_into_pg_uri(base_uri: &str, password: Option<&str>) -> String {
    let password = match password {
        Some(p) if !p.is_empty() => p,
        _ => return base_uri.to_string(),
    };

    if !base_uri.starts_with("postgresql://") && !base_uri.starts_with("postgres://") {
        return base_uri.to_string();
    }

    let prefix_end = if base_uri.starts_with("postgresql://") {
        13
    } else {
        11
    };

    let rest = &base_uri[prefix_end..];
    let prefix = &base_uri[..prefix_end];

    if let Some(at_pos) = rest.find('@') {
        let user_pass = &rest[..at_pos];
        let after_at = &rest[at_pos..];

        if let Some(colon_pos) = user_pass.find(':') {
            if user_pass[colon_pos + 1..].is_empty() {
                let user = &user_pass[..colon_pos];
                let encoded_password = urlencoding::encode(password);
                return format!("{}{}:{}{}", prefix, user, encoded_password, after_at);
            }
            return base_uri.to_string();
        } else {
            let encoded_password = urlencoding::encode(password);
            return format!("{}{}:{}{}", prefix, user_pass, encoded_password, after_at);
        }
    }

    base_uri.to_string()
}

fn pg_quote_ident(ident: &str) -> String {
    debug_assert!(!ident.is_empty(), "identifier cannot be empty");
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn pg_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", pg_quote_ident(s), pg_quote_ident(name)),
        None => pg_quote_ident(name),
    }
}

fn get_schema_indexes(client: &mut Client, schema: &str) -> Result<Vec<SchemaIndexInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                i.relname::text as index_name,
                t.relname::text as table_name,
                array_agg(a.attname::text ORDER BY array_position(ix.indkey, a.attnum)) as columns,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = $1
                AND t.relkind = 'r'
            GROUP BY i.relname, t.relname, ix.indisunique, ix.indisprimary
            ORDER BY t.relname, i.relname
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let name: String = row.try_get(0).ok()?;
            let table_name: String = row.try_get(1).ok()?;
            let columns: Vec<String> = row.try_get(2).ok()?;
            let is_unique: bool = row.try_get(3).ok()?;
            let is_primary: bool = row.try_get(4).ok()?;

            Some(SchemaIndexInfo {
                name,
                table_name,
                columns,
                is_unique,
                is_primary,
            })
        })
        .collect())
}

fn get_schema_foreign_keys(
    client: &mut Client,
    schema: &str,
) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                kcu.constraint_name::text,
                kcu.table_name::text,
                kcu.column_name::text,
                ccu.table_schema::text as referenced_schema,
                ccu.table_name::text as referenced_table,
                ccu.column_name::text as referenced_column,
                rc.delete_rule::text,
                rc.update_rule::text
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema = tc.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON kcu.constraint_name = ccu.constraint_name
                AND kcu.constraint_schema = ccu.constraint_schema
            JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND kcu.table_schema = $1
            ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut builder = SchemaForeignKeyBuilder::new();

    for row in &rows {
        let name: String = row.get(0);
        let table_name: String = row.get(1);
        let column: String = row.get(2);
        let referenced_schema: Option<String> = row.get(3);
        let referenced_table: String = row.get(4);
        let referenced_column: String = row.get(5);
        let on_delete: Option<String> =
            row.get::<_, Option<String>>(6).filter(|s| s != "NO ACTION");
        let on_update: Option<String> =
            row.get::<_, Option<String>>(7).filter(|s| s != "NO ACTION");

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
