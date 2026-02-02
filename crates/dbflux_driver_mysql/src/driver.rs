use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use std::collections::HashMap;

use dbflux_core::{
    CodeGenScope, CodeGeneratorInfo, ColumnInfo, ColumnMeta, Connection, ConnectionProfile,
    ConstraintInfo, ConstraintKind, CrudResult, DatabaseCategory, DatabaseInfo, DbConfig, DbDriver,
    DbError, DbKind, DbSchemaInfo, DriverCapabilities, DriverFormDef, DriverMetadata,
    ForeignKeyInfo, FormValues, Icon, IndexInfo, MYSQL_FORM, PlaceholderStyle, QueryCancelHandle,
    QueryHandle, QueryLanguage, QueryRequest, QueryResult, RecordIdentity, Row, RowDelete,
    RowInsert, RowPatch, SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy,
    SchemaSnapshot, SqlDialect, SqlQueryBuilder, SshTunnelConfig, SslMode, TableInfo, Value,
    ViewInfo,
};
use dbflux_ssh::SshTunnel;
use mysql::prelude::*;
use mysql::{Conn, Opts, OptsBuilder, SslOpts};

/// MySQL driver metadata.
pub static MYSQL_METADATA: DriverMetadata = DriverMetadata {
    id: "mysql",
    display_name: "MySQL",
    description: "Popular open-source relational database",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::RELATIONAL_BASE.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::FOREIGN_KEYS.bits()
            | DriverCapabilities::TRIGGERS.bits()
            | DriverCapabilities::STORED_PROCEDURES.bits(),
    ),
    default_port: Some(3306),
    uri_scheme: "mysql",
    icon: Icon::Mysql,
};

/// MariaDB driver metadata.
pub static MARIADB_METADATA: DriverMetadata = DriverMetadata {
    id: "mariadb",
    display_name: "MariaDB",
    description: "Community-developed fork of MySQL",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::RELATIONAL_BASE.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::FOREIGN_KEYS.bits()
            | DriverCapabilities::CHECK_CONSTRAINTS.bits()
            | DriverCapabilities::TRIGGERS.bits()
            | DriverCapabilities::STORED_PROCEDURES.bits()
            | DriverCapabilities::SEQUENCES.bits(),
    ),
    default_port: Some(3306),
    uri_scheme: "mariadb",
    icon: Icon::Mariadb,
};

/// MySQL/MariaDB SQL dialect implementation.
pub struct MysqlDialect;

impl SqlDialect for MysqlDialect {
    fn quote_identifier(&self, name: &str) -> String {
        mysql_quote_ident(name)
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        mysql_qualified_name(schema, table)
    }

    fn value_to_literal(&self, value: &Value) -> String {
        value_to_mysql_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        mysql_escape_string(s)
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }
}

static MYSQL_DIALECT: MysqlDialect = MysqlDialect;

pub struct MysqlDriver {
    kind: DbKind,
}

impl MysqlDriver {
    pub fn new(kind: DbKind) -> Self {
        Self { kind }
    }
}

impl DbDriver for MysqlDriver {
    fn kind(&self) -> DbKind {
        self.kind
    }

    fn metadata(&self) -> &'static DriverMetadata {
        match self.kind {
            DbKind::MariaDB => &MARIADB_METADATA,
            _ => &MYSQL_METADATA,
        }
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_mysql_config(&profile.config)?;

        if let Some(tunnel_config) = &config.ssh_tunnel {
            self.connect_via_ssh_tunnel(
                tunnel_config,
                ssh_secret,
                &config.host,
                config.port,
                &config.user,
                config.database.as_deref(),
                password,
                config.ssl_mode,
            )
        } else {
            self.connect_direct(
                &config.host,
                config.port,
                &config.user,
                config.database.as_deref(),
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
        &MYSQL_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
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

        Ok(DbConfig::MySQL {
            host,
            port,
            user,
            database,
            ssl_mode: SslMode::Disable,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::MySQL {
            host,
            port,
            user,
            database,
            ..
        } = config
        {
            values.insert("host".to_string(), host.clone());
            values.insert("port".to_string(), port.to_string());
            values.insert("user".to_string(), user.clone());
            values.insert("database".to_string(), database.clone().unwrap_or_default());
        }

        values
    }
}

struct ExtractedMysqlConfig {
    host: String,
    port: u16,
    user: String,
    database: Option<String>,
    ssl_mode: SslMode,
    ssh_tunnel: Option<SshTunnelConfig>,
}

fn extract_mysql_config(config: &DbConfig) -> Result<ExtractedMysqlConfig, DbError> {
    match config {
        DbConfig::MySQL {
            host,
            port,
            user,
            database,
            ssl_mode,
            ssh_tunnel,
            ..
        } => Ok(ExtractedMysqlConfig {
            host: host.clone(),
            port: *port,
            user: user.clone(),
            database: database.clone(),
            ssl_mode: *ssl_mode,
            ssh_tunnel: ssh_tunnel.clone(),
        }),
        _ => Err(DbError::InvalidProfile(
            "Expected MySQL configuration".to_string(),
        )),
    }
}

fn build_mysql_opts(
    host: &str,
    port: u16,
    user: &str,
    database: Option<&str>,
    password: Option<&str>,
    ssl_mode: SslMode,
) -> Opts {
    let mut builder = OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(password);

    if let Some(db) = database {
        builder = builder.db_name(Some(db));
    }

    // Configure SSL based on mode
    match ssl_mode {
        SslMode::Disable => {
            // No SSL - don't set ssl_opts
        }
        SslMode::Prefer => {
            // Try SSL but accept invalid certs (self-signed, expired, etc.)
            let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
            builder = builder.ssl_opts(ssl_opts);
        }
        SslMode::Require => {
            // SSL required with strict certificate validation
            let ssl_opts = SslOpts::default();
            builder = builder.ssl_opts(ssl_opts);
        }
    }

    builder.into()
}

impl MysqlDriver {
    fn connect_direct(
        &self,
        host: &str,
        port: u16,
        user: &str,
        database: Option<&str>,
        password: Option<&str>,
        ssl_mode: SslMode,
    ) -> Result<Box<dyn Connection>, DbError> {
        log::info!(
            "Connecting directly to MySQL at {}:{} as {} (database: {:?}, ssl: {:?})",
            host,
            port,
            user,
            database,
            ssl_mode
        );

        // Create catalog connection with SSL fallback (this is the first real connection)
        let (opts, catalog_conn) = if ssl_mode == SslMode::Prefer {
            let ssl_opts = build_mysql_opts(host, port, user, database, password, SslMode::Prefer);
            match Conn::new(ssl_opts.clone()) {
                Ok(c) => {
                    log::info!("[SSL] Catalog connection established with SSL (Prefer mode)");
                    (ssl_opts, c)
                }
                Err(ssl_err) => {
                    log::info!(
                        "[SSL] SSL connection failed ({}), falling back to non-SSL",
                        ssl_err
                    );
                    let no_ssl_opts =
                        build_mysql_opts(host, port, user, database, password, SslMode::Disable);
                    let c = Conn::new(no_ssl_opts.clone())
                        .map_err(|e| format_mysql_error(&e, host, port))?;
                    (no_ssl_opts, c)
                }
            }
        } else {
            let opts = build_mysql_opts(host, port, user, database, password, ssl_mode);
            let c = Conn::new(opts.clone()).map_err(|e| format_mysql_error(&e, host, port))?;
            (opts, c)
        };

        log::info!("[CONNECT] Catalog connection established");

        // Create query connection (reusing same opts that worked for catalog)
        let mut query_conn =
            Conn::new(opts.clone()).map_err(|e| format_mysql_error(&e, host, port))?;

        // Get connection ID from query connection for KILL QUERY support
        let query_connection_id: u64 = query_conn
            .query_first("SELECT CONNECTION_ID()")
            .map_err(|e| format_mysql_query_error(&e))?
            .unwrap_or(0);

        log::info!(
            "[CONNECT] Query connection established (id: {})",
            query_connection_id
        );

        Ok(Box::new(MysqlConnection {
            catalog_conn: Mutex::new(catalog_conn),
            query_conn: Mutex::new(QueryConnState {
                conn: query_conn,
                current_database: None,
            }),
            ssh_catalog_tunnel: None,
            ssh_query_tunnel: None,
            query_connection_id,
            kill_opts: opts,
            cancelled: Arc::new(AtomicBool::new(false)),
            kind: self.kind,
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
        database: Option<&str>,
        db_password: Option<&str>,
        ssl_mode: SslMode,
    ) -> Result<Box<dyn Connection>, DbError> {
        let total_start = Instant::now();

        log::info!(
            "[SSH] Starting dual tunnels to {}:{} via {}@{}:{}",
            db_host,
            db_port,
            tunnel_config.user,
            tunnel_config.host,
            tunnel_config.port
        );

        // === Tunnel 1: Catalog connection ===
        log::info!("[SSH] Creating catalog tunnel (session 1/2)");
        let session1 = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        let tunnel1 = SshTunnel::start(session1, db_host.to_string(), db_port)?;
        let local_port1 = tunnel1.local_port();
        log::info!("[SSH] Catalog tunnel on local port {}", local_port1);
        let ssh_catalog_tunnel = Arc::new(std::sync::Mutex::new(tunnel1));

        // Create catalog connection with SSL fallback
        log::info!("[DB] Connecting catalog via tunnel (ssl: {:?})", ssl_mode);
        let (working_ssl_mode, catalog_conn) = if ssl_mode == SslMode::Prefer {
            let ssl_opts = build_mysql_opts(
                "127.0.0.1",
                local_port1,
                db_user,
                database,
                db_password,
                SslMode::Prefer,
            );
            match Conn::new(ssl_opts) {
                Ok(c) => {
                    log::info!("[SSL] Catalog connection established with SSL");
                    (SslMode::Prefer, c)
                }
                Err(ssl_err) => {
                    log::info!("[SSL] SSL failed ({}), falling back to non-SSL", ssl_err);
                    let no_ssl_opts = build_mysql_opts(
                        "127.0.0.1",
                        local_port1,
                        db_user,
                        database,
                        db_password,
                        SslMode::Disable,
                    );
                    let c = Conn::new(no_ssl_opts)
                        .map_err(|e| format_mysql_error(&e, "127.0.0.1", local_port1))?;
                    (SslMode::Disable, c)
                }
            }
        } else {
            let opts = build_mysql_opts(
                "127.0.0.1",
                local_port1,
                db_user,
                database,
                db_password,
                ssl_mode,
            );
            let c =
                Conn::new(opts).map_err(|e| format_mysql_error(&e, "127.0.0.1", local_port1))?;
            (ssl_mode, c)
        };
        log::info!("[CONNECT] Catalog connection established");

        // === Tunnel 2: Query connection ===
        log::info!("[SSH] Creating query tunnel (session 2/2)");
        let session2 = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        let tunnel2 = SshTunnel::start(session2, db_host.to_string(), db_port)?;
        let local_port2 = tunnel2.local_port();
        log::info!("[SSH] Query tunnel on local port {}", local_port2);
        let ssh_query_tunnel = Arc::new(std::sync::Mutex::new(tunnel2));

        // Create query connection using the SSL mode that worked for catalog
        let query_opts = build_mysql_opts(
            "127.0.0.1",
            local_port2,
            db_user,
            database,
            db_password,
            working_ssl_mode,
        );
        let mut query_conn = Conn::new(query_opts.clone())
            .map_err(|e| format_mysql_error(&e, "127.0.0.1", local_port2))?;

        // Get connection ID for KILL QUERY support
        let query_connection_id: u64 = query_conn
            .query_first("SELECT CONNECTION_ID()")
            .map_err(|e| format_mysql_query_error(&e))?
            .unwrap_or(0);

        log::info!(
            "[CONNECT] Query connection established (id: {})",
            query_connection_id
        );

        log::info!(
            "[CONNECT] Total connection time: {:.2}ms ({}:{} via SSH {})",
            total_start.elapsed().as_secs_f64() * 1000.0,
            db_host,
            db_port,
            tunnel_config.host
        );

        Ok(Box::new(MysqlConnection {
            catalog_conn: Mutex::new(catalog_conn),
            query_conn: Mutex::new(QueryConnState {
                conn: query_conn,
                current_database: None,
            }),
            ssh_catalog_tunnel: Some(ssh_catalog_tunnel),
            ssh_query_tunnel: Some(ssh_query_tunnel),
            query_connection_id,
            kill_opts: query_opts, // Use query tunnel's opts for KILL
            cancelled: Arc::new(AtomicBool::new(false)),
            kind: self.kind,
        }))
    }
}

fn format_mysql_error(e: &mysql::Error, host: &str, port: u16) -> DbError {
    let msg = e.to_string();

    if msg.contains("Connection refused") {
        DbError::ConnectionFailed(format!(
            "Connection refused at {}:{}. Is MySQL running?",
            host, port
        ))
    } else if msg.contains("Access denied") {
        DbError::ConnectionFailed(
            "Access denied for user. Check username and password.".to_string(),
        )
    } else if msg.contains("Unknown database") {
        DbError::ConnectionFailed("Database does not exist.".to_string())
    } else if msg.contains("caching_sha2_password")
        || msg.contains("Authentication requires secure connection")
    {
        // MySQL 8+ with caching_sha2_password requires SSL for initial authentication
        DbError::ConnectionFailed(
            "Authentication failed. MySQL 8+ requires SSL for initial authentication \
             with caching_sha2_password. Try changing SSL mode to 'Require' or 'Prefer'."
                .to_string(),
        )
    } else {
        DbError::ConnectionFailed(msg)
    }
}

fn format_mysql_query_error(e: &mysql::Error) -> DbError {
    let message = e.to_string();
    log::error!("MySQL query failed: {}", message);
    DbError::QueryFailed(message)
}

/// State for the query connection, bundled in a single mutex to avoid deadlocks.
struct QueryConnState {
    conn: Conn,
    current_database: Option<String>,
}

pub struct MysqlConnection {
    /// Connection for catalog/schema operations (schema browsing, table details).
    catalog_conn: Mutex<Conn>,

    /// Connection for query execution (editor queries, table browser).
    query_conn: Mutex<QueryConnState>,

    /// SSH tunnel for catalog connection (kept alive while connection exists).
    #[allow(dead_code)]
    ssh_catalog_tunnel: Option<Arc<std::sync::Mutex<SshTunnel>>>,

    /// SSH tunnel for query connection (kept alive while connection exists).
    #[allow(dead_code)]
    ssh_query_tunnel: Option<Arc<std::sync::Mutex<SshTunnel>>>,

    /// Connection ID of the query connection (for KILL QUERY).
    query_connection_id: u64,

    kill_opts: Opts,
    cancelled: Arc<AtomicBool>,
    kind: DbKind,
}

struct MysqlCancelHandle {
    kill_opts: Opts,
    query_connection_id: u64,
    cancelled: Arc<AtomicBool>,
}

impl QueryCancelHandle for MysqlCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        // Open a separate connection to send KILL QUERY
        let mut kill_conn = Conn::new(self.kill_opts.clone())
            .map_err(|e| DbError::QueryFailed(format!("Failed to open kill connection: {}", e)))?;

        // Try KILL QUERY first (just cancels the query)
        let kill_query = format!("KILL QUERY {}", self.query_connection_id);
        match kill_conn.query_drop(&kill_query) {
            Ok(_) => {
                log::info!(
                    "[CANCEL] KILL QUERY {} sent successfully",
                    self.query_connection_id
                );
                Ok(())
            }
            Err(e) => {
                // If KILL QUERY fails (e.g., no permission), try KILL (kills whole connection)
                log::warn!("[CANCEL] KILL QUERY failed ({}), trying KILL...", e);
                let kill_conn_cmd = format!("KILL {}", self.query_connection_id);
                kill_conn.query_drop(&kill_conn_cmd).map_err(|e2| {
                    log::error!("[CANCEL] Both KILL QUERY and KILL failed: {}", e2);
                    DbError::QueryFailed(format!(
                        "Permission denied to cancel query. KILL QUERY: {}, KILL: {}",
                        e, e2
                    ))
                })
            }
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

const MYSQL_CODE_GENERATORS: &[CodeGeneratorInfo] = &[
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

impl Connection for MysqlConnection {
    fn metadata(&self) -> &'static DriverMetadata {
        match self.kind {
            DbKind::MariaDB => &MARIADB_METADATA,
            _ => &MYSQL_METADATA,
        }
    }

    fn ping(&self) -> Result<(), DbError> {
        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        conn.query_drop("SELECT 1")
            .map_err(|e| format_mysql_query_error(&e))
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.cancelled.store(false, Ordering::SeqCst);

        let start = Instant::now();

        let sql_preview = if req.sql.len() > 80 {
            format!("{}...", &req.sql[..80])
        } else {
            req.sql.clone()
        };
        log::debug!("[QUERY] Executing: {}", sql_preview.replace('\n', " "));

        let mut state = match self.query_conn.lock() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("[CLEANUP] Recovering from poisoned mutex");
                poison_err.into_inner()
            }
        };

        // Switch database if needed (USE statement)
        if let Some(ref db) = req.database
            && state.current_database.as_ref() != Some(db)
        {
            log::debug!("[USE] Switching to database: {}", db);
            state
                .conn
                .query_drop(format!("USE `{}`", db))
                .map_err(|e| DbError::QueryFailed(format!("USE database failed: {}", e)))?;
            state.current_database = Some(db.clone());
        }

        // Prepare the statement to get column metadata
        let stmt = state
            .conn
            .prep(&req.sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        // Extract column metadata from the prepared statement
        let columns: Vec<ColumnMeta> = stmt
            .columns()
            .iter()
            .map(|col| ColumnMeta {
                name: col.name_str().to_string(),
                type_name: format!("{:?}", col.column_type()),
                nullable: true,
            })
            .collect();

        // Execute the prepared statement
        let result: Result<Vec<mysql::Row>, mysql::Error> = state.conn.exec(&stmt, ());

        let query_time = start.elapsed();

        match result {
            Ok(rows) => {
                if rows.is_empty() {
                    // Check if it was a SELECT that returned 0 rows vs an INSERT/UPDATE
                    let sql_upper = req.sql.trim().to_uppercase();
                    if sql_upper.starts_with("SELECT")
                        || sql_upper.starts_with("SHOW")
                        || sql_upper.starts_with("DESCRIBE")
                    {
                        log::debug!(
                            "[QUERY] Completed in {:.2}ms, 0 rows",
                            query_time.as_secs_f64() * 1000.0
                        );
                        return Ok(QueryResult {
                            columns,
                            rows: Vec::new(),
                            affected_rows: None,
                            execution_time: query_time,
                        });
                    } else {
                        // Non-SELECT query, get affected rows from conn
                        let affected = state.conn.affected_rows();
                        log::debug!(
                            "[QUERY] Completed in {:.2}ms, {} rows affected",
                            query_time.as_secs_f64() * 1000.0,
                            affected
                        );
                        return Ok(QueryResult {
                            columns,
                            rows: Vec::new(),
                            affected_rows: Some(affected),
                            execution_time: query_time,
                        });
                    }
                }

                // Convert rows
                let result_rows: Vec<Row> = rows
                    .iter()
                    .map(|row| {
                        let row_cols = row.columns_ref();
                        (0..columns.len())
                            .map(|i| mysql_value_to_value(row, i, &row_cols[i]))
                            .collect()
                    })
                    .collect();

                log::debug!(
                    "[QUERY] Completed in {:.2}ms, {} rows",
                    query_time.as_secs_f64() * 1000.0,
                    result_rows.len()
                );

                Ok(QueryResult {
                    columns,
                    rows: result_rows,
                    affected_rows: None,
                    execution_time: query_time,
                })
            }
            Err(e) => {
                if self.cancelled.load(Ordering::SeqCst) {
                    return Err(DbError::Cancelled);
                }
                Err(format_mysql_query_error(&e))
            }
        }
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        let handle = MysqlCancelHandle {
            kill_opts: self.kill_opts.clone(),
            query_connection_id: self.query_connection_id,
            cancelled: self.cancelled.clone(),
        };
        handle.cancel()
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(MysqlCancelHandle {
            kill_opts: self.kill_opts.clone(),
            query_connection_id: self.query_connection_id,
            cancelled: self.cancelled.clone(),
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        self.cancel_active()
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let databases = self.list_databases()?;
        log::info!("[SCHEMA] Found {} databases", databases.len());

        Ok(SchemaSnapshot {
            databases,
            current_database: None,
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        log::info!("[SCHEMA] Fetching schema for database: {}", database);

        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        // Fetch tables (shallow - without columns/indexes)
        let tables = fetch_tables_shallow(&mut conn, database)?;
        log::info!("[SCHEMA] Found {} tables in {}", tables.len(), database);

        // Fetch views
        let views = fetch_views(&mut conn, database)?;
        log::info!("[SCHEMA] Found {} views in {}", views.len(), database);

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
        _schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        log::info!(
            "[SCHEMA] Fetching details for table: {}.{}",
            database,
            table
        );

        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let columns = fetch_columns(&mut conn, database, table)?;
        let indexes = fetch_indexes(&mut conn, database, table)?;
        let foreign_keys = fetch_foreign_keys(&mut conn, database, table)?;
        let constraints = fetch_constraints(&mut conn, database, table)?;

        log::info!(
            "[SCHEMA] Table {}.{}: {} columns, {} indexes, {} FKs, {} constraints",
            database,
            table,
            columns.len(),
            indexes.len(),
            foreign_keys.len(),
            constraints.len()
        );

        Ok(TableInfo {
            name: table.to_string(),
            schema: Some(database.to_string()),
            columns: Some(columns),
            indexes: Some(indexes),
            foreign_keys: Some(foreign_keys),
            constraints: Some(constraints),
        })
    }

    fn view_details(
        &self,
        database: &str,
        _schema: Option<&str>,
        view: &str,
    ) -> Result<ViewInfo, DbError> {
        log::info!("[SCHEMA] Fetching details for view: {}.{}", database, view);

        // Views don't have columns/indexes in our model, just return basic info
        Ok(ViewInfo {
            name: view.to_string(),
            schema: Some(database.to_string()),
        })
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let databases: Vec<String> = conn
            .query("SHOW DATABASES")
            .map_err(|e| format_mysql_query_error(&e))?;

        Ok(databases
            .into_iter()
            .filter(|db| {
                db != "information_schema"
                    && db != "mysql"
                    && db != "performance_schema"
                    && db != "sys"
            })
            .map(|name| DatabaseInfo {
                name,
                is_current: false,
            })
            .collect())
    }

    fn kind(&self) -> DbKind {
        self.kind
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn code_generators(&self) -> &'static [CodeGeneratorInfo] {
        MYSQL_CODE_GENERATORS
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(mysql_generate_select_star(table)),
            "insert" => Ok(mysql_generate_insert(table)),
            "update" => Ok(mysql_generate_update(table)),
            "delete" => Ok(mysql_generate_delete(table)),
            "create_table" => self.mysql_generate_create_table(table),
            "truncate" => Ok(mysql_generate_truncate(table)),
            "drop_table" => Ok(mysql_generate_drop_table(table)),
            _ => Err(DbError::NotSupported(format!(
                "Unknown generator: {}",
                generator_id
            ))),
        }
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        let mut state = self
            .query_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        // Skip if already on the same database
        if state.current_database.as_deref() == database {
            return Ok(());
        }

        if let Some(db) = database {
            log::info!("[USE] Switching to database: {}", db);
            state
                .conn
                .query_drop(format!("USE `{}`", db))
                .map_err(|e| DbError::QueryFailed(format!("USE database failed: {}", e)))?;
        }

        state.current_database = database.map(|s| s.to_string());
        Ok(())
    }

    fn active_database(&self) -> Option<String> {
        self.query_conn
            .lock()
            .ok()
            .and_then(|state| state.current_database.clone())
    }

    fn schema_indexes(
        &self,
        database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        fetch_schema_indexes(&mut conn, database)
    }

    fn schema_foreign_keys(
        &self,
        database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        fetch_schema_foreign_keys(&mut conn, database)
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        if !patch.identity.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot update row: invalid row identity (missing primary key)".to_string(),
            ));
        }

        if !patch.has_changes() {
            return Err(DbError::QueryFailed("No changes to save".to_string()));
        }

        let builder = SqlQueryBuilder::new(&MYSQL_DIALECT);

        let update_sql = builder
            .build_update(patch, false)
            .ok_or_else(|| DbError::QueryFailed("Failed to build UPDATE query".to_string()))?;
        let update_sql = format!("{} LIMIT 1", update_sql);

        log::debug!("[UPDATE] Executing: {}", update_sql);

        let mut state = self
            .query_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        if let Some(ref db) = patch.schema
            && state.current_database.as_ref() != Some(db)
        {
            log::debug!("[USE] Switching to database: {}", db);
            state
                .conn
                .query_drop(format!("USE `{}`", db))
                .map_err(|e| DbError::QueryFailed(format!("USE database failed: {}", e)))?;
            state.current_database = Some(db.clone());
        }

        state
            .conn
            .query_drop(&update_sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        let affected = state.conn.affected_rows();

        if affected == 0 {
            return Ok(CrudResult::empty());
        }

        let select_sql = builder
            .build_select_by_identity(patch.schema.as_deref(), &patch.table, &patch.identity)
            .ok_or_else(|| DbError::QueryFailed("Failed to build SELECT query".to_string()))?;

        log::debug!("[UPDATE] Re-querying: {}", select_sql);

        let rows: Vec<mysql::Row> = state
            .conn
            .query(&select_sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        if let Some(row) = rows.first() {
            let row_cols = row.columns_ref();
            let returning_row: Row = (0..row_cols.len())
                .map(|i| mysql_value_to_value(row, i, &row_cols[i]))
                .collect();
            Ok(CrudResult::success(returning_row))
        } else {
            Ok(CrudResult::new(affected, None))
        }
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot insert row: no columns specified".to_string(),
            ));
        }

        let builder = SqlQueryBuilder::new(&MYSQL_DIALECT);

        let insert_sql = builder
            .build_insert(insert, false)
            .ok_or_else(|| DbError::QueryFailed("Failed to build INSERT query".to_string()))?;

        log::debug!("[INSERT] Executing: {}", insert_sql);

        let mut state = self
            .query_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        if let Some(ref db) = insert.schema
            && state.current_database.as_ref() != Some(db)
        {
            log::debug!("[USE] Switching to database: {}", db);
            state
                .conn
                .query_drop(format!("USE `{}`", db))
                .map_err(|e| DbError::QueryFailed(format!("USE database failed: {}", e)))?;
            state.current_database = Some(db.clone());
        }

        state
            .conn
            .query_drop(&insert_sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        let last_id = state.conn.last_insert_id();

        let select_sql = if last_id > 0 {
            let first_col = insert
                .columns
                .first()
                .map(|c| MYSQL_DIALECT.quote_identifier(c))
                .unwrap_or_else(|| "`id`".to_string());
            let table = MYSQL_DIALECT.qualified_table(insert.schema.as_deref(), &insert.table);

            format!(
                "SELECT * FROM {} WHERE {} = {} LIMIT 1",
                table, first_col, last_id
            )
        } else {
            let identity = RecordIdentity::composite(
                insert.columns.clone(),
                insert.values.clone(),
            );
            builder
                .build_select_by_identity(insert.schema.as_deref(), &insert.table, &identity)
                .ok_or_else(|| DbError::QueryFailed("Failed to build SELECT query".to_string()))?
        };

        log::debug!("[INSERT] Re-querying: {}", select_sql);

        let rows: Vec<mysql::Row> = state
            .conn
            .query(&select_sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        if let Some(row) = rows.first() {
            let row_cols = row.columns_ref();
            let returning_row: Row = (0..row_cols.len())
                .map(|i| mysql_value_to_value(row, i, &row_cols[i]))
                .collect();
            Ok(CrudResult::success(returning_row))
        } else {
            Ok(CrudResult::new(1, None))
        }
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot delete row: invalid row identity (missing primary key)".to_string(),
            ));
        }

        let builder = SqlQueryBuilder::new(&MYSQL_DIALECT);

        let select_sql = builder
            .build_select_by_identity(delete.schema.as_deref(), &delete.table, &delete.identity)
            .ok_or_else(|| DbError::QueryFailed("Failed to build SELECT query".to_string()))?;

        log::debug!("[DELETE] Fetching row: {}", select_sql);

        let mut state = self
            .query_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        if let Some(ref db) = delete.schema
            && state.current_database.as_ref() != Some(db)
        {
            log::debug!("[USE] Switching to database: {}", db);
            state
                .conn
                .query_drop(format!("USE `{}`", db))
                .map_err(|e| DbError::QueryFailed(format!("USE database failed: {}", e)))?;
            state.current_database = Some(db.clone());
        }

        let rows: Vec<mysql::Row> = state
            .conn
            .query(&select_sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        let returning_row = rows.first().map(|row| {
            let row_cols = row.columns_ref();
            (0..row_cols.len())
                .map(|i| mysql_value_to_value(row, i, &row_cols[i]))
                .collect::<Row>()
        });

        let delete_sql = builder
            .build_delete(delete, false)
            .ok_or_else(|| DbError::QueryFailed("Failed to build DELETE query".to_string()))?;
        let delete_sql = format!("{} LIMIT 1", delete_sql);

        log::debug!("[DELETE] Executing: {}", delete_sql);

        state
            .conn
            .query_drop(&delete_sql)
            .map_err(|e| format_mysql_query_error(&e))?;

        let affected = state.conn.affected_rows();

        if affected == 0 {
            return Ok(CrudResult::empty());
        }

        Ok(CrudResult::new(affected, returning_row))
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &MYSQL_DIALECT
    }
}

fn mysql_value_to_value(row: &mysql::Row, idx: usize, col: &mysql::Column) -> Value {
    use mysql::consts::{ColumnFlags, ColumnType};

    let col_type = col.column_type();

    // TINYINT(1) is MySQL's boolean type
    // column_length() returns the display width; for TINYINT(1) it's 1
    if col_type == ColumnType::MYSQL_TYPE_TINY
        && col.column_length() == 1
        && let Some(val) = row.get_opt::<Option<i8>, _>(idx)
    {
        match val {
            Ok(Some(v)) => return Value::Bool(v != 0),
            Ok(None) => return Value::Null,
            Err(_) => {}
        }
    }

    // UNSIGNED BIGINT can exceed i64::MAX, handle specially
    if col_type == ColumnType::MYSQL_TYPE_LONGLONG
        && col.flags().contains(ColumnFlags::UNSIGNED_FLAG)
        && let Some(val) = row.get_opt::<Option<u64>, _>(idx)
    {
        match val {
            Ok(Some(v)) => {
                // If it fits in i64, use Int; otherwise convert to Text
                return if v <= i64::MAX as u64 {
                    Value::Int(v as i64)
                } else {
                    Value::Text(v.to_string())
                };
            }
            Ok(None) => return Value::Null,
            Err(_) => {}
        }
    }

    // Try signed integer (covers most integer types)
    if let Some(val) = row.get_opt::<Option<i64>, _>(idx) {
        match val {
            Ok(Some(v)) => return Value::Int(v),
            Ok(None) => return Value::Null,
            Err(_) => {}
        }
    }

    if let Some(val) = row.get_opt::<Option<f64>, _>(idx) {
        match val {
            Ok(Some(v)) => return Value::Float(v),
            Ok(None) => return Value::Null,
            Err(_) => {}
        }
    }

    if let Some(val) = row.get_opt::<Option<String>, _>(idx) {
        match val {
            Ok(Some(v)) => return Value::Text(v),
            Ok(None) => return Value::Null,
            Err(_) => {}
        }
    }

    if let Some(val) = row.get_opt::<Option<Vec<u8>>, _>(idx) {
        match val {
            Ok(Some(v)) => return Value::Bytes(v),
            Ok(None) => return Value::Null,
            Err(_) => {}
        }
    }

    // Fallback: try to get as string
    row.get_opt::<Option<String>, _>(idx)
        .and_then(|r| r.ok())
        .map(|opt| opt.map(Value::Text).unwrap_or(Value::Null))
        .unwrap_or(Value::Null)
}

/// Convert a Value to a safe MySQL literal string.
fn value_to_mysql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                // MySQL doesn't have NaN/Infinity, store as NULL
                "NULL".to_string()
            } else {
                f.to_string()
            }
        }
        Value::Decimal(s) => format!("'{}'", mysql_escape_string(s)),
        Value::Text(s) => format!("'{}'", mysql_escape_string(s)),
        Value::Json(s) => format!("'{}'", mysql_escape_string(s)),
        Value::Bytes(b) => format!("X'{}'", hex::encode(b)),
        Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S")),
        Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        Value::Time(t) => format!("'{}'", t.format("%H:%M:%S")),
        Value::ObjectId(id) => format!("'{}'", mysql_escape_string(id)),
        Value::Array(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
            format!("'{}'", mysql_escape_string(&json))
        }
        Value::Document(doc) => {
            let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
            format!("'{}'", mysql_escape_string(&json))
        }
    }
}

/// Quote an identifier (table/column name) for MySQL using backticks.
fn mysql_quote_ident(ident: &str) -> String {
    debug_assert!(!ident.is_empty(), "identifier cannot be empty");
    format!("`{}`", ident.replace('`', "``"))
}

/// Build a qualified table name for MySQL.
fn mysql_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", mysql_quote_ident(s), mysql_quote_ident(name)),
        None => mysql_quote_ident(name),
    }
}

/// Escape a string for use inside a MySQL single-quoted literal.
fn mysql_escape_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('"', "\\\"")
        .replace('\0', "\\0")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn fetch_tables_shallow(conn: &mut Conn, database: &str) -> Result<Vec<TableInfo>, DbError> {
    let query = format!(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        "#,
        database
    );

    let table_names: Vec<String> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    Ok(table_names
        .into_iter()
        .map(|name| TableInfo {
            name,
            schema: Some(database.to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
        })
        .collect())
}

fn fetch_views(conn: &mut Conn, database: &str) -> Result<Vec<ViewInfo>, DbError> {
    let query = format!(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{}'
          AND table_type = 'VIEW'
        ORDER BY table_name
        "#,
        database
    );

    let view_names: Vec<String> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    Ok(view_names
        .into_iter()
        .map(|name| ViewInfo {
            name,
            schema: Some(database.to_string()),
        })
        .collect())
}

fn fetch_columns(conn: &mut Conn, database: &str, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
    let query = format!(
        r#"
        SELECT
            column_name,
            column_type,
            is_nullable,
            column_default,
            column_key
        FROM information_schema.columns
        WHERE table_schema = '{}'
          AND table_name = '{}'
        ORDER BY ordinal_position
        "#,
        database, table
    );

    let rows: Vec<(String, String, String, Option<String>, String)> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    Ok(rows
        .into_iter()
        .map(|(name, type_name, nullable, default, key)| ColumnInfo {
            name,
            type_name,
            nullable: nullable == "YES",
            default_value: default,
            is_primary_key: key == "PRI",
        })
        .collect())
}

fn fetch_indexes(conn: &mut Conn, database: &str, table: &str) -> Result<Vec<IndexInfo>, DbError> {
    let query = format!("SHOW INDEX FROM `{}`.`{}`", database, table);

    let rows: Vec<mysql::Row> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    let mut indexes_map: std::collections::HashMap<String, IndexInfo> =
        std::collections::HashMap::new();

    for row in rows {
        let key_name: String = row.get("Key_name").unwrap_or_default();
        let column_name: String = row.get("Column_name").unwrap_or_default();
        let non_unique: i32 = row.get("Non_unique").unwrap_or(1);

        let entry = indexes_map
            .entry(key_name.clone())
            .or_insert_with(|| IndexInfo {
                name: key_name,
                columns: Vec::new(),
                is_unique: non_unique == 0,
                is_primary: false,
            });

        entry.columns.push(column_name);
    }

    // Mark PRIMARY as primary
    if let Some(pk) = indexes_map.get_mut("PRIMARY") {
        pk.is_primary = true;
    }

    Ok(indexes_map.into_values().collect())
}

// Code generators

fn get_schema_prefix(table: &TableInfo) -> String {
    table
        .schema
        .as_ref()
        .map(|s| format!("`{}`.", s))
        .unwrap_or_default()
}

fn mysql_generate_select_star(table: &TableInfo) -> String {
    format!(
        "SELECT *\nFROM {}`{}`\nLIMIT 100;",
        get_schema_prefix(table),
        table.name
    )
}

fn mysql_generate_insert(table: &TableInfo) -> String {
    let cols = table.columns.as_deref().unwrap_or(&[]);
    let columns: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    let placeholders: Vec<&str> = cols.iter().map(|_| "?").collect();

    format!(
        "INSERT INTO {}`{}` ({})\nVALUES ({});",
        get_schema_prefix(table),
        table.name,
        columns.join(", "),
        placeholders.join(", ")
    )
}

fn mysql_generate_update(table: &TableInfo) -> String {
    let cols = table.columns.as_deref().unwrap_or(&[]);
    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();

    let set_clause: String = cols
        .iter()
        .filter(|c| !c.is_primary_key)
        .map(|c| format!("`{}` = ?", c.name))
        .collect::<Vec<_>>()
        .join(",\n    ");

    let where_clause = if pk_columns.is_empty() {
        "1 = 0 -- WARNING: No primary key found".to_string()
    } else {
        pk_columns
            .iter()
            .map(|c| format!("`{}` = ?", c.name))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!(
        "UPDATE {}`{}`\nSET {}\nWHERE {};",
        get_schema_prefix(table),
        table.name,
        set_clause,
        where_clause
    )
}

fn mysql_generate_delete(table: &TableInfo) -> String {
    let cols = table.columns.as_deref().unwrap_or(&[]);
    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();

    let where_clause = if pk_columns.is_empty() {
        "1 = 0 -- WARNING: No primary key found".to_string()
    } else {
        pk_columns
            .iter()
            .map(|c| format!("`{}` = ?", c.name))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!(
        "DELETE FROM {}`{}`\nWHERE {};",
        get_schema_prefix(table),
        table.name,
        where_clause
    )
}

fn mysql_generate_truncate(table: &TableInfo) -> String {
    format!(
        "TRUNCATE TABLE {}`{}`;",
        get_schema_prefix(table),
        table.name
    )
}

fn mysql_generate_drop_table(table: &TableInfo) -> String {
    format!("DROP TABLE {}`{}`;", get_schema_prefix(table), table.name)
}

impl MysqlConnection {
    fn mysql_generate_create_table(&self, table: &TableInfo) -> Result<String, DbError> {
        let mut conn = self
            .catalog_conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let schema_prefix = get_schema_prefix(table);
        let query = format!("SHOW CREATE TABLE {}`{}`", schema_prefix, table.name);

        let result: Option<(String, String)> = conn
            .query_first(&query)
            .map_err(|e| format_mysql_query_error(&e))?;

        match result {
            Some((_, create_statement)) => Ok(format!("{};\n", create_statement)),
            None => Err(DbError::QueryFailed(format!(
                "Could not get CREATE TABLE for {}{}",
                schema_prefix, table.name
            ))),
        }
    }
}

fn fetch_foreign_keys(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<Vec<ForeignKeyInfo>, DbError> {
    let query = format!(
        r#"
        SELECT
            kcu.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            kcu.REFERENCED_TABLE_SCHEMA,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME,
            rc.DELETE_RULE,
            rc.UPDATE_RULE
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
            ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
        WHERE kcu.TABLE_SCHEMA = '{}'
            AND kcu.TABLE_NAME = '{}'
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        "#,
        database, table
    );

    let rows: Vec<mysql::Row> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    let mut fk_map: HashMap<String, ForeignKeyInfo> = HashMap::new();

    for row in rows {
        let constraint_name: String = row.get("CONSTRAINT_NAME").unwrap_or_default();
        let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();
        let ref_schema: Option<String> = row.get("REFERENCED_TABLE_SCHEMA");
        let ref_table: String = row.get("REFERENCED_TABLE_NAME").unwrap_or_default();
        let ref_column: String = row.get("REFERENCED_COLUMN_NAME").unwrap_or_default();
        let on_delete: Option<String> = row.get("DELETE_RULE");
        let on_update: Option<String> = row.get("UPDATE_RULE");

        let entry = fk_map
            .entry(constraint_name.clone())
            .or_insert_with(|| ForeignKeyInfo {
                name: constraint_name,
                columns: Vec::new(),
                referenced_table: ref_table,
                referenced_schema: ref_schema,
                referenced_columns: Vec::new(),
                on_delete,
                on_update,
            });

        entry.columns.push(column_name);
        entry.referenced_columns.push(ref_column);
    }

    Ok(fk_map.into_values().collect())
}

fn fetch_constraints(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<Vec<ConstraintInfo>, DbError> {
    let query = format!(
        r#"
        SELECT
            tc.CONSTRAINT_NAME,
            tc.CONSTRAINT_TYPE,
            GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) as COLUMNS,
            cc.CHECK_CLAUSE
        FROM information_schema.TABLE_CONSTRAINTS tc
        LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        LEFT JOIN information_schema.CHECK_CONSTRAINTS cc
            ON tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            AND tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
        WHERE tc.TABLE_SCHEMA = '{}'
            AND tc.TABLE_NAME = '{}'
            AND tc.CONSTRAINT_TYPE IN ('UNIQUE', 'CHECK')
        GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, cc.CHECK_CLAUSE
        ORDER BY tc.CONSTRAINT_NAME
        "#,
        database, table
    );

    let rows: Vec<mysql::Row> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let name: String = row.get("CONSTRAINT_NAME")?;
            let constraint_type: String = row.get("CONSTRAINT_TYPE")?;
            let columns_str: Option<String> = row.get("COLUMNS");
            let check_clause: Option<String> = row.get("CHECK_CLAUSE");

            let kind = match constraint_type.as_str() {
                "UNIQUE" => ConstraintKind::Unique,
                "CHECK" => ConstraintKind::Check,
                _ => return None,
            };

            let columns = columns_str
                .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
                .unwrap_or_default();

            Some(ConstraintInfo {
                name,
                kind,
                columns,
                check_clause,
            })
        })
        .collect())
}

fn fetch_schema_indexes(conn: &mut Conn, database: &str) -> Result<Vec<SchemaIndexInfo>, DbError> {
    let query = format!(
        r#"
        SELECT
            s.INDEX_NAME,
            s.TABLE_NAME,
            GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX) as COLUMNS,
            s.NON_UNIQUE
        FROM information_schema.STATISTICS s
        WHERE s.TABLE_SCHEMA = '{}'
        GROUP BY s.INDEX_NAME, s.TABLE_NAME, s.NON_UNIQUE
        ORDER BY s.TABLE_NAME, s.INDEX_NAME
        "#,
        database
    );

    let rows: Vec<mysql::Row> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let name: String = row.get("INDEX_NAME")?;
            let table_name: String = row.get("TABLE_NAME")?;
            let columns_str: String = row.get("COLUMNS")?;
            let non_unique: i32 = row.get("NON_UNIQUE").unwrap_or(1);

            let columns: Vec<String> = columns_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            let is_unique = non_unique == 0;
            let is_primary = name == "PRIMARY";

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

fn fetch_schema_foreign_keys(
    conn: &mut Conn,
    database: &str,
) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
    let query = format!(
        r#"
        SELECT
            kcu.CONSTRAINT_NAME,
            kcu.TABLE_NAME,
            kcu.COLUMN_NAME,
            kcu.REFERENCED_TABLE_SCHEMA,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME,
            rc.DELETE_RULE,
            rc.UPDATE_RULE
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
            ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
        WHERE kcu.TABLE_SCHEMA = '{}'
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        "#,
        database
    );

    let rows: Vec<mysql::Row> = conn
        .query(&query)
        .map_err(|e| format_mysql_query_error(&e))?;

    let mut fk_map: HashMap<(String, String), SchemaForeignKeyInfo> = HashMap::new();

    for row in rows {
        let constraint_name: String = row.get("CONSTRAINT_NAME").unwrap_or_default();
        let table_name: String = row.get("TABLE_NAME").unwrap_or_default();
        let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();
        let ref_schema: Option<String> = row.get("REFERENCED_TABLE_SCHEMA");
        let ref_table: String = row.get("REFERENCED_TABLE_NAME").unwrap_or_default();
        let ref_column: String = row.get("REFERENCED_COLUMN_NAME").unwrap_or_default();
        let on_delete: Option<String> = row.get("DELETE_RULE");
        let on_update: Option<String> = row.get("UPDATE_RULE");

        let key = (table_name.clone(), constraint_name.clone());
        let entry = fk_map.entry(key).or_insert_with(|| SchemaForeignKeyInfo {
            name: constraint_name,
            table_name,
            columns: Vec::new(),
            referenced_schema: ref_schema,
            referenced_table: ref_table,
            referenced_columns: Vec::new(),
            on_delete,
            on_update,
        });

        entry.columns.push(column_name);
        entry.referenced_columns.push(ref_column);
    }

    Ok(fk_map.into_values().collect())
}
