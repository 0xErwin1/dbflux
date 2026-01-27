use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use std::collections::HashMap;

use dbflux_core::{
    CodeGenScope, CodeGeneratorInfo, ColumnInfo, ColumnMeta, Connection, ConnectionProfile,
    DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DriverFormDef, FormValues,
    IndexInfo, QueryCancelHandle, QueryHandle, QueryRequest, QueryResult, Row, SchemaSnapshot,
    SshTunnelConfig, SslMode, TableInfo, Value, ViewInfo, MYSQL_FORM,
};
use dbflux_ssh::SshTunnel;
use mysql::prelude::*;
use mysql::{Conn, Opts, OptsBuilder, SslOpts};

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

    fn display_name(&self) -> &'static str {
        match self.kind {
            DbKind::MySQL => "MySQL",
            DbKind::MariaDB => "MariaDB",
            _ => "MySQL",
        }
    }

    fn description(&self) -> &'static str {
        match self.kind {
            DbKind::MySQL => "Popular open source relational database",
            DbKind::MariaDB => "Community-developed fork of MySQL",
            _ => "MySQL-compatible database",
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

        // For Prefer mode, try SSL first and fall back to non-SSL if it fails
        let (opts, mut conn) = if ssl_mode == SslMode::Prefer {
            let ssl_opts = build_mysql_opts(host, port, user, database, password, SslMode::Prefer);
            match Conn::new(ssl_opts.clone()) {
                Ok(c) => {
                    log::info!("[SSL] Connected with SSL (Prefer mode)");
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

        // Get connection ID for cancellation support
        let connection_id: u64 = conn
            .query_first("SELECT CONNECTION_ID()")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
            .unwrap_or(0);

        log::info!(
            "Successfully connected to {}:{} (connection_id: {})",
            host,
            port,
            connection_id
        );

        Ok(Box::new(MysqlConnection {
            conn: Mutex::new(conn),
            ssh_tunnel: None,
            connection_id,
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
            "[SSH] Starting tunnel to {}:{} via {}@{}:{}",
            db_host,
            db_port,
            tunnel_config.user,
            tunnel_config.host,
            tunnel_config.port
        );

        // Establish SSH session
        let session = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;

        // Start the tunnel
        let tunnel = SshTunnel::start(session, db_host.to_string(), db_port)?;
        let local_port = tunnel.local_port();

        log::info!("[SSH] Tunnel established on local port {}", local_port);
        log::info!("[DB] Connecting to MySQL via tunnel (ssl: {:?})", ssl_mode);

        // Note: SSL over SSH tunnel is redundant but supported for consistency
        // For Prefer mode, try SSL first and fall back to non-SSL if it fails
        let (opts, mut conn) = if ssl_mode == SslMode::Prefer {
            let ssl_opts = build_mysql_opts(
                "127.0.0.1",
                local_port,
                db_user,
                database,
                db_password,
                SslMode::Prefer,
            );
            match Conn::new(ssl_opts.clone()) {
                Ok(c) => {
                    log::info!("[SSL] Connected with SSL via tunnel (Prefer mode)");
                    (ssl_opts, c)
                }
                Err(ssl_err) => {
                    log::info!(
                        "[SSL] SSL connection failed via tunnel ({}), falling back to non-SSL",
                        ssl_err
                    );
                    let no_ssl_opts = build_mysql_opts(
                        "127.0.0.1",
                        local_port,
                        db_user,
                        database,
                        db_password,
                        SslMode::Disable,
                    );
                    let c = Conn::new(no_ssl_opts.clone())
                        .map_err(|e| format_mysql_error(&e, "127.0.0.1", local_port))?;
                    (no_ssl_opts, c)
                }
            }
        } else {
            let opts = build_mysql_opts(
                "127.0.0.1",
                local_port,
                db_user,
                database,
                db_password,
                ssl_mode,
            );
            let c = Conn::new(opts.clone())
                .map_err(|e| format_mysql_error(&e, "127.0.0.1", local_port))?;
            (opts, c)
        };

        let connection_id: u64 = conn
            .query_first("SELECT CONNECTION_ID()")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
            .unwrap_or(0);

        log::info!(
            "[CONNECT] Total connection time: {:.2}ms ({}:{} via SSH {})",
            total_start.elapsed().as_secs_f64() * 1000.0,
            db_host,
            db_port,
            tunnel_config.host
        );

        Ok(Box::new(MysqlConnection {
            conn: Mutex::new(conn),
            ssh_tunnel: Some(tunnel),
            connection_id,
            kill_opts: opts,
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

pub struct MysqlConnection {
    conn: Mutex<Conn>,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
    connection_id: u64,
    kill_opts: Opts,
    cancelled: Arc<AtomicBool>,
    kind: DbKind,
}

struct MysqlCancelHandle {
    kill_opts: Opts,
    connection_id: u64,
    cancelled: Arc<AtomicBool>,
}

impl QueryCancelHandle for MysqlCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        // Open a separate connection to send KILL QUERY
        let mut kill_conn = Conn::new(self.kill_opts.clone())
            .map_err(|e| DbError::QueryFailed(format!("Failed to open kill connection: {}", e)))?;

        // Try KILL QUERY first (just cancels the query)
        let kill_query = format!("KILL QUERY {}", self.connection_id);
        match kill_conn.query_drop(&kill_query) {
            Ok(_) => {
                log::info!(
                    "[CANCEL] KILL QUERY {} sent successfully",
                    self.connection_id
                );
                Ok(())
            }
            Err(e) => {
                // If KILL QUERY fails (e.g., no permission), try KILL (kills whole connection)
                log::warn!("[CANCEL] KILL QUERY failed ({}), trying KILL...", e);
                let kill_conn_cmd = format!("KILL {}", self.connection_id);
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
    fn ping(&self) -> Result<(), DbError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        conn.query_drop("SELECT 1")
            .map_err(|e| DbError::QueryFailed(e.to_string()))
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

        let mut conn = match self.conn.lock() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("[CLEANUP] Recovering from poisoned mutex");
                poison_err.into_inner()
            }
        };

        // Execute the query using query_iter for flexibility
        let result: Result<Vec<mysql::Row>, mysql::Error> = conn.query(&req.sql);

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
                            columns: Vec::new(),
                            rows: Vec::new(),
                            affected_rows: None,
                            execution_time: query_time,
                        });
                    } else {
                        // Non-SELECT query, get affected rows from conn
                        let affected = conn.affected_rows();
                        log::debug!(
                            "[QUERY] Completed in {:.2}ms, {} rows affected",
                            query_time.as_secs_f64() * 1000.0,
                            affected
                        );
                        return Ok(QueryResult {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            affected_rows: Some(affected),
                            execution_time: query_time,
                        });
                    }
                }

                // Build columns from first row
                let columns: Vec<ColumnMeta> = rows
                    .first()
                    .map(|row| {
                        row.columns()
                            .iter()
                            .map(|col| ColumnMeta {
                                name: col.name_str().to_string(),
                                type_name: format!("{:?}", col.column_type()),
                                nullable: true,
                            })
                            .collect()
                    })
                    .unwrap_or_default();

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
                Err(DbError::QueryFailed(e.to_string()))
            }
        }
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        let handle = MysqlCancelHandle {
            kill_opts: self.kill_opts.clone(),
            connection_id: self.connection_id,
            cancelled: self.cancelled.clone(),
        };
        handle.cancel()
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(MysqlCancelHandle {
            kill_opts: self.kill_opts.clone(),
            connection_id: self.connection_id,
            cancelled: self.cancelled.clone(),
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        self.cancel_active()
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        log::info!("[SCHEMA] Starting schema fetch");

        let mut conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        log::info!("[SCHEMA] Got connection lock, querying SHOW DATABASES");

        // Get list of databases
        let databases: Vec<String> = conn
            .query("SHOW DATABASES")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        log::info!("[SCHEMA] Found {} databases", databases.len());

        // Filter out system databases
        let user_databases: Vec<String> = databases
            .into_iter()
            .filter(|db| {
                db != "information_schema"
                    && db != "mysql"
                    && db != "performance_schema"
                    && db != "sys"
            })
            .collect();

        let mut db_schemas: Vec<DbSchemaInfo> = Vec::new();

        log::info!(
            "[SCHEMA] Fetching schema for {} user databases: {:?}",
            user_databases.len(),
            user_databases
        );

        for db_name in &user_databases {
            log::info!("[SCHEMA] Fetching tables for database: {}", db_name);
            let tables = fetch_tables(&mut conn, db_name)?;
            log::info!(
                "[SCHEMA] Found {} tables in {}, fetching views",
                tables.len(),
                db_name
            );
            let views = fetch_views(&mut conn, db_name)?;
            log::info!("[SCHEMA] Found {} views in {}", views.len(), db_name);

            db_schemas.push(DbSchemaInfo {
                name: db_name.clone(),
                tables,
                views,
            });
        }

        log::info!("[SCHEMA] Schema fetch complete");

        Ok(SchemaSnapshot {
            databases: user_databases
                .iter()
                .map(|db| DatabaseInfo {
                    name: db.clone(),
                    is_current: false,
                })
                .collect(),
            current_database: None,
            schemas: db_schemas,
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let databases: Vec<String> = conn
            .query("SHOW DATABASES")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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

fn fetch_tables(conn: &mut Conn, database: &str) -> Result<Vec<TableInfo>, DbError> {
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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    let mut tables = Vec::new();

    for table_name in table_names {
        let columns = fetch_columns(conn, database, &table_name)?;
        let indexes = fetch_indexes(conn, database, &table_name)?;

        tables.push(TableInfo {
            name: table_name,
            schema: Some(database.to_string()),
            columns,
            indexes,
        });
    }

    Ok(tables)
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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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
    let columns: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    let placeholders: Vec<&str> = table.columns.iter().map(|_| "?").collect();

    format!(
        "INSERT INTO {}`{}` ({})\nVALUES ({});",
        get_schema_prefix(table),
        table.name,
        columns.join(", "),
        placeholders.join(", ")
    )
}

fn mysql_generate_update(table: &TableInfo) -> String {
    let pk_columns: Vec<&ColumnInfo> = table.columns.iter().filter(|c| c.is_primary_key).collect();

    let set_clause: String = table
        .columns
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
    let pk_columns: Vec<&ColumnInfo> = table.columns.iter().filter(|c| c.is_primary_key).collect();

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
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let schema_prefix = get_schema_prefix(table);
        let query = format!("SHOW CREATE TABLE {}`{}`", schema_prefix, table.name);

        let result: Option<(String, String)> = conn
            .query_first(&query)
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        match result {
            Some((_, create_statement)) => Ok(format!("{};\n", create_statement)),
            None => Err(DbError::QueryFailed(format!(
                "Could not get CREATE TABLE for {}{}",
                schema_prefix, table.name
            ))),
        }
    }
}
