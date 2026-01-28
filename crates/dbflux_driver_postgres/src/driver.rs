use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use dbflux_core::{
    CodeGenScope, CodeGeneratorInfo, ColumnInfo, ColumnMeta, Connection, ConnectionProfile,
    DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DriverFormDef, FormValues,
    IndexInfo, POSTGRES_FORM, QueryCancelHandle, QueryHandle, QueryRequest, QueryResult, Row,
    SchemaLoadingStrategy, SchemaSnapshot, SshTunnelConfig, SslMode, TableInfo, Value, ViewInfo,
};
use dbflux_ssh::SshTunnel;
use native_tls::TlsConnector;
use postgres::{CancelToken as PgCancelToken, Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use uuid::Uuid;

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

    fn description(&self) -> &'static str {
        "Advanced open source relational database"
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_postgres_config(&profile.config)?;

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
            values.insert("database".to_string(), database.clone());
        }

        values
    }
}

struct ExtractedPostgresConfig {
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
            host,
            port,
            user,
            database,
            ssl_mode,
            ssh_tunnel,
            ..
        } => Ok(ExtractedPostgresConfig {
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
                .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {}", e)))?;

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
            DbError::QueryFailed(format!("Failed to cancel query: {}", e))
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
    fn ping(&self) -> Result<(), DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;
        client
            .simple_query("SELECT 1")
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;
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
                .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;
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

        let query_result = {
            let mut client = match self.client.lock() {
                Ok(guard) => guard,
                Err(poison_err) => {
                    log::warn!("[CLEANUP] Recovering from poisoned mutex during cleanup");
                    poison_err.into_inner()
                }
            };

            client.query(&req.sql, &[])
        };

        {
            let mut active = self
                .active_query
                .write()
                .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;
            *active = None;
        }

        let rows = query_result.map_err(|e| {
            if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
                log::info!("[QUERY] Query {} was cancelled", query_id);
                DbError::Cancelled
            } else {
                DbError::QueryFailed(format!("{:?}", e))
            }
        })?;

        let query_time = start.elapsed();

        if rows.is_empty() {
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
        }

        let columns: Vec<ColumnMeta> = rows[0]
            .columns()
            .iter()
            .map(|col| ColumnMeta {
                name: col.name().to_string(),
                type_name: col.type_().name().to_string(),
                nullable: true,
            })
            .collect();

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

        Ok(QueryResult {
            columns,
            rows: result_rows,
            affected_rows: None,
            execution_time: total_time,
        })
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        if *active != Some(handle.id) {
            return Err(DbError::QueryFailed(
                "No matching active query to cancel".to_string(),
            ));
        }

        drop(active);

        log::info!("[CANCEL] Sending cancel request for query {}", handle.id);

        self.cancel_token.cancel_query(NoTls).map_err(|e| {
            log::error!("[CANCEL] Failed to cancel query: {}", e);
            DbError::QueryFailed(format!("Failed to cancel query: {}", e))
        })?;

        log::info!("[CANCEL] Cancel request sent successfully");
        Ok(())
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

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
            DbError::QueryFailed(format!("Failed to cancel query: {}", e))
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
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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

        Ok(SchemaSnapshot {
            databases,
            current_database,
            schemas,
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        get_databases(&mut client)
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::ConnectionPerDatabase
    }

    fn code_generators(&self) -> &'static [CodeGeneratorInfo] {
        POSTGRES_CODE_GENERATORS
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(pg_generate_select_star(table)),
            "insert" => Ok(pg_generate_insert(table)),
            "update" => Ok(pg_generate_update(table)),
            "delete" => Ok(pg_generate_delete(table)),
            "create_table" => Ok(pg_generate_create_table(table)),
            "truncate" => Ok(pg_generate_truncate(table)),
            "drop_table" => Ok(pg_generate_drop_table(table)),
            _ => Err(DbError::NotSupported(format!(
                "Code generator '{}' not supported",
                generator_id
            ))),
        }
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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

    let table_names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    if table_names.is_empty() {
        return Ok(Vec::new());
    }

    let columns_map = get_all_columns_for_schema(client, schema)?;
    let indexes_map = get_all_indexes_for_schema(client, schema)?;

    let tables = table_names
        .into_iter()
        .map(|name| {
            let columns = columns_map.get(&name).cloned().unwrap_or_default();
            let indexes = indexes_map.get(&name).cloned().unwrap_or_default();
            TableInfo {
                name,
                schema: Some(schema.to_string()),
                columns: Some(columns),
                indexes: Some(indexes),
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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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
        .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

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

fn format_pg_error(e: &postgres::Error, host: &str, port: u16) -> DbError {
    let source = e.to_string();

    let message = if source.contains("timed out") {
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
    } else if source.contains("error connecting to server") || source.contains("could not connect")
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
    };

    log::error!("PostgreSQL connection failed: {}", message);
    DbError::ConnectionFailed(message)
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

fn pg_generate_select_star(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    format!("SELECT * FROM {} LIMIT 100;", qualified)
}

fn pg_generate_insert(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!("INSERT INTO {} DEFAULT VALUES;", qualified);
    }

    let columns: Vec<String> = cols.iter().map(|c| pg_quote_ident(&c.name)).collect();

    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("${}", i)).collect();

    format!(
        "INSERT INTO {} ({})\nVALUES ({});",
        qualified,
        columns.join(", "),
        placeholders.join(", ")
    )
}

fn pg_generate_update(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!(
            "UPDATE {}\nSET -- no columns\nWHERE <condition>;",
            qualified
        );
    }

    let pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let non_pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| !c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let set_columns = if non_pk_columns.is_empty() {
        &cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()[..]
    } else {
        &non_pk_columns[..]
    };

    let set_clauses: Vec<String> = set_columns
        .iter()
        .enumerate()
        .map(|(i, col)| format!("{} = ${}", pg_quote_ident(col), i + 1))
        .collect();

    let where_clause = if pk_columns.is_empty() {
        "<condition>".to_string()
    } else {
        let start_idx = set_columns.len() + 1;
        pk_columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{} = ${}", pg_quote_ident(col), start_idx + i))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!(
        "UPDATE {}\nSET {}\nWHERE {};",
        qualified,
        set_clauses.join(",\n    "),
        where_clause
    )
}

fn pg_generate_delete(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    let pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let where_clause = if pk_columns.is_empty() {
        "<condition>".to_string()
    } else {
        pk_columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{} = ${}", pg_quote_ident(col), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!("DELETE FROM {}\nWHERE {};", qualified, where_clause)
}

fn pg_generate_create_table(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    let mut sql = format!("CREATE TABLE {} (\n", qualified);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    let pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    for (i, col) in cols.iter().enumerate() {
        // TODO: Consider normalizing type names for more canonical output
        // (e.g., "character varying" → "varchar", handle arrays, domains, etc.)
        let mut line = format!("    {} {}", pg_quote_ident(&col.name), col.type_name);

        if !col.nullable {
            line.push_str(" NOT NULL");
        }

        if let Some(ref default) = col.default_value {
            line.push_str(&format!(" DEFAULT {}", default));
        }

        let is_last_column = i == cols.len() - 1;
        let has_pk_constraint = !pk_columns.is_empty();

        if !is_last_column || has_pk_constraint {
            line.push(',');
        }

        sql.push_str(&line);
        sql.push('\n');
    }

    if !pk_columns.is_empty() {
        let pk_quoted: Vec<String> = pk_columns.iter().map(|c| pg_quote_ident(c)).collect();
        sql.push_str(&format!("    PRIMARY KEY ({})\n", pk_quoted.join(", ")));
    }

    sql.push_str(");");
    sql
}

fn pg_generate_truncate(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    format!("TRUNCATE {};", qualified)
}

fn pg_generate_drop_table(table: &TableInfo) -> String {
    let qualified = pg_qualified_name(table.schema.as_deref(), &table.name);
    format!("DROP TABLE {};", qualified)
}
