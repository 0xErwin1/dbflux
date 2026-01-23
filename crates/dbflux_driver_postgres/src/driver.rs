use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use dbflux_core::{
    ColumnInfo, ColumnMeta, Connection, ConnectionProfile, DatabaseInfo, DbConfig, DbDriver,
    DbError, DbKind, DbSchemaInfo, IndexInfo, QueryHandle, QueryRequest, QueryResult, Row,
    SchemaSnapshot, SshAuthMethod, SshTunnelConfig, SslMode, TableInfo, Value, ViewInfo,
};
use native_tls::TlsConnector;
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use ssh2::Session;

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

        log::info!("Successfully connected to {}:{}", host, port);

        Ok(Box::new(PostgresConnection {
            client: Mutex::new(client),
            ssh_tunnel: None,
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
        log::info!(
            "Connecting via SSH tunnel: {}@{}:{} -> {}:{}",
            tunnel_config.user,
            tunnel_config.host,
            tunnel_config.port,
            db_host,
            db_port
        );

        let ssh_session = establish_ssh_session(tunnel_config, ssh_secret)?;

        log::info!(
            "SSH session established, setting up local port forwarding to {}:{}",
            db_host,
            db_port
        );

        let tunnel = SshTunnel::start(ssh_session, db_host.to_string(), db_port)?;
        let local_port = tunnel.local_port();

        log::info!(
            "SSH tunnel listening on 127.0.0.1:{}, connecting to PostgreSQL",
            local_port
        );

        let client = connect_postgres(&PostgresConnectParams {
            host: "127.0.0.1",
            port: local_port,
            user: db_user,
            password: db_password.unwrap_or(""),
            database,
            ssl_mode,
        })?;

        log::info!(
            "Successfully connected to {}:{} via SSH tunnel through {}",
            db_host,
            db_port,
            tunnel_config.host
        );

        Ok(Box::new(PostgresConnection {
            client: Mutex::new(client),
            ssh_tunnel: Some(tunnel),
        }))
    }
}

fn establish_ssh_session(
    config: &SshTunnelConfig,
    secret: Option<&str>,
) -> Result<Session, DbError> {
    let tcp = TcpStream::connect((&*config.host, config.port)).map_err(|e| {
        DbError::ConnectionFailed(format!(
            "Failed to connect to SSH server {}:{}: {}",
            config.host, config.port, e
        ))
    })?;

    tcp.set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .ok();
    tcp.set_write_timeout(Some(std::time::Duration::from_secs(30)))
        .ok();

    let mut session = Session::new()
        .map_err(|e| DbError::ConnectionFailed(format!("Failed to create SSH session: {}", e)))?;

    session.set_tcp_stream(tcp);
    session.set_timeout(30000);

    session
        .handshake()
        .map_err(|e| DbError::ConnectionFailed(format!("SSH handshake failed: {}", e)))?;

    match &config.auth_method {
        SshAuthMethod::PrivateKey { key_path } => {
            authenticate_with_key(&session, &config.user, key_path.as_deref(), secret)?;
        }
        SshAuthMethod::Password => {
            let password = secret.ok_or_else(|| {
                DbError::ConnectionFailed("SSH password required but not provided".to_string())
            })?;
            session
                .userauth_password(&config.user, password)
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("SSH password authentication failed: {}", e))
                })?;
        }
    }

    if !session.authenticated() {
        return Err(DbError::ConnectionFailed(
            "SSH authentication failed".to_string(),
        ));
    }

    log::info!("SSH authenticated as {}", config.user);
    Ok(session)
}

fn authenticate_with_key(
    session: &Session,
    user: &str,
    key_path: Option<&Path>,
    passphrase: Option<&str>,
) -> Result<(), DbError> {
    if session.userauth_agent(user).is_ok() && session.authenticated() {
        log::info!("Authenticated via SSH agent");
        return Ok(());
    }

    let key_paths: Vec<std::path::PathBuf> = if let Some(path) = key_path {
        vec![path.to_path_buf()]
    } else {
        let home = dirs::home_dir().unwrap_or_default();
        vec![
            home.join(".ssh/id_rsa"),
            home.join(".ssh/id_ed25519"),
            home.join(".ssh/id_ecdsa"),
        ]
    };

    for path in &key_paths {
        if !path.exists() {
            continue;
        }

        log::debug!("Trying key: {}", path.display());

        let result = session.userauth_pubkey_file(user, None, path, passphrase);

        match result {
            Ok(()) if session.authenticated() => {
                log::info!("Authenticated with key: {}", path.display());
                return Ok(());
            }
            Ok(()) => continue,
            Err(e) => {
                log::debug!("Key {} failed: {}", path.display(), e);
                continue;
            }
        }
    }

    Err(DbError::ConnectionFailed(
        "SSH key authentication failed. Check your key path and passphrase.".to_string(),
    ))
}

struct SshTunnel {
    local_port: u16,
    shutdown: Arc<AtomicBool>,
    #[allow(dead_code)]
    forwarder_thread: JoinHandle<()>,
}

impl SshTunnel {
    fn start(session: Session, remote_host: String, remote_port: u16) -> Result<Self, DbError> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| {
            DbError::ConnectionFailed(format!("Failed to bind local tunnel port: {}", e))
        })?;

        let local_port = listener
            .local_addr()
            .map_err(|e| {
                DbError::ConnectionFailed(format!("Failed to get local tunnel address: {}", e))
            })?
            .port();

        listener.set_nonblocking(true).map_err(|e| {
            DbError::ConnectionFailed(format!("Failed to set listener non-blocking: {}", e))
        })?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let session = Arc::new(Mutex::new(session));

        let thread = thread::spawn(move || {
            run_tunnel_loop(listener, session, remote_host, remote_port, shutdown_clone);
        });

        Ok(Self {
            local_port,
            shutdown,
            forwarder_thread: thread,
        })
    }

    fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

fn run_tunnel_loop(
    listener: TcpListener,
    session: Arc<Mutex<Session>>,
    remote_host: String,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((client_stream, _)) => {
                let session = session.clone();
                let remote_host = remote_host.clone();
                let shutdown = shutdown.clone();

                thread::spawn(move || {
                    if let Err(e) = handle_tunnel_connection(
                        client_stream,
                        session,
                        &remote_host,
                        remote_port,
                        shutdown,
                    ) {
                        log::error!("SSH tunnel connection error: {}", e);
                    }
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(e) => {
                log::error!("SSH tunnel listener error: {}", e);
                break;
            }
        }
    }
}

fn open_ssh_channel_blocking(
    session: &Session,
    remote_host: &str,
    remote_port: u16,
) -> Result<ssh2::Channel, ssh2::Error> {
    session.set_blocking(true);
    session.channel_direct_tcpip(remote_host, remote_port, None)
}

fn handle_tunnel_connection(
    mut client_stream: TcpStream,
    session: Arc<Mutex<Session>>,
    remote_host: &str,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut channel = {
        let session = session
            .lock()
            .map_err(|e| format!("Session lock failed: {}", e))?;

        let channel = open_ssh_channel_blocking(&session, remote_host, remote_port)?;
        session.set_blocking(false);
        channel
    };

    client_stream.set_nonblocking(true)?;

    let mut client_buf = [0u8; 8192];
    let mut channel_buf = [0u8; 8192];

    while !shutdown.load(Ordering::SeqCst) {
        let mut activity = false;

        match client_stream.read(&mut client_buf) {
            Ok(0) => break,
            Ok(n) => {
                channel.write_all(&client_buf[..n])?;
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(Box::new(e)),
        }

        match channel.read(&mut channel_buf) {
            Ok(0) => break,
            Ok(n) => {
                client_stream.write_all(&channel_buf[..n])?;
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(Box::new(e)),
        }

        if !activity {
            thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    Ok(())
}

pub struct PostgresConnection {
    client: Mutex<Client>,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
}

impl Connection for PostgresConnection {
    fn ping(&self) -> Result<(), DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;
        client
            .simple_query("SELECT 1")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();

        let rows = {
            let mut client = self
                .client
                .lock()
                .map_err(|e| DbError::QueryFailed(e.to_string()))?;

            client
                .query(&req.sql, &[])
                .map_err(|e| DbError::QueryFailed(e.to_string()))?
        };

        if rows.is_empty() {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
                execution_time: start.elapsed(),
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

        Ok(QueryResult {
            columns,
            rows: result_rows,
            affected_rows: None,
            execution_time: start.elapsed(),
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not yet implemented".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let databases = get_databases(&mut client)?;
        let current_database = get_current_database(&mut client)?;
        let schemas = get_schemas(&mut client)?;

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
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        get_databases(&mut client)
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows.first().map(|row| row.get(0)))
}

fn get_schemas(client: &mut Client) -> Result<Vec<DbSchemaInfo>, DbError> {
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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    let mut schemas = Vec::new();

    for row in schema_rows {
        let schema_name: String = row.get(0);
        let tables = get_tables_for_schema(client, &schema_name)?;
        let views = get_views_for_schema(client, &schema_name)?;

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    let mut tables = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        let columns = get_columns(client, schema, &name)?;
        let indexes = get_indexes(client, schema, &name)?;

        tables.push(TableInfo {
            name,
            schema: Some(schema.to_string()),
            columns,
            indexes,
        });
    }

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| ViewInfo {
            name: row.get(0),
            schema: Some(schema.to_string()),
        })
        .collect())
}

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

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
