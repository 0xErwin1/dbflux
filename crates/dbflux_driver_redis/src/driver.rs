use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use dbflux_core::{
    ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionProfile, DatabaseCategory,
    DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DefaultSqlDialect,
    DriverCapabilities, DriverFormDef, DriverMetadata, FormValues, FormattedError,
    HashDeleteRequest, HashSetRequest, Icon, KeyBulkGetRequest, KeyDeleteRequest, KeyEntry,
    KeyExistsRequest, KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest,
    KeyRenameRequest, KeyScanPage, KeyScanRequest, KeySetRequest, KeySpaceInfo, KeyTtlRequest,
    KeyType, KeyTypeRequest, KeyValueApi, KeyValueSchema, ListEnd, ListPushRequest,
    ListRemoveRequest, ListSetRequest, QueryErrorFormatter, QueryHandle, QueryLanguage,
    QueryRequest, QueryResult, REDIS_FORM, SchemaLoadingStrategy, SchemaSnapshot, SetAddRequest,
    SetCondition, SetRemoveRequest, SqlDialect, SshTunnelConfig, Value, ValueRepr, ZSetAddRequest,
    ZSetRemoveRequest, sanitize_uri,
};
use dbflux_ssh::SshTunnel;
/// Redis driver metadata.
pub static REDIS_METADATA: DriverMetadata = DriverMetadata {
    id: "redis",
    display_name: "Redis",
    description: "In-memory key-value database",
    category: DatabaseCategory::KeyValue,
    query_language: QueryLanguage::RedisCommands,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::KEYVALUE_BASE.bits()
            | DriverCapabilities::KV_TTL.bits()
            | DriverCapabilities::KV_KEY_TYPES.bits()
            | DriverCapabilities::KV_VALUE_SIZE.bits()
            | DriverCapabilities::KV_RENAME.bits()
            | DriverCapabilities::KV_BULK_GET.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits(),
    ),
    default_port: Some(6379),
    uri_scheme: "redis",
    icon: Icon::Redis,
};

pub struct RedisDriver;

impl RedisDriver {
    pub fn new() -> Self {
        Self
    }

    fn connect_direct(
        &self,
        params: DirectConnectParams<'_>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let scheme = if params.tls { "rediss" } else { "redis" };
        let uri = format!("{}://{}:{}/", scheme, params.host, params.port);
        let client = redis::Client::open(uri.as_str())
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;
        let mut connection = client
            .get_connection()
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;

        authenticate(&mut connection, params.user, params.password)
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;

        if let Some(db) = params.database {
            select_db(&mut connection, db)
                .map_err(|e| format_redis_error(&e, params.host, params.port))?;
        }

        redis::cmd("PING")
            .query::<String>(&mut connection)
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;

        Ok(Box::new(RedisConnection {
            connection: Mutex::new(connection),
            active_database: Mutex::new(params.database),
            _ssh_tunnel: params.ssh_tunnel,
        }))
    }

    fn connect_with_uri(
        &self,
        uri: &str,
        user: Option<&str>,
        password: Option<&str>,
        database: Option<u32>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let client = redis::Client::open(uri).map_err(|e| format_redis_uri_error(&e, uri))?;
        let mut connection = client
            .get_connection()
            .map_err(|e| format_redis_uri_error(&e, uri))?;

        let has_credentials = uri_authority_has_credentials(uri);
        if !has_credentials {
            authenticate(&mut connection, user, password)
                .map_err(|e| format_redis_uri_error(&e, uri))?;
        }

        if let Some(db) = database {
            select_db(&mut connection, db).map_err(|e| format_redis_uri_error(&e, uri))?;
        }

        redis::cmd("PING")
            .query::<String>(&mut connection)
            .map_err(|e| format_redis_uri_error(&e, uri))?;

        Ok(Box::new(RedisConnection {
            connection: Mutex::new(connection),
            active_database: Mutex::new(database),
            _ssh_tunnel: None,
        }))
    }

    fn connect_via_ssh_tunnel(
        &self,
        tunnel_config: &SshTunnelConfig,
        config: &ExtractedRedisConfig,
        ssh_secret: Option<&str>,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let ssh_session = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        let tunnel = SshTunnel::start(ssh_session, config.host.clone(), config.port)?;
        let local_port = tunnel.local_port();

        self.connect_direct(DirectConnectParams {
            host: "127.0.0.1",
            port: local_port,
            tls: config.tls,
            user: config.user.as_deref(),
            password,
            database: config.database,
            ssh_tunnel: Some(tunnel),
        })
    }
}

impl Default for RedisDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for RedisDriver {
    fn kind(&self) -> DbKind {
        DbKind::Redis
    }

    fn metadata(&self) -> &'static DriverMetadata {
        &REDIS_METADATA
    }

    fn form_definition(&self) -> &'static DriverFormDef {
        &REDIS_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let use_uri = values.get("use_uri").map(|s| s == "true").unwrap_or(false);
        let uri = values.get("uri").filter(|s| !s.is_empty()).cloned();
        let user = values.get("user").filter(|s| !s.is_empty()).cloned();
        let database = values
            .get("database")
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<u32>())
            .transpose()
            .map_err(|_| DbError::InvalidProfile("Invalid database index".to_string()))?;
        let tls = values.get("tls").map(|s| s == "true").unwrap_or(false);

        if use_uri {
            if uri.is_none() {
                return Err(DbError::InvalidProfile(
                    "Connection URI is required when using URI mode".to_string(),
                ));
            }

            return Ok(DbConfig::Redis {
                use_uri,
                uri,
                host: String::new(),
                port: 6379,
                user,
                database,
                tls,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            });
        }

        let host = values
            .get("host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Host is required".to_string()))?
            .clone();
        let port = values
            .get("port")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Port is required".to_string()))?
            .parse::<u16>()
            .map_err(|_| DbError::InvalidProfile("Invalid port number".to_string()))?;

        Ok(DbConfig::Redis {
            use_uri,
            uri: None,
            host,
            port,
            user,
            database,
            tls,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::Redis {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            tls,
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
            values.insert("user".to_string(), user.clone().unwrap_or_default());
            values.insert(
                "database".to_string(),
                database.map(|d| d.to_string()).unwrap_or_default(),
            );
            values.insert(
                "tls".to_string(),
                if *tls { "true" } else { "" }.to_string(),
            );
        }

        values
    }

    fn build_uri(&self, values: &FormValues, password: &str) -> Option<String> {
        let host = values
            .get("host")
            .map(String::as_str)
            .unwrap_or("localhost");
        let port = values.get("port").map(String::as_str).unwrap_or("6379");
        let user = values.get("user").map(String::as_str).unwrap_or("");
        let db_index = values.get("database").map(String::as_str).unwrap_or("");
        let tls = values.get("tls").map(|s| s == "true").unwrap_or(false);

        let scheme = if tls { "rediss" } else { "redis" };
        let auth = if !user.is_empty() {
            if password.is_empty() {
                format!("{}@", urlencoding::encode(user))
            } else {
                format!(
                    "{}:{}@",
                    urlencoding::encode(user),
                    urlencoding::encode(password)
                )
            }
        } else if !password.is_empty() {
            format!(":{}@", urlencoding::encode(password))
        } else {
            String::new()
        };

        let path = if db_index.is_empty() {
            String::new()
        } else {
            format!("/{}", db_index)
        };

        Some(format!("{}://{}{}:{}{}", scheme, auth, host, port, path))
    }

    fn parse_uri(&self, uri: &str) -> Option<FormValues> {
        let (scheme, rest) = uri.split_once("://")?;
        if scheme != "redis" && scheme != "rediss" {
            return None;
        }

        let mut values = HashMap::new();
        values.insert("use_uri".to_string(), "true".to_string());
        values.insert("uri".to_string(), uri.to_string());
        values.insert(
            "tls".to_string(),
            if scheme == "rediss" { "true" } else { "" }.to_string(),
        );

        let (authority, path) = match rest.split_once('/') {
            Some((a, p)) => (a, p),
            None => (rest, ""),
        };

        let host_port = if let Some((auth, hp)) = authority.rsplit_once('@') {
            if let Some((user, _)) = auth.split_once(':') {
                values.insert("user".to_string(), user.to_string());
            } else if !auth.starts_with(':') {
                values.insert("user".to_string(), auth.to_string());
            }
            hp
        } else {
            authority
        };

        if let Some((host, port)) = host_port.rsplit_once(':') {
            values.insert("host".to_string(), host.to_string());
            values.insert("port".to_string(), port.to_string());
        } else {
            values.insert("host".to_string(), host_port.to_string());
            values.insert("port".to_string(), "6379".to_string());
        }

        let db = path.split('/').next().unwrap_or_default();
        if !db.is_empty() {
            values.insert("database".to_string(), db.to_string());
        }

        Some(values)
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_redis_config(&profile.config)?;

        if config.use_uri {
            if config.ssh_tunnel.is_some() {
                return Err(DbError::InvalidProfile(
                    "SSH tunnel is not supported when URI mode is enabled for Redis".to_string(),
                ));
            }

            return self.connect_with_uri(
                config.uri.as_deref().unwrap_or_default(),
                config.user.as_deref(),
                password,
                config.database,
            );
        }

        if let Some(tunnel_config) = config.ssh_tunnel.as_ref() {
            self.connect_via_ssh_tunnel(tunnel_config, &config, ssh_secret, password)
        } else {
            self.connect_direct(DirectConnectParams {
                host: &config.host,
                port: config.port,
                tls: config.tls,
                user: config.user.as_deref(),
                password,
                database: config.database,
                ssh_tunnel: None,
            })
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }
}

struct ExtractedRedisConfig {
    use_uri: bool,
    uri: Option<String>,
    host: String,
    port: u16,
    user: Option<String>,
    database: Option<u32>,
    tls: bool,
    ssh_tunnel: Option<SshTunnelConfig>,
}

struct DirectConnectParams<'a> {
    host: &'a str,
    port: u16,
    tls: bool,
    user: Option<&'a str>,
    password: Option<&'a str>,
    database: Option<u32>,
    ssh_tunnel: Option<SshTunnel>,
}

fn extract_redis_config(config: &DbConfig) -> Result<ExtractedRedisConfig, DbError> {
    match config {
        DbConfig::Redis {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            tls,
            ssh_tunnel,
            ..
        } => Ok(ExtractedRedisConfig {
            use_uri: *use_uri,
            uri: uri.clone(),
            host: host.clone(),
            port: *port,
            user: user.clone(),
            database: *database,
            tls: *tls,
            ssh_tunnel: ssh_tunnel.clone(),
        }),
        _ => Err(DbError::InvalidProfile(
            "Expected Redis configuration".to_string(),
        )),
    }
}

pub struct RedisConnection {
    connection: Mutex<redis::Connection>,
    active_database: Mutex<Option<u32>>,
    _ssh_tunnel: Option<SshTunnel>,
}

impl RedisConnection {
    fn active_db_index(&self) -> Result<Option<u32>, DbError> {
        self.active_database
            .lock()
            .map(|db| *db)
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))
    }

    fn set_active_db_index(&self, database: Option<u32>) -> Result<(), DbError> {
        let mut active = self
            .active_database
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;
        *active = database;
        Ok(())
    }

    fn with_connection<T>(
        &self,
        keyspace: Option<u32>,
        f: impl FnOnce(&mut redis::Connection) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut conn = self
            .connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let active = self.active_db_index()?;
        let target_db = keyspace.or(active);

        if let Some(db) = target_db {
            select_db(&mut conn, db).map_err(|e| format_redis_query_error(&e))?;
        }

        let result = f(&mut conn);

        // Restore the active database if we temporarily switched to a different one
        if keyspace.is_some() && keyspace != active && let Some(db) = active {
            let _ = select_db(&mut conn, db);
        }

        result
    }
}

impl Connection for RedisConnection {
    fn metadata(&self) -> &'static DriverMetadata {
        &REDIS_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.with_connection(None, |conn| {
            redis::cmd("PING")
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(())
        })
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();
        let parts = parse_command(req.sql.trim())?;

        if parts.is_empty() {
            return Ok(QueryResult::empty());
        }

        let query_db = req
            .database
            .as_deref()
            .map(parse_database_name)
            .transpose()?;

        let value = self.with_connection(query_db, |conn| {
            let mut command = redis::cmd(&parts[0]);
            for arg in parts.iter().skip(1) {
                command.arg(arg);
            }

            command
                .query::<redis::Value>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })?;

        Ok(QueryResult {
            columns: vec![ColumnMeta {
                name: "result".to_string(),
                type_name: "redis".to_string(),
                nullable: false,
            }],
            rows: vec![vec![Value::Text(format!("{:?}", value))]],
            affected_rows: None,
            execution_time: start.elapsed(),
            is_document_result: false,
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not supported for Redis".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        self.with_connection(None, |conn| {
            let current_db = self.active_db_index()?.unwrap_or(0);
            let keyspace_stats = fetch_keyspace_stats(conn)?;
            let db_count = fetch_database_count(conn).ok().unwrap_or_else(|| {
                keyspace_stats
                    .keys()
                    .copied()
                    .max()
                    .map(|max| max + 1)
                    .unwrap_or(current_db + 1)
            });

            let keyspaces = (0..db_count)
                .map(|db_index| {
                    let stats = keyspace_stats.get(&db_index);
                    KeySpaceInfo {
                        db_index,
                        key_count: stats.map(|s| s.key_count),
                        memory_bytes: None,
                        avg_ttl_seconds: stats.and_then(|s| s.avg_ttl_seconds),
                    }
                })
                .collect();

            Ok(SchemaSnapshot::key_value(KeyValueSchema {
                keyspaces,
                current_keyspace: Some(current_db),
            }))
        })
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let schema = self.schema()?;
        let current = schema.as_key_value().and_then(|s| s.current_keyspace);

        Ok(schema
            .keyspaces()
            .iter()
            .map(|space| DatabaseInfo {
                name: format!("db{}", space.db_index),
                is_current: Some(space.db_index) == current,
            })
            .collect())
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        let db_index = parse_database_name(database)?;

        self.with_connection(Some(db_index), |conn| {
            redis::cmd("DBSIZE")
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            Ok(DbSchemaInfo {
                name: database.to_string(),
                tables: Vec::new(),
                views: Vec::new(),
                custom_types: None,
            })
        })
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        let target = database.map(parse_database_name).transpose()?;

        let mut conn = self
            .connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        if let Some(db) = target {
            select_db(&mut conn, db).map_err(|e| format_redis_query_error(&e))?;
        }

        drop(conn);
        self.set_active_db_index(target)
    }

    fn active_database(&self) -> Option<String> {
        self.active_db_index()
            .ok()
            .flatten()
            .map(|db| format!("db{}", db))
    }

    fn kind(&self) -> DbKind {
        DbKind::Redis
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn key_value_api(&self) -> Option<&dyn KeyValueApi> {
        Some(self)
    }

    fn dialect(&self) -> &dyn SqlDialect {
        static DIALECT: DefaultSqlDialect = DefaultSqlDialect;
        &DIALECT
    }
}

impl KeyValueApi for RedisConnection {
    fn scan_keys(&self, request: &KeyScanRequest) -> Result<KeyScanPage, DbError> {
        let cursor = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<u64>()
            .map_err(|_| DbError::InvalidProfile("Invalid key scan cursor".to_string()))?;

        let count = if request.limit == 0 {
            100
        } else {
            request.limit
        };

        self.with_connection(request.keyspace, |conn| {
            let mut command = redis::cmd("SCAN");
            command.arg(cursor);

            if let Some(filter) = request.filter.as_ref()
                && !filter.is_empty()
            {
                command.arg("MATCH").arg(filter);
            }

            command.arg("COUNT").arg(count);

            let (next_cursor, keys): (u64, Vec<String>) = command
                .query(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let entries = keys
                .into_iter()
                .map(|key| {
                    let type_name = redis::cmd("TYPE")
                        .arg(&key)
                        .query::<String>(conn)
                        .map_err(|e| format_redis_query_error(&e))?;

                    Ok(KeyEntry {
                        key,
                        key_type: Some(parse_key_type(&type_name)),
                        ttl_seconds: None,
                        size_bytes: None,
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()?;

            let next_cursor = if next_cursor == 0 {
                None
            } else {
                Some(next_cursor.to_string())
            };

            Ok(KeyScanPage {
                entries,
                next_cursor,
            })
        })
    }

    fn get_key(&self, request: &KeyGetRequest) -> Result<KeyGetResult, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let key_type_name = redis::cmd("TYPE")
                .arg(&request.key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let key_type = parse_key_type(&key_type_name);
            if key_type == KeyType::Unknown && key_type_name.eq_ignore_ascii_case("none") {
                return Err(DbError::object_not_found(format!(
                    "Key '{}' not found",
                    request.key
                )));
            }

            let (value, repr) = fetch_key_payload(conn, &request.key, key_type)?;

            let ttl_seconds = if request.include_ttl {
                let ttl = redis::cmd("TTL")
                    .arg(&request.key)
                    .query::<i64>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                if ttl >= 0 { Some(ttl) } else { None }
            } else {
                None
            };

            let key_type = normalize_key_type_for_payload(key_type, repr);

            let entry = KeyEntry {
                key: request.key.clone(),
                key_type: if request.include_type {
                    Some(key_type)
                } else {
                    None
                },
                ttl_seconds,
                size_bytes: if request.include_size {
                    Some(value.len() as u64)
                } else {
                    None
                },
            };

            Ok(KeyGetResult { entry, value, repr })
        })
    }

    fn set_key(&self, request: &KeySetRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut command = redis::cmd("SET");
            command.arg(&request.key).arg(&request.value);

            if let Some(ttl_seconds) = request.ttl_seconds {
                command.arg("EX").arg(ttl_seconds);
            }

            match request.condition {
                SetCondition::Always => {}
                SetCondition::IfNotExists => {
                    command.arg("NX");
                }
                SetCondition::IfExists => {
                    command.arg("XX");
                }
            }

            let response = command
                .query::<Option<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            if response.is_none() {
                return Err(DbError::query_failed(
                    "SET condition was not satisfied".to_string(),
                ));
            }

            Ok(())
        })
    }

    fn delete_key(&self, request: &KeyDeleteRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let deleted = redis::cmd("DEL")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(deleted > 0)
        })
    }

    fn exists_key(&self, request: &KeyExistsRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let exists = redis::cmd("EXISTS")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(exists > 0)
        })
    }

    fn key_type(&self, request: &KeyTypeRequest) -> Result<KeyType, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let type_name = redis::cmd("TYPE")
                .arg(&request.key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let key_type = parse_key_type(&type_name);
            if key_type == KeyType::Unknown && type_name.eq_ignore_ascii_case("none") {
                return Err(DbError::object_not_found(format!(
                    "Key '{}' not found",
                    request.key
                )));
            }

            Ok(key_type)
        })
    }

    fn key_ttl(&self, request: &KeyTtlRequest) -> Result<Option<i64>, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let ttl = redis::cmd("TTL")
                .arg(&request.key)
                .query::<i64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            if ttl == -2 {
                return Err(DbError::object_not_found(format!(
                    "Key '{}' not found",
                    request.key
                )));
            }

            if ttl < 0 { Ok(None) } else { Ok(Some(ttl)) }
        })
    }

    fn expire_key(&self, request: &KeyExpireRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let changed = redis::cmd("EXPIRE")
                .arg(&request.key)
                .arg(request.ttl_seconds)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(changed > 0)
        })
    }

    fn persist_key(&self, request: &KeyPersistRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let changed = redis::cmd("PERSIST")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(changed > 0)
        })
    }

    fn rename_key(&self, request: &KeyRenameRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            redis::cmd("RENAME")
                .arg(&request.from_key)
                .arg(&request.to_key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(())
        })
    }

    fn bulk_get(&self, request: &KeyBulkGetRequest) -> Result<Vec<Option<KeyGetResult>>, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut values = Vec::with_capacity(request.keys.len());

            for key in &request.keys {
                let type_name = redis::cmd("TYPE")
                    .arg(key)
                    .query::<String>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                let key_type = parse_key_type(&type_name);
                if key_type == KeyType::Unknown && type_name.eq_ignore_ascii_case("none") {
                    values.push(None);
                    continue;
                }

                let (payload, repr) =
                    if matches!(key_type, KeyType::String | KeyType::Json | KeyType::Unknown) {
                        let fetched = redis::cmd("GET")
                            .arg(key)
                            .query::<Option<Vec<u8>>>(conn)
                            .map_err(|e| format_redis_query_error(&e))?;

                        match fetched {
                            Some(v) => {
                                let repr = detect_value_repr(&v);
                                (v, repr)
                            }
                            None => {
                                values.push(None);
                                continue;
                            }
                        }
                    } else {
                        fetch_key_payload(conn, key, key_type)?
                    };

                let ttl_seconds = if request.include_ttl {
                    let ttl = redis::cmd("TTL")
                        .arg(key)
                        .query::<i64>(conn)
                        .map_err(|e| format_redis_query_error(&e))?;

                    if ttl >= 0 { Some(ttl) } else { None }
                } else {
                    None
                };

                let key_type = normalize_key_type_for_payload(key_type, repr);

                values.push(Some(KeyGetResult {
                    entry: KeyEntry {
                        key: key.clone(),
                        key_type: if request.include_type {
                            Some(key_type)
                        } else {
                            None
                        },
                        ttl_seconds,
                        size_bytes: if request.include_size {
                            Some(payload.len() as u64)
                        } else {
                            None
                        },
                    },
                    value: payload,
                    repr,
                }));
            }

            Ok(values)
        })
    }

    // -- Hash member operations --

    fn hash_set(&self, request: &HashSetRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            redis::cmd("HSET")
                .arg(&request.key)
                .arg(&request.field)
                .arg(&request.value)
                .query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn hash_delete(&self, request: &HashDeleteRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let removed = redis::cmd("HDEL")
                .arg(&request.key)
                .arg(&request.field)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- List member operations --

    fn list_set(&self, request: &ListSetRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            redis::cmd("LSET")
                .arg(&request.key)
                .arg(request.index)
                .arg(&request.value)
                .query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn list_push(&self, request: &ListPushRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            let cmd_name = match request.end {
                ListEnd::Head => "LPUSH",
                ListEnd::Tail => "RPUSH",
            };

            redis::cmd(cmd_name)
                .arg(&request.key)
                .arg(&request.value)
                .query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn list_remove(&self, request: &ListRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let removed = redis::cmd("LREM")
                .arg(&request.key)
                .arg(request.count)
                .arg(&request.value)
                .query::<i64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Set member operations --

    fn set_add(&self, request: &SetAddRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let added = redis::cmd("SADD")
                .arg(&request.key)
                .arg(&request.member)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(added > 0)
        })
    }

    fn set_remove(&self, request: &SetRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let removed = redis::cmd("SREM")
                .arg(&request.key)
                .arg(&request.member)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Sorted Set member operations --

    fn zset_add(&self, request: &ZSetAddRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let added = redis::cmd("ZADD")
                .arg(&request.key)
                .arg(request.score)
                .arg(&request.member)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(added > 0)
        })
    }

    fn zset_remove(&self, request: &ZSetRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let removed = redis::cmd("ZREM")
                .arg(&request.key)
                .arg(&request.member)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }
}

struct RedisErrorFormatter;

impl RedisErrorFormatter {
    fn format_connection_message(source: &str, host: &str, port: u16) -> String {
        let lower = source.to_ascii_lowercase();

        if lower.contains("connection refused") {
            format!("Connection refused. Is Redis running at {}:{}?", host, port)
        } else if lower.contains("timed out") {
            "Connection timed out".to_string()
        } else if lower.contains("noauth") || lower.contains("wrongpass") {
            "Authentication failed. Check credentials.".to_string()
        } else {
            source.to_string()
        }
    }
}

impl QueryErrorFormatter for RedisErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        FormattedError::new(error.to_string())
    }
}

impl ConnectionErrorFormatter for RedisErrorFormatter {
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
        let lower = source.to_ascii_lowercase();

        if lower.contains("connection refused") {
            return FormattedError::new(format!(
                "Connection refused. Check URI: {}",
                sanitized_uri
            ));
        }

        if lower.contains("noauth") || lower.contains("wrongpass") {
            return FormattedError::new("Authentication failed. Check credentials.");
        }

        if lower.contains("timed out") {
            return FormattedError::new("Connection timed out");
        }

        FormattedError::new(source)
    }
}

static REDIS_ERROR_FORMATTER: RedisErrorFormatter = RedisErrorFormatter;

fn format_redis_error(error: &redis::RedisError, host: &str, port: u16) -> DbError {
    let formatted = REDIS_ERROR_FORMATTER.format_connection_error(error, host, port);
    formatted.into_connection_error()
}

fn format_redis_uri_error(error: &redis::RedisError, uri: &str) -> DbError {
    let sanitized = sanitize_uri(uri);
    let formatted = REDIS_ERROR_FORMATTER.format_uri_error(error, &sanitized);
    formatted.into_connection_error()
}

fn format_redis_query_error(error: &redis::RedisError) -> DbError {
    let formatted = REDIS_ERROR_FORMATTER.format_query_error(error);
    formatted.into_query_error()
}

fn authenticate(
    conn: &mut redis::Connection,
    user: Option<&str>,
    password: Option<&str>,
) -> redis::RedisResult<()> {
    if let Some(password) = password {
        let mut command = redis::cmd("AUTH");
        if let Some(user) = user
            && !user.is_empty()
        {
            command.arg(user);
        }
        command.arg(password);
        command.query::<String>(conn)?;
    }

    Ok(())
}

fn select_db(conn: &mut redis::Connection, db_index: u32) -> redis::RedisResult<()> {
    redis::cmd("SELECT").arg(db_index).query::<String>(conn)?;
    Ok(())
}

fn uri_authority_has_credentials(uri: &str) -> bool {
    if let Some((_, rest)) = uri.split_once("://") {
        let authority = rest.split('/').next().unwrap_or_default();
        return authority.contains('@');
    }

    false
}

#[derive(Clone, Copy)]
struct KeyspaceStats {
    key_count: u64,
    avg_ttl_seconds: Option<u64>,
}

fn parse_database_name(database: &str) -> Result<u32, DbError> {
    let trimmed = database.trim();
    let digits = trimmed.strip_prefix("db").unwrap_or(trimmed);

    digits.parse::<u32>().map_err(|_| {
        DbError::InvalidProfile(format!(
            "Invalid database name '{}': expected dbN",
            database
        ))
    })
}

fn fetch_database_count(conn: &mut redis::Connection) -> Result<u32, DbError> {
    let values: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("databases")
        .query(conn)
        .map_err(|e| format_redis_query_error(&e))?;

    if values.len() < 2 {
        return Err(DbError::query_failed(
            "Invalid CONFIG GET databases response",
        ));
    }

    values[1]
        .parse::<u32>()
        .map_err(|_| DbError::query_failed("Invalid Redis databases count"))
}

fn fetch_keyspace_stats(
    conn: &mut redis::Connection,
) -> Result<HashMap<u32, KeyspaceStats>, DbError> {
    let info = redis::cmd("INFO")
        .arg("keyspace")
        .query::<String>(conn)
        .map_err(|e| format_redis_query_error(&e))?;

    let mut stats = HashMap::new();

    for line in info.lines() {
        let line = line.trim();
        if !line.starts_with("db") {
            continue;
        }

        let Some((db_part, fields_part)) = line.split_once(':') else {
            continue;
        };

        let Ok(db_index) = db_part.trim_start_matches("db").parse::<u32>() else {
            continue;
        };

        let mut key_count = 0_u64;
        let mut avg_ttl_seconds = None;

        for field in fields_part.split(',') {
            let Some((name, value)) = field.split_once('=') else {
                continue;
            };

            if name == "keys" {
                key_count = value.parse::<u64>().unwrap_or(0);
            }

            if name == "avg_ttl" {
                let avg_ttl_ms = value.parse::<u64>().unwrap_or(0);
                avg_ttl_seconds = if avg_ttl_ms == 0 {
                    None
                } else {
                    Some(avg_ttl_ms / 1000)
                };
            }
        }

        stats.insert(
            db_index,
            KeyspaceStats {
                key_count,
                avg_ttl_seconds,
            },
        );
    }

    Ok(stats)
}

fn fetch_key_payload(
    conn: &mut redis::Connection,
    key: &str,
    key_type: KeyType,
) -> Result<(Vec<u8>, ValueRepr), DbError> {
    match key_type {
        KeyType::String | KeyType::Json | KeyType::Unknown => {
            let fetched = redis::cmd("GET")
                .arg(key)
                .query::<Option<Vec<u8>>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let value = fetched
                .ok_or_else(|| DbError::object_not_found(format!("Key '{}' not found", key)))?;
            let repr = detect_value_repr(&value);
            Ok((value, repr))
        }
        KeyType::Hash => {
            let entries = redis::cmd("HGETALL")
                .arg(key)
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let mut object = serde_json::Map::new();
            for chunk in entries.chunks(2) {
                if let [field, value] = chunk {
                    object.insert(field.clone(), serde_json::Value::String(value.clone()));
                }
            }

            let value = serde_json::to_vec(&serde_json::Value::Object(object))
                .map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured))
        }
        KeyType::List => {
            let entries = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let value =
                serde_json::to_vec(&entries).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured))
        }
        KeyType::Set => {
            let entries = redis::cmd("SMEMBERS")
                .arg(key)
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let value =
                serde_json::to_vec(&entries).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured))
        }
        KeyType::SortedSet => {
            let entries = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let items: Vec<serde_json::Value> = entries
                .chunks(2)
                .filter_map(|chunk| {
                    if let [member, score] = chunk {
                        Some(serde_json::json!({"member": member, "score": score}))
                    } else {
                        None
                    }
                })
                .collect();

            let value =
                serde_json::to_vec(&items).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured))
        }
        KeyType::Stream | KeyType::Bytes => {
            let payload = redis::cmd("DUMP")
                .arg(key)
                .query::<Vec<u8>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok((payload, ValueRepr::Binary))
        }
    }
}

fn parse_key_type(type_name: &str) -> KeyType {
    let normalized = type_name.trim().to_ascii_lowercase();

    match normalized.as_str() {
        "string" => KeyType::String,
        "hash" => KeyType::Hash,
        "list" => KeyType::List,
        "set" => KeyType::Set,
        "zset" => KeyType::SortedSet,
        "stream" => KeyType::Stream,
        "json" | "rejson-rl" => KeyType::Json,
        _ if normalized.contains("json") => KeyType::Json,
        _ => KeyType::Unknown,
    }
}

fn normalize_key_type_for_payload(key_type: KeyType, repr: ValueRepr) -> KeyType {
    if key_type == KeyType::String && repr == ValueRepr::Binary {
        KeyType::Bytes
    } else {
        key_type
    }
}

fn detect_value_repr(value: &[u8]) -> ValueRepr {
    if let Ok(text) = std::str::from_utf8(value) {
        if serde_json::from_str::<serde_json::Value>(text).is_ok() {
            ValueRepr::Json
        } else {
            ValueRepr::Text
        }
    } else {
        ValueRepr::Binary
    }
}

fn split_command(input: &str) -> Result<Vec<String>, DbError> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            continue;
        }

        if ch == '\'' && !in_double {
            in_single = !in_single;
            continue;
        }

        if ch == '"' && !in_single {
            in_double = !in_double;
            continue;
        }

        if ch.is_whitespace() && !in_single && !in_double {
            if !current.is_empty() {
                items.push(std::mem::take(&mut current));
            }
            continue;
        }

        current.push(ch);
    }

    if escaped {
        return Err(DbError::query_failed(
            "Dangling escape character in command",
        ));
    }

    if in_single {
        return Err(DbError::query_failed("Unterminated single-quoted string"));
    }

    if in_double {
        return Err(DbError::query_failed("Unterminated double-quoted string"));
    }

    if !current.is_empty() {
        items.push(current);
    }

    Ok(items)
}

fn parse_command(input: &str) -> Result<Vec<String>, DbError> {
    let cleaned = input
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with('#')
        })
        .collect::<Vec<_>>()
        .join("\n");

    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        return Ok(Vec::new());
    }

    let cleaned = cleaned.trim_end_matches(';').trim();
    split_command(cleaned)
}
