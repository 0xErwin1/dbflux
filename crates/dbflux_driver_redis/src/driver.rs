use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use dbflux_core::{
    ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionProfile, DatabaseCategory,
    DbConfig, DbDriver, DbError, DbKind, DefaultSqlDialect, DriverCapabilities, DriverFormDef,
    DriverMetadata, FormValues, FormattedError, Icon, KeyBulkGetRequest, KeyDeleteRequest,
    KeyEntry, KeyExistsRequest, KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest,
    KeyRenameRequest, KeyScanPage, KeyScanRequest, KeySetRequest, KeySpaceInfo, KeyTtlRequest,
    KeyType, KeyTypeRequest, KeyValueApi, KeyValueSchema, QueryErrorFormatter, QueryHandle,
    QueryLanguage, QueryRequest, QueryResult, REDIS_FORM, SchemaLoadingStrategy, SchemaSnapshot,
    SetCondition, SqlDialect, SshTunnelConfig, Value, ValueRepr, sanitize_uri,
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
            default_database: params.database,
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
            default_database: database,
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
    default_database: Option<u32>,
    _ssh_tunnel: Option<SshTunnel>,
}

impl RedisConnection {
    fn with_connection<T>(
        &self,
        keyspace: Option<u32>,
        f: impl FnOnce(&mut redis::Connection) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut conn = self
            .connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        if let Some(db) = keyspace {
            select_db(&mut conn, db).map_err(|e| format_redis_query_error(&e))?;
        }

        f(&mut conn)
    }
}

impl Connection for RedisConnection {
    fn metadata(&self) -> &'static DriverMetadata {
        &REDIS_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.with_connection(self.default_database, |conn| {
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
        let parts = split_command(req.sql.trim());

        if parts.is_empty() {
            return Ok(QueryResult::empty());
        }

        let value = self.with_connection(self.default_database, |conn| {
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
        let db_index = self.default_database.unwrap_or(0);
        let key_count = self.with_connection(Some(db_index), |conn| {
            redis::cmd("DBSIZE")
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })?;

        Ok(SchemaSnapshot::key_value(KeyValueSchema {
            keyspaces: vec![KeySpaceInfo {
                db_index,
                key_count: Some(key_count),
                memory_bytes: None,
                avg_ttl_seconds: None,
            }],
            current_keyspace: Some(db_index),
        }))
    }

    fn kind(&self) -> DbKind {
        DbKind::Redis
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
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

        self.with_connection(request.keyspace.or(self.default_database), |conn| {
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

            let entries = keys.into_iter().map(KeyEntry::new).collect();
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
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
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

            let value = match key_type {
                KeyType::String | KeyType::Json | KeyType::Unknown => {
                    let fetched = redis::cmd("GET")
                        .arg(&request.key)
                        .query::<Option<Vec<u8>>>(conn)
                        .map_err(|e| format_redis_query_error(&e))?;

                    fetched.ok_or_else(|| {
                        DbError::object_not_found(format!("Key '{}' not found", request.key))
                    })?
                }
                _ => redis::cmd("DUMP")
                    .arg(&request.key)
                    .query::<Vec<u8>>(conn)
                    .map_err(|e| format_redis_query_error(&e))?,
            };

            let ttl_seconds = if request.include_ttl {
                let ttl = redis::cmd("TTL")
                    .arg(&request.key)
                    .query::<i64>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                if ttl >= 0 { Some(ttl) } else { None }
            } else {
                None
            };

            let repr = if matches!(key_type, KeyType::String | KeyType::Json | KeyType::Unknown)
            {
                detect_value_repr(&value)
            } else {
                ValueRepr::Structured
            };

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
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
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
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
            let deleted = redis::cmd("DEL")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(deleted > 0)
        })
    }

    fn exists_key(&self, request: &KeyExistsRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
            let exists = redis::cmd("EXISTS")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(exists > 0)
        })
    }

    fn key_type(&self, request: &KeyTypeRequest) -> Result<KeyType, DbError> {
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
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
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
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
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
            let changed = redis::cmd("EXPIRE")
                .arg(&request.key)
                .arg(request.ttl_seconds)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(changed > 0)
        })
    }

    fn persist_key(&self, request: &KeyPersistRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
            let changed = redis::cmd("PERSIST")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(changed > 0)
        })
    }

    fn rename_key(&self, request: &KeyRenameRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
            redis::cmd("RENAME")
                .arg(&request.from_key)
                .arg(&request.to_key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(())
        })
    }

    fn bulk_get(&self, request: &KeyBulkGetRequest) -> Result<Vec<Option<KeyGetResult>>, DbError> {
        self.with_connection(request.keyspace.or(self.default_database), |conn| {
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

                let payload = match key_type {
                    KeyType::String | KeyType::Json | KeyType::Unknown => {
                        match redis::cmd("GET")
                            .arg(key)
                            .query::<Option<Vec<u8>>>(conn)
                            .map_err(|e| format_redis_query_error(&e))?
                        {
                            Some(v) => v,
                            None => {
                                values.push(None);
                                continue;
                            }
                        }
                    }
                    _ => redis::cmd("DUMP")
                        .arg(key)
                        .query::<Vec<u8>>(conn)
                        .map_err(|e| format_redis_query_error(&e))?,
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

                let repr =
                    if matches!(key_type, KeyType::String | KeyType::Json | KeyType::Unknown) {
                        detect_value_repr(&payload)
                    } else {
                        ValueRepr::Structured
                    };

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

fn parse_key_type(type_name: &str) -> KeyType {
    match type_name {
        "string" => KeyType::String,
        "hash" => KeyType::Hash,
        "list" => KeyType::List,
        "set" => KeyType::Set,
        "zset" => KeyType::SortedSet,
        "stream" => KeyType::Stream,
        "json" => KeyType::Json,
        _ => KeyType::Unknown,
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

fn split_command(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}
