use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Instant;

use std::sync::Arc;

use dbflux_core::{
    ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionProfile, DatabaseCategory,
    DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DefaultSqlDialect, Diagnostic,
    DiagnosticSeverity, DriverCapabilities, DriverFormDef, DriverMetadata, EditorDiagnostic,
    FormFieldDef, FormFieldKind, FormSection, FormTab, FormValues, FormattedError, HashDeleteRequest,
    HashSetRequest, Icon, KeyBulkGetRequest, KeyDeleteRequest, KeyEntry, KeyExistsRequest,
    KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest, KeyRenameRequest, KeyScanPage,
    KeyScanRequest, KeySetRequest, KeySpaceInfo, KeyTtlRequest, KeyType, KeyTypeRequest,
    KeyValueApi, KeyValueSchema, LanguageService, ListEnd, ListPushRequest, ListRemoveRequest,
    ListSetRequest, QueryErrorFormatter, QueryGenerator, QueryHandle, QueryLanguage, QueryRequest,
    QueryResult, REDIS_FORM, SchemaLoadingStrategy, SchemaSnapshot, SetAddRequest, SetCondition,
    SetRemoveRequest, SqlDialect, SshTunnelConfig, StreamAddRequest, StreamDeleteRequest,
    StreamEntryId, TextPosition, TextPositionRange, ValidationResult, Value, ValueRepr,
    ZSetAddRequest, ZSetRemoveRequest, sanitize_uri,
};
use dbflux_ssh::SshTunnel;
/// Redis driver metadata.
pub static REDIS_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "redis".into(),
    display_name: "Redis".into(),
    description: "In-memory key-value database".into(),
    category: DatabaseCategory::KeyValue,
    query_language: QueryLanguage::RedisCommands,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::KEYVALUE_BASE.bits()
            | DriverCapabilities::MULTIPLE_DATABASES.bits()
            | DriverCapabilities::KV_TTL.bits()
            | DriverCapabilities::KV_KEY_TYPES.bits()
            | DriverCapabilities::KV_VALUE_SIZE.bits()
            | DriverCapabilities::KV_RENAME.bits()
            | DriverCapabilities::KV_BULK_GET.bits()
            | DriverCapabilities::KV_STREAM_RANGE.bits()
            | DriverCapabilities::KV_STREAM_ADD.bits()
            | DriverCapabilities::KV_STREAM_DELETE.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits(),
    ),
    default_port: Some(6379),
    uri_scheme: "redis".into(),
    icon: Icon::Redis,
});

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

    fn metadata(&self) -> &DriverMetadata {
        &REDIS_METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:redis".into()
    }

    fn settings_schema(&self) -> Option<Arc<DriverFormDef>> {
        Some(Arc::new(DriverFormDef {
            tabs: vec![FormTab {
                id: "settings".into(),
                label: "Settings".into(),
                sections: vec![
                    FormSection {
                        title: "Key Scanning".into(),
                        fields: vec![
                            FormFieldDef {
                                id: "scan_batch_size".into(),
                                label: "Scan batch size".into(),
                                kind: FormFieldKind::Number,
                                placeholder: "100".into(),
                                required: false,
                                default_value: "100".into(),
                                enabled_when_checked: None,
                                enabled_when_unchecked: None,
                            },
                            FormFieldDef {
                                id: "stream_preview_limit".into(),
                                label: "Stream preview limit".into(),
                                kind: FormFieldKind::Number,
                                placeholder: "50".into(),
                                required: false,
                                default_value: "50".into(),
                                enabled_when_checked: None,
                                enabled_when_unchecked: None,
                            },
                        ],
                    },
                    FormSection {
                        title: "Safety".into(),
                        fields: vec![FormFieldDef {
                            id: "allow_flush".into(),
                            label: "Allow FLUSHALL / FLUSHDB".into(),
                            kind: FormFieldKind::Checkbox,
                            placeholder: String::new(),
                            required: false,
                            default_value: "false".into(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                        }],
                    },
                ],
            }],
        }))
    }

    fn form_definition(&self) -> &DriverFormDef {
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
        if keyspace.is_some()
            && keyspace != active
            && let Some(db) = active
        {
            let _ = select_db(&mut conn, db);
        }

        result
    }
}

impl Connection for RedisConnection {
    fn metadata(&self) -> &DriverMetadata {
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

        Ok(redis_value_to_result(value, start.elapsed()))
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not supported for Redis".to_string(),
        ))
    }

    fn language_service(&self) -> &dyn LanguageService {
        &RedisLanguageService
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

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: crate::command_generator::RedisCommandGenerator =
            crate::command_generator::RedisCommandGenerator;
        Some(&GENERATOR)
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
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&request.key);
            for (field, value) in &request.fields {
                cmd.arg(field).arg(value);
            }
            cmd.query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn hash_delete(&self, request: &HashDeleteRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("HDEL");
            cmd.arg(&request.key);
            for field in &request.fields {
                cmd.arg(field);
            }
            let removed = cmd
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

            let mut cmd = redis::cmd(cmd_name);
            cmd.arg(&request.key);
            for value in &request.values {
                cmd.arg(value);
            }
            cmd.query::<()>(conn)
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
            let mut cmd = redis::cmd("SADD");
            cmd.arg(&request.key);
            for member in &request.members {
                cmd.arg(member);
            }
            let added = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(added > 0)
        })
    }

    fn set_remove(&self, request: &SetRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("SREM");
            cmd.arg(&request.key);
            for member in &request.members {
                cmd.arg(member);
            }
            let removed = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Sorted Set member operations --

    fn zset_add(&self, request: &ZSetAddRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("ZADD");
            cmd.arg(&request.key);
            for (member, score) in &request.members {
                cmd.arg(*score).arg(member);
            }
            let added = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(added > 0)
        })
    }

    fn zset_remove(&self, request: &ZSetRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("ZREM");
            cmd.arg(&request.key);
            for member in &request.members {
                cmd.arg(member);
            }
            let removed = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Stream operations --

    fn stream_add(&self, request: &StreamAddRequest) -> Result<String, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(&request.key);

            if let Some(maxlen) = &request.maxlen {
                cmd.arg("MAXLEN");
                if maxlen.approximate {
                    cmd.arg("~");
                }
                cmd.arg(maxlen.count);
            }

            match &request.id {
                StreamEntryId::Auto => {
                    cmd.arg("*");
                }
                StreamEntryId::Explicit(id) => {
                    cmd.arg(id);
                }
            }

            for (field, value) in &request.fields {
                cmd.arg(field).arg(value);
            }

            let entry_id: String = cmd.query(conn).map_err(|e| format_redis_query_error(&e))?;

            Ok(entry_id)
        })
    }

    fn stream_delete(&self, request: &StreamDeleteRequest) -> Result<u64, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("XDEL");
            cmd.arg(&request.key);

            for id in &request.ids {
                cmd.arg(id);
            }

            let deleted: u64 = cmd.query(conn).map_err(|e| format_redis_query_error(&e))?;

            Ok(deleted)
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

// -- Redis Value → QueryResult --

fn redis_value_to_result(value: redis::Value, execution_time: std::time::Duration) -> QueryResult {
    match value {
        redis::Value::Nil => QueryResult::text("(nil)".to_string(), execution_time),

        redis::Value::Int(i) => QueryResult::text(format!("(integer) {}", i), execution_time),

        redis::Value::BulkString(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(s) => QueryResult::text(s, execution_time),
            Err(_) => QueryResult::binary(bytes, execution_time),
        },

        redis::Value::SimpleString(s) => QueryResult::text(s, execution_time),

        redis::Value::Array(items) => redis_array_to_result(items, execution_time),

        redis::Value::Map(entries) => {
            let mut lines = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                let key_str = redis_value_to_display(&k);
                let val_str = redis_value_to_display(&v);
                lines.push(format!("{}: {}", key_str, val_str));
            }
            QueryResult::text(lines.join("\n"), execution_time)
        }

        redis::Value::Boolean(b) => QueryResult::text(
            if b { "(true)" } else { "(false)" }.to_string(),
            execution_time,
        ),

        redis::Value::Double(f) => QueryResult::text(format!("(double) {}", f), execution_time),

        redis::Value::BigNumber(n) => {
            QueryResult::text(format!("(big number) {}", n), execution_time)
        }

        redis::Value::VerbatimString { format: _, text } => QueryResult::text(text, execution_time),

        redis::Value::Set(items) => redis_array_to_result(items, execution_time),

        redis::Value::Okay => QueryResult::text("OK".to_string(), execution_time),

        redis::Value::ServerError(e) => QueryResult::text(
            format!("(error) {}", e.details().unwrap_or("unknown")),
            execution_time,
        ),

        redis::Value::Push { kind: _, data } => redis_array_to_result(data, execution_time),

        redis::Value::Attribute {
            data,
            attributes: _,
        } => redis_value_to_result(*data, execution_time),
    }
}

/// Try to present a redis array as a table (if elements are uniform key-value pairs)
/// or fall back to numbered text lines.
fn redis_array_to_result(
    items: Vec<redis::Value>,
    execution_time: std::time::Duration,
) -> QueryResult {
    if items.is_empty() {
        return QueryResult::text("(empty array)".to_string(), execution_time);
    }

    // Check if all items are simple scalars → table with index + value columns
    let all_scalar = items.iter().all(|v| {
        matches!(
            v,
            redis::Value::Int(_)
                | redis::Value::BulkString(_)
                | redis::Value::SimpleString(_)
                | redis::Value::Nil
                | redis::Value::Boolean(_)
                | redis::Value::Double(_)
                | redis::Value::Okay
        )
    });

    if all_scalar {
        let columns = vec![
            ColumnMeta {
                name: "#".to_string(),
                type_name: "int".to_string(),
                nullable: false,
            },
            ColumnMeta {
                name: "value".to_string(),
                type_name: "redis".to_string(),
                nullable: true,
            },
        ];

        let rows: Vec<Vec<Value>> = items
            .into_iter()
            .enumerate()
            .map(|(i, v)| vec![Value::Int(i as i64), redis_scalar_to_value(v)])
            .collect();

        return QueryResult::table(columns, rows, None, execution_time);
    }

    // Fallback: numbered text dump
    let lines: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, v)| format!("{}) {}", i + 1, redis_value_to_display(v)))
        .collect();

    QueryResult::text(lines.join("\n"), execution_time)
}

fn redis_scalar_to_value(v: redis::Value) -> Value {
    match v {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(i) => Value::Int(i),
        redis::Value::BulkString(bytes) => match String::from_utf8(bytes) {
            Ok(s) => Value::Text(s),
            Err(e) => Value::Bytes(e.into_bytes()),
        },
        redis::Value::SimpleString(s) => Value::Text(s),
        redis::Value::Boolean(b) => Value::Bool(b),
        redis::Value::Double(f) => Value::Float(f),
        redis::Value::Okay => Value::Text("OK".to_string()),
        _ => Value::Text(redis_value_to_display(&v)),
    }
}

fn redis_value_to_display(v: &redis::Value) -> String {
    match v {
        redis::Value::Nil => "(nil)".to_string(),
        redis::Value::Int(i) => i.to_string(),
        redis::Value::BulkString(bytes) => {
            String::from_utf8(bytes.clone()).unwrap_or_else(|_| format!("<{} bytes>", bytes.len()))
        }
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Array(items) | redis::Value::Set(items) => {
            let inner: Vec<String> = items.iter().map(redis_value_to_display).collect();
            format!("[{}]", inner.join(", "))
        }
        redis::Value::Map(entries) => {
            let inner: Vec<String> = entries
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        redis_value_to_display(k),
                        redis_value_to_display(v)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        redis::Value::Boolean(b) => b.to_string(),
        redis::Value::Double(f) => f.to_string(),
        redis::Value::BigNumber(n) => n.to_string(),
        redis::Value::VerbatimString { text, .. } => text.clone(),
        redis::Value::Okay => "OK".to_string(),
        redis::Value::ServerError(e) => {
            format!("ERR {}", e.details().unwrap_or("unknown"))
        }
        redis::Value::Push { data, .. } => {
            let inner: Vec<String> = data.iter().map(redis_value_to_display).collect();
            format!("PUSH[{}]", inner.join(", "))
        }
        redis::Value::Attribute { data, .. } => redis_value_to_display(data),
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
        KeyType::Stream => {
            let raw_entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
                .arg(key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(50)
                .query(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let entries: Vec<serde_json::Value> = raw_entries
                .into_iter()
                .map(|(id, fields)| {
                    let mut map = serde_json::Map::new();
                    for chunk in fields.chunks(2) {
                        if let [f, v] = chunk {
                            map.insert(f.clone(), serde_json::Value::String(v.clone()));
                        }
                    }
                    serde_json::json!({ "id": id, "fields": map })
                })
                .collect();

            let value =
                serde_json::to_vec(&entries).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Stream))
        }
        KeyType::Bytes => {
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

struct RedisLanguageService;

impl LanguageService for RedisLanguageService {
    fn validate(&self, query: &str) -> ValidationResult {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return ValidationResult::Valid;
        }

        let lower = trimmed.to_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert ")
            || lower.starts_with("update ")
            || lower.starts_with("delete ")
        {
            return ValidationResult::WrongLanguage {
                expected: QueryLanguage::RedisCommands,
                message:
                    "SQL syntax not supported for Redis. Use Redis command syntax (e.g. GET key)."
                        .to_string(),
            };
        }

        match parse_command(query) {
            Ok(_) => ValidationResult::Valid,
            Err(e) => ValidationResult::SyntaxError(
                Diagnostic::error(format!("Invalid Redis command: {}", e))
                    .with_hint("Use Redis command syntax, for example: SET mykey myvalue"),
            ),
        }
    }

    fn detect_dangerous(&self, query: &str) -> Option<dbflux_core::DangerousQueryKind> {
        dbflux_core::detect_dangerous_redis(query)
    }

    fn editor_diagnostics(&self, query: &str) -> Vec<EditorDiagnostic> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return vec![];
        }

        let lower = trimmed.to_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert ")
            || lower.starts_with("update ")
            || lower.starts_with("delete ")
        {
            return vec![EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message:
                    "SQL syntax not supported for Redis. Use Redis command syntax (e.g. GET key)."
                        .to_string(),
                range: redis_first_line_range(query),
            }];
        }

        match parse_command(query) {
            Ok(tokens) => check_redis_arity(&tokens, query),
            Err(e) => vec![EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message: format!("Invalid Redis command: {}", e),
                range: redis_first_line_range(query),
            }],
        }
    }
}

/// Arity rule for a Redis command.
///
/// - `min`: minimum number of arguments (excluding the command name itself)
/// - `max`: maximum number of arguments, or `None` for variadic commands
enum Arity {
    Exact(usize),
    AtLeast(usize),
    Range(usize, usize),
}

/// Look up the arity expectation for a known Redis command. Returns `None`
/// for unknown commands (no arity check performed).
fn command_arity(command: &str) -> Option<Arity> {
    match command {
        // Key inspection / manipulation
        "GET" => Some(Arity::Exact(1)),
        "SET" => Some(Arity::Range(2, 7)),
        "SETNX" => Some(Arity::Exact(2)),
        "GETSET" => Some(Arity::Exact(2)),
        "GETRANGE" => Some(Arity::Exact(3)),
        "SETRANGE" => Some(Arity::Exact(3)),
        "APPEND" => Some(Arity::Exact(2)),
        "STRLEN" => Some(Arity::Exact(1)),
        "MGET" => Some(Arity::AtLeast(1)),
        "MSET" => Some(Arity::AtLeast(2)),
        "DEL" => Some(Arity::AtLeast(1)),
        "EXISTS" => Some(Arity::AtLeast(1)),
        "EXPIRE" => Some(Arity::Range(2, 3)),
        "TTL" => Some(Arity::Exact(1)),
        "PTTL" => Some(Arity::Exact(1)),
        "TYPE" => Some(Arity::Exact(1)),
        "PERSIST" => Some(Arity::Exact(1)),
        "RENAME" => Some(Arity::Exact(2)),
        "INCR" => Some(Arity::Exact(1)),
        "DECR" => Some(Arity::Exact(1)),
        "INCRBY" => Some(Arity::Exact(2)),
        "DECRBY" => Some(Arity::Exact(2)),
        "DUMP" => Some(Arity::Exact(1)),
        "OBJECT" => Some(Arity::AtLeast(1)),
        "KEYS" => Some(Arity::Exact(1)),
        "SCAN" => Some(Arity::AtLeast(1)),
        "SELECT" => Some(Arity::Exact(1)),

        // Hash
        "HGET" => Some(Arity::Exact(2)),
        "HSET" => Some(Arity::AtLeast(3)),
        "HDEL" => Some(Arity::AtLeast(2)),
        "HGETALL" => Some(Arity::Exact(1)),
        "HLEN" => Some(Arity::Exact(1)),

        // List
        "LPUSH" => Some(Arity::AtLeast(2)),
        "RPUSH" => Some(Arity::AtLeast(2)),
        "LPOP" => Some(Arity::Range(1, 2)),
        "RPOP" => Some(Arity::Range(1, 2)),
        "LRANGE" => Some(Arity::Exact(3)),
        "LLEN" => Some(Arity::Exact(1)),
        "LINDEX" => Some(Arity::Exact(2)),
        "LSET" => Some(Arity::Exact(3)),

        // Set
        "SADD" => Some(Arity::AtLeast(2)),
        "SREM" => Some(Arity::AtLeast(2)),
        "SMEMBERS" => Some(Arity::Exact(1)),
        "SCARD" => Some(Arity::Exact(1)),
        "SISMEMBER" => Some(Arity::Exact(2)),

        // Sorted Set
        "ZADD" => Some(Arity::AtLeast(3)),
        "ZREM" => Some(Arity::AtLeast(2)),
        "ZRANGE" => Some(Arity::Range(3, 7)),
        "ZCARD" => Some(Arity::Exact(1)),
        "ZSCORE" => Some(Arity::Exact(2)),
        "ZRANK" => Some(Arity::Exact(2)),

        // Server
        "PING" => Some(Arity::Range(0, 1)),
        "INFO" => Some(Arity::Range(0, 1)),

        _ => None,
    }
}

/// Check argument count for a parsed Redis command, returning diagnostics if
/// the arity is wrong.
fn check_redis_arity(tokens: &[String], query: &str) -> Vec<EditorDiagnostic> {
    if tokens.is_empty() {
        return vec![];
    }

    let command = tokens[0].to_uppercase();
    let arg_count = tokens.len() - 1;

    let Some(arity) = command_arity(&command) else {
        return vec![];
    };

    let problem = match arity {
        Arity::Exact(n) if arg_count != n => Some(if n == 1 {
            format!("{command} requires exactly {n} argument, got {arg_count}")
        } else {
            format!("{command} requires exactly {n} arguments, got {arg_count}")
        }),

        Arity::AtLeast(n) if arg_count < n => Some(if n == 1 {
            format!("{command} requires at least {n} argument, got {arg_count}")
        } else {
            format!("{command} requires at least {n} arguments, got {arg_count}")
        }),

        Arity::Range(min, max) if arg_count < min || arg_count > max => Some(format!(
            "{command} accepts {min}–{max} arguments, got {arg_count}"
        )),

        _ => None,
    };

    if let Some(message) = problem {
        return vec![EditorDiagnostic {
            severity: DiagnosticSeverity::Warning,
            message,
            range: redis_first_line_range(query),
        }];
    }

    if let Some(pairing_msg) = check_pairing(&command, arg_count) {
        return vec![EditorDiagnostic {
            severity: DiagnosticSeverity::Warning,
            message: pairing_msg,
            range: redis_first_line_range(query),
        }];
    }

    vec![]
}

/// Commands that require arguments in pairs (key/value or field/value).
fn check_pairing(command: &str, arg_count: usize) -> Option<String> {
    match command {
        // MSET key value [key value ...] — total args must be even
        "MSET" | "MSETNX" if !arg_count.is_multiple_of(2) => Some(format!(
            "{command} requires key-value pairs (even number of arguments), got {arg_count}"
        )),

        // HSET key field value [field value ...] — args after the key must be in pairs
        "HSET" | "HMSET" if arg_count >= 3 && !(arg_count - 1).is_multiple_of(2) => Some(format!(
            "{command} requires a key followed by field-value pairs, got {arg_count} arguments"
        )),

        _ => None,
    }
}

fn redis_first_line_range(query: &str) -> TextPositionRange {
    let first_line_len = query
        .lines()
        .next()
        .map(|line| line.chars().count())
        .unwrap_or(1) as u32;
    let end_col = first_line_len.max(1);

    TextPositionRange::new(TextPosition::new(0, 0), TextPosition::new(0, end_col))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DatabaseCategory, DbDriver, QueryLanguage, ValidationResult};

    #[test]
    fn build_config_requires_uri_when_uri_mode_enabled() {
        let driver = RedisDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_rejects_invalid_database_index() {
        let driver = RedisDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "localhost".to_string());
        values.insert("port".to_string(), "6379".to_string());
        values.insert("database".to_string(), "nope".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_requires_host_and_valid_port_in_manual_mode() {
        let driver = RedisDriver::new();

        let mut missing_host = FormValues::new();
        missing_host.insert("port".to_string(), "6379".to_string());
        let result = driver.build_config(&missing_host);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));

        let mut bad_port = FormValues::new();
        bad_port.insert("host".to_string(), "localhost".to_string());
        bad_port.insert("port".to_string(), "not-a-port".to_string());
        let result = driver.build_config(&bad_port);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn extract_values_includes_tls_and_database() {
        let driver = RedisDriver::new();
        let config = DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "cache.local".to_string(),
            port: 6380,
            user: Some("svc".to_string()),
            database: Some(3),
            tls: true,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(values.get("host").map(String::as_str), Some("cache.local"));
        assert_eq!(values.get("port").map(String::as_str), Some("6380"));
        assert_eq!(values.get("database").map(String::as_str), Some("3"));
        assert_eq!(values.get("tls").map(String::as_str), Some("true"));
    }

    #[test]
    fn build_uri_and_parse_uri_keep_tls_user_and_db() {
        let driver = RedisDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cache.local".to_string());
        values.insert("port".to_string(), "6380".to_string());
        values.insert("user".to_string(), "service user".to_string());
        values.insert("database".to_string(), "2".to_string());
        values.insert("tls".to_string(), "true".to_string());

        let uri = driver
            .build_uri(&values, "s3cr@t")
            .expect("redis driver should support uri build");
        assert_eq!(uri, "rediss://service%20user:s3cr%40t@cache.local:6380/2");

        let parsed = driver.parse_uri(&uri).expect("uri should parse");
        assert_eq!(parsed.get("tls").map(String::as_str), Some("true"));
        assert_eq!(parsed.get("host").map(String::as_str), Some("cache.local"));
        assert_eq!(parsed.get("port").map(String::as_str), Some("6380"));
        assert_eq!(
            parsed.get("user").map(String::as_str),
            Some("service%20user")
        );
        assert_eq!(parsed.get("database").map(String::as_str), Some("2"));
    }

    #[test]
    fn parse_uri_rejects_unsupported_scheme() {
        let driver = RedisDriver::new();
        assert!(driver.parse_uri("http://localhost:6379").is_none());
    }

    #[test]
    fn parse_uri_defaults_port_when_missing() {
        let driver = RedisDriver::new();
        let parsed = driver
            .parse_uri("redis://localhost/0")
            .expect("uri should parse");

        assert_eq!(parsed.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(parsed.get("port").map(String::as_str), Some("6379"));
        assert_eq!(parsed.get("database").map(String::as_str), Some("0"));
    }

    #[test]
    fn parse_database_name_supports_prefix_and_plain_numbers() {
        assert_eq!(parse_database_name("db3").unwrap(), 3);
        assert_eq!(parse_database_name(" 7 ").unwrap(), 7);
    }

    #[test]
    fn parse_database_name_rejects_invalid_values() {
        let error = parse_database_name("dbx").expect_err("invalid db name should fail");
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn parse_command_strips_comments_and_semicolon() {
        let tokens = parse_command("# comment\nGET my_key;").expect("command should parse");
        assert_eq!(tokens, vec!["GET", "my_key"]);
    }

    #[test]
    fn parse_command_handles_quotes_and_escapes() {
        let tokens =
            parse_command("SET \"my key\" 'hello world'\\n").expect("quoted command should parse");
        assert_eq!(tokens, vec!["SET", "my key", "hello worldn"]);
    }

    #[test]
    fn parse_command_reports_unterminated_quote() {
        let error = parse_command("SET 'abc").expect_err("unterminated quote should fail");
        assert!(matches!(error, DbError::QueryFailed(_)));
    }

    #[test]
    fn check_pairing_detects_mset_odd_arguments() {
        let message = check_pairing("MSET", 3).expect("odd mset arity should warn");
        assert!(message.contains("even number of arguments"));
    }

    #[test]
    fn check_redis_arity_reports_exact_argument_mismatch() {
        let diagnostics = check_redis_arity(&["GET".to_string()], "GET");
        assert_eq!(diagnostics.len(), 1);
        assert!(
            diagnostics[0]
                .message
                .contains("requires exactly 1 argument")
        );
    }

    #[test]
    fn language_service_flags_sql_as_wrong_language() {
        let service = RedisLanguageService;
        let validation = service.validate("SELECT * FROM users");

        assert!(matches!(validation, ValidationResult::WrongLanguage { .. }));
    }

    #[test]
    fn uri_authority_has_credentials_detects_at_symbol() {
        assert!(uri_authority_has_credentials(
            "redis://:pass@localhost:6379/0"
        ));
        assert!(!uri_authority_has_credentials("redis://localhost:6379/0"));
    }

    #[test]
    #[ignore = "TODO: decode percent-encoded username in redis parse_uri"]
    fn pending_redis_parse_uri_username_decoding() {
        panic!("TODO: percent-decode username in Redis parse_uri result");
    }

    #[test]
    fn metadata_and_form_definition_match_redis_contract() {
        let driver = RedisDriver::new();
        let metadata = driver.metadata();

        assert_eq!(metadata.category, DatabaseCategory::KeyValue);
        assert_eq!(metadata.query_language, QueryLanguage::RedisCommands);
        assert_eq!(metadata.default_port, Some(6379));
        assert_eq!(metadata.uri_scheme, "redis");
        assert!(!driver.form_definition().tabs.is_empty());
    }

    #[test]
    fn settings_schema_exposes_scan_and_safety_fields() {
        let driver = RedisDriver::new();
        let schema = driver
            .settings_schema()
            .expect("redis should have a settings schema");

        assert_eq!(schema.tabs.len(), 1);
        assert_eq!(schema.tabs[0].sections.len(), 2);

        let scanning = &schema.tabs[0].sections[0];
        assert_eq!(scanning.title, "Key Scanning");
        assert_eq!(scanning.fields.len(), 2);
        assert_eq!(scanning.fields[0].id, "scan_batch_size");
        assert_eq!(scanning.fields[0].default_value, "100");
        assert_eq!(scanning.fields[1].id, "stream_preview_limit");
        assert_eq!(scanning.fields[1].default_value, "50");

        let safety = &schema.tabs[0].sections[1];
        assert_eq!(safety.title, "Safety");
        assert_eq!(safety.fields.len(), 1);
        assert_eq!(safety.fields[0].id, "allow_flush");
        assert_eq!(safety.fields[0].default_value, "false");
    }

    #[test]
    fn driver_key_is_builtin_redis() {
        let driver = RedisDriver::new();
        assert_eq!(driver.driver_key(), "builtin:redis");
    }
}
