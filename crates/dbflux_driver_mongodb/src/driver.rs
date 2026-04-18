use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use std::sync::Arc;

use bson::{Bson, Document, doc};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionIndexInfo, ColumnMeta, Connection,
    ConnectionErrorFormatter, ConnectionExt, ConnectionProfile, CrudResult, DangerousQueryKind,
    DatabaseCategory, DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo,
    DdlCapabilities, DescribeRequest, Diagnostic, DiagnosticSeverity, DocumentConnection,
    DocumentDelete, DocumentInsert, DocumentSchema, DocumentUpdate, DriverCapabilities,
    DriverFormDef, DriverLimits, DriverMetadata, EditorDiagnostic, FieldInfo, FormFieldDef,
    FormFieldKind, FormSection, FormTab, FormValues, FormattedError, Icon, IndexData,
    IndexDirection, KeyValueConnection, LanguageService, MONGODB_FORM, MutationCapabilities,
    OrderByColumn, PaginationStyle, PlaceholderStyle, QueryCancelHandle, QueryCapabilities,
    QueryErrorFormatter, QueryGenerator, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    RelationalConnection, Row, SchemaDropTarget, SchemaLoadingStrategy, SchemaObjectKind,
    SchemaSnapshot, SemanticFieldRef, SemanticFilter, SemanticPlan, SemanticPlanKind,
    SemanticRequest, SqlDialect, SshTunnelConfig, TableInfo, TextPosition, TextPositionRange,
    TransactionCapabilities, ValidationResult, Value, ViewInfo, WhereOperator,
    detect_dangerous_mongo, sanitize_uri,
};
use dbflux_ssh::SshTunnel;
use mongodb::sync::{Client, Database};
use uuid::Uuid;

/// MongoDB driver metadata.
pub static MONGODB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "mongodb".into(),
    display_name: "MongoDB".into(),
    description: "Document database for modern applications".into(),
    category: DatabaseCategory::Document,
    query_language: QueryLanguage::MongoQuery,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::DOCUMENT_BASE.bits()
            | DriverCapabilities::AGGREGATION.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::INDEXES.bits(),
    ),
    default_port: Some(27017),
    uri_scheme: "mongodb".into(),
    icon: Icon::Mongodb,
    syntax: None,
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Cursor, PaginationStyle::PageToken],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: false,
        supports_limit: true,
        supports_offset: true,
        supports_joins: false,
        supports_subqueries: false,
        supports_union: false,
        supports_intersect: false,
        supports_except: false,
        supports_case_expressions: false,
        supports_window_functions: false,
        supports_ctes: false,
        supports_explain: false,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: true,
        supports_returning: false,
        supports_batch: false,
        supports_bulk_update: false,
        supports_bulk_delete: false,
        max_insert_values: 0,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: false,
        supports_drop_database: true,
        supports_create_table: false,
        supports_drop_table: true,
        supports_alter_table: false,
        supports_create_index: true,
        supports_drop_index: true,
        supports_create_view: false,
        supports_drop_view: false,
        supports_create_trigger: false,
        supports_drop_trigger: false,
        transactional_ddl: false,
        supports_add_column: false,
        supports_drop_column: false,
        supports_rename_column: false,
        supports_alter_column: false,
        supports_add_constraint: false,
        supports_drop_constraint: false,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: Some(DriverLimits {
        max_query_length: 0,
        max_parameters: 0,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 0,
        max_identifier_length: 63,
        max_columns: 0,
        max_indexes_per_table: 64,
    }),
    classification_override: None,
});

pub struct MongoDriver;

impl MongoDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MongoDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for MongoDriver {
    fn kind(&self) -> DbKind {
        DbKind::MongoDB
    }

    fn metadata(&self) -> &DriverMetadata {
        &MONGODB_METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:mongodb".into()
    }

    fn settings_schema(&self) -> Option<Arc<DriverFormDef>> {
        Some(Arc::new(DriverFormDef {
            tabs: vec![FormTab {
                id: "settings".into(),
                label: "Settings".into(),
                sections: vec![FormSection {
                    title: "Schema".into(),
                    fields: vec![
                        FormFieldDef {
                            id: "schema_sample_size".into(),
                            label: "Schema sample size".into(),
                            kind: FormFieldKind::Number,
                            placeholder: "100".into(),
                            required: false,
                            default_value: "100".into(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                        },
                        FormFieldDef {
                            id: "show_system_databases".into(),
                            label: "Show system databases".into(),
                            kind: FormFieldKind::Checkbox,
                            placeholder: String::new(),
                            required: false,
                            default_value: "false".into(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                        },
                    ],
                }],
            }],
        }))
    }

    fn form_definition(&self) -> &DriverFormDef {
        &MONGODB_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let use_uri = values.get("use_uri").map(|s| s == "true").unwrap_or(false);

        let uri = values.get("uri").filter(|s| !s.is_empty()).cloned();

        let host = values
            .get("host")
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| "localhost".to_string());

        let port = values
            .get("port")
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse().ok())
            .unwrap_or(27017);

        let user = values.get("user").filter(|s| !s.is_empty()).cloned();
        let database = values.get("database").filter(|s| !s.is_empty()).cloned();
        let auth_database = values
            .get("auth_database")
            .filter(|s| !s.is_empty())
            .cloned();

        if use_uri && uri.is_none() {
            return Err(DbError::InvalidProfile(
                "Connection URI is required when using URI mode".to_string(),
            ));
        }

        if !use_uri && host.is_empty() {
            return Err(DbError::InvalidProfile("Host is required".to_string()));
        }

        Ok(DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            auth_database,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            auth_database,
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
            values.insert("database".to_string(), database.clone().unwrap_or_default());
            values.insert(
                "auth_database".to_string(),
                auth_database.clone().unwrap_or_default(),
            );
        }

        values
    }

    fn build_uri(&self, values: &FormValues, password: &str) -> Option<String> {
        let host = values.get("host").map(|s| s.as_str()).unwrap_or("");
        let port = values.get("port").map(|s| s.as_str()).unwrap_or("27017");
        let user = values.get("user").map(|s| s.as_str()).unwrap_or("");
        let database = values.get("database").map(|s| s.as_str()).unwrap_or("");
        let auth_db = values
            .get("auth_database")
            .map(|s| s.as_str())
            .unwrap_or("");

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

        let query = if !auth_db.is_empty() {
            format!("?authSource={}", urlencoding::encode(auth_db))
        } else {
            String::new()
        };

        Some(format!(
            "mongodb://{}{}:{}{}{}",
            credentials, host, port, db_part, query
        ))
    }

    fn parse_uri(&self, uri: &str) -> Option<FormValues> {
        if uri.starts_with("mongodb+srv://") {
            return Some(parse_srv_uri(uri));
        }

        let stripped = uri.strip_prefix("mongodb://")?;

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
        } else {
            values.insert("user".to_string(), String::new());
        }

        let (host_port_db, query) = if let Some(q) = host_part.find('?') {
            (&host_part[..q], Some(&host_part[q + 1..]))
        } else {
            (host_part, None)
        };

        let (host_port, database) = if let Some(slash) = host_port_db.find('/') {
            (&host_port_db[..slash], &host_port_db[slash + 1..])
        } else {
            (host_port_db, "")
        };

        values.insert("database".to_string(), database.to_string());

        if let Some(colon) = host_port.rfind(':') {
            values.insert("host".to_string(), host_port[..colon].to_string());
            values.insert("port".to_string(), host_port[colon + 1..].to_string());
        } else {
            values.insert("host".to_string(), host_port.to_string());
            values.insert("port".to_string(), "27017".to_string());
        }

        let mut found_auth_source = false;
        if let Some(query_str) = query {
            for param in query_str.split('&') {
                if let Some(val) = param.strip_prefix("authSource=") {
                    let auth_db = urlencoding::decode(val).unwrap_or_default().into_owned();
                    values.insert("auth_database".to_string(), auth_db);
                    found_auth_source = true;
                }
            }
        }
        if !found_auth_source {
            values.insert("auth_database".to_string(), String::new());
        }

        Some(values)
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_mongodb_config(&profile.config)?;
        let schema_settings = Self::schema_settings(profile);

        let password = password.map(|value| value.expose_secret());
        let ssh_secret = ssh_secret.map(|value| value.expose_secret());

        if config.use_uri {
            self.connect_with_uri(
                config.uri.as_deref().unwrap_or(""),
                config.user.as_deref(),
                password,
                config.database,
                schema_settings,
            )
        } else if let Some(tunnel_config) = &config.ssh_tunnel {
            self.connect_via_ssh_tunnel(
                tunnel_config,
                ssh_secret,
                &config.host,
                config.port,
                config.user.as_deref(),
                config.database.clone(),
                config.auth_database.as_deref(),
                password,
                schema_settings,
            )
        } else {
            self.connect_direct(
                &config.host,
                config.port,
                config.user.as_deref(),
                config.database,
                config.auth_database.as_deref(),
                password,
                schema_settings,
            )
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }
}

impl MongoDriver {
    fn schema_settings(profile: &ConnectionProfile) -> MongoSchemaSettings {
        let settings = profile.connection_settings.as_ref();

        let schema_sample_size = settings
            .and_then(|values| values.get("schema_sample_size"))
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SAMPLE_SIZE);

        let show_system_databases = settings
            .and_then(|values| values.get("show_system_databases"))
            .map(|value| value == "true")
            .unwrap_or(false);

        MongoSchemaSettings {
            schema_sample_size,
            show_system_databases,
        }
    }

    fn connect_with_uri(
        &self,
        base_uri: &str,
        user: Option<&str>,
        password: Option<&str>,
        database: Option<String>,
        schema_settings: MongoSchemaSettings,
    ) -> Result<Box<dyn Connection>, DbError> {
        let uri = inject_credentials_into_uri(base_uri, user, password);

        log::info!("Connecting to MongoDB with URI");

        let client =
            Client::with_uri_str(&uri).map_err(|e| format_mongo_uri_error(&e, base_uri))?;

        client
            .list_database_names()
            .run()
            .map_err(|e| format_mongo_uri_error(&e, base_uri))?;

        log::info!("[CONNECT] MongoDB connection established via URI");

        Ok(Box::new(MongoConnection {
            client: Mutex::new(client),
            default_database: database,
            schema_settings,
            ssh_tunnel: None,
            connection_uri: sanitize_uri(&uri),
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_direct(
        &self,
        host: &str,
        port: u16,
        user: Option<&str>,
        database: Option<String>,
        auth_database: Option<&str>,
        password: Option<&str>,
        schema_settings: MongoSchemaSettings,
    ) -> Result<Box<dyn Connection>, DbError> {
        let uri = build_mongodb_uri(host, port, user, password, auth_database);

        log::info!("Connecting to MongoDB at {}:{}", host, port);

        let client = Client::with_uri_str(&uri).map_err(|e| format_mongo_error(&e, host, port))?;

        client
            .list_database_names()
            .run()
            .map_err(|e| format_mongo_error(&e, host, port))?;

        log::info!("[CONNECT] MongoDB connection established");

        Ok(Box::new(MongoConnection {
            client: Mutex::new(client),
            default_database: database,
            schema_settings,
            ssh_tunnel: None,
            connection_uri: sanitize_uri(&uri),
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
        user: Option<&str>,
        database: Option<String>,
        auth_database: Option<&str>,
        password: Option<&str>,
        schema_settings: MongoSchemaSettings,
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

        log::info!("[DB] Connecting to MongoDB via tunnel");
        let phase_start = Instant::now();

        let uri = build_mongodb_uri("127.0.0.1", local_port, user, password, auth_database);
        let client = Client::with_uri_str(&uri)
            .map_err(|e| format_mongo_error(&e, "127.0.0.1", local_port))?;

        client
            .list_database_names()
            .run()
            .map_err(|e| format_mongo_error(&e, "127.0.0.1", local_port))?;

        log::info!(
            "[DB] MongoDB connection established in {:.2}ms",
            phase_start.elapsed().as_secs_f64() * 1000.0
        );

        log::info!(
            "[CONNECT] Total connection time: {:.2}ms ({}:{} via SSH {})",
            total_start.elapsed().as_secs_f64() * 1000.0,
            db_host,
            db_port,
            tunnel_config.host
        );

        Ok(Box::new(MongoConnection {
            client: Mutex::new(client),
            default_database: database,
            schema_settings,
            ssh_tunnel: Some(tunnel),
            connection_uri: sanitize_uri(&uri),
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }
}

#[derive(Clone, Copy)]
struct MongoSchemaSettings {
    schema_sample_size: i32,
    show_system_databases: bool,
}

struct ExtractedMongoConfig {
    use_uri: bool,
    uri: Option<String>,
    host: String,
    port: u16,
    user: Option<String>,
    database: Option<String>,
    auth_database: Option<String>,
    ssh_tunnel: Option<SshTunnelConfig>,
}

fn extract_mongodb_config(config: &DbConfig) -> Result<ExtractedMongoConfig, DbError> {
    match config {
        DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            auth_database,
            ssh_tunnel,
            ..
        } => Ok(ExtractedMongoConfig {
            use_uri: *use_uri,
            uri: uri.clone(),
            host: host.clone(),
            port: *port,
            user: user.clone(),
            database: database.clone(),
            auth_database: auth_database.clone(),
            ssh_tunnel: ssh_tunnel.clone(),
        }),
        _ => Err(DbError::InvalidProfile(
            "Expected MongoDB configuration".to_string(),
        )),
    }
}

fn build_mongodb_uri(
    host: &str,
    port: u16,
    user: Option<&str>,
    password: Option<&str>,
    auth_database: Option<&str>,
) -> String {
    let mut uri = String::from("mongodb://");

    if let Some(u) = user {
        uri.push_str(&urlencoding::encode(u));
        if let Some(p) = password {
            uri.push(':');
            uri.push_str(&urlencoding::encode(p));
        }
        uri.push('@');
    }

    uri.push_str(host);
    uri.push(':');
    uri.push_str(&port.to_string());
    uri.push_str("/?appName=dbflux");

    // Add authSource if specified, or default to "admin" when user is provided
    if let Some(auth_db) = auth_database {
        uri.push_str("&authSource=");
        uri.push_str(&urlencoding::encode(auth_db));
    } else if user.is_some() {
        // Default to admin for authenticated connections
        uri.push_str("&authSource=admin");
    }

    uri
}

fn parse_srv_uri(uri: &str) -> FormValues {
    let mut values = HashMap::new();
    values.insert("use_uri".to_string(), "true".to_string());
    values.insert("uri".to_string(), uri.to_string());
    // Clear host/port so sync_uri_to_fields does not retain stale values
    // from a previous non-SRV profile when switching to SRV URI mode.
    values.insert("host".to_string(), String::new());
    values.insert("port".to_string(), String::new());

    let stripped = uri
        .strip_prefix("mongodb+srv://")
        .expect("caller verified mongodb+srv:// prefix");

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
    } else {
        values.insert("user".to_string(), String::new());
    }

    let (host_port_db, query) = if let Some(q) = host_part.find('?') {
        (&host_part[..q], Some(&host_part[q + 1..]))
    } else {
        (host_part, None)
    };

    if let Some(slash) = host_port_db.find('/') {
        values.insert(
            "database".to_string(),
            host_port_db[slash + 1..].to_string(),
        );
    } else {
        values.insert("database".to_string(), String::new());
    }

    let mut found_auth_source = false;
    if let Some(query_str) = query {
        for param in query_str.split('&') {
            if let Some(val) = param.strip_prefix("authSource=") {
                let auth_db = urlencoding::decode(val).unwrap_or_default().into_owned();
                values.insert("auth_database".to_string(), auth_db);
                found_auth_source = true;
            }
        }
    }
    if !found_auth_source {
        values.insert("auth_database".to_string(), String::new());
    }

    values
}

fn inject_credentials_into_uri(
    base_uri: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> String {
    let user_val = user.unwrap_or("");

    // Do not inject when there is no username — injecting ":password@" produces
    // a malformed authority and would silently use the stored password against
    // a URI that was never intended to carry credentials.
    if user_val.is_empty() {
        return base_uri.to_string();
    }

    if base_uri.contains('@') {
        base_uri.to_string()
    } else if let Some(rest) = base_uri.strip_prefix("mongodb://") {
        format!(
            "mongodb://{}:{}@{}",
            urlencoding::encode(user_val),
            urlencoding::encode(password.unwrap_or("")),
            rest
        )
    } else if let Some(rest) = base_uri.strip_prefix("mongodb+srv://") {
        format!(
            "mongodb+srv://{}:{}@{}",
            urlencoding::encode(user_val),
            urlencoding::encode(password.unwrap_or("")),
            rest
        )
    } else {
        base_uri.to_string()
    }
}

pub struct MongoErrorFormatter;

impl MongoErrorFormatter {
    fn format_connection_message(source: &str, host: &str, port: u16) -> String {
        if source.contains("Connection refused") || source.contains("No servers available") {
            format!(
                "Connection refused. Is MongoDB running at {}:{}?",
                host, port
            )
        } else if source.contains("Authentication failed") {
            "Authentication failed. Check username and password.".to_string()
        } else if source.contains("timed out") {
            "Connection timed out.".to_string()
        } else {
            source.to_string()
        }
    }
}

impl QueryErrorFormatter for MongoErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        FormattedError::new(error.to_string())
    }
}

impl ConnectionErrorFormatter for MongoErrorFormatter {
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

        let message =
            if source.contains("Connection refused") || source.contains("No servers available") {
                format!("Connection refused. Check URI: {}", sanitized_uri)
            } else if source.contains("Authentication failed") {
                "Authentication failed. Check username and password.".to_string()
            } else if source.contains("timed out") {
                "Connection timed out.".to_string()
            } else {
                source
            };

        FormattedError::new(message)
    }
}

static MONGO_ERROR_FORMATTER: MongoErrorFormatter = MongoErrorFormatter;

fn format_mongo_uri_error(e: &mongodb::error::Error, uri: &str) -> DbError {
    let sanitized = sanitize_uri(uri);
    let formatted = MONGO_ERROR_FORMATTER.format_uri_error(e, &sanitized);
    formatted.into_connection_error()
}

fn format_mongo_error(e: &mongodb::error::Error, host: &str, port: u16) -> DbError {
    let formatted = MONGO_ERROR_FORMATTER.format_connection_error(e, host, port);
    formatted.into_connection_error()
}

fn format_mongo_query_error(e: &mongodb::error::Error) -> DbError {
    let formatted = MONGO_ERROR_FORMATTER.format_query_error(e);
    let message = formatted.to_display_string();
    log::error!("MongoDB query failed: {}", message);
    formatted.into_query_error()
}

pub struct MongoConnection {
    client: Mutex<Client>,
    default_database: Option<String>,
    schema_settings: MongoSchemaSettings,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
    #[allow(dead_code)]
    connection_uri: String,
    active_query: RwLock<Option<Uuid>>,
    cancelled: Arc<AtomicBool>,
}

struct MongoCancelHandle {
    cancelled: Arc<AtomicBool>,
}

impl QueryCancelHandle for MongoCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);
        log::info!("[CANCEL] MongoDB cancel flag set");
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
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;
        *active = Some(query_id);
        drop(active);
        Ok(Self { active_query })
    }
}

impl Drop for ActiveQueryGuard<'_> {
    fn drop(&mut self) {
        match self.active_query.write() {
            Ok(mut active) => {
                *active = None;
            }
            Err(error) => {
                log::warn!(
                    "[CLEANUP] Failed to clear active MongoDB query state: {}",
                    error
                );
            }
        }
    }
}

fn mongo_filter_json_from_request(
    legacy_filter: Option<&serde_json::Value>,
    semantic_filter: Option<&SemanticFilter>,
) -> Result<Option<serde_json::Value>, DbError> {
    match semantic_filter {
        Some(filter) => Ok(Some(mongo_filter_json_from_semantic(filter)?)),
        None => Ok(legacy_filter.cloned()),
    }
}

fn mongo_filter_document_from_request(
    legacy_filter: Option<&serde_json::Value>,
    semantic_filter: Option<&SemanticFilter>,
) -> Result<Document, DbError> {
    let filter = mongo_filter_json_from_request(legacy_filter, semantic_filter)?;

    filter
        .map(|value| json_to_bson_doc(&value))
        .transpose()
        .map(|value| value.unwrap_or_default())
}

fn mongo_filter_json_from_semantic(filter: &SemanticFilter) -> Result<serde_json::Value, DbError> {
    match filter {
        SemanticFilter::Predicate(predicate) => {
            let field_name = mongo_filter_field_name(&predicate.field)?;

            let filter_value = match predicate.operator {
                WhereOperator::Eq => predicate
                    .value
                    .as_ref()
                    .map(Value::to_serde_json)
                    .ok_or_else(|| {
                        DbError::query_failed("MongoDB semantic filter requires a value")
                    })?,
                WhereOperator::Null => serde_json::Value::Null,
                WhereOperator::Ne => mongo_operator_filter("$ne", predicate.value.as_ref())?,
                WhereOperator::Gt => mongo_operator_filter("$gt", predicate.value.as_ref())?,
                WhereOperator::Gte => mongo_operator_filter("$gte", predicate.value.as_ref())?,
                WhereOperator::Lt => mongo_operator_filter("$lt", predicate.value.as_ref())?,
                WhereOperator::Lte => mongo_operator_filter("$lte", predicate.value.as_ref())?,
                WhereOperator::In => mongo_operator_filter("$in", predicate.value.as_ref())?,
                WhereOperator::NotIn => mongo_operator_filter("$nin", predicate.value.as_ref())?,
                WhereOperator::Regex => mongo_operator_filter("$regex", predicate.value.as_ref())?,
                unsupported => {
                    return Err(DbError::NotSupported(format!(
                        "MongoDB semantic filters do not support operator {:?}",
                        unsupported
                    )));
                }
            };

            let mut object = serde_json::Map::new();
            object.insert(field_name, filter_value);
            Ok(serde_json::Value::Object(object))
        }
        SemanticFilter::And(filters) => mongo_logical_filter_json("$and", filters),
        SemanticFilter::Or(filters) => mongo_logical_filter_json("$or", filters),
        SemanticFilter::Not(filter) => {
            let inner = mongo_filter_json_from_semantic(filter)?;
            Ok(serde_json::json!({ "$nor": [inner] }))
        }
    }
}

fn mongo_logical_filter_json(
    operator: &str,
    filters: &[SemanticFilter],
) -> Result<serde_json::Value, DbError> {
    if filters.is_empty() {
        return Err(DbError::query_failed(format!(
            "MongoDB semantic filter '{}' requires at least one child expression",
            operator
        )));
    }

    let mut object = serde_json::Map::new();
    object.insert(
        operator.to_string(),
        serde_json::Value::Array(
            filters
                .iter()
                .map(mongo_filter_json_from_semantic)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    );
    Ok(serde_json::Value::Object(object))
}

fn mongo_operator_filter(
    operator: &str,
    value: Option<&Value>,
) -> Result<serde_json::Value, DbError> {
    let value =
        value.ok_or_else(|| DbError::query_failed("MongoDB semantic filter requires a value"))?;

    let mut object = serde_json::Map::new();
    object.insert(operator.to_string(), Value::to_serde_json(value));
    Ok(serde_json::Value::Object(object))
}

fn mongo_filter_field_name(field: &SemanticFieldRef) -> Result<String, DbError> {
    match field {
        SemanticFieldRef::Column(column) => Ok(column.qualified_name()),
        SemanticFieldRef::Path(segments) => {
            if segments.is_empty() {
                return Err(DbError::query_failed(
                    "MongoDB semantic filter path must contain at least one segment",
                ));
            }

            Ok(segments.join("."))
        }
    }
}

fn mongo_collection_shell_prefix(collection: &dbflux_core::CollectionRef) -> String {
    format!(
        "db.getSiblingDB({}).getCollection({})",
        serde_json::to_string(&collection.database)
            .unwrap_or_else(|_| format!("\"{}\"", collection.database)),
        serde_json::to_string(&collection.name)
            .unwrap_or_else(|_| format!("\"{}\"", collection.name)),
    )
}

fn plan_mongo_collection_browse(
    request: &CollectionBrowseRequest,
) -> Result<SemanticPlan, DbError> {
    let filter =
        mongo_filter_json_from_request(request.filter.as_ref(), request.semantic_filter.as_ref())?
            .unwrap_or_else(|| serde_json::json!({}));
    let filter_text = serde_json::to_string_pretty(&filter).map_err(|error| {
        DbError::query_failed(format!("Failed to render MongoDB filter preview: {error}"))
    })?;

    let mut query = format!(
        "{}.find({filter_text})",
        mongo_collection_shell_prefix(&request.collection)
    );

    let offset = request.pagination.offset();
    if offset > 0 {
        query.push_str(&format!(".skip({offset})"));
    }

    query.push_str(&format!(".limit({})", request.pagination.limit()));

    Ok(SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::MongoQuery, query)
            .with_database(Some(request.collection.database.clone())),
    ))
}

fn plan_mongo_collection_count(request: &CollectionCountRequest) -> Result<SemanticPlan, DbError> {
    let filter =
        mongo_filter_json_from_request(request.filter.as_ref(), request.semantic_filter.as_ref())?
            .unwrap_or_else(|| serde_json::json!({}));
    let filter_text = serde_json::to_string_pretty(&filter).map_err(|error| {
        DbError::query_failed(format!("Failed to render MongoDB filter preview: {error}"))
    })?;

    let query = format!(
        "{}.countDocuments({filter_text})",
        mongo_collection_shell_prefix(&request.collection)
    );

    Ok(SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::MongoQuery, query)
            .with_database(Some(request.collection.database.clone())),
    ))
}

fn validate_mongo_output_field_name(name: &str, context: &str) -> Result<(), DbError> {
    if name.is_empty() {
        return Err(DbError::query_failed(format!(
            "MongoDB aggregate {context} cannot be empty"
        )));
    }

    if name.starts_with('$') || name.contains('.') {
        return Err(DbError::NotSupported(format!(
            "MongoDB aggregate {context} '{name}' is not supported because output field names cannot contain '.' or start with '$'"
        )));
    }

    Ok(())
}

fn mongo_field_path_from_column(column: &dbflux_core::ColumnRef) -> String {
    column.qualified_name()
}

fn mongo_field_path_from_semantic_field(field: &SemanticFieldRef) -> Result<String, DbError> {
    match field {
        SemanticFieldRef::Column(column) => Ok(mongo_field_path_from_column(column)),
        SemanticFieldRef::Path(segments) => {
            if segments.is_empty() {
                return Err(DbError::query_failed(
                    "MongoDB aggregate field path must contain at least one segment",
                ));
            }

            Ok(segments.join("."))
        }
    }
}

fn mongo_aggregate_output_field_from_semantic(field: &SemanticFieldRef) -> Result<String, DbError> {
    let name = mongo_field_path_from_semantic_field(field)?;
    validate_mongo_output_field_name(&name, "field")?;
    Ok(name)
}

fn mongo_aggregate_output_field_from_column(
    column: &dbflux_core::ColumnRef,
) -> Result<String, DbError> {
    mongo_aggregate_output_field_from_semantic(&column.clone().into())
}

fn mongo_aggregate_accumulator(
    aggregation: &dbflux_core::AggregateSpec,
) -> Result<serde_json::Value, DbError> {
    use dbflux_core::AggregateFunction;

    let column_path = aggregation
        .column
        .as_ref()
        .map(mongo_field_path_from_column);

    match aggregation.function {
        AggregateFunction::Count => {
            if let Some(column_path) = column_path {
                Ok(serde_json::json!({
                    "$sum": {
                        "$cond": [
                            {
                                "$ne": [
                                    { "$ifNull": [format!("${column_path}"), serde_json::Value::Null] },
                                    serde_json::Value::Null
                                ]
                            },
                            1,
                            0
                        ]
                    }
                }))
            } else {
                Ok(serde_json::json!({ "$sum": 1 }))
            }
        }
        AggregateFunction::Sum => {
            let column_path = column_path
                .ok_or_else(|| DbError::query_failed("MongoDB SUM aggregate requires a column"))?;
            Ok(serde_json::json!({ "$sum": format!("${column_path}") }))
        }
        AggregateFunction::Avg => {
            let column_path = column_path
                .ok_or_else(|| DbError::query_failed("MongoDB AVG aggregate requires a column"))?;
            Ok(serde_json::json!({ "$avg": format!("${column_path}") }))
        }
        AggregateFunction::Min => {
            let column_path = column_path
                .ok_or_else(|| DbError::query_failed("MongoDB MIN aggregate requires a column"))?;
            Ok(serde_json::json!({ "$min": format!("${column_path}") }))
        }
        AggregateFunction::Max => {
            let column_path = column_path
                .ok_or_else(|| DbError::query_failed("MongoDB MAX aggregate requires a column"))?;
            Ok(serde_json::json!({ "$max": format!("${column_path}") }))
        }
    }
}

fn plan_mongo_aggregate(request: &dbflux_core::AggregateRequest) -> Result<SemanticPlan, DbError> {
    if request.aggregations.is_empty() {
        return Err(DbError::query_failed(
            "MongoDB aggregate request requires at least one aggregation",
        ));
    }

    let mut pipeline = Vec::new();

    if let Some(filter) = &request.filter {
        pipeline.push(serde_json::json!({
            "$match": mongo_filter_json_from_semantic(filter)?
        }));
    }

    let mut group_id = serde_json::Map::new();
    let mut project = serde_json::Map::new();
    project.insert("_id".to_string(), serde_json::json!(0));

    for column in &request.group_by {
        let field_name = column.qualified_name();
        validate_mongo_output_field_name(&field_name, "group-by column")?;

        group_id.insert(
            field_name.clone(),
            serde_json::json!(format!("${field_name}")),
        );
        project.insert(
            field_name.clone(),
            serde_json::json!(format!("$_id.{field_name}")),
        );
    }

    let mut group = serde_json::Map::new();
    group.insert(
        "_id".to_string(),
        if group_id.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(group_id)
        },
    );

    for aggregation in &request.aggregations {
        validate_mongo_output_field_name(&aggregation.alias, "aggregation alias")?;
        group.insert(
            aggregation.alias.clone(),
            mongo_aggregate_accumulator(aggregation)?,
        );
        project.insert(
            aggregation.alias.clone(),
            serde_json::json!(format!("${}", aggregation.alias)),
        );
    }

    pipeline.push(serde_json::json!({ "$group": serde_json::Value::Object(group) }));
    pipeline.push(serde_json::json!({ "$project": serde_json::Value::Object(project) }));

    if let Some(having) = &request.having {
        pipeline.push(serde_json::json!({
            "$match": mongo_filter_json_from_semantic(having)?
        }));
    }

    if !request.order_by.is_empty() {
        let mut sort = serde_json::Map::new();

        for order in &request.order_by {
            let field_name = mongo_aggregate_output_field_from_column(&order.column)?;
            sort.insert(
                field_name,
                serde_json::json!(match order.direction {
                    dbflux_core::SortDirection::Ascending => 1,
                    dbflux_core::SortDirection::Descending => -1,
                }),
            );
        }

        pipeline.push(serde_json::json!({ "$sort": serde_json::Value::Object(sort) }));
    }

    if let Some(limit) = request.limit {
        pipeline.push(serde_json::json!({ "$limit": limit }));
    }

    let mut query = serde_json::Map::new();
    query.insert(
        "collection".to_string(),
        serde_json::Value::String(request.table.name.clone()),
    );
    query.insert("aggregate".to_string(), serde_json::Value::Array(pipeline));

    let target_database = request
        .target_database
        .clone()
        .or_else(|| request.table.schema.clone());

    if let Some(database) = &target_database {
        query.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    let query_text =
        serde_json::to_string_pretty(&serde_json::Value::Object(query)).map_err(|error| {
            DbError::query_failed(format!(
                "Failed to render MongoDB aggregate preview: {error}"
            ))
        })?;

    Ok(SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::MongoQuery, query_text)
            .with_database(target_database),
    ))
}

fn plan_mongo_mutation(mutation: &dbflux_core::MutationRequest) -> Result<SemanticPlan, DbError> {
    static GENERATOR: crate::query_generator::MongoShellGenerator =
        crate::query_generator::MongoShellGenerator;

    GENERATOR.plan_mutation(mutation).ok_or_else(|| {
        DbError::NotSupported("MongoDB semantic planning does not support this mutation".into())
    })
}

fn plan_mongo_semantic_request(request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
    match request {
        SemanticRequest::CollectionBrowse(request) => plan_mongo_collection_browse(request),
        SemanticRequest::CollectionCount(request) => plan_mongo_collection_count(request),
        SemanticRequest::Aggregate(request) => plan_mongo_aggregate(request),
        SemanticRequest::Mutation(mutation) => plan_mongo_mutation(mutation),
        _ => Err(DbError::NotSupported(
            "MongoDB semantic planning does not support this request".into(),
        )),
    }
}

impl Connection for MongoConnection {
    fn metadata(&self) -> &DriverMetadata {
        &MONGODB_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .run()
            .map_err(|e| format_mongo_query_error(&e))?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        // MongoDB sync client doesn't require explicit close
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.cancelled.store(false, Ordering::SeqCst);
        let query_id = Uuid::new_v4();
        let _active_query_guard = ActiveQueryGuard::activate(&self.active_query, query_id)?;

        let start = Instant::now();

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

        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let query: MongoQuery = crate::query_parser::parse_query(&req.sql)?;

        let db_name = query
            .database
            .as_ref()
            .or(req.database.as_ref())
            .or(self.default_database.as_ref())
            .ok_or_else(|| DbError::query_failed("No database specified".to_string()))?;

        let db = client.database(db_name);

        let result = execute_mongo_query(&client, &db, &query, self.cancelled.clone())?;

        let query_time = start.elapsed();

        log::debug!(
            "[QUERY] Completed in {:.2}ms, {} documents",
            query_time.as_secs_f64() * 1000.0,
            result.rows.len()
        );

        let mut qr = QueryResult::json(result.columns, result.rows, query_time);
        qr.affected_rows = result.affected_rows;
        Ok(qr)
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        match *active {
            Some(id) if id == handle.id => {
                drop(active);
                self.cancelled.store(true, Ordering::SeqCst);
                log::info!("[CANCEL] MongoDB cancel requested for query {}", handle.id);
                Ok(())
            }
            Some(_) => Err(DbError::QueryFailed(
                "No matching active query to cancel".to_string().into(),
            )),
            None => {
                log::debug!(
                    "[CANCEL] Query {} already completed, cancel is a no-op",
                    handle.id
                );
                Ok(())
            }
        }
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);

        let active = self
            .active_query
            .read()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        match *active {
            Some(id) => {
                drop(active);
                log::info!("[CANCEL] MongoDB cancel requested for active query {}", id);
                Ok(())
            }
            None => {
                log::debug!("[CANCEL] No active MongoDB query to cancel");
                Ok(())
            }
        }
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(MongoCancelHandle {
            cancelled: self.cancelled.clone(),
        })
    }

    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        if !self.cancelled.load(Ordering::SeqCst) {
            return Ok(());
        }

        log::info!("[CLEANUP] MongoDB connection cleanup after cancel");
        self.cancelled.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let databases = self.list_databases()?;
        log::info!("[SCHEMA] Found {} databases", databases.len());

        Ok(SchemaSnapshot::document(DocumentSchema {
            databases,
            current_database: self.default_database.clone(),
            collections: Vec::new(),
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db_names = client
            .list_database_names()
            .run()
            .map_err(|e| format_mongo_query_error(&e))?;

        Ok(db_names
            .into_iter()
            .filter(|name| {
                self.schema_settings.show_system_databases
                    || (name != "admin" && name != "config" && name != "local")
            })
            .map(|name| {
                let is_current = self.default_database.as_ref() == Some(&name);
                DatabaseInfo { name, is_current }
            })
            .collect())
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        log::info!("[SCHEMA] Fetching schema for database: {}", database);

        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db = client.database(database);

        let collection_names = db
            .list_collection_names()
            .run()
            .map_err(|e| format_mongo_query_error(&e))?;

        log::info!(
            "[SCHEMA] Found {} collections in {}",
            collection_names.len(),
            database
        );

        // Map collections to TableInfo with stats and indexes
        let tables: Vec<TableInfo> = collection_names
            .into_iter()
            .map(|name| {
                let indexes = fetch_collection_indexes(&db, &name).map(IndexData::Document);
                TableInfo {
                    name,
                    schema: Some(database.to_string()),
                    columns: None,
                    indexes,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                }
            })
            .collect();

        Ok(DbSchemaInfo {
            name: database.to_string(),
            tables,
            views: Vec::new(),
            custom_types: None,
        })
    }

    fn drop_schema_object(
        &self,
        target: &SchemaDropTarget,
        _cascade: bool,
        if_exists: bool,
    ) -> Result<(), DbError> {
        let client = self
            .client
            .lock()
            .map_err(|error| DbError::query_failed(format!("Lock error: {}", error)))?;

        match target.kind {
            SchemaObjectKind::Collection => {
                let database_name = target
                    .database
                    .as_deref()
                    .or(self.default_database.as_deref())
                    .ok_or_else(|| {
                        DbError::query_failed(
                            "MongoDB collection drop requires a target database".to_string(),
                        )
                    })?;

                let database = client.database(database_name);
                let collection_exists = database
                    .list_collection_names()
                    .run()
                    .map_err(|error| format_mongo_query_error(&error))?
                    .into_iter()
                    .any(|name| name == target.name);

                if !collection_exists {
                    return if if_exists {
                        Ok(())
                    } else {
                        Err(DbError::object_not_found(format!(
                            "Collection '{}.{}' was not found",
                            database_name, target.name
                        )))
                    };
                }

                database
                    .collection::<Document>(&target.name)
                    .drop()
                    .run()
                    .map_err(|error| format_mongo_query_error(&error))?;

                Ok(())
            }
            SchemaObjectKind::Database => {
                let database_exists = client
                    .list_database_names()
                    .run()
                    .map_err(|error| format_mongo_query_error(&error))?
                    .into_iter()
                    .any(|name| name == target.name);

                if !database_exists {
                    return if if_exists {
                        Ok(())
                    } else {
                        Err(DbError::object_not_found(format!(
                            "Database '{}' was not found",
                            target.name
                        )))
                    };
                }

                client
                    .database(&target.name)
                    .drop()
                    .run()
                    .map_err(|error| format_mongo_query_error(&error))?;

                Ok(())
            }
            unsupported_kind => Err(DbError::NotSupported(format!(
                "MongoDB drop_schema_object only supports collections and databases, got {:?}",
                unsupported_kind
            ))),
        }
    }

    fn table_details(
        &self,
        database: &str,
        _schema: Option<&str>,
        collection: &str,
    ) -> Result<TableInfo, DbError> {
        log::info!(
            "[SCHEMA] Fetching details for collection: {}.{}",
            database,
            collection
        );

        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db = client.database(database);
        let indexes = fetch_collection_indexes(&db, collection).map(IndexData::Document);

        let sample_fields =
            sample_collection_fields(&db, collection, self.schema_settings.schema_sample_size);

        Ok(TableInfo {
            name: collection.to_string(),
            schema: Some(database.to_string()),
            columns: None,
            indexes,
            foreign_keys: None,
            constraints: None,
            sample_fields: Some(sample_fields),
        })
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();

        let database = request
            .table
            .schema
            .as_deref()
            .or(self.default_database.as_deref())
            .ok_or_else(|| DbError::query_failed("No database specified".to_string()))?;

        let details = self.table_details(database, None, &request.table.name)?;

        let index_lookup = details
            .indexes
            .as_ref()
            .and_then(|indexes| match indexes {
                IndexData::Document(indexes) => Some(build_document_index_lookup(indexes)),
                IndexData::Relational(_) => None,
            })
            .unwrap_or_default();

        let mut flattened_fields = Vec::new();
        if let Some(fields) = details.sample_fields.as_ref() {
            flatten_field_infos(fields, None, &mut flattened_fields);
        }

        let rows = flattened_fields
            .into_iter()
            .map(|field| {
                let index_names = index_lookup.get(&field.name).cloned().unwrap_or_default();

                vec![
                    Value::Text(field.name),
                    Value::Text(field.common_type),
                    field
                        .occurrence_rate
                        .map(|rate| Value::Float(rate as f64))
                        .unwrap_or(Value::Null),
                    Value::Bool(!index_names.is_empty()),
                    if index_names.is_empty() {
                        Value::Null
                    } else {
                        Value::Text(index_names.join(", "))
                    },
                    Value::Int(field.nested_field_count as i64),
                ]
            })
            .collect();

        let columns = vec![
            ColumnMeta {
                name: "field_name".to_string(),
                type_name: "text".to_string(),
                nullable: false,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "common_type".to_string(),
                type_name: "text".to_string(),
                nullable: false,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "occurrence_rate".to_string(),
                type_name: "float".to_string(),
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "is_indexed".to_string(),
                type_name: "bool".to_string(),
                nullable: false,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "index_names".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "nested_field_count".to_string(),
                type_name: "int".to_string(),
                nullable: false,
                is_primary_key: false,
            },
        ];

        Ok(QueryResult::table(columns, rows, None, start.elapsed()))
    }

    fn view_details(
        &self,
        database: &str,
        _schema: Option<&str>,
        view: &str,
    ) -> Result<ViewInfo, DbError> {
        Ok(ViewInfo {
            name: view.to_string(),
            schema: Some(database.to_string()),
        })
    }

    fn kind(&self) -> DbKind {
        DbKind::MongoDB
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn update_document(&self, update: &DocumentUpdate) -> Result<CrudResult, DbError> {
        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db_name = update
            .database
            .as_ref()
            .or(self.default_database.as_ref())
            .ok_or_else(|| DbError::query_failed("No database specified".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(&update.collection);

        let filter = json_to_bson_doc(&update.filter.filter)?;
        let update_doc = json_to_bson_doc(&update.update)?;

        let mut options = mongodb::options::UpdateOptions::default();
        options.upsert = Some(update.upsert);

        let result = if update.many {
            collection
                .update_many(filter, update_doc)
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?
        } else {
            collection
                .update_one(filter, update_doc)
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?
        };

        let affected = result.modified_count + result.upserted_id.map(|_| 1).unwrap_or(0);

        Ok(CrudResult::new(affected, None))
    }

    fn insert_document(&self, insert: &DocumentInsert) -> Result<CrudResult, DbError> {
        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db_name = insert
            .database
            .as_ref()
            .or(self.default_database.as_ref())
            .ok_or_else(|| DbError::query_failed("No database specified".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(&insert.collection);

        if insert.documents.is_empty() {
            return Ok(CrudResult::empty());
        }

        let docs: Vec<Document> = insert
            .documents
            .iter()
            .map(json_to_bson_doc)
            .collect::<Result<Vec<_>, _>>()?;

        if docs.len() == 1 {
            let result = collection
                .insert_one(docs.into_iter().next().unwrap())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let inserted_id = bson_to_value(&Bson::ObjectId(
                result.inserted_id.as_object_id().unwrap_or_default(),
            ));

            Ok(CrudResult::new(1, Some(vec![inserted_id])))
        } else {
            let result = collection
                .insert_many(docs)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(CrudResult::new(result.inserted_ids.len() as u64, None))
        }
    }

    fn delete_document(&self, delete: &DocumentDelete) -> Result<CrudResult, DbError> {
        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db_name = delete
            .database
            .as_ref()
            .or(self.default_database.as_ref())
            .ok_or_else(|| DbError::query_failed("No database specified".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(&delete.collection);

        let filter = json_to_bson_doc(&delete.filter.filter)?;

        let result = if delete.many {
            collection
                .delete_many(filter)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?
        } else {
            collection
                .delete_one(filter)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?
        };

        Ok(CrudResult::new(result.deleted_count, None))
    }

    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();

        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db_name = request.collection.database.as_str();
        let db = client.database(db_name);

        let filter = mongo_filter_document_from_request(
            request.filter.as_ref(),
            request.semantic_filter.as_ref(),
        )?;

        let collection = db.collection::<Document>(&request.collection.name);

        let cursor = collection
            .find(filter)
            .skip(request.pagination.offset())
            .limit(request.pagination.limit() as i64)
            .run()
            .map_err(|e| format_mongo_query_error(&e))?;

        let docs = collect_cursor_documents(cursor, &self.cancelled)?;

        let internal = documents_to_result(docs)?;
        let query_time = start.elapsed();

        log::debug!(
            "[BROWSE] Collection {}.{}: {} documents in {:.2}ms",
            db_name,
            request.collection.name,
            internal.rows.len(),
            query_time.as_secs_f64() * 1000.0,
        );

        let mut qr = QueryResult::json(internal.columns, internal.rows, query_time);
        qr.affected_rows = internal.affected_rows;
        Ok(qr)
    }

    fn count_collection(&self, request: &CollectionCountRequest) -> Result<u64, DbError> {
        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let db_name = request.collection.database.as_str();
        let db = client.database(db_name);
        let collection = db.collection::<Document>(&request.collection.name);

        let filter = mongo_filter_document_from_request(
            request.filter.as_ref(),
            request.semantic_filter.as_ref(),
        )?;

        let count = collection
            .count_documents(filter)
            .run()
            .map_err(|e| format_mongo_query_error(&e))?;

        Ok(count)
    }

    fn language_service(&self) -> &dyn LanguageService {
        &MongoLanguageService
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &MongoDialect
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: crate::query_generator::MongoShellGenerator =
            crate::query_generator::MongoShellGenerator;
        Some(&GENERATOR)
    }

    fn plan_semantic_request(&self, request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
        plan_mongo_semantic_request(request)
    }

    fn build_select_sql(
        &self,
        _table: &str,
        _columns: &[String],
        _filter: Option<&Value>,
        _order_by: &[OrderByColumn],
        _limit: u32,
        _offset: u32,
    ) -> String {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        "SELECT * FROM table WHERE filter LIMIT offset".to_string()
    }

    fn build_insert_sql(
        &self,
        _table: &str,
        _columns: &[String],
        _values: &[Value],
    ) -> (String, Vec<Value>) {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        (
            "INSERT INTO table (columns) VALUES (values)".to_string(),
            Vec::new(),
        )
    }

    fn build_update_sql(
        &self,
        _table: &str,
        _set: &[(String, Value)],
        _filter: Option<&Value>,
    ) -> (String, Vec<Value>) {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        (
            "UPDATE table SET col=val WHERE filter".to_string(),
            Vec::new(),
        )
    }

    fn build_delete_sql(&self, _table: &str, _filter: Option<&Value>) -> (String, Vec<Value>) {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        ("DELETE FROM table WHERE filter".to_string(), Vec::new())
    }

    fn build_upsert_sql(
        &self,
        _table: &str,
        _columns: &[String],
        _values: &[Value],
        _conflict_columns: &[String],
        _update_columns: &[String],
    ) -> (String, Vec<Value>) {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        (
            "INSERT INTO table VALUES (vals) ON CONFLICT DO UPDATE".to_string(),
            Vec::new(),
        )
    }

    fn build_count_sql(&self, _table: &str, _filter: Option<&Value>) -> String {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        "SELECT COUNT(*) FROM table".to_string()
    }

    fn build_truncate_sql(&self, _table: &str) -> String {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        "TRUNCATE TABLE table".to_string()
    }

    fn build_drop_index_sql(
        &self,
        _index_name: &str,
        _table_name: Option<&str>,
        _if_exists: bool,
    ) -> String {
        // MongoDB doesn't use SQL - this is for SQL-based drivers
        "DROP INDEX index_name".to_string()
    }

    fn version_query(&self) -> &'static str {
        // MongoDB uses db.version() shell command, not SQL
        "db.version()"
    }

    fn supports_transactional_ddl(&self) -> bool {
        false
    }

    fn translate_filter(&self, _filter: &Value) -> Result<String, DbError> {
        // For MongoDB, the Value filter is already in document format
        // This is used by SQL-based drivers to convert JSON filters to SQL WHERE clauses
        Err(DbError::NotSupported(
            "translate_filter is not applicable to MongoDB - it uses document-based filters, not SQL".to_string(),
        ))
    }
}

impl DocumentConnection for MongoConnection {}

impl ConnectionExt for MongoConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        None
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        Some(self)
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
    }
}

/// MongoDB language service that validates shell syntax and detects dangerous operations.
struct MongoLanguageService;

impl LanguageService for MongoLanguageService {
    fn validate(&self, query: &str) -> ValidationResult {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return ValidationResult::Valid;
        }

        let lower = trimmed.to_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert into")
            || lower.starts_with("update ")
            || lower.starts_with("delete from")
        {
            return ValidationResult::WrongLanguage {
                expected: QueryLanguage::MongoQuery,
                message: "SQL syntax not supported for MongoDB. Use db.collection.method() or db.method() syntax."
                    .to_string(),
            };
        }

        match crate::query_parser::validate_query(query) {
            Ok(_) => ValidationResult::Valid,
            Err(e) => ValidationResult::SyntaxError(
                Diagnostic::error(format!("Invalid MongoDB query: {}", e))
                    .with_hint("Use db.collection.method() or db.method() syntax"),
            ),
        }
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        detect_dangerous_mongo(query)
    }

    fn editor_diagnostics(&self, query: &str) -> Vec<EditorDiagnostic> {
        let trimmed = query.trim();

        if trimmed.is_empty() {
            return vec![];
        }

        let lower = trimmed.to_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert into")
            || lower.starts_with("update ")
            || lower.starts_with("delete from")
        {
            return vec![EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message: "SQL syntax not supported for MongoDB. Use db.collection.method() or db.method() syntax."
                    .to_string(),
                range: full_first_line_range(query),
            }];
        }

        let errors = crate::query_parser::validate_query_positional(query);
        errors
            .into_iter()
            .map(|err| {
                let range = byte_offset_to_range(query, err.offset, err.len);
                EditorDiagnostic {
                    severity: DiagnosticSeverity::Error,
                    message: err.message,
                    range,
                }
            })
            .collect()
    }
}

fn byte_offset_to_range(source: &str, offset: usize, len: usize) -> TextPositionRange {
    let clamped_offset = offset.min(source.len());
    let clamped_end = (offset + len.max(1))
        .min(source.len())
        .max(clamped_offset + 1);

    let start = byte_offset_to_position(source, clamped_offset);
    let end = byte_offset_to_position(source, clamped_end);

    if start == end {
        let end_col = start.column + 1;
        return TextPositionRange::new(start, TextPosition::new(start.line, end_col));
    }

    TextPositionRange::new(start, end)
}

fn byte_offset_to_position(source: &str, offset: usize) -> TextPosition {
    let before = &source[..offset.min(source.len())];
    let line = before.matches('\n').count() as u32;
    let last_newline = before.rfind('\n').map(|i| i + 1).unwrap_or(0);
    let column = before[last_newline..].chars().count() as u32;
    TextPosition::new(line, column)
}

fn full_first_line_range(query: &str) -> TextPositionRange {
    let first_line_len = query
        .lines()
        .next()
        .map(|line| line.chars().count())
        .unwrap_or(1) as u32;

    let end_col = first_line_len.max(1);

    TextPositionRange::new(TextPosition::new(0, 0), TextPosition::new(0, end_col))
}

/// Stub dialect for MongoDB. SQL generation is not used for document databases.
struct MongoDialect;

impl SqlDialect for MongoDialect {
    fn quote_identifier(&self, name: &str) -> String {
        name.to_string()
    }

    fn qualified_table(&self, database: Option<&str>, collection: &str) -> String {
        match database {
            Some(db) => format!("{}.{}", db, collection),
            None => collection.to_string(),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => format!("\"{}\"", s.replace('\"', "\\\"")),
            Value::Bytes(b) => format!("BinData(0, \"{}\")", base64_encode(b)),
            Value::Json(j) => j.clone(),
            Value::Decimal(d) => d.clone(),
            Value::DateTime(dt) => format!("ISODate(\"{}\")", dt.to_rfc3339()),
            Value::Date(d) => format!("ISODate(\"{}T00:00:00Z\")", d),
            Value::Time(t) => format!("\"{}\"", t),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| self.value_to_literal(v)).collect();
                format!("[{}]", items.join(", "))
            }
            Value::Document(doc) => {
                let pairs: Vec<String> = doc
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, self.value_to_literal(v)))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
            Value::ObjectId(oid) => format!("ObjectId(\"{}\")", oid),
            Value::Unsupported(type_name) => format!("\"UNSUPPORTED<{}>\"", type_name),
        }
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\\', "\\\\").replace('\"', "\\\"")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub struct MongoQuery {
    pub database: Option<String>,
    /// `None` for database-level commands (`db.getName()`, `db.stats()`, etc.).
    pub collection: Option<String>,
    pub operation: MongoOperation,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum MongoOperation {
    Find {
        filter: Document,
        projection: Option<Document>,
        sort: Option<Document>,
        limit: Option<i64>,
        skip: Option<u64>,
    },
    Aggregate {
        pipeline: Vec<Document>,
    },
    Count {
        filter: Document,
    },
    InsertOne {
        document: Document,
    },
    InsertMany {
        documents: Vec<Document>,
    },
    UpdateOne {
        filter: Document,
        update: Document,
        upsert: bool,
    },
    UpdateMany {
        filter: Document,
        update: Document,
        upsert: bool,
    },
    DeleteOne {
        filter: Document,
    },
    DeleteMany {
        filter: Document,
    },
    ReplaceOne {
        filter: Document,
        replacement: Document,
        upsert: bool,
    },
    Drop,

    // -- Database-level operations (db.method()) --
    GetName,
    GetCollectionNames,
    GetCollectionInfos,
    DbStats,
    ServerStatus,
    CreateCollection {
        name: String,
    },
    DropDatabase,
    RunCommand {
        command: Document,
    },
    AdminCommand {
        command: Document,
    },
    Version,
    HostInfo,
    CurrentOp,
}

pub fn json_to_bson_doc(val: &serde_json::Value) -> Result<Document, DbError> {
    let bson = json_to_bson(val)?;
    match bson {
        Bson::Document(doc) => Ok(doc),
        _ => Err(DbError::query_failed("Expected BSON document".to_string())),
    }
}

pub fn json_array_to_bson_docs(val: &serde_json::Value) -> Result<Vec<Document>, DbError> {
    let arr = val
        .as_array()
        .ok_or_else(|| DbError::query_failed("Expected array".to_string()))?;

    arr.iter().map(json_to_bson_doc).collect()
}

fn json_to_bson(val: &serde_json::Value) -> Result<Bson, DbError> {
    match val {
        serde_json::Value::Null => Ok(Bson::Null),
        serde_json::Value::Bool(b) => Ok(Bson::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Bson::Int64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Bson::Double(f))
            } else {
                Err(DbError::query_failed("Invalid number".to_string()))
            }
        }
        serde_json::Value::String(s) => {
            // Check for ObjectId format (24 hex chars)
            if s.len() == 24
                && s.chars().all(|c| c.is_ascii_hexdigit())
                && let Ok(oid) = bson::oid::ObjectId::parse_str(s)
            {
                return Ok(Bson::ObjectId(oid));
            }
            Ok(Bson::String(s.clone()))
        }
        serde_json::Value::Array(arr) => {
            let bson_arr: Result<Vec<Bson>, _> = arr.iter().map(json_to_bson).collect();
            Ok(Bson::Array(bson_arr?))
        }
        serde_json::Value::Object(obj) => {
            // Check for special BSON types like {"$oid": "..."}, {"$date": ...}
            if let Some(oid_val) = obj.get("$oid")
                && let Some(oid_str) = oid_val.as_str()
            {
                let oid = bson::oid::ObjectId::parse_str(oid_str)
                    .map_err(|e| DbError::query_failed(format!("Invalid ObjectId: {}", e)))?;
                return Ok(Bson::ObjectId(oid));
            }

            let mut doc = Document::new();
            for (k, v) in obj {
                doc.insert(k.clone(), json_to_bson(v)?);
            }
            Ok(Bson::Document(doc))
        }
    }
}

struct QueryResultInternal {
    columns: Vec<ColumnMeta>,
    rows: Vec<Row>,
    affected_rows: Option<u64>,
}

fn collect_cursor_documents(
    cursor: mongodb::sync::Cursor<Document>,
    cancelled: &Arc<AtomicBool>,
) -> Result<Vec<Document>, DbError> {
    let mut documents = Vec::new();
    for result in cursor {
        if cancelled.load(Ordering::SeqCst) {
            log::info!("[QUERY] MongoDB query cancelled during cursor iteration");
            return Err(DbError::Cancelled);
        }
        let doc = result.map_err(|e| format_mongo_query_error(&e))?;
        documents.push(doc);
    }
    Ok(documents)
}

fn execute_mongo_query(
    client: &Client,
    db: &Database,
    query: &MongoQuery,
    cancelled: Arc<AtomicBool>,
) -> Result<QueryResultInternal, DbError> {
    if query.collection.is_none() {
        return execute_db_operation(client, db, &query.operation);
    }

    let collection_name = query.collection.as_deref().unwrap_or_default();
    let collection = db.collection::<Document>(collection_name);

    match &query.operation {
        MongoOperation::Find {
            filter,
            projection,
            sort,
            limit,
            skip,
        } => {
            let mut find_options = mongodb::options::FindOptions::default();
            find_options.projection = projection.clone();
            find_options.sort = sort.clone();
            find_options.limit = *limit;
            find_options.skip = *skip;

            let cursor = collection
                .find(filter.clone())
                .with_options(find_options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let documents = collect_cursor_documents(cursor, &cancelled)?;

            documents_to_result(documents)
        }

        MongoOperation::Aggregate { pipeline } => {
            let cursor = collection
                .aggregate(pipeline.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let documents = collect_cursor_documents(cursor, &cancelled)?;

            documents_to_result(documents)
        }

        MongoOperation::Count { filter } => {
            let count = collection
                .count_documents(filter.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "count".to_string(),
                    type_name: "Int64".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Int(count as i64)]],
                affected_rows: None,
            })
        }

        MongoOperation::InsertOne { document } => {
            let result = collection
                .insert_one(document.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let inserted_id = bson_to_value(&result.inserted_id);

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "insertedId".to_string(),
                    type_name: "ObjectId".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![inserted_id]],
                affected_rows: Some(1),
            })
        }

        MongoOperation::InsertMany { documents } => {
            let result = collection
                .insert_many(documents.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let count = result.inserted_ids.len() as u64;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "insertedCount".to_string(),
                    type_name: "Int64".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Int(count as i64)]],
                affected_rows: Some(count),
            })
        }

        MongoOperation::UpdateOne {
            filter,
            update,
            upsert,
        } => {
            let mut options = mongodb::options::UpdateOptions::default();
            options.upsert = Some(*upsert);

            let result = collection
                .update_one(filter.clone(), update.clone())
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let matched = result.matched_count;
            let modified = result.modified_count;
            let upserted = result.upserted_id.is_some();

            Ok(QueryResultInternal {
                columns: vec![
                    ColumnMeta {
                        name: "matchedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                    ColumnMeta {
                        name: "modifiedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                    ColumnMeta {
                        name: "upserted".to_string(),
                        type_name: "Bool".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                ],
                rows: vec![vec![
                    Value::Int(matched as i64),
                    Value::Int(modified as i64),
                    Value::Bool(upserted),
                ]],
                affected_rows: Some(modified + if upserted { 1 } else { 0 }),
            })
        }

        MongoOperation::UpdateMany {
            filter,
            update,
            upsert,
        } => {
            let mut options = mongodb::options::UpdateOptions::default();
            options.upsert = Some(*upsert);

            let result = collection
                .update_many(filter.clone(), update.clone())
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let matched = result.matched_count;
            let modified = result.modified_count;
            let upserted = result.upserted_id.is_some();

            Ok(QueryResultInternal {
                columns: vec![
                    ColumnMeta {
                        name: "matchedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                    ColumnMeta {
                        name: "modifiedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                    ColumnMeta {
                        name: "upserted".to_string(),
                        type_name: "Bool".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                ],
                rows: vec![vec![
                    Value::Int(matched as i64),
                    Value::Int(modified as i64),
                    Value::Bool(upserted),
                ]],
                affected_rows: Some(modified + if upserted { 1 } else { 0 }),
            })
        }

        MongoOperation::DeleteOne { filter } => {
            let result = collection
                .delete_one(filter.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let deleted = result.deleted_count;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "deletedCount".to_string(),
                    type_name: "Int64".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Int(deleted as i64)]],
                affected_rows: Some(deleted),
            })
        }

        MongoOperation::DeleteMany { filter } => {
            let result = collection
                .delete_many(filter.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let deleted = result.deleted_count;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "deletedCount".to_string(),
                    type_name: "Int64".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Int(deleted as i64)]],
                affected_rows: Some(deleted),
            })
        }

        MongoOperation::ReplaceOne {
            filter,
            replacement,
            upsert,
        } => {
            let mut options = mongodb::options::ReplaceOptions::default();
            options.upsert = Some(*upsert);

            let result = collection
                .replace_one(filter.clone(), replacement.clone())
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let matched = result.matched_count;
            let modified = result.modified_count;
            let upserted = result.upserted_id.is_some();

            Ok(QueryResultInternal {
                columns: vec![
                    ColumnMeta {
                        name: "matchedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                    ColumnMeta {
                        name: "modifiedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                    ColumnMeta {
                        name: "upserted".to_string(),
                        type_name: "Bool".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    },
                ],
                rows: vec![vec![
                    Value::Int(matched as i64),
                    Value::Int(modified as i64),
                    Value::Bool(upserted),
                ]],
                affected_rows: Some(modified + if upserted { 1 } else { 0 }),
            })
        }

        MongoOperation::Drop => {
            collection
                .drop()
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "result".to_string(),
                    type_name: "Text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Text("Collection dropped".to_string())]],
                affected_rows: None,
            })
        }

        MongoOperation::GetName
        | MongoOperation::GetCollectionNames
        | MongoOperation::GetCollectionInfos
        | MongoOperation::DbStats
        | MongoOperation::ServerStatus
        | MongoOperation::CreateCollection { .. }
        | MongoOperation::DropDatabase
        | MongoOperation::RunCommand { .. }
        | MongoOperation::AdminCommand { .. }
        | MongoOperation::Version
        | MongoOperation::HostInfo
        | MongoOperation::CurrentOp => {
            unreachable!("db-level ops handled before collection dispatch")
        }
    }
}

fn execute_db_operation(
    client: &Client,
    db: &Database,
    operation: &MongoOperation,
) -> Result<QueryResultInternal, DbError> {
    match operation {
        MongoOperation::GetName => {
            let name = db.name().to_string();
            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "name".to_string(),
                    type_name: "Text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Text(name)]],
                affected_rows: None,
            })
        }

        MongoOperation::GetCollectionNames => {
            let names = db
                .list_collection_names()
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let rows = names.into_iter().map(|n| vec![Value::Text(n)]).collect();

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "collection".to_string(),
                    type_name: "Text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows,
                affected_rows: None,
            })
        }

        MongoOperation::GetCollectionInfos => {
            let cursor = db
                .list_collections()
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let specs: Vec<mongodb::results::CollectionSpecification> = cursor
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format_mongo_query_error(&e))?;

            let documents: Vec<Document> = specs
                .into_iter()
                .map(|spec| {
                    let mut doc = Document::new();
                    doc.insert("name", spec.name);
                    doc.insert("type", format!("{:?}", spec.collection_type));
                    if let Ok(bson) = bson::to_bson(&spec.options) {
                        doc.insert("options", bson);
                    }
                    doc
                })
                .collect();

            documents_to_result(documents)
        }

        MongoOperation::DbStats => {
            let result = db
                .run_command(doc! { "dbStats": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(vec![result])
        }

        MongoOperation::ServerStatus => {
            let result = db
                .run_command(doc! { "serverStatus": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(vec![result])
        }

        MongoOperation::CreateCollection { name } => {
            db.create_collection(name)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "result".to_string(),
                    type_name: "Text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Text(format!("Collection '{}' created", name))]],
                affected_rows: None,
            })
        }

        MongoOperation::DropDatabase => {
            db.drop().run().map_err(|e| format_mongo_query_error(&e))?;

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "result".to_string(),
                    type_name: "Text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Text("Database dropped".to_string())]],
                affected_rows: None,
            })
        }

        MongoOperation::RunCommand { command } => {
            let result = db
                .run_command(command.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(vec![result])
        }

        MongoOperation::AdminCommand { command } => {
            let admin_db = client.database("admin");
            let result = admin_db
                .run_command(command.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(vec![result])
        }

        MongoOperation::Version => {
            let result = db
                .run_command(doc! { "buildInfo": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let version = result.get_str("version").unwrap_or("unknown").to_string();

            Ok(QueryResultInternal {
                columns: vec![ColumnMeta {
                    name: "version".to_string(),
                    type_name: "Text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                }],
                rows: vec![vec![Value::Text(version)]],
                affected_rows: None,
            })
        }

        MongoOperation::HostInfo => {
            let result = db
                .run_command(doc! { "hostInfo": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(vec![result])
        }

        MongoOperation::CurrentOp => {
            let result = db
                .run_command(doc! { "currentOp": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(vec![result])
        }

        _ => Err(DbError::query_failed(
            "Operation requires a collection target".to_string(),
        )),
    }
}

fn documents_to_result(documents: Vec<Document>) -> Result<QueryResultInternal, DbError> {
    if documents.is_empty() {
        return Ok(QueryResultInternal {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
        });
    }

    // Collect all unique field names from all documents
    let mut field_names: Vec<String> = Vec::new();
    let mut seen_fields: std::collections::HashSet<String> = std::collections::HashSet::new();

    for doc in &documents {
        for key in doc.keys() {
            if !seen_fields.contains(key) {
                seen_fields.insert(key.clone());
                field_names.push(key.clone());
            }
        }
    }

    // Put _id first if present
    if let Some(pos) = field_names.iter().position(|k| k == "_id") {
        field_names.remove(pos);
        field_names.insert(0, "_id".to_string());
    }

    let columns: Vec<ColumnMeta> = field_names
        .iter()
        .map(|name| ColumnMeta {
            name: name.clone(),
            type_name: "BSON".to_string(),
            nullable: true,
            is_primary_key: name == "_id",
        })
        .collect();

    let rows: Vec<Row> = documents
        .iter()
        .map(|doc| {
            field_names
                .iter()
                .map(|field| doc.get(field).map(bson_to_value).unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    Ok(QueryResultInternal {
        columns,
        rows,
        affected_rows: None,
    })
}

fn bson_to_value(bson: &Bson) -> Value {
    match bson {
        Bson::Null => Value::Null,
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::Int32(i) => Value::Int(*i as i64),
        Bson::Int64(i) => Value::Int(*i),
        Bson::Double(f) => Value::Float(*f),
        Bson::String(s) => Value::Text(s.clone()),
        Bson::ObjectId(oid) => Value::ObjectId(oid.to_hex()),
        Bson::DateTime(dt) => {
            // Convert BSON DateTime to chrono DateTime
            let millis = dt.timestamp_millis();
            if let Some(datetime) = chrono::DateTime::from_timestamp_millis(millis) {
                Value::DateTime(datetime)
            } else {
                Value::Text(dt.to_string())
            }
        }
        Bson::Binary(bin) => Value::Bytes(bin.bytes.clone()),
        Bson::Array(arr) => {
            let values: Vec<Value> = arr.iter().map(bson_to_value).collect();
            Value::Array(values)
        }
        Bson::Document(doc) => {
            let map: BTreeMap<String, Value> = doc
                .iter()
                .map(|(k, v)| (k.clone(), bson_to_value(v)))
                .collect();
            Value::Document(map)
        }
        Bson::Decimal128(d) => Value::Decimal(d.to_string()),
        Bson::RegularExpression(regex) => {
            Value::Text(format!("/{}/{}", regex.pattern, regex.options))
        }
        Bson::JavaScriptCode(code) => Value::Text(code.clone()),
        Bson::JavaScriptCodeWithScope(code) => Value::Text(code.code.clone()),
        Bson::Timestamp(ts) => Value::Text(format!("Timestamp({}, {})", ts.time, ts.increment)),
        Bson::Symbol(s) => Value::Text(s.clone()),
        Bson::Undefined => Value::Null,
        Bson::MaxKey => Value::Text("MaxKey".to_string()),
        Bson::MinKey => Value::Text("MinKey".to_string()),
        Bson::DbPointer(_) => Value::Text("DBPointer".to_string()),
    }
}

/// Fetch indexes for a collection. Returns `None` on failure or empty results.
fn fetch_collection_indexes(
    db: &Database,
    collection_name: &str,
) -> Option<Vec<CollectionIndexInfo>> {
    let collection = db.collection::<Document>(collection_name);

    let cursor = match collection.list_indexes().run() {
        Ok(c) => c,
        Err(e) => {
            log::warn!(
                "[SCHEMA] Failed to fetch indexes for {}: {}",
                collection_name,
                e
            );
            return None;
        }
    };

    let indexes: Vec<CollectionIndexInfo> = cursor
        .filter_map(|result| {
            let index_model = match result {
                Ok(model) => model,
                Err(e) => {
                    log::warn!(
                        "[SCHEMA] Failed to deserialize index for {}: {}",
                        collection_name,
                        e
                    );
                    return None;
                }
            };

            let keys: Vec<(String, IndexDirection)> = index_model
                .keys
                .iter()
                .map(|(field, value)| {
                    let direction = bson_to_index_direction(value);
                    (field.to_string(), direction)
                })
                .collect();

            let name = index_model
                .options
                .as_ref()
                .and_then(|opts| opts.name.clone())
                .unwrap_or_else(|| {
                    keys.iter()
                        .map(|(f, _)| f.as_str())
                        .collect::<Vec<_>>()
                        .join("_")
                });

            let is_unique = index_model
                .options
                .as_ref()
                .and_then(|opts| opts.unique)
                .unwrap_or(false);

            let is_sparse = index_model
                .options
                .as_ref()
                .and_then(|opts| opts.sparse)
                .unwrap_or(false);

            let expire_after_seconds = index_model
                .options
                .as_ref()
                .and_then(|opts| opts.expire_after)
                .map(|d| d.as_secs());

            Some(CollectionIndexInfo {
                name,
                keys,
                is_unique,
                is_sparse,
                expire_after_seconds,
            })
        })
        .collect();

    if indexes.is_empty() {
        None
    } else {
        Some(indexes)
    }
}

fn bson_to_index_direction(value: &Bson) -> IndexDirection {
    match value {
        Bson::Int32(1) | Bson::Int64(1) => IndexDirection::Ascending,
        Bson::Int32(-1) | Bson::Int64(-1) => IndexDirection::Descending,
        Bson::Double(v) if *v == 1.0 => IndexDirection::Ascending,
        Bson::Double(v) if *v == -1.0 => IndexDirection::Descending,
        Bson::String(s) => match s.as_str() {
            "text" => IndexDirection::Text,
            "hashed" => IndexDirection::Hashed,
            "2d" => IndexDirection::Geo2d,
            "2dsphere" => IndexDirection::Geo2dSphere,
            _ => IndexDirection::Ascending,
        },
        _ => IndexDirection::Ascending,
    }
}

const DEFAULT_SAMPLE_SIZE: i32 = 100;

fn sample_collection_fields(
    db: &Database,
    collection_name: &str,
    sample_size: i32,
) -> Vec<FieldInfo> {
    let collection = db.collection::<Document>(collection_name);

    let pipeline = vec![doc! { "$sample": { "size": sample_size } }];
    let cursor = match collection.aggregate(pipeline).run() {
        Ok(c) => c,
        Err(e) => {
            log::warn!(
                "[SCHEMA] Failed to sample documents with $sample for {}: {}",
                collection_name,
                e
            );

            match collection.find(doc! {}).limit(sample_size as i64).run() {
                Ok(c) => c,
                Err(find_error) => {
                    log::warn!(
                        "[SCHEMA] Fallback sampling (find+limit) failed for {}: {}",
                        collection_name,
                        find_error
                    );
                    return Vec::new();
                }
            }
        }
    };

    let documents: Vec<Document> = cursor
        .filter_map(|r| match r {
            Ok(doc) => Some(doc),
            Err(e) => {
                log::warn!("[SCHEMA] Error reading sample document: {}", e);
                None
            }
        })
        .collect();

    if documents.is_empty() {
        return Vec::new();
    }

    let total = documents.len() as f32;
    let mut field_stats: BTreeMap<String, FieldStats> = BTreeMap::new();

    for doc in &documents {
        collect_field_stats(doc, &mut field_stats);
    }

    let mut fields: Vec<FieldInfo> = field_stats
        .into_iter()
        .map(|(name, stats)| build_field_info(&name, &stats, total))
        .collect();

    // _id always first, then alphabetical (BTreeMap already sorts the rest)
    fields.sort_by(|a, b| {
        let a_is_id = a.name == "_id";
        let b_is_id = b.name == "_id";
        match (a_is_id, b_is_id) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.cmp(&b.name),
        }
    });

    fields
}

#[derive(Debug)]
struct FlattenedFieldInfo {
    name: String,
    common_type: String,
    occurrence_rate: Option<f32>,
    nested_field_count: usize,
}

fn flatten_field_infos(
    fields: &[FieldInfo],
    prefix: Option<&str>,
    output: &mut Vec<FlattenedFieldInfo>,
) {
    for field in fields {
        let full_name = match prefix {
            Some(prefix) => format!("{}.{}", prefix, field.name),
            None => field.name.clone(),
        };

        let nested_field_count = field.nested_fields.as_ref().map_or(0, Vec::len);

        output.push(FlattenedFieldInfo {
            name: full_name.clone(),
            common_type: field.common_type.clone(),
            occurrence_rate: field.occurrence_rate,
            nested_field_count,
        });

        if let Some(nested_fields) = field.nested_fields.as_ref() {
            flatten_field_infos(nested_fields, Some(&full_name), output);
        }
    }
}

fn build_document_index_lookup(indexes: &[CollectionIndexInfo]) -> BTreeMap<String, Vec<String>> {
    let mut lookup = BTreeMap::new();

    for index in indexes {
        for (field_name, _) in &index.keys {
            lookup
                .entry(field_name.clone())
                .or_insert_with(Vec::new)
                .push(index.name.clone());
        }
    }

    lookup
}

struct FieldStats {
    occurrence_count: u32,
    type_counts: HashMap<String, u32>,
    nested_stats: Option<BTreeMap<String, FieldStats>>,
}

fn collect_field_stats(doc: &Document, stats: &mut BTreeMap<String, FieldStats>) {
    for (key, value) in doc {
        let type_name = bson_type_name(value);
        let entry = stats.entry(key.clone()).or_insert_with(|| FieldStats {
            occurrence_count: 0,
            type_counts: HashMap::new(),
            nested_stats: None,
        });

        entry.occurrence_count += 1;
        *entry.type_counts.entry(type_name).or_insert(0) += 1;

        if let Bson::Document(nested_doc) = value {
            let nested = entry.nested_stats.get_or_insert_with(BTreeMap::new);
            collect_field_stats(nested_doc, nested);
            continue;
        }

        if let Bson::Array(items) = value {
            let nested = entry.nested_stats.get_or_insert_with(BTreeMap::new);

            for item in items {
                if let Bson::Document(nested_doc) = item {
                    collect_field_stats(nested_doc, nested);
                }
            }
        }
    }
}

fn build_field_info(name: &str, stats: &FieldStats, total: f32) -> FieldInfo {
    let common_type = stats
        .type_counts
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(t, _)| t.clone())
        .unwrap_or_else(|| "Unknown".to_string());

    let occurrence_rate = stats.occurrence_count as f32 / total;

    let nested_fields = stats.nested_stats.as_ref().map(|nested| {
        nested
            .iter()
            .map(|(n, s)| build_field_info(n, s, stats.occurrence_count as f32))
            .collect()
    });

    FieldInfo {
        name: name.to_string(),
        common_type,
        occurrence_rate: Some(occurrence_rate),
        nested_fields,
    }
}

fn bson_type_name(value: &Bson) -> String {
    match value {
        Bson::Double(_) => "Double".to_string(),
        Bson::String(_) => "String".to_string(),
        Bson::Document(_) => "Document".to_string(),
        Bson::Array(_) => "Array".to_string(),
        Bson::Binary(_) => "Binary".to_string(),
        Bson::ObjectId(_) => "ObjectId".to_string(),
        Bson::Boolean(_) => "Boolean".to_string(),
        Bson::DateTime(_) => "DateTime".to_string(),
        Bson::Null => "Null".to_string(),
        Bson::RegularExpression(_) => "Regex".to_string(),
        Bson::Int32(_) => "Int32".to_string(),
        Bson::Int64(_) => "Int64".to_string(),
        Bson::Timestamp(_) => "Timestamp".to_string(),
        Bson::Decimal128(_) => "Decimal128".to_string(),
        _ => "Unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_parser::parse_query;
    use dbflux_core::{
        CollectionBrowseRequest, CollectionCountRequest, CollectionRef, DatabaseCategory, DbDriver,
        DbError, QueryLanguage, SemanticFilter, SemanticPlanKind, SemanticRequest, Value,
        WhereOperator,
    };

    #[test]
    fn build_config_requires_uri_in_uri_mode() {
        let driver = MongoDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_defaults_host_and_port() {
        let driver = MongoDriver::new();
        let values = FormValues::new();

        let config = driver.build_config(&values).expect("config should build");
        let DbConfig::MongoDB { host, port, .. } = config else {
            panic!("expected mongodb config");
        };

        assert_eq!(host, "localhost");
        assert_eq!(port, 27017);
    }

    #[test]
    fn extract_values_includes_uri_flags_and_auth_database() {
        let driver = MongoDriver::new();
        let config = DbConfig::MongoDB {
            use_uri: true,
            uri: Some("mongodb://user:pass@host:27017/app?authSource=admin".to_string()),
            host: String::new(),
            port: 27017,
            user: Some("user".to_string()),
            database: Some("app".to_string()),
            auth_database: Some("admin".to_string()),
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(values.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(
            values.get("auth_database").map(String::as_str),
            Some("admin")
        );
    }

    #[test]
    fn build_uri_encodes_credentials_and_auth_source() {
        let driver = MongoDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "localhost".to_string());
        values.insert("port".to_string(), "27017".to_string());
        values.insert("user".to_string(), "app user".to_string());
        values.insert("database".to_string(), "main".to_string());
        values.insert("auth_database".to_string(), "admin db".to_string());

        let uri = driver
            .build_uri(&values, "s3cr@t")
            .expect("mongodb should support uri build");

        assert_eq!(
            uri,
            "mongodb://app%20user:s3cr%40t@localhost:27017/main?authSource=admin%20db"
        );
    }

    #[test]
    fn parse_uri_supports_srv_and_auth_source() {
        let driver = MongoDriver::new();
        let values = driver
            .parse_uri("mongodb+srv://user:pass@cluster0.example.net/main?authSource=admin")
            .expect("mongodb+srv uri should parse");

        assert_eq!(values.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(
            values.get("uri").map(String::as_str),
            Some("mongodb+srv://user:pass@cluster0.example.net/main?authSource=admin")
        );
        assert_eq!(values.get("user").map(String::as_str), Some("user"));
        assert_eq!(values.get("database").map(String::as_str), Some("main"));
        assert_eq!(
            values.get("auth_database").map(String::as_str),
            Some("admin")
        );
        assert_eq!(values.get("host").map(String::as_str), Some(""));
        assert_eq!(values.get("port").map(String::as_str), Some(""));
    }

    #[test]
    fn parse_uri_srv_preserves_original_string() {
        let driver = MongoDriver::new();
        let original = "mongodb+srv://user:pass@cluster.mongodb.net/mydb";
        let values = driver.parse_uri(original).expect("SRV URI should parse");

        assert_eq!(values.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(values.get("uri").map(String::as_str), Some(original));
    }

    #[test]
    fn parse_uri_srv_without_credentials() {
        let driver = MongoDriver::new();
        let values = driver
            .parse_uri("mongodb+srv://cluster.mongodb.net/mydb")
            .expect("SRV URI without credentials should parse");

        assert_eq!(values.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(values.get("user").map(String::as_str), Some(""));
        assert_eq!(values.get("database").map(String::as_str), Some("mydb"));
        assert_eq!(values.get("auth_database").map(String::as_str), Some(""));
    }

    #[test]
    fn parse_uri_standard_mongodb_unchanged() {
        let driver = MongoDriver::new();
        let values = driver
            .parse_uri("mongodb://user:pass@localhost:27017/mydb?authSource=admin")
            .expect("standard mongodb uri should parse");

        assert!(
            values.get("use_uri").is_none(),
            "standard URI should not set use_uri"
        );
        assert!(
            values.get("uri").is_none(),
            "standard URI should not set uri"
        );
        assert_eq!(values.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(values.get("port").map(String::as_str), Some("27017"));
        assert_eq!(values.get("database").map(String::as_str), Some("mydb"));
        assert_eq!(values.get("user").map(String::as_str), Some("user"));
        assert_eq!(
            values.get("auth_database").map(String::as_str),
            Some("admin")
        );
    }

    #[test]
    fn build_config_srv_uri_round_trip() {
        let driver = MongoDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());
        values.insert(
            "uri".to_string(),
            "mongodb+srv://user:pass@cluster.mongodb.net/mydb".to_string(),
        );

        let config = driver.build_config(&values).expect("config should build");
        let DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            ..
        } = config
        else {
            panic!("expected mongodb config");
        };

        assert!(use_uri);
        assert_eq!(
            uri.as_deref(),
            Some("mongodb+srv://user:pass@cluster.mongodb.net/mydb")
        );
        // host/port are defaults when use_uri=true
        assert_eq!(host, "localhost");
        assert_eq!(port, 27017);
    }

    #[test]
    fn parse_uri_rejects_non_mongodb_scheme() {
        let driver = MongoDriver::new();
        assert!(driver.parse_uri("redis://localhost:6379/0").is_none());
    }

    #[test]
    fn srv_uri_full_round_trip_parse_config_extract() {
        let driver = MongoDriver::new();
        let original = "mongodb+srv://user:pass@cluster.mongodb.net/mydb?authSource=admin";

        // Phase 1: parse_uri (simulates user pasting URI into connection form)
        let parsed = driver.parse_uri(original).expect("SRV URI should parse");
        assert_eq!(parsed.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(parsed.get("uri").map(String::as_str), Some(original));

        // Phase 2: build_config (simulates saving the profile)
        let config = driver.build_config(&parsed).expect("config should build");
        let DbConfig::MongoDB {
            use_uri,
            uri,
            user,
            database,
            auth_database,
            ..
        } = config
        else {
            panic!("expected mongodb config");
        };

        assert!(use_uri);
        assert_eq!(uri.as_deref(), Some(original));
        assert_eq!(user.as_deref(), Some("user"));
        assert_eq!(database.as_deref(), Some("mydb"));
        assert_eq!(auth_database.as_deref(), Some("admin"));

        // Phase 3: extract_values (simulates reloading profile for editing)
        let reloaded = driver.extract_values(&DbConfig::MongoDB {
            use_uri,
            uri,
            host: "localhost".to_string(),
            port: 27017,
            user,
            database,
            auth_database,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        });

        assert_eq!(reloaded.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(reloaded.get("uri").map(String::as_str), Some(original));
        assert!(reloaded.get("uri").unwrap().starts_with("mongodb+srv://"));
        assert!(!reloaded.get("uri").unwrap().contains(":27017"));
    }

    #[test]
    fn standard_mongodb_uri_full_round_trip() {
        let driver = MongoDriver::new();
        let original = "mongodb://user:pass@localhost:27017/mydb?authSource=admin";

        let parsed = driver
            .parse_uri(original)
            .expect("standard URI should parse");
        assert!(parsed.get("use_uri").is_none());
        assert!(parsed.get("uri").is_none());

        // Standard URI uses host/port form, not URI mode
        let config = driver.build_config(&parsed).expect("config should build");
        let DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            auth_database,
            ..
        } = config
        else {
            panic!("expected mongodb config");
        };

        assert!(!use_uri);
        assert!(uri.is_none());
        assert_eq!(host, "localhost");
        assert_eq!(port, 27017);
        assert_eq!(user.as_deref(), Some("user"));
        assert_eq!(database.as_deref(), Some("mydb"));
        assert_eq!(auth_database.as_deref(), Some("admin"));

        // Extract values round-trip preserves non-URI mode
        let reloaded = driver.extract_values(&DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            auth_database,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        });

        assert_eq!(reloaded.get("use_uri").map(String::as_str), Some(""));
        assert_eq!(reloaded.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(reloaded.get("port").map(String::as_str), Some("27017"));
    }

    #[test]
    fn inject_credentials_preserves_srv_prefix() {
        let injected = inject_credentials_into_uri(
            "mongodb+srv://cluster.mongodb.net/mydb",
            Some("alice"),
            Some("pw"),
        );
        assert!(injected.starts_with("mongodb+srv://"));
        assert!(!injected.contains(":27017"));
        assert_eq!(injected, "mongodb+srv://alice:pw@cluster.mongodb.net/mydb");

        let untouched = inject_credentials_into_uri(
            "mongodb+srv://existing:creds@cluster.mongodb.net/mydb",
            Some("alice"),
            Some("pw"),
        );
        assert_eq!(
            untouched,
            "mongodb+srv://existing:creds@cluster.mongodb.net/mydb"
        );
    }

    #[test]
    fn parse_srv_uri_clears_host_and_port_from_previous_non_srv() {
        let driver = MongoDriver::new();

        // Simulate previous non-SRV parse that set host/port
        let old = driver
            .parse_uri("mongodb://myhost:27017/mydb")
            .expect("standard URI should parse");
        assert_eq!(old.get("host").map(String::as_str), Some("myhost"));
        assert_eq!(old.get("port").map(String::as_str), Some("27017"));

        // Now parse an SRV URI — host/port must be cleared
        let new = driver
            .parse_uri("mongodb+srv://cluster.mongodb.net/otherdb")
            .expect("SRV URI should parse");
        assert_eq!(new.get("host").map(String::as_str), Some(""));
        assert_eq!(new.get("port").map(String::as_str), Some(""));
    }

    #[test]
    fn standard_parse_uri_clears_user_and_auth_database_when_absent() {
        let driver = MongoDriver::new();

        let with_auth = driver
            .parse_uri("mongodb://alice:pw@localhost/db?authSource=admin")
            .expect("URI with auth should parse");
        assert_eq!(with_auth.get("user").map(String::as_str), Some("alice"));
        assert_eq!(
            with_auth.get("auth_database").map(String::as_str),
            Some("admin")
        );

        let without_auth = driver
            .parse_uri("mongodb://localhost/otherdb")
            .expect("URI without auth should parse");
        assert_eq!(without_auth.get("user").map(String::as_str), Some(""));
        assert_eq!(
            without_auth.get("auth_database").map(String::as_str),
            Some("")
        );
    }

    #[test]
    fn inject_credentials_skips_when_user_is_empty() {
        let result = inject_credentials_into_uri(
            "mongodb://cluster.mongodb.net/mydb",
            Some(""),
            Some("secretpw"),
        );
        assert_eq!(result, "mongodb://cluster.mongodb.net/mydb");

        let result_none = inject_credentials_into_uri(
            "mongodb+srv://cluster.mongodb.net/mydb",
            None,
            Some("secretpw"),
        );
        assert_eq!(result_none, "mongodb+srv://cluster.mongodb.net/mydb");

        let both_none = inject_credentials_into_uri("mongodb://localhost:27017/admin", None, None);
        assert_eq!(both_none, "mongodb://localhost:27017/admin");
    }

    #[test]
    fn parse_srv_uri_clears_optional_fields_when_absent() {
        let driver = MongoDriver::new();
        let values = driver
            .parse_uri("mongodb+srv://cluster.mongodb.net")
            .expect("SRV URI without optional fields should parse");

        assert_eq!(values.get("user").map(String::as_str), Some(""));
        assert_eq!(values.get("database").map(String::as_str), Some(""));
        assert_eq!(values.get("auth_database").map(String::as_str), Some(""));
        assert_eq!(values.get("host").map(String::as_str), Some(""));
        assert_eq!(values.get("port").map(String::as_str), Some(""));
    }

    #[test]
    fn parse_srv_uri_clears_previous_user_when_new_uri_has_none() {
        let driver = MongoDriver::new();

        let with_user = driver
            .parse_uri("mongodb+srv://alice:pw@cluster.mongodb.net/db1?authSource=admin")
            .expect("SRV URI with user should parse");
        assert_eq!(with_user.get("user").map(String::as_str), Some("alice"));
        assert_eq!(with_user.get("database").map(String::as_str), Some("db1"));
        assert_eq!(
            with_user.get("auth_database").map(String::as_str),
            Some("admin")
        );

        let without_user = driver
            .parse_uri("mongodb+srv://cluster.mongodb.net")
            .expect("SRV URI without user should parse");
        assert_eq!(without_user.get("user").map(String::as_str), Some(""));
        assert_eq!(without_user.get("database").map(String::as_str), Some(""));
        assert_eq!(
            without_user.get("auth_database").map(String::as_str),
            Some("")
        );
    }

    #[test]
    fn inject_credentials_adds_only_when_absent() {
        let injected = inject_credentials_into_uri(
            "mongodb://localhost:27017/admin",
            Some("alice"),
            Some("pw"),
        );
        assert_eq!(injected, "mongodb://alice:pw@localhost:27017/admin");

        let untouched = inject_credentials_into_uri(
            "mongodb://existing:creds@localhost:27017/admin",
            Some("alice"),
            Some("pw"),
        );
        assert_eq!(untouched, "mongodb://existing:creds@localhost:27017/admin");
    }

    #[test]
    fn build_mongodb_uri_defaults_auth_source_for_authenticated_connections() {
        let uri = build_mongodb_uri("localhost", 27017, Some("user"), Some("pass"), None);
        assert!(uri.contains("authSource=admin"));
    }

    #[test]
    fn extract_mongodb_config_rejects_non_mongodb_config() {
        let result = extract_mongodb_config(&DbConfig::default_postgres());
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn metadata_and_form_definition_match_mongodb_contract() {
        let driver = MongoDriver::new();
        let metadata = driver.metadata();

        assert_eq!(metadata.category, DatabaseCategory::Document);
        assert_eq!(metadata.query_language, QueryLanguage::MongoQuery);
        assert_eq!(metadata.default_port, Some(27017));
        assert_eq!(metadata.uri_scheme, "mongodb");
        assert!(!driver.form_definition().tabs.is_empty());
    }

    #[test]
    fn test_parse_find_query() {
        let json = r#"{"collection": "users", "filter": {"name": "John"}}"#;
        let query = parse_query(json).unwrap();
        assert_eq!(query.collection.as_deref(), Some("users"));
        assert!(matches!(query.operation, MongoOperation::Find { .. }));
    }

    #[test]
    fn test_parse_aggregate_query() {
        let json = r#"{"collection": "orders", "aggregate": [{"$match": {"status": "active"}}]}"#;
        let query = parse_query(json).unwrap();
        assert_eq!(query.collection.as_deref(), Some("orders"));
        assert!(matches!(query.operation, MongoOperation::Aggregate { .. }));
    }

    #[test]
    fn test_parse_count_query() {
        let json = r#"{"collection": "products", "count": {}}"#;
        let query = parse_query(json).unwrap();
        assert_eq!(query.collection.as_deref(), Some("products"));
        assert!(matches!(query.operation, MongoOperation::Count { .. }));
    }

    #[test]
    fn test_bson_to_value_primitives() {
        assert_eq!(bson_to_value(&Bson::Null), Value::Null);
        assert_eq!(bson_to_value(&Bson::Boolean(true)), Value::Bool(true));
        assert_eq!(bson_to_value(&Bson::Int32(42)), Value::Int(42));
        assert_eq!(bson_to_value(&Bson::Int64(100)), Value::Int(100));
        assert_eq!(bson_to_value(&Bson::Double(3.14)), Value::Float(3.14));
        assert_eq!(
            bson_to_value(&Bson::String("hello".to_string())),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_bson_to_value_objectid() {
        let oid = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let value = bson_to_value(&Bson::ObjectId(oid));
        assert!(matches!(value, Value::ObjectId(_)));
        if let Value::ObjectId(id) = value {
            assert_eq!(id, "507f1f77bcf86cd799439011");
        }
    }

    #[test]
    fn test_bson_to_value_array() {
        let arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
        let value = bson_to_value(&arr);
        assert!(matches!(value, Value::Array(_)));
        if let Value::Array(arr) = value {
            assert_eq!(arr.len(), 3);
        }
    }

    #[test]
    fn test_bson_to_value_document() {
        let doc = doc! { "name": "test", "value": 42 };
        let value = bson_to_value(&Bson::Document(doc));
        assert!(matches!(value, Value::Document(_)));
        if let Value::Document(map) = value {
            assert_eq!(map.len(), 2);
        }
    }

    #[test]
    fn settings_schema_exposes_schema_fields() {
        let driver = MongoDriver::new();
        let schema = driver
            .settings_schema()
            .expect("mongodb should have a settings schema");

        assert_eq!(schema.tabs.len(), 1);
        assert_eq!(schema.tabs[0].sections.len(), 1);

        let section = &schema.tabs[0].sections[0];
        assert_eq!(section.title, "Schema");
        assert_eq!(section.fields.len(), 2);
        assert_eq!(section.fields[0].id, "schema_sample_size");
        assert_eq!(section.fields[0].default_value, "100");
        assert_eq!(section.fields[1].id, "show_system_databases");
        assert_eq!(section.fields[1].default_value, "false");
    }

    #[test]
    fn driver_key_is_builtin_mongodb() {
        let driver = MongoDriver::new();
        assert_eq!(driver.driver_key(), "builtin:mongodb");
    }

    #[test]
    fn semantic_filter_translates_to_mongo_json() {
        let filter = SemanticFilter::and(vec![
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            SemanticFilter::compare("score", WhereOperator::Gte, Value::Int(10)),
        ]);

        let translated = mongo_filter_json_from_semantic(&filter)
            .expect("filter should translate to mongo json");

        assert_eq!(
            translated,
            serde_json::json!({
                "$and": [
                    {"status": "active"},
                    {"score": {"$gte": 10}}
                ]
            })
        );
    }

    #[test]
    fn semantic_planner_builds_collection_browse_preview() {
        let request =
            CollectionBrowseRequest::new(CollectionRef::new("app", "users")).with_semantic_filter(
                SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            );

        let plan = plan_mongo_semantic_request(&SemanticRequest::CollectionBrowse(request))
            .expect("browse request should plan");

        assert_eq!(plan.kind, SemanticPlanKind::Query);
        assert_eq!(plan.queries[0].language, QueryLanguage::MongoQuery);
        assert_eq!(plan.queries[0].target_database.as_deref(), Some("app"));
        assert!(plan.queries[0].text.contains("find"));
        assert!(plan.queries[0].text.contains("status"));
    }

    #[test]
    fn semantic_planner_builds_collection_count_preview() {
        let request =
            CollectionCountRequest::new(CollectionRef::new("app", "users")).with_semantic_filter(
                SemanticFilter::compare("score", WhereOperator::Gt, Value::Int(5)),
            );

        let plan = plan_mongo_semantic_request(&SemanticRequest::CollectionCount(request))
            .expect("count request should plan");

        assert_eq!(plan.kind, SemanticPlanKind::Query);
        assert_eq!(plan.queries[0].language, QueryLanguage::MongoQuery);
        assert!(plan.queries[0].text.contains("countDocuments"));
        assert!(plan.queries[0].text.contains("score"));
    }

    #[test]
    fn semantic_planner_builds_aggregate_preview() {
        let request = dbflux_core::AggregateRequest::new(dbflux_core::TableRef::new("orders"))
            .with_filter(SemanticFilter::compare(
                "status",
                WhereOperator::Eq,
                Value::Text("active".into()),
            ))
            .with_group_by(vec![dbflux_core::ColumnRef::new("customer_id")])
            .with_aggregations(vec![
                dbflux_core::AggregateSpec::count_all("total_orders"),
                dbflux_core::AggregateSpec::new(
                    dbflux_core::AggregateFunction::Sum,
                    Some(dbflux_core::ColumnRef::new("amount")),
                    "total_amount",
                ),
            ])
            .with_having(SemanticFilter::compare(
                "total_orders",
                WhereOperator::Gt,
                Value::Int(1),
            ))
            .with_order_by(vec![dbflux_core::OrderByColumn::desc("total_amount")])
            .with_limit(Some(10))
            .with_target_database(Some("app".into()));

        let plan = plan_mongo_semantic_request(&SemanticRequest::Aggregate(request))
            .expect("mongo aggregate request should plan");

        assert_eq!(plan.kind, SemanticPlanKind::Query);
        assert_eq!(plan.queries[0].language, QueryLanguage::MongoQuery);
        assert_eq!(plan.queries[0].target_database.as_deref(), Some("app"));
        assert!(plan.queries[0].text.contains("\"collection\": \"orders\""));
        assert!(plan.queries[0].text.contains("\"$group\""));
        assert!(plan.queries[0].text.contains("\"total_orders\""));
        assert!(plan.queries[0].text.contains("\"total_amount\""));
        assert!(plan.queries[0].text.contains("\"$match\""));
        assert!(plan.queries[0].text.contains("\"$sort\""));
        assert!(plan.queries[0].text.contains("\"$limit\""));
    }

    #[test]
    fn flatten_field_infos_uses_dot_paths() {
        let fields = vec![FieldInfo {
            name: "profile".to_string(),
            common_type: "Document".to_string(),
            occurrence_rate: Some(1.0),
            nested_fields: Some(vec![FieldInfo {
                name: "email".to_string(),
                common_type: "String".to_string(),
                occurrence_rate: Some(0.5),
                nested_fields: None,
            }]),
        }];

        let mut flattened = Vec::new();
        flatten_field_infos(&fields, None, &mut flattened);

        assert_eq!(flattened.len(), 2);
        assert_eq!(flattened[0].name, "profile");
        assert_eq!(flattened[0].nested_field_count, 1);
        assert_eq!(flattened[1].name, "profile.email");
        assert_eq!(flattened[1].nested_field_count, 0);
    }

    #[test]
    fn build_document_index_lookup_groups_indexes_per_field() {
        let indexes = vec![
            CollectionIndexInfo {
                name: "email_idx".to_string(),
                keys: vec![("profile.email".to_string(), IndexDirection::Ascending)],
                is_unique: true,
                is_sparse: false,
                expire_after_seconds: None,
            },
            CollectionIndexInfo {
                name: "email_text_idx".to_string(),
                keys: vec![("profile.email".to_string(), IndexDirection::Text)],
                is_unique: false,
                is_sparse: false,
                expire_after_seconds: None,
            },
        ];

        let lookup = build_document_index_lookup(&indexes);

        assert_eq!(
            lookup.get("profile.email"),
            Some(&vec!["email_idx".to_string(), "email_text_idx".to_string()])
        );
    }

    #[test]
    fn collect_field_stats_infers_embedded_document_fields_inside_arrays() {
        let document = doc! {
            "items": [
                { "sku": "A-1", "qty": 2 },
                { "sku": "B-2" }
            ]
        };
        let mut stats = BTreeMap::new();

        collect_field_stats(&document, &mut stats);

        let items = stats.get("items").expect("items field should be tracked");
        let field = build_field_info("items", items, 1.0);
        let nested_fields = field
            .nested_fields
            .expect("array-of-document fields should expose nested fields");

        assert_eq!(field.common_type, "Array");
        assert!(nested_fields.iter().any(|nested| nested.name == "sku"));
        assert!(nested_fields.iter().any(|nested| nested.name == "qty"));
    }
}
