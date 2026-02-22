use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Instant;

use bson::{Bson, Document, doc};
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, ColumnMeta, Connection,
    ConnectionErrorFormatter, ConnectionProfile, CrudResult, DangerousQueryKind, DatabaseCategory,
    DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, Diagnostic,
    DiagnosticSeverity, DocumentDelete, DocumentInsert, DocumentSchema, DocumentUpdate,
    DriverCapabilities, DriverFormDef, DriverMetadata, EditorDiagnostic, FormValues,
    FormattedError, Icon, IndexInfo, LanguageService, MONGODB_FORM, PlaceholderStyle,
    QueryErrorFormatter, QueryGenerator, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    Row, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, SshTunnelConfig, TableInfo,
    TextPosition, TextPositionRange, ValidationResult, Value, ViewInfo, detect_dangerous_mongo,
    sanitize_uri,
};
use dbflux_ssh::SshTunnel;
use mongodb::sync::{Client, Database};

/// MongoDB driver metadata.
pub static MONGODB_METADATA: DriverMetadata = DriverMetadata {
    id: "mongodb",
    display_name: "MongoDB",
    description: "Document database for modern applications",
    category: DatabaseCategory::Document,
    query_language: QueryLanguage::MongoQuery,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::DOCUMENT_BASE.bits()
            | DriverCapabilities::AGGREGATION.bits()
            | DriverCapabilities::INDEXES.bits(),
    ),
    default_port: Some(27017),
    uri_scheme: "mongodb",
    icon: Icon::Mongodb,
};

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

    fn metadata(&self) -> &'static DriverMetadata {
        &MONGODB_METADATA
    }

    fn form_definition(&self) -> &'static DriverFormDef {
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
        let stripped = uri
            .strip_prefix("mongodb+srv://")
            .or_else(|| uri.strip_prefix("mongodb://"))?;

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

        if let Some(query_str) = query {
            for param in query_str.split('&') {
                if let Some(val) = param.strip_prefix("authSource=") {
                    let auth_db = urlencoding::decode(val).unwrap_or_default().into_owned();
                    values.insert("auth_database".to_string(), auth_db);
                }
            }
        }

        Some(values)
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_mongodb_config(&profile.config)?;

        if config.use_uri {
            self.connect_with_uri(
                config.uri.as_deref().unwrap_or(""),
                config.user.as_deref(),
                password,
                config.database,
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
            )
        } else {
            self.connect_direct(
                &config.host,
                config.port,
                config.user.as_deref(),
                config.database,
                config.auth_database.as_deref(),
                password,
            )
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }
}

impl MongoDriver {
    fn connect_with_uri(
        &self,
        base_uri: &str,
        user: Option<&str>,
        password: Option<&str>,
        database: Option<String>,
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
            ssh_tunnel: None,
        }))
    }

    fn connect_direct(
        &self,
        host: &str,
        port: u16,
        user: Option<&str>,
        database: Option<String>,
        auth_database: Option<&str>,
        password: Option<&str>,
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
        user: Option<&str>,
        database: Option<String>,
        auth_database: Option<&str>,
        password: Option<&str>,
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
            ssh_tunnel: Some(tunnel),
        }))
    }
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

fn inject_credentials_into_uri(
    base_uri: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> String {
    if user.is_none() && password.is_none() {
        return base_uri.to_string();
    }

    let user = user.unwrap_or("");
    let password = password.unwrap_or("");

    if base_uri.contains('@') {
        base_uri.to_string()
    } else if let Some(rest) = base_uri.strip_prefix("mongodb://") {
        format!(
            "mongodb://{}:{}@{}",
            urlencoding::encode(user),
            urlencoding::encode(password),
            rest
        )
    } else if let Some(rest) = base_uri.strip_prefix("mongodb+srv://") {
        format!(
            "mongodb+srv://{}:{}@{}",
            urlencoding::encode(user),
            urlencoding::encode(password),
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
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
}

impl Connection for MongoConnection {
    fn metadata(&self) -> &'static DriverMetadata {
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
        let start = Instant::now();

        let sql_preview = if req.sql.len() > 80 {
            format!("{}...", &req.sql[..80])
        } else {
            req.sql.clone()
        };
        log::debug!("[QUERY] Executing: {}", sql_preview.replace('\n', " "));

        let client = self
            .client
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        // Parse the query (supports both shell syntax and JSON format)
        let query: MongoQuery = crate::query_parser::parse_query(&req.sql)?;

        // Determine database to use
        let db_name = query
            .database
            .as_ref()
            .or(req.database.as_ref())
            .or(self.default_database.as_ref())
            .ok_or_else(|| DbError::query_failed("No database specified".to_string()))?;

        let db = client.database(db_name);

        let result = execute_mongo_query(&client, &db, &query)?;

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

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        // MongoDB sync driver doesn't support query cancellation
        Err(DbError::NotSupported(
            "Query cancellation not supported for MongoDB".to_string(),
        ))
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
            .filter(|name| name != "admin" && name != "config" && name != "local")
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
                let indexes = fetch_collection_indexes(&db, &name);
                TableInfo {
                    name,
                    schema: Some(database.to_string()),
                    columns: None,
                    indexes,
                    foreign_keys: None,
                    constraints: None,
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

        // For now, return basic info without schema sampling
        Ok(TableInfo {
            name: collection.to_string(),
            schema: Some(database.to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
        })
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

        let filter = request
            .filter
            .as_ref()
            .map(json_to_bson_doc)
            .transpose()?
            .unwrap_or_default();

        let collection = db.collection::<Document>(&request.collection.name);

        let cursor = collection
            .find(filter)
            .skip(request.pagination.offset())
            .limit(request.pagination.limit() as i64)
            .run()
            .map_err(|e| format_mongo_query_error(&e))?;

        let docs: Vec<Document> = cursor
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format_mongo_query_error(&e))?;

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

        let filter = request
            .filter
            .as_ref()
            .map(json_to_bson_doc)
            .transpose()?
            .unwrap_or_default();

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

fn execute_mongo_query(
    client: &Client,
    db: &Database,
    query: &MongoQuery,
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

            let documents: Vec<Document> = cursor
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format_mongo_query_error(&e))?;

            documents_to_result(documents)
        }

        MongoOperation::Aggregate { pipeline } => {
            let cursor = collection
                .aggregate(pipeline.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            let documents: Vec<Document> = cursor
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format_mongo_query_error(&e))?;

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
                    },
                    ColumnMeta {
                        name: "modifiedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                    },
                    ColumnMeta {
                        name: "upserted".to_string(),
                        type_name: "Bool".to_string(),
                        nullable: false,
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
                    },
                    ColumnMeta {
                        name: "modifiedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                    },
                    ColumnMeta {
                        name: "upserted".to_string(),
                        type_name: "Bool".to_string(),
                        nullable: false,
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
                    },
                    ColumnMeta {
                        name: "modifiedCount".to_string(),
                        type_name: "Int64".to_string(),
                        nullable: false,
                    },
                    ColumnMeta {
                        name: "upserted".to_string(),
                        type_name: "Bool".to_string(),
                        nullable: false,
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

/// Fetch indexes for a collection. Returns None if fetching fails (non-blocking).
fn fetch_collection_indexes(db: &Database, collection_name: &str) -> Option<Vec<IndexInfo>> {
    let collection = db.collection::<Document>(collection_name);

    let indexes_result = collection.list_indexes().run();
    let cursor = match indexes_result {
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

    let indexes: Vec<IndexInfo> = cursor
        .filter_map(|result| {
            let index_model = result.ok()?;
            let keys = index_model.keys;

            // Extract column names from the key document
            let columns: Vec<String> = keys.keys().cloned().collect();

            // Get index name
            let name = index_model
                .options
                .as_ref()
                .and_then(|opts| opts.name.clone())
                .unwrap_or_else(|| columns.join("_"));

            // Check if unique
            let is_unique = index_model
                .options
                .as_ref()
                .and_then(|opts| opts.unique)
                .unwrap_or(false);

            // Check if this is the _id index (primary)
            let is_primary = columns.len() == 1 && columns[0] == "_id";

            Some(IndexInfo {
                name,
                columns,
                is_unique,
                is_primary,
            })
        })
        .collect();

    if indexes.is_empty() {
        None
    } else {
        Some(indexes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_parser::parse_query;

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
}
