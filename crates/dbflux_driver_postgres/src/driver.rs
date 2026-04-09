use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    AddEnumValueRequest, AddForeignKeyRequest, CodeGenCapabilities, CodeGenScope, CodeGenerator,
    CodeGeneratorInfo, ColumnInfo, ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionExt,
    ConnectionProfile, ConstraintInfo, ConstraintKind, CreateIndexRequest, CreateTypeRequest,
    CrudResult, CustomTypeInfo, CustomTypeKind, DatabaseCategory, DatabaseInfo, DbConfig, DbDriver,
    DbError, DbKind, DbSchemaInfo, DdlCapabilities, DescribeRequest, DocumentConnection,
    DriverCapabilities, DriverFormDef, DriverLimits, DriverMetadata, DropForeignKeyRequest,
    DropIndexRequest, DropTypeRequest, ErrorLocation, ExplainRequest, ForeignKeyBuilder,
    ForeignKeyInfo, FormValues, FormattedError, Icon, IndexData, IndexInfo, IsolationLevel,
    KeyValueConnection, MutationCapabilities, OrderByColumn, POSTGRES_FORM, PaginationStyle,
    PlaceholderStyle, QueryCancelHandle, QueryCapabilities, QueryErrorFormatter, QueryGenerator,
    QueryHandle, QueryLanguage, QueryRequest, QueryResult, ReindexRequest, RelationalConnection,
    RelationalSchema, Row, RowDelete, RowInsert, RowPatch, SchemaFeatures, SchemaForeignKeyBuilder,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan,
    SemanticPlanKind, SemanticRequest, SortDirection, SqlDialect, SqlMutationGenerator,
    SqlQueryBuilder, SshTunnelConfig, SslMode, SyntaxInfo, TableInfo, TransactionCapabilities,
    TypeDefinition, Value, ViewInfo, WhereOperator, generate_create_table,
    generate_delete_template, generate_drop_table, generate_insert_template, generate_select_star,
    generate_truncate, generate_update_template, render_semantic_filter_sql, sanitize_uri,
};
use dbflux_ssh::SshTunnel;
use native_tls::TlsConnector;
use postgres::types::{FromSql, Kind, Type};
use postgres::{CancelToken as PgCancelToken, Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde_json::Value as JsonValue;
use uuid::Uuid;

/// PostgreSQL driver metadata.
pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "postgres".into(),
    display_name: "PostgreSQL".into(),
    description: "Advanced open-source relational database".into(),
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
            | DriverCapabilities::RETURNING.bits()
            | DriverCapabilities::TRANSACTIONAL_DDL.bits(),
    ),
    default_port: Some(5432),
    uri_scheme: "postgresql".into(),
    icon: Icon::Postgres,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::DollarNumber,
        supports_schemas: true,
        default_schema: Some("public".to_string()),
        case_sensitive_identifiers: true,
    }),
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Offset],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::Like,
            WhereOperator::ILike,
            WhereOperator::Regex,
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::Contains,
            WhereOperator::Overlap,
            WhereOperator::ContainsAll,
            WhereOperator::ContainsAny,
            WhereOperator::Size,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: true,
        max_query_parameters: 32767,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: true,
        supports_returning: true,
        supports_batch: true,
        supports_bulk_update: true,
        supports_bulk_delete: true,
        max_insert_values: 0,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: true,
        supports_drop_database: true,
        supports_create_table: true,
        supports_drop_table: true,
        supports_alter_table: true,
        supports_create_index: true,
        supports_drop_index: true,
        supports_create_view: true,
        supports_drop_view: true,
        supports_create_trigger: false,
        supports_drop_trigger: false,
        transactional_ddl: true,
        supports_add_column: true,
        supports_drop_column: true,
        supports_rename_column: true,
        supports_alter_column: true,
        supports_add_constraint: true,
        supports_drop_constraint: true,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
        ],
        default_isolation_level: Some(IsolationLevel::ReadCommitted),
        supports_savepoints: true,
        supports_nested_transactions: true,
        supports_read_only: true,
        supports_deferrable: false,
    }),
    limits: Some(DriverLimits {
        max_query_length: 0,
        max_parameters: 32767,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 16,
        max_identifier_length: 63,
        max_columns: 250,
        max_indexes_per_table: 32,
    }),
    classification_override: None,
});

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

    fn comparison_column_expr(&self, col_name: &str, col_type: &str) -> String {
        if needs_postgres_text_comparison_cast(col_type) {
            format!("({})::text", col_name)
        } else {
            col_name.to_string()
        }
    }

    fn json_filter_expr(&self, col_name: &str, op: &str, literal: &str, col_type: &str) -> String {
        if col_type.contains("json") {
            format!("({})::jsonb {} ({})", col_name, op, literal)
        } else {
            format!("{} {} {}", col_name, op, literal)
        }
    }

    fn build_upsert_statement(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[String],
        values: &[Value],
        conflict_columns: &[String],
        update_assignments: &[(String, Value)],
    ) -> Option<String> {
        if columns.is_empty() || columns.len() != values.len() || conflict_columns.is_empty() {
            return None;
        }

        let table = self.qualified_table(schema, table);
        let columns = columns
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        let values = values
            .iter()
            .map(|value| self.value_to_literal(value))
            .collect::<Vec<_>>()
            .join(", ");
        let conflict_columns = conflict_columns
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        if update_assignments.is_empty() {
            return Some(format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                table, columns, values, conflict_columns
            ));
        }

        let update_clause = update_assignments
            .iter()
            .map(|(column, value)| {
                format!(
                    "{} = {}",
                    self.quote_identifier(column),
                    self.value_to_literal(value)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table, columns, values, conflict_columns, update_clause
        ))
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
                if values.is_empty() {
                    return None;
                }

                let vals = values
                    .iter()
                    .map(|v| format!("'{}'", POSTGRES_DIALECT.escape_string(v)))
                    .collect::<Vec<_>>()
                    .join(", ");

                Some(format!("CREATE TYPE {} AS ENUM ({});", type_name, vals))
            }

            TypeDefinition::Domain { base_type } => {
                if !is_safe_postgres_type_expression(base_type) {
                    return None;
                }

                Some(format!("CREATE DOMAIN {} AS {};", type_name, base_type))
            }

            TypeDefinition::Composite { attributes } => {
                if attributes.is_empty() {
                    return None;
                }

                let fields = attributes
                    .iter()
                    .map(|attribute| {
                        if !is_safe_postgres_type_expression(&attribute.type_name) {
                            return None;
                        }

                        Some(format!(
                            "    {} {}",
                            self.quote(&attribute.name),
                            attribute.type_name
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?
                    .join(",\n");

                Some(format!("CREATE TYPE {} AS (\n{}\n);", type_name, fields))
            }
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

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:postgres".into()
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_postgres_config(&profile.config)?;

        let password = password.map(|value| value.expose_secret());
        let ssh_secret = ssh_secret.map(|value| value.expose_secret());

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

    fn form_definition(&self) -> &DriverFormDef {
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

    fn with_database(&self, config: &DbConfig, database: &str) -> Option<DbConfig> {
        match config {
            DbConfig::Postgres {
                use_uri,
                uri,
                host,
                port,
                user,
                ssl_mode,
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => Some(DbConfig::Postgres {
                use_uri: *use_uri,
                uri: uri.clone(),
                host: host.clone(),
                port: *port,
                user: user.clone(),
                database: database.to_string(),
                ssl_mode: *ssl_mode,
                ssh_tunnel: ssh_tunnel.clone(),
                ssh_tunnel_profile_id: *ssh_tunnel_profile_id,
            }),
            _ => None,
        }
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

        let ssl_mode = parse_pg_uri_sslmode(&uri);

        if ssl_mode == PgUriSslMode::Disable {
            let client =
                Client::connect(&uri, NoTls).map_err(|e| format_pg_uri_error(&e, base_uri))?;

            let cancel_token = client.cancel_token();
            log::info!("[CONNECT] PostgreSQL connection established via URI");

            return Ok(Box::new(PostgresConnection {
                client: Mutex::new(client),
                ssh_tunnel: None,
                cancel_token,
                active_query: RwLock::new(None),
                cancelled: Arc::new(AtomicBool::new(false)),
            }));
        }

        let accept_invalid_certs = matches!(ssl_mode, PgUriSslMode::Prefer | PgUriSslMode::Require);

        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into()))?;

        let tls = MakeTlsConnector::new(connector);

        let client = match Client::connect(&uri, tls) {
            Ok(c) => c,
            Err(_) if ssl_mode == PgUriSslMode::Prefer => {
                Client::connect(&uri, NoTls).map_err(|e| format_pg_uri_error(&e, base_uri))?
            }
            Err(e) => return Err(format_pg_uri_error(&e, base_uri)),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgUriSslMode {
    Disable,
    Prefer,
    Require,
    Verify,
}

fn parse_pg_uri_sslmode(uri: &str) -> PgUriSslMode {
    let Some(query_start) = uri.find('?') else {
        return PgUriSslMode::Prefer;
    };

    let query = &uri[query_start + 1..];

    let sslmode = query
        .split('&')
        .find_map(|pair| pair.split_once('=').filter(|(key, _)| *key == "sslmode"))
        .map(|(_, value)| value.to_ascii_lowercase());

    match sslmode.as_deref() {
        Some("disable") => PgUriSslMode::Disable,
        Some("prefer") | Some("allow") => PgUriSslMode::Prefer,
        Some("require") => PgUriSslMode::Require,
        Some("verify-ca") | Some("verify-full") => PgUriSslMode::Verify,
        _ => PgUriSslMode::Prefer,
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

fn postgres_code_generators() -> Vec<CodeGeneratorInfo> {
    vec![
        CodeGeneratorInfo {
            id: "create_table".into(),
            label: "CREATE TABLE".into(),
            scope: CodeGenScope::Table,
            order: 10,
            destructive: false,
        },
        CodeGeneratorInfo {
            id: "truncate".into(),
            label: "TRUNCATE".into(),
            scope: CodeGenScope::Table,
            order: 20,
            destructive: true,
        },
        CodeGeneratorInfo {
            id: "drop_table".into(),
            label: "DROP TABLE".into(),
            scope: CodeGenScope::Table,
            order: 21,
            destructive: true,
        },
    ]
}

fn plan_postgres_table_browse(
    request: &dbflux_core::TableBrowseRequest,
) -> Result<SemanticPlan, DbError> {
    let sql = if let Some(filter) = request.semantic_filter.as_ref() {
        let mut sql = format!(
            "SELECT * FROM {}",
            request.table.quoted_with(&POSTGRES_DIALECT)
        );
        let where_clause = render_semantic_filter_sql(filter, &POSTGRES_DIALECT)?;
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause);

        if !request.order_by.is_empty() {
            let order_by = request
                .order_by
                .iter()
                .map(|column| {
                    let direction = match column.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!(
                        "{} {}",
                        column.column.quoted_with(&POSTGRES_DIALECT),
                        direction
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" ORDER BY ");
            sql.push_str(&order_by);
        }

        sql.push_str(&format!(
            " LIMIT {} OFFSET {}",
            request.pagination.limit(),
            request.pagination.offset()
        ));
        sql
    } else {
        request.build_sql_with(&POSTGRES_DIALECT)
    };

    Ok(SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::Sql, sql),
    ))
}

fn plan_postgres_table_count(
    request: &dbflux_core::TableCountRequest,
) -> Result<SemanticPlan, DbError> {
    let quoted_table = request.table.quoted_with(&POSTGRES_DIALECT);
    let sql = if let Some(filter) = request.semantic_filter.as_ref() {
        let where_clause = render_semantic_filter_sql(filter, &POSTGRES_DIALECT)?;
        format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            quoted_table, where_clause
        )
    } else {
        match request.filter.as_deref().map(str::trim) {
            Some(filter) if !filter.is_empty() => {
                format!("SELECT COUNT(*) FROM {} WHERE {}", quoted_table, filter)
            }
            _ => format!("SELECT COUNT(*) FROM {}", quoted_table),
        }
    };

    Ok(SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::Sql, sql),
    ))
}

fn plan_postgres_aggregate(
    request: &dbflux_core::AggregateRequest,
) -> Result<SemanticPlan, DbError> {
    let sql = request.build_sql_with(&POSTGRES_DIALECT)?;

    Ok(SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::Sql, sql)
            .with_database(request.target_database.clone()),
    ))
}

fn plan_postgres_explain(request: &ExplainRequest) -> SemanticPlan {
    let query = request.query.clone().unwrap_or_else(|| {
        format!(
            "SELECT * FROM {} LIMIT 100",
            request.table.quoted_with(&POSTGRES_DIALECT)
        )
    });

    SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(
            QueryLanguage::Sql,
            format!("EXPLAIN (FORMAT JSON) {}", query),
        ),
    )
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
                    "[CLEANUP] Failed to clear active PostgreSQL query state: {}",
                    error
                );
            }
        }
    }
}

fn plan_postgres_describe(request: &DescribeRequest) -> SemanticPlan {
    let schema = request.table.schema.as_deref().unwrap_or("public");
    let escaped_schema = schema.replace('\'', "''");
    let escaped_table = request.table.name.replace('\'', "''");

    let sql = format!(
        "SELECT \
                a.attname AS column_name, \
                format_type(a.atttypid, a.atttypmod) AS data_type, \
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, \
                pg_get_expr(d.adbin, d.adrelid) AS column_default, \
                CASE WHEN a.atttypmod > 0 AND t.typname IN ('varchar', 'bpchar') \
                     THEN a.atttypmod - 4 \
                     ELSE NULL \
                END AS character_maximum_length \
            FROM pg_attribute a \
            JOIN pg_class c ON c.oid = a.attrelid \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            JOIN pg_type t ON t.oid = a.atttypid \
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
            WHERE n.nspname = '{}' \
              AND c.relname = '{}' \
              AND a.attnum > 0 \
              AND NOT a.attisdropped \
            ORDER BY a.attnum",
        escaped_schema, escaped_table
    );

    SemanticPlan::single_query(
        SemanticPlanKind::Query,
        dbflux_core::PlannedQuery::new(QueryLanguage::Sql, sql),
    )
}

fn plan_postgres_mutation(
    mutation: &dbflux_core::MutationRequest,
) -> Result<SemanticPlan, DbError> {
    static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&POSTGRES_DIALECT);

    GENERATOR.plan_mutation(mutation).ok_or_else(|| {
        DbError::NotSupported("PostgreSQL semantic planning does not support this mutation".into())
    })
}

fn plan_postgres_semantic_request(request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
    match request {
        SemanticRequest::TableBrowse(request) => plan_postgres_table_browse(request),
        SemanticRequest::TableCount(request) => plan_postgres_table_count(request),
        SemanticRequest::Aggregate(request) => plan_postgres_aggregate(request),
        SemanticRequest::Explain(request) => Ok(plan_postgres_explain(request)),
        SemanticRequest::Describe(request) => Ok(plan_postgres_describe(request)),
        SemanticRequest::Mutation(mutation) => plan_postgres_mutation(mutation),
        _ => Err(DbError::NotSupported(
            "PostgreSQL semantic planning does not support this request".into(),
        )),
    }
}

impl Connection for PostgresConnection {
    fn metadata(&self) -> &DriverMetadata {
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
        let _active_query_guard = ActiveQueryGuard::activate(&self.active_query, query_id)?;

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
                    is_primary_key: false,
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
            sample_fields: None,
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

    fn code_generators(&self) -> Vec<CodeGeneratorInfo> {
        postgres_code_generators()
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

        let sql = format!("EXPLAIN (FORMAT JSON) {}", query);
        self.execute(&QueryRequest::new(sql))
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        let schema = request.table.schema.as_deref().unwrap_or("public");
        let escaped_schema = schema.replace('\'', "''");
        let escaped_table = request.table.name.replace('\'', "''");

        let sql = format!(
            "SELECT \
                a.attname AS column_name, \
                format_type(a.atttypid, a.atttypmod) AS data_type, \
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, \
                pg_get_expr(d.adbin, d.adrelid) AS column_default, \
                CASE WHEN a.atttypmod > 0 AND t.typname IN ('varchar', 'bpchar') \
                     THEN a.atttypmod - 4 \
                     ELSE NULL \
                END AS character_maximum_length \
            FROM pg_attribute a \
            JOIN pg_class c ON c.oid = a.attrelid \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            JOIN pg_type t ON t.oid = a.atttypid \
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
            WHERE n.nspname = '{}' \
              AND c.relname = '{}' \
              AND a.attnum > 0 \
              AND NOT a.attisdropped \
            ORDER BY a.attnum",
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

    fn plan_semantic_request(&self, request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
        plan_postgres_semantic_request(request)
    }

    fn build_select_sql(
        &self,
        table: &str,
        columns: &[String],
        filter: Option<&Value>,
        order_by: &[OrderByColumn],
        limit: u32,
        offset: u32,
    ) -> String {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);
        let cols = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| POSTGRES_DIALECT.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!("SELECT {} FROM {}", cols, quoted_table);

        if let Some(f) = filter {
            let where_clause = translate_filter_to_sql(f);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
        }

        if !order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_parts = order_by
                .iter()
                .map(|col| {
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {}", col.column.quoted_with(&POSTGRES_DIALECT), dir)
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&order_parts);
        }

        sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));
        sql
    }

    fn build_insert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
    ) -> (String, Vec<Value>) {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);
        let cols = columns
            .iter()
            .map(|c| POSTGRES_DIALECT.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let placeholders: Vec<String> = values
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect();
        let placeholders_str = placeholders.join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quoted_table, cols, placeholders_str
        );

        (sql, values.to_vec())
    }

    fn build_update_sql(
        &self,
        table: &str,
        set: &[(String, Value)],
        filter: Option<&Value>,
    ) -> (String, Vec<Value>) {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);

        let set_parts: Vec<String> = set
            .iter()
            .enumerate()
            .map(|(i, (col, _))| format!("{} = ${}", POSTGRES_DIALECT.quote_identifier(col), i + 1))
            .collect();
        let set_str = set_parts.join(", ");

        let mut sql = format!("UPDATE {} SET {}", quoted_table, set_str);

        if let Some(f) = filter {
            let where_clause = translate_filter_to_sql(f);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
        }

        let mut params: Vec<Value> = set.iter().map(|(_, v)| v.clone()).collect();
        if let Some(f) = filter {
            collect_filter_values(f, &mut params);
        }

        (sql, params)
    }

    fn build_delete_sql(&self, table: &str, filter: Option<&Value>) -> (String, Vec<Value>) {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);
        let mut sql = format!("DELETE FROM {}", quoted_table);
        let mut params = Vec::new();

        if let Some(f) = filter {
            let where_clause = translate_filter_to_sql(f);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
            collect_filter_values(f, &mut params);
        }

        (sql, params)
    }

    fn build_upsert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        conflict_columns: &[String],
        update_columns: &[String],
    ) -> (String, Vec<Value>) {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);
        let cols = columns
            .iter()
            .map(|c| POSTGRES_DIALECT.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let placeholders: Vec<String> = values
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect();
        let placeholders_str = placeholders.join(", ");

        let conflict_cols = conflict_columns
            .iter()
            .map(|c| POSTGRES_DIALECT.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let update_parts: Vec<String> = update_columns
            .iter()
            .map(|col| {
                let idx = columns.iter().position(|c| c == col).unwrap_or(0) + 1;
                format!("{} = ${}", POSTGRES_DIALECT.quote_identifier(col), idx)
            })
            .collect();
        let update_str = update_parts.join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            quoted_table, cols, placeholders_str, conflict_cols, update_str
        );

        (sql, values.to_vec())
    }

    fn build_count_sql(&self, table: &str, filter: Option<&Value>) -> String {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);
        let mut sql = format!("SELECT COUNT(*) FROM {}", quoted_table);

        if let Some(f) = filter {
            let where_clause = translate_filter_to_sql(f);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
        }

        sql
    }

    fn build_truncate_sql(&self, table: &str) -> String {
        let quoted_table = POSTGRES_DIALECT.quote_identifier(table);
        format!("TRUNCATE {} RESTART IDENTITY CASCADE", quoted_table)
    }

    fn build_drop_index_sql(
        &self,
        index_name: &str,
        _table_name: Option<&str>,
        if_exists: bool,
    ) -> String {
        let quoted_index = POSTGRES_DIALECT.quote_identifier(index_name);
        if if_exists {
            format!("DROP INDEX IF EXISTS {} CASCADE", quoted_index)
        } else {
            format!("DROP INDEX {} CASCADE", quoted_index)
        }
    }

    fn version_query(&self) -> &'static str {
        "SELECT version()"
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }

    fn translate_filter(&self, filter: &Value) -> Result<String, DbError> {
        Ok(translate_filter_to_sql(filter))
    }
}

impl RelationalConnection for PostgresConnection {}

impl ConnectionExt for PostgresConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        Some(self)
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        None
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
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
                sample_fields: None,
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
                a.attname AS column_name,
                format_type(a.atttypid, a.atttypmod) AS type_name,
                NOT a.attnotnull AS nullable,
                pg_get_expr(d.adbin, d.adrelid) AS column_default,
                COALESCE(
                    (SELECT true FROM pg_index ix
                     WHERE ix.indrelid = c.oid
                       AND ix.indisprimary
                       AND a.attnum = ANY(ix.indkey)),
                    false
                ) AS is_pk
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            "#,
            &[&schema, &table],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut columns: Vec<ColumnInfo> = rows
        .iter()
        .map(|row| ColumnInfo {
            name: row.get(0),
            type_name: row.get(1),
            nullable: row.get(2),
            default_value: row.get(3),
            is_primary_key: row.get(4),
            enum_values: None,
        })
        .collect();

    let enum_values = fetch_enum_values_for_columns(client, schema, table)?;
    for col in &mut columns {
        if let Some(values) = enum_values.get(&col.type_name) {
            col.enum_values = Some(values.clone());
        }
    }

    Ok(columns)
}

/// Fetch enum values for all enum-typed columns in a table, keyed by type name.
fn fetch_enum_values_for_columns(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<HashMap<String, Vec<String>>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT DISTINCT
                t.typname,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND t.typtype = 'e'
            GROUP BY t.typname
            "#,
            &[&schema, &table],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut result = HashMap::new();
    for row in rows {
        let type_name: String = row.get(0);
        let values: Vec<String> = row.get(1);
        result.insert(type_name, values);
    }
    Ok(result)
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
                c.relname AS table_name,
                a.attname AS column_name,
                format_type(a.atttypid, a.atttypmod) AS type_name,
                NOT a.attnotnull AS nullable,
                pg_get_expr(d.adbin, d.adrelid) AS column_default,
                COALESCE(
                    (SELECT true FROM pg_index ix
                     WHERE ix.indrelid = c.oid
                       AND ix.indisprimary
                       AND a.attnum = ANY(ix.indkey)),
                    false
                ) AS is_pk
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = $1
              AND c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum
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
            enum_values: None,
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
                        pg_catalog.format_type(t.typbasetype, t.typtypmod)
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

fn needs_postgres_text_comparison_cast(type_name: &str) -> bool {
    let normalized = type_name.to_ascii_lowercase();
    normalized == "uuid" || normalized == "tsvector" || normalized == "tsquery"
}

/// Convert a Value to a safe PostgreSQL literal string.
///
/// Uses escaped single-quoted literals for readable generated SQL.
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
        Value::Unsupported(_) => "NULL".to_string(),
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

/// Quote a string as a PostgreSQL literal.
fn pg_quote_string(s: &str) -> String {
    format!("'{}'", pg_escape_string(s))
}

/// Wrapper that decodes textual PostgreSQL values.
///
/// The `postgres` crate's `FromSql<String>` only accepts TEXT/VARCHAR/BPCHAR OIDs,
/// so custom types (enums, domains, composites) fail silently. This wrapper accepts
/// text-compatible custom types and reads the raw bytes as UTF-8.
struct PgText(String);

fn is_textual_pg_type(ty: &Type) -> bool {
    match ty.name() {
        "text" | "varchar" | "bpchar" | "name" | "citext" | "tsvector" | "tsquery" => true,
        _ => match ty.kind() {
            Kind::Enum(_) => true,
            Kind::Domain(inner) => is_textual_pg_type(inner),
            _ => false,
        },
    }
}

impl<'a> FromSql<'a> for PgText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(PgText(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        is_textual_pg_type(ty)
    }
}

fn postgres_array_to_value(row: &postgres::Row, idx: usize, type_name: &str) -> Option<Value> {
    match type_name {
        "_bool" => match row.try_get::<_, Option<Vec<bool>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::Bool).collect())),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_int2" => match row.try_get::<_, Option<Vec<i16>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter().map(|v| Value::Int(v as i64)).collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_int4" => match row.try_get::<_, Option<Vec<i32>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter().map(|v| Value::Int(v as i64)).collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_int8" => match row.try_get::<_, Option<Vec<i64>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::Int).collect())),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_float4" => match row.try_get::<_, Option<Vec<f32>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter().map(|v| Value::Float(v as f64)).collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_float8" => match row.try_get::<_, Option<Vec<f64>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::Float).collect())),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_text" | "_varchar" | "_bpchar" | "_name" | "_citext" => {
            match row.try_get::<_, Option<Vec<String>>>(idx) {
                Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::Text).collect())),
                Ok(None) => Some(Value::Null),
                Err(_) => None,
            }
        }

        "_uuid" => match row.try_get::<_, Option<Vec<Uuid>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter()
                    .map(|uuid| Value::Text(uuid.to_string()))
                    .collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_json" | "_jsonb" => match row.try_get::<_, Option<Vec<JsonValue>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter()
                    .map(|json| Value::Json(json.to_string()))
                    .collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_date" => match row.try_get::<_, Option<Vec<NaiveDate>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::Date).collect())),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_time" => match row.try_get::<_, Option<Vec<NaiveTime>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::Time).collect())),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_timestamp" => match row.try_get::<_, Option<Vec<NaiveDateTime>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter()
                    .map(|ts| Value::DateTime(DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc)))
                    .collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_timestamptz" => match row.try_get::<_, Option<Vec<DateTime<Utc>>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(arr.into_iter().map(Value::DateTime).collect())),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        "_inet" => match row.try_get::<_, Option<Vec<IpAddr>>>(idx) {
            Ok(Some(arr)) => Some(Value::Array(
                arr.into_iter()
                    .map(|ip| Value::Text(ip.to_string()))
                    .collect(),
            )),
            Ok(None) => Some(Value::Null),
            Err(_) => None,
        },

        _ => None,
    }
}

fn postgres_value_to_value(row: &postgres::Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_();
    let type_name = col_type.name();

    if let Some(array_value) = postgres_array_to_value(row, idx, type_name) {
        return array_value;
    }

    match type_name {
        "bool" => row
            .try_get::<_, Option<bool>>(idx)
            .map(|value| value.map(Value::Bool).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "int2" => row
            .try_get::<_, Option<i16>>(idx)
            .map(|value| value.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "int4" => row
            .try_get::<_, Option<i32>>(idx)
            .map(|value| value.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "int8" => row
            .try_get::<_, Option<i64>>(idx)
            .map(|value| value.map(Value::Int).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "float4" => row
            .try_get::<_, Option<f32>>(idx)
            .map(|value| {
                value
                    .map(|float| Value::Float(float as f64))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "float8" | "numeric" => row
            .try_get::<_, Option<f64>>(idx)
            .map(|value| value.map(Value::Float).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "text" | "varchar" | "bpchar" | "name" | "citext" => row
            .try_get::<_, Option<String>>(idx)
            .map(|value| value.map(Value::Text).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "tsvector" | "tsquery" => row
            .try_get::<_, Option<PgText>>(idx)
            .map(|value| {
                value
                    .map(|PgText(text)| Value::Text(text))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "uuid" => row
            .try_get::<_, Option<Uuid>>(idx)
            .map(|value| {
                value
                    .map(|uuid| Value::Text(uuid.to_string()))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "json" | "jsonb" => row
            .try_get::<_, Option<JsonValue>>(idx)
            .map(|value| {
                value
                    .map(|json| Value::Json(json.to_string()))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "date" => row
            .try_get::<_, Option<NaiveDate>>(idx)
            .map(|value| value.map(Value::Date).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "time" => row
            .try_get::<_, Option<NaiveTime>>(idx)
            .map(|value| value.map(Value::Time).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "timestamp" => row
            .try_get::<_, Option<NaiveDateTime>>(idx)
            .map(|value| {
                value
                    .map(|timestamp| {
                        Value::DateTime(DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc))
                    })
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "timestamptz" => row
            .try_get::<_, Option<DateTime<Utc>>>(idx)
            .map(|value| value.map(Value::DateTime).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        "inet" => row
            .try_get::<_, Option<IpAddr>>(idx)
            .map(|value| {
                value
                    .map(|ip| Value::Text(ip.to_string()))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or(Value::Null),

        "bytea" => row
            .try_get::<_, Option<Vec<u8>>>(idx)
            .map(|value| value.map(Value::Bytes).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),

        _ => match col_type.kind() {
            Kind::Enum(_) => match row.try_get::<_, Option<PgText>>(idx) {
                Ok(Some(PgText(s))) => Value::Text(s),
                Ok(None) => Value::Null,
                Err(e) => {
                    let col_name = row.columns()[idx].name();
                    log::info!(
                        "Unsupported PostgreSQL type '{}' (kind: {:?}) for column '{}': {}",
                        type_name,
                        col_type.kind(),
                        col_name,
                        e
                    );
                    Value::Unsupported(type_name.to_string())
                }
            },

            Kind::Domain(inner) if is_textual_pg_type(inner) => {
                match row.try_get::<_, Option<PgText>>(idx) {
                    Ok(Some(PgText(s))) => Value::Text(s),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        let col_name = row.columns()[idx].name();
                        log::info!(
                            "Unsupported PostgreSQL type '{}' (kind: {:?}) for column '{}': {}",
                            type_name,
                            col_type.kind(),
                            col_name,
                            e
                        );
                        Value::Unsupported(type_name.to_string())
                    }
                }
            }

            Kind::Array(inner) if is_textual_pg_type(inner) => {
                match row.try_get::<_, Option<Vec<PgText>>>(idx) {
                    Ok(Some(arr)) => {
                        Value::Array(arr.into_iter().map(|PgText(s)| Value::Text(s)).collect())
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        let col_name = row.columns()[idx].name();
                        log::info!(
                            "Unsupported PostgreSQL array type '{}' for column '{}': {}",
                            type_name,
                            col_name,
                            e
                        );
                        Value::Unsupported(type_name.to_string())
                    }
                }
            }

            _ => {
                let col_name = row.columns()[idx].name();
                log::info!(
                    "Unsupported PostgreSQL type '{}' (kind: {:?}) for column '{}': fallback decode disabled",
                    type_name,
                    col_type.kind(),
                    col_name
                );
                Value::Unsupported(type_name.to_string())
            }
        },
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

fn is_safe_postgres_type_expression(expression: &str) -> bool {
    let trimmed = expression.trim();

    if trimmed.is_empty()
        || trimmed.contains('"')
        || trimmed.contains('\'')
        || trimmed.contains(';')
        || trimmed.contains("--")
        || trimmed.contains("/*")
        || trimmed.contains("*/")
    {
        return false;
    }

    let chars: Vec<char> = trimmed.chars().collect();
    let mut paren_depth = 0usize;
    let mut saw_identifier = false;
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        match ch {
            'A'..='Z' | 'a'..='z' | '_' => saw_identifier = true,
            '0'..='9' | ' ' | '\t' | '\n' | '\r' | '.' | ',' => {}
            '(' => paren_depth += 1,
            ')' => {
                if paren_depth == 0 {
                    return false;
                }
                paren_depth -= 1;
            }
            '[' => {
                if chars.get(index + 1) != Some(&']') {
                    return false;
                }
                index += 1;
            }
            _ => return false,
        }

        index += 1;
    }

    paren_depth == 0 && saw_identifier
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

/// Translate a Value filter expression to a SQL WHERE clause string for PostgreSQL.
fn translate_filter_to_sql(filter: &Value) -> String {
    match filter {
        Value::Document(doc) => {
            let mut parts = Vec::new();
            for (key, value) in doc {
                let quoted_col = POSTGRES_DIALECT.quote_identifier(key);
                let expr = match value {
                    Value::Null => format!("{} IS NULL", quoted_col),
                    Value::Text(s) => format!("{} = '{}'", quoted_col, pg_escape_string(s)),
                    Value::Int(i) => format!("{} = {}", quoted_col, i),
                    Value::Bool(b) => {
                        format!("{} = {}", quoted_col, if *b { "TRUE" } else { "FALSE" })
                    }
                    Value::Float(f) => format!("{} = {}", quoted_col, f),
                    Value::Array(arr) => {
                        if arr.is_empty() {
                            "1=1".to_string()
                        } else {
                            let items: Vec<String> = arr.iter().map(value_to_pg_literal).collect();
                            format!("{} = ANY(ARRAY[{}])", quoted_col, items.join(", "))
                        }
                    }
                    _ => format!("{} = {}", quoted_col, value_to_pg_literal(value)),
                };
                parts.push(expr);
            }
            if parts.is_empty() {
                String::new()
            } else {
                parts.join(" AND ")
            }
        }
        Value::Text(s) => {
            // Treat a plain text filter as a raw SQL expression (for advanced users)
            s.clone()
        }
        _ => String::new(),
    }
}

/// Collect all Value items from a filter expression into a vector for parameterized queries.
fn collect_filter_values(filter: &Value, params: &mut Vec<Value>) {
    if let Value::Document(doc) = filter {
        for value in doc.values() {
            match value {
                Value::Array(arr) => {
                    for item in arr {
                        if !matches!(item, Value::Null) {
                            params.push(item.clone());
                        }
                    }
                }
                Value::Null => {}
                _ => params.push(value.clone()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PgUriSslMode, PostgresCodeGenerator, PostgresDialect, PostgresDriver,
        inject_password_into_pg_uri, parse_pg_uri_sslmode, plan_postgres_semantic_request,
    };
    use dbflux_core::{
        CodeGenerator, CreateTypeRequest, DatabaseCategory, DbConfig, DbDriver, DbError,
        FormValues, MutationRequest, QueryLanguage, RowInsert, SemanticRequest, SqlDialect,
        TableBrowseRequest, TableRef, TypeAttributeDefinition, TypeDefinition, Value,
        WhereOperator,
    };

    #[test]
    fn build_uri_encodes_user_and_password() {
        let driver = PostgresDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "localhost".to_string());
        values.insert("port".to_string(), "5432".to_string());
        values.insert("user".to_string(), "test user".to_string());
        values.insert("database".to_string(), "dbflux".to_string());

        let uri = driver
            .build_uri(&values, "p@ss:word")
            .expect("postgres driver should support URI building");

        assert_eq!(
            uri,
            "postgresql://test%20user:p%40ss%3Aword@localhost:5432/dbflux"
        );
    }

    #[test]
    fn parse_uri_accepts_postgres_and_postgresql_schemes() {
        let driver = PostgresDriver::new();

        let short = driver
            .parse_uri("postgres://user:pass@db.local:5433/app?sslmode=require")
            .expect("short postgres URI should parse");

        assert_eq!(short.get("user").map(String::as_str), Some("user"));
        assert_eq!(short.get("host").map(String::as_str), Some("db.local"));
        assert_eq!(short.get("port").map(String::as_str), Some("5433"));
        assert_eq!(short.get("database").map(String::as_str), Some("app"));

        let long = driver
            .parse_uri("postgresql://alice@localhost/mydb")
            .expect("long postgresql URI should parse");

        assert_eq!(long.get("user").map(String::as_str), Some("alice"));
        assert_eq!(long.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(long.get("port").map(String::as_str), Some("5432"));
        assert_eq!(long.get("database").map(String::as_str), Some("mydb"));
    }

    #[test]
    fn postgres_dialect_formats_special_float_values() {
        let dialect = PostgresDialect;

        assert_eq!(
            dialect.value_to_literal(&Value::Float(f64::NAN)),
            "'NaN'::float8"
        );
        assert_eq!(
            dialect.value_to_literal(&Value::Float(f64::INFINITY)),
            "'Infinity'::float8"
        );
        assert_eq!(
            dialect.value_to_literal(&Value::Float(f64::NEG_INFINITY)),
            "'-Infinity'::float8"
        );
    }

    #[test]
    fn build_config_requires_uri_when_uri_mode_is_enabled() {
        let driver = PostgresDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_validates_manual_fields() {
        let driver = PostgresDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "localhost".to_string());
        values.insert("port".to_string(), "invalid".to_string());
        values.insert("user".to_string(), "postgres".to_string());
        values.insert("database".to_string(), "app".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn extract_values_includes_uri_mode_flags() {
        let driver = PostgresDriver::new();
        let config = DbConfig::Postgres {
            use_uri: true,
            uri: Some("postgresql://u:p@localhost:5432/app".to_string()),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: String::new(),
            ssl_mode: dbflux_core::SslMode::Prefer,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(values.get("use_uri").map(String::as_str), Some("true"));
        assert_eq!(
            values.get("uri").map(String::as_str),
            Some("postgresql://u:p@localhost:5432/app")
        );
    }

    #[test]
    fn parse_uri_rejects_non_postgres_schemes() {
        let driver = PostgresDriver::new();
        assert!(
            driver
                .parse_uri("mysql://root@localhost:3306/app")
                .is_none()
        );
    }

    #[test]
    fn inject_password_into_uri_adds_password_for_user_without_one() {
        let uri =
            inject_password_into_pg_uri("postgresql://user@localhost:5432/app", Some("new pass"));
        assert_eq!(uri, "postgresql://user:new%20pass@localhost:5432/app");
    }

    #[test]
    fn parse_pg_uri_sslmode_uses_reasonable_defaults() {
        assert_eq!(
            parse_pg_uri_sslmode("postgresql://localhost:5432/app"),
            PgUriSslMode::Prefer
        );
        assert_eq!(
            parse_pg_uri_sslmode("postgresql://localhost:5432/app?sslmode=disable"),
            PgUriSslMode::Disable
        );
        assert_eq!(
            parse_pg_uri_sslmode("postgresql://localhost:5432/app?sslmode=require"),
            PgUriSslMode::Require
        );
        assert_eq!(
            parse_pg_uri_sslmode("postgresql://localhost:5432/app?sslmode=verify-full"),
            PgUriSslMode::Verify
        );
    }

    #[test]
    fn metadata_and_form_definition_match_postgres_contract() {
        let driver = PostgresDriver::new();
        let metadata = driver.metadata();

        assert_eq!(metadata.category, DatabaseCategory::Relational);
        assert_eq!(metadata.query_language, QueryLanguage::Sql);
        assert_eq!(metadata.default_port, Some(5432));
        assert_eq!(metadata.uri_scheme, "postgresql");
        assert!(!driver.form_definition().tabs.is_empty());
    }

    #[test]
    fn semantic_planner_builds_browse_query_from_legacy_request_fields() {
        let plan = plan_postgres_semantic_request(&SemanticRequest::TableBrowse(
            TableBrowseRequest::new(TableRef::with_schema("public", "users"))
                .with_filter("status = 'active'"),
        ))
        .expect("postgres planner should handle table browse");

        assert_eq!(plan.kind, dbflux_core::SemanticPlanKind::Query);
        assert_eq!(plan.queries[0].language, QueryLanguage::Sql);
        assert_eq!(
            plan.queries[0].text,
            "SELECT * FROM \"public\".\"users\" WHERE status = 'active' LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn semantic_planner_wraps_sql_mutation_preview() {
        let plan = plan_postgres_semantic_request(&SemanticRequest::Mutation(
            MutationRequest::sql_insert(RowInsert::new(
                "users".to_string(),
                Some("public".to_string()),
                vec!["id".to_string()],
                vec![Value::Int(1)],
            )),
        ))
        .expect("postgres planner should preview sql mutations");

        assert_eq!(plan.kind, dbflux_core::SemanticPlanKind::MutationPreview);
        assert!(plan.queries[0].text.contains("INSERT INTO"));
    }

    #[test]
    fn semantic_planner_builds_aggregate_query() {
        let request = dbflux_core::AggregateRequest::new(TableRef::with_schema("public", "orders"))
            .with_group_by(vec![dbflux_core::ColumnRef::new("customer_id")])
            .with_aggregations(vec![dbflux_core::AggregateSpec::new(
                dbflux_core::AggregateFunction::Sum,
                Some(dbflux_core::ColumnRef::new("amount")),
                "total_amount",
            )])
            .with_having(dbflux_core::SemanticFilter::compare(
                "total_amount",
                WhereOperator::Gt,
                Value::Int(100),
            ))
            .with_limit(Some(10));

        let plan = plan_postgres_semantic_request(&SemanticRequest::Aggregate(request))
            .expect("postgres planner should handle aggregate requests");

        assert_eq!(plan.kind, dbflux_core::SemanticPlanKind::Query);
        assert_eq!(plan.queries[0].language, QueryLanguage::Sql);
        assert_eq!(
            plan.queries[0].text,
            "SELECT \"customer_id\", SUM(\"amount\") AS \"total_amount\" FROM \"public\".\"orders\" GROUP BY \"customer_id\" HAVING \"total_amount\" > 100 LIMIT 10"
        );
    }

    #[test]
    fn postgres_codegen_escapes_enum_values_when_creating_types() {
        let generator = PostgresCodeGenerator;
        let request = CreateTypeRequest {
            type_name: "mood",
            schema_name: Some("public"),
            definition: TypeDefinition::Enum {
                values: vec!["happy".to_string(), "Bob's".to_string()],
            },
        };

        let sql = generator
            .generate_create_type(&request)
            .expect("postgres should generate create type sql");

        assert_eq!(
            sql,
            "CREATE TYPE \"public\".\"mood\" AS ENUM ('happy', 'Bob''s');"
        );
    }

    #[test]
    fn postgres_codegen_uses_composite_attributes_when_creating_types() {
        let generator = PostgresCodeGenerator;
        let request = CreateTypeRequest {
            type_name: "inventory_item",
            schema_name: Some("public"),
            definition: TypeDefinition::Composite {
                attributes: vec![
                    TypeAttributeDefinition {
                        name: "name".to_string(),
                        type_name: "text".to_string(),
                    },
                    TypeAttributeDefinition {
                        name: "supplier_id".to_string(),
                        type_name: "integer".to_string(),
                    },
                ],
            },
        };

        let sql = generator
            .generate_create_type(&request)
            .expect("postgres should generate composite type sql");

        assert_eq!(
            sql,
            "CREATE TYPE \"public\".\"inventory_item\" AS (\n    \"name\" text,\n    \"supplier_id\" integer\n);"
        );
    }

    #[test]
    fn postgres_codegen_skips_enum_types_without_real_values() {
        let generator = PostgresCodeGenerator;
        let request = CreateTypeRequest {
            type_name: "mood",
            schema_name: Some("public"),
            definition: TypeDefinition::Enum { values: vec![] },
        };

        assert!(generator.generate_create_type(&request).is_none());
    }

    #[test]
    fn postgres_codegen_skips_composite_types_without_real_attributes() {
        let generator = PostgresCodeGenerator;
        let request = CreateTypeRequest {
            type_name: "inventory_item",
            schema_name: Some("public"),
            definition: TypeDefinition::Composite { attributes: vec![] },
        };

        assert!(generator.generate_create_type(&request).is_none());
    }

    #[test]
    fn postgres_codegen_rejects_unsafe_domain_type_expression() {
        let generator = PostgresCodeGenerator;
        let request = CreateTypeRequest {
            type_name: "email",
            schema_name: Some("public"),
            definition: TypeDefinition::Domain {
                base_type: "text; DROP TABLE users;".to_string(),
            },
        };

        assert!(generator.generate_create_type(&request).is_none());
    }

    #[test]
    fn postgres_codegen_rejects_unsafe_composite_attribute_type_expression() {
        let generator = PostgresCodeGenerator;
        let request = CreateTypeRequest {
            type_name: "inventory_item",
            schema_name: Some("public"),
            definition: TypeDefinition::Composite {
                attributes: vec![TypeAttributeDefinition {
                    name: "supplier_id".to_string(),
                    type_name: "integer); DROP TYPE mood; --".to_string(),
                }],
            },
        };

        assert!(generator.generate_create_type(&request).is_none());
    }
}
