use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    AddColumnRequest, AddEnumValueRequest, AddForeignKeyRequest, AlterColumnRequest,
    CodeGenCapabilities, CodeGenScope, CodeGenerator, CodeGeneratorInfo, ColumnInfo, ColumnKind,
    ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionExt, ConnectionProfile,
    ConstraintInfo, ConstraintKind, CreateIndexRequest, CreateTypeRequest, CrudResult,
    CustomTypeInfo, CustomTypeKind, DatabaseCategory, DatabaseInfo, DbConfig, DbDriver, DbError,
    DbKind, DbSchemaInfo, DdlCapabilities, DdlRejection, DefaultSpec, DeploymentClass,
    DescribeRequest, DocumentConnection, DriverCapabilities, DriverFormDef, DriverLimits,
    DriverMetadata, DropColumnRequest, DropForeignKeyRequest, DropIndexRequest, DropTypeRequest,
    ErrorLocation, ExecutionSourceContext, ExplainRequest, FieldExportTransform, ForeignKeyBuilder,
    ForeignKeyInfo, FormFieldKind, FormSection, FormTab, FormValues, FormattedError, Icon,
    IndexData, IndexInfo, InstanceCatalog, IsolationLevel, KeyValueConnection,
    MutationCapabilities, OrderByColumn, PaginationStyle, PlaceholderStyle, QueryCancelHandle,
    QueryCapabilities, QueryErrorFormatter, QueryGenerator, QueryHandle, QueryLanguage,
    QueryRequest, QueryResult, ReindexRequest, RelationalConnection, RelationalSchema, RoutineInfo,
    RoutineKind, Row, RowDelete, RowInsert, RowPatch, SchemaFeatures, SchemaForeignKeyBuilder,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan,
    SemanticPlanKind, SemanticRequest, SortDirection, SqlDialect, SqlMutationGenerator,
    SqlQueryBuilder, SshTunnelConfig, SyntaxInfo, TableInfo, TransactionCapabilities,
    TransferFamily, TypeDefinition, Value, ViewInfo, WhereOperator, field_password, field_required,
    field_use_uri, generate_create_table, generate_delete_template, generate_drop_table,
    generate_insert_template, generate_select_star, generate_truncate, generate_update_template,
    render_semantic_filter_sql, sanitize_uri, ssh_tab, validate_ddl_fragment, when_checked,
    when_unchecked, with_default, with_help,
};
use dbflux_ssh::SshTunnel;
use half::f16;
use native_tls::TlsConnector;
use postgres::types::{FromSql, Kind, Type};
use postgres::{CancelToken as PgCancelToken, Client, NoTls, SimpleQueryMessage};
use postgres_native_tls::MakeTlsConnector;
use serde_json::Value as JsonValue;
use uuid::Uuid;

/// PostgreSQL driver metadata.
pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "postgres".into(),
    display_name: "PostgreSQL".into(),
    description: "Advanced open-source relational database".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: Some(DeploymentClass::SelfHosted),
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
            | DriverCapabilities::TRANSACTIONAL_DDL.bits()
            | DriverCapabilities::ROUTINES.bits()
            | DriverCapabilities::MULTI_STATEMENT.bits()
            | DriverCapabilities::INSTANCE_METRICS.bits()
            | DriverCapabilities::INSTANCE_INSPECTOR.bits()
            | DriverCapabilities::CHART_AUTHORING.bits()
            | DriverCapabilities::BULK_INSERT.bits()
            | DriverCapabilities::TRUNCATE_TABLE.bits()
            | DriverCapabilities::DISABLE_FK_CHECKS.bits(),
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
        order_by_mode: dbflux_core::OrderByMode::AnyColumns,
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
        max_bulk_insert_rows: 0,
    }),
    ssl_modes: Some(&[
        dbflux_core::SslModeOption {
            id: "disable",
            label: "disable",
        },
        dbflux_core::SslModeOption {
            id: "allow",
            label: "allow",
        },
        dbflux_core::SslModeOption {
            id: "prefer",
            label: "prefer",
        },
        dbflux_core::SslModeOption {
            id: "require",
            label: "require",
        },
        dbflux_core::SslModeOption {
            id: "verify-ca",
            label: "verify-ca",
        },
        dbflux_core::SslModeOption {
            id: "verify-full",
            label: "verify-full",
        },
    ]),
    ssl_cert_fields: Some(dbflux_core::SslCertFields {
        root_cert: true,
        client_cert: true,
    }),
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
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

    fn value_to_literal_typed(&self, value: &Value, col_type: Option<&str>) -> String {
        value_to_pg_literal_typed(value, col_type)
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
        assignments: &[dbflux_core::ColumnAssignment],
        conflict_columns: &[String],
        update_assignments: &[dbflux_core::ColumnAssignment],
    ) -> Option<String> {
        if assignments.is_empty() || conflict_columns.is_empty() {
            return None;
        }

        let table = self.qualified_table(schema, table);
        let columns = assignments
            .iter()
            .map(|a| self.quote_identifier(&a.name))
            .collect::<Vec<_>>()
            .join(", ");
        let values = assignments
            .iter()
            .map(|a| self.value_to_literal_typed(&a.value, a.type_name.as_deref()))
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
            .map(|a| {
                format!(
                    "{} = {}",
                    self.quote_identifier(&a.name),
                    self.value_to_literal_typed(&a.value, a.type_name.as_deref())
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
            | CodeGenCapabilities::ADD_COLUMN
            | CodeGenCapabilities::DROP_COLUMN
            | CodeGenCapabilities::ALTER_COLUMN
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

    fn generate_add_column(&self, req: &AddColumnRequest) -> Result<Vec<String>, DdlRejection> {
        validate_ddl_fragment(req.type_name, "column type")?;
        if let Some(default) = req.default {
            validate_ddl_fragment(default, "column default")?;
        }

        let table = self.qualified(req.schema_name, req.table_name);
        let mut sql = format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            table,
            self.quote(req.column_name),
            req.type_name
        );

        if !req.nullable {
            sql.push_str(" NOT NULL");
        }
        if let Some(default) = req.default {
            sql.push_str(&format!(" DEFAULT {}", default));
        }
        sql.push(';');

        Ok(vec![sql])
    }

    fn generate_drop_column(&self, req: &DropColumnRequest) -> Result<Vec<String>, DdlRejection> {
        let table = self.qualified(req.schema_name, req.table_name);
        Ok(vec![format!(
            "ALTER TABLE {} DROP COLUMN {};",
            table,
            self.quote(req.column_name)
        )])
    }

    fn generate_alter_column(&self, req: &AlterColumnRequest) -> Result<Vec<String>, DdlRejection> {
        if let Some(new_type) = req.new_type {
            validate_ddl_fragment(new_type, "column type")?;
        }
        if let Some(DefaultSpec::Set(value)) = req.default {
            validate_ddl_fragment(value, "column default")?;
        }

        let table = self.qualified(req.schema_name, req.table_name);
        let column = self.quote(req.column_name);
        let mut statements = Vec::new();

        if let Some(new_type) = req.new_type {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                table, column, new_type
            ));
        }

        if let Some(nullable) = req.nullable {
            let clause = if nullable {
                "DROP NOT NULL"
            } else {
                "SET NOT NULL"
            };
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} {};",
                table, column, clause
            ));
        }

        match req.default {
            Some(DefaultSpec::Drop) => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                    table, column
                ));
            }
            Some(DefaultSpec::Set(value)) => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                    table, column, value
                ));
            }
            None => {}
        }

        if statements.is_empty() {
            return Err(DdlRejection {
                reason: "ALTER COLUMN requires at least one of: type, nullable, default"
                    .to_string(),
                followup: None,
            });
        }

        Ok(statements)
    }
}

// =============================================================================

pub static POSTGRES_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "postgresql://user:pass@localhost:5432/db",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("host", "Host", FormFieldKind::Text, "localhost"),
                                "localhost",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "5432"),
                                "5432",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required(
                                    "database",
                                    "Database",
                                    FormFieldKind::Text,
                                    "postgres",
                                ),
                                "postgres",
                            ),
                            "use_uri",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        when_unchecked(
                            with_default(
                                field_required("user", "User", FormFieldKind::Text, "postgres"),
                                "postgres",
                            ),
                            "use_uri",
                        ),
                        with_help(
                            field_password(),
                            "via Auth Profile · resolved at runtime, never persisted on disk",
                        ),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

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
                &config.ssl_mode,
            )
        } else {
            self.connect_direct(
                &config.host,
                config.port,
                &config.user,
                &config.database,
                password,
                &config.ssl_mode,
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

    fn export_field_transform(&self, field_id: &str, values: &FormValues) -> FieldExportTransform {
        if field_id != "uri" {
            return FieldExportTransform::None;
        }

        let use_uri = values.get("use_uri").map(|s| s == "true").unwrap_or(false);
        if !use_uri {
            return FieldExportTransform::None;
        }

        let uri = match values.get("uri") {
            Some(u) if !u.is_empty() => u.as_str(),
            _ => return FieldExportTransform::None,
        };

        split_postgres_uri_secret(uri)
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
                ssl_mode: Some("prefer".to_string()),
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
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
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
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
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
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
                ssl_mode: ssl_mode.clone(),
                ssl_root_cert_path: ssl_root_cert_path.clone(),
                ssl_client_cert_path: ssl_client_cert_path.clone(),
                ssl_client_key_path: ssl_client_key_path.clone(),
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
    /// Postgres native sslmode id (e.g. `"prefer"`, `"verify-ca"`). Defaults to `"prefer"` when absent.
    ssl_mode: String,
    ssh_tunnel: Option<SshTunnelConfig>,
}

/// Map a PostgreSQL type OID to a semantic `ColumnKind`.
///
/// Only the most common OIDs are listed; everything else is `Unknown`.
fn pg_oid_to_kind(oid: u32) -> ColumnKind {
    match oid {
        1114 | 1184 | 1082 => ColumnKind::Timestamp, // TIMESTAMP, TIMESTAMPTZ, DATE
        21 | 23 | 20 => ColumnKind::Integer,         // INT2, INT4, INT8
        700 | 701 | 1700 => ColumnKind::Float,       // FLOAT4, FLOAT8, NUMERIC
        25 | 1043 | 1042 | 19 => ColumnKind::Text,   // TEXT, VARCHAR, BPCHAR, NAME
        _ => ColumnKind::Unknown,
    }
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
            ssl_mode: ssl_mode.clone().unwrap_or_else(|| "prefer".to_string()),
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
    /// Postgres native sslmode id (e.g. `"prefer"`, `"verify-ca"`).
    ssl_mode: &'a str,
}

/// Establishes a PostgreSQL connection using the native sslmode identifier from the profile.
///
/// Maps sslmode string values directly to the appropriate TLS strategy, matching PostgreSQL's
/// libpq semantics:
/// - `"disable"` — no TLS
/// - `"allow"` / `"prefer"` — try TLS first, fall back to plain
/// - `"require"` — TLS required, self-signed certs accepted
/// - `"verify-ca"` / `"verify-full"` — TLS required with certificate validation
fn connect_postgres(params: &PostgresConnectParams) -> Result<Client, DbError> {
    let conn_string = with_client_identity(&format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=30",
        params.host, params.port, params.user, params.password, params.database
    ));

    match params.ssl_mode {
        "disable" => Client::connect(&conn_string, NoTls)
            .map_err(|e| format_pg_error(&e, params.host, params.port)),

        "allow" | "prefer" => {
            // Try SSL with permissive cert checking; fall back to plain if SSL fails.
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into())
                })?;

            let tls = MakeTlsConnector::new(connector);

            match Client::connect(&conn_string, tls) {
                Ok(client) => Ok(client),
                Err(_) => Client::connect(&conn_string, NoTls)
                    .map_err(|e| format_pg_error(&e, params.host, params.port)),
            }
        }

        "require" => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into())
                })?;

            let tls = MakeTlsConnector::new(connector);

            Client::connect(&conn_string, tls)
                .map_err(|e| format_pg_error(&e, params.host, params.port))
        }

        "verify-ca" | "verify-full" => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(false)
                .build()
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into())
                })?;

            let tls = MakeTlsConnector::new(connector);

            Client::connect(&conn_string, tls)
                .map_err(|e| format_pg_error(&e, params.host, params.port))
        }

        // Unknown modes fall back to prefer behaviour (try TLS, allow plain fallback).
        _ => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("TLS setup failed: {}", e).into())
                })?;

            let tls = MakeTlsConnector::new(connector);

            match Client::connect(&conn_string, tls) {
                Ok(client) => Ok(client),
                Err(_) => Client::connect(&conn_string, NoTls)
                    .map_err(|e| format_pg_error(&e, params.host, params.port)),
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
        let uri = with_client_identity(&uri);

        let ssl_mode = parse_pg_uri_sslmode(&uri);

        if ssl_mode == PgUriSslMode::Disable {
            let client =
                Client::connect(&uri, NoTls).map_err(|e| format_pg_uri_error(&e, base_uri))?;

            let cancel_token = client.cancel_token();
            log::info!("[CONNECT] PostgreSQL connection established via URI");

            return Ok(Box::new(PostgresConnection {
                client: Arc::new(Mutex::new(client)),
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
            client: Arc::new(Mutex::new(client)),
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
        ssl_mode: &str,
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
            client: Arc::new(Mutex::new(client)),
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
        ssl_mode: &str,
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
            client: Arc::new(Mutex::new(client)),
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
    client: Arc<Mutex<Client>>,
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

    fn set_referential_integrity(&self, enabled: bool) -> Result<(), DbError> {
        let role = if enabled { "origin" } else { "replica" };
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;
        client
            .simple_query(&format!("SET session_replication_role = '{role}'"))
            .map_err(|e| format_pg_query_error(&e))?;
        Ok(())
    }

    fn instance_catalog(&self) -> Option<Box<dyn InstanceCatalog>> {
        let pg_signal_backend = self
            .client
            .lock()
            .ok()
            .map(|mut c| {
                crate::instance_catalog::PgInstanceCatalog::probe_pg_signal_backend(&mut c)
            })
            .unwrap_or(false);

        Some(Box::new(
            crate::instance_catalog::PgInstanceCatalog::new_probed(
                Arc::clone(&self.client),
                pg_signal_backend,
            ),
        ))
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.cancelled.store(false, Ordering::SeqCst);

        if let Some(source) = req
            .execution_context
            .as_ref()
            .and_then(|ctx| ctx.source.as_ref())
        {
            match source {
                ExecutionSourceContext::InstanceMetricQuery { metric_id, .. } => {
                    let mut client = self.client.lock().map_err(|_| {
                        DbError::QueryFailed("postgres client mutex poisoned".to_string().into())
                    })?;
                    return crate::instance_catalog::dispatch_metric_series(&mut client, metric_id);
                }
                ExecutionSourceContext::InstanceInspectorQuery { metric_id } => {
                    let mut client = self.client.lock().map_err(|_| {
                        DbError::QueryFailed("postgres client mutex poisoned".to_string().into())
                    })?;
                    return crate::instance_catalog::dispatch_inspector_snapshot(
                        &mut client,
                        metric_id,
                    );
                }
                _ => {}
            }
        }

        let start = Instant::now();
        let query_id = Uuid::new_v4();
        let _active_query_guard = ActiveQueryGuard::activate(&self.active_query, query_id)?;

        let sql_preview = dbflux_core::truncate_string_safe(&req.sql, 80);
        log::debug!(
            "[QUERY] Executing (id={}): {}",
            query_id,
            sql_preview.replace('\n', " ")
        );

        let mut client = match self.client.lock() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("[CLEANUP] Recovering from poisoned mutex during cleanup");
                poison_err.into_inner()
            }
        };

        // A multi-statement batch cannot use the extended (prepared) protocol,
        // which rejects more than one command per statement (SQLSTATE 42601).
        // Route it through the simple query protocol, which executes the whole
        // batch and returns one result set per statement.
        if QueryLanguage::Sql.statement_count(&req.sql) > 1 {
            return execute_statement_batch(&mut client, &req.sql, query_id, start, req.limit);
        }

        let (columns, rows) = {
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
                    kind: pg_oid_to_kind(col.type_().oid()),
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

        drop(client);

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

        let mut result = QueryResult::table(columns, result_rows, None, total_time);
        result.set_unsupported_types(unsupported_type_names(&result.rows));

        Ok(result)
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
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
        })
    }

    fn schema_features(&self) -> SchemaFeatures {
        SchemaFeatures::FOREIGN_KEYS
            | SchemaFeatures::CHECK_CONSTRAINTS
            | SchemaFeatures::UNIQUE_CONSTRAINTS
            | SchemaFeatures::CUSTOM_TYPES
            | SchemaFeatures::FUNCTIONS
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

    fn schema_routines(
        &self,
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<RoutineInfo>, DbError> {
        let schema_name = schema.unwrap_or("public");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        get_schema_routines(&mut client, schema_name)
    }

    fn routine_definition(
        &self,
        _database: &str,
        schema: &str,
        specific_name: &str,
    ) -> Result<String, DbError> {
        // Parse out the bare name and the identity arguments from specific_name.
        // specific_name is formatted as "name(identity_args)", e.g. "add(integer, integer)".
        let (bare_name, identity_args) = if let Some(paren_pos) = specific_name.find('(') {
            let name = &specific_name[..paren_pos];
            let args = specific_name
                .get(paren_pos + 1..specific_name.len().saturating_sub(1))
                .unwrap_or("");
            (name, args)
        } else {
            (specific_name, "")
        };

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        // First look up the prokind so we can synthesize a body for aggregates/windows
        // instead of calling pg_get_functiondef (which errors for those kinds).
        let kind_rows = client
            .query(
                r#"
                SELECT p.prokind::char AS prokind
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = $1
                  AND p.proname = $2
                  AND pg_catalog.pg_get_function_identity_arguments(p.oid) = $3
                "#,
                &[&schema, &bare_name, &identity_args],
            )
            .map_err(|e| format_pg_query_error(&e))?;

        let prokind_char = kind_rows
            .first()
            .and_then(|r| {
                let s: &str = r.get("prokind");
                s.chars().next()
            })
            .unwrap_or('f');

        // Aggregate and window functions cannot be described via pg_get_functiondef.
        if prokind_char == 'a' || prokind_char == 'w' {
            let kind_label = if prokind_char == 'a' {
                "aggregate"
            } else {
                "window"
            };
            return Ok(format!(
                "-- {} {}\n-- Source definition not available via pg_get_functiondef for {} functions.\n",
                kind_label, specific_name, kind_label,
            ));
        }

        let def_rows = client
            .query(
                r#"
                SELECT pg_catalog.pg_get_functiondef(p.oid) AS definition
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = $1
                  AND p.proname = $2
                  AND pg_catalog.pg_get_function_identity_arguments(p.oid) = $3
                "#,
                &[&schema, &bare_name, &identity_args],
            )
            .map_err(|e| format_pg_query_error(&e))?;

        if let Some(row) = def_rows.first() {
            let definition: String = row.get("definition");
            Ok(definition)
        } else {
            Ok(format!(
                "-- Routine {} not found in schema {}.\n",
                specific_name, schema
            ))
        }
    }

    fn fetch_dependents(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<dbflux_core::RelationRef>, DbError> {
        let schema_name = schema.unwrap_or("public");

        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e).into()))?;

        fetch_dependents(&mut client, schema_name, table)
    }

    fn fetch_row_by_pk(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
        pk_column: &str,
        pk_value: &dbflux_core::Value,
    ) -> Result<Option<std::collections::HashMap<String, dbflux_core::Value>>, dbflux_core::DbError>
    {
        let pk_literal = POSTGRES_DIALECT.value_to_literal(pk_value);
        let sql = format!(
            "SELECT * FROM {}.{} WHERE {} = {} LIMIT 1",
            POSTGRES_DIALECT.quote_identifier(schema),
            POSTGRES_DIALECT.quote_identifier(table),
            POSTGRES_DIALECT.quote_identifier(pk_column),
            pk_literal,
        );

        let result = self.execute(&dbflux_core::QueryRequest::new(sql))?;
        let columns = result.columns;
        let Some(row) = result.rows.into_iter().next() else {
            return Ok(None);
        };

        let map = columns
            .into_iter()
            .zip(row)
            .map(|(col, val)| (col.name, val))
            .collect();

        Ok(Some(map))
    }

    fn referenced_tables(&self, query: &str) -> Option<Vec<dbflux_core::QueryTableRef>> {
        Some(dbflux_core::extract_referenced_tables(query))
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

        // Invariant: rows is non-empty — checked above.
        #[allow(clippy::indexing_slicing)]
        let row = &rows[0];
        let returning_row: Row = (0..row.columns().len())
            .map(|i| postgres_value_to_value(row, i))
            .collect();

        Ok(crud_result_with_unsupported_types(returning_row))
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

        // Invariant: rows is non-empty — checked above.
        #[allow(clippy::indexing_slicing)]
        let row = &rows[0];
        let returning_row: Row = (0..row.columns().len())
            .map(|i| postgres_value_to_value(row, i))
            .collect();

        Ok(crud_result_with_unsupported_types(returning_row))
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

        // Invariant: rows is non-empty — checked above.
        #[allow(clippy::indexing_slicing)]
        let row = &rows[0];
        let returning_row: Row = (0..row.columns().len())
            .map(|i| postgres_value_to_value(row, i))
            .collect();

        Ok(crud_result_with_unsupported_types(returning_row))
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
                presentation: dbflux_core::CollectionPresentation::DataGrid,
                child_items: None,
                storage_hints: None,
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

/// Convert a Value to a safe PostgreSQL literal string, guided by the target
/// column's driver-reported type.
///
/// When the column is an array type, this emits a typed `ARRAY[...]::T[]`
/// literal so that round-trip insert/update against `text[]`, `int4[]`, etc.
/// works regardless of whether the cell value arrived as `Value::Array`
/// (untouched from the driver) or `Value::Json(s)` (user edited the cell as
/// a JSON array text). For non-array columns it falls back to the untyped
/// formatter.
fn value_to_pg_literal_typed(value: &Value, col_type: Option<&str>) -> String {
    if let Some(ty) = col_type
        && let Some(elem_type) = pg_array_element_type(ty)
    {
        return format_pg_array_literal(value, elem_type);
    }

    value_to_pg_literal(value)
}

/// Returns the canonical PostgreSQL element type for an array column type,
/// or None if the type is not an array.
///
/// Accepts both the internal name (`_text`, `_int4`) returned by the
/// `postgres` crate via `Type::name()` and the SQL-level name (`text[]`,
/// `int4[]`) that may come from other paths like `information_schema`.
fn pg_array_element_type(type_name: &str) -> Option<&'static str> {
    let normalized = type_name.trim().to_ascii_lowercase();

    let elem = if let Some(stripped) = normalized.strip_prefix('_') {
        stripped
    } else if let Some(stripped) = normalized.strip_suffix("[]") {
        stripped.trim()
    } else {
        return None;
    };

    // Map driver-reported element names to a canonical PostgreSQL type name
    // suitable for use in an `::T[]` cast. Unknown element types fall back
    // to `text` — safer than failing the literal emission, and the server
    // will reject the cast if it really doesn't fit.
    Some(match elem {
        "bool" | "boolean" => "boolean",
        "int2" | "smallint" => "int2",
        "int4" | "integer" | "int" => "int4",
        "int8" | "bigint" => "int8",
        "float4" | "real" => "float4",
        "float8" | "double precision" => "float8",
        "numeric" | "decimal" => "numeric",
        "text" => "text",
        "varchar" | "character varying" => "varchar",
        "bpchar" | "character" => "bpchar",
        "uuid" => "uuid",
        "json" => "json",
        "jsonb" => "jsonb",
        "date" => "date",
        "time" => "time",
        "timestamp" => "timestamp",
        "timestamptz" => "timestamptz",
        "inet" => "inet",
        "citext" => "citext",
        "name" => "name",
        _ => "text",
    })
}

/// Build a typed `ARRAY[...]::elem[]` literal for the given value and
/// element type.
///
/// Accepts:
/// - `Value::Null` — emitted as `NULL::elem[]`.
/// - `Value::Array(items)` — items formatted individually as untyped PG
///   literals.
/// - `Value::Json(s)` — `s` is parsed as a JSON array (so a user-edited cell
///   containing `["a","b"]` round-trips). Falls back to a Json fallback if
///   parsing fails.
/// - Any other variant — passed through `value_to_pg_literal` and wrapped as
///   `ARRAY[<lit>]::elem[]`.
fn format_pg_array_literal(value: &Value, elem_type: &str) -> String {
    match value {
        Value::Null => format!("NULL::{}[]", elem_type),

        Value::Array(items) => {
            let lits: Vec<String> = items.iter().map(value_to_pg_literal).collect();
            format!("ARRAY[{}]::{}[]", lits.join(", "), elem_type)
        }

        Value::Json(s) => match serde_json::from_str::<JsonValue>(s) {
            Ok(JsonValue::Array(items)) => {
                let lits: Vec<String> = items.iter().map(json_value_to_pg_literal).collect();
                format!("ARRAY[{}]::{}[]", lits.join(", "), elem_type)
            }
            // Non-array JSON for an array column is a real type mismatch —
            // surface it by letting the server reject the cast rather than
            // silently mangling the data.
            _ => format!("{}::{}[]", pg_quote_string(s), elem_type),
        },

        other => format!("ARRAY[{}]::{}[]", value_to_pg_literal(other), elem_type),
    }
}

/// Convert a JSON scalar to a PostgreSQL literal suitable as an array element.
fn json_value_to_pg_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => pg_quote_string(s),
        // Nested objects/arrays in a flat array column don't have a clean
        // mapping; pass the raw JSON text through as a quoted string and let
        // the server error if the destination element type can't take it.
        JsonValue::Array(_) | JsonValue::Object(_) => pg_quote_string(&value.to_string()),
    }
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
        "text" | "varchar" | "bpchar" | "name" | "citext" => true,
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

struct PgVectorText(String);

fn pgvector_decode_error(message: &'static str) -> Box<dyn std::error::Error + Sync + Send> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

fn read_pgvector_u16(
    raw: &[u8],
    offset: usize,
) -> Result<u16, Box<dyn std::error::Error + Sync + Send>> {
    let bytes = raw
        .get(offset..offset + 2)
        .ok_or_else(|| pgvector_decode_error("pgvector payload ended unexpectedly"))?
        .try_into()
        .map_err(|_| pgvector_decode_error("invalid pgvector u16 payload"))?;

    Ok(u16::from_be_bytes(bytes))
}

fn read_pgvector_i32(
    raw: &[u8],
    offset: usize,
) -> Result<i32, Box<dyn std::error::Error + Sync + Send>> {
    let bytes = raw
        .get(offset..offset + 4)
        .ok_or_else(|| pgvector_decode_error("pgvector payload ended unexpectedly"))?
        .try_into()
        .map_err(|_| pgvector_decode_error("invalid pgvector i32 payload"))?;

    Ok(i32::from_be_bytes(bytes))
}

fn read_pgvector_f32(
    raw: &[u8],
    offset: usize,
) -> Result<f32, Box<dyn std::error::Error + Sync + Send>> {
    let bytes = raw
        .get(offset..offset + 4)
        .ok_or_else(|| pgvector_decode_error("pgvector payload ended unexpectedly"))?
        .try_into()
        .map_err(|_| pgvector_decode_error("invalid pgvector float payload"))?;
    let value = f32::from_be_bytes(bytes);

    if !value.is_finite() {
        return Err(pgvector_decode_error("pgvector values must be finite"));
    }

    Ok(value)
}

fn decode_pgvector_dense(
    raw: &[u8],
    element_size: usize,
    decode_element: impl Fn(&[u8], usize) -> Result<f32, Box<dyn std::error::Error + Sync + Send>>,
) -> Result<PgVectorText, Box<dyn std::error::Error + Sync + Send>> {
    let dimension = usize::from(read_pgvector_u16(raw, 0)?);
    let reserved = read_pgvector_u16(raw, 2)?;

    if dimension == 0 || reserved != 0 {
        return Err(pgvector_decode_error("invalid pgvector dense header"));
    }

    let payload_size = dimension
        .checked_mul(element_size)
        .ok_or_else(|| pgvector_decode_error("pgvector dimension is too large"))?;
    let expected_length = 4usize
        .checked_add(payload_size)
        .ok_or_else(|| pgvector_decode_error("pgvector payload is too large"))?;

    if raw.len() != expected_length {
        return Err(pgvector_decode_error(
            "invalid pgvector dense payload length",
        ));
    }

    let values = (0..dimension)
        .map(|index| decode_element(raw, 4 + index * element_size))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PgVectorText(format_pgvector_dense(&values)))
}

fn decode_pgvector_vector(
    raw: &[u8],
) -> Result<PgVectorText, Box<dyn std::error::Error + Sync + Send>> {
    decode_pgvector_dense(raw, 4, read_pgvector_f32)
}

fn decode_pgvector_halfvec(
    raw: &[u8],
) -> Result<PgVectorText, Box<dyn std::error::Error + Sync + Send>> {
    decode_pgvector_dense(raw, 2, |raw, offset| {
        let bits = read_pgvector_u16(raw, offset)?;
        let value = f16::from_bits(bits).to_f32();

        if !value.is_finite() {
            return Err(pgvector_decode_error("pgvector values must be finite"));
        }

        Ok(value)
    })
}

fn decode_pgvector_sparsevec(
    raw: &[u8],
) -> Result<PgVectorText, Box<dyn std::error::Error + Sync + Send>> {
    let dimension = read_pgvector_i32(raw, 0)?;
    let non_zero_count = read_pgvector_i32(raw, 4)?;
    let reserved = read_pgvector_i32(raw, 8)?;

    if dimension <= 0 || non_zero_count < 0 || non_zero_count > dimension || reserved != 0 {
        return Err(pgvector_decode_error("invalid pgvector sparse header"));
    }

    let non_zero_count = usize::try_from(non_zero_count)
        .map_err(|_| pgvector_decode_error("invalid pgvector sparse count"))?;
    let values_size = non_zero_count
        .checked_mul(8)
        .ok_or_else(|| pgvector_decode_error("pgvector payload is too large"))?;
    let expected_length = 12usize
        .checked_add(values_size)
        .ok_or_else(|| pgvector_decode_error("pgvector payload is too large"))?;

    if raw.len() != expected_length {
        return Err(pgvector_decode_error(
            "invalid pgvector sparse payload length",
        ));
    }

    let dimension = usize::try_from(dimension)
        .map_err(|_| pgvector_decode_error("invalid pgvector sparse dimension"))?;
    let values_offset = 12 + non_zero_count * 4;
    let mut previous_index = None;
    let mut entries = Vec::with_capacity(non_zero_count);

    for position in 0..non_zero_count {
        let index = read_pgvector_i32(raw, 12 + position * 4)?;
        let index = usize::try_from(index)
            .map_err(|_| pgvector_decode_error("invalid pgvector sparse index"))?;

        if index >= dimension {
            return Err(pgvector_decode_error("invalid pgvector sparse index"));
        }

        if previous_index.is_some_and(|previous_index| index <= previous_index) {
            return Err(pgvector_decode_error(
                "invalid pgvector sparse index ordering",
            ));
        }

        let value = read_pgvector_f32(raw, values_offset + position * 4)?;
        if value == 0.0 {
            return Err(pgvector_decode_error(
                "pgvector sparse values must be non-zero",
            ));
        }

        entries.push((index + 1, value));
        previous_index = Some(index);
    }

    Ok(PgVectorText(format_pgvector_sparse(&entries, dimension)))
}

fn decode_pgvector(
    type_name: &str,
    raw: &[u8],
) -> Result<PgVectorText, Box<dyn std::error::Error + Sync + Send>> {
    match type_name {
        "vector" => decode_pgvector_vector(raw),
        "halfvec" => decode_pgvector_halfvec(raw),
        "sparsevec" => decode_pgvector_sparsevec(raw),
        _ => Err(pgvector_decode_error("unsupported pgvector type")),
    }
}

fn format_pgvector_float4(value: f32) -> String {
    let mut buffer = ryu::Buffer::new();
    let shortest = buffer.format_finite(value);

    let (sign, number) = shortest
        .strip_prefix('-')
        .map_or_else(|| ("", shortest), |number| ("-", number));
    let (coefficient, exponent) =
        number
            .split_once('e')
            .map_or((number, 0), |(coefficient, exponent)| {
                (
                    coefficient,
                    parse_pgvector_exponent(exponent).unwrap_or_default(),
                )
            });
    let integer_digits = coefficient.find('.').unwrap_or(coefficient.len());
    let digits: String = coefficient.chars().filter(char::is_ascii_digit).collect();
    let Some(first_digit) = digits.find(|digit| digit != '0') else {
        return format!("{sign}0");
    };

    let exponent = exponent + integer_digits as i32 - first_digit as i32 - 1;
    let digits = digits[first_digit..].trim_end_matches('0');

    if (-4..6).contains(&exponent) {
        format_pgvector_fixed(sign, digits, exponent)
    } else {
        format_pgvector_scientific(sign, digits, exponent)
    }
}

fn parse_pgvector_exponent(exponent: &str) -> Option<i32> {
    let (sign, digits) = exponent.strip_prefix('-').map_or_else(
        || (1, exponent.strip_prefix('+').unwrap_or(exponent)),
        |digits| (-1, digits),
    );

    if digits.is_empty() {
        return None;
    }

    let value = digits.chars().try_fold(0_i32, |value, digit| {
        let digit = i32::try_from(digit.to_digit(10)?).ok()?;
        value.checked_mul(10)?.checked_add(digit)
    })?;

    value.checked_mul(sign)
}

fn format_pgvector_fixed(sign: &str, digits: &str, exponent: i32) -> String {
    let integer_digits = usize::try_from(exponent + 1).unwrap_or_default();

    if integer_digits >= digits.len() {
        format!(
            "{sign}{digits}{}",
            "0".repeat(integer_digits - digits.len())
        )
    } else if integer_digits == 0 {
        format!("{sign}0.{}{digits}", "0".repeat((-exponent - 1) as usize))
    } else {
        format!(
            "{sign}{}.{}",
            &digits[..integer_digits],
            &digits[integer_digits..]
        )
    }
}

fn format_pgvector_scientific(sign: &str, digits: &str, exponent: i32) -> String {
    let digits = digits.trim_end_matches('0');
    let mantissa = if digits.len() == 1 {
        digits.to_string()
    } else {
        format!("{}.{}", &digits[..1], &digits[1..])
    };
    let exponent_sign = if exponent < 0 { '-' } else { '+' };
    let exponent = exponent.unsigned_abs();

    format!("{sign}{mantissa}e{exponent_sign}{exponent:02}")
}

fn format_pgvector_dense(values: &[f32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format_pgvector_float4(*value))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn format_pgvector_sparse(entries: &[(usize, f32)], dimension: usize) -> String {
    let entries = entries
        .iter()
        .map(|(index, value)| format!("{index}:{}", format_pgvector_float4(*value)))
        .collect::<Vec<_>>()
        .join(",");

    format!("{{{entries}}}/{dimension}")
}

impl<'a> FromSql<'a> for PgVectorText {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        decode_pgvector(ty.name(), raw)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "vector" | "halfvec" | "sparsevec")
    }
}

/// Wrapper that renders full-text search values in their canonical text form.
///
/// `tsvector` and `tsquery` travel in a binary wire format that is not valid
/// UTF-8, so reading the raw bytes as text fails and the value silently
/// degrades to NULL. This decoder reproduces the server-side output of
/// `tsvectorout` / `tsqueryout`.
struct PgTextSearchText(String);

fn text_search_decode_error(message: &'static str) -> Box<dyn std::error::Error + Sync + Send> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

fn read_text_search_u16(
    raw: &[u8],
    offset: usize,
) -> Result<u16, Box<dyn std::error::Error + Sync + Send>> {
    let bytes: [u8; 2] = raw
        .get(offset..offset + 2)
        .ok_or_else(|| text_search_decode_error("text search payload ended unexpectedly"))?
        .try_into()
        .map_err(|_| text_search_decode_error("invalid text search u16 payload"))?;

    Ok(u16::from_be_bytes(bytes))
}

fn read_text_search_i32(
    raw: &[u8],
    offset: usize,
) -> Result<i32, Box<dyn std::error::Error + Sync + Send>> {
    let bytes: [u8; 4] = raw
        .get(offset..offset + 4)
        .ok_or_else(|| text_search_decode_error("text search payload ended unexpectedly"))?
        .try_into()
        .map_err(|_| text_search_decode_error("invalid text search i32 payload"))?;

    Ok(i32::from_be_bytes(bytes))
}

fn read_text_search_byte(
    raw: &[u8],
    offset: usize,
) -> Result<u8, Box<dyn std::error::Error + Sync + Send>> {
    raw.get(offset)
        .copied()
        .ok_or_else(|| text_search_decode_error("text search payload ended unexpectedly"))
}

/// Read a NUL-terminated UTF-8 string, advancing `offset` past the terminator.
fn read_text_search_cstring<'a>(
    raw: &'a [u8],
    offset: &mut usize,
) -> Result<&'a str, Box<dyn std::error::Error + Sync + Send>> {
    let remaining = raw
        .get(*offset..)
        .ok_or_else(|| text_search_decode_error("text search payload ended unexpectedly"))?;
    let terminator = remaining
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| text_search_decode_error("unterminated text search lexeme"))?;

    let lexeme = remaining
        .get(..terminator)
        .ok_or_else(|| text_search_decode_error("text search payload ended unexpectedly"))?;

    let text = std::str::from_utf8(lexeme)?;
    *offset += terminator + 1;

    Ok(text)
}

/// Append a lexeme quoted the way PostgreSQL renders it: wrapped in single
/// quotes, with embedded quotes and backslashes doubled.
fn push_text_search_lexeme(output: &mut String, lexeme: &str) {
    output.push('\'');

    for character in lexeme.chars() {
        if character == '\'' || character == '\\' {
            output.push(character);
        }

        output.push(character);
    }

    output.push('\'');
}

fn decode_tsvector(
    raw: &[u8],
) -> Result<PgTextSearchText, Box<dyn std::error::Error + Sync + Send>> {
    let lexeme_count = read_text_search_i32(raw, 0)?;
    if lexeme_count < 0 {
        return Err(text_search_decode_error("invalid tsvector lexeme count"));
    }

    let mut offset = 4;
    let mut output = String::new();

    for index in 0..lexeme_count {
        let lexeme = read_text_search_cstring(raw, &mut offset)?;

        if index > 0 {
            output.push(' ');
        }
        push_text_search_lexeme(&mut output, lexeme);

        let position_count = read_text_search_u16(raw, offset)?;
        offset += 2;

        if position_count == 0 {
            continue;
        }

        output.push(':');

        for position_index in 0..position_count {
            let entry = read_text_search_u16(raw, offset)?;
            offset += 2;

            if position_index > 0 {
                output.push(',');
            }

            output.push_str(&(entry & 0x3fff).to_string());

            match entry >> 14 {
                3 => output.push('A'),
                2 => output.push('B'),
                1 => output.push('C'),
                _ => {}
            }
        }
    }

    if offset != raw.len() {
        return Err(text_search_decode_error("trailing tsvector payload"));
    }

    Ok(PgTextSearchText(output))
}

const TSQUERY_OP_NOT: u8 = 1;
const TSQUERY_OP_AND: u8 = 2;
const TSQUERY_OP_OR: u8 = 3;
const TSQUERY_OP_PHRASE: u8 = 4;

enum TsQueryNode {
    Operand {
        lexeme: String,
        weight: u8,
        prefix: bool,
    },
    Not(Box<TsQueryNode>),
    Binary {
        operator: u8,
        distance: u16,
        left: Box<TsQueryNode>,
        right: Box<TsQueryNode>,
    },
}

fn tsquery_operator_priority(operator: u8) -> u8 {
    match operator {
        TSQUERY_OP_NOT => 4,
        TSQUERY_OP_PHRASE => 3,
        TSQUERY_OP_AND => 2,
        _ => 1,
    }
}

/// Parse one node of the prefix-ordered item stream.
///
/// PostgreSQL serializes a tsquery in polish notation and stores the *right*
/// operand of a binary operator before the left one, so the recursion order
/// here is not the display order.
fn parse_tsquery_node(
    raw: &[u8],
    offset: &mut usize,
    remaining_items: &mut i32,
) -> Result<TsQueryNode, Box<dyn std::error::Error + Sync + Send>> {
    if *remaining_items <= 0 {
        return Err(text_search_decode_error(
            "malformed tsquery: operand not found",
        ));
    }
    *remaining_items -= 1;

    let item_type = read_text_search_byte(raw, *offset)?;
    *offset += 1;

    match item_type {
        1 => {
            let weight = read_text_search_byte(raw, *offset)?;
            let prefix = read_text_search_byte(raw, *offset + 1)?;
            *offset += 2;

            if weight > 0xF {
                return Err(text_search_decode_error("invalid tsquery weight bitmap"));
            }

            let lexeme = read_text_search_cstring(raw, offset)?.to_string();

            Ok(TsQueryNode::Operand {
                lexeme,
                weight,
                prefix: prefix != 0,
            })
        }

        2 => {
            let operator = read_text_search_byte(raw, *offset)?;
            *offset += 1;

            if operator == TSQUERY_OP_NOT {
                let operand = parse_tsquery_node(raw, offset, remaining_items)?;
                return Ok(TsQueryNode::Not(Box::new(operand)));
            }

            if !matches!(operator, TSQUERY_OP_AND | TSQUERY_OP_OR | TSQUERY_OP_PHRASE) {
                return Err(text_search_decode_error("unrecognized tsquery operator"));
            }

            let distance = if operator == TSQUERY_OP_PHRASE {
                let distance = read_text_search_u16(raw, *offset)?;
                *offset += 2;
                distance
            } else {
                0
            };

            let right = parse_tsquery_node(raw, offset, remaining_items)?;
            let left = parse_tsquery_node(raw, offset, remaining_items)?;

            Ok(TsQueryNode::Binary {
                operator,
                distance,
                left: Box::new(left),
                right: Box::new(right),
            })
        }

        _ => Err(text_search_decode_error("unrecognized tsquery node type")),
    }
}

/// Render a parsed node as infix text, mirroring PostgreSQL's `infix()`:
/// parentheses appear only when the child binds looser than its parent, or
/// when a phrase operator sits on the right-hand side of another phrase.
fn render_tsquery_node(
    node: &TsQueryNode,
    parent_priority: u8,
    right_phrase_operand: bool,
    output: &mut String,
) {
    match node {
        TsQueryNode::Operand {
            lexeme,
            weight,
            prefix,
        } => {
            push_text_search_lexeme(output, lexeme);

            if *weight != 0 || *prefix {
                output.push(':');

                if *prefix {
                    output.push('*');
                }
                for (mask, label) in [(1 << 3, 'A'), (1 << 2, 'B'), (1 << 1, 'C'), (1, 'D')] {
                    if weight & mask != 0 {
                        output.push(label);
                    }
                }
            }
        }

        TsQueryNode::Not(operand) => {
            let priority = tsquery_operator_priority(TSQUERY_OP_NOT);
            let needs_parenthesis = priority < parent_priority;

            if needs_parenthesis {
                output.push_str("( ");
            }

            output.push('!');
            render_tsquery_node(operand, priority, false, output);

            if needs_parenthesis {
                output.push_str(" )");
            }
        }

        TsQueryNode::Binary {
            operator,
            distance,
            left,
            right,
        } => {
            let priority = tsquery_operator_priority(*operator);
            let needs_parenthesis = priority < parent_priority
                || (*operator == TSQUERY_OP_PHRASE && right_phrase_operand);

            if needs_parenthesis {
                output.push_str("( ");
            }

            render_tsquery_node(left, priority, false, output);

            match *operator {
                TSQUERY_OP_OR => output.push_str(" | "),
                TSQUERY_OP_AND => output.push_str(" & "),
                _ if *distance == 1 => output.push_str(" <-> "),
                _ => output.push_str(&format!(" <{distance}> ")),
            }

            render_tsquery_node(right, priority, *operator == TSQUERY_OP_PHRASE, output);

            if needs_parenthesis {
                output.push_str(" )");
            }
        }
    }
}

fn decode_tsquery(
    raw: &[u8],
) -> Result<PgTextSearchText, Box<dyn std::error::Error + Sync + Send>> {
    let item_count = read_text_search_i32(raw, 0)?;
    if item_count < 0 {
        return Err(text_search_decode_error("invalid tsquery item count"));
    }

    if item_count == 0 {
        return Ok(PgTextSearchText(String::new()));
    }

    let mut offset = 4;
    let mut remaining_items = item_count;
    let root = parse_tsquery_node(raw, &mut offset, &mut remaining_items)?;

    if remaining_items != 0 {
        return Err(text_search_decode_error("malformed tsquery: extra nodes"));
    }
    if offset != raw.len() {
        return Err(text_search_decode_error("trailing tsquery payload"));
    }

    let mut output = String::new();
    render_tsquery_node(&root, 0, false, &mut output);

    Ok(PgTextSearchText(output))
}

impl<'a> FromSql<'a> for PgTextSearchText {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty.name() {
            "tsvector" => decode_tsvector(raw),
            "tsquery" => decode_tsquery(raw),
            _ => Err(text_search_decode_error("unsupported text search type")),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "tsvector" | "tsquery")
    }
}

fn text_search_array_values_to_value(values: Option<Vec<Option<PgTextSearchText>>>) -> Value {
    match values {
        Some(values) => Value::Array(
            values
                .into_iter()
                .map(|value| {
                    value
                        .map(|PgTextSearchText(text)| Value::Text(text))
                        .unwrap_or(Value::Null)
                })
                .collect(),
        ),
        None => Value::Null,
    }
}

fn postgres_text_search_array_to_value(
    row: &postgres::Row,
    idx: usize,
    type_name: &str,
) -> Option<Value> {
    if !matches!(type_name, "_tsvector" | "_tsquery") {
        return None;
    }

    Some(
        row.try_get::<_, Option<Vec<Option<PgTextSearchText>>>>(idx)
            .map(text_search_array_values_to_value)
            .unwrap_or_else(|_| Value::Unsupported(type_name.to_string())),
    )
}

fn pgvector_array_values_to_value(values: Option<Vec<Option<PgVectorText>>>) -> Value {
    match values {
        Some(values) => Value::Array(
            values
                .into_iter()
                .map(|value| {
                    value
                        .map(|PgVectorText(text)| Value::Text(text))
                        .unwrap_or(Value::Null)
                })
                .collect(),
        ),
        None => Value::Null,
    }
}

fn pgvector_array_decode_to_value<E>(
    type_name: &str,
    decoded: Result<Option<Vec<Option<PgVectorText>>>, E>,
) -> Value {
    decoded
        .map(pgvector_array_values_to_value)
        .unwrap_or_else(|_| Value::Unsupported(type_name.to_string()))
}

fn postgres_pgvector_array_to_value(
    row: &postgres::Row,
    idx: usize,
    type_name: &str,
) -> Option<Value> {
    if !matches!(type_name, "_vector" | "_halfvec" | "_sparsevec") {
        return None;
    }

    Some(pgvector_array_decode_to_value(
        type_name,
        row.try_get::<_, Option<Vec<Option<PgVectorText>>>>(idx),
    ))
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

// Invariant: `idx` is always in `0..row.columns().len()` — callers iterate over that range.
#[allow(clippy::indexing_slicing)]
fn postgres_value_to_value(row: &postgres::Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_();
    let type_name = col_type.name();

    if let Some(array_value) = postgres_array_to_value(row, idx, type_name) {
        return array_value;
    }

    if let Some(array_value) = postgres_pgvector_array_to_value(row, idx, type_name) {
        return array_value;
    }

    if let Some(array_value) = postgres_text_search_array_to_value(row, idx, type_name) {
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
            .try_get::<_, Option<PgTextSearchText>>(idx)
            .map(|value| {
                value
                    .map(|PgTextSearchText(text)| Value::Text(text))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or_else(|_| Value::Unsupported(type_name.to_string())),

        "vector" | "halfvec" | "sparsevec" => row
            .try_get::<_, Option<PgVectorText>>(idx)
            .map(|value| {
                value
                    .map(|PgVectorText(text)| Value::Text(text))
                    .unwrap_or(Value::Null)
            })
            .unwrap_or_else(|_| Value::Unsupported(type_name.to_string())),

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
                Err(_) => Value::Unsupported(type_name.to_string()),
            },

            Kind::Domain(inner) if is_textual_pg_type(inner) => {
                match row.try_get::<_, Option<PgText>>(idx) {
                    Ok(Some(PgText(s))) => Value::Text(s),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Unsupported(type_name.to_string()),
                }
            }

            Kind::Array(inner) if is_textual_pg_type(inner) => {
                match row.try_get::<_, Option<Vec<PgText>>>(idx) {
                    Ok(Some(arr)) => {
                        Value::Array(arr.into_iter().map(|PgText(s)| Value::Text(s)).collect())
                    }
                    Ok(None) => Value::Null,
                    Err(_) => Value::Unsupported(type_name.to_string()),
                }
            }

            _ => Value::Unsupported(type_name.to_string()),
        },
    }
}

fn unsupported_type_names(rows: &[Row]) -> BTreeSet<String> {
    rows.iter()
        .flatten()
        .filter_map(|value| match value {
            Value::Unsupported(type_name) => Some(type_name.clone()),
            _ => None,
        })
        .collect()
}

fn crud_result_with_unsupported_types(returning_row: Row) -> CrudResult {
    let mut result = CrudResult::success(returning_row);
    let type_names = unsupported_type_names(result.returning_row.as_slice());
    result.set_unsupported_types(type_names);

    result
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

/// Executes a multi-statement batch via the simple query protocol.
///
/// The extended (prepared) protocol used by [`PostgresConnection::execute`]
/// rejects batches with more than one command (SQLSTATE 42601), so a script
/// must go through `simple_query`. The trade-off is that the simple protocol
/// returns every value as text and carries no type metadata, so result columns
/// are reported with [`ColumnKind::Unknown`]. Each statement in the batch
/// becomes a separate result set; the first is the primary result and the rest
/// are attached as `additional_results`.
fn execute_statement_batch(
    client: &mut Client,
    sql: &str,
    query_id: Uuid,
    start: Instant,
    limit: Option<u32>,
) -> Result<QueryResult, DbError> {
    let messages = client.simple_query(sql).map_err(|e| {
        if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
            log::info!("[QUERY] Batch query {} was cancelled", query_id);
            DbError::Cancelled
        } else {
            format_pg_query_error(&e)
        }
    })?;

    let total_time = start.elapsed();
    let mut result_sets = simple_query_messages_to_results(messages, total_time, limit);

    log::debug!(
        "[QUERY] Batch completed in {:.2}ms, {} result set(s)",
        total_time.as_secs_f64() * 1000.0,
        result_sets.len()
    );

    if result_sets.is_empty() {
        return Ok(QueryResult::table(Vec::new(), Vec::new(), None, total_time));
    }

    let mut primary = result_sets.remove(0);
    for extra in result_sets {
        primary.push_additional_result(extra);
    }

    Ok(primary)
}

/// Groups the flat stream of [`SimpleQueryMessage`]s into one [`QueryResult`]
/// per statement. A `CommandComplete` closes the current statement: if it
/// produced rows the result is a table, otherwise it reports the affected-row
/// count. Row values arrive as text and columns are typed `Unknown`.
fn simple_query_messages_to_results(
    messages: Vec<SimpleQueryMessage>,
    total_time: std::time::Duration,
    limit: Option<u32>,
) -> Vec<QueryResult> {
    let row_limit = limit.unwrap_or(u32::MAX) as usize;

    let mut results = Vec::new();
    let mut columns: Option<Vec<ColumnMeta>> = None;
    let mut rows: Vec<Row> = Vec::new();

    for message in messages {
        match message {
            SimpleQueryMessage::Row(row) => {
                if columns.is_none() {
                    columns = Some(
                        row.columns()
                            .iter()
                            .map(|col| ColumnMeta {
                                name: col.name().to_string(),
                                type_name: String::new(),
                                kind: ColumnKind::Unknown,
                                nullable: true,
                                is_primary_key: false,
                            })
                            .collect(),
                    );
                }

                if rows.len() < row_limit {
                    let values = (0..row.columns().len())
                        .map(|i| match row.get(i) {
                            Some(text) => Value::Text(text.to_string()),
                            None => Value::Null,
                        })
                        .collect();
                    rows.push(values);
                }
            }
            SimpleQueryMessage::CommandComplete(affected) => {
                let statement_columns = columns.take().unwrap_or_default();
                let statement_rows = std::mem::take(&mut rows);
                let returned_rows = !statement_columns.is_empty();

                let affected_rows = if returned_rows { None } else { Some(affected) };

                results.push(QueryResult::table(
                    statement_columns,
                    statement_rows,
                    affected_rows,
                    total_time,
                ));
            }
            _ => {}
        }
    }

    // Guard against a trailing row group that never received a CommandComplete.
    if columns.is_some() || !rows.is_empty() {
        let statement_columns = columns.take().unwrap_or_default();
        results.push(QueryResult::table(
            statement_columns,
            std::mem::take(&mut rows),
            None,
            total_time,
        ));
    }

    results
}

fn format_pg_uri_error(e: &postgres::Error, uri: &str) -> DbError {
    let sanitized = sanitize_uri(uri);
    let formatted = POSTGRES_ERROR_FORMATTER.format_uri_error(e, &sanitized);
    log::error!("PostgreSQL URI connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

/// Extract the password from a postgres/postgresql URI into a `SplitSecret` transform.
///
/// When the URI carries embedded credentials (`scheme://user:pass@host/db`), this
/// returns the URI with an empty password placeholder as the skeleton and the
/// extracted (URL-decoded) password as the secret.
///
/// Returns `None` (i.e. `FieldExportTransform::None`) when:
/// - the URI has no `@` (no credentials), or
/// - the user portion has no colon-separated password.
fn split_postgres_uri_secret(uri: &str) -> FieldExportTransform {
    let prefix_end = if uri.starts_with("postgresql://") {
        13
    } else if uri.starts_with("postgres://") {
        11
    } else {
        return FieldExportTransform::None;
    };

    let prefix = &uri[..prefix_end];
    let rest = &uri[prefix_end..];

    let at_pos = match rest.find('@') {
        Some(p) => p,
        None => return FieldExportTransform::None,
    };

    let user_pass = &rest[..at_pos];
    let after_at = &rest[at_pos..];

    let colon_pos = match user_pass.find(':') {
        Some(p) => p,
        None => return FieldExportTransform::None,
    };

    let user = &user_pass[..colon_pos];
    let encoded_pass = &user_pass[colon_pos + 1..];

    if encoded_pass.is_empty() {
        return FieldExportTransform::None;
    }

    let password = urlencoding::decode(encoded_pass)
        .unwrap_or_else(|_| encoded_pass.into())
        .into_owned();

    let skeleton = format!("{}{}:{}", prefix, user, after_at);

    FieldExportTransform::SplitSecret {
        skeleton,
        secret: dbflux_core::secrecy::SecretString::from(password),
    }
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

/// Appends dbflux's client identity as `application_name` unless the connection string
/// already declares one, so a user-supplied `application_name` always wins.
fn with_client_identity(conn_str: &str) -> String {
    if has_application_name(conn_str) {
        return conn_str.to_string();
    }

    let identity = dbflux_core::client_identity();
    let is_uri = conn_str.starts_with("postgres://") || conn_str.starts_with("postgresql://");

    if is_uri {
        let separator = if conn_str.contains('?') { '&' } else { '?' };
        format!("{}{}application_name={}", conn_str, separator, identity)
    } else {
        format!("{} application_name={}", conn_str, identity)
    }
}

/// Detects an `application_name` key in both the libpq keyword syntax (space-separated
/// `key=value` pairs) and the URI query syntax (`?application_name=` / `&application_name=`).
fn has_application_name(conn_str: &str) -> bool {
    conn_str.match_indices("application_name=").any(|(idx, _)| {
        idx == 0 || matches!(conn_str.as_bytes().get(idx - 1), Some(b' ' | b'?' | b'&'))
    })
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
        // Invariant: index < chars.len() — loop guard ensures this.
        #[allow(clippy::indexing_slicing)]
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

// =============================================================================
// Dependents introspection (stub — not yet wired into ConnectedProfile cache)
// =============================================================================
//
// Wiring note: `table_details()` on the `RelationalConnection` trait returns
// `TableInfo` synchronously and has no access to `ConnectedProfile`. The app
// layer would need to call `fetch_dependents` in the same background task that
// fetches table details, then write the result via `ConnectedProfile::populate_dependents`.
// That wiring is deferred to a follow-up slice once the fetch task pattern is
// extended to return both `TableInfo` and `Vec<RelationRef>`.

/// Fetch objects that depend on `schema.table` from a live PostgreSQL client.
///
/// Covers:
///  - Views (`pg_class.relkind = 'v'`) depending on the table via `pg_depend`.
///  - Materialized views (`pg_class.relkind = 'm'`).
///  - Tables with a foreign-key referencing this table (`information_schema`).
///  - Triggers defined on the table.
///
/// Returns an error if the query fails; returns an empty `Vec` when the table
/// has no dependents.
pub fn fetch_dependents(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Vec<dbflux_core::RelationRef>, DbError> {
    use dbflux_core::{RelationKind, RelationRef};

    let mut deps: Vec<RelationRef> = Vec::new();

    // Views and materialized views via pg_depend
    let view_rows = client
        .query(
            "
        SELECT
            n.nspname AS dep_schema,
            c.relname AS dep_name,
            c.relkind  AS dep_kind
        FROM pg_depend d
        JOIN pg_class c  ON c.oid = d.objid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class src ON src.oid = d.refobjid
        JOIN pg_namespace sn ON sn.oid = src.relnamespace
        WHERE d.deptype = 'n'
          AND src.relname = $1
          AND sn.nspname  = $2
          AND c.relkind IN ('v', 'm')
        ",
            &[&table, &schema],
        )
        .map_err(|e| DbError::QueryFailed(format!("fetch_dependents views: {}", e).into()))?;

    for row in view_rows {
        let dep_schema: &str = row.get("dep_schema");
        let dep_name: &str = row.get("dep_name");
        let relkind: i8 = row.get("dep_kind");
        let kind = if relkind == b'm' as i8 {
            RelationKind::MaterializedView
        } else {
            RelationKind::View
        };
        deps.push(RelationRef {
            kind,
            qualified_name: format!("{}.{}", dep_schema, dep_name),
        });
    }

    // FK child tables via information_schema
    let fk_rows = client
        .query(
            "
        SELECT
            kcu.table_schema AS child_schema,
            kcu.table_name   AS child_table,
            kcu.column_name  AS child_col
        FROM information_schema.referential_constraints rc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = rc.constraint_name
         AND kcu.constraint_schema = rc.constraint_schema
        JOIN information_schema.key_column_usage pku
          ON pku.constraint_name = rc.unique_constraint_name
         AND pku.constraint_schema = rc.unique_constraint_schema
        WHERE pku.table_schema = $1
          AND pku.table_name   = $2
        GROUP BY kcu.table_schema, kcu.table_name, kcu.column_name
        ",
            &[&schema, &table],
        )
        .map_err(|e| DbError::QueryFailed(format!("fetch_dependents fk_children: {}", e).into()))?;

    for row in fk_rows {
        let child_schema: &str = row.get("child_schema");
        let child_table: &str = row.get("child_table");
        let child_col: &str = row.get("child_col");
        let qualified = format!("{}.{}.{}", child_schema, child_table, child_col);

        // Deduplicate: only add the table reference once per unique table.
        let table_qname = format!("{}.{}", child_schema, child_table);
        if !deps
            .iter()
            .any(|d| d.kind == RelationKind::ForeignKeyChild && d.qualified_name == table_qname)
        {
            let _ = child_col;
            deps.push(RelationRef {
                kind: RelationKind::ForeignKeyChild,
                qualified_name: table_qname,
            });
        }
        let _ = qualified;
    }

    // Triggers on the table
    let trigger_rows = client
        .query(
            "
        SELECT trigger_schema, trigger_name
        FROM information_schema.triggers
        WHERE event_object_schema = $1
          AND event_object_table  = $2
        GROUP BY trigger_schema, trigger_name
        ",
            &[&schema, &table],
        )
        .map_err(|e| DbError::QueryFailed(format!("fetch_dependents triggers: {}", e).into()))?;

    for row in trigger_rows {
        let trig_schema: &str = row.get("trigger_schema");
        let trig_name: &str = row.get("trigger_name");
        deps.push(RelationRef {
            kind: RelationKind::Trigger,
            qualified_name: format!("{}.{}", trig_schema, trig_name),
        });
    }

    Ok(deps)
}

/// Map PostgreSQL `pg_proc.prokind` to `RoutineKind`.
///
/// Returns `None` for prokind values that are excluded from the routines folder
/// (e.g. trigger functions with prokind = 't').
fn prokind_to_routine_kind(prokind: char) -> Option<RoutineKind> {
    match prokind {
        'f' => Some(RoutineKind::Function),
        'p' => Some(RoutineKind::Procedure),
        'a' => Some(RoutineKind::Aggregate),
        'w' => Some(RoutineKind::Window),
        _ => None,
    }
}

fn get_schema_routines(
    client: &mut postgres::Client,
    schema: &str,
) -> Result<Vec<RoutineInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                p.proname AS name,
                p.prokind::char AS prokind,
                pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args,
                pg_catalog.pg_get_function_result(p.oid) AS return_type
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = $1
              AND p.prokind IN ('f','p','a','w')
            ORDER BY p.proname, identity_args
            "#,
            &[&schema],
        )
        .map_err(|e| format_pg_query_error(&e))?;

    let mut routines = Vec::with_capacity(rows.len());

    for row in &rows {
        let name: String = row.get("name");
        let prokind_str: &str = row.get("prokind");
        let identity_args: String = row.get("identity_args");
        let return_type: Option<String> = row.get("return_type");

        let prokind_char = prokind_str.chars().next().unwrap_or('f');
        let Some(kind) = prokind_to_routine_kind(prokind_char) else {
            continue;
        };

        let specific_name = format!("{}({})", name, identity_args);

        let parameter_types: Vec<String> = if identity_args.is_empty() {
            Vec::new()
        } else {
            vec![identity_args.clone()]
        };

        routines.push(RoutineInfo {
            name,
            kind,
            specific_name,
            parameter_types,
            return_type_hint: return_type,
        });
    }

    Ok(routines)
}

#[cfg(test)]
mod tests {
    use super::{
        POSTGRES_DIALECT, PgTextSearchText, PgUriSslMode, PgVectorText, PostgresCodeGenerator,
        PostgresDialect, PostgresDriver, TSQUERY_OP_AND, TSQUERY_OP_NOT, TSQUERY_OP_OR,
        TSQUERY_OP_PHRASE, decode_pgvector_halfvec, decode_pgvector_sparsevec,
        decode_pgvector_vector, decode_tsquery, decode_tsvector, format_pgvector_dense,
        format_pgvector_float4, format_pgvector_sparse, inject_password_into_pg_uri,
        parse_pg_uri_sslmode, pgvector_array_decode_to_value, pgvector_array_values_to_value,
        plan_postgres_semantic_request, prokind_to_routine_kind, text_search_array_values_to_value,
        unsupported_type_names, with_client_identity,
    };
    use dbflux_core::{
        AddColumnRequest, AlterColumnRequest, CodeGenerator, ColumnAssignment, CreateTableSpec,
        CreateTypeRequest, DatabaseCategory, DbConfig, DbDriver, DbError, DdlRejection,
        DefaultSpec, DropColumnRequest, FormValues, MutationRequest, QueryLanguage, RowInsert,
        SemanticRequest, SqlDialect, SqlMutationGenerator, SqlQueryBuilder, TableBrowseRequest,
        TableRef, TransferFamily, TypeAttributeDefinition, TypeDefinition, Value, WhereOperator,
    };
    use postgres::types::{FromSql, Kind, Type};

    fn sparsevec_payload(dimension: i32, indices: &[i32], values: &[f32]) -> Vec<u8> {
        assert_eq!(indices.len(), values.len());

        let mut payload = Vec::new();
        payload.extend_from_slice(&dimension.to_be_bytes());
        payload.extend_from_slice(&(indices.len() as i32).to_be_bytes());
        payload.extend_from_slice(&0i32.to_be_bytes());

        for index in indices {
            payload.extend_from_slice(&index.to_be_bytes());
        }

        for value in values {
            payload.extend_from_slice(&value.to_be_bytes());
        }

        payload
    }

    #[test]
    fn pgvector_scalar_decoders_render_canonical_text() {
        let mut vector = Vec::new();
        vector.extend_from_slice(&2u16.to_be_bytes());
        vector.extend_from_slice(&0u16.to_be_bytes());
        vector.extend_from_slice(&1.5f32.to_be_bytes());
        vector.extend_from_slice(&(-2.25f32).to_be_bytes());

        let mut halfvec = Vec::new();
        halfvec.extend_from_slice(&2u16.to_be_bytes());
        halfvec.extend_from_slice(&0u16.to_be_bytes());
        halfvec.extend_from_slice(&half::f16::from_f32(1.5).to_bits().to_be_bytes());
        halfvec.extend_from_slice(&half::f16::from_f32(-2.25).to_bits().to_be_bytes());

        let sparsevec = sparsevec_payload(4, &[0, 3], &[1.5, -2.25]);

        assert_eq!(
            decode_pgvector_vector(&vector).expect("valid vector").0,
            "[1.5,-2.25]"
        );
        assert_eq!(
            decode_pgvector_halfvec(&halfvec).expect("valid halfvec").0,
            "[1.5,-2.25]"
        );
        assert_eq!(
            decode_pgvector_sparsevec(&sparsevec)
                .expect("valid sparsevec")
                .0,
            "{1:1.5,4:-2.25}/4"
        );
    }

    #[test]
    fn pgvector_float4_formatter_matches_postgres_shortest_decimal_boundaries() {
        let cases = [
            (1e-6_f32, "1e-06"),
            (1e-5_f32, "1e-05"),
            (1e-4_f32, "0.0001"),
            (999_999_f32, "999999"),
            (1e6_f32, "1e+06"),
            (1e20_f32, "1e+20"),
            (-1e-6_f32, "-1e-06"),
            (-1e5_f32, "-100000"),
            (-1e20_f32, "-1e+20"),
            (f32::MIN_POSITIVE, "1.1754944e-38"),
            (f32::MAX, "3.4028235e+38"),
            (0.0_f32, "0"),
            (-0.0_f32, "-0"),
        ];

        for (value, expected) in cases {
            assert_eq!(format_pgvector_float4(value), expected, "value: {value:?}");
        }
    }

    #[test]
    fn pgvector_dense_and_sparse_formatters_share_float4_formatting() {
        assert_eq!(
            format_pgvector_dense(&[1e-6, 1e20, -0.0]),
            "[1e-06,1e+20,-0]"
        );
        assert_eq!(
            format_pgvector_sparse(&[(1, 1e-6), (4, -1e20)], 4),
            "{1:1e-06,4:-1e+20}/4"
        );
    }

    #[test]
    fn pgvector_decoders_reject_malformed_and_non_finite_payloads() {
        let malformed = [0, 1, 0, 0];
        assert!(decode_pgvector_vector(&malformed).is_err());

        let mut non_finite = Vec::new();
        non_finite.extend_from_slice(&1u16.to_be_bytes());
        non_finite.extend_from_slice(&0u16.to_be_bytes());
        non_finite.extend_from_slice(&f32::NAN.to_be_bytes());
        assert!(decode_pgvector_vector(&non_finite).is_err());
    }

    #[test]
    fn pgvector_sparsevec_rejects_invalid_indices_and_zero_values() {
        assert!(decode_pgvector_sparsevec(&sparsevec_payload(4, &[4], &[1.5])).is_err());
        assert!(decode_pgvector_sparsevec(&sparsevec_payload(4, &[1, 1], &[1.5, -2.25])).is_err());
        assert!(decode_pgvector_sparsevec(&sparsevec_payload(4, &[2, 1], &[1.5, -2.25])).is_err());
        assert!(decode_pgvector_sparsevec(&sparsevec_payload(4, &[0], &[0.0])).is_err());
        assert!(decode_pgvector_sparsevec(&sparsevec_payload(4, &[3], &[-0.0])).is_err());
    }

    #[test]
    fn pgvector_array_values_preserve_element_nulls() {
        let sparsevec = decode_pgvector_sparsevec(&sparsevec_payload(4, &[0, 3], &[1.5, -2.25]))
            .expect("valid sparsevec");
        let value = pgvector_array_values_to_value(Some(vec![
            Some(sparsevec),
            None,
            Some(PgVectorText("[3,4]".to_string())),
        ]));

        assert_eq!(
            value,
            Value::Array(vec![
                Value::Text("{1:1.5,4:-2.25}/4".to_string()),
                Value::Null,
                Value::Text("[3,4]".to_string()),
            ])
        );
        assert_eq!(pgvector_array_values_to_value(None), Value::Null);
    }

    fn tsvector_lexeme(lexeme: &str, positions: &[u16]) -> Vec<u8> {
        let mut payload = lexeme.as_bytes().to_vec();
        payload.push(0);
        payload.extend_from_slice(&(positions.len() as u16).to_be_bytes());

        for position in positions {
            payload.extend_from_slice(&position.to_be_bytes());
        }

        payload
    }

    fn tsvector_payload(lexemes: &[Vec<u8>]) -> Vec<u8> {
        let mut payload = (lexemes.len() as i32).to_be_bytes().to_vec();

        for lexeme in lexemes {
            payload.extend_from_slice(lexeme);
        }

        payload
    }

    fn wep(position: u16, weight: u16) -> u16 {
        (weight << 14) | position
    }

    #[test]
    fn tsvector_binary_payload_renders_postgres_text_output() {
        let payload = tsvector_payload(&[
            tsvector_lexeme("fat", &[wep(2, 0), wep(4, 3)]),
            tsvector_lexeme("rat", &[]),
            tsvector_lexeme("it's", &[wep(1, 1)]),
        ]);

        let PgTextSearchText(text) = decode_tsvector(&payload).expect("valid tsvector payload");

        assert_eq!(text, "'fat':2,4A 'rat' 'it''s':1C");
    }

    #[test]
    fn empty_tsvector_renders_as_empty_text() {
        let PgTextSearchText(text) =
            decode_tsvector(&tsvector_payload(&[])).expect("empty payload");

        assert_eq!(text, "");
    }

    #[test]
    fn truncated_tsvector_payload_is_rejected() {
        let mut payload = tsvector_payload(&[tsvector_lexeme("fat", &[wep(2, 0)])]);
        payload.pop();

        assert!(decode_tsvector(&payload).is_err());
    }

    fn tsquery_operand(lexeme: &str, weight: u8, prefix: bool) -> Vec<u8> {
        let mut payload = vec![1, weight, u8::from(prefix)];
        payload.extend_from_slice(lexeme.as_bytes());
        payload.push(0);

        payload
    }

    fn tsquery_operator(operator: u8, distance: Option<u16>) -> Vec<u8> {
        let mut payload = vec![2, operator];

        if let Some(distance) = distance {
            payload.extend_from_slice(&distance.to_be_bytes());
        }

        payload
    }

    fn tsquery_payload(items: &[Vec<u8>]) -> Vec<u8> {
        let mut payload = (items.len() as i32).to_be_bytes().to_vec();

        for item in items {
            payload.extend_from_slice(item);
        }

        payload
    }

    #[test]
    fn tsquery_binary_payload_preserves_operand_order() {
        let payload = tsquery_payload(&[
            tsquery_operator(TSQUERY_OP_AND, None),
            tsquery_operand("rat", 0, false),
            tsquery_operand("fat", 0, false),
        ]);

        let PgTextSearchText(text) = decode_tsquery(&payload).expect("valid tsquery payload");

        assert_eq!(text, "'fat' & 'rat'");
    }

    #[test]
    fn tsquery_lower_priority_operand_is_parenthesized() {
        let payload = tsquery_payload(&[
            tsquery_operator(TSQUERY_OP_AND, None),
            tsquery_operator(TSQUERY_OP_OR, None),
            tsquery_operand("rat", 0, false),
            tsquery_operand("cat", 0, false),
            tsquery_operand("fat", 0, false),
        ]);

        let PgTextSearchText(text) = decode_tsquery(&payload).expect("valid tsquery payload");

        assert_eq!(text, "'fat' & ( 'cat' | 'rat' )");
    }

    #[test]
    fn tsquery_renders_negation_weights_prefix_and_phrase_distance() {
        let payload = tsquery_payload(&[
            tsquery_operator(TSQUERY_OP_PHRASE, Some(3)),
            tsquery_operator(TSQUERY_OP_NOT, None),
            tsquery_operand("rat", 0, false),
            tsquery_operand("fat", 0b1010, true),
        ]);

        let PgTextSearchText(text) = decode_tsquery(&payload).expect("valid tsquery payload");

        assert_eq!(text, "'fat':*AC <3> !'rat'");
    }

    #[test]
    fn tsquery_with_extra_nodes_is_rejected() {
        let payload = tsquery_payload(&[
            tsquery_operand("fat", 0, false),
            tsquery_operand("rat", 0, false),
        ]);

        assert!(decode_tsquery(&payload).is_err());
    }

    #[test]
    fn malformed_tsvector_column_is_unsupported_and_warning_eligible() {
        let value = Value::Unsupported("tsvector".to_string());

        assert_eq!(
            unsupported_type_names(&[vec![value]]),
            ["tsvector".to_string()].into()
        );
    }

    #[test]
    fn text_search_array_values_map_nulls_to_null_cells() {
        assert_eq!(
            text_search_array_values_to_value(Some(vec![
                Some(PgTextSearchText("'fat':1".to_string())),
                None,
            ])),
            Value::Array(vec![Value::Text("'fat':1".to_string()), Value::Null])
        );
        assert_eq!(text_search_array_values_to_value(None), Value::Null);
    }

    #[test]
    fn malformed_pgvector_array_element_is_unsupported_and_warning_eligible() {
        let vector_type = Type::new("vector".to_string(), 0, Kind::Simple, "public".to_string());
        let vector_array_type = Type::new(
            "_vector".to_string(),
            0,
            Kind::Array(vector_type),
            "public".to_string(),
        );
        let malformed_element = [0, 1, 0, 0];
        let mut array = Vec::new();
        array.extend_from_slice(&1i32.to_be_bytes());
        array.extend_from_slice(&0i32.to_be_bytes());
        array.extend_from_slice(&0i32.to_be_bytes());
        array.extend_from_slice(&1i32.to_be_bytes());
        array.extend_from_slice(&1i32.to_be_bytes());
        array.extend_from_slice(&(malformed_element.len() as i32).to_be_bytes());
        array.extend_from_slice(&malformed_element);

        let decoded = Vec::<Option<PgVectorText>>::from_sql(&vector_array_type, &array).map(Some);

        let value = pgvector_array_decode_to_value("_vector", decoded);

        assert_eq!(value, Value::Unsupported("_vector".to_string()));
        assert_eq!(
            unsupported_type_names(&[vec![value]]),
            ["_vector".to_string()].into()
        );
    }

    #[test]
    fn unsupported_type_aggregation_is_deduplicated() {
        let rows = vec![
            vec![
                Value::Unsupported("vector".to_string()),
                Value::Unsupported("bit".to_string()),
            ],
            vec![
                Value::Unsupported("vector".to_string()),
                Value::Unsupported("varbit".to_string()),
            ],
        ];

        assert_eq!(
            unsupported_type_names(&rows)
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["bit", "varbit", "vector"]
        );
    }

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
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
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
    fn with_client_identity_appends_to_keyword_string_when_absent() {
        let identity = dbflux_core::client_identity();
        let conn_str = "host=localhost port=5432 user=app password=secret dbname=app";

        let result = with_client_identity(conn_str);

        assert_eq!(
            result,
            format!("{} application_name={}", conn_str, identity)
        );
    }

    #[test]
    fn with_client_identity_keeps_user_supplied_value_in_keyword_string() {
        let conn_str =
            "host=localhost port=5432 user=app password=secret dbname=app application_name=myapp";

        let result = with_client_identity(conn_str);

        assert_eq!(result, conn_str);
    }

    #[test]
    fn with_client_identity_appends_query_param_to_uri_without_existing_query() {
        let identity = dbflux_core::client_identity();
        let conn_str = "postgresql://user:pass@localhost:5432/app";

        let result = with_client_identity(conn_str);

        assert_eq!(
            result,
            format!("{}?application_name={}", conn_str, identity)
        );
    }

    #[test]
    fn with_client_identity_appends_query_param_to_uri_with_existing_query() {
        let identity = dbflux_core::client_identity();
        let conn_str = "postgresql://user:pass@localhost:5432/app?sslmode=require";

        let result = with_client_identity(conn_str);

        assert_eq!(
            result,
            format!("{}&application_name={}", conn_str, identity)
        );
    }

    #[test]
    fn with_client_identity_keeps_user_supplied_value_in_uri_query() {
        let conn_str = "postgresql://user:pass@localhost:5432/app?application_name=myapp";

        let result = with_client_identity(conn_str);

        assert_eq!(result, conn_str);
    }

    #[test]
    fn with_client_identity_keeps_user_supplied_value_as_first_query_param() {
        let conn_str =
            "postgres://user:pass@localhost:5432/app?application_name=myapp&sslmode=require";

        let result = with_client_identity(conn_str);

        assert_eq!(result, conn_str);
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
        assert_eq!(metadata.transfer_family, TransferFamily::Sql);
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

    // ===== Column ALTER seam (DBF-24) =====

    #[test]
    fn postgres_codegen_generates_add_column_with_default() {
        let generator = PostgresCodeGenerator;
        let request = AddColumnRequest {
            table_name: "users",
            schema_name: Some("public"),
            column_name: "age",
            type_name: "INTEGER",
            nullable: false,
            default: Some("0"),
        };

        let statements = generator
            .generate_add_column(&request)
            .expect("postgres should generate add column sql");

        assert_eq!(
            statements,
            vec!["ALTER TABLE \"public\".\"users\" ADD COLUMN \"age\" INTEGER NOT NULL DEFAULT 0;"]
        );
    }

    #[test]
    fn postgres_codegen_rejects_injected_default_and_type() {
        let generator = PostgresCodeGenerator;

        let injected_default = AddColumnRequest {
            table_name: "users",
            schema_name: Some("public"),
            column_name: "age",
            type_name: "INTEGER",
            nullable: true,
            default: Some("0; DROP TABLE users; --"),
        };
        assert!(
            generator.generate_add_column(&injected_default).is_err(),
            "a default carrying a stacked statement must be rejected, not emitted"
        );

        let injected_type = AlterColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "age",
            new_type: Some("TEXT; DROP TABLE users; --"),
            nullable: None,
            default: None,
        };
        assert!(
            generator.generate_alter_column(&injected_type).is_err(),
            "a type carrying a stacked statement must be rejected, not emitted"
        );

        let legit = AddColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "name",
            type_name: "VARCHAR(255)",
            nullable: false,
            default: Some("now()"),
        };
        assert!(
            generator.generate_add_column(&legit).is_ok(),
            "legitimate VARCHAR(255)/now() must still pass"
        );
    }

    #[test]
    fn postgres_codegen_generates_add_column_nullable_without_default() {
        let generator = PostgresCodeGenerator;
        let request = AddColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "nickname",
            type_name: "TEXT",
            nullable: true,
            default: None,
        };

        let statements = generator
            .generate_add_column(&request)
            .expect("postgres should generate add column sql");

        assert_eq!(
            statements,
            vec!["ALTER TABLE \"users\" ADD COLUMN \"nickname\" TEXT;"]
        );
    }

    #[test]
    fn postgres_codegen_generates_drop_column() {
        let generator = PostgresCodeGenerator;
        let request = DropColumnRequest {
            table_name: "users",
            schema_name: Some("public"),
            column_name: "age",
        };

        let statements = generator
            .generate_drop_column(&request)
            .expect("postgres should generate drop column sql");

        assert_eq!(
            statements,
            vec!["ALTER TABLE \"public\".\"users\" DROP COLUMN \"age\";"]
        );
    }

    #[test]
    fn postgres_codegen_alter_column_emits_independent_type_nullable_default_clauses() {
        let generator = PostgresCodeGenerator;
        let request = AlterColumnRequest {
            table_name: "users",
            schema_name: Some("public"),
            column_name: "age",
            new_type: Some("BIGINT"),
            nullable: Some(true),
            default: Some(DefaultSpec::Set("0")),
        };

        let statements = generator
            .generate_alter_column(&request)
            .expect("postgres should generate alter column sql");

        assert_eq!(
            statements,
            vec![
                "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE BIGINT;",
                "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" DROP NOT NULL;",
                "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" SET DEFAULT 0;",
            ]
        );
    }

    #[test]
    fn postgres_codegen_alter_column_can_drop_default_alone() {
        let generator = PostgresCodeGenerator;
        let request = AlterColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "age",
            new_type: None,
            nullable: None,
            default: Some(DefaultSpec::Drop),
        };

        let statements = generator
            .generate_alter_column(&request)
            .expect("postgres should generate alter column sql");

        assert_eq!(
            statements,
            vec!["ALTER TABLE \"users\" ALTER COLUMN \"age\" DROP DEFAULT;"]
        );
    }

    #[test]
    fn postgres_codegen_alter_column_rejects_when_nothing_to_change() {
        let generator = PostgresCodeGenerator;
        let request = AlterColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "age",
            new_type: None,
            nullable: None,
            default: None,
        };

        let result = generator.generate_alter_column(&request);

        assert_eq!(
            result,
            Err(DdlRejection {
                reason: "ALTER COLUMN requires at least one of: type, nullable, default"
                    .to_string(),
                followup: None,
            })
        );
    }

    // ===== Array literal emission (#76) =====

    use super::{format_pg_array_literal, pg_array_element_type, value_to_pg_literal_typed};

    #[test]
    fn pg_array_element_type_handles_internal_and_sql_names() {
        assert_eq!(pg_array_element_type("_text"), Some("text"));
        assert_eq!(pg_array_element_type("_int4"), Some("int4"));
        assert_eq!(pg_array_element_type("text[]"), Some("text"));
        assert_eq!(pg_array_element_type("integer[]"), Some("int4"));
        assert_eq!(pg_array_element_type("BIGINT[]"), Some("int8"));
        assert_eq!(pg_array_element_type("text"), None);
        assert_eq!(pg_array_element_type("jsonb"), None);
    }

    #[test]
    fn array_literal_from_value_array() {
        let value = Value::Array(vec![
            Value::Text("Espacio".into()),
            Value::Text("hola".into()),
        ]);
        let sql = format_pg_array_literal(&value, "text");
        assert_eq!(sql, "ARRAY['Espacio', 'hola']::text[]");
    }

    #[test]
    fn array_literal_from_json_array_string() {
        // Cell was edited as JSON text in the data grid.
        let value = Value::Json(r#"["Espacio","hola"]"#.into());
        let sql = format_pg_array_literal(&value, "text");
        assert_eq!(sql, "ARRAY['Espacio', 'hola']::text[]");
    }

    #[test]
    fn array_literal_int_elements() {
        let value = Value::Json(r#"[1, 2, 3]"#.into());
        let sql = format_pg_array_literal(&value, "int4");
        assert_eq!(sql, "ARRAY[1, 2, 3]::int4[]");
    }

    #[test]
    fn array_literal_null() {
        let sql = format_pg_array_literal(&Value::Null, "text");
        assert_eq!(sql, "NULL::text[]");
    }

    #[test]
    fn array_literal_empty() {
        let value = Value::Array(vec![]);
        let sql = format_pg_array_literal(&value, "text");
        assert_eq!(sql, "ARRAY[]::text[]");
    }

    #[test]
    fn array_literal_escapes_single_quotes() {
        let value = Value::Array(vec![Value::Text("it's".into())]);
        let sql = format_pg_array_literal(&value, "text");
        assert_eq!(sql, "ARRAY['it''s']::text[]");
    }

    #[test]
    fn typed_literal_falls_back_to_jsonb_for_jsonb_column() {
        let value = Value::Json(r#"{"k":1}"#.into());
        let sql = value_to_pg_literal_typed(&value, Some("jsonb"));
        assert_eq!(sql, "'{\"k\":1}'::jsonb");
    }

    #[test]
    fn typed_literal_no_type_info_uses_default() {
        let value = Value::Text("hi".into());
        let sql = value_to_pg_literal_typed(&value, None);
        assert_eq!(sql, "'hi'");
    }

    #[test]
    fn typed_literal_routes_text_array_via_array_path() {
        let value = Value::Array(vec![Value::Text("a".into())]);
        let sql = value_to_pg_literal_typed(&value, Some("_text"));
        assert_eq!(sql, "ARRAY['a']::text[]");
    }

    #[test]
    fn typed_literal_null_for_array_column() {
        // When the column type is known to be an array, emit `NULL::elem[]`
        // explicitly. Bare `NULL` would also work in INSERT/UPDATE column
        // position because the server infers from the destination, but an
        // explicit cast is safer in expression contexts (e.g. COALESCE).
        let sql = value_to_pg_literal_typed(&Value::Null, Some("_text"));
        assert_eq!(sql, "NULL::text[]");
    }

    #[test]
    fn typed_literal_null_no_type_info_stays_bare() {
        let sql = value_to_pg_literal_typed(&Value::Null, None);
        assert_eq!(sql, "NULL");
    }

    #[test]
    fn pg_array_element_type_covers_common_string_aliases() {
        assert_eq!(pg_array_element_type("_varchar"), Some("varchar"));
        assert_eq!(pg_array_element_type("_bpchar"), Some("bpchar"));
        assert_eq!(pg_array_element_type("varchar[]"), Some("varchar"));
        assert_eq!(
            pg_array_element_type("character varying[]"),
            Some("varchar")
        );
    }

    #[test]
    fn array_literal_wraps_scalar_value_for_array_column() {
        // Fallback path: caller passed a scalar where an array was expected.
        // The dialect wraps it as a single-element ARRAY[...] literal and
        // lets the server validate the element type.
        let sql = format_pg_array_literal(&Value::Text("solo".into()), "text");
        assert_eq!(sql, "ARRAY['solo']::text[]");
    }

    #[test]
    fn prokind_to_routine_kind_mapping() {
        use dbflux_core::RoutineKind;

        assert_eq!(prokind_to_routine_kind('f'), Some(RoutineKind::Function));
        assert_eq!(prokind_to_routine_kind('p'), Some(RoutineKind::Procedure));
        assert_eq!(prokind_to_routine_kind('a'), Some(RoutineKind::Aggregate));
        assert_eq!(prokind_to_routine_kind('w'), Some(RoutineKind::Window));
        // Trigger functions are excluded
        assert_eq!(prokind_to_routine_kind('t'), None);
        // Unknown characters are excluded
        assert_eq!(prokind_to_routine_kind('x'), None);
    }

    #[test]
    #[ignore = "requires live Postgres connection"]
    fn live_schema_routines_returns_results() {
        // This test requires a live Postgres fixture. Run with:
        //   cargo nextest run -p dbflux_driver_postgres --run-ignored
        // Skipped in normal CI.
        let _ = "placeholder for live integration test";
    }

    #[test]
    fn postgres_metadata_advertises_chart_authoring() {
        use super::METADATA;
        use dbflux_core::DriverCapabilities;

        assert!(
            METADATA
                .capabilities
                .contains(DriverCapabilities::CHART_AUTHORING),
            "CHART_AUTHORING must be set: drivers advertising INSTANCE_METRICS also need \
             CHART_AUTHORING so the sidebar surfaces Dashboards / Saved Charts folders"
        );
    }

    #[test]
    fn postgres_metadata_advertises_instance_metrics() {
        use super::METADATA;
        use dbflux_core::DriverCapabilities;

        assert!(
            METADATA
                .capabilities
                .contains(DriverCapabilities::INSTANCE_METRICS),
            "INSTANCE_METRICS must remain set on PostgreSQL driver"
        );
    }

    #[test]
    fn postgres_metadata_advertises_bulk_transfer_capabilities() {
        use super::METADATA;
        use dbflux_core::DriverCapabilities;

        assert!(
            METADATA
                .capabilities
                .contains(DriverCapabilities::BULK_INSERT)
        );
        assert!(
            METADATA
                .capabilities
                .contains(DriverCapabilities::TRUNCATE_TABLE)
        );
        assert!(
            METADATA
                .capabilities
                .contains(DriverCapabilities::DISABLE_FK_CHECKS)
        );
    }

    #[test]
    fn postgres_generate_bulk_insert_emits_multi_row_values() {
        use dbflux_core::QueryGenerator;

        let generator = SqlMutationGenerator::new(&POSTGRES_DIALECT);
        let columns = vec!["name".to_string(), "age".to_string()];
        let owned_rows: Vec<Vec<dbflux_core::Value>> = vec![
            vec![
                dbflux_core::Value::Text("Alice".to_string()),
                dbflux_core::Value::Int(25),
            ],
            vec![
                dbflux_core::Value::Text("Bob".to_string()),
                dbflux_core::Value::Int(30),
            ],
        ];
        let rows: Vec<&[dbflux_core::Value]> = owned_rows.iter().map(|r| r.as_slice()).collect();

        let generated = generator
            .generate_bulk_insert(None, "users", &columns, &[], &rows)
            .unwrap()
            .expect("postgres generator must support native bulk insert");

        assert_eq!(
            generated.text,
            "INSERT INTO \"users\" (\"name\", \"age\") VALUES ('Alice', 25), ('Bob', 30)"
        );
    }

    /// JD-C2 regression (bulk route): a `text[]` column's `Value::Array` must
    /// emit `ARRAY[...]::text[]` when the generator is given the column's
    /// type, not the untyped `'...'::jsonb` fallback.
    #[test]
    fn postgres_generate_bulk_insert_emits_array_literal_when_column_type_is_known() {
        use dbflux_core::QueryGenerator;

        let generator = SqlMutationGenerator::new(&POSTGRES_DIALECT);
        let columns = vec!["tags".to_string()];
        let column_types = vec![Some("text[]".to_string())];
        let owned_rows: Vec<Vec<Value>> = vec![vec![Value::Array(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ])]];
        let rows: Vec<&[Value]> = owned_rows.iter().map(|r| r.as_slice()).collect();

        let generated = generator
            .generate_bulk_insert(None, "t", &columns, &column_types, &rows)
            .unwrap()
            .expect("postgres generator must support native bulk insert");

        assert_eq!(
            generated.text,
            "INSERT INTO \"t\" (\"tags\") VALUES (ARRAY['a', 'b']::text[])"
        );
        assert!(
            !generated.text.contains("::jsonb"),
            "a typed array column must never fall back to a jsonb cast: {}",
            generated.text
        );
    }

    /// JD-C2 regression (per-row route): the same `text[]` column must emit
    /// `ARRAY[...]::text[]` through `RowInsert::with_typed_assignments` +
    /// `build_insert` — the path `TableSink`'s per-row fallback now uses
    /// instead of the untyped `RowInsert::new`.
    #[test]
    fn postgres_build_insert_emits_array_literal_for_typed_assignment() {
        let insert = RowInsert::with_typed_assignments(
            "t".to_string(),
            None,
            vec![ColumnAssignment {
                name: "tags".to_string(),
                value: Value::Array(vec![
                    Value::Text("a".to_string()),
                    Value::Text("b".to_string()),
                ]),
                type_name: Some("text[]".to_string()),
            }],
        );

        let builder = SqlQueryBuilder::new(&POSTGRES_DIALECT);
        let sql = builder.build_insert(&insert, false).unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"tags\") VALUES (ARRAY['a', 'b']::text[])"
        );
        assert!(!sql.contains("::jsonb"));
    }

    #[test]
    fn postgres_generate_create_table_preserves_types_and_pk() {
        use dbflux_core::QueryGenerator;

        let generator = SqlMutationGenerator::new(&POSTGRES_DIALECT);
        let spec = CreateTableSpec {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            columns: vec![
                dbflux_core::TransferColumn {
                    name: "id".to_string(),
                    type_name: Some("integer".to_string()),
                    nullable: false,
                    is_primary_key: true,
                },
                dbflux_core::TransferColumn {
                    name: "name".to_string(),
                    type_name: Some("text".to_string()),
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            if_not_exists: false,
        };

        let generated = generator
            .generate_create_table(&spec)
            .unwrap()
            .expect("postgres generator must support native CREATE TABLE");

        assert_eq!(
            generated.text,
            "CREATE TABLE \"public\".\"users\" (\n    \"id\" integer NOT NULL,\n    \"name\" text,\n    PRIMARY KEY (\"id\")\n);"
        );
    }

    // --- Phase 2.4: URI transform splits password (R-SEC-1 / C1 / ADR-1) ---

    #[test]
    fn uri_transform_splits_password() {
        use dbflux_core::secrecy::ExposeSecret;
        use dbflux_core::{FieldExportTransform, FormValues};

        let driver = PostgresDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());
        values.insert(
            "uri".to_string(),
            "postgres://alice:s3cr3t@db.example/app".to_string(),
        );

        let transform = driver.export_field_transform("uri", &values);

        let FieldExportTransform::SplitSecret { skeleton, secret } = transform else {
            panic!("expected SplitSecret but got None");
        };

        assert!(
            !skeleton.contains("s3cr3t"),
            "skeleton must not contain the password: {skeleton}"
        );
        assert!(
            skeleton.contains("alice"),
            "skeleton must contain the username: {skeleton}"
        );
        assert_eq!(
            secret.expose_secret(),
            "s3cr3t",
            "secret must be the extracted password"
        );
    }

    #[test]
    fn uri_transform_no_credentials_returns_none() {
        use dbflux_core::{FieldExportTransform, FormValues};

        let driver = PostgresDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());
        values.insert("uri".to_string(), "postgres://db.example/app".to_string());

        assert!(
            matches!(
                driver.export_field_transform("uri", &values),
                FieldExportTransform::None
            ),
            "URI without credentials must return None"
        );
    }

    #[test]
    fn uri_transform_non_uri_mode_returns_none() {
        use dbflux_core::{FieldExportTransform, FormValues};

        let driver = PostgresDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "false".to_string());
        values.insert("host".to_string(), "localhost".to_string());

        assert!(
            matches!(
                driver.export_field_transform("uri", &values),
                FieldExportTransform::None
            ),
            "non-URI mode must return None"
        );
    }
}
