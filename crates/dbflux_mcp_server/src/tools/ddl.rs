//! DDL operation tools for MCP server.
//!
//! Provides type-safe parameter structs for data definition operations:
//! - `create_table`: Create a new table with columns and constraints
//! - `alter_table`: Modify table structure (add/drop/alter columns, constraints)
//! - `create_index`: Create an index on one or more columns
//! - `drop_index`: Remove an index
//! - `create_type`: Create custom types (PostgreSQL only)
//! - `drop_table`: Drop a table (requires confirmation)
//! - `drop_database`: Drop a database (requires confirmation)
//!
//! Operations are classified per-tool and per-alter action risk level.

use crate::{
    DbFluxServer,
    helper::{IntoErrorData, *},
    state::ServerState,
};
use dbflux_core::{
    AddForeignKeyRequest, CodeGenCapabilities, Connection, CreateTypeRequest, DbKind,
    DropForeignKeyRequest, QueryRequest, TableRef, TypeAttributeDefinition, TypeDefinition, Value,
};
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData},
    schemars::JsonSchema,
    tool, tool_router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

fn default_true() -> Option<bool> {
    Some(true)
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ColumnDef {
    #[schemars(description = "Column name")]
    pub name: String,

    #[schemars(description = "Column type (e.g., 'integer', 'varchar(255)', 'timestamp')")]
    pub r#type: String,

    pub nullable: Option<bool>,

    pub primary_key: Option<bool>,

    pub auto_increment: Option<bool>,

    pub default: Option<serde_json::Value>,

    pub references: Option<ForeignKeyRef>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ForeignKeyRef {
    pub table: String,

    pub column: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateTableParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table name to create")]
    pub table: String,

    #[schemars(description = "Column definitions")]
    pub columns: Vec<ColumnDef>,

    #[schemars(description = "Columns for composite primary key (if not defined per-column)")]
    pub primary_key: Option<Vec<String>>,

    #[schemars(default = "default_true")]
    pub if_not_exists: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct AlterOperation {
    #[schemars(
        description = "Action type: add_column, drop_column, rename_column, alter_column, add_constraint, drop_constraint"
    )]
    pub action: String,

    pub column: Option<String>,

    pub definition: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AlterTableParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table name to alter")]
    pub table: String,

    #[schemars(description = "Alter operations to perform")]
    pub operations: Vec<AlterOperation>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateIndexParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table name")]
    pub table: String,

    #[schemars(description = "Columns to include in the index")]
    pub columns: Vec<String>,

    #[schemars(description = "Index name (auto-generated if not provided)")]
    pub index_name: Option<String>,

    pub unique: Option<bool>,

    pub if_not_exists: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DropIndexParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    pub table: Option<String>,

    #[schemars(description = "Index name to drop")]
    pub index_name: String,

    pub if_exists: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TypeAttribute {
    pub name: String,

    pub r#type: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateTypeParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Type name")]
    pub name: String,

    #[schemars(description = "Type: enum, composite, or domain")]
    pub r#type: String,

    #[schemars(description = "Values for enum type")]
    pub values: Option<Vec<String>>,

    #[schemars(description = "Attributes for composite type")]
    pub attributes: Option<Vec<TypeAttribute>>,

    #[schemars(description = "Base type for domain type")]
    pub base_type: Option<String>,

    pub if_not_exists: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DropTableParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table name to drop")]
    pub table: String,

    pub cascade: Option<bool>,

    pub if_exists: Option<bool>,

    #[schemars(description = "Confirmation string - must match table name exactly")]
    pub confirm: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DropDatabaseParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Database name to drop")]
    pub database: String,

    pub if_exists: Option<bool>,

    #[schemars(description = "Confirmation string - must match database name exactly")]
    pub confirm: String,
}

#[derive(Debug, Clone)]
struct CreateTypeRequestParams {
    connection_id: String,
    name: String,
    type_type: String,
    values: Option<Vec<String>>,
    attributes: Option<Vec<TypeAttribute>>,
    base_type: Option<String>,
    if_not_exists: bool,
}

const DROP_TABLE_CONFIRMATION_ERROR: &str = "Confirmation string must match table name exactly";
const DROP_DATABASE_CONFIRMATION_ERROR: &str =
    "Confirmation string must match database name exactly";
const CREATE_TYPE_POSTGRES_ONLY_ERROR: &str =
    "CREATE TYPE is only supported for PostgreSQL connections";
const POSTGRES_DUPLICATE_OBJECT_SQLSTATE: &str = "42710";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateTypeKind {
    Enum,
    Composite,
    Domain,
}

impl CreateTypeKind {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "enum" => Ok(Self::Enum),
            "composite" => Ok(Self::Composite),
            "domain" => Ok(Self::Domain),
            other => Err(format!(
                "Unsupported type '{}'. Expected one of: enum, composite, domain",
                other
            )),
        }
    }
}

fn is_simple_postgres_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
}

fn validate_postgres_type_name(name: &str) -> Result<(), String> {
    let trimmed = name.trim();

    if trimmed.contains('"') {
        return Err(
            "Quoted PostgreSQL type names are not supported; use unquoted name or schema.name"
                .to_string(),
        );
    }

    let parts: Vec<&str> = trimmed.split('.').map(str::trim).collect();
    if parts.len() > 2
        || parts
            .iter()
            .any(|part| !is_simple_postgres_identifier(part))
    {
        return Err(
            "Type name must be an unquoted PostgreSQL identifier or schema-qualified pair"
                .to_string(),
        );
    }

    Ok(())
}

fn validate_postgres_type_expression(expression: &str, field_name: &str) -> Result<(), String> {
    let trimmed = expression.trim();

    if trimmed.is_empty() {
        return Err(format!("{} must be a non-empty string", field_name));
    }

    if trimmed.contains('"') {
        return Err(format!(
            "{} does not support quoted PostgreSQL identifiers",
            field_name
        ));
    }

    if trimmed.contains('"')
        || trimmed.contains('\'')
        || trimmed.contains(';')
        || trimmed.contains("--")
        || trimmed.contains("/*")
        || trimmed.contains("*/")
    {
        return Err(format!(
            "{} contains unsupported PostgreSQL type syntax",
            field_name
        ));
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
                    return Err(format!("{} has unbalanced parentheses", field_name));
                }
                paren_depth -= 1;
            }
            '[' => {
                if chars.get(index + 1) != Some(&']') {
                    return Err(format!("{} contains unsupported array syntax", field_name));
                }
                index += 1;
            }
            _ => {
                return Err(format!(
                    "{} contains unsupported character '{}'",
                    field_name, ch
                ));
            }
        }

        index += 1;
    }

    if paren_depth != 0 {
        return Err(format!("{} has unbalanced parentheses", field_name));
    }

    if !saw_identifier {
        return Err(format!(
            "{} must include a PostgreSQL type name",
            field_name
        ));
    }

    Ok(())
}

fn is_postgres_duplicate_type_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();

    normalized.contains(POSTGRES_DUPLICATE_OBJECT_SQLSTATE)
        || normalized.contains("duplicate_object")
        || normalized.contains("already exists")
}

fn normalize_table_ref(name: &str) -> TableRef {
    let trimmed = name.trim();

    if let Some((schema, type_name)) = trimmed.split_once('.') {
        TableRef::with_schema(schema.trim(), type_name.trim())
    } else {
        TableRef::new(trimmed)
    }
}

fn normalize_type_expression(expression: &str) -> String {
    expression.trim().to_string()
}

fn normalize_identifier(identifier: &str) -> String {
    identifier.trim().to_string()
}

fn validate_unique_trimmed_values(values: &[String], field_name: &str) -> Result<(), String> {
    let mut seen = HashSet::new();

    for value in values {
        let normalized = value.trim();
        if !seen.insert(normalized.to_string()) {
            return Err(format!("{} must be unique", field_name));
        }
    }

    Ok(())
}

/// Classify ALTER TABLE operations based on their risk level.
///
/// Returns the highest (most restrictive) classification among all operations.
pub fn classify_alter_operations(
    operations: &[AlterOperation],
) -> dbflux_policy::ExecutionClassification {
    use dbflux_policy::ExecutionClassification;

    let classifications: Vec<ExecutionClassification> = operations
        .iter()
        .map(|op| {
            let action_upper = op.action.to_uppercase();
            match action_upper.as_str() {
                "ADD_COLUMN" | "ADD COLUMN" => {
                    if is_add_column_safe(op) {
                        ExecutionClassification::AdminSafe
                    } else {
                        ExecutionClassification::Admin
                    }
                }
                "DROP_COLUMN" | "DROP COLUMN" => ExecutionClassification::AdminDestructive,
                "RENAME_COLUMN" | "RENAME COLUMN" => ExecutionClassification::AdminSafe,
                "ALTER_COLUMN" | "ALTER COLUMN" => classify_alter_column(op),
                "ADD_CONSTRAINT" | "ADD CONSTRAINT" => ExecutionClassification::Admin,
                "DROP_CONSTRAINT" | "DROP CONSTRAINT" => ExecutionClassification::AdminDestructive,
                _ => ExecutionClassification::Admin,
            }
        })
        .collect();

    // Return the highest classification, defaulting to AdminSafe if empty
    classifications
        .into_iter()
        .fold(ExecutionClassification::AdminSafe, |acc, c| acc.max(c))
}

/// Check if ADD_COLUMN operation is safe (nullable or has default).
fn is_add_column_safe(op: &AlterOperation) -> bool {
    if let Some(ref def) = op.definition {
        let nullable = def
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let has_default = def.get("default").is_some();
        nullable || has_default
    } else {
        true // No definition means using driver defaults (usually nullable)
    }
}

/// Classify ALTER_COLUMN operation.
///
/// MVP: All ALTER_COLUMN operations are Admin level.
/// Future: Detect widening vs narrowing types for more granular classification.
fn classify_alter_column(_op: &AlterOperation) -> dbflux_policy::ExecutionClassification {
    use dbflux_policy::ExecutionClassification;

    // MVP: Treat all column alterations as Admin
    // Future: Detect type widening (safe) vs narrowing (destructive)
    ExecutionClassification::Admin
}

fn normalize_foreign_key_constraint_type(definition: &serde_json::Value) -> Option<String> {
    definition
        .get("type")
        .and_then(|value| value.as_str())
        .map(|value| value.to_uppercase().replace(' ', "_"))
}

fn json_array_to_strings(
    definition: &serde_json::Value,
    field: &str,
    error_message: &'static str,
) -> Result<Vec<String>, String> {
    let values = definition
        .get(field)
        .and_then(|value| value.as_array())
        .ok_or_else(|| error_message.to_string())?;

    Ok(values
        .iter()
        .filter_map(|value| value.as_str())
        .map(ToOwned::to_owned)
        .collect())
}

fn generate_add_foreign_key_sql(
    code_generator: &dyn dbflux_core::CodeGenerator,
    table: &TableRef,
    constraint_name: &str,
    definition: &serde_json::Value,
) -> Result<Option<String>, String> {
    if normalize_foreign_key_constraint_type(definition).as_deref() != Some("FOREIGN_KEY") {
        return Ok(None);
    }

    let columns = json_array_to_strings(
        definition,
        "columns",
        "FOREIGN KEY constraint requires columns array",
    )?;

    let ref_table_raw = definition
        .get("ref_table")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "FOREIGN KEY constraint requires ref_table".to_string())?;

    let ref_table = TableRef::from_qualified(ref_table_raw);
    let ref_columns = json_array_to_strings(
        definition,
        "ref_columns",
        "FOREIGN KEY constraint requires ref_columns array",
    )?;

    let request = AddForeignKeyRequest {
        constraint_name,
        table_name: &table.name,
        schema_name: table.schema.as_deref(),
        columns: &columns,
        ref_table: &ref_table.name,
        ref_schema: definition
            .get("ref_schema")
            .and_then(|value| value.as_str())
            .or(ref_table.schema.as_deref()),
        ref_columns: &ref_columns,
        on_delete: definition.get("on_delete").and_then(|value| value.as_str()),
        on_update: definition.get("on_update").and_then(|value| value.as_str()),
    };

    Ok(code_generator.generate_add_foreign_key(&request))
}

fn generate_drop_foreign_key_sql(
    code_generator: &dyn dbflux_core::CodeGenerator,
    table: &TableRef,
    constraint_name: &str,
    definition: Option<&serde_json::Value>,
) -> Option<String> {
    let definition = definition?;

    if normalize_foreign_key_constraint_type(definition).as_deref() != Some("FOREIGN_KEY") {
        return None;
    }

    let request = DropForeignKeyRequest {
        constraint_name,
        table_name: &table.name,
        schema_name: table.schema.as_deref(),
    };

    code_generator.generate_drop_foreign_key(&request)
}

pub fn validate_drop_table_params(params: &DropTableParams) -> Result<(), ErrorData> {
    if params.confirm != params.table {
        return Err(ErrorData::invalid_params(
            DROP_TABLE_CONFIRMATION_ERROR,
            None,
        ));
    }
    Ok(())
}

pub fn validate_drop_database_params(params: &DropDatabaseParams) -> Result<(), ErrorData> {
    if params.confirm != params.database {
        return Err(ErrorData::invalid_params(
            DROP_DATABASE_CONFIRMATION_ERROR,
            None,
        ));
    }
    Ok(())
}

pub fn validate_create_type_params(params: &CreateTypeParams) -> Result<(), String> {
    if params.name.trim().is_empty() {
        return Err("Type name is required".to_string());
    }

    validate_postgres_type_name(&params.name)?;

    let kind = CreateTypeKind::parse(&params.r#type)?;

    match kind {
        CreateTypeKind::Enum => {
            let values = params
                .values
                .as_ref()
                .ok_or_else(|| "Enum type requires values".to_string())?;

            if values.is_empty() {
                return Err("Enum type requires at least one value".to_string());
            }

            if values.iter().any(|value| value.trim().is_empty()) {
                return Err("Enum values must be non-empty strings".to_string());
            }

            validate_unique_trimmed_values(values, "Enum values")?;

            if params.attributes.is_some() {
                return Err("Enum type does not accept attributes".to_string());
            }

            if params.base_type.is_some() {
                return Err("Enum type does not accept base_type".to_string());
            }
        }
        CreateTypeKind::Composite => {
            let attributes = params
                .attributes
                .as_ref()
                .ok_or_else(|| "Composite type requires attributes".to_string())?;

            if attributes.is_empty() {
                return Err("Composite type requires at least one attribute".to_string());
            }

            if attributes.iter().any(|attribute| {
                attribute.name.trim().is_empty() || attribute.r#type.trim().is_empty()
            }) {
                return Err("Composite attributes require non-empty name and type".to_string());
            }

            let attribute_names = attributes
                .iter()
                .map(|attribute| attribute.name.clone())
                .collect::<Vec<_>>();
            validate_unique_trimmed_values(&attribute_names, "Composite attribute names")?;

            for attribute in attributes {
                validate_postgres_type_expression(
                    &attribute.r#type,
                    &format!("Composite attribute '{}' type", attribute.name),
                )?;
            }

            if params.values.is_some() {
                return Err("Composite type does not accept enum values".to_string());
            }

            if params.base_type.is_some() {
                return Err("Composite type does not accept base_type".to_string());
            }
        }
        CreateTypeKind::Domain => {
            let base_type = params
                .base_type
                .as_deref()
                .ok_or_else(|| "Domain type requires base_type".to_string())?;

            if base_type.trim().is_empty() {
                return Err("Domain base_type must be a non-empty string".to_string());
            }

            validate_postgres_type_expression(base_type, "Domain base_type")?;

            if params.values.is_some() {
                return Err("Domain type does not accept enum values".to_string());
            }

            if params.attributes.is_some() {
                return Err("Domain type does not accept attributes".to_string());
            }
        }
    }

    Ok(())
}

fn build_create_type_definition(
    type_type: &str,
    values: Option<&[String]>,
    attributes: Option<&[crate::tools::TypeAttribute]>,
    base_type: Option<&str>,
) -> Result<TypeDefinition, String> {
    match CreateTypeKind::parse(type_type)? {
        CreateTypeKind::Enum => Ok(TypeDefinition::Enum {
            values: values
                .unwrap_or_default()
                .iter()
                .map(|value| normalize_identifier(value))
                .collect(),
        }),
        CreateTypeKind::Composite => Ok(TypeDefinition::Composite {
            attributes: attributes
                .unwrap_or_default()
                .iter()
                .map(|attribute| TypeAttributeDefinition {
                    name: normalize_identifier(&attribute.name),
                    type_name: normalize_type_expression(&attribute.r#type),
                })
                .collect(),
        }),
        CreateTypeKind::Domain => Ok(TypeDefinition::Domain {
            base_type: normalize_type_expression(base_type.unwrap_or_default()),
        }),
    }
}

fn build_postgres_custom_type_kind_sql(type_ref: &TableRef) -> String {
    let type_name = type_ref.name.replace('\'', "''");

    let schema_filter = match type_ref.schema.as_deref() {
        Some(schema) => format!("n.nspname = '{}'", schema.replace('\'', "''")),
        None => "n.nspname = current_schema()".to_string(),
    };

    format!(
        "SELECT CASE\n    WHEN t.typtype = 'e' THEN 'enum'\n    WHEN t.typtype = 'd' THEN 'domain'\n    WHEN t.typtype = 'c' AND c.relkind = 'c' THEN 'composite'\n    ELSE NULL\nEND AS type_kind\nFROM pg_catalog.pg_type t\nJOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\nLEFT JOIN pg_catalog.pg_class c ON c.oid = t.typrelid\nWHERE t.typname = '{}'\n  AND {}\n  AND (\n        t.typtype IN ('e', 'd')\n     OR (t.typtype = 'c' AND c.relkind = 'c')\n  )\nLIMIT 1",
        type_name, schema_filter
    )
}

fn parse_custom_type_kind_result(
    result: &dbflux_core::QueryResult,
) -> Result<Option<CreateTypeKind>, String> {
    let Some(value) = result.rows.first().and_then(|row| row.first()) else {
        return Ok(None);
    };

    match value {
        Value::Text(type_kind) => CreateTypeKind::parse(type_kind).map(Some),
        Value::Null => Ok(None),
        other => Err(format!(
            "CREATE TYPE existence check returned unsupported value: {}",
            other
        )),
    }
}

async fn lookup_postgres_custom_type_kind(
    connection: std::sync::Arc<dyn Connection>,
    type_ref: &TableRef,
) -> Result<Option<CreateTypeKind>, String> {
    let sql = build_postgres_custom_type_kind_sql(type_ref);
    let request = QueryRequest::new(sql);

    DbFluxServer::execute_connection_blocking(connection, move |connection| {
        connection
            .execute(&request)
            .map_err(|e| format!("Create type existence check error: {}", e))
    })
    .await
    .and_then(|result| parse_custom_type_kind_result(&result))
}

#[tool_router(router = ddl_router, vis = "pub")]
impl DbFluxServer {
    #[tool(description = "Create a new table with columns and constraints")]
    async fn create_table(
        &self,
        Parameters(params): Parameters<CreateTableParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let columns = params.columns.clone();
        let primary_key = params.primary_key.clone();
        let if_not_exists = params.if_not_exists.unwrap_or(true);

        self.governance
            .authorize_and_execute(
                "create_table",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result = Self::create_table_impl(
                        state,
                        &connection_id,
                        &table,
                        &columns,
                        primary_key.as_deref(),
                        if_not_exists,
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Alter a table structure (add/drop/rename columns, constraints)")]
    async fn alter_table(
        &self,
        Parameters(params): Parameters<AlterTableParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let operations = params.operations.clone();

        // Classify based on operations
        let classification = classify_alter_operations(&params.operations);

        self.governance
            .authorize_and_execute(
                "alter_table",
                Some(&params.connection_id),
                classification,
                move || async move {
                    let result = Self::alter_table_impl(state, &connection_id, &table, &operations)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Create an index on one or more columns")]
    async fn create_index(
        &self,
        Parameters(params): Parameters<CreateIndexParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let columns = params.columns.clone();
        let index_name = params.index_name.clone();
        let unique = params.unique.unwrap_or(false);
        let if_not_exists = params.if_not_exists.unwrap_or(true);

        self.governance
            .authorize_and_execute(
                "create_index",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result = Self::create_index_impl(
                        state,
                        &connection_id,
                        &table,
                        &columns,
                        index_name.as_deref(),
                        unique,
                        if_not_exists,
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Drop an index")]
    async fn drop_index(
        &self,
        Parameters(params): Parameters<DropIndexParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let index_name = params.index_name.clone();
        let if_exists = params.if_exists.unwrap_or(true);

        self.governance
            .authorize_and_execute(
                "drop_index",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result = Self::drop_index_impl(
                        state,
                        &connection_id,
                        table.as_deref(),
                        &index_name,
                        if_exists,
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Create a custom type (enum, composite, domain) - PostgreSQL only")]
    async fn create_type(
        &self,
        Parameters(params): Parameters<CreateTypeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        validate_create_type_params(&params).map_err(|e| ErrorData::invalid_params(e, None))?;

        let state = self.state.clone();
        let request = CreateTypeRequestParams {
            connection_id: params.connection_id.clone(),
            name: params.name.clone(),
            type_type: params.r#type.clone(),
            values: params.values.clone(),
            attributes: params.attributes.clone(),
            base_type: params.base_type.clone(),
            if_not_exists: params.if_not_exists.unwrap_or(true),
        };

        self.governance
            .authorize_and_execute(
                "create_type",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result = Self::create_type_impl(state, &request)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    // === DDL Operations Implementation ===

    async fn create_table_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        columns: &[crate::tools::ColumnDef],
        primary_key: Option<&[String]>,
        if_not_exists: bool,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let table_ref = TableRef::from_qualified(table);
        let dialect = connection.dialect();
        let table_quoted = table_ref.quoted_with(dialect);

        let if_not_exists_clause = if if_not_exists { "IF NOT EXISTS " } else { "" };

        // Build column definitions
        let column_defs: Vec<String> = columns
            .iter()
            .map(|col| {
                let mut def = format!("{} {}", dialect.quote_identifier(&col.name), col.r#type);

                if col.nullable == Some(false) {
                    def.push_str(" NOT NULL");
                }

                if col.primary_key == Some(true) {
                    def.push_str(" PRIMARY KEY");
                }

                if col.auto_increment == Some(true) {
                    def.push_str(" AUTOINCREMENT");
                }

                if let Some(ref default) = col.default {
                    def.push_str(&format!(
                        " DEFAULT {}",
                        json_to_sql_literal(default, dialect)
                    ));
                }

                if let Some(ref fk) = col.references {
                    let fk_table = dialect.quote_identifier(&fk.table);
                    let fk_col = dialect.quote_identifier(&fk.column);
                    def.push_str(&format!(" REFERENCES {} ({})", fk_table, fk_col));
                }

                def
            })
            .collect();

        let pk_clause = if let Some(pk_cols) = primary_key {
            let pk_quoted: Vec<String> = pk_cols
                .iter()
                .map(|c| dialect.quote_identifier(c))
                .collect();
            format!(", PRIMARY KEY ({})", pk_quoted.join(", "))
        } else {
            "".to_string()
        };

        let sql = format!(
            "CREATE TABLE {}{} ({}{})",
            if_not_exists_clause,
            table_quoted,
            column_defs.join(", "),
            pk_clause
        );

        let request = QueryRequest::new(&sql);
        Self::execute_connection_blocking(connection.clone(), move |connection| {
            connection
                .execute(&request)
                .map_err(|e| format!("Create table error: {}", e))
                .map(|_| ())
        })
        .await?;

        Ok(serde_json::json!({
            "created": true,
            "table": table,
        }))
    }

    async fn alter_table_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        operations: &[crate::tools::AlterOperation],
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();
        let code_generator = connection.code_generator();

        let table_ref = TableRef::from_qualified(table);
        let table_quoted = table_ref.quoted_with(dialect);

        let mut results = Vec::new();

        for op in operations {
            let action_upper = op.action.to_uppercase();
            let sql = match action_upper.as_str() {
                "ADD_COLUMN" | "ADD COLUMN" => {
                    if let Some(ref def) = op.definition {
                        let col_name = op.column.as_deref().unwrap_or("");
                        let col_type = def.get("type").and_then(|v| v.as_str()).unwrap_or("TEXT");
                        format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            table_quoted,
                            dialect.quote_identifier(col_name),
                            col_type
                        )
                    } else {
                        return Err("ADD_COLUMN requires definition".to_string());
                    }
                }
                "DROP_COLUMN" | "DROP COLUMN" => {
                    let col_name = op.column.as_deref().unwrap_or("");
                    format!(
                        "ALTER TABLE {} DROP COLUMN {}",
                        table_quoted,
                        dialect.quote_identifier(col_name)
                    )
                }
                "RENAME_COLUMN" | "RENAME COLUMN" => {
                    if let Some(ref def) = op.definition {
                        let old_name = def.get("old_name").and_then(|v| v.as_str()).unwrap_or("");
                        let new_name = def.get("new_name").and_then(|v| v.as_str()).unwrap_or("");
                        format!(
                            "ALTER TABLE {} RENAME COLUMN {} TO {}",
                            table_quoted,
                            dialect.quote_identifier(old_name),
                            dialect.quote_identifier(new_name)
                        )
                    } else {
                        return Err("RENAME_COLUMN requires definition".to_string());
                    }
                }
                "ALTER_COLUMN" | "ALTER COLUMN" => {
                    if let Some(ref def) = op.definition {
                        let col_name = op.column.as_deref().unwrap_or("");
                        let mut parts = Vec::new();

                        // Build ALTER COLUMN clause based on what's being changed
                        if let Some(new_type) = def.get("type").and_then(|v| v.as_str()) {
                            parts.push(format!(
                                "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                                table_quoted,
                                dialect.quote_identifier(col_name),
                                new_type
                            ));
                        }

                        if let Some(nullable) = def.get("nullable").and_then(|v| v.as_bool()) {
                            let null_clause = if nullable {
                                "DROP NOT NULL"
                            } else {
                                "SET NOT NULL"
                            };
                            parts.push(format!(
                                "ALTER TABLE {} ALTER COLUMN {} {}",
                                table_quoted,
                                dialect.quote_identifier(col_name),
                                null_clause
                            ));
                        }

                        if let Some(default_val) = def.get("default") {
                            if default_val.is_null() {
                                parts.push(format!(
                                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                                    table_quoted,
                                    dialect.quote_identifier(col_name)
                                ));
                            } else {
                                parts.push(format!(
                                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                                    table_quoted,
                                    dialect.quote_identifier(col_name),
                                    json_to_sql_literal(default_val, dialect)
                                ));
                            }
                        }

                        if parts.is_empty() {
                            return Err(
                                "ALTER_COLUMN requires at least one of: type, nullable, default"
                                    .to_string(),
                            );
                        }

                        // Execute all parts and collect results
                        for part_sql in parts {
                            let request = QueryRequest::new(&part_sql);
                            match Self::execute_connection_blocking(
                                connection.clone(),
                                move |connection| {
                                    connection
                                        .execute(&request)
                                        .map_err(|e| format!("Alter column error: {}", e))
                                        .map(|_| ())
                                },
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(e) => return Err(e),
                            }
                        }

                        // Return early since we already executed
                        results.push(serde_json::json!({"action": op.action, "success": true}));
                        continue;
                    } else {
                        return Err("ALTER_COLUMN requires definition".to_string());
                    }
                }
                "ADD_CONSTRAINT" | "ADD CONSTRAINT" => {
                    if let Some(ref def) = op.definition {
                        let constraint_name =
                            def.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let constraint_type =
                            def.get("type").and_then(|v| v.as_str()).unwrap_or("");

                        let constraint_clause = match constraint_type.to_uppercase().as_str() {
                            "CHECK" => {
                                let condition =
                                    def.get("condition").and_then(|v| v.as_str()).unwrap_or("");
                                format!("CHECK ({})", condition)
                            }
                            "UNIQUE" => {
                                let columns =
                                    def.get("columns").and_then(|v| v.as_array()).ok_or_else(
                                        || "UNIQUE constraint requires columns array".to_string(),
                                    )?;
                                let col_names: Vec<String> = columns
                                    .iter()
                                    .filter_map(|v| v.as_str())
                                    .map(|c| dialect.quote_identifier(c))
                                    .collect();
                                format!("UNIQUE ({})", col_names.join(", "))
                            }
                            "FOREIGN_KEY" | "FOREIGN KEY" => {
                                if let Some(sql) = generate_add_foreign_key_sql(
                                    code_generator,
                                    &table_ref,
                                    constraint_name,
                                    def,
                                )? {
                                    sql
                                } else {
                                    let columns = def
                                        .get("columns")
                                        .and_then(|v| v.as_array())
                                        .ok_or_else(|| {
                                            "FOREIGN KEY constraint requires columns array"
                                                .to_string()
                                        })?;
                                    let ref_table = def
                                        .get("ref_table")
                                        .and_then(|v| v.as_str())
                                        .ok_or_else(|| {
                                        "FOREIGN KEY constraint requires ref_table".to_string()
                                    })?;
                                    let ref_columns = def
                                        .get("ref_columns")
                                        .and_then(|v| v.as_array())
                                        .ok_or_else(|| {
                                            "FOREIGN KEY constraint requires ref_columns array"
                                                .to_string()
                                        })?;

                                    let col_names: Vec<String> = columns
                                        .iter()
                                        .filter_map(|v| v.as_str())
                                        .map(|c| dialect.quote_identifier(c))
                                        .collect();
                                    let ref_col_names: Vec<String> = ref_columns
                                        .iter()
                                        .filter_map(|v| v.as_str())
                                        .map(|c| dialect.quote_identifier(c))
                                        .collect();

                                    format!(
                                        "FOREIGN KEY ({}) REFERENCES {} ({})",
                                        col_names.join(", "),
                                        dialect.quote_identifier(ref_table),
                                        ref_col_names.join(", ")
                                    )
                                }
                            }
                            _ => {
                                return Err(format!(
                                    "Unsupported constraint type: {}",
                                    constraint_type
                                ));
                            }
                        };

                        format!(
                            "ALTER TABLE {} ADD CONSTRAINT {} {}",
                            table_quoted,
                            dialect.quote_identifier(constraint_name),
                            constraint_clause
                        )
                    } else {
                        return Err("ADD_CONSTRAINT requires definition".to_string());
                    }
                }
                "DROP_CONSTRAINT" | "DROP CONSTRAINT" => {
                    if let Some(ref def) = op.definition {
                        let constraint_name =
                            def.get("name").and_then(|v| v.as_str()).unwrap_or("");

                        if let Some(sql) = generate_drop_foreign_key_sql(
                            code_generator,
                            &table_ref,
                            constraint_name,
                            Some(def),
                        ) {
                            sql
                        } else {
                            format!(
                                "ALTER TABLE {} DROP CONSTRAINT {}",
                                table_quoted,
                                dialect.quote_identifier(constraint_name)
                            )
                        }
                    } else {
                        return Err("DROP_CONSTRAINT requires definition".to_string());
                    }
                }
                _ => return Err(format!("Unsupported alter operation: {}", op.action)),
            };

            let request = QueryRequest::new(&sql);
            match Self::execute_connection_blocking(connection.clone(), move |connection| {
                connection
                    .execute(&request)
                    .map_err(|e| format!("ALTER TABLE error: {}", e))
                    .map(|_| ())
            })
            .await
            {
                Ok(_) => results.push(serde_json::json!({"action": op.action, "success": true})),
                Err(e) => results.push(serde_json::json!({
                    "action": op.action,
                    "success": false,
                    "error": format!("{}", e)
                })),
            }
        }

        Ok(serde_json::json!({
            "altered": true,
            "table": table,
            "operations": results,
        }))
    }

    async fn create_index_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        columns: &[String],
        index_name: Option<&str>,
        unique: bool,
        if_not_exists: bool,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);
        let table_quoted = table_ref.quoted_with(dialect);

        let index_name = index_name
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("idx_{}_{}", table.replace('.', "_"), columns.join("_")));
        let index_quoted = dialect.quote_identifier(&index_name);

        let unique_clause = if unique { "UNIQUE " } else { "" };
        let if_not_exists_clause = if if_not_exists { "IF NOT EXISTS " } else { "" };

        let col_quoted: Vec<String> = columns
            .iter()
            .map(|c| dialect.quote_identifier(c))
            .collect();

        let sql = format!(
            "CREATE {}{}INDEX {} ON {} ({})",
            unique_clause,
            if_not_exists_clause,
            index_quoted,
            table_quoted,
            col_quoted.join(", ")
        );

        let request = QueryRequest::new(&sql);
        Self::execute_connection_blocking(connection.clone(), move |connection| {
            connection
                .execute(&request)
                .map_err(|e| format!("Create index error: {}", e))
                .map(|_| ())
        })
        .await?;

        Ok(serde_json::json!({
            "created": true,
            "index_name": index_name,
            "table": table,
        }))
    }

    async fn drop_index_impl(
        state: ServerState,
        connection_id: &str,
        table: Option<&str>,
        index_name: &str,
        if_exists: bool,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        // Build SQL using driver abstraction
        let sql = connection.build_drop_index_sql(index_name, table, if_exists);

        let request = QueryRequest::new(&sql);
        Self::execute_connection_blocking(connection.clone(), move |connection| {
            connection
                .execute(&request)
                .map_err(|e| format!("Drop index error: {}", e))
                .map(|_| ())
        })
        .await?;

        Ok(serde_json::json!({
            "dropped": true,
            "index_name": index_name,
        }))
    }

    async fn create_type_impl(
        state: ServerState,
        request: &CreateTypeRequestParams,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, &request.connection_id).await?;

        if connection.kind() != DbKind::Postgres
            || !connection
                .code_generator()
                .supports(CodeGenCapabilities::CREATE_TYPE)
        {
            return Err(CREATE_TYPE_POSTGRES_ONLY_ERROR.to_string());
        }

        let type_ref = normalize_table_ref(&request.name);
        let requested_kind = CreateTypeKind::parse(&request.type_type)?;
        let definition = build_create_type_definition(
            &request.type_type,
            request.values.as_deref(),
            request.attributes.as_deref(),
            request.base_type.as_deref(),
        )?;

        if request.if_not_exists {
            let existing_kind =
                lookup_postgres_custom_type_kind(connection.clone(), &type_ref).await?;

            if existing_kind == Some(requested_kind) {
                return Ok(serde_json::json!({
                    "created": false,
                    "type": type_ref.qualified_name(),
                    "skipped": true,
                }));
            }
        }

        let sql = connection
            .code_generator()
            .generate_create_type(&CreateTypeRequest {
                type_name: &type_ref.name,
                schema_name: type_ref.schema.as_deref(),
                definition,
            })
            .ok_or_else(|| CREATE_TYPE_POSTGRES_ONLY_ERROR.to_string())?;

        let query_request = QueryRequest::new(sql);
        match Self::execute_connection_blocking(connection.clone(), move |connection| {
            connection
                .execute(&query_request)
                .map_err(|e| format!("Create type error: {}", e))
                .map(|_| ())
        })
        .await
        {
            Ok(()) => {}
            Err(error) if request.if_not_exists && is_postgres_duplicate_type_error(&error) => {
                if lookup_postgres_custom_type_kind(connection.clone(), &type_ref)
                    .await
                    .ok()
                    == Some(Some(requested_kind))
                {
                    return Ok(serde_json::json!({
                        "created": false,
                        "type": type_ref.qualified_name(),
                        "skipped": true,
                    }));
                }

                return Err(error);
            }
            Err(error) => return Err(error),
        }

        Ok(serde_json::json!({
            "created": true,
            "type": type_ref.qualified_name(),
        }))
    }
}

// =============================================================================
// DDL Preview Parameters
// =============================================================================

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PreviewDdlParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,

    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,

    #[schemars(description = "DDL statement to preview (CREATE, ALTER, DROP, etc.)")]
    pub sql: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        AddForeignKeyRequest, CodeGenCapabilities, CodeGenerator, DropForeignKeyRequest,
    };

    struct TestForeignKeyCodeGenerator;

    impl CodeGenerator for TestForeignKeyCodeGenerator {
        fn capabilities(&self) -> CodeGenCapabilities {
            CodeGenCapabilities::FOREIGN_KEYS
        }

        fn generate_add_foreign_key(&self, request: &AddForeignKeyRequest) -> Option<String> {
            Some(format!(
                "ADD_FK:{}:{}:{}:{}:{}:{}",
                request.schema_name.unwrap_or(""),
                request.table_name,
                request.constraint_name,
                request.ref_schema.unwrap_or(""),
                request.ref_table,
                request.columns.join(",")
            ))
        }

        fn generate_drop_foreign_key(&self, request: &DropForeignKeyRequest) -> Option<String> {
            Some(format!(
                "DROP_FK:{}:{}:{}",
                request.schema_name.unwrap_or(""),
                request.table_name,
                request.constraint_name,
            ))
        }
    }

    #[test]
    fn test_validate_drop_table_success() {
        let params = DropTableParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            cascade: None,
            if_exists: None,
            confirm: "users".to_string(),
        };
        assert!(validate_drop_table_params(&params).is_ok());
    }

    #[test]
    fn test_validate_drop_table_mismatch() {
        let params = DropTableParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            cascade: None,
            if_exists: None,
            confirm: "wrong".to_string(),
        };
        let result = validate_drop_table_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_drop_database_success() {
        let params = DropDatabaseParams {
            connection_id: "test".to_string(),
            database: "mydb".to_string(),
            if_exists: None,
            confirm: "mydb".to_string(),
        };
        assert!(validate_drop_database_params(&params).is_ok());
    }

    #[test]
    fn test_validate_drop_database_mismatch() {
        let params = DropDatabaseParams {
            connection_id: "test".to_string(),
            database: "mydb".to_string(),
            if_exists: None,
            confirm: "otherdb".to_string(),
        };
        let result = validate_drop_database_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_create_type_enum_success() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "mood".to_string(),
            r#type: "enum".to_string(),
            values: Some(vec!["happy".to_string(), "sad".to_string()]),
            attributes: None,
            base_type: None,
            if_not_exists: Some(true),
        };

        assert!(validate_create_type_params(&params).is_ok());
    }

    #[test]
    fn test_validate_create_type_composite_requires_attributes() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "inventory_item".to_string(),
            r#type: "composite".to_string(),
            values: None,
            attributes: Some(Vec::new()),
            base_type: None,
            if_not_exists: Some(true),
        };

        let result = validate_create_type_params(&params);
        assert_eq!(
            result.expect_err("empty composite attributes should fail"),
            "Composite type requires at least one attribute"
        );
    }

    #[test]
    fn test_validate_create_type_domain_requires_base_type() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "email".to_string(),
            r#type: "domain".to_string(),
            values: None,
            attributes: None,
            base_type: None,
            if_not_exists: Some(true),
        };

        let result = validate_create_type_params(&params);
        assert_eq!(
            result.expect_err("domain without base_type should fail"),
            "Domain type requires base_type"
        );
    }

    #[test]
    fn test_build_create_type_definition_uses_composite_attributes() {
        let attributes = vec![TypeAttribute {
            name: "price".to_string(),
            r#type: "numeric(10,2)".to_string(),
        }];

        let definition = build_create_type_definition("composite", None, Some(&attributes), None)
            .expect("composite definition should build");

        assert_eq!(
            definition,
            TypeDefinition::Composite {
                attributes: vec![TypeAttributeDefinition {
                    name: "price".to_string(),
                    type_name: "numeric(10,2)".to_string(),
                }],
            }
        );
    }

    #[test]
    fn test_build_postgres_custom_type_kind_sql_uses_current_schema_for_unqualified_type() {
        let sql = build_postgres_custom_type_kind_sql(&TableRef::new("mood"));

        assert!(sql.contains("t.typname = 'mood'"));
        assert!(sql.contains("n.nspname = current_schema()"));
        assert!(sql.contains("t.typtype IN ('e', 'd')"));
        assert!(sql.contains("t.typtype = 'c' AND c.relkind = 'c'"));
    }

    #[test]
    fn test_validate_create_type_rejects_duplicate_enum_values_after_trimming() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "mood".to_string(),
            r#type: "enum".to_string(),
            values: Some(vec!["happy".to_string(), " happy ".to_string()]),
            attributes: None,
            base_type: None,
            if_not_exists: Some(true),
        };

        assert_eq!(
            validate_create_type_params(&params)
                .expect_err("duplicate enum values should be rejected"),
            "Enum values must be unique"
        );
    }

    #[test]
    fn test_validate_create_type_rejects_duplicate_attribute_names_after_trimming() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "inventory_item".to_string(),
            r#type: "composite".to_string(),
            values: None,
            attributes: Some(vec![
                TypeAttribute {
                    name: "price".to_string(),
                    r#type: "numeric".to_string(),
                },
                TypeAttribute {
                    name: " price ".to_string(),
                    r#type: "integer".to_string(),
                },
            ]),
            base_type: None,
            if_not_exists: Some(true),
        };

        assert_eq!(
            validate_create_type_params(&params)
                .expect_err("duplicate attribute names should be rejected"),
            "Composite attribute names must be unique"
        );
    }

    #[test]
    fn test_validate_create_type_rejects_quoted_type_name() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "\"public\".mood".to_string(),
            r#type: "enum".to_string(),
            values: Some(vec!["happy".to_string()]),
            attributes: None,
            base_type: None,
            if_not_exists: Some(true),
        };

        assert_eq!(
            validate_create_type_params(&params).expect_err("quoted type names should be rejected"),
            "Quoted PostgreSQL type names are not supported; use unquoted name or schema.name"
        );
    }

    #[test]
    fn test_validate_create_type_rejects_unsafe_domain_base_type() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "email".to_string(),
            r#type: "domain".to_string(),
            values: None,
            attributes: None,
            base_type: Some("text; DROP TABLE users;".to_string()),
            if_not_exists: Some(true),
        };

        assert_eq!(
            validate_create_type_params(&params).expect_err("unsafe base_type should be rejected"),
            "Domain base_type contains unsupported PostgreSQL type syntax"
        );
    }

    #[test]
    fn test_validate_create_type_rejects_unsafe_composite_attribute_type() {
        let params = CreateTypeParams {
            connection_id: "test".to_string(),
            name: "inventory_item".to_string(),
            r#type: "composite".to_string(),
            values: None,
            attributes: Some(vec![TypeAttribute {
                name: "supplier_id".to_string(),
                r#type: "integer); DROP TYPE mood; --".to_string(),
            }]),
            base_type: None,
            if_not_exists: Some(true),
        };

        assert_eq!(
            validate_create_type_params(&params)
                .expect_err("unsafe composite type should be rejected"),
            "Composite attribute 'supplier_id' type contains unsupported PostgreSQL type syntax"
        );
    }

    #[test]
    fn test_duplicate_type_error_detection_handles_sqlstate_and_message() {
        assert!(is_postgres_duplicate_type_error(
            "Create type error: type \"mood\" already exists"
        ));
        assert!(is_postgres_duplicate_type_error(
            "Create type error: SQLSTATE 42710 duplicate_object"
        ));
        assert!(!is_postgres_duplicate_type_error(
            "Create type error: permission denied"
        ));
    }

    #[test]
    fn test_parse_custom_type_kind_result_reads_kind_values() {
        let result = dbflux_core::QueryResult::table(
            Vec::new(),
            vec![vec![Value::Text("domain".to_string())]],
            None,
            std::time::Duration::ZERO,
        );

        assert_eq!(
            parse_custom_type_kind_result(&result).expect("kind result should parse"),
            Some(CreateTypeKind::Domain)
        );
    }

    #[test]
    fn test_build_create_type_definition_trims_strings() {
        let definition = build_create_type_definition(
            "composite",
            None,
            Some(&[TypeAttribute {
                name: " price ".to_string(),
                r#type: " numeric(10,2) ".to_string(),
            }]),
            None,
        )
        .expect("composite definition should build");

        assert_eq!(
            definition,
            TypeDefinition::Composite {
                attributes: vec![TypeAttributeDefinition {
                    name: "price".to_string(),
                    type_name: "numeric(10,2)".to_string(),
                }],
            }
        );
    }

    #[test]
    fn test_generate_add_foreign_key_sql_uses_shared_codegen_request() {
        let generator = TestForeignKeyCodeGenerator;
        let table = TableRef::from_qualified("public.orders");
        let definition = serde_json::json!({
            "type": "foreign_key",
            "columns": ["customer_id"],
            "ref_table": "crm.customers",
            "ref_columns": ["id"],
            "on_delete": "CASCADE",
        });

        let sql =
            generate_add_foreign_key_sql(&generator, &table, "fk_orders_customer", &definition)
                .unwrap();

        assert_eq!(
            sql,
            Some("ADD_FK:public:orders:fk_orders_customer:crm:customers:customer_id".to_string())
        );
    }

    #[test]
    fn test_generate_drop_foreign_key_sql_uses_shared_codegen_request() {
        let generator = TestForeignKeyCodeGenerator;
        let table = TableRef::from_qualified("public.orders");
        let definition = serde_json::json!({
            "type": "FOREIGN KEY",
        });

        let sql = generate_drop_foreign_key_sql(
            &generator,
            &table,
            "fk_orders_customer",
            Some(&definition),
        );

        assert_eq!(
            sql,
            Some("DROP_FK:public:orders:fk_orders_customer".to_string())
        );
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;
    use dbflux_policy::ExecutionClassification;

    #[test]
    fn test_add_nullable_column_is_safe() {
        let op = AlterOperation {
            action: "ADD_COLUMN".to_string(),
            column: Some("new_col".to_string()),
            definition: Some(serde_json::json!({
                "type": "VARCHAR(255)",
                "nullable": true
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::AdminSafe);
    }

    #[test]
    fn test_add_column_with_default_is_safe() {
        let op = AlterOperation {
            action: "ADD_COLUMN".to_string(),
            column: Some("new_col".to_string()),
            definition: Some(serde_json::json!({
                "type": "INTEGER",
                "nullable": false,
                "default": 0
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::AdminSafe);
    }

    #[test]
    fn test_add_not_null_no_default_is_admin() {
        let op = AlterOperation {
            action: "ADD_COLUMN".to_string(),
            column: Some("new_col".to_string()),
            definition: Some(serde_json::json!({
                "type": "INTEGER",
                "nullable": false
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::Admin);
    }

    #[test]
    fn test_drop_column_is_destructive() {
        let op = AlterOperation {
            action: "DROP_COLUMN".to_string(),
            column: Some("old_col".to_string()),
            definition: None,
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::AdminDestructive);
    }

    #[test]
    fn test_rename_column_is_safe() {
        let op = AlterOperation {
            action: "RENAME_COLUMN".to_string(),
            column: None,
            definition: Some(serde_json::json!({
                "old_name": "old_col",
                "new_name": "new_col"
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::AdminSafe);
    }

    #[test]
    fn test_alter_column_is_admin() {
        let op = AlterOperation {
            action: "ALTER_COLUMN".to_string(),
            column: Some("col".to_string()),
            definition: Some(serde_json::json!({
                "type": "BIGINT"
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::Admin);
    }

    #[test]
    fn test_add_constraint_is_admin() {
        let op = AlterOperation {
            action: "ADD_CONSTRAINT".to_string(),
            column: None,
            definition: Some(serde_json::json!({
                "name": "chk_positive",
                "type": "CHECK",
                "condition": "value > 0"
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::Admin);
    }

    #[test]
    fn test_drop_constraint_is_destructive() {
        let op = AlterOperation {
            action: "DROP_CONSTRAINT".to_string(),
            column: None,
            definition: Some(serde_json::json!({
                "name": "chk_positive"
            })),
        };

        let classification = classify_alter_operations(&[op]);
        assert_eq!(classification, ExecutionClassification::AdminDestructive);
    }

    #[test]
    fn test_mixed_operations_use_highest_classification() {
        let ops = vec![
            AlterOperation {
                action: "ADD_COLUMN".to_string(),
                column: Some("safe_col".to_string()),
                definition: Some(serde_json::json!({
                    "type": "TEXT",
                    "nullable": true
                })),
            },
            AlterOperation {
                action: "RENAME_COLUMN".to_string(),
                column: None,
                definition: Some(serde_json::json!({
                    "old_name": "old",
                    "new_name": "new"
                })),
            },
            AlterOperation {
                action: "DROP_COLUMN".to_string(),
                column: Some("obsolete_col".to_string()),
                definition: None,
            },
        ];

        let classification = classify_alter_operations(&ops);
        assert_eq!(classification, ExecutionClassification::AdminDestructive);
    }

    #[test]
    fn test_all_safe_operations_remain_safe() {
        let ops = vec![
            AlterOperation {
                action: "ADD_COLUMN".to_string(),
                column: Some("col1".to_string()),
                definition: Some(serde_json::json!({
                    "type": "TEXT",
                    "nullable": true
                })),
            },
            AlterOperation {
                action: "RENAME_COLUMN".to_string(),
                column: None,
                definition: Some(serde_json::json!({
                    "old_name": "old",
                    "new_name": "new"
                })),
            },
        ];

        let classification = classify_alter_operations(&ops);
        assert_eq!(classification, ExecutionClassification::AdminSafe);
    }
}
