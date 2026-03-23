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
//! All operations are classified as Admin level.

use crate::{
    DbFluxServer,
    helper::{IntoErrorData, *},
    state::ServerState,
};
use dbflux_core::{QueryRequest, TableRef};
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData},
    schemars::JsonSchema,
    tool,
};
use serde::Deserialize;

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

const DROP_TABLE_CONFIRMATION_ERROR: &str = "Confirmation string must match table name exactly";
const DROP_DATABASE_CONFIRMATION_ERROR: &str =
    "Confirmation string must match database name exactly";

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
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let operations = params.operations.clone();

        self.governance
            .authorize_and_execute(
                "alter_table",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
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

    #[tool(description = "Create a custom type (enum, composite) - PostgreSQL only")]
    async fn create_type(
        &self,
        Parameters(params): Parameters<CreateTypeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let name = params.name.clone();
        let type_type = params.r#type.clone();
        let values = params.values.clone();
        let attributes = params.attributes.clone();
        let if_not_exists = params.if_not_exists.unwrap_or(true);

        self.governance
            .authorize_and_execute(
                "create_type",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result = Self::create_type_impl(
                        state,
                        &connection_id,
                        &name,
                        &type_type,
                        values.as_deref(),
                        attributes.as_deref(),
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
        let dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);
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
        connection
            .execute(&request)
            .map_err(|e| format!("Create table error: {}", e))?;

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
                            let null_clause = if nullable { "DROP NOT NULL" } else { "SET NOT NULL" };
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
                            return Err("ALTER_COLUMN requires at least one of: type, nullable, default".to_string());
                        }
                        
                        // Execute all parts and collect results
                        for part_sql in parts {
                            let request = QueryRequest::new(&part_sql);
                            match connection.execute(&request) {
                                Ok(_) => {},
                                Err(e) => return Err(format!("Alter column error: {}", e)),
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
                        let constraint_name = def.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let constraint_type = def.get("type").and_then(|v| v.as_str()).unwrap_or("");
                        
                        let constraint_clause = match constraint_type.to_uppercase().as_str() {
                            "CHECK" => {
                                let condition = def.get("condition").and_then(|v| v.as_str()).unwrap_or("");
                                format!("CHECK ({})", condition)
                            }
                            "UNIQUE" => {
                                let columns = def.get("columns").and_then(|v| v.as_array())
                                    .ok_or_else(|| "UNIQUE constraint requires columns array".to_string())?;
                                let col_names: Vec<String> = columns.iter()
                                    .filter_map(|v| v.as_str())
                                    .map(|c| dialect.quote_identifier(c))
                                    .collect();
                                format!("UNIQUE ({})", col_names.join(", "))
                            }
                            "FOREIGN_KEY" | "FOREIGN KEY" => {
                                let columns = def.get("columns").and_then(|v| v.as_array())
                                    .ok_or_else(|| "FOREIGN KEY constraint requires columns array".to_string())?;
                                let ref_table = def.get("ref_table").and_then(|v| v.as_str())
                                    .ok_or_else(|| "FOREIGN KEY constraint requires ref_table".to_string())?;
                                let ref_columns = def.get("ref_columns").and_then(|v| v.as_array())
                                    .ok_or_else(|| "FOREIGN KEY constraint requires ref_columns array".to_string())?;
                                
                                let col_names: Vec<String> = columns.iter()
                                    .filter_map(|v| v.as_str())
                                    .map(|c| dialect.quote_identifier(c))
                                    .collect();
                                let ref_col_names: Vec<String> = ref_columns.iter()
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
                            _ => return Err(format!("Unsupported constraint type: {}", constraint_type)),
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
                        let constraint_name = def.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        format!(
                            "ALTER TABLE {} DROP CONSTRAINT {}",
                            table_quoted,
                            dialect.quote_identifier(constraint_name)
                        )
                    } else {
                        return Err("DROP_CONSTRAINT requires definition".to_string());
                    }
                }
                _ => return Err(format!("Unsupported alter operation: {}", op.action)),
            };

            let request = QueryRequest::new(&sql);
            match connection.execute(&request) {
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
        connection
            .execute(&request)
            .map_err(|e| format!("Create index error: {}", e))?;

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
        let dialect = connection.dialect();

        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let index_quoted = dialect.quote_identifier(index_name);

        let sql = if let Some(tbl) = table {
            let table_quoted = dialect.quote_identifier(tbl);
            format!("DROP INDEX {} ON {}", index_quoted, table_quoted)
        } else {
            format!("DROP INDEX {}{}", if_exists_clause, index_quoted)
        };

        let request = QueryRequest::new(&sql);
        connection
            .execute(&request)
            .map_err(|e| format!("Drop index error: {}", e))?;

        Ok(serde_json::json!({
            "dropped": true,
            "index_name": index_name,
        }))
    }

    async fn create_type_impl(
        _state: ServerState,
        _connection_id: &str,
        _name: &str,
        _type_type: &str,
        _values: Option<&[String]>,
        _attributes: Option<&[crate::tools::TypeAttribute]>,
        _if_not_exists: bool,
    ) -> Result<serde_json::Value, String> {
        // This is PostgreSQL-specific and requires special handling
        // For now, return a not supported response
        Err(
            "CREATE TYPE is database-specific and not yet fully implemented."
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
