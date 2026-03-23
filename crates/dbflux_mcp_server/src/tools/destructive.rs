//! Destructive operation tools (DELETE, TRUNCATE).
//!
//! These tools require special validation and authorization due to their
//! destructive nature. All operations are classified as `Destructive` and
//! may require human approval.

use crate::{
    DbFluxServer,
    helper::{IntoErrorData, *},
    state::ServerState,
    tools::{DropDatabaseParams, DropTableParams},
};
use dbflux_core::{QueryRequest, TableRef};
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool,
};
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteRecordsParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Filter conditions (REQUIRED - cannot be empty)")]
    pub r#where: serde_json::Value,

    #[schemars(description = "Columns to return from deleted records")]
    pub returning: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TruncateTableParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table name to truncate")]
    pub table: String,

    #[schemars(description = "Confirmation string - must match table name exactly")]
    pub confirm: String,
}

pub const DELETE_WHERE_REQUIRED_ERROR: &str =
    "DELETE requires a WHERE clause to prevent accidental full table deletion";

pub const TRUNCATE_CONFIRMATION_ERROR: &str = "Confirmation string must match table name exactly";

pub fn validate_delete_params(params: &DeleteRecordsParams) -> Result<(), String> {
    if params.r#where.is_null()
        || (params.r#where.is_object() && params.r#where.as_object().is_none_or(|o| o.is_empty()))
        || (params.r#where.is_array() && params.r#where.as_array().is_none_or(|a| a.is_empty()))
        || (params.r#where.is_string()
            && params.r#where.as_str().is_none_or(|s| s.trim().is_empty()))
    {
        return Err(DELETE_WHERE_REQUIRED_ERROR.to_string());
    }
    Ok(())
}

pub fn validate_truncate_params(params: &TruncateTableParams) -> Result<(), String> {
    if params.confirm != params.table {
        return Err(TRUNCATE_CONFIRMATION_ERROR.to_string());
    }
    Ok(())
}

impl DbFluxServer {
    #[tool(description = "Delete records matching a filter (requires WHERE clause)")]
    async fn delete_records(
        &self,
        Parameters(params): Parameters<DeleteRecordsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use crate::tools::validate_delete_params;
        use dbflux_policy::ExecutionClassification;

        // Validate WHERE clause is present and not empty
        validate_delete_params(&params).map_err(|e| ErrorData::invalid_params(e, None))?;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let filter = params.r#where.clone();
        let returning = params.returning.clone();

        self.governance
            .authorize_and_execute(
                "delete_records",
                Some(&params.connection_id),
                ExecutionClassification::Destructive,
                move || async move {
                    let result = Self::delete_records_impl(
                        state,
                        &connection_id,
                        &table,
                        &filter,
                        returning.as_deref(),
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

    #[tool(description = "Truncate a table (remove all records, requires confirmation)")]
    async fn truncate_table(
        &self,
        Parameters(params): Parameters<TruncateTableParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use crate::tools::validate_truncate_params;
        use dbflux_policy::ExecutionClassification;

        // Validate confirmation matches table name
        validate_truncate_params(&params).map_err(|e| ErrorData::invalid_params(e, None))?;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();

        self.governance
            .authorize_and_execute(
                "truncate_table",
                Some(&params.connection_id),
                ExecutionClassification::Destructive,
                move || async move {
                    let result = Self::truncate_table_impl(state, &connection_id, &table)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Drop a table (requires confirmation matching table name)")]
    async fn drop_table(
        &self,
        Parameters(params): Parameters<DropTableParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use crate::tools::validate_drop_table_params;
        use dbflux_policy::ExecutionClassification;

        // Validate confirmation matches table name
        validate_drop_table_params(&params)?;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let cascade = params.cascade.unwrap_or(false);
        let if_exists = params.if_exists.unwrap_or(true);

        self.governance
            .authorize_and_execute(
                "drop_table",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result =
                        Self::drop_table_impl(state, &connection_id, &table, cascade, if_exists)
                            .await
                            .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Drop a database (requires confirmation matching database name)")]
    async fn drop_database(
        &self,
        Parameters(params): Parameters<DropDatabaseParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use crate::tools::validate_drop_database_params;
        use dbflux_policy::ExecutionClassification;

        // Validate confirmation matches database name
        validate_drop_database_params(&params)?;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let database = params.database.clone();
        let if_exists = params.if_exists.unwrap_or(true);

        self.governance
            .authorize_and_execute(
                "drop_database",
                Some(&params.connection_id),
                ExecutionClassification::Admin,
                move || async move {
                    let result =
                        Self::drop_database_impl(state, &connection_id, &database, if_exists)
                            .await
                            .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    async fn delete_records_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        filter: &serde_json::Value,
        _returning: Option<&[String]>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);
        let table_quoted = table_ref.quoted_with(dialect);

        // Build WHERE clause
        let where_clause = json_filter_to_sql(filter, dialect)?;

        let sql = format!("DELETE FROM {} WHERE {}", table_quoted, where_clause);

        let request = QueryRequest::new(&sql);
        let result = connection
            .execute(&request)
            .map_err(|e| format!("Delete error: {}", e))?;

        Ok(serde_json::json!({
            "deleted": result.rows.len() as u64,
        }))
    }

    async fn truncate_table_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);
        let table_quoted = table_ref.quoted_with(dialect);

        let sql = format!("TRUNCATE TABLE {}", table_quoted);

        let request = QueryRequest::new(&sql);
        connection
            .execute(&request)
            .map_err(|e| format!("Truncate error: {}", e))?;

        Ok(serde_json::json!({
            "truncated": true,
            "table": table,
        }))
    }

    async fn drop_table_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        cascade: bool,
        if_exists: bool,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);
        let table_quoted = table_ref.quoted_with(dialect);

        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let cascade_clause = if cascade { " CASCADE" } else { "" };

        let sql = format!(
            "DROP TABLE {}{}{}",
            if_exists_clause, table_quoted, cascade_clause
        );

        let request = QueryRequest::new(&sql);
        connection
            .execute(&request)
            .map_err(|e| format!("Drop table error: {}", e))?;

        Ok(serde_json::json!({
            "dropped": true,
            "table": table,
        }))
    }

    async fn drop_database_impl(
        state: ServerState,
        connection_id: &str,
        database: &str,
        if_exists: bool,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let db_quoted = dialect.quote_identifier(database);

        let sql = format!("DROP DATABASE {}{}", if_exists_clause, db_quoted);

        let request = QueryRequest::new(&sql);
        connection
            .execute(&request)
            .map_err(|e| format!("Drop database error: {}", e))?;

        Ok(serde_json::json!({
            "dropped": true,
            "database": database,
        }))
    }
}
