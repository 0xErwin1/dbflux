//! Write operation tools for MCP server.
//!
//! Provides type-safe parameter structs for write operations:
//! - `insert_record`: Insert one or more records into a table
//! - `update_records`: Update records matching a filter (requires WHERE clause)
//! - `upsert_record`: Insert or update on conflict

use dbflux_core::{QueryRequest, RowInsert, TableRef, Value};
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;

use crate::{
    helper::{IntoErrorData, *},
    server::DbFluxServer,
    state::ServerState,
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InsertRecordParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Records to insert (array of objects)")]
    pub records: Vec<serde_json::Value>,

    #[schemars(description = "Columns to return from inserted records")]
    pub returning: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateRecordsParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Filter conditions (REQUIRED - cannot be empty)")]
    pub r#where: serde_json::Value,

    #[schemars(description = "Fields to update")]
    pub set: serde_json::Value,

    #[schemars(description = "Columns to return from updated records")]
    pub returning: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpsertRecordParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Record to insert or update")]
    pub record: serde_json::Value,

    #[schemars(description = "Columns that define uniqueness for conflict detection")]
    pub conflict_columns: Vec<String>,

    #[schemars(description = "Fields to update on conflict (default: the record itself)")]
    pub update_on_conflict: Option<serde_json::Value>,
}

impl UpdateRecordsParams {
    pub const UPDATE_WHERE_REQUIRED_ERROR: &str =
        "UPDATE requires a WHERE clause to prevent accidental full table updates";

    pub fn validate_where_clause(&self) -> Result<(), String> {
        let is_empty = match &self.r#where {
            serde_json::Value::Null => true,
            serde_json::Value::Object(map) => map.is_empty(),
            serde_json::Value::Array(arr) => arr.is_empty(),
            serde_json::Value::String(s) => s.trim().is_empty(),
            _ => false,
        };

        if is_empty {
            return Err(Self::UPDATE_WHERE_REQUIRED_ERROR.to_string());
        }

        Ok(())
    }
}

#[tool_router(router = write_router, vis = "pub")]
impl DbFluxServer {
    #[tool(description = "Insert one or more records into a table")]
    async fn insert_record(
        &self,
        Parameters(params): Parameters<InsertRecordParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let records = params.records.clone();
        let returning = params.returning.clone();

        self.governance
            .authorize_and_execute(
                "insert_record",
                Some(&params.connection_id),
                ExecutionClassification::Write,
                move || async move {
                    let result = Self::insert_record_impl(
                        state,
                        &connection_id,
                        &table,
                        &records,
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

    #[tool(description = "Update records matching a filter (requires WHERE clause)")]
    async fn update_records(
        &self,
        Parameters(params): Parameters<UpdateRecordsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        // Validate WHERE clause is present and not empty
        params
            .validate_where_clause()
            .map_err(|e| ErrorData::invalid_params(e, None))?;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let filter = params.r#where.clone();
        let set = params.set.clone();
        let returning = params.returning.clone();

        self.governance
            .authorize_and_execute(
                "update_records",
                Some(&params.connection_id),
                ExecutionClassification::Write,
                move || async move {
                    let result = Self::update_records_impl(
                        state,
                        &connection_id,
                        &table,
                        &filter,
                        &set,
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

    #[tool(description = "Insert or update a record based on conflict columns (upsert)")]
    async fn upsert_record(
        &self,
        Parameters(params): Parameters<UpsertRecordParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let record = params.record.clone();
        let conflict_columns = params.conflict_columns.clone();
        let update_on_conflict = params.update_on_conflict.clone();

        self.governance
            .authorize_and_execute(
                "upsert_record",
                Some(&params.connection_id),
                ExecutionClassification::Write,
                move || async move {
                    let result = Self::upsert_record_impl(
                        state,
                        &connection_id,
                        &table,
                        &record,
                        &conflict_columns,
                        update_on_conflict.as_ref(),
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

    async fn insert_record_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        records: &[serde_json::Value],
        returning: Option<&[String]>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let _dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);

        let mut inserted_count = 0;
        let mut returned_records = Vec::new();

        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| "Each record must be a JSON object".to_string())?;

            let columns: Vec<String> = obj.keys().cloned().collect();
            let values: Vec<Value> = obj.values().map(|v| json_to_db_value(v.clone())).collect();

            let row_insert = RowInsert::new(
                table_ref.name.clone(),
                table_ref.schema.clone(),
                columns,
                values,
            );

            let result = connection
                .insert_row(&row_insert)
                .map_err(|e| format!("Insert error: {}", e))?;

            inserted_count += result.affected_rows;

            // Build return record if RETURNING requested
            if let Some(return_cols) = returning
                && let Some(ref row) = result.returning_row
            {
                let mut return_obj = serde_json::Map::new();
                for (col, val) in return_cols.iter().zip(row.iter()) {
                    return_obj.insert(col.clone(), value_to_json(val));
                }
                returned_records.push(serde_json::Value::Object(return_obj));
            }
        }

        Ok(serde_json::json!({
            "inserted": inserted_count,
            "records": returned_records,
        }))
    }

    async fn update_records_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        filter: &serde_json::Value,
        set: &serde_json::Value,
        _returning: Option<&[String]>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        let table_ref = TableRef::from_qualified(table);
        let table_quoted = table_ref.quoted_with(dialect);

        // Build SET clause
        let set_obj = set
            .as_object()
            .ok_or_else(|| "SET must be a JSON object".to_string())?;

        let set_clauses: Vec<String> = set_obj
            .iter()
            .map(|(col, val)| {
                let db_val = json_to_db_value(val.clone());
                format!(
                    "{} = {}",
                    dialect.quote_identifier(col),
                    db_value_to_sql(&db_val, dialect)
                )
            })
            .collect();

        // Build WHERE clause
        let where_clause = json_filter_to_sql(filter, dialect)?;

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            table_quoted,
            set_clauses.join(", "),
            where_clause
        );

        let request = QueryRequest::new(&sql);
        let result = connection
            .execute(&request)
            .map_err(|e| format!("Update error: {}", e))?;

        Ok(serde_json::json!({
            "updated": result.rows.len() as u64,
        }))
    }

    async fn upsert_record_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        record: &serde_json::Value,
        conflict_columns: &[String],
        update_on_conflict: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state.clone(), connection_id).await?;

        let table_ref = TableRef::from_qualified(table);

        let obj = record
            .as_object()
            .ok_or_else(|| "Record must be a JSON object".to_string())?;

        let columns: Vec<String> = obj.keys().cloned().collect();
        let values: Vec<Value> = obj.values().map(|v| json_to_db_value(v.clone())).collect();

        // For PostgreSQL, try to use ON CONFLICT
        // For other databases, this is a simplified implementation
        // that does INSERT and handles the error

        let row_insert = RowInsert::new(
            table_ref.name.clone(),
            table_ref.schema.clone(),
            columns.clone(),
            values.clone(),
        );

        match connection.insert_row(&row_insert) {
            Ok(result) => Ok(serde_json::json!({
                "upserted": result.affected_rows,
                "action": "insert",
            })),
            Err(_) => {
                // Try to update existing record
                // Build filter from conflict columns
                let filter_map: serde_json::Map<String, serde_json::Value> = conflict_columns
                    .iter()
                    .filter_map(|col| obj.get(col).map(|val| (col.clone(), val.clone())))
                    .collect();

                let filter = serde_json::Value::Object(filter_map);

                let update = update_on_conflict.unwrap_or(record);

                // Remove conflict columns from update if using record itself
                let update_obj = if update_on_conflict.is_some() {
                    update.clone()
                } else {
                    let mut filtered = update.as_object().cloned().unwrap_or_default();
                    for col in conflict_columns {
                        filtered.remove(col);
                    }
                    serde_json::Value::Object(filtered)
                };

                Self::update_records_impl(
                    state.clone(),
                    connection_id,
                    table,
                    &filter,
                    &update_obj,
                    None,
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_params_validates_empty_where() {
        let params = UpdateRecordsParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            r#where: serde_json::Value::Null,
            set: serde_json::json!({"name": "test"}),
            returning: None,
        };

        assert!(params.validate_where_clause().is_err());
        assert_eq!(
            params.validate_where_clause().unwrap_err(),
            UpdateRecordsParams::UPDATE_WHERE_REQUIRED_ERROR
        );
    }

    #[test]
    fn test_update_params_validates_empty_object() {
        let params = UpdateRecordsParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            r#where: serde_json::json!({}),
            set: serde_json::json!({"name": "test"}),
            returning: None,
        };

        assert!(params.validate_where_clause().is_err());
    }

    #[test]
    fn test_update_params_accepts_valid_where() {
        let params = UpdateRecordsParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            r#where: serde_json::json!({"id": 1}),
            set: serde_json::json!({"name": "test"}),
            returning: None,
        };

        assert!(params.validate_where_clause().is_ok());
    }
}
