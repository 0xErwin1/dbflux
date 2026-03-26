//! Write operation tools for MCP server.
//!
//! Provides type-safe parameter structs for write operations:
//! - `insert_record`: Insert one or more records into a table
//! - `update_records`: Update records matching a filter (requires WHERE clause)
//! - `upsert_record`: Insert or update on conflict

use std::collections::BTreeMap;

use dbflux_core::{
    DatabaseCategory, DocumentFilter, DocumentInsert, DocumentUpdate, MutationRequest, RowInsert,
    SemanticRequest, SqlUpdateRequest, SqlUpsertRequest, TableRef, Value,
    parse_semantic_filter_json,
};
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
    pub records: Vec<BTreeMap<String, serde_json::Value>>,

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
        records: &[BTreeMap<String, serde_json::Value>],
        returning: Option<&[String]>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        if connection.metadata().category == DatabaseCategory::Document {
            let documents = records
                .iter()
                .map(|record| {
                    serde_json::to_value(record)
                        .map_err(|e| format!("Failed to serialize document payload: {}", e))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let result = connection
                .insert_document(&DocumentInsert::many(table.to_string(), documents))
                .map_err(|e| format!("Insert error: {}", e))?;

            return Ok(serde_json::json!({
                "inserted": result.affected_rows,
                "records": [],
            }));
        }

        let table_ref = TableRef::from_qualified(table);

        let mut inserted_count = 0;
        let mut returned_records = Vec::new();

        for record in records {
            let columns: Vec<String> = record.keys().cloned().collect();
            let values: Vec<Value> = record
                .values()
                .map(|v| json_to_db_value(v.clone()))
                .collect();

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
        returning: Option<&[String]>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        if connection.metadata().category == DatabaseCategory::Document {
            let update = Self::build_document_update(table, filter, set)?;
            let result = connection
                .update_document(&update)
                .map_err(|e| format!("Update error: {}", e))?;

            return Ok(serde_json::json!({
                "updated": result.affected_rows,
            }));
        }

        let mutation = Self::build_relational_update_mutation(table, filter, set, returning)?;
        let result = connection
            .execute_semantic_request(&SemanticRequest::Mutation(mutation))
            .map_err(|e| format!("Update error: {}", e))?;

        Ok(serialize_mutation_result(
            &result,
            "updated",
            returning.is_some(),
        ))
    }

    fn build_relational_update_mutation(
        table: &str,
        filter: &serde_json::Value,
        set: &serde_json::Value,
        returning: Option<&[String]>,
    ) -> Result<MutationRequest, String> {
        let semantic_filter = parse_semantic_filter_json(filter)?
            .ok_or_else(|| UpdateRecordsParams::UPDATE_WHERE_REQUIRED_ERROR.to_string())?;

        let set_obj = set
            .as_object()
            .ok_or_else(|| "SET must be a JSON object".to_string())?;

        let changes: Vec<(String, Value)> = set_obj
            .iter()
            .map(|(col, val)| (col.clone(), json_to_db_value(val.clone())))
            .collect();

        let table_ref = TableRef::from_qualified(table);

        let mut update =
            SqlUpdateRequest::new(table_ref.name, table_ref.schema, semantic_filter, changes);

        if let Some(returning) = returning
            && !returning.is_empty()
        {
            update = update.with_returning(returning.to_vec());
        }

        Ok(MutationRequest::sql_update_many(update))
    }

    fn build_document_update(
        table: &str,
        filter: &serde_json::Value,
        set: &serde_json::Value,
    ) -> Result<DocumentUpdate, String> {
        let set_obj = set
            .as_object()
            .ok_or_else(|| "SET must be a JSON object".to_string())?;

        Ok(DocumentUpdate::new(
            table.to_string(),
            DocumentFilter::new(filter.clone()),
            serde_json::json!({ "$set": set_obj.clone() }),
        )
        .many())
    }

    async fn upsert_record_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        record: &serde_json::Value,
        conflict_columns: &[String],
        update_on_conflict: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        let supports_upsert = connection
            .metadata()
            .mutation
            .as_ref()
            .is_some_and(|capabilities| capabilities.supports_upsert);

        if !supports_upsert {
            return Err(format!(
                "Upsert is not supported by the {} driver",
                connection.metadata().display_name
            ));
        }

        let mutation = match connection.metadata().category {
            DatabaseCategory::Document => Self::build_document_upsert_mutation(
                table,
                record,
                conflict_columns,
                update_on_conflict,
            )?,
            DatabaseCategory::Relational => Self::build_relational_upsert_mutation(
                table,
                record,
                conflict_columns,
                update_on_conflict,
            )?,
            _ => {
                return Err(format!(
                    "Upsert is not supported for {:?} databases",
                    connection.metadata().category
                ));
            }
        };

        let result = connection
            .execute_semantic_request(&SemanticRequest::Mutation(mutation))
            .map_err(|e| format!("Upsert error: {}", e))?;

        Ok(serialize_mutation_result(&result, "upserted", false))
    }

    fn build_relational_upsert_mutation(
        table: &str,
        record: &serde_json::Value,
        conflict_columns: &[String],
        update_on_conflict: Option<&serde_json::Value>,
    ) -> Result<MutationRequest, String> {
        let obj = record
            .as_object()
            .ok_or_else(|| "Record must be a JSON object".to_string())?;

        if conflict_columns.is_empty() {
            return Err("conflict_columns must not be empty".to_string());
        }

        for column in conflict_columns {
            if !obj.contains_key(column) {
                return Err(format!(
                    "conflict column '{}' must be present in record",
                    column
                ));
            }
        }

        let columns: Vec<String> = obj.keys().cloned().collect();
        let values: Vec<Value> = obj
            .values()
            .map(|value| json_to_db_value(value.clone()))
            .collect();

        let update_assignments =
            Self::parse_upsert_assignments(obj, conflict_columns, update_on_conflict)?;

        let table_ref = TableRef::from_qualified(table);

        Ok(MutationRequest::sql_upsert(SqlUpsertRequest::new(
            table_ref.name,
            table_ref.schema,
            columns,
            values,
            conflict_columns.to_vec(),
            update_assignments,
        )))
    }

    fn build_document_upsert_mutation(
        table: &str,
        record: &serde_json::Value,
        conflict_columns: &[String],
        update_on_conflict: Option<&serde_json::Value>,
    ) -> Result<MutationRequest, String> {
        let obj = record
            .as_object()
            .ok_or_else(|| "Record must be a JSON object".to_string())?;

        if conflict_columns.is_empty() {
            return Err("conflict_columns must not be empty".to_string());
        }

        let mut filter = serde_json::Map::new();
        for column in conflict_columns {
            let value = obj
                .get(column)
                .ok_or_else(|| format!("conflict column '{}' must be present in record", column))?;
            filter.insert(column.clone(), value.clone());
        }

        let update_assignments =
            Self::parse_upsert_assignment_json(obj, conflict_columns, update_on_conflict)?;

        let mut update_doc = serde_json::Map::new();
        if !update_assignments.is_empty() {
            update_doc.insert(
                "$set".to_string(),
                serde_json::Value::Object(update_assignments),
            );
        }
        update_doc.insert(
            "$setOnInsert".to_string(),
            serde_json::Value::Object(obj.clone()),
        );

        Ok(MutationRequest::document_update(
            DocumentUpdate::new(
                table.to_string(),
                DocumentFilter::new(serde_json::Value::Object(filter)),
                serde_json::Value::Object(update_doc),
            )
            .upsert(),
        ))
    }

    fn parse_upsert_assignments(
        record: &serde_json::Map<String, serde_json::Value>,
        conflict_columns: &[String],
        update_on_conflict: Option<&serde_json::Value>,
    ) -> Result<Vec<(String, Value)>, String> {
        let assignment_json =
            Self::parse_upsert_assignment_json(record, conflict_columns, update_on_conflict)?;

        Ok(assignment_json
            .into_iter()
            .map(|(column, value)| (column, json_to_db_value(value)))
            .collect())
    }

    fn parse_upsert_assignment_json(
        record: &serde_json::Map<String, serde_json::Value>,
        conflict_columns: &[String],
        update_on_conflict: Option<&serde_json::Value>,
    ) -> Result<serde_json::Map<String, serde_json::Value>, String> {
        if let Some(update) = update_on_conflict {
            let update_obj = update
                .as_object()
                .ok_or_else(|| "update_on_conflict must be a JSON object".to_string())?;

            return Ok(update_obj.clone());
        }

        Ok(record
            .iter()
            .filter(|(column, _)| !conflict_columns.contains(column))
            .map(|(column, value)| (column.clone(), value.clone()))
            .collect())
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

    #[test]
    fn test_build_document_update_wraps_set_fields() {
        let update = DbFluxServer::build_document_update(
            "users",
            &serde_json::json!({"status": "active"}),
            &serde_json::json!({"name": "Ada", "visits": 3}),
        )
        .expect("document update should build");

        assert_eq!(update.collection, "users");
        assert_eq!(
            update.filter.filter,
            serde_json::json!({"status": "active"})
        );
        assert_eq!(
            update.update,
            serde_json::json!({"$set": {"name": "Ada", "visits": 3}})
        );
        assert!(update.many);
        assert!(!update.upsert);
    }

    #[test]
    fn test_build_document_update_rejects_non_object_set() {
        let error = DbFluxServer::build_document_update(
            "users",
            &serde_json::json!({"status": "active"}),
            &serde_json::json!("invalid"),
        )
        .expect_err("non-object set should fail");

        assert_eq!(error, "SET must be a JSON object");
    }

    #[test]
    fn test_build_relational_update_mutation_uses_semantic_filter_and_returning() {
        let mutation = DbFluxServer::build_relational_update_mutation(
            "public.users",
            &serde_json::json!({"status": "active"}),
            &serde_json::json!({"archived": true}),
            Some(&["id".to_string(), "archived".to_string()]),
        )
        .expect("relational update mutation should build");

        let MutationRequest::SqlUpdateMany(update) = mutation else {
            panic!("expected a sql update-many mutation");
        };

        assert_eq!(update.table, "users");
        assert_eq!(update.schema.as_deref(), Some("public"));
        assert_eq!(update.changes.len(), 1);
        assert_eq!(
            update.returning.as_deref(),
            Some(&["id".to_string(), "archived".to_string()][..])
        );
    }

    #[test]
    fn test_build_relational_upsert_mutation_preserves_custom_update_values() {
        let mutation = DbFluxServer::build_relational_upsert_mutation(
            "public.users",
            &serde_json::json!({"id": 1, "name": "Ada", "visits": 3}),
            &["id".to_string()],
            Some(&serde_json::json!({"name": "Grace", "visits": 4})),
        )
        .expect("relational upsert mutation should build");

        let MutationRequest::SqlUpsert(upsert) = mutation else {
            panic!("expected a sql upsert mutation");
        };

        assert_eq!(upsert.table, "users");
        assert_eq!(upsert.schema.as_deref(), Some("public"));
        assert_eq!(upsert.conflict_columns, vec!["id".to_string()]);
        assert_eq!(upsert.update_assignments.len(), 2);
        assert!(
            upsert
                .update_assignments
                .contains(&("name".to_string(), Value::Text("Grace".to_string())))
        );
        assert!(
            upsert
                .update_assignments
                .contains(&("visits".to_string(), Value::Int(4)))
        );
    }

    #[test]
    fn test_build_document_upsert_mutation_uses_set_and_set_on_insert() {
        let mutation = DbFluxServer::build_document_upsert_mutation(
            "users",
            &serde_json::json!({"email": "ada@example.com", "name": "Ada", "visits": 3}),
            &["email".to_string()],
            Some(&serde_json::json!({"name": "Grace"})),
        )
        .expect("document upsert mutation should build");

        let MutationRequest::DocumentUpdate(update) = mutation else {
            panic!("expected a document update mutation");
        };

        assert!(update.upsert);
        assert_eq!(
            update.filter.filter,
            serde_json::json!({"email": "ada@example.com"})
        );
        assert_eq!(
            update.update,
            serde_json::json!({
                "$set": {"name": "Grace"},
                "$setOnInsert": {
                    "email": "ada@example.com",
                    "name": "Ada",
                    "visits": 3
                }
            })
        );
    }
}
