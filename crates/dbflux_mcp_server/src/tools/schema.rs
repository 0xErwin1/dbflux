use std::sync::Arc;

use dbflux_core::{Connection, DataStructure, DescribeRequest, TableRef};
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;

use crate::{
    error_messages,
    helper::{IntoErrorData, *},
    server::DbFluxServer,
    state::ServerState,
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListSchemasParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,

    #[schemars(description = "Optional database filter")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListDatabasesParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListTablesParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,

    #[schemars(description = "Optional database/schema filter")]
    pub database: Option<String>,

    #[schemars(description = "Optional schema filter (for relational databases)")]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DescribeObjectParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,

    #[schemars(description = "Object name (table/view/collection)")]
    pub name: String,

    #[schemars(description = "Optional database name")]
    pub database: Option<String>,

    #[schemars(description = "Optional schema name (for relational databases)")]
    pub schema: Option<String>,
}

#[tool_router(router = schema_router, vis = "pub")]
impl DbFluxServer {
    #[tool(description = "List all databases available on the connection")]
    async fn list_databases(
        &self,
        Parameters(params): Parameters<ListDatabasesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();

        self.governance
            .authorize_and_execute(
                "list_databases",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    log::debug!("list_databases handler: before list_databases_impl");
                    let result = Self::list_databases_impl(state, &connection_id)
                        .await
                        .map_err(|e| e.into_error_data())?;
                    log::debug!("list_databases handler: impl returned, serializing response");

                    let json_str = serde_json::to_string_pretty(&result)
                        .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
                    log::debug!("list_databases handler: serialization complete");
                    Ok(CallToolResult::success(vec![Content::text(json_str)]))
                },
            )
            .await
    }

    #[tool(description = "List all schemas (namespaces) in a database")]
    async fn list_schemas(
        &self,
        Parameters(params): Parameters<ListSchemasParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let database = params.database.clone();

        self.governance
            .authorize_and_execute(
                "list_schemas",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    let result =
                        Self::list_schemas_impl(state, &connection_id, database.as_deref())
                            .await
                            .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "List all tables and views in a database")]
    async fn list_tables(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let database = params.database.clone();
        let schema = params.schema.clone();

        self.governance
            .authorize_and_execute(
                "list_tables",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    let result = Self::list_tables_impl(
                        state,
                        &connection_id,
                        database.as_deref(),
                        schema.as_deref(),
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

    #[tool(
        description = "List all collections in a database (alias for list_tables, used for document databases)"
    )]
    async fn list_collections(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.list_tables(Parameters(params)).await
    }

    #[tool(
        description = "Describe the structure of a table or collection (columns, types, constraints)"
    )]
    async fn describe_object(
        &self,
        Parameters(params): Parameters<DescribeObjectParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let name = params.name.clone();
        let database = params.database.clone();
        let schema = params.schema.clone();

        self.governance
            .authorize_and_execute(
                "describe_object",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    let result = Self::describe_object_impl(
                        state,
                        &connection_id,
                        &name,
                        database.as_deref(),
                        schema.as_deref(),
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

    async fn list_databases_impl(
        state: ServerState,
        connection_id: &str,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        let conn = connection.clone();
        let connection_id = connection_id.to_string();
        log::debug!("list_databases_impl: spawning blocking task");
        let databases = tokio::task::spawn_blocking(move || -> Result<_, String> {
            #[allow(clippy::large_enum_variant)]
            conn.list_databases().map_err(|e| {
                error_messages::schema_operation_error(
                    "list databases",
                    &connection_id,
                    None,
                    None,
                    None,
                    e,
                )
            })
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        let databases = databases?;
        log::debug!(
            "list_databases_impl: got {} databases, serializing",
            databases.len()
        );

        let items: Vec<serde_json::Value> = databases
            .iter()
            .map(|db| {
                serde_json::json!({
                    "name": db.name,
                    "is_current": db.is_current,
                })
            })
            .collect();

        log::debug!("list_databases_impl: serialization complete, returning");
        Ok(serde_json::json!({ "databases": items }))
    }

    async fn list_schemas_impl(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state, connection_id, target_db).await?
            } else {
                Self::get_or_connect(state, connection_id).await?
            }
        } else {
            Self::get_or_connect(state, connection_id).await?
        };

        let conn = connection.clone();
        let connection_id = connection_id.to_string();
        let database = database.map(|d| d.to_string());
        let snapshot = tokio::task::spawn_blocking(move || -> Result<_, String> {
            #[allow(clippy::large_enum_variant)]
            conn.schema().map_err(|e| {
                error_messages::schema_operation_error(
                    "list schemas",
                    &connection_id,
                    database.as_deref(),
                    None,
                    None,
                    e,
                )
            })
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        let snapshot = snapshot?;

        let schemas: Vec<serde_json::Value> = match &snapshot.structure {
            DataStructure::Relational(relational) => relational
                .schemas
                .iter()
                .map(|s| serde_json::json!({ "name": s.name }))
                .collect(),
            DataStructure::Document(doc) => doc
                .databases
                .iter()
                .map(|db| serde_json::json!({ "name": db.name }))
                .collect(),
            _ => vec![serde_json::json!({ "name": "default" })],
        };

        Ok(serde_json::json!({ "schemas": schemas }))
    }

    /// Handle list_tables for document databases (MongoDB, DynamoDB)
    async fn list_tables_document(
        connection: &Arc<dyn Connection>,
        database: &str,
    ) -> Result<serde_json::Value, String> {
        let conn = connection.clone();
        let db_name = database.to_string();
        #[allow(clippy::result_large_err)]
        let db_schema = tokio::task::spawn_blocking(move || conn.schema_for_database(&db_name))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
            .map_err(|e| format!("Schema error: {}", e))?;

        let collections: Vec<serde_json::Value> = db_schema
            .tables
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "schema": t.schema,
                    "kind": "Collection",
                })
            })
            .collect();

        Ok(serde_json::json!({ "tables": collections }))
    }

    async fn list_tables_impl(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
        schema: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state, connection_id, target_db).await?
            } else {
                Self::get_or_connect(state, connection_id).await?
            }
        } else {
            Self::get_or_connect(state, connection_id).await?
        };

        let category = connection.metadata().category;

        // For Document databases, we need to call schema_for_database() to get collections
        // since schema() returns empty collections for MongoDB
        if matches!(category, dbflux_core::DatabaseCategory::Document) {
            #[allow(clippy::unnecessary_lazy_evaluations)]
            let db_name = database.ok_or_else(|| {
                "Database parameter is required for document databases (MongoDB). \
                 Use: list_tables(connection_id, database=\"db_name\", ...)"
            })?;

            return Self::list_tables_document(&connection, db_name).await;
        }

        let conn = connection.clone();
        let connection_id = connection_id.to_string();
        let database = database.map(|d| d.to_string());
        let schema_str = schema.map(|s| s.to_string());
        let schema_for_closure = schema_str.clone();
        let schema_snapshot = tokio::task::spawn_blocking(move || -> Result<_, String> {
            #[allow(clippy::large_enum_variant)]
            conn.schema().map_err(|e| {
                error_messages::schema_operation_error(
                    "list tables",
                    &connection_id,
                    database.as_deref(),
                    schema_for_closure.as_deref(),
                    None,
                    e,
                )
            })
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        let schema_snapshot = schema_snapshot?;

        use dbflux_core::DataStructure;

        match category {
            dbflux_core::DatabaseCategory::Relational => {
                let relational = match schema_snapshot.structure {
                    DataStructure::Relational(r) => r,
                    _ => return Err("Expected relational schema".to_string()),
                };

                // Only apply default schema if the database supports schemas
                let supports_schemas = connection
                    .metadata()
                    .syntax
                    .as_ref()
                    .map(|s| s.supports_schemas)
                    .unwrap_or(false);

                let target_schema = if supports_schemas {
                    schema_str.unwrap_or_else(|| "public".to_string())
                } else {
                    // For databases without schema support (SQLite), use the first schema or default
                    schema_str.unwrap_or_else(|| {
                        relational
                            .schemas
                            .first()
                            .map(|s| s.name.clone())
                            .unwrap_or_default()
                    })
                };

                let schema_data = relational
                    .schemas
                    .iter()
                    .find(|s| s.name == target_schema.as_str())
                    .ok_or_else(|| format!("Schema '{}' not found", target_schema))?;

                let mut tables: Vec<serde_json::Value> = schema_data
                    .tables
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "name": t.name,
                            "schema": t.schema,
                            "kind": "Table",
                        })
                    })
                    .collect();

                let views: Vec<serde_json::Value> = schema_data
                    .views
                    .iter()
                    .map(|v| {
                        serde_json::json!({
                            "name": v.name,
                            "schema": v.schema,
                            "kind": "View",
                        })
                    })
                    .collect();

                tables.extend(views);

                Ok(serde_json::json!({ "tables": tables }))
            }
            dbflux_core::DatabaseCategory::KeyValue => {
                // For key-value stores (Redis), return keyspace info
                let kv = match schema_snapshot.structure {
                    DataStructure::KeyValue(k) => k,
                    _ => return Err("Expected key-value schema".to_string()),
                };

                let patterns: Vec<serde_json::Value> = kv
                    .keyspaces
                    .iter()
                    .map(|k| {
                        serde_json::json!({
                            "db_index": k.db_index,
                            "key_count": k.key_count,
                            "kind": "Keyspace",
                        })
                    })
                    .collect();

                Ok(serde_json::json!({ "tables": patterns }))
            }
            _ => Err(format!("Unsupported database category: {:?}", category)),
        }
    }

    async fn describe_object_impl(
        state: ServerState,
        connection_id: &str,
        name: &str,
        database: Option<&str>,
        schema: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        let table_ref = TableRef {
            schema: schema.map(str::to_string),
            name: name.to_string(),
        };

        let request = DescribeRequest::new(table_ref);
        let result = connection.describe_table(&request).map_err(|e| {
            error_messages::schema_operation_error(
                "describe object",
                connection_id,
                database,
                schema,
                Some(name),
                e,
            )
        })?;

        Ok(serialize_query_result(&result))
    }
}
