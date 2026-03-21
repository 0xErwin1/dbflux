//! MCP server implementation using rmcp SDK.

use rmcp::{
    tool, tool_router, tool_handler,
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars::JsonSchema,
};
use serde::Deserialize;
use std::sync::Arc;

use dbflux_core::{Connection, DataStructure, DescribeRequest, QueryResult, TableRef, Value};

use crate::state::ServerState;
use crate::governance::GovernanceMiddleware;
use crate::error_messages;

/// Helper trait to convert String errors to ErrorData
trait IntoErrorData {
    fn into_error_data(self) -> ErrorData;
}

impl IntoErrorData for String {
    fn into_error_data(self) -> ErrorData {
        ErrorData::internal_error(self, None)
    }
}

/// Main DBFlux MCP Server
#[derive(Clone)]
pub struct DbFluxServer {
    state: ServerState,
    governance: GovernanceMiddleware,
    tool_router: ToolRouter<DbFluxServer>,
}

// ===== Parameter Schemas =====

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ConnectParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExecuteQueryParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
    
    #[schemars(description = "SQL query or database command to execute")]
    pub sql: String,
    
    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
    
    #[schemars(description = "Maximum number of rows to return")]
    pub limit: Option<u32>,
    
    #[schemars(description = "Number of rows to skip")]
    pub offset: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExplainQueryParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
    
    #[schemars(description = "SQL query to explain (optional, requires table if not provided)")]
    pub sql: Option<String>,
    
    #[schemars(description = "Table name to explain (optional, requires sql if not provided)")]
    pub table: Option<String>,
    
    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PreviewMutationParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
    
    #[schemars(description = "SQL mutation query to preview (INSERT, UPDATE, DELETE, etc.)")]
    pub sql: String,
    
    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListDatabasesParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListSchemasParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
    
    #[schemars(description = "Optional database filter")]
    pub database: Option<String>,
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

// ===== Tool Router Implementation =====

#[tool_router]
impl DbFluxServer {
    pub fn new(state: ServerState) -> Self {
        let governance = GovernanceMiddleware::new(state.clone());
        Self {
            state,
            governance,
            tool_router: Self::tool_router(),
        }
    }

    // === Connection Management Tools ===
    
    #[tool(description = "List all available database connections configured in DBFlux")]
    async fn list_connections(&self) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        let state = self.governance.state.clone();
        self.governance.authorize_and_execute(
            "list_connections",
            None,
            ExecutionClassification::Metadata,
            move || async move {
                let pm = state.profile_manager.read().await;
                let runtime = state.runtime.read().await;
                let client_id = &state.client_id;
                
                // Only show connections where this client has an assignment
                let connections: Vec<serde_json::Value> = pm
                    .profiles
                    .iter()
                    .filter(|profile| {
                        // Check if profile has MCP enabled
                        let mcp_enabled = profile.mcp_governance
                            .as_ref()
                            .map(|g| g.enabled)
                            .unwrap_or(state.mcp_enabled_by_default);
                        
                        if !mcp_enabled {
                            return false;
                        }
                        
                        // Check if client has an assignment for this connection
                        runtime.policy_assignments_for_engine()
                            .iter()
                            .any(|assignment| {
                                assignment.actor_id == *client_id 
                                    && assignment.scope.connection_id == profile.id.to_string()
                            })
                    })
                    .map(|profile| {
                        serde_json::json!({
                            "id": profile.id.to_string(),
                            "name": profile.name,
                            "driver_id": profile.driver_id(),
                            "kind": format!("{:?}", profile.kind()),
                        })
                    })
                    .collect();
                
                let json_output = serde_json::json!({ "connections": connections });
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&json_output).unwrap())
                ]))
            },
        ).await
    }

    #[tool(description = "Connect to a database using a configured connection")]
    async fn connect(
        &self,
        Parameters(params): Parameters<ConnectParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        self.governance.authorize_and_execute(
            "connect",
            Some(&params.connection_id),
            ExecutionClassification::Metadata,
            || async {
                // TODO: Implement connect_impl
                Ok(CallToolResult::success(vec![
                    Content::text(format!("Connected to {} (TODO: implement)", params.connection_id))
                ]))
            },
        ).await
    }

    // === Query Tools ===
    
    #[tool(description = "Execute a database query (SELECT, INSERT, UPDATE, DELETE, etc.)")]
    async fn execute_query(
        &self,
        Parameters(params): Parameters<ExecuteQueryParams>,
    ) -> Result<CallToolResult, ErrorData> {
        
        
        let classification = self.classify_query(&params.sql);
        
        let state = self.state.clone();
        let sql = params.sql.clone();
        let database = params.database.clone();
        let connection_id = params.connection_id.clone();
        let limit = params.limit;
        let offset = params.offset;
        
        self.governance.authorize_and_execute(
            "execute_query",
            Some(&params.connection_id),
            classification,
            move || async move {
                let result = Self::execute_query_impl(
                    state,
                    &connection_id,
                    &sql,
                    database.as_deref(),
                    limit,
                    offset,
                ).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
    }
    
    #[tool(description = "Explain query execution plan or table access strategy")]
    async fn explain_query(
        &self,
        Parameters(params): Parameters<ExplainQueryParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        let state = self.state.clone();
        let sql = params.sql.clone();
        let table = params.table.clone();
        let database = params.database.clone();
        let connection_id = params.connection_id.clone();
        
        self.governance.authorize_and_execute(
            "explain_query",
            Some(&params.connection_id),
            ExecutionClassification::Read,
            move || async move {
                let result = Self::explain_query_impl(
                    state,
                    &connection_id,
                    sql.as_deref(),
                    table.as_deref(),
                    database.as_deref(),
                ).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
    }
    
    #[tool(description = "Preview the execution plan for a mutation query without executing it")]
    async fn preview_mutation(
        &self,
        Parameters(params): Parameters<PreviewMutationParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        let state = self.state.clone();
        let sql = params.sql.clone();
        let database = params.database.clone();
        let connection_id = params.connection_id.clone();
        
        self.governance.authorize_and_execute(
            "preview_mutation",
            Some(&params.connection_id),
            ExecutionClassification::Read,
            move || async move {
                let result = Self::preview_mutation_impl(
                    state,
                    &connection_id,
                    &sql,
                    database.as_deref(),
                ).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
    }

    // === Schema Tools ===
    
    #[tool(description = "List all databases available on the connection")]
    async fn list_databases(
        &self,
        Parameters(params): Parameters<ListDatabasesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        
        self.governance.authorize_and_execute(
            "list_databases",
            Some(&params.connection_id),
            ExecutionClassification::Metadata,
            move || async move {
                let result = Self::list_databases_impl(state, &connection_id).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
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
        
        self.governance.authorize_and_execute(
            "list_schemas",
            Some(&params.connection_id),
            ExecutionClassification::Metadata,
            move || async move {
                let result = Self::list_schemas_impl(
                    state,
                    &connection_id,
                    database.as_deref(),
                ).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
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
        
        self.governance.authorize_and_execute(
            "list_tables",
            Some(&params.connection_id),
            ExecutionClassification::Metadata,
            move || async move {
                let result = Self::list_tables_impl(
                    state,
                    &connection_id,
                    database.as_deref(),
                    schema.as_deref(),
                ).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
    }

    #[tool(description = "Describe the structure of a table or collection (columns, types, constraints)")]
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
        
        self.governance.authorize_and_execute(
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
                ).await.map_err(|e| e.into_error_data())?;
                
                Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&result).unwrap())
                ]))
            },
        ).await
    }

    // === Helper Methods ===

    /// Classify a query based on its SQL content
    fn classify_query(&self, query: &str) -> dbflux_policy::ExecutionClassification {
        use dbflux_policy::ExecutionClassification;
        
        let query_upper = query.trim().to_uppercase();
        
        if query_upper.starts_with("SELECT") 
            || query_upper.starts_with("SHOW") 
            || query_upper.starts_with("DESCRIBE")
            || query_upper.starts_with("EXPLAIN") {
            ExecutionClassification::Read
        } else if query_upper.starts_with("INSERT") 
            || query_upper.starts_with("UPDATE") {
            ExecutionClassification::Write
        } else if query_upper.starts_with("DELETE") 
            || query_upper.starts_with("DROP") 
            || query_upper.starts_with("TRUNCATE") {
            ExecutionClassification::Destructive
        } else if query_upper.starts_with("CREATE") 
            || query_upper.starts_with("ALTER") 
            || query_upper.starts_with("GRANT") 
            || query_upper.starts_with("REVOKE") {
            ExecutionClassification::Admin
        } else {
            // Default to read for unknown queries
            ExecutionClassification::Read
        }
    }
    
    /// Get or establish a connection for the given connection_id
    async fn get_or_connect(
        state: ServerState,
        connection_id: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        {
            let cache = state.connection_cache.read().await;
            if let Some(conn) = cache.get(connection_id) {
                return Ok(conn);
            }
        }
        
        let profile_uuid = connection_id
            .parse::<uuid::Uuid>()
            .map_err(|_| error_messages::invalid_connection_id(connection_id))?;
        
        let profile = {
            let profile_manager = state.profile_manager.read().await;
            profile_manager
                .find_by_id(profile_uuid)
                .cloned()
                .ok_or_else(|| error_messages::connection_not_found(connection_id))?
        };
        
        let driver_id = profile.driver_id();
        
        let available_drivers: Vec<String> = state.driver_registry.keys().cloned().collect();
        
        let driver = state
            .driver_registry
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| error_messages::driver_not_available(&driver_id, &available_drivers))?;
        
        let connection = driver
            .connect_with_secrets(&profile, None, None)
            .map_err(|e| error_messages::connection_error(connection_id, &driver_id, e))?;
        
        let connection: Arc<dyn Connection> = Arc::from(connection);
        
        {
            let mut cache = state.connection_cache.write().await;
            cache.insert(connection_id.to_string(), connection.clone());
        }
        
        Ok(connection)
    }
    
    // === Implementation Methods ===
    
    async fn execute_query_impl(
        _state: ServerState,
        _connection_id: &str,
        _sql: &str,
        _database: Option<&str>,
        _limit: Option<u32>,
        _offset: Option<u32>,
    ) -> Result<serde_json::Value, String> {
        // TODO: Implement
        Ok(serde_json::json!({"status": "TODO: implement execute_query"}))
    }
    
    async fn explain_query_impl(
        _state: ServerState,
        _connection_id: &str,
        _sql: Option<&str>,
        _table: Option<&str>,
        _database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        // TODO: Implement
        Ok(serde_json::json!({"status": "TODO: implement explain_query"}))
    }
    
    async fn preview_mutation_impl(
        _state: ServerState,
        _connection_id: &str,
        _sql: &str,
        _database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        // TODO: Implement
        Ok(serde_json::json!({"status": "TODO: implement preview_mutation"}))
    }
    
    async fn list_databases_impl(
        state: ServerState,
        connection_id: &str,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        
        let databases = connection.list_databases().map_err(|e| {
            error_messages::schema_operation_error(
                "list databases",
                connection_id,
                None,
                None,
                None,
                e,
            )
        })?;
        
        let items: Vec<serde_json::Value> = databases
            .iter()
            .map(|db| {
                serde_json::json!({
                    "name": db.name,
                    "is_current": db.is_current,
                })
            })
            .collect();
        
        Ok(serde_json::json!({ "databases": items }))
    }
    
    async fn list_schemas_impl(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        
        let snapshot = connection.schema().map_err(|e| {
            error_messages::schema_operation_error(
                "list schemas",
                connection_id,
                database,
                None,
                None,
                e,
            )
        })?;
        
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
    
    async fn list_tables_impl(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
        schema: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;
        
        let database_str = database.unwrap_or("");
        let schema_info = connection.schema_for_database(database_str).map_err(|e| {
            error_messages::schema_operation_error(
                "list tables",
                connection_id,
                database,
                schema,
                None,
                e,
            )
        })?;
        
        let mut tables: Vec<serde_json::Value> = schema_info
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
        
        let views: Vec<serde_json::Value> = schema_info
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

// ===== ServerHandler Implementation =====

#[tool_handler]
impl ServerHandler for DbFluxServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_logging()
                .build()
        )
        .with_instructions(
            "DBFlux MCP Server - AI-powered database client with governance controls.\n\
             \n\
             Supports multiple database types:\n\
             • PostgreSQL, MySQL/MariaDB\n\
             • MongoDB, Redis, DynamoDB\n\
             • SQLite\n\
             \n\
             All operations are subject to role-based access control and audit logging.\n\
             Destructive operations may require manual approval before execution."
        )
    }
}

// ===== Helper Functions =====

/// Serialize a QueryResult into a JSON value suitable for MCP responses
fn serialize_query_result(result: &QueryResult) -> serde_json::Value {
    let columns: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (col, cell) in columns.iter().zip(row.iter()) {
                obj.insert((*col).to_string(), value_to_json(cell));
            }
            serde_json::Value::Object(obj)
        })
        .collect();
    
    serde_json::json!({
        "columns": columns,
        "rows": rows,
        "row_count": result.rows.len(),
    })
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(f.to_string())),
        Value::Text(s)
        | Value::Json(s)
        | Value::Decimal(s)
        | Value::ObjectId(s)
        | Value::Unsupported(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::json!({ "_type": "bytes", "length": b.len() }),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Document(doc) => {
            let map: serde_json::Map<_, _> = doc
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}
