//! MCP server implementation using rmcp SDK.

use rmcp::{
    tool, tool_router, tool_handler,
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars::JsonSchema,
    service::RequestContext,
    RoleServer,
};
use serde::{Deserialize, Serialize};

use crate::state::ServerState;
use crate::governance::GovernanceMiddleware;

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
    pub query: String,
    
    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListTablesParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
    
    #[schemars(description = "Optional database/schema filter")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DescribeTableParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,
    
    #[schemars(description = "Table name")]
    pub table: String,
    
    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
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
        
        self.governance.authorize_and_execute(
            "list_connections",
            None,
            ExecutionClassification::Metadata,
            || async {
                // TODO: Implement list_connections_impl
                Ok(CallToolResult::success(vec![
                    Content::text("Connections listed successfully (TODO: implement)")
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
        use dbflux_policy::ExecutionClassification;
        
        // Classify query based on content
        let classification = self.classify_query(&params.query);
        
        self.governance.authorize_and_execute(
            "execute_query",
            Some(&params.connection_id),
            classification,
            || async {
                // TODO: Implement execute_query_impl
                Ok(CallToolResult::success(vec![
                    Content::text(format!("Query executed (TODO: implement): {}", params.query))
                ]))
            },
        ).await
    }

    // === Schema Tools ===
    
    #[tool(description = "List all tables in a database")]
    async fn list_tables(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        self.governance.authorize_and_execute(
            "list_tables",
            Some(&params.connection_id),
            ExecutionClassification::Metadata,
            || async {
                // TODO: Implement list_tables_impl
                Ok(CallToolResult::success(vec![
                    Content::text(format!("Tables listed (TODO: implement) for {}", params.connection_id))
                ]))
            },
        ).await
    }

    #[tool(description = "Describe the structure of a table (columns, types, constraints)")]
    async fn describe_table(
        &self,
        Parameters(params): Parameters<DescribeTableParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;
        
        self.governance.authorize_and_execute(
            "describe_table",
            Some(&params.connection_id),
            ExecutionClassification::Metadata,
            || async {
                // TODO: Implement describe_table_impl
                Ok(CallToolResult::success(vec![
                    Content::text(format!(
                        "Table {} described (TODO: implement)",
                        params.table
                    ))
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
