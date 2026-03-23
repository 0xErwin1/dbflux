use rmcp::{
    ServerHandler, handler::server::router::tool::ToolRouter, model::*, tool_handler, tool_router,
};
use std::sync::Arc;

use dbflux_core::Connection;

use crate::{error_messages, governance::GovernanceMiddleware, state::ServerState};

/// Main DBFlux MCP Server
#[derive(Clone)]
pub(crate) struct DbFluxServer {
    pub(crate) state: ServerState,
    pub(crate) governance: GovernanceMiddleware,
    pub(crate) tool_router: ToolRouter<DbFluxServer>,
}

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

    /// Classify a query based on its SQL content
    fn classify_query(&self, query: &str) -> dbflux_policy::ExecutionClassification {
        use dbflux_policy::ExecutionClassification;

        let query_upper = query.trim().to_uppercase();

        if query_upper.starts_with("SELECT")
            || query_upper.starts_with("SHOW")
            || query_upper.starts_with("DESCRIBE")
            || query_upper.starts_with("EXPLAIN")
        {
            ExecutionClassification::Read
        } else if query_upper.starts_with("INSERT") || query_upper.starts_with("UPDATE") {
            ExecutionClassification::Write
        } else if query_upper.starts_with("DELETE")
            || query_upper.starts_with("DROP")
            || query_upper.starts_with("TRUNCATE")
        {
            ExecutionClassification::Destructive
        } else if query_upper.starts_with("CREATE")
            || query_upper.starts_with("ALTER")
            || query_upper.starts_with("GRANT")
            || query_upper.starts_with("REVOKE")
        {
            ExecutionClassification::Admin
        } else {
            // Default to read for unknown queries
            ExecutionClassification::Read
        }
    }

    /// Get or establish a connection for the given connection_id
    pub(crate) async fn get_or_connect(
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
}

#[tool_handler]
impl ServerHandler for DbFluxServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_logging()
                .build(),
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
             Destructive operations may require manual approval before execution.",
        )
    }
}
