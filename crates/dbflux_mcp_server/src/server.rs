use rmcp::{
    ServerHandler, handler::server::router::tool::ToolRouter, model::*, tool_handler, tool_router,
};
use std::sync::Arc;

use dbflux_core::Connection;
use dbflux_core::secrecy::SecretString;

use crate::{error_messages, governance::GovernanceMiddleware, state::ServerState};

/// Resolved secrets from a profile
struct ResolvedSecrets {
    password: Option<SecretString>,
    ssh_secret: Option<SecretString>,
}

/// Main DBFlux MCP Server
#[derive(Clone)]
pub(crate) struct DbFluxServer {
    #[allow(dead_code)] // Used by governance middleware and tools
    pub(crate) state: ServerState,
    #[allow(dead_code)] // Used for policy evaluation
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
            tool_router: Self::tool_router()
                + Self::connection_router()
                + Self::schema_router()
                + Self::query_router()
                + Self::read_router()
                + Self::write_router()
                + Self::destructive_router()
                + Self::ddl_router()
                + Self::scripts_router()
                + Self::approval_router()
                + Self::audit_router(),
        }
    }

    /// Classify a query based on its SQL content
    #[allow(dead_code)] // May be used by future governance features
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

    /// Execute a blocking operation in a spawned thread
    #[allow(dead_code)]
    pub(crate) async fn execute_blocking<F, T, E>(f: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        tokio::task::spawn_blocking(move || f().map_err(|e| format!("{}", e)))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    /// Get connection and execute a query in a blocking context
    /// This is the preferred method for tools that need to execute queries
    #[allow(dead_code)]
    pub(crate) async fn get_connected_query<F, T>(
        state: ServerState,
        connection_id: &str,
        f: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Arc<dyn Connection>) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Self::get_or_connect(state, connection_id).await?;

        tokio::task::spawn_blocking(move || f(conn))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    /// Get or establish a connection for the given connection_id
    ///
    /// IMPORTANT: This method creates a fresh connection for each request to avoid
    /// lock contention issues when multiple concurrent requests share the same
    /// connection object. The underlying drivers (PostgreSQL, SQLite, etc.) manage
    /// their own internal locking, but sharing connections across concurrent
    /// async contexts can lead to "poisoned lock" errors when one query panics
    /// while holding the internal mutex.
    ///
    /// Connection caching at this level would require proper connection pooling
    /// with per-request semantics (like deadpool-postgres), which is not
    /// currently implemented.
    #[allow(dead_code)] // Used by tool implementations
    pub(crate) async fn get_or_connect(
        state: ServerState,
        connection_id: &str,
    ) -> Result<Arc<dyn Connection>, String> {
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

        // Resolve secrets and values from the profile
        let resolved_secrets = Self::resolve_profile_secrets(&state, &profile)?;

        let driver_id = profile.driver_id();

        let available_drivers: Vec<String> = state.driver_registry.keys().cloned().collect();

        let driver = state
            .driver_registry
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| error_messages::driver_not_available(&driver_id, &available_drivers))?;

        let connection_id_owned = connection_id.to_string();
        let driver_id_owned = driver_id.clone();
        let profile_for_connect = profile.clone();
        let password = resolved_secrets.password;
        let ssh_secret = resolved_secrets.ssh_secret;

        let connection = tokio::task::spawn_blocking(move || {
            driver
                .connect_with_secrets(&profile_for_connect, password.as_ref(), ssh_secret.as_ref())
                .map_err(|e| {
                    error_messages::connection_error(&connection_id_owned, &driver_id_owned, e)
                })
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))??;

        let connection: Arc<dyn Connection> = Arc::from(connection);

        Ok(connection)
    }

    /// Resolve secrets from a profile using keyring
    fn resolve_profile_secrets(
        state: &ServerState,
        profile: &dbflux_core::ConnectionProfile,
    ) -> Result<ResolvedSecrets, String> {
        let mut password: Option<SecretString> = None;
        let mut ssh_secret: Option<SecretString> = None;

        // Try to get password from keyring
        if let Some(pwd) = state.secret_manager.get_password(profile) {
            password = Some(pwd);
        }

        // Resolve SSH secret if needed
        if password.is_none()
            && let Some(pwd) = state.secret_manager.get_password(profile)
        {
            password = Some(pwd);
        }

        // Resolve SSH secret if needed
        if let Some(ssh) = state.secret_manager.get_ssh_password(profile) {
            ssh_secret = Some(ssh);
        }

        Ok(ResolvedSecrets {
            password,
            ssh_secret,
        })
    }

    /// Get the current database for a connection from the cache
    pub(crate) async fn get_current_database(
        state: &ServerState,
        connection_id: &str,
    ) -> Result<Option<String>, String> {
        let cache = state.connection_cache.read().await;

        if let Some(conn) = cache.get(connection_id) {
            let conn = conn.clone();
            let db = tokio::task::spawn_blocking(move || conn.active_database())
                .await
                .map_err(|e| format!("Blocking task failed: {}", e))?;
            return Ok(db);
        }

        drop(cache);

        let profile_uuid = connection_id
            .parse::<uuid::Uuid>()
            .map_err(|_| error_messages::invalid_connection_id(connection_id))?;

        let profile_manager = state.profile_manager.read().await;
        let profile = profile_manager
            .find_by_id(profile_uuid)
            .ok_or_else(|| error_messages::connection_not_found(connection_id))?;

        Ok(profile.config.database())
    }

    /// Connect to a different database using the same profile
    ///
    /// IMPORTANT: Creates a fresh connection for each request to avoid lock
    /// contention issues (see get_or_connect for details).
    pub(crate) async fn connect_with_database(
        state: ServerState,
        connection_id: &str,
        database: &str,
    ) -> Result<Arc<dyn Connection>, String> {
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

        let mut new_profile = profile.clone();
        Self::set_database_in_profile(&mut new_profile, database)?;

        let resolved_secrets = Self::resolve_profile_secrets(&state, &new_profile)?;

        let driver_id = new_profile.driver_id();

        let available_drivers: Vec<String> = state.driver_registry.keys().cloned().collect();

        let driver = state
            .driver_registry
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| error_messages::driver_not_available(&driver_id, &available_drivers))?;

        let cache_key_owned = format!("{}:{}", connection_id, database);
        let driver_id_owned = driver_id.clone();
        let profile_for_connect = new_profile.clone();
        let password = resolved_secrets.password;
        let ssh_secret = resolved_secrets.ssh_secret;

        let connection = tokio::task::spawn_blocking(move || {
            driver
                .connect_with_secrets(&profile_for_connect, password.as_ref(), ssh_secret.as_ref())
                .map_err(|e| {
                    error_messages::connection_error(&cache_key_owned, &driver_id_owned, e)
                })
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))??;

        let connection: Arc<dyn Connection> = Arc::from(connection);

        Ok(connection)
    }

    /// Set database in profile config (cloned and modified)
    fn set_database_in_profile(
        profile: &mut dbflux_core::ConnectionProfile,
        database: &str,
    ) -> Result<(), String> {
        profile.config = profile.config.clone().with_database(database)?;
        Ok(())
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
