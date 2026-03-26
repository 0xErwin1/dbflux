use rmcp::{
    ServerHandler, handler::server::router::tool::ToolRouter, model::*, tool_handler, tool_router,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};
use dbflux_core::auth::{
    AuthFormDef, AuthProfile, AuthSession, AuthSessionState, DynAuthProvider, ImportableProfile,
    ResolvedCredentials, UrlCallback,
};
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::{CompositeValueResolver, ValueCache, ValueRef};
use dbflux_core::{CancelToken, Connection, ConnectionOverrides, PipelineInput};

use crate::{
    connection_cache::CachedConnection, error_messages, governance::GovernanceMiddleware,
    state::ServerState,
};

/// Resolved secrets from a profile
struct ResolvedSecrets {
    password: Option<SecretString>,
    ssh_secret: Option<SecretString>,
}

struct SharedAuthProvider {
    provider: Arc<dyn DynAuthProvider>,
}

impl SharedAuthProvider {
    fn boxed(provider: Arc<dyn DynAuthProvider>) -> Box<dyn DynAuthProvider> {
        Box::new(Self { provider })
    }
}

#[async_trait::async_trait]
impl DynAuthProvider for SharedAuthProvider {
    fn provider_id(&self) -> &'static str {
        self.provider.provider_id()
    }

    fn display_name(&self) -> &'static str {
        self.provider.display_name()
    }

    fn form_def(&self) -> &'static AuthFormDef {
        self.provider.form_def()
    }

    async fn validate_session(
        &self,
        profile: &AuthProfile,
    ) -> Result<AuthSessionState, dbflux_core::DbError> {
        self.provider.validate_session(profile).await
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, dbflux_core::DbError> {
        self.provider.login(profile, url_callback).await
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, dbflux_core::DbError> {
        self.provider.resolve_credentials(profile).await
    }

    fn register_value_providers(
        &self,
        profile: &AuthProfile,
        session: Option<&AuthSession>,
        resolver: &mut CompositeValueResolver,
    ) -> Result<(), dbflux_core::DbError> {
        self.provider
            .register_value_providers(profile, session, resolver)
    }

    fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
        self.provider.detect_importable_profiles()
    }

    fn after_profile_saved(&self, profile: &AuthProfile) {
        self.provider.after_profile_saved(profile);
    }
}

struct McpAccessManager {
    #[cfg(feature = "aws")]
    ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>,
}

impl McpAccessManager {
    #[cfg(feature = "aws")]
    fn new(ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>) -> Self {
        Self { ssm_factory }
    }

    #[cfg(not(feature = "aws"))]
    fn new() -> Self {
        Self {}
    }

    async fn open_managed(
        &self,
        provider: &str,
        params: &HashMap<String, String>,
        remote_host: &str,
    ) -> Result<AccessHandle, dbflux_core::DbError> {
        match provider {
            #[cfg(feature = "aws")]
            "aws-ssm" => {
                let instance_id = params.get("instance_id").map(String::as_str).unwrap_or("");
                let region = params
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or("us-east-1");
                let remote_port = params
                    .get("remote_port")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);

                let factory = self.ssm_factory.as_ref().ok_or_else(|| {
                    dbflux_core::DbError::connection_failed("SSM tunnel factory not available")
                })?;

                let tunnel = factory.start(instance_id, region, remote_host, remote_port)?;
                Ok(AccessHandle::tunnel(tunnel.local_port(), Box::new(tunnel)))
            }
            other => Err(dbflux_core::DbError::connection_failed(format!(
                "Unknown managed access provider: '{}'. No handler registered.",
                other
            ))),
        }
    }
}

#[async_trait::async_trait]
impl AccessManager for McpAccessManager {
    async fn open(
        &self,
        access_kind: &AccessKind,
        remote_host: &str,
        _remote_port: u16,
    ) -> Result<AccessHandle, dbflux_core::DbError> {
        match access_kind {
            AccessKind::Direct => Ok(AccessHandle::direct()),
            AccessKind::Ssh { .. } => Err(dbflux_core::DbError::connection_failed(
                "SSH tunnels are managed by the legacy connect path",
            )),
            AccessKind::Proxy { .. } => Err(dbflux_core::DbError::connection_failed(
                "Proxy tunnels are managed by the legacy connect path",
            )),
            AccessKind::Managed { provider, params } => {
                self.open_managed(provider, params, remote_host).await
            }
        }
    }
}

struct McpConnectionFactory {
    state: ServerState,
}

impl McpConnectionFactory {
    fn new(state: ServerState) -> Self {
        Self { state }
    }

    async fn connect(
        &self,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<Arc<CachedConnection>, String> {
        let profile_uuid = connection_id
            .parse::<uuid::Uuid>()
            .map_err(|_| error_messages::invalid_connection_id(connection_id))?;

        let mut profile = {
            let profile_manager = self.state.profile_manager.read().await;
            profile_manager
                .find_by_id(profile_uuid)
                .cloned()
                .ok_or_else(|| error_messages::connection_not_found(connection_id))?
        };

        if let Some(database) = database {
            profile.config = profile.config.clone().with_database(database)?;
        }

        let driver_id = profile.driver_id();
        let available_drivers: Vec<String> = self.state.driver_registry.keys().cloned().collect();
        let driver = self
            .state
            .driver_registry
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| error_messages::driver_not_available(&driver_id, &available_drivers))?;

        let connection_key = database
            .map(|database| format!("{}:{}", connection_id, database))
            .unwrap_or_else(|| connection_id.to_string());

        if profile.uses_pipeline() {
            self.connect_with_pipeline(connection_key, driver_id, driver, profile)
                .await
        } else {
            self.connect_direct(connection_key, driver_id, driver, profile)
                .await
        }
    }

    async fn connect_with_pipeline(
        &self,
        connection_key: String,
        driver_id: String,
        driver: Arc<dyn dbflux_core::DbDriver>,
        profile: dbflux_core::ConnectionProfile,
    ) -> Result<Arc<CachedConnection>, String> {
        let pipeline_input = self.build_pipeline_input(profile).await?;
        let (state_tx, _state_rx) = dbflux_core::pipeline_state_channel();
        let pipeline_output = dbflux_core::run_pipeline(pipeline_input, &state_tx)
            .await
            .map_err(|error| format!("Pipeline stage '{}': {}", error.stage, error.source))?;

        let mut profile = pipeline_output.resolved_profile;
        let access_handle = pipeline_output.access_handle;

        if access_handle.is_tunneled() {
            profile
                .config
                .redirect_to_tunnel(access_handle.local_port());
        }

        let overrides = ConnectionOverrides::new(pipeline_output.resolved_password);
        let driver_id_for_error = driver_id.clone();
        let connection_key_for_error = connection_key.clone();

        let connection = tokio::task::spawn_blocking(move || {
            driver
                .connect_with_overrides(&profile, &overrides)
                .map_err(|error| {
                    error_messages::connection_error(
                        &connection_key_for_error,
                        &driver_id_for_error,
                        error,
                    )
                })
        })
        .await
        .map_err(|error| format!("Blocking task failed: {}", error))??;

        Ok(Arc::new(CachedConnection::new(
            Arc::from(connection),
            Some(Box::new(access_handle)),
        )))
    }

    async fn connect_direct(
        &self,
        connection_key: String,
        driver_id: String,
        driver: Arc<dyn dbflux_core::DbDriver>,
        profile: dbflux_core::ConnectionProfile,
    ) -> Result<Arc<CachedConnection>, String> {
        let resolved_secrets = DbFluxServer::resolve_profile_secrets(&self.state, &profile)?;
        let password = resolved_secrets.password;
        let ssh_secret = resolved_secrets.ssh_secret;
        let connection_key_for_error = connection_key.clone();
        let driver_id_for_error = driver_id.clone();

        let connection = tokio::task::spawn_blocking(move || {
            driver
                .connect_with_secrets(&profile, password.as_ref(), ssh_secret.as_ref())
                .map_err(|error| {
                    error_messages::connection_error(
                        &connection_key_for_error,
                        &driver_id_for_error,
                        error,
                    )
                })
        })
        .await
        .map_err(|error| format!("Blocking task failed: {}", error))??;

        Ok(Arc::new(CachedConnection::new(Arc::from(connection), None)))
    }

    async fn build_pipeline_input(
        &self,
        profile: dbflux_core::ConnectionProfile,
    ) -> Result<PipelineInput, String> {
        let selected_auth_profile_id = profile
            .access_kind
            .as_ref()
            .and_then(|kind| match kind {
                AccessKind::Managed { params, .. } => params
                    .get("auth_profile_id")
                    .and_then(|value| value.parse().ok()),
                _ => None,
            })
            .or(profile.auth_profile_id);

        let auth_profile = {
            let auth_profiles = self.state.auth_profile_manager.read().await;
            selected_auth_profile_id.and_then(|auth_id| {
                auth_profiles
                    .items
                    .iter()
                    .find(|profile| profile.id == auth_id && profile.enabled)
                    .cloned()
            })
        };

        let uses_managed_access = matches!(profile.access_kind, Some(AccessKind::Managed { .. }));
        if uses_managed_access && auth_profile.is_none() {
            return Err(
                "Managed access requires an auth profile. Select one in Access > SSM Auth Profile."
                    .to_string(),
            );
        }

        let registered_auth_provider_ids: HashSet<&str> = self
            .state
            .auth_provider_registry
            .keys()
            .map(String::as_str)
            .collect();

        let uses_registered_auth_value_sources = profile.value_refs.values().any(|value_ref| {
            matches!(
                value_ref,
                ValueRef::Secret { provider, .. } | ValueRef::Parameter { provider, .. }
                    if registered_auth_provider_ids.contains(provider.as_str())
            )
        });

        if uses_registered_auth_value_sources && auth_profile.is_none() {
            return Err(
                "Value sources requiring auth providers need an auth profile. Select one before connecting."
                    .to_string(),
            );
        }

        let auth_provider = if let Some(auth_profile) = auth_profile.as_ref() {
            let provider = self
                .state
                .auth_provider_registry
                .get(&auth_profile.provider_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "Auth provider '{}' is not available",
                        auth_profile.provider_id
                    )
                })?;

            Some(SharedAuthProvider::boxed(provider))
        } else {
            None
        };

        let resolver = CompositeValueResolver::new(Arc::new(ValueCache::new(
            std::time::Duration::from_secs(300),
        )));

        #[cfg(feature = "aws")]
        let aws_profile_name = auth_profile
            .as_ref()
            .and_then(|profile| profile.fields.get("profile_name").cloned());

        let access_manager: Arc<dyn AccessManager> = Arc::new(McpAccessManager::new(
            #[cfg(feature = "aws")]
            Some(Arc::new(dbflux_ssm::SsmTunnelFactory::new(
                aws_profile_name,
            ))),
        ));

        Ok(PipelineInput {
            profile,
            auth_provider,
            auth_profile,
            resolver,
            access_manager,
            cancel: CancelToken::new(),
        })
    }

    async fn connect_and_cache(&self, connection_id: &str) -> Result<(), String> {
        let connection = self.connect(connection_id, None).await?;
        let mut cache = self.state.connection_cache.write().await;
        cache.insert(connection_id.to_string(), connection);
        Ok(())
    }
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
        let connection = Self::get_or_connect(state, connection_id).await?;

        tokio::task::spawn_blocking(move || f(connection))
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
        let connection = McpConnectionFactory::new(state)
            .connect(connection_id, None)
            .await?;
        Ok(connection)
    }

    /// Resolve secrets from a profile using keyring
    fn resolve_profile_secrets(
        state: &ServerState,
        profile: &dbflux_core::ConnectionProfile,
    ) -> Result<ResolvedSecrets, String> {
        let password = if profile.save_password {
            match state.secret_manager.get_password(profile) {
                Some(pw) => {
                    log::debug!(
                        "Resolved password from keyring for profile '{}' (id={})",
                        profile.name,
                        profile.id
                    );
                    Some(pw)
                }
                None => {
                    log::warn!(
                        "No password found in keyring for profile '{}' (id={}, save_password={}). \
                         Connection may fail with authentication error.",
                        profile.name,
                        profile.id,
                        profile.save_password
                    );
                    None
                }
            }
        } else {
            log::debug!(
                "Profile '{}' has save_password=false, skipping keyring lookup",
                profile.name
            );
            None
        };

        let ssh_secret = if profile.config.has_ssh_tunnel() {
            match state.secret_manager.get_ssh_password(profile) {
                Some(ssh) => {
                    log::debug!(
                        "Resolved SSH password from keyring for profile '{}'",
                        profile.name
                    );
                    Some(ssh)
                }
                None => {
                    log::warn!(
                        "No SSH password found in keyring for profile '{}' (has_ssh_tunnel=true). \
                         SSH tunnel connection may fail.",
                        profile.name
                    );
                    None
                }
            }
        } else {
            None
        };

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
            let connection = conn.connection();
            let db = tokio::task::spawn_blocking(move || connection.active_database())
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
        let connection = McpConnectionFactory::new(state)
            .connect(connection_id, Some(database))
            .await?;
        Ok(connection)
    }

    pub(crate) async fn connect_cached(
        state: ServerState,
        connection_id: &str,
    ) -> Result<(), String> {
        McpConnectionFactory::new(state)
            .connect_and_cache(connection_id)
            .await
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

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::secrecy::{ExposeSecret, SecretString};
    use dbflux_core::{
        ConnectionOverrides, ConnectionProfile, DbConfig, DbDriver, DbKind, FormValues,
        NoopSecretStore, ValueRef,
    };
    use dbflux_mcp::McpRuntime;
    use dbflux_test_support::FakeDriver;
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock;

    #[derive(Debug, Clone)]
    struct ConnectInvocation {
        profile: ConnectionProfile,
        password: Option<String>,
    }

    #[derive(Clone)]
    struct RecordingDriver {
        inner: FakeDriver,
        invocations: Arc<Mutex<Vec<ConnectInvocation>>>,
    }

    impl RecordingDriver {
        fn new(inner: FakeDriver) -> Self {
            Self {
                inner,
                invocations: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn invocations(&self) -> Vec<ConnectInvocation> {
            self.invocations
                .lock()
                .expect("recording driver mutex poisoned")
                .clone()
        }
    }

    impl DbDriver for RecordingDriver {
        fn kind(&self) -> DbKind {
            self.inner.kind()
        }

        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            self.inner.metadata()
        }

        fn driver_key(&self) -> dbflux_core::DriverKey {
            self.inner.driver_key()
        }

        fn form_definition(&self) -> &dbflux_core::DriverFormDef {
            self.inner.form_definition()
        }

        fn build_config(&self, values: &FormValues) -> Result<DbConfig, dbflux_core::DbError> {
            self.inner.build_config(values)
        }

        fn extract_values(&self, config: &DbConfig) -> FormValues {
            self.inner.extract_values(config)
        }

        fn connect_with_secrets(
            &self,
            profile: &ConnectionProfile,
            password: Option<&SecretString>,
            ssh_secret: Option<&SecretString>,
        ) -> Result<Box<dyn Connection>, dbflux_core::DbError> {
            self.inner
                .connect_with_secrets(profile, password, ssh_secret)
        }

        fn connect_with_overrides(
            &self,
            profile: &ConnectionProfile,
            overrides: &ConnectionOverrides,
        ) -> Result<Box<dyn Connection>, dbflux_core::DbError> {
            self.invocations
                .lock()
                .expect("recording driver mutex poisoned")
                .push(ConnectInvocation {
                    profile: profile.clone(),
                    password: overrides
                        .password
                        .as_ref()
                        .map(|value| value.expose_secret().to_string()),
                });

            self.inner.connect_with_overrides(profile, overrides)
        }

        fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), dbflux_core::DbError> {
            self.inner.test_connection(profile)
        }
    }

    fn test_state_with_driver(
        driver_id: &str,
        driver: Arc<dyn DbDriver>,
        profile: ConnectionProfile,
    ) -> ServerState {
        let audit_path = dbflux_audit::temp_sqlite_path(&format!(
            "server_test_{}.sqlite",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_nanos()
        ));
        let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
            .expect("failed to create test audit service");

        let mut profile_manager = dbflux_core::ProfileManager::new_in_memory();
        profile_manager.add(profile);

        ServerState {
            client_id: "test-client".to_string(),
            runtime: Arc::new(RwLock::new(McpRuntime::new(audit_service))),
            profile_manager: Arc::new(RwLock::new(profile_manager)),
            auth_profile_manager: Arc::new(RwLock::new(dbflux_core::AuthProfileManager::default())),
            driver_registry: Arc::new(HashMap::from([(driver_id.to_string(), driver)])),
            auth_provider_registry: Arc::new(HashMap::new()),
            connection_cache: Arc::new(
                RwLock::new(crate::connection_cache::ConnectionCache::new()),
            ),
            secret_manager: Arc::new(dbflux_core::SecretManager::new(Box::new(NoopSecretStore))),
            mcp_enabled_by_default: true,
        }
    }

    #[tokio::test]
    async fn mcp_connection_factory_uses_pipeline_resolved_values_and_database_override() {
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;

        let mut profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        profile.value_refs.insert(
            "host".to_string(),
            ValueRef::literal("pipeline.example.internal"),
        );
        profile.value_refs.insert(
            "password".to_string(),
            ValueRef::literal("pipeline-password"),
        );

        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let connection = McpConnectionFactory::new(state)
            .connect(&connection_id, Some("analytics"))
            .await
            .expect("pipeline-backed connection should succeed");

        let invocations = driver.invocations();
        assert_eq!(invocations.len(), 1, "expected a single driver connection");

        let invocation = &invocations[0];
        match &invocation.profile.config {
            DbConfig::Postgres { host, database, .. } => {
                assert_eq!(host, "pipeline.example.internal");
                assert_eq!(database, "analytics");
            }
            other => panic!("expected postgres config, got {other:?}"),
        }

        assert_eq!(
            invocation.password.as_deref(),
            Some("pipeline-password"),
            "pipeline should pass resolved password as an override"
        );
        assert_eq!(
            connection.connection().active_database().as_deref(),
            Some("analytics")
        );
    }
}
