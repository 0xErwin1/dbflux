use rmcp::{
    ServerHandler, handler::server::router::tool::ToolRouter, model::*, tool_handler, tool_router,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};
use dbflux_core::auth::SharedDynAuthProvider;
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::{CompositeValueResolver, ValueCache, ValueRef};
use dbflux_core::{
    CancelToken, Connection, ConnectionOverrides, DetachedProcessHandle, DetachedProcessReceiver,
    DetachedProcessSender, HookContext, HookExecution, HookExecutor, HookKind, HookPhase,
    HookPhaseOutcome, HookResult, HookRunner, LogErr, PipelineInput, ProcessExecutor,
};
use std::time::{Duration, Instant};

use crate::{
    connection_cache::CachedConnection, error_messages, governance::GovernanceMiddleware,
    state::ServerState,
};

/// Resolved secrets from a profile
struct ResolvedSecrets {
    password: Option<SecretString>,
    ssh_secret: Option<SecretString>,
}

struct McpHookExecutor {
    detached: DetachedProcessSender,
}

impl HookExecutor for McpHookExecutor {
    fn execute_hook(
        &self,
        hook: &dbflux_core::ConnectionHook,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        _output: Option<&dbflux_core::OutputSender>,
        _detached: Option<&DetachedProcessSender>,
    ) -> Result<HookResult, String> {
        let mut result = ProcessExecutor.execute_hook(
            hook,
            context,
            cancel_token,
            parent_cancel_token,
            None,
            Some(&self.detached),
        )?;

        if result.detached {
            result.detached = false;
            result.exit_code = Some(0);
            result.warnings.clear();
        }

        Ok(result)
    }
}

fn supervise_detached_hook_processes(receiver: DetachedProcessReceiver) -> Result<(), String> {
    std::thread::Builder::new()
        .name("dbflux-mcp-hook-supervisor".to_string())
        .spawn(move || {
            for handle in receiver {
                let _ = std::thread::Builder::new()
                    .name("dbflux-mcp-hook-process".to_string())
                    .spawn(move || supervise_detached_hook_process(handle));
            }
        })
        .map(|_| ())
        .map_err(|error| format!("Unable to start MCP hook process supervisor: {error}"))
}

fn supervise_detached_hook_process(mut handle: DetachedProcessHandle) {
    let stdout_reader = handle.child.stdout.take().map(|mut stdout| {
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut stdout, &mut std::io::sink());
        })
    });
    let stderr_reader = handle.child.stderr.take().map(|mut stderr| {
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut stderr, &mut std::io::sink());
        })
    });
    let started = Instant::now();

    loop {
        match handle.child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                if handle
                    .timeout
                    .is_some_and(|timeout| started.elapsed() >= timeout)
                {
                    let _ = handle.child.kill();
                    let _ = handle.child.wait();
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => {
                let _ = handle.child.kill();
                let _ = handle.child.wait();
                break;
            }
        }
    }

    if let Some(reader) = stdout_reader {
        let _ = reader.join();
    }
    if let Some(reader) = stderr_reader {
        let _ = reader.join();
    }
}

async fn run_connection_hook_phase(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    phase: HookPhase,
) -> Result<(), String> {
    let hooks = profile
        .hooks
        .as_ref()
        .map(|hooks| hooks.phase_hooks(phase).to_vec())
        .unwrap_or_default();
    if hooks.is_empty() {
        return Ok(());
    }

    let mut context = HookContext::from_profile(profile);
    context.phase = Some(phase);
    let cancel_token = CancelToken::new();
    let (detached, receiver) = dbflux_core::detached_process_channel();
    supervise_detached_hook_processes(receiver)?;
    let executor = McpHookExecutor { detached };
    let hook_phase = phase;
    let outcome = tokio::task::spawn_blocking(move || {
        HookRunner::run_phase(hook_phase, &hooks, &context, &cancel_token, None, &executor)
    })
    .await
    .map_err(|error| format!("MCP hook execution task failed: {error}"))?;

    let executions = match &outcome {
        HookPhaseOutcome::Success { executions }
        | HookPhaseOutcome::Aborted { executions, .. }
        | HookPhaseOutcome::CompletedWithWarnings { executions, .. } => executions,
    };
    record_hook_executions(state, profile, phase, executions).await;

    if let Some((index, hook)) = executions.iter().enumerate().find(|(_, execution)| {
        execution.hook.enabled && matches!(&execution.hook.kind, HookKind::Lua { .. })
    }) {
        return Err(format!(
            "{} hook #{} ('{}') cannot run in the MCP server because Lua hooks require the 'lua' feature",
            phase.label(),
            index + 1,
            hook.hook.summary()
        ));
    }

    match outcome {
        HookPhaseOutcome::Aborted { error, .. } => Err(error),
        HookPhaseOutcome::CompletedWithWarnings { warnings, .. } => {
            for _ in warnings {
                log::warn!(
                    "MCP connection hook for profile '{}' completed with a warning during {}",
                    profile.name,
                    phase.label().to_ascii_lowercase()
                );
            }
            Ok(())
        }
        HookPhaseOutcome::Success { .. } => Ok(()),
    }
}

async fn record_hook_executions(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    phase: HookPhase,
    executions: &[HookExecution],
) {
    use dbflux_core::observability::types::{
        EventCategory, EventOutcome, EventRecord, EventSeverity,
    };
    use dbflux_core::observability::{EventOrigin, actions};

    let audit_service = {
        let runtime = state.runtime.read().await;
        runtime.audit_service().clone()
    };
    let database = profile.config.database().unwrap_or_default();

    for (index, execution) in executions.iter().enumerate() {
        let succeeded = execution.result.as_ref().is_ok_and(HookResult::is_success);
        let phase_name = phase.label().to_ascii_lowercase();
        let event = EventRecord::new(
            chrono::Utc::now().timestamp_millis(),
            if succeeded {
                EventSeverity::Info
            } else {
                EventSeverity::Error
            },
            EventCategory::Hook,
            if succeeded {
                EventOutcome::Success
            } else {
                EventOutcome::Failure
            },
        )
        .with_typed_action(if succeeded {
            actions::HOOK_EXECUTE
        } else {
            actions::HOOK_EXECUTE_FAILED
        })
        .with_origin(EventOrigin::mcp())
        .with_actor_id(&state.client_id)
        .with_connection_context(
            profile.id.to_string(),
            database.clone(),
            profile.driver_id(),
        )
        .with_object_ref("connection_hook", format!("{phase_name}:{}", index + 1))
        .with_summary(format!(
            "Connection hook {} during {} for profile '{}'",
            if succeeded { "completed" } else { "failed" },
            phase_name,
            profile.name
        ))
        .with_details_json(
            serde_json::json!({
                "phase": phase_name,
                "hook_index": index + 1,
            })
            .to_string(),
        );
        let event = if succeeded {
            event
        } else {
            event.with_error("hook_execution_failed", "Connection hook failed")
        };

        if let Err(error) = audit_service.record(event) {
            log::warn!("Unable to record MCP connection hook audit event: {error}");
        }
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
            #[cfg(not(feature = "aws"))]
            _ => {
                let _ = params;
                let _ = remote_host;
                Err(dbflux_core::DbError::connection_failed(
                    "Managed access providers are not available in this build",
                ))
            }
            #[cfg(feature = "aws")]
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
                "SSH tunnels are not supported by the MCP server access manager",
            )),
            AccessKind::Proxy { .. } => Err(dbflux_core::DbError::connection_failed(
                "Proxy tunnels are not supported by the MCP server access manager",
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

        if let Some(driver_defaults) = self.state.driver_settings.get(&profile.driver_id()) {
            let mut merged_settings = driver_defaults.clone();

            if let Some(connection_settings) = profile.connection_settings.take() {
                merged_settings.extend(connection_settings);
            }

            profile.connection_settings = if merged_settings.is_empty() {
                None
            } else {
                Some(merged_settings)
            };
        }

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

        run_connection_hook_phase(&self.state, &profile, HookPhase::PreConnect).await?;

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
        // The keyring password is resolved before the pipeline runs: the pipeline
        // only yields a password when the profile carries a `ValueRef` for it, so a
        // profile that routes through the pipeline (auth profile, access kind, value
        // refs) but stores its password in the OS keyring would otherwise connect
        // with no password at all. The GUI applies the same fallback.
        let keyring_password =
            DbFluxServer::resolve_profile_secrets(&self.state, &profile)?.password;
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

        let hook_profile = profile.clone();

        let overrides =
            ConnectionOverrides::new(pipeline_output.resolved_password.or(keyring_password));
        let driver_id_for_error = driver_id.clone();
        let connection_key_for_error = connection_key.clone();

        let mut connection = tokio::task::spawn_blocking(move || {
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

        if let Err(hook_error) =
            run_connection_hook_phase(&self.state, &hook_profile, HookPhase::PostConnect).await
        {
            let close_error = tokio::task::spawn_blocking(move || {
                connection.close().err().map(|error| error.to_string())
            })
            .await
            .map_err(|error| {
                format!("Blocking task failed while closing rejected connection: {error}")
            })?;
            drop(access_handle);
            return Err(match close_error {
                Some(close_error) => {
                    format!("{hook_error}; failed to close rejected connection: {close_error}")
                }
                None => hook_error,
            });
        }

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
        let hook_profile = profile.clone();

        let mut connection = tokio::task::spawn_blocking(move || {
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

        if let Err(hook_error) =
            run_connection_hook_phase(&self.state, &hook_profile, HookPhase::PostConnect).await
        {
            let close_error = tokio::task::spawn_blocking(move || {
                connection.close().err().map(|error| error.to_string())
            })
            .await
            .map_err(|error| {
                format!("Blocking task failed while closing rejected connection: {error}")
            })?;
            return Err(match close_error {
                Some(close_error) => {
                    format!("{hook_error}; failed to close rejected connection: {close_error}")
                }
                None => hook_error,
            });
        }

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

        let (auth_profile, auth_provider) = if let Some(profile) = auth_profile {
            let provider = self
                .state
                .auth_provider_registry
                .get(&profile.provider_id)
                .cloned()
                .ok_or_else(|| {
                    format!("Auth provider '{}' is not available", profile.provider_id)
                })?;

            let profile_registry_snapshot: Vec<dbflux_core::auth::AuthProfile> = {
                let manager = self.state.auth_profile_manager.read().await;
                manager.items.clone()
            };
            let expanded = dbflux_core::auth::expand_auth_profile_refs(
                &profile,
                provider.form_def(),
                &|target_id| {
                    profile_registry_snapshot
                        .iter()
                        .find(|p| p.id == *target_id)
                        .cloned()
                },
            );

            (Some(expanded), Some(SharedDynAuthProvider::boxed(provider)))
        } else {
            (None, None)
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

    async fn connect_and_cache(&self, connection_id: &str) -> Result<Arc<dyn Connection>, String> {
        DbFluxServer::get_or_connect(self.state.clone(), connection_id).await
    }
}

/// Database context of a live connection, as reported to an agent by `connect`.
///
/// Both parts are empty for a driver that has no concept of databases.
pub(crate) struct DatabaseContext {
    pub(crate) current_database: Option<String>,
    pub(crate) databases: Vec<String>,
}

/// Main DBFlux MCP Server
///
/// Public so embedders (and the transport-level end-to-end tests) can serve the
/// same handler over a transport of their choice. `run_mcp_server` is the stdio
/// entry point; this type is the seam for anything else.
#[derive(Clone)]
pub struct DbFluxServer {
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
        } else if query_upper.starts_with("DELETE") || query_upper.starts_with("TRUNCATE") {
            ExecutionClassification::Destructive
        } else if query_upper.starts_with("CREATE INDEX") || query_upper.starts_with("CREATE TABLE")
        {
            ExecutionClassification::AdminSafe
        } else if query_upper.starts_with("DROP TABLE")
            || query_upper.starts_with("DROP DATABASE")
            || query_upper.contains(" DROP COLUMN")
        {
            ExecutionClassification::AdminDestructive
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

        tokio::task::spawn_blocking(move || {
            let mut scope = dbflux_core::ExecutionSessionScope::new(connection)
                .map_err(|error| error.to_string())?;
            let result = f(scope.connection()).map_err(dbflux_core::DbError::query_failed);
            scope.finish(result).map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    pub(crate) async fn execute_connection_blocking<F, T>(
        connection: Arc<dyn Connection>,
        f: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Arc<dyn Connection>) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(move || {
            let mut scope = dbflux_core::ExecutionSessionScope::new(connection)
                .map_err(|error| error.to_string())?;
            let result = f(scope.connection()).map_err(dbflux_core::DbError::query_failed);
            scope.finish(result).map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    /// Get or establish a connection for the given connection_id
    ///
    /// Reuse cached connections when available to avoid dropping the last driver
    /// handle at the end of each request. PostgreSQL in particular can block while
    /// tearing down the final client handle, which prevents the MCP response from
    /// being sent back to the caller.
    #[allow(dead_code)] // Used by tool implementations
    pub(crate) async fn get_or_connect(
        state: ServerState,
        connection_id: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        Self::get_or_connect_cached(state, connection_id, None).await
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

        Self::profile_database(state, connection_id).await
    }

    /// Get the database a connection's profile is configured for
    async fn profile_database(
        state: &ServerState,
        connection_id: &str,
    ) -> Result<Option<String>, String> {
        let profile_uuid = connection_id
            .parse::<uuid::Uuid>()
            .map_err(|_| error_messages::invalid_connection_id(connection_id))?;

        let profile_manager = state.profile_manager.read().await;
        let profile = profile_manager
            .find_by_id(profile_uuid)
            .ok_or_else(|| error_messages::connection_not_found(connection_id))?;

        Ok(profile.config.database())
    }

    /// Resolve the database a live connection is on and the databases it can reach.
    ///
    /// The current database comes from the connection's active database, then
    /// from the listed database flagged as current, then from the profile. A
    /// failed lookup is logged and leaves its part empty, because the
    /// connection itself is already established.
    pub(crate) async fn database_context(
        state: &ServerState,
        connection_id: &str,
        connection: Arc<dyn Connection>,
    ) -> DatabaseContext {
        let lookup = tokio::task::spawn_blocking(move || {
            (connection.active_database(), connection.list_databases())
        })
        .await
        .log_err_with("Database context lookup task failed");

        let (active_database, listed) = match lookup {
            Some((active_database, listed)) => (
                active_database,
                listed
                    .log_err_with("Failed to list databases after connecting")
                    .unwrap_or_default(),
            ),
            None => (None, Vec::new()),
        };

        let listed_current = listed
            .iter()
            .find(|database| database.is_current)
            .map(|database| database.name.clone());

        let current_database = match active_database.or(listed_current) {
            Some(database) => Some(database),
            None => Self::profile_database(state, connection_id)
                .await
                .log_err_with("Failed to read the profile database after connecting")
                .flatten(),
        };

        DatabaseContext {
            current_database,
            databases: listed.into_iter().map(|database| database.name).collect(),
        }
    }

    /// Connect to a different database using the same profile
    ///
    /// Reuse cached per-database connections for the same reason as `get_or_connect`.
    pub(crate) async fn connect_with_database(
        state: ServerState,
        connection_id: &str,
        database: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        Self::get_or_connect_cached(state, connection_id, Some(database)).await
    }

    pub(crate) async fn disconnect_impl(
        state: ServerState,
        connection_id: &str,
    ) -> Result<(), String> {
        let _setup_guard = state.connection_setup_lock.lock().await;
        let retired_connections = {
            let mut cache = state.connection_cache.write().await;
            cache.drain_connection_variants(connection_id)
        };
        if retired_connections.is_empty() {
            return Ok(());
        }

        let profile = if let Ok(profile_id) = connection_id.parse::<uuid::Uuid>() {
            let profile_manager = state.profile_manager.read().await;
            profile_manager.find_by_id(profile_id).cloned()
        } else {
            None
        };
        let mut errors = Vec::new();

        if let Some(profile) = profile.as_ref()
            && let Err(error) =
                run_connection_hook_phase(&state, profile, HookPhase::PreDisconnect).await
        {
            errors.push(error);
        }

        let teardown = tokio::task::spawn_blocking(move || {
            let mut errors = Vec::new();
            for cached in retired_connections {
                let connection = cached.connection();
                if let Some(factory) = connection.execution_session_factory()
                    && let Err(error) = factory.shutdown()
                {
                    errors.push(error.to_string());
                }
            }
            errors
        })
        .await;
        match teardown {
            Ok(teardown_errors) => errors.extend(teardown_errors),
            Err(error) => errors.push(format!("Connection teardown task failed: {error}")),
        }

        if let Some(profile) = profile.as_ref()
            && let Err(error) =
                run_connection_hook_phase(&state, profile, HookPhase::PostDisconnect).await
        {
            errors.push(error);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    async fn get_or_connect_cached(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<Arc<dyn Connection>, String> {
        let cache_key = database
            .map(|database| format!("{}:{}", connection_id, database))
            .unwrap_or_else(|| connection_id.to_string());

        {
            let cache = state.connection_cache.read().await;
            if let Some(connection) = cache.get(&cache_key) {
                return Ok(connection.connection());
            }
        }

        let _setup_guard = state.connection_setup_lock.lock().await;

        {
            let cache = state.connection_cache.read().await;
            if let Some(connection) = cache.get(&cache_key) {
                return Ok(connection.connection());
            }
        }

        let connection = McpConnectionFactory::new(state.clone())
            .connect(connection_id, database)
            .await?;
        let trait_object = connection.connection();

        let mut cache = state.connection_cache.write().await;
        if let Some(existing) = cache.get(&cache_key) {
            return Ok(existing.connection());
        }

        cache.insert(cache_key, connection);
        Ok(trait_object)
    }

    pub(crate) async fn connect_cached(
        state: ServerState,
        connection_id: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        McpConnectionFactory::new(state)
            .connect_and_cache(connection_id)
            .await
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for DbFluxServer {
    /// Dispatches a tool call with its arguments in scope, so governance can
    /// queue a call that needs approval and match it to an approval later.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let arguments = serde_json::Value::Object(request.arguments.clone().unwrap_or_default());
        let tool_call = rmcp::handler::server::tool::ToolCallContext::new(self, request, context);

        crate::governance::TOOL_CALL_ARGUMENTS
            .scope(arguments, self.tool_router.call(tool_call))
            .await
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "DBFlux MCP Server - AI-powered database client with governance controls.\n\
             \n\
             Supports multiple database types:\n\
             • PostgreSQL, MySQL/MariaDB\n\
             • MongoDB, Redis, DynamoDB\n\
             • SQLite\n\
             \n\
             All operations are subject to role-based access control and audit logging.\n\
             \n\
             Approvals: a policy can require a person to approve a call. Such a call does not \
             run; it fails with error data code \"approval_required\" and a \"pending_id\". \
             Then: (1) tell the user that pending execution <pending_id> is waiting for their \
             approval in DBFlux (Workspace > Pending Approvals); (2) wait, and never call \
             approve_execution or reject_execution, which are always denied to MCP clients; \
             (3) check the status with get_pending_execution: while its status is \"pending\", \
             it is still waiting, and status \"rejected\" carries the user's reason; (4) after \
             the user approves it, repeat the identical call (same tool, same arguments) and \
             it runs once. A rejected or expired call, or one \
             repeated with different arguments, is queued again instead of running.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::secrecy::{ExposeSecret, SecretString};
    use dbflux_core::{
        ConnectionHooks, ConnectionOverrides, ConnectionProfile, DbConfig, DbDriver, DbKind,
        FormValues, HookFailureMode, HookKind, NoopSecretStore, ValueRef,
    };
    use dbflux_mcp::McpRuntime;
    use dbflux_test_support::FakeDriver;
    use std::io::Write;
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
        event_trace: Option<std::path::PathBuf>,
    }

    impl RecordingDriver {
        fn new(inner: FakeDriver) -> Self {
            Self {
                inner,
                invocations: Arc::new(Mutex::new(Vec::new())),
                event_trace: None,
            }
        }

        fn with_event_trace(mut self, path: std::path::PathBuf) -> Self {
            self.event_trace = Some(path);
            self
        }

        fn record_connection(&self) {
            if let Some(path) = &self.event_trace {
                use std::io::Write;

                let mut trace = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .expect("open hook event trace");
                writeln!(trace, "driver").expect("record driver connection");
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
            self.record_connection();
            self.inner
                .connect_with_secrets(profile, password, ssh_secret)
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test recording driver: mutex poisoning makes the fixture's recorded \
                state untrustworthy, so aborting the test is intentional"
        )]
        fn connect_with_overrides(
            &self,
            profile: &ConnectionProfile,
            overrides: &ConnectionOverrides,
        ) -> Result<Box<dyn Connection>, dbflux_core::DbError> {
            self.record_connection();
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

    /// In-memory secret store so tests can exercise the keyring-backed
    /// resolution paths without touching the OS keyring.
    #[derive(Default)]
    struct InMemorySecretStore {
        secrets: Mutex<HashMap<String, String>>,
    }

    impl dbflux_core::SecretStore for InMemorySecretStore {
        fn is_available(&self) -> bool {
            true
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test in-memory secret store: mutex poisoning makes the fixture's \
                secret map untrustworthy, so aborting the test is intentional"
        )]
        fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, dbflux_core::DbError> {
            Ok(self
                .secrets
                .lock()
                .expect("secret store mutex poisoned")
                .get(secret_ref)
                .map(|value| SecretString::from(value.clone())))
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test in-memory secret store: mutex poisoning makes the fixture's \
                secret map untrustworthy, so aborting the test is intentional"
        )]
        fn set(&self, secret_ref: &str, value: &SecretString) -> Result<(), dbflux_core::DbError> {
            self.secrets
                .lock()
                .expect("secret store mutex poisoned")
                .insert(secret_ref.to_string(), value.expose_secret().to_string());
            Ok(())
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test in-memory secret store: mutex poisoning makes the fixture's \
                secret map untrustworthy, so aborting the test is intentional"
        )]
        fn delete(&self, secret_ref: &str) -> Result<(), dbflux_core::DbError> {
            self.secrets
                .lock()
                .expect("secret store mutex poisoned")
                .remove(secret_ref);
            Ok(())
        }
    }

    fn test_state_with_driver(
        driver_id: &str,
        driver: Arc<dyn DbDriver>,
        profile: ConnectionProfile,
    ) -> ServerState {
        test_state_with_driver_and_store(driver_id, driver, profile, Box::new(NoopSecretStore))
    }

    fn test_state_with_driver_and_store(
        driver_id: &str,
        driver: Arc<dyn DbDriver>,
        profile: ConnectionProfile,
        secret_store: Box<dyn dbflux_core::SecretStore>,
    ) -> ServerState {
        let audit_path = dbflux_audit::temp_sqlite_path("server_test_audit.sqlite");
        let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
            .expect("failed to create test audit service");

        let mut profile_manager = dbflux_core::ProfileManager::new_in_memory();
        profile_manager.add(profile);

        ServerState {
            client_id: "test-client".to_string(),
            runtime: Arc::new(RwLock::new(McpRuntime::new(
                audit_service,
                Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
            ))),
            profile_manager: Arc::new(RwLock::new(profile_manager)),
            auth_profile_manager: Arc::new(RwLock::new(dbflux_core::AuthProfileManager::default())),
            driver_registry: Arc::new(HashMap::from([(driver_id.to_string(), driver)])),
            auth_provider_registry: Arc::new(HashMap::new()),
            driver_settings: Arc::new(HashMap::new()),
            connection_cache: Arc::new(
                RwLock::new(crate::connection_cache::ConnectionCache::new()),
            ),
            connection_setup_lock: Arc::new(tokio::sync::Mutex::new(())),
            secret_manager: Arc::new(dbflux_core::SecretManager::new(secret_store)),
            mcp_enabled_by_default: true,
        }
    }

    fn process_hook(
        trace: &std::path::Path,
        phase: &str,
        fail: bool,
    ) -> dbflux_core::ConnectionHook {
        let mut env = HashMap::from([
            (
                "DBFLUX_MCP_HOOK_TEST_TRACE".to_string(),
                trace.to_string_lossy().into_owned(),
            ),
            ("DBFLUX_MCP_HOOK_TEST_PHASE".to_string(), phase.to_string()),
            (
                "PATH".to_string(),
                std::env::var("PATH").expect("test process PATH"),
            ),
        ]);
        if fail {
            env.insert("DBFLUX_MCP_HOOK_TEST_FAIL".to_string(), "1".to_string());
        }

        dbflux_core::ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: std::env::current_exe()
                    .expect("test executable path")
                    .to_string_lossy()
                    .into_owned(),
                args: vec!["mcp_hook_child".to_string(), "--nocapture".to_string()],
            },
            cwd: None,
            env,
            inherit_env: false,
            env_denylist: Vec::new(),
            timeout_ms: Some(10_000),
            execution_mode: dbflux_core::HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn pipeline_profile_with_hooks(hooks: ConnectionHooks) -> ConnectionProfile {
        let mut profile = ConnectionProfile::new("hooked-pg", DbConfig::default_postgres());
        profile
            .value_refs
            .insert("host".to_string(), ValueRef::literal("hook-test.example"));
        profile.hooks = Some(hooks);
        profile
    }

    #[test]
    fn mcp_hook_child() {
        let Ok(trace_path) = std::env::var("DBFLUX_MCP_HOOK_TEST_TRACE") else {
            return;
        };
        let phase = std::env::var("DBFLUX_MCP_HOOK_TEST_PHASE").expect("hook phase");
        let mut trace = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(trace_path)
            .expect("open hook event trace");
        writeln!(trace, "{phase}").expect("record hook event");
        if let Ok(sleep_ms) = std::env::var("DBFLUX_MCP_HOOK_TEST_SLEEP_MS") {
            std::thread::sleep(Duration::from_millis(
                sleep_ms.parse().expect("test sleep duration"),
            ));
            let mut trace = std::fs::OpenOptions::new()
                .append(true)
                .open(std::env::var("DBFLUX_MCP_HOOK_TEST_TRACE").expect("hook trace path"))
                .expect("reopen hook event trace");
            writeln!(trace, "complete").expect("record hook completion");
        }
        if std::env::var_os("DBFLUX_MCP_HOOK_TEST_FAIL").is_some() {
            std::process::exit(1);
        }
    }

    #[tokio::test]
    async fn connection_factory_runs_connect_hooks_around_driver_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", false));
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", false));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("connection should succeed");

        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["pre_connect", "driver", "post_connect"]
        );

        let runtime = state.runtime.read().await;
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("hook".to_string()),
            object_type: Some("connection_hook".to_string()),
            ..Default::default()
        };
        let audit_events = runtime
            .audit_service()
            .query_extended(&filter)
            .expect("hook audit events");
        assert_eq!(audit_events.len(), 2);
        assert!(audit_events.iter().all(|event| {
            event.action.as_deref() == Some("hook_execute")
                && event.outcome.as_deref() == Some("success")
        }));
        let audit_json = serde_json::to_string(&audit_events).expect("serialize hook audit events");
        assert!(!audit_json.contains("DBFLUX_MCP_HOOK_TEST_TRACE"));
        assert!(audit_json.contains("hook_index"));
    }

    #[tokio::test]
    async fn direct_connection_factory_runs_connect_hooks_around_driver_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", false));
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", false));
        let mut profile = ConnectionProfile::new("direct-hooked-pg", DbConfig::default_postgres());
        profile.hooks = Some(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        McpConnectionFactory::new(state)
            .connect(&connection_id, None)
            .await
            .expect("direct connection should succeed");

        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["pre_connect", "driver", "post_connect"]
        );
    }

    #[tokio::test]
    async fn pre_connect_hook_failure_prevents_driver_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", true));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = DbFluxServer::get_or_connect(state.clone(), &connection_id)
            .await
            .err()
            .expect("failed PreConnect hook must abort connection");

        assert!(error.to_ascii_lowercase().contains("pre-connect"));
        assert!(driver.invocations().is_empty());
        assert!(
            state
                .connection_cache
                .read()
                .await
                .get(&connection_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn unsupported_lua_hook_error_names_phase_and_hook() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.kind = HookKind::Lua {
            source: dbflux_core::ScriptSource::Inline {
                content: String::new(),
            },
            capabilities: dbflux_core::LuaCapabilities::default(),
        };
        hooks.pre_connect.push(hook);
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = DbFluxServer::get_or_connect(state, &connection_id)
            .await
            .err()
            .expect("unsupported Lua hook must abort connection");

        assert!(error.to_ascii_lowercase().contains("pre-connect"));
        assert!(error.to_ascii_lowercase().contains("lua"));
        assert!(driver.invocations().is_empty());
    }

    #[tokio::test]
    async fn detached_hook_supervisor_stops_process_after_timeout() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        hook.timeout_ms = Some(50);
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "500".to_string(),
        );
        hooks.pre_connect.push(hook);
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        McpConnectionFactory::new(state)
            .connect(&connection_id, None)
            .await
            .expect("detached hook should start without blocking connection setup");
        tokio::time::sleep(Duration::from_millis(700)).await;

        let events = std::fs::read_to_string(trace).expect("hook events");
        let events = events.lines().collect::<Vec<_>>();
        assert!(events.contains(&"pre_connect"));
        assert!(events.contains(&"driver"));
        assert!(!events.contains(&"complete"));
    }

    #[tokio::test]
    async fn post_connect_hook_failure_does_not_cache_connection() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", true));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        for expected_attempt in 1..=2 {
            let error = DbFluxServer::get_or_connect(state.clone(), &connection_id)
                .await
                .err()
                .expect("failed PostConnect hook must reject connection");
            assert!(error.to_ascii_lowercase().contains("post-connect"));
            assert!(
                state
                    .connection_cache
                    .read()
                    .await
                    .get(&connection_id)
                    .is_none()
            );
            assert_eq!(driver.invocations().len(), expected_attempt);
        }
    }

    #[tokio::test]
    async fn disconnect_runs_post_hook_and_removes_cache_after_pre_hook_failure() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_disconnect
            .push(process_hook(&trace, "pre_disconnect", true));
        hooks
            .post_disconnect
            .push(process_hook(&trace, "post_disconnect", false));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);
        let connection = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("connection should succeed");
        state
            .connection_cache
            .write()
            .await
            .insert(connection_id.clone(), connection);

        let error = DbFluxServer::disconnect_impl(state.clone(), &connection_id)
            .await
            .expect_err("PreDisconnect failure should be reported after teardown");

        assert!(error.contains("Pre-disconnect hook failed"));
        assert!(
            state
                .connection_cache
                .read()
                .await
                .get(&connection_id)
                .is_none()
        );
        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["driver", "pre_disconnect", "post_disconnect"]
        );

        let runtime = state.runtime.read().await;
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("hook".to_string()),
            object_type: Some("connection_hook".to_string()),
            ..Default::default()
        };
        let audit_events = runtime
            .audit_service()
            .query_extended(&filter)
            .expect("disconnect hook audit events");
        assert_eq!(audit_events.len(), 2);
        assert!(audit_events.iter().any(|event| {
            event.action.as_deref() == Some("hook_execute_failed")
                && event.outcome.as_deref() == Some("failure")
        }));
        assert!(audit_events.iter().any(|event| {
            event.action.as_deref() == Some("hook_execute")
                && event.outcome.as_deref() == Some("success")
        }));
    }

    #[tokio::test]
    async fn server_handler_exposes_tools_from_all_sub_routers() {
        let driver = Arc::new(FakeDriver::new(DbKind::Postgres)) as Arc<dyn DbDriver>;
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let state = test_state_with_driver("postgres", driver, profile);

        let server = DbFluxServer::new(state);
        let tools = server.tool_router.list_all();

        assert!(!tools.is_empty(), "combined tool router must not be empty");
        for tool in tools {
            assert!(
                ServerHandler::get_tool(&server, &tool.name).is_some(),
                "tool '{}' must be exposed through the ServerHandler",
                tool.name
            );
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

    #[tokio::test]
    async fn mcp_connection_factory_falls_back_to_keyring_password_on_pipeline_path() {
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;

        // A pipeline profile whose only ValueRef is the host: the pipeline resolves
        // no password, so the keyring value is the only credential available.
        let mut profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        profile.save_password = true;
        profile.value_refs.insert(
            "host".to_string(),
            ValueRef::literal("pipeline.example.internal"),
        );
        assert!(profile.uses_pipeline());

        let store = InMemorySecretStore::default();
        dbflux_core::SecretStore::set(
            &store,
            &profile.secret_ref(),
            &SecretString::from("keyring-password".to_string()),
        )
        .expect("in-memory secret store write should succeed");

        let connection_id = profile.id.to_string();
        let state =
            test_state_with_driver_and_store("postgres", driver_handle, profile, Box::new(store));

        McpConnectionFactory::new(state)
            .connect(&connection_id, None)
            .await
            .expect("pipeline-backed connection should succeed");

        let invocations = driver.invocations();
        assert_eq!(invocations.len(), 1, "expected a single driver connection");
        assert_eq!(
            invocations[0].password.as_deref(),
            Some("keyring-password"),
            "pipeline path must fall back to the keyring password when the \
             pipeline resolves none"
        );
    }

    fn relational_schema_with_databases(databases: &[(&str, bool)]) -> dbflux_core::SchemaSnapshot {
        dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases: databases
                .iter()
                .map(|(name, is_current)| dbflux_core::DatabaseInfo {
                    name: (*name).to_string(),
                    is_current: *is_current,
                })
                .collect(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn connect_reports_the_active_database_and_the_database_names() {
        let driver =
            FakeDriver::new(DbKind::Postgres).with_schema(relational_schema_with_databases(&[
                ("postgres", false),
                ("analytics", true),
            ]));
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::connect_impl(state, &connection_id)
            .await
            .expect("connect should succeed");

        assert_eq!(response["success"], serde_json::json!(true));
        assert_eq!(
            response["message"],
            serde_json::json!(format!("Connected to {connection_id}"))
        );
        assert_eq!(
            response["current_database"],
            serde_json::json!("postgres"),
            "the connection's active database wins over the listing's is_current flag"
        );
        assert_eq!(
            response["databases"],
            serde_json::json!(["postgres", "analytics"])
        );
    }

    #[tokio::test]
    async fn connect_falls_back_to_the_listed_current_database() {
        let driver =
            FakeDriver::new(DbKind::MySQL).with_schema(relational_schema_with_databases(&[
                ("information_schema", false),
                ("app", true),
            ]));
        let profile = ConnectionProfile::new("test-mysql", DbConfig::default_mysql());
        assert_eq!(
            profile.config.database(),
            None,
            "the profile must name no database for the fallback to be exercised"
        );

        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let connection = DbFluxServer::get_or_connect(state.clone(), &connection_id)
            .await
            .expect("connect should succeed");
        assert_eq!(connection.active_database(), None);

        let response = DbFluxServer::connect_impl(state, &connection_id)
            .await
            .expect("connect should succeed");

        assert_eq!(response["current_database"], serde_json::json!("app"));
        assert_eq!(
            response["databases"],
            serde_json::json!(["information_schema", "app"])
        );
    }

    fn relational_schema_with_table_and_view() -> dbflux_core::SchemaSnapshot {
        let mut snapshot =
            dbflux_test_support::fixtures::relational_schema_with_table("app", "public", "users");

        if let dbflux_core::DataStructure::Relational(relational) = &mut snapshot.structure
            && let Some(schema) = relational.schemas.first_mut()
        {
            schema.views.push(dbflux_core::ViewInfo {
                name: "active_users".to_string(),
                schema: Some("public".to_string()),
            });
        }

        snapshot
    }

    #[tokio::test]
    async fn list_tables_returns_one_object_per_table_and_view_by_default() {
        let driver =
            FakeDriver::new(DbKind::Postgres).with_schema(relational_schema_with_table_and_view());
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, false)
            .await
            .expect("list_tables should succeed");

        assert_eq!(
            response,
            serde_json::json!({
                "tables": [
                    { "name": "users", "schema": "public", "kind": "Table" },
                    { "name": "active_users", "schema": "public", "kind": "View" },
                ]
            })
        );
    }

    #[tokio::test]
    async fn list_tables_names_only_returns_table_and_view_names() {
        let driver =
            FakeDriver::new(DbKind::Postgres).with_schema(relational_schema_with_table_and_view());
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, true)
            .await
            .expect("list_tables should succeed");

        assert_eq!(
            response,
            serde_json::json!({ "tables": ["users", "active_users"] })
        );
    }

    fn key_value_schema_with_keyspaces() -> dbflux_core::SchemaSnapshot {
        let keyspace = |db_index, key_count| dbflux_core::KeySpaceInfo {
            db_index,
            key_count,
            memory_bytes: None,
            avg_ttl_seconds: None,
        };

        dbflux_core::SchemaSnapshot::key_value(dbflux_core::KeyValueSchema {
            keyspaces: vec![keyspace(0, Some(12)), keyspace(3, None)],
            current_keyspace: Some(0),
        })
    }

    #[tokio::test]
    async fn list_tables_returns_one_object_per_keyspace_by_default() {
        let driver = FakeDriver::new(DbKind::Redis).with_schema(key_value_schema_with_keyspaces());
        let profile = ConnectionProfile::new("test-redis", DbConfig::default_redis());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, false)
            .await
            .expect("list_tables should succeed");

        assert_eq!(
            response,
            serde_json::json!({
                "tables": [
                    { "db_index": 0, "key_count": 12, "kind": "Keyspace" },
                    { "db_index": 3, "key_count": null, "kind": "Keyspace" },
                ]
            })
        );
    }

    #[tokio::test]
    async fn list_tables_names_only_names_keyspaces_like_the_rest_of_the_app() {
        let driver = FakeDriver::new(DbKind::Redis).with_schema(key_value_schema_with_keyspaces());
        let profile = ConnectionProfile::new("test-redis", DbConfig::default_redis());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, true)
            .await
            .expect("list_tables should succeed");

        assert_eq!(response, serde_json::json!({ "tables": ["db0", "db3"] }));
    }

    #[tokio::test]
    async fn connect_omits_the_database_context_without_a_database_concept() {
        let driver = FakeDriver::new(DbKind::DynamoDB);
        let profile = ConnectionProfile::new("test-dynamodb", DbConfig::default_dynamodb());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::connect_impl(state, &connection_id)
            .await
            .expect("connect should succeed");

        let fields = response
            .as_object()
            .expect("connect should return a JSON object");

        assert_eq!(response["success"], serde_json::json!(true));
        assert!(!fields.contains_key("current_database"), "got {response}");
        assert!(!fields.contains_key("databases"), "got {response}");
    }
}
