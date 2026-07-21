use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use dbflux_core::observability::actions::CONFIG_CHANGE;
use dbflux_core::observability::{
    EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
};
use dbflux_core::{
    AuthProfile, ConnectionProfile, DbDriver, DriverKey, FormValues, GeneralSettings,
    GlobalOverrides, ProfileManager, ProxyProfile, ScriptsDirectory, ServiceConfig, SessionFacade,
    SshTunnelProfile,
};

use dbflux_storage::SavedQueryRepo;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotRepo;
use dbflux_storage::repositories::viz_dashboard_panels::DashboardPanelsRepository;
use dbflux_storage::repositories::viz_dashboards::DashboardsRepository;
use dbflux_storage::repositories::viz_saved_chart_binding_y::SavedChartBindingYRepository;
use dbflux_storage::repositories::viz_saved_chart_series::SavedChartSeriesRepository;
use dbflux_storage::repositories::viz_saved_charts::SavedChartsRepository;

#[cfg(feature = "mcp")]
use dbflux_mcp::{
    ConnectionPolicyAssignmentDto, McpRuntime, PolicyRoleDto, ToolPolicyDto, TrustedClientDto,
};

#[cfg(feature = "sqlite")]
use dbflux_driver_sqlite::SqliteDriver;

#[cfg(feature = "postgres")]
use dbflux_driver_postgres::PostgresDriver;

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;

#[cfg(feature = "mysql")]
use dbflux_driver_mysql::MysqlDriver;

#[cfg(feature = "mongodb")]
use dbflux_driver_mongodb::MongoDriver;

#[cfg(feature = "redis")]
use dbflux_driver_redis::RedisDriver;

#[cfg(feature = "dynamodb")]
use dbflux_driver_dynamodb::DynamoDriver;

#[cfg(feature = "cloudwatch")]
use dbflux_driver_cloudwatch::CloudWatchDriver;

#[cfg(feature = "influxdb")]
use dbflux_driver_influxdb::InfluxDriver;

#[cfg(feature = "mssql")]
use dbflux_driver_mssql::MssqlDriver;

use crate::auth_provider_registry::AuthProviderRegistry;
use crate::config_loader::{EditableGlobalHook, HookLoadDiagnostic, ProtectedHookRow};
use crate::rpc_services::external_audit::{ExternalAuditSink, NoOpContextProvider};
use crate::rpc_services::{
    AuthProviderServiceAdaptation, DriverServiceAdaptation, ExternalDriverDiagnostic,
    RpcServiceDiscovery, adapt_auth_provider_service, adapt_driver_service, discover_services,
};

#[cfg(test)]
use crate::rpc_services::{
    DriverProbe, adapt_auth_provider_service_with, adapt_driver_service_with,
};

#[cfg(test)]
use dbflux_driver_ipc::driver::IpcDriverLaunchConfig;

use super::AppState;

type DefaultDriverBuild = (
    BuiltDrivers,
    StorageRuntime,
    Vec<ConnectionProfile>,
    Vec<AuthProfile>,
    Vec<ProxyProfile>,
    Vec<SshTunnelProfile>,
);

pub(super) struct BuiltDrivers {
    pub(super) drivers: HashMap<String, Arc<dyn DbDriver>>,
    pub(super) external_driver_diagnostics: HashMap<String, ExternalDriverDiagnostic>,
    pub(super) general_settings: GeneralSettings,
    pub(super) driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    pub(super) driver_settings: HashMap<DriverKey, FormValues>,
    pub(super) hook_definitions: HashMap<String, EditableGlobalHook>,
    pub(super) hook_load_diagnostics: Vec<HookLoadDiagnostic>,
    pub(super) protected_hook_rows: Vec<ProtectedHookRow>,
    pub(super) services: Vec<ServiceConfig>,
}

impl AppState {
    pub fn new() -> Result<Self, dbflux_storage::error::StorageError> {
        let (built, storage_runtime, profiles, auth_profiles, proxies, ssh_tunnels) =
            Self::build_default_drivers()?;

        Self::new_with_drivers_and_settings(
            built.drivers,
            built.external_driver_diagnostics,
            built.general_settings,
            built.driver_overrides,
            built.driver_settings,
            built.hook_definitions,
            built.hook_load_diagnostics,
            built.protected_hook_rows,
            built.services,
            storage_runtime,
            profiles,
            auth_profiles,
            proxies,
            ssh_tunnels,
        )
    }

    pub fn new_with_storage_runtime(
        storage_runtime: StorageRuntime,
    ) -> Result<Self, dbflux_storage::error::StorageError> {
        let (built, storage_runtime, profiles, auth_profiles, proxies, ssh_tunnels) =
            Self::build_default_drivers_with_runtime(storage_runtime)?;

        Self::new_with_drivers_and_settings(
            built.drivers,
            built.external_driver_diagnostics,
            built.general_settings,
            built.driver_overrides,
            built.driver_settings,
            built.hook_definitions,
            built.hook_load_diagnostics,
            built.protected_hook_rows,
            built.services,
            storage_runtime,
            profiles,
            auth_profiles,
            proxies,
            ssh_tunnels,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_drivers_and_settings(
        mut drivers: HashMap<String, Arc<dyn DbDriver>>,
        mut external_driver_diagnostics: HashMap<String, ExternalDriverDiagnostic>,
        general_settings: GeneralSettings,
        driver_overrides: HashMap<DriverKey, GlobalOverrides>,
        driver_settings: HashMap<DriverKey, FormValues>,
        hook_definitions: HashMap<String, EditableGlobalHook>,
        hook_load_diagnostics: Vec<HookLoadDiagnostic>,
        protected_hook_rows: Vec<ProtectedHookRow>,
        services: Vec<ServiceConfig>,
        storage_runtime: dbflux_storage::bootstrap::StorageRuntime,
        profiles: Vec<ConnectionProfile>,
        auth_profiles: Vec<dbflux_core::AuthProfile>,
        proxies: Vec<dbflux_core::ProxyProfile>,
        ssh_tunnels: Vec<SshTunnelProfile>,
    ) -> Result<Self, dbflux_storage::error::StorageError> {
        let scripts_directory = ScriptsDirectory::new()
            .inspect_err(|e| log::warn!("Failed to initialize scripts directory: {}", e))
            .ok();

        let (audit_service, audit_degraded, audit_emitter) =
            Self::init_audit_backend(storage_runtime.dbflux_db_path());

        if !services.is_empty() {
            Self::launch_rpc_services(
                &mut drivers,
                &mut external_driver_diagnostics,
                services.clone(),
                Some(audit_emitter.clone()),
            );
        }

        let auth_provider_registry = Self::build_auth_provider_registry(services, audit_emitter);

        let facade = Self::build_session_facade(
            drivers,
            profiles,
            auth_profiles,
            proxies,
            ssh_tunnels,
            &storage_runtime,
        );

        let mut history_manager =
            crate::history_manager_sqlite::HistoryManager::new(&storage_runtime);
        history_manager.set_max_entries(general_settings.max_history_entries);

        #[cfg(feature = "mcp")]
        let mcp_runtime = Self::init_mcp_runtime(&audit_service, &storage_runtime);

        let (
            saved_charts_repo,
            saved_chart_series_repo,
            saved_chart_binding_y_repo,
            dashboards_repo,
            dashboard_panels_repo,
            saved_query_repo,
            schema_snapshot_repo,
        ) = Self::build_viz_repositories(&storage_runtime)?;

        let mut state = Self {
            facade,
            external_driver_diagnostics,
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
            hook_load_diagnostics,
            protected_hook_rows,
            detached_hook_tasks: HashMap::new(),
            auth_provider_registry,
            history_manager,
            scripts_directory,
            storage_runtime,
            audit_service,
            audit_degraded,
            session_passphrase_vault: Arc::new(RwLock::new(
                dbflux_ssh::SessionPassphraseVault::new(),
            )),
            metric_catalog_cache: crate::metric_catalog_cache::MetricCatalogCache::new(),
            remote_dashboard_cache: crate::remote_dashboard_cache::RemoteDashboardCache::new(),
            #[cfg(feature = "mcp")]
            mcp_runtime,
            saved_charts_repo,
            saved_chart_series_repo,
            saved_chart_binding_y_repo,
            dashboards_repo,
            dashboard_panels_repo,
            saved_query_repo,
            schema_snapshot_repo,
        };

        Self::run_post_construction_bootstraps(&mut state);

        Ok(state)
    }

    /// Initializes the audit backend used during startup, falling back to a
    /// degraded in-memory store when the real SQLite-backed store cannot be
    /// opened, and derives the external-audit emitter from it.
    fn init_audit_backend(
        db_path: &std::path::Path,
    ) -> (
        dbflux_audit::AuditService,
        bool,
        Arc<dyn dbflux_ipc::ExternalAuditEmitter>,
    ) {
        let (audit_service, audit_degraded) = match dbflux_audit::AuditService::new_sqlite(db_path)
        {
            Ok(service) => (service, false),
            Err(e) => {
                log::error!("Failed to initialize audit service at {:?}: {}", db_path, e);
                let store = dbflux_audit::store::sqlite::SqliteAuditStore::new(":memory:")
                    .expect("in-memory audit store must work: rusqlite :memory: unavailable");
                let svc = dbflux_audit::AuditService::new(store);
                svc.set_enabled(false);
                (svc, true)
            }
        };

        let drop_counter = audit_service.external_audit_drop_counter();
        let audit_emitter: Arc<dyn dbflux_ipc::ExternalAuditEmitter> =
            Arc::new(ExternalAuditSink::new(
                Arc::new(audit_service.clone()) as Arc<dyn EventSink>,
                drop_counter,
                Arc::new(NoOpContextProvider),
                crate::rpc_services::external_audit::ExternalAuditConfig::default(),
            ));

        (audit_service, audit_degraded, audit_emitter)
    }

    /// Builds the auth-provider registry, registering built-in AWS providers
    /// (when the `aws` feature is enabled) and any RPC-backed auth providers
    /// declared by `services`.
    fn build_auth_provider_registry(
        services: Vec<ServiceConfig>,
        audit_emitter: Arc<dyn dbflux_ipc::ExternalAuditEmitter>,
    ) -> AuthProviderRegistry {
        let mut auth_provider_registry = AuthProviderRegistry::new();
        #[cfg(feature = "aws")]
        {
            auth_provider_registry.register(Arc::new(dbflux_aws::AwsSsoSessionAuthProvider::new()));
            auth_provider_registry.register(Arc::new(dbflux_aws::AwsSsoAuthProvider::new()));
            auth_provider_registry
                .register(Arc::new(dbflux_aws::AwsSharedCredentialsAuthProvider::new()));
        }

        if !services.is_empty() {
            Self::launch_rpc_auth_providers(
                &mut auth_provider_registry,
                services,
                Some(audit_emitter),
            );
        }

        auth_provider_registry
    }

    /// Builds the `SessionFacade` from the profile/ssh/proxy/auth managers and
    /// the SQLite-backed connection tree store.
    fn build_session_facade(
        drivers: HashMap<String, Arc<dyn DbDriver>>,
        profiles: Vec<ConnectionProfile>,
        auth_profiles: Vec<dbflux_core::AuthProfile>,
        proxies: Vec<dbflux_core::ProxyProfile>,
        ssh_tunnels: Vec<SshTunnelProfile>,
        storage_runtime: &dbflux_storage::bootstrap::StorageRuntime,
    ) -> SessionFacade {
        let profile_manager = ProfileManager::with_profiles(profiles, None);
        let ssh_manager =
            dbflux_core::SshTunnelManager::with_items(ssh_tunnels, None, "SSH tunnel profiles");
        let proxy_manager = dbflux_core::ProxyManager::with_items(proxies, None, "proxy profiles");
        let auth_manager =
            dbflux_core::AuthProfileManager::with_items(auth_profiles, None, "auth profiles");

        let tree_store: Box<dyn dbflux_core::TreeStore> =
            Box::new(dbflux_storage::sqlite_tree_store::SqliteTreeStore::new(
                storage_runtime.dbflux_db_path().to_path_buf(),
            ));

        SessionFacade::with_all_custom_managers_and_tree_store(
            drivers,
            profile_manager,
            ssh_manager,
            proxy_manager,
            auth_manager,
            tree_store,
        )
    }

    #[cfg(feature = "mcp")]
    fn init_mcp_runtime(
        audit_service: &dbflux_audit::AuditService,
        storage_runtime: &dbflux_storage::bootstrap::StorageRuntime,
    ) -> McpRuntime {
        let approval_store: Box<dyn dbflux_approval::PendingExecutionStore> =
            match storage_runtime.pending_executions() {
                Ok(store) => Box::new(store),
                Err(e) => {
                    log::error!(
                        "Failed to open pending executions store; \
                         approvals will not survive restart: {e}"
                    );
                    Box::new(dbflux_approval::InMemoryPendingExecutionStore::default())
                }
            };
        McpRuntime::new(audit_service.clone(), approval_store)
    }

    /// Constructs the `viz_*` repositories sharing a single connection.
    ///
    /// Decision C.1: one shared `Arc<Mutex<Connection>>` is used for all repos
    /// so they serialize through the same lock, matching the pattern used by
    /// saved_filters.
    #[allow(clippy::type_complexity)]
    fn build_viz_repositories(
        storage_runtime: &dbflux_storage::bootstrap::StorageRuntime,
    ) -> Result<
        (
            Arc<SavedChartsRepository>,
            Arc<SavedChartSeriesRepository>,
            Arc<SavedChartBindingYRepository>,
            Arc<DashboardsRepository>,
            Arc<DashboardPanelsRepository>,
            Arc<SavedQueryRepo>,
            Arc<SchemaSnapshotRepo>,
        ),
        dbflux_storage::error::StorageError,
    > {
        let viz_conn = storage_runtime.viz_connection()?;
        let saved_charts_repo = Arc::new(SavedChartsRepository::new(Arc::clone(&viz_conn)));
        let saved_chart_series_repo =
            Arc::new(SavedChartSeriesRepository::new(Arc::clone(&viz_conn)));
        let saved_chart_binding_y_repo =
            Arc::new(SavedChartBindingYRepository::new(Arc::clone(&viz_conn)));
        let dashboards_repo = Arc::new(DashboardsRepository::new(Arc::clone(&viz_conn)));
        let dashboard_panels_repo = Arc::new(DashboardPanelsRepository::new(Arc::clone(&viz_conn)));
        let saved_query_repo = Arc::new(SavedQueryRepo::new(Arc::clone(&viz_conn)));
        let schema_snapshot_repo = Arc::new(SchemaSnapshotRepo::new(Arc::clone(&viz_conn)));

        Ok((
            saved_charts_repo,
            saved_chart_series_repo,
            saved_chart_binding_y_repo,
            dashboards_repo,
            dashboard_panels_repo,
            saved_query_repo,
            schema_snapshot_repo,
        ))
    }

    /// Runs the bootstrap steps that require a fully constructed `AppState`:
    /// MCP runtime hydration, the one-time AWS config reflect migration,
    /// audit settings bootstrap, and auth secret hydration/migration.
    fn run_post_construction_bootstraps(state: &mut Self) {
        #[cfg(feature = "mcp")]
        if let Err(e) = state.bootstrap_mcp_runtime_from_persistence() {
            log::warn!("Failed to bootstrap MCP runtime from persistence: {}", e);
        }

        #[cfg(feature = "aws")]
        if let Err(e) = state.bootstrap_aws_config_reflect_migration() {
            log::warn!("aws_config_reflect_migration failed (non-fatal): {}", e);
        }

        if let Err(e) = state.bootstrap_audit_settings() {
            log::warn!("Failed to bootstrap audit settings: {}", e);
            let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
            let event = EventRecord::new(
                now_ms,
                EventSeverity::Error,
                EventCategory::System,
                EventOutcome::Failure,
            )
            .with_typed_action(CONFIG_CHANGE)
            .with_summary(format!("Audit bootstrap failed: {}", e))
            .with_actor_id("system");
            if let Err(rec_err) = state.audit_service().record(event) {
                log::warn!(
                    "Failed to record audit bootstrap failure event: {}",
                    rec_err
                );
            }
        }

        state.hydrate_and_migrate_auth_secrets();
    }

    /// Re-hydrates secret-kind auth profile fields from the OS keyring and
    /// migrates any legacy plaintext secrets out of SQLite.
    ///
    /// Runs once at startup, after the provider registry and secret manager
    /// exist. For every stored profile it asks the owning provider's form
    /// definition which fields are secret-kind (`Password` / `WriteOnly`), then:
    ///
    /// - if the value is still sitting in `fields` as plaintext (a profile saved
    ///   before secrets were keyring-routed), it is moved into the keyring and,
    ///   ONLY once the keyring write is confirmed, dropped from `fields` and the
    ///   profile re-persisted so SQLite keeps just a keyring-reference marker.
    ///   If the keyring is unavailable the plaintext is left untouched so the
    ///   secret is never destroyed; migration retries on a later launch; otherwise
    /// - the value is read back from the keyring into `secret_fields`.
    ///
    /// Providers not present in the registry are skipped (their secret layout is
    /// unknown), leaving their data untouched.
    fn hydrate_and_migrate_auth_secrets(&mut self) {
        let mut needs_persist = false;
        let count = self.facade.auth_profiles.items.len();

        for idx in 0..count {
            let provider_id = self.facade.auth_profiles.items[idx].provider_id.clone();
            let profile_id = self.facade.auth_profiles.items[idx].id;

            let Some(provider) = self.auth_provider_by_id(&provider_id) else {
                continue;
            };

            let secret_field_ids: Vec<String> = provider
                .form_def()
                .tabs
                .iter()
                .flat_map(|tab| tab.sections.iter())
                .flat_map(|section| section.fields.iter())
                .filter(|field| {
                    matches!(
                        field.kind,
                        dbflux_core::FormFieldKind::Password
                            | dbflux_core::FormFieldKind::WriteOnly
                    )
                })
                .map(|field| field.id.clone())
                .collect();
            drop(provider);

            for field_id in secret_field_ids {
                let secret_ref = dbflux_core::auth_field_secret_ref(&profile_id, &field_id);

                // Already-migrated profile: the secret lives in the keyring only.
                let Some(plaintext) = self.facade.auth_profiles.items[idx]
                    .fields
                    .get(&field_id)
                    .cloned()
                else {
                    if let Some(secret) = self.facade.secrets.get_by_ref(&secret_ref) {
                        self.facade.auth_profiles.items[idx]
                            .secret_fields
                            .insert(field_id, secret);
                    }
                    continue;
                };

                // Legacy plaintext secret: move it to the keyring, but only drop
                // the plaintext copy once the write is confirmed. A locked or
                // unavailable keyring must never destroy the only copy.
                let secret = dbflux_core::secrecy::SecretString::from(plaintext);
                if self.facade.secrets.set_by_ref(&secret_ref, &secret) {
                    self.facade.auth_profiles.items[idx]
                        .fields
                        .remove(&field_id);
                    self.facade.auth_profiles.items[idx]
                        .secret_fields
                        .insert(field_id, secret);
                    needs_persist = true;
                } else {
                    log::warn!(
                        "Deferred auth secret migration for field '{}': keyring \
                         unavailable; leaving the existing value in place",
                        field_id
                    );
                }
            }
        }

        if needs_persist
            && let Err(e) = crate::config_loader::save_auth_profiles(
                &self.storage_runtime,
                &self.facade.auth_profiles.items,
            )
        {
            log::error!("Failed to persist migrated auth profile secrets: {}", e);
        }
    }

    #[cfg(feature = "mcp")]
    pub(super) fn bootstrap_mcp_runtime_from_persistence(&mut self) -> Result<(), String> {
        let repo = self.storage_runtime.governance_settings();

        if let Some(settings) = repo.get().map_err(|e| e.to_string())? {
            self.mcp_runtime
                .set_mcp_enabled(settings.mcp_enabled_by_default != 0);
        }

        let storage_clients = repo.get_trusted_clients().map_err(|e| e.to_string())?;
        for client in storage_clients {
            self.mcp_runtime
                .upsert_trusted_client_mut(TrustedClientDto {
                    id: client.client_id,
                    name: client.name,
                    issuer: client.issuer,
                    active: client.active != 0,
                })
                .map_err(|e| format!("failed to upsert trusted client: {}", e))?;
        }

        let storage_roles = repo.get_policy_roles().map_err(|e| e.to_string())?;
        for role in storage_roles {
            self.mcp_runtime
                .upsert_role_mut(PolicyRoleDto {
                    id: role.role_id,
                    policy_ids: Vec::new(),
                })
                .map_err(|e| format!("failed to upsert policy role: {}", e))?;
        }

        let storage_policies = repo.get_tool_policies().map_err(|e| e.to_string())?;
        for policy in storage_policies {
            self.mcp_runtime
                .upsert_policy_mut(ToolPolicyDto {
                    id: policy.policy_id,
                    allowed_tools: policy.allowed_tools,
                    allowed_classes: policy.allowed_classes,
                })
                .map_err(|e| format!("failed to upsert tool policy: {}", e))?;
        }

        for profile in self.facade.profiles.profiles.clone() {
            let Some(profile_governance) = profile.mcp_governance else {
                continue;
            };

            if !profile_governance.enabled {
                continue;
            }

            let assignments = profile_governance
                .policy_bindings
                .into_iter()
                .map(|binding| dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: binding.actor_id,
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: profile.id.to_string(),
                    },
                    role_ids: binding.role_ids,
                    policy_ids: binding.policy_ids,
                })
                .collect();

            self.mcp_runtime
                .save_connection_policy_assignment_mut(ConnectionPolicyAssignmentDto {
                    connection_id: profile.id.to_string(),
                    assignments,
                })
                .map_err(|e| format!("failed to save connection policy assignment: {}", e))?;
        }

        self.mcp_runtime.drain_events();
        Ok(())
    }

    fn bootstrap_audit_settings(&mut self) -> Result<(), String> {
        use dbflux_storage::repositories::audit_settings::AuditSettingsDto;

        let repo = self.storage_runtime.audit_settings();

        let settings = match repo.get().map_err(|e| e.to_string())? {
            Some(s) => s,
            None => {
                let defaults = AuditSettingsDto::default();
                repo.upsert(&defaults).map_err(|e| e.to_string())?;
                defaults
            }
        };

        if self.audit_degraded {
            log::warn!(
                "Audit service is in degraded state (DB init failed); preserving disabled status \
                 even though settings have enabled={}. Audit events will not be recorded.",
                settings.enabled
            );
        } else {
            self.audit_service.set_enabled(settings.enabled);
        }
        self.audit_service
            .set_redact_sensitive(settings.redact_sensitive_values);
        self.audit_service
            .set_capture_query_text(settings.capture_query_text);
        self.audit_service
            .set_max_detail_bytes(settings.max_detail_bytes);

        if settings.purge_on_startup && settings.retention_days > 0 {
            self.run_startup_audit_purge(settings.retention_days);
        }

        if settings.background_purge_interval_minutes > 0 {
            log::info!(
                "Background audit purge configured (interval={}min, retention={}days)",
                settings.background_purge_interval_minutes,
                settings.retention_days
            );
        }

        let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::System,
            EventOutcome::Success,
        )
        .with_typed_action(CONFIG_CHANGE)
        .with_summary("Audit settings bootstrapped successfully")
        .with_actor_id("system");
        if let Err(rec_err) = self.audit_service.record(event) {
            log::warn!(
                "Failed to record audit bootstrap success event: {}",
                rec_err
            );
        }

        Ok(())
    }

    /// Runs the startup audit-log purge when `purge_on_startup` is enabled,
    /// recording a success or failure audit event for the purge itself.
    fn run_startup_audit_purge(&self, retention_days: u32) {
        log::info!(
            "Audit purge_on_startup enabled (retention_days={}), running purge...",
            retention_days
        );
        match self.audit_service.purge_old_events(retention_days, 500) {
            Ok(stats) => {
                log::info!(
                    "Audit purge completed: deleted {} events in {} batches ({}ms)",
                    stats.deleted_count,
                    stats.batches,
                    stats.duration_ms
                );
                let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                let event = EventRecord::new(
                    now_ms,
                    EventSeverity::Info,
                    EventCategory::System,
                    EventOutcome::Success,
                )
                .with_typed_action(CONFIG_CHANGE)
                .with_summary(format!(
                    "Startup audit purge completed: deleted {} events",
                    stats.deleted_count
                ))
                .with_duration_ms(stats.duration_ms as i64)
                .with_actor_id("system");
                if let Err(rec_err) = self.audit_service.record(event) {
                    log::warn!(
                        "Failed to record startup purge success audit event: {}",
                        rec_err
                    );
                }
            }
            Err(e) => {
                log::warn!("Audit purge on startup failed: {}", e);
                let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                let event = EventRecord::new(
                    now_ms,
                    EventSeverity::Error,
                    EventCategory::System,
                    EventOutcome::Failure,
                )
                .with_typed_action(CONFIG_CHANGE)
                .with_summary(format!("Startup audit purge failed: {}", e))
                .with_actor_id("system");
                if let Err(rec_err) = self.audit_service.record(event) {
                    log::warn!(
                        "Failed to record startup purge failure audit event: {}",
                        rec_err
                    );
                }
            }
        }
    }

    /// Runs the one-time AWS config reflection migration at startup.
    ///
    /// Reads the live AWS config files once, passes the section name sets to the
    /// migration, and logs any dangling profiles as non-blocking warnings. This
    /// method is only compiled when the `aws` feature is enabled because it
    /// depends on `CachedAwsConfig` from `dbflux_aws`.
    ///
    /// Failure is non-fatal: the caller logs the error and continues startup.
    #[cfg(feature = "aws")]
    fn bootstrap_aws_config_reflect_migration(&self) -> Result<(), String> {
        use std::collections::HashSet;

        use dbflux_aws::CachedAwsConfig;

        use crate::aws_config_reflect_migration::run_aws_config_reflect_migration;

        // Obtain section names from the AWS config files.
        // CachedAwsConfig::new() initializes the cache; errors are non-fatal
        // (an empty config means zero stored AWS rows match → no rebinds).
        let mut aws_config = CachedAwsConfig::new();

        // All profile section names from ~/.aws/config (SSO and non-SSO).
        let config_section_names: HashSet<String> = aws_config
            .profiles()
            .iter()
            .map(|p| p.name.clone())
            .collect();

        // Section names from ~/.aws/credentials (bare [NAME] headers).
        let credentials_names: HashSet<String> =
            aws_config.credentials_names().iter().cloned().collect();

        // Union of both: any name present in either file is a match candidate.
        let all_config_names: HashSet<String> = config_section_names
            .union(&credentials_names)
            .cloned()
            .collect();

        // We do not have access to the real keyring in this context without
        // loading a full SecretManager. Use a best-effort probe: the stored
        // auth profile's `provider_id == "aws-static-credentials"` is a
        // sufficient signal for the dangling-origin classification. The keyring
        // predicate is used only to choose between "keyring-only" and
        // "file-gone" for static profiles; we pass a conservative closure that
        // returns `true` for all static-credentials profiles so they are
        // classified as "keyring-only" (the safer choice: preserves the keyring
        // entry and keeps the stored row).
        //
        // The actual keyring secret is never read or deleted by this migration.
        let static_provider = "aws-static-credentials";
        let auth_repo = self.storage_runtime.auth_profiles();
        let static_ids: std::collections::HashSet<String> = auth_repo
            .all()
            .unwrap_or_default()
            .into_iter()
            .filter(|r| r.provider_id == static_provider)
            .map(|r| r.id)
            .collect();

        let summary = run_aws_config_reflect_migration(
            &self.storage_runtime,
            &all_config_names,
            &credentials_names,
            move |id: &str| static_ids.contains(id),
        )
        .map_err(|e| e.to_string())?;

        for dangling in &summary.dangling {
            if let crate::aws_config_reflect_migration::ProfileMigrationOutcome::Dangling {
                id,
                origin,
            } = dangling
            {
                log::warn!(
                    "AWS config reflect migration: profile {} is dangling \
                     (origin={}). The connection will fail at connect time until \
                     the credential source is restored.",
                    id,
                    origin.as_str()
                );
            }
        }

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn build_default_drivers() -> Result<DefaultDriverBuild, dbflux_storage::error::StorageError> {
        let runtime = dbflux_storage::bootstrap::initialize()
            .expect("failed to initialize internal storage — cannot continue");

        Self::build_default_drivers_with_runtime(runtime)
    }

    #[allow(clippy::result_large_err)]
    fn build_default_drivers_with_runtime(
        runtime: StorageRuntime,
    ) -> Result<DefaultDriverBuild, dbflux_storage::error::StorageError> {
        let drivers = Self::build_builtin_drivers();
        let external_driver_diagnostics = HashMap::new();

        let (loaded, runtime) = Self::load_app_config_from_runtime(runtime)?;

        Ok((
            BuiltDrivers {
                drivers,
                external_driver_diagnostics,
                general_settings: loaded.general_settings,
                driver_overrides: loaded.driver_overrides,
                driver_settings: loaded.driver_settings,
                hook_definitions: loaded.hook_definitions,
                hook_load_diagnostics: loaded.hook_load_diagnostics,
                protected_hook_rows: loaded.protected_hook_rows,
                services: loaded.services,
            },
            runtime,
            loaded.profiles,
            loaded.auth_profiles,
            loaded.proxy_profiles,
            loaded.ssh_tunnels,
        ))
    }

    fn load_app_config_from_runtime(
        runtime: dbflux_storage::bootstrap::StorageRuntime,
    ) -> Result<
        (
            crate::config_loader::LoadedConfig,
            dbflux_storage::bootstrap::StorageRuntime,
        ),
        dbflux_storage::error::StorageError,
    > {
        let loaded = crate::config_loader::load_config(&runtime)?;
        Ok((loaded, runtime))
    }

    fn launch_rpc_services(
        drivers: &mut HashMap<String, Arc<dyn DbDriver>>,
        diagnostics: &mut HashMap<String, ExternalDriverDiagnostic>,
        services: Vec<ServiceConfig>,
        audit_emitter: Option<Arc<dyn dbflux_ipc::ExternalAuditEmitter>>,
    ) {
        for discovery in discover_services(services) {
            let descriptor = match discovery {
                RpcServiceDiscovery::Descriptor(descriptor) => descriptor,
                RpcServiceDiscovery::InvalidConfig { diagnostic } => {
                    log::warn!(
                        "Skipping RPC service '{}': invalid launch configuration: {}",
                        diagnostic.socket_id,
                        diagnostic.summary
                    );
                    diagnostics.insert(diagnostic.socket_id.clone(), diagnostic);
                    continue;
                }
            };

            match adapt_driver_service(
                descriptor,
                |driver_id| drivers.contains_key(driver_id),
                audit_emitter.clone(),
            ) {
                DriverServiceAdaptation::Registered { driver_id, service } => {
                    if let Some(socket_id) = driver_id.strip_prefix("rpc:") {
                        diagnostics.remove(socket_id);
                    }
                    drivers.insert(driver_id, service);
                }
                DriverServiceAdaptation::SkippedDisabled { socket_id } => {
                    log::info!("Skipping disabled service '{}'", socket_id);
                }
                DriverServiceAdaptation::SkippedNonDriver { socket_id, kind } => {
                    log::info!(
                        "Deferring non-driver RPC service '{}' of kind {:?}",
                        socket_id,
                        kind
                    );
                }
                DriverServiceAdaptation::SkippedDuplicate { socket_id } => {
                    log::warn!(
                        "Skipping external RPC service '{}': driver id already exists",
                        socket_id
                    );
                }
                DriverServiceAdaptation::ProbeFailed { diagnostic } => {
                    log::warn!(
                        "Skipping RPC service '{}': {}",
                        diagnostic.socket_id,
                        diagnostic.summary
                    );
                    diagnostics.insert(diagnostic.socket_id.clone(), diagnostic);
                }
            }
        }
    }

    fn launch_rpc_auth_providers(
        registry: &mut AuthProviderRegistry,
        services: Vec<ServiceConfig>,
        audit_emitter: Option<Arc<dyn dbflux_ipc::ExternalAuditEmitter>>,
    ) {
        for discovery in discover_services(services) {
            let descriptor = match discovery {
                RpcServiceDiscovery::Descriptor(descriptor) => descriptor,
                RpcServiceDiscovery::InvalidConfig { diagnostic } => {
                    log::warn!(
                        "Skipping RPC auth-provider service '{}': invalid launch configuration: {}",
                        diagnostic.socket_id,
                        diagnostic.summary
                    );
                    continue;
                }
            };

            match adapt_auth_provider_service(
                descriptor,
                |provider_id| registry.get(provider_id).is_some(),
                audit_emitter.clone(),
            ) {
                AuthProviderServiceAdaptation::Registered {
                    provider_id,
                    service,
                } => {
                    log::info!("Registered external RPC auth provider '{}'", provider_id);
                    registry.register(service);
                }
                AuthProviderServiceAdaptation::SkippedDisabled { socket_id } => {
                    log::info!("Skipping disabled auth-provider service '{}'", socket_id);
                }
                AuthProviderServiceAdaptation::SkippedNonAuthProvider { socket_id, kind } => {
                    log::info!(
                        "Deferring non-auth RPC service '{}' of kind {:?}",
                        socket_id,
                        kind
                    );
                }
                AuthProviderServiceAdaptation::SkippedDuplicate {
                    socket_id,
                    provider_id,
                } => {
                    log::warn!(
                        "Skipping RPC auth-provider service '{}': provider id '{}' already exists",
                        socket_id,
                        provider_id
                    );
                }
                AuthProviderServiceAdaptation::Incompatible { diagnostic }
                | AuthProviderServiceAdaptation::ProbeFailed { diagnostic } => {
                    log::warn!(
                        "Skipping RPC auth-provider service '{}': {}",
                        diagnostic.socket_id,
                        diagnostic.summary
                    );
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn launch_rpc_services_with<Probe, Build>(
        drivers: &mut HashMap<String, Arc<dyn DbDriver>>,
        diagnostics: &mut HashMap<String, ExternalDriverDiagnostic>,
        services: Vec<ServiceConfig>,
        mut probe: Probe,
        mut build: Build,
    ) where
        Probe: FnMut(
            &str,
            Option<&IpcDriverLaunchConfig>,
        ) -> Result<DriverProbe, Box<dbflux_core::DbError>>,
        Build:
            FnMut(String, String, DriverProbe, Option<IpcDriverLaunchConfig>) -> Arc<dyn DbDriver>,
    {
        for discovery in discover_services(services) {
            let descriptor = match discovery {
                RpcServiceDiscovery::Descriptor(descriptor) => descriptor,
                RpcServiceDiscovery::InvalidConfig { diagnostic } => {
                    diagnostics.insert(diagnostic.socket_id.clone(), diagnostic);
                    continue;
                }
            };

            match adapt_driver_service_with(
                descriptor,
                |driver_id| drivers.contains_key(driver_id),
                |socket_id, launch| probe(socket_id, launch),
                |driver_id, socket_id, probe_result, launch| {
                    build(driver_id, socket_id, probe_result, launch)
                },
            ) {
                DriverServiceAdaptation::Registered { driver_id, service } => {
                    if let Some(socket_id) = driver_id.strip_prefix("rpc:") {
                        diagnostics.remove(socket_id);
                    }
                    drivers.insert(driver_id, service);
                }
                DriverServiceAdaptation::SkippedDisabled { socket_id } => {
                    log::info!("Skipping disabled service '{}'", socket_id);
                }
                DriverServiceAdaptation::SkippedNonDriver { socket_id, kind } => {
                    log::info!(
                        "Deferring non-driver RPC service '{}' of kind {:?}",
                        socket_id,
                        kind
                    );
                }
                DriverServiceAdaptation::SkippedDuplicate { socket_id } => {
                    log::warn!(
                        "Skipping external RPC service '{}': driver id already exists",
                        socket_id
                    );
                }
                DriverServiceAdaptation::ProbeFailed { diagnostic } => {
                    diagnostics.insert(diagnostic.socket_id.clone(), diagnostic);
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn launch_rpc_auth_providers_with<Probe>(
        registry: &mut AuthProviderRegistry,
        services: Vec<ServiceConfig>,
        mut probe: Probe,
    ) where
        Probe:
            FnMut(
                &str,
                Option<&dbflux_ipc::IpcServiceLaunchConfig>,
            )
                -> Result<Arc<dyn dbflux_core::auth::DynAuthProvider>, Box<dbflux_core::DbError>>,
    {
        for discovery in discover_services(services) {
            let descriptor = match discovery {
                RpcServiceDiscovery::Descriptor(descriptor) => descriptor,
                RpcServiceDiscovery::InvalidConfig { .. } => continue,
            };

            match adapt_auth_provider_service_with(
                descriptor,
                |provider_id| registry.get(provider_id).is_some(),
                |socket_id, launch| probe(socket_id, launch),
            ) {
                AuthProviderServiceAdaptation::Registered { service, .. } => {
                    registry.register(service);
                }
                AuthProviderServiceAdaptation::SkippedDisabled { .. }
                | AuthProviderServiceAdaptation::SkippedNonAuthProvider { .. }
                | AuthProviderServiceAdaptation::SkippedDuplicate { .. }
                | AuthProviderServiceAdaptation::Incompatible { .. }
                | AuthProviderServiceAdaptation::ProbeFailed { .. } => {}
            }
        }
    }

    pub(super) fn build_builtin_drivers() -> HashMap<String, Arc<dyn DbDriver>> {
        #[allow(unused_mut)]
        let mut drivers: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();

        #[cfg(feature = "sqlite")]
        {
            drivers.insert("sqlite".to_string(), Arc::new(SqliteDriver::new()));
        }

        #[cfg(feature = "postgres")]
        {
            drivers.insert("postgres".to_string(), Arc::new(PostgresDriver::new()));
        }

        #[cfg(feature = "mysql")]
        {
            drivers.insert(
                "mysql".to_string(),
                Arc::new(MysqlDriver::new(DbKind::MySQL)),
            );
            drivers.insert(
                "mariadb".to_string(),
                Arc::new(MysqlDriver::new(DbKind::MariaDB)),
            );
        }

        #[cfg(feature = "mongodb")]
        {
            drivers.insert("mongodb".to_string(), Arc::new(MongoDriver::new()));
        }

        #[cfg(feature = "redis")]
        {
            drivers.insert("redis".to_string(), Arc::new(RedisDriver::new()));
        }

        #[cfg(feature = "dynamodb")]
        {
            drivers.insert("dynamodb".to_string(), Arc::new(DynamoDriver::new()));
        }

        #[cfg(feature = "cloudwatch")]
        {
            drivers.insert("cloudwatch".to_string(), Arc::new(CloudWatchDriver::new()));
        }

        #[cfg(feature = "influxdb")]
        {
            drivers.insert("influxdb".to_string(), Arc::new(InfluxDriver::new()));
        }

        #[cfg(feature = "mssql")]
        {
            drivers.insert("mssql".to_string(), Arc::new(MssqlDriver::new()));
        }

        drivers
    }
}
