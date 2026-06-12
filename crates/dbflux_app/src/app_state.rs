//! Application state for DBFlux.
//!
//! This module contains the core `AppState` struct which manages all application-level
//! state including connections, profiles, settings, and audit services.

use dbflux_core::observability::actions::{
    CONFIG_CHANGE, CONFIG_CREATE, CONFIG_DELETE, CONFIG_UPDATE,
};
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity, EventSink,
};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    AuthProfile, CancelToken, Connection, ConnectionHook, ConnectionHooks, ConnectionProfile,
    DbDriver, DbSchemaInfo, DriverKey, EffectiveSettings, FetchCollectionChildrenParams,
    FormValues, GeneralSettings, GlobalOverrides, HistoryEntry, HistoryManager, HookContext,
    HookPhase, ProfileManager, ProxyProfile, SavedQuery, SavedQueryManager, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaSnapshot, ScriptsDirectory, SecretStore, ServiceConfig, SessionFacade,
    ShutdownPhase, SshTunnelProfile, TaskId, TaskKind, TaskSnapshot,
};
use dbflux_storage::SavedQueryRepo;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::repositories::viz_dashboard_panels::DashboardPanelsRepository;
use dbflux_storage::repositories::viz_dashboards::DashboardsRepository;
use dbflux_storage::repositories::viz_saved_chart_binding_y::SavedChartBindingYRepository;
use dbflux_storage::repositories::viz_saved_chart_series::SavedChartSeriesRepository;
use dbflux_storage::repositories::viz_saved_charts::SavedChartsRepository;

#[cfg(feature = "mcp")]
use dbflux_mcp::{
    ApprovalOutcome, ConnectionPolicyAssignmentDto, McpGovernanceService, McpRuntime,
    McpRuntimeEvent, PendingExecutionDetail, PendingExecutionSummary, PolicyRoleDto, ToolPolicyDto,
    TrustedClientDto,
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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

use crate::auth_provider_registry::{AuthProviderRegistry, RegistryAuthProviderWrapper};
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

pub use dbflux_core::{
    ConnectProfileParams, ConnectedProfile, DangerousQuerySuppressions, FetchDatabaseSchemaParams,
    FetchSchemaForeignKeysParams, FetchSchemaIndexesParams, FetchSchemaRoutinesParams,
    FetchSchemaTypesParams, FetchTableDetailsParams, SwitchDatabaseParams,
};

struct BuiltDrivers {
    drivers: HashMap<String, Arc<dyn DbDriver>>,
    external_driver_diagnostics: HashMap<String, ExternalDriverDiagnostic>,
    general_settings: GeneralSettings,
    driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    driver_settings: HashMap<DriverKey, FormValues>,
    hook_definitions: HashMap<String, ConnectionHook>,
    services: Vec<ServiceConfig>,
}

pub struct AppState {
    pub facade: SessionFacade,
    external_driver_diagnostics: HashMap<String, ExternalDriverDiagnostic>,
    general_settings: GeneralSettings,
    driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    driver_settings: HashMap<DriverKey, FormValues>,
    hook_definitions: HashMap<String, ConnectionHook>,
    detached_hook_tasks: HashMap<Uuid, HashSet<TaskId>>,
    auth_provider_registry: AuthProviderRegistry,
    history_manager: crate::history_manager_sqlite::HistoryManager,
    scripts_directory: Option<ScriptsDirectory>,
    storage_runtime: StorageRuntime,
    audit_service: dbflux_audit::AuditService,
    /// Session-scoped cache for metric catalog data (namespaces + metrics pages).
    ///
    /// Shared via `Arc` so multiple chart documents can read and write it
    /// without holding a reference to `AppState` itself.
    pub metric_catalog_cache: Arc<crate::metric_catalog_cache::MetricCatalogCache>,
    /// Session-scoped cache for upstream dashboard listings (read-only browse).
    ///
    /// Shared via `Arc` so the sidebar can read and write it without holding a
    /// reference to `AppState` itself.
    pub remote_dashboard_cache: Arc<crate::remote_dashboard_cache::RemoteDashboardCache>,
    /// Tracks whether the audit service was initialized from a degraded (in-memory)
    /// store because the real SQLite database could not be opened. When true,
    /// bootstrap_audit_settings will not enable the service even if persisted
    /// settings say enabled=true, preserving an honest degraded-state signal.
    audit_degraded: bool,
    /// Session-scoped passphrase vault for SSH tunnel private keys.
    ///
    /// Passphrases entered via the modal prompt are held here for the process
    /// lifetime. Never serialized, logged, or written to disk.
    pub session_passphrase_vault: Arc<RwLock<dbflux_ssh::SessionPassphraseVault>>,
    #[cfg(feature = "mcp")]
    mcp_runtime: McpRuntime,

    /// Repository for `viz_saved_charts` and its child tables.
    pub saved_charts_repo: Arc<SavedChartsRepository>,
    /// Repository for `viz_saved_chart_series` child rows.
    pub saved_chart_series_repo: Arc<SavedChartSeriesRepository>,
    /// Repository for `viz_saved_chart_binding_y` child rows.
    pub saved_chart_binding_y_repo: Arc<SavedChartBindingYRepository>,
    /// Repository for `viz_dashboards`.
    pub dashboards_repo: Arc<DashboardsRepository>,
    /// Repository for `viz_dashboard_panels` child rows.
    pub dashboard_panels_repo: Arc<DashboardPanelsRepository>,
    /// Repository for `qry_saved_queries` and its child tables.
    pub saved_query_repo: Arc<SavedQueryRepo>,
}

impl AppState {
    pub fn new() -> Result<Self, dbflux_storage::error::StorageError> {
        let (built, storage_runtime, profiles, auth_profiles, proxies, ssh_tunnels) =
            Self::build_default_drivers();

        Self::new_with_drivers_and_settings(
            built.drivers,
            built.external_driver_diagnostics,
            built.general_settings,
            built.driver_overrides,
            built.driver_settings,
            built.hook_definitions,
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
            Self::build_default_drivers_with_runtime(storage_runtime);

        Self::new_with_drivers_and_settings(
            built.drivers,
            built.external_driver_diagnostics,
            built.general_settings,
            built.driver_overrides,
            built.driver_settings,
            built.hook_definitions,
            built.services,
            storage_runtime,
            profiles,
            auth_profiles,
            proxies,
            ssh_tunnels,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_drivers_and_settings(
        mut drivers: HashMap<String, Arc<dyn DbDriver>>,
        mut external_driver_diagnostics: HashMap<String, ExternalDriverDiagnostic>,
        general_settings: GeneralSettings,
        driver_overrides: HashMap<DriverKey, GlobalOverrides>,
        driver_settings: HashMap<DriverKey, FormValues>,
        hook_definitions: HashMap<String, ConnectionHook>,
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

        let (audit_service, audit_degraded) =
            match dbflux_audit::AuditService::new_sqlite(storage_runtime.dbflux_db_path()) {
                Ok(service) => (service, false),
                Err(e) => {
                    log::error!(
                        "Failed to initialize audit service at {:?}: {}",
                        storage_runtime.dbflux_db_path(),
                        e
                    );
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

        if !services.is_empty() {
            Self::launch_rpc_services(
                &mut drivers,
                &mut external_driver_diagnostics,
                services.clone(),
                Some(audit_emitter.clone()),
            );
        }

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

        let facade = SessionFacade::with_all_custom_managers_and_tree_store(
            drivers,
            profile_manager,
            ssh_manager,
            proxy_manager,
            auth_manager,
            HistoryManager::new(),
            SavedQueryManager::new(),
            tree_store,
        );

        let mut history_manager =
            crate::history_manager_sqlite::HistoryManager::new(&storage_runtime);
        history_manager.set_max_entries(general_settings.max_history_entries);

        #[cfg(feature = "mcp")]
        let mcp_runtime = {
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
        };

        // Construct viz repositories sharing a single connection.
        // Decision C.1: one shared Arc<Mutex<Connection>> is used for all five repos so
        // they serialize through the same lock, matching the pattern used by saved_filters.
        let viz_conn = storage_runtime.viz_connection()?;
        let saved_charts_repo = Arc::new(SavedChartsRepository::new(Arc::clone(&viz_conn)));
        let saved_chart_series_repo =
            Arc::new(SavedChartSeriesRepository::new(Arc::clone(&viz_conn)));
        let saved_chart_binding_y_repo =
            Arc::new(SavedChartBindingYRepository::new(Arc::clone(&viz_conn)));
        let dashboards_repo = Arc::new(DashboardsRepository::new(Arc::clone(&viz_conn)));
        let dashboard_panels_repo = Arc::new(DashboardPanelsRepository::new(Arc::clone(&viz_conn)));
        let saved_query_repo = Arc::new(SavedQueryRepo::new(Arc::clone(&viz_conn)));

        let mut state = Self {
            facade,
            external_driver_diagnostics,
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
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
        };

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

        Ok(state)
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

    /// Writes a profile's secret-kind field values to the OS keyring under
    /// per-field references. The matching SQLite rows store only the reference.
    ///
    /// Returns `true` only if every secret was actually persisted (vacuously
    /// true when the profile has none). A `false` result means at least one
    /// secret could not be written (e.g. the keyring is unavailable) and the
    /// caller should warn the user.
    fn persist_auth_secret_fields(&self, profile: &dbflux_core::AuthProfile) -> bool {
        let mut all_persisted = true;
        for (field_id, secret) in &profile.secret_fields {
            let secret_ref = dbflux_core::auth_field_secret_ref(&profile.id, field_id);
            if !self.facade.secrets.set_by_ref(&secret_ref, secret) {
                all_persisted = false;
            }
        }
        all_persisted
    }

    /// Deletes a profile's secret-kind field values from the OS keyring.
    fn delete_auth_secret_fields(&self, profile: &dbflux_core::AuthProfile) {
        for field_id in profile.secret_fields.keys() {
            let secret_ref = dbflux_core::auth_field_secret_ref(&profile.id, field_id);
            self.facade.secrets.delete_by_ref(&secret_ref);
        }
    }

    #[cfg(feature = "mcp")]
    fn bootstrap_mcp_runtime_from_persistence(&mut self) -> Result<(), String> {
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
            log::info!(
                "Audit purge_on_startup enabled (retention_days={}), running purge...",
                settings.retention_days
            );
            match self
                .audit_service
                .purge_old_events(settings.retention_days, 500)
            {
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
    fn build_default_drivers() -> (
        BuiltDrivers,
        dbflux_storage::bootstrap::StorageRuntime,
        Vec<ConnectionProfile>,
        Vec<AuthProfile>,
        Vec<ProxyProfile>,
        Vec<SshTunnelProfile>,
    ) {
        let runtime = dbflux_storage::bootstrap::initialize()
            .expect("failed to initialize internal storage — cannot continue");

        Self::build_default_drivers_with_runtime(runtime)
    }

    #[allow(clippy::result_large_err)]
    fn build_default_drivers_with_runtime(
        runtime: dbflux_storage::bootstrap::StorageRuntime,
    ) -> (
        BuiltDrivers,
        dbflux_storage::bootstrap::StorageRuntime,
        Vec<ConnectionProfile>,
        Vec<AuthProfile>,
        Vec<ProxyProfile>,
        Vec<SshTunnelProfile>,
    ) {
        let drivers = Self::build_builtin_drivers();
        let external_driver_diagnostics = HashMap::new();

        let (
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
            services,
            runtime,
        ) = Self::load_app_config_from_runtime(runtime);

        let loaded = crate::config_loader::load_config(&runtime);

        (
            BuiltDrivers {
                drivers,
                external_driver_diagnostics,
                general_settings,
                driver_overrides,
                driver_settings,
                hook_definitions,
                services,
            },
            runtime,
            loaded.profiles,
            loaded.auth_profiles,
            loaded.proxy_profiles,
            loaded.ssh_tunnels,
        )
    }

    #[allow(clippy::type_complexity)]
    fn load_app_config_from_runtime(
        runtime: dbflux_storage::bootstrap::StorageRuntime,
    ) -> (
        GeneralSettings,
        HashMap<DriverKey, GlobalOverrides>,
        HashMap<DriverKey, FormValues>,
        HashMap<String, ConnectionHook>,
        Vec<ServiceConfig>,
        dbflux_storage::bootstrap::StorageRuntime,
    ) {
        let loaded = crate::config_loader::load_config(&runtime);

        (
            loaded.general_settings,
            loaded.driver_overrides,
            loaded.driver_settings,
            loaded.hook_definitions,
            loaded.services,
            runtime,
        )
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
    fn launch_rpc_services_with<Probe, Build>(
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
    fn launch_rpc_auth_providers_with<Probe>(
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

    fn build_builtin_drivers() -> HashMap<String, Arc<dyn DbDriver>> {
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

    // --- ConnectionManager ---

    pub fn active_connection(&self) -> Option<&ConnectedProfile> {
        self.facade.connections.active_connection()
    }

    #[allow(dead_code)]
    pub fn is_connected(&self) -> bool {
        self.facade.connections.is_connected()
    }

    pub fn has_connections(&self) -> bool {
        self.facade.connections.has_connections()
    }

    #[allow(dead_code)]
    pub fn connection_display_name(&self) -> Option<&str> {
        self.facade.connections.connection_display_name()
    }

    #[allow(dead_code)]
    pub fn active_schema(&self) -> Option<&SchemaSnapshot> {
        self.facade.connections.active_schema()
    }

    pub fn get_connection(&self, profile_id: Uuid) -> Option<Arc<dyn Connection>> {
        self.facade.connections.get_connection(profile_id)
    }

    pub fn set_active_connection(&mut self, profile_id: Uuid) {
        self.facade.connections.set_active_connection(profile_id);
    }

    pub fn disconnect(&mut self, profile_id: Uuid) {
        self.facade.connections.disconnect(profile_id);
        // Evict stale metric catalog data for this connection.
        self.metric_catalog_cache.invalidate(profile_id);
        // Evict the cached remote dashboard listing for this connection.
        self.remote_dashboard_cache.invalidate(profile_id);
    }

    /// Access the session-scoped metric catalog cache.
    pub fn metric_catalog_cache(&self) -> &Arc<crate::metric_catalog_cache::MetricCatalogCache> {
        &self.metric_catalog_cache
    }

    /// Access the session-scoped remote dashboard listing cache.
    pub fn remote_dashboard_cache(
        &self,
    ) -> &Arc<crate::remote_dashboard_cache::RemoteDashboardCache> {
        &self.remote_dashboard_cache
    }

    #[allow(dead_code)]
    pub fn disconnect_all(&mut self) {
        self.facade.connections.disconnect_all();
    }

    // --- Schema cache ---

    #[allow(dead_code)]
    pub fn get_database_schema(&self, profile_id: Uuid, database: &str) -> Option<&DbSchemaInfo> {
        self.facade
            .connections
            .get_database_schema(profile_id, database)
    }

    pub fn set_database_schema(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: DbSchemaInfo,
    ) {
        self.facade
            .connections
            .set_database_schema(profile_id, database, schema);
    }

    pub fn needs_database_schema(&self, profile_id: Uuid, database: &str) -> bool {
        self.facade
            .connections
            .needs_database_schema(profile_id, database)
    }

    #[allow(dead_code)]
    pub fn get_table_details(
        &self,
        profile_id: Uuid,
        database: &str,
        table: &str,
    ) -> Option<&dbflux_core::TableInfo> {
        self.facade
            .connections
            .get_table_details(profile_id, database, table)
    }

    #[allow(dead_code)]
    pub fn set_table_details(
        &mut self,
        profile_id: Uuid,
        database: String,
        table: String,
        details: dbflux_core::TableInfo,
    ) {
        self.facade
            .connections
            .set_table_details(profile_id, database, table, details);
    }

    pub fn set_dependents(
        &mut self,
        profile_id: Uuid,
        database: String,
        table: String,
        deps: Vec<dbflux_core::RelationRef>,
    ) {
        self.facade
            .connections
            .set_dependents(profile_id, database, table, deps);
    }

    #[allow(dead_code)]
    pub fn needs_table_details(&self, profile_id: Uuid, database: &str, table: &str) -> bool {
        self.facade
            .connections
            .needs_table_details(profile_id, database, table)
    }

    #[allow(dead_code)]
    pub fn get_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Option<&Vec<dbflux_core::CustomTypeInfo>> {
        self.facade
            .connections
            .get_schema_types(profile_id, database, schema)
    }

    pub fn set_schema_types(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        types: Vec<dbflux_core::CustomTypeInfo>,
    ) {
        self.facade
            .connections
            .set_schema_types(profile_id, database, schema, types);
    }

    pub fn needs_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_types(profile_id, database, schema)
    }

    pub fn set_schema_indexes(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        indexes: Vec<SchemaIndexInfo>,
    ) {
        self.facade
            .connections
            .set_schema_indexes(profile_id, database, schema, indexes);
    }

    pub fn needs_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_indexes(profile_id, database, schema)
    }

    pub fn set_schema_foreign_keys(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
    ) {
        self.facade
            .connections
            .set_schema_foreign_keys(profile_id, database, schema, foreign_keys);
    }

    pub fn needs_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_foreign_keys(profile_id, database, schema)
    }

    pub fn set_schema_routines(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        routines: Vec<dbflux_core::RoutineInfo>,
    ) {
        self.facade
            .connections
            .set_schema_routines(profile_id, database, schema, routines);
    }

    pub fn needs_schema_routines(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_routines(profile_id, database, schema)
    }

    pub fn prepare_fetch_schema_routines(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaRoutinesParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_routines(profile_id, database, schema)
    }

    #[allow(dead_code)]
    pub fn get_active_database(&self, profile_id: Uuid) -> Option<String> {
        self.facade.connections.get_active_database(profile_id)
    }

    pub fn set_active_database(&mut self, profile_id: Uuid, database: Option<String>) {
        self.facade
            .connections
            .set_active_database(profile_id, database);
    }

    // --- Redis key cache ---

    #[allow(dead_code)]
    pub fn get_redis_cached_keys(&self, profile_id: Uuid, keyspace: &str) -> Option<Arc<[String]>> {
        self.facade
            .connections
            .connections
            .get(&profile_id)
            .and_then(|conn| conn.redis_key_cache.get_keys(keyspace))
    }

    pub fn set_redis_cached_keys(&mut self, profile_id: Uuid, keyspace: String, keys: Vec<String>) {
        if let Some(conn) = self.facade.connections.connections.get_mut(&profile_id) {
            conn.redis_key_cache.set_keys(keyspace, keys);
        }
    }

    #[allow(dead_code)]
    pub fn redis_keys_stale(&self, profile_id: Uuid, keyspace: &str) -> bool {
        self.facade
            .connections
            .connections
            .get(&profile_id)
            .map(|conn| conn.redis_key_cache.is_stale(keyspace))
            .unwrap_or(true)
    }

    // --- Pending operations ---

    pub fn is_operation_pending(&self, profile_id: Uuid, database: Option<&str>) -> bool {
        self.facade
            .connections
            .is_operation_pending(profile_id, database)
    }

    pub fn start_pending_operation(&mut self, profile_id: Uuid, database: Option<&str>) -> bool {
        self.facade
            .connections
            .start_pending_operation(profile_id, database)
    }

    pub fn finish_pending_operation(&mut self, profile_id: Uuid, database: Option<&str>) {
        self.facade
            .connections
            .finish_pending_operation(profile_id, database);
    }

    // --- Prepare/Apply ---

    pub fn prepare_connect_profile(
        &self,
        profile_id: Uuid,
    ) -> Result<ConnectProfileParams, dbflux_core::PrepareConnectError> {
        self.prepare_connect_profile_with_passphrase(profile_id, None)
    }

    /// Like `prepare_connect_profile` but allows supplying an explicit SSH
    /// passphrase (from the tunnel-auth modal) that overrides both the session
    /// vault and the OS keyring.
    pub fn prepare_connect_profile_with_passphrase(
        &self,
        profile_id: Uuid,
        override_passphrase: Option<&str>,
    ) -> Result<ConnectProfileParams, dbflux_core::PrepareConnectError> {
        let secrets = &self.facade.secrets;

        let proxy_secret = {
            let profile = self
                .facade
                .profiles
                .profiles
                .iter()
                .find(|p| p.id == profile_id);
            match profile {
                Some(p) => secrets.get_proxy_secret_for_profile(p, &self.facade.proxies.items),
                None => None,
            }
        };

        // Priority: explicit override > session vault > OS keyring.
        let vault = self.session_passphrase_vault.clone();
        let override_passphrase = override_passphrase.map(str::to_owned);

        // Capture what we need from secrets before the closure; secrets itself
        // cannot be moved into the closure because it borrows self.
        let keyring_ssh_secret: Option<SecretString> = {
            let profile = self
                .facade
                .profiles
                .profiles
                .iter()
                .find(|p| p.id == profile_id);
            match profile {
                Some(p) => secrets.get_ssh_secret_for_profile(p, &self.facade.ssh_tunnels.items),
                None => None,
            }
        };

        self.facade.connections.prepare_connect_profile(
            profile_id,
            &self.facade.profiles.profiles,
            &self.facade.ssh_tunnels.items,
            &self.facade.proxies.items,
            &secrets.secret_store_arc(),
            move |profile, _ssh_tunnels| {
                use dbflux_core::secrecy::SecretString;

                // An explicit passphrase (from the modal) always wins.
                if let Some(ref p) = override_passphrase {
                    return Some(SecretString::from(p.clone()));
                }

                // Check the session vault for a remembered passphrase.
                let tunnel_id = profile.config.ssh_tunnel_profile_id();
                if let Some(id) = tunnel_id
                    && let Ok(guard) = vault.read()
                    && let Some(vault_pass) = guard.get(&id)
                {
                    return Some(SecretString::from(vault_pass.to_owned()));
                }

                // Fall back to the OS keyring result captured above.
                keyring_ssh_secret
            },
            proxy_secret,
        )
    }

    /// Resolve the SSH tunnel profile ID associated with a connection profile, if any.
    pub fn ssh_tunnel_id_for_profile(&self, profile_id: Uuid) -> Option<Uuid> {
        self.facade
            .profiles
            .profiles
            .iter()
            .find(|p| p.id == profile_id)
            .and_then(|p| p.config.ssh_tunnel_profile_id())
    }

    /// Retrieve the SSH tunnel profile for the given tunnel ID, if it exists.
    pub fn ssh_tunnel_profile(&self, tunnel_id: Uuid) -> Option<&SshTunnelProfile> {
        self.facade
            .ssh_tunnels
            .items
            .iter()
            .find(|t| t.id == tunnel_id)
    }

    /// Retrieve the remembered passphrase for the given SSH tunnel profile ID, if any.
    pub fn passphrase_for(&self, tunnel_id: Uuid) -> Option<String> {
        self.session_passphrase_vault
            .read()
            .ok()
            .and_then(|guard| guard.get(&tunnel_id).map(str::to_owned))
    }

    /// Store a passphrase for the given SSH tunnel profile ID for the rest of the process lifetime.
    ///
    /// The passphrase is held only in memory and is never persisted to disk.
    pub fn cache_passphrase(&self, tunnel_id: Uuid, passphrase: String) {
        if let Ok(mut guard) = self.session_passphrase_vault.write() {
            guard.insert(tunnel_id, passphrase);
        }
    }

    pub fn apply_connect_profile(
        &mut self,
        profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
        proxy_tunnel: Option<Box<dyn std::any::Any + Send + Sync>>,
        is_mcp_actor: bool,
    ) {
        self.facade.connections.apply_connect_profile(
            profile,
            connection,
            schema,
            proxy_tunnel,
            is_mcp_actor,
        );
    }

    pub fn prepare_database_connection(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<SwitchDatabaseParams, String> {
        self.facade.connections.prepare_database_connection(
            profile_id,
            database,
            &self.facade.secrets.secret_store_arc(),
        )
    }

    #[allow(dead_code)]
    pub fn prepare_switch_database(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<SwitchDatabaseParams, String> {
        self.facade.connections.prepare_switch_database(
            profile_id,
            database,
            &self.facade.secrets.secret_store_arc(),
        )
    }

    #[allow(dead_code)]
    pub fn apply_switch_database(
        &mut self,
        profile_id: Uuid,
        original_profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.facade.connections.apply_switch_database(
            profile_id,
            original_profile,
            connection,
            schema,
        );
    }

    pub fn add_database_connection(
        &mut self,
        profile_id: Uuid,
        database: String,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.facade
            .connections
            .add_database_connection(profile_id, database, connection, schema);
    }

    pub fn prepare_fetch_database_schema(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<FetchDatabaseSchemaParams, String> {
        self.facade
            .connections
            .prepare_fetch_database_schema(profile_id, database)
    }

    #[allow(dead_code)]
    pub fn prepare_fetch_table_details(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<FetchTableDetailsParams, String> {
        self.facade
            .connections
            .prepare_fetch_table_details(profile_id, database, schema, table)
    }

    pub fn prepare_fetch_collection_children(
        &self,
        profile_id: Uuid,
        database: &str,
        collection: &str,
        limit: u32,
    ) -> Result<FetchCollectionChildrenParams, String> {
        self.facade
            .connections
            .prepare_fetch_collection_children(profile_id, database, collection, limit)
    }

    pub fn set_collection_children_page(
        &mut self,
        profile_id: Uuid,
        database: String,
        collection: String,
        page: dbflux_core::CollectionChildrenPage,
    ) {
        self.facade
            .connections
            .set_collection_children_page(profile_id, database, collection, page);
    }

    pub fn prepare_fetch_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaTypesParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_types(profile_id, database, schema)
    }

    pub fn prepare_fetch_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaIndexesParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_indexes(profile_id, database, schema)
    }

    pub fn prepare_fetch_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaForeignKeysParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_foreign_keys(profile_id, database, schema)
    }

    // --- SecretManager ---

    pub fn secret_store_available(&self) -> bool {
        self.facade.secrets.is_available()
    }

    #[allow(dead_code)]
    pub fn secret_store(&self) -> Arc<RwLock<Box<dyn SecretStore>>> {
        self.facade.secrets.secret_store_arc()
    }

    pub fn save_password(&self, profile: &ConnectionProfile, password: &SecretString) {
        self.facade.secrets.save_password(profile, password);
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        self.facade.secrets.delete_password(profile);
    }

    pub fn get_password(&self, profile: &ConnectionProfile) -> Option<SecretString> {
        self.facade.secrets.get_password(profile)
    }

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<SecretString> {
        self.facade.secrets.get_ssh_password(profile)
    }

    pub fn save_ssh_password(&self, profile: &ConnectionProfile, secret: &SecretString) {
        self.facade.secrets.save_ssh_password(profile, secret);
    }

    pub fn delete_ssh_password(&self, profile: &ConnectionProfile) {
        self.facade.secrets.delete_ssh_password(profile);
    }

    pub fn get_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) -> Option<SecretString> {
        self.facade.secrets.get_ssh_tunnel_secret(tunnel)
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &SecretString) {
        self.facade.secrets.save_ssh_tunnel_secret(tunnel, secret);
    }

    fn record_config_event(
        &self,
        outcome: EventOutcome,
        action: dbflux_core::observability::AuditAction,
        object_type: &'static str,
        object_id: String,
        summary: String,
        error_message: Option<String>,
    ) {
        let severity = match outcome {
            EventOutcome::Failure => EventSeverity::Error,
            EventOutcome::Cancelled => EventSeverity::Warn,
            EventOutcome::Pending => EventSeverity::Info,
            EventOutcome::Success => EventSeverity::Info,
        };

        let mut event = EventRecord::new(
            dbflux_core::chrono::Utc::now().timestamp_millis(),
            severity,
            EventCategory::Config,
            outcome,
        );
        event = event.with_origin(EventOrigin::local());

        let mut event = event
            .with_action(action.as_str())
            .with_summary(summary)
            .with_actor_id("local")
            .with_object_ref(object_type, object_id);

        if let Some(error_message) = error_message {
            event.error_message = Some(error_message);
        }

        if let Err(error) = self.audit_service.record(event) {
            log::warn!(
                "Failed to record {} audit event for {}: {}",
                action.as_str(),
                object_type,
                error
            );
        }
    }

    // --- ProfileManager ---

    pub fn add_profile_in_folder(&mut self, profile: ConnectionProfile, folder_id: Option<Uuid>) {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        self.facade.add_profile_in_folder(profile, folder_id);

        self.record_config_event(
            EventOutcome::Success,
            CONFIG_CREATE,
            "connection_profile",
            profile_id,
            format!("Created connection profile '{}'", profile_name),
            None,
        );

        // Persist the new profile to disk. The in-memory ProfileManager is
        // constructed with `None` for its JsonStore, so its `save()` is a
        // no-op; the app drives persistence through `save_profiles()`.
        self.save_profiles();
    }

    pub fn remove_profile(&mut self, idx: usize) -> Option<ConnectionProfile> {
        let removed = self.facade.remove_profile(idx)?;

        self.record_config_event(
            EventOutcome::Success,
            CONFIG_DELETE,
            "connection_profile",
            removed.id.to_string(),
            format!("Deleted connection profile '{}'", removed.name),
            None,
        );

        // Delete the row from SQLite. `save_profiles()` is upsert-only over
        // the *remaining* in-memory profiles — it will not remove a row
        // whose profile is no longer in memory, so without this explicit
        // delete the deleted profile reappears on next launch.
        if let Err(e) = self
            .storage_runtime
            .connection_profiles()
            .delete(&removed.id.to_string())
        {
            log::error!("Failed to delete profile from storage: {}", e);
        }

        Some(removed)
    }

    pub fn update_profile(&mut self, profile: ConnectionProfile) {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        self.facade.profiles.update(profile);

        self.record_config_event(
            EventOutcome::Success,
            CONFIG_UPDATE,
            "connection_profile",
            profile_id,
            format!("Updated connection profile '{}'", profile_name),
            None,
        );

        // Persist the edit to disk. Previously this only worked as a side
        // effect of `persist_mcp_governance()` after the form save; if MCP
        // was disabled or the call path skipped that step, the edit would
        // not survive a restart.
        self.save_profiles();
    }

    pub fn save_profiles(&self) {
        if let Err(e) = crate::config_loader::save_profiles(
            &self.storage_runtime,
            &self.facade.profiles.profiles,
        ) {
            log::error!("Failed to save connection profiles: {}", e);
        }
    }

    // --- SshTunnelManager ---

    pub fn add_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        let tunnel_name = tunnel.name.clone();
        let tunnel_id = tunnel.id.to_string();
        self.facade.ssh_tunnels.items.push(tunnel.clone());
        let save_result = crate::config_loader::save_ssh_tunnels(
            &self.storage_runtime,
            &self.facade.ssh_tunnels.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_CREATE,
            "ssh_tunnel_profile",
            tunnel_id,
            format!("Created SSH tunnel '{}'", tunnel_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save SSH tunnel profiles: {}", e);
        }
    }

    #[allow(dead_code)]
    pub fn remove_ssh_tunnel(&mut self, idx: usize) -> Option<SshTunnelProfile> {
        let removed = self.facade.remove_ssh_tunnel(idx)?;
        let tunnel_name = removed.name.clone();
        let tunnel_id = removed.id.to_string();

        let save_result = crate::config_loader::save_ssh_tunnels(
            &self.storage_runtime,
            &self.facade.ssh_tunnels.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_DELETE,
            "ssh_tunnel_profile",
            tunnel_id,
            format!("Deleted SSH tunnel '{}'", tunnel_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save SSH tunnel profiles after remove: {}", e);
        }
        Some(removed)
    }

    #[allow(dead_code)]
    pub fn update_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        let tunnel_name = tunnel.name.clone();
        let tunnel_id = tunnel.id.to_string();
        if let Some(existing) = self
            .facade
            .ssh_tunnels
            .items
            .iter_mut()
            .find(|t| t.id == tunnel.id)
        {
            *existing = tunnel.clone();
            let save_result = crate::config_loader::save_ssh_tunnels(
                &self.storage_runtime,
                &self.facade.ssh_tunnels.items,
            );

            let (outcome, error_msg) = match &save_result {
                Ok(()) => (EventOutcome::Success, None),
                Err(e) => (EventOutcome::Failure, Some(e.to_string())),
            };

            self.record_config_event(
                outcome,
                CONFIG_UPDATE,
                "ssh_tunnel_profile",
                tunnel_id,
                format!("Updated SSH tunnel '{}'", tunnel_name),
                error_msg,
            );

            if let Err(e) = save_result {
                log::error!("Failed to save SSH tunnel profiles: {}", e);
            }
        }
    }

    // --- ProxyManager ---

    pub fn add_proxy(&mut self, proxy: dbflux_core::ProxyProfile) {
        let proxy_name = proxy.name.clone();
        let proxy_id = proxy.id.to_string();
        self.facade.proxies.items.push(proxy.clone());
        let save_result = crate::config_loader::save_proxy_profiles(
            &self.storage_runtime,
            &self.facade.proxies.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_CREATE,
            "proxy_profile",
            proxy_id,
            format!("Created proxy '{}'", proxy_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save proxy profiles: {}", e);
        }
    }

    pub fn remove_proxy(&mut self, idx: usize) -> Option<dbflux_core::ProxyProfile> {
        let removed = self.facade.remove_proxy(idx)?;
        let proxy_name = removed.name.clone();
        let proxy_id = removed.id.to_string();

        let save_result = crate::config_loader::save_proxy_profiles(
            &self.storage_runtime,
            &self.facade.proxies.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_DELETE,
            "proxy_profile",
            proxy_id,
            format!("Deleted proxy '{}'", proxy_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save proxy profiles after remove: {}", e);
        }
        Some(removed)
    }

    pub fn update_proxy(&mut self, proxy: dbflux_core::ProxyProfile) {
        let proxy_name = proxy.name.clone();
        let proxy_id = proxy.id.to_string();
        if let Some(existing) = self
            .facade
            .proxies
            .items
            .iter_mut()
            .find(|p| p.id == proxy.id)
        {
            *existing = proxy.clone();
            let save_result = crate::config_loader::save_proxy_profiles(
                &self.storage_runtime,
                &self.facade.proxies.items,
            );

            let (outcome, error_msg) = match &save_result {
                Ok(()) => (EventOutcome::Success, None),
                Err(e) => (EventOutcome::Failure, Some(e.to_string())),
            };

            self.record_config_event(
                outcome,
                CONFIG_UPDATE,
                "proxy_profile",
                proxy_id,
                format!("Updated proxy '{}'", proxy_name),
                error_msg,
            );

            if let Err(e) = save_result {
                log::error!("Failed to save proxy profiles: {}", e);
            }
        }
    }

    pub fn get_proxy_secret(&self, proxy: &dbflux_core::ProxyProfile) -> Option<SecretString> {
        self.facade.secrets.get_proxy_secret(proxy)
    }

    pub fn save_proxy_secret(&self, proxy: &dbflux_core::ProxyProfile, secret: &SecretString) {
        self.facade.secrets.save_proxy_secret(proxy, secret);
    }

    pub fn delete_proxy_secret(&self, proxy: &dbflux_core::ProxyProfile) {
        self.facade.secrets.delete_proxy_secret(proxy);
    }

    // --- AuthProfileManager ---

    /// Adds a stored auth profile. Returns `true` if its secret-kind fields (if
    /// any) were persisted to the keyring; `false` means the secret could not be
    /// stored and the caller should warn the user.
    pub fn add_auth_profile(&mut self, profile: dbflux_core::AuthProfile) -> bool {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        self.facade.auth_profiles.items.push(profile.clone());
        let save_result = crate::config_loader::save_auth_profiles(
            &self.storage_runtime,
            &self.facade.auth_profiles.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_CREATE,
            "auth_profile",
            profile_id,
            format!("Created auth profile '{}'", profile_name),
            error_msg,
        );

        match save_result {
            Ok(()) => self.persist_auth_secret_fields(&profile),
            Err(e) => {
                log::error!("Failed to save auth profiles: {}", e);
                false
            }
        }
    }

    pub fn remove_auth_profile(&mut self, idx: usize) -> Option<dbflux_core::AuthProfile> {
        if idx >= self.facade.auth_profiles.items.len() {
            return None;
        }
        let removed = self.facade.auth_profiles.items.remove(idx);
        let profile_name = removed.name.clone();
        let profile_id = removed.id.to_string();

        let save_result = crate::config_loader::save_auth_profiles(
            &self.storage_runtime,
            &self.facade.auth_profiles.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_DELETE,
            "auth_profile",
            profile_id,
            format!("Deleted auth profile '{}'", profile_name),
            error_msg,
        );

        match save_result {
            Ok(()) => self.delete_auth_secret_fields(&removed),
            Err(e) => log::error!("Failed to save auth profiles after remove: {}", e),
        }
        Some(removed)
    }

    /// Updates a stored auth profile. Returns `true` if its secret-kind fields
    /// (if any) were persisted to the keyring; `false` means the secret could
    /// not be stored and the caller should warn the user. A profile that is not
    /// present is a no-op and returns `true`.
    pub fn update_auth_profile(&mut self, profile: dbflux_core::AuthProfile) -> bool {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();

        // Delete keyring entries for secret fields that the updated profile no
        // longer carries, so a removed or replaced secret does not linger in the
        // OS keyring as orphaned credential material.
        let old_secret_keys: Vec<String> = self
            .facade
            .auth_profiles
            .items
            .iter()
            .find(|i| i.id == profile.id)
            .map(|existing| existing.secret_fields.keys().cloned().collect())
            .unwrap_or_default();

        for field_id in old_secret_keys
            .iter()
            .filter(|key| !profile.secret_fields.contains_key(*key))
        {
            let secret_ref = dbflux_core::auth_field_secret_ref(&profile.id, field_id);
            self.facade.secrets.delete_by_ref(&secret_ref);
        }

        let Some(existing) = self
            .facade
            .auth_profiles
            .items
            .iter_mut()
            .find(|i| i.id == profile.id)
        else {
            return true;
        };

        *existing = profile.clone();
        let save_result = crate::config_loader::save_auth_profiles(
            &self.storage_runtime,
            &self.facade.auth_profiles.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_UPDATE,
            "auth_profile",
            profile_id,
            format!("Updated auth profile '{}'", profile_name),
            error_msg,
        );

        match save_result {
            Ok(()) => self.persist_auth_secret_fields(&profile),
            Err(e) => {
                log::error!("Failed to save auth profiles: {}", e);
                false
            }
        }
    }

    /// Returns all auth profiles visible to the application: stored non-AWS profiles
    /// unioned with AWS profiles reflected live from `~/.aws/config` and
    /// `~/.aws/credentials`.
    ///
    /// This is the single read seam for auth profiles. All callers must use this
    /// method instead of reading `facade.auth_profiles.items` directly, so that
    /// reflected AWS profiles are always included.
    pub fn list_auth_profiles(&self) -> Vec<dbflux_core::AuthProfile> {
        const AWS_REFLECTED_PROVIDER_IDS: &[&str] =
            &["aws-sso", "aws-sso-session", "aws-shared-credentials"];

        let mut profiles: Vec<dbflux_core::AuthProfile> = self
            .facade
            .auth_profiles
            .items
            .iter()
            .filter(|p| !AWS_REFLECTED_PROVIDER_IDS.contains(&p.provider_id.as_str()))
            .cloned()
            .collect();

        for provider in self.auth_provider_registry.providers() {
            profiles.extend(provider.reflect_profiles());
        }

        profiles
    }

    /// Returns only profiles stored in the SQLite database.
    ///
    /// Reflected AWS profiles (from `~/.aws/config`) are NOT included.
    /// All callers that resolve auth identity at connect time, dropdown
    /// fetch time, or SSO login time MUST use `list_auth_profiles()` instead
    /// so that reflected profiles participate in resolution.
    ///
    /// This method is retained only for callers that genuinely need the raw
    /// stored slice: migration code, config loaders that write back to storage,
    /// and MCP state that lists persisted profiles only.
    #[deprecated(note = "use list_auth_profiles(); stored-only view hides reflected AWS profiles")]
    pub fn auth_profiles(&self) -> &[dbflux_core::AuthProfile] {
        &self.facade.auth_profiles.items
    }

    /// Returns a clone of the re-hydrated secret-kind fields for a stored
    /// profile, used by the Settings UI to preserve a WriteOnly secret the user
    /// left blank when re-saving. `None` if no stored profile matches `id`.
    pub fn stored_auth_profile_secret_fields(
        &self,
        id: Uuid,
    ) -> Option<HashMap<String, SecretString>> {
        self.facade
            .auth_profiles
            .items
            .iter()
            .find(|profile| profile.id == id)
            .map(|profile| profile.secret_fields.clone())
    }

    // --- ConnectionTreeManager ---

    pub fn save_connection_tree(&self) {
        self.facade.tree.save();
    }

    pub fn create_folder(&mut self, name: impl Into<String>, parent_id: Option<Uuid>) -> Uuid {
        self.facade.tree.create_folder(name, parent_id)
    }

    pub fn rename_folder(&mut self, folder_id: Uuid, new_name: impl Into<String>) -> bool {
        self.facade.tree.rename_folder(folder_id, new_name)
    }

    pub fn delete_folder(&mut self, folder_id: Uuid) -> Vec<Uuid> {
        self.facade.tree.delete_folder(folder_id)
    }

    pub fn move_tree_node(&mut self, node_id: Uuid, new_parent_id: Option<Uuid>) -> bool {
        self.facade.tree.move_node(node_id, new_parent_id)
    }

    pub fn move_tree_node_to_position(
        &mut self,
        node_id: Uuid,
        new_parent_id: Option<Uuid>,
        after_id: Option<Uuid>,
    ) -> bool {
        self.facade
            .tree
            .move_node_to_position(node_id, new_parent_id, after_id)
    }

    #[allow(dead_code)]
    pub fn toggle_folder_collapsed(&mut self, folder_id: Uuid) -> Option<bool> {
        self.facade.tree.toggle_folder_collapsed(folder_id)
    }

    pub fn set_folder_collapsed(&mut self, folder_id: Uuid, collapsed: bool) {
        self.facade.tree.set_folder_collapsed(folder_id, collapsed);
    }

    // --- HistoryManager (SQLite-backed via history_manager_sqlite) ---

    pub fn history_entries(&self) -> &[HistoryEntry] {
        self.history_manager.entries()
    }

    pub fn add_history_entry(&mut self, entry: HistoryEntry) {
        self.history_manager.add(entry);
    }

    #[allow(dead_code)]
    pub fn toggle_history_favorite(&mut self, id: Uuid) -> bool {
        self.history_manager.toggle_favorite(id)
    }

    #[allow(dead_code)]
    pub fn remove_history_entry(&mut self, id: Uuid) {
        self.history_manager.remove(id);
    }

    // --- SavedQueryManager (SQLite-backed via history_manager_sqlite) ---

    #[allow(dead_code)]
    pub fn take_saved_query_warning(&mut self) -> Option<String> {
        None
    }

    pub fn add_saved_query(&mut self, query: SavedQuery) {
        self.history_manager.add_saved_query(query);
    }

    pub fn update_saved_query(&mut self, id: Uuid, name: String, sql: String) -> bool {
        self.history_manager.update_saved_query(id, name, sql)
    }

    pub fn remove_saved_query(&mut self, id: Uuid) -> bool {
        self.history_manager.remove_saved_query(id)
    }

    pub fn toggle_saved_query_favorite(&mut self, id: Uuid) -> bool {
        self.history_manager.toggle_saved_query_favorite(id)
    }

    pub fn update_saved_query_last_used(&mut self, id: Uuid) -> bool {
        self.history_manager.update_saved_query_last_used(id)
    }

    #[allow(dead_code)]
    pub fn update_saved_query_sql(&mut self, id: Uuid, sql: &str) -> bool {
        self.history_manager.update_saved_query_sql(id, sql)
    }

    #[allow(dead_code)]
    pub fn update_saved_query_name(&mut self, id: Uuid, name: &str) -> bool {
        self.history_manager.update_saved_query_name(id, name)
    }

    #[allow(dead_code)]
    pub fn get_saved_query(&self, id: Uuid) -> Option<&SavedQuery> {
        self.history_manager.get_saved_query(id)
    }

    pub fn saved_queries(&self) -> &[SavedQuery] {
        self.history_manager.saved_queries_list()
    }

    // --- RecentFiles (SQLite-backed) ---

    #[allow(dead_code)]
    pub fn recent_files(&self) -> &[dbflux_core::RecentFile] {
        self.history_manager.recent_files_entries()
    }

    pub fn record_recent_file(&mut self, path: PathBuf) {
        self.history_manager.record_recent_file(path);
    }

    #[allow(dead_code)]
    pub fn remove_recent_file(&mut self, path: &PathBuf) {
        self.history_manager.remove_recent_file(path);
    }

    // --- ScriptsDirectory ---

    pub fn scripts_directory(&self) -> Option<&ScriptsDirectory> {
        self.scripts_directory.as_ref()
    }

    pub fn scripts_directory_mut(&mut self) -> Option<&mut ScriptsDirectory> {
        self.scripts_directory.as_mut()
    }

    pub fn refresh_scripts(&mut self) {
        if let Some(dir) = self.scripts_directory.as_mut() {
            dir.refresh();
        }
    }

    // --- ArtifactStore (filesystem boundary for scratch/shadow) ---

    pub fn scratch_path(&self, doc_id: &str, extension: &str) -> std::path::PathBuf {
        self.storage_runtime.scratch_path(doc_id, extension)
    }

    pub fn shadow_path(&self, doc_id: &str) -> std::path::PathBuf {
        self.storage_runtime.shadow_path(doc_id)
    }

    // --- TaskManager ---

    pub fn start_task(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
    ) -> (TaskId, CancelToken) {
        self.facade.tasks.start(kind, description)
    }

    pub fn start_task_for_target(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        target: Option<dbflux_core::TaskTarget>,
    ) -> (TaskId, CancelToken) {
        self.facade
            .tasks
            .start_for_target(kind, description, target)
    }

    pub fn start_task_for_profile(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        profile_id: Option<Uuid>,
    ) -> (TaskId, CancelToken) {
        let target = profile_id.map(|profile_id| dbflux_core::TaskTarget {
            profile_id,
            database: None,
        });

        self.start_task_for_target(kind, description, target)
    }

    pub fn start_hook_task_for_profile(
        &mut self,
        phase: HookPhase,
        profile_id: Uuid,
        profile_name: &str,
        command: &str,
    ) -> (TaskId, CancelToken) {
        self.start_task_for_profile(
            TaskKind::Hook { phase },
            format!("Hook: {} — {} — {}", phase.label(), profile_name, command),
            Some(profile_id),
        )
    }

    pub fn complete_task(&mut self, id: TaskId) {
        self.facade.tasks.complete(id);
    }

    pub fn complete_task_with_details(&mut self, id: TaskId, details: impl Into<String>) {
        self.facade.tasks.complete_with_details(id, details);
    }

    pub fn append_task_details(&mut self, id: TaskId, details: impl AsRef<str>) {
        self.facade.tasks.append_details(id, details);
    }

    pub fn fail_task(&mut self, id: TaskId, error: impl Into<String>) {
        self.facade.tasks.fail(id, error);
    }

    pub fn fail_task_with_details(
        &mut self,
        id: TaskId,
        error: impl Into<String>,
        details: impl Into<String>,
    ) {
        self.facade.tasks.fail_with_details(id, error, details);
    }

    #[allow(dead_code)]
    pub fn cancel_task(&mut self, id: TaskId) -> bool {
        self.facade.tasks.cancel(id)
    }

    pub fn register_detached_hook_task(&mut self, profile_id: Uuid, task_id: TaskId) {
        self.detached_hook_tasks
            .entry(profile_id)
            .or_default()
            .insert(task_id);
    }

    pub fn unregister_detached_hook_task(&mut self, profile_id: Uuid, task_id: TaskId) {
        if let Some(tasks) = self.detached_hook_tasks.get_mut(&profile_id) {
            tasks.remove(&task_id);

            if tasks.is_empty() {
                self.detached_hook_tasks.remove(&profile_id);
            }
        }
    }

    pub fn cancel_detached_hook_tasks(&mut self, profile_id: Uuid) -> usize {
        let Some(task_ids) = self.detached_hook_tasks.remove(&profile_id) else {
            return 0;
        };

        task_ids
            .into_iter()
            .filter(|task_id| self.facade.tasks.cancel(*task_id))
            .count()
    }

    pub fn cancel_all_detached_hook_tasks(&mut self) -> usize {
        let profile_ids: Vec<Uuid> = self.detached_hook_tasks.keys().copied().collect();

        profile_ids
            .into_iter()
            .map(|profile_id| self.cancel_detached_hook_tasks(profile_id))
            .sum()
    }

    #[allow(dead_code)]
    pub fn running_tasks(&self) -> Vec<TaskSnapshot> {
        self.facade.tasks.running_tasks()
    }

    pub fn has_running_tasks(&self) -> bool {
        self.facade.tasks.has_running_tasks()
    }

    // --- Shutdown ---

    pub fn begin_shutdown(&self) -> bool {
        self.facade.begin_shutdown()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.facade.is_shutting_down()
    }

    pub fn shutdown_phase(&self) -> ShutdownPhase {
        self.facade.shutdown_phase()
    }

    pub fn cancel_all_tasks(&mut self) -> usize {
        self.facade.cancel_all_tasks()
    }

    pub fn close_all_connections(&mut self) {
        self.cancel_all_detached_hook_tasks();
        self.facade.close_all_connections();
    }

    pub fn complete_shutdown(&self) {
        self.facade.complete_shutdown();
    }

    #[allow(dead_code)]
    pub fn fail_shutdown(&self) {
        self.facade.fail_shutdown();
    }
}

// --- Field accessors ---

impl AppState {
    pub fn build_hook_context(&self, profile: &ConnectionProfile) -> HookContext {
        HookContext::from_profile(profile)
    }

    pub fn drivers(&self) -> &HashMap<String, Arc<dyn DbDriver>> {
        &self.facade.connections.drivers
    }

    pub fn storage_runtime(&self) -> &StorageRuntime {
        &self.storage_runtime
    }

    pub fn external_driver_diagnostic(&self, socket_id: &str) -> Option<&ExternalDriverDiagnostic> {
        self.external_driver_diagnostics.get(socket_id)
    }

    pub fn driver_for_profile(&self, profile: &ConnectionProfile) -> Option<Arc<dyn DbDriver>> {
        self.facade
            .connections
            .drivers
            .get(&profile.driver_id())
            .cloned()
    }

    pub fn profiles(&self) -> &[ConnectionProfile] {
        &self.facade.profiles.profiles
    }

    pub fn profiles_mut(&mut self) -> &mut Vec<ConnectionProfile> {
        &mut self.facade.profiles.profiles
    }

    pub fn ssh_tunnels(&self) -> &[SshTunnelProfile] {
        &self.facade.ssh_tunnels.items
    }

    pub fn proxies(&self) -> &[dbflux_core::ProxyProfile] {
        &self.facade.proxies.items
    }

    pub fn connections(&self) -> &HashMap<Uuid, ConnectedProfile> {
        &self.facade.connections.connections
    }

    pub fn remove_database_connection(&mut self, profile_id: Uuid, database: &str) -> bool {
        self.facade
            .connections
            .remove_database_connection(profile_id, database)
    }

    pub fn cancel_query_for_target(&self, target: &dbflux_core::TaskTarget) {
        let Some(connection) = self.facade.connections.connection_for_task_target(target) else {
            return;
        };

        let cancel_handle = connection.cancel_handle();
        if let Err(error) = cancel_handle.cancel() {
            log::warn!("Failed to send cancel via handle: {}", error);
        }

        if let Err(error) = connection.cancel_active() {
            log::warn!("Failed to send cancel to database: {}", error);
        }
    }

    pub fn cancel_running_connect_tasks_for_profile(&mut self, profile_id: Uuid) -> usize {
        let connect_task_ids: Vec<TaskId> = self
            .facade
            .tasks
            .running_tasks()
            .into_iter()
            .filter(|task| task.kind == TaskKind::Connect && task.profile_id == Some(profile_id))
            .map(|task| task.id)
            .collect();

        let cancelled = connect_task_ids
            .into_iter()
            .filter(|task_id| self.facade.tasks.cancel(*task_id))
            .count();

        // Clear the profile-level pending-operation entry so the sidebar can
        // reflect the cancelled state and the user can retry without waiting
        // for the (potentially long-running) async connect task to unwind.
        // `finish_pending_operation` is a HashSet remove, so the eventual
        // duplicate call from the async task's own cancellation path is a no-op.
        if cancelled > 0 {
            self.facade
                .connections
                .finish_pending_operation(profile_id, None);
        }

        cancelled
    }

    pub fn connections_mut(&mut self) -> &mut HashMap<Uuid, ConnectedProfile> {
        &mut self.facade.connections.connections
    }

    pub fn active_connection_id(&self) -> Option<Uuid> {
        self.facade.connections.active_connection_id
    }

    pub fn tasks(&self) -> &dbflux_core::TaskManager {
        &self.facade.tasks
    }

    pub fn tasks_mut(&mut self) -> &mut dbflux_core::TaskManager {
        &mut self.facade.tasks
    }

    pub fn dangerous_query_suppressions(&self) -> &DangerousQuerySuppressions {
        &self.facade.dangerous_query_suppressions
    }

    pub fn dangerous_query_suppressions_mut(&mut self) -> &mut DangerousQuerySuppressions {
        &mut self.facade.dangerous_query_suppressions
    }

    pub fn audit_service(&self) -> &dbflux_audit::AuditService {
        &self.audit_service
    }

    /// Wires the tracing bridge's shared atomics into the audit service so that
    /// `set_log_capture_min_level` can update the bridge threshold at runtime
    /// and `dropped_log_event_count` can report the drop counter.
    ///
    /// Must be called before any clone of `AuditService` is handed out; the
    /// `Option<Arc<AtomicU8>>` inside `AuditService` is not shared across
    /// clones.
    pub fn attach_tracing_bridge(
        &mut self,
        min_level: std::sync::Arc<std::sync::atomic::AtomicU8>,
        drop_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        self.audit_service.attach_bridge(min_level, drop_counter);
    }

    /// Returns the persisted `log_capture_min_level` value from audit settings.
    ///
    /// Returns `"info"` if the settings row has not been seeded yet.
    pub fn log_capture_min_level_setting(&self) -> String {
        self.storage_runtime
            .audit_settings()
            .get()
            .ok()
            .flatten()
            .map(|s| s.log_capture_min_level)
            .unwrap_or_else(|| "info".to_owned())
    }

    pub fn is_audit_degraded(&self) -> bool {
        self.audit_degraded
    }

    /// Record a `Config` failure event for a storage write that did not
    /// reach the database (typically a `StorageError` from a Manager).
    ///
    /// `action`, `object_type`, and `object_id` describe the attempted
    /// mutation; `summary` is the human-readable headline shown in the audit
    /// viewer; `error_message` carries the underlying error string. The
    /// summary and error message must NOT contain secret values.
    pub fn record_storage_failure(
        &self,
        action: dbflux_core::observability::AuditAction,
        object_type: &'static str,
        object_id: String,
        summary: String,
        error_message: String,
    ) {
        self.record_config_event(
            EventOutcome::Failure,
            action,
            object_type,
            object_id,
            summary,
            Some(error_message),
        );
    }

    pub fn connection_tree(&self) -> &dbflux_core::ConnectionTree {
        &self.facade.tree.tree
    }

    pub fn connection_tree_mut(&mut self) -> &mut dbflux_core::ConnectionTree {
        &mut self.facade.tree.tree
    }

    pub fn shutdown(&self) -> &dbflux_core::ShutdownCoordinator {
        &self.facade.shutdown
    }

    pub fn general_settings(&self) -> &GeneralSettings {
        &self.general_settings
    }

    pub fn effective_settings(&self, driver_key: &str) -> EffectiveSettings {
        let empty_values = FormValues::new();
        let driver_values = self
            .driver_settings
            .get(driver_key)
            .unwrap_or(&empty_values);

        EffectiveSettings::resolve(
            &self.general_settings,
            self.driver_overrides.get(driver_key),
            driver_values,
            None,
            None,
        )
    }

    pub fn effective_settings_for_connection(
        &self,
        connection_id: Option<Uuid>,
    ) -> EffectiveSettings {
        let empty_values = FormValues::new();

        let Some(connection_id) = connection_id else {
            return EffectiveSettings::resolve(
                &self.general_settings,
                None,
                &empty_values,
                None,
                None,
            );
        };

        let profile = self
            .connections()
            .get(&connection_id)
            .map(|connected| connected.profile.clone());

        let Some(profile) = profile else {
            return EffectiveSettings::resolve(
                &self.general_settings,
                None,
                &empty_values,
                None,
                None,
            );
        };

        let Some(driver) = self.driver_for_profile(&profile) else {
            return EffectiveSettings::resolve(
                &self.general_settings,
                None,
                &empty_values,
                None,
                None,
            );
        };

        let driver_key = driver.driver_key();
        let driver_values = self
            .driver_settings
            .get(&driver_key)
            .unwrap_or(&empty_values);

        EffectiveSettings::resolve(
            &self.general_settings,
            self.driver_overrides.get(&driver_key),
            driver_values,
            profile.settings_overrides.as_ref(),
            profile.connection_settings.as_ref(),
        )
    }

    #[allow(dead_code)]
    pub fn driver_overrides(&self) -> &HashMap<DriverKey, GlobalOverrides> {
        &self.driver_overrides
    }

    #[allow(dead_code)]
    pub fn driver_settings(&self) -> &HashMap<DriverKey, FormValues> {
        &self.driver_settings
    }

    pub fn is_background_task_limit_reached(&self) -> bool {
        let limit = self.general_settings.max_concurrent_background_tasks;
        self.facade.tasks.background_task_count() >= limit
    }

    pub fn update_general_settings(&mut self, settings: GeneralSettings) {
        self.history_manager
            .set_max_entries(settings.max_history_entries);

        self.general_settings = settings;
    }

    #[allow(dead_code)]
    pub fn update_driver_overrides(&mut self, key: DriverKey, overrides: GlobalOverrides) {
        if overrides.is_empty() {
            self.driver_overrides.remove(&key);
            return;
        }

        self.driver_overrides.insert(key, overrides);
    }

    #[allow(dead_code)]
    pub fn update_driver_settings(&mut self, key: DriverKey, values: FormValues) {
        if values.is_empty() {
            self.driver_settings.remove(&key);
            return;
        }

        self.driver_settings.insert(key, values);
    }

    pub fn hook_definitions(&self) -> &HashMap<String, ConnectionHook> {
        &self.hook_definitions
    }

    pub fn set_hook_definitions(&mut self, definitions: HashMap<String, ConnectionHook>) {
        let hook_count = definitions.len();
        self.hook_definitions = definitions;

        let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let summary = format!("Updated hook definitions ({} hooks)", hook_count);
        let event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::Config,
            EventOutcome::Success,
        )
        .with_typed_action(CONFIG_CHANGE)
        .with_summary(&summary)
        .with_origin(EventOrigin::local())
        .with_actor_id("local")
        .with_object_ref("hook_definition", "global")
        .with_details_json(serde_json::json!({ "hook_count": hook_count }).to_string());

        if let Err(error) = self.audit_service.record(event) {
            log::warn!(
                "Failed to record hook definitions update audit event: {}",
                error
            );
        }
    }
}

#[cfg(feature = "mcp")]
impl AppState {
    pub fn list_mcp_trusted_clients(&self) -> Result<Vec<TrustedClientDto>, String> {
        dbflux_mcp::McpGovernanceService::list_trusted_clients(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn upsert_mcp_trusted_client(&mut self, client: TrustedClientDto) -> Result<(), String> {
        self.mcp_runtime
            .upsert_trusted_client_mut(client)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn delete_mcp_trusted_client(&mut self, client_id: &str) -> Result<(), String> {
        self.mcp_runtime
            .delete_trusted_client_mut(client_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    #[allow(dead_code)]
    pub fn list_mcp_connection_policy_assignments(
        &self,
    ) -> Result<Vec<ConnectionPolicyAssignmentDto>, String> {
        dbflux_mcp::McpGovernanceService::list_connection_policy_assignments(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn save_mcp_connection_policy_assignment(
        &mut self,
        assignment: ConnectionPolicyAssignmentDto,
    ) -> Result<(), String> {
        self.mcp_runtime
            .save_connection_policy_assignment_mut(assignment)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    #[allow(dead_code)]
    pub fn request_mcp_execution(
        &mut self,
        actor_id: String,
        connection_id: String,
        tool_id: String,
        classification: dbflux_policy::ExecutionClassification,
        payload: serde_json::Value,
    ) -> Result<PendingExecutionSummary, String> {
        let plan = self.mcp_runtime.classify_plan(
            classification,
            payload,
            actor_id,
            connection_id,
            tool_id,
        );

        self.mcp_runtime
            .request_execution_mut(plan)
            .map_err(|e| e.to_string())
    }

    pub fn list_mcp_pending_executions(&self) -> Result<Vec<PendingExecutionSummary>, String> {
        dbflux_mcp::McpGovernanceService::list_pending_executions(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn get_mcp_pending_execution(
        &self,
        pending_id: &str,
    ) -> Result<PendingExecutionDetail, String> {
        dbflux_mcp::McpGovernanceService::get_pending_execution(&self.mcp_runtime, pending_id)
            .map_err(|error| error.to_string())
    }

    pub fn approve_mcp_pending_execution(
        &mut self,
        pending_id: &str,
    ) -> Result<ApprovalOutcome, String> {
        self.mcp_runtime
            .approve_pending_execution_with_origin_mut(pending_id, "local", EventOrigin::local())
            .map_err(|error| error.to_string())
    }

    pub fn reject_mcp_pending_execution(
        &mut self,
        pending_id: &str,
    ) -> Result<ApprovalOutcome, String> {
        self.mcp_runtime
            .reject_pending_execution_with_origin_mut(
                pending_id,
                "local",
                None,
                EventOrigin::local(),
            )
            .map_err(|error| error.to_string())
    }

    pub fn drain_mcp_runtime_events(&mut self) -> Vec<McpRuntimeEvent> {
        self.mcp_runtime.drain_events()
    }

    pub fn list_mcp_roles(&self) -> Result<Vec<PolicyRoleDto>, String> {
        let user_roles = dbflux_mcp::McpGovernanceService::list_roles(&self.mcp_runtime)
            .map_err(|error| error.to_string())?;

        let mut all = dbflux_mcp::builtin_roles();
        all.extend(user_roles);
        Ok(all)
    }

    pub fn upsert_mcp_role(&mut self, role: PolicyRoleDto) -> Result<(), String> {
        if dbflux_mcp::is_builtin(&role.id) {
            return Err("Built-in roles cannot be modified".to_string());
        }

        self.mcp_runtime
            .upsert_role_mut(role)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn delete_mcp_role(&mut self, role_id: &str) -> Result<(), String> {
        if dbflux_mcp::is_builtin(role_id) {
            return Err("Built-in roles cannot be deleted".to_string());
        }

        self.mcp_runtime
            .delete_role_mut(role_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn list_mcp_policies(&self) -> Result<Vec<ToolPolicyDto>, String> {
        let user_policies = dbflux_mcp::McpGovernanceService::list_policies(&self.mcp_runtime)
            .map_err(|error| error.to_string())?;

        let mut all = dbflux_mcp::builtin_policies();
        all.extend(user_policies);
        Ok(all)
    }

    pub fn upsert_mcp_policy(&mut self, policy: ToolPolicyDto) -> Result<(), String> {
        if dbflux_mcp::is_builtin(&policy.id) {
            return Err("Built-in policies cannot be modified".to_string());
        }

        self.mcp_runtime
            .upsert_policy_mut(policy)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn delete_mcp_policy(&mut self, policy_id: &str) -> Result<(), String> {
        if dbflux_mcp::is_builtin(policy_id) {
            return Err("Built-in policies cannot be deleted".to_string());
        }

        self.mcp_runtime
            .delete_policy_mut(policy_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    #[allow(dead_code)]
    pub fn set_profile_mcp_governance(
        &mut self,
        profile_id: Uuid,
        governance: Option<dbflux_core::ConnectionMcpGovernance>,
    ) -> Result<(), String> {
        let Some(profile) = self
            .facade
            .profiles
            .profiles
            .iter_mut()
            .find(|profile| profile.id == profile_id)
        else {
            return Err(format!("profile not found: {profile_id}"));
        };

        profile.mcp_governance = governance;
        self.save_profiles();

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub fn persist_mcp_governance(&mut self) -> Result<(), String> {
        let repo = self.storage_runtime.governance_settings();

        let mcp_enabled_by_default = repo
            .get()
            .map_err(|e| e.to_string())?
            .map(|s| s.mcp_enabled_by_default)
            .unwrap_or(0);

        let governance_settings =
            dbflux_storage::repositories::governance_settings::GovernanceSettingsDto {
                id: 1,
                mcp_enabled_by_default,
                updated_at: String::new(),
            };
        repo.upsert(&governance_settings)
            .map_err(|e| e.to_string())?;

        let mcp_clients = self
            .mcp_runtime
            .list_trusted_clients()
            .map_err(|e| e.to_string())?;
        let storage_clients = mcp_clients
            .into_iter()
            .map(
                |client| dbflux_storage::repositories::governance_settings::TrustedClientDto {
                    id: Uuid::new_v4().to_string(),
                    governance_id: 1,
                    client_id: client.id,
                    name: client.name,
                    issuer: client.issuer,
                    active: if client.active { 1 } else { 0 },
                },
            )
            .collect::<Vec<_>>();
        repo.replace_trusted_clients(&storage_clients)
            .map_err(|e| e.to_string())?;

        let mcp_roles = self.mcp_runtime.list_roles().map_err(|e| e.to_string())?;
        let storage_roles = mcp_roles
            .into_iter()
            .map(
                |role| dbflux_storage::repositories::governance_settings::PolicyRoleDto {
                    id: Uuid::new_v4().to_string(),
                    governance_id: 1,
                    role_id: role.id,
                },
            )
            .collect::<Vec<_>>();
        repo.replace_policy_roles(&storage_roles)
            .map_err(|e| e.to_string())?;

        let mcp_policies = self
            .mcp_runtime
            .list_policies()
            .map_err(|e| e.to_string())?;
        let storage_policies = mcp_policies
            .into_iter()
            .map(
                |policy| dbflux_storage::repositories::governance_settings::ToolPolicyDto {
                    id: Uuid::new_v4().to_string(),
                    governance_id: 1,
                    policy_id: policy.id,
                    allowed_tools: policy.allowed_tools,
                    allowed_classes: policy.allowed_classes,
                },
            )
            .collect::<Vec<_>>();
        repo.replace_tool_policies(&storage_policies)
            .map_err(|e| e.to_string())?;

        self.save_profiles();

        Ok(())
    }

    #[allow(dead_code)]
    pub fn reload_mcp_runtime_from_db(&mut self) -> Result<(), String> {
        self.mcp_runtime.clear();
        self.bootstrap_mcp_runtime_from_persistence()
    }
}

impl AppState {
    pub fn auth_provider_registry(&self) -> &AuthProviderRegistry {
        &self.auth_provider_registry
    }

    pub fn auth_provider_by_id(
        &self,
        provider_id: &str,
    ) -> Option<Arc<dyn dbflux_core::DynAuthProvider>> {
        self.auth_provider_registry.get(provider_id)
    }

    /// Provider ids that exist only to be referenced by another provider's
    /// `AuthProfileRef` field (e.g. an SSO-session block referenced by an SSO
    /// profile). Profiles from these providers are building blocks, not
    /// standalone connection credentials, so a connection's Auth Profile picker
    /// must exclude them. Derived from the form definitions so the UI stays
    /// agnostic to concrete provider ids.
    pub fn reference_only_auth_provider_ids(&self) -> HashSet<String> {
        use dbflux_core::FormFieldKind;

        let mut ids = HashSet::new();
        for provider in self.auth_provider_registry.providers() {
            let form = provider.form_def();
            for tab in &form.tabs {
                for section in &tab.sections {
                    for field in &section.fields {
                        if let FormFieldKind::AuthProfileRef {
                            provider_id: Some(provider_id),
                        } = &field.kind
                        {
                            ids.insert(provider_id.clone());
                        }
                    }
                }
            }
        }
        ids
    }

    pub fn resolve_profile_hooks(&self, profile: &ConnectionProfile) -> ConnectionHooks {
        ConnectionHooks::resolve_from_bindings(profile, &self.hook_definitions)
    }

    pub fn profile_uses_connect_pipeline(&self, profile: &ConnectionProfile) -> bool {
        profile.uses_pipeline() || self.infer_auth_profile_for_connection(profile).is_some()
    }

    pub fn prepare_pipeline_input(
        &self,
        profile_id: Uuid,
        cancel: CancelToken,
    ) -> Result<(dbflux_core::PipelineInput, String, Arc<dyn DbDriver>), String> {
        let profile = self
            .facade
            .profiles
            .profiles
            .iter()
            .find(|p| p.id == profile_id)
            .ok_or_else(|| format!("Profile {} not found", profile_id))?
            .clone();

        let driver = self
            .driver_for_profile(&profile)
            .ok_or_else(|| format!("Driver '{}' not found", profile.driver_id()))?;

        let profile_name = profile.name.clone();
        let input = self.build_pipeline_input_for_profile(profile, cancel)?;

        Ok((input, profile_name, driver))
    }

    pub fn build_pipeline_input_for_profile(
        &self,
        profile: ConnectionProfile,
        cancel: CancelToken,
    ) -> Result<dbflux_core::PipelineInput, String> {
        let selected_auth_profile_id = profile
            .access_kind
            .as_ref()
            .and_then(|kind| match kind {
                dbflux_core::access::AccessKind::Managed { params, .. } => {
                    params.get("auth_profile_id").and_then(|s| s.parse().ok())
                }
                _ => None,
            })
            .or(profile.auth_profile_id);

        let selected_auth_profile = selected_auth_profile_id.and_then(|auth_id| {
            self.list_auth_profiles()
                .into_iter()
                .find(|p| p.id == auth_id && p.enabled)
        });

        let auth_profile =
            selected_auth_profile.or_else(|| self.infer_auth_profile_for_connection(&profile));

        let uses_managed_access = matches!(
            profile.access_kind,
            Some(dbflux_core::access::AccessKind::Managed { .. })
        );
        if uses_managed_access && auth_profile.is_none() {
            return Err(
                "Managed access requires an auth profile. Select one in Access > SSM Auth Profile."
                    .to_string(),
            );
        }

        let registered_auth_provider_ids: HashSet<String> = self
            .auth_provider_registry
            .providers()
            .map(|provider| provider.provider_id().to_string())
            .collect();

        let uses_registered_auth_value_sources =
            profile
                .value_refs
                .values()
                .any(|value_ref| match value_ref {
                    dbflux_core::values::ValueRef::Secret { provider, .. }
                    | dbflux_core::values::ValueRef::Parameter { provider, .. } => {
                        registered_auth_provider_ids.contains(provider)
                    }
                    _ => false,
                });

        if uses_registered_auth_value_sources && auth_profile.is_none() {
            return Err(
                "Value sources requiring auth providers need an auth profile. Select one before connecting."
                    .to_string(),
            );
        }

        let (auth_profile, auth_provider): (
            Option<dbflux_core::auth::AuthProfile>,
            Option<Box<dyn dbflux_core::auth::DynAuthProvider>>,
        ) = if let Some(profile) = auth_profile {
            let provider = self
                .auth_provider_registry
                .get(&profile.provider_id)
                .ok_or_else(|| {
                    format!("Auth provider '{}' is not available", profile.provider_id)
                })?;

            let profile_registry_snapshot: Vec<dbflux_core::auth::AuthProfile> =
                self.list_auth_profiles();
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

            (
                Some(expanded),
                Some(RegistryAuthProviderWrapper::boxed(provider)),
            )
        } else {
            (None, None)
        };

        let cache = Arc::new(dbflux_core::values::ValueCache::new(
            std::time::Duration::from_secs(300),
        ));
        let resolver = dbflux_core::values::CompositeValueResolver::new(cache);

        #[cfg(feature = "aws")]
        let aws_profile_name = auth_profile
            .as_ref()
            .and_then(|p| p.fields.get("profile_name").cloned());

        let ssh_tunnels = self
            .facade
            .ssh_tunnels
            .items
            .iter()
            .map(|tunnel| {
                (
                    tunnel.id,
                    crate::access_manager::ResolvedSshTunnel {
                        config: tunnel.config.clone(),
                        secret: self.facade.secrets.get_ssh_tunnel_secret(tunnel),
                    },
                )
            })
            .collect();

        let proxy_tunnels = self
            .facade
            .proxies
            .items
            .iter()
            .map(|proxy| {
                (
                    proxy.id,
                    dbflux_core::ResolvedProxy {
                        profile: proxy.clone(),
                        secret: self.facade.secrets.get_proxy_secret(proxy),
                    },
                )
            })
            .collect();

        let access_manager: Arc<dyn dbflux_core::access::AccessManager> =
            Arc::new(crate::access_manager::AppAccessManager::new(
                ssh_tunnels,
                proxy_tunnels,
                #[cfg(feature = "aws")]
                Some(Arc::new(dbflux_ssm::SsmTunnelFactory::new(
                    aws_profile_name,
                ))),
            ));

        Ok(dbflux_core::PipelineInput {
            profile,
            auth_provider,
            auth_profile,
            resolver,
            access_manager,
            cancel,
        })
    }

    fn infer_auth_profile_for_connection(
        &self,
        profile: &ConnectionProfile,
    ) -> Option<AuthProfile> {
        let aws_profile_name = profile.external_auth_profile_name()?.trim();

        if aws_profile_name.is_empty() {
            return None;
        }

        if let Some(profile) = self.list_auth_profiles().into_iter().find(|auth_profile| {
            auth_profile.enabled
                && auth_profile
                    .fields
                    .get("profile_name")
                    .is_some_and(|name| name == aws_profile_name)
                && self
                    .auth_provider_registry
                    .get(&auth_profile.provider_id)
                    .is_some_and(|provider| provider.capabilities().login.supported)
        }) {
            return Some(profile);
        }

        self.auth_provider_registry
            .providers()
            .filter(|provider| provider.capabilities().login.supported)
            .flat_map(|provider| provider.detect_importable_profiles())
            .find(|candidate| {
                candidate
                    .fields
                    .get("profile_name")
                    .is_some_and(|name| name == aws_profile_name)
            })
            .map(|candidate| {
                AuthProfile::new(
                    candidate.display_name,
                    candidate.provider_id,
                    candidate.fields,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::access::AccessKind;
    use dbflux_core::auth::{
        AuthFormDef, AuthProfile, AuthSession, AuthSessionState, DynAuthProvider,
        ImportableProfile, ResolvedCredentials, UrlCallback,
    };
    use dbflux_core::{
        ConnectionProfile, DatabaseCategory, DbConfig, DbError, DbKind, DriverFormDef,
        DriverMetadataBuilder, PrepareConnectError, QueryLanguage, RpcServiceKind,
        ServiceRpcApiContract,
    };
    use dbflux_driver_ipc::IpcDriver;

    fn fake_probe() -> crate::rpc_services::DriverProbe {
        let metadata = DriverMetadataBuilder::new(
            "sqlite",
            "SQLite",
            DatabaseCategory::Relational,
            QueryLanguage::Sql,
        )
        .build();

        (
            DbKind::SQLite,
            metadata,
            DriverFormDef { tabs: vec![] },
            None,
        )
    }

    fn test_service(kind: RpcServiceKind) -> ServiceConfig {
        ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled: true,
            command: Some("dbflux-driver-host".to_string()),
            args: vec!["--stdio".to_string()],
            env: HashMap::new(),
            startup_timeout_ms: Some(1_000),
            kind,
            api_contract: None,
        }
    }

    struct TestAuthProvider {
        provider_id: String,
        importable_profile_name: Option<String>,
    }

    impl TestAuthProvider {
        fn new(provider_id: impl Into<String>) -> Self {
            Self {
                provider_id: provider_id.into(),
                importable_profile_name: None,
            }
        }

        fn with_importable_profile(
            provider_id: impl Into<String>,
            profile_name: impl Into<String>,
        ) -> Self {
            Self {
                provider_id: provider_id.into(),
                importable_profile_name: Some(profile_name.into()),
            }
        }
    }

    #[async_trait::async_trait]
    impl DynAuthProvider for TestAuthProvider {
        fn provider_id(&self) -> &str {
            &self.provider_id
        }

        fn display_name(&self) -> &str {
            "Test Auth Provider"
        }

        fn form_def(&self) -> &AuthFormDef {
            static FORM: std::sync::OnceLock<AuthFormDef> = std::sync::OnceLock::new();
            FORM.get_or_init(|| AuthFormDef { tabs: vec![] })
        }

        fn capabilities(&self) -> &dbflux_core::auth::AuthProviderCapabilities {
            static CAPABILITIES: dbflux_core::auth::AuthProviderCapabilities =
                dbflux_core::auth::AuthProviderCapabilities {
                    login: dbflux_core::auth::AuthProviderLoginCapabilities {
                        supported: true,
                        verification_url_progress: true,
                    },
                    edit: None,
                };

            &CAPABILITIES
        }

        async fn validate_session(
            &self,
            _profile: &dbflux_core::AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            Ok(AuthSessionState::LoginRequired)
        }

        async fn login(
            &self,
            profile: &dbflux_core::AuthProfile,
            url_callback: UrlCallback,
        ) -> Result<AuthSession, DbError> {
            url_callback(None);

            Ok(AuthSession {
                provider_id: self.provider_id.clone(),
                profile_id: profile.id,
                expires_at: None,
                data: None,
            })
        }

        async fn resolve_credentials(
            &self,
            _profile: &dbflux_core::AuthProfile,
        ) -> Result<ResolvedCredentials, DbError> {
            Ok(ResolvedCredentials::default())
        }

        fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
            let Some(profile_name) = self.importable_profile_name.as_ref() else {
                return Vec::new();
            };

            let mut fields = HashMap::new();
            fields.insert("profile_name".to_string(), profile_name.clone());

            vec![ImportableProfile {
                display_name: profile_name.clone(),
                provider_id: self.provider_id.clone(),
                fields,
            }]
        }
    }

    fn test_state_with_profiles(
        drivers: HashMap<String, Arc<dyn DbDriver>>,
        profiles: Vec<ConnectionProfile>,
    ) -> AppState {
        test_state_with_profiles_and_auth_profiles(drivers, profiles, Vec::new())
    }

    fn test_state_with_profiles_and_auth_profiles(
        drivers: HashMap<String, Arc<dyn DbDriver>>,
        profiles: Vec<ConnectionProfile>,
        auth_profiles: Vec<AuthProfile>,
    ) -> AppState {
        let runtime =
            dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("storage runtime");

        AppState::new_with_drivers_and_settings(
            drivers,
            HashMap::new(),
            GeneralSettings::default(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Vec::new(),
            runtime,
            profiles,
            auth_profiles,
            Vec::new(),
            Vec::new(),
        )
        .expect("test storage setup")
    }

    #[test]
    fn build_builtin_drivers_registers_cloudwatch_driver() {
        let drivers = AppState::build_builtin_drivers();

        assert!(drivers.contains_key("cloudwatch"));

        let driver = drivers.get("cloudwatch").expect("cloudwatch driver");
        assert_eq!(driver.metadata().id, "cloudwatch");
        assert_eq!(driver.display_name(), "CloudWatch Logs");
    }

    #[test]
    fn launch_rpc_services_registers_driver_services_into_runtime_map() {
        let mut drivers = HashMap::new();
        let mut diagnostics = HashMap::new();

        AppState::launch_rpc_services_with(
            &mut drivers,
            &mut diagnostics,
            vec![test_service(RpcServiceKind::Driver)],
            |socket_id, _launch| {
                assert_eq!(socket_id, "svc-socket");
                Ok(fake_probe())
            },
            |_, socket_id, (kind, metadata, form_definition, settings_schema), launch| {
                let launch = launch.expect("managed service should keep launch config");
                Arc::new(
                    IpcDriver::new(socket_id, kind, metadata, form_definition, settings_schema)
                        .with_launch_config(launch),
                ) as Arc<dyn DbDriver>
            },
        );

        assert!(drivers.contains_key("rpc:svc-socket"));
    }

    #[test]
    fn launch_rpc_services_registers_legacy_driver_services_without_api_metadata() {
        let mut drivers = HashMap::new();
        let mut diagnostics = HashMap::new();
        let service = test_service(RpcServiceKind::Driver);

        assert_eq!(service.api_contract, None);
        assert_eq!(
            service.resolved_api_contract(),
            ServiceRpcApiContract::new("driver_rpc", 1, 1)
        );

        AppState::launch_rpc_services_with(
            &mut drivers,
            &mut diagnostics,
            vec![service],
            |socket_id, _launch| {
                assert_eq!(socket_id, "svc-socket");
                Ok(fake_probe())
            },
            |_, socket_id, (kind, metadata, form_definition, settings_schema), launch| {
                let launch = launch.expect("managed service should keep launch config");
                Arc::new(
                    IpcDriver::new(socket_id, kind, metadata, form_definition, settings_schema)
                        .with_launch_config(launch),
                ) as Arc<dyn DbDriver>
            },
        );

        assert!(drivers.contains_key("rpc:svc-socket"));
    }

    #[test]
    fn launch_rpc_services_defers_non_driver_services_without_registration() {
        let mut drivers = HashMap::new();
        let mut diagnostics = HashMap::new();

        AppState::launch_rpc_services_with(
            &mut drivers,
            &mut diagnostics,
            vec![test_service(RpcServiceKind::AuthProvider)],
            |_, _| panic!("non-driver services must not be probed"),
            |_, _, _, _| panic!("non-driver services must not be registered"),
        );

        assert!(drivers.is_empty());
    }

    #[test]
    fn launch_rpc_services_skips_failed_driver_probes_without_registration() {
        let mut drivers = HashMap::new();
        let mut diagnostics = HashMap::new();

        AppState::launch_rpc_services_with(
            &mut drivers,
            &mut diagnostics,
            vec![test_service(RpcServiceKind::Driver)],
            |_, _| Err(Box::new(DbError::connection_failed("probe failed"))),
            |_, _, _, _| panic!("failed probes must not build a driver"),
        );

        assert!(drivers.is_empty());
    }

    #[test]
    fn launch_rpc_services_records_config_diagnostics_without_registration() {
        let mut drivers = HashMap::new();
        let mut diagnostics = HashMap::new();
        let invalid_service = ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled: true,
            command: None,
            args: vec!["--stdio".to_string()],
            env: HashMap::new(),
            startup_timeout_ms: Some(1_000),
            kind: RpcServiceKind::Driver,
            api_contract: None,
        };

        AppState::launch_rpc_services_with(
            &mut drivers,
            &mut diagnostics,
            vec![invalid_service],
            |_, _| panic!("invalid config must not reach probe"),
            |_, _, _, _| panic!("invalid config must not build a driver"),
        );

        assert!(drivers.is_empty());

        let diagnostic = diagnostics.get("svc-socket").expect("config diagnostic");
        assert_eq!(
            diagnostic.stage,
            crate::rpc_services::ExternalDriverStage::Config
        );
        assert!(diagnostic.summary.contains("--driver"));
    }

    #[test]
    fn launch_rpc_services_records_probe_diagnostics_without_registration() {
        let mut drivers = HashMap::new();
        let mut diagnostics = HashMap::new();

        AppState::launch_rpc_services_with(
            &mut drivers,
            &mut diagnostics,
            vec![test_service(RpcServiceKind::Driver)],
            |_, _| Err(Box::new(DbError::connection_failed("probe failed"))),
            |_, _, _, _| panic!("failed probes must not build a driver"),
        );

        assert!(drivers.is_empty());

        let diagnostic = diagnostics.get("svc-socket").expect("probe diagnostic");
        assert_eq!(
            diagnostic.stage,
            crate::rpc_services::ExternalDriverStage::Probe
        );
        assert_eq!(diagnostic.summary, "probe failed");
    }

    #[test]
    fn launch_rpc_auth_providers_registers_runtime_provider_without_driver_side_effects() {
        let mut registry = AuthProviderRegistry::new();

        AppState::launch_rpc_auth_providers_with(
            &mut registry,
            vec![test_service(RpcServiceKind::AuthProvider)],
            |socket_id, launch| {
                let launch = launch.expect("managed auth provider should keep launch config");
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(launch.program, "dbflux-driver-host");

                Ok(Arc::new(TestAuthProvider::new("rpc-auth")) as Arc<dyn DynAuthProvider>)
            },
        );

        assert!(registry.get("rpc-auth").is_some());
        assert!(registry.get("rpc:svc-socket").is_none());
    }

    #[test]
    fn launch_rpc_auth_providers_preserves_existing_provider_on_duplicate() {
        let mut registry = AuthProviderRegistry::new();
        registry.register(Arc::new(TestAuthProvider::new("aws-sso")));

        AppState::launch_rpc_auth_providers_with(
            &mut registry,
            vec![test_service(RpcServiceKind::AuthProvider)],
            |_, _| Ok(Arc::new(TestAuthProvider::new("aws-sso")) as Arc<dyn DynAuthProvider>),
        );

        let providers: Vec<String> = registry
            .providers()
            .map(|provider| provider.provider_id().to_string())
            .collect();

        assert_eq!(providers, vec!["aws-sso".to_string()]);
    }

    #[test]
    fn build_pipeline_input_preserves_connection_auth_profile_id_selection() {
        let auth_profile = AuthProfile::new("OIDC", "custom-oidc", HashMap::new());

        let mut profile = ConnectionProfile::new("rpc profile", DbConfig::default_postgres());
        profile.auth_profile_id = Some(auth_profile.id);

        let mut state = test_state_with_profiles_and_auth_profiles(
            HashMap::new(),
            vec![profile.clone()],
            vec![auth_profile.clone()],
        );
        state
            .auth_provider_registry
            .register(Arc::new(TestAuthProvider::new("custom-oidc")));

        let input = state
            .build_pipeline_input_for_profile(profile, CancelToken::new())
            .expect("connection auth profile should be preserved");

        let selected_auth_profile = input
            .auth_profile
            .expect("pipeline input should include the selected auth profile");

        assert_eq!(selected_auth_profile.id, auth_profile.id);
        assert_eq!(selected_auth_profile.provider_id, "custom-oidc");
    }

    #[test]
    fn build_pipeline_input_preserves_managed_access_auth_profile_id_param() {
        let fallback_auth_profile = AuthProfile::new("Fallback", "custom-oidc", HashMap::new());
        let managed_auth_profile = AuthProfile::new("Managed", "custom-oidc", HashMap::new());

        let mut managed_params = HashMap::new();
        managed_params.insert("instance_id".to_string(), "i-abc123".to_string());
        managed_params.insert("region".to_string(), "us-east-1".to_string());
        managed_params.insert("remote_port".to_string(), "5432".to_string());
        managed_params.insert(
            "auth_profile_id".to_string(),
            managed_auth_profile.id.to_string(),
        );

        let mut profile = ConnectionProfile::new("rpc profile", DbConfig::default_postgres());
        profile.auth_profile_id = Some(fallback_auth_profile.id);
        profile.access_kind = Some(AccessKind::Managed {
            provider: "aws-ssm".to_string(),
            params: managed_params,
        });

        let mut state = test_state_with_profiles_and_auth_profiles(
            HashMap::new(),
            vec![profile.clone()],
            vec![fallback_auth_profile, managed_auth_profile.clone()],
        );
        state
            .auth_provider_registry
            .register(Arc::new(TestAuthProvider::new("custom-oidc")));

        let input = state
            .build_pipeline_input_for_profile(profile, CancelToken::new())
            .expect("managed access auth profile should be preserved");

        let selected_auth_profile = input
            .auth_profile
            .expect("pipeline input should include the managed access auth profile");

        assert_eq!(selected_auth_profile.id, managed_auth_profile.id);
        assert_eq!(selected_auth_profile.provider_id, "custom-oidc");
    }

    #[test]
    fn profile_uses_connect_pipeline_for_importable_aws_sso_profile() {
        let profile = ConnectionProfile::new(
            "cloudwatch",
            DbConfig::CloudWatchLogs {
                region: "us-east-1".to_string(),
                profile: Some("example-sso-profile".to_string()),
                endpoint: None,
            },
        );

        let mut state = test_state_with_profiles(HashMap::new(), vec![profile.clone()]);
        state
            .auth_provider_registry
            .register(Arc::new(TestAuthProvider::with_importable_profile(
                "aws-sso",
                "example-sso-profile",
            )));

        assert!(state.profile_uses_connect_pipeline(&profile));
    }

    #[test]
    fn build_pipeline_input_infers_importable_aws_sso_profile() {
        let profile = ConnectionProfile::new(
            "cloudwatch",
            DbConfig::CloudWatchLogs {
                region: "us-east-1".to_string(),
                profile: Some("example-sso-profile".to_string()),
                endpoint: None,
            },
        );

        let mut state = test_state_with_profiles(HashMap::new(), vec![profile.clone()]);
        state
            .auth_provider_registry
            .register(Arc::new(TestAuthProvider::with_importable_profile(
                "aws-sso",
                "example-sso-profile",
            )));

        let input = state
            .build_pipeline_input_for_profile(profile, CancelToken::new())
            .expect("importable AWS SSO profile should build pipeline input");

        let auth_profile = input
            .auth_profile
            .expect("pipeline input should include inferred auth profile");

        assert_eq!(auth_profile.name, "example-sso-profile");
        assert_eq!(auth_profile.provider_id, "aws-sso");
        assert_eq!(
            auth_profile.fields.get("profile_name").map(String::as_str),
            Some("example-sso-profile")
        );
        assert!(input.auth_provider.is_some());
    }

    #[test]
    fn build_pipeline_input_infers_importable_aws_sso_profile_when_selected_id_is_stale() {
        let mut profile = ConnectionProfile::new(
            "cloudwatch",
            DbConfig::CloudWatchLogs {
                region: "us-east-1".to_string(),
                profile: Some("example-sso-profile".to_string()),
                endpoint: None,
            },
        );
        profile.auth_profile_id = Some(Uuid::new_v4());

        let mut state = test_state_with_profiles(HashMap::new(), vec![profile.clone()]);
        state
            .auth_provider_registry
            .register(Arc::new(TestAuthProvider::with_importable_profile(
                "aws-sso",
                "example-sso-profile",
            )));

        let input = state
            .build_pipeline_input_for_profile(profile, CancelToken::new())
            .expect("stale auth profile id should fall back to importable AWS SSO profile");

        let auth_profile = input
            .auth_profile
            .expect("pipeline input should include inferred auth profile");

        assert_eq!(auth_profile.provider_id, "aws-sso");
        assert_eq!(
            auth_profile.fields.get("profile_name").map(String::as_str),
            Some("example-sso-profile")
        );
        assert!(input.auth_provider.is_some());
    }

    #[test]
    fn prepare_connect_profile_preserves_external_driver_unavailable_and_app_diagnostic() {
        let mut profile = ConnectionProfile::new("rpc profile", DbConfig::default_postgres());
        profile.set_driver_id("rpc:missing.sock".to_string());
        let profile_id = profile.id;

        let mut state = test_state_with_profiles(HashMap::new(), vec![profile]);
        state.external_driver_diagnostics.insert(
            "missing.sock".to_string(),
            crate::rpc_services::ExternalDriverDiagnostic {
                socket_id: "missing.sock".to_string(),
                stage: crate::rpc_services::ExternalDriverStage::Probe,
                summary: "Probe failed".to_string(),
                details: Some("host exited before ready".to_string()),
            },
        );

        let error = match state.prepare_connect_profile(profile_id) {
            Ok(_) => panic!("missing rpc driver must return a typed error"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            PrepareConnectError::ExternalDriverUnavailable {
                driver_id: "rpc:missing.sock".to_string(),
                socket_id: "missing.sock".to_string(),
            }
        );

        let diagnostic = state
            .external_driver_diagnostic("missing.sock")
            .expect("app diagnostic");
        assert_eq!(diagnostic.summary, "Probe failed");
    }

    /// D.2.1 — With the influxdb feature enabled, the builtin driver registry must contain
    #[test]
    fn test_appstate_repos_accessible() {
        // Construct AppState with an in-memory StorageRuntime and verify that
        // the viz repositories are accessible and return empty lists on a fresh DB.
        let storage_runtime =
            dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
        let state =
            AppState::new_with_storage_runtime(storage_runtime).expect("test storage setup");

        let charts = state.saved_charts_repo.list().expect("list saved_charts");
        assert!(charts.is_empty(), "fresh DB must return empty saved charts");

        let dashboards = state.dashboards_repo.list().expect("list dashboards");
        assert!(
            dashboards.is_empty(),
            "fresh DB must return empty dashboards"
        );
    }

    // --- T-3.6: list_auth_profiles() union seam ---
    // Tests for a driver whose `driver_key()` is `"builtin:influxdb"`.

    struct ReflectingTestAuthProvider {
        provider_id: String,
        reflected: Vec<AuthProfile>,
    }

    impl ReflectingTestAuthProvider {
        fn new(provider_id: impl Into<String>, reflected: Vec<AuthProfile>) -> Self {
            Self {
                provider_id: provider_id.into(),
                reflected,
            }
        }
    }

    #[async_trait::async_trait]
    impl DynAuthProvider for ReflectingTestAuthProvider {
        fn provider_id(&self) -> &str {
            &self.provider_id
        }

        fn display_name(&self) -> &str {
            "Reflecting Test Provider"
        }

        fn form_def(&self) -> &AuthFormDef {
            static FORM: std::sync::OnceLock<AuthFormDef> = std::sync::OnceLock::new();
            FORM.get_or_init(|| AuthFormDef { tabs: vec![] })
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            Ok(AuthSessionState::LoginRequired)
        }

        async fn login(
            &self,
            profile: &AuthProfile,
            url_callback: UrlCallback,
        ) -> Result<AuthSession, DbError> {
            url_callback(None);
            Ok(AuthSession {
                provider_id: self.provider_id.clone(),
                profile_id: profile.id,
                expires_at: None,
                data: None,
            })
        }

        async fn resolve_credentials(
            &self,
            _profile: &AuthProfile,
        ) -> Result<ResolvedCredentials, DbError> {
            Ok(ResolvedCredentials::default())
        }

        fn reflect_profiles(&self) -> Vec<AuthProfile> {
            self.reflected.clone()
        }
    }

    fn make_reflected_sso_profile(name: &str) -> AuthProfile {
        use dbflux_core::auth::aws_profile_uuid;
        let id = aws_profile_uuid("aws-sso", name);
        AuthProfile {
            id,
            name: name.to_string(),
            provider_id: "aws-sso".to_string(),
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
            enabled: true,
            read_only: true,
            dangling_origin: None,
        }
    }

    #[test]
    fn list_auth_profiles_returns_reflected_aws_profiles_when_store_is_empty() {
        let reflected_a = make_reflected_sso_profile("dev");
        let reflected_b = make_reflected_sso_profile("prod");

        let mut state =
            test_state_with_profiles_and_auth_profiles(HashMap::new(), Vec::new(), Vec::new());

        // Replace the registry with a clean one so real AWS providers that read
        // from the test machine's ~/.aws/config do not pollute the result.
        state.auth_provider_registry = AuthProviderRegistry::new();
        state
            .auth_provider_registry
            .register(Arc::new(ReflectingTestAuthProvider::new(
                "aws-sso",
                vec![reflected_a.clone(), reflected_b.clone()],
            )));

        let result = state.list_auth_profiles();

        assert_eq!(result.len(), 2, "expected exactly two reflected profiles");
        assert!(
            result
                .iter()
                .any(|p| p.id == reflected_a.id && p.name == "dev")
        );
        assert!(
            result
                .iter()
                .any(|p| p.id == reflected_b.id && p.name == "prod")
        );
    }

    #[test]
    fn list_auth_profiles_includes_stored_non_aws_profile_alongside_reflected() {
        let stored_non_aws = AuthProfile {
            id: Uuid::new_v4(),
            name: "OIDC Provider".to_string(),
            provider_id: "custom-oidc".to_string(),
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
            enabled: true,
            read_only: false,
            dangling_origin: None,
        };
        let reflected_sso = make_reflected_sso_profile("staging");

        let mut state = test_state_with_profiles_and_auth_profiles(
            HashMap::new(),
            Vec::new(),
            vec![stored_non_aws.clone()],
        );

        // Replace the registry with a clean one so real AWS providers that read
        // from the test machine's ~/.aws/config do not pollute the result.
        state.auth_provider_registry = AuthProviderRegistry::new();
        state
            .auth_provider_registry
            .register(Arc::new(ReflectingTestAuthProvider::new(
                "aws-sso",
                vec![reflected_sso.clone()],
            )));

        let result = state.list_auth_profiles();

        assert_eq!(result.len(), 2);
        assert!(
            result.iter().any(|p| p.id == stored_non_aws.id),
            "stored non-AWS profile must appear in union"
        );
        assert!(
            result.iter().any(|p| p.id == reflected_sso.id),
            "reflected AWS profile must appear in union"
        );
    }

    #[test]
    fn list_auth_profiles_excludes_stored_aws_rows_in_favour_of_reflection() {
        // A stored aws-sso row — this should be excluded from the union
        // because aws-sso is a reflected provider-id.
        let stored_aws_row = AuthProfile {
            id: Uuid::new_v4(),
            name: "legacy-sso".to_string(),
            provider_id: "aws-sso".to_string(),
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
            enabled: true,
            read_only: false,
            dangling_origin: None,
        };

        let reflected_sso = make_reflected_sso_profile("current-sso");

        let mut state = test_state_with_profiles_and_auth_profiles(
            HashMap::new(),
            Vec::new(),
            vec![stored_aws_row.clone()],
        );

        state
            .auth_provider_registry
            .register(Arc::new(ReflectingTestAuthProvider::new(
                "aws-sso",
                vec![reflected_sso.clone()],
            )));

        let result = state.list_auth_profiles();

        assert!(
            result.iter().all(|p| p.id != stored_aws_row.id),
            "stored aws-sso row must NOT appear in union (reflection supersedes stored)"
        );
        assert!(
            result.iter().any(|p| p.id == reflected_sso.id),
            "reflected profile must appear in union"
        );
    }

    #[test]
    #[cfg(feature = "influxdb")]
    fn influxdb_registration_present_when_feature_enabled() {
        let drivers = AppState::build_builtin_drivers();
        assert!(
            drivers.contains_key("influxdb"),
            "driver map must contain the 'influxdb' key when the influxdb feature is enabled"
        );

        let driver = drivers
            .get("influxdb")
            .expect("influxdb driver must be registered");
        let key: String = driver.driver_key();
        assert_eq!(
            key, "builtin:influxdb",
            "driver_key() must be 'builtin:influxdb'"
        );
    }

    #[test]
    fn appstate_new_with_storage_runtime_returns_result_and_propagates_viz_failure() {
        // Uses a directory as the DB path. open_dbflux_db will succeed (migrations
        // ran during StorageRuntime construction on the real path), but viz_connection()
        // opens a second connection to the same path.  For an in-memory runtime that
        // succeeded, viz_connection should also succeed — the test verifies the
        // constructor signature is Result, not the panic path.
        let rt = dbflux_storage::bootstrap::StorageRuntime::in_memory()
            .expect("in-memory storage must work");
        let state = AppState::new_with_storage_runtime(rt).expect("viz repos must open");
        assert!(
            !state.saved_charts_repo.list().unwrap().is_empty()
                || state.saved_charts_repo.list().unwrap().is_empty(),
            "fresh in-memory DB list is empty — but call must not panic"
        );
    }

    // --- reference_only_auth_provider_ids ---

    /// Scans `form` the same way `reference_only_auth_provider_ids` does and
    /// returns collected ids. Used to unit-test the scanning logic in isolation.
    fn collect_ref_ids_from_form(form: &dbflux_core::DriverFormDef) -> HashSet<String> {
        use dbflux_core::FormFieldKind;
        let mut ids = HashSet::new();
        for tab in &form.tabs {
            for section in &tab.sections {
                for field in &section.fields {
                    if let FormFieldKind::AuthProfileRef {
                        provider_id: Some(provider_id),
                    } = &field.kind
                    {
                        ids.insert(provider_id.clone());
                    }
                }
            }
        }
        ids
    }

    fn make_form_with_auth_profile_ref(provider_id: Option<String>) -> dbflux_core::DriverFormDef {
        use dbflux_core::{FormFieldDef, FormFieldKind, FormSection, FormTab};
        dbflux_core::DriverFormDef {
            tabs: vec![FormTab {
                id: "main".to_string(),
                label: "Main".to_string(),
                sections: vec![FormSection {
                    title: "Auth".to_string(),
                    fields: vec![FormFieldDef {
                        id: "ref_field".to_string(),
                        label: "Ref".to_string(),
                        kind: FormFieldKind::AuthProfileRef { provider_id },
                        placeholder: String::new(),
                        required: false,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: None,
                    }],
                }],
            }],
        }
    }

    #[test]
    fn reference_only_auth_provider_ids_ignores_none_filter() {
        let form = make_form_with_auth_profile_ref(None);
        let ids = collect_ref_ids_from_form(&form);
        assert!(
            ids.is_empty(),
            "AuthProfileRef with provider_id: None must not contribute any id to the reference-only set"
        );
    }

    #[test]
    fn reference_only_auth_provider_ids_inserts_some_filter() {
        let form = make_form_with_auth_profile_ref(Some("aws-sso-session".to_string()));
        let ids = collect_ref_ids_from_form(&form);
        assert!(
            ids.contains("aws-sso-session"),
            "AuthProfileRef with Some(provider_id) must contribute that id to the reference-only set"
        );
    }
}
