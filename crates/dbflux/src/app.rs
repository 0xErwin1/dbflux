//! DBFlux application state re-exports.
//!
//! This module re-exports `AppState` from `dbflux_app` (a plain struct with no GPUI dependency).
//! GPUI-coupled event types and `EventEmitter` implementations are in `dbflux_ui::AppStateEntity`.

pub use dbflux_app::AppState;

// Re-export GPUI-coupled event types from dbflux_ui for backwards compatibility
pub use dbflux_ui::AppStateChanged;
pub use dbflux_ui::AuthProfileCreated;

#[cfg(feature = "mcp")]
pub use dbflux_ui::McpRuntimeEventRaised;

// Re-export the GPUI entity wrapper
pub use dbflux_ui::AppStateEntity;

// ============================================================================
// Tests — remain in dbflux crate since they test AppState with dbflux dependencies
// ============================================================================

#[cfg(test)]
mod tests {
    use super::AppState;
    use dbflux_core::{
        AuthProfile, CancelToken, ConnectionMcpGovernance, ConnectionMcpPolicyBinding, DbDriver,
        DbKind, FormValues, GeneralSettings, RefreshPolicySetting,
    };
    use dbflux_storage::bootstrap::StorageRuntime;

    #[cfg(feature = "mcp")]
    use dbflux_mcp::server::authorization::{authorize_request, AuthorizationRequest};
    #[cfg(feature = "mcp")]
    use dbflux_mcp::server::request_context::RequestIdentity;
    #[cfg(feature = "mcp")]
    use dbflux_mcp::{
        AuditExportFormat, AuditQuery, ConnectionPolicyAssignmentDto, McpRuntimeEvent,
        TrustedClientDto,
    };
    #[cfg(feature = "mcp")]
    use dbflux_policy::{
        ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
        ToolPolicy,
    };

    use dbflux_test_support::FakeDriver;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, OnceLock};
    use uuid::Uuid;

    static TEST_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct TestEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        previous_home: Option<OsString>,
        previous_xdg_config_home: Option<OsString>,
        previous_xdg_data_home: Option<OsString>,
        root: PathBuf,
    }

    impl TestEnvGuard {
        fn new() -> Self {
            let lock = TEST_ENV_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .expect("test environment lock should not be poisoned");

            let root = std::env::temp_dir().join(format!("dbflux-test-{}", Uuid::new_v4()));
            let config_home = root.join("config");
            let data_home = root.join("data");

            std::fs::create_dir_all(&config_home).expect("create temp config directory");
            std::fs::create_dir_all(&data_home).expect("create temp data directory");

            let previous_home = std::env::var_os("HOME");
            let previous_xdg_config_home = std::env::var_os("XDG_CONFIG_HOME");
            let previous_xdg_data_home = std::env::var_os("XDG_DATA_HOME");

            unsafe {
                std::env::set_var("HOME", &root);
                std::env::set_var("XDG_CONFIG_HOME", &config_home);
                std::env::set_var("XDG_DATA_HOME", &data_home);
            }

            Self {
                _lock: lock,
                previous_home,
                previous_xdg_config_home,
                previous_xdg_data_home,
                root,
            }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.previous_home {
                    Some(value) => std::env::set_var("HOME", value),
                    None => std::env::remove_var("HOME"),
                }

                match &self.previous_xdg_config_home {
                    Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                    None => std::env::remove_var("XDG_CONFIG_HOME"),
                }

                match &self.previous_xdg_data_home {
                    Some(value) => std::env::set_var("XDG_DATA_HOME", value),
                    None => std::env::remove_var("XDG_DATA_HOME"),
                }
            }

            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn with_isolated_user_dirs<R>(test: impl FnOnce() -> R) -> R {
        let _guard = TestEnvGuard::new();
        test()
    }

    fn test_state(general_settings: GeneralSettings) -> AppState {
        let fake = FakeDriver::new(DbKind::SQLite);
        let mut drivers: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();
        drivers.insert(fake.metadata().id.clone(), Arc::new(fake));

        AppState::new_with_drivers_and_settings(
            drivers,
            general_settings,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            StorageRuntime::in_memory().unwrap(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    #[test]
    fn saved_query_store_is_optional() {
        with_isolated_user_dirs(|| {
            let state = AppState::new();
            let _ = state.saved_queries();
        });
    }

    #[test]
    fn new_with_drivers_uses_injected_registry() {
        with_isolated_user_dirs(|| {
            let fake = FakeDriver::new(DbKind::SQLite);
            let driver_id = fake.metadata().id.clone();
            let mut drivers: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();
            drivers.insert(driver_id.clone(), Arc::new(fake));

            let state = AppState::new_with_drivers_and_settings(
                drivers,
                GeneralSettings::default(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                StorageRuntime::in_memory().unwrap(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );

            assert_eq!(state.drivers().len(), 1);
            assert!(state.drivers().contains_key(&driver_id));
        });
    }

    #[test]
    fn effective_settings_use_global_defaults_without_driver_entries() {
        with_isolated_user_dirs(|| {
            let mut general_settings = GeneralSettings::default();
            general_settings.default_refresh_policy = RefreshPolicySetting::Interval;
            general_settings.default_refresh_interval_secs = 15;
            general_settings.confirm_dangerous_queries = false;
            general_settings.dangerous_requires_where = false;
            general_settings.dangerous_requires_preview = true;

            let state = test_state(general_settings.clone());
            let effective = state.effective_settings("builtin:redis");

            assert_eq!(
                effective.refresh_policy,
                general_settings.default_refresh_policy
            );
            assert_eq!(
                effective.refresh_interval_secs,
                general_settings.default_refresh_interval_secs
            );
            assert_eq!(
                effective.confirm_dangerous,
                general_settings.confirm_dangerous_queries
            );
            assert_eq!(
                effective.requires_where,
                general_settings.dangerous_requires_where
            );
            assert_eq!(
                effective.requires_preview,
                general_settings.dangerous_requires_preview
            );
            assert!(effective.driver_values.is_empty());
        });
    }

    #[test]
    fn effective_settings_apply_driver_overrides_and_values() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());
            state.update_driver_overrides(
                "builtin:redis".to_string(),
                dbflux_core::GlobalOverrides {
                    refresh_policy: Some(RefreshPolicySetting::Interval),
                    refresh_interval_secs: Some(3),
                    confirm_dangerous: Some(false),
                    requires_where: Some(false),
                    requires_preview: Some(true),
                },
            );

            let mut values = FormValues::new();
            values.insert("scan_batch_size".to_string(), "500".to_string());
            state.update_driver_settings("builtin:redis".to_string(), values.clone());

            let effective = state.effective_settings("builtin:redis");

            assert_eq!(effective.refresh_policy, RefreshPolicySetting::Interval);
            assert_eq!(effective.refresh_interval_secs, 3);
            assert!(!effective.confirm_dangerous);
            assert!(!effective.requires_where);
            assert!(effective.requires_preview);
            assert_eq!(effective.driver_values, values);
        });
    }

    fn insert_connected_profile(state: &mut AppState, profile: &dbflux_core::ConnectionProfile) {
        let driver = state
            .drivers()
            .get(&profile.driver_id())
            .expect("driver must be registered")
            .clone();

        let connection: Arc<dyn dbflux_core::Connection> = Arc::from(
            driver
                .connect_with_secrets(profile, None, None)
                .expect("FakeDriver never fails"),
        );

        state.connections_mut().insert(
            profile.id,
            dbflux_core::ConnectedProfile {
                profile: profile.clone(),
                connection,
                schema: None,
                database_schemas: HashMap::new(),
                table_details: HashMap::new(),
                schema_types: HashMap::new(),
                schema_indexes: HashMap::new(),
                schema_foreign_keys: HashMap::new(),
                active_database: None,
                redis_key_cache: Default::default(),
                database_connections: HashMap::new(),
                proxy_tunnel: None,
            },
        );
    }

    fn fake_driver_key(state: &AppState) -> String {
        state.drivers().values().next().unwrap().driver_key()
    }

    fn fake_profile(state: &AppState) -> dbflux_core::ConnectionProfile {
        let driver_id = state.drivers().keys().next().unwrap().clone();
        let mut profile =
            dbflux_core::ConnectionProfile::new("test", dbflux_core::DbConfig::default_sqlite());
        profile.set_driver_id(driver_id);
        profile
    }

    #[test]
    fn connection_overrides_win_over_driver_overrides() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());
            let driver_key = fake_driver_key(&state);

            state.update_driver_overrides(
                driver_key,
                dbflux_core::GlobalOverrides {
                    confirm_dangerous: Some(true),
                    requires_where: Some(true),
                    ..Default::default()
                },
            );

            let mut profile = fake_profile(&state);
            profile.settings_overrides = Some(dbflux_core::GlobalOverrides {
                confirm_dangerous: Some(false),
                ..Default::default()
            });

            insert_connected_profile(&mut state, &profile);

            let effective = state.effective_settings_for_connection(Some(profile.id));

            assert!(
                !effective.confirm_dangerous,
                "connection override should win"
            );
            assert!(
                effective.requires_where,
                "driver override should fall through"
            );
        });
    }

    #[test]
    fn connection_without_overrides_falls_through_to_driver() {
        with_isolated_user_dirs(|| {
            let mut general = GeneralSettings::default();
            general.confirm_dangerous_queries = false;

            let mut state = test_state(general);
            let driver_key = fake_driver_key(&state);

            state.update_driver_overrides(
                driver_key,
                dbflux_core::GlobalOverrides {
                    confirm_dangerous: Some(true),
                    ..Default::default()
                },
            );

            let profile = fake_profile(&state);
            insert_connected_profile(&mut state, &profile);

            let effective = state.effective_settings_for_connection(Some(profile.id));

            assert!(
                effective.confirm_dangerous,
                "driver override should apply when connection has no overrides"
            );
        });
    }

    #[test]
    fn connection_settings_merge_on_top_of_driver_settings() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());
            let driver_key = fake_driver_key(&state);

            let mut driver_vals = FormValues::new();
            driver_vals.insert("scan_batch_size".to_string(), "100".to_string());
            driver_vals.insert("allow_flush".to_string(), "false".to_string());
            state.update_driver_settings(driver_key, driver_vals);

            let mut profile = fake_profile(&state);

            let mut conn_settings = FormValues::new();
            conn_settings.insert("scan_batch_size".to_string(), "500".to_string());
            profile.connection_settings = Some(conn_settings);

            insert_connected_profile(&mut state, &profile);

            let effective = state.effective_settings_for_connection(Some(profile.id));

            assert_eq!(
                effective.driver_values.get("scan_batch_size"),
                Some(&"500".to_string()),
                "connection setting should override driver setting"
            );
            assert_eq!(
                effective.driver_values.get("allow_flush"),
                Some(&"false".to_string()),
                "driver setting should fall through when connection doesn't override"
            );
        });
    }

    #[test]
    fn update_driver_maps_remove_empty_entries() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());

            state.update_driver_overrides(
                "builtin:redis".to_string(),
                dbflux_core::GlobalOverrides {
                    confirm_dangerous: Some(false),
                    ..Default::default()
                },
            );

            let mut values = FormValues::new();
            values.insert("allow_flush".to_string(), "true".to_string());
            state.update_driver_settings("builtin:redis".to_string(), values);

            assert!(state.driver_overrides().contains_key("builtin:redis"));
            assert!(state.driver_settings().contains_key("builtin:redis"));

            state.update_driver_overrides(
                "builtin:redis".to_string(),
                dbflux_core::GlobalOverrides::default(),
            );
            state.update_driver_settings("builtin:redis".to_string(), FormValues::new());

            assert!(!state.driver_overrides().contains_key("builtin:redis"));
            assert!(!state.driver_settings().contains_key("builtin:redis"));
        });
    }

    #[test]
    fn build_pipeline_input_fails_for_unknown_auth_provider() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());

            let auth_profile_id = Uuid::new_v4();
            state.add_auth_profile(AuthProfile {
                id: auth_profile_id,
                name: "Unknown Provider Profile".to_string(),
                provider_id: "unknown-provider".to_string(),
                fields: HashMap::new(),
                enabled: true,
            });

            let mut profile = fake_profile(&state);
            profile.auth_profile_id = Some(auth_profile_id);

            let error = match state.build_pipeline_input_for_profile(profile, CancelToken::new()) {
                Ok(_) => panic!("unknown provider must fail before pipeline start"),
                Err(error) => error,
            };

            assert_eq!(error, "Auth provider 'unknown-provider' is not available");
        });
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_trusted_client_upsert_emits_runtime_event() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());

            state
                .upsert_mcp_trusted_client(TrustedClientDto {
                    id: "agent-a".to_string(),
                    name: "Agent A".to_string(),
                    issuer: None,
                    active: true,
                })
                .expect("trusted client upsert should succeed");

            let clients = state
                .list_mcp_trusted_clients()
                .expect("trusted clients should be listable");
            assert_eq!(clients.len(), 1);

            let events = state.drain_mcp_runtime_events();
            assert!(events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::TrustedClientsUpdated)));
        });
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_execution_request_updates_pending_queue_event() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());

            let pending = state.request_mcp_execution(
                "agent-a".to_string(),
                "conn-a".to_string(),
                "request_execution".to_string(),
                dbflux_policy::ExecutionClassification::Write,
                serde_json::json!({"query": "UPDATE users SET active = true"}),
            );

            assert_eq!(pending.actor_id, "agent-a");

            let events = state.drain_mcp_runtime_events();
            assert!(events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::PendingExecutionsUpdated)));
        });
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_ui_workflow_drives_enforcement_and_audit_export() {
        with_isolated_user_dirs(|| {
            let mut state = test_state(GeneralSettings::default());

            let mut profile_a = fake_profile(&state);
            profile_a.name = "Connection A".to_string();

            let mut profile_b = fake_profile(&state);
            profile_b.name = "Connection B".to_string();

            state.add_profile_in_folder(profile_a.clone(), None);
            state.add_profile_in_folder(profile_b.clone(), None);

            state
                .set_profile_mcp_governance(
                    profile_a.id,
                    Some(ConnectionMcpGovernance {
                        enabled: true,
                        policy_bindings: vec![ConnectionMcpPolicyBinding {
                            actor_id: "agent-a".to_string(),
                            role_ids: vec!["read-only".to_string()],
                            policy_ids: vec![],
                        }],
                    }),
                )
                .expect("set governance should succeed");

            let mut effective = state.effective_settings_for_connection(Some(profile_a.id));
            effective.confirm_dangerous = true;
            assert!(effective.confirm_dangerous);

            let result = state.query_mcp_audit_entries(&AuditQuery {
                actor_id: Some("agent-a".to_string()),
                ..Default::default()
            });
            assert!(result.is_ok());
        });
    }
}
