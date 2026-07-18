use crate::ssh_shared;
use dbflux_components::components::form_renderer;
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::ValueRef;
use dbflux_core::{
    AccessKind, CancelToken, ConnectionMcpGovernance, ConnectionMcpPolicyBinding,
    ConnectionOverrides, ConnectionProfile, DbConfig, FormFieldKind, HookPhase, SshTunnelConfig,
};
use dbflux_ui_base::hook_phase_runner::{DetachedHookScope, HookPhaseState, run_hook_phase};
use dbflux_ui_base::toast::{Toast, now_hms};
use gpui::*;
use log::info;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use super::{ConnectionManagerWindow, DismissEvent, TestStatus};

impl ConnectionManagerWindow {
    fn collect_mcp_governance(&self, cx: &Context<Self>) -> Option<ConnectionMcpGovernance> {
        if !self.mcp_tab.conn_mcp_enabled {
            return None;
        }

        let actor_id = self
            .mcp_tab
            .conn_mcp_actor_dropdown
            .read(cx)
            .selected_value()
            .map(|v| v.to_string())
            .unwrap_or_default();

        let mut role_ids = Vec::new();
        if let Some(primary_role) = self
            .mcp_tab
            .conn_mcp_role_dropdown
            .read(cx)
            .selected_value()
        {
            let primary_str = primary_role.to_string();
            if !primary_str.is_empty() {
                role_ids.push(primary_str);
            }
        }
        role_ids.extend(
            self.mcp_tab
                .conn_mcp_role_multi_select
                .read(cx)
                .selected_values()
                .into_iter()
                .map(|s| s.to_string()),
        );

        let mut policy_ids = Vec::new();
        if let Some(primary_policy) = self
            .mcp_tab
            .conn_mcp_policy_dropdown
            .read(cx)
            .selected_value()
        {
            let primary_str = primary_policy.to_string();
            if !primary_str.is_empty() {
                policy_ids.push(primary_str);
            }
        }
        policy_ids.extend(
            self.mcp_tab
                .conn_mcp_policy_multi_select
                .read(cx)
                .selected_values()
                .into_iter()
                .map(|s| s.to_string()),
        );

        let policy_bindings = if actor_id.is_empty() {
            Vec::new()
        } else {
            vec![ConnectionMcpPolicyBinding {
                actor_id,
                role_ids,
                policy_ids,
            }]
        };

        Some(ConnectionMcpGovernance {
            enabled: true,
            policy_bindings,
        })
    }

    pub(super) fn validate_form(&mut self, require_name: bool, cx: &mut Context<Self>) -> bool {
        self.validation_errors.clear();

        if require_name {
            let name = self.form.input_name.read(cx).value().to_string();
            if name.trim().is_empty() {
                self.validation_errors
                    .push("Connection name is required".to_string());
            }
        }

        let Some(driver) = &self.form.selected_driver else {
            self.validation_errors
                .push("No driver selected".to_string());
            return false;
        };

        let form = driver.form_definition();

        for tab in form.tabs.iter().filter(|t| t.id != "ssh") {
            for section in &tab.sections {
                for field in &section.fields {
                    if field.id == "password" || field.kind == FormFieldKind::Checkbox {
                        continue;
                    }

                    let field_enabled = self.is_field_enabled(field);
                    if !field_enabled {
                        continue;
                    }

                    let value = self
                        .form
                        .driver_inputs
                        .get(&field.id)
                        .map(|input| input.read(cx).value().to_string())
                        .unwrap_or_default();

                    if field.required
                        && value.trim().is_empty()
                        && !self.has_dynamic_value_ref_for_field(&field.id, cx)
                    {
                        self.validation_errors
                            .push(format!("{} is required", field.label));
                    }

                    if !value.trim().is_empty()
                        && field.kind == FormFieldKind::Number
                        && value.parse::<u16>().is_err()
                    {
                        self.validation_errors
                            .push(format!("{} must be a valid number", field.label));
                    }
                }
            }
        }

        if self.access.ssh_enabled && form.supports_ssh() {
            let ssh_host = self.access.input_ssh_host.read(cx).value().to_string();
            if ssh_host.trim().is_empty() {
                self.validation_errors
                    .push("SSH Host is required when SSH is enabled".to_string());
            }

            let ssh_user = self.access.input_ssh_user.read(cx).value().to_string();
            if ssh_user.trim().is_empty() {
                self.validation_errors
                    .push("SSH User is required when SSH is enabled".to_string());
            }

            let ssh_port_str = self.access.input_ssh_port.read(cx).value().to_string();
            if !ssh_port_str.trim().is_empty() && ssh_port_str.parse::<u16>().is_err() {
                self.validation_errors
                    .push("SSH Port must be a valid number".to_string());
            }
        }

        // Validate SSM fields if SSM access method is selected (T-7.3)
        if self.is_ssm_selected() {
            let instance_id = self
                .access
                .input_ssm_instance_id
                .read(cx)
                .value()
                .to_string();
            if instance_id.trim().is_empty() {
                self.validation_errors
                    .push("SSM Instance ID is required".to_string());
            } else if !self.has_dynamic_value_ref_for_field("ssm_instance_id", cx)
                && !instance_id.starts_with("i-")
                && !instance_id.starts_with("mi-")
            {
                self.validation_errors
                    .push("SSM Instance ID must start with 'i-' or 'mi-'".to_string());
            }

            let region = self.access.input_ssm_region.read(cx).value().to_string();
            if region.trim().is_empty() {
                self.validation_errors
                    .push("SSM Region is required".to_string());
            }

            let port_str = self
                .access
                .input_ssm_remote_port
                .read(cx)
                .value()
                .to_string();
            if !self.has_dynamic_value_ref_for_field("ssm_remote_port", cx) {
                match port_str.parse::<u16>() {
                    Ok(0) => {
                        self.validation_errors
                            .push("SSM Remote Port must be greater than 0".to_string());
                    }
                    Err(_) => {
                        self.validation_errors
                            .push("SSM Remote Port must be a valid number".to_string());
                    }
                    _ => {}
                }
            }
        }

        // T-5.4: Dangling-reference guard. When a bound auth-profile UUID no
        // longer resolves to any entry in the current reflected+stored union,
        // block the connect with a user-facing message. When the stored profile
        // is present but marked dangling, tailor the message to the origin.
        if let Some(auth_profile_id) = self.auth_profile.selected_auth_profile_id {
            let bound_profile = self
                .app_state
                .read(cx)
                .list_auth_profiles()
                .into_iter()
                .find(|profile| profile.id == auth_profile_id);

            match bound_profile {
                None => {
                    self.validation_errors.push(format!(
                        "AWS profile '{}' not found in ~/.aws/config — please restore or \
                         recreate the profile in ~/.aws/config before connecting.",
                        auth_profile_id
                    ));
                }

                Some(profile) if profile.dangling_origin.as_deref() == Some("keyring-only") => {
                    self.validation_errors.push(format!(
                        "Auth profile '{}' is only in the DBFlux keyring and no longer has a \
                         corresponding entry in ~/.aws/config or ~/.aws/credentials. \
                         Add the credentials to ~/.aws/credentials to connect with this profile.",
                        profile.name
                    ));
                }

                Some(profile) if profile.dangling_origin.is_some() => {
                    self.validation_errors.push(format!(
                        "Auth profile '{}' could not be found in ~/.aws/config. \
                         Please recreate the profile or update the connection binding.",
                        profile.name
                    ));
                }

                _ => {}
            }
        }

        let uses_dynamic_auth_sources = self.collect_value_refs(cx).values().any(|value_ref| {
            matches!(
                value_ref,
                ValueRef::Secret { .. } | ValueRef::Parameter { .. } | ValueRef::Auth { .. }
            )
        });

        if uses_dynamic_auth_sources {
            let Some(auth_profile_id) = self.auth_profile.selected_auth_profile_id else {
                self.validation_errors.push(
                    "Dynamic value sources require an Auth Profile. Select one in Access tab."
                        .to_string(),
                );
                return self.validation_errors.is_empty();
            };

            let bound_profile = self
                .app_state
                .read(cx)
                .list_auth_profiles()
                .into_iter()
                .find(|profile| profile.id == auth_profile_id);

            if let Some(profile) = bound_profile {
                if self
                    .app_state
                    .read(cx)
                    .auth_provider_by_id(&profile.provider_id)
                    .is_none()
                {
                    self.validation_errors.push(format!(
                        "Selected Auth Profile '{}' has no registered provider for dynamic value sources.",
                        profile.name
                    ));
                }
            } else {
                // Already reported above in the dangling-reference guard; no
                // duplicate message needed.
            }
        }

        self.validate_hook_bindings(cx);

        self.validation_errors.is_empty()
    }

    pub(super) fn build_ssh_config(&self, cx: &Context<Self>) -> Option<SshTunnelConfig> {
        if !self.access.ssh_enabled {
            return None;
        }

        let host = self.access.input_ssh_host.read(cx).value().to_string();
        let port_str = self.access.input_ssh_port.read(cx).value().to_string();
        let user = self.access.input_ssh_user.read(cx).value().to_string();
        let key_path_str = self.access.input_ssh_key_path.read(cx).value().to_string();

        Some(ssh_shared::build_ssh_config(
            &host,
            &port_str,
            &user,
            self.access.ssh_auth_method,
            &key_path_str,
        ))
    }

    pub(super) fn build_config(&self, cx: &Context<Self>) -> Option<DbConfig> {
        let driver = self.form.selected_driver.as_ref()?;
        let values = self.collect_form_values(driver.form_definition(), cx);

        let mut config = match driver.build_config(&values) {
            Ok(config) => config,
            Err(e) => {
                log::error!("Failed to build config: {}", e);
                return None;
            }
        };

        // Persist the SSL mode id string selected in the UI. Drivers hardcode a default; we
        // overwrite here so the user's selection is saved without each driver reading form values.
        if !self.form.selected_ssl_mode.is_empty() {
            let selected = self.form.selected_ssl_mode.clone();
            match &mut config {
                DbConfig::Postgres { ssl_mode, .. }
                | DbConfig::MySQL { ssl_mode, .. }
                | DbConfig::MongoDB { ssl_mode, .. }
                | DbConfig::Redis { ssl_mode, .. }
                | DbConfig::SqlServer { ssl_mode, .. }
                | DbConfig::Redshift { ssl_mode, .. } => {
                    *ssl_mode = Some(selected);
                }
                _ => {}
            }
        }

        // Apply SSL cert path inputs.
        let ssl_root_cert = {
            let v = self.form.ssl_ca_cert_input.read(cx).value().to_string();
            if v.trim().is_empty() { None } else { Some(v) }
        };
        let ssl_client_cert = {
            let v = self.form.ssl_client_cert_input.read(cx).value().to_string();
            if v.trim().is_empty() { None } else { Some(v) }
        };
        let ssl_client_key = {
            let v = self.form.ssl_client_key_input.read(cx).value().to_string();
            if v.trim().is_empty() { None } else { Some(v) }
        };

        match &mut config {
            DbConfig::Postgres {
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ..
            }
            | DbConfig::MySQL {
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ..
            }
            | DbConfig::MongoDB {
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ..
            }
            | DbConfig::Redis {
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ..
            }
            | DbConfig::Redshift {
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ..
            } => {
                *ssl_root_cert_path = ssl_root_cert;
                *ssl_client_cert_path = ssl_client_cert;
                *ssl_client_key_path = ssl_client_key;
            }
            _ => {}
        }

        let ssh_tunnel_profile_id = self.access.selected_ssh_tunnel_id;
        let ssh_tunnel = if ssh_tunnel_profile_id.is_some() {
            None
        } else {
            self.build_ssh_config(cx)
        };

        match &mut config {
            DbConfig::Postgres {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::MySQL {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::MongoDB {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::Redis {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::SqlServer {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::Redshift {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            } => {
                *tunnel = ssh_tunnel;
                *profile_id = ssh_tunnel_profile_id;
            }
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::InfluxDB { .. }
            | DbConfig::External { .. } => {}
        }

        Some(config)
    }

    pub(super) fn build_profile(&self, cx: &Context<Self>) -> Option<ConnectionProfile> {
        let name = self.form.input_name.read(cx).value().to_string();
        let kind = self.selected_kind()?;
        let driver_id = self.selected_driver_id()?;
        let config = self.build_config(cx)?;

        let mut profile = if let Some(existing_id) = self.editing_profile_id {
            let mut p = ConnectionProfile::new_with_driver(name, kind, driver_id, config);
            p.id = existing_id;
            p
        } else {
            ConnectionProfile::new_with_driver(name, kind, driver_id, config)
        };

        profile.save_password = self.form.form_save_password;
        profile.proxy_profile_id = self.access.selected_proxy_id;
        profile.auth_profile_id = self.auth_profile.selected_auth_profile_id;
        profile.value_refs = self.collect_value_refs(cx);
        profile.settings_overrides = self.collect_connection_overrides(cx);
        profile.connection_settings = self.collect_connection_settings(cx);
        profile.hook_bindings = self.collect_hook_bindings(cx);
        profile.mcp_governance = self.collect_mcp_governance(cx);

        // Collect access kind — keep SSH/proxy profile selections as references instead
        // of flattening them into inline connection fields.
        let access_kind = if self.is_ssm_selected() {
            Some(self.collect_managed_access_kind(cx))
        } else if let Some(ssh_tunnel_profile_id) = self.access.selected_ssh_tunnel_id {
            Some(AccessKind::Ssh {
                ssh_tunnel_profile_id,
            })
        } else if let Some(proxy_profile_id) = self.access.selected_proxy_id {
            Some(AccessKind::Proxy { proxy_profile_id })
        } else {
            self.access.access_kind.clone()
        };
        profile.access_kind = access_kind;

        if profile.hook_bindings.is_some() {
            profile.hooks = None;
        } else if let Some(existing_id) = self.editing_profile_id {
            let existing_hooks = self
                .app_state
                .read(cx)
                .profiles()
                .iter()
                .find(|item| item.id == existing_id)
                .and_then(|item| item.hooks.clone());
            profile.hooks = existing_hooks;
        }

        Some(profile)
    }

    pub(super) fn get_ssh_secret(&self, cx: &Context<Self>) -> Option<String> {
        if !self.access.ssh_enabled {
            return None;
        }

        let passphrase = self
            .access
            .input_ssh_key_passphrase
            .read(cx)
            .value()
            .to_string();
        let password = self.access.input_ssh_password.read(cx).value().to_string();

        ssh_shared::get_ssh_secret(self.access.ssh_auth_method, &passphrase, &password)
    }

    pub(super) fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.apply_pending_auth_profile(window, cx);
        self.apply_pending_ssm_auth_profile();

        if !self.validate_form(true, cx) {
            cx.notify();
            return;
        }

        let Some(mut profile) = self.build_profile(cx) else {
            return;
        };

        let saved_profile_id = profile.id;

        let mut password = self.form.input_password.read(cx).value().to_string();
        let uri_password = profile.config.strip_uri_password();

        if password.is_empty()
            && let Some(uri_password) = uri_password
        {
            password = uri_password;
        }

        let ssh_secret = self.get_ssh_secret(cx);
        let is_edit = self.editing_profile_id.is_some();
        let password_source_is_literal = self
            .form
            .password_value_source_selector
            .read(cx)
            .is_literal(cx);

        info!(
            "{} profile: {}, save_password={}, password_len={}, ssh_enabled={}, ssh_auth={:?}",
            if is_edit { "Updating" } else { "Saving" },
            profile.name,
            profile.save_password,
            password.len(),
            self.access.ssh_enabled,
            self.access.ssh_auth_method
        );

        if self.settings_tab.conn_override_refresh_interval
            && profile
                .settings_overrides
                .as_ref()
                .is_none_or(|ov| ov.refresh_interval_secs.is_none())
        {
            Toast::warning("Refresh interval override ignored: value must be a positive number")
                .meta_right(now_hms())
                .push(cx);
        }

        if let Some(ref conn_settings) = profile.connection_settings
            && let Some(driver) = &self.form.selected_driver
            && let Some(schema) = driver.settings_schema()
        {
            let warnings = form_renderer::validate_values(&schema, conn_settings);
            for warning in warnings {
                Toast::warning(warning).meta_right(now_hms()).push(cx);
            }
        }

        self.app_state.update(cx, |state, cx| {
            if !password_source_is_literal {
                state.delete_password(&profile);
            } else if profile.save_password && !password.is_empty() {
                info!("Saving password to keyring for profile {}", profile.id);
                state.save_password(&profile, &SecretString::from(password.clone()));
            } else if !profile.save_password {
                state.delete_password(&profile);
            }

            if self.form.form_save_ssh_secret {
                if let Some(ref secret) = ssh_secret {
                    info!("Saving SSH secret to keyring for profile {}", profile.id);
                    state.save_ssh_password(&profile, &SecretString::from(secret.clone()));
                }
            } else {
                state.delete_ssh_password(&profile);
            }

            if is_edit {
                state.update_profile(profile);

                // If the edited profile is currently connected, surface a
                // reconnect prompt — the sidebar consumes this flag on the
                // next AppStateChanged and shows a toast with the choice.
                // The profile change itself is already persisted; only the
                // live session needs the explicit reconnect to pick it up.
                if state.connections().contains_key(&saved_profile_id) {
                    state.pending_edit_reconnect_prompt = Some(saved_profile_id);
                }
            } else {
                state.add_profile_in_folder(profile, self.target_folder_id);
            }

            #[cfg(feature = "mcp")]
            {
                use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

                if let Some(governance) = state
                    .profiles()
                    .iter()
                    .find(|item| item.id == saved_profile_id)
                    .and_then(|item| item.mcp_governance.clone())
                {
                    let assignments = governance
                        .policy_bindings
                        .into_iter()
                        .map(|binding| dbflux_policy::ConnectionPolicyAssignment {
                            actor_id: binding.actor_id,
                            scope: dbflux_policy::PolicyBindingScope {
                                connection_id: saved_profile_id.to_string(),
                            },
                            role_ids: binding.role_ids,
                            policy_ids: binding.policy_ids,
                        })
                        .collect();

                    if let Err(e) = state.save_mcp_connection_policy_assignment(
                        dbflux_mcp::ConnectionPolicyAssignmentDto {
                            connection_id: saved_profile_id.to_string(),
                            assignments,
                        },
                    ) {
                        report_error(
                            UserFacingError::new(
                                ErrorKind::Config,
                                format!("Failed to save MCP connection policy assignment: {e}"),
                            ),
                            cx,
                        );
                    }
                } else if let Err(e) = state.save_mcp_connection_policy_assignment(
                    dbflux_mcp::ConnectionPolicyAssignmentDto {
                        connection_id: saved_profile_id.to_string(),
                        assignments: Vec::new(),
                    },
                ) {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Config,
                            format!("Failed to clear MCP connection policy assignment: {e}"),
                        ),
                        cx,
                    );
                }

                cx.emit(dbflux_ui_base::McpRuntimeEventRaised {
                    event: dbflux_mcp::McpRuntimeEvent::ConnectionPolicyUpdated {
                        connection_id: saved_profile_id.to_string(),
                    },
                });
            }

            cx.emit(dbflux_ui_base::AppStateChanged);
        });

        cx.emit(DismissEvent);
        window.remove_window();
    }

    pub(super) fn test_connection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.apply_pending_auth_profile(window, cx);
        self.apply_pending_ssm_auth_profile();

        if !self.validate_form(false, cx) {
            cx.notify();
            return;
        }

        self.test_status = TestStatus::Testing;
        self.test_error = None;
        self.test_result = None;
        cx.notify();

        let Some(profile) = self.build_profile(cx) else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("Failed to build profile".to_string());
            cx.notify();
            return;
        };

        let Some(driver) = self.form.selected_driver.clone() else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("No driver selected".to_string());
            cx.notify();
            return;
        };

        let profile_name = profile.name.clone();
        let app_state = self.app_state.clone();
        let hook_context = self.app_state.read(cx).build_hook_context(&profile);
        let hooks = self.app_state.read(cx).resolve_profile_hooks(&profile);
        let hook_cancel_token = CancelToken::new();
        let detached_hook_scope = DetachedHookScope::default();
        let cleanup_cancel_token = hook_cancel_token.clone();
        let cleanup_detached_hook_scope = detached_hook_scope.clone();
        let cleanup_app_state = app_state.clone();
        let this = cx.entity().clone();

        let pipeline_input = if profile.uses_pipeline() {
            match self
                .app_state
                .read(cx)
                .build_pipeline_input_for_profile(profile.clone(), hook_cancel_token.clone())
            {
                Ok(input) => Some(input),
                Err(error) => {
                    self.test_status = TestStatus::Failed;
                    self.test_error = Some(error);
                    cx.notify();
                    return;
                }
            }
        } else {
            None
        };

        let password = self.form.input_password.read(cx).value().to_string();
        let password = (!password.is_empty()).then(|| SecretString::from(password));
        let ssh_secret = self.get_ssh_secret(cx).map(SecretString::from);

        cx.spawn(async move |_this, cx| {
            let profile_id = profile.id;
            let profile_name_for_hooks = profile_name.clone();
            let profile_name_for_cleanup = profile_name.clone();
            let phase_cx = cx.clone();
            let cleanup_cx = cx.clone();
            let background_executor = cx.background_executor().clone();

            let result = run_test_connection_orchestration(
                hooks,
                move |phase, phase_hooks, context| {
                    let app_state = app_state.clone();
                    let profile_name = profile_name_for_hooks.clone();
                    let hook_cancel_token = hook_cancel_token.clone();
                    let detached_hook_scope = detached_hook_scope.clone();
                    let mut phase_cx = phase_cx.clone();

                    Box::pin(async move {
                        run_hook_phase(
                            app_state,
                            profile_id,
                            profile_name,
                            phase,
                            phase_hooks,
                            context,
                            Some(hook_cancel_token),
                            &detached_hook_scope,
                            &mut phase_cx,
                        )
                        .await
                    })
                },
                move |drop_guards| {
                    let driver = driver.clone();
                    let profile = profile.clone();
                    let password = password.clone();
                    let ssh_secret = ssh_secret.clone();
                    let background_executor = background_executor.clone();

                    Box::pin(async move {
                        if let Some(pipeline_input) = pipeline_input {
                            background_executor
                                .spawn(async move {
                                    let (state_tx, _state_rx) =
                                        dbflux_core::pipeline_state_channel();
                                    let pipeline_output =
                                        dbflux_core::run_pipeline(pipeline_input, &state_tx)
                                            .await
                                            .map_err(|error| {
                                                format!(
                                                    "Pipeline stage '{}': {}",
                                                    error.stage, error.source
                                                )
                                            })?;

                                    let mut profile = pipeline_output.resolved_profile;
                                    if pipeline_output.access_handle.is_tunneled() {
                                        profile.config.redirect_to_tunnel(
                                            pipeline_output.access_handle.local_port(),
                                        );
                                    }

                                    let overrides =
                                        ConnectionOverrides::new(pipeline_output.resolved_password);
                                    let access_handle_drop = TestConnectionProbeResource {
                                        name: "pipeline access handle",
                                        drop_guard: drop_guards.access_handle,
                                    };
                                    let connection = driver
                                        .connect_with_overrides(&profile, &overrides)
                                        .map_err(|error| error.to_string())?;
                                    let connection_drop = TestConnectionProbeResource {
                                        name: "probe connection",
                                        drop_guard: drop_guards.connection,
                                    };

                                    drop(connection);
                                    drop(connection_drop);
                                    drop(pipeline_output.access_handle);
                                    drop(access_handle_drop);

                                    Ok(dbflux_core::TestConnectionResult::default())
                                })
                                .await
                        } else {
                            background_executor
                                .spawn(async move {
                                    let start = std::time::Instant::now();
                                    driver
                                        .test_connection_rich_with_secrets(
                                            &profile,
                                            password.as_ref(),
                                            ssh_secret.as_ref(),
                                        )
                                        .map(|mut result| {
                                            if result.rtt_ms.is_none() {
                                                result.rtt_ms =
                                                    Some(start.elapsed().as_millis() as u64);
                                            }
                                            result
                                        })
                                        .map_err(|error| error.to_string())
                                })
                                .await
                        }
                    })
                },
                move || {
                    let cleanup_app_state = cleanup_app_state.clone();
                    let cleanup_detached_hook_scope = cleanup_detached_hook_scope.clone();
                    let cleanup_cancel_token = cleanup_cancel_token.clone();
                    let mut cleanup_cx = cleanup_cx.clone();

                    Box::pin(async move {
                        cleanup_cancel_token.cancel();
                        cleanup_detached_hook_scope
                            .cancel_and_wait(cleanup_app_state, &mut cleanup_cx)
                            .await
                            .map_err(|error| {
                                format_detached_hook_cleanup_failure(
                                    profile_id,
                                    &profile_name_for_cleanup,
                                    &error,
                                )
                            })
                    })
                },
                hook_context,
            )
            .await;

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(result) => {
                            info!("Test connection successful for {}", profile_name);
                            this.test_status = if result.warnings.is_empty() {
                                TestStatus::Success
                            } else {
                                TestStatus::SuccessWithWarning
                            };
                            this.test_error =
                                (!result.warnings.is_empty()).then(|| result.warnings.join("\n"));
                            this.test_result = Some(result.test_result);
                        }
                        Err(error) => {
                            info!("Test connection failed: {}", error);
                            this.test_status = TestStatus::Failed;
                            this.test_error =
                                Some(normalize_aws_credentials_error(&profile_name, &error));
                            this.test_result = None;
                        }
                    }
                    cx.notify();
                });
            }) {
                log::warn!(
                    "Failed to apply test connection result to UI state: {:?}",
                    error
                );
            }
        })
        .detach();
    }
}

type TestConnectionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

type TestConnectionDropGuard = Arc<Mutex<Vec<&'static str>>>;

#[derive(Default)]
struct TestConnectionProbeDropGuards {
    connection: Option<TestConnectionDropGuard>,
    access_handle: Option<TestConnectionDropGuard>,
}

struct TestConnectionProbeResource {
    name: &'static str,
    drop_guard: Option<TestConnectionDropGuard>,
}

impl TestConnectionProbeResource {
    #[cfg(test)]
    fn new(name: &'static str, drop_guard: Option<TestConnectionDropGuard>) -> Self {
        Self { name, drop_guard }
    }
}

impl Drop for TestConnectionProbeResource {
    fn drop(&mut self) {
        if let Some(drop_guard) = &self.drop_guard {
            drop_guard
                .lock()
                .expect("drop log poisoned")
                .push(self.name);
        }
    }
}

struct TestConnectionOrchestrationResult {
    test_result: dbflux_core::TestConnectionResult,
    warnings: Vec<String>,
}

async fn run_test_connection_orchestration<'a, RunPhase, RunProbe, RunCleanup>(
    hooks: dbflux_core::ConnectionHooks,
    run_phase: RunPhase,
    run_probe: RunProbe,
    run_cleanup: RunCleanup,
    hook_context: dbflux_core::HookContext,
) -> Result<TestConnectionOrchestrationResult, String>
where
    RunPhase: FnMut(
        HookPhase,
        Vec<dbflux_core::ConnectionHook>,
        dbflux_core::HookContext,
    ) -> TestConnectionFuture<'a, HookPhaseState>,
    RunProbe: FnOnce(
        TestConnectionProbeDropGuards,
    )
        -> TestConnectionFuture<'a, Result<dbflux_core::TestConnectionResult, String>>,
    RunCleanup: FnOnce() -> TestConnectionFuture<'a, Result<(), String>>,
{
    run_test_connection_orchestration_with_drop_guards(
        hooks,
        run_phase,
        run_probe,
        run_cleanup,
        hook_context,
        TestConnectionProbeDropGuards::default(),
    )
    .await
}

async fn run_test_connection_orchestration_with_drop_guards<'a, RunPhase, RunProbe, RunCleanup>(
    hooks: dbflux_core::ConnectionHooks,
    mut run_phase: RunPhase,
    run_probe: RunProbe,
    run_cleanup: RunCleanup,
    hook_context: dbflux_core::HookContext,
    drop_guards: TestConnectionProbeDropGuards,
) -> Result<TestConnectionOrchestrationResult, String>
where
    RunPhase: FnMut(
        HookPhase,
        Vec<dbflux_core::ConnectionHook>,
        dbflux_core::HookContext,
    ) -> TestConnectionFuture<'a, HookPhaseState>,
    RunProbe: FnOnce(
        TestConnectionProbeDropGuards,
    )
        -> TestConnectionFuture<'a, Result<dbflux_core::TestConnectionResult, String>>,
    RunCleanup: FnOnce() -> TestConnectionFuture<'a, Result<(), String>>,
{
    let outcome =
        run_test_connection_phases(hooks, &mut run_phase, run_probe, hook_context, drop_guards)
            .await;
    let cleanup = run_cleanup().await;

    match (outcome, cleanup) {
        (Ok(result), Ok(())) => Ok(result),
        (Ok(_), Err(cleanup_error)) => {
            Err(format!("Test connection cleanup failed: {cleanup_error}"))
        }
        (Err(primary_error), Ok(())) => Err(primary_error),
        (Err(primary_error), Err(cleanup_error)) => Err(format!(
            "{primary_error} (cleanup warning: {cleanup_error})"
        )),
    }
}

async fn run_test_connection_phases<'a, RunPhase, RunProbe>(
    hooks: dbflux_core::ConnectionHooks,
    run_phase: &mut RunPhase,
    run_probe: RunProbe,
    hook_context: dbflux_core::HookContext,
    drop_guards: TestConnectionProbeDropGuards,
) -> Result<TestConnectionOrchestrationResult, String>
where
    RunPhase: FnMut(
        HookPhase,
        Vec<dbflux_core::ConnectionHook>,
        dbflux_core::HookContext,
    ) -> TestConnectionFuture<'a, HookPhaseState>,
    RunProbe: FnOnce(
        TestConnectionProbeDropGuards,
    )
        -> TestConnectionFuture<'a, Result<dbflux_core::TestConnectionResult, String>>,
{
    let pre_connect = run_phase(
        HookPhase::PreConnect,
        hooks.pre_connect,
        hook_context.clone(),
    )
    .await;
    let mut warnings = match pre_connect {
        HookPhaseState::Continue { warnings } => warnings,
        HookPhaseState::Aborted { error } => return Err(error),
        HookPhaseState::Cancelled => {
            return Err("Test connection cancelled by pre-connect hook".to_string());
        }
    };

    let result = run_probe(drop_guards).await?;

    let post_connect = run_phase(HookPhase::PostConnect, hooks.post_connect, hook_context).await;
    match post_connect {
        HookPhaseState::Continue {
            warnings: post_connect_warnings,
        } => {
            warnings.extend(post_connect_warnings);
            Ok(TestConnectionOrchestrationResult {
                test_result: result,
                warnings,
            })
        }
        HookPhaseState::Aborted { error } => Err(error),
        HookPhaseState::Cancelled => {
            Err("Test connection cancelled by post-connect hook".to_string())
        }
    }
}

fn format_detached_hook_cleanup_failure(
    profile_id: uuid::Uuid,
    profile_name: &str,
    error: &dbflux_ui_base::hook_phase_runner::DetachedHookCleanupError,
) -> String {
    let task_ids = error
        .task_ids()
        .iter()
        .map(uuid::Uuid::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "Failed to release detached test hook tasks for profile '{profile_name}' ({profile_id}); scoped task IDs [{task_ids}]: {}",
        error.source()
    )
}

/// Detect AWS SDK credential-resolution failures and replace them with a
/// user-facing message that directs to `~/.aws/credentials`.
///
/// AWS SDK error strings for missing credentials typically contain phrases
/// like "no credentials" or "CredentialsNotLoaded". We normalise these so the
/// user sees a clear, actionable message and is never prompted to enter a
/// secret access key directly into DBFlux.
fn normalize_aws_credentials_error(profile_name: &str, error: &str) -> String {
    let lower = error.to_ascii_lowercase();

    let is_missing_credentials = lower.contains("no credentials")
        || lower.contains("credentials not found")
        || lower.contains("credentialsnotloaded")
        || lower.contains("no credential providers")
        || lower.contains("no credentials in chain");

    if is_missing_credentials {
        return format!(
            "AWS credentials for profile '{}' could not be resolved. \
             Add the credentials to ~/.aws/credentials (or use environment \
             variables / IAM role) and retry. \
             DBFlux does not store AWS access keys — credentials are read \
             directly by the AWS SDK.",
            profile_name
        );
    }

    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        ConnectionHook, ConnectionHooks, HookExecutionMode, HookFailureMode, HookKind,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn hook(command: &str) -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: command.to_string(),
                args: Vec::new(),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            env_denylist: Vec::new(),
            timeout_ms: None,
            execution_mode: HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn current_unsaved_hook_context() -> dbflux_core::HookContext {
        dbflux_core::HookContext {
            profile_id: uuid::Uuid::from_u128(0x295),
            profile_name: "current unsaved profile".to_string(),
            db_kind: "postgres".to_string(),
            host: None,
            port: None,
            database: None,
            phase: None,
        }
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        use std::sync::Arc;
        use std::task::{Context, Poll, Wake, Waker};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(NoopWaker));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("test callback must complete without waiting"),
        }
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_passes_current_unsaved_context_and_returns_only_test_result() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let hooks = ConnectionHooks {
            pre_connect: vec![hook("current-pre")],
            post_connect: vec![hook("current-post")],
            ..Default::default()
        };

        let result = block_on(run_test_connection_orchestration(
            hooks,
            |phase, hooks, context| {
                let calls = calls.clone();
                let contexts = contexts.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("call log poisoned")
                        .push((phase, hooks[0].display_command()));
                    contexts.lock().expect("context log poisoned").push(context);
                    HookPhaseState::Continue {
                        warnings: Vec::new(),
                    }
                })
            },
            |_| {
                let calls = calls.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("call log poisoned")
                        .push((HookPhase::PreConnect, "direct-probe".to_string()));
                    Ok(dbflux_core::TestConnectionResult {
                        engine: Some("direct probe".to_string()),
                        ..Default::default()
                    })
                })
            },
            || Box::pin(async { Ok(()) }),
            current_unsaved_hook_context(),
        ));

        assert_eq!(
            result
                .expect("direct probe succeeds")
                .test_result
                .engine
                .as_deref(),
            Some("direct probe"),
        );
        assert_eq!(
            *calls.lock().expect("call log poisoned"),
            vec![
                (HookPhase::PreConnect, "current-pre".to_string()),
                (HookPhase::PreConnect, "direct-probe".to_string()),
                (HookPhase::PostConnect, "current-post".to_string()),
            ],
        );
        let contexts = contexts.lock().expect("context log poisoned");
        assert_eq!(contexts.len(), 2);
        for context in contexts.iter() {
            assert_eq!(context.profile_id, uuid::Uuid::from_u128(0x295));
            assert_eq!(context.profile_name, "current unsaved profile");
            assert_eq!(context.db_kind, "postgres");
        }
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_runs_direct_and_pipeline_success_through_connect_phases_once()
    {
        for probe_name in ["direct-probe", "pipeline-probe"] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let cleanup_calls = Arc::new(Mutex::new(0));
            let hooks = ConnectionHooks {
                pre_connect: vec![hook("pre-connect")],
                post_connect: vec![hook("post-connect")],
                pre_disconnect: vec![hook("must-not-run")],
                post_disconnect: vec![hook("must-not-run")],
            };

            let result = block_on(run_test_connection_orchestration(
                hooks,
                |phase, hooks, _| {
                    let calls = calls.clone();
                    Box::pin(async move {
                        calls
                            .lock()
                            .expect("call log poisoned")
                            .push((phase, hooks[0].display_command()));
                        HookPhaseState::Continue {
                            warnings: Vec::new(),
                        }
                    })
                },
                |_| {
                    let calls = calls.clone();
                    Box::pin(async move {
                        calls
                            .lock()
                            .expect("call log poisoned")
                            .push((HookPhase::PreConnect, probe_name.to_string()));
                        Ok(dbflux_core::TestConnectionResult {
                            engine: Some(probe_name.to_string()),
                            ..Default::default()
                        })
                    })
                },
                || {
                    let cleanup_calls = cleanup_calls.clone();
                    Box::pin(async move {
                        *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                        Ok(())
                    })
                },
                current_unsaved_hook_context(),
            ));

            assert_eq!(
                result
                    .expect("successful probe publishes one result")
                    .test_result
                    .engine
                    .as_deref(),
                Some(probe_name),
            );
            assert_eq!(
                *calls.lock().expect("call log poisoned"),
                vec![
                    (HookPhase::PreConnect, "pre-connect".to_string()),
                    (HookPhase::PreConnect, probe_name.to_string()),
                    (HookPhase::PostConnect, "post-connect".to_string()),
                ],
                "Test Connection must not invoke disconnect phases",
            );
            assert_eq!(
                *cleanup_calls.lock().expect("cleanup log poisoned"),
                1,
                "each terminal outcome performs one scoped cleanup",
            );
        }
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_cleans_up_after_pre_hook_abort_without_probe_or_disconnect() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let cleanup_calls = Arc::new(Mutex::new(0));
        let hooks = ConnectionHooks {
            pre_connect: vec![hook("pre-abort")],
            post_connect: vec![hook("must-not-run")],
            pre_disconnect: vec![hook("must-not-run")],
            post_disconnect: vec![hook("must-not-run")],
        };

        let result = block_on(run_test_connection_orchestration(
            hooks,
            |phase, hooks, _| {
                let calls = calls.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("call log poisoned")
                        .push((phase, hooks[0].display_command()));
                    HookPhaseState::Aborted {
                        error: "pre-connect Disconnect policy failed".to_string(),
                    }
                })
            },
            |_| Box::pin(async { panic!("pre-hook abort must skip probe") }),
            || {
                let cleanup_calls = cleanup_calls.clone();
                Box::pin(async move {
                    *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                    Ok(())
                })
            },
            current_unsaved_hook_context(),
        ));

        assert!(matches!(result, Err(error) if error == "pre-connect Disconnect policy failed"));
        assert_eq!(
            *calls.lock().expect("call log poisoned"),
            vec![(HookPhase::PreConnect, "pre-abort".to_string())],
        );
        assert_eq!(*cleanup_calls.lock().expect("cleanup log poisoned"), 1);
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_cleans_up_after_post_hook_disconnect_without_disconnect_phases()
     {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let cleanup_calls = Arc::new(Mutex::new(0));
        let hooks = ConnectionHooks {
            pre_connect: vec![hook("pre-connect")],
            post_connect: vec![hook("post-disconnect")],
            pre_disconnect: vec![hook("must-not-run")],
            post_disconnect: vec![hook("must-not-run")],
        };

        let result = block_on(run_test_connection_orchestration(
            hooks,
            |phase, hooks, _| {
                let calls = calls.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("call log poisoned")
                        .push((phase, hooks[0].display_command()));
                    match phase {
                        HookPhase::PreConnect => HookPhaseState::Continue {
                            warnings: Vec::new(),
                        },
                        HookPhase::PostConnect => HookPhaseState::Aborted {
                            error: "post-connect Disconnect policy failed".to_string(),
                        },
                        HookPhase::PreDisconnect | HookPhase::PostDisconnect => {
                            panic!("Test Connection must not run disconnect phases")
                        }
                    }
                })
            },
            |_| {
                Box::pin(async {
                    Ok(dbflux_core::TestConnectionResult {
                        engine: Some("reachable".to_string()),
                        ..Default::default()
                    })
                })
            },
            || {
                let cleanup_calls = cleanup_calls.clone();
                Box::pin(async move {
                    *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                    Ok(())
                })
            },
            current_unsaved_hook_context(),
        ));

        assert!(matches!(result, Err(error) if error == "post-connect Disconnect policy failed"));
        assert_eq!(
            *calls.lock().expect("call log poisoned"),
            vec![
                (HookPhase::PreConnect, "pre-connect".to_string()),
                (HookPhase::PostConnect, "post-disconnect".to_string()),
            ],
        );
        assert_eq!(*cleanup_calls.lock().expect("cleanup log poisoned"), 1);
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_stops_after_failed_pipeline_probe() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let hooks = ConnectionHooks {
            pre_connect: vec![hook("pipeline-pre")],
            post_connect: vec![hook("pipeline-post")],
            pre_disconnect: vec![hook("must-not-run")],
            post_disconnect: vec![hook("must-not-run")],
        };

        let result = block_on(run_test_connection_orchestration(
            hooks,
            |phase, hooks, _| {
                let calls = calls.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("call log poisoned")
                        .push((phase, hooks[0].display_command()));
                    HookPhaseState::Continue {
                        warnings: Vec::new(),
                    }
                })
            },
            |_| {
                let calls = calls.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("call log poisoned")
                        .push((HookPhase::PreConnect, "pipeline-probe".to_string()));
                    Err("pipeline probe failed".to_string())
                })
            },
            || Box::pin(async { Ok(()) }),
            current_unsaved_hook_context(),
        ));

        assert!(matches!(result, Err(error) if error == "pipeline probe failed"));
        assert_eq!(
            *calls.lock().expect("call log poisoned"),
            vec![
                (HookPhase::PreConnect, "pipeline-pre".to_string()),
                (HookPhase::PreConnect, "pipeline-probe".to_string()),
            ],
        );
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_preserves_probe_failure_when_cleanup_fails() {
        let cleanup_calls = Arc::new(Mutex::new(0));

        let result = block_on(run_test_connection_orchestration(
            ConnectionHooks::default(),
            |_phase, _hooks, _context| {
                Box::pin(async move {
                    HookPhaseState::Continue {
                        warnings: Vec::new(),
                    }
                })
            },
            |_| Box::pin(async { Err("driver probe failed".to_string()) }),
            || {
                let cleanup_calls = cleanup_calls.clone();
                Box::pin(async move {
                    *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                    Err("detached hook cleanup failed".to_string())
                })
            },
            current_unsaved_hook_context(),
        ));

        assert!(
            matches!(result, Err(error) if error == "driver probe failed (cleanup warning: detached hook cleanup failed)")
        );
        assert_eq!(*cleanup_calls.lock().expect("cleanup log poisoned"), 1);
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_returns_hook_warnings_after_cleanup() {
        let cleanup_calls = Arc::new(Mutex::new(0));

        let result = block_on(run_test_connection_orchestration(
            ConnectionHooks::default(),
            |phase, _hooks, _context| {
                Box::pin(async move {
                    HookPhaseState::Continue {
                        warnings: vec![format!("{} hook warning", phase.label())],
                    }
                })
            },
            |_| {
                Box::pin(async {
                    Ok(dbflux_core::TestConnectionResult {
                        engine: Some("reachable".to_string()),
                        ..Default::default()
                    })
                })
            },
            || {
                let cleanup_calls = cleanup_calls.clone();
                Box::pin(async move {
                    *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                    Ok(())
                })
            },
            current_unsaved_hook_context(),
        ));

        let result = result.expect("warnings do not fail the test");
        assert_eq!(result.test_result.engine.as_deref(), Some("reachable"));
        assert_eq!(
            result.warnings,
            vec!["Pre-connect hook warning", "Post-connect hook warning"],
        );
        assert_eq!(*cleanup_calls.lock().expect("cleanup log poisoned"), 1);
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_returns_post_hook_warn_but_not_ignore_as_warning() {
        for (post_hook_warnings, expected_warnings) in [
            (
                vec!["post-connect Warn policy failed".to_string()],
                vec!["post-connect Warn policy failed"],
            ),
            (Vec::new(), Vec::new()),
        ] {
            let phase_calls = Arc::new(Mutex::new(0));
            let cleanup_calls = Arc::new(Mutex::new(0));

            let result = block_on(run_test_connection_orchestration(
                ConnectionHooks::default(),
                |_phase, _hooks, _context| {
                    let phase_calls = phase_calls.clone();
                    let post_hook_warnings = post_hook_warnings.clone();
                    Box::pin(async move {
                        let mut phase_calls = phase_calls.lock().expect("phase log poisoned");
                        *phase_calls += 1;

                        if *phase_calls == 1 {
                            HookPhaseState::Continue {
                                warnings: Vec::new(),
                            }
                        } else {
                            HookPhaseState::Continue {
                                warnings: post_hook_warnings,
                            }
                        }
                    })
                },
                |_| {
                    Box::pin(async {
                        Ok(dbflux_core::TestConnectionResult {
                            engine: Some("reachable".to_string()),
                            ..Default::default()
                        })
                    })
                },
                || {
                    let cleanup_calls = cleanup_calls.clone();
                    Box::pin(async move {
                        *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                        Ok(())
                    })
                },
                current_unsaved_hook_context(),
            ));

            let result = result.expect("Warn and Ignore post hooks preserve successful probe");
            assert_eq!(result.test_result.engine.as_deref(), Some("reachable"));
            assert_eq!(result.warnings, expected_warnings);
            assert_eq!(*phase_calls.lock().expect("phase log poisoned"), 2);
            assert_eq!(*cleanup_calls.lock().expect("cleanup log poisoned"), 1);
        }
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_cleans_up_after_cancelled_hook() {
        let cleanup_calls = Arc::new(Mutex::new(0));

        let result = block_on(run_test_connection_orchestration(
            ConnectionHooks::default(),
            |_phase, _hooks, _context| Box::pin(async { HookPhaseState::Cancelled }),
            |_| Box::pin(async { panic!("cancelled pre-connect hook must skip probe") }),
            || {
                let cleanup_calls = cleanup_calls.clone();
                Box::pin(async move {
                    *cleanup_calls.lock().expect("cleanup log poisoned") += 1;
                    Ok(())
                })
            },
            current_unsaved_hook_context(),
        ));

        assert!(
            matches!(result, Err(error) if error == "Test connection cancelled by pre-connect hook")
        );
        assert_eq!(*cleanup_calls.lock().expect("cleanup log poisoned"), 1);
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_fails_successful_probe_when_cleanup_fails() {
        let result = block_on(run_test_connection_orchestration(
            ConnectionHooks::default(),
            |_phase, _hooks, _context| {
                Box::pin(async move {
                    HookPhaseState::Continue {
                        warnings: Vec::new(),
                    }
                })
            },
            |_| Box::pin(async { Ok(dbflux_core::TestConnectionResult::default()) }),
            || Box::pin(async { Err("access handle did not close".to_string()) }),
            current_unsaved_hook_context(),
        ));

        assert!(
            matches!(result, Err(error) if error == "Test connection cleanup failed: access handle did not close")
        );
    }

    #[::core::prelude::v1::test]
    fn test_connection_orchestration_drops_probe_connection_and_access_handle_before_cleanup() {
        let drops = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let cleanup_observed_drops = drops.clone();

        let result = block_on(run_test_connection_orchestration_with_drop_guards(
            ConnectionHooks::default(),
            |_phase, _hooks, _context| {
                Box::pin(async move {
                    HookPhaseState::Continue {
                        warnings: Vec::new(),
                    }
                })
            },
            |drop_guards| {
                Box::pin(async move {
                    let connection = TestConnectionProbeResource::new(
                        "probe connection",
                        drop_guards.connection,
                    );
                    let access_handle = TestConnectionProbeResource::new(
                        "pipeline access handle",
                        drop_guards.access_handle,
                    );

                    drop(connection);
                    drop(access_handle);

                    Ok(dbflux_core::TestConnectionResult::default())
                })
            },
            move || {
                Box::pin(async move {
                    assert_eq!(
                        *cleanup_observed_drops.lock().expect("drop log poisoned"),
                        vec!["probe connection", "pipeline access handle"],
                        "the actual probe resources must drop before cleanup starts",
                    );
                    Ok(())
                })
            },
            current_unsaved_hook_context(),
            TestConnectionProbeDropGuards {
                connection: Some(drops.clone()),
                access_handle: Some(drops),
            },
        ));

        assert!(result.is_ok(), "cleanup follows the dropped resources");
    }

    #[::core::prelude::v1::test]
    fn detached_hook_cleanup_failure_keeps_scope_and_source_context() {
        let task_id = uuid::Uuid::from_u128(0x2_500);
        let error = dbflux_ui_base::hook_phase_runner::DetachedHookCleanupError::new(
            vec![task_id],
            "app state was released",
        );

        let message = format_detached_hook_cleanup_failure(
            uuid::Uuid::from_u128(0x295),
            "current unsaved profile",
            &error,
        );

        assert!(message.contains("current unsaved profile"));
        assert!(message.contains("00000000-0000-0000-0000-000000000295"));
        assert!(message.contains(&task_id.to_string()));
        assert!(message.contains("app state was released"));
    }

    #[::core::prelude::v1::test]
    fn normalize_aws_credentials_error_rewrites_no_credentials() {
        let result = normalize_aws_credentials_error("my-profile", "no credentials provided");
        assert!(
            result.contains("~/.aws/credentials"),
            "error should direct user to ~/.aws/credentials"
        );
        assert!(!result.contains("secret"), "error must not mention secrets");
        assert!(
            result.contains("my-profile"),
            "error should name the profile"
        );
    }

    #[::core::prelude::v1::test]
    fn normalize_aws_credentials_error_rewrites_credentials_not_found() {
        let result =
            normalize_aws_credentials_error("ci-user", "Credentials not found for profile");
        assert!(result.contains("~/.aws/credentials"));
        assert!(result.contains("ci-user"));
    }

    #[::core::prelude::v1::test]
    fn normalize_aws_credentials_error_rewrites_no_credentials_in_chain() {
        let result = normalize_aws_credentials_error("prod", "no credentials in chain");
        assert!(result.contains("~/.aws/credentials"));
    }

    #[::core::prelude::v1::test]
    fn normalize_aws_credentials_error_preserves_unrelated_errors() {
        let original = "connection refused: 127.0.0.1:5432";
        let result = normalize_aws_credentials_error("pg-local", original);
        assert_eq!(result, original);
    }

    #[::core::prelude::v1::test]
    fn normalize_aws_credentials_error_is_case_insensitive() {
        let result = normalize_aws_credentials_error("dev", "NO CREDENTIALS");
        assert!(result.contains("~/.aws/credentials"));
    }
}
