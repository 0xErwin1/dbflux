use crate::ui::components::form_renderer;
use crate::ui::components::toast::ToastExt;
use crate::ui::windows::ssh_shared;
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::ValueRef;
use dbflux_core::{
    CancelToken, ConnectionOverrides, ConnectionProfile, DbConfig, FormFieldKind, SshTunnelConfig,
};
use gpui::*;
use log::info;

use super::{ConnectionManagerWindow, DismissEvent, TestStatus};

impl ConnectionManagerWindow {
    pub(super) fn validate_form(&mut self, require_name: bool, cx: &mut Context<Self>) -> bool {
        self.validation_errors.clear();

        if require_name {
            let name = self.input_name.read(cx).value().to_string();
            if name.trim().is_empty() {
                self.validation_errors
                    .push("Connection name is required".to_string());
            }
        }

        let Some(driver) = &self.selected_driver else {
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

        if self.ssh_enabled && form.supports_ssh() {
            let ssh_host = self.input_ssh_host.read(cx).value().to_string();
            if ssh_host.trim().is_empty() {
                self.validation_errors
                    .push("SSH Host is required when SSH is enabled".to_string());
            }

            let ssh_user = self.input_ssh_user.read(cx).value().to_string();
            if ssh_user.trim().is_empty() {
                self.validation_errors
                    .push("SSH User is required when SSH is enabled".to_string());
            }

            let ssh_port_str = self.input_ssh_port.read(cx).value().to_string();
            if !ssh_port_str.trim().is_empty() && ssh_port_str.parse::<u16>().is_err() {
                self.validation_errors
                    .push("SSH Port must be a valid number".to_string());
            }
        }

        // Validate SSM fields if SSM access method is selected (T-7.3)
        if self.is_ssm_selected() {
            let instance_id = self.input_ssm_instance_id.read(cx).value().to_string();
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

            let region = self.input_ssm_region.read(cx).value().to_string();
            if region.trim().is_empty() {
                self.validation_errors
                    .push("SSM Region is required".to_string());
            }

            let port_str = self.input_ssm_remote_port.read(cx).value().to_string();
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

        let uses_dynamic_auth_sources = self.collect_value_refs(cx).values().any(|value_ref| {
            matches!(
                value_ref,
                ValueRef::Secret { .. } | ValueRef::Parameter { .. } | ValueRef::Auth { .. }
            )
        });

        if uses_dynamic_auth_sources {
            let Some(auth_profile_id) = self.selected_auth_profile_id else {
                self.validation_errors.push(
                    "Dynamic value sources require an Auth Profile. Select one in Access tab."
                        .to_string(),
                );
                return self.validation_errors.is_empty();
            };

            let selected_profile = self
                .app_state
                .read(cx)
                .auth_profiles()
                .iter()
                .find(|profile| profile.id == auth_profile_id);

            if let Some(profile) = selected_profile {
                if !profile.provider_id.starts_with("aws") {
                    self.validation_errors.push(format!(
                        "Selected Auth Profile '{}' cannot resolve AWS Secret/Parameter value sources.",
                        profile.name
                    ));
                }
            } else {
                self.validation_errors.push(
                    "Selected Auth Profile no longer exists. Re-select it in Access tab."
                        .to_string(),
                );
            }
        }

        if let Some(bindings) = self.collect_hook_bindings(cx) {
            let hook_definitions = self.app_state.read(cx).hook_definitions();

            for hook_id in &bindings.pre_connect {
                if !hook_definitions.contains_key(hook_id) {
                    self.validation_errors.push(format!(
                        "Unknown pre-connect hook ID '{}'. Configure it in Settings > Hooks",
                        hook_id
                    ));
                }
            }

            for hook_id in &bindings.post_connect {
                if !hook_definitions.contains_key(hook_id) {
                    self.validation_errors.push(format!(
                        "Unknown post-connect hook ID '{}'. Configure it in Settings > Hooks",
                        hook_id
                    ));
                }
            }

            for hook_id in &bindings.pre_disconnect {
                if !hook_definitions.contains_key(hook_id) {
                    self.validation_errors.push(format!(
                        "Unknown pre-disconnect hook ID '{}'. Configure it in Settings > Hooks",
                        hook_id
                    ));
                }
            }

            for hook_id in &bindings.post_disconnect {
                if !hook_definitions.contains_key(hook_id) {
                    self.validation_errors.push(format!(
                        "Unknown post-disconnect hook ID '{}'. Configure it in Settings > Hooks",
                        hook_id
                    ));
                }
            }
        }

        self.validation_errors.is_empty()
    }

    pub(super) fn build_ssh_config(&self, cx: &Context<Self>) -> Option<SshTunnelConfig> {
        if !self.ssh_enabled {
            return None;
        }

        let host = self.input_ssh_host.read(cx).value().to_string();
        let port_str = self.input_ssh_port.read(cx).value().to_string();
        let user = self.input_ssh_user.read(cx).value().to_string();
        let key_path_str = self.input_ssh_key_path.read(cx).value().to_string();

        Some(ssh_shared::build_ssh_config(
            &host,
            &port_str,
            &user,
            self.ssh_auth_method,
            &key_path_str,
        ))
    }

    pub(super) fn build_config(&self, cx: &Context<Self>) -> Option<DbConfig> {
        let driver = self.selected_driver.as_ref()?;
        let values = self.collect_form_values(driver.form_definition(), cx);

        let mut config = match driver.build_config(&values) {
            Ok(config) => config,
            Err(e) => {
                log::error!("Failed to build config: {}", e);
                return None;
            }
        };

        let ssh_tunnel = self.build_ssh_config(cx);
        let ssh_tunnel_profile_id = self.selected_ssh_tunnel_id;

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
            } => {
                *tunnel = ssh_tunnel;
                *profile_id = ssh_tunnel_profile_id;
            }
            DbConfig::SQLite { .. } | DbConfig::External { .. } => {}
        }

        Some(config)
    }

    pub(super) fn build_profile(&self, cx: &Context<Self>) -> Option<ConnectionProfile> {
        let name = self.input_name.read(cx).value().to_string();
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

        profile.save_password = self.form_save_password;
        profile.proxy_profile_id = self.selected_proxy_id;
        profile.auth_profile_id = self.selected_auth_profile_id;
        profile.value_refs = self.collect_value_refs(cx);
        profile.settings_overrides = self.collect_connection_overrides(cx);
        profile.connection_settings = self.collect_connection_settings(cx);
        profile.hook_bindings = self.collect_hook_bindings(cx);

        // Collect access kind — update SSM fields from inputs if SSM is selected
        let access_kind = if self.is_ssm_selected() {
            Some(self.collect_ssm_access_kind(cx))
        } else {
            self.access_kind.clone()
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
        if !self.ssh_enabled {
            return None;
        }

        let passphrase = self.input_ssh_key_passphrase.read(cx).value().to_string();
        let password = self.input_ssh_password.read(cx).value().to_string();

        ssh_shared::get_ssh_secret(self.ssh_auth_method, &passphrase, &password)
    }

    pub(super) fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.apply_pending_auth_profile();
        self.apply_pending_ssm_auth_profile();

        if !self.validate_form(true, cx) {
            cx.notify();
            return;
        }

        let Some(mut profile) = self.build_profile(cx) else {
            return;
        };

        let mut password = self.input_password.read(cx).value().to_string();
        let uri_password = profile.config.strip_uri_password();

        if password.is_empty()
            && let Some(uri_password) = uri_password
        {
            password = uri_password;
        }

        let ssh_secret = self.get_ssh_secret(cx);
        let is_edit = self.editing_profile_id.is_some();
        let password_source_is_literal =
            self.password_value_source_selector.read(cx).is_literal(cx);

        info!(
            "{} profile: {}, save_password={}, password_len={}, ssh_enabled={}, ssh_auth={:?}",
            if is_edit { "Updating" } else { "Saving" },
            profile.name,
            profile.save_password,
            password.len(),
            self.ssh_enabled,
            self.ssh_auth_method
        );

        if self.conn_override_refresh_interval
            && profile
                .settings_overrides
                .as_ref()
                .is_none_or(|ov| ov.refresh_interval_secs.is_none())
        {
            cx.toast_warning(
                "Refresh interval override ignored: value must be a positive number".to_string(),
                window,
            );
        }

        if let Some(ref conn_settings) = profile.connection_settings
            && let Some(driver) = &self.selected_driver
            && let Some(schema) = driver.settings_schema()
        {
            let warnings = form_renderer::validate_values(&schema, conn_settings);
            for warning in warnings {
                cx.toast_warning(warning, window);
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

            if self.form_save_ssh_secret {
                if let Some(ref secret) = ssh_secret {
                    info!("Saving SSH secret to keyring for profile {}", profile.id);
                    state.save_ssh_password(&profile, &SecretString::from(secret.clone()));
                }
            } else {
                state.delete_ssh_password(&profile);
            }

            if is_edit {
                state.update_profile(profile);
            } else {
                state.add_profile_in_folder(profile, self.target_folder_id);
            }

            cx.emit(crate::app::AppStateChanged);
        });

        cx.emit(DismissEvent);
        window.remove_window();
    }

    pub(super) fn test_connection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.apply_pending_auth_profile();
        self.apply_pending_ssm_auth_profile();

        if !self.validate_form(false, cx) {
            cx.notify();
            return;
        }

        self.test_status = TestStatus::Testing;
        self.test_error = None;
        cx.notify();

        let Some(profile) = self.build_profile(cx) else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("Failed to build profile".to_string());
            cx.notify();
            return;
        };

        let Some(driver) = self.selected_driver.clone() else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("No driver selected".to_string());
            cx.notify();
            return;
        };

        let profile_name = profile.name.clone();
        let this = cx.entity().clone();

        let task = if profile.uses_pipeline() {
            let pipeline_input = match self
                .app_state
                .read(cx)
                .build_pipeline_input_for_profile(profile, CancelToken::new())
            {
                Ok(input) => input,
                Err(error) => {
                    self.test_status = TestStatus::Failed;
                    self.test_error = Some(error);
                    cx.notify();
                    return;
                }
            };

            cx.background_executor().spawn(async move {
                let mut input = pipeline_input;

                #[cfg(feature = "aws")]
                {
                    if let Some(auth_profile) = input.auth_profile.as_ref()
                        && auth_profile.provider_id.starts_with("aws")
                    {
                        // Use the blocking variant — the GPUI background executor
                        // has no Tokio runtime, so the async version panics.
                        match dbflux_aws::value_providers_for_auth_profile_blocking(auth_profile) {
                            Ok((secret_provider, parameter_provider)) => {
                                input
                                    .resolver
                                    .register_secret_provider(std::sync::Arc::new(secret_provider));
                                input
                                    .resolver
                                    .register_parameter_provider(std::sync::Arc::new(
                                        parameter_provider,
                                    ));
                            }
                            Err(error) => {
                                log::warn!(
                                    "Failed to initialize AWS value providers for profile '{}': {}",
                                    auth_profile.name,
                                    error
                                );
                            }
                        }
                    }
                }

                let (state_tx, _state_rx) = dbflux_core::pipeline_state_channel();
                let pipeline_output =
                    dbflux_core::run_pipeline(input, &state_tx)
                        .await
                        .map_err(|error| {
                            format!("Pipeline stage '{}': {}", error.stage, error.source)
                        })?;

                let mut profile = pipeline_output.resolved_profile;
                if pipeline_output.access_handle.is_tunneled() {
                    profile
                        .config
                        .redirect_to_tunnel(pipeline_output.access_handle.local_port());
                }

                let overrides = ConnectionOverrides::new(pipeline_output.resolved_password);
                let connection = driver
                    .connect_with_overrides(&profile, &overrides)
                    .map_err(|error| error.to_string())?;

                drop(connection);

                Ok::<(), String>(())
            })
        } else {
            let password = self.input_password.read(cx).value().to_string();
            let password_opt = if password.is_empty() {
                None
            } else {
                Some(SecretString::from(password))
            };

            let ssh_secret = self.get_ssh_secret(cx).map(SecretString::from);

            cx.background_executor().spawn(async move {
                driver
                    .connect_with_secrets(&profile, password_opt.as_ref(), ssh_secret.as_ref())
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            })
        };

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(()) => {
                            info!("Test connection successful for {}", profile_name);
                            this.test_status = TestStatus::Success;
                            this.test_error = None;
                        }
                        Err(error) => {
                            info!("Test connection failed: {}", error);
                            this.test_status = TestStatus::Failed;
                            this.test_error = Some(error);
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
