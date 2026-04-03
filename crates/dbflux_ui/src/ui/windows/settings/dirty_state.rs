use super::SettingsWindow;
use crate::ui::windows::ssh_shared::SshAuthSelection;
use gpui::App;

impl SettingsWindow {
    pub(super) fn has_unsaved_changes(&self, cx: &App) -> bool {
        self.has_unsaved_general_changes(cx)
            || self.has_unsaved_ssh_changes(cx)
            || self.has_unsaved_svc_changes(cx)
            || self.has_unsaved_hook_changes(cx)
            || self.has_unsaved_driver_changes(cx)
    }

    pub(super) fn has_unsaved_general_changes(&self, cx: &App) -> bool {
        let saved = self.app_state.read(cx).general_settings();

        if self.gen_settings.theme != saved.theme
            || self.gen_settings.restore_session_on_startup != saved.restore_session_on_startup
            || self.gen_settings.reopen_last_connections != saved.reopen_last_connections
            || self.gen_settings.default_focus_on_startup != saved.default_focus_on_startup
            || self.gen_settings.default_refresh_policy != saved.default_refresh_policy
            || self.gen_settings.auto_refresh_pause_on_error != saved.auto_refresh_pause_on_error
            || self.gen_settings.auto_refresh_only_if_visible != saved.auto_refresh_only_if_visible
            || self.gen_settings.confirm_dangerous_queries != saved.confirm_dangerous_queries
            || self.gen_settings.dangerous_requires_where != saved.dangerous_requires_where
            || self.gen_settings.dangerous_requires_preview != saved.dangerous_requires_preview
        {
            return true;
        }

        let history_val = self.input_max_history.read(cx).value().trim().to_string();
        if history_val != saved.max_history_entries.to_string() {
            return true;
        }

        let auto_save_val = self.input_auto_save.read(cx).value().trim().to_string();
        if auto_save_val != saved.auto_save_interval_ms.to_string() {
            return true;
        }

        let refresh_val = self
            .input_refresh_interval
            .read(cx)
            .value()
            .trim()
            .to_string();
        if refresh_val != saved.default_refresh_interval_secs.to_string() {
            return true;
        }

        let bg_tasks_val = self.input_max_bg_tasks.read(cx).value().trim().to_string();
        if bg_tasks_val != saved.max_concurrent_background_tasks.to_string() {
            return true;
        }

        false
    }

    pub(super) fn has_unsaved_ssh_changes(&self, cx: &App) -> bool {
        if let Some(id) = self.editing_tunnel_id {
            let tunnels = self.app_state.read(cx).ssh_tunnels();
            let Some(saved) = tunnels.iter().find(|t| t.id == id) else {
                return true;
            };

            let name = self.input_tunnel_name.read(cx).value().trim().to_string();
            let host = self.input_ssh_host.read(cx).value().trim().to_string();
            let port_str = self.input_ssh_port.read(cx).value().trim().to_string();
            let user = self.input_ssh_user.read(cx).value().trim().to_string();

            if name != saved.name
                || host != saved.config.host
                || port_str != saved.config.port.to_string()
                || user != saved.config.user
                || self.form_save_secret != saved.save_secret
            {
                return true;
            }

            match (&self.ssh_auth_method, &saved.config.auth_method) {
                (SshAuthSelection::PrivateKey, dbflux_core::SshAuthMethod::PrivateKey { .. }) => {
                    let key_path = self.input_ssh_key_path.read(cx).value().trim().to_string();
                    let saved_key_path = match &saved.config.auth_method {
                        dbflux_core::SshAuthMethod::PrivateKey { key_path } => key_path
                            .as_ref()
                            .map(|p| p.to_string_lossy().to_string())
                            .unwrap_or_default(),
                        _ => String::new(),
                    };
                    if key_path != saved_key_path {
                        return true;
                    }
                }
                (SshAuthSelection::Password, dbflux_core::SshAuthMethod::Password) => {}
                _ => return true,
            }

            false
        } else {
            let name = self.input_tunnel_name.read(cx).value().trim().to_string();
            let host = self.input_ssh_host.read(cx).value().trim().to_string();
            !name.is_empty() || !host.is_empty()
        }
    }

    pub(super) fn has_unsaved_svc_changes(&self, cx: &App) -> bool {
        if let Some(idx) = self.editing_svc_idx {
            let Some(saved) = self.svc_services.get(idx) else {
                return true;
            };

            let socket_id = self.input_socket_id.read(cx).value().trim().to_string();
            let command = self.input_svc_command.read(cx).value().trim().to_string();
            let timeout = self.input_svc_timeout.read(cx).value().trim().to_string();

            let saved_command = saved.command.as_deref().unwrap_or("").to_string();
            let saved_timeout = saved
                .startup_timeout_ms
                .map(|v| v.to_string())
                .unwrap_or_default();

            if socket_id != saved.socket_id
                || command != saved_command
                || timeout != saved_timeout
                || self.svc_enabled != saved.enabled
            {
                return true;
            }

            let form_args: Vec<String> = self
                .svc_arg_inputs
                .iter()
                .map(|input| input.read(cx).value().trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if form_args != saved.args {
                return true;
            }

            let form_env: Vec<(String, String)> = self
                .svc_env_key_inputs
                .iter()
                .zip(self.svc_env_value_inputs.iter())
                .filter_map(|(k, v)| {
                    let key = k.read(cx).value().trim().to_string();
                    if key.is_empty() {
                        return None;
                    }
                    Some((key, v.read(cx).value().to_string()))
                })
                .collect();
            let mut saved_env: Vec<(String, String)> = saved
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            saved_env.sort_by(|a, b| a.0.cmp(&b.0));
            let mut form_env_sorted = form_env;
            form_env_sorted.sort_by(|a, b| a.0.cmp(&b.0));
            if form_env_sorted != saved_env {
                return true;
            }

            false
        } else {
            let socket_id = self.input_socket_id.read(cx).value().trim().to_string();
            !socket_id.is_empty()
        }
    }
}
