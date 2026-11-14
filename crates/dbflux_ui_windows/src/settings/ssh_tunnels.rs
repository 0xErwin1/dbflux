use crate::ssh_shared::{self, SshAuthSelection};
use dbflux_app::keymap::Modifiers;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{SshAuthMethod, SshTunnelProfile};
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;
use uuid::Uuid;

use super::form_nav::FormGridNav;
use super::form_section::FormSection;
use super::ssh_tunnels_section::{
    SshFocus, SshFormField, SshManualValues, SshTestStatus, SshTunnelsSection,
};

/// SSH form navigation. Wraps `FormGridNav` with auth/editing context
/// to compute which rows are visible.
#[derive(Clone)]
pub(super) struct SshFormNav {
    pub(super) auth_method: SshAuthSelection,
    pub(super) editing_id: Option<Uuid>,
    nav: FormGridNav<SshFormField>,
}

impl SshFormNav {
    pub(super) fn new(
        auth_method: SshAuthSelection,
        editing_id: Option<Uuid>,
        field: SshFormField,
    ) -> Self {
        Self {
            auth_method,
            editing_id,
            nav: FormGridNav::new(field),
        }
    }

    pub(super) fn field(&self) -> SshFormField {
        self.nav.focused
    }

    #[cfg(test)]
    pub(super) fn set_field(&mut self, field: SshFormField) {
        self.nav.focused = field;
    }

    pub(super) fn form_rows(&self) -> Vec<Vec<SshFormField>> {
        let mut rows = vec![
            vec![SshFormField::Name],
            vec![SshFormField::SshConfigHost],
            vec![SshFormField::Host, SshFormField::Port],
            vec![SshFormField::User],
            vec![SshFormField::AuthPrivateKey, SshFormField::AuthPassword],
        ];

        match self.auth_method {
            SshAuthSelection::PrivateKey => {
                rows.push(vec![SshFormField::KeyPath, SshFormField::KeyBrowse]);
                rows.push(vec![SshFormField::Passphrase, SshFormField::SaveSecret]);
            }
            SshAuthSelection::Password => {
                rows.push(vec![SshFormField::Password, SshFormField::SaveSecret]);
            }
        }

        if self.editing_id.is_some() {
            rows.push(vec![
                SshFormField::ExportButton,
                SshFormField::DeleteButton,
                SshFormField::TestButton,
                SshFormField::SaveButton,
            ]);
        } else {
            rows.push(vec![SshFormField::TestButton, SshFormField::SaveButton]);
        }

        rows
    }

    pub(super) fn validate_field(&mut self) {
        let fallback = match self.auth_method {
            SshAuthSelection::PrivateKey => SshFormField::KeyPath,
            SshAuthSelection::Password => SshFormField::Password,
        };
        let rows = self.form_rows();
        self.nav.validate(&rows, fallback);
    }

    #[cfg(test)]
    pub(super) fn move_down(&mut self) {
        let rows = self.form_rows();
        self.nav.move_down(&rows);
    }

    #[cfg(test)]
    pub(super) fn move_right(&mut self) {
        let rows = self.form_rows();
        self.nav.move_right(&rows);
    }

    #[cfg(test)]
    pub(super) fn tab_next(&mut self) {
        let rows = self.form_rows();
        self.nav.tab_next(&rows);
    }

    #[cfg(test)]
    pub(super) fn move_first(&mut self) {
        let rows = self.form_rows();
        self.nav.move_first(&rows);
    }

    #[cfg(test)]
    pub(super) fn move_last(&mut self) {
        let rows = self.form_rows();
        self.nav.move_last(&rows);
    }

    pub(super) fn is_input_field(field: SshFormField) -> bool {
        matches!(
            field,
            SshFormField::Name
                | SshFormField::Host
                | SshFormField::Port
                | SshFormField::User
                | SshFormField::KeyPath
                | SshFormField::Passphrase
                | SshFormField::Password
        )
    }
}

impl SshTunnelsSection {
    pub(super) fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_tunnel_id = None;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_secret = true;
        self.show_ssh_passphrase = false;
        self.show_ssh_password = false;
        self.ssh_focus = SshFocus::Form;
        self.ssh_form_field = SshFormField::Name;
        self.ssh_editing_field = false;
        self.ssh_test_status = SshTestStatus::None;
        self.ssh_test_error = None;
        self.ssh_manual_backup = None;
        self.pending_ssh_config_host_index = None;
        self.ssh_host_picker
            .update(cx, |picker, cx| picker.set_selected_alias(None, cx));

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_port
            .update(cx, |s, cx| s.set_value("22", window, cx));
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_path
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_passphrase
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_password
            .update(cx, |s, cx| s.set_value("", window, cx));

        cx.notify();
    }

    pub(super) fn edit_tunnel(
        &mut self,
        tunnel: &SshTunnelProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editing_tunnel_id = Some(tunnel.id);
        self.ssh_form_field = SshFormField::Name;
        self.ssh_editing_field = false;
        self.ssh_test_status = SshTestStatus::None;
        self.ssh_test_error = None;
        self.ssh_manual_backup = None;

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value(&tunnel.name, window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value(&tunnel.config.host, window, cx));
        self.input_ssh_port.update(cx, |s, cx| {
            s.set_value(tunnel.config.port.to_string(), window, cx)
        });
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value(&tunnel.config.user, window, cx));

        self.input_ssh_key_path
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_passphrase
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_password
            .update(cx, |s, cx| s.set_value("", window, cx));

        match &tunnel.config.auth_method {
            SshAuthMethod::PrivateKey { key_path } => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                let path_str = key_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                self.input_ssh_key_path
                    .update(cx, |s, cx| s.set_value(&path_str, window, cx));

                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    let secret = secret.expose_secret().to_string();
                    self.input_ssh_key_passphrase
                        .update(cx, |s, cx| s.set_value(secret, window, cx));
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    let secret = secret.expose_secret().to_string();
                    self.input_ssh_password
                        .update(cx, |s, cx| s.set_value(secret, window, cx));
                }
            }
        }

        self.form_save_secret = tunnel.save_secret;

        let saved_alias = tunnel.config.ssh_config_host.clone();
        if saved_alias.is_some() {
            // While an alias is active the manual fields must not present a
            // value that is not the target: they stay cleared and disabled
            // (A7) and the resolved target shows next to the picker.
            self.input_ssh_host
                .update(cx, |s, cx| s.set_value("", window, cx));
            self.input_ssh_port
                .update(cx, |s, cx| s.set_value("", window, cx));
            self.input_ssh_user
                .update(cx, |s, cx| s.set_value("", window, cx));
        }
        self.ssh_host_picker.update(cx, |picker, cx| {
            picker.set_selected_alias(saved_alias.as_deref(), cx)
        });

        cx.notify();
    }

    /// Maps a dropdown selection to the form: index 0 is the manual entry,
    /// any other index stores its alias.
    pub(super) fn apply_ssh_config_host_index(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let alias = self.ssh_host_picker.read(cx).alias_at(index);
        self.set_ssh_config_host(alias.as_deref(), window, cx);
    }

    /// Applies an SSH config host selection to the form. Picking an alias
    /// keeps what the user had typed and clears the manual fields (A7);
    /// choosing the manual entry restores them exactly.
    pub(super) fn set_ssh_config_host(
        &mut self,
        alias: Option<&str>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let was_active = self.ssh_host_picker.read(cx).selected_alias().is_some();
        let is_active = alias.is_some();

        if is_active && !was_active {
            self.ssh_manual_backup = Some(SshManualValues {
                host: self.input_ssh_host.read(cx).value().to_string(),
                port: self.input_ssh_port.read(cx).value().to_string(),
                user: self.input_ssh_user.read(cx).value().to_string(),
            });
        }

        if is_active {
            self.input_ssh_host
                .update(cx, |s, cx| s.set_value("", window, cx));
            self.input_ssh_port
                .update(cx, |s, cx| s.set_value("", window, cx));
            self.input_ssh_user
                .update(cx, |s, cx| s.set_value("", window, cx));
        } else if was_active {
            let backup = self.ssh_manual_backup.take().unwrap_or(SshManualValues {
                host: String::new(),
                port: "22".to_string(),
                user: String::new(),
            });
            self.input_ssh_host
                .update(cx, |s, cx| s.set_value(backup.host, window, cx));
            self.input_ssh_port
                .update(cx, |s, cx| s.set_value(backup.port, window, cx));
            self.input_ssh_user
                .update(cx, |s, cx| s.set_value(backup.user, window, cx));
        }

        self.ssh_host_picker
            .update(cx, |picker, cx| picker.set_selected_alias(alias, cx));
        cx.notify();
    }

    pub(super) fn save_tunnel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_tunnel_name.read(cx).value().trim().to_string();
        if name.is_empty() {
            return;
        }

        let host = self.input_ssh_host.read(cx).value().trim().to_string();
        let port_str = self.input_ssh_port.read(cx).value().trim().to_string();
        let user = self.input_ssh_user.read(cx).value().trim().to_string();
        let key_path_str = self.input_ssh_key_path.read(cx).value().trim().to_string();
        let passphrase = self.input_ssh_key_passphrase.read(cx).value().to_string();
        let password = self.input_ssh_password.read(cx).value().to_string();
        let ssh_config_host = self
            .ssh_host_picker
            .read(cx)
            .selected_alias()
            .map(str::to_string);

        let config = ssh_shared::build_ssh_config_with_alias(
            &host,
            &port_str,
            &user,
            self.ssh_auth_method,
            &key_path_str,
            ssh_config_host.as_deref(),
        );

        let secret = ssh_shared::get_ssh_secret(self.ssh_auth_method, &passphrase, &password)
            .unwrap_or_default();

        let tunnel = SshTunnelProfile {
            id: self.editing_tunnel_id.unwrap_or_else(Uuid::new_v4),
            name,
            config,
            save_secret: self.form_save_secret,
        };

        let is_edit = self.editing_tunnel_id.is_some();

        self.app_state.update(cx, |state, cx| {
            if tunnel.save_secret && !secret.is_empty() {
                state.save_ssh_tunnel_secret(&tunnel, &SecretString::from(secret.clone()));
            }

            if is_edit {
                state.update_ssh_tunnel(tunnel.clone());
            } else {
                state.add_ssh_tunnel(tunnel.clone());
            }

            cx.emit(AppStateChanged);
        });

        let runtime = self.app_state.read(cx).storage_runtime();
        if let Err(e) = dbflux_app::config_loader::save_ssh_tunnels(
            runtime,
            self.app_state.read(cx).ssh_tunnels(),
        ) {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.ssh_tunnels.error.save_failed", error = e),
                ),
                cx,
            );
        }

        self.ssh_selected_idx = self
            .app_state
            .read(cx)
            .ssh_tunnels()
            .iter()
            .position(|candidate| candidate.id == tunnel.id);

        self.clear_form(window, cx);
    }

    pub(super) fn test_ssh_tunnel(&mut self, cx: &mut Context<Self>) {
        let host = self.input_ssh_host.read(cx).value().trim().to_string();
        let port_str = self.input_ssh_port.read(cx).value().trim().to_string();
        let user = self.input_ssh_user.read(cx).value().trim().to_string();

        let key_path_str = self.input_ssh_key_path.read(cx).value().trim().to_string();
        let passphrase = self.input_ssh_key_passphrase.read(cx).value().to_string();
        let password = self.input_ssh_password.read(cx).value().to_string();
        let ssh_config_host = self
            .ssh_host_picker
            .read(cx)
            .selected_alias()
            .map(str::to_string);

        // With an alias the alias is the only source of the target (A7);
        // the manual host and user stay empty and are not required.
        if ssh_config_host.is_none() && (host.is_empty() || user.is_empty()) {
            self.ssh_test_status = SshTestStatus::Failed;
            self.ssh_test_error = Some(dbflux_i18n::t!(
                "settings.ssh_tunnels.error.host_and_user_required"
            ));
            cx.notify();
            return;
        }

        let config = ssh_shared::build_ssh_config_with_alias(
            &host,
            &port_str,
            &user,
            self.ssh_auth_method,
            &key_path_str,
            ssh_config_host.as_deref(),
        );

        let secret = ssh_shared::get_ssh_secret(self.ssh_auth_method, &passphrase, &password);

        self.ssh_test_status = SshTestStatus::Testing;
        self.ssh_test_error = None;
        cx.notify();

        let this = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            match dbflux_ssh::establish_session(&config, secret.as_deref()) {
                Ok(_session) => Ok(()),
                Err(error) => Err(format!("{:?}", error)),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(()) => {
                            this.ssh_test_status = SshTestStatus::Success;
                            this.ssh_test_error = None;
                        }
                        Err(error) => {
                            this.ssh_test_status = SshTestStatus::Failed;
                            this.ssh_test_error = Some(error);
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }

    pub(super) fn request_delete_tunnel(&mut self, tunnel_id: Uuid, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = Some(tunnel_id);
        cx.notify();
    }

    pub(super) fn confirm_delete_tunnel(&mut self, cx: &mut Context<Self>) {
        let Some(tunnel_id) = self.pending_delete_tunnel_id.take() else {
            return;
        };

        let deleted_idx = self.app_state.update(cx, |state, cx| {
            let idx = state.ssh_tunnels().iter().position(|t| t.id == tunnel_id);
            if let Some(index) = idx {
                state.remove_ssh_tunnel(index);
            }
            cx.emit(AppStateChanged);
            idx
        });

        if self.editing_tunnel_id == Some(tunnel_id) {
            self.editing_tunnel_id = None;
        }

        if let Some(deleted_idx) = deleted_idx {
            let new_count = self.ssh_tunnel_count(cx);
            if new_count == 0 {
                self.ssh_selected_idx = None;
            } else if let Some(selected_idx) = self.ssh_selected_idx {
                if selected_idx >= new_count {
                    self.ssh_selected_idx = Some(new_count - 1);
                } else if selected_idx > deleted_idx {
                    self.ssh_selected_idx = Some(selected_idx - 1);
                }
            }
        }

        cx.notify();
    }

    pub(super) fn cancel_delete_tunnel(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = None;
        cx.notify();
    }

    pub(super) fn browse_ssh_key(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        crate::connection_manager::browse_ssh_key_into(cx, |this, path| {
            this.pending_ssh_key_path = Some(path);
        });
    }

    pub(super) fn ssh_tunnel_count(&self, cx: &App) -> usize {
        self.app_state.read(cx).ssh_tunnels().len()
    }

    pub(super) fn has_unsaved_ssh_changes(&self, cx: &App) -> bool {
        if let Some(id) = self.editing_tunnel_id {
            let tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();
            let Some(saved) = tunnels.iter().find(|tunnel| tunnel.id == id) else {
                return true;
            };

            let name = self.input_tunnel_name.read(cx).value().trim().to_string();
            let host = self.input_ssh_host.read(cx).value().trim().to_string();
            let port_str = self.input_ssh_port.read(cx).value().trim().to_string();
            let user = self.input_ssh_user.read(cx).value().trim().to_string();

            if name != saved.name || self.form_save_secret != saved.save_secret {
                return true;
            }

            // With an alias the manual host/port/user inputs are cleared
            // and disabled, so only the alias itself is compared (A7).
            let form_alias = self
                .ssh_host_picker
                .read(cx)
                .selected_alias()
                .map(str::to_string);
            if form_alias != saved.config.ssh_config_host {
                return true;
            }
            if form_alias.is_none()
                && (host != saved.config.host
                    || port_str != saved.config.port.to_string()
                    || user != saved.config.user)
            {
                return true;
            }

            match (&self.ssh_auth_method, &saved.config.auth_method) {
                (SshAuthSelection::PrivateKey, SshAuthMethod::PrivateKey { key_path }) => {
                    let form_key_path = self.input_ssh_key_path.read(cx).value().trim().to_string();
                    let saved_key_path = key_path
                        .as_ref()
                        .map(|path| path.to_string_lossy().to_string())
                        .unwrap_or_default();

                    if form_key_path != saved_key_path {
                        return true;
                    }

                    let saved_secret = self
                        .app_state
                        .read(cx)
                        .get_ssh_tunnel_secret(saved)
                        .map(|secret| secret.expose_secret().to_string())
                        .unwrap_or_default();

                    self.input_ssh_key_passphrase.read(cx).value() != saved_secret
                }
                (SshAuthSelection::Password, SshAuthMethod::Password) => {
                    let saved_secret = self
                        .app_state
                        .read(cx)
                        .get_ssh_tunnel_secret(saved)
                        .map(|secret| secret.expose_secret().to_string())
                        .unwrap_or_default();

                    self.input_ssh_password.read(cx).value() != saved_secret
                }
                _ => true,
            }
        } else {
            let port_is_default = self.input_ssh_port.read(cx).value().trim() == "22";

            !self.input_tunnel_name.read(cx).value().trim().is_empty()
                || !self.input_ssh_host.read(cx).value().trim().is_empty()
                || !port_is_default
                || !self.input_ssh_user.read(cx).value().trim().is_empty()
                || !self.input_ssh_key_path.read(cx).value().trim().is_empty()
                || !self.input_ssh_key_passphrase.read(cx).value().is_empty()
                || !self.input_ssh_password.read(cx).value().is_empty()
                || self.ssh_auth_method != SshAuthSelection::PrivateKey
                || !self.form_save_secret
                || self.ssh_host_picker.read(cx).selected_alias().is_some()
        }
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.pending_delete_tunnel_id.is_some() {
            return;
        }

        if !self.content_focused && !self.ssh_editing_field {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match self.ssh_focus {
            SshFocus::ProfileList => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.ssh_move_next_profile(cx);
                    self.ssh_load_selected_profile(window, cx);
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.ssh_move_prev_profile(cx);
                    self.ssh_load_selected_profile(window, cx);
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                    if modifiers == Modifiers::none() =>
                {
                    self.ssh_enter_form(window, cx);
                    cx.notify();
                }
                ("d", modifiers) if modifiers == Modifiers::none() => {
                    if let Some(idx) = self.ssh_selected_idx {
                        let tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();
                        if let Some(tunnel) = tunnels.get(idx) {
                            self.request_delete_tunnel(tunnel.id, cx);
                        }
                    }
                }
                ("i", modifiers) if modifiers == Modifiers::none() => {
                    self.request_import(cx);
                }
                ("n", modifiers) if modifiers == Modifiers::none() => {
                    self.ssh_selected_idx = None;
                    self.ssh_load_selected_profile(window, cx);
                    self.ssh_enter_form(window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.ssh_selected_idx = None;
                    self.ssh_load_selected_profile(window, cx);
                    cx.notify();
                }
                ("G", modifiers) if modifiers == Modifiers::none() => {
                    let count = self.ssh_tunnel_count(cx);
                    if count > 0 {
                        self.ssh_selected_idx = Some(count - 1);
                        self.ssh_load_selected_profile(window, cx);
                    }
                    cx.notify();
                }
                _ => {}
            },
            SshFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.move_down();
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.move_up();
                    cx.notify();
                }
                ("h", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("left", modifiers) if modifiers == Modifiers::none() => {
                    self.move_left();
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) if modifiers == Modifiers::none() => {
                    self.move_right();
                    cx.notify();
                }
                ("enter", modifiers) if modifiers == Modifiers::none() => {
                    self.activate_current_field(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.tab_next();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.tab_prev();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.move_first();
                    cx.notify();
                }
                ("G", modifiers) if modifiers == Modifiers::none() => {
                    self.move_last();
                    cx.notify();
                }
                _ => {}
            },
        }
    }

    pub(super) fn sync_from_app_state(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();

        if tunnels.is_empty() {
            self.ssh_selected_idx = None;
            if self.editing_tunnel_id.is_some() && !self.has_unsaved_ssh_changes(cx) {
                self.clear_form(window, cx);
            }
            return;
        }

        if let Some(selected_idx) = self.ssh_selected_idx
            && selected_idx >= tunnels.len()
        {
            self.ssh_selected_idx = Some(tunnels.len() - 1);
        }

        if let Some(tunnel_id) = self.editing_tunnel_id
            && !tunnels.iter().any(|tunnel| tunnel.id == tunnel_id)
            && !self.has_unsaved_ssh_changes(cx)
        {
            self.clear_form(window, cx);
        }
    }

    pub(super) fn edit_tunnel_at_selected_index(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();

        if let Some(idx) = self.ssh_selected_idx
            && let Some(tunnel) = tunnels.get(idx)
        {
            self.edit_tunnel(tunnel, window, cx);
        }
    }

    fn ssh_move_next_profile(&mut self, cx: &App) {
        let count = self.ssh_tunnel_count(cx);
        if count == 0 {
            self.ssh_selected_idx = None;
            return;
        }

        match self.ssh_selected_idx {
            None => self.ssh_selected_idx = Some(0),
            Some(idx) if idx + 1 < count => self.ssh_selected_idx = Some(idx + 1),
            _ => {}
        }
    }

    fn ssh_move_prev_profile(&mut self, cx: &App) {
        let count = self.ssh_tunnel_count(cx);
        if count == 0 {
            self.ssh_selected_idx = None;
            return;
        }

        match self.ssh_selected_idx {
            Some(idx) if idx > 0 => self.ssh_selected_idx = Some(idx - 1),
            Some(0) => self.ssh_selected_idx = None,
            _ => {}
        }
    }

    fn ssh_load_selected_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();

        if let Some(idx) = self.ssh_selected_idx
            && idx >= tunnels.len()
        {
            self.ssh_selected_idx = if tunnels.is_empty() {
                None
            } else {
                Some(tunnels.len() - 1)
            };
        }

        if let Some(idx) = self.ssh_selected_idx
            && let Some(tunnel) = tunnels.get(idx)
        {
            self.edit_tunnel(tunnel, window, cx);
            return;
        }

        self.clear_form(window, cx);
        self.ssh_focus = SshFocus::ProfileList;
    }

    fn ssh_enter_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.ssh_focus = SshFocus::Form;
        self.ssh_form_field = SshFormField::Name;
        self.ssh_editing_field = false;

        if self.ssh_selected_idx.is_some() {
            self.ssh_load_selected_profile(window, cx);
        } else {
            self.clear_form(window, cx);
        }
    }

    pub(super) fn validate_ssh_form_field(&mut self) {
        let mut nav = SshFormNav::new(
            self.ssh_auth_method,
            self.editing_tunnel_id,
            self.ssh_form_field,
        );
        nav.validate_field();
        self.ssh_form_field = nav.field();
    }
}

#[cfg(test)]
mod tests {
    use super::SshFormNav;
    use crate::settings::ssh_tunnels_section::SshFormField;
    use crate::settings::ssh_tunnels_section::SshTunnelsSection;
    use crate::ssh_host_picker::load_ssh_hosts;
    use crate::ssh_shared::SshAuthSelection;
    use dbflux_core::{SshAuthMethod, SshTunnelConfig, SshTunnelProfile};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateChanged;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::keymap::init_keymap;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;
    use uuid::Uuid;

    /// A throwaway SSH config directory. Never the real home (S7).
    struct SshConfigFixture(PathBuf);

    impl SshConfigFixture {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "dbflux-ssh-tunnels-{name}-{}-{}",
                std::process::id(),
                Uuid::new_v4()
            ));
            std::fs::create_dir_all(&dir).expect("create fixture dir");
            Self(dir)
        }

        fn write_config(&self, text: &str) {
            std::fs::write(self.0.join("config"), text).expect("write fixture config");
        }

        /// The config directory and the home the resolver expands against:
        /// both inside the fixture, so `~` never reaches the real home (S7).
        fn paths(&self) -> (PathBuf, PathBuf) {
            (self.0.clone(), self.0.join("home"))
        }
    }

    impl Drop for SshConfigFixture {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0) {
                eprintln!("fixture cleanup failed: {error}");
            }
        }
    }

    /// A settings SSH section with its app state, in a real window. The
    /// body receives the app state, the section and the window.
    fn with_ssh_section_harness<T>(
        cx: &mut TestAppContext,
        body: impl FnOnce(
            &Entity<AppStateEntity>,
            &Entity<SshTunnelsSection>,
            &mut VisualTestContext,
        ) -> T,
    ) -> T {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(init_keymap);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = StorageRuntime::in_memory().expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let slot: Rc<RefCell<Option<Entity<SshTunnelsSection>>>> = Rc::default();
        let (_, window) = cx.add_window_view({
            let slot = slot.clone();
            let app_state = app_state.clone();
            move |window, cx| {
                let section = cx.new(|cx| SshTunnelsSection::new(app_state, window, cx));
                slot.replace(Some(section.clone()));
                gpui_component::Root::new(section, window, cx)
            }
        });
        window.run_until_parked();

        let section = slot.borrow().clone().expect("the SSH section is built");
        body(&app_state, &section, window)
    }

    /// Loads `fixture`'s hosts into the section's picker and selects the
    /// first host the way the user does: open, highlight, accept — the
    /// dropdown's own event path.
    fn select_first_host_from_fixture(
        section: &Entity<SshTunnelsSection>,
        fixture: &SshConfigFixture,
        window: &mut VisualTestContext,
    ) {
        window.update(|_, cx| {
            section.update(cx, |section, cx| {
                let (config_dir, home) = fixture.paths();
                let snapshot = load_ssh_hosts(&config_dir, &home);
                assert!(snapshot.load_error.is_none());
                section
                    .ssh_host_picker
                    .update(cx, |picker, cx| picker.apply_snapshot(snapshot, cx));
            });
            let dropdown = section.read(cx).ssh_host_picker.read(cx).dropdown().clone();
            dropdown.update(cx, |dropdown, cx| {
                dropdown.open(cx);
                dropdown.select_next_item(cx);
                dropdown.accept_selection(cx);
            });
        });
        window.run_until_parked();

        // The render pass the user sees between actions, which drains the
        // dropdown selection into the form.
        window.update(|window, _| window.refresh());
        window.run_until_parked();
    }

    /// A state sync arrives after the user has picked an alias and must not
    /// discard it: the config the form would save still references the alias
    /// and stores no resolved manual values (the regression behind the empty
    /// `ssh_config_host` row observed in the running app).
    #[gpui::test]
    fn selecting_an_alias_survives_a_state_sync(cx: &mut gpui::TestAppContext) {
        let fixture = SshConfigFixture::new("sync-survives");
        fixture.write_config(
            "Host alpha\n  HostName alpha.example.com\n  User deploy\n  Port 2222\n\nHost *\n  ServerAliveInterval 30\n",
        );
        let result = with_ssh_section_harness(cx, |app_state, section, window| {
            select_first_host_from_fixture(section, &fixture, window);

            window.update(|_, cx| {
                app_state.update(cx, |_, cx| cx.emit(AppStateChanged));
            });
            window.run_until_parked();
            window.update(|window, _| window.refresh());
            window.run_until_parked();

            window.update(|_, cx| {
                section.update(cx, |section, cx| {
                    let host = section.input_ssh_host.read(cx).value().trim().to_string();
                    let port_str = section.input_ssh_port.read(cx).value().trim().to_string();
                    let user = section.input_ssh_user.read(cx).value().trim().to_string();
                    let alias = section
                        .ssh_host_picker
                        .read(cx)
                        .selected_alias()
                        .map(str::to_string);
                    crate::ssh_shared::build_ssh_config_with_alias(
                        &host,
                        &port_str,
                        &user,
                        section.ssh_auth_method,
                        "",
                        alias.as_deref(),
                    )
                })
            })
        });

        let config = result;
        assert_eq!(config.ssh_config_host.as_deref(), Some("alpha"));
        assert_eq!(config.host, "");
        assert_eq!(config.user, "");
        assert_eq!(config.port, 22);
    }

    /// The same ordering with manual values typed first: the sync must not
    /// put the manual host and user back while the unsaved alias is active.
    #[gpui::test]
    fn a_state_sync_does_not_restore_manual_fields_over_an_unsaved_alias(
        cx: &mut gpui::TestAppContext,
    ) {
        let fixture = SshConfigFixture::new("sync-keeps-manual-cleared");
        fixture.write_config(
            "Host alpha\n  HostName alpha.example.com\n  User deploy\n  Port 2222\n\nHost *\n  ServerAliveInterval 30\n",
        );
        let (alias, host, port, user) =
            with_ssh_section_harness(cx, |app_state, section, window| {
                window.update(|window, cx| {
                    section.update(cx, |section, cx| {
                        section.input_ssh_host.update(cx, |state, cx| {
                            state.set_value("manual.example.com", window, cx)
                        });
                        section
                            .input_ssh_port
                            .update(cx, |state, cx| state.set_value("2200", window, cx));
                        section
                            .input_ssh_user
                            .update(cx, |state, cx| state.set_value("manualuser", window, cx));
                    });
                });

                select_first_host_from_fixture(section, &fixture, window);

                window.update(|_, cx| {
                    app_state.update(cx, |_, cx| cx.emit(AppStateChanged));
                });
                window.run_until_parked();
                window.update(|window, _| window.refresh());
                window.run_until_parked();

                window.update(|_, cx| {
                    section.update(cx, |section, cx| {
                        (
                            section
                                .ssh_host_picker
                                .read(cx)
                                .selected_alias()
                                .map(str::to_string),
                            section.input_ssh_host.read(cx).value().to_string(),
                            section.input_ssh_port.read(cx).value().to_string(),
                            section.input_ssh_user.read(cx).value().to_string(),
                        )
                    })
                })
            });

        assert_eq!(alias.as_deref(), Some("alpha"));
        assert_eq!(host, "", "the manual host stays cleared under the alias");
        assert_eq!(port, "", "the manual port stays cleared under the alias");
        assert_eq!(user, "", "the manual user stays cleared under the alias");
    }

    /// Loading a saved tunnel that references an SSH config alias shows the
    /// alias in the picker and is not dirty: the alias and the cleared
    /// manual fields are exactly what the profile stores.
    #[gpui::test]
    fn loading_an_aliased_profile_reports_no_changes(cx: &mut gpui::TestAppContext) {
        with_ssh_section_harness(cx, |app_state, section, window| {
            let tunnel = SshTunnelProfile {
                id: Uuid::new_v4(),
                name: "aliased".to_string(),
                config: SshTunnelConfig {
                    host: String::new(),
                    port: 22,
                    user: String::new(),
                    auth_method: SshAuthMethod::PrivateKey { key_path: None },
                    ssh_config_host: Some("alpha".to_string()),
                },
                save_secret: true,
            };

            let (alias_loaded, not_dirty) = window.update(|window, cx| {
                app_state.update(cx, |state, _| state.add_ssh_tunnel(tunnel.clone()));
                section.update(cx, |section, cx| {
                    section.edit_tunnel(&tunnel, window, cx);
                    (
                        section
                            .ssh_host_picker
                            .read(cx)
                            .selected_alias()
                            .map(str::to_string),
                        !section.has_unsaved_ssh_changes(cx),
                    )
                })
            });

            assert_eq!(
                alias_loaded.as_deref(),
                Some("alpha"),
                "the form shows the loaded alias"
            );
            assert!(not_dirty, "a freshly loaded aliased profile is not dirty");
        });
    }

    fn nav_private_key_new() -> SshFormNav {
        SshFormNav::new(SshAuthSelection::PrivateKey, None, SshFormField::Name)
    }

    fn nav_password_new() -> SshFormNav {
        SshFormNav::new(SshAuthSelection::Password, None, SshFormField::Name)
    }

    fn nav_private_key_editing() -> SshFormNav {
        SshFormNav::new(
            SshAuthSelection::PrivateKey,
            Some(Uuid::new_v4()),
            SshFormField::Name,
        )
    }

    #[test]
    fn form_rows_private_key_includes_key_fields() {
        let nav = nav_private_key_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(all_fields.contains(&&SshFormField::KeyPath));
        assert!(all_fields.contains(&&SshFormField::KeyBrowse));
        assert!(all_fields.contains(&&SshFormField::Passphrase));
        assert!(!all_fields.contains(&&SshFormField::Password));
    }

    #[test]
    fn form_rows_password_includes_password_field() {
        let nav = nav_password_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(all_fields.contains(&&SshFormField::Password));
        assert!(!all_fields.contains(&&SshFormField::KeyPath));
        assert!(!all_fields.contains(&&SshFormField::Passphrase));
    }

    #[test]
    fn form_rows_new_tunnel_no_delete_button() {
        let nav = nav_private_key_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(!all_fields.contains(&&SshFormField::DeleteButton));
        assert!(all_fields.contains(&&SshFormField::TestButton));
        assert!(all_fields.contains(&&SshFormField::SaveButton));
    }

    #[test]
    fn form_rows_editing_has_delete_button() {
        let nav = nav_private_key_editing();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(all_fields.contains(&&SshFormField::DeleteButton));
        assert!(all_fields.contains(&&SshFormField::TestButton));
        assert!(all_fields.contains(&&SshFormField::SaveButton));
    }

    #[test]
    fn form_rows_editing_has_export_button() {
        let nav = nav_private_key_editing();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(all_fields.contains(&&SshFormField::ExportButton));
    }

    #[test]
    fn form_rows_new_tunnel_no_export_button() {
        let nav = nav_private_key_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(!all_fields.contains(&&SshFormField::ExportButton));
    }

    #[test]
    fn move_down_from_name_to_config_host_and_to_host() {
        let mut nav = nav_private_key_new();
        nav.set_field(SshFormField::Name);
        nav.move_down();
        assert_eq!(nav.field(), SshFormField::SshConfigHost);
        nav.move_down();
        assert_eq!(nav.field(), SshFormField::Host);
    }

    #[test]
    fn move_right_in_host_row() {
        let mut nav = nav_private_key_new();
        nav.set_field(SshFormField::Host);
        nav.move_right();
        assert_eq!(nav.field(), SshFormField::Port);
        nav.move_right();
        assert_eq!(nav.field(), SshFormField::Port);
    }

    #[test]
    fn tab_next_crosses_row_boundary() {
        let mut nav = nav_private_key_new();
        nav.set_field(SshFormField::User);
        nav.tab_next();
        assert_eq!(nav.field(), SshFormField::AuthPrivateKey);
    }

    #[test]
    fn validate_resets_orphaned_private_key() {
        let mut nav = SshFormNav::new(SshAuthSelection::PrivateKey, None, SshFormField::Password);
        nav.validate_field();
        assert_eq!(nav.field(), SshFormField::KeyPath);
    }

    #[test]
    fn validate_resets_orphaned_password() {
        let mut nav = SshFormNav::new(SshAuthSelection::Password, None, SshFormField::KeyPath);
        nav.validate_field();
        assert_eq!(nav.field(), SshFormField::Password);
    }

    #[test]
    fn validate_keeps_valid_field() {
        let mut nav = nav_private_key_new();
        nav.set_field(SshFormField::Host);
        nav.validate_field();
        assert_eq!(nav.field(), SshFormField::Host);
    }

    #[test]
    fn move_first_and_last() {
        let mut nav = nav_private_key_new();
        nav.set_field(SshFormField::User);
        nav.move_first();
        assert_eq!(nav.field(), SshFormField::Name);
        nav.move_last();
        assert_eq!(nav.field(), SshFormField::SaveButton);
    }

    #[test]
    fn is_ssh_input_field_correctness() {
        assert!(SshFormNav::is_input_field(SshFormField::Name));
        assert!(SshFormNav::is_input_field(SshFormField::Host));
        assert!(SshFormNav::is_input_field(SshFormField::Port));
        assert!(SshFormNav::is_input_field(SshFormField::User));
        assert!(SshFormNav::is_input_field(SshFormField::KeyPath));
        assert!(SshFormNav::is_input_field(SshFormField::Passphrase));
        assert!(SshFormNav::is_input_field(SshFormField::Password));

        assert!(!SshFormNav::is_input_field(SshFormField::SshConfigHost));
        assert!(!SshFormNav::is_input_field(SshFormField::AuthPrivateKey));
        assert!(!SshFormNav::is_input_field(SshFormField::AuthPassword));
        assert!(!SshFormNav::is_input_field(SshFormField::KeyBrowse));
        assert!(!SshFormNav::is_input_field(SshFormField::SaveSecret));
        assert!(!SshFormNav::is_input_field(SshFormField::TestButton));
        assert!(!SshFormNav::is_input_field(SshFormField::SaveButton));
        assert!(!SshFormNav::is_input_field(SshFormField::DeleteButton));
    }
}
