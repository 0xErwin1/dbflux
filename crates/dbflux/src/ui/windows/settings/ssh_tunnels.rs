use crate::app::AppStateChanged;
use crate::ui::windows::ssh_shared::{self, SshAuthSelection};
use dbflux_core::{SshAuthMethod, SshTunnelProfile};
use gpui::*;
use uuid::Uuid;

use super::{SettingsWindow, SshFocus, SshFormField, SshTestStatus};

impl SettingsWindow {
    pub(super) fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_tunnel_id = None;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_secret = false;
        self.ssh_test_status = SshTestStatus::None;
        self.ssh_test_error = None;

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
        self.ssh_test_status = SshTestStatus::None;
        self.ssh_test_error = None;

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value(&tunnel.name, window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value(&tunnel.config.host, window, cx));
        self.input_ssh_port.update(cx, |s, cx| {
            s.set_value(tunnel.config.port.to_string(), window, cx)
        });
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value(&tunnel.config.user, window, cx));

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
                    self.input_ssh_key_passphrase
                        .update(cx, |s, cx| s.set_value(&secret, window, cx));
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    self.input_ssh_password
                        .update(cx, |s, cx| s.set_value(&secret, window, cx));
                }
            }
        }

        self.form_save_secret = tunnel.save_secret;
        cx.notify();
    }

    pub(super) fn save_tunnel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_tunnel_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }

        let host = self.input_ssh_host.read(cx).value().to_string();
        let port_str = self.input_ssh_port.read(cx).value().to_string();
        let user = self.input_ssh_user.read(cx).value().to_string();
        let key_path_str = self.input_ssh_key_path.read(cx).value().to_string();
        let passphrase = self.input_ssh_key_passphrase.read(cx).value().to_string();
        let password = self.input_ssh_password.read(cx).value().to_string();

        let config = ssh_shared::build_ssh_config(
            &host,
            &port_str,
            &user,
            self.ssh_auth_method,
            &key_path_str,
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
                state.save_ssh_tunnel_secret(&tunnel, &secret);
            }

            if is_edit {
                state.update_ssh_tunnel(tunnel);
            } else {
                state.add_ssh_tunnel(tunnel);
            }

            cx.emit(AppStateChanged);
        });

        self.clear_form(window, cx);
    }

    pub(super) fn test_ssh_tunnel(&mut self, cx: &mut Context<Self>) {
        let host = self.input_ssh_host.read(cx).value().to_string();
        let port_str = self.input_ssh_port.read(cx).value().to_string();
        let user = self.input_ssh_user.read(cx).value().to_string();

        if host.trim().is_empty() || user.trim().is_empty() {
            self.ssh_test_status = SshTestStatus::Failed;
            self.ssh_test_error = Some("Host and user are required".to_string());
            cx.notify();
            return;
        }

        let key_path_str = self.input_ssh_key_path.read(cx).value().to_string();
        let passphrase = self.input_ssh_key_passphrase.read(cx).value().to_string();
        let password = self.input_ssh_password.read(cx).value().to_string();

        let config = ssh_shared::build_ssh_config(
            &host,
            &port_str,
            &user,
            self.ssh_auth_method,
            &key_path_str,
        );

        let secret = ssh_shared::get_ssh_secret(self.ssh_auth_method, &passphrase, &password);

        self.ssh_test_status = SshTestStatus::Testing;
        self.ssh_test_error = None;
        cx.notify();

        let this = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            match dbflux_ssh::establish_session(&config, secret.as_deref()) {
                Ok(_session) => Ok(()),
                Err(e) => Err(format!("{:?}", e)),
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
                        Err(e) => {
                            this.ssh_test_status = SshTestStatus::Failed;
                            this.ssh_test_error = Some(e);
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
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
            if let Some(i) = idx {
                state.remove_ssh_tunnel(i);
            }
            cx.emit(AppStateChanged);
            idx
        });

        if self.editing_tunnel_id == Some(tunnel_id) {
            self.editing_tunnel_id = None;
        }

        if let Some(deleted) = deleted_idx {
            let new_count = self.ssh_tunnel_count(cx);
            if new_count == 0 {
                self.ssh_selected_idx = None;
            } else if let Some(sel) = self.ssh_selected_idx {
                if sel >= new_count {
                    self.ssh_selected_idx = Some(new_count - 1);
                } else if sel > deleted {
                    self.ssh_selected_idx = Some(sel - 1);
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
        let this = cx.entity().clone();

        let start_dir = dirs::home_dir().map(|h| h.join(".ssh")).unwrap_or_default();

        let task = cx.background_executor().spawn(async move {
            let dialog = rfd::FileDialog::new()
                .set_title("Select SSH Private Key")
                .set_directory(&start_dir);

            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let path = task.await;

            if let Some(path) = path {
                cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_ssh_key_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
                .ok();
            }
        })
        .detach();
    }

    pub(super) fn ssh_tunnel_count(&self, cx: &Context<Self>) -> usize {
        self.app_state.read(cx).ssh_tunnels().len()
    }

    pub(super) fn ssh_move_next_profile(&mut self, cx: &Context<Self>) {
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

    pub(super) fn ssh_move_prev_profile(&mut self, cx: &Context<Self>) {
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

    pub(super) fn ssh_load_selected_profile(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let tunnels = {
            let state = self.app_state.read(cx);
            state.ssh_tunnels().to_vec()
        };

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
    }

    pub(super) fn ssh_enter_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.ssh_focus = SshFocus::Form;
        self.ssh_form_field = SshFormField::Name;
        self.ssh_editing_field = false;

        self.ssh_load_selected_profile(window, cx);
    }

    pub(super) fn ssh_exit_form(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.ssh_focus = SshFocus::ProfileList;
        self.ssh_editing_field = false;
        self.focus_handle.focus(window);
    }

    /// Returns form rows. Each row contains fields navigable with h/l.
    fn ssh_form_rows(&self) -> Vec<Vec<SshFormField>> {
        let mut rows = vec![
            vec![SshFormField::Name],
            vec![SshFormField::Host, SshFormField::Port],
            vec![SshFormField::User],
            vec![SshFormField::AuthPrivateKey, SshFormField::AuthPassword],
        ];

        match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                rows.push(vec![SshFormField::KeyPath, SshFormField::KeyBrowse]);
                rows.push(vec![SshFormField::Passphrase, SshFormField::SaveSecret]);
            }
            SshAuthSelection::Password => {
                rows.push(vec![SshFormField::Password, SshFormField::SaveSecret]);
            }
        }

        if self.editing_tunnel_id.is_some() {
            rows.push(vec![
                SshFormField::DeleteButton,
                SshFormField::TestButton,
                SshFormField::SaveButton,
            ]);
        } else {
            rows.push(vec![SshFormField::TestButton, SshFormField::SaveButton]);
        }

        rows
    }

    /// Find current row and column index for the current field.
    fn ssh_field_position(&self) -> Option<(usize, usize)> {
        let rows = self.ssh_form_rows();
        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(col_idx) = row.iter().position(|&f| f == self.ssh_form_field) {
                return Some((row_idx, col_idx));
            }
        }
        None
    }

    pub(super) fn ssh_move_down(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some((row_idx, col_idx)) = self.ssh_field_position()
            && row_idx + 1 < rows.len()
        {
            let next_row = &rows[row_idx + 1];
            if next_row.is_empty() {
                return;
            }
            let new_col = col_idx.min(next_row.len() - 1);
            self.ssh_form_field = next_row[new_col];
        }
    }

    pub(super) fn ssh_move_up(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some((row_idx, col_idx)) = self.ssh_field_position()
            && row_idx > 0
        {
            let prev_row = &rows[row_idx - 1];
            if prev_row.is_empty() {
                return;
            }
            let new_col = col_idx.min(prev_row.len() - 1);
            self.ssh_form_field = prev_row[new_col];
        }
    }

    pub(super) fn ssh_move_right(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some((row_idx, col_idx)) = self.ssh_field_position() {
            let row = &rows[row_idx];
            if col_idx + 1 < row.len() {
                self.ssh_form_field = row[col_idx + 1];
            }
        }
    }

    pub(super) fn ssh_move_left(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some((row_idx, col_idx)) = self.ssh_field_position()
            && col_idx > 0
        {
            self.ssh_form_field = rows[row_idx][col_idx - 1];
        }
    }

    pub(super) fn ssh_move_first(&mut self) {
        self.ssh_form_field = SshFormField::Name;
    }

    pub(super) fn ssh_move_last(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some(last_row) = rows.last()
            && let Some(last_field) = last_row.last()
        {
            self.ssh_form_field = *last_field;
        }
    }

    pub(super) fn ssh_tab_next(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some((row_idx, col_idx)) = self.ssh_field_position() {
            let row = &rows[row_idx];
            if col_idx + 1 < row.len() {
                self.ssh_form_field = row[col_idx + 1];
            } else if row_idx + 1 < rows.len() && !rows[row_idx + 1].is_empty() {
                self.ssh_form_field = rows[row_idx + 1][0];
            }
        }
    }

    pub(super) fn ssh_tab_prev(&mut self) {
        let rows = self.ssh_form_rows();
        if let Some((row_idx, col_idx)) = self.ssh_field_position() {
            if col_idx > 0 {
                self.ssh_form_field = rows[row_idx][col_idx - 1];
            } else if row_idx > 0 {
                let prev_row = &rows[row_idx - 1];
                if let Some(last_field) = prev_row.last() {
                    self.ssh_form_field = *last_field;
                }
            }
        }
    }

    pub(super) fn ssh_focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.ssh_editing_field = true;

        match self.ssh_form_field {
            SshFormField::Name => {
                self.input_tunnel_name
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Host => {
                self.input_ssh_host.update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Port => {
                self.input_ssh_port.update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::User => {
                self.input_ssh_user.update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::KeyPath => {
                self.input_ssh_key_path
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Passphrase => {
                self.input_ssh_key_passphrase
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Password => {
                self.input_ssh_password
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            _ => {
                self.ssh_editing_field = false;
            }
        }
    }

    pub(super) fn ssh_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.ssh_form_field {
            SshFormField::AuthPrivateKey => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                cx.notify();
            }
            SshFormField::AuthPassword => {
                self.ssh_auth_method = SshAuthSelection::Password;
                cx.notify();
            }
            SshFormField::KeyBrowse => {
                self.browse_ssh_key(window, cx);
            }
            SshFormField::SaveSecret => {
                self.form_save_secret = !self.form_save_secret;
                cx.notify();
            }
            SshFormField::SaveButton => {
                self.save_tunnel(window, cx);
            }
            SshFormField::TestButton => {
                self.test_ssh_tunnel(cx);
            }
            SshFormField::DeleteButton => {
                if let Some(id) = self.editing_tunnel_id {
                    self.request_delete_tunnel(id, cx);
                }
            }
            field if self.is_input_field(field) => {
                self.ssh_focus_current_field(window, cx);
            }
            _ => {}
        }
    }

    fn is_input_field(&self, field: SshFormField) -> bool {
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

    pub(super) fn validate_ssh_form_field(&mut self) {
        let rows = self.ssh_form_rows();
        let is_valid = rows.iter().any(|row| row.contains(&self.ssh_form_field));
        if !is_valid {
            self.ssh_form_field = match self.ssh_auth_method {
                SshAuthSelection::PrivateKey => SshFormField::KeyPath,
                SshAuthSelection::Password => SshFormField::Password,
            };
        }
    }
}
