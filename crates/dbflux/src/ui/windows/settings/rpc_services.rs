use crate::ui::toast::ToastExt;
use dbflux_core::{AppConfig, AppConfigStore, ServiceConfig};
use gpui::*;
use gpui_component::input::InputState;
use std::collections::HashMap;

use super::{ServiceFocus, ServiceFormRow, SettingsWindow};

impl SettingsWindow {
    pub(super) fn load_services(&mut self) {
        let store = match AppConfigStore::new() {
            Ok(s) => s,
            Err(e) => {
                log::error!("Failed to create config store: {}", e);
                self.svc_services = Vec::new();
                self.svc_config_store = None;
                return;
            }
        };

        self.svc_services = match store.load() {
            Ok(config) => config.services,
            Err(e) => {
                log::error!("Failed to load config: {}", e);
                Vec::new()
            }
        };

        self.svc_config_store = Some(store);
    }

    fn persist_services(&self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(ref store) = self.svc_config_store else {
            cx.toast_error("Cannot save: config store unavailable", window);
            return;
        };

        let mut config = match store.load() {
            Ok(c) => c,
            Err(e) => {
                log::error!("Failed to load config before save: {}", e);
                AppConfig::default()
            }
        };

        config.services = self.svc_services.clone();

        if let Err(e) = store.save(&config) {
            log::error!("Failed to save config: {}", e);
            cx.toast_error(format!("Failed to save config: {}", e), window);
        }
    }

    // --- Form lifecycle ---

    pub(super) fn clear_svc_form(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.editing_svc_idx = None;
        self.svc_enabled = true;
        self.svc_form_cursor = 0;
        self.svc_env_col = 0;
        self.svc_editing_field = false;
        self.svc_arg_inputs.clear();
        self.svc_env_key_inputs.clear();
        self.svc_env_value_inputs.clear();

        self.input_socket_id
            .update(_cx, |s, cx| s.set_value("", _window, cx));
        self.input_svc_command
            .update(_cx, |s, cx| s.set_value("", _window, cx));
        self.input_svc_timeout
            .update(_cx, |s, cx| s.set_value("", _window, cx));

        _cx.notify();
    }

    pub(super) fn edit_service(&mut self, idx: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(service) = self.svc_services.get(idx).cloned() else {
            return;
        };

        self.editing_svc_idx = Some(idx);
        self.svc_enabled = service.enabled;
        self.svc_form_cursor = 0;
        self.svc_env_col = 0;
        self.svc_editing_field = false;

        self.input_socket_id
            .update(cx, |s, cx| s.set_value(&service.socket_id, window, cx));
        let command_str = service.command.as_deref().unwrap_or("").to_string();
        self.input_svc_command
            .update(cx, |s, cx| s.set_value(&command_str, window, cx));

        let timeout_str = service
            .startup_timeout_ms
            .map(|v| v.to_string())
            .unwrap_or_default();
        self.input_svc_timeout
            .update(cx, |s, cx| s.set_value(&timeout_str, window, cx));

        self.svc_arg_inputs = service
            .args
            .iter()
            .map(|arg| {
                let arg = arg.clone();
                cx.new(|cx| {
                    let mut state = InputState::new(window, cx);
                    state.set_value(&arg, window, cx);
                    state
                })
            })
            .collect();

        let mut env_entries: Vec<(String, String)> = service.env.into_iter().collect();
        env_entries.sort_by(|a, b| a.0.cmp(&b.0));

        self.svc_env_key_inputs.clear();
        self.svc_env_value_inputs.clear();

        for (key, value) in &env_entries {
            let key = key.clone();
            let value = value.clone();
            self.svc_env_key_inputs.push(cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("KEY");
                state.set_value(&key, window, cx);
                state
            }));
            self.svc_env_value_inputs.push(cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("value");
                state.set_value(&value, window, cx);
                state
            }));
        }

        cx.notify();
    }

    pub(super) fn save_service(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let socket_id = self.input_socket_id.read(cx).value().trim().to_string();
        if socket_id.is_empty() {
            cx.toast_error("Socket ID is required", window);
            return;
        }

        let is_duplicate = self
            .svc_services
            .iter()
            .enumerate()
            .any(|(i, s)| s.socket_id == socket_id && Some(i) != self.editing_svc_idx);
        if is_duplicate {
            cx.toast_error(
                format!("A service with socket ID \"{}\" already exists", socket_id),
                window,
            );
            return;
        }

        let timeout_str = self.input_svc_timeout.read(cx).value().trim().to_string();
        let startup_timeout_ms = if timeout_str.is_empty() {
            None
        } else {
            match timeout_str.parse::<u64>() {
                Ok(v) => Some(v),
                Err(_) => {
                    cx.toast_error("Timeout must be a valid number (milliseconds)", window);
                    return;
                }
            }
        };

        let command_str = self.input_svc_command.read(cx).value().trim().to_string();
        let command = if command_str.is_empty() {
            None
        } else {
            Some(command_str)
        };

        let args: Vec<String> = self
            .svc_arg_inputs
            .iter()
            .map(|input| input.read(cx).value().trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let env: HashMap<String, String> = self
            .svc_env_key_inputs
            .iter()
            .zip(self.svc_env_value_inputs.iter())
            .filter_map(|(key_input, val_input)| {
                let key = key_input.read(cx).value().trim().to_string();
                if key.is_empty() {
                    return None;
                }
                let value = val_input.read(cx).value().to_string();
                Some((key, value))
            })
            .collect();

        let service = ServiceConfig {
            socket_id,
            enabled: self.svc_enabled,
            command,
            args,
            env,
            startup_timeout_ms,
        };

        let saved_idx = if let Some(idx) = self.editing_svc_idx {
            if idx < self.svc_services.len() {
                self.svc_services[idx] = service;
            }
            idx
        } else {
            self.svc_services.push(service);
            self.svc_services.len() - 1
        };

        self.persist_services(window, cx);
        cx.toast_info("Service saved. Restart required to apply changes.", window);

        self.svc_selected_idx = Some(saved_idx);
        self.edit_service(saved_idx, window, cx);
    }

    // --- Delete flow ---

    pub(super) fn request_delete_service(&mut self, idx: usize, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = None;
        self.pending_delete_svc_idx = Some(idx);
        cx.notify();
    }

    pub(super) fn confirm_delete_service(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(idx) = self.pending_delete_svc_idx.take() else {
            return;
        };

        if idx >= self.svc_services.len() {
            return;
        }

        self.svc_services.remove(idx);

        if self.editing_svc_idx == Some(idx) {
            self.clear_svc_form(window, cx);
        } else if let Some(edit_idx) = self.editing_svc_idx
            && edit_idx > idx
        {
            self.editing_svc_idx = Some(edit_idx - 1);
        }

        let count = self.svc_services.len();
        if count == 0 {
            self.svc_selected_idx = None;
        } else if let Some(sel) = self.svc_selected_idx {
            if sel >= count {
                self.svc_selected_idx = Some(count - 1);
            } else if sel > idx {
                self.svc_selected_idx = Some(sel - 1);
            }
        }

        self.persist_services(window, cx);
        cx.toast_info(
            "Service deleted. Restart required to apply changes.",
            window,
        );
        cx.notify();
    }

    pub(super) fn cancel_delete_service(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_svc_idx = None;
        cx.notify();
    }

    // --- Dynamic list management ---

    pub(super) fn add_arg_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| InputState::new(window, cx).placeholder("argument"));
        self.svc_arg_inputs.push(input);

        let new_idx = self.svc_arg_inputs.len() - 1;
        let rows = self.svc_form_rows();
        if let Some(pos) = rows.iter().position(|r| *r == ServiceFormRow::Arg(new_idx)) {
            self.svc_form_cursor = pos;
        }

        cx.notify();
    }

    pub(super) fn remove_arg_row(
        &mut self,
        idx: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if idx < self.svc_arg_inputs.len() {
            self.svc_arg_inputs.remove(idx);
            self.svc_editing_field = false;
            self.validate_svc_form_cursor();
            cx.notify();
        }
    }

    pub(super) fn add_env_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.svc_env_key_inputs
            .push(cx.new(|cx| InputState::new(window, cx).placeholder("KEY")));
        self.svc_env_value_inputs
            .push(cx.new(|cx| InputState::new(window, cx).placeholder("value")));

        let new_idx = self.svc_env_key_inputs.len() - 1;
        let rows = self.svc_form_rows();
        if let Some(pos) = rows
            .iter()
            .position(|r| *r == ServiceFormRow::EnvKey(new_idx))
        {
            self.svc_form_cursor = pos;
            self.svc_env_col = 0;
        }

        cx.notify();
    }

    pub(super) fn remove_env_row(
        &mut self,
        idx: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if idx < self.svc_env_key_inputs.len() {
            self.svc_env_key_inputs.remove(idx);
            self.svc_env_value_inputs.remove(idx);
            self.svc_editing_field = false;
            self.validate_svc_form_cursor();
            cx.notify();
        }
    }

    // --- List navigation ---

    pub(super) fn svc_move_next_profile(&mut self) {
        let count = self.svc_services.len();
        if count == 0 {
            self.svc_selected_idx = None;
            return;
        }

        match self.svc_selected_idx {
            None => self.svc_selected_idx = Some(0),
            Some(idx) if idx + 1 < count => self.svc_selected_idx = Some(idx + 1),
            _ => {}
        }
    }

    pub(super) fn svc_move_prev_profile(&mut self) {
        let count = self.svc_services.len();
        if count == 0 {
            self.svc_selected_idx = None;
            return;
        }

        match self.svc_selected_idx {
            Some(idx) if idx > 0 => self.svc_selected_idx = Some(idx - 1),
            Some(0) => self.svc_selected_idx = None,
            _ => {}
        }
    }

    pub(super) fn svc_load_selected_profile(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(idx) = self.svc_selected_idx
            && idx >= self.svc_services.len()
        {
            self.svc_selected_idx = if self.svc_services.is_empty() {
                None
            } else {
                Some(self.svc_services.len() - 1)
            };
        }

        if let Some(idx) = self.svc_selected_idx {
            self.edit_service(idx, window, cx);
            return;
        }

        self.clear_svc_form(window, cx);
    }

    pub(super) fn svc_enter_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.svc_focus = ServiceFocus::Form;
        self.svc_form_cursor = 0;
        self.svc_editing_field = false;

        self.svc_load_selected_profile(window, cx);
    }

    pub(super) fn svc_exit_form(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.svc_focus = ServiceFocus::List;
        self.svc_editing_field = false;
        self.focus_handle.focus(window);
    }

    // --- Form navigation (linear cursor) ---

    pub(super) fn svc_form_rows(&self) -> Vec<ServiceFormRow> {
        let mut rows = vec![
            ServiceFormRow::SocketId,
            ServiceFormRow::Command,
            ServiceFormRow::Timeout,
            ServiceFormRow::Enabled,
        ];

        for i in 0..self.svc_arg_inputs.len() {
            rows.push(ServiceFormRow::Arg(i));
        }
        rows.push(ServiceFormRow::AddArg);

        for i in 0..self.svc_env_key_inputs.len() {
            rows.push(ServiceFormRow::EnvKey(i));
        }
        rows.push(ServiceFormRow::AddEnv);

        if self.editing_svc_idx.is_some() {
            rows.push(ServiceFormRow::DeleteButton);
        }
        rows.push(ServiceFormRow::SaveButton);

        rows
    }

    fn current_form_row(&self) -> Option<ServiceFormRow> {
        let rows = self.svc_form_rows();
        rows.get(self.svc_form_cursor).copied()
    }

    pub(super) fn svc_move_down(&mut self) {
        let count = self.svc_form_rows().len();
        if self.svc_form_cursor + 1 < count {
            self.svc_form_cursor += 1;
            self.svc_env_col = 0;
        }
    }

    pub(super) fn svc_move_up(&mut self) {
        if self.svc_form_cursor > 0 {
            self.svc_form_cursor -= 1;
            self.svc_env_col = 0;
        }
    }

    pub(super) fn svc_move_right(&mut self) {
        if let Some(row) = self.current_form_row() {
            match row {
                ServiceFormRow::EnvKey(_) => {
                    if self.svc_env_col < 2 {
                        self.svc_env_col += 1;
                    }
                }
                ServiceFormRow::Arg(_) => {
                    if self.svc_env_col < 1 {
                        self.svc_env_col += 1;
                    }
                }
                _ => {}
            }
        }
    }

    pub(super) fn svc_move_left(&mut self) {
        if self.svc_env_col > 0 {
            self.svc_env_col -= 1;
        }
    }

    pub(super) fn svc_move_first(&mut self) {
        self.svc_form_cursor = 0;
        self.svc_env_col = 0;
    }

    pub(super) fn svc_move_last(&mut self) {
        let count = self.svc_form_rows().len();
        if count > 0 {
            self.svc_form_cursor = count - 1;
        }
        self.svc_env_col = 0;
    }

    pub(super) fn svc_tab_next(&mut self) {
        let rows = self.svc_form_rows();
        if let Some(row) = rows.get(self.svc_form_cursor) {
            let max_col = match row {
                ServiceFormRow::EnvKey(_) => 2,
                ServiceFormRow::Arg(_) => 1,
                _ => 0,
            };

            if self.svc_env_col < max_col {
                self.svc_env_col += 1;
                return;
            }
        }

        if self.svc_form_cursor + 1 < rows.len() {
            self.svc_form_cursor += 1;
            self.svc_env_col = 0;
        }
    }

    pub(super) fn svc_tab_prev(&mut self) {
        if self.svc_env_col > 0 {
            self.svc_env_col -= 1;
            return;
        }

        if self.svc_form_cursor > 0 {
            self.svc_form_cursor -= 1;
            let rows = self.svc_form_rows();
            if let Some(row) = rows.get(self.svc_form_cursor) {
                self.svc_env_col = match row {
                    ServiceFormRow::EnvKey(_) => 2,
                    ServiceFormRow::Arg(_) => 1,
                    _ => 0,
                };
            }
        }
    }

    pub(super) fn svc_focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.svc_editing_field = true;

        match self.current_form_row() {
            Some(ServiceFormRow::SocketId) => {
                self.input_socket_id.update(cx, |s, cx| s.focus(window, cx));
            }
            Some(ServiceFormRow::Command) => {
                self.input_svc_command
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            Some(ServiceFormRow::Timeout) => {
                self.input_svc_timeout
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            Some(ServiceFormRow::Arg(i)) if self.svc_env_col == 0 => {
                if let Some(input) = self.svc_arg_inputs.get(i) {
                    input.update(cx, |s, cx| s.focus(window, cx));
                } else {
                    self.svc_editing_field = false;
                }
            }
            Some(ServiceFormRow::EnvKey(i)) if self.svc_env_col == 0 => {
                if let Some(input) = self.svc_env_key_inputs.get(i) {
                    input.update(cx, |s, cx| s.focus(window, cx));
                } else {
                    self.svc_editing_field = false;
                }
            }
            Some(ServiceFormRow::EnvKey(i)) if self.svc_env_col == 1 => {
                if let Some(input) = self.svc_env_value_inputs.get(i) {
                    input.update(cx, |s, cx| s.focus(window, cx));
                } else {
                    self.svc_editing_field = false;
                }
            }
            _ => {
                self.svc_editing_field = false;
            }
        }
    }

    pub(super) fn svc_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.current_form_row() {
            Some(ServiceFormRow::SocketId)
            | Some(ServiceFormRow::Command)
            | Some(ServiceFormRow::Timeout) => {
                self.svc_focus_current_field(window, cx);
            }

            Some(ServiceFormRow::Arg(i)) => {
                if self.svc_env_col == 1 {
                    self.remove_arg_row(i, window, cx);
                } else {
                    self.svc_focus_current_field(window, cx);
                }
            }

            Some(ServiceFormRow::EnvKey(i)) => {
                if self.svc_env_col == 2 {
                    self.remove_env_row(i, window, cx);
                } else {
                    self.svc_focus_current_field(window, cx);
                }
            }

            Some(ServiceFormRow::Enabled) => {
                self.svc_enabled = !self.svc_enabled;
                cx.notify();
            }

            Some(ServiceFormRow::AddArg) => {
                self.add_arg_row(window, cx);
            }
            Some(ServiceFormRow::AddEnv) => {
                self.add_env_row(window, cx);
            }

            Some(ServiceFormRow::SaveButton) => {
                self.save_service(window, cx);
            }
            Some(ServiceFormRow::DeleteButton) => {
                if let Some(idx) = self.editing_svc_idx {
                    self.request_delete_service(idx, cx);
                }
            }

            None => {}
        }
    }

    fn validate_svc_form_cursor(&mut self) {
        let count = self.svc_form_rows().len();
        if count == 0 {
            self.svc_form_cursor = 0;
        } else if self.svc_form_cursor >= count {
            self.svc_form_cursor = count - 1;
        }
        self.svc_env_col = 0;
    }
}
