use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_components::controls::{Button, Checkbox, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, SegmentedControl, SegmentedItem, Text};
use dbflux_core::{RpcServiceKind, ServiceConfig};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::collections::HashMap;

use super::layout;
use super::services_section::{ServiceFocus, ServiceFormRow, ServicesSection};
use crate::labels::rpc_services_duplicate_socket_id;

fn services_section_title() -> String {
    dbflux_i18n::t!("settings.rpc_services.section_title")
}

fn services_section_description() -> String {
    dbflux_i18n::t!("settings.rpc_services.section_description")
}

fn empty_services_message() -> String {
    dbflux_i18n::t!("settings.rpc_services.empty")
}

fn new_service_button_label() -> String {
    dbflux_i18n::t!("settings.rpc_services.new_title")
}

fn editable_service_kind(service: &ServiceConfig) -> RpcServiceKind {
    service.kind
}

fn build_service_config(
    socket_id: String,
    enabled: bool,
    command: Option<String>,
    args: Vec<String>,
    env: HashMap<String, String>,
    startup_timeout_ms: Option<u64>,
    kind: RpcServiceKind,
) -> ServiceConfig {
    ServiceConfig {
        socket_id,
        enabled,
        command,
        args,
        env,
        startup_timeout_ms,
        kind,
        api_contract: None,
    }
}

fn preserved_api_contract_for_edit(
    services: &[ServiceConfig],
    editing_svc_idx: Option<usize>,
) -> Option<dbflux_core::ServiceRpcApiContract> {
    editing_svc_idx
        .and_then(|idx| services.get(idx))
        .and_then(|service| service.api_contract.clone())
}

fn service_form_rows(
    arg_count: usize,
    env_count: usize,
    editing_service: bool,
) -> Vec<ServiceFormRow> {
    let mut rows = vec![
        ServiceFormRow::SocketId,
        ServiceFormRow::Command,
        ServiceFormRow::Timeout,
        ServiceFormRow::Kind,
        ServiceFormRow::Enabled,
    ];

    for index in 0..arg_count {
        rows.push(ServiceFormRow::Arg(index));
    }
    rows.push(ServiceFormRow::AddArg);

    for index in 0..env_count {
        rows.push(ServiceFormRow::EnvKey(index));
    }
    rows.push(ServiceFormRow::AddEnv);

    if editing_service {
        rows.push(ServiceFormRow::DeleteButton);
    }

    rows.push(ServiceFormRow::SaveButton);
    rows
}

fn service_row_max_col(row: ServiceFormRow) -> usize {
    match row {
        ServiceFormRow::Kind | ServiceFormRow::Arg(_) => 1,
        ServiceFormRow::EnvKey(_) => 2,
        _ => 0,
    }
}

impl ServicesSection {
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
                .map(|value| value.to_string())
                .unwrap_or_default();

            if socket_id != saved.socket_id
                || command != saved_command
                || timeout != saved_timeout
                || self.svc_kind != saved.kind
                || self.svc_enabled != saved.enabled
            {
                return true;
            }

            let form_args: Vec<String> = self
                .svc_arg_inputs
                .iter()
                .map(|input| input.read(cx).value().trim().to_string())
                .filter(|value| !value.is_empty())
                .collect();
            if form_args != saved.args {
                return true;
            }

            let mut form_env: Vec<(String, String)> = self
                .svc_env_key_inputs
                .iter()
                .zip(self.svc_env_value_inputs.iter())
                .filter_map(|(key_input, value_input)| {
                    let key = key_input.read(cx).value().trim().to_string();
                    if key.is_empty() {
                        return None;
                    }

                    Some((key, value_input.read(cx).value().to_string()))
                })
                .collect();
            form_env.sort_by(|left, right| left.0.cmp(&right.0));

            let mut saved_env: Vec<(String, String)> = saved
                .env
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            saved_env.sort_by(|left, right| left.0.cmp(&right.0));

            return form_env != saved_env;
        }

        !self.input_socket_id.read(cx).value().trim().is_empty()
            || !self.input_svc_command.read(cx).value().trim().is_empty()
            || !self.input_svc_timeout.read(cx).value().trim().is_empty()
            || !self.svc_arg_inputs.is_empty()
            || !self.svc_env_key_inputs.is_empty()
            || !self.svc_env_value_inputs.is_empty()
            || self.svc_kind != RpcServiceKind::Driver
            || !self.svc_enabled
    }

    pub(super) fn load_services(&mut self, runtime: &StorageRuntime) {
        let config = match dbflux_app::config_loader::load_config(runtime) {
            Ok(config) => config,
            Err(error) => {
                log::warn!("Failed to load RPC services: {error}");
                return;
            }
        };
        self.svc_services = config.services;
    }

    // --- Form lifecycle ---

    pub(super) fn clear_svc_form(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.editing_svc_idx = None;
        self.svc_kind = RpcServiceKind::Driver;
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
        self.svc_kind = editable_service_kind(&service);
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
            let msg = dbflux_i18n::t!("settings.rpc_services.error.socket_id_required");
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
            return;
        }

        let is_duplicate = self
            .svc_services
            .iter()
            .enumerate()
            .any(|(i, s)| s.socket_id == socket_id && Some(i) != self.editing_svc_idx);
        if is_duplicate {
            let msg = rpc_services_duplicate_socket_id(&socket_id);
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
            return;
        }

        let timeout_str = self.input_svc_timeout.read(cx).value().trim().to_string();
        let startup_timeout_ms = if timeout_str.is_empty() {
            None
        } else {
            match timeout_str.parse::<u64>() {
                Ok(v) => Some(v),
                Err(_) => {
                    let msg = dbflux_i18n::t!("settings.rpc_services.error.timeout_invalid");
                    Toast::error(msg.clone())
                        .meta_right(now_hms())
                        .action(copy_action(msg))
                        .push(cx);
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

        let mut service = build_service_config(
            socket_id,
            self.svc_enabled,
            command,
            args,
            env,
            startup_timeout_ms,
            self.svc_kind,
        );

        service.api_contract =
            preserved_api_contract_for_edit(&self.svc_services, self.editing_svc_idx);

        let saved_idx = if let Some(idx) = self.editing_svc_idx {
            if idx < self.svc_services.len() {
                self.svc_services[idx] = service;
            }
            idx
        } else {
            self.svc_services.push(service);
            self.svc_services.len() - 1
        };

        let runtime = self.app_state.read(cx).storage_runtime();
        if let Err(e) = dbflux_app::config_loader::save_services(runtime, &self.svc_services) {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.rpc_services.error.save_failed", error = e),
                ),
                cx,
            );
            return;
        }
        Toast::info(dbflux_i18n::t!("settings.rpc_services.toast.saved"))
            .meta_right(now_hms())
            .push(cx);

        self.svc_selected_idx = Some(saved_idx);
        self.edit_service(saved_idx, window, cx);
    }

    // --- Delete flow ---

    pub(super) fn request_delete_service(&mut self, idx: usize, cx: &mut Context<Self>) {
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

        let runtime = self.app_state.read(cx).storage_runtime();
        if let Err(e) = dbflux_app::config_loader::save_services(runtime, &self.svc_services) {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.rpc_services.error.save_failed", error = e),
                ),
                cx,
            );
            return;
        }
        Toast::info(dbflux_i18n::t!("settings.rpc_services.toast.deleted"))
            .meta_right(now_hms())
            .push(cx);
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
        let _ = window;
        self.svc_focus = ServiceFocus::List;
        self.svc_editing_field = false;
    }

    // --- Form navigation (linear cursor) ---

    pub(super) fn svc_form_rows(&self) -> Vec<ServiceFormRow> {
        service_form_rows(
            self.svc_arg_inputs.len(),
            self.svc_env_key_inputs.len(),
            self.editing_svc_idx.is_some(),
        )
    }

    pub(super) fn current_form_row(&self) -> Option<ServiceFormRow> {
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
                row if self.svc_env_col < service_row_max_col(row) => {
                    self.svc_env_col += 1;
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
            let max_col = service_row_max_col(*row);

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
                self.svc_env_col = service_row_max_col(*row);
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
            Some(ServiceFormRow::Kind) => {
                self.svc_editing_field = false;
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
            | Some(ServiceFormRow::Timeout)
            | Some(ServiceFormRow::EnvValue(_)) => {
                self.svc_focus_current_field(window, cx);
            }

            Some(ServiceFormRow::Kind) => {
                self.svc_kind = if self.svc_env_col == 0 {
                    RpcServiceKind::Driver
                } else {
                    RpcServiceKind::AuthProvider
                };
                cx.notify();
            }

            Some(ServiceFormRow::Arg(i)) => {
                if self.svc_env_col == 1 {
                    self.remove_arg_row(i, window, cx);
                } else {
                    self.svc_focus_current_field(window, cx);
                }
            }

            Some(ServiceFormRow::ArgDelete(i)) => {
                self.remove_arg_row(i, window, cx);
            }

            Some(ServiceFormRow::EnvKey(i)) => {
                if self.svc_env_col == 2 {
                    self.remove_env_row(i, window, cx);
                } else {
                    self.svc_focus_current_field(window, cx);
                }
            }

            Some(ServiceFormRow::EnvDelete(i)) => {
                self.remove_env_row(i, window, cx);
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

impl ServicesSection {
    pub(super) fn render_services_section(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let services = self.svc_services.clone();
        let editing_idx = self.editing_svc_idx;

        layout::split_section_shell(
            cx.theme().border,
            dbflux_components::composites::page_header(
                services_section_title(),
                services_section_description(),
                cx,
            ),
            self.render_service_list(&services, editing_idx, cx),
            self.render_service_form(cx),
        )
    }

    fn render_service_list(
        &mut self,
        services: &[ServiceConfig],
        editing_idx: Option<usize>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let is_list_focused = self.content_focused && self.svc_focus == ServiceFocus::List;
        let is_new_button_focused = is_list_focused && self.svc_selected_idx.is_none();

        if let Some(scroll_idx) = self.svc_pending_scroll_idx.take() {
            self.svc_list_scroll_handle.scroll_to_item(scroll_idx);
        }

        let toolbar = layout::master_list_toolbar(vec![
            Button::new("new-service", new_service_button_label())
                .small()
                .primary()
                .icon(AppIcon::Plus)
                .focused(is_new_button_focused)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.svc_selected_idx = None;
                    this.clear_svc_form(window, cx);
                }))
                .into_any_element(),
        ]);

        let rows: Vec<AnyElement> = services
            .iter()
            .enumerate()
            .map(|(idx, service)| {
                let is_selected = editing_idx == Some(idx);
                let is_focused = is_list_focused && self.svc_selected_idx == Some(idx);

                let command = service
                    .command
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| {
                        dbflux_i18n::t!("settings.rpc_services.field.default_command")
                    });
                let kind = match service.kind {
                    RpcServiceKind::Driver => dbflux_i18n::t!("settings.rpc_services.kind.driver"),
                    RpcServiceKind::AuthProvider => {
                        dbflux_i18n::t!("settings.rpc_services.kind.auth_provider")
                    }
                };

                let trailing = (!service.enabled).then(|| {
                    Badge::new(
                        dbflux_i18n::t!("settings.rpc_services.field.disabled"),
                        BadgeTone::Neutral,
                    )
                    .into_any_element()
                });

                layout::master_list_row(
                    SharedString::from(format!("svc-item-{}", idx)),
                    layout::MasterRow {
                        icon: Some(AppIcon::Plug),
                        title: service.socket_id.clone().into(),
                        detail: Some(format!("{kind} · {command}").into()),
                        trailing,
                    },
                    is_selected,
                    is_focused,
                    cx,
                )
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.svc_selected_idx = Some(idx);
                    this.edit_service(idx, window, cx);
                }))
                .into_any_element()
            })
            .collect();

        let empty = rows.is_empty();
        let theme = cx.theme();

        div()
            .w(SettingsMetrics::LIST_WIDTH)
            .h_full()
            .min_h_0()
            .flex_shrink_0()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(toolbar)
            .child(
                div()
                    .id("services-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.svc_list_scroll_handle)
                    .flex()
                    .flex_col()
                    .when(empty, |list| {
                        list.child(layout::master_list_empty(empty_services_message()))
                    })
                    .children(rows),
            )
    }

    /// Whether the keyboard cursor is on `row` (and on `column` of it).
    fn svc_cursor_on(&self, row: ServiceFormRow, column: usize) -> bool {
        self.content_focused
            && self.svc_focus == ServiceFocus::Form
            && !self.svc_editing_field
            && self.svc_form_rows().get(self.svc_form_cursor).copied() == Some(row)
            && self.svc_env_col == column
    }

    /// Moves the keyboard cursor to `row`/`column` and focuses its input.
    fn svc_select(
        &mut self,
        row: ServiceFormRow,
        column: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.svc_focus = ServiceFocus::Form;

        if let Some(position) = self
            .svc_form_rows()
            .iter()
            .position(|candidate| *candidate == row)
        {
            self.svc_form_cursor = position;
            self.svc_env_col = column;
        }

        self.svc_focus_current_field(window, cx);
        cx.notify();
    }

    fn render_service_form(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        layout::sticky_form_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.rpc_services.group.service"),
                Some(AppIcon::Plug.into()),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .child(self.render_svc_input_field(
                    dbflux_i18n::t!("settings.rpc_services.field.socket_id"),
                    &self.input_socket_id,
                    ServiceFormRow::SocketId,
                    Some(SettingsMetrics::TEXT_FIELD_WIDTH),
                    None,
                    cx,
                ))
                .child(self.render_svc_input_field(
                    dbflux_i18n::t!("settings.rpc_services.field.command"),
                    &self.input_svc_command,
                    ServiceFormRow::Command,
                    None,
                    None,
                    cx,
                ))
                .child(self.render_svc_input_field(
                    dbflux_i18n::t!("settings.rpc_services.field.startup_timeout"),
                    &self.input_svc_timeout,
                    ServiceFormRow::Timeout,
                    Some(SettingsMetrics::NUMBER_FIELD_WIDTH),
                    Some(dbflux_i18n::t!("settings.general.unit.milliseconds")),
                    cx,
                ))
                .child(self.render_svc_kind_selector(cx))
                .child(self.render_svc_enabled_checkbox(cx))
                .child(self.render_svc_args_section(cx))
                .child(self.render_svc_env_section(cx)),
            None,
            &theme,
        )
    }

    /// Delete, on the left of the footer, for a saved service.
    pub(super) fn render_service_footer_leading_actions(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.editing_svc_idx?;

        Some(
            Button::new(
                "delete-service",
                dbflux_i18n::t!("settings.rpc_services.action.delete"),
            )
            .small()
            .danger()
            .icon(AppIcon::Delete)
            .focused(self.svc_cursor_on(ServiceFormRow::DeleteButton, 0))
            .on_click(cx.listener(move |this, _, _, cx| {
                if let Some(idx) = this.editing_svc_idx {
                    this.request_delete_service(idx, cx);
                }
            }))
            .into_any_element(),
        )
    }

    pub(super) fn render_service_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        Button::new(
            "save-service",
            if self.editing_svc_idx.is_some() {
                dbflux_i18n::t!("settings.rpc_services.action.update")
            } else {
                dbflux_i18n::t!("settings.rpc_services.action.create")
            },
        )
        .small()
        .primary()
        .icon(AppIcon::Check)
        .kbd("Ctrl S")
        .focused(self.svc_cursor_on(ServiceFormRow::SaveButton, 0))
        .on_click(cx.listener(|this, _, window, cx| {
            this.save_service(window, cx);
        }))
        .into_any_element()
    }

    fn render_svc_input_field(
        &self,
        label: String,
        input: &Entity<InputState>,
        row: ServiceFormRow,
        width: Option<Pixels>,
        unit: Option<String>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let control = Input::new(input)
            .aria_label(label.clone())
            .when_some(unit, |control, unit| {
                control.suffix(Text::code(unit).muted_foreground())
            });

        layout::form_row(
            label,
            layout::field_frame(self.svc_cursor_on(row, 0), width, true, control, cx)
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, window, cx| {
                        this.svc_select(row, 0, window, cx);
                    }),
                ),
            None,
        )
    }

    fn render_svc_enabled_checkbox(&self, cx: &mut Context<Self>) -> impl IntoElement {
        layout::check_row(
            layout::cursor_ring(
                self.svc_cursor_on(ServiceFormRow::Enabled, 0),
                Checkbox::new("svc-enabled")
                    .checked(self.svc_enabled)
                    .label(dbflux_i18n::t!("settings.rpc_services.field.enable"))
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.svc_enabled = *checked;
                        cx.notify();
                    })),
                cx,
            ),
            None,
        )
    }

    fn render_svc_kind_selector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity();
        let active = match self.svc_kind {
            RpcServiceKind::Driver => "driver",
            RpcServiceKind::AuthProvider => "auth_provider",
        };

        let control = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "driver",
                    dbflux_i18n::t!("settings.rpc_services.kind.driver"),
                )
                .icon(AppIcon::Database),
                SegmentedItem::new(
                    "auth_provider",
                    dbflux_i18n::t!("settings.rpc_services.kind.auth_provider"),
                )
                .icon(AppIcon::KeyRound),
            ],
            active,
            move |selected, _window, cx| {
                let (kind, column) = if selected.as_ref() == "auth_provider" {
                    (RpcServiceKind::AuthProvider, 1)
                } else {
                    (RpcServiceKind::Driver, 0)
                };

                entity.update(cx, |this, cx| {
                    this.svc_focus = ServiceFocus::Form;
                    if let Some(position) = this
                        .svc_form_rows()
                        .iter()
                        .position(|candidate| *candidate == ServiceFormRow::Kind)
                    {
                        this.svc_form_cursor = position;
                    }
                    this.svc_env_col = column;
                    this.svc_kind = kind;
                    this.svc_editing_field = false;
                    cx.notify();
                });
            },
        );

        let cursor_item = if self.svc_cursor_on(ServiceFormRow::Kind, 0) {
            Some("driver")
        } else if self.svc_cursor_on(ServiceFormRow::Kind, 1) {
            Some("auth_provider")
        } else {
            None
        };

        let control = control
            .focused(cursor_item.is_some())
            .when_some(cursor_item, |control, id| control.focused_item(id));

        layout::form_row(
            dbflux_i18n::t!("settings.rpc_services.field.service_type"),
            div().flex().child(control),
            None,
        )
    }

    fn remove_button(
        &self,
        id: SharedString,
        focused: bool,
        on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> impl IntoElement {
        Button::new(id, dbflux_i18n::t!("settings.rpc_services.action.remove"))
            .small()
            .ghost()
            .icon(AppIcon::X)
            .icon_only()
            .focused(focused)
            .on_click(on_click)
    }

    fn add_button(
        &self,
        id: &'static str,
        label: String,
        focused: bool,
        on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> impl IntoElement {
        Button::new(id, label)
            .small()
            .secondary()
            .icon(AppIcon::Plus)
            .focused(focused)
            .on_click(on_click)
    }

    fn render_svc_args_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let arg_rows = self.svc_arg_inputs.iter().enumerate().map(|(idx, input)| {
            let row = ServiceFormRow::Arg(idx);

            layout::inline_controls()
                .child(
                    layout::field_frame(
                        self.svc_cursor_on(row, 0),
                        None,
                        true,
                        Input::new(input)
                            .aria_label(dbflux_i18n::t!("settings.rpc_services.field.arguments")),
                        cx,
                    )
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.svc_select(row, 0, window, cx);
                        }),
                    ),
                )
                .child(self.remove_button(
                    SharedString::from(format!("rm-arg-{}", idx)),
                    self.svc_cursor_on(row, 1),
                    cx.listener(move |this, _, window, cx| {
                        this.remove_arg_row(idx, window, cx);
                    }),
                ))
        });

        layout::form_row(
            dbflux_i18n::t!("settings.rpc_services.field.arguments"),
            div()
                .flex()
                .flex_col()
                .gap(FormMetrics::INLINE_GAP)
                .children(arg_rows)
                .child(div().flex().child(self.add_button(
                    "add-arg",
                    dbflux_i18n::t!("settings.rpc_services.action.add_argument"),
                    self.svc_cursor_on(ServiceFormRow::AddArg, 0),
                    cx.listener(|this, _, window, cx| {
                        this.add_arg_row(window, cx);
                    }),
                ))),
            None,
        )
    }

    fn render_svc_env_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let env_rows = self
            .svc_env_key_inputs
            .iter()
            .zip(self.svc_env_value_inputs.iter())
            .enumerate()
            .map(|(idx, (key_input, value_input))| {
                let row = ServiceFormRow::EnvKey(idx);

                layout::inline_controls()
                    .child(
                        layout::field_frame(
                            self.svc_cursor_on(row, 0),
                            None,
                            true,
                            Input::new(key_input)
                                .aria_label(dbflux_i18n::t!("settings.rpc_services.field.env_key")),
                            cx,
                        )
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                this.svc_select(row, 0, window, cx);
                            }),
                        ),
                    )
                    .child(Text::code("=").muted_foreground())
                    .child(
                        layout::field_frame(
                            self.svc_cursor_on(row, 1),
                            None,
                            true,
                            Input::new(value_input).aria_label(dbflux_i18n::t!(
                                "settings.rpc_services.field.env_value"
                            )),
                            cx,
                        )
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                this.svc_select(row, 1, window, cx);
                            }),
                        ),
                    )
                    .child(self.remove_button(
                        SharedString::from(format!("rm-env-{}", idx)),
                        self.svc_cursor_on(row, 2),
                        cx.listener(move |this, _, window, cx| {
                            this.remove_env_row(idx, window, cx);
                        }),
                    ))
            });

        layout::form_row(
            dbflux_i18n::t!("settings.rpc_services.field.env_vars"),
            div()
                .flex()
                .flex_col()
                .gap(FormMetrics::INLINE_GAP)
                .children(env_rows)
                .child(div().flex().child(self.add_button(
                    "add-env",
                    dbflux_i18n::t!("settings.rpc_services.action.add_variable"),
                    self.svc_cursor_on(ServiceFormRow::AddEnv, 0),
                    cx.listener(|this, _, window, cx| {
                        this.add_env_row(window, cx);
                    }),
                ))),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ServiceFormRow, build_service_config, editable_service_kind,
        preserved_api_contract_for_edit, service_form_rows, service_row_max_col,
    };
    use dbflux_core::{RpcServiceKind, ServiceConfig, ServiceRpcApiContract};
    use std::collections::HashMap;

    #[test]
    fn service_form_rows_include_kind_selector_before_enabled_toggle() {
        let rows = service_form_rows(0, 0, false);

        assert_eq!(
            rows,
            vec![
                ServiceFormRow::SocketId,
                ServiceFormRow::Command,
                ServiceFormRow::Timeout,
                ServiceFormRow::Kind,
                ServiceFormRow::Enabled,
                ServiceFormRow::AddArg,
                ServiceFormRow::AddEnv,
                ServiceFormRow::SaveButton,
            ]
        );
    }

    #[test]
    fn service_row_max_col_allows_driver_and_auth_provider_selection() {
        assert_eq!(service_row_max_col(ServiceFormRow::Kind), 1);
        assert_eq!(service_row_max_col(ServiceFormRow::Enabled), 0);
    }

    #[test]
    fn editable_service_kind_preserves_saved_auth_provider_kind() {
        let service = ServiceConfig {
            socket_id: "auth.sock".into(),
            enabled: true,
            command: Some("dbflux-auth".into()),
            args: vec!["--serve".into()],
            env: HashMap::new(),
            startup_timeout_ms: Some(5000),
            kind: RpcServiceKind::AuthProvider,
            api_contract: None,
        };

        assert_eq!(
            editable_service_kind(&service),
            RpcServiceKind::AuthProvider
        );
    }

    #[test]
    fn build_service_config_preserves_selected_service_kind() {
        let service = build_service_config(
            "auth.sock".into(),
            true,
            Some("dbflux-auth".into()),
            vec!["--serve".into()],
            HashMap::from([("MODE".into(), "auth".into())]),
            Some(5000),
            RpcServiceKind::AuthProvider,
        );

        assert_eq!(service.kind, RpcServiceKind::AuthProvider);
        assert_eq!(service.socket_id, "auth.sock");
    }

    #[test]
    fn preserved_api_contract_for_edit_returns_saved_metadata() {
        let api_contract = ServiceRpcApiContract::new("auth_provider_rpc", 1, 0);
        let services = vec![ServiceConfig {
            socket_id: "auth.sock".into(),
            enabled: true,
            command: Some("dbflux-auth".into()),
            args: vec!["--serve".into()],
            env: HashMap::from([("MODE".into(), "auth".into())]),
            startup_timeout_ms: Some(5000),
            kind: RpcServiceKind::AuthProvider,
            api_contract: Some(api_contract.clone()),
        }];

        assert_eq!(
            preserved_api_contract_for_edit(&services, Some(0)),
            Some(api_contract)
        );
    }

    const RPC_SERVICES_KEYS: &[&str] = &[
        "settings.rpc_services.section_title",
        "settings.rpc_services.section_description",
        "settings.rpc_services.empty",
        "settings.rpc_services.edit_title",
        "settings.rpc_services.new_title",
        "settings.rpc_services.error.socket_id_required",
        "settings.rpc_services.error.timeout_invalid",
        "settings.rpc_services.error.duplicate_socket_id",
        "settings.rpc_services.toast.saved",
        "settings.rpc_services.toast.deleted",
        "settings.rpc_services.field.disabled",
        "settings.rpc_services.field.socket_id",
        "settings.rpc_services.field.command",
        "settings.rpc_services.field.startup_timeout",
        "settings.rpc_services.field.enable",
        "settings.rpc_services.field.service_type",
        "settings.rpc_services.field.arguments",
        "settings.rpc_services.field.env_vars",
        "settings.rpc_services.field.default_command",
        "settings.rpc_services.kind.driver",
        "settings.rpc_services.kind.auth_provider",
        "settings.rpc_services.action.delete",
        "settings.rpc_services.action.update",
        "settings.rpc_services.action.create",
    ];

    #[test]
    fn rpc_services_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in RPC_SERVICES_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn rpc_services_section_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.rpc_services.section_title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.rpc_services.section_title", locale = "es");

        assert_eq!(english, "RPC services");
        assert_eq!(spanish, "Servicios RPC");
        assert_ne!(english, spanish);
    }

    #[test]
    fn rpc_services_kind_labels_resolve_for_driver_and_auth_provider() {
        let driver_en = dbflux_i18n::t!("settings.rpc_services.kind.driver", locale = "en");
        let auth_provider_en =
            dbflux_i18n::t!("settings.rpc_services.kind.auth_provider", locale = "en");
        let driver_es = dbflux_i18n::t!("settings.rpc_services.kind.driver", locale = "es");
        let auth_provider_es =
            dbflux_i18n::t!("settings.rpc_services.kind.auth_provider", locale = "es");

        assert_eq!(driver_en, "Driver");
        assert_eq!(auth_provider_en, "Auth Provider");
        assert_eq!(driver_es, "Driver");
        assert_eq!(auth_provider_es, "Proveedor de autenticación");
        assert_ne!(auth_provider_en, auth_provider_es);
    }

    #[test]
    fn rpc_services_duplicate_socket_id_message_embeds_socket_id() {
        let english = dbflux_i18n::t!(
            "settings.rpc_services.error.duplicate_socket_id",
            id = "svc.sock"
        );

        assert!(english.contains("svc.sock"));
    }
}
