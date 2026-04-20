use crate::ui::components::toast::ToastExt;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Heights, Radii};
use dbflux_components::controls::{GpuiInput as Input, InputState};
use dbflux_components::primitives::{Icon as PrimitiveIcon, Label};
use dbflux_components::typography::{Body, MonoCaption, MonoLabel, MonoMeta, PanelTitle};
use dbflux_core::ServiceConfig;
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::{Icon, IconName};
use std::collections::HashMap;

use super::layout;
use super::services_section::{ServiceFocus, ServiceFormRow, ServicesSection};

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
            || !self.svc_enabled
    }

    pub(super) fn load_services(&mut self, runtime: &StorageRuntime) {
        let config = dbflux_app::config_loader::load_config(runtime);
        self.svc_services = config.services;
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

        let runtime = self.app_state.read(cx).storage_runtime();
        if let Err(e) = dbflux_app::config_loader::save_services(runtime, &self.svc_services) {
            log::error!("Failed to save services to SQLite: {}", e);
            cx.toast_error(format!("Failed to save config: {}", e), window);
            return;
        }
        cx.toast_info("Service saved. Restart required to apply changes.", window);

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
            log::error!("Failed to save services to SQLite: {}", e);
            cx.toast_error(format!("Failed to save config: {}", e), window);
            return;
        }
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
        let _ = window;
        self.svc_focus = ServiceFocus::List;
        self.svc_editing_field = false;
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
                ServiceFormRow::EnvKey(_) if self.svc_env_col < 2 => {
                    self.svc_env_col += 1;
                }
                ServiceFormRow::Arg(_) if self.svc_env_col < 1 => {
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
            | Some(ServiceFormRow::Timeout)
            | Some(ServiceFormRow::EnvValue(_)) => {
                self.svc_focus_current_field(window, cx);
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
            dbflux_components::composites::section_header(
                "Services",
                "Manage external driver services. Changes require restart.",
                cx,
            ),
            self.render_service_list(&services, editing_idx, cx),
            self.render_service_form(editing_idx, cx),
        )
    }

    fn render_service_list(
        &mut self,
        services: &[ServiceConfig],
        editing_idx: Option<usize>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_list_focused = self.content_focused && self.svc_focus == ServiceFocus::List;
        let is_new_button_focused = is_list_focused && self.svc_selected_idx.is_none();

        if let Some(scroll_idx) = self.svc_pending_scroll_idx.take() {
            self.svc_list_scroll_handle.scroll_to_item(scroll_idx);
        }

        div()
            .w(px(250.0))
            .h_full()
            .min_h_0()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_new_button_focused {
                            theme.primary
                        } else {
                            transparent_black()
                        })
                        .child(
                            Button::new("new-service")
                                .icon(Icon::new(IconName::Plus))
                                .label("New Service")
                                .small()
                                .w_full()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.svc_selected_idx = None;
                                    this.clear_svc_form(window, cx);
                                })),
                        ),
                ),
            )
            .child(
                div()
                    .id("services-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.svc_list_scroll_handle)
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(services.is_empty(), |container| {
                        container.child(div().p_4().child(
                            Body::new("No services configured").color(theme.muted_foreground),
                        ))
                    })
                    .children(services.iter().enumerate().map(|(idx, service)| {
                        let is_selected = editing_idx == Some(idx);
                        let is_focused = is_list_focused && self.svc_selected_idx == Some(idx);
                        let is_disabled = !service.enabled;

                        let subtitle = service
                            .command
                            .as_deref()
                            .filter(|value| !value.is_empty())
                            .unwrap_or("(default)");

                        div()
                            .id(SharedString::from(format!("svc-item-{}", idx)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .bg(theme.list_even)
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_focused && !is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .when(is_selected, |div| div.bg(theme.secondary))
                            .hover(|div| div.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.svc_selected_idx = Some(idx);
                                this.edit_service(idx, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(div().mt(px(2.0)).child(
                                        PrimitiveIcon::new(AppIcon::Plug).size(px(16.0)).muted(),
                                    ))
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(if is_disabled {
                                                        MonoLabel::new(service.socket_id.clone())
                                                            .color(theme.muted_foreground)
                                                            .into_any_element()
                                                    } else {
                                                        MonoLabel::new(service.socket_id.clone())
                                                            .into_any_element()
                                                    })
                                                    .when(is_disabled, |container| {
                                                        container.child(
                                                            div()
                                                                .px_1()
                                                                .rounded(px(3.0))
                                                                .bg(theme.secondary)
                                                                .child(MonoCaption::new(
                                                                    "Disabled",
                                                                )),
                                                        )
                                                    }),
                                            )
                                            .child(MonoMeta::new(subtitle.to_string())),
                                    ),
                            )
                    })),
            )
    }

    fn render_service_form(
        &self,
        editing_idx: Option<usize>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;
        let is_form_focused = self.content_focused && self.svc_focus == ServiceFocus::Form;
        let cursor = self.svc_form_cursor;
        let rows = self.svc_form_rows();

        let title = if editing_idx.is_some() {
            "Edit Service"
        } else {
            "New Service"
        };

        let is_row_focused = |row: ServiceFormRow| -> bool {
            is_form_focused && rows.get(cursor).copied() == Some(row)
        };

        layout::sticky_form_shell(
            PanelTitle::new(title),
            div()
                .flex()
                .flex_col()
                .gap_4()
                .child(self.render_svc_input_field(
                    "Socket ID",
                    &self.input_socket_id,
                    is_row_focused(ServiceFormRow::SocketId),
                    primary,
                    ServiceFormRow::SocketId,
                    cx,
                ))
                .child(self.render_svc_input_field(
                    "Command",
                    &self.input_svc_command,
                    is_row_focused(ServiceFormRow::Command),
                    primary,
                    ServiceFormRow::Command,
                    cx,
                ))
                .child(self.render_svc_input_field(
                    "Startup Timeout (ms)",
                    &self.input_svc_timeout,
                    is_row_focused(ServiceFormRow::Timeout),
                    primary,
                    ServiceFormRow::Timeout,
                    cx,
                ))
                .child(self.render_svc_enabled_checkbox(
                    is_row_focused(ServiceFormRow::Enabled),
                    primary,
                    cx,
                ))
                .child(self.render_svc_args_section(is_form_focused, cursor, &rows, primary, cx))
                .child(self.render_svc_env_section(is_form_focused, cursor, &rows, primary, cx)),
            None,
            &theme,
        )
    }

    pub(super) fn render_service_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let is_form_focused = self.content_focused && self.svc_focus == ServiceFocus::Form;
        let cursor = self.svc_form_cursor;
        let rows = self.svc_form_rows();
        let is_row_focused = |row: ServiceFormRow| -> bool {
            is_form_focused && rows.get(cursor).copied() == Some(row)
        };

        div()
            .flex()
            .items_center()
            .gap_3()
            .when(self.editing_svc_idx.is_some(), |container| {
                container.child(layout::footer_action_frame(
                    is_row_focused(ServiceFormRow::DeleteButton),
                    theme.primary,
                    Button::new("delete-service")
                        .label("Delete")
                        .small()
                        .danger()
                        .w_full()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            if let Some(idx) = this.editing_svc_idx {
                                this.request_delete_service(idx, cx);
                            }
                        })),
                ))
            })
            .child(layout::footer_action_frame(
                is_row_focused(ServiceFormRow::SaveButton),
                theme.primary,
                Button::new("save-service")
                    .label(if self.editing_svc_idx.is_some() {
                        "Update"
                    } else {
                        "Create"
                    })
                    .small()
                    .primary()
                    .w_full()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save_service(window, cx);
                    })),
            ))
            .into_any_element()
    }

    fn render_svc_input_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        row: ServiceFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()))
            .child(
                div()
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(if is_focused {
                        primary
                    } else {
                        transparent_black()
                    })
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.svc_focus = ServiceFocus::Form;
                            let rows = this.svc_form_rows();
                            if let Some(pos) = rows.iter().position(|candidate| *candidate == row) {
                                this.svc_form_cursor = pos;
                                this.svc_env_col = 0;
                            }
                            this.svc_focus_current_field(window, cx);
                            cx.notify();
                        }),
                    )
                    .child(Input::new(input).small()),
            )
    }

    fn render_svc_enabled_checkbox(
        &self,
        is_focused: bool,
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .px_2()
            .py_1()
            .rounded(px(4.0))
            .border_1()
            .border_color(if is_focused {
                primary
            } else {
                transparent_black()
            })
            .child(
                Checkbox::new("svc-enabled")
                    .checked(self.svc_enabled)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.svc_enabled = *checked;
                        cx.notify();
                    })),
            )
            .child(Body::new("Enable this service"))
    }

    fn render_svc_args_section(
        &self,
        is_form_focused: bool,
        cursor: usize,
        rows: &[ServiceFormRow],
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let is_add_focused =
            is_form_focused && rows.get(cursor).copied() == Some(ServiceFormRow::AddArg);

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Label::new("Arguments"))
            .children(self.svc_arg_inputs.iter().enumerate().map(|(idx, input)| {
                let is_row_at_cursor =
                    is_form_focused && rows.get(cursor).copied() == Some(ServiceFormRow::Arg(idx));
                let input_focused = is_row_at_cursor && self.svc_env_col == 0;
                let remove_focused = is_row_at_cursor && self.svc_env_col == 1;

                div()
                    .flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .flex_1()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if input_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    this.svc_focus = ServiceFocus::Form;
                                    let rows = this.svc_form_rows();
                                    if let Some(pos) = rows.iter().position(|candidate| {
                                        *candidate == ServiceFormRow::Arg(idx)
                                    }) {
                                        this.svc_form_cursor = pos;
                                        this.svc_env_col = 0;
                                    }
                                    this.svc_focus_current_field(window, cx);
                                    cx.notify();
                                }),
                            )
                            .child(Input::new(input).small()),
                    )
                    .child(
                        div()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if remove_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .child(
                                Button::new(SharedString::from(format!("rm-arg-{}", idx)))
                                    .label("x")
                                    .small()
                                    .ghost()
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.remove_arg_row(idx, window, cx);
                                    })),
                            ),
                    )
            }))
            .child(
                div().flex().justify_center().child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_add_focused {
                            primary
                        } else {
                            transparent_black()
                        })
                        .child(
                            div()
                                .id("add-arg")
                                .w(Heights::ICON_LG)
                                .h(Heights::ICON_LG)
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.primary)
                                .hover(|div| div.opacity(0.8))
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.add_arg_row(window, cx);
                                    }),
                                )
                                .child(
                                    PrimitiveIcon::new(AppIcon::Plus)
                                        .size(Heights::ICON_SM)
                                        .color(theme.primary_foreground),
                                ),
                        ),
                ),
            )
    }

    fn render_svc_env_section(
        &self,
        is_form_focused: bool,
        cursor: usize,
        rows: &[ServiceFormRow],
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let is_add_focused =
            is_form_focused && rows.get(cursor).copied() == Some(ServiceFormRow::AddEnv);

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Label::new("Environment Variables"))
            .children(
                self.svc_env_key_inputs
                    .iter()
                    .zip(self.svc_env_value_inputs.iter())
                    .enumerate()
                    .map(|(idx, (key_input, value_input))| {
                        let is_row_at_cursor = is_form_focused
                            && rows.get(cursor).copied() == Some(ServiceFormRow::EnvKey(idx));
                        let key_focused = is_row_at_cursor && self.svc_env_col == 0;
                        let value_focused = is_row_at_cursor && self.svc_env_col == 1;
                        let remove_focused = is_row_at_cursor && self.svc_env_col == 2;

                        div()
                            .flex()
                            .gap_2()
                            .items_center()
                            .child(
                                div()
                                    .flex_1()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if key_focused {
                                        primary
                                    } else {
                                        transparent_black()
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, window, cx| {
                                            this.svc_focus = ServiceFocus::Form;
                                            let rows = this.svc_form_rows();
                                            if let Some(pos) = rows.iter().position(|candidate| {
                                                *candidate == ServiceFormRow::EnvKey(idx)
                                            }) {
                                                this.svc_form_cursor = pos;
                                                this.svc_env_col = 0;
                                            }
                                            this.svc_focus_current_field(window, cx);
                                            cx.notify();
                                        }),
                                    )
                                    .child(Input::new(key_input).small()),
                            )
                            .child(MonoCaption::new("="))
                            .child(
                                div()
                                    .flex_1()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if value_focused {
                                        primary
                                    } else {
                                        transparent_black()
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, window, cx| {
                                            this.svc_focus = ServiceFocus::Form;
                                            let rows = this.svc_form_rows();
                                            if let Some(pos) = rows.iter().position(|candidate| {
                                                *candidate == ServiceFormRow::EnvKey(idx)
                                            }) {
                                                this.svc_form_cursor = pos;
                                                this.svc_env_col = 1;
                                            }
                                            this.svc_focus_current_field(window, cx);
                                            cx.notify();
                                        }),
                                    )
                                    .child(Input::new(value_input).small()),
                            )
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if remove_focused {
                                        primary
                                    } else {
                                        transparent_black()
                                    })
                                    .child(
                                        Button::new(SharedString::from(format!("rm-env-{}", idx)))
                                            .label("x")
                                            .small()
                                            .ghost()
                                            .on_click(cx.listener(move |this, _, window, cx| {
                                                this.remove_env_row(idx, window, cx);
                                            })),
                                    ),
                            )
                    }),
            )
            .child(
                div().flex().justify_center().child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_add_focused {
                            primary
                        } else {
                            transparent_black()
                        })
                        .child(
                            div()
                                .id("add-env")
                                .w(Heights::ICON_LG)
                                .h(Heights::ICON_LG)
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.primary)
                                .hover(|div| div.opacity(0.8))
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.add_env_row(window, cx);
                                    }),
                                )
                                .child(
                                    PrimitiveIcon::new(AppIcon::Plus)
                                        .size(Heights::ICON_SM)
                                        .color(theme.primary_foreground),
                                ),
                        ),
                ),
            )
    }
}
