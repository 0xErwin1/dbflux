use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::FormSection;
use super::section_trait::SectionFocusEvent;
use dbflux_app::keymap::Modifiers;
use dbflux_components::controls::InputState;
use dbflux_components::modals::{ConfirmOutcome, ConfirmRequest, ModalConfirm, ModalVariant};
use dbflux_core::{RpcServiceKind, ServiceConfig};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use gpui::prelude::*;
use gpui::*;

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum ServiceFocus {
    List,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum ServiceFormRow {
    SocketId,
    Command,
    Timeout,
    Kind,
    Enabled,
    Arg(usize),
    #[allow(dead_code)]
    ArgDelete(usize),
    AddArg,
    EnvKey(usize),
    #[allow(dead_code)]
    EnvValue(usize),
    #[allow(dead_code)]
    EnvDelete(usize),
    AddEnv,
    DeleteButton,
    SaveButton,
}

pub(super) struct ServicesSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) svc_services: Vec<ServiceConfig>,

    pub(super) svc_focus: ServiceFocus,
    pub(super) svc_selected_idx: Option<usize>,
    pub(super) svc_list_scroll_handle: ScrollHandle,
    pub(super) svc_pending_scroll_idx: Option<usize>,
    pub(super) svc_form_cursor: usize,
    pub(super) svc_env_col: usize,
    pub(super) svc_editing_field: bool,
    pub(super) switching_input: bool,

    pub(super) input_socket_id: Entity<InputState>,
    pub(super) input_svc_command: Entity<InputState>,
    pub(super) input_svc_timeout: Entity<InputState>,
    pub(super) svc_kind: RpcServiceKind,
    pub(super) svc_enabled: bool,

    pub(super) svc_arg_inputs: Vec<Entity<InputState>>,
    pub(super) svc_env_key_inputs: Vec<Entity<InputState>>,
    pub(super) svc_env_value_inputs: Vec<Entity<InputState>>,

    pub(super) editing_svc_idx: Option<usize>,
    pub(super) pending_delete_svc_idx: Option<usize>,
    pub(super) content_focused: bool,

    /// Confirmation of the service delete, opened by `render` while a delete
    /// is pending.
    delete_confirm: Entity<ModalConfirm>,
    /// Outcome of `delete_confirm`, drained in `render` where a `Window` is
    /// available (the outcome subscription recorded it without one).
    pending_delete_confirm_resolution: Option<ConfirmOutcome>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for ServicesSection {}

impl ServicesSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_socket_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("my-driver.sock"));
        let input_svc_command =
            cx.new(|cx| InputState::new(window, cx).placeholder("dbflux-driver-host"));
        let input_svc_timeout = cx.new(|cx| InputState::new(window, cx).placeholder("5000"));

        let delete_confirm = cx.new(ModalConfirm::new);
        let delete_confirm_subscription =
            cx.subscribe(&delete_confirm, |this, _, outcome: &ConfirmOutcome, cx| {
                this.pending_delete_confirm_resolution = Some(*outcome);
                cx.notify();
            });

        let mut section = Self {
            app_state,
            svc_services: Vec::new(),
            svc_focus: ServiceFocus::List,
            svc_selected_idx: None,
            svc_list_scroll_handle: ScrollHandle::new(),
            svc_pending_scroll_idx: None,
            svc_form_cursor: 0,
            svc_env_col: 0,
            svc_editing_field: false,
            switching_input: false,
            input_socket_id,
            input_svc_command,
            input_svc_timeout,
            svc_kind: RpcServiceKind::Driver,
            svc_enabled: true,
            svc_arg_inputs: Vec::new(),
            svc_env_key_inputs: Vec::new(),
            svc_env_value_inputs: Vec::new(),
            editing_svc_idx: None,
            pending_delete_svc_idx: None,
            content_focused: false,
            delete_confirm,
            pending_delete_confirm_resolution: None,
            _subscriptions: vec![delete_confirm_subscription],
        };

        let runtime = section.app_state.read(cx).storage_runtime();
        section.load_services(runtime);
        section
    }
}

impl SettingsSection for ServicesSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Services
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.pending_delete_svc_idx.is_some() || !self.content_focused {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match self.svc_focus {
            ServiceFocus::List => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_next_profile();
                    self.svc_load_selected_profile(window, cx);
                    self.svc_pending_scroll_idx = self.svc_selected_idx;
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_prev_profile();
                    self.svc_load_selected_profile(window, cx);
                    self.svc_pending_scroll_idx = self.svc_selected_idx;
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                    if modifiers == Modifiers::none() =>
                {
                    self.svc_enter_form(window, cx);
                    cx.notify();
                }
                ("d", modifiers) if modifiers == Modifiers::none() => {
                    if let Some(idx) = self.svc_selected_idx {
                        self.request_delete_service(idx, cx);
                    }
                }
                ("n", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_selected_idx = None;
                    self.svc_load_selected_profile(window, cx);
                    self.svc_enter_form(window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_selected_idx = None;
                    self.svc_load_selected_profile(window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    if !self.svc_services.is_empty() {
                        self.svc_selected_idx = Some(self.svc_services.len() - 1);
                        self.svc_load_selected_profile(window, cx);
                        self.svc_pending_scroll_idx = self.svc_selected_idx;
                    }
                    cx.notify();
                }
                _ => {}
            },
            ServiceFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_exit_form(window, cx);
                    cx.notify();
                }
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_down();
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_up();
                    cx.notify();
                }
                ("h", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("left", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_left();
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_right();
                    cx.notify();
                }
                ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_activate_current_field(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_tab_next();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.svc_tab_prev();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_first();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    self.svc_move_last();
                    cx.notify();
                }
                _ => {}
            },
        }
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.svc_editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_svc_changes(cx)
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_service_footer_actions(cx))
    }

    fn render_footer_leading_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.render_service_footer_leading_actions(cx)
    }

    fn save_from_shortcut(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.save_service(window, cx);
    }
}

impl FormSection for ServicesSection {
    type Focus = ServiceFocus;
    type FormField = ServiceFormRow;

    fn focus_area(&self) -> Self::Focus {
        self.svc_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.svc_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.current_form_row().unwrap_or(ServiceFormRow::SocketId)
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        let rows = self.svc_form_rows();
        if let Some(pos) = rows.iter().position(|f| *f == field) {
            self.svc_form_cursor = pos;
            self.svc_env_col = 0;
        }
    }

    fn editing_field(&self) -> bool {
        self.svc_editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.svc_editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        ServiceFocus::List
    }

    fn form_focus() -> Self::Focus {
        ServiceFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        ServiceFormRow::SocketId
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        self.svc_form_rows().into_iter().map(|f| vec![f]).collect()
    }

    fn is_input_field(field: Self::FormField) -> bool {
        matches!(
            field,
            ServiceFormRow::SocketId
                | ServiceFormRow::Command
                | ServiceFormRow::Timeout
                | ServiceFormRow::Arg(_)
                | ServiceFormRow::EnvKey(_)
                | ServiceFormRow::EnvValue(_)
        )
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.svc_focus_current_field(window, cx);
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.svc_activate_current_field(window, cx);
    }

    fn move_down(&mut self) {
        self.svc_move_down();
    }

    fn move_up(&mut self) {
        self.svc_move_up();
    }

    fn move_left(&mut self) {
        self.svc_move_left();
    }

    fn move_right(&mut self) {
        self.svc_move_right();
    }

    fn move_first(&mut self) {
        self.svc_move_first();
    }

    fn move_last(&mut self) {
        self.svc_move_last();
    }

    fn tab_next(&mut self) {
        self.svc_tab_next();
    }

    fn tab_prev(&mut self) {
        self.svc_tab_prev();
    }
}

impl Render for ServicesSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // A confirmation resolved while it was open; run the outcome here
        // where a `Window` is available (the outcome subscription recorded it
        // without one).
        if let Some(outcome) = self.pending_delete_confirm_resolution.take() {
            match outcome {
                ConfirmOutcome::Confirmed => self.confirm_delete_service(window, cx),
                ConfirmOutcome::Cancelled => self.cancel_delete_service(cx),
            }
        }

        // Open the delete confirmation while a delete is pending; opening
        // only when the request changed keeps later renders from stealing
        // focus back.
        if let Some(idx) = self.pending_delete_svc_idx {
            let name = self
                .svc_services
                .get(idx)
                .map(|service| service.socket_id.clone())
                .unwrap_or_default();
            let body = crate::labels::rpc_services_delete_body(&name);
            let needs_open = !self.delete_confirm.read(cx).is_visible()
                || self.delete_confirm.read(cx).current_body() != Some(body.as_str());
            if needs_open {
                self.delete_confirm.update(cx, |confirm, cx| {
                    confirm.open(
                        ConfirmRequest {
                            title: dbflux_i18n::t!("settings.rpc_services.delete_dialog_title"),
                            body,
                            variant: ModalVariant::Danger,
                            confirm_label: dbflux_i18n::t!("settings.rpc_services.action.delete"),
                        },
                        cx,
                    );
                });
            }
        }

        div()
            .size_full()
            .child(self.render_services_section(cx))
            .child(self.delete_confirm.clone())
    }
}

#[cfg(test)]
mod tests {
    const SERVICES_KEYS: &[&str] = &[
        "settings.rpc_services.delete_dialog_title",
        "settings.rpc_services.delete_dialog.body",
    ];

    #[test]
    fn services_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in SERVICES_KEYS {
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
    fn services_delete_dialog_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.rpc_services.delete_dialog_title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.rpc_services.delete_dialog_title", locale = "es");

        assert_ne!(english, spanish);
    }
}

#[cfg(test)]
mod confirm_tests;
