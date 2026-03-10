use super::SettingsSection;
use super::SettingsSectionId;
use crate::keymap::{KeyChord, Modifiers};
use dbflux_core::{AppConfigStore, ServiceConfig};
use gpui::prelude::*;
use gpui::*;
use gpui_component::dialog::Dialog;
use gpui_component::input::InputState;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ServiceFocus {
    List,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum ServiceFormRow {
    SocketId,
    Command,
    Timeout,
    Enabled,
    Arg(usize),
    AddArg,
    EnvKey(usize),
    AddEnv,
    DeleteButton,
    SaveButton,
}

pub(super) struct ServicesSection {
    pub(super) svc_services: Vec<ServiceConfig>,
    pub(super) svc_config_store: Option<AppConfigStore>,

    pub(super) svc_focus: ServiceFocus,
    pub(super) svc_selected_idx: Option<usize>,
    pub(super) svc_form_cursor: usize,
    pub(super) svc_env_col: usize,
    pub(super) svc_editing_field: bool,

    pub(super) input_socket_id: Entity<InputState>,
    pub(super) input_svc_command: Entity<InputState>,
    pub(super) input_svc_timeout: Entity<InputState>,
    pub(super) svc_enabled: bool,

    pub(super) svc_arg_inputs: Vec<Entity<InputState>>,
    pub(super) svc_env_key_inputs: Vec<Entity<InputState>>,
    pub(super) svc_env_value_inputs: Vec<Entity<InputState>>,

    pub(super) editing_svc_idx: Option<usize>,
    pub(super) pending_delete_svc_idx: Option<usize>,
    pub(super) content_focused: bool,
}

impl ServicesSection {
    pub(super) fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_socket_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("my-driver.sock"));
        let input_svc_command =
            cx.new(|cx| InputState::new(window, cx).placeholder("dbflux-driver-host"));
        let input_svc_timeout = cx.new(|cx| InputState::new(window, cx).placeholder("5000"));

        let mut section = Self {
            svc_services: Vec::new(),
            svc_config_store: None,
            svc_focus: ServiceFocus::List,
            svc_selected_idx: None,
            svc_form_cursor: 0,
            svc_env_col: 0,
            svc_editing_field: false,
            input_socket_id,
            input_svc_command,
            input_svc_timeout,
            svc_enabled: true,
            svc_arg_inputs: Vec::new(),
            svc_env_key_inputs: Vec::new(),
            svc_env_value_inputs: Vec::new(),
            editing_svc_idx: None,
            pending_delete_svc_idx: None,
            content_focused: false,
        };

        section.load_services();
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

        let chord = KeyChord::from_gpui(&event.keystroke);

        if self.svc_focus == ServiceFocus::Form && self.svc_editing_field {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_editing_field = false;
                    cx.notify();
                }
                ("enter", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.svc_move_down();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.svc_tab_next();
                    self.svc_focus_current_field(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.svc_editing_field = false;
                    self.svc_tab_prev();
                    self.svc_focus_current_field(window, cx);
                    cx.notify();
                }
                _ => {}
            }

            return;
        }

        match self.svc_focus {
            ServiceFocus::List => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_next_profile();
                    self.svc_load_selected_profile(window, cx);
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_move_prev_profile();
                    self.svc_load_selected_profile(window, cx);
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
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.svc_selected_idx = None;
                    self.svc_load_selected_profile(window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    if !self.svc_services.is_empty() {
                        self.svc_selected_idx = Some(self.svc_services.len() - 1);
                        self.svc_load_selected_profile(window, cx);
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
                ("h", modifiers) | ("left", modifiers) if modifiers == Modifiers::none() => {
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
}

impl Render for ServicesSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let show_svc_delete = self.pending_delete_svc_idx.is_some();
        let svc_delete_name = self
            .pending_delete_svc_idx
            .and_then(|idx| self.svc_services.get(idx))
            .map(|service| service.socket_id.clone())
            .unwrap_or_default();

        div()
            .size_full()
            .child(self.render_services_section(cx))
            .when(show_svc_delete, |element| {
                let entity = cx.entity().clone();
                let entity_cancel = entity.clone();

                element.child(
                    Dialog::new(window, cx)
                        .title("Delete Service")
                        .confirm()
                        .on_ok(move |_, window, cx| {
                            entity.update(cx, |section, cx| {
                                section.confirm_delete_service(window, cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            entity_cancel.update(cx, |section, cx| {
                                section.cancel_delete_service(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(format!(
                            "Are you sure you want to delete \"{}\"?",
                            svc_delete_name
                        ))),
                )
            })
    }
}
