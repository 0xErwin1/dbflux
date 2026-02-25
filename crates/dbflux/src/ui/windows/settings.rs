mod keybindings;
mod render;
mod ssh_tunnels;

use crate::app::AppState;
use crate::keymap::{ContextId, KeyChord, Modifiers};
use crate::ui::windows::ssh_shared::SshAuthSelection;
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::InputState;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq)]
enum SettingsSection {
    Keybindings,
    SshTunnels,
    About,
}

#[derive(Clone, Copy, PartialEq)]
enum SettingsFocus {
    Sidebar,
    Content,
}

/// Represents the currently selected item in the keybindings list
#[derive(Clone, Copy, PartialEq, Debug)]
enum KeybindingsSelection {
    /// A context header row (e.g., "Global", "Sidebar")
    Context(usize),
    /// A binding row within an expanded context (context_idx, binding_idx)
    Binding(usize, usize),
}

impl KeybindingsSelection {
    fn context_idx(&self) -> usize {
        match self {
            Self::Context(idx) | Self::Binding(idx, _) => *idx,
        }
    }
}

enum KeybindingsListItem {
    ContextHeader {
        context: ContextId,
        ctx_idx: usize,
        is_expanded: bool,
        is_selected: bool,
        binding_count: usize,
    },
    Binding {
        chord: KeyChord,
        cmd_name: String,
        is_inherited: bool,
        is_selected: bool,
        ctx_idx: usize,
        binding_idx: usize,
    },
}

#[derive(Clone, Copy, PartialEq)]
enum SshFocus {
    ProfileList,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum SshFormField {
    Name,
    Host,
    Port,
    User,
    AuthPrivateKey,
    AuthPassword,
    KeyPath,
    KeyBrowse,
    Passphrase,
    Password,
    SaveSecret,
    TestButton,
    SaveButton,
    DeleteButton,
}

#[derive(Clone, Copy, PartialEq, Debug, Default)]
enum SshTestStatus {
    #[default]
    None,
    Testing,
    Success,
    Failed,
}

pub struct SettingsWindow {
    app_state: Entity<AppState>,
    active_section: SettingsSection,
    focus_area: SettingsFocus,
    focus_handle: FocusHandle,

    // Keybindings section state
    keybindings_filter: Entity<InputState>,
    keybindings_expanded: HashSet<ContextId>,
    keybindings_selection: KeybindingsSelection,
    keybindings_editing_filter: bool,
    keybindings_scroll_handle: ScrollHandle,
    keybindings_pending_scroll: Option<usize>,

    // SSH Tunnels section state
    editing_tunnel_id: Option<Uuid>,
    input_tunnel_name: Entity<InputState>,
    input_ssh_host: Entity<InputState>,
    input_ssh_port: Entity<InputState>,
    input_ssh_user: Entity<InputState>,
    input_ssh_key_path: Entity<InputState>,
    input_ssh_key_passphrase: Entity<InputState>,
    input_ssh_password: Entity<InputState>,
    ssh_auth_method: SshAuthSelection,
    form_save_secret: bool,
    show_ssh_passphrase: bool,
    show_ssh_password: bool,

    // SSH navigation state
    ssh_focus: SshFocus,
    ssh_selected_idx: Option<usize>,
    ssh_form_field: SshFormField,
    ssh_editing_field: bool,

    // SSH test connection state
    ssh_test_status: SshTestStatus,
    ssh_test_error: Option<String>,

    pending_ssh_key_path: Option<String>,
    pending_delete_tunnel_id: Option<Uuid>,
    _subscriptions: Vec<Subscription>,
}

impl SettingsWindow {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();

        let keybindings_filter =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter keybindings..."));

        let input_tunnel_name = cx.new(|cx| InputState::new(window, cx).placeholder("Tunnel name"));
        let input_ssh_host = cx.new(|cx| InputState::new(window, cx).placeholder("hostname"));
        let input_ssh_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("22")
                .default_value("22")
        });
        let input_ssh_user = cx.new(|cx| InputState::new(window, cx).placeholder("username"));
        let input_ssh_key_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("~/.ssh/id_rsa"));
        let input_ssh_key_passphrase =
            cx.new(|cx| InputState::new(window, cx).placeholder("passphrase").masked(true));
        let input_ssh_password =
            cx.new(|cx| InputState::new(window, cx).placeholder("password").masked(true));

        let subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.editing_tunnel_id = None;
            cx.notify();
        });

        // Start with Global context expanded
        let mut keybindings_expanded = HashSet::new();
        keybindings_expanded.insert(ContextId::Global);

        // Focus the window on creation
        focus_handle.focus(window);

        Self {
            app_state,
            active_section: SettingsSection::Keybindings,
            focus_area: SettingsFocus::Sidebar,
            focus_handle,

            keybindings_filter,
            keybindings_expanded,
            keybindings_selection: KeybindingsSelection::Context(0),
            keybindings_editing_filter: false,
            keybindings_scroll_handle: ScrollHandle::new(),
            keybindings_pending_scroll: None,

            editing_tunnel_id: None,
            input_tunnel_name,
            input_ssh_host,
            input_ssh_port,
            input_ssh_user,
            input_ssh_key_path,
            input_ssh_key_passphrase,
            input_ssh_password,
            ssh_auth_method: SshAuthSelection::PrivateKey,
            form_save_secret: false,
            show_ssh_passphrase: false,
            show_ssh_password: false,

            ssh_focus: SshFocus::ProfileList,
            ssh_selected_idx: None,
            ssh_form_field: SshFormField::Name,
            ssh_editing_field: false,

            ssh_test_status: SshTestStatus::None,
            ssh_test_error: None,

            pending_ssh_key_path: None,
            pending_delete_tunnel_id: None,
            _subscriptions: vec![subscription],
        }
    }

    fn sidebar_index_for_section(&self, section: SettingsSection) -> usize {
        match section {
            SettingsSection::Keybindings => 0,
            SettingsSection::SshTunnels => 1,
            SettingsSection::About => 2,
        }
    }

    fn section_for_sidebar_index(&self, idx: usize) -> SettingsSection {
        match idx {
            0 => SettingsSection::Keybindings,
            1 => SettingsSection::SshTunnels,
            2 => SettingsSection::About,
            _ => SettingsSection::Keybindings,
        }
    }

    fn sidebar_section_count(&self) -> usize {
        3
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let chord = KeyChord::from_gpui(&event.keystroke);

        if self.keybindings_editing_filter {
            if chord.key == "escape" && chord.modifiers == Modifiers::none() {
                self.keybindings_editing_filter = false;
                self.focus_handle.focus(window);
                cx.notify();
            }
            return;
        }

        if self.active_section == SettingsSection::SshTunnels
            && self.ssh_focus == SshFocus::Form
            && self.ssh_editing_field
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    self.ssh_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    self.ssh_tab_next();
                    self.ssh_focus_current_field(window, cx);
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    self.ssh_tab_prev();
                    self.ssh_focus_current_field(window, cx);
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        if self.active_section == SettingsSection::SshTunnels
            && self.focus_area == SettingsFocus::Content
        {
            match self.ssh_focus {
                SshFocus::ProfileList => match (chord.key.as_str(), chord.modifiers) {
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.ssh_move_next_profile(cx);
                        self.ssh_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.ssh_move_prev_profile(cx);
                        self.ssh_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) | ("enter", m) if m == Modifiers::none() => {
                        self.ssh_enter_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("d", m) if m == Modifiers::none() => {
                        if let Some(idx) = self.ssh_selected_idx {
                            let tunnels = {
                                let state = self.app_state.read(cx);
                                state.ssh_tunnels().to_vec()
                            };
                            if let Some(tunnel) = tunnels.get(idx) {
                                self.request_delete_tunnel(tunnel.id, cx);
                            }
                        }
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.ssh_selected_idx = None;
                        self.ssh_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        let count = self.ssh_tunnel_count(cx);
                        if count > 0 {
                            self.ssh_selected_idx = Some(count - 1);
                            self.ssh_load_selected_profile(window, cx);
                        }
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.focus_area = SettingsFocus::Sidebar;
                        cx.notify();
                        return;
                    }
                    ("escape", m) if m == Modifiers::none() => {
                        self.focus_area = SettingsFocus::Sidebar;
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
                SshFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                    ("escape", m) if m == Modifiers::none() => {
                        self.ssh_exit_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.ssh_move_down();
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.ssh_move_up();
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.ssh_move_left();
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) if m == Modifiers::none() => {
                        self.ssh_move_right();
                        cx.notify();
                        return;
                    }
                    ("enter", m) | ("space", m) if m == Modifiers::none() => {
                        self.ssh_activate_current_field(window, cx);
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::none() => {
                        self.ssh_tab_next();
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::shift() => {
                        self.ssh_tab_prev();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.ssh_move_first();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        self.ssh_move_last();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
            }
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("h", m) | ("left", m) if m == Modifiers::none() => {
                self.focus_area = SettingsFocus::Sidebar;
                cx.notify();
            }
            ("l", m) | ("right", m) if m == Modifiers::none() => {
                self.focus_area = SettingsFocus::Content;
                cx.notify();
            }
            ("j", m) | ("down", m) if m == Modifiers::none() => {
                match self.focus_area {
                    SettingsFocus::Sidebar => {
                        let current_idx = self.sidebar_index_for_section(self.active_section);
                        let next_idx = (current_idx + 1) % self.sidebar_section_count();
                        self.active_section = self.section_for_sidebar_index(next_idx);
                    }
                    SettingsFocus::Content => {
                        if self.active_section == SettingsSection::Keybindings {
                            self.keybindings_move_next(cx);
                            self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                        }
                    }
                }
                cx.notify();
            }
            ("k", m) | ("up", m) if m == Modifiers::none() => {
                match self.focus_area {
                    SettingsFocus::Sidebar => {
                        let current_idx = self.sidebar_index_for_section(self.active_section);
                        let prev_idx = if current_idx == 0 {
                            self.sidebar_section_count() - 1
                        } else {
                            current_idx - 1
                        };
                        self.active_section = self.section_for_sidebar_index(prev_idx);
                    }
                    SettingsFocus::Content => {
                        if self.active_section == SettingsSection::Keybindings {
                            self.keybindings_move_prev(cx);
                            self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                        }
                    }
                }
                cx.notify();
            }
            ("g", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Content
                    && self.active_section == SettingsSection::Keybindings
                {
                    let first = self.first_visible_context(cx);
                    self.keybindings_selection = KeybindingsSelection::Context(first);
                    self.keybindings_pending_scroll = Some(0);
                    cx.notify();
                }
            }
            ("g", m) if m == Modifiers::shift() => {
                if self.focus_area == SettingsFocus::Content
                    && self.active_section == SettingsSection::Keybindings
                {
                    let last = self.last_visible_context(cx);
                    let binding_count = self.get_visible_binding_count(last, cx);
                    if binding_count > 0 {
                        self.keybindings_selection =
                            KeybindingsSelection::Binding(last, binding_count - 1);
                    } else {
                        self.keybindings_selection = KeybindingsSelection::Context(last);
                    }
                    self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                    cx.notify();
                }
            }
            ("enter", m) | ("space", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Sidebar {
                    self.focus_area = SettingsFocus::Content;
                } else if self.active_section == SettingsSection::Keybindings
                    && let KeybindingsSelection::Context(ctx_idx) = self.keybindings_selection
                    && let Some(context) = ContextId::all_variants().get(ctx_idx)
                {
                    if self.keybindings_expanded.contains(context) {
                        self.keybindings_expanded.remove(context);
                    } else {
                        self.keybindings_expanded.insert(*context);
                    }
                }
                cx.notify();
            }
            ("/", m) | ("f", m) if m == Modifiers::none() => {
                if self.active_section == SettingsSection::Keybindings {
                    self.keybindings_editing_filter = true;
                    self.keybindings_filter.update(cx, |state, cx| {
                        state.focus(window, cx);
                    });
                    cx.notify();
                }
            }
            ("escape", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Content {
                    self.focus_area = SettingsFocus::Sidebar;
                    cx.notify();
                }
            }

            _ => {}
        }
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for SettingsWindow {}
