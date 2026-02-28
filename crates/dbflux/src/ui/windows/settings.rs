mod general;
mod keybindings;
mod render;
mod rpc_services;
mod ssh_tunnels;

use crate::app::AppState;
use crate::keymap::{ContextId, KeyChord, Modifiers};
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::{AppConfigStore, GeneralSettings, ServiceConfig};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::InputState;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq)]
enum SettingsSection {
    General,
    Keybindings,
    SshTunnels,
    Services,
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

#[derive(Clone, Copy, PartialEq)]
enum ServiceFocus {
    List,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum ServiceFormRow {
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

#[derive(Clone, Copy, PartialEq, Debug)]
enum GeneralFormRow {
    // Startup & Session
    RestoreSession,
    ReopenConnections,
    DefaultFocus,
    MaxHistory,
    AutoSaveInterval,

    // Refresh & Background
    DefaultRefreshPolicy,
    DefaultRefreshInterval,
    MaxBackgroundTasks,
    PauseRefreshOnError,
    RefreshOnlyIfVisible,

    // Execution Safety
    ConfirmDangerous,
    RequiresWhere,
    RequiresPreview,
    AllowRedisFlush,

    // Actions
    SaveButton,
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

    // Services section state
    svc_services: Vec<ServiceConfig>,
    svc_config_store: Option<AppConfigStore>,

    svc_focus: ServiceFocus,
    svc_selected_idx: Option<usize>,
    svc_form_cursor: usize,
    svc_env_col: usize,
    svc_editing_field: bool,

    input_socket_id: Entity<InputState>,
    input_svc_command: Entity<InputState>,
    input_svc_timeout: Entity<InputState>,
    svc_enabled: bool,

    svc_arg_inputs: Vec<Entity<InputState>>,
    svc_env_key_inputs: Vec<Entity<InputState>>,
    svc_env_value_inputs: Vec<Entity<InputState>>,

    editing_svc_idx: Option<usize>,
    pending_delete_svc_idx: Option<usize>,

    // General section state
    gen_settings: GeneralSettings,
    gen_form_cursor: usize,
    gen_editing_field: bool,

    input_max_history: Entity<InputState>,
    input_auto_save: Entity<InputState>,
    input_refresh_interval: Entity<InputState>,
    input_max_bg_tasks: Entity<InputState>,

    pending_close_confirm: bool,

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
        let input_ssh_key_passphrase = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("passphrase")
                .masked(true)
        });
        let input_ssh_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("password")
                .masked(true)
        });

        let subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.editing_tunnel_id = None;
            cx.notify();
        });

        // Start with Global context expanded
        let mut keybindings_expanded = HashSet::new();
        keybindings_expanded.insert(ContextId::Global);

        // General inputs
        let gen_settings = app_state.read(cx).general_settings().clone();

        let input_max_history = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("1000");
            s.set_value(&gen_settings.max_history_entries.to_string(), window, cx);
            s
        });
        let input_auto_save = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("2000");
            s.set_value(&gen_settings.auto_save_interval_ms.to_string(), window, cx);
            s
        });
        let input_refresh_interval = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("5");
            s.set_value(
                &gen_settings.default_refresh_interval_secs.to_string(),
                window,
                cx,
            );
            s
        });
        let input_max_bg_tasks = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("8");
            s.set_value(
                &gen_settings.max_concurrent_background_tasks.to_string(),
                window,
                cx,
            );
            s
        });

        // Services inputs
        let input_socket_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("my-driver.sock"));
        let input_svc_command =
            cx.new(|cx| InputState::new(window, cx).placeholder("dbflux-driver-host"));
        let input_svc_timeout = cx.new(|cx| InputState::new(window, cx).placeholder("5000"));

        // Focus the window on creation
        focus_handle.focus(window);

        let mut this = Self {
            app_state,
            active_section: SettingsSection::General,
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

            gen_settings,
            gen_form_cursor: 0,
            gen_editing_field: false,
            input_max_history,
            input_auto_save,
            input_refresh_interval,
            input_max_bg_tasks,

            pending_close_confirm: false,

            _subscriptions: vec![subscription],
        };

        this.load_services();

        let entity = cx.entity().clone();
        window.on_window_should_close(cx, move |_window, cx| {
            let has_changes = entity.read(cx).has_unsaved_changes(cx);
            if has_changes {
                entity.update(cx, |this, cx| {
                    this.pending_close_confirm = true;
                    cx.notify();
                });
                false
            } else {
                true
            }
        });

        this
    }

    fn sidebar_index_for_section(&self, section: SettingsSection) -> usize {
        match section {
            SettingsSection::General => 0,
            SettingsSection::Keybindings => 1,
            SettingsSection::SshTunnels => 2,
            SettingsSection::Services => 3,
            SettingsSection::About => 4,
        }
    }

    fn section_for_sidebar_index(&self, idx: usize) -> SettingsSection {
        match idx {
            0 => SettingsSection::General,
            1 => SettingsSection::Keybindings,
            2 => SettingsSection::SshTunnels,
            3 => SettingsSection::Services,
            4 => SettingsSection::About,
            _ => SettingsSection::General,
        }
    }

    fn sidebar_section_count(&self) -> usize {
        5
    }

    // -- Unsaved-changes detection --

    fn has_unsaved_changes(&self, cx: &App) -> bool {
        self.has_unsaved_general_changes(cx)
            || self.has_unsaved_ssh_changes(cx)
            || self.has_unsaved_svc_changes(cx)
    }

    fn has_unsaved_general_changes(&self, cx: &App) -> bool {
        let saved = self.app_state.read(cx).general_settings();

        // Toggle/enum fields are modified directly on self.gen_settings,
        // so compare them against persisted state.
        if self.gen_settings.restore_session_on_startup != saved.restore_session_on_startup
            || self.gen_settings.reopen_last_connections != saved.reopen_last_connections
            || self.gen_settings.default_focus_on_startup != saved.default_focus_on_startup
            || self.gen_settings.default_refresh_policy != saved.default_refresh_policy
            || self.gen_settings.auto_refresh_pause_on_error != saved.auto_refresh_pause_on_error
            || self.gen_settings.auto_refresh_only_if_visible != saved.auto_refresh_only_if_visible
            || self.gen_settings.confirm_dangerous_queries != saved.confirm_dangerous_queries
            || self.gen_settings.dangerous_requires_where != saved.dangerous_requires_where
            || self.gen_settings.dangerous_requires_preview != saved.dangerous_requires_preview
            || self.gen_settings.allow_redis_flush != saved.allow_redis_flush
        {
            return true;
        }

        // Input fields live in InputState entities and aren't merged into
        // gen_settings until save — compare trimmed text against saved numbers.
        let history_val = self.input_max_history.read(cx).value().trim().to_string();
        if history_val != saved.max_history_entries.to_string() {
            return true;
        }

        let auto_save_val = self.input_auto_save.read(cx).value().trim().to_string();
        if auto_save_val != saved.auto_save_interval_ms.to_string() {
            return true;
        }

        let refresh_val = self.input_refresh_interval.read(cx).value().trim().to_string();
        if refresh_val != saved.default_refresh_interval_secs.to_string() {
            return true;
        }

        let bg_tasks_val = self.input_max_bg_tasks.read(cx).value().trim().to_string();
        if bg_tasks_val != saved.max_concurrent_background_tasks.to_string() {
            return true;
        }

        false
    }

    fn has_unsaved_ssh_changes(&self, cx: &App) -> bool {
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
                (
                    SshAuthSelection::PrivateKey,
                    dbflux_core::SshAuthMethod::PrivateKey { .. },
                ) => {
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

    fn has_unsaved_svc_changes(&self, cx: &App) -> bool {
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
            let mut saved_env: Vec<(String, String)> =
                saved.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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

    fn save_all_and_close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_unsaved_general_changes(cx) {
            self.save_general_settings(window, cx);

            // save_general_settings returns early on validation failure — check
            // whether it's still dirty to detect that case.
            if self.has_unsaved_general_changes(cx) {
                return;
            }
        }

        if self.has_unsaved_ssh_changes(cx) {
            self.save_tunnel(window, cx);
            if self.has_unsaved_ssh_changes(cx) {
                return;
            }
        }

        if self.has_unsaved_svc_changes(cx) {
            self.save_service(window, cx);
            if self.has_unsaved_svc_changes(cx) {
                return;
            }
        }

        window.remove_window();
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

        if self.pending_delete_tunnel_id.is_some()
            || self.pending_delete_svc_idx.is_some()
            || self.pending_close_confirm
        {
            return;
        }

        // General: editing input mode
        if self.active_section == SettingsSection::General && self.gen_editing_field {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    self.gen_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    self.gen_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    self.gen_move_up();
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        // General: form navigation
        if self.active_section == SettingsSection::General
            && self.focus_area == SettingsFocus::Content
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("j", m) | ("down", m) if m == Modifiers::none() => {
                    self.gen_move_down();
                    cx.notify();
                    return;
                }
                ("k", m) | ("up", m) if m == Modifiers::none() => {
                    self.gen_move_up();
                    cx.notify();
                    return;
                }
                ("enter", m) | ("space", m) if m == Modifiers::none() => {
                    self.gen_activate_current_field(window, cx);
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
                ("g", m) if m == Modifiers::none() => {
                    self.gen_form_cursor = 0;
                    cx.notify();
                    return;
                }
                ("g", m) if m == Modifiers::shift() => {
                    let count = self.gen_form_rows().len();
                    if count > 0 {
                        self.gen_form_cursor = count - 1;
                    }
                    cx.notify();
                    return;
                }
                _ => {}
            }
        }

        // Services: editing input mode
        if self.active_section == SettingsSection::Services
            && self.svc_focus == ServiceFocus::Form
            && self.svc_editing_field
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    self.svc_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    self.svc_tab_next();
                    self.svc_focus_current_field(window, cx);
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    self.svc_tab_prev();
                    self.svc_focus_current_field(window, cx);
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        // Services: list and form navigation
        if self.active_section == SettingsSection::Services
            && self.focus_area == SettingsFocus::Content
        {
            match self.svc_focus {
                ServiceFocus::List => match (chord.key.as_str(), chord.modifiers) {
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.svc_move_next_profile();
                        self.svc_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.svc_move_prev_profile();
                        self.svc_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) | ("enter", m) if m == Modifiers::none() => {
                        self.svc_enter_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("d", m) if m == Modifiers::none() => {
                        if let Some(idx) = self.svc_selected_idx {
                            self.request_delete_service(idx, cx);
                        }
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.svc_selected_idx = None;
                        self.svc_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        if !self.svc_services.is_empty() {
                            self.svc_selected_idx = Some(self.svc_services.len() - 1);
                            self.svc_load_selected_profile(window, cx);
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
                ServiceFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                    ("escape", m) if m == Modifiers::none() => {
                        self.svc_exit_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.svc_move_down();
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.svc_move_up();
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.svc_move_left();
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) if m == Modifiers::none() => {
                        self.svc_move_right();
                        cx.notify();
                        return;
                    }
                    ("enter", m) | ("space", m) if m == Modifiers::none() => {
                        self.svc_activate_current_field(window, cx);
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::none() => {
                        self.svc_tab_next();
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::shift() => {
                        self.svc_tab_prev();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.svc_move_first();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        self.svc_move_last();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
            }
        }

        // SSH: editing input mode
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
