mod dirty_state;
mod drivers;
mod form_nav;
mod general;
mod hooks;
mod keybindings;
mod lifecycle;
mod proxies;
mod render;
mod rpc_services;
mod sidebar_nav;
mod ssh_tunnels;

use crate::app::AppState;
use crate::keymap::{ContextId, KeyChord, Modifiers};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::form_renderer::FormRendererState;
use crate::ui::components::tree_nav::{TreeNav, TreeNavAction};
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::{
    AppConfigStore, ConnectionHook, DriverFormDef, DriverKey, DriverMetadata, FormValues,
    GeneralSettings, GlobalOverrides, ServiceConfig,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::InputState;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq)]
enum SettingsSection {
    General,
    Keybindings,
    Proxies,
    SshTunnels,
    Services,
    Hooks,
    Drivers,
    About,
}

#[derive(Clone)]
struct DriverSettingsEntry {
    driver_key: DriverKey,
    metadata: DriverMetadata,
    settings_schema: Option<Arc<DriverFormDef>>,
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
enum ProxyAuthSelection {
    None,
    Basic,
}

#[derive(Clone, Copy, PartialEq)]
enum ProxyFocus {
    ProfileList,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum ProxyFormField {
    Name,
    KindHttp,
    KindHttps,
    KindSocks5,
    Host,
    Port,
    AuthNone,
    AuthBasic,
    Username,
    Password,
    NoProxy,
    Enabled,
    SaveSecret,
    SaveButton,
    DeleteButton,
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
    // Appearance
    Theme,

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

    // Actions
    SaveButton,
}

pub struct SettingsWindow {
    app_state: Entity<AppState>,
    active_section: SettingsSection,
    focus_area: SettingsFocus,
    focus_handle: FocusHandle,

    sidebar_tree: TreeNav,

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

    // Proxy section state
    editing_proxy_id: Option<Uuid>,
    input_proxy_name: Entity<InputState>,
    input_proxy_host: Entity<InputState>,
    input_proxy_port: Entity<InputState>,
    input_proxy_username: Entity<InputState>,
    input_proxy_password: Entity<InputState>,
    input_proxy_no_proxy: Entity<InputState>,
    proxy_kind: dbflux_core::ProxyKind,
    proxy_auth_selection: ProxyAuthSelection,
    proxy_save_secret: bool,
    proxy_enabled: bool,
    show_proxy_password: bool,

    proxy_focus: ProxyFocus,
    proxy_selected_idx: Option<usize>,
    proxy_form_field: ProxyFormField,
    proxy_editing_field: bool,
    pending_delete_proxy_id: Option<Uuid>,

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

    // Hooks section state
    hook_definitions: HashMap<String, ConnectionHook>,
    hook_selected_id: Option<String>,
    editing_hook_id: Option<String>,
    pending_delete_hook_id: Option<String>,
    input_hook_id: Entity<InputState>,
    hook_kind_dropdown: Entity<Dropdown>,
    input_hook_command: Entity<InputState>,
    input_hook_args: Entity<InputState>,
    script_language_dropdown: Entity<Dropdown>,
    script_source_dropdown: Entity<Dropdown>,
    input_hook_script_file_path: Entity<InputState>,
    input_hook_script_content: Entity<InputState>,
    hook_script_content_subscription: Option<Subscription>,
    input_hook_interpreter: Entity<InputState>,
    hook_execution_mode_dropdown: Entity<Dropdown>,
    input_hook_ready_signal: Entity<InputState>,
    input_hook_cwd: Entity<InputState>,
    input_hook_env: Entity<InputState>,
    input_hook_timeout: Entity<InputState>,
    hook_enabled: bool,
    hook_inherit_env: bool,
    hook_lua_logging: bool,
    hook_lua_env_read: bool,
    hook_lua_connection_metadata: bool,
    hook_lua_process_run: bool,
    hook_failure_dropdown: Entity<Dropdown>,

    // Drivers section state
    drv_entries: Vec<DriverSettingsEntry>,
    drv_selected_idx: Option<usize>,
    drv_overrides: HashMap<DriverKey, GlobalOverrides>,
    drv_settings: HashMap<DriverKey, FormValues>,

    drv_editor_dirty: bool,
    drv_loading_selected_editor: bool,

    drv_override_refresh_policy: bool,
    drv_override_refresh_interval: bool,

    drv_refresh_policy_dropdown: Entity<Dropdown>,
    drv_refresh_interval_input: Entity<InputState>,
    drv_confirm_dangerous_dropdown: Entity<Dropdown>,
    drv_requires_where_dropdown: Entity<Dropdown>,
    drv_requires_preview_dropdown: Entity<Dropdown>,

    drv_form_state: FormRendererState,
    drv_form_subscriptions: Vec<Subscription>,

    // General section state
    gen_settings: GeneralSettings,
    gen_form_cursor: usize,
    gen_editing_field: bool,

    dropdown_theme: Entity<Dropdown>,
    dropdown_default_focus: Entity<Dropdown>,
    dropdown_refresh_policy: Entity<Dropdown>,

    input_max_history: Entity<InputState>,
    input_auto_save: Entity<InputState>,
    input_refresh_interval: Entity<InputState>,
    input_max_bg_tasks: Entity<InputState>,

    pending_close_confirm: bool,

    _subscriptions: Vec<Subscription>,
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for SettingsWindow {}

#[derive(Clone, Debug)]
pub enum SettingsEvent {
    OpenScript { path: std::path::PathBuf },
}

impl EventEmitter<SettingsEvent> for SettingsWindow {}
