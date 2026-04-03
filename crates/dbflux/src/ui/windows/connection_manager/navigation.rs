use crate::keymap::{key_chord_from_gpui, Command, ContextId};
use crate::platform;
use crate::ui::components::dropdown::DropdownItem;
use crate::ui::windows::settings::{SettingsSectionId, SettingsWindow};
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::FormFieldKind;
use gpui::*;
use gpui_component::Root;

use super::{
    AccessTabMode, ActiveTab, ConnectionManagerWindow, DismissEvent, DriverFocus, EditState,
    FormFocus, View,
};

fn next_active_tab(current: ActiveTab, has_access_tab: bool) -> ActiveTab {
    match current {
        ActiveTab::Main if has_access_tab => ActiveTab::Access,
        ActiveTab::Main => ActiveTab::Settings,
        ActiveTab::Access => ActiveTab::Settings,
        ActiveTab::Settings => ActiveTab::Mcp,
        ActiveTab::Mcp => ActiveTab::Main,
    }
}

fn prev_active_tab(current: ActiveTab, has_access_tab: bool) -> ActiveTab {
    match current {
        ActiveTab::Main => ActiveTab::Mcp,
        ActiveTab::Access => ActiveTab::Main,
        ActiveTab::Settings if has_access_tab => ActiveTab::Access,
        ActiveTab::Settings => ActiveTab::Main,
        ActiveTab::Mcp => ActiveTab::Settings,
    }
}

#[derive(Clone, Copy)]
pub(super) struct SshNavState {
    pub(super) enabled: bool,
    pub(super) has_tunnels: bool,
    pub(super) has_selected_tunnel: bool,
    pub(super) auth_method: SshAuthSelection,
    pub(super) can_save_tunnel: bool,
}

impl SshNavState {
    pub(super) fn new(
        enabled: bool,
        has_tunnels: bool,
        has_selected_tunnel: bool,
        auth_method: SshAuthSelection,
        can_save_tunnel: bool,
    ) -> Self {
        Self {
            enabled,
            has_tunnels,
            has_selected_tunnel,
            auth_method,
            can_save_tunnel,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ProxyNavState {
    pub(super) has_proxies: bool,
    pub(super) has_selected_proxy: bool,
}

impl ProxyNavState {
    pub(super) fn new(has_proxies: bool, has_selected_proxy: bool) -> Self {
        Self {
            has_proxies,
            has_selected_proxy,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct MainNavState {
    /// True for file-based databases (SQLite), false for server-based (PostgreSQL, MySQL, MariaDB)
    pub(super) uses_file_form: bool,
    /// True if the driver has a "Use Connection URI" checkbox option
    pub(super) has_uri_option: bool,
    /// True when "Use Connection URI" is checked (skip disabled individual fields).
    pub(super) uri_mode_active: bool,
    /// True when password source is literal.
    pub(super) password_source_is_literal: bool,
    /// True when save-password action is visible.
    pub(super) can_save_password: bool,
}

impl FormFocus {
    // === Main Tab: Vertical Navigation (j/k) ===

    pub(super) fn down_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            return match self {
                Name => Database,
                Database | FileBrowse => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            };
        }

        if state.has_uri_option && state.uri_mode_active {
            return match self {
                Name => UseUri,
                UseUri => HostValueSource,
                HostValueSource | Host | Port => PasswordValueSource,
                DatabaseValueSource | Database | UserValueSource | User => PasswordValueSource,
                PasswordValueSource | Password | PasswordToggle | PasswordSave => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            };
        }

        if state.has_uri_option {
            return match self {
                Name => UseUri,
                UseUri => HostValueSource,
                HostValueSource | Host | Port => DatabaseValueSource,
                DatabaseValueSource | Database => UserValueSource,
                UserValueSource | User => PasswordValueSource,
                PasswordValueSource | Password | PasswordToggle | PasswordSave => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            };
        }

        match self {
            Name => HostValueSource,
            HostValueSource | Host | Port => DatabaseValueSource,
            DatabaseValueSource | Database => UserValueSource,
            UserValueSource | User => PasswordValueSource,
            PasswordValueSource | Password | PasswordToggle | PasswordSave => TestConnection,
            TestConnection => Save,
            Save => Name,
            _ => Name,
        }
    }

    pub(super) fn up_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            return match self {
                Name => Save,
                Database | FileBrowse => Name,
                TestConnection => Database,
                Save => TestConnection,
                _ => Save,
            };
        }

        if state.has_uri_option && state.uri_mode_active {
            return match self {
                Name => Save,
                UseUri => Name,
                HostValueSource | Host | Port => UseUri,
                DatabaseValueSource | Database | UserValueSource | User => HostValueSource,
                PasswordValueSource | Password | PasswordToggle | PasswordSave => HostValueSource,
                TestConnection => PasswordValueSource,
                Save => TestConnection,
                _ => Save,
            };
        }

        if state.has_uri_option {
            return match self {
                Name => Save,
                UseUri => Name,
                HostValueSource | Host | Port => UseUri,
                DatabaseValueSource | Database => HostValueSource,
                UserValueSource | User => DatabaseValueSource,
                PasswordValueSource | Password | PasswordToggle | PasswordSave => UserValueSource,
                TestConnection => PasswordValueSource,
                Save => TestConnection,
                _ => Save,
            };
        }

        match self {
            Name => Save,
            HostValueSource | Host | Port => Name,
            DatabaseValueSource | Database => HostValueSource,
            UserValueSource | User => DatabaseValueSource,
            PasswordValueSource | Password | PasswordToggle | PasswordSave => UserValueSource,
            TestConnection => PasswordValueSource,
            Save => TestConnection,
            _ => Save,
        }
    }

    // === Main Tab: Horizontal Navigation (h/l) ===

    pub(super) fn left_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            return match self {
                FileBrowse => Database,
                Save => TestConnection,
                other => other,
            };
        }

        match self {
            Host => HostValueSource,
            Port => Host,
            Database => DatabaseValueSource,
            User => UserValueSource,
            Password => PasswordValueSource,
            PasswordToggle => Password,
            PasswordSave => {
                if state.password_source_is_literal {
                    PasswordToggle
                } else {
                    Password
                }
            }
            Save => TestConnection,
            other => other,
        }
    }

    pub(super) fn right_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            return match self {
                Database => FileBrowse,
                TestConnection => Save,
                other => other,
            };
        }

        match self {
            HostValueSource => Host,
            Host => {
                if state.uri_mode_active {
                    Host
                } else {
                    Port
                }
            }
            DatabaseValueSource => Database,
            UserValueSource => User,
            PasswordValueSource => Password,
            Password => {
                if state.password_source_is_literal {
                    PasswordToggle
                } else {
                    Password
                }
            }
            PasswordToggle => {
                if state.can_save_password {
                    PasswordSave
                } else {
                    PasswordToggle
                }
            }
            TestConnection => Save,
            other => other,
        }
    }

    // === SSH Tab: Vertical Navigation (j/k) ===

    pub(super) fn down_ssh(self, state: SshNavState) -> Self {
        use FormFocus::*;

        if !state.enabled {
            match self {
                Name => SshEnabled,
                SshEnabled => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else if state.has_selected_tunnel {
            // Read-only mode: tunnel selected, skip editable fields
            match self {
                Name => SshEnabled,
                SshEnabled => {
                    if state.has_tunnels {
                        SshTunnelSelector
                    } else {
                        SshEditInSettings
                    }
                }
                SshTunnelSelector | SshTunnelClear => SshEditInSettings,
                SshEditInSettings => TestSsh,
                TestSsh => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else {
            match self {
                Name => SshEnabled,
                SshEnabled => {
                    if state.has_tunnels {
                        SshTunnelSelector
                    } else {
                        SshHost
                    }
                }
                SshTunnelSelector | SshTunnelClear => SshHost,
                SshHost | SshPort => SshUser,
                SshUser => SshAuthPrivateKey,
                SshAuthPrivateKey | SshAuthPassword => {
                    if state.auth_method == SshAuthSelection::PrivateKey {
                        SshKeyPath
                    } else {
                        SshPassword
                    }
                }
                SshKeyPath | SshKeyBrowse => SshPassphrase,
                SshPassphrase | SshSaveSecret
                    if state.auth_method == SshAuthSelection::PrivateKey =>
                {
                    TestSsh
                }
                SshPassword | SshSaveSecret => TestSsh,
                TestSsh => {
                    if state.can_save_tunnel {
                        SaveAsTunnel
                    } else {
                        TestConnection
                    }
                }
                SaveAsTunnel => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        }
    }

    pub(super) fn up_ssh(self, state: SshNavState) -> Self {
        use FormFocus::*;

        if !state.enabled {
            match self {
                Name => Save,
                SshEnabled => Name,
                TestConnection => SshEnabled,
                Save => TestConnection,
                _ => Name,
            }
        } else if state.has_selected_tunnel {
            // Read-only mode: tunnel selected, skip editable fields
            match self {
                Name => Save,
                SshEnabled => Name,
                SshTunnelSelector | SshTunnelClear => SshEnabled,
                SshEditInSettings => {
                    if state.has_tunnels {
                        SshTunnelSelector
                    } else {
                        SshEnabled
                    }
                }
                TestSsh => SshEditInSettings,
                TestConnection => TestSsh,
                Save => TestConnection,
                _ => Save,
            }
        } else {
            match self {
                Name => Save,
                SshEnabled => Name,
                SshTunnelSelector | SshTunnelClear => SshEnabled,
                SshHost | SshPort => {
                    if state.has_tunnels {
                        SshTunnelSelector
                    } else {
                        SshEnabled
                    }
                }
                SshUser => SshHost,
                SshAuthPrivateKey | SshAuthPassword => SshUser,
                SshKeyPath | SshKeyBrowse => SshAuthPrivateKey,
                SshPassphrase | SshSaveSecret
                    if state.auth_method == SshAuthSelection::PrivateKey =>
                {
                    SshKeyPath
                }
                SshPassword | SshSaveSecret => SshAuthPassword,
                TestSsh | SaveAsTunnel => {
                    if state.auth_method == SshAuthSelection::PrivateKey {
                        SshPassphrase
                    } else {
                        SshPassword
                    }
                }
                TestConnection => {
                    if state.can_save_tunnel {
                        SaveAsTunnel
                    } else {
                        TestSsh
                    }
                }
                Save => TestConnection,
                _ => Save,
            }
        }
    }

    // === SSH Tab: Horizontal Navigation (h/l) ===

    pub(super) fn left_ssh(self, state: SshNavState) -> Self {
        use FormFocus::*;
        if !state.enabled {
            match self {
                Save => TestConnection,
                other => other,
            }
        } else if state.has_selected_tunnel {
            match self {
                SshTunnelClear => SshTunnelSelector,
                Save => TestConnection,
                other => other,
            }
        } else {
            match self {
                SshTunnelClear => SshTunnelSelector,
                SshPort => SshHost,
                SshAuthPassword => SshAuthPrivateKey,
                SshKeyBrowse => SshKeyPath,
                SshSaveSecret => {
                    if state.auth_method == SshAuthSelection::PrivateKey {
                        SshPassphrase
                    } else {
                        SshPassword
                    }
                }
                SaveAsTunnel => TestSsh,
                Save => TestConnection,
                other => other,
            }
        }
    }

    pub(super) fn right_ssh(self, state: SshNavState) -> Self {
        use FormFocus::*;
        if !state.enabled {
            match self {
                TestConnection => Save,
                other => other,
            }
        } else if state.has_selected_tunnel {
            match self {
                SshTunnelSelector if state.has_selected_tunnel => SshTunnelClear,
                TestConnection => Save,
                other => other,
            }
        } else {
            match self {
                SshTunnelSelector if state.has_selected_tunnel => SshTunnelClear,
                SshHost => SshPort,
                SshAuthPrivateKey => SshAuthPassword,
                SshKeyPath => SshKeyBrowse,
                SshPassphrase => SshSaveSecret,
                SshPassword => SshSaveSecret,
                TestSsh if state.can_save_tunnel => SaveAsTunnel,
                TestConnection => Save,
                other => other,
            }
        }
    }

    // === Proxy Tab: Vertical Navigation (j/k) ===

    pub(super) fn down_proxy(self, state: ProxyNavState) -> Self {
        use FormFocus::*;
        if !state.has_proxies {
            match self {
                Name => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else if state.has_selected_proxy {
            match self {
                Name => ProxySelector,
                ProxySelector | ProxyClear => ProxyEditInSettings,
                ProxyEditInSettings => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else {
            match self {
                Name => ProxySelector,
                ProxySelector | ProxyClear => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        }
    }

    pub(super) fn up_proxy(self, state: ProxyNavState) -> Self {
        use FormFocus::*;
        if !state.has_proxies {
            match self {
                Name => Save,
                TestConnection => Name,
                Save => TestConnection,
                _ => Save,
            }
        } else if state.has_selected_proxy {
            match self {
                Name => Save,
                ProxySelector | ProxyClear => Name,
                ProxyEditInSettings => ProxySelector,
                TestConnection => ProxyEditInSettings,
                Save => TestConnection,
                _ => Save,
            }
        } else {
            match self {
                Name => Save,
                ProxySelector | ProxyClear => Name,
                TestConnection => ProxySelector,
                Save => TestConnection,
                _ => Save,
            }
        }
    }

    // === Proxy Tab: Horizontal Navigation (h/l) ===

    pub(super) fn left_proxy(self, state: ProxyNavState) -> Self {
        use FormFocus::*;
        match self {
            ProxyClear if state.has_selected_proxy => ProxySelector,
            Save => TestConnection,
            other => other,
        }
    }

    pub(super) fn right_proxy(self, state: ProxyNavState) -> Self {
        use FormFocus::*;
        match self {
            ProxySelector if state.has_selected_proxy => ProxyClear,
            TestConnection => Save,
            other => other,
        }
    }

    fn normalize_access_focus(self) -> Self {
        match self {
            FormFocus::AccessMethod => FormFocus::Name,
            other => other,
        }
    }

    fn denormalize_access_focus(self) -> Self {
        match self {
            FormFocus::Name => FormFocus::AccessMethod,
            other => other,
        }
    }

    pub(super) fn down_access(
        self,
        mode: AccessTabMode,
        ssh_state: SshNavState,
        proxy_state: ProxyNavState,
    ) -> Self {
        use FormFocus::*;

        match mode {
            AccessTabMode::Direct => match self {
                AccessMethod => SsmAuthProfile,
                SsmAuthProfile => SsmAuthManage,
                SsmAuthManage | SsmAuthLogin | SsmAuthRefresh => TestConnection,
                TestConnection => Save,
                Save => AccessMethod,
                _ => AccessMethod,
            },
            AccessTabMode::ManagedSsm => match self {
                AccessMethod => SsmInstanceIdValueSource,
                SsmInstanceIdValueSource | SsmInstanceId => SsmRegionValueSource,
                SsmRegionValueSource | SsmRegion => SsmRemotePortValueSource,
                SsmRemotePortValueSource | SsmRemotePort => SsmAuthProfile,
                SsmAuthProfile => SsmAuthManage,
                SsmAuthManage | SsmAuthLogin | SsmAuthRefresh => TestConnection,
                TestConnection => Save,
                Save => AccessMethod,
                _ => AccessMethod,
            },
            AccessTabMode::Ssh => self
                .normalize_access_focus()
                .down_ssh(ssh_state)
                .denormalize_access_focus(),
            AccessTabMode::Proxy => self
                .normalize_access_focus()
                .down_proxy(proxy_state)
                .denormalize_access_focus(),
        }
    }

    pub(super) fn up_access(
        self,
        mode: AccessTabMode,
        ssh_state: SshNavState,
        proxy_state: ProxyNavState,
    ) -> Self {
        use FormFocus::*;

        match mode {
            AccessTabMode::Direct => match self {
                AccessMethod => Save,
                SsmAuthProfile => AccessMethod,
                SsmAuthManage | SsmAuthLogin | SsmAuthRefresh => SsmAuthProfile,
                TestConnection => SsmAuthManage,
                Save => TestConnection,
                _ => Save,
            },
            AccessTabMode::ManagedSsm => match self {
                AccessMethod => Save,
                SsmInstanceIdValueSource | SsmInstanceId => AccessMethod,
                SsmRegionValueSource | SsmRegion => SsmInstanceIdValueSource,
                SsmRemotePortValueSource | SsmRemotePort => SsmRegionValueSource,
                SsmAuthProfile => SsmRemotePortValueSource,
                SsmAuthManage | SsmAuthLogin | SsmAuthRefresh => SsmAuthProfile,
                TestConnection => SsmAuthManage,
                Save => TestConnection,
                _ => Save,
            },
            AccessTabMode::Ssh => self
                .normalize_access_focus()
                .up_ssh(ssh_state)
                .denormalize_access_focus(),
            AccessTabMode::Proxy => self
                .normalize_access_focus()
                .up_proxy(proxy_state)
                .denormalize_access_focus(),
        }
    }

    pub(super) fn left_access(
        self,
        mode: AccessTabMode,
        ssh_state: SshNavState,
        proxy_state: ProxyNavState,
    ) -> Self {
        use FormFocus::*;

        match mode {
            AccessTabMode::ManagedSsm => match self {
                SsmInstanceId => SsmInstanceIdValueSource,
                SsmRegion => SsmRegionValueSource,
                SsmRemotePort => SsmRemotePortValueSource,
                SsmAuthManage => SsmAuthProfile,
                SsmAuthLogin => SsmAuthManage,
                SsmAuthRefresh => SsmAuthLogin,
                Save => TestConnection,
                other => other,
            },
            AccessTabMode::Ssh => self
                .normalize_access_focus()
                .left_ssh(ssh_state)
                .denormalize_access_focus(),
            AccessTabMode::Proxy => self
                .normalize_access_focus()
                .left_proxy(proxy_state)
                .denormalize_access_focus(),
            AccessTabMode::Direct => match self {
                SsmAuthManage => SsmAuthProfile,
                SsmAuthLogin => SsmAuthManage,
                SsmAuthRefresh => SsmAuthLogin,
                Save => TestConnection,
                other => other,
            },
        }
    }

    pub(super) fn right_access(
        self,
        mode: AccessTabMode,
        ssh_state: SshNavState,
        proxy_state: ProxyNavState,
    ) -> Self {
        use FormFocus::*;

        match mode {
            AccessTabMode::ManagedSsm => match self {
                SsmInstanceIdValueSource => SsmInstanceId,
                SsmRegionValueSource => SsmRegion,
                SsmRemotePortValueSource => SsmRemotePort,
                SsmAuthProfile => SsmAuthManage,
                SsmAuthManage => SsmAuthLogin,
                SsmAuthLogin => SsmAuthRefresh,
                TestConnection => Save,
                other => other,
            },
            AccessTabMode::Ssh => self
                .normalize_access_focus()
                .right_ssh(ssh_state)
                .denormalize_access_focus(),
            AccessTabMode::Proxy => self
                .normalize_access_focus()
                .right_proxy(proxy_state)
                .denormalize_access_focus(),
            AccessTabMode::Direct => match self {
                SsmAuthProfile => SsmAuthManage,
                SsmAuthManage => SsmAuthLogin,
                SsmAuthLogin => SsmAuthRefresh,
                TestConnection => Save,
                other => other,
            },
        }
    }

    // === Settings Tab: Vertical Navigation (j/k) ===

    pub(super) fn down_settings(self, driver_field_count: u8) -> Self {
        use FormFocus::*;
        match self {
            Name => SettingsRefreshPolicy,
            SettingsRefreshPolicy => SettingsRefreshInterval,
            SettingsRefreshInterval => SettingsConfirmDangerous,
            SettingsConfirmDangerous => SettingsRequiresWhere,
            SettingsRequiresWhere => SettingsRequiresPreview,
            SettingsRequiresPreview => {
                if driver_field_count > 0 {
                    SettingsDriverField(0)
                } else {
                    TestConnection
                }
            }
            SettingsDriverField(idx) => {
                let next = idx + 1;
                if next < driver_field_count {
                    SettingsDriverField(next)
                } else {
                    TestConnection
                }
            }
            TestConnection => Save,
            Save => Name,
            _ => Name,
        }
    }

    pub(super) fn up_settings(self, driver_field_count: u8) -> Self {
        use FormFocus::*;
        match self {
            Name => Save,
            SettingsRefreshPolicy => Name,
            SettingsRefreshInterval => SettingsRefreshPolicy,
            SettingsConfirmDangerous => SettingsRefreshInterval,
            SettingsRequiresWhere => SettingsConfirmDangerous,
            SettingsRequiresPreview => SettingsRequiresWhere,
            SettingsDriverField(0) => SettingsRequiresPreview,
            SettingsDriverField(idx) => SettingsDriverField(idx - 1),
            TestConnection => {
                if driver_field_count > 0 {
                    SettingsDriverField(driver_field_count - 1)
                } else {
                    SettingsRequiresPreview
                }
            }
            Save => TestConnection,
            _ => Save,
        }
    }

    // === Settings Tab: Horizontal Navigation (h/l) ===

    pub(super) fn left_settings(self) -> Self {
        use FormFocus::*;
        match self {
            Save => TestConnection,
            other => other,
        }
    }

    pub(super) fn right_settings(self) -> Self {
        use FormFocus::*;
        match self {
            TestConnection => Save,
            other => other,
        }
    }

    #[allow(dead_code)]
    pub(super) fn is_input_field(self) -> bool {
        use FormFocus::*;
        matches!(
            self,
            Name | Host
                | Port
                | Database
                | User
                | Password
                | SshHost
                | SshPort
                | SshUser
                | SshKeyPath
                | SshPassphrase
                | SshPassword
        )
    }
}

impl ConnectionManagerWindow {
    pub(super) fn active_context(&self) -> ContextId {
        ContextId::ConnectionManager
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let chord = key_chord_from_gpui(&event.keystroke);
        let context = self.active_context();

        if let Some(command) = self.keymap.resolve(context, &chord) {
            return self.dispatch_command(command, window, cx);
        }

        false
    }

    pub(super) fn dispatch_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match self.view {
            View::DriverSelect => self.handle_driver_select_command(command, window, cx),
            View::EditForm => self.handle_form_command(command, window, cx),
        }
    }

    fn handle_driver_select_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let driver_count = self.available_drivers.len();
        if driver_count == 0 {
            return false;
        }

        match command {
            Command::SelectNext => {
                let current = self.driver_focus.index();
                let next = (current + 1) % driver_count;
                self.driver_focus = DriverFocus::Index(next);
                cx.notify();
                true
            }
            Command::SelectPrev => {
                let current = self.driver_focus.index();
                let prev = if current == 0 {
                    driver_count - 1
                } else {
                    current - 1
                };
                self.driver_focus = DriverFocus::Index(prev);
                cx.notify();
                true
            }
            Command::Execute => {
                let idx = self.driver_focus.index();
                if let Some(driver_info) = self.available_drivers.get(idx) {
                    let driver_id = driver_info.id.clone();
                    self.select_driver(&driver_id, window, cx);
                }
                true
            }
            Command::Cancel => {
                cx.emit(DismissEvent);
                window.remove_window();
                true
            }
            _ => false,
        }
    }

    fn handle_form_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.proxy_dropdown.read(cx).is_open() && self.handle_proxy_dropdown_command(command, cx)
        {
            return true;
        }

        if self.ssh_tunnel_dropdown.read(cx).is_open() && self.handle_dropdown_command(command, cx)
        {
            return true;
        }

        if self.access_method_dropdown.read(cx).is_open()
            && self.handle_access_method_dropdown_command(command, cx)
        {
            return true;
        }

        if self.auth_profile_dropdown.read(cx).is_open()
            && self.handle_auth_profile_dropdown_command(command, cx)
        {
            return true;
        }

        if self.ssm_auth_profile_dropdown.read(cx).is_open()
            && self.handle_ssm_auth_profile_dropdown_command(command, cx)
        {
            return true;
        }

        if self.handle_focused_value_source_dropdown_command(command, cx) {
            return true;
        }

        match self.edit_state {
            EditState::Navigating => self.handle_navigating_command(command, window, cx),
            EditState::Editing => self.handle_editing_command(command, window, cx),
            EditState::DropdownOpen => false,
        }
    }

    fn handle_dropdown_command(&mut self, command: Command, cx: &mut Context<Self>) -> bool {
        match command {
            Command::SelectNext => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_item(cx);
                });
                true
            }
            Command::SelectPrev => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_item(cx);
                });
                true
            }
            Command::Execute => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.accept_selection(cx);
                });
                true
            }
            Command::PageDown => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_page(cx);
                });
                true
            }
            Command::PageUp => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_page(cx);
                });
                true
            }
            Command::Cancel => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.close(cx);
                });
                true
            }
            _ => false,
        }
    }

    fn handle_proxy_dropdown_command(&mut self, command: Command, cx: &mut Context<Self>) -> bool {
        match command {
            Command::SelectNext => {
                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_item(cx);
                });
                true
            }
            Command::SelectPrev => {
                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_item(cx);
                });
                true
            }
            Command::Execute => {
                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.accept_selection(cx);
                });
                true
            }
            Command::PageDown => {
                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_page(cx);
                });
                true
            }
            Command::PageUp => {
                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_page(cx);
                });
                true
            }
            Command::Cancel => {
                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.close(cx);
                });
                true
            }
            _ => false,
        }
    }

    fn handle_access_method_dropdown_command(
        &mut self,
        command: Command,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::SelectNext => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_item(cx);
                });
                true
            }
            Command::SelectPrev => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_item(cx);
                });
                true
            }
            Command::Execute => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.accept_selection(cx);
                });
                true
            }
            Command::PageDown => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_page(cx);
                });
                true
            }
            Command::PageUp => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_page(cx);
                });
                true
            }
            Command::Cancel => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.close(cx);
                });
                true
            }
            _ => false,
        }
    }

    fn handle_auth_profile_dropdown_command(
        &mut self,
        command: Command,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::SelectNext => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_item(cx);
                });
                true
            }
            Command::SelectPrev => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_item(cx);
                });
                true
            }
            Command::PageDown => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_page(cx);
                });
                true
            }
            Command::PageUp => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_page(cx);
                });
                true
            }
            Command::Execute => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.accept_selection(cx);
                });
                true
            }
            Command::Cancel => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.close(cx);
                });
                true
            }
            _ => false,
        }
    }

    fn handle_ssm_auth_profile_dropdown_command(
        &mut self,
        command: Command,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::SelectNext => {
                self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_item(cx);
                });
                true
            }
            Command::SelectPrev => {
                self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_item(cx);
                });
                true
            }
            Command::PageDown => {
                self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_next_page(cx);
                });
                true
            }
            Command::PageUp => {
                self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.select_prev_page(cx);
                });
                true
            }
            Command::Execute => {
                self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.accept_selection(cx);
                });
                true
            }
            Command::Cancel => {
                self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.close(cx);
                });
                true
            }
            _ => false,
        }
    }

    fn focused_value_source_selector(
        &self,
    ) -> Option<&Entity<crate::ui::components::value_source_selector::ValueSourceSelector>> {
        use FormFocus::*;

        match self.form_focus {
            HostValueSource => Some(&self.host_value_source_selector),
            DatabaseValueSource => Some(&self.database_value_source_selector),
            UserValueSource => Some(&self.user_value_source_selector),
            PasswordValueSource => Some(&self.password_value_source_selector),
            SsmInstanceIdValueSource => Some(&self.ssm_instance_id_value_source_selector),
            SsmRegionValueSource => Some(&self.ssm_region_value_source_selector),
            SsmRemotePortValueSource => Some(&self.ssm_remote_port_value_source_selector),
            _ => None,
        }
    }

    fn handle_focused_value_source_dropdown_command(
        &mut self,
        command: Command,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(selector) = self.focused_value_source_selector().cloned() else {
            return false;
        };

        if !selector.read(cx).is_source_dropdown_open(cx) {
            return false;
        }

        match command {
            Command::SelectNext => {
                selector.update(cx, |selector, cx| {
                    selector.source_dropdown_next(cx);
                });
                true
            }
            Command::SelectPrev => {
                selector.update(cx, |selector, cx| {
                    selector.source_dropdown_prev(cx);
                });
                true
            }
            Command::PageDown => {
                selector.update(cx, |selector, cx| {
                    selector.source_dropdown_next_page(cx);
                });
                true
            }
            Command::PageUp => {
                selector.update(cx, |selector, cx| {
                    selector.source_dropdown_prev_page(cx);
                });
                true
            }
            Command::Execute => {
                selector.update(cx, |selector, cx| {
                    selector.source_dropdown_accept(cx);
                });
                true
            }
            Command::Cancel => {
                selector.update(cx, |selector, cx| {
                    selector.close_source_dropdown(cx);
                });
                true
            }
            _ => false,
        }
    }

    fn handle_navigating_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::SelectNext => {
                self.focus_down(cx);
                true
            }
            Command::SelectPrev => {
                self.focus_up(cx);
                true
            }
            Command::FocusLeft => {
                self.focus_left(cx);
                true
            }
            Command::FocusRight => {
                self.focus_right(cx);
                true
            }
            Command::CycleFocusForward => {
                self.next_tab(cx);
                true
            }
            Command::CycleFocusBackward => {
                self.prev_tab(cx);
                true
            }
            Command::Execute => {
                self.activate_focused_field(window, cx);
                true
            }
            Command::Cancel => {
                if self.editing_profile_id.is_none() {
                    self.back_to_driver_select(window, cx);
                } else {
                    cx.emit(DismissEvent);
                    window.remove_window();
                }
                true
            }
            _ => false,
        }
    }

    fn handle_editing_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::Cancel => {
                self.exit_edit_mode(window, cx);
                true
            }
            Command::Execute => {
                self.exit_edit_mode(window, cx);
                self.focus_down(cx);
                true
            }
            _ => false,
        }
    }

    pub(super) fn exit_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.edit_state = EditState::Navigating;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    pub(super) fn exit_edit_mode_on_blur(&mut self, cx: &mut Context<Self>) {
        self.edit_state = EditState::Navigating;
        cx.notify();
    }

    pub(super) fn enter_edit_mode_for_field(
        &mut self,
        field: FormFocus,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.form_focus = field;
        self.activate_focused_field(window, cx);
        cx.notify();
    }

    pub(super) fn begin_inline_editor_interaction(&mut self, cx: &mut Context<Self>) {
        self.edit_state = EditState::Editing;
        cx.notify();
    }

    pub(super) fn ssh_nav_state(&self, cx: &Context<Self>) -> SshNavState {
        let state = self.app_state.read(cx);
        let has_tunnels = !state.ssh_tunnels().is_empty();
        let has_selected_tunnel = self.selected_ssh_tunnel_id.is_some();
        let can_save_tunnel = self.selected_ssh_tunnel_id.is_none();
        SshNavState::new(
            self.ssh_enabled,
            has_tunnels,
            has_selected_tunnel,
            self.ssh_auth_method,
            can_save_tunnel,
        )
    }

    pub(super) fn proxy_nav_state(&self, cx: &Context<Self>) -> ProxyNavState {
        let state = self.app_state.read(cx);
        let has_proxies = !state.proxies().is_empty();
        let has_selected_proxy = self.selected_proxy_id.is_some();
        ProxyNavState::new(has_proxies, has_selected_proxy)
    }

    pub(super) fn main_nav_state(&self, cx: &Context<Self>) -> MainNavState {
        let has_uri_option = self
            .selected_driver
            .as_ref()
            .and_then(|d| d.form_definition().field("use_uri"))
            .is_some();

        let uri_mode_active = has_uri_option
            && self
                .checkbox_states
                .get("use_uri")
                .copied()
                .unwrap_or(false);

        let password_source_is_literal =
            self.password_value_source_selector.read(cx).is_literal(cx);
        let can_save_password =
            password_source_is_literal && self.app_state.read(cx).secret_store_available();

        MainNavState {
            uses_file_form: self.uses_file_form(),
            has_uri_option,
            uri_mode_active,
            password_source_is_literal,
            can_save_password,
        }
    }

    fn ssm_auth_login_enabled(&self, cx: &Context<Self>) -> bool {
        !self.auth_profile_login_in_progress && self.selected_auth_profile_needs_login(cx)
    }

    pub(super) fn normalize_focus_for_state(&mut self, cx: &Context<Self>) -> bool {
        use FormFocus::*;

        let mut next_focus = self.form_focus;

        match self.active_tab {
            ActiveTab::Main => {
                let state = self.main_nav_state(cx);

                if state.uri_mode_active && next_focus == Port {
                    next_focus = Host;
                }

                if !state.password_source_is_literal {
                    if next_focus == PasswordToggle || next_focus == PasswordSave {
                        next_focus = Password;
                    }
                } else if !state.can_save_password && next_focus == PasswordSave {
                    next_focus = PasswordToggle;
                }
            }
            ActiveTab::Access => {
                if (self.access_tab_mode == AccessTabMode::ManagedSsm
                    || self.access_tab_mode == AccessTabMode::Direct)
                    && !self.ssm_auth_login_enabled(cx)
                    && next_focus == SsmAuthLogin
                {
                    next_focus = SsmAuthManage;
                }
            }
            ActiveTab::Settings | ActiveTab::Mcp => {}
        }

        if next_focus != self.form_focus {
            self.form_focus = next_focus;
            return true;
        }

        false
    }

    fn focus_scroll_index(&self) -> usize {
        use FormFocus::*;
        match self.active_tab {
            ActiveTab::Main => match self.form_focus {
                UseUri | HostValueSource | Host | Port => 0,
                DatabaseValueSource | Database | UserValueSource | User | PasswordValueSource
                | Password | PasswordToggle | PasswordSave => 1,
                _ => 0,
            },
            ActiveTab::Access => match self.access_tab_mode {
                AccessTabMode::Direct => match self.form_focus {
                    AccessMethod => 0,
                    SsmAuthProfile => 1,
                    SsmAuthManage | SsmAuthLogin | SsmAuthRefresh => 2,
                    _ => 3,
                },
                AccessTabMode::ManagedSsm => match self.form_focus {
                    AccessMethod => 0,
                    SsmInstanceIdValueSource
                    | SsmInstanceId
                    | SsmRegionValueSource
                    | SsmRegion
                    | SsmRemotePortValueSource
                    | SsmRemotePort => 1,
                    SsmAuthProfile | SsmAuthManage | SsmAuthLogin | SsmAuthRefresh => 2,
                    _ => 3,
                },
                AccessTabMode::Ssh => {
                    let has_tunnels = self.ssh_enabled && !self.ssh_tunnel_uuids.is_empty();
                    let tunnel_offset = if has_tunnels { 1 } else { 0 };

                    match self.form_focus {
                        AccessMethod => 0,
                        SshEnabled => 1,
                        SshTunnelSelector | SshTunnelClear => 2,
                        SshEditInSettings => 2 + tunnel_offset,
                        SshHost | SshPort | SshUser => 2 + tunnel_offset,
                        SshAuthPrivateKey | SshAuthPassword => 3 + tunnel_offset,
                        SshKeyPath | SshKeyBrowse | SshPassphrase | SshSaveSecret | SshPassword => {
                            4 + tunnel_offset
                        }
                        TestSsh | SaveAsTunnel => 5 + tunnel_offset,
                        _ => 0,
                    }
                }
                AccessTabMode::Proxy => match self.form_focus {
                    AccessMethod => 0,
                    ProxySelector | ProxyClear => 1,
                    ProxyEditInSettings => 2,
                    _ => 0,
                },
            },
            ActiveTab::Settings => match self.form_focus {
                SettingsRefreshPolicy | SettingsRefreshInterval => 0,
                SettingsConfirmDangerous | SettingsRequiresWhere | SettingsRequiresPreview => 1,
                SettingsDriverField(idx) => 2 + idx as usize,
                _ => 0,
            },
            ActiveTab::Mcp => 0,
        }
    }

    fn scroll_to_focused(&mut self) {
        let index = self.focus_scroll_index();
        self.form_scroll_handle.scroll_to_item(index);
    }

    pub(super) fn focus_down(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.down_main(self.main_nav_state(cx)),
            ActiveTab::Access => self.form_focus.down_access(
                self.access_tab_mode,
                self.ssh_nav_state(cx),
                self.proxy_nav_state(cx),
            ),
            ActiveTab::Settings => self
                .form_focus
                .down_settings(self.settings_driver_field_count()),
            ActiveTab::Mcp => self.form_focus,
        };
        self.normalize_focus_for_state(cx);
        self.scroll_to_focused();
        cx.notify();
    }

    fn focus_up(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.up_main(self.main_nav_state(cx)),
            ActiveTab::Access => self.form_focus.up_access(
                self.access_tab_mode,
                self.ssh_nav_state(cx),
                self.proxy_nav_state(cx),
            ),
            ActiveTab::Settings => self
                .form_focus
                .up_settings(self.settings_driver_field_count()),
            ActiveTab::Mcp => self.form_focus,
        };
        self.normalize_focus_for_state(cx);
        self.scroll_to_focused();
        cx.notify();
    }

    fn focus_left(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.left_main(self.main_nav_state(cx)),
            ActiveTab::Access => self.form_focus.left_access(
                self.access_tab_mode,
                self.ssh_nav_state(cx),
                self.proxy_nav_state(cx),
            ),
            ActiveTab::Settings => self.form_focus.left_settings(),
            ActiveTab::Mcp => self.form_focus,
        };

        if self.active_tab == ActiveTab::Access
            && (self.access_tab_mode == AccessTabMode::ManagedSsm
                || self.access_tab_mode == AccessTabMode::Direct)
            && self.form_focus == FormFocus::SsmAuthLogin
            && !self.ssm_auth_login_enabled(cx)
        {
            self.form_focus = FormFocus::SsmAuthManage;
        }

        self.normalize_focus_for_state(cx);
        self.scroll_to_focused();
        cx.notify();
    }

    fn focus_right(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.right_main(self.main_nav_state(cx)),
            ActiveTab::Access => self.form_focus.right_access(
                self.access_tab_mode,
                self.ssh_nav_state(cx),
                self.proxy_nav_state(cx),
            ),
            ActiveTab::Settings => self.form_focus.right_settings(),
            ActiveTab::Mcp => self.form_focus,
        };

        if self.active_tab == ActiveTab::Access
            && (self.access_tab_mode == AccessTabMode::ManagedSsm
                || self.access_tab_mode == AccessTabMode::Direct)
            && self.form_focus == FormFocus::SsmAuthLogin
            && !self.ssm_auth_login_enabled(cx)
        {
            self.form_focus = FormFocus::SsmAuthRefresh;
        }

        self.normalize_focus_for_state(cx);
        self.scroll_to_focused();
        cx.notify();
    }

    fn next_tab(&mut self, cx: &mut Context<Self>) {
        let has_access_tab = !self.uses_file_form();

        self.active_tab = next_active_tab(self.active_tab, has_access_tab);

        self.form_focus = self.initial_focus_for_tab(cx);

        self.scroll_to_focused();
        cx.notify();
    }

    fn prev_tab(&mut self, cx: &mut Context<Self>) {
        let has_access_tab = !self.uses_file_form();

        self.active_tab = prev_active_tab(self.active_tab, has_access_tab);

        self.form_focus = self.initial_focus_for_tab(cx);

        self.scroll_to_focused();
        cx.notify();
    }

    fn initial_focus_for_tab(&self, _cx: &Context<Self>) -> FormFocus {
        match self.active_tab {
            ActiveTab::Main => FormFocus::Name,
            ActiveTab::Access => FormFocus::AccessMethod,
            ActiveTab::Settings => FormFocus::SettingsRefreshPolicy,
            ActiveTab::Mcp => FormFocus::Name,
        }
    }

    pub(super) fn open_settings_section(
        &mut self,
        section: SettingsSectionId,
        cx: &mut Context<Self>,
    ) {
        // Phase 3: settings_window removed from AppState - always open a new window
        // TODO: Phase 4 will track settings window in AppStateEntity
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(950.0), px(700.0)), cx);

        let mut options = WindowOptions {
            app_id: Some("dbflux".into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Settings".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 800.0, 600.0);

        let _ = cx.open_window(options, move |window, cx| {
            let settings = cx
                .new(|cx| SettingsWindow::new_with_section(app_state.clone(), section, window, cx));
            cx.new(|cx| Root::new(settings, window, cx))
        });
    }

    pub(super) fn activate_focused_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.form_focus {
            FormFocus::Name => {
                self.edit_state = EditState::Editing;
                self.input_name.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            FormFocus::Host | FormFocus::Port | FormFocus::Database | FormFocus::User => {
                if let Some(input) = self.input_for_focus(self.form_focus).cloned() {
                    self.edit_state = EditState::Editing;
                    input.update(cx, |state, cx| {
                        state.focus(window, cx);
                    });
                }
            }

            FormFocus::HostValueSource => {
                self.host_value_source_selector.update(cx, |selector, cx| {
                    selector.open_source_dropdown(cx);
                });
            }

            FormFocus::DatabaseValueSource => {
                self.database_value_source_selector
                    .update(cx, |selector, cx| {
                        selector.open_source_dropdown(cx);
                    });
            }

            FormFocus::UserValueSource => {
                self.user_value_source_selector.update(cx, |selector, cx| {
                    selector.open_source_dropdown(cx);
                });
            }

            FormFocus::PasswordValueSource => {
                self.password_value_source_selector
                    .update(cx, |selector, cx| {
                        selector.open_source_dropdown(cx);
                    });
            }

            FormFocus::Password => {
                self.edit_state = EditState::Editing;
                self.input_password.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            FormFocus::PasswordToggle => {
                if self.password_value_source_selector.read(cx).is_literal(cx) {
                    self.show_password = !self.show_password;
                }
            }

            FormFocus::AccessMethod => {
                self.access_method_dropdown.update(cx, |dropdown, cx| {
                    dropdown.open(cx);
                });
            }

            FormFocus::SshHost => {
                self.edit_state = EditState::Editing;
                self.input_ssh_host.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
            FormFocus::SshPort => {
                self.edit_state = EditState::Editing;
                self.input_ssh_port.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
            FormFocus::SshUser => {
                self.edit_state = EditState::Editing;
                self.input_ssh_user.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
            FormFocus::SshKeyPath => {
                self.edit_state = EditState::Editing;
                self.input_ssh_key_path.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
            FormFocus::SshPassphrase => {
                self.edit_state = EditState::Editing;
                self.input_ssh_key_passphrase.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
            FormFocus::SshPassword => {
                self.edit_state = EditState::Editing;
                self.input_ssh_password.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
            FormFocus::SsmInstanceId => {
                self.edit_state = EditState::Editing;
                self.input_ssm_instance_id.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            FormFocus::SsmInstanceIdValueSource => {
                self.ssm_instance_id_value_source_selector
                    .update(cx, |selector, cx| {
                        selector.open_source_dropdown(cx);
                    });
            }

            FormFocus::SsmRegion => {
                self.edit_state = EditState::Editing;
                self.input_ssm_region.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            FormFocus::SsmRegionValueSource => {
                self.ssm_region_value_source_selector
                    .update(cx, |selector, cx| {
                        selector.open_source_dropdown(cx);
                    });
            }

            FormFocus::SsmRemotePort => {
                self.edit_state = EditState::Editing;
                self.input_ssm_remote_port.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            FormFocus::SsmRemotePortValueSource => {
                self.ssm_remote_port_value_source_selector
                    .update(cx, |selector, cx| {
                        selector.open_source_dropdown(cx);
                    });
            }

            FormFocus::SsmAuthProfile => {
                self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                    dropdown.open(cx);
                });
            }

            FormFocus::SsmAuthManage => {
                self.open_auth_profiles_settings(cx);
            }

            FormFocus::SsmAuthLogin => {
                if !self.auth_profile_login_in_progress
                    && self.selected_auth_profile_needs_login(cx)
                {
                    self.login_selected_auth_profile(cx);
                }
            }

            FormFocus::SsmAuthRefresh => {
                self.refresh_auth_profile_statuses(cx);
            }

            FormFocus::FileBrowse => {
                self.browse_file_path(window, cx);
            }

            FormFocus::SshKeyBrowse => {
                self.browse_ssh_key(window, cx);
            }

            FormFocus::UseUri => {
                let current = self
                    .checkbox_states
                    .get("use_uri")
                    .copied()
                    .unwrap_or(false);
                let new_value = !current;
                self.checkbox_states
                    .insert("use_uri".to_string(), new_value);

                if new_value {
                    self.sync_fields_to_uri(window, cx);
                } else {
                    self.sync_uri_to_fields(window, cx);
                }
            }
            FormFocus::PasswordSave => {
                if self.password_value_source_selector.read(cx).is_literal(cx)
                    && self.app_state.read(cx).secret_store_available()
                {
                    self.form_save_password = !self.form_save_password;
                }
            }
            FormFocus::ProxySelector => {
                let proxies = self.app_state.read(cx).proxies().to_vec();
                let proxy_items: Vec<DropdownItem> = proxies
                    .iter()
                    .map(|p| {
                        let label = if p.enabled {
                            p.name.clone()
                        } else {
                            format!("{} (disabled)", p.name)
                        };
                        DropdownItem::with_value(&label, p.id.to_string())
                    })
                    .collect();
                self.proxy_uuids = proxies.iter().map(|p| p.id).collect();

                self.proxy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.set_items(proxy_items, cx);
                    dropdown.open(cx);
                });
            }

            FormFocus::ProxyClear => {
                self.clear_proxy_selection(cx);
            }

            FormFocus::SshEnabled => {
                self.ssh_enabled = !self.ssh_enabled;
            }
            FormFocus::SshSaveSecret => {
                self.form_save_ssh_secret = !self.form_save_ssh_secret;
            }

            FormFocus::SshTunnelSelector => {
                let ssh_tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();
                let tunnel_items: Vec<DropdownItem> = ssh_tunnels
                    .iter()
                    .map(|t| DropdownItem::with_value(&t.name, t.id.to_string()))
                    .collect();
                self.ssh_tunnel_uuids = ssh_tunnels.iter().map(|t| t.id).collect();

                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.set_items(tunnel_items, cx);
                    dropdown.open(cx);
                });
            }

            FormFocus::SshTunnelClear => {
                self.clear_ssh_tunnel_selection(window, cx);
            }

            FormFocus::SshAuthPrivateKey => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
            }
            FormFocus::SshAuthPassword => {
                self.ssh_auth_method = SshAuthSelection::Password;
            }

            FormFocus::SshEditInSettings => {
                self.open_settings_section(SettingsSectionId::SshTunnels, cx);
            }

            FormFocus::ProxyEditInSettings => {
                self.open_settings_section(SettingsSectionId::Proxies, cx);
            }

            FormFocus::TestSsh => {
                self.test_ssh_connection(window, cx);
            }
            FormFocus::SaveAsTunnel => {
                self.save_current_ssh_as_tunnel(cx);
            }

            FormFocus::SettingsRefreshPolicy => {
                self.conn_override_refresh_policy = !self.conn_override_refresh_policy;
            }
            FormFocus::SettingsRefreshInterval => {
                if self.conn_override_refresh_interval {
                    self.edit_state = EditState::Editing;
                    self.conn_refresh_interval_input.update(cx, |state, cx| {
                        state.focus(window, cx);
                    });
                } else {
                    self.conn_override_refresh_interval = true;
                }
            }
            FormFocus::SettingsConfirmDangerous
            | FormFocus::SettingsRequiresWhere
            | FormFocus::SettingsRequiresPreview => {
                // These are dropdowns — no toggle action needed in navigate mode
            }

            FormFocus::SettingsDriverField(idx) => {
                if let Some(field) = self.settings_driver_field_def(idx) {
                    match &field.kind {
                        FormFieldKind::Checkbox => {
                            let current = self
                                .conn_form_state
                                .checkboxes
                                .get(&field.id)
                                .copied()
                                .unwrap_or(false);
                            self.conn_form_state
                                .checkboxes
                                .insert(field.id.clone(), !current);
                        }
                        FormFieldKind::Select { .. } => {}
                        _ => {
                            if let Some(input) = self.conn_form_state.inputs.get(&field.id).cloned()
                            {
                                self.edit_state = EditState::Editing;
                                input.update(cx, |state, cx| {
                                    state.focus(window, cx);
                                });
                            }
                        }
                    }
                }
            }

            FormFocus::TestConnection => {
                self.test_connection(window, cx);
            }
            FormFocus::Save => {
                self.save_profile(window, cx);
            }
        }
        self.normalize_focus_for_state(cx);
        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        next_active_tab, prev_active_tab, AccessTabMode, ActiveTab, FormFocus, MainNavState,
        ProxyNavState, SshNavState,
    };
    use crate::ui::windows::ssh_shared::SshAuthSelection;

    fn ssh_disabled() -> SshNavState {
        SshNavState::new(false, false, false, SshAuthSelection::PrivateKey, false)
    }

    fn ssh_enabled_no_tunnels() -> SshNavState {
        SshNavState::new(true, false, false, SshAuthSelection::PrivateKey, true)
    }

    fn ssh_enabled_with_tunnel_selected() -> SshNavState {
        SshNavState::new(true, true, true, SshAuthSelection::PrivateKey, false)
    }

    fn proxy_state(has_proxies: bool, has_selected: bool) -> ProxyNavState {
        ProxyNavState::new(has_proxies, has_selected)
    }

    fn main_state(
        has_uri_option: bool,
        uri_mode_active: bool,
        password_source_is_literal: bool,
        can_save_password: bool,
    ) -> MainNavState {
        MainNavState {
            uses_file_form: false,
            has_uri_option,
            uri_mode_active,
            password_source_is_literal,
            can_save_password,
        }
    }

    // --- Main tab ---

    #[test]
    fn main_with_uri_inactive_vertical_flow() {
        let state = main_state(true, false, true, true);

        let mut focus = FormFocus::Name;
        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::UseUri);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::HostValueSource);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::DatabaseValueSource);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::UserValueSource);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::PasswordValueSource);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::TestConnection);
    }

    #[test]
    fn main_with_uri_active_skips_database_and_user_rows() {
        let state = main_state(true, true, true, true);

        let mut focus = FormFocus::Name;
        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::UseUri);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::HostValueSource);

        focus = focus.down_main(state);
        assert_eq!(focus, FormFocus::PasswordValueSource);
    }

    #[test]
    fn main_horizontal_host_row_moves_selector_input_port() {
        let state = main_state(false, false, true, true);

        assert_eq!(
            FormFocus::HostValueSource.right_main(state),
            FormFocus::Host
        );
        assert_eq!(FormFocus::Host.right_main(state), FormFocus::Port);
        assert_eq!(FormFocus::Port.left_main(state), FormFocus::Host);
        assert_eq!(FormFocus::Host.left_main(state), FormFocus::HostValueSource);
    }

    #[test]
    fn main_password_non_literal_stops_at_input() {
        let state = main_state(false, false, false, false);

        assert_eq!(
            FormFocus::PasswordValueSource.right_main(state),
            FormFocus::Password
        );
        assert_eq!(FormFocus::Password.right_main(state), FormFocus::Password);
        assert_eq!(
            FormFocus::PasswordSave.left_main(state),
            FormFocus::Password
        );
    }

    // --- Managed SSM access mode ---

    #[test]
    fn managed_ssm_vertical_flow_uses_selector_then_input_rows() {
        let ssh_state = ssh_disabled();
        let proxy_state = proxy_state(false, false);

        let mut focus = FormFocus::AccessMethod;
        focus = focus.down_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmInstanceIdValueSource);

        focus = focus.down_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmRegionValueSource);

        focus = focus.down_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmRemotePortValueSource);

        focus = focus.down_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmAuthProfile);

        focus = focus.down_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmAuthManage);

        focus = focus.down_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::TestConnection);
    }

    #[test]
    fn managed_ssm_horizontal_flow_moves_selector_and_input() {
        let ssh_state = ssh_disabled();
        let proxy_state = proxy_state(false, false);

        assert_eq!(
            FormFocus::SsmInstanceIdValueSource.right_access(
                AccessTabMode::ManagedSsm,
                ssh_state,
                proxy_state,
            ),
            FormFocus::SsmInstanceId
        );

        assert_eq!(
            FormFocus::SsmRegion.left_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state,),
            FormFocus::SsmRegionValueSource
        );
    }

    #[test]
    fn managed_ssm_horizontal_flow_moves_across_auth_actions() {
        let ssh_state = ssh_disabled();
        let proxy_state = proxy_state(false, false);

        assert_eq!(
            FormFocus::SsmAuthProfile.right_access(
                AccessTabMode::ManagedSsm,
                ssh_state,
                proxy_state,
            ),
            FormFocus::SsmAuthManage
        );

        assert_eq!(
            FormFocus::SsmAuthManage.right_access(
                AccessTabMode::ManagedSsm,
                ssh_state,
                proxy_state,
            ),
            FormFocus::SsmAuthLogin
        );

        assert_eq!(
            FormFocus::SsmAuthRefresh.left_access(
                AccessTabMode::ManagedSsm,
                ssh_state,
                proxy_state,
            ),
            FormFocus::SsmAuthLogin
        );
    }

    #[test]
    fn managed_ssm_up_from_test_connection_goes_to_auth_manage() {
        let ssh_state = ssh_disabled();
        let proxy_state = proxy_state(false, false);

        assert_eq!(
            FormFocus::TestConnection.up_access(AccessTabMode::ManagedSsm, ssh_state, proxy_state),
            FormFocus::SsmAuthManage
        );
    }

    // --- Direct access mode ---

    #[test]
    fn direct_vertical_flow_includes_auth_profile_and_manage() {
        let ssh_state = ssh_disabled();
        let proxy_state = proxy_state(false, false);

        let mut focus = FormFocus::AccessMethod;
        focus = focus.down_access(AccessTabMode::Direct, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmAuthProfile);

        focus = focus.down_access(AccessTabMode::Direct, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::SsmAuthManage);

        focus = focus.down_access(AccessTabMode::Direct, ssh_state, proxy_state);
        assert_eq!(focus, FormFocus::TestConnection);
    }

    #[test]
    fn direct_horizontal_flow_moves_across_auth_actions() {
        let ssh_state = ssh_disabled();
        let proxy_state = proxy_state(false, false);

        assert_eq!(
            FormFocus::SsmAuthProfile.right_access(AccessTabMode::Direct, ssh_state, proxy_state,),
            FormFocus::SsmAuthManage
        );

        assert_eq!(
            FormFocus::SsmAuthManage.right_access(AccessTabMode::Direct, ssh_state, proxy_state,),
            FormFocus::SsmAuthLogin
        );

        assert_eq!(
            FormFocus::SsmAuthRefresh.left_access(AccessTabMode::Direct, ssh_state, proxy_state,),
            FormFocus::SsmAuthLogin
        );
    }

    // --- SSH tab: disabled ---

    #[test]
    fn ssh_disabled_full_traversal() {
        let state = ssh_disabled();

        let mut focus = FormFocus::Name;
        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::SshEnabled);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::TestConnection);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::Save);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::Name);
    }

    #[test]
    fn ssh_disabled_up_from_enabled() {
        let state = ssh_disabled();
        assert_eq!(FormFocus::SshEnabled.up_ssh(state), FormFocus::Name);
    }

    // --- SSH tab: enabled, no tunnels ---

    #[test]
    fn ssh_enabled_name_to_ssh_enabled() {
        let state = ssh_enabled_no_tunnels();
        assert_eq!(FormFocus::Name.down_ssh(state), FormFocus::SshEnabled);
    }

    #[test]
    fn ssh_enabled_skips_tunnel_selector_when_no_tunnels() {
        let state = ssh_enabled_no_tunnels();
        assert_eq!(FormFocus::SshEnabled.down_ssh(state), FormFocus::SshHost);
    }

    // --- SSH tab: read-only mode (tunnel selected) ---

    #[test]
    fn ssh_readonly_skips_to_edit_button() {
        let state = ssh_enabled_with_tunnel_selected();

        let mut focus = FormFocus::Name;
        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::SshEnabled);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::SshTunnelSelector);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::SshEditInSettings);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::TestSsh);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::TestConnection);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::Save);

        focus = focus.down_ssh(state);
        assert_eq!(focus, FormFocus::Name);
    }

    #[test]
    fn ssh_readonly_up_from_edit_button() {
        let state = ssh_enabled_with_tunnel_selected();
        assert_eq!(
            FormFocus::SshEditInSettings.up_ssh(state),
            FormFocus::SshTunnelSelector
        );
    }

    // --- Proxy tab ---

    #[test]
    fn proxy_no_proxies_traversal() {
        let state = proxy_state(false, false);

        let mut focus = FormFocus::Name;
        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::TestConnection);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::Save);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::Name);
    }

    #[test]
    fn proxy_with_proxies_no_selection() {
        let state = proxy_state(true, false);

        let mut focus = FormFocus::Name;
        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::ProxySelector);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::TestConnection);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::Save);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::Name);
    }

    #[test]
    fn proxy_with_selection_shows_edit_button() {
        let state = proxy_state(true, true);

        let mut focus = FormFocus::Name;
        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::ProxySelector);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::ProxyEditInSettings);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::TestConnection);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::Save);

        focus = focus.down_proxy(state);
        assert_eq!(focus, FormFocus::Name);
    }

    #[test]
    fn proxy_right_selector_to_clear_when_selected() {
        let state = proxy_state(true, true);
        assert_eq!(
            FormFocus::ProxySelector.right_proxy(state),
            FormFocus::ProxyClear
        );
    }

    #[test]
    fn proxy_right_selector_stays_when_no_selection() {
        let state = proxy_state(true, false);
        assert_eq!(
            FormFocus::ProxySelector.right_proxy(state),
            FormFocus::ProxySelector
        );
    }

    #[test]
    fn proxy_left_clear_to_selector() {
        let state = proxy_state(true, true);
        assert_eq!(
            FormFocus::ProxyClear.left_proxy(state),
            FormFocus::ProxySelector
        );
    }

    #[test]
    fn proxy_up_from_edit_button() {
        let state = proxy_state(true, true);
        assert_eq!(
            FormFocus::ProxyEditInSettings.up_proxy(state),
            FormFocus::ProxySelector
        );
    }

    #[test]
    fn tab_cycle_includes_mcp_tab() {
        let mut tab = ActiveTab::Main;

        tab = next_active_tab(tab, true);
        assert!(matches!(tab, ActiveTab::Access));

        tab = next_active_tab(tab, true);
        assert!(matches!(tab, ActiveTab::Settings));

        tab = next_active_tab(tab, true);
        assert!(matches!(tab, ActiveTab::Mcp));

        tab = next_active_tab(tab, true);
        assert!(matches!(tab, ActiveTab::Main));
    }

    #[test]
    fn tab_reverse_cycle_from_main_enters_mcp() {
        let tab = prev_active_tab(ActiveTab::Main, true);
        assert!(matches!(tab, ActiveTab::Mcp));
    }
}
