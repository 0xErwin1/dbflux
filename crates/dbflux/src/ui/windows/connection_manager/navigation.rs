use crate::keymap::{Command, ContextId};
use crate::ui::dropdown::DropdownItem;
use crate::ui::windows::ssh_shared::SshAuthSelection;
use gpui::*;

use super::{
    ActiveTab, ConnectionManagerWindow, DismissEvent, DriverFocus, EditState, FormFocus, View,
};

/// State needed for SSH tab navigation
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

/// State needed for Main tab navigation (depends on database type)
#[derive(Clone, Copy)]
pub(super) struct MainNavState {
    /// True for file-based databases (SQLite), false for server-based (PostgreSQL, MySQL, MariaDB)
    pub(super) uses_file_form: bool,
    /// True if the driver has a "Use Connection URI" checkbox option
    pub(super) has_uri_option: bool,
}

impl FormFocus {
    // === Main Tab: Vertical Navigation (j/k) ===

    pub(super) fn down_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            match self {
                Name => Database,
                Database => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else if state.has_uri_option {
            match self {
                Name => UseUri,
                UseUri => Host,
                Host | Port => Database,
                Database => User,
                User => Password,
                Password | PasswordSave => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else {
            match self {
                Name => Host,
                Host | Port => Database,
                Database => User,
                User => Password,
                Password | PasswordSave => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        }
    }

    pub(super) fn up_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            match self {
                Name => Save,
                Database => Name,
                TestConnection => Database,
                Save => TestConnection,
                _ => Save,
            }
        } else if state.has_uri_option {
            match self {
                Name => Save,
                UseUri => Name,
                Host | Port => UseUri,
                Database => Host,
                User => Database,
                Password | PasswordSave => User,
                TestConnection => Password,
                Save => TestConnection,
                _ => Save,
            }
        } else {
            match self {
                Name => Save,
                Host | Port => Name,
                Database => Host,
                User => Database,
                Password | PasswordSave => User,
                TestConnection => Password,
                Save => TestConnection,
                _ => Save,
            }
        }
    }

    // === Main Tab: Horizontal Navigation (h/l) ===

    pub(super) fn left_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            match self {
                Save => TestConnection,
                other => other,
            }
        } else {
            match self {
                Port => Host,
                PasswordSave => Password,
                Save => TestConnection,
                other => other,
            }
        }
    }

    pub(super) fn right_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            match self {
                TestConnection => Save,
                other => other,
            }
        } else {
            match self {
                Host => Port,
                Password => PasswordSave,
                TestConnection => Save,
                other => other,
            }
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
        let chord = crate::keymap::KeyChord::from_gpui(&event.keystroke);
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
                    let kind = driver_info.kind;
                    self.select_driver(kind, window, cx);
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
        if self.ssh_tunnel_dropdown.read(cx).is_open() && self.handle_dropdown_command(command, cx)
        {
            return true;
        }

        match self.edit_state {
            EditState::Navigating => self.handle_navigating_command(command, window, cx),
            EditState::Editing => self.handle_editing_command(command, window, cx),
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
            Command::Cancel => {
                self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
                    dropdown.close(cx);
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

    pub(super) fn ssh_nav_state(&self, cx: &Context<Self>) -> SshNavState {
        let has_tunnels = !self.app_state.read(cx).ssh_tunnels().is_empty();
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

    pub(super) fn main_nav_state(&self) -> MainNavState {
        let has_uri_option = self
            .selected_driver
            .as_ref()
            .and_then(|d| d.form_definition().field("use_uri"))
            .is_some();

        MainNavState {
            uses_file_form: self.uses_file_form(),
            has_uri_option,
        }
    }

    fn focus_scroll_index(&self) -> usize {
        use FormFocus::*;
        match self.active_tab {
            ActiveTab::Main => match self.form_focus {
                UseUri | Host | Port | Database => 0,
                User | Password | PasswordSave => 1,
                _ => 0,
            },
            ActiveTab::Ssh => {
                let has_tunnels = self.ssh_enabled && !self.ssh_tunnel_uuids.is_empty();
                let offset = if has_tunnels { 1 } else { 0 };

                match self.form_focus {
                    SshEnabled => 0,
                    SshTunnelSelector | SshTunnelClear => 1,
                    SshHost | SshPort | SshUser => 1 + offset,
                    SshAuthPrivateKey | SshAuthPassword => 2 + offset,
                    SshKeyPath | SshKeyBrowse | SshPassphrase | SshSaveSecret | SshPassword => {
                        3 + offset
                    }
                    TestSsh | SaveAsTunnel => 4 + offset,
                    _ => 0,
                }
            }
        }
    }

    fn scroll_to_focused(&mut self) {
        let index = self.focus_scroll_index();
        self.form_scroll_handle.scroll_to_item(index);
    }

    pub(super) fn focus_down(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.down_main(self.main_nav_state()),
            ActiveTab::Ssh => self.form_focus.down_ssh(self.ssh_nav_state(cx)),
        };
        self.scroll_to_focused();
        cx.notify();
    }

    fn focus_up(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.up_main(self.main_nav_state()),
            ActiveTab::Ssh => self.form_focus.up_ssh(self.ssh_nav_state(cx)),
        };
        self.scroll_to_focused();
        cx.notify();
    }

    fn focus_left(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.left_main(self.main_nav_state()),
            ActiveTab::Ssh => self.form_focus.left_ssh(self.ssh_nav_state(cx)),
        };
        self.scroll_to_focused();
        cx.notify();
    }

    fn focus_right(&mut self, cx: &mut Context<Self>) {
        self.form_focus = match self.active_tab {
            ActiveTab::Main => self.form_focus.right_main(self.main_nav_state()),
            ActiveTab::Ssh => self.form_focus.right_ssh(self.ssh_nav_state(cx)),
        };
        self.scroll_to_focused();
        cx.notify();
    }

    fn next_tab(&mut self, cx: &mut Context<Self>) {
        if self.supports_ssh() {
            self.active_tab = match self.active_tab {
                ActiveTab::Main => ActiveTab::Ssh,
                ActiveTab::Ssh => ActiveTab::Main,
            };
            self.form_focus = match self.active_tab {
                ActiveTab::Main => FormFocus::Name,
                ActiveTab::Ssh => FormFocus::SshEnabled,
            };
            self.scroll_to_focused();
            cx.notify();
        }
    }

    fn prev_tab(&mut self, cx: &mut Context<Self>) {
        self.next_tab(cx);
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

            FormFocus::Password => {
                self.edit_state = EditState::Editing;
                self.input_password.update(cx, |state, cx| {
                    state.focus(window, cx);
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

            FormFocus::SshKeyBrowse => {
                self.browse_ssh_key(window, cx);
            }

            FormFocus::UseUri => {
                let current = self
                    .checkbox_states
                    .get("use_uri")
                    .copied()
                    .unwrap_or(false);
                self.checkbox_states.insert("use_uri".to_string(), !current);
            }
            FormFocus::PasswordSave => {
                self.form_save_password = !self.form_save_password;
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

            FormFocus::TestSsh => {
                self.test_ssh_connection(window, cx);
            }
            FormFocus::SaveAsTunnel => {
                self.save_current_ssh_as_tunnel(cx);
            }

            FormFocus::TestConnection => {
                self.test_connection(window, cx);
            }
            FormFocus::Save => {
                self.save_profile(window, cx);
            }
        }
        cx.notify();
    }
}
