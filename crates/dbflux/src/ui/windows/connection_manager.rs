use crate::app::AppState;
use crate::keymap::{Command, ContextId, KeyChord, KeymapStack};
use crate::ui::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::icons::AppIcon;
use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbKind, DriverFormDef, FormFieldDef, FormFieldKind,
    FormTab, SshAuthMethod, SshTunnelConfig, SshTunnelProfile,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::list::ListItem;

use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::Sizable;
use gpui_component::{Icon, IconName};
use log::info;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// Focus state for driver selection screen
#[derive(Clone, Copy, PartialEq, Default)]
enum DriverFocus {
    #[default]
    First,
    Index(usize),
}

impl DriverFocus {
    fn index(&self) -> usize {
        match self {
            DriverFocus::First => 0,
            DriverFocus::Index(i) => *i,
        }
    }
}

/// Focus state for form fields (Main tab)
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Debug)]
enum FormFocus {
    // Main tab fields
    Name,
    UseUri,
    Host,
    Port,
    Database,
    User,
    Password,
    PasswordSave,
    // SSH tab fields
    SshEnabled,
    SshTunnelSelector,
    SshTunnelClear,
    SshHost,
    SshPort,
    SshUser,
    SshAuthPrivateKey,
    SshAuthPassword,
    SshKeyPath,
    SshKeyBrowse,
    SshPassphrase,
    SshSaveSecret,
    SshPassword,
    TestSsh,
    SaveAsTunnel,
    // Actions (shared between tabs)
    TestConnection,
    Save,
}

/// State needed for SSH tab navigation
#[derive(Clone, Copy)]
struct SshNavState {
    enabled: bool,
    has_tunnels: bool,
    has_selected_tunnel: bool,
    auth_method: SshAuthSelection,
    can_save_tunnel: bool,
}

impl FormFocus {
    // === Main Tab: Vertical Navigation (j/k) ===

    fn down_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            // File-based (SQLite): Name -> Database -> TestConnection -> Save -> Name
            match self {
                Name => Database,
                Database => TestConnection,
                TestConnection => Save,
                Save => Name,
                _ => Name,
            }
        } else if state.has_uri_option {
            // Server-based with URI option: Name -> UseUri -> Host -> Database -> User -> Password -> TestConnection -> Save -> Name
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
            // Server-based without URI option: Name -> Host -> Database -> User -> Password -> TestConnection -> Save -> Name
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

    fn up_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            // File-based (SQLite): Name <- Database <- TestConnection <- Save <- Name
            match self {
                Name => Save,
                Database => Name,
                TestConnection => Database,
                Save => TestConnection,
                _ => Save,
            }
        } else if state.has_uri_option {
            // Server-based with URI option
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
            // Server-based without URI option
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

    fn left_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            // File-based (SQLite): only TestConnection <-> Save horizontal pair
            match self {
                Save => TestConnection,
                other => other,
            }
        } else {
            // Server-based (PostgreSQL, MySQL, MariaDB)
            match self {
                Port => Host,
                PasswordSave => Password,
                Save => TestConnection,
                other => other,
            }
        }
    }

    fn right_main(self, state: MainNavState) -> Self {
        use FormFocus::*;

        if state.uses_file_form {
            // File-based (SQLite): only TestConnection <-> Save horizontal pair
            match self {
                TestConnection => Save,
                other => other,
            }
        } else {
            // Server-based (PostgreSQL, MySQL, MariaDB)
            match self {
                Host => Port,
                Password => PasswordSave,
                TestConnection => Save,
                other => other,
            }
        }
    }

    // === SSH Tab: Vertical Navigation (j/k) ===
    //
    // Full navigation order when SSH enabled:
    // Name -> SshEnabled -> SshTunnelSelector (if has_tunnels) -> SshHost -> SshUser
    // -> SshAuthPrivateKey -> SshKeyPath/SshPassword (based on auth_method)
    // -> SshPassphrase (if PrivateKey) -> TestSsh -> SaveAsTunnel (if can_save)
    // -> TestConnection -> Save -> (wrap to Name)
    //
    // Horizontal elements on same row use h/l navigation, j/k moves to left-most
    // element of next/previous row:
    // - [SshKeyPath, SshKeyBrowse] are on same row
    // - [SshPassphrase, SshSaveSecret] are on same row (PrivateKey mode)
    // - [SshPassword, SshSaveSecret] are on same row (Password mode)

    fn down_ssh(self, state: SshNavState) -> Self {
        use FormFocus::*;

        if !state.enabled {
            // When SSH is disabled: Name -> SshEnabled -> TestConnection -> Save -> Name
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
                // PrivateKey mode: KeyPath row -> Passphrase row -> TestSsh row
                SshKeyPath | SshKeyBrowse => SshPassphrase,
                SshPassphrase | SshSaveSecret
                    if state.auth_method == SshAuthSelection::PrivateKey =>
                {
                    TestSsh
                }
                // Password mode: Password row -> TestSsh row
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

    fn up_ssh(self, state: SshNavState) -> Self {
        use FormFocus::*;

        if !state.enabled {
            // When SSH is disabled: Name -> Save -> TestConnection -> SshEnabled -> Name
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
                // PrivateKey mode
                SshKeyPath | SshKeyBrowse => SshAuthPrivateKey,
                SshPassphrase | SshSaveSecret
                    if state.auth_method == SshAuthSelection::PrivateKey =>
                {
                    SshKeyPath
                }
                // Password mode
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
    //
    // Horizontal pairs:
    // - [SshHost, SshPort]
    // - [SshAuthPrivateKey, SshAuthPassword]
    // - [SshKeyPath, SshKeyBrowse] (PrivateKey mode)
    // - [SshPassphrase, SshSaveSecret] (PrivateKey mode)
    // - [SshPassword, SshSaveSecret] (Password mode)
    // - [TestSsh, SaveAsTunnel] (if can_save_tunnel)
    // - [TestConnection, Save]

    fn left_ssh(self, state: SshNavState) -> Self {
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

    fn right_ssh(self, state: SshNavState) -> Self {
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
    fn is_input_field(self) -> bool {
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

impl SshNavState {
    fn new(
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
struct MainNavState {
    /// True for file-based databases (SQLite), false for server-based (PostgreSQL, MySQL, MariaDB)
    uses_file_form: bool,
    /// True if the driver has a "Use Connection URI" checkbox option
    has_uri_option: bool,
}

/// Edit state within the form - determines how keyboard input is handled
#[derive(Clone, Copy, PartialEq, Debug, Default)]
enum EditState {
    /// Navigating between fields with j/k, inputs don't have real focus
    #[default]
    Navigating,
    /// Actively typing in an input field (input has real focus)
    Editing,
}

#[derive(Clone, Copy, PartialEq)]
enum View {
    DriverSelect,
    EditForm,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum ActiveTab {
    Main,
    Ssh,
}

#[derive(Clone, Copy, PartialEq)]
enum TestStatus {
    None,
    Testing,
    Success,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum SshAuthSelection {
    PrivateKey,
    Password,
}

#[derive(Clone)]
struct DriverInfo {
    kind: DbKind,
    icon: dbflux_core::Icon,
    name: String,
    description: String,
}

pub struct ConnectionManagerWindow {
    app_state: Entity<AppState>,
    view: View,
    active_tab: ActiveTab,
    available_drivers: Vec<DriverInfo>,
    selected_driver: Option<Arc<dyn DbDriver>>,
    form_save_password: bool,
    form_save_ssh_secret: bool,
    editing_profile_id: Option<uuid::Uuid>,

    input_name: Entity<InputState>,
    /// Driver-specific field inputs, keyed by field ID.
    driver_inputs: HashMap<String, Entity<InputState>>,
    /// Password is separate due to visibility toggle and save checkbox UI.
    input_password: Entity<InputState>,

    ssh_enabled: bool,
    ssh_auth_method: SshAuthSelection,
    /// Checkbox states keyed by field ID (e.g., "use_uri" -> true).
    checkbox_states: HashMap<String, bool>,
    selected_ssh_tunnel_id: Option<Uuid>,
    ssh_tunnel_dropdown: Entity<crate::ui::dropdown::Dropdown>,
    input_ssh_host: Entity<InputState>,
    input_ssh_port: Entity<InputState>,
    input_ssh_user: Entity<InputState>,
    input_ssh_key_path: Entity<InputState>,
    input_ssh_key_passphrase: Entity<InputState>,
    input_ssh_password: Entity<InputState>,

    validation_errors: Vec<String>,
    test_status: TestStatus,
    test_error: Option<String>,
    ssh_test_status: TestStatus,
    ssh_test_error: Option<String>,
    pending_ssh_key_path: Option<String>,
    pending_ssh_tunnel_selection: Option<Uuid>,

    show_password: bool,
    show_ssh_passphrase: bool,
    show_ssh_password: bool,

    // Keyboard navigation state
    focus_handle: FocusHandle,
    keymap: &'static KeymapStack,
    driver_focus: DriverFocus,
    form_focus: FormFocus,
    edit_state: EditState,

    // Scroll handle for form content
    form_scroll_handle: ScrollHandle,

    // Dropdown state
    ssh_tunnel_uuids: Vec<Uuid>,
    _subscriptions: Vec<Subscription>,

    // Target folder for new connections
    target_folder_id: Option<Uuid>,
}

impl ConnectionManagerWindow {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let available_drivers: Vec<DriverInfo> = app_state
            .read(cx)
            .drivers
            .values()
            .map(|driver| DriverInfo {
                kind: driver.kind(),
                icon: driver.metadata().icon,
                name: driver.display_name().to_string(),
                description: driver.description().to_string(),
            })
            .collect();

        let input_name = cx.new(|cx| InputState::new(window, cx).placeholder("Connection name"));
        let input_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Password")
                .masked(true)
        });

        let input_ssh_host =
            cx.new(|cx| InputState::new(window, cx).placeholder("bastion.example.com"));
        let input_ssh_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("22")
                .default_value("22")
        });
        let input_ssh_user = cx.new(|cx| InputState::new(window, cx).placeholder("ec2-user"));
        let input_ssh_key_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("~/.ssh/id_rsa"));
        let input_ssh_key_passphrase = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Key passphrase (optional)")
                .masked(true)
        });
        let input_ssh_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("SSH password")
                .masked(true)
        });

        let ssh_tunnel_dropdown =
            cx.new(|_cx| Dropdown::new("ssh-tunnel-dropdown").placeholder("Select SSH Tunnel"));

        let dropdown_subscription = cx.subscribe(
            &ssh_tunnel_dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.handle_ssh_tunnel_dropdown_selection(event, cx);
            },
        );

        // Helper to create input subscriptions for handling Enter/Blur
        fn subscribe_input(
            cx: &mut Context<ConnectionManagerWindow>,
            window: &mut Window,
            input: &Entity<InputState>,
        ) -> Subscription {
            cx.subscribe_in(
                input,
                window,
                |this, _, event: &InputEvent, window, cx| match event {
                    InputEvent::PressEnter { secondary: false } => {
                        this.exit_edit_mode(window, cx);
                        this.focus_down(cx);
                    }
                    InputEvent::Blur => {
                        this.exit_edit_mode(window, cx);
                    }
                    _ => {}
                },
            )
        }

        let mut subscriptions = vec![dropdown_subscription];
        subscriptions.push(subscribe_input(cx, window, &input_name));
        subscriptions.push(subscribe_input(cx, window, &input_password));
        subscriptions.push(subscribe_input(cx, window, &input_ssh_host));
        subscriptions.push(subscribe_input(cx, window, &input_ssh_port));
        subscriptions.push(subscribe_input(cx, window, &input_ssh_user));
        subscriptions.push(subscribe_input(cx, window, &input_ssh_key_path));
        subscriptions.push(subscribe_input(cx, window, &input_ssh_key_passphrase));
        subscriptions.push(subscribe_input(cx, window, &input_ssh_password));

        let focus_handle = cx.focus_handle();
        window.focus(&focus_handle);

        Self {
            app_state,
            view: View::DriverSelect,
            active_tab: ActiveTab::Main,
            available_drivers,
            selected_driver: None,
            form_save_password: false,
            form_save_ssh_secret: false,
            editing_profile_id: None,
            input_name,
            driver_inputs: HashMap::new(),
            input_password,
            ssh_enabled: false,
            ssh_auth_method: SshAuthSelection::PrivateKey,
            checkbox_states: HashMap::new(),
            selected_ssh_tunnel_id: None,
            ssh_tunnel_dropdown,
            input_ssh_host,
            input_ssh_port,
            input_ssh_user,
            input_ssh_key_path,
            input_ssh_key_passphrase,
            input_ssh_password,
            validation_errors: Vec::new(),
            test_status: TestStatus::None,
            test_error: None,
            ssh_test_status: TestStatus::None,
            ssh_test_error: None,
            pending_ssh_key_path: None,
            pending_ssh_tunnel_selection: None,
            show_password: false,
            show_ssh_passphrase: false,
            show_ssh_password: false,
            focus_handle,
            keymap: crate::keymap::default_keymap(),
            driver_focus: DriverFocus::First,
            form_focus: FormFocus::Name,
            edit_state: EditState::Navigating,
            form_scroll_handle: ScrollHandle::new(),
            ssh_tunnel_uuids: Vec::new(),
            _subscriptions: subscriptions,
            target_folder_id: None,
        }
    }

    pub fn new_in_folder(
        app_state: Entity<AppState>,
        folder_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut instance = Self::new(app_state, window, cx);
        instance.target_folder_id = Some(folder_id);
        instance
    }

    pub fn new_for_edit(
        app_state: Entity<AppState>,
        profile: &ConnectionProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut instance = Self::new(app_state.clone(), window, cx);
        instance.editing_profile_id = Some(profile.id);

        let driver = app_state.read(cx).drivers.get(&profile.kind()).cloned();
        instance.selected_driver = driver.clone();
        instance.form_save_password = profile.save_password;
        instance.view = View::EditForm;

        if let Some(driver) = &driver {
            let form = driver.form_definition();
            instance.create_driver_inputs(form, window, cx);
            let values = driver.extract_values(&profile.config);
            instance.apply_form_values(&values, form, window, cx);
        }

        instance.input_name.update(cx, |state, cx| {
            state.set_value(&profile.name, window, cx);
        });

        if let Some(ssh) = profile.config.ssh_tunnel() {
            instance.ssh_enabled = true;
            instance.input_ssh_host.update(cx, |state, cx| {
                state.set_value(&ssh.host, window, cx);
            });
            instance.input_ssh_port.update(cx, |state, cx| {
                state.set_value(ssh.port.to_string(), window, cx);
            });
            instance.input_ssh_user.update(cx, |state, cx| {
                state.set_value(&ssh.user, window, cx);
            });

            match &ssh.auth_method {
                SshAuthMethod::PrivateKey { key_path } => {
                    instance.ssh_auth_method = SshAuthSelection::PrivateKey;
                    if let Some(path) = key_path {
                        let path_str: String = path.to_string_lossy().into_owned();
                        instance.input_ssh_key_path.update(cx, |state, cx| {
                            state.set_value(path_str, window, cx);
                        });
                    }
                }
                SshAuthMethod::Password => {
                    instance.ssh_auth_method = SshAuthSelection::Password;
                }
            }

            if let Some(ssh_secret) = app_state.read(cx).get_ssh_password(profile) {
                match instance.ssh_auth_method {
                    SshAuthSelection::PrivateKey => {
                        instance.input_ssh_key_passphrase.update(cx, |state, cx| {
                            state.set_value(&ssh_secret, window, cx);
                        });
                    }
                    SshAuthSelection::Password => {
                        instance.input_ssh_password.update(cx, |state, cx| {
                            state.set_value(&ssh_secret, window, cx);
                        });
                    }
                }
                instance.form_save_ssh_secret = true;
            }
        }

        instance
    }

    fn select_driver(&mut self, kind: DbKind, window: &mut Window, cx: &mut Context<Self>) {
        let driver = self.app_state.read(cx).drivers.get(&kind).cloned();
        self.selected_driver = driver.clone();
        self.form_save_password = false;
        self.ssh_enabled = false;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_ssh_secret = false;
        self.active_tab = ActiveTab::Main;
        self.validation_errors.clear();
        self.test_status = TestStatus::None;
        self.test_error = None;

        self.input_name.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        if let Some(driver) = driver {
            self.create_driver_inputs(driver.form_definition(), window, cx);
        }

        self.view = View::EditForm;
        self.edit_state = EditState::Navigating;
        self.form_focus = FormFocus::Name;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    /// Create input states from the driver's form definition.
    fn create_driver_inputs(
        &mut self,
        form: &dbflux_core::DriverFormDef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.driver_inputs.clear();

        let fields: Vec<&FormFieldDef> = form
            .tabs
            .iter()
            .filter(|tab| tab.id != "ssh")
            .flat_map(|tab| tab.sections.iter())
            .flat_map(|section| section.fields.iter())
            .filter(|field| field.id != "password")
            .collect();

        for field in fields {
            let placeholder = field.placeholder;
            let default_value = field.default_value;
            let is_masked = field.kind == FormFieldKind::Password;

            let input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder(placeholder);
                if !default_value.is_empty() {
                    state = state.default_value(default_value);
                }
                if is_masked {
                    state = state.masked(true);
                }
                state
            });

            let subscription =
                cx.subscribe_in(&input, window, |this, _, event: &InputEvent, window, cx| {
                    match event {
                        InputEvent::PressEnter { secondary: false } => {
                            this.exit_edit_mode(window, cx);
                            this.focus_down(cx);
                        }
                        InputEvent::Blur => {
                            this.exit_edit_mode(window, cx);
                        }
                        _ => {}
                    }
                });
            self._subscriptions.push(subscription);

            self.driver_inputs.insert(field.id.to_string(), input);
        }
    }

    fn apply_form_values(
        &mut self,
        values: &dbflux_core::FormValues,
        form: &DriverFormDef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for tab in form.tabs {
            for section in tab.sections {
                for field in section.fields {
                    if field.kind == FormFieldKind::Checkbox {
                        let is_checked = values.get(field.id).map(|v| v == "true").unwrap_or(false);
                        self.checkbox_states
                            .insert(field.id.to_string(), is_checked);
                    }
                }
            }
        }

        for (field_id, value) in values {
            if let Some(input) = self.driver_inputs.get(field_id) {
                input.update(cx, |state, cx| {
                    state.set_value(value, window, cx);
                });
            }
        }
    }

    fn collect_form_values(
        &self,
        form: &DriverFormDef,
        cx: &Context<Self>,
    ) -> dbflux_core::FormValues {
        let mut values = HashMap::new();

        for tab in form.tabs {
            for section in tab.sections {
                for field in section.fields {
                    if field.kind == FormFieldKind::Checkbox {
                        let is_checked =
                            self.checkbox_states.get(field.id).copied().unwrap_or(false);
                        values.insert(
                            field.id.to_string(),
                            if is_checked {
                                "true".to_string()
                            } else {
                                String::new()
                            },
                        );
                    }
                }
            }
        }

        for (field_id, input) in &self.driver_inputs {
            if !values.contains_key(field_id) {
                values.insert(field_id.clone(), input.read(cx).value().to_string());
            }
        }

        values
    }

    fn back_to_driver_select(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        window.focus(&self.focus_handle);
        self.view = View::DriverSelect;
        self.selected_driver = None;
        self.validation_errors.clear();
        self.test_status = TestStatus::None;
        self.test_error = None;
        cx.notify();
    }

    fn handle_ssh_tunnel_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if let Some(uuid) = self.ssh_tunnel_uuids.get(event.index).copied() {
            self.pending_ssh_tunnel_selection = Some(uuid);
            cx.notify();
        }
    }

    fn apply_ssh_tunnel(
        &mut self,
        tunnel: &SshTunnelProfile,
        secret: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.selected_ssh_tunnel_id = Some(tunnel.id);
        self.ssh_enabled = true;

        self.input_ssh_host.update(cx, |state, cx| {
            state.set_value(&tunnel.config.host, window, cx);
        });
        self.input_ssh_port.update(cx, |state, cx| {
            state.set_value(tunnel.config.port.to_string(), window, cx);
        });
        self.input_ssh_user.update(cx, |state, cx| {
            state.set_value(&tunnel.config.user, window, cx);
        });

        match &tunnel.config.auth_method {
            SshAuthMethod::PrivateKey { key_path } => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                if let Some(path) = key_path {
                    self.input_ssh_key_path.update(cx, |state, cx| {
                        state.set_value(path.to_string_lossy().to_string(), window, cx);
                    });
                }
                if let Some(ref passphrase) = secret {
                    self.input_ssh_key_passphrase.update(cx, |state, cx| {
                        state.set_value(passphrase, window, cx);
                    });
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(ref password) = secret {
                    self.input_ssh_password.update(cx, |state, cx| {
                        state.set_value(password, window, cx);
                    });
                }
            }
        }

        self.form_save_ssh_secret = tunnel.save_secret && secret.is_some();
        self.ssh_test_status = TestStatus::None;
        self.ssh_test_error = None;
        cx.notify();
    }

    fn clear_ssh_tunnel_selection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_ssh_tunnel_id = None;

        self.input_ssh_host.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_port.update(cx, |state, cx| {
            state.set_value("22", window, cx);
        });
        self.input_ssh_user.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_key_path.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_ssh_secret = false;
        self.ssh_test_status = TestStatus::None;
        self.ssh_test_error = None;
        cx.notify();
    }

    fn selected_kind(&self) -> Option<DbKind> {
        self.selected_driver.as_ref().map(|d| d.kind())
    }

    /// Returns true if this driver uses the server form (host/port/user/database)
    /// instead of a file-based form (path only).
    #[allow(dead_code)]
    fn uses_server_form(&self) -> bool {
        let Some(driver) = &self.selected_driver else {
            return false;
        };
        !driver.form_definition().uses_file_form()
    }

    /// Returns true if this driver uses a file-based form (path only).
    fn uses_file_form(&self) -> bool {
        let Some(driver) = &self.selected_driver else {
            return false;
        };
        driver.form_definition().uses_file_form()
    }

    /// Returns true if this driver supports SSH tunneling.
    fn supports_ssh(&self) -> bool {
        let Some(driver) = &self.selected_driver else {
            return false;
        };
        driver.form_definition().supports_ssh()
    }

    fn validate_form(&mut self, require_name: bool, cx: &mut Context<Self>) -> bool {
        self.validation_errors.clear();

        if require_name {
            let name = self.input_name.read(cx).value().to_string();
            if name.trim().is_empty() {
                self.validation_errors
                    .push("Connection name is required".to_string());
            }
        }

        let Some(driver) = &self.selected_driver else {
            self.validation_errors
                .push("No driver selected".to_string());
            return false;
        };

        let form = driver.form_definition();

        for tab in form.tabs.iter().filter(|t| t.id != "ssh") {
            for section in tab.sections {
                for field in section.fields {
                    if field.id == "password" || field.kind == FormFieldKind::Checkbox {
                        continue;
                    }

                    let field_enabled = self.is_field_enabled(field);
                    if !field_enabled {
                        continue;
                    }

                    let value = self
                        .driver_inputs
                        .get(field.id)
                        .map(|input| input.read(cx).value().to_string())
                        .unwrap_or_default();

                    if field.required && value.trim().is_empty() {
                        self.validation_errors
                            .push(format!("{} is required", field.label));
                    }

                    if !value.trim().is_empty()
                        && field.kind == FormFieldKind::Number
                        && value.parse::<u16>().is_err()
                    {
                        self.validation_errors
                            .push(format!("{} must be a valid number", field.label));
                    }
                }
            }
        }

        if self.ssh_enabled && form.supports_ssh() {
            let ssh_host = self.input_ssh_host.read(cx).value().to_string();
            if ssh_host.trim().is_empty() {
                self.validation_errors
                    .push("SSH Host is required when SSH is enabled".to_string());
            }

            let ssh_user = self.input_ssh_user.read(cx).value().to_string();
            if ssh_user.trim().is_empty() {
                self.validation_errors
                    .push("SSH User is required when SSH is enabled".to_string());
            }

            let ssh_port_str = self.input_ssh_port.read(cx).value().to_string();
            if !ssh_port_str.trim().is_empty() && ssh_port_str.parse::<u16>().is_err() {
                self.validation_errors
                    .push("SSH Port must be a valid number".to_string());
            }
        }

        self.validation_errors.is_empty()
    }

    fn expand_path(path_str: &str) -> PathBuf {
        if path_str.starts_with("~/") {
            std::env::var("HOME")
                .map(|home| PathBuf::from(home).join(&path_str[2..]))
                .unwrap_or_else(|_| PathBuf::from(path_str))
        } else {
            PathBuf::from(path_str)
        }
    }

    fn build_ssh_config(&self, cx: &Context<Self>) -> Option<SshTunnelConfig> {
        if !self.ssh_enabled {
            return None;
        }

        let host = self.input_ssh_host.read(cx).value().to_string();
        let port = self.input_ssh_port.read(cx).value().parse().unwrap_or(22);
        let user = self.input_ssh_user.read(cx).value().to_string();

        let auth_method = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                let key_path_str = self.input_ssh_key_path.read(cx).value().to_string();
                let key_path = if key_path_str.trim().is_empty() {
                    None
                } else {
                    Some(Self::expand_path(&key_path_str))
                };
                SshAuthMethod::PrivateKey { key_path }
            }
            SshAuthSelection::Password => SshAuthMethod::Password,
        };

        Some(SshTunnelConfig {
            host,
            port,
            user,
            auth_method,
        })
    }

    fn save_current_ssh_as_tunnel(&mut self, cx: &mut Context<Self>) {
        let Some(config) = self.build_ssh_config(cx) else {
            return;
        };

        let name = format!("{}@{}", config.user, config.host);
        let secret = self.get_ssh_secret(cx);

        let tunnel = SshTunnelProfile {
            id: Uuid::new_v4(),
            name,
            config,
            save_secret: self.form_save_ssh_secret,
        };

        self.app_state.update(cx, |state, cx| {
            if tunnel.save_secret
                && let Some(ref secret) = secret
            {
                state.save_ssh_tunnel_secret(&tunnel, secret);
            }
            state.add_ssh_tunnel(tunnel.clone());
            cx.emit(crate::app::AppStateChanged);
        });

        self.selected_ssh_tunnel_id = Some(tunnel.id);
        cx.notify();
    }

    fn build_config(&self, cx: &Context<Self>) -> Option<DbConfig> {
        let driver = self.selected_driver.as_ref()?;
        let values = self.collect_form_values(driver.form_definition(), cx);

        let mut config = match driver.build_config(&values) {
            Ok(config) => config,
            Err(e) => {
                log::error!("Failed to build config: {}", e);
                return None;
            }
        };

        let ssh_tunnel = self.build_ssh_config(cx);
        let ssh_tunnel_profile_id = self.selected_ssh_tunnel_id;

        match &mut config {
            DbConfig::Postgres {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::MySQL {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            }
            | DbConfig::MongoDB {
                ssh_tunnel: tunnel,
                ssh_tunnel_profile_id: profile_id,
                ..
            } => {
                *tunnel = ssh_tunnel;
                *profile_id = ssh_tunnel_profile_id;
            }
            DbConfig::SQLite { .. } => {}
        }

        Some(config)
    }

    fn build_profile(&self, cx: &Context<Self>) -> Option<ConnectionProfile> {
        let name = self.input_name.read(cx).value().to_string();
        let kind = self.selected_kind()?;
        let config = self.build_config(cx)?;

        let mut profile = if let Some(existing_id) = self.editing_profile_id {
            let mut p = ConnectionProfile::new_with_kind(name, kind, config);
            p.id = existing_id;
            p
        } else {
            ConnectionProfile::new_with_kind(name, kind, config)
        };

        profile.save_password = self.form_save_password;
        Some(profile)
    }

    fn get_ssh_secret(&self, cx: &Context<Self>) -> Option<String> {
        if !self.ssh_enabled {
            return None;
        }

        let secret = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                self.input_ssh_key_passphrase.read(cx).value().to_string()
            }
            SshAuthSelection::Password => self.input_ssh_password.read(cx).value().to_string(),
        };

        if secret.is_empty() {
            None
        } else {
            Some(secret)
        }
    }

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.validate_form(true, cx) {
            cx.notify();
            return;
        }

        let Some(profile) = self.build_profile(cx) else {
            return;
        };

        let password = self.input_password.read(cx).value().to_string();
        let ssh_secret = self.get_ssh_secret(cx);
        let is_edit = self.editing_profile_id.is_some();

        info!(
            "{} profile: {}, save_password={}, password_len={}, ssh_enabled={}, ssh_auth={:?}",
            if is_edit { "Updating" } else { "Saving" },
            profile.name,
            profile.save_password,
            password.len(),
            self.ssh_enabled,
            self.ssh_auth_method
        );

        self.app_state.update(cx, |state, cx| {
            if profile.save_password && !password.is_empty() {
                info!("Saving password to keyring for profile {}", profile.id);
                state.save_password(&profile, &password);
            } else if !profile.save_password {
                state.delete_password(&profile);
            }

            if self.form_save_ssh_secret {
                if let Some(ref secret) = ssh_secret {
                    info!("Saving SSH secret to keyring for profile {}", profile.id);
                    state.save_ssh_password(&profile, secret);
                }
            } else {
                state.delete_ssh_password(&profile);
            }

            if is_edit {
                state.update_profile(profile);
            } else {
                state.add_profile_in_folder(profile, self.target_folder_id);
            }

            cx.emit(crate::app::AppStateChanged);
        });

        cx.emit(DismissEvent);
        window.remove_window();
    }

    fn test_connection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.validate_form(false, cx) {
            cx.notify();
            return;
        }

        self.test_status = TestStatus::Testing;
        self.test_error = None;
        cx.notify();

        let Some(profile) = self.build_profile(cx) else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("Failed to build profile".to_string());
            cx.notify();
            return;
        };

        let password = self.input_password.read(cx).value().to_string();
        let password_opt = if password.is_empty() {
            None
        } else {
            Some(password)
        };

        let ssh_secret = self.get_ssh_secret(cx);

        let Some(driver) = self.selected_driver.clone() else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("No driver selected".to_string());
            cx.notify();
            return;
        };

        let profile_name = profile.name.clone();
        let this = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            driver.connect_with_secrets(&profile, password_opt.as_deref(), ssh_secret.as_deref())
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(_connection) => {
                            info!("Test connection successful for {}", profile_name);
                            this.test_status = TestStatus::Success;
                            this.test_error = None;
                        }
                        Err(e) => {
                            info!("Test connection failed: {:?}", e);
                            this.test_status = TestStatus::Failed;
                            this.test_error = Some(format!("{:?}", e));
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn test_ssh_connection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.ssh_enabled {
            return;
        }

        self.ssh_test_status = TestStatus::Testing;
        self.ssh_test_error = None;
        cx.notify();

        let Some(ssh_config) = self.build_ssh_config(cx) else {
            self.ssh_test_status = TestStatus::Failed;
            self.ssh_test_error = Some("SSH configuration incomplete".to_string());
            cx.notify();
            return;
        };

        let ssh_secret = self.get_ssh_secret(cx);

        let this = cx.entity().clone();

        // Use the same establish_session function that the actual connection uses
        let task = cx.background_executor().spawn(async move {
            match dbflux_ssh::establish_session(&ssh_config, ssh_secret.as_deref()) {
                Ok(_session) => Ok(()),
                Err(e) => Err(format!("{:?}", e)),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(()) => {
                            info!("SSH test connection successful");
                            this.ssh_test_status = TestStatus::Success;
                            this.ssh_test_error = None;
                        }
                        Err(e) => {
                            info!("SSH test connection failed: {}", e);
                            this.ssh_test_status = TestStatus::Failed;
                            this.ssh_test_error = Some(e);
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn browse_ssh_key(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();

        let start_dir = dirs::home_dir().map(|h| h.join(".ssh")).unwrap_or_default();

        let task = cx.background_executor().spawn(async move {
            let dialog = rfd::FileDialog::new()
                .set_title("Select SSH Private Key")
                .set_directory(&start_dir);

            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let path = task.await;

            if let Some(path) = path {
                cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_ssh_key_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
                .ok();
            }
        })
        .detach();
    }

    fn active_context(&self) -> ContextId {
        ContextId::ConnectionManager
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let chord = KeyChord::from_gpui(&event.keystroke);
        let context = self.active_context();

        if let Some(command) = self.keymap.resolve(context, &chord) {
            return self.dispatch_command(command, window, cx);
        }

        false
    }

    fn dispatch_command(
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
        // If the SSH tunnel dropdown is open, route commands to it first
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
            // Vertical navigation (j/k)
            Command::SelectNext => {
                self.focus_down(cx);
                true
            }
            Command::SelectPrev => {
                self.focus_up(cx);
                true
            }
            // Horizontal navigation (h/l)
            Command::FocusLeft => {
                self.focus_left(cx);
                true
            }
            Command::FocusRight => {
                self.focus_right(cx);
                true
            }
            // Tab switching (C-h/C-l)
            Command::CycleFocusForward => {
                self.next_tab(cx);
                true
            }
            Command::CycleFocusBackward => {
                self.prev_tab(cx);
                true
            }
            // Actions
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
                // Enter while editing: exit edit mode and move to next field
                self.exit_edit_mode(window, cx);
                self.focus_down(cx);
                true
            }
            _ => false,
        }
    }

    fn exit_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.edit_state = EditState::Navigating;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    /// Enter edit mode for a specific field (used when clicking on an input).
    fn enter_edit_mode_for_field(
        &mut self,
        field: FormFocus,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.form_focus = field;
        self.activate_focused_field(window, cx);
        cx.notify();
    }

    fn ssh_nav_state(&self, cx: &Context<Self>) -> SshNavState {
        let has_tunnels = !self.app_state.read(cx).ssh_tunnels.is_empty();
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

    fn main_nav_state(&self) -> MainNavState {
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

    /// Returns the scroll index for the current form focus.
    /// Maps focus to section index in the Vec returned by render_*_tab.
    fn focus_scroll_index(&self) -> usize {
        use FormFocus::*;
        match self.active_tab {
            ActiveTab::Main => match self.form_focus {
                // Section 0: Server (UseUri, Host, Port, Database)
                UseUri | Host | Port | Database => 0,
                // Section 1: Authentication (User, Password)
                User | Password | PasswordSave => 1,
                // Footer buttons are outside scroll area
                _ => 0,
            },
            ActiveTab::Ssh => {
                // Sections depend on whether tunnel_selector is present
                // tunnel_selector is added when ssh_enabled && !ssh_tunnels.is_empty()
                let has_tunnels = self.ssh_enabled && !self.ssh_tunnel_uuids.is_empty();
                let offset = if has_tunnels { 1 } else { 0 };

                match self.form_focus {
                    // Section 0: SSH toggle (always present)
                    SshEnabled => 0,
                    // Section 1: Tunnel selector (only if has_tunnels)
                    SshTunnelSelector | SshTunnelClear => 1,
                    // SSH Server section
                    SshHost | SshPort | SshUser => 1 + offset,
                    // Auth selector section
                    SshAuthPrivateKey | SshAuthPassword => 2 + offset,
                    // Auth inputs section
                    SshKeyPath | SshKeyBrowse | SshPassphrase | SshSaveSecret | SshPassword => {
                        3 + offset
                    }
                    // Test/Save section
                    TestSsh | SaveAsTunnel => 4 + offset,
                    // Footer buttons are outside scroll area
                    _ => 0,
                }
            }
        }
    }

    fn scroll_to_focused(&mut self) {
        let index = self.focus_scroll_index();
        self.form_scroll_handle.scroll_to_item(index);
    }

    fn focus_down(&mut self, cx: &mut Context<Self>) {
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
            // Reset focus to first field of the new tab
            self.form_focus = match self.active_tab {
                ActiveTab::Main => FormFocus::Name,
                ActiveTab::Ssh => FormFocus::SshEnabled,
            };
            self.scroll_to_focused();
            cx.notify();
        }
    }

    fn prev_tab(&mut self, cx: &mut Context<Self>) {
        self.next_tab(cx); // Same behavior for 2 tabs
    }

    fn activate_focused_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.form_focus {
            // Input fields - enter edit mode
            FormFocus::Name => {
                self.edit_state = EditState::Editing;
                self.input_name.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            // Driver-specific fields (dynamic)
            FormFocus::Host | FormFocus::Port | FormFocus::Database | FormFocus::User => {
                if let Some(input) = self.input_for_focus(self.form_focus).cloned() {
                    self.edit_state = EditState::Editing;
                    input.update(cx, |state, cx| {
                        state.focus(window, cx);
                    });
                }
            }

            // Password (handled separately for special UI)
            FormFocus::Password => {
                self.edit_state = EditState::Editing;
                self.input_password.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }

            // SSH fields (shared across drivers)
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

            // SSH Browse button - trigger file picker
            FormFocus::SshKeyBrowse => {
                self.browse_ssh_key(window, cx);
            }

            // Toggles - just toggle the value, stay in Navigating mode
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
                // Ensure items are populated before opening (items are normally set in render)
                let ssh_tunnels = self.app_state.read(cx).ssh_tunnels.clone();
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

            // SSH tunnel clear button - clear the selected tunnel
            FormFocus::SshTunnelClear => {
                self.clear_ssh_tunnel_selection(window, cx);
            }

            // SSH auth method selection
            FormFocus::SshAuthPrivateKey => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
            }
            FormFocus::SshAuthPassword => {
                self.ssh_auth_method = SshAuthSelection::Password;
            }

            // SSH-specific actions
            FormFocus::TestSsh => {
                self.test_ssh_connection(window, cx);
            }
            FormFocus::SaveAsTunnel => {
                self.save_current_ssh_as_tunnel(cx);
            }

            // Main actions - execute them, stay in Navigating mode
            FormFocus::TestConnection => {
                self.test_connection(window, cx);
            }
            FormFocus::Save => {
                self.save_profile(window, cx);
            }
        }
        cx.notify();
    }

    fn render_driver_select(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let drivers = self.available_drivers.clone();
        let focused_idx = self.driver_focus.index();
        let ring_color = theme.ring;

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .p_3()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("New Connection"),
                    ),
            )
            .child(
                div().flex_1().p_3().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .mb_2()
                                .child("Select database type (j/k to navigate, Enter to select)"),
                        )
                        .children(drivers.into_iter().enumerate().map(|(idx, driver_info)| {
                            let kind = driver_info.kind;
                            let icon = driver_info.icon;
                            let is_focused = idx == focused_idx;

                            div()
                                .rounded(px(6.0))
                                .border_2()
                                .when(is_focused, |d| d.border_color(ring_color))
                                .when(!is_focused, |d| d.border_color(gpui::transparent_black()))
                                .child(
                                    ListItem::new(("driver", idx))
                                        .py(px(8.0))
                                        .on_click(cx.listener(move |this, _, window, cx| {
                                            this.select_driver(kind, window, cx);
                                        }))
                                        .child(
                                            div()
                                                .flex()
                                                .flex_row()
                                                .items_center()
                                                .gap_3()
                                                .child(
                                                    svg()
                                                        .path(AppIcon::from_icon(icon).path())
                                                        .size_8()
                                                        .text_color(theme.foreground),
                                                )
                                                .child(
                                                    div()
                                                        .flex()
                                                        .flex_col()
                                                        .gap_1()
                                                        .child(
                                                            div()
                                                                .text_sm()
                                                                .font_weight(FontWeight::SEMIBOLD)
                                                                .child(driver_info.name),
                                                        )
                                                        .child(
                                                            div()
                                                                .text_xs()
                                                                .text_color(theme.muted_foreground)
                                                                .child(driver_info.description),
                                                        ),
                                                ),
                                        ),
                                )
                        })),
                ),
            )
            .child(
                div()
                    .p_3()
                    .border_t_1()
                    .border_color(theme.border)
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("j/k Navigate  h/l Horizontal  Enter Select  Esc Close"),
            )
    }

    fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active_tab = self.active_tab;

        div()
            .flex()
            .items_center()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .id("tab-main")
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(active_tab == ActiveTab::Main, |d| {
                        d.border_color(theme.primary).text_color(theme.foreground)
                    })
                    .when(active_tab != ActiveTab::Main, |d| {
                        d.border_color(gpui::transparent_black())
                            .text_color(theme.muted_foreground)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = ActiveTab::Main;
                        cx.notify();
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(svg().path(AppIcon::Plug.path()).size_4().text_color(
                                if active_tab == ActiveTab::Main {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(div().text_sm().child("Main")),
                    ),
            )
            .child(
                div()
                    .id("tab-ssh")
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(active_tab == ActiveTab::Ssh, |d| {
                        d.border_color(theme.primary).text_color(theme.foreground)
                    })
                    .when(active_tab != ActiveTab::Ssh, |d| {
                        d.border_color(gpui::transparent_black())
                            .text_color(theme.muted_foreground)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = ActiveTab::Ssh;
                        cx.notify();
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(
                                svg()
                                    .path(AppIcon::FingerprintPattern.path())
                                    .size_4()
                                    .text_color(if active_tab == ActiveTab::Ssh {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            )
                            .child(div().text_sm().child("SSH"))
                            .when(self.ssh_enabled, |d| {
                                d.child(
                                    div()
                                        .w(px(6.0))
                                        .h(px(6.0))
                                        .rounded_full()
                                        .bg(gpui::rgb(0x22C55E)),
                                )
                            }),
                    ),
            )
    }

    fn render_main_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let Some(driver) = &self.selected_driver else {
            return Vec::new();
        };

        let keyring_available = self.app_state.read(cx).secret_store_available();
        let requires_password = driver.requires_password();
        let save_password = self.form_save_password;

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Main;
        let focus = self.form_focus;

        let ring_color = cx.theme().ring;

        let form_def = driver.form_definition();
        let Some(main_tab) = form_def.main_tab() else {
            return Vec::new();
        };

        let mut sections = self.render_form_tab(main_tab, false, show_focus, ring_color, cx);

        // Add password field to the last section (Authentication) if driver requires password.
        // Password is not included in dynamic rendering because it has special UI
        // (visibility toggle, save checkbox).
        if requires_password {
            let password_field = self.render_password_field(
                show_focus && focus == FormFocus::Password,
                show_focus && focus == FormFocus::PasswordSave,
                keyring_available,
                save_password,
                ring_color,
                cx,
            );

            sections.push(password_field);
        }

        sections
    }

    fn render_password_field(
        &self,
        password_focused: bool,
        checkbox_focused: bool,
        show_save_checkbox: bool,
        save_password: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Password"),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    // Password input with focus ring
                    .child(
                        div()
                            .flex_1()
                            .rounded(px(4.0))
                            .border_2()
                            .when(password_focused, |d| d.border_color(ring_color))
                            .when(!password_focused, |d| {
                                d.border_color(gpui::transparent_black())
                            })
                            .p(px(2.0))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.enter_edit_mode_for_field(FormFocus::Password, window, cx);
                                }),
                            )
                            .child(Input::new(&self.input_password)),
                    )
                    .child(
                        Self::render_password_toggle(self.show_password, "toggle-password", &theme)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.show_password = !this.show_password;
                                cx.notify();
                            })),
                    )
                    // Save checkbox with focus ring
                    .when(show_save_checkbox, |d| {
                        d.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .rounded(px(4.0))
                                .border_2()
                                .when(checkbox_focused, |dd| dd.border_color(ring_color))
                                .when(!checkbox_focused, |dd| {
                                    dd.border_color(gpui::transparent_black())
                                })
                                .p(px(2.0))
                                .child(
                                    Checkbox::new("save-password")
                                        .checked(save_password)
                                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                            this.form_save_password = *checked;
                                            cx.notify();
                                        })),
                                )
                                .child(div().text_sm().child("Save")),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_ssh_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let ssh_enabled = self.ssh_enabled;
        let ssh_auth_method = self.ssh_auth_method;
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let save_ssh_secret = self.form_save_ssh_secret;
        let ssh_tunnels = self.app_state.read(cx).ssh_tunnels.clone();
        let selected_tunnel_id = self.selected_ssh_tunnel_id;

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Ssh;
        let focus = self.form_focus;

        // Get ring_color early, before mutable borrows
        let ring_color = cx.theme().ring;

        let ssh_enabled_focused = show_focus && focus == FormFocus::SshEnabled;
        let ssh_toggle = div()
            .flex()
            .items_center()
            .gap_2()
            .rounded(px(4.0))
            .border_2()
            .when(ssh_enabled_focused, |d| d.border_color(ring_color))
            .when(!ssh_enabled_focused, |d| {
                d.border_color(gpui::transparent_black())
            })
            .p(px(2.0))
            .child(
                Checkbox::new("ssh-enabled")
                    .checked(ssh_enabled)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.ssh_enabled = *checked;
                        cx.notify();
                    })),
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Use SSH Tunnel"),
            );

        let tunnel_items: Vec<DropdownItem> = ssh_tunnels
            .iter()
            .map(|t| DropdownItem::with_value(&t.name, t.id.to_string()))
            .collect();
        self.ssh_tunnel_uuids = ssh_tunnels.iter().map(|t| t.id).collect();

        let selected_tunnel_index =
            selected_tunnel_id.and_then(|id| ssh_tunnels.iter().position(|t| t.id == id));

        let tunnel_selector_focused = show_focus && focus == FormFocus::SshTunnelSelector;
        let tunnel_clear_focused = show_focus && focus == FormFocus::SshTunnelClear;
        self.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(tunnel_items, cx);
            dropdown.set_selected_index(selected_tunnel_index, cx);
            let focus_color = if tunnel_selector_focused {
                Some(ring_color)
            } else {
                None
            };
            dropdown.set_focus_ring(focus_color, cx);
        });

        let tunnel_selector: Option<AnyElement> = if ssh_enabled && !ssh_tunnels.is_empty() {
            let selected_tunnel_name = selected_tunnel_id
                .and_then(|id| ssh_tunnels.iter().find(|t| t.id == id))
                .map(|t| t.name.clone());

            Some(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(cx.theme().muted_foreground)
                            .child("SSH Tunnel"),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(div().flex_1().child(self.ssh_tunnel_dropdown.clone()))
                            .when(selected_tunnel_name.is_some(), |d| {
                                d.child(
                                    div()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(tunnel_clear_focused, |dd| {
                                            dd.border_color(ring_color)
                                        })
                                        .when(!tunnel_clear_focused, |dd| {
                                            dd.border_color(gpui::transparent_black())
                                        })
                                        .child(
                                            Button::new("clear-ssh-tunnel")
                                                .label("Clear")
                                                .small()
                                                .ghost()
                                                .on_click(cx.listener(|this, _, window, cx| {
                                                    this.clear_ssh_tunnel_selection(window, cx);
                                                })),
                                        ),
                                )
                            }),
                    )
                    .into_any_element(),
            )
        } else {
            None
        };

        let auth_private_key_focused = show_focus && focus == FormFocus::SshAuthPrivateKey;
        let auth_password_focused = show_focus && focus == FormFocus::SshAuthPassword;
        let (auth_selector, auth_inputs) = if ssh_enabled {
            let selector = self
                .render_ssh_auth_selector(
                    ssh_auth_method,
                    auth_private_key_focused,
                    auth_password_focused,
                    ring_color,
                    cx,
                )
                .into_any_element();
            let inputs = self
                .render_ssh_auth_inputs(
                    ssh_auth_method,
                    keyring_available,
                    save_ssh_secret,
                    show_focus,
                    focus,
                    ring_color,
                    cx,
                )
                .into_any_element();
            (Some(selector), Some(inputs))
        } else {
            (None, None)
        };

        let theme = cx.theme().clone();
        let muted_fg = theme.muted_foreground;

        let ssh_server_section: Option<AnyElement> = if ssh_enabled {
            Some(
                self.render_section(
                    "SSH Server",
                    div()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .child(
                            div()
                                .id(2usize)
                                .flex()
                                .gap_3()
                                .child(div().flex_1().child(self.form_field_input(
                                    "Host",
                                    &self.input_ssh_host,
                                    true,
                                    show_focus && focus == FormFocus::SshHost,
                                    ring_color,
                                    FormFocus::SshHost,
                                    cx,
                                )))
                                .child(div().w(px(80.0)).child(self.form_field_input(
                                    "Port",
                                    &self.input_ssh_port,
                                    false,
                                    show_focus && focus == FormFocus::SshPort,
                                    ring_color,
                                    FormFocus::SshPort,
                                    cx,
                                ))),
                        )
                        .child(div().id(3usize).child(self.form_field_input(
                            "Username",
                            &self.input_ssh_user,
                            true,
                            show_focus && focus == FormFocus::SshUser,
                            ring_color,
                            FormFocus::SshUser,
                            cx,
                        ))),
                    &theme,
                )
                .into_any_element(),
            )
        } else {
            None
        };

        let ssh_test_section: Option<AnyElement> = if ssh_enabled {
            let ssh_test_status = self.ssh_test_status;
            let ssh_test_error = self.ssh_test_error.clone();

            let test_ssh_focused = show_focus && focus == FormFocus::TestSsh;
            let test_button = div()
                .rounded(px(4.0))
                .border_2()
                .when(test_ssh_focused, |d| d.border_color(ring_color))
                .when(!test_ssh_focused, |d| {
                    d.border_color(gpui::transparent_black())
                })
                .child(
                    Button::new("test-ssh")
                        .icon(Icon::new(IconName::ExternalLink))
                        .label("Test SSH")
                        .small()
                        .ghost()
                        .disabled(ssh_test_status == TestStatus::Testing)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.test_ssh_connection(window, cx);
                        })),
                );

            let status_el: Option<AnyElement> = match ssh_test_status {
                TestStatus::None => None,
                TestStatus::Testing => Some(
                    div()
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("Testing SSH connection...")
                        .into_any_element(),
                ),
                TestStatus::Success => Some(
                    div()
                        .text_sm()
                        .text_color(theme.success)
                        .child("SSH connection successful")
                        .into_any_element(),
                ),
                TestStatus::Failed => Some(
                    div()
                        .text_sm()
                        .text_color(theme.danger)
                        .child(
                            ssh_test_error.unwrap_or_else(|| "SSH connection failed".to_string()),
                        )
                        .into_any_element(),
                ),
            };

            let show_save_tunnel = self.selected_ssh_tunnel_id.is_none();
            let save_tunnel_button: Option<AnyElement> = if show_save_tunnel {
                let save_tunnel_focused = show_focus && focus == FormFocus::SaveAsTunnel;
                Some(
                    div()
                        .rounded(px(4.0))
                        .border_2()
                        .when(save_tunnel_focused, |d| d.border_color(ring_color))
                        .when(!save_tunnel_focused, |d| {
                            d.border_color(gpui::transparent_black())
                        })
                        .child(
                            Button::new("save-ssh-tunnel")
                                .icon(Icon::new(IconName::Plus))
                                .label("Save as tunnel")
                                .small()
                                .ghost()
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.save_current_ssh_as_tunnel(cx);
                                })),
                        )
                        .into_any_element(),
                )
            } else {
                None
            };

            Some(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .mt_2()
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(test_button)
                            .when_some(save_tunnel_button, |d, btn| d.child(btn)),
                    )
                    .when_some(status_el, |d, el| d.child(el))
                    .into_any_element(),
            )
        } else {
            None
        };

        let mut sections: Vec<AnyElement> = Vec::new();

        // Section 0: SSH toggle
        sections.push(ssh_toggle.into_any_element());

        // Section 1: Tunnel selector (conditional)
        if let Some(selector) = tunnel_selector {
            sections.push(selector);
        }

        // Section 2-3: SSH server section (contains Host/Port and User rows)
        if let Some(section) = ssh_server_section {
            sections.push(section);
        }

        // Section 4: Auth selector
        if let Some(selector) = auth_selector {
            sections.push(
                self.render_section("Authentication", selector, &theme)
                    .into_any_element(),
            );
        }

        // Section 5-6: Auth inputs
        if let Some(inputs) = auth_inputs {
            sections.push(inputs);
        }

        // Section 7: Test/Save SSH buttons
        if let Some(section) = ssh_test_section {
            sections.push(section);
        }

        // Disabled message (when SSH is off)
        if !ssh_enabled {
            sections.push(
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div().text_sm().text_color(muted_fg).child(
                            "Enable SSH tunnel to configure connection through a bastion host",
                        ),
                    )
                    .into_any_element(),
            );
        }

        sections
    }
    fn render_ssh_auth_selector(
        &self,
        current: SshAuthSelection,
        private_key_focused: bool,
        password_focused: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let click_key = cx.listener(|this, _, _, cx| {
            this.ssh_auth_method = SshAuthSelection::PrivateKey;
            cx.notify();
        });
        let click_pw = cx.listener(|this, _, _, cx| {
            this.ssh_auth_method = SshAuthSelection::Password;
            cx.notify();
        });

        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;

        div()
            .flex()
            .gap_4()
            .child(
                div()
                    .id("auth-private-key")
                    .flex()
                    .items_center()
                    .gap_2()
                    .cursor_pointer()
                    .rounded(px(4.0))
                    .border_2()
                    .when(private_key_focused, |d| d.border_color(ring_color))
                    .when(!private_key_focused, |d| {
                        d.border_color(gpui::transparent_black())
                    })
                    .p(px(2.0))
                    .on_click(click_key)
                    .child(self.render_radio_button(
                        current == SshAuthSelection::PrivateKey,
                        primary,
                        border,
                    ))
                    .child(div().text_sm().child("Private Key")),
            )
            .child(
                div()
                    .id("auth-password")
                    .flex()
                    .items_center()
                    .gap_2()
                    .cursor_pointer()
                    .rounded(px(4.0))
                    .border_2()
                    .when(password_focused, |d| d.border_color(ring_color))
                    .when(!password_focused, |d| {
                        d.border_color(gpui::transparent_black())
                    })
                    .p(px(2.0))
                    .on_click(click_pw)
                    .child(self.render_radio_button(
                        current == SshAuthSelection::Password,
                        primary,
                        border,
                    ))
                    .child(div().text_sm().child("Password")),
            )
    }

    fn render_radio_button(&self, selected: bool, primary: Hsla, border: Hsla) -> impl IntoElement {
        div()
            .w(px(16.0))
            .h(px(16.0))
            .rounded_full()
            .border_2()
            .border_color(if selected { primary } else { border })
            .when(selected, |d| {
                d.child(
                    div()
                        .absolute()
                        .top(px(3.0))
                        .left(px(3.0))
                        .w(px(6.0))
                        .h(px(6.0))
                        .rounded_full()
                        .bg(primary),
                )
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn render_ssh_auth_inputs(
        &self,
        auth_method: SshAuthSelection,
        keyring_available: bool,
        save_ssh_secret: bool,
        show_focus: bool,
        focus: FormFocus,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let passphrase_checkbox = if keyring_available {
            Some(
                Checkbox::new("save-ssh-passphrase")
                    .checked(save_ssh_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_ssh_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        let password_checkbox = if keyring_available {
            Some(
                Checkbox::new("save-ssh-password")
                    .checked(save_ssh_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_ssh_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        let theme = cx.theme();
        let muted_fg = theme.muted_foreground;

        let key_path_focused = show_focus && focus == FormFocus::SshKeyPath;
        let key_browse_focused = show_focus && focus == FormFocus::SshKeyBrowse;
        let passphrase_focused = show_focus && focus == FormFocus::SshPassphrase;
        let save_secret_focused = show_focus && focus == FormFocus::SshSaveSecret;
        let password_focused = show_focus && focus == FormFocus::SshPassword;

        match auth_method {
            SshAuthSelection::PrivateKey => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(
                    div()
                        .id(5usize)
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child("Private Key Path"),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(key_path_focused, |d| d.border_color(ring_color))
                                        .when(!key_path_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, window, cx| {
                                                this.enter_edit_mode_for_field(
                                                    FormFocus::SshKeyPath,
                                                    window,
                                                    cx,
                                                );
                                            }),
                                        )
                                        .child(Input::new(&self.input_ssh_key_path).small()),
                                )
                                .child(
                                    div()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(key_browse_focused, |d| d.border_color(ring_color))
                                        .when(!key_browse_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .child(
                                            Button::new("browse-ssh-key")
                                                .label("Browse")
                                                .small()
                                                .ghost()
                                                .on_click(cx.listener(|this, _, window, cx| {
                                                    this.browse_ssh_key(window, cx);
                                                })),
                                        ),
                                ),
                        ),
                )
                .child(
                    div()
                        .text_xs()
                        .text_color(muted_fg)
                        .child("Leave empty to use SSH agent or default keys (~/.ssh/id_rsa)"),
                )
                .child(
                    div()
                        .id(6usize)
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child("Key Passphrase"),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(passphrase_focused, |d| d.border_color(ring_color))
                                        .when(!passphrase_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, window, cx| {
                                                this.enter_edit_mode_for_field(
                                                    FormFocus::SshPassphrase,
                                                    window,
                                                    cx,
                                                );
                                            }),
                                        )
                                        .child(Input::new(&self.input_ssh_key_passphrase)),
                                )
                                .child(
                                    Self::render_password_toggle(
                                        self.show_ssh_passphrase,
                                        "toggle-ssh-passphrase",
                                        theme,
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.show_ssh_passphrase = !this.show_ssh_passphrase;
                                            cx.notify();
                                        },
                                    )),
                                )
                                .when_some(passphrase_checkbox, |d, checkbox| {
                                    d.child(
                                        div()
                                            .rounded(px(4.0))
                                            .border_2()
                                            .when(save_secret_focused, |d| {
                                                d.border_color(ring_color)
                                            })
                                            .when(!save_secret_focused, |d| {
                                                d.border_color(gpui::transparent_black())
                                            })
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(checkbox)
                                                    .child(div().text_sm().child("Save")),
                                            ),
                                    )
                                }),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(muted_fg)
                                .child("Leave empty if key has no passphrase"),
                        ),
                )
                .into_any_element(),

            SshAuthSelection::Password => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(
                    div()
                        .id(5usize)
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::MEDIUM)
                                        .child("SSH Password"),
                                )
                                .child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*")),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(password_focused, |d| d.border_color(ring_color))
                                        .when(!password_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, window, cx| {
                                                this.enter_edit_mode_for_field(
                                                    FormFocus::SshPassword,
                                                    window,
                                                    cx,
                                                );
                                            }),
                                        )
                                        .child(Input::new(&self.input_ssh_password)),
                                )
                                .child(
                                    Self::render_password_toggle(
                                        self.show_ssh_password,
                                        "toggle-ssh-password",
                                        theme,
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.show_ssh_password = !this.show_ssh_password;
                                            cx.notify();
                                        },
                                    )),
                                )
                                .when_some(password_checkbox, |d, checkbox| {
                                    d.child(
                                        div()
                                            .rounded(px(4.0))
                                            .border_2()
                                            .when(save_secret_focused, |d| {
                                                d.border_color(ring_color)
                                            })
                                            .when(!save_secret_focused, |d| {
                                                d.border_color(gpui::transparent_black())
                                            })
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(checkbox)
                                                    .child(div().text_sm().child("Save")),
                                            ),
                                    )
                                }),
                        ),
                )
                .into_any_element(),
        }
    }

    fn render_section(
        &self,
        title: &str,
        content: impl IntoElement,
        theme: &gpui_component::Theme,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .child(title.to_uppercase()),
            )
            .child(content)
    }

    fn input_state_for_field(&self, field_id: &str) -> Option<&Entity<InputState>> {
        if let Some(input) = self.driver_inputs.get(field_id) {
            return Some(input);
        }

        if field_id == "password" {
            return Some(&self.input_password);
        }

        match field_id {
            "ssh_host" => Some(&self.input_ssh_host),
            "ssh_port" => Some(&self.input_ssh_port),
            "ssh_user" => Some(&self.input_ssh_user),
            "ssh_key_path" => Some(&self.input_ssh_key_path),
            "ssh_passphrase" => Some(&self.input_ssh_key_passphrase),
            "ssh_password" => Some(&self.input_ssh_password),
            _ => None,
        }
    }

    /// Check if a field is enabled based on its conditional dependencies.
    fn is_field_enabled(&self, field: &FormFieldDef) -> bool {
        if let Some(checkbox_id) = field.enabled_when_checked {
            let is_checked = self
                .checkbox_states
                .get(checkbox_id)
                .copied()
                .unwrap_or(false);
            if !is_checked {
                return false;
            }
        }

        if let Some(checkbox_id) = field.enabled_when_unchecked {
            let is_checked = self
                .checkbox_states
                .get(checkbox_id)
                .copied()
                .unwrap_or(false);
            if is_checked {
                return false;
            }
        }

        true
    }

    /// Map a field ID to its FormFocus variant.
    fn field_id_to_focus(field_id: &str, is_ssh_tab: bool) -> Option<FormFocus> {
        use FormFocus::*;

        if is_ssh_tab {
            match field_id {
                "ssh_enabled" => Some(SshEnabled),
                "ssh_host" => Some(SshHost),
                "ssh_port" => Some(SshPort),
                "ssh_user" => Some(SshUser),
                "ssh_key_path" => Some(SshKeyPath),
                "ssh_passphrase" => Some(SshPassphrase),
                "ssh_password" => Some(SshPassword),
                _ => None,
            }
        } else {
            match field_id {
                "use_uri" => Some(UseUri),
                "host" | "uri" => Some(Host),
                "port" => Some(Port),
                "database" | "path" => Some(Database),
                "user" => Some(User),
                "password" => Some(Password),
                _ => None,
            }
        }
    }

    /// Map a FormFocus variant to its field ID.
    fn focus_to_field_id(focus: FormFocus) -> Option<&'static str> {
        use FormFocus::*;
        match focus {
            Host => Some("host"),
            Port => Some("port"),
            Database => Some("database"),
            User => Some("user"),
            Password => Some("password"),
            SshHost => Some("ssh_host"),
            SshPort => Some("ssh_port"),
            SshUser => Some("ssh_user"),
            SshKeyPath => Some("ssh_key_path"),
            SshPassphrase => Some("ssh_passphrase"),
            SshPassword => Some("ssh_password"),
            _ => None,
        }
    }

    fn input_for_focus(&self, focus: FormFocus) -> Option<&Entity<InputState>> {
        if let Some(field_id) = Self::focus_to_field_id(focus)
            && let Some(input) = self.driver_inputs.get(field_id)
        {
            return Some(input);
        }

        // Field name aliases (uri -> Host, path -> Database)
        match focus {
            FormFocus::Host => self
                .driver_inputs
                .get("uri")
                .or_else(|| self.driver_inputs.get("host")),
            FormFocus::Database => self
                .driver_inputs
                .get("path")
                .or_else(|| self.driver_inputs.get("database")),
            _ => None,
        }
    }

    fn render_form(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(driver) = &self.selected_driver else {
            return div().into_any_element();
        };

        let driver_name = driver.display_name().to_string();
        let supports_ssh = self.supports_ssh();
        let validation_errors = self.validation_errors.clone();
        let test_status = self.test_status;
        let test_error = self.test_error.clone();
        let is_editing = self.editing_profile_id.is_some();
        let title = if is_editing {
            format!("Edit {} Connection", driver_name)
        } else {
            format!("New {} Connection", driver_name)
        };

        // Focus state for buttons
        let show_focus = self.edit_state == EditState::Navigating;
        let focus = self.form_focus;
        let test_focused = show_focus && focus == FormFocus::TestConnection;
        let save_focused = show_focus && focus == FormFocus::Save;

        let tab_bar = if supports_ssh {
            Some(self.render_tab_bar(cx).into_any_element())
        } else {
            None
        };

        let tab_content: Vec<AnyElement> = match (supports_ssh, self.active_tab) {
            (true, ActiveTab::Main) => self.render_main_tab(cx),
            (true, ActiveTab::Ssh) => self.render_ssh_tab(cx),
            (false, _) => self.render_main_tab(cx),
        };

        let theme = cx.theme();
        let border_color = theme.border;
        let ring_color = theme.ring;

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .p_3()
                    .border_b_1()
                    .border_color(border_color)
                    .when(!is_editing, |d| {
                        d.child(Button::new("back").ghost().label("<").small().on_click(
                            cx.listener(|this, _, window, cx| {
                                this.back_to_driver_select(window, cx);
                            }),
                        ))
                    })
                    .child({
                        let brand_icon = self
                            .selected_driver
                            .as_ref()
                            .map(|driver| AppIcon::from_icon(driver.metadata().icon));

                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .when_some(brand_icon, |el, icon| {
                                el.child(
                                    svg()
                                        .path(icon.path())
                                        .size_6()
                                        .text_color(theme.foreground),
                                )
                            })
                            .child(
                                div()
                                    .text_lg()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child(title),
                            )
                    })
                    .child(div().flex_1())
                    .child(self.form_field_input_inline(
                        "Name",
                        &self.input_name,
                        show_focus && focus == FormFocus::Name,
                        ring_color,
                        FormFocus::Name,
                        cx,
                    )),
            )
            .when_some(tab_bar, |d, tab_bar| d.child(tab_bar))
            .child(
                div()
                    .id("form-scroll-content")
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.form_scroll_handle)
                    .gap_4()
                    .p_4()
                    .when(!validation_errors.is_empty(), |d| {
                        d.child(div().child(
                            div().p_2().rounded(px(4.0)).bg(gpui::rgb(0x7F1D1D)).child(
                                div().flex().flex_col().gap_1().children(
                                    validation_errors.iter().map(|err| {
                                        div()
                                            .text_sm()
                                            .text_color(gpui::rgb(0xFCA5A5))
                                            .child(err.clone())
                                    }),
                                ),
                            ),
                        ))
                    })
                    .children(tab_content),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .p_3()
                    .border_t_1()
                    .border_color(border_color)
                    .when(test_status != TestStatus::None, |d| {
                        let (bg, text_color, message) = match test_status {
                            TestStatus::Testing => (
                                gpui::rgb(0x1E3A5F),
                                gpui::rgb(0x93C5FD),
                                "Testing connection...".to_string(),
                            ),
                            TestStatus::Success => (
                                gpui::rgb(0x14532D),
                                gpui::rgb(0x86EFAC),
                                "Connection successful!".to_string(),
                            ),
                            TestStatus::Failed => (
                                gpui::rgb(0x7F1D1D),
                                gpui::rgb(0xFCA5A5),
                                test_error.unwrap_or_else(|| "Connection failed".to_string()),
                            ),
                            TestStatus::None => unreachable!(),
                        };

                        d.child(
                            div()
                                .p_2()
                                .rounded(px(4.0))
                                .bg(bg)
                                .child(div().text_sm().text_color(text_color).child(message)),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_end()
                            .gap_2()
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_2()
                                    .when(test_focused, |d| d.border_color(ring_color))
                                    .when(!test_focused, |d| {
                                        d.border_color(gpui::transparent_black())
                                    })
                                    .child(
                                        Button::new("test-connection")
                                            .ghost()
                                            .icon(Icon::new(IconName::ExternalLink))
                                            .label("Test Connection")
                                            .small()
                                            .disabled(test_status == TestStatus::Testing)
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.test_connection(window, cx);
                                            })),
                                    ),
                            )
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_2()
                                    .when(save_focused, |d| d.border_color(ring_color))
                                    .when(!save_focused, |d| {
                                        d.border_color(gpui::transparent_black())
                                    })
                                    .child(
                                        Button::new("save-connection")
                                            .primary()
                                            .icon(Icon::new(IconName::Check))
                                            .label("Save")
                                            .small()
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.save_profile(window, cx);
                                            })),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }

    /// Render a form field based on its definition.
    fn render_form_field(
        &self,
        field_def: &FormFieldDef,
        is_ssh_tab: bool,
        show_focus: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let field_focus = Self::field_id_to_focus(field_def.id, is_ssh_tab);
        let focused = show_focus && field_focus == Some(self.form_focus);

        match field_def.kind {
            FormFieldKind::Text
            | FormFieldKind::Password
            | FormFieldKind::Number
            | FormFieldKind::FilePath => {
                let Some(input_state) = self.input_state_for_field(field_def.id) else {
                    return div().into_any_element();
                };

                let field_enabled = self.is_field_enabled(field_def);

                div()
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .when(!field_enabled, |d| d.opacity(0.5))
                    .when(field_enabled, |d| {
                        d.on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                if let Some(field) = field_focus {
                                    this.enter_edit_mode_for_field(field, window, cx);
                                }
                            }),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .mb_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(field_def.label),
                            )
                            .when(field_def.required && field_enabled, |d| {
                                d.child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*"))
                            }),
                    )
                    .child(Input::new(input_state).disabled(!field_enabled))
                    .into_any_element()
            }

            FormFieldKind::Checkbox => {
                let field_id = field_def.id;
                let is_checked = if field_id == "ssh_enabled" {
                    self.ssh_enabled
                } else {
                    self.checkbox_states.get(field_id).copied().unwrap_or(false)
                };

                div()
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .child(
                        Checkbox::new(field_id)
                            .checked(is_checked)
                            .label(field_def.label)
                            .on_click(cx.listener(move |this, checked: &bool, window, cx| {
                                if field_id == "ssh_enabled" {
                                    this.ssh_enabled = *checked;
                                } else {
                                    this.checkbox_states.insert(field_id.to_string(), *checked);
                                }
                                window.focus(&this.focus_handle);
                                cx.notify();
                            })),
                    )
                    .into_any_element()
            }

            FormFieldKind::Select { options } => {
                if field_def.id == "ssh_auth_method" {
                    let selected_index = match self.ssh_auth_method {
                        SshAuthSelection::PrivateKey => 0,
                        SshAuthSelection::Password => 1,
                    };

                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child(field_def.label),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .children(options.iter().enumerate().map(|(idx, opt)| {
                                    let is_selected = idx == selected_index;
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap_1()
                                        .cursor_pointer()
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(move |this, _, window, cx| {
                                                this.ssh_auth_method = if idx == 0 {
                                                    SshAuthSelection::PrivateKey
                                                } else {
                                                    SshAuthSelection::Password
                                                };
                                                window.focus(&this.focus_handle);
                                                cx.notify();
                                            }),
                                        )
                                        .child(
                                            div()
                                                .w(px(16.0))
                                                .h(px(16.0))
                                                .rounded(px(3.0))
                                                .border_2()
                                                .border_color(cx.theme().muted_foreground)
                                                .flex()
                                                .items_center()
                                                .justify_center()
                                                .when(is_selected, |d| {
                                                    d.bg(cx.theme().ring)
                                                        .border_color(cx.theme().ring)
                                                })
                                                .when(is_selected, |d| {
                                                    d.child(
                                                        div()
                                                            .w(px(8.0))
                                                            .h(px(8.0))
                                                            .rounded(px(1.0))
                                                            .bg(gpui::white()),
                                                    )
                                                }),
                                        )
                                        .child(div().text_sm().child(opt.label))
                                        .into_any_element()
                                })),
                        )
                        .into_any_element()
                } else {
                    div().into_any_element()
                }
            }
        }
    }

    /// Render all sections in a tab.
    ///
    /// For the main tab, password is skipped since it's handled by `render_password_field`
    /// which includes the visibility toggle and save checkbox.
    fn render_form_tab(
        &mut self,
        tab: &FormTab,
        is_ssh_tab: bool,
        show_focus: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let mut sections: Vec<AnyElement> = Vec::new();

        for section in tab.sections {
            let fields: Vec<&FormFieldDef> = section
                .fields
                .iter()
                .filter(|field| {
                    // Skip password field on main tab - it's rendered separately
                    // with visibility toggle and save checkbox
                    field.id != "password" || is_ssh_tab
                })
                .collect();

            // Don't render empty sections
            if fields.is_empty() {
                continue;
            }

            // Build field elements, grouping host+port on same row
            let mut field_elements: Vec<AnyElement> = Vec::new();
            let mut i = 0;
            while i < fields.len() {
                let field = fields[i];

                // Check if this is host followed by port - render them in a row
                if field.id == "host" && i + 1 < fields.len() && fields[i + 1].id == "port" {
                    let port_field = fields[i + 1];
                    let host_element = self
                        .render_form_field(field, is_ssh_tab, show_focus, ring_color, cx)
                        .into_any_element();
                    let port_element = self
                        .render_form_field(port_field, is_ssh_tab, show_focus, ring_color, cx)
                        .into_any_element();

                    field_elements.push(
                        div()
                            .flex()
                            .gap_2()
                            .child(div().flex_1().child(host_element))
                            .child(div().w(px(100.0)).child(port_element))
                            .into_any_element(),
                    );
                    i += 2;
                } else {
                    field_elements.push(
                        self.render_form_field(field, is_ssh_tab, show_focus, ring_color, cx)
                            .into_any_element(),
                    );
                    i += 1;
                }
            }

            sections.push(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.muted_foreground)
                            .child(section.title.to_uppercase()),
                    )
                    .children(field_elements)
                    .into_any_element(),
            );
        }

        sections
    }

    #[allow(clippy::too_many_arguments)]
    fn form_field_input(
        &self,
        label: &str,
        input: &Entity<InputState>,
        required: bool,
        focused: bool,
        ring_color: Hsla,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .rounded(px(4.0))
            .border_2()
            .when(focused, |d| d.border_color(ring_color))
            .when(!focused, |d| d.border_color(gpui::transparent_black()))
            .p(px(2.0))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, window, cx| {
                    this.enter_edit_mode_for_field(field, window, cx);
                }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .mb_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child(label.to_string()),
                    )
                    .when(required, |d| {
                        d.child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*"))
                    }),
            )
            .child(Input::new(input))
    }

    fn form_field_input_inline(
        &self,
        label: &str,
        input: &Entity<InputState>,
        focused: bool,
        ring_color: Hsla,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(format!("{}:", label)),
            )
            .child(
                div()
                    .w(px(200.0))
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.enter_edit_mode_for_field(field, window, cx);
                        }),
                    )
                    .child(Input::new(input)),
            )
    }

    fn render_password_toggle(
        show: bool,
        toggle_id: &'static str,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let secondary = theme.secondary;
        let muted_foreground = theme.muted_foreground;

        let icon_path = if show {
            AppIcon::EyeOff.path()
        } else {
            AppIcon::Eye.path()
        };

        div()
            .id(toggle_id)
            .w(px(32.0))
            .h(px(32.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(px(4.0))
            .cursor_pointer()
            .hover(move |d| d.bg(secondary))
            .child(svg().path(icon_path).size_4().text_color(muted_foreground))
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for ConnectionManagerWindow {}

impl Render for ConnectionManagerWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_ssh_key_path.take() {
            self.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(tunnel_id) = self.pending_ssh_tunnel_selection.take() {
            let tunnel = self
                .app_state
                .read(cx)
                .ssh_tunnels
                .iter()
                .find(|t| t.id == tunnel_id)
                .cloned();
            if let Some(tunnel) = tunnel {
                let secret = self.app_state.read(cx).get_ssh_tunnel_secret(&tunnel);
                self.apply_ssh_tunnel(&tunnel, secret, window, cx);
            }
        }

        let show_password = self.show_password;
        let show_ssh_passphrase = self.show_ssh_passphrase;
        let show_ssh_password = self.show_ssh_password;

        self.input_password.update(cx, |state, cx| {
            state.set_masked(!show_password, window, cx);
        });
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_masked(!show_ssh_passphrase, window, cx);
        });
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_masked(!show_ssh_password, window, cx);
        });

        let theme = cx.theme();

        // Always use the main focus_handle for GPUI focus tracking
        // Internal navigation state is tracked via form_focus/driver_focus fields
        div()
            .id("connection-manager")
            .key_context(ContextId::ConnectionManager.as_gpui_context())
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    // Restore keyboard focus when clicking anywhere on the window
                    if this.edit_state == EditState::Navigating {
                        window.focus(&this.focus_handle);
                        cx.notify();
                    }
                }),
            )
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                if this.handle_key_event(event, window, cx) {
                    cx.stop_propagation();
                }
            }))
            .size_full()
            .bg(theme.background)
            .child(match self.view {
                View::DriverSelect => self.render_driver_select(window, cx).into_any_element(),
                View::EditForm => self.render_form(window, cx).into_any_element(),
            })
    }
}

impl Focusable for ConnectionManagerWindow {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
