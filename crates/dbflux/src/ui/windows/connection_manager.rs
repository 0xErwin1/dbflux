mod form;
mod navigation;
mod render;
mod ssh;

use crate::app::AppState;
use crate::keymap::KeymapStack;
use crate::ui::components::form_renderer::{self, FormRendererState};
use crate::ui::dropdown::{Dropdown, DropdownSelectionChanged};
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::{DbDriver, DbKind, DriverFormDef, FormFieldDef, FormFieldKind, GlobalOverrides};
use gpui::*;
use gpui_component::input::{InputEvent, InputState};
use std::collections::HashMap;
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
    // Settings tab fields
    SettingsRefreshPolicy,
    SettingsRefreshInterval,
    SettingsConfirmDangerous,
    SettingsRequiresWhere,
    SettingsRequiresPreview,
    SettingsDriverField(u8),
    // Actions (shared between tabs)
    TestConnection,
    Save,
}

use crate::ui::components::form_navigation::FormEditState;

type EditState = FormEditState;

#[derive(Clone, Copy, PartialEq)]
enum View {
    DriverSelect,
    EditForm,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum ActiveTab {
    Main,
    Ssh,
    Settings,
}

#[derive(Clone, Copy, PartialEq)]
enum TestStatus {
    None,
    Testing,
    Success,
    Failed,
}

#[derive(Clone)]
struct DriverInfo {
    id: String,
    icon: dbflux_core::Icon,
    name: String,
    description: String,
}

pub struct ConnectionManagerWindow {
    app_state: Entity<AppState>,
    view: View,
    active_tab: ActiveTab,
    available_drivers: Vec<DriverInfo>,
    selected_driver_id: Option<String>,
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

    syncing_uri: bool,

    // Settings tab state
    conn_override_refresh_policy: bool,
    conn_override_refresh_interval: bool,
    conn_refresh_policy_dropdown: Entity<Dropdown>,
    conn_refresh_interval_input: Entity<InputState>,
    conn_confirm_dangerous_dropdown: Entity<Dropdown>,
    conn_requires_where_dropdown: Entity<Dropdown>,
    conn_requires_preview_dropdown: Entity<Dropdown>,
    conn_form_state: FormRendererState,
    conn_form_subscriptions: Vec<Subscription>,
    conn_loading_settings: bool,
}

impl ConnectionManagerWindow {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let available_drivers: Vec<DriverInfo> = app_state
            .read(cx)
            .drivers()
            .iter()
            .map(|(driver_id, driver)| DriverInfo {
                id: driver_id.clone(),
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

        let conn_refresh_policy_dropdown =
            cx.new(|_cx| Dropdown::new("conn-refresh-policy").placeholder("Use Driver Default"));
        let conn_refresh_interval_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("seconds")
                .default_value("5")
        });
        let conn_confirm_dangerous_dropdown =
            cx.new(|_cx| Dropdown::new("conn-confirm-dangerous").placeholder("Use Driver Default"));
        let conn_requires_where_dropdown =
            cx.new(|_cx| Dropdown::new("conn-requires-where").placeholder("Use Driver Default"));
        let conn_requires_preview_dropdown =
            cx.new(|_cx| Dropdown::new("conn-requires-preview").placeholder("Use Driver Default"));

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

        let password_change_sub = cx.subscribe_in(
            &input_password,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { secondary: false } => {
                    this.exit_edit_mode(window, cx);
                    this.focus_down(cx);
                }
                InputEvent::Blur => {
                    this.exit_edit_mode(window, cx);
                }
                InputEvent::Change => {
                    this.handle_field_change("password", window, cx);
                }
                _ => {}
            },
        );

        let subscriptions = vec![
            dropdown_subscription,
            subscribe_input(cx, window, &input_name),
            password_change_sub,
            subscribe_input(cx, window, &input_ssh_host),
            subscribe_input(cx, window, &input_ssh_port),
            subscribe_input(cx, window, &input_ssh_user),
            subscribe_input(cx, window, &input_ssh_key_path),
            subscribe_input(cx, window, &input_ssh_key_passphrase),
            subscribe_input(cx, window, &input_ssh_password),
        ];

        let focus_handle = cx.focus_handle();
        window.focus(&focus_handle);

        Self {
            app_state,
            view: View::DriverSelect,
            active_tab: ActiveTab::Main,
            available_drivers,
            selected_driver_id: None,
            selected_driver: None,
            form_save_password: true,
            form_save_ssh_secret: true,
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
            syncing_uri: false,

            conn_override_refresh_policy: false,
            conn_override_refresh_interval: false,
            conn_refresh_policy_dropdown,
            conn_refresh_interval_input,
            conn_confirm_dangerous_dropdown,
            conn_requires_where_dropdown,
            conn_requires_preview_dropdown,
            conn_form_state: FormRendererState::default(),
            conn_form_subscriptions: Vec::new(),
            conn_loading_settings: false,
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
        profile: &dbflux_core::ConnectionProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut instance = Self::new(app_state.clone(), window, cx);
        instance.editing_profile_id = Some(profile.id);

        let driver = app_state.read(cx).driver_for_profile(profile);
        instance.selected_driver = driver.clone();
        instance.selected_driver_id = Some(profile.driver_id());
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

        if let Some(password) = app_state.read(cx).get_password(profile) {
            instance.input_password.update(cx, |state, cx| {
                state.set_value(&password, window, cx);
            });
        }

        instance.load_settings_tab(
            profile.settings_overrides.as_ref(),
            profile.connection_settings.as_ref(),
            window,
            cx,
        );

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
                dbflux_core::SshAuthMethod::PrivateKey { key_path } => {
                    instance.ssh_auth_method = SshAuthSelection::PrivateKey;
                    if let Some(path) = key_path {
                        let path_str: String = path.to_string_lossy().into_owned();
                        instance.input_ssh_key_path.update(cx, |state, cx| {
                            state.set_value(path_str, window, cx);
                        });
                    }
                }
                dbflux_core::SshAuthMethod::Password => {
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

    fn select_driver(&mut self, driver_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let driver = self.app_state.read(cx).drivers().get(driver_id).cloned();
        self.selected_driver_id = Some(driver_id.to_string());
        self.selected_driver = driver.clone();
        self.form_save_password = true;
        self.ssh_enabled = false;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_ssh_secret = true;
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

        self.load_settings_tab(None, None, window, cx);

        self.view = View::EditForm;
        self.edit_state = EditState::Navigating;
        self.form_focus = FormFocus::Name;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    /// Create input states from the driver's form definition.
    fn create_driver_inputs(
        &mut self,
        form: &DriverFormDef,
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
            let placeholder = &field.placeholder;
            let default_value = &field.default_value;
            let is_masked = field.kind == FormFieldKind::Password;
            let field_id = field.id.clone();

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

            let subscription = cx.subscribe_in(
                &input,
                window,
                move |this, _, event: &InputEvent, window, cx| match event {
                    InputEvent::PressEnter { secondary: false } => {
                        this.exit_edit_mode(window, cx);
                        this.focus_down(cx);
                    }
                    InputEvent::Blur => {
                        this.exit_edit_mode(window, cx);
                    }
                    InputEvent::Change => {
                        this.handle_field_change(&field_id, window, cx);
                    }
                    _ => {}
                },
            );
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
        for tab in &form.tabs {
            for section in &tab.sections {
                for field in &section.fields {
                    if field.kind == FormFieldKind::Checkbox {
                        let is_checked =
                            values.get(&field.id).map(|v| v == "true").unwrap_or(false);
                        self.checkbox_states.insert(field.id.clone(), is_checked);
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
        let dropdowns = HashMap::new();

        form_renderer::collect_values(
            form,
            &self.driver_inputs,
            &self.checkbox_states,
            &dropdowns,
            cx,
        )
    }

    fn back_to_driver_select(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        window.focus(&self.focus_handle);
        self.view = View::DriverSelect;
        self.selected_driver_id = None;
        self.selected_driver = None;
        self.validation_errors.clear();
        self.test_status = TestStatus::None;
        self.test_error = None;
        cx.notify();
    }

    fn selected_kind(&self) -> Option<DbKind> {
        self.selected_driver.as_ref().map(|d| d.kind())
    }

    fn selected_driver_id(&self) -> Option<&str> {
        self.selected_driver_id.as_deref()
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
        form_renderer::is_field_enabled(field, &self.checkbox_states)
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
        let uri_mode = self
            .checkbox_states
            .get("use_uri")
            .copied()
            .unwrap_or(false);

        if focus == FormFocus::Host && uri_mode {
            return self.driver_inputs.get("uri");
        }

        if let Some(field_id) = Self::focus_to_field_id(focus)
            && let Some(input) = self.driver_inputs.get(field_id)
        {
            return Some(input);
        }

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

    /// Initialize the Settings tab controls from the selected driver's defaults
    /// and (if editing) the profile's saved overrides.
    fn load_settings_tab(
        &mut self,
        overrides: Option<&GlobalOverrides>,
        connection_settings: Option<&dbflux_core::FormValues>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.conn_loading_settings = true;
        self.conn_form_subscriptions.clear();
        self.conn_form_state.clear();

        let overrides = overrides.cloned().unwrap_or_default();

        self.conn_override_refresh_policy = overrides.refresh_policy.is_some();
        self.conn_override_refresh_interval = overrides.refresh_interval_secs.is_some();

        let effective = self.resolve_driver_effective_settings(cx);

        let policy_items = vec![
            crate::ui::dropdown::DropdownItem::with_value("Manual", "manual"),
            crate::ui::dropdown::DropdownItem::with_value("Interval", "interval"),
        ];
        let policy_index = match overrides.refresh_policy.unwrap_or(effective.refresh_policy) {
            dbflux_core::RefreshPolicySetting::Manual => 0,
            dbflux_core::RefreshPolicySetting::Interval => 1,
        };
        self.conn_refresh_policy_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_items(policy_items, cx);
                dropdown.set_selected_index(Some(policy_index), cx);
            });

        let interval_val = overrides
            .refresh_interval_secs
            .unwrap_or(effective.refresh_interval_secs);
        self.conn_refresh_interval_input.update(cx, |input, cx| {
            input.set_value(interval_val.to_string(), window, cx);
        });

        let boolean_items = vec![
            crate::ui::dropdown::DropdownItem::with_value("Use Driver Default", "default"),
            crate::ui::dropdown::DropdownItem::with_value("On", "on"),
            crate::ui::dropdown::DropdownItem::with_value("Off", "off"),
        ];

        let bool_index = |opt: Option<bool>| -> usize {
            match opt {
                None => 0,
                Some(true) => 1,
                Some(false) => 2,
            }
        };

        self.conn_confirm_dangerous_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_items(boolean_items.clone(), cx);
                dropdown.set_selected_index(Some(bool_index(overrides.confirm_dangerous)), cx);
            });
        self.conn_requires_where_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_items(boolean_items.clone(), cx);
                dropdown.set_selected_index(Some(bool_index(overrides.requires_where)), cx);
            });
        self.conn_requires_preview_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_items(boolean_items, cx);
                dropdown.set_selected_index(Some(bool_index(overrides.requires_preview)), cx);
            });

        if let Some(driver) = &self.selected_driver
            && let Some(schema) = driver.settings_schema()
        {
            let values = connection_settings.cloned().unwrap_or_default();
            self.conn_form_state = form_renderer::create_inputs(&schema, &values, window, cx);

            let mut subscriptions = Vec::new();
            for input in self.conn_form_state.inputs.values() {
                subscriptions.push(cx.subscribe_in(
                    input,
                    window,
                    |_this, _, event: &InputEvent, _window, _cx| {
                        if matches!(event, InputEvent::Change) {
                            // Nothing to track — the form state is read on save
                        }
                    },
                ));
            }
            for dropdown in self.conn_form_state.dropdowns.values() {
                subscriptions.push(cx.subscribe(
                    dropdown,
                    |_this, _dropdown, _event: &DropdownSelectionChanged, _cx| {
                        // Nothing to track — the form state is read on save
                    },
                ));
            }
            self.conn_form_subscriptions = subscriptions;
        }

        self.conn_loading_settings = false;
    }

    /// Resolve driver-level effective settings (without connection overrides)
    /// for showing defaults in the Settings tab.
    fn resolve_driver_effective_settings(
        &self,
        cx: &Context<Self>,
    ) -> dbflux_core::EffectiveSettings {
        let state = self.app_state.read(cx);
        if let Some(driver) = &self.selected_driver {
            state.effective_settings(&driver.driver_key())
        } else {
            let empty = dbflux_core::FormValues::new();
            dbflux_core::EffectiveSettings::resolve(
                state.general_settings(),
                None,
                &empty,
                None,
                None,
            )
        }
    }

    /// Collect connection-level global overrides from the Settings tab controls.
    fn collect_connection_overrides(&self, cx: &Context<Self>) -> Option<GlobalOverrides> {
        let mut overrides = GlobalOverrides::default();

        if self.conn_override_refresh_policy {
            let value = self
                .conn_refresh_policy_dropdown
                .read(cx)
                .selected_value()
                .map(|v| v.to_string())
                .unwrap_or_default();

            overrides.refresh_policy = Some(if value == "interval" {
                dbflux_core::RefreshPolicySetting::Interval
            } else {
                dbflux_core::RefreshPolicySetting::Manual
            });
        }

        if self.conn_override_refresh_interval {
            let text = self
                .conn_refresh_interval_input
                .read(cx)
                .value()
                .to_string();

            if let Ok(secs) = text.parse::<u32>()
                && secs > 0
            {
                overrides.refresh_interval_secs = Some(secs);
            }
        }

        fn parse_boolean_dropdown(
            dropdown: &Entity<Dropdown>,
            cx: &Context<ConnectionManagerWindow>,
        ) -> Option<bool> {
            match dropdown
                .read(cx)
                .selected_value()
                .map(|v| v.to_string())
                .as_deref()
            {
                Some("on") => Some(true),
                Some("off") => Some(false),
                _ => None,
            }
        }

        overrides.confirm_dangerous =
            parse_boolean_dropdown(&self.conn_confirm_dangerous_dropdown, cx);
        overrides.requires_where = parse_boolean_dropdown(&self.conn_requires_where_dropdown, cx);
        overrides.requires_preview =
            parse_boolean_dropdown(&self.conn_requires_preview_dropdown, cx);

        if overrides.is_empty() {
            None
        } else {
            Some(overrides)
        }
    }

    /// Collect connection-level driver settings from the Settings tab form.
    ///
    /// Unchecked checkboxes are stored as `"false"` (not stripped) so they can
    /// explicitly override a driver-level `"true"` value.
    fn collect_connection_settings(&self, cx: &Context<Self>) -> Option<dbflux_core::FormValues> {
        let driver = self.selected_driver.as_ref()?;
        let schema = driver.settings_schema()?;

        let collected = form_renderer::collect_values(
            &schema,
            &self.conn_form_state.inputs,
            &self.conn_form_state.checkboxes,
            &self.conn_form_state.dropdowns,
            cx,
        );

        let checkbox_ids: std::collections::HashSet<&str> = schema
            .tabs
            .iter()
            .flat_map(|t| t.sections.iter())
            .flat_map(|s| s.fields.iter())
            .filter(|f| matches!(f.kind, FormFieldKind::Checkbox))
            .map(|f| f.id.as_str())
            .collect();

        let mut values = collected;

        for (key, val) in values.iter_mut() {
            if val.is_empty() && checkbox_ids.contains(key.as_str()) {
                *val = "false".to_string();
            }
        }

        values.retain(|k, v| !v.is_empty() || checkbox_ids.contains(k.as_str()));

        if values.is_empty() {
            None
        } else {
            Some(values)
        }
    }

    /// Returns the number of driver schema fields (for Settings tab navigation).
    fn settings_driver_field_count(&self) -> u8 {
        let Some(driver) = &self.selected_driver else {
            return 0;
        };
        let Some(schema) = driver.settings_schema() else {
            return 0;
        };
        schema
            .tabs
            .iter()
            .flat_map(|t| t.sections.iter())
            .flat_map(|s| s.fields.iter())
            .count() as u8
    }

    /// Returns the field definition for a driver schema field at the given flat index.
    fn settings_driver_field_def(&self, idx: u8) -> Option<FormFieldDef> {
        let driver = self.selected_driver.as_ref()?;
        let schema = driver.settings_schema()?;
        schema
            .tabs
            .iter()
            .flat_map(|t| t.sections.iter())
            .flat_map(|s| s.fields.iter())
            .nth(idx as usize)
            .cloned()
    }

    fn handle_field_change(&mut self, field_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if self.syncing_uri {
            return;
        }

        let use_uri = self
            .checkbox_states
            .get("use_uri")
            .copied()
            .unwrap_or(false);

        if field_id == "uri" && use_uri {
            self.sync_uri_to_fields(window, cx);
        } else if field_id != "uri" && !use_uri {
            self.sync_fields_to_uri(window, cx);
        }
    }

    pub(super) fn sync_fields_to_uri(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(driver) = &self.selected_driver else {
            return;
        };

        let form = driver.form_definition();
        let values = self.collect_form_values(form, cx);
        let password = self.input_password.read(cx).value().to_string();

        let Some(uri) = driver.build_uri(&values, &password) else {
            return;
        };

        if let Some(uri_input) = self.driver_inputs.get("uri") {
            let current = uri_input.read(cx).value().to_string();
            if current != uri {
                self.syncing_uri = true;
                uri_input.update(cx, |state, cx| {
                    state.set_value(&uri, window, cx);
                });
                self.syncing_uri = false;
            }
        }
    }

    pub(super) fn sync_uri_to_fields(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(driver) = &self.selected_driver else {
            return;
        };

        let Some(uri_input) = self.driver_inputs.get("uri") else {
            return;
        };
        let uri_value = uri_input.read(cx).value().to_string();

        if uri_value.is_empty() {
            return;
        }

        let Some(parsed) = driver.parse_uri(&uri_value) else {
            return;
        };

        self.syncing_uri = true;

        for (field_id, value) in &parsed {
            if let Some(input) = self.driver_inputs.get(field_id.as_str()) {
                let current = input.read(cx).value().to_string();
                if current != *value {
                    input.update(cx, |state, cx| {
                        state.set_value(value, window, cx);
                    });
                }
            }
        }

        self.syncing_uri = false;
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for ConnectionManagerWindow {}
