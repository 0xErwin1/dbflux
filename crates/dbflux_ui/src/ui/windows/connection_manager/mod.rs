mod access_tab;
mod form;
mod hooks_tab;
mod navigation;
mod render;
mod render_driver_select;
mod render_tabs;

use crate::app::{AppStateEntity, AuthProfileCreated};
use crate::keymap::KeymapStack;
use crate::platform;
use crate::ui::components::dropdown::{Dropdown, DropdownSelectionChanged};
use crate::ui::components::form_renderer::{self, FormRendererState};
use crate::ui::components::multi_select::MultiSelect;
use crate::ui::components::value_source_selector::ValueSourceSelector;
use crate::ui::overlays::sso_wizard::SsoWizard;
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::access::AccessKind;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    AuthProfile, AuthSessionState, ConnectionHookBindings, DbDriver, DbKind, DriverFormDef,
    FormFieldDef, FormFieldKind, GlobalOverrides, SshAuthMethod, SshTunnelProfile, ValueRef,
};
use gpui::*;
use gpui_component::Root;
use gpui_component::input::{InputEvent, InputState};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

const AUTH_PROFILE_NONE_INDEX: usize = 0;

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
    AccessMethod,
    UseUri,
    HostValueSource,
    Host,
    Port,
    DatabaseValueSource,
    Database,
    FileBrowse,
    UserValueSource,
    User,
    PasswordValueSource,
    Password,
    PasswordToggle,
    PasswordSave,
    // SSH tab fields
    SshEnabled,
    SshTunnelSelector,
    SshTunnelClear,
    SshEditInSettings,
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
    // Proxy tab fields
    ProxySelector,
    ProxyClear,
    ProxyEditInSettings,
    SsmInstanceIdValueSource,
    SsmInstanceId,
    SsmRegionValueSource,
    SsmRegion,
    SsmRemotePortValueSource,
    SsmRemotePort,
    SsmAuthProfile,
    SsmAuthManage,
    SsmAuthLogin,
    SsmAuthRefresh,
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
    Access,
    Settings,
    Mcp,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum AccessTabMode {
    #[default]
    Direct,
    Ssh,
    Proxy,
    ManagedSsm,
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
    app_state: Entity<AppStateEntity>,
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
    host_value_source_selector: Entity<ValueSourceSelector>,
    database_value_source_selector: Entity<ValueSourceSelector>,
    user_value_source_selector: Entity<ValueSourceSelector>,
    password_value_source_selector: Entity<ValueSourceSelector>,

    selected_proxy_id: Option<Uuid>,
    proxy_dropdown: Entity<Dropdown>,
    proxy_uuids: Vec<Uuid>,
    pending_proxy_selection: Option<Uuid>,

    ssh_enabled: bool,
    ssh_auth_method: SshAuthSelection,
    /// Checkbox states keyed by field ID (e.g., "use_uri" -> true).
    checkbox_states: HashMap<String, bool>,
    selected_ssh_tunnel_id: Option<Uuid>,
    ssh_tunnel_dropdown: Entity<crate::ui::components::dropdown::Dropdown>,
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
    pending_file_path: Option<String>,
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

    // Auth profile dropdown (T-7.1)
    auth_profile_dropdown: Entity<Dropdown>,
    auth_profile_uuids: Vec<Uuid>,
    selected_auth_profile_id: Option<Uuid>,
    pending_auth_profile_selection: Option<Option<Uuid>>,
    auth_profile_session_states: HashMap<Uuid, AuthSessionState>,
    auth_profile_login_in_progress: bool,
    auth_profile_action_message: Option<String>,
    pending_wizard_auth_profile_selection: bool,
    known_auth_profile_ids: HashSet<Uuid>,

    // Access method dropdown (T-7.2)
    access_method_dropdown: Entity<Dropdown>,
    access_kind: Option<AccessKind>,
    access_tab_mode: AccessTabMode,

    // SSM inline fields (T-7.3)
    input_ssm_instance_id: Entity<InputState>,
    ssm_instance_id_value_source_selector: Entity<ValueSourceSelector>,
    input_ssm_region: Entity<InputState>,
    ssm_region_value_source_selector: Entity<ValueSourceSelector>,
    input_ssm_remote_port: Entity<InputState>,
    ssm_remote_port_value_source_selector: Entity<ValueSourceSelector>,
    ssm_auth_profile_dropdown: Entity<Dropdown>,
    ssm_auth_profile_uuids: Vec<Uuid>,
    selected_ssm_auth_profile_id: Option<Uuid>,
    pending_ssm_auth_profile_selection: Option<Option<Uuid>>,

    // Settings tab state
    conn_override_refresh_policy: bool,
    conn_override_refresh_interval: bool,
    conn_refresh_policy_dropdown: Entity<Dropdown>,
    conn_refresh_interval_input: Entity<InputState>,
    conn_confirm_dangerous_dropdown: Entity<Dropdown>,
    conn_requires_where_dropdown: Entity<Dropdown>,
    conn_requires_preview_dropdown: Entity<Dropdown>,
    conn_pre_hook_dropdown: Entity<Dropdown>,
    conn_post_hook_dropdown: Entity<Dropdown>,
    conn_pre_disconnect_hook_dropdown: Entity<Dropdown>,
    conn_post_disconnect_hook_dropdown: Entity<Dropdown>,
    conn_pre_hook_extra_input: Entity<InputState>,
    conn_post_hook_extra_input: Entity<InputState>,
    conn_pre_disconnect_hook_extra_input: Entity<InputState>,
    conn_post_disconnect_hook_extra_input: Entity<InputState>,
    conn_form_state: FormRendererState,
    conn_form_subscriptions: Vec<Subscription>,
    conn_loading_settings: bool,

    // MCP tab state
    conn_mcp_enabled: bool,
    conn_mcp_actor_dropdown: Entity<Dropdown>,
    conn_mcp_role_dropdown: Entity<Dropdown>,
    conn_mcp_role_multi_select: Entity<MultiSelect>,
    conn_mcp_policy_dropdown: Entity<Dropdown>,
    conn_mcp_policy_multi_select: Entity<MultiSelect>,
}

impl ConnectionManagerWindow {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
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
        let host_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-host", window, cx));
        let database_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-database", window, cx));
        let user_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-user", window, cx));
        let password_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-password", window, cx));

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
        let proxy_dropdown =
            cx.new(|_cx| Dropdown::new("proxy-dropdown").placeholder("Select Proxy"));

        let auth_profile_dropdown =
            cx.new(|_cx| Dropdown::new("auth-profile-dropdown").placeholder("None"));
        let access_method_dropdown =
            cx.new(|_cx| Dropdown::new("access-method-dropdown").placeholder("Direct"));

        let input_ssm_instance_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("i-0123456789abcdef0"));
        let ssm_instance_id_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-ssm-instance-id", window, cx));
        let input_ssm_region = cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1"));
        let ssm_region_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-ssm-region", window, cx));
        let input_ssm_remote_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("5432")
                .default_value("5432")
        });
        let ssm_remote_port_value_source_selector =
            cx.new(|cx| ValueSourceSelector::new("cm-ssm-remote-port", window, cx));
        let ssm_auth_profile_dropdown = cx.new(|_cx| {
            Dropdown::new("ssm-auth-profile-dropdown").placeholder("Use Connection Auth Profile")
        });

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
        let conn_pre_hook_dropdown =
            cx.new(|_cx| Dropdown::new("conn-pre-hook").placeholder("No hook"));
        let conn_post_hook_dropdown =
            cx.new(|_cx| Dropdown::new("conn-post-hook").placeholder("No hook"));
        let conn_pre_disconnect_hook_dropdown =
            cx.new(|_cx| Dropdown::new("conn-pre-disconnect-hook").placeholder("No hook"));
        let conn_post_disconnect_hook_dropdown =
            cx.new(|_cx| Dropdown::new("conn-post-disconnect-hook").placeholder("No hook"));
        let conn_pre_hook_extra_input = cx
            .new(|cx| InputState::new(window, cx).placeholder("extra hook IDs (comma-separated)"));
        let conn_post_hook_extra_input = cx
            .new(|cx| InputState::new(window, cx).placeholder("extra hook IDs (comma-separated)"));
        let conn_pre_disconnect_hook_extra_input = cx
            .new(|cx| InputState::new(window, cx).placeholder("extra hook IDs (comma-separated)"));
        let conn_post_disconnect_hook_extra_input = cx
            .new(|cx| InputState::new(window, cx).placeholder("extra hook IDs (comma-separated)"));
        let conn_mcp_actor_dropdown =
            cx.new(|_cx| Dropdown::new("conn-mcp-actor").placeholder("Select trusted client"));
        let conn_mcp_role_dropdown =
            cx.new(|_cx| Dropdown::new("conn-mcp-role").placeholder("No role"));
        let conn_mcp_role_multi_select = cx.new(|_cx| {
            MultiSelect::new("conn-mcp-extra-roles").placeholder("Select additional roles…")
        });
        let conn_mcp_policy_dropdown =
            cx.new(|_cx| Dropdown::new("conn-mcp-policy").placeholder("No policy"));
        let conn_mcp_policy_multi_select = cx.new(|_cx| {
            MultiSelect::new("conn-mcp-extra-policies").placeholder("Select additional policies…")
        });

        let dropdown_subscription = cx.subscribe(
            &ssh_tunnel_dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.handle_ssh_tunnel_dropdown_selection(event, cx);
            },
        );

        let proxy_dropdown_subscription = cx.subscribe(
            &proxy_dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.handle_proxy_dropdown_selection(event, cx);
            },
        );

        let auth_profile_dropdown_sub = cx.subscribe(
            &auth_profile_dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.handle_auth_profile_dropdown_selection(event, cx);
            },
        );

        let access_method_dropdown_sub = cx.subscribe(
            &access_method_dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.handle_access_method_dropdown_selection(event, cx);
            },
        );

        let ssm_auth_profile_dropdown_sub = cx.subscribe(
            &ssm_auth_profile_dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.handle_ssm_auth_profile_dropdown_selection(event, cx);
            },
        );

        let app_state_changed_sub = cx.subscribe(
            &app_state,
            |this, _, _: &crate::app::AppStateChanged, cx| {
                this.handle_app_state_changed(cx);
            },
        );

        let auth_profile_created_sub =
            cx.subscribe(&app_state, |this, _, event: &AuthProfileCreated, cx| {
                this.handle_auth_profile_created(event.profile_id, cx);
            });

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
                        this.exit_edit_mode_on_blur(cx);
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
                    this.exit_edit_mode_on_blur(cx);
                }
                InputEvent::Change => {
                    this.handle_field_change("password", window, cx);
                }
                _ => {}
            },
        );

        let subscriptions = vec![
            dropdown_subscription,
            proxy_dropdown_subscription,
            auth_profile_dropdown_sub,
            access_method_dropdown_sub,
            ssm_auth_profile_dropdown_sub,
            app_state_changed_sub,
            auth_profile_created_sub,
            subscribe_input(cx, window, &input_name),
            password_change_sub,
            subscribe_input(cx, window, &input_ssh_host),
            subscribe_input(cx, window, &input_ssh_port),
            subscribe_input(cx, window, &input_ssh_user),
            subscribe_input(cx, window, &input_ssh_key_path),
            subscribe_input(cx, window, &input_ssh_key_passphrase),
            subscribe_input(cx, window, &input_ssh_password),
            subscribe_input(cx, window, &input_ssm_instance_id),
            subscribe_input(cx, window, &input_ssm_region),
            subscribe_input(cx, window, &input_ssm_remote_port),
        ];

        let focus_handle = cx.focus_handle();
        window.focus(&focus_handle);

        Self {
            app_state: app_state.clone(),
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
            host_value_source_selector,
            database_value_source_selector,
            user_value_source_selector,
            password_value_source_selector,
            selected_proxy_id: None,
            proxy_dropdown,
            proxy_uuids: Vec::new(),
            pending_proxy_selection: None,

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
            pending_file_path: None,
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

            auth_profile_dropdown,
            auth_profile_uuids: Vec::new(),
            selected_auth_profile_id: None,
            pending_auth_profile_selection: None,
            auth_profile_session_states: HashMap::new(),
            auth_profile_login_in_progress: false,
            auth_profile_action_message: None,
            pending_wizard_auth_profile_selection: false,
            known_auth_profile_ids: app_state
                .read(cx)
                .auth_profiles()
                .iter()
                .map(|profile| profile.id)
                .collect(),

            access_method_dropdown,
            access_kind: None,
            access_tab_mode: AccessTabMode::Direct,

            input_ssm_instance_id,
            ssm_instance_id_value_source_selector,
            input_ssm_region,
            ssm_region_value_source_selector,
            input_ssm_remote_port,
            ssm_remote_port_value_source_selector,
            ssm_auth_profile_dropdown,
            ssm_auth_profile_uuids: Vec::new(),
            selected_ssm_auth_profile_id: None,
            pending_ssm_auth_profile_selection: None,

            conn_override_refresh_policy: false,
            conn_override_refresh_interval: false,
            conn_refresh_policy_dropdown,
            conn_refresh_interval_input,
            conn_confirm_dangerous_dropdown,
            conn_requires_where_dropdown,
            conn_requires_preview_dropdown,
            conn_pre_hook_dropdown,
            conn_post_hook_dropdown,
            conn_pre_disconnect_hook_dropdown,
            conn_post_disconnect_hook_dropdown,
            conn_pre_hook_extra_input,
            conn_post_hook_extra_input,
            conn_pre_disconnect_hook_extra_input,
            conn_post_disconnect_hook_extra_input,
            conn_form_state: FormRendererState::default(),
            conn_form_subscriptions: Vec::new(),
            conn_loading_settings: false,
            conn_mcp_enabled: false,
            conn_mcp_actor_dropdown,
            conn_mcp_role_dropdown,
            conn_mcp_role_multi_select,
            conn_mcp_policy_dropdown,
            conn_mcp_policy_multi_select,
        }
    }

    pub fn new_in_folder(
        app_state: Entity<AppStateEntity>,
        folder_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut instance = Self::new(app_state, window, cx);
        instance.target_folder_id = Some(folder_id);
        instance
    }

    pub fn new_for_edit(
        app_state: Entity<AppStateEntity>,
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
            let password = password.expose_secret().to_string();
            instance.input_password.update(cx, |state, cx| {
                state.set_value(password.clone(), window, cx);
            });
        }

        instance.load_settings_tab(
            profile.settings_overrides.as_ref(),
            profile.connection_settings.as_ref(),
            profile.hook_bindings.as_ref(),
            window,
            cx,
        );

        instance.conn_mcp_enabled = profile.mcp_governance.as_ref().is_some_and(|g| g.enabled);

        let first_binding = profile
            .mcp_governance
            .as_ref()
            .and_then(|governance| governance.policy_bindings.first().cloned());

        #[cfg(feature = "mcp")]
        instance.load_mcp_dropdowns(first_binding.as_ref(), window, cx);

        instance.selected_proxy_id = profile.proxy_profile_id;
        instance.selected_auth_profile_id = profile.auth_profile_id;
        instance.selected_ssm_auth_profile_id = None;
        instance.access_kind = profile.access_kind.clone();

        if let Some(AccessKind::Proxy { proxy_profile_id }) = &profile.access_kind {
            instance.selected_proxy_id.get_or_insert(*proxy_profile_id);
        }

        if let Some(AccessKind::Ssh {
            ssh_tunnel_profile_id,
        }) = &profile.access_kind
        {
            instance.selected_ssh_tunnel_id = Some(*ssh_tunnel_profile_id);
            instance.ssh_enabled = true;

            let selected_tunnel = app_state
                .read(cx)
                .ssh_tunnels()
                .iter()
                .find(|tunnel| tunnel.id == *ssh_tunnel_profile_id)
                .cloned();

            if let Some(tunnel) = selected_tunnel {
                let secret = app_state.read(cx).get_ssh_tunnel_secret(&tunnel);
                instance.apply_ssh_tunnel(&tunnel, secret, window, cx);
            }
        }

        // Populate SSM fields if access kind is a managed aws-ssm access
        if let Some(AccessKind::Managed { provider, params }) = &profile.access_kind
            && provider == "aws-ssm"
        {
            let instance_id = params.get("instance_id").cloned().unwrap_or_default();
            let region = params.get("region").cloned().unwrap_or_default();
            let remote_port = params.get("remote_port").cloned().unwrap_or_default();
            let auth_profile_id: Option<uuid::Uuid> =
                params.get("auth_profile_id").and_then(|s| s.parse().ok());

            instance.input_ssm_instance_id.update(cx, |state, cx| {
                state.set_value(instance_id, window, cx);
            });
            instance.input_ssm_region.update(cx, |state, cx| {
                state.set_value(region, window, cx);
            });
            instance.input_ssm_remote_port.update(cx, |state, cx| {
                state.set_value(remote_port, window, cx);
            });
            instance.selected_ssm_auth_profile_id = auth_profile_id;
            if instance.selected_auth_profile_id.is_none() {
                instance.selected_auth_profile_id = auth_profile_id;
            }
        }

        instance.populate_auth_profile_dropdown(cx);
        instance.refresh_auth_profile_sessions(cx);
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
                let ssh_secret = ssh_secret.expose_secret().to_string();
                match instance.ssh_auth_method {
                    SshAuthSelection::PrivateKey => {
                        instance.input_ssh_key_passphrase.update(cx, |state, cx| {
                            state.set_value(ssh_secret.clone(), window, cx);
                        });
                    }
                    SshAuthSelection::Password => {
                        instance.input_ssh_password.update(cx, |state, cx| {
                            state.set_value(ssh_secret.clone(), window, cx);
                        });
                    }
                }
                instance.form_save_ssh_secret = true;
            }
        }

        instance.load_value_source_selectors(profile, window, cx);
        instance.sync_access_tab_mode_from_state();
        instance.populate_access_method_dropdown(cx);

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

        self.selected_auth_profile_id = None;
        self.selected_ssm_auth_profile_id = None;
        self.access_kind = None;
        self.access_tab_mode = AccessTabMode::Direct;

        self.input_name.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        if let Some(driver) = driver {
            self.create_driver_inputs(driver.form_definition(), window, cx);
        }

        self.reset_value_source_selectors(window, cx);

        self.load_settings_tab(None, None, None, window, cx);
        #[cfg(feature = "mcp")]
        self.load_mcp_dropdowns(None, window, cx);
        self.populate_auth_profile_dropdown(cx);
        self.refresh_auth_profile_sessions(cx);
        self.populate_access_method_dropdown(cx);

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
                        this.exit_edit_mode_on_blur(cx);
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

    fn reset_value_source_selectors(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.host_value_source_selector.update(cx, |selector, cx| {
            let _ = selector.set_value_ref(None, window, cx);
        });
        self.ssm_instance_id_value_source_selector
            .update(cx, |selector, cx| {
                let _ = selector.set_value_ref(None, window, cx);
            });
        self.ssm_region_value_source_selector
            .update(cx, |selector, cx| {
                let _ = selector.set_value_ref(None, window, cx);
            });
        self.ssm_remote_port_value_source_selector
            .update(cx, |selector, cx| {
                let _ = selector.set_value_ref(None, window, cx);
            });
        self.database_value_source_selector
            .update(cx, |selector, cx| {
                let _ = selector.set_value_ref(None, window, cx);
            });
        self.user_value_source_selector.update(cx, |selector, cx| {
            let _ = selector.set_value_ref(None, window, cx);
        });
        self.password_value_source_selector
            .update(cx, |selector, cx| {
                let _ = selector.set_value_ref(None, window, cx);
            });
    }

    fn load_value_source_selectors(
        &mut self,
        profile: &dbflux_core::ConnectionProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.ssm_instance_id_value_source_selector
            .update(cx, |selector, cx| {
                let primary =
                    selector.set_value_ref(profile.value_refs.get("ssm_instance_id"), window, cx);
                if !primary.is_empty() {
                    self.input_ssm_instance_id.update(cx, |state, cx| {
                        state.set_value(primary.clone(), window, cx);
                    });
                }
            });

        self.ssm_region_value_source_selector
            .update(cx, |selector, cx| {
                let primary =
                    selector.set_value_ref(profile.value_refs.get("ssm_region"), window, cx);
                if !primary.is_empty() {
                    self.input_ssm_region.update(cx, |state, cx| {
                        state.set_value(primary.clone(), window, cx);
                    });
                }
            });

        self.ssm_remote_port_value_source_selector
            .update(cx, |selector, cx| {
                let primary =
                    selector.set_value_ref(profile.value_refs.get("ssm_remote_port"), window, cx);
                if !primary.is_empty() {
                    self.input_ssm_remote_port.update(cx, |state, cx| {
                        state.set_value(primary.clone(), window, cx);
                    });
                }
            });

        self.host_value_source_selector.update(cx, |selector, cx| {
            let primary = selector.set_value_ref(profile.value_refs.get("host"), window, cx);
            if !primary.is_empty()
                && let Some(input) = self.driver_inputs.get("host")
            {
                input.update(cx, |state, cx| {
                    state.set_value(primary.clone(), window, cx);
                });
            }
        });

        self.database_value_source_selector
            .update(cx, |selector, cx| {
                let primary =
                    selector.set_value_ref(profile.value_refs.get("database"), window, cx);
                if !primary.is_empty()
                    && let Some(input) = self.driver_inputs.get("database")
                {
                    input.update(cx, |state, cx| {
                        state.set_value(primary.clone(), window, cx);
                    });
                }
            });

        self.user_value_source_selector.update(cx, |selector, cx| {
            let primary = selector.set_value_ref(profile.value_refs.get("user"), window, cx);
            if !primary.is_empty()
                && let Some(input) = self.driver_inputs.get("user")
            {
                input.update(cx, |state, cx| {
                    state.set_value(primary.clone(), window, cx);
                });
            }
        });

        self.password_value_source_selector
            .update(cx, |selector, cx| {
                let primary =
                    selector.set_value_ref(profile.value_refs.get("password"), window, cx);
                if !primary.is_empty() {
                    self.input_password.update(cx, |state, cx| {
                        state.set_value(primary, window, cx);
                    });
                }
            });

        self.sync_fields_to_uri(window, cx);
    }

    pub(super) fn collect_value_refs(&self, cx: &App) -> HashMap<String, ValueRef> {
        let mut refs = HashMap::new();

        let ssm_instance_id = self.input_ssm_instance_id.read(cx).value().to_string();
        if let Some(value_ref) = self
            .ssm_instance_id_value_source_selector
            .read(cx)
            .value_ref(&ssm_instance_id, cx)
        {
            refs.insert("ssm_instance_id".to_string(), value_ref);
        }

        let ssm_region = self.input_ssm_region.read(cx).value().to_string();
        if let Some(value_ref) = self
            .ssm_region_value_source_selector
            .read(cx)
            .value_ref(&ssm_region, cx)
        {
            refs.insert("ssm_region".to_string(), value_ref);
        }

        let ssm_remote_port = self.input_ssm_remote_port.read(cx).value().to_string();
        if let Some(value_ref) = self
            .ssm_remote_port_value_source_selector
            .read(cx)
            .value_ref(&ssm_remote_port, cx)
        {
            refs.insert("ssm_remote_port".to_string(), value_ref);
        }

        let host_value = self
            .driver_inputs
            .get("host")
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default();
        if let Some(value_ref) = self
            .host_value_source_selector
            .read(cx)
            .value_ref(&host_value, cx)
        {
            refs.insert("host".to_string(), value_ref);
        }

        let database_value = self
            .driver_inputs
            .get("database")
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default();
        if let Some(value_ref) = self
            .database_value_source_selector
            .read(cx)
            .value_ref(&database_value, cx)
        {
            refs.insert("database".to_string(), value_ref);
        }

        let user_value = self
            .driver_inputs
            .get("user")
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default();
        if let Some(value_ref) = self
            .user_value_source_selector
            .read(cx)
            .value_ref(&user_value, cx)
        {
            refs.insert("user".to_string(), value_ref);
        }

        let password_value = self.input_password.read(cx).value().to_string();
        if let Some(value_ref) = self
            .password_value_source_selector
            .read(cx)
            .value_ref(&password_value, cx)
        {
            refs.insert("password".to_string(), value_ref);
        }

        refs
    }

    pub(super) fn has_dynamic_value_ref_for_field(&self, field_id: &str, cx: &App) -> bool {
        match field_id {
            "ssm_instance_id" => !self
                .ssm_instance_id_value_source_selector
                .read(cx)
                .is_literal(cx),
            "ssm_region" => !self
                .ssm_region_value_source_selector
                .read(cx)
                .is_literal(cx),
            "ssm_remote_port" => !self
                .ssm_remote_port_value_source_selector
                .read(cx)
                .is_literal(cx),
            "host" => !self.host_value_source_selector.read(cx).is_literal(cx),
            "database" => !self.database_value_source_selector.read(cx).is_literal(cx),
            "user" => !self.user_value_source_selector.read(cx).is_literal(cx),
            "password" => !self.password_value_source_selector.read(cx).is_literal(cx),
            _ => false,
        }
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
    #[allow(dead_code)]
    fn supports_ssh(&self) -> bool {
        let Some(driver) = &self.selected_driver else {
            return false;
        };
        driver.form_definition().supports_ssh()
    }

    #[allow(dead_code)]
    fn supports_proxy(&self) -> bool {
        !self.uses_file_form()
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
            SsmInstanceId => Some("ssm_instance_id"),
            SsmRegion => Some("ssm_region"),
            SsmRemotePort => Some("ssm_remote_port"),
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

    /// Populate the MCP actor/role/policy dropdowns from the global governance state and
    /// optionally pre-select the actor/role/policy from an existing policy binding.
    #[cfg(feature = "mcp")]
    fn load_mcp_dropdowns(
        &mut self,
        binding: Option<&dbflux_core::ConnectionMcpPolicyBinding>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let clients = self
            .app_state
            .read(cx)
            .list_mcp_trusted_clients()
            .unwrap_or_default();
        let roles = self.app_state.read(cx).list_mcp_roles().unwrap_or_default();
        let policies = self
            .app_state
            .read(cx)
            .list_mcp_policies()
            .unwrap_or_default();

        let actor_items: Vec<crate::ui::components::dropdown::DropdownItem> = clients
            .iter()
            .map(|c| {
                crate::ui::components::dropdown::DropdownItem::with_value(
                    format!("{} ({})", c.name, c.id),
                    c.id.clone(),
                )
            })
            .collect();

        let mut role_items = vec![crate::ui::components::dropdown::DropdownItem::with_value(
            "No role", "",
        )];
        role_items.extend(roles.iter().map(|r| {
            let label = dbflux_mcp::builtin_display_name(&r.id)
                .map(|name| format!("{} (built-in)", name))
                .unwrap_or_else(|| r.id.clone());
            crate::ui::components::dropdown::DropdownItem::with_value(label, r.id.clone())
        }));

        let mut policy_items = vec![crate::ui::components::dropdown::DropdownItem::with_value(
            "No policy",
            "",
        )];
        policy_items.extend(policies.iter().map(|p| {
            let label = dbflux_mcp::builtin_display_name(&p.id)
                .map(|name| format!("{} (built-in)", name))
                .unwrap_or_else(|| p.id.clone());
            crate::ui::components::dropdown::DropdownItem::with_value(label, p.id.clone())
        }));

        let actor_index = binding.and_then(|b| {
            actor_items
                .iter()
                .position(|item| item.value.as_ref() == b.actor_id.as_str())
        });
        let role_index = binding.and_then(|b| {
            b.role_ids.first().and_then(|role_id| {
                role_items
                    .iter()
                    .position(|item| item.value.as_ref() == role_id.as_str())
            })
        });
        let policy_index = binding.and_then(|b| {
            b.policy_ids.first().and_then(|policy_id| {
                policy_items
                    .iter()
                    .position(|item| item.value.as_ref() == policy_id.as_str())
            })
        });

        self.conn_mcp_actor_dropdown.update(cx, |d, cx| {
            d.set_items(actor_items, cx);
            d.set_selected_index(actor_index, cx);
        });
        self.conn_mcp_role_dropdown.update(cx, |d, cx| {
            d.set_items(role_items, cx);
            d.set_selected_index(role_index.or(Some(0)), cx);
        });
        self.conn_mcp_policy_dropdown.update(cx, |d, cx| {
            d.set_items(policy_items.clone(), cx);
            d.set_selected_index(policy_index.or(Some(0)), cx);
        });

        // Load MultiSelect components with all available roles/policies
        let all_role_items: Vec<crate::ui::components::dropdown::DropdownItem> = roles
            .iter()
            .map(|r| {
                let label = dbflux_mcp::builtin_display_name(&r.id)
                    .map(|name| format!("{} (built-in)", name))
                    .unwrap_or_else(|| r.id.clone());
                crate::ui::components::dropdown::DropdownItem::with_value(label, r.id.clone())
            })
            .collect();

        let all_policy_items: Vec<crate::ui::components::dropdown::DropdownItem> = policies
            .iter()
            .map(|p| {
                let label = dbflux_mcp::builtin_display_name(&p.id)
                    .map(|name| format!("{} (built-in)", name))
                    .unwrap_or_else(|| p.id.clone());
                crate::ui::components::dropdown::DropdownItem::with_value(label, p.id.clone())
            })
            .collect();

        self.conn_mcp_role_multi_select.update(cx, |ms, cx| {
            ms.set_items(all_role_items, cx);
        });

        self.conn_mcp_policy_multi_select.update(cx, |ms, cx| {
            ms.set_items(all_policy_items, cx);
        });

        // Set selected values from binding
        if let Some(binding) = binding {
            let extra_roles: Vec<String> = binding.role_ids.iter().skip(1).cloned().collect();
            let extra_policies: Vec<String> = binding.policy_ids.iter().skip(1).cloned().collect();

            self.conn_mcp_role_multi_select.update(cx, |ms, cx| {
                ms.set_selected_values(&extra_roles, cx);
            });

            self.conn_mcp_policy_multi_select.update(cx, |ms, cx| {
                ms.set_selected_values(&extra_policies, cx);
            });
        }
    }

    /// Initialize the Settings tab controls from the selected driver's defaults
    /// and (if editing) the profile's saved overrides.
    fn load_settings_tab(
        &mut self,
        overrides: Option<&GlobalOverrides>,
        connection_settings: Option<&dbflux_core::FormValues>,
        hook_bindings: Option<&ConnectionHookBindings>,
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
            crate::ui::components::dropdown::DropdownItem::with_value("Manual", "manual"),
            crate::ui::components::dropdown::DropdownItem::with_value("Interval", "interval"),
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
            crate::ui::components::dropdown::DropdownItem::with_value(
                "Use Driver Default",
                "default",
            ),
            crate::ui::components::dropdown::DropdownItem::with_value("On", "on"),
            crate::ui::components::dropdown::DropdownItem::with_value("Off", "off"),
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

        let mut hook_items = vec![crate::ui::components::dropdown::DropdownItem::with_value(
            "No hook", "",
        )];

        let hook_definitions = self.app_state.read(cx).hook_definitions().clone();

        let mut hook_ids: Vec<String> = hook_definitions.keys().cloned().collect();
        hook_ids.sort();
        hook_items.extend(hook_ids.iter().map(|hook_id| {
            let label = hook_definitions
                .get(hook_id)
                .map(|hook| format!("{} - {}", hook_id, hook.summary()))
                .unwrap_or_else(|| hook_id.clone());

            crate::ui::components::dropdown::DropdownItem::with_value(label, hook_id)
        }));

        let (pre_selected, pre_extra) = hook_bindings
            .map(|bindings| Self::split_primary_and_extra(&bindings.pre_connect))
            .unwrap_or_default();
        let (post_selected, post_extra) = hook_bindings
            .map(|bindings| Self::split_primary_and_extra(&bindings.post_connect))
            .unwrap_or_default();
        let (pre_disconnect_selected, pre_disconnect_extra) = hook_bindings
            .map(|bindings| Self::split_primary_and_extra(&bindings.pre_disconnect))
            .unwrap_or_default();
        let (post_disconnect_selected, post_disconnect_extra) = hook_bindings
            .map(|bindings| Self::split_primary_and_extra(&bindings.post_disconnect))
            .unwrap_or_default();

        let selection_index = |selected: &str| {
            hook_items
                .iter()
                .position(|item| item.value.as_ref() == selected)
                .unwrap_or(0)
        };

        let pre_index = selection_index(&pre_selected);
        let post_index = selection_index(&post_selected);
        let pre_disconnect_index = selection_index(&pre_disconnect_selected);
        let post_disconnect_index = selection_index(&post_disconnect_selected);

        self.conn_pre_hook_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(hook_items.clone(), cx);
            dropdown.set_selected_index(Some(pre_index), cx);
        });

        self.conn_post_hook_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(hook_items.clone(), cx);
            dropdown.set_selected_index(Some(post_index), cx);
        });

        self.conn_pre_hook_extra_input.update(cx, |input, cx| {
            input.set_value(pre_extra, window, cx);
        });

        self.conn_post_hook_extra_input.update(cx, |input, cx| {
            input.set_value(post_extra, window, cx);
        });

        self.conn_pre_disconnect_hook_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_items(hook_items.clone(), cx);
                dropdown.set_selected_index(Some(pre_disconnect_index), cx);
            });

        self.conn_pre_disconnect_hook_extra_input
            .update(cx, |input, cx| {
                input.set_value(pre_disconnect_extra, window, cx);
            });

        self.conn_post_disconnect_hook_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_items(hook_items, cx);
                dropdown.set_selected_index(Some(post_disconnect_index), cx);
            });

        self.conn_post_disconnect_hook_extra_input
            .update(cx, |input, cx| {
                input.set_value(post_disconnect_extra, window, cx);
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
                    |_this, _, _event: &InputEvent, _window, _cx| {},
                ));
            }
            for dropdown in self.conn_form_state.dropdowns.values() {
                subscriptions.push(cx.subscribe(
                    dropdown,
                    |_this, _dropdown, _event: &DropdownSelectionChanged, _cx| {},
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

    fn collect_hook_bindings(&self, cx: &Context<Self>) -> Option<ConnectionHookBindings> {
        let pre_connect = Self::merge_hook_ids(
            self.conn_pre_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_pre_hook_extra_input.read(cx).value().to_string(),
        );

        let post_connect = Self::merge_hook_ids(
            self.conn_post_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_post_hook_extra_input.read(cx).value().to_string(),
        );

        let pre_disconnect = Self::merge_hook_ids(
            self.conn_pre_disconnect_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_pre_disconnect_hook_extra_input
                .read(cx)
                .value()
                .to_string(),
        );

        let post_disconnect = Self::merge_hook_ids(
            self.conn_post_disconnect_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_post_disconnect_hook_extra_input
                .read(cx)
                .value()
                .to_string(),
        );

        if pre_connect.is_empty()
            && post_connect.is_empty()
            && pre_disconnect.is_empty()
            && post_disconnect.is_empty()
        {
            return None;
        }

        Some(ConnectionHookBindings {
            pre_connect,
            post_connect,
            pre_disconnect,
            post_disconnect,
        })
    }

    fn split_primary_and_extra(hooks: &[String]) -> (String, String) {
        let Some((first, rest)) = hooks.split_first() else {
            return (String::new(), String::new());
        };

        (first.clone(), rest.join(", "))
    }

    fn merge_hook_ids(primary: Option<String>, extra_text: String) -> Vec<String> {
        let mut ordered = Vec::new();

        if let Some(primary) = primary.filter(|value| !value.trim().is_empty()) {
            ordered.push(primary);
        }

        for id in Self::parse_hook_ids(&extra_text) {
            if !ordered.iter().any(|existing| existing == &id) {
                ordered.push(id);
            }
        }

        ordered
    }

    fn parse_hook_ids(text: &str) -> Vec<String> {
        text.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect()
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

        // When use_uri is checked, the URI field is authoritative — do not
        // regenerate it from host/port or the form will overwrite a saved
        // mongodb+srv:// (or any URI-mode) connection with a reconstructed
        // mongodb://host:port/... string built from fallback fields.
        let use_uri = self
            .checkbox_states
            .get("use_uri")
            .copied()
            .unwrap_or(false);
        if use_uri {
            return;
        }

        let has_dynamic_refs = self.has_dynamic_value_ref_for_field("host", cx)
            || self.has_dynamic_value_ref_for_field("database", cx)
            || self.has_dynamic_value_ref_for_field("user", cx)
            || self.has_dynamic_value_ref_for_field("password", cx);

        if has_dynamic_refs {
            if let Some(uri_input) = self.driver_inputs.get("uri") {
                let current = uri_input.read(cx).value().to_string();
                if !current.is_empty() {
                    self.syncing_uri = true;
                    uri_input.update(cx, |state, cx| {
                        state.set_value("", window, cx);
                    });
                    self.syncing_uri = false;
                }
            }
            return;
        }

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

    // -----------------------------------------------------------------
    // Auth profile dropdown (T-7.1)
    // -----------------------------------------------------------------

    /// Populate the auth profile dropdown from the current list of saved profiles.
    fn populate_auth_profile_dropdown(&mut self, cx: &mut Context<Self>) {
        let profiles = self.app_state.read(cx).auth_profiles().to_vec();

        let mut auth_items = vec![crate::ui::components::dropdown::DropdownItem::with_value(
            "None", "",
        )];
        let mut ssm_items = vec![crate::ui::components::dropdown::DropdownItem::with_value(
            "Use Connection Auth Profile",
            "",
        )];

        self.auth_profile_uuids.clear();
        self.ssm_auth_profile_uuids.clear();

        for profile in &profiles {
            if !profile.enabled {
                continue;
            }
            let session_status = match self.auth_profile_session_states.get(&profile.id) {
                Some(AuthSessionState::Valid { .. }) => "valid",
                Some(AuthSessionState::Expired) => "expired",
                Some(AuthSessionState::LoginRequired) => "login required",
                None => "checking",
            };
            let label = format!(
                "{} — {} [{}]",
                profile.provider_id, profile.name, session_status
            );
            auth_items.push(crate::ui::components::dropdown::DropdownItem::with_value(
                label,
                profile.id.to_string(),
            ));
            let ssm_label = format!("{} [{}]", profile.name, session_status);
            ssm_items.push(crate::ui::components::dropdown::DropdownItem::with_value(
                ssm_label,
                profile.id.to_string(),
            ));
            self.auth_profile_uuids.push(profile.id);
            self.ssm_auth_profile_uuids.push(profile.id);
        }

        auth_items.push(crate::ui::components::dropdown::DropdownItem::with_value(
            "New Auth Profile...",
            "__new_auth_profile__",
        ));

        ssm_items.push(crate::ui::components::dropdown::DropdownItem::with_value(
            "New Auth Profile...",
            "__new_auth_profile__",
        ));

        let selected_index = self
            .selected_auth_profile_id
            .and_then(|id| {
                self.auth_profile_uuids
                    .iter()
                    .position(|uid| *uid == id)
                    .map(|pos| pos + 1)
            })
            .unwrap_or(0);

        self.auth_profile_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(auth_items, cx);
            dropdown.set_selected_index(Some(selected_index), cx);
        });

        let ssm_selected_index = self
            .selected_ssm_auth_profile_id
            .and_then(|id| {
                self.ssm_auth_profile_uuids
                    .iter()
                    .position(|uid| *uid == id)
                    .map(|pos| pos + 1)
            })
            .unwrap_or(0);

        self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(ssm_items, cx);
            dropdown.set_selected_index(Some(ssm_selected_index), cx);
        });
    }

    fn selected_auth_profile(&self, cx: &App) -> Option<AuthProfile> {
        let selected_id = self.selected_auth_profile_id?;

        self.app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .find(|profile| profile.id == selected_id && profile.enabled)
            .cloned()
    }

    fn refresh_auth_profile_sessions(&mut self, cx: &mut Context<Self>) {
        let profiles = self
            .app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .filter(|profile| profile.enabled)
            .cloned()
            .collect::<Vec<_>>();

        let this = cx.entity().clone();
        cx.spawn(async move |_entity, cx| {
            for profile in profiles {
                let provider = match cx.update(|cx| {
                    this.read(cx)
                        .app_state
                        .read(cx)
                        .auth_provider_by_id(&profile.provider_id)
                }) {
                    Ok(Some(provider)) => provider,
                    Ok(None) => continue,
                    Err(_) => continue,
                };

                let status = provider
                    .validate_session(&profile)
                    .await
                    .unwrap_or(AuthSessionState::LoginRequired);

                if cx
                    .update(|cx| {
                        this.update(cx, |this, cx| {
                            this.auth_profile_session_states.insert(profile.id, status);
                            this.populate_auth_profile_dropdown(cx);
                        });
                    })
                    .is_err()
                {
                    return;
                }
            }
        })
        .detach();
    }

    fn open_auth_profiles_settings(&mut self, cx: &mut Context<Self>) {
        self.open_settings_section(
            crate::ui::windows::settings::SettingsSectionId::AuthProfiles,
            cx,
        );
    }

    fn open_sso_wizard(&mut self, cx: &mut Context<Self>) {
        self.pending_wizard_auth_profile_selection = true;
        self.known_auth_profile_ids = self
            .app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .map(|profile| profile.id)
            .collect();

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(720.0), px(620.0)), cx);

        let mut options = WindowOptions {
            app_id: Some("dbflux".into()),
            titlebar: Some(TitlebarOptions {
                title: Some("AWS SSO Wizard".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        let _ = cx.open_window(options, move |window, cx| {
            let wizard = cx.new(|cx| {
                let mut wizard = SsoWizard::new(app_state.clone(), window, cx);
                wizard.open(window, cx);
                wizard
            });
            cx.new(|cx| Root::new(wizard, window, cx))
        });
    }

    fn handle_app_state_changed(&mut self, cx: &mut Context<Self>) {
        let current_profiles = self.app_state.read(cx).auth_profiles().to_vec();
        let current_ids = current_profiles
            .iter()
            .map(|profile| profile.id)
            .collect::<HashSet<_>>();

        if self.pending_wizard_auth_profile_selection {
            let newest = current_profiles
                .iter()
                .rev()
                .find(|profile| !self.known_auth_profile_ids.contains(&profile.id))
                .map(|profile| profile.id);

            if let Some(profile_id) = newest {
                self.selected_auth_profile_id = Some(profile_id);

                if self.selected_ssm_auth_profile_id.is_none() {
                    self.selected_ssm_auth_profile_id = Some(profile_id);
                }

                self.auth_profile_action_message =
                    Some("Selected profile created by AWS SSO wizard.".to_string());
            }

            self.pending_wizard_auth_profile_selection = false;
        }

        self.known_auth_profile_ids = current_ids;
        self.populate_auth_profile_dropdown(cx);
        self.refresh_auth_profile_sessions(cx);
        cx.notify();
    }

    fn handle_auth_profile_created(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.selected_auth_profile_id = Some(profile_id);

        if self.selected_ssm_auth_profile_id.is_none() {
            self.selected_ssm_auth_profile_id = Some(profile_id);
        }

        self.pending_wizard_auth_profile_selection = false;
        self.auth_profile_action_message = Some("Selected profile created by wizard.".to_string());

        self.populate_auth_profile_dropdown(cx);
        self.refresh_auth_profile_sessions(cx);
        cx.notify();
    }

    fn refresh_auth_profile_statuses(&mut self, cx: &mut Context<Self>) {
        self.auth_profile_action_message = Some("Refreshing auth profile sessions...".to_string());
        self.refresh_auth_profile_sessions(cx);
        cx.notify();
    }

    fn login_selected_auth_profile(&mut self, cx: &mut Context<Self>) {
        let Some(profile) = self.selected_auth_profile(cx) else {
            self.auth_profile_action_message =
                Some("Select an auth profile before logging in.".to_string());
            cx.notify();
            return;
        };

        if profile.provider_id != "aws-sso" {
            self.auth_profile_action_message =
                Some("Login is available only for AWS SSO profiles.".to_string());
            cx.notify();
            return;
        }

        #[cfg(feature = "aws")]
        {
            let profile_name = profile
                .fields
                .get("profile_name")
                .cloned()
                .unwrap_or_else(|| profile.name.clone());
            let region = profile.fields.get("region").cloned().unwrap_or_default();
            let start_url = profile
                .fields
                .get("sso_start_url")
                .cloned()
                .unwrap_or_default();
            let account_id = profile
                .fields
                .get("sso_account_id")
                .cloned()
                .unwrap_or_default();
            let role_name = profile
                .fields
                .get("sso_role_name")
                .cloned()
                .unwrap_or_default();

            if region.is_empty() || start_url.is_empty() {
                self.auth_profile_action_message =
                    Some("Selected AWS SSO profile is missing region or start URL.".to_string());
                cx.notify();
                return;
            }

            self.auth_profile_login_in_progress = true;
            self.auth_profile_action_message =
                Some(format!("Starting AWS SSO login for '{}'...", profile_name));
            cx.notify();

            let this = cx.entity().clone();
            let task = cx.background_executor().spawn(async move {
                dbflux_aws::login_sso_blocking(
                    profile.id,
                    &profile_name,
                    &start_url,
                    &region,
                    &account_id,
                    &role_name,
                )
            });

            cx.spawn(async move |_entity, cx| {
                let result = task.await;

                if cx
                    .update(|cx| {
                        this.update(cx, |this, cx| {
                            this.auth_profile_login_in_progress = false;

                            this.auth_profile_action_message = Some(match result {
                                Ok(_) => "AWS SSO login completed.".to_string(),
                                Err(error) => format!("AWS SSO login failed: {}", error),
                            });

                            this.refresh_auth_profile_sessions(cx);
                        });
                    })
                    .is_err()
                {
                    // Window may have closed before async completion.
                }
            })
            .detach();
        }

        #[cfg(not(feature = "aws"))]
        {
            self.auth_profile_action_message =
                Some("AWS support is disabled in this build.".to_string());
            cx.notify();
        }
    }

    fn selected_auth_profile_status_text(&self, cx: &App) -> Option<String> {
        let profile = self.selected_auth_profile(cx)?;

        let status = self.auth_profile_session_states.get(&profile.id)?;
        let text = match status {
            AuthSessionState::Valid { expires_at } => {
                if let Some(expires_at) = expires_at {
                    return Some(format!("Session status: valid (expires at {})", expires_at));
                }

                "Session status: valid"
            }
            AuthSessionState::Expired => "Session status: expired",
            AuthSessionState::LoginRequired => "Session status: login required",
        };

        Some(text.to_string())
    }

    fn selected_auth_profile_is_valid(&self, cx: &App) -> bool {
        let Some(profile) = self.selected_auth_profile(cx) else {
            return false;
        };

        matches!(
            self.auth_profile_session_states.get(&profile.id),
            Some(AuthSessionState::Valid { .. })
        )
    }

    fn selected_auth_profile_needs_login(&self, cx: &App) -> bool {
        let Some(profile) = self.selected_auth_profile(cx) else {
            return false;
        };

        if profile.provider_id != "aws-sso" {
            return false;
        }

        matches!(
            self.auth_profile_session_states.get(&profile.id),
            Some(AuthSessionState::Expired) | Some(AuthSessionState::LoginRequired)
        )
    }

    fn handle_auth_profile_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if event.index == AUTH_PROFILE_NONE_INDEX {
            self.pending_auth_profile_selection = Some(None);
        } else if event.item.value.as_ref() == "__new_auth_profile__" {
            self.open_sso_wizard(cx);

            let selected_index = self
                .selected_auth_profile_id
                .and_then(|id| {
                    self.auth_profile_uuids
                        .iter()
                        .position(|uid| *uid == id)
                        .map(|pos| pos + 1)
                })
                .unwrap_or(AUTH_PROFILE_NONE_INDEX);

            self.auth_profile_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_selected_index(Some(selected_index), cx);
            });
        } else {
            let uuid_index = event.index - 1;
            if let Some(&id) = self.auth_profile_uuids.get(uuid_index) {
                self.pending_auth_profile_selection = Some(Some(id));
            }
        }
        cx.notify();
    }

    fn apply_pending_auth_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(selection) = self.pending_auth_profile_selection.take() {
            self.selected_auth_profile_id = selection;
            self.selected_ssm_auth_profile_id = selection;

            self.sync_dynamodb_fields_from_auth_profile(window, cx);
        }
    }

    fn sync_dynamodb_fields_from_auth_profile(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.selected_driver_id() != Some("dynamodb") {
            return;
        }

        let Some(auth_profile_id) = self.selected_auth_profile_id else {
            return;
        };

        let selected_profile = self
            .app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .find(|profile| profile.id == auth_profile_id)
            .cloned();

        let Some(profile) = selected_profile else {
            return;
        };

        let profile_name = profile
            .fields
            .get("profile_name")
            .cloned()
            .unwrap_or_else(|| profile.name.clone());

        if let Some(input) = self.driver_inputs.get("profile").cloned() {
            input.update(cx, |state, cx| {
                state.set_value(profile_name, window, cx);
            });
        }

        if let Some(region) = profile.fields.get("region").cloned()
            && let Some(input) = self.driver_inputs.get("region").cloned()
        {
            input.update(cx, |state, cx| {
                state.set_value(region, window, cx);
            });
        }
    }

    fn handle_ssm_auth_profile_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if event.index == 0 {
            self.pending_ssm_auth_profile_selection = Some(None);
        } else if event.item.value.as_ref() == "__new_auth_profile__" {
            self.open_sso_wizard(cx);

            let selected_index = self
                .selected_ssm_auth_profile_id
                .and_then(|id| {
                    self.ssm_auth_profile_uuids
                        .iter()
                        .position(|uid| *uid == id)
                        .map(|pos| pos + 1)
                })
                .unwrap_or(0);

            self.ssm_auth_profile_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_selected_index(Some(selected_index), cx);
            });
        } else {
            let uuid_index = event.index - 1;
            if let Some(&id) = self.ssm_auth_profile_uuids.get(uuid_index) {
                self.pending_ssm_auth_profile_selection = Some(Some(id));
            }
        }

        cx.notify();
    }

    fn apply_pending_ssm_auth_profile(&mut self) {
        if let Some(selection) = self.pending_ssm_auth_profile_selection.take() {
            self.selected_ssm_auth_profile_id = selection;
        }
    }

    pub(super) fn handle_proxy_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if let Some(uuid) = self.proxy_uuids.get(event.index).copied() {
            self.pending_proxy_selection = Some(uuid);
            cx.notify();
        }
    }

    pub(super) fn apply_proxy(
        &mut self,
        proxy: &dbflux_core::ProxyProfile,
        _cx: &mut Context<Self>,
    ) {
        self.selected_proxy_id = Some(proxy.id);
    }

    pub(super) fn clear_proxy_selection(&mut self, cx: &mut Context<Self>) {
        self.selected_proxy_id = None;
        cx.notify();
    }

    pub(super) fn handle_ssh_tunnel_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if let Some(uuid) = self.ssh_tunnel_uuids.get(event.index).copied() {
            self.pending_ssh_tunnel_selection = Some(uuid);
            cx.notify();
        }
    }

    pub(super) fn apply_ssh_tunnel(
        &mut self,
        tunnel: &SshTunnelProfile,
        secret: Option<SecretString>,
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
                    let passphrase = passphrase.expose_secret().to_string();
                    self.input_ssh_key_passphrase.update(cx, |state, cx| {
                        state.set_value(passphrase.clone(), window, cx);
                    });
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(ref password) = secret {
                    let password = password.expose_secret().to_string();
                    self.input_ssh_password.update(cx, |state, cx| {
                        state.set_value(password.clone(), window, cx);
                    });
                }
            }
        }

        self.form_save_ssh_secret = tunnel.save_secret && secret.is_some();
        self.ssh_test_status = TestStatus::None;
        self.ssh_test_error = None;
        cx.notify();
    }

    pub(super) fn clear_ssh_tunnel_selection(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
        self.form_save_ssh_secret = true;
        self.ssh_test_status = TestStatus::None;
        self.ssh_test_error = None;
        cx.notify();
    }

    pub(super) fn save_current_ssh_as_tunnel(&mut self, cx: &mut Context<Self>) {
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
                state.save_ssh_tunnel_secret(&tunnel, &SecretString::from(secret.clone()));
            }
            state.add_ssh_tunnel(tunnel.clone());
            cx.emit(crate::app::AppStateChanged);
        });

        self.selected_ssh_tunnel_id = Some(tunnel.id);
        cx.notify();
    }

    fn effective_ssh_test_target(
        &self,
        cx: &Context<Self>,
    ) -> Option<(dbflux_core::SshTunnelConfig, Option<String>)> {
        if let Some(tunnel_id) = self.selected_ssh_tunnel_id {
            let tunnel = self
                .app_state
                .read(cx)
                .ssh_tunnels()
                .iter()
                .find(|candidate| candidate.id == tunnel_id)
                .cloned()?;

            let secret = self
                .app_state
                .read(cx)
                .get_ssh_tunnel_secret(&tunnel)
                .map(|secret| secret.expose_secret().to_string());

            return Some((tunnel.config, secret));
        }

        let ssh_config = self.build_ssh_config(cx)?;
        let ssh_secret = self.get_ssh_secret(cx);

        Some((ssh_config, ssh_secret))
    }

    pub(super) fn test_ssh_connection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.ssh_enabled {
            return;
        }

        self.ssh_test_status = TestStatus::Testing;
        self.ssh_test_error = None;
        cx.notify();

        let Some((ssh_config, ssh_secret)) = self.effective_ssh_test_target(cx) else {
            self.ssh_test_status = TestStatus::Failed;
            self.ssh_test_error = Some("SSH configuration incomplete".to_string());
            cx.notify();
            return;
        };

        let this = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            match dbflux_ssh::establish_session(&ssh_config, ssh_secret.as_deref()) {
                Ok(_session) => Ok(()),
                Err(e) => Err(format!("{:?}", e)),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(()) => {
                            this.ssh_test_status = TestStatus::Success;
                            this.ssh_test_error = None;
                        }
                        Err(e) => {
                            this.ssh_test_status = TestStatus::Failed;
                            this.ssh_test_error = Some(e);
                        }
                    }
                    cx.notify();
                });
            }) {
                log::warn!("Failed to apply SSH test result to UI state: {:?}", error);
            }
        })
        .detach();
    }

    pub(super) fn browse_ssh_key(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
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

            if let Some(path) = path
                && let Err(error) = cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_ssh_key_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
            {
                log::warn!(
                    "Failed to apply selected SSH key path to UI state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    pub(super) fn browse_file_path(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();

        let current_value = self
            .driver_inputs
            .get("path")
            .map(|input| input.read(cx).value().to_string());

        let start_dir = current_value
            .as_deref()
            .filter(|v| !v.is_empty())
            .and_then(|v| {
                let path = std::path::Path::new(v);
                path.parent().map(|p| p.to_path_buf())
            })
            .or_else(dirs::home_dir)
            .unwrap_or_default();

        let task = cx.background_executor().spawn(async move {
            let dialog = rfd::FileDialog::new()
                .set_title("Select Database File")
                .set_directory(&start_dir);
            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let path = task.await;

            if let Some(path) = path
                && let Err(error) = cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_file_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
            {
                log::warn!(
                    "Failed to apply selected file path to UI state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    // -----------------------------------------------------------------
    // Access method dropdown (T-7.2)
    // -----------------------------------------------------------------

    fn sync_access_tab_mode_from_state(&mut self) {
        self.access_tab_mode = if matches!(self.access_kind, Some(AccessKind::Managed { .. })) {
            AccessTabMode::ManagedSsm
        } else if self.selected_proxy_id.is_some()
            || matches!(self.access_kind, Some(AccessKind::Proxy { .. }))
        {
            AccessTabMode::Proxy
        } else if self.ssh_enabled
            || self.selected_ssh_tunnel_id.is_some()
            || matches!(self.access_kind, Some(AccessKind::Ssh { .. }))
        {
            AccessTabMode::Ssh
        } else {
            AccessTabMode::Direct
        };
    }

    /// Populate the access method dropdown with the unified access modes.
    fn populate_access_method_dropdown(&mut self, cx: &mut Context<Self>) {
        let items = vec![
            crate::ui::components::dropdown::DropdownItem::with_value("Direct", "direct"),
            crate::ui::components::dropdown::DropdownItem::with_value("SSH Tunnel", "ssh"),
            crate::ui::components::dropdown::DropdownItem::with_value("Proxy", "proxy"),
            crate::ui::components::dropdown::DropdownItem::with_value("SSM Port Forwarding", "ssm"),
        ];

        let selected_index = self.access_tab_mode_to_dropdown_index();

        self.access_method_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(Some(selected_index), cx);
        });
    }

    fn access_tab_mode_to_dropdown_index(&self) -> usize {
        match self.access_tab_mode {
            AccessTabMode::Direct => 0,
            AccessTabMode::Ssh => 1,
            AccessTabMode::Proxy => 2,
            AccessTabMode::ManagedSsm => 3,
        }
    }

    fn handle_access_method_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        self.access_tab_mode = match event.index {
            1 => AccessTabMode::Ssh,
            2 => AccessTabMode::Proxy,
            3 => AccessTabMode::ManagedSsm,
            _ => AccessTabMode::Direct,
        };

        match self.access_tab_mode {
            AccessTabMode::Direct => {
                self.ssh_enabled = false;
                self.selected_ssh_tunnel_id = None;
                self.selected_proxy_id = None;
                self.access_kind = None;
            }
            AccessTabMode::Ssh => {
                self.ssh_enabled = true;
                self.selected_proxy_id = None;
                self.access_kind = None;
            }
            AccessTabMode::Proxy => {
                self.ssh_enabled = false;
                self.selected_ssh_tunnel_id = None;
                self.access_kind = None;
            }
            AccessTabMode::ManagedSsm => {
                self.ssh_enabled = false;
                self.selected_ssh_tunnel_id = None;
                self.selected_proxy_id = None;
                self.access_kind = Some(self.collect_managed_access_kind(cx));
            }
        }

        cx.notify();
    }

    /// Returns true when SSM Tunnel is the currently selected access method.
    fn is_ssm_selected(&self) -> bool {
        self.access_tab_mode == AccessTabMode::ManagedSsm
    }

    /// Collect the current managed (aws-ssm) AccessKind from the inline fields.
    fn collect_managed_access_kind(&self, cx: &Context<Self>) -> AccessKind {
        let instance_id = self.input_ssm_instance_id.read(cx).value().to_string();
        let region = self.input_ssm_region.read(cx).value().to_string();
        let remote_port = self.input_ssm_remote_port.read(cx).value().to_string();

        let auth_profile_id = self
            .selected_ssm_auth_profile_id
            .or(self.selected_auth_profile_id);

        let mut params = std::collections::HashMap::new();
        params.insert("instance_id".to_string(), instance_id);
        params.insert("region".to_string(), region);
        params.insert("remote_port".to_string(), remote_port);
        if let Some(id) = auth_profile_id {
            params.insert("auth_profile_id".to_string(), id.to_string());
        }

        AccessKind::Managed {
            provider: "aws-ssm".to_string(),
            params,
        }
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for ConnectionManagerWindow {}

#[cfg(test)]
mod tests {
    use super::ConnectionManagerWindow;

    // --- parse_hook_ids ---

    #[test]
    fn parse_hook_ids_comma_separated() {
        let ids = ConnectionManagerWindow::parse_hook_ids("pre-check, lint, deploy");
        assert_eq!(ids, vec!["pre-check", "lint", "deploy"]);
    }

    #[test]
    fn parse_hook_ids_trims_whitespace() {
        let ids = ConnectionManagerWindow::parse_hook_ids("  a ,  b  , c ");
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_hook_ids_skips_empty() {
        let ids = ConnectionManagerWindow::parse_hook_ids(",, a ,, b ,,");
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn parse_hook_ids_empty_string() {
        let ids = ConnectionManagerWindow::parse_hook_ids("");
        assert!(ids.is_empty());
    }

    // --- merge_hook_ids ---

    #[test]
    fn merge_hook_ids_primary_plus_extras() {
        let result =
            ConnectionManagerWindow::merge_hook_ids(Some("main".into()), "extra1, extra2".into());
        assert_eq!(result, vec!["main", "extra1", "extra2"]);
    }

    #[test]
    fn merge_hook_ids_deduplicates() {
        let result = ConnectionManagerWindow::merge_hook_ids(Some("a".into()), "b, a, c".into());
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn merge_hook_ids_no_primary() {
        let result = ConnectionManagerWindow::merge_hook_ids(None, "x, y".into());
        assert_eq!(result, vec!["x", "y"]);
    }

    #[test]
    fn merge_hook_ids_empty_primary_is_skipped() {
        let result = ConnectionManagerWindow::merge_hook_ids(Some("  ".into()), "a".into());
        assert_eq!(result, vec!["a"]);
    }

    // --- split_primary_and_extra ---

    #[test]
    fn split_primary_and_extra_multiple() {
        let hooks = vec!["first".into(), "second".into(), "third".into()];
        let (primary, extra) = ConnectionManagerWindow::split_primary_and_extra(&hooks);
        assert_eq!(primary, "first");
        assert_eq!(extra, "second, third");
    }

    #[test]
    fn split_primary_and_extra_single() {
        let hooks = vec!["only".into()];
        let (primary, extra) = ConnectionManagerWindow::split_primary_and_extra(&hooks);
        assert_eq!(primary, "only");
        assert_eq!(extra, "");
    }

    #[test]
    fn split_primary_and_extra_empty() {
        let hooks: Vec<String> = vec![];
        let (primary, extra) = ConnectionManagerWindow::split_primary_and_extra(&hooks);
        assert_eq!(primary, "");
        assert_eq!(extra, "");
    }
}
