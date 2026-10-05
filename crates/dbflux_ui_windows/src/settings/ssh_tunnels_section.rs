use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::{FormSection, create_blur_subscription};
use super::layout;
use super::section_trait::{SectionFocusEvent, SectionPortabilityEvent};
use super::ssh_tunnels::SshFormNav;
use crate::connection_manager::ExportTarget;
use crate::labels::ssh_tunnels_delete_body;
use crate::ssh_shared::SshAuthSelection;
use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_components::controls::{Button, Checkbox, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{BannerBlock, BannerVariant, SegmentedControl, SegmentedItem};
use dbflux_core::SshTunnelProfile;
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::dialog::{Dialog, DialogButtonProps};
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum SshFocus {
    ProfileList,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum SshFormField {
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
    ExportButton,
    DeleteButton,
    TestButton,
    SaveButton,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SshTestStatus {
    None,
    Testing,
    Success,
    Failed,
}

pub(super) struct SshTunnelsSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) editing_tunnel_id: Option<Uuid>,
    pub(super) input_tunnel_name: Entity<InputState>,
    pub(super) input_ssh_host: Entity<InputState>,
    pub(super) input_ssh_port: Entity<InputState>,
    pub(super) input_ssh_user: Entity<InputState>,
    pub(super) input_ssh_key_path: Entity<InputState>,
    pub(super) input_ssh_key_passphrase: Entity<InputState>,
    pub(super) input_ssh_password: Entity<InputState>,
    pub(super) ssh_auth_method: SshAuthSelection,
    pub(super) form_save_secret: bool,
    pub(super) show_ssh_passphrase: bool,
    pub(super) show_ssh_password: bool,
    pub(super) ssh_focus: SshFocus,
    pub(super) ssh_selected_idx: Option<usize>,
    pub(super) ssh_form_field: SshFormField,
    pub(super) ssh_editing_field: bool,
    pub(super) ssh_test_status: SshTestStatus,
    pub(super) ssh_test_error: Option<String>,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    pub(super) pending_ssh_key_path: Option<String>,
    pub(super) pending_delete_tunnel_id: Option<Uuid>,
    pub(super) pending_sync_from_app_state: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for SshTunnelsSection {}
impl EventEmitter<SectionPortabilityEvent> for SshTunnelsSection {}

impl SshTunnelsSection {
    /// Ask the coordinator to export the SSH tunnel currently loaded in the form.
    pub(super) fn request_export(&mut self, cx: &mut Context<Self>) {
        if let Some(id) = self.editing_tunnel_id {
            cx.emit(SectionPortabilityEvent::OpenExport(
                ExportTarget::SshTunnel(id),
            ));
        }
    }

    /// Ask the coordinator to open the import wizard.
    pub(super) fn request_import(&mut self, cx: &mut Context<Self>) {
        cx.emit(SectionPortabilityEvent::OpenImport);
    }
}

impl FormSection for SshTunnelsSection {
    type Focus = SshFocus;
    type FormField = SshFormField;

    fn focus_area(&self) -> Self::Focus {
        self.ssh_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.ssh_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.ssh_form_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.ssh_form_field = field;
    }

    fn editing_field(&self) -> bool {
        self.ssh_editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.ssh_editing_field = editing;
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
        SshFocus::ProfileList
    }

    fn form_focus() -> Self::Focus {
        SshFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        SshFormField::Name
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        let nav = SshFormNav::new(
            self.ssh_auth_method,
            self.editing_tunnel_id,
            self.ssh_form_field,
        );
        nav.form_rows()
    }

    fn is_input_field(field: Self::FormField) -> bool {
        SshFormNav::is_input_field(field)
    }

    fn validate_form_field(&mut self) {
        self.validate_ssh_form_field();
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.ssh_editing_field = true;

        match self.ssh_form_field {
            SshFormField::Name => {
                self.input_tunnel_name
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Host => {
                self.input_ssh_host.update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Port => {
                self.input_ssh_port.update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::User => {
                self.input_ssh_user.update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::KeyPath => {
                self.input_ssh_key_path
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Passphrase => {
                self.input_ssh_key_passphrase
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            SshFormField::Password => {
                self.input_ssh_password
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            _ => {
                self.ssh_editing_field = false;
            }
        }
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.ssh_form_field {
            SshFormField::AuthPrivateKey => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                self.validate_form_field();
            }
            SshFormField::AuthPassword => {
                self.ssh_auth_method = SshAuthSelection::Password;
                self.validate_form_field();
            }
            SshFormField::KeyBrowse => {
                self.browse_ssh_key(window, cx);
            }
            SshFormField::SaveSecret => {
                self.form_save_secret = !self.form_save_secret;
            }
            SshFormField::SaveButton => {
                self.save_tunnel(window, cx);
            }
            SshFormField::TestButton => {
                self.test_ssh_tunnel(cx);
            }
            SshFormField::DeleteButton => {
                if let Some(id) = self.editing_tunnel_id {
                    self.request_delete_tunnel(id, cx);
                }
            }
            SshFormField::ExportButton => {
                self.request_export(cx);
            }
            field if Self::is_input_field(field) => {
                self.focus_current_field(window, cx);
            }
            _ => {}
        }

        cx.notify();
    }
}

impl SshTunnelsSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_tunnel_name = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.ssh_tunnels.placeholder_name"))
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
                .placeholder(dbflux_i18n::t!(
                    "settings.ssh_tunnels.placeholder_passphrase"
                ))
                .masked(true)
        });
        let input_ssh_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.ssh_tunnels.placeholder_password"))
                .masked(true)
        });

        let subscription = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.pending_sync_from_app_state = true;
            cx.notify();
        });

        let blur_tunnel_name = create_blur_subscription(cx, &input_tunnel_name);
        let blur_ssh_host = create_blur_subscription(cx, &input_ssh_host);
        let blur_ssh_port = create_blur_subscription(cx, &input_ssh_port);
        let blur_ssh_user = create_blur_subscription(cx, &input_ssh_user);
        let blur_ssh_key_path = create_blur_subscription(cx, &input_ssh_key_path);
        let blur_ssh_key_passphrase = create_blur_subscription(cx, &input_ssh_key_passphrase);
        let blur_ssh_password = create_blur_subscription(cx, &input_ssh_password);

        Self {
            app_state,
            editing_tunnel_id: None,
            input_tunnel_name,
            input_ssh_host,
            input_ssh_port,
            input_ssh_user,
            input_ssh_key_path,
            input_ssh_key_passphrase,
            input_ssh_password,
            ssh_auth_method: SshAuthSelection::PrivateKey,
            form_save_secret: true,
            show_ssh_passphrase: false,
            show_ssh_password: false,
            ssh_focus: SshFocus::ProfileList,
            ssh_selected_idx: None,
            ssh_form_field: SshFormField::Name,
            ssh_editing_field: false,
            ssh_test_status: SshTestStatus::None,
            ssh_test_error: None,
            content_focused: false,
            switching_input: false,
            pending_ssh_key_path: None,
            pending_delete_tunnel_id: None,
            pending_sync_from_app_state: false,
            _subscriptions: vec![
                subscription,
                blur_tunnel_name,
                blur_ssh_host,
                blur_ssh_port,
                blur_ssh_user,
                blur_ssh_key_path,
                blur_ssh_key_passphrase,
                blur_ssh_password,
            ],
        }
    }

    /// Eye button inside a secret field that shows or hides its value.
    fn render_password_toggle(
        show: bool,
        toggle_id: &'static str,
        on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> impl IntoElement {
        let (icon, label) = if show {
            (
                AppIcon::EyeOff,
                dbflux_i18n::t!("settings.field.hide_secret"),
            )
        } else {
            (AppIcon::Eye, dbflux_i18n::t!("settings.field.show_secret"))
        };

        Button::new(toggle_id, label)
            .ghost()
            .inline()
            .icon(icon)
            .icon_only()
            .tab_stop(false)
            .on_click(on_toggle)
    }

    fn is_cursor_on(&self, field: SshFormField) -> bool {
        self.content_focused
            && self.ssh_focus == SshFocus::Form
            && self.ssh_form_field == field
            && !self.ssh_editing_field
    }

    fn select_field(&mut self, field: SshFormField, window: &mut Window, cx: &mut Context<Self>) {
        self.switching_input = true;
        self.ssh_focus = SshFocus::Form;
        self.ssh_form_field = field;
        self.focus_current_field(window, cx);
        cx.notify();
    }

    /// Text field of the detail form, framed for the keyboard cursor.
    #[allow(clippy::too_many_arguments)]
    fn render_ssh_input(
        &self,
        input: &Entity<InputState>,
        field: SshFormField,
        label: String,
        width: Option<Rems>,
        mono: bool,
        suffix: Option<AnyElement>,
        cx: &mut Context<Self>,
    ) -> Div {
        let is_secret = matches!(field, SshFormField::Passphrase | SshFormField::Password);
        let element_id = format!("ssh-tunnel-{}", ssh_field_id(field));

        let mut control = Input::new(input)
            .id(SharedString::from(element_id))
            .aria_label(label)
            .secret(is_secret);

        if let Some(suffix) = suffix {
            control = control.suffix(suffix);
        }

        layout::field_frame(self.is_cursor_on(field), width, mono, control, cx).on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, window, cx| {
                this.select_field(field, window, cx);
            }),
        )
    }

    fn render_ssh_auth_selector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity();
        let active = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => "private-key",
            SshAuthSelection::Password => "password",
        };

        let control = SegmentedControl::new(
            vec![
                SegmentedItem::new("private-key", dbflux_i18n::t!("ssh.private_key_short"))
                    .icon(AppIcon::KeyRound),
                SegmentedItem::new(
                    "password",
                    dbflux_i18n::t!("settings.ssh_tunnels.field.password"),
                )
                .icon(AppIcon::Lock),
            ],
            active,
            move |selected: &SharedString, _window, cx| {
                let (method, field) = if selected.as_ref() == "password" {
                    (SshAuthSelection::Password, SshFormField::AuthPassword)
                } else {
                    (SshAuthSelection::PrivateKey, SshFormField::AuthPrivateKey)
                };

                entity.update(cx, |this, cx| {
                    this.ssh_focus = SshFocus::Form;
                    this.ssh_form_field = field;
                    this.ssh_auth_method = method;
                    this.validate_ssh_form_field();
                    cx.notify();
                });
            },
        );

        let cursor_item = if self.is_cursor_on(SshFormField::AuthPrivateKey) {
            Some("private-key")
        } else if self.is_cursor_on(SshFormField::AuthPassword) {
            Some("password")
        } else {
            None
        };

        let control = control
            .focused(cursor_item.is_some())
            .when_some(cursor_item, |control, id| control.focused_item(id));

        layout::form_row(
            dbflux_i18n::t!("settings.ssh_tunnels.field.method"),
            div().flex().child(control),
            None,
        )
    }

    fn render_save_secret_checkbox(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let checkbox = Checkbox::new("ssh-save-secret")
            .checked(self.form_save_secret)
            .label(dbflux_i18n::t!(
                "settings.ssh_tunnels.field.keep_in_keyring"
            ))
            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                this.ssh_focus = SshFocus::Form;
                this.ssh_form_field = SshFormField::SaveSecret;
                this.form_save_secret = *checked;
                cx.notify();
            }));

        layout::cursor_ring(self.is_cursor_on(SshFormField::SaveSecret), checkbox, cx)
    }

    fn render_private_key_fields(
        &self,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let passphrase_toggle = Self::render_password_toggle(
            self.show_ssh_passphrase,
            "toggle-ssh-passphrase",
            cx.listener(|this, _, _, cx| {
                this.show_ssh_passphrase = !this.show_ssh_passphrase;
                cx.notify();
            }),
        )
        .into_any_element();

        let key_file = layout::inline_controls()
            .child(self.render_ssh_input(
                &self.input_ssh_key_path,
                SshFormField::KeyPath,
                dbflux_i18n::t!("settings.ssh_tunnels.field.key_file"),
                None,
                true,
                None,
                cx,
            ))
            .child(
                Button::new("browse-ssh-key", dbflux_i18n::t!("ssh.browse"))
                    .secondary()
                    .icon(AppIcon::Folder)
                    .focused(self.is_cursor_on(SshFormField::KeyBrowse))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.ssh_focus = SshFocus::Form;
                        this.ssh_form_field = SshFormField::KeyBrowse;
                        this.browse_ssh_key(window, cx);
                    })),
            );

        let passphrase = layout::inline_controls()
            .gap(FormMetrics::ROW_GAP)
            .child(self.render_ssh_input(
                &self.input_ssh_key_passphrase,
                SshFormField::Passphrase,
                dbflux_i18n::t!("settings.ssh_tunnels.field.passphrase"),
                Some(SettingsMetrics::SELECT_WIDTH),
                false,
                Some(passphrase_toggle),
                cx,
            ))
            .when(keyring_available, |row| {
                row.child(self.render_save_secret_checkbox(cx))
            });

        div()
            .flex()
            .flex_col()
            .child(layout::form_row(
                dbflux_i18n::t!("settings.ssh_tunnels.field.key_file"),
                key_file,
                Some(dbflux_i18n::t!("ssh.private_key_hint").into()),
            ))
            .child(layout::form_row(
                dbflux_i18n::t!("settings.ssh_tunnels.field.passphrase"),
                passphrase,
                Some(dbflux_i18n::t!("ssh.passphrase_hint").into()),
            ))
    }

    fn render_password_fields(
        &self,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let password_toggle = Self::render_password_toggle(
            self.show_ssh_password,
            "toggle-ssh-password",
            cx.listener(|this, _, _, cx| {
                this.show_ssh_password = !this.show_ssh_password;
                cx.notify();
            }),
        )
        .into_any_element();

        let password = layout::inline_controls()
            .gap(FormMetrics::ROW_GAP)
            .child(self.render_ssh_input(
                &self.input_ssh_password,
                SshFormField::Password,
                dbflux_i18n::t!("settings.ssh_tunnels.field.password"),
                Some(SettingsMetrics::SELECT_WIDTH),
                false,
                Some(password_toggle),
                cx,
            ))
            .when(keyring_available, |row| {
                row.child(self.render_save_secret_checkbox(cx))
            });

        layout::form_row(
            dbflux_i18n::t!("settings.ssh_tunnels.field.password"),
            password,
            None,
        )
    }

    fn render_ssh_list(
        &self,
        tunnels: &[SshTunnelProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let is_list_focused = self.content_focused && self.ssh_focus == SshFocus::ProfileList;
        let is_new_button_focused = is_list_focused && self.ssh_selected_idx.is_none();

        let toolbar = layout::master_list_toolbar(vec![
            Button::new(
                "new-ssh-tunnel",
                dbflux_i18n::t!("settings.ssh_tunnels.new"),
            )
            .primary()
            .icon(AppIcon::Plus)
            .focused(is_new_button_focused)
            .on_click(cx.listener(|this, _, window, cx| {
                this.clear_form(window, cx);
            }))
            .into_any_element(),
            Button::new(
                "import-ssh-tunnel",
                dbflux_i18n::t!("settings.ssh_tunnels.action.import"),
            )
            .secondary()
            .icon(AppIcon::Download)
            .on_click(cx.listener(|this, _, _, cx| {
                this.request_import(cx);
            }))
            .into_any_element(),
        ]);

        let rows: Vec<AnyElement> = tunnels
            .iter()
            .enumerate()
            .map(|(idx, tunnel)| {
                let tunnel_id = tunnel.id;
                let is_selected = editing_id == Some(tunnel_id);
                let is_focused = is_list_focused && self.ssh_selected_idx == Some(idx);
                let auth_label = match tunnel.config.auth_method {
                    dbflux_core::SshAuthMethod::PrivateKey { .. } => {
                        dbflux_i18n::t!("settings.ssh_tunnels.list.key")
                    }
                    dbflux_core::SshAuthMethod::Password => {
                        dbflux_i18n::t!("settings.ssh_tunnels.list.password")
                    }
                };
                let detail = format!(
                    "{}@{}:{} · {}",
                    tunnel.config.user, tunnel.config.host, tunnel.config.port, auth_label
                );

                layout::master_list_row(
                    SharedString::from(format!("ssh-tunnel-item-{}", tunnel_id)),
                    layout::MasterRow {
                        icon: Some(AppIcon::Lock),
                        title: tunnel.name.clone().into(),
                        detail: Some(detail.into()),
                        trailing: None,
                    },
                    is_selected,
                    is_focused,
                    cx,
                )
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.ssh_selected_idx = Some(idx);
                    this.edit_tunnel_at_selected_index(window, cx);
                    this.ssh_focus = SshFocus::Form;
                    this.ssh_form_field = SshFormField::Name;
                }))
                .into_any_element()
            })
            .collect();

        let body = if rows.is_empty() {
            layout::master_list_empty(dbflux_i18n::t!("settings.ssh_tunnels.empty"))
                .into_any_element()
        } else {
            div().flex().flex_col().children(rows).into_any_element()
        };

        layout::master_list_panel("ssh-tunnel-list", toolbar, body, cx)
    }

    fn render_test_status(&self) -> Option<AnyElement> {
        let banner = match self.ssh_test_status {
            SshTestStatus::None => return None,
            SshTestStatus::Testing => {
                BannerBlock::new(BannerVariant::Info, dbflux_i18n::t!("access.testing_ssh"))
            }
            SshTestStatus::Success => BannerBlock::new(
                BannerVariant::Success,
                dbflux_i18n::t!("access.ssh_success"),
            ),
            SshTestStatus::Failed => {
                BannerBlock::new(BannerVariant::Danger, dbflux_i18n::t!("access.ssh_failed"))
                    .with_body(self.ssh_test_error.clone().unwrap_or_default())
            }
        };

        Some(
            div()
                .mt(FormMetrics::ROW_GAP)
                .child(banner)
                .into_any_element(),
        )
    }

    fn render_ssh_form(&self, keyring_available: bool, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        let host_port = layout::inline_controls()
            .child(self.render_ssh_input(
                &self.input_ssh_host,
                SshFormField::Host,
                dbflux_i18n::t!("ssh.host"),
                None,
                true,
                None,
                cx,
            ))
            .child(self.render_ssh_input(
                &self.input_ssh_port,
                SshFormField::Port,
                dbflux_i18n::t!("ssh.port"),
                Some(SettingsMetrics::PORT_FIELD_WIDTH),
                true,
                None,
                cx,
            ));

        layout::sticky_form_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.ssh_tunnels.group.tunnel"),
                Some(AppIcon::Lock.into()),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .child(layout::form_row(
                    dbflux_i18n::t!("settings.ssh_tunnels.field.name"),
                    self.render_ssh_input(
                        &self.input_tunnel_name,
                        SshFormField::Name,
                        dbflux_i18n::t!("settings.ssh_tunnels.field.name"),
                        Some(SettingsMetrics::TEXT_FIELD_WIDTH),
                        false,
                        None,
                        cx,
                    ),
                    None,
                ))
                .child(layout::form_row(
                    dbflux_i18n::t!("settings.ssh_tunnels.field.host_port"),
                    host_port,
                    None,
                ))
                .child(layout::form_row(
                    dbflux_i18n::t!("settings.ssh_tunnels.field.user"),
                    self.render_ssh_input(
                        &self.input_ssh_user,
                        SshFormField::User,
                        dbflux_i18n::t!("ssh.username"),
                        Some(SettingsMetrics::TEXT_FIELD_WIDTH),
                        true,
                        None,
                        cx,
                    ),
                    None,
                ))
                .child(dbflux_components::composites::section_header(
                    dbflux_i18n::t!("settings.ssh_tunnels.group.authentication"),
                    Some(AppIcon::KeyRound.into()),
                    cx,
                ))
                .child(self.render_ssh_auth_selector(cx))
                .child(match self.ssh_auth_method {
                    SshAuthSelection::PrivateKey => self
                        .render_private_key_fields(keyring_available, cx)
                        .into_any_element(),
                    SshAuthSelection::Password => self
                        .render_password_fields(keyring_available, cx)
                        .into_any_element(),
                })
                .children(self.render_test_status()),
            None,
            &theme,
        )
    }

    /// Export and Delete, on the left of the footer, for a saved tunnel.
    fn render_section_footer_leading_actions(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let tunnel_id = self.editing_tunnel_id?;

        Some(
            layout::inline_controls()
                .child(
                    Button::new(
                        "export-ssh-tunnel",
                        dbflux_i18n::t!("settings.ssh_tunnels.action.export"),
                    )
                    .secondary()
                    .icon(AppIcon::ExternalLink)
                    .focused(self.is_cursor_on(SshFormField::ExportButton))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.request_export(cx);
                    })),
                )
                .child(
                    Button::new(
                        "delete-ssh-tunnel",
                        dbflux_i18n::t!("settings.ssh_tunnels.action.delete"),
                    )
                    .danger()
                    .icon(AppIcon::Delete)
                    .focused(self.is_cursor_on(SshFormField::DeleteButton))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.request_delete_tunnel(tunnel_id, cx);
                    })),
                )
                .into_any_element(),
        )
    }

    /// Test and Save, on the right of the footer.
    fn render_section_footer_actions(
        &self,
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        layout::inline_controls()
            .child(
                Button::new(
                    "test-ssh-tunnel",
                    dbflux_i18n::t!("settings.ssh_tunnels.action.test"),
                )
                .secondary()
                .icon(AppIcon::Plug)
                .focused(self.is_cursor_on(SshFormField::TestButton))
                .disabled(self.ssh_test_status == SshTestStatus::Testing)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.test_ssh_tunnel(cx);
                })),
            )
            .child(
                Button::new(
                    "save-ssh-tunnel",
                    if editing_id.is_some() {
                        dbflux_i18n::t!("ssh.update")
                    } else {
                        dbflux_i18n::t!("ssh.create")
                    },
                )
                .primary()
                .icon(AppIcon::Check)
                .when_some(crate::settings::save_shortcut(), Button::kbd)
                .focused(self.is_cursor_on(SshFormField::SaveButton))
                .on_click(cx.listener(|this, _, window, cx| {
                    this.save_tunnel(window, cx);
                })),
            )
            .into_any_element()
    }
}

/// Element-id suffix of a detail form field.
fn ssh_field_id(field: SshFormField) -> &'static str {
    match field {
        SshFormField::Name => "name",
        SshFormField::Host => "host",
        SshFormField::Port => "port",
        SshFormField::User => "user",
        SshFormField::KeyPath => "key-path",
        SshFormField::Passphrase => "passphrase",
        SshFormField::Password => "password",
        _ => "control",
    }
}

impl SettingsSection for SshTunnelsSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::SshTunnels
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        SshTunnelsSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.set_editing_field(false);
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_ssh_changes(cx)
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_section_footer_actions(self.editing_tunnel_id, cx))
    }

    fn render_footer_leading_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.render_section_footer_leading_actions(cx)
    }

    fn save_from_shortcut(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.save_tunnel(window, cx);
    }
}

impl Render for SshTunnelsSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_app_state {
            self.pending_sync_from_app_state = false;
            self.sync_from_app_state(window, cx);
        }

        if let Some(key_path) = self.pending_ssh_key_path.take() {
            self.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(key_path, window, cx);
            });
            self.ssh_focus = SshFocus::Form;
            self.ssh_form_field = SshFormField::KeyPath;
        }

        let show_ssh_passphrase = self.show_ssh_passphrase;
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_masked(!show_ssh_passphrase, window, cx);
        });

        let show_ssh_password = self.show_ssh_password;
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_masked(!show_ssh_password, window, cx);
        });

        let (tunnels, keyring_available) = {
            let state = self.app_state.read(cx);
            (state.ssh_tunnels().to_vec(), state.secret_store_available())
        };

        let editing_id = self.editing_tunnel_id;
        let show_delete_confirm = self.pending_delete_tunnel_id.is_some();

        let tunnel_delete_name = self
            .pending_delete_tunnel_id
            .and_then(|tunnel_id| {
                self.app_state
                    .read(cx)
                    .ssh_tunnels()
                    .iter()
                    .find(|tunnel| tunnel.id == tunnel_id)
                    .map(|tunnel| tunnel.name.clone())
            })
            .unwrap_or_default();

        layout::split_section_shell(
            cx.theme().border,
            dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.ssh_tunnels.section_title"),
                dbflux_i18n::t!("settings.ssh_tunnels.section_description"),
                cx,
            ),
            self.render_ssh_list(&tunnels, editing_id, cx),
            self.render_ssh_form(keyring_available, cx),
        )
        .when(show_delete_confirm, |element| {
            let entity = cx.entity().clone();
            let entity_cancel = entity.clone();

            element.child(
                Dialog::new(cx)
                    .title(dbflux_i18n::t!("settings.ssh_tunnels.delete_dialog_title"))
                    .button_props(DialogButtonProps::default().show_cancel(true))
                    .overlay_closable(false)
                    .close_button(false)
                    .on_ok(move |_, _, cx| {
                        entity.update(cx, |section, cx| {
                            section.confirm_delete_tunnel(cx);
                        });
                        true
                    })
                    .on_cancel(move |_, _, cx| {
                        entity_cancel.update(cx, |section, cx| {
                            section.cancel_delete_tunnel(cx);
                        });
                        true
                    })
                    .child(
                        div()
                            .text_sm()
                            .child(ssh_tunnels_delete_body(&tunnel_delete_name)),
                    ),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    const SSH_TUNNELS_KEYS: &[&str] = &[
        "settings.ssh_tunnels.placeholder_name",
        "settings.ssh_tunnels.placeholder_password",
        "settings.ssh_tunnels.placeholder_passphrase",
        "settings.ssh_tunnels.section_title",
        "settings.ssh_tunnels.section_description",
        "settings.ssh_tunnels.empty",
        "settings.ssh_tunnels.new",
        "settings.ssh_tunnels.field.name",
        "settings.ssh_tunnels.delete_dialog_title",
        "settings.ssh_tunnels.delete_dialog.body",
        "settings.ssh_tunnels.action.import",
        "settings.ssh_tunnels.action.export",
        "settings.ssh_tunnels.action.delete",
        "settings.ssh_tunnels.error.host_and_user_required",
    ];

    // Reused from the shared `ssh.*` vocabulary (slice S10, `access_tab.rs`).
    const SSH_TUNNELS_REUSED_SSH_KEYS: &[&str] = &[
        "ssh.authentication",
        "ssh.private_key",
        "ssh.password",
        "ssh.private_key_path",
        "ssh.browse",
        "ssh.key_passphrase",
        "ssh.save",
        "ssh.passphrase_hint",
        "ssh.private_key_hint",
        "ssh.ssh_password",
        "ssh.host",
        "ssh.port",
        "ssh.username",
        "ssh.private_key_short",
        "ssh.update",
        "ssh.create",
        "ssh.test",
    ];

    // Reused from the `access.*` namespace (slice S10, `access_tab.rs`) since
    // the SSH test-status strings are identical to the Access tab's.
    const SSH_TUNNELS_REUSED_ACCESS_KEYS: &[&str] = &[
        "access.testing_ssh",
        "access.ssh_success",
        "access.ssh_failed",
        "access.ssh_tunnel_label",
    ];

    #[test]
    fn ssh_tunnels_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in SSH_TUNNELS_KEYS
                .iter()
                .chain(SSH_TUNNELS_REUSED_SSH_KEYS)
                .chain(SSH_TUNNELS_REUSED_ACCESS_KEYS)
            {
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
    fn ssh_tunnels_section_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.ssh_tunnels.section_title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.ssh_tunnels.section_title", locale = "es");

        assert_eq!(english, "SSH tunnels");
        assert_eq!(spanish, "Túneles SSH");
        assert_ne!(english, spanish);
    }

    #[test]
    fn ssh_tunnels_empty_state_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.ssh_tunnels.empty", locale = "en");
        let spanish = dbflux_i18n::t!("settings.ssh_tunnels.empty", locale = "es");

        assert_eq!(english, "No saved SSH tunnels");
        assert_eq!(spanish, "No hay túneles SSH guardados");
        assert_ne!(english, spanish);
    }
}
