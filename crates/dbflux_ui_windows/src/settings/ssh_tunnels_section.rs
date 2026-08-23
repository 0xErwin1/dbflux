use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::{FormSection, create_blur_subscription};
use super::layout;
use super::section_trait::{SectionFocusEvent, SectionPortabilityEvent};
use super::ssh_tunnels::SshFormNav;
use crate::connection_manager::ExportTarget;
use crate::labels::ssh_tunnels_delete_body;
use crate::ssh_shared::{self, SshAuthSelection};
use dbflux_components::controls::Button;
use dbflux_components::controls::{GpuiInput as Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::focus_frame;
use dbflux_components::primitives::{Icon as FluxIcon, Label};
use dbflux_components::tokens::{Heights, Radii};
use dbflux_components::typography::{Body, MonoCaption, MonoMeta, PanelTitle};
use dbflux_core::SshTunnelProfile;
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::prelude::*;
use gpui::*;
use gpui_component::checkbox::Checkbox;
use gpui_component::dialog::Dialog;
use gpui_component::{ActiveTheme, Icon, Sizable};
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

    fn render_password_toggle(
        show: bool,
        toggle_id: &'static str,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let icon_name = if show { AppIcon::EyeOff } else { AppIcon::Eye };

        div()
            .id(toggle_id)
            .w(px(32.0))
            .h(px(32.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover({
                let secondary = theme.secondary;
                move |div| div.bg(secondary)
            })
            .child(
                FluxIcon::new(icon_name)
                    .size(Heights::ICON_SM)
                    .color(theme.muted_foreground),
            )
    }

    fn render_ssh_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        field: SshFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()))
            .child(
                focus_frame(
                    is_focused,
                    Some(primary),
                    layout::compact_input_shell(Input::new(input).small()),
                    cx,
                )
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, window, cx| {
                        this.switching_input = true;
                        this.ssh_focus = SshFocus::Form;
                        this.ssh_form_field = field;
                        this.focus_current_field(window, cx);
                        cx.notify();
                    }),
                ),
            )
    }

    fn render_ssh_auth_selector(
        &self,
        is_form_focused: bool,
        current_field: SshFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;
        let border = theme.border;
        let current_auth = self.ssh_auth_method;

        let is_private_key_focused =
            is_form_focused && current_field == SshFormField::AuthPrivateKey;
        let is_password_focused = is_form_focused && current_field == SshFormField::AuthPassword;

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Label::new(dbflux_i18n::t!("ssh.authentication")))
            .child(
                div()
                    .flex()
                    .gap_4()
                    .child(
                        div()
                            .id("ssh-auth-private-key")
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_2()
                            .py_1()
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_private_key_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::PrivateKey;
                                this.validate_ssh_form_field();
                                cx.notify();
                            }))
                            .child(ssh_shared::render_radio_button(
                                current_auth == SshAuthSelection::PrivateKey,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child(dbflux_i18n::t!("ssh.private_key"))),
                    )
                    .child(
                        div()
                            .id("ssh-auth-password")
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_2()
                            .py_1()
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_password_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::Password;
                                this.validate_ssh_form_field();
                                cx.notify();
                            }))
                            .child(ssh_shared::render_radio_button(
                                current_auth == SshAuthSelection::Password,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child(dbflux_i18n::t!("ssh.password"))),
                    ),
            )
    }

    fn render_save_secret_checkbox(
        &self,
        is_form_focused: bool,
        current_field: SshFormField,
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_save_secret_focused = is_form_focused && current_field == SshFormField::SaveSecret;

        div()
            .flex()
            .items_center()
            .gap_2()
            .pb(px(2.0))
            .px_2()
            .py_1()
            .rounded(Radii::SM)
            .border_1()
            .border_color(if is_save_secret_focused {
                primary
            } else {
                transparent_black()
            })
            .child(
                Checkbox::new("ssh-save-secret")
                    .checked(self.form_save_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_secret = *checked;
                        cx.notify();
                    })),
            )
            .child(div().text_sm().child(dbflux_i18n::t!("ssh.save")))
    }

    fn render_private_key_fields(
        &self,
        keyring_available: bool,
        is_form_focused: bool,
        current_field: SshFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;
        let _muted_foreground = theme.muted_foreground;

        let password_toggle =
            Self::render_password_toggle(self.show_ssh_passphrase, "toggle-ssh-passphrase", &theme)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.show_ssh_passphrase = !this.show_ssh_passphrase;
                    cx.notify();
                }));

        let save_checkbox = if keyring_available {
            Some(
                self.render_save_secret_checkbox(is_form_focused, current_field, primary, cx)
                    .into_any_element(),
            )
        } else {
            None
        };

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .items_end()
                    .gap_3()
                    .child(div().flex_1().child(self.render_ssh_field(
                        &dbflux_i18n::t!("ssh.private_key_path"),
                        &self.input_ssh_key_path,
                        is_form_focused && current_field == SshFormField::KeyPath,
                        primary,
                        SshFormField::KeyPath,
                        cx,
                    )))
                    .child({
                        let is_browse_focused =
                            is_form_focused && current_field == SshFormField::KeyBrowse;

                        div()
                            .rounded(Radii::SM)
                            .border_1()
                            .border_color(if is_browse_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .child(
                                Button::new("browse-ssh-key", dbflux_i18n::t!("ssh.browse"))
                                    .small()
                                    .ghost()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.browse_ssh_key(window, cx);
                                    })),
                            )
                    }),
            )
            .child(
                Body::new(dbflux_i18n::t!("ssh.private_key_hint"))
                    .color(cx.theme().muted_foreground),
            )
            .child(
                div()
                    .flex()
                    .items_end()
                    .gap_3()
                    .child(
                        div()
                            .flex_1()
                            .flex()
                            .items_end()
                            .gap_1()
                            .child(div().flex_1().child(self.render_ssh_field(
                                &dbflux_i18n::t!("ssh.key_passphrase"),
                                &self.input_ssh_key_passphrase,
                                is_form_focused && current_field == SshFormField::Passphrase,
                                primary,
                                SshFormField::Passphrase,
                                cx,
                            )))
                            .child(password_toggle),
                    )
                    .when_some(save_checkbox, |div, checkbox| div.child(checkbox)),
            )
            .child(
                Body::new(dbflux_i18n::t!("ssh.passphrase_hint"))
                    .color(cx.theme().muted_foreground),
            )
    }

    fn render_password_fields(
        &self,
        keyring_available: bool,
        is_form_focused: bool,
        current_field: SshFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;

        let password_toggle =
            Self::render_password_toggle(self.show_ssh_password, "toggle-ssh-password", &theme)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.show_ssh_password = !this.show_ssh_password;
                    cx.notify();
                }));

        let save_checkbox = if keyring_available {
            Some(
                self.render_save_secret_checkbox(is_form_focused, current_field, primary, cx)
                    .into_any_element(),
            )
        } else {
            None
        };

        div()
            .flex()
            .items_end()
            .gap_3()
            .child(
                div()
                    .flex_1()
                    .flex()
                    .items_end()
                    .gap_1()
                    .child(div().flex_1().child(self.render_ssh_field(
                        &dbflux_i18n::t!("ssh.ssh_password"),
                        &self.input_ssh_password,
                        is_form_focused && current_field == SshFormField::Password,
                        primary,
                        SshFormField::Password,
                        cx,
                    )))
                    .child(password_toggle),
            )
            .when_some(save_checkbox, |div, checkbox| div.child(checkbox))
    }

    fn render_ssh_list(
        &self,
        tunnels: &[SshTunnelProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_list_focused = self.ssh_focus == SshFocus::ProfileList;
        let is_new_button_focused = is_list_focused && self.ssh_selected_idx.is_none();

        div()
            .w(px(250.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div()
                    .p_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .rounded(Radii::SM)
                            .border_1()
                            .border_color(if is_new_button_focused {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .child(
                                Button::new(
                                    "new-ssh-tunnel",
                                    dbflux_i18n::t!("settings.ssh_tunnels.new"),
                                )
                                .icon(Icon::new(AppIcon::Plus))
                                .small()
                                .w_full()
                                .on_click(cx.listener(
                                    |this, _, window, cx| {
                                        this.clear_form(window, cx);
                                    },
                                )),
                            ),
                    )
                    .child(
                        Button::new(
                            "import-ssh-tunnel",
                            dbflux_i18n::t!("settings.ssh_tunnels.action.import"),
                        )
                        .icon(Icon::new(AppIcon::Download))
                        .small()
                        .ghost()
                        .w_full()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.request_import(cx);
                        })),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(tunnels.is_empty(), |root: Div| {
                        root.child(
                            div().p_4().child(
                                Body::new(dbflux_i18n::t!("settings.ssh_tunnels.empty"))
                                    .color(theme.muted_foreground),
                            ),
                        )
                    })
                    .children(tunnels.iter().enumerate().map(|(idx, tunnel)| {
                        let tunnel_id = tunnel.id;
                        let is_selected = editing_id == Some(tunnel_id);
                        let is_focused = is_list_focused && self.ssh_selected_idx == Some(idx);
                        let subtitle = format!(
                            "{}@{}:{}",
                            tunnel.config.user, tunnel.config.host, tunnel.config.port
                        );
                        let auth_label = match tunnel.config.auth_method {
                            dbflux_core::SshAuthMethod::PrivateKey { .. } => {
                                dbflux_i18n::t!("ssh.private_key_short")
                            }
                            dbflux_core::SshAuthMethod::Password => dbflux_i18n::t!("ssh.password"),
                        };

                        div()
                            .id(SharedString::from(format!("ssh-tunnel-item-{}", tunnel_id)))
                            .px_3()
                            .py_2()
                            .rounded(Radii::SM)
                            .bg(theme.list_even)
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_focused && !is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .when(is_selected, |div| div.bg(theme.secondary))
                            .hover({
                                let secondary = theme.secondary;
                                move |div| div.bg(secondary)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.ssh_selected_idx = Some(idx);
                                this.edit_tunnel_at_selected_index(window, cx);
                                this.ssh_focus = SshFocus::Form;
                                this.ssh_form_field = SshFormField::Name;
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        div().flex_shrink_0().mt(px(2.0)).child(
                                            FluxIcon::new(AppIcon::Globe)
                                                .size(px(14.0))
                                                .color(theme.muted_foreground),
                                        ),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .min_w_0()
                                            .gap_1()
                                            .child(Body::new(tunnel.name.clone()))
                                            .child(MonoMeta::new(subtitle))
                                            .child(MonoCaption::new(auth_label)),
                                    ),
                            )
                    })),
            )
    }

    fn render_test_status(&self, _cx: &mut Context<Self>) -> Option<AnyElement> {
        match self.ssh_test_status {
            SshTestStatus::None => None,
            SshTestStatus::Testing => Some(
                Body::new(dbflux_i18n::t!("access.testing_ssh"))
                    .color(_cx.theme().muted_foreground)
                    .into_any_element(),
            ),
            SshTestStatus::Success => Some(
                Body::new(dbflux_i18n::t!("access.ssh_success"))
                    .color(_cx.theme().success)
                    .into_any_element(),
            ),
            SshTestStatus::Failed => Some(
                Body::new(
                    self.ssh_test_error
                        .clone()
                        .unwrap_or_else(|| dbflux_i18n::t!("access.ssh_failed")),
                )
                .color(_cx.theme().danger)
                .into_any_element(),
            ),
        }
    }

    fn render_ssh_form(
        &self,
        editing_id: Option<Uuid>,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;

        let is_form_focused = self.ssh_focus == SshFocus::Form;
        let field = self.ssh_form_field;

        layout::sticky_form_shell(
            PanelTitle::new(layout::editor_panel_title(
                &dbflux_i18n::t!("access.ssh_tunnel_label"),
                editing_id.is_some(),
            )),
            div()
                .flex()
                .flex_col()
                .gap_4()
                .child(self.render_ssh_field(
                    &dbflux_i18n::t!("settings.ssh_tunnels.field.name"),
                    &self.input_tunnel_name,
                    is_form_focused && field == SshFormField::Name,
                    primary,
                    SshFormField::Name,
                    cx,
                ))
                .child(
                    div()
                        .flex()
                        .gap_3()
                        .child(div().flex_1().child(self.render_ssh_field(
                            &dbflux_i18n::t!("ssh.host"),
                            &self.input_ssh_host,
                            is_form_focused && field == SshFormField::Host,
                            primary,
                            SshFormField::Host,
                            cx,
                        )))
                        .child(div().w(px(80.0)).child(self.render_ssh_field(
                            &dbflux_i18n::t!("ssh.port"),
                            &self.input_ssh_port,
                            is_form_focused && field == SshFormField::Port,
                            primary,
                            SshFormField::Port,
                            cx,
                        ))),
                )
                .child(self.render_ssh_field(
                    &dbflux_i18n::t!("ssh.username"),
                    &self.input_ssh_user,
                    is_form_focused && field == SshFormField::User,
                    primary,
                    SshFormField::User,
                    cx,
                ))
                .child(self.render_ssh_auth_selector(is_form_focused, field, cx))
                .child(match self.ssh_auth_method {
                    SshAuthSelection::PrivateKey => self
                        .render_private_key_fields(keyring_available, is_form_focused, field, cx)
                        .into_any_element(),
                    SshAuthSelection::Password => self
                        .render_password_fields(keyring_available, is_form_focused, field, cx)
                        .into_any_element(),
                })
                .when_some(self.render_test_status(cx), |div, status| div.child(status)),
            None,
            &theme,
        )
    }

    fn render_section_footer_actions(
        &self,
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let is_form_focused = self.ssh_focus == SshFocus::Form;
        let field = self.ssh_form_field;
        let primary = cx.theme().primary;

        div()
            .flex()
            .items_center()
            .gap_3()
            .when(editing_id.is_some(), |root| {
                let tunnel_id = editing_id.expect("checked is_some");

                root.child(layout::footer_action_frame(
                    is_form_focused && field == SshFormField::ExportButton,
                    primary,
                    Button::new(
                        "export-ssh-tunnel",
                        dbflux_i18n::t!("settings.ssh_tunnels.action.export"),
                    )
                    .small()
                    .ghost()
                    .w_full()
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.request_export(cx);
                    })),
                ))
                .child(layout::footer_action_frame(
                    is_form_focused && field == SshFormField::DeleteButton,
                    primary,
                    Button::new(
                        "delete-ssh-tunnel",
                        dbflux_i18n::t!("settings.ssh_tunnels.action.delete"),
                    )
                    .small()
                    .danger()
                    .w_full()
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.request_delete_tunnel(tunnel_id, cx);
                    })),
                ))
            })
            .child(layout::footer_action_frame(
                is_form_focused && field == SshFormField::TestButton,
                primary,
                Button::new("test-ssh-tunnel", dbflux_i18n::t!("ssh.test"))
                    .small()
                    .ghost()
                    .w_full()
                    .disabled(self.ssh_test_status == SshTestStatus::Testing)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.test_ssh_tunnel(cx);
                    })),
            ))
            .child(layout::footer_action_frame(
                is_form_focused && field == SshFormField::SaveButton,
                primary,
                Button::new(
                    "save-ssh-tunnel",
                    if editing_id.is_some() {
                        dbflux_i18n::t!("ssh.update")
                    } else {
                        dbflux_i18n::t!("ssh.create")
                    },
                )
                .small()
                .primary()
                .w_full()
                .on_click(cx.listener(|this, _, window, cx| {
                    this.save_tunnel(window, cx);
                })),
            ))
            .into_any_element()
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
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.ssh_tunnels.section_title"),
                dbflux_i18n::t!("settings.ssh_tunnels.section_description"),
                cx,
            ),
            self.render_ssh_list(&tunnels, editing_id, cx),
            self.render_ssh_form(editing_id, keyring_available, cx),
        )
        .when(show_delete_confirm, |element| {
            let entity = cx.entity().clone();
            let entity_cancel = entity.clone();

            element.child(
                Dialog::new(window, cx)
                    .title(dbflux_i18n::t!("settings.ssh_tunnels.delete_dialog_title"))
                    .confirm()
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

        assert_eq!(english, "SSH Tunnels");
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
