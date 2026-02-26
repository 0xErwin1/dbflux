use crate::ui::icons::AppIcon;
use crate::ui::windows::ssh_shared::{self, SshAuthSelection};
use dbflux_core::SshTunnelProfile;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::dialog::Dialog;
use gpui_component::input::{Input, InputState};
use gpui_component::{Icon, IconName};
use uuid::Uuid;

use super::{
    SettingsFocus, SettingsSection, SettingsWindow, SshFocus, SshFormField, SshTestStatus,
};

impl SettingsWindow {
    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active = self.active_section;
        let focused = self.focus_area == SettingsFocus::Sidebar;

        div()
            .w(px(180.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .flex()
            .flex_col()
            .p_2()
            .gap_1()
            .child(self.render_sidebar_item(
                "section-keybindings",
                "Keybindings",
                AppIcon::Keyboard,
                SettingsSection::Keybindings,
                active,
                focused && self.sidebar_index_for_section(active) == 0,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-ssh-tunnels",
                "SSH Tunnels",
                AppIcon::FingerprintPattern,
                SettingsSection::SshTunnels,
                active,
                focused && self.sidebar_index_for_section(active) == 1,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-about",
                "About",
                AppIcon::Info,
                SettingsSection::About,
                active,
                focused && self.sidebar_index_for_section(active) == 2,
                cx,
            ))
    }

    #[allow(clippy::too_many_arguments)]
    fn render_sidebar_item(
        &self,
        id: &'static str,
        label: &'static str,
        icon: AppIcon,
        section: SettingsSection,
        active: SettingsSection,
        is_focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let is_active = active == section;

        div()
            .id(id)
            .px_3()
            .py_2()
            .rounded(px(4.0))
            .text_sm()
            .cursor_pointer()
            .border_1()
            .border_color(if is_focused && !is_active {
                theme.primary
            } else {
                gpui::transparent_black()
            })
            .when(is_active, |d| {
                d.bg(theme.secondary).font_weight(FontWeight::MEDIUM)
            })
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.active_section = section;
                this.focus_area = SettingsFocus::Content;
                cx.notify();
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        svg()
                            .path(icon.path())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    )
                    .child(label),
            )
    }

    fn render_ssh_tunnels_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let (tunnels, keyring_available) = {
            let state = self.app_state.read(cx);
            (state.ssh_tunnels().to_vec(), state.secret_store_available())
        };
        let editing_id = self.editing_tunnel_id;

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("SSH Tunnels"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Manage reusable SSH tunnel configurations"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_tunnel_list(&tunnels, editing_id, cx))
                    .child(self.render_tunnel_form(editing_id, keyring_available, cx)),
            )
    }

    fn render_tunnel_list(
        &self,
        tunnels: &[SshTunnelProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_list_focused =
            self.focus_area == SettingsFocus::Content && self.ssh_focus == SshFocus::ProfileList;
        let is_new_button_focused = is_list_focused && self.ssh_selected_idx.is_none();

        div()
            .w(px(250.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_new_button_focused {
                            theme.primary
                        } else {
                            gpui::transparent_black()
                        })
                        .child(
                            Button::new("new-tunnel")
                                .icon(Icon::new(IconName::Plus))
                                .label("New Tunnel")
                                .small()
                                .w_full()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.ssh_selected_idx = None;
                                    this.clear_form(window, cx);
                                })),
                        ),
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
                    .when(tunnels.is_empty(), |d: Div| {
                        d.child(
                            div()
                                .p_4()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No saved tunnels"),
                        )
                    })
                    .children(tunnels.iter().enumerate().map(|(idx, tunnel)| {
                        let tunnel_id = tunnel.id;
                        let is_selected = editing_id == Some(tunnel_id);
                        let is_focused = is_list_focused && self.ssh_selected_idx == Some(idx);
                        let tunnel_clone = tunnel.clone();

                        div()
                            .id(SharedString::from(format!("tunnel-item-{}", tunnel_id)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_focused && !is_selected {
                                theme.primary
                            } else {
                                gpui::transparent_black()
                            })
                            .when(is_selected, |d| d.bg(theme.secondary))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.ssh_selected_idx = Some(idx);
                                this.edit_tunnel(&tunnel_clone, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        Icon::new(IconName::SquareTerminal)
                                            .size(px(14.0))
                                            .text_color(theme.muted_foreground)
                                            .mt(px(2.0)),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child(tunnel.name.clone()),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child(format!(
                                                        "{}@{}:{}",
                                                        tunnel.config.user,
                                                        tunnel.config.host,
                                                        tunnel.config.port
                                                    )),
                                            ),
                                    ),
                            )
                    })),
            )
    }

    fn render_tunnel_form(
        &self,
        editing_id: Option<Uuid>,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let auth_method = self.ssh_auth_method;
        let save_secret = self.form_save_secret;
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;
        let muted_fg = theme.muted_foreground;

        let is_form_focused =
            self.focus_area == SettingsFocus::Content && self.ssh_focus == SshFocus::Form;
        let current_field = self.ssh_form_field;

        let title = if editing_id.is_some() {
            "Edit Tunnel"
        } else {
            "New Tunnel"
        };

        let auth_selector = self
            .render_auth_selector_with_focus(auth_method, is_form_focused, current_field, cx)
            .into_any_element();
        let auth_fields = self
            .render_auth_fields_with_focus(
                auth_method,
                keyring_available,
                save_secret,
                is_form_focused,
                current_field,
                cx,
            )
            .into_any_element();

        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div().p_4().border_b_1().border_color(border).child(
                    div()
                        .text_base()
                        .font_weight(FontWeight::MEDIUM)
                        .child(title),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(self.render_form_field_with_focus(
                        "Name",
                        &self.input_tunnel_name,
                        is_form_focused && current_field == SshFormField::Name,
                        primary,
                        SshFormField::Name,
                        cx,
                    ))
                    .child(
                        div()
                            .flex()
                            .gap_3()
                            .child(div().flex_1().child(self.render_form_field_with_focus(
                                "Host",
                                &self.input_ssh_host,
                                is_form_focused && current_field == SshFormField::Host,
                                primary,
                                SshFormField::Host,
                                cx,
                            )))
                            .child(div().w(px(80.0)).child(self.render_form_field_with_focus(
                                "Port",
                                &self.input_ssh_port,
                                is_form_focused && current_field == SshFormField::Port,
                                primary,
                                SshFormField::Port,
                                cx,
                            ))),
                    )
                    .child(self.render_form_field_with_focus(
                        "Username",
                        &self.input_ssh_user,
                        is_form_focused && current_field == SshFormField::User,
                        primary,
                        SshFormField::User,
                        cx,
                    ))
                    .child(auth_selector)
                    .child(auth_fields),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(border)
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        // Test status message
                        div()
                            .h(px(20.0))
                            .flex()
                            .items_center()
                            .when(self.ssh_test_status == SshTestStatus::Testing, |d| {
                                d.child(
                                    div()
                                        .text_sm()
                                        .text_color(muted_fg)
                                        .child("Testing SSH connection..."),
                                )
                            })
                            .when(self.ssh_test_status == SshTestStatus::Success, |d| {
                                d.child(
                                    div()
                                        .text_sm()
                                        .text_color(gpui::rgb(0x22C55E))
                                        .child("SSH connection successful"),
                                )
                            })
                            .when(self.ssh_test_status == SshTestStatus::Failed, |d| {
                                let error = self
                                    .ssh_test_error
                                    .clone()
                                    .unwrap_or_else(|| "Connection failed".to_string());
                                d.child(
                                    div().text_sm().text_color(gpui::rgb(0xEF4444)).child(error),
                                )
                            }),
                    )
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .justify_end()
                            .when(editing_id.is_some(), |d| {
                                let tunnel_id = editing_id.unwrap();
                                let is_delete_focused =
                                    is_form_focused && current_field == SshFormField::DeleteButton;
                                d.child(
                                    div()
                                        .rounded(px(4.0))
                                        .border_1()
                                        .border_color(if is_delete_focused {
                                            primary
                                        } else {
                                            gpui::transparent_black()
                                        })
                                        .child(
                                            Button::new("delete-tunnel")
                                                .label("Delete")
                                                .small()
                                                .danger()
                                                .on_click(cx.listener(move |this, _, _, cx| {
                                                    this.request_delete_tunnel(tunnel_id, cx);
                                                })),
                                        ),
                                )
                            })
                            .child(div().flex_1())
                            .child({
                                let is_test_focused =
                                    is_form_focused && current_field == SshFormField::TestButton;
                                div()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if is_test_focused {
                                        primary
                                    } else {
                                        gpui::transparent_black()
                                    })
                                    .child(
                                        Button::new("test-tunnel")
                                            .label("Test Connection")
                                            .small()
                                            .ghost()
                                            .disabled(
                                                self.ssh_test_status == SshTestStatus::Testing,
                                            )
                                            .on_click(cx.listener(|this, _, _window, cx| {
                                                this.test_ssh_tunnel(cx);
                                            })),
                                    )
                            })
                            .child({
                                let is_save_focused =
                                    is_form_focused && current_field == SshFormField::SaveButton;
                                div()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if is_save_focused {
                                        primary
                                    } else {
                                        gpui::transparent_black()
                                    })
                                    .child(
                                        Button::new("save-tunnel")
                                            .label(if editing_id.is_some() {
                                                "Update"
                                            } else {
                                                "Create"
                                            })
                                            .small()
                                            .primary()
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.save_tunnel(window, cx);
                                            })),
                                    )
                            }),
                    ),
            )
    }

    fn render_about_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        const VERSION: &str = env!("CARGO_PKG_VERSION");
        const REPOSITORY: &str = env!("CARGO_PKG_REPOSITORY");
        const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");
        const LICENSE: &str = env!("CARGO_PKG_LICENSE");

        #[cfg(debug_assertions)]
        const PROFILE: &str = "debug";
        #[cfg(not(debug_assertions))]
        const PROFILE: &str = "release";

        let issues_url = format!("{}/issues", REPOSITORY);
        let author_name = AUTHORS.split('<').next().unwrap_or(AUTHORS).trim();
        let license_display = LICENSE.replace(" OR ", " and ");

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div().p_4().border_b_1().border_color(theme.border).child(
                    div()
                        .text_lg()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child("About"),
                ),
            )
            .child(
                div().flex_1().p_6().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_3()
                                .child(
                                    svg()
                                        .path(AppIcon::DbFlux.path())
                                        .size(px(48.0))
                                        .text_color(theme.foreground),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_xl()
                                                .font_weight(FontWeight::BOLD)
                                                .child("DBFlux"),
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.muted_foreground)
                                                .child(format!("{} ({})", VERSION, PROFILE)),
                                        ),
                                ),
                        )
                        .child(
                            div().text_sm().child(
                                div()
                                    .flex()
                                    .gap_1()
                                    .child(
                                        div()
                                            .id("link-issues")
                                            .text_color(theme.link)
                                            .cursor_pointer()
                                            .hover(|d| d.underline())
                                            .on_mouse_down(
                                                gpui::MouseButton::Left,
                                                move |_, _, cx| {
                                                    cx.open_url(&issues_url);
                                                },
                                            )
                                            .child("Report a bug"),
                                    )
                                    .child("or")
                                    .child(
                                        div()
                                            .id("link-repo")
                                            .text_color(theme.link)
                                            .cursor_pointer()
                                            .hover(|d| d.underline())
                                            .on_mouse_down(gpui::MouseButton::Left, |_, _, cx| {
                                                cx.open_url(REPOSITORY);
                                            })
                                            .child("view the source code"),
                                    )
                                    .child("on GitHub."),
                            ),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child(format!(
                                    "Copyright © 2026 {} and contributors.",
                                    author_name
                                )),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child(format!("Licensed under the {} licenses.", license_display)),
                        )
                        .child(
                            div()
                                .mt_4()
                                .pt_4()
                                .border_t_1()
                                .border_color(theme.border)
                                .flex()
                                .flex_col()
                                .gap_2()
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .child("Third-Party Licenses"),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("UI icons from Lucide (ISC License)"),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("Brand icons from Simple Icons (CC0 1.0)"),
                                ),
                        ),
                ),
            )
    }

    fn render_form_field_with_focus(
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
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(label.to_string()),
            )
            .child(
                div()
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(if is_focused {
                        primary
                    } else {
                        gpui::transparent_black()
                    })
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.ssh_focus = SshFocus::Form;
                            this.ssh_form_field = field;
                            this.ssh_focus_current_field(window, cx);
                            cx.notify();
                        }),
                    )
                    .child(Input::new(input).small()),
            )
    }

    fn render_auth_selector_with_focus(
        &self,
        current: SshAuthSelection,
        is_form_focused: bool,
        current_field: SshFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;

        let is_key_focused = is_form_focused && current_field == SshFormField::AuthPrivateKey;
        let is_pw_focused = is_form_focused && current_field == SshFormField::AuthPassword;

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Authentication"),
            )
            .child(
                div()
                    .flex()
                    .gap_4()
                    .child(
                        div()
                            .id("auth-key")
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_2()
                            .py_1()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_key_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::PrivateKey;
                                this.validate_ssh_form_field();
                                cx.notify();
                            }))
                            .child(ssh_shared::render_radio_button(
                                current == SshAuthSelection::PrivateKey,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Private Key")),
                    )
                    .child(
                        div()
                            .id("auth-pw")
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_2()
                            .py_1()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_pw_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::Password;
                                this.validate_ssh_form_field();
                                cx.notify();
                            }))
                            .child(ssh_shared::render_radio_button(
                                current == SshAuthSelection::Password,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Password")),
                    ),
            )
    }

    fn render_auth_fields_with_focus(
        &self,
        auth_method: SshAuthSelection,
        keyring_available: bool,
        save_secret: bool,
        is_form_focused: bool,
        current_field: SshFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;

        let is_save_secret_focused = is_form_focused && current_field == SshFormField::SaveSecret;

        let save_checkbox =
            if keyring_available {
                Some(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .pb(px(2.0))
                        .px_2()
                        .py_1()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_save_secret_focused {
                            primary
                        } else {
                            gpui::transparent_black()
                        })
                        .child(Checkbox::new("save-secret").checked(save_secret).on_click(
                            cx.listener(|this, checked: &bool, _, cx| {
                                this.form_save_secret = *checked;
                                cx.notify();
                            }),
                        ))
                        .child(div().text_sm().child("Save")),
                )
            } else {
                None
            };

        match auth_method {
            SshAuthSelection::PrivateKey => {
                let is_key_path_focused = is_form_focused && current_field == SshFormField::KeyPath;
                let is_browse_focused = is_form_focused && current_field == SshFormField::KeyBrowse;
                let is_passphrase_focused =
                    is_form_focused && current_field == SshFormField::Passphrase;

                div()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
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
                                            .border_1()
                                            .border_color(if is_key_path_focused {
                                                primary
                                            } else {
                                                gpui::transparent_black()
                                            })
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(|this, _, window, cx| {
                                                    this.ssh_focus = SshFocus::Form;
                                                    this.ssh_form_field = SshFormField::KeyPath;
                                                    this.ssh_focus_current_field(window, cx);
                                                    cx.notify();
                                                }),
                                            )
                                            .child(Input::new(&self.input_ssh_key_path).small()),
                                    )
                                    .child(
                                        div()
                                            .rounded(px(4.0))
                                            .border_1()
                                            .border_color(if is_browse_focused {
                                                primary
                                            } else {
                                                gpui::transparent_black()
                                            })
                                            .child(
                                                Button::new("browse-key")
                                                    .label("Browse")
                                                    .small()
                                                    .ghost()
                                                    .on_click(cx.listener(
                                                        |this, _, window, cx| {
                                                            this.browse_ssh_key(window, cx);
                                                        },
                                                    )),
                                            ),
                                    ),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child("Leave empty to use SSH agent"),
                            ),
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
                                    .child(div().flex_1().child(self.render_form_field_with_focus(
                                        "Key Passphrase",
                                        &self.input_ssh_key_passphrase,
                                        is_passphrase_focused,
                                        primary,
                                        SshFormField::Passphrase,
                                        cx,
                                    )))
                                    .child(
                                        Self::render_password_toggle(
                                            self.show_ssh_passphrase,
                                            "toggle-ssh-passphrase",
                                            &theme,
                                        )
                                        .on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.show_ssh_passphrase =
                                                    !this.show_ssh_passphrase;
                                                cx.notify();
                                            }),
                                        ),
                                    ),
                            )
                            .when_some(save_checkbox, |d, checkbox| d.child(checkbox)),
                    )
                    .into_any_element()
            }

            SshAuthSelection::Password => {
                let is_password_focused =
                    is_form_focused && current_field == SshFormField::Password;

                div()
                    .flex()
                    .flex_col()
                    .gap_3()
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
                                    .child(div().flex_1().child(self.render_form_field_with_focus(
                                        "Password",
                                        &self.input_ssh_password,
                                        is_password_focused,
                                        primary,
                                        SshFormField::Password,
                                        cx,
                                    )))
                                    .child(
                                        Self::render_password_toggle(
                                            self.show_ssh_password,
                                            "toggle-ssh-password",
                                            &theme,
                                        )
                                        .on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.show_ssh_password = !this.show_ssh_password;
                                                cx.notify();
                                            }),
                                        ),
                                    ),
                            )
                            .when_some(save_checkbox, |d, checkbox| d.child(checkbox)),
                    )
                    .into_any_element()
            }
        }
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

impl Render for SettingsWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_ssh_key_path.take() {
            self.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        let show_ssh_passphrase = self.show_ssh_passphrase;
        let show_ssh_password = self.show_ssh_password;
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_masked(!show_ssh_passphrase, window, cx);
        });
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_masked(!show_ssh_password, window, cx);
        });

        let theme = cx.theme();
        let show_delete_confirm = self.pending_delete_tunnel_id.is_some();

        let tunnel_name = self
            .pending_delete_tunnel_id
            .and_then(|id| {
                self.app_state
                    .read(cx)
                    .ssh_tunnels()
                    .iter()
                    .find(|t| t.id == id)
                    .map(|t| t.name.clone())
            })
            .unwrap_or_default();

        div()
            .size_full()
            .bg(theme.background)
            .flex()
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                this.handle_key_event(event, window, cx);
            }))
            .child(self.render_sidebar(cx))
            .child(match self.active_section {
                SettingsSection::Keybindings => {
                    self.render_keybindings_section(cx).into_any_element()
                }
                SettingsSection::SshTunnels => {
                    self.render_ssh_tunnels_section(cx).into_any_element()
                }
                SettingsSection::About => self.render_about_section(cx).into_any_element(),
            })
            .when(show_delete_confirm, |el| {
                let this = cx.entity().clone();
                let this_cancel = this.clone();

                el.child(
                    Dialog::new(window, cx)
                        .title("Delete SSH Tunnel")
                        .confirm()
                        .on_ok(move |_, _, cx| {
                            this.update(cx, |settings, cx| {
                                settings.confirm_delete_tunnel(cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            this_cancel.update(cx, |settings, cx| {
                                settings.cancel_delete_tunnel(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(format!(
                            "Are you sure you want to delete \"{}\"?",
                            tunnel_name
                        ))),
                )
            })
    }
}
