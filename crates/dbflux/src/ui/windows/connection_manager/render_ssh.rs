use crate::ui::components::dropdown::DropdownItem;
use crate::ui::windows::ssh_shared::{self, SshAuthSelection};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::Input;
use gpui_component::{Icon, IconName};

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus, TestStatus};

impl ConnectionManagerWindow {
    pub(super) fn render_ssh_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let ssh_enabled = self.ssh_enabled;
        let ssh_auth_method = self.ssh_auth_method;
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let save_ssh_secret = self.form_save_ssh_secret;
        let ssh_tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();
        let selected_tunnel_id = self.selected_ssh_tunnel_id;
        let has_selected_tunnel = selected_tunnel_id.is_some();

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Ssh;
        let focus = self.form_focus;

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

        let theme = cx.theme().clone();
        let muted_fg = theme.muted_foreground;

        // When a saved tunnel is selected, show read-only summary + Edit in Settings button
        let (auth_selector, auth_inputs, ssh_server_section) = if ssh_enabled && has_selected_tunnel
        {
            let selected_tunnel = selected_tunnel_id
                .and_then(|id| ssh_tunnels.iter().find(|t| t.id == id))
                .cloned();

            let readonly_section: Option<AnyElement> = selected_tunnel.map(|tunnel| {
                let auth_label = match &tunnel.config.auth_method {
                    dbflux_core::SshAuthMethod::PrivateKey { key_path } => {
                        let path_str = key_path
                            .as_ref()
                            .map(|p| p.to_string_lossy().to_string())
                            .unwrap_or_else(|| "SSH Agent / default".to_string());
                        format!("Private Key ({})", path_str)
                    }
                    dbflux_core::SshAuthMethod::Password => "Password".to_string(),
                };

                let edit_focused = show_focus && focus == FormFocus::SshEditInSettings;

                self.render_section(
                    "SSH Server (saved tunnel)",
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(self.render_readonly_row("Host", &tunnel.config.host, &theme))
                        .child(self.render_readonly_row(
                            "Port",
                            &tunnel.config.port.to_string(),
                            &theme,
                        ))
                        .child(self.render_readonly_row("Username", &tunnel.config.user, &theme))
                        .child(self.render_readonly_row("Auth", &auth_label, &theme))
                        .child(
                            div()
                                .mt_1()
                                .rounded(px(4.0))
                                .border_2()
                                .when(edit_focused, |d| d.border_color(ring_color))
                                .when(!edit_focused, |d| d.border_color(gpui::transparent_black()))
                                .child(
                                    Button::new("ssh-edit-in-settings")
                                        .label("Edit in Settings")
                                        .small()
                                        .ghost()
                                        .icon(Icon::new(IconName::ExternalLink)),
                                ),
                        ),
                    &theme,
                )
                .into_any_element()
            });

            (None, None, readonly_section)
        } else if ssh_enabled {
            let auth_private_key_focused = show_focus && focus == FormFocus::SshAuthPrivateKey;
            let auth_password_focused = show_focus && focus == FormFocus::SshAuthPassword;

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

            let server_section = self
                .render_section(
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
                .into_any_element();

            (Some(selector), Some(inputs), Some(server_section))
        } else {
            (None, None, None)
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

            let show_save_tunnel = !has_selected_tunnel;
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

        sections.push(ssh_toggle.into_any_element());

        if let Some(selector) = tunnel_selector {
            sections.push(selector);
        }

        if let Some(section) = ssh_server_section {
            sections.push(section);
        }

        if let Some(selector) = auth_selector {
            sections.push(
                self.render_section("Authentication", selector, &theme)
                    .into_any_element(),
            );
        }

        if let Some(inputs) = auth_inputs {
            sections.push(inputs);
        }

        if let Some(section) = ssh_test_section {
            sections.push(section);
        }

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
                    .child(ssh_shared::render_radio_button(
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
                    .child(ssh_shared::render_radio_button(
                        current == SshAuthSelection::Password,
                        primary,
                        border,
                    ))
                    .child(div().text_sm().child("Password")),
            )
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
}
