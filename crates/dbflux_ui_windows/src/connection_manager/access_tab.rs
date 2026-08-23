use crate::ssh_shared::{self, SshAuthSelection};
use dbflux_components::controls::DropdownItem;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Label, Status, StatusIndicator, Text};
use dbflux_components::tokens::{Radii, Widths};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Icon;
use gpui_component::checkbox::Checkbox;

use super::{AccessTabMode, ActiveTab, ConnectionManagerWindow, EditState, FormFocus, TestStatus};

impl ConnectionManagerWindow {
    pub(super) fn render_access_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let ring_color = theme.ring;
        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Access;

        self.access
            .access_method_dropdown
            .update(cx, |dropdown, cx| {
                let focus_color = if show_focus && self.form_focus == FormFocus::AccessMethod {
                    Some(ring_color)
                } else {
                    None
                };

                dropdown.set_focus_ring(focus_color, cx);
            });

        self.auth_profile
            .auth_profile_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_focus_ring(None, cx);
            });

        let login_enabled = !self.auth_profile.auth_profile_login_in_progress
            && self.selected_auth_profile_needs_login(cx);
        let auth_profile_is_valid = self.selected_auth_profile_is_valid(cx);

        let access_tab_label = dbflux_i18n::t!("access.tab_label");

        let mut sections = vec![
            self.render_section(
                &access_tab_label,
                div().flex().flex_col().gap_2().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Label::new(dbflux_i18n::t!("access.method_label")))
                        .child(
                            div()
                                .min_w(Widths::CM_FORM_DROPDOWN)
                                .child(self.access.access_method_dropdown.clone()),
                        ),
                ),
                &theme,
            )
            .into_any_element(),
        ];

        match self.access.access_tab_mode {
            AccessTabMode::Direct => {
                sections.push(
                    div()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .child(Text::muted(dbflux_i18n::t!("access.direct_hint")))
                        .child(
                            div()
                                .flex()
                                .flex_col()
                                .gap_1()
                                .child(Label::new(dbflux_i18n::t!("access.auth_profile_optional")))
                                .child(
                                    self.render_focus_shell(
                                        show_focus && self.form_focus == FormFocus::SsmAuthProfile,
                                        ring_color,
                                        self.auth_profile.auth_profile_dropdown.clone(),
                                        cx,
                                    )
                                    .min_w(px(280.0))
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, window, cx| {
                                            this.enter_edit_mode_for_field(
                                                FormFocus::SsmAuthProfile,
                                                window,
                                                cx,
                                            );
                                        }),
                                    ),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            self.render_focus_shell(
                                                show_focus
                                                    && self.form_focus == FormFocus::SsmAuthManage,
                                                ring_color,
                                                Button::new(
                                                    "auth-open-settings",
                                                    dbflux_i18n::t!("access.manage"),
                                                )
                                                .ghost()
                                                .small()
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.open_auth_profiles_settings(cx);
                                                })),
                                                cx,
                                            )
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(|this, _, window, cx| {
                                                    this.enter_edit_mode_for_field(
                                                        FormFocus::SsmAuthManage,
                                                        window,
                                                        cx,
                                                    );
                                                }),
                                            ),
                                        )
                                        .child(
                                            self.render_focus_shell(
                                                show_focus
                                                    && self.form_focus == FormFocus::SsmAuthLogin,
                                                ring_color,
                                                Button::new(
                                                    "auth-login-selected",
                                                    dbflux_i18n::t!("access.login"),
                                                )
                                                .ghost()
                                                .small()
                                                .disabled(!login_enabled)
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.login_selected_auth_profile(cx);
                                                })),
                                                cx,
                                            )
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(|this, _, window, cx| {
                                                    this.enter_edit_mode_for_field(
                                                        FormFocus::SsmAuthLogin,
                                                        window,
                                                        cx,
                                                    );
                                                }),
                                            ),
                                        )
                                        .child(
                                            self.render_focus_shell(
                                                show_focus
                                                    && self.form_focus == FormFocus::SsmAuthRefresh,
                                                ring_color,
                                                Button::new(
                                                    "auth-refresh-session",
                                                    dbflux_i18n::t!("access.refresh"),
                                                )
                                                .ghost()
                                                .small()
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.refresh_auth_profile_statuses(cx);
                                                })),
                                                cx,
                                            )
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(|this, _, window, cx| {
                                                    this.enter_edit_mode_for_field(
                                                        FormFocus::SsmAuthRefresh,
                                                        window,
                                                        cx,
                                                    );
                                                }),
                                            ),
                                        ),
                                )
                                .child(Text::caption(dbflux_i18n::t!("access.auth_profile_hint"))),
                        )
                        .when_some(self.selected_auth_profile_status_text(cx), |d, status| {
                            d.child(Text::caption(status))
                        })
                        .when(auth_profile_is_valid, |d| {
                            d.child(
                                StatusIndicator::new(Status::Connected)
                                    .label(dbflux_i18n::t!("access.session_valid")),
                            )
                        })
                        .when_some(
                            self.auth_profile.auth_profile_action_message.as_ref(),
                            |d, message| d.child(Text::caption(message.clone())),
                        )
                        .into_any_element(),
                );
            }
            AccessTabMode::Ssh => sections.extend(self.render_ssh_tab(cx)),
            AccessTabMode::Proxy => sections.extend(self.render_proxy_tab(cx)),
            AccessTabMode::ManagedSsm => sections.push(self.render_ssm_access_section(cx)),
        }

        sections
    }

    fn render_ssm_access_section(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let ring_color = theme.ring;
        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Access;
        let login_enabled = !self.auth_profile.auth_profile_login_in_progress
            && self.selected_auth_profile_needs_login(cx);
        let auth_profile_is_valid = self.selected_auth_profile_is_valid(cx);

        let ssm_title = dbflux_i18n::t!("access.ssm_title");
        let ssm_instance_id_label = dbflux_i18n::t!("access.ssm_instance_id");
        let ssm_region_label = dbflux_i18n::t!("access.ssm_region");
        let ssm_remote_port_label = dbflux_i18n::t!("access.ssm_remote_port");

        self.render_section(
            &ssm_title,
            div()
                .flex()
                .flex_col()
                .gap_3()
                .child(self.render_ssm_value_field(
                    &ssm_instance_id_label,
                    &self.access.input_ssm_instance_id,
                    self.access.ssm_instance_id_value_source_selector.clone(),
                    true,
                    show_focus && self.form_focus == FormFocus::SsmInstanceIdValueSource,
                    show_focus && self.form_focus == FormFocus::SsmInstanceId,
                    ring_color,
                    FormFocus::SsmInstanceIdValueSource,
                    FormFocus::SsmInstanceId,
                    cx,
                ))
                .child(self.render_ssm_value_field(
                    &ssm_region_label,
                    &self.access.input_ssm_region,
                    self.access.ssm_region_value_source_selector.clone(),
                    true,
                    show_focus && self.form_focus == FormFocus::SsmRegionValueSource,
                    show_focus && self.form_focus == FormFocus::SsmRegion,
                    ring_color,
                    FormFocus::SsmRegionValueSource,
                    FormFocus::SsmRegion,
                    cx,
                ))
                .child(self.render_ssm_value_field(
                    &ssm_remote_port_label,
                    &self.access.input_ssm_remote_port,
                    self.access.ssm_remote_port_value_source_selector.clone(),
                    false,
                    show_focus && self.form_focus == FormFocus::SsmRemotePortValueSource,
                    show_focus && self.form_focus == FormFocus::SsmRemotePort,
                    ring_color,
                    FormFocus::SsmRemotePortValueSource,
                    FormFocus::SsmRemotePort,
                    cx,
                ))
                .child(Text::caption(dbflux_i18n::t!("access.ssm_port_hint")))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Label::new(dbflux_i18n::t!("access.auth_profile")))
                        .child(
                            self.render_focus_shell(
                                show_focus && self.form_focus == FormFocus::SsmAuthProfile,
                                ring_color,
                                self.auth_profile.auth_profile_dropdown.clone(),
                                cx,
                            )
                            .min_w(px(280.0))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.enter_edit_mode_for_field(
                                        FormFocus::SsmAuthProfile,
                                        window,
                                        cx,
                                    );
                                }),
                            ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    self.render_focus_shell(
                                        show_focus && self.form_focus == FormFocus::SsmAuthManage,
                                        ring_color,
                                        Button::new(
                                            "ssm-auth-open-settings",
                                            dbflux_i18n::t!("access.manage"),
                                        )
                                        .ghost()
                                        .small()
                                        .on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.open_auth_profiles_settings(cx);
                                            }),
                                        ),
                                        cx,
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, window, cx| {
                                            this.enter_edit_mode_for_field(
                                                FormFocus::SsmAuthManage,
                                                window,
                                                cx,
                                            );
                                        }),
                                    ),
                                )
                                .child(
                                    self.render_focus_shell(
                                        show_focus && self.form_focus == FormFocus::SsmAuthLogin,
                                        ring_color,
                                        Button::new(
                                            "ssm-auth-login-selected",
                                            dbflux_i18n::t!("access.login"),
                                        )
                                        .ghost()
                                        .small()
                                        .disabled(!login_enabled)
                                        .on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.login_selected_auth_profile(cx);
                                            }),
                                        ),
                                        cx,
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, window, cx| {
                                            this.enter_edit_mode_for_field(
                                                FormFocus::SsmAuthLogin,
                                                window,
                                                cx,
                                            );
                                        }),
                                    ),
                                )
                                .child(
                                    self.render_focus_shell(
                                        show_focus && self.form_focus == FormFocus::SsmAuthRefresh,
                                        ring_color,
                                        Button::new(
                                            "ssm-auth-refresh-session",
                                            dbflux_i18n::t!("access.refresh"),
                                        )
                                        .ghost()
                                        .small()
                                        .on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.refresh_auth_profile_statuses(cx);
                                            }),
                                        ),
                                        cx,
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, window, cx| {
                                            this.enter_edit_mode_for_field(
                                                FormFocus::SsmAuthRefresh,
                                                window,
                                                cx,
                                            );
                                        }),
                                    ),
                                ),
                        )
                        .when_some(self.selected_auth_profile_status_text(cx), |d, status| {
                            d.child(Text::caption(status))
                        })
                        .when(auth_profile_is_valid, |d| {
                            d.child(
                                StatusIndicator::new(Status::Connected)
                                    .label(dbflux_i18n::t!("access.session_valid")),
                            )
                        }),
                ),
            &theme,
        )
        .into_any_element()
    }

    #[allow(clippy::too_many_arguments)]
    fn render_ssm_value_field(
        &self,
        label: &str,
        input: &Entity<dbflux_components::controls::InputState>,
        selector: Entity<dbflux_components::components::value_source_selector::ValueSourceSelector>,
        required: bool,
        selector_focused: bool,
        focused: bool,
        ring_color: gpui::Hsla,
        selector_focus: FormFocus,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()).required(required))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        self.render_focus_shell(selector_focused, ring_color, selector, cx)
                            .w(px(170.0))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    this.enter_edit_mode_for_field(selector_focus, window, cx);
                                }),
                            ),
                    )
                    .child(
                        self.render_control_focus_shell(focused, ring_color, Input::new(input), cx)
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    this.enter_edit_mode_for_field(field, window, cx);
                                }),
                            ),
                    ),
            )
            .into_any_element()
    }

    pub(super) fn render_proxy_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let proxies = self.app_state.read(cx).proxies().to_vec();
        let selected_proxy_id = self.access.selected_proxy_id;

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Access;
        let focus = self.form_focus;

        let ring_color = cx.theme().ring;
        let theme = cx.theme().clone();
        let _muted_fg = theme.muted_foreground;

        let mut sections: Vec<AnyElement> = Vec::new();

        if proxies.is_empty() {
            sections.push(
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .items_center()
                            .gap_2()
                            .child(Text::muted(dbflux_i18n::t!("access.proxy_no_profiles")))
                            .child(Text::caption(dbflux_i18n::t!(
                                "access.proxy_no_profiles_hint"
                            ))),
                    )
                    .into_any_element(),
            );
            return sections;
        }

        let proxy_items: Vec<DropdownItem> = proxies
            .iter()
            .map(|p| {
                let label = if p.enabled {
                    p.name.clone()
                } else {
                    crate::labels::access_proxy_disabled_label(&p.name)
                };

                DropdownItem::with_value(&label, p.id.to_string())
            })
            .collect();
        self.access.proxy_uuids = proxies.iter().map(|p| p.id).collect();

        let selected_proxy_index =
            selected_proxy_id.and_then(|id| proxies.iter().position(|p| p.id == id));

        let proxy_selector_focused = show_focus && focus == FormFocus::ProxySelector;
        let proxy_clear_focused = show_focus && focus == FormFocus::ProxyClear;
        self.access.proxy_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(proxy_items, cx);
            dropdown.set_selected_index(selected_proxy_index, cx);

            let focus_color = if proxy_selector_focused {
                Some(ring_color)
            } else {
                None
            };
            dropdown.set_focus_ring(focus_color, cx);
        });

        let has_selection = selected_proxy_id.is_some();

        let selector_row = div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Label::new(dbflux_i18n::t!("access.select_proxy")))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(div().flex_1().child(self.access.proxy_dropdown.clone()))
                    .when(has_selection, |d| {
                        d.child(
                            div()
                                .rounded(Radii::SM)
                                .border_2()
                                .when(proxy_clear_focused, |dd| dd.border_color(ring_color))
                                .when(!proxy_clear_focused, |dd| {
                                    dd.border_color(gpui::transparent_black())
                                })
                                .child(
                                    Button::new("clear-proxy", dbflux_i18n::t!("access.clear"))
                                        .small()
                                        .ghost()
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.clear_proxy_selection(cx);
                                        })),
                                ),
                        )
                    }),
            );

        sections.push(selector_row.into_any_element());

        if let Some(proxy) = selected_proxy_id
            .and_then(|id| proxies.iter().find(|p| p.id == id))
            .cloned()
        {
            let kind_label = format!("{:?}", proxy.kind);
            let host_port = format!("{}:{}", proxy.host, proxy.port);
            let auth_label = format!("{:?}", proxy.auth);
            let enabled_label = if proxy.enabled {
                dbflux_i18n::t!("access.value_yes")
            } else {
                dbflux_i18n::t!("access.value_no")
            };
            let no_proxy_label = proxy
                .no_proxy
                .clone()
                .unwrap_or_else(|| dbflux_i18n::t!("access.no_proxy_placeholder"));

            let edit_focused = show_focus && focus == FormFocus::ProxyEditInSettings;

            let proxy_details_title = dbflux_i18n::t!("access.proxy_details");
            let proxy_type_label = dbflux_i18n::t!("access.proxy_type");
            let proxy_host_label = dbflux_i18n::t!("access.proxy_host");
            let proxy_auth_label = dbflux_i18n::t!("access.proxy_auth");
            let proxy_enabled_label = dbflux_i18n::t!("access.proxy_enabled");
            let proxy_no_proxy_label = dbflux_i18n::t!("access.proxy_no_proxy");

            let details = self.render_section(
                &proxy_details_title,
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(self.render_readonly_row(&proxy_type_label, &kind_label, &theme))
                    .child(self.render_readonly_row(&proxy_host_label, &host_port, &theme))
                    .child(self.render_readonly_row(&proxy_auth_label, &auth_label, &theme))
                    .child(self.render_readonly_row(&proxy_enabled_label, &enabled_label, &theme))
                    .child(self.render_readonly_row(&proxy_no_proxy_label, &no_proxy_label, &theme))
                    .child(
                        div()
                            .mt_1()
                            .rounded(Radii::SM)
                            .border_2()
                            .when(edit_focused, |d| d.border_color(ring_color))
                            .when(!edit_focused, |d| d.border_color(gpui::transparent_black()))
                            .child(
                                Button::new(
                                    "proxy-edit-in-settings",
                                    dbflux_i18n::t!("access.edit_in_settings"),
                                )
                                .small()
                                .ghost()
                                .icon(Icon::new(AppIcon::ExternalLink)),
                            ),
                    ),
                &theme,
            );

            sections.push(details.into_any_element());
        }

        sections
    }

    pub(super) fn render_ssh_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let ssh_enabled = self.access.ssh_enabled;
        let ssh_auth_method = self.access.ssh_auth_method;
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let save_ssh_secret = self.form.form_save_ssh_secret;
        let ssh_tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();
        let selected_tunnel_id = self.access.selected_ssh_tunnel_id;
        let has_selected_tunnel = selected_tunnel_id.is_some();

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Access;
        let focus = self.form_focus;

        let ring_color = cx.theme().ring;

        let ssh_enabled_focused = show_focus && focus == FormFocus::SshEnabled;
        let ssh_toggle = div()
            .flex()
            .items_center()
            .gap_2()
            .rounded(Radii::SM)
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
                        this.access.ssh_enabled = *checked;
                        cx.notify();
                    })),
            )
            .child(Label::new(dbflux_i18n::t!("access.use_ssh_tunnel")));

        let tunnel_items: Vec<DropdownItem> = ssh_tunnels
            .iter()
            .map(|t| DropdownItem::with_value(&t.name, t.id.to_string()))
            .collect();
        self.access.ssh_tunnel_uuids = ssh_tunnels.iter().map(|t| t.id).collect();

        let selected_tunnel_index =
            selected_tunnel_id.and_then(|id| ssh_tunnels.iter().position(|t| t.id == id));

        let tunnel_selector_focused = show_focus && focus == FormFocus::SshTunnelSelector;
        let tunnel_clear_focused = show_focus && focus == FormFocus::SshTunnelClear;
        self.access.ssh_tunnel_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(tunnel_items, cx);
            dropdown.set_selected_index(selected_tunnel_index, cx);
            let focus_color = if tunnel_selector_focused {
                Some(ring_color)
            } else {
                None
            };
            dropdown.set_focus_ring(focus_color, cx);
        });

        let tunnel_selector: Option<AnyElement> =
            if ssh_enabled && !ssh_tunnels.is_empty() {
                let selected_tunnel_name = selected_tunnel_id
                    .and_then(|id| ssh_tunnels.iter().find(|t| t.id == id))
                    .map(|t| t.name.clone());

                Some(
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(Label::new(dbflux_i18n::t!("access.ssh_tunnel_label")))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .child(self.access.ssh_tunnel_dropdown.clone()),
                                )
                                .when(selected_tunnel_name.is_some(), |d| {
                                    d.child(
                                        div()
                                            .rounded(Radii::SM)
                                            .border_2()
                                            .when(tunnel_clear_focused, |dd| {
                                                dd.border_color(ring_color)
                                            })
                                            .when(!tunnel_clear_focused, |dd| {
                                                dd.border_color(gpui::transparent_black())
                                            })
                                            .child(
                                                Button::new(
                                                    "clear-ssh-tunnel",
                                                    dbflux_i18n::t!("access.clear"),
                                                )
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
        let _muted_fg = theme.muted_foreground;

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
                            .unwrap_or_else(|| dbflux_i18n::t!("ssh.agent_default"));
                        crate::labels::ssh_private_key_with_path(&path_str)
                    }
                    dbflux_core::SshAuthMethod::Password => dbflux_i18n::t!("ssh.password"),
                };

                let edit_focused = show_focus && focus == FormFocus::SshEditInSettings;

                let ssh_server_saved_title = dbflux_i18n::t!("access.ssh_server_saved");
                let ssh_host_label = dbflux_i18n::t!("ssh.host");
                let ssh_port_label = dbflux_i18n::t!("ssh.port");
                let ssh_username_label = dbflux_i18n::t!("ssh.username");
                let ssh_auth_label = dbflux_i18n::t!("ssh.auth_label");

                self.render_section(
                    &ssh_server_saved_title,
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(self.render_readonly_row(
                            &ssh_host_label,
                            &tunnel.config.host,
                            &theme,
                        ))
                        .child(self.render_readonly_row(
                            &ssh_port_label,
                            &tunnel.config.port.to_string(),
                            &theme,
                        ))
                        .child(self.render_readonly_row(
                            &ssh_username_label,
                            &tunnel.config.user,
                            &theme,
                        ))
                        .child(self.render_readonly_row(&ssh_auth_label, &auth_label, &theme))
                        .child(
                            div()
                                .mt_1()
                                .rounded(Radii::SM)
                                .border_2()
                                .when(edit_focused, |d| d.border_color(ring_color))
                                .when(!edit_focused, |d| d.border_color(gpui::transparent_black()))
                                .child(
                                    Button::new(
                                        "ssh-edit-in-settings",
                                        dbflux_i18n::t!("access.edit_in_settings"),
                                    )
                                    .small()
                                    .ghost()
                                    .icon(Icon::new(AppIcon::ExternalLink)),
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

            let ssh_server_title = dbflux_i18n::t!("ssh.ssh_server");
            let ssh_host_label = dbflux_i18n::t!("ssh.host");
            let ssh_port_label = dbflux_i18n::t!("ssh.port");
            let ssh_username_label = dbflux_i18n::t!("ssh.username");

            let server_section = self
                .render_section(
                    &ssh_server_title,
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
                                    &ssh_host_label,
                                    &self.access.input_ssh_host,
                                    true,
                                    show_focus && focus == FormFocus::SshHost,
                                    ring_color,
                                    FormFocus::SshHost,
                                    cx,
                                )))
                                .child(div().w(px(80.0)).child(self.form_field_input(
                                    &ssh_port_label,
                                    &self.access.input_ssh_port,
                                    false,
                                    show_focus && focus == FormFocus::SshPort,
                                    ring_color,
                                    FormFocus::SshPort,
                                    cx,
                                ))),
                        )
                        .child(div().id(3usize).child(self.form_field_input(
                            &ssh_username_label,
                            &self.access.input_ssh_user,
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
                .rounded(Radii::SM)
                .border_2()
                .when(test_ssh_focused, |d| d.border_color(ring_color))
                .when(!test_ssh_focused, |d| {
                    d.border_color(gpui::transparent_black())
                })
                .child(
                    Button::new("test-ssh", dbflux_i18n::t!("access.test_ssh"))
                        .icon(Icon::new(AppIcon::ExternalLink))
                        .small()
                        .ghost()
                        .disabled(ssh_test_status == TestStatus::Testing)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.test_ssh_connection(window, cx);
                        })),
                );

            let status_el: Option<AnyElement> = match ssh_test_status {
                TestStatus::None => None,
                TestStatus::Testing => {
                    Some(Text::muted(dbflux_i18n::t!("access.testing_ssh")).into_any_element())
                }
                TestStatus::Success | TestStatus::SuccessWithWarning => Some(
                    StatusIndicator::new(Status::Connected)
                        .label(dbflux_i18n::t!("access.ssh_success"))
                        .into_any_element(),
                ),
                TestStatus::Failed => Some(
                    StatusIndicator::new(Status::Error)
                        .label(
                            ssh_test_error.unwrap_or_else(|| dbflux_i18n::t!("access.ssh_failed")),
                        )
                        .into_any_element(),
                ),
            };

            let show_save_tunnel = !has_selected_tunnel;
            let save_tunnel_button: Option<AnyElement> = if show_save_tunnel {
                let save_tunnel_focused = show_focus && focus == FormFocus::SaveAsTunnel;
                Some(
                    div()
                        .rounded(Radii::SM)
                        .border_2()
                        .when(save_tunnel_focused, |d| d.border_color(ring_color))
                        .when(!save_tunnel_focused, |d| {
                            d.border_color(gpui::transparent_black())
                        })
                        .child(
                            Button::new(
                                "save-ssh-tunnel",
                                dbflux_i18n::t!("access.save_as_tunnel"),
                            )
                            .icon(Icon::new(AppIcon::Plus))
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
            let authentication_title = dbflux_i18n::t!("ssh.authentication");
            sections.push(
                self.render_section(&authentication_title, selector, &theme)
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
                    .child(Text::muted(dbflux_i18n::t!("access.ssh_disabled_hint")))
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
            this.access.ssh_auth_method = SshAuthSelection::PrivateKey;
            cx.notify();
        });
        let click_pw = cx.listener(|this, _, _, cx| {
            this.access.ssh_auth_method = SshAuthSelection::Password;
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
                    .rounded(Radii::SM)
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
                    .child(div().text_sm().child(dbflux_i18n::t!("ssh.private_key"))),
            )
            .child(
                div()
                    .id("auth-password")
                    .flex()
                    .items_center()
                    .gap_2()
                    .cursor_pointer()
                    .rounded(Radii::SM)
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
                    .child(div().text_sm().child(dbflux_i18n::t!("ssh.password"))),
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
                        this.form.form_save_ssh_secret = *checked;
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
                        this.form.form_save_ssh_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        let theme = cx.theme();
        let _muted_fg = theme.muted_foreground;

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
                        .child(Label::new(dbflux_i18n::t!("ssh.private_key_path")))
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(Radii::SM)
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
                                        .child(Input::new(&self.access.input_ssh_key_path).small()),
                                )
                                .child(
                                    div()
                                        .rounded(Radii::SM)
                                        .border_2()
                                        .when(key_browse_focused, |d| d.border_color(ring_color))
                                        .when(!key_browse_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .child(
                                            Button::new(
                                                "browse-ssh-key",
                                                dbflux_i18n::t!("ssh.browse"),
                                            )
                                            .small()
                                            .ghost()
                                            .on_click(
                                                cx.listener(|this, _, window, cx| {
                                                    this.browse_ssh_key(window, cx);
                                                }),
                                            ),
                                        ),
                                ),
                        ),
                )
                .child(Text::caption(dbflux_i18n::t!("ssh.private_key_hint")))
                .child(
                    div()
                        .id(6usize)
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Label::new(dbflux_i18n::t!("ssh.key_passphrase")))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(Radii::SM)
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
                                        .child(Input::new(&self.access.input_ssh_key_passphrase)),
                                )
                                .child(
                                    Self::render_password_toggle(
                                        self.form.show_ssh_passphrase,
                                        "toggle-ssh-passphrase",
                                        theme,
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.form.show_ssh_passphrase =
                                                !this.form.show_ssh_passphrase;
                                            cx.notify();
                                        },
                                    )),
                                )
                                .when_some(passphrase_checkbox, |d, checkbox| {
                                    d.child(
                                        div()
                                            .rounded(Radii::SM)
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
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .child(dbflux_i18n::t!("ssh.save")),
                                                    ),
                                            ),
                                    )
                                }),
                        )
                        .child(Text::caption(dbflux_i18n::t!("ssh.passphrase_hint"))),
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
                        .child(Label::new(dbflux_i18n::t!("ssh.ssh_password")).required(true))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(Radii::SM)
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
                                        .child(Input::new(&self.access.input_ssh_password)),
                                )
                                .child(
                                    Self::render_password_toggle(
                                        self.form.show_ssh_password,
                                        "toggle-ssh-password",
                                        theme,
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.form.show_ssh_password =
                                                !this.form.show_ssh_password;
                                            cx.notify();
                                        },
                                    )),
                                )
                                .when_some(password_checkbox, |d, checkbox| {
                                    d.child(
                                        div()
                                            .rounded(Radii::SM)
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
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .child(dbflux_i18n::t!("ssh.save")),
                                                    ),
                                            ),
                                    )
                                }),
                        ),
                )
                .into_any_element(),
        }
    }
}

#[cfg(test)]
mod tests {
    const ACCESS_DIRECT_SSM_KEYS: &[&str] = &[
        "access.tab_label",
        "access.method_label",
        "access.direct_hint",
        "access.auth_profile_optional",
        "access.manage",
        "access.login",
        "access.refresh",
        "access.auth_profile_hint",
        "access.session_valid",
        "access.ssm_title",
        "access.ssm_instance_id",
        "access.ssm_region",
        "access.ssm_remote_port",
        "access.ssm_port_hint",
        "access.auth_profile",
    ];

    #[test]
    fn access_direct_ssm_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in ACCESS_DIRECT_SSM_KEYS {
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
    fn access_method_label_differs_between_locales() {
        let en = dbflux_i18n::t!("access.method_label", locale = "en");
        let es = dbflux_i18n::t!("access.method_label", locale = "es");

        assert_ne!(
            en, es,
            "access.method_label should differ between en and es"
        );
    }

    #[test]
    fn access_ssm_instance_id_label_exact_values() {
        let en = dbflux_i18n::t!("access.ssm_instance_id", locale = "en");
        let es = dbflux_i18n::t!("access.ssm_instance_id", locale = "es");

        assert_eq!(en, "Instance ID");
        assert_eq!(es, "ID de instancia");
    }

    const ACCESS_PROXY_KEYS: &[&str] = &[
        "access.proxy_no_profiles",
        "access.proxy_no_profiles_hint",
        "access.proxy_disabled_label",
        "access.select_proxy",
        "access.clear",
        "access.proxy_details",
        "access.proxy_type",
        "access.proxy_host",
        "access.proxy_auth",
        "access.proxy_enabled",
        "access.proxy_no_proxy",
        "access.value_yes",
        "access.value_no",
        "access.no_proxy_placeholder",
        "access.edit_in_settings",
    ];

    #[test]
    fn access_proxy_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in ACCESS_PROXY_KEYS {
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
    fn access_proxy_details_differs_between_locales() {
        let en = dbflux_i18n::t!("access.proxy_details", locale = "en");
        let es = dbflux_i18n::t!("access.proxy_details", locale = "es");

        assert_ne!(
            en, es,
            "access.proxy_details should differ between en and es"
        );
    }

    #[test]
    fn access_select_proxy_label_exact_values() {
        let en = dbflux_i18n::t!("access.select_proxy", locale = "en");
        let es = dbflux_i18n::t!("access.select_proxy", locale = "es");

        assert_eq!(en, "Select Proxy");
        assert_eq!(es, "Seleccionar proxy");
    }

    const ACCESS_SSH_KEYS: &[&str] = &[
        "access.use_ssh_tunnel",
        "access.ssh_tunnel_label",
        "access.ssh_server_saved",
        "access.test_ssh",
        "access.testing_ssh",
        "access.ssh_success",
        "access.ssh_failed",
        "access.save_as_tunnel",
        "access.ssh_disabled_hint",
    ];

    #[test]
    fn access_ssh_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in ACCESS_SSH_KEYS {
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
    fn access_use_ssh_tunnel_label_exact_values() {
        let en = dbflux_i18n::t!("access.use_ssh_tunnel", locale = "en");
        let es = dbflux_i18n::t!("access.use_ssh_tunnel", locale = "es");

        assert_eq!(en, "Use SSH Tunnel");
        assert_eq!(es, "Usar túnel SSH");
    }

    const SSH_VOCABULARY_KEYS: &[&str] = &[
        "ssh.agent_default",
        "ssh.authentication",
        "ssh.private_key",
        "ssh.password",
        "ssh.private_key_path",
        "ssh.browse",
        "ssh.key_passphrase",
        "ssh.save",
        "ssh.passphrase_hint",
        "ssh.ssh_password",
        "ssh.ssh_server",
        "ssh.host",
        "ssh.port",
        "ssh.username",
        "ssh.host_user_required",
        "ssh.private_key_short",
        "ssh.update",
        "ssh.create",
        "ssh.test",
        "ssh.private_key_with_path",
        "ssh.private_key_hint",
        "ssh.auth_label",
    ];

    #[test]
    fn ssh_vocabulary_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in SSH_VOCABULARY_KEYS {
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
    fn ssh_authentication_differs_between_locales() {
        let en = dbflux_i18n::t!("ssh.authentication", locale = "en");
        let es = dbflux_i18n::t!("ssh.authentication", locale = "es");

        assert_ne!(en, es, "ssh.authentication should differ between en and es");
    }

    #[test]
    fn ssh_passphrase_hint_exact_values() {
        let en = dbflux_i18n::t!("ssh.passphrase_hint", locale = "en");
        let es = dbflux_i18n::t!("ssh.passphrase_hint", locale = "es");

        assert_eq!(en, "Leave empty if key has no passphrase");
        assert_eq!(es, "Déjala vacía si la clave no tiene frase");
    }
}
