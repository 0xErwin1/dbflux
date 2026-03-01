use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Heights, Radii};
use crate::ui::windows::ssh_shared::{self, SshAuthSelection};
use dbflux_core::{ServiceConfig, SshTunnelProfile};
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
    ServiceFocus, ServiceFormRow, SettingsFocus, SettingsSection, SettingsWindow, SshFocus,
    SshFormField, SshTestStatus,
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
                "section-general",
                "General",
                AppIcon::Settings,
                SettingsSection::General,
                active,
                focused && self.sidebar_index_for_section(active) == 0,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-keybindings",
                "Keybindings",
                AppIcon::Keyboard,
                SettingsSection::Keybindings,
                active,
                focused && self.sidebar_index_for_section(active) == 1,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-ssh-tunnels",
                "SSH Tunnels",
                AppIcon::FingerprintPattern,
                SettingsSection::SshTunnels,
                active,
                focused && self.sidebar_index_for_section(active) == 2,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-services",
                "Services",
                AppIcon::Plug,
                SettingsSection::Services,
                active,
                focused && self.sidebar_index_for_section(active) == 3,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-drivers",
                "Drivers",
                AppIcon::Database,
                SettingsSection::Drivers,
                active,
                focused && self.sidebar_index_for_section(active) == 4,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-about",
                "About",
                AppIcon::Info,
                SettingsSection::About,
                active,
                focused && self.sidebar_index_for_section(active) == 5,
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

    // --- Services section ---

    fn render_services_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let services = &self.svc_services;
        let editing_idx = self.editing_svc_idx;

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
                            .child("Services"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Manage external driver services. Changes require restart."),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_service_list(services, editing_idx, cx))
                    .child(self.render_service_form(editing_idx, cx)),
            )
    }

    fn render_service_list(
        &self,
        services: &[ServiceConfig],
        editing_idx: Option<usize>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_list_focused =
            self.focus_area == SettingsFocus::Content && self.svc_focus == ServiceFocus::List;
        let is_new_button_focused = is_list_focused && self.svc_selected_idx.is_none();

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
                            Button::new("new-service")
                                .icon(Icon::new(IconName::Plus))
                                .label("New Service")
                                .small()
                                .w_full()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.svc_selected_idx = None;
                                    this.clear_svc_form(window, cx);
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
                    .when(services.is_empty(), |d: Div| {
                        d.child(
                            div()
                                .p_4()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No services configured"),
                        )
                    })
                    .children(services.iter().enumerate().map(|(idx, service)| {
                        let is_selected = editing_idx == Some(idx);
                        let is_focused = is_list_focused && self.svc_selected_idx == Some(idx);
                        let is_disabled = !service.enabled;

                        let subtitle = service
                            .command
                            .as_deref()
                            .filter(|s| !s.is_empty())
                            .unwrap_or("(default)");

                        div()
                            .id(SharedString::from(format!("svc-item-{}", idx)))
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
                                this.svc_selected_idx = Some(idx);
                                this.edit_service(idx, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        svg()
                                            .path(AppIcon::Plug.path())
                                            .size_4()
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
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .font_weight(FontWeight::MEDIUM)
                                                            .when(is_disabled, |d| {
                                                                d.text_color(theme.muted_foreground)
                                                            })
                                                            .child(service.socket_id.clone()),
                                                    )
                                                    .when(is_disabled, |d| {
                                                        d.child(
                                                            div()
                                                                .text_xs()
                                                                .px_1()
                                                                .rounded(px(3.0))
                                                                .bg(theme.secondary)
                                                                .text_color(theme.muted_foreground)
                                                                .child("Disabled"),
                                                        )
                                                    }),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child(subtitle.to_string()),
                                            ),
                                    ),
                            )
                    })),
            )
    }

    fn render_service_form(
        &self,
        editing_idx: Option<usize>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;

        let is_form_focused =
            self.focus_area == SettingsFocus::Content && self.svc_focus == ServiceFocus::Form;
        let cursor = self.svc_form_cursor;
        let rows = self.svc_form_rows();

        let title = if editing_idx.is_some() {
            "Edit Service"
        } else {
            "New Service"
        };

        let is_row_focused = |row: ServiceFormRow| -> bool {
            is_form_focused && rows.get(cursor).copied() == Some(row)
        };

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
                    .overflow_y_hidden()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(self.render_svc_input_field(
                        "Socket ID",
                        &self.input_socket_id,
                        is_row_focused(ServiceFormRow::SocketId),
                        primary,
                        ServiceFormRow::SocketId,
                        cx,
                    ))
                    .child(self.render_svc_input_field(
                        "Command",
                        &self.input_svc_command,
                        is_row_focused(ServiceFormRow::Command),
                        primary,
                        ServiceFormRow::Command,
                        cx,
                    ))
                    .child(self.render_svc_input_field(
                        "Startup Timeout (ms)",
                        &self.input_svc_timeout,
                        is_row_focused(ServiceFormRow::Timeout),
                        primary,
                        ServiceFormRow::Timeout,
                        cx,
                    ))
                    .child(self.render_svc_enabled_checkbox(
                        is_row_focused(ServiceFormRow::Enabled),
                        primary,
                        cx,
                    ))
                    .child(self.render_svc_args_section(
                        is_form_focused,
                        cursor,
                        &rows,
                        primary,
                        cx,
                    ))
                    .child(self.render_svc_env_section(
                        is_form_focused,
                        cursor,
                        &rows,
                        primary,
                        cx,
                    )),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(border)
                    .flex()
                    .gap_2()
                    .justify_end()
                    .when(editing_idx.is_some(), |d| {
                        let is_delete_focused = is_row_focused(ServiceFormRow::DeleteButton);
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
                                    Button::new("delete-service")
                                        .label("Delete")
                                        .small()
                                        .danger()
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            if let Some(idx) = this.editing_svc_idx {
                                                this.request_delete_service(idx, cx);
                                            }
                                        })),
                                ),
                        )
                    })
                    .child(div().flex_1())
                    .child({
                        let is_save_focused = is_row_focused(ServiceFormRow::SaveButton);
                        div()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_save_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .child(
                                Button::new("save-service")
                                    .label(if editing_idx.is_some() {
                                        "Update"
                                    } else {
                                        "Create"
                                    })
                                    .small()
                                    .primary()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_service(window, cx);
                                    })),
                            )
                    }),
            )
    }

    fn render_svc_input_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        row: ServiceFormRow,
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
                            this.svc_focus = ServiceFocus::Form;
                            let rows = this.svc_form_rows();
                            if let Some(pos) = rows.iter().position(|r| *r == row) {
                                this.svc_form_cursor = pos;
                                this.svc_env_col = 0;
                            }
                            this.svc_focus_current_field(window, cx);
                            cx.notify();
                        }),
                    )
                    .child(Input::new(input).small()),
            )
    }

    fn render_svc_enabled_checkbox(
        &self,
        is_focused: bool,
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .px_2()
            .py_1()
            .rounded(px(4.0))
            .border_1()
            .border_color(if is_focused {
                primary
            } else {
                gpui::transparent_black()
            })
            .child(
                Checkbox::new("svc-enabled")
                    .checked(self.svc_enabled)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.svc_enabled = *checked;
                        cx.notify();
                    })),
            )
            .child(div().text_sm().child("Enable this service"))
    }

    fn render_svc_args_section(
        &self,
        is_form_focused: bool,
        cursor: usize,
        rows: &[ServiceFormRow],
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let is_add_focused =
            is_form_focused && rows.get(cursor).copied() == Some(ServiceFormRow::AddArg);

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Arguments"),
            )
            .children(self.svc_arg_inputs.iter().enumerate().map(|(i, input)| {
                let is_row_at_cursor =
                    is_form_focused && rows.get(cursor).copied() == Some(ServiceFormRow::Arg(i));
                let input_focused = is_row_at_cursor && self.svc_env_col == 0;
                let remove_focused = is_row_at_cursor && self.svc_env_col == 1;

                div()
                    .flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .flex_1()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if input_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    this.svc_focus = ServiceFocus::Form;
                                    let rows = this.svc_form_rows();
                                    if let Some(pos) =
                                        rows.iter().position(|r| *r == ServiceFormRow::Arg(i))
                                    {
                                        this.svc_form_cursor = pos;
                                        this.svc_env_col = 0;
                                    }
                                    this.svc_focus_current_field(window, cx);
                                    cx.notify();
                                }),
                            )
                            .child(Input::new(input).small()),
                    )
                    .child(
                        div()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if remove_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .child(
                                Button::new(SharedString::from(format!("rm-arg-{}", i)))
                                    .label("x")
                                    .small()
                                    .ghost()
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.remove_arg_row(i, window, cx);
                                    })),
                            ),
                    )
            }))
            .child(
                div().flex().justify_center().child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_add_focused {
                            primary
                        } else {
                            gpui::transparent_black()
                        })
                        .child(
                            div()
                                .id("add-arg")
                                .w(Heights::ICON_LG)
                                .h(Heights::ICON_LG)
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.primary)
                                .hover(|d| d.opacity(0.8))
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.add_arg_row(window, cx);
                                    }),
                                )
                                .child(
                                    svg()
                                        .path(AppIcon::Plus.path())
                                        .size(Heights::ICON_SM)
                                        .text_color(theme.primary_foreground),
                                ),
                        ),
                ),
            )
    }

    fn render_svc_env_section(
        &self,
        is_form_focused: bool,
        cursor: usize,
        rows: &[ServiceFormRow],
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let is_add_focused =
            is_form_focused && rows.get(cursor).copied() == Some(ServiceFormRow::AddEnv);

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Environment Variables"),
            )
            .children(
                self.svc_env_key_inputs
                    .iter()
                    .zip(self.svc_env_value_inputs.iter())
                    .enumerate()
                    .map(|(i, (key_input, val_input))| {
                        let is_row_at_cursor = is_form_focused
                            && rows.get(cursor).copied() == Some(ServiceFormRow::EnvKey(i));
                        let key_focused = is_row_at_cursor && self.svc_env_col == 0;
                        let val_focused = is_row_at_cursor && self.svc_env_col == 1;
                        let remove_focused = is_row_at_cursor && self.svc_env_col == 2;

                        div()
                            .flex()
                            .gap_2()
                            .items_center()
                            .child(
                                div()
                                    .flex_1()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if key_focused {
                                        primary
                                    } else {
                                        gpui::transparent_black()
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, window, cx| {
                                            this.svc_focus = ServiceFocus::Form;
                                            let rows = this.svc_form_rows();
                                            if let Some(pos) = rows
                                                .iter()
                                                .position(|r| *r == ServiceFormRow::EnvKey(i))
                                            {
                                                this.svc_form_cursor = pos;
                                                this.svc_env_col = 0;
                                            }
                                            this.svc_focus_current_field(window, cx);
                                            cx.notify();
                                        }),
                                    )
                                    .child(Input::new(key_input).small()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(theme.muted_foreground)
                                    .child("="),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if val_focused {
                                        primary
                                    } else {
                                        gpui::transparent_black()
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, window, cx| {
                                            this.svc_focus = ServiceFocus::Form;
                                            let rows = this.svc_form_rows();
                                            if let Some(pos) = rows
                                                .iter()
                                                .position(|r| *r == ServiceFormRow::EnvKey(i))
                                            {
                                                this.svc_form_cursor = pos;
                                                this.svc_env_col = 1;
                                            }
                                            this.svc_focus_current_field(window, cx);
                                            cx.notify();
                                        }),
                                    )
                                    .child(Input::new(val_input).small()),
                            )
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if remove_focused {
                                        primary
                                    } else {
                                        gpui::transparent_black()
                                    })
                                    .child(
                                        Button::new(SharedString::from(format!("rm-env-{}", i)))
                                            .label("x")
                                            .small()
                                            .ghost()
                                            .on_click(cx.listener(move |this, _, window, cx| {
                                                this.remove_env_row(i, window, cx);
                                            })),
                                    ),
                            )
                    }),
            )
            .child(
                div().flex().justify_center().child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_add_focused {
                            primary
                        } else {
                            gpui::transparent_black()
                        })
                        .child(
                            div()
                                .id("add-env")
                                .w(Heights::ICON_LG)
                                .h(Heights::ICON_LG)
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.primary)
                                .hover(|d| d.opacity(0.8))
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.add_env_row(window, cx);
                                    }),
                                )
                                .child(
                                    svg()
                                        .path(AppIcon::Plus.path())
                                        .size(Heights::ICON_SM)
                                        .text_color(theme.primary_foreground),
                                ),
                        ),
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
        let dialog_backdrop_bg = theme.background.opacity(0.6);
        let dialog_card_bg = theme.background;
        let dialog_border = theme.border;
        let dialog_fg = theme.foreground;
        let dialog_muted_fg = theme.muted_foreground;

        let show_ssh_delete = self.pending_delete_tunnel_id.is_some();
        let show_svc_delete = self.pending_delete_svc_idx.is_some();
        let show_close_confirm = self.pending_close_confirm;

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

        let svc_delete_name = self
            .pending_delete_svc_idx
            .and_then(|idx| self.svc_services.get(idx))
            .map(|s| s.socket_id.clone())
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
                SettingsSection::General => self.render_general_section(cx).into_any_element(),
                SettingsSection::Keybindings => {
                    self.render_keybindings_section(cx).into_any_element()
                }
                SettingsSection::SshTunnels => {
                    self.render_ssh_tunnels_section(cx).into_any_element()
                }
                SettingsSection::Services => self.render_services_section(cx).into_any_element(),
                SettingsSection::Drivers => self.render_drivers_section(cx).into_any_element(),
                SettingsSection::About => self.render_about_section(cx).into_any_element(),
            })
            .when(show_ssh_delete, |el| {
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
            .when(show_svc_delete, |el| {
                let this = cx.entity().clone();
                let this_cancel = this.clone();

                el.child(
                    Dialog::new(window, cx)
                        .title("Delete Service")
                        .confirm()
                        .on_ok(move |_, window, cx| {
                            this.update(cx, |settings, cx| {
                                settings.confirm_delete_service(window, cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            this_cancel.update(cx, |settings, cx| {
                                settings.cancel_delete_service(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(format!(
                            "Are you sure you want to delete \"{}\"?",
                            svc_delete_name
                        ))),
                )
            })
            .when(show_close_confirm, |el| {
                let this_cancel = cx.entity().clone();
                let this_discard = cx.entity().clone();
                let this_save = cx.entity().clone();

                el.child(
                    div()
                        .absolute()
                        .inset_0()
                        .flex()
                        .items_center()
                        .justify_center()
                        .bg(dialog_backdrop_bg)
                        .child(
                            div()
                                .w(px(400.0))
                                .bg(dialog_card_bg)
                                .border_1()
                                .border_color(dialog_border)
                                .rounded(px(8.0))
                                .p_6()
                                .flex()
                                .flex_col()
                                .gap_4()
                                .child(
                                    div()
                                        .text_base()
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .text_color(dialog_fg)
                                        .child("Unsaved Changes"),
                                )
                                .child(
                                    div().text_sm().text_color(dialog_muted_fg).child(
                                        "You have unsaved changes. What would you like to do?",
                                    ),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .justify_end()
                                        .gap_2()
                                        .child(
                                            Button::new("close-cancel")
                                                .label("Cancel")
                                                .ghost()
                                                .small()
                                                .on_click(move |_, _, cx| {
                                                    this_cancel.update(cx, |this, cx| {
                                                        this.pending_close_confirm = false;
                                                        cx.notify();
                                                    });
                                                }),
                                        )
                                        .child(
                                            Button::new("close-discard")
                                                .label("Discard")
                                                .danger()
                                                .small()
                                                .on_click(move |_, window, cx| {
                                                    this_discard.update(cx, |this, _cx| {
                                                        this.pending_close_confirm = false;
                                                    });
                                                    window.remove_window();
                                                }),
                                        )
                                        .child(
                                            Button::new("close-save")
                                                .label("Save & Close")
                                                .primary()
                                                .small()
                                                .on_click(move |_, window, cx| {
                                                    this_save.update(cx, |this, cx| {
                                                        this.pending_close_confirm = false;
                                                        this.save_all_and_close(window, cx);
                                                    });
                                                }),
                                        ),
                                ),
                        ),
                )
            })
    }
}
