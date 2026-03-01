use crate::keymap::ContextId;
use crate::ui::dropdown::DropdownItem;
use crate::ui::icons::AppIcon;
use crate::ui::windows::ssh_shared::{self, SshAuthSelection};
use dbflux_core::{FormFieldDef, FormFieldKind, FormTab};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::list::ListItem;
use gpui_component::{Icon, IconName};

use crate::ui::components::form_renderer;

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus, TestStatus, View};

impl ConnectionManagerWindow {
    pub(super) fn render_driver_select(
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
                            let driver_id = driver_info.id.clone();
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
                                            this.select_driver(&driver_id, window, cx);
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

    pub(super) fn render_tab_bar(
        &self,
        supports_ssh: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
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
            .when(supports_ssh, |d| {
                d.child(
                    div()
                        .id("tab-ssh")
                        .px_4()
                        .py_2()
                        .cursor_pointer()
                        .border_b_2()
                        .when(active_tab == ActiveTab::Ssh, |dd| {
                            dd.border_color(theme.primary).text_color(theme.foreground)
                        })
                        .when(active_tab != ActiveTab::Ssh, |dd| {
                            dd.border_color(gpui::transparent_black())
                                .text_color(theme.muted_foreground)
                        })
                        .hover(|dd| dd.bg(theme.secondary))
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
                                .when(self.ssh_enabled, |dd| {
                                    dd.child(
                                        div()
                                            .w(px(6.0))
                                            .h(px(6.0))
                                            .rounded_full()
                                            .bg(gpui::rgb(0x22C55E)),
                                    )
                                }),
                        ),
                )
            })
            .child(
                div()
                    .id("tab-settings")
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(active_tab == ActiveTab::Settings, |d| {
                        d.border_color(theme.primary).text_color(theme.foreground)
                    })
                    .when(active_tab != ActiveTab::Settings, |d| {
                        d.border_color(gpui::transparent_black())
                            .text_color(theme.muted_foreground)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = ActiveTab::Settings;
                        cx.notify();
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(svg().path(AppIcon::Settings.path()).size_4().text_color(
                                if active_tab == ActiveTab::Settings {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(div().text_sm().child("Settings")),
                    ),
            )
    }

    pub(super) fn render_main_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
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
        let Some(main_tab) = form_def.main_tab().cloned() else {
            return Vec::new();
        };

        let mut sections = self.render_form_tab(&main_tab, false, show_focus, ring_color, cx);

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

    pub(super) fn render_settings_tab(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let effective = self.resolve_driver_effective_settings(cx);

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Settings;
        let focus = self.form_focus;

        let ring_color = theme.ring;
        let muted = theme.muted_foreground;

        let mut sections: Vec<AnyElement> = Vec::new();

        // --- Global Overrides Section ---
        let policy_label = match effective.refresh_policy {
            dbflux_core::RefreshPolicySetting::Manual => "Manual",
            dbflux_core::RefreshPolicySetting::Interval => "Interval",
        };

        let override_rows = div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(200.0)))
                    .child(
                        div()
                            .w(px(160.0))
                            .text_xs()
                            .text_color(muted)
                            .child("Override Value"),
                    ),
            )
            // Refresh policy row
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(px(4.0))
                    .border_2()
                    .when(
                        show_focus && focus == FormFocus::SettingsRefreshPolicy,
                        |d| d.border_color(ring_color),
                    )
                    .when(
                        !(show_focus && focus == FormFocus::SettingsRefreshPolicy),
                        |d| d.border_color(gpui::transparent_black()),
                    )
                    .p(px(2.0))
                    .child(
                        Checkbox::new("conn-override-refresh-policy")
                            .checked(self.conn_override_refresh_policy)
                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                this.conn_override_refresh_policy = *checked;
                                cx.notify();
                            })),
                    )
                    .child(div().w(px(180.0)).text_sm().child("Refresh policy"))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .relative()
                            .opacity(if self.conn_override_refresh_policy {
                                1.0
                            } else {
                                0.6
                            })
                            .child(self.conn_refresh_policy_dropdown.clone())
                            .when(!self.conn_override_refresh_policy, |d| {
                                d.child(
                                    div()
                                        .absolute()
                                        .top_0()
                                        .left_0()
                                        .size_full()
                                        .cursor_default(),
                                )
                            }),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(muted)
                            .child(format!("Default: {}", policy_label)),
                    ),
            )
            // Refresh interval row
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(px(4.0))
                    .border_2()
                    .when(
                        show_focus && focus == FormFocus::SettingsRefreshInterval,
                        |d| d.border_color(ring_color),
                    )
                    .when(
                        !(show_focus && focus == FormFocus::SettingsRefreshInterval),
                        |d| d.border_color(gpui::transparent_black()),
                    )
                    .p(px(2.0))
                    .child(
                        Checkbox::new("conn-override-refresh-interval")
                            .checked(self.conn_override_refresh_interval)
                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                this.conn_override_refresh_interval = *checked;
                                cx.notify();
                            })),
                    )
                    .child(div().w(px(180.0)).text_sm().child("Refresh interval (s)"))
                    .child(
                        div()
                            .w(px(100.0))
                            .opacity(if self.conn_override_refresh_interval {
                                1.0
                            } else {
                                0.6
                            })
                            .child(
                                Input::new(&self.conn_refresh_interval_input)
                                    .small()
                                    .disabled(!self.conn_override_refresh_interval),
                            ),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(muted)
                            .child(format!("Default: {}s", effective.refresh_interval_secs)),
                    ),
            )
            // Confirm dangerous queries
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(px(4.0))
                    .border_2()
                    .when(
                        show_focus && focus == FormFocus::SettingsConfirmDangerous,
                        |d| d.border_color(ring_color),
                    )
                    .when(
                        !(show_focus && focus == FormFocus::SettingsConfirmDangerous),
                        |d| d.border_color(gpui::transparent_black()),
                    )
                    .p(px(2.0))
                    .child(
                        div()
                            .w(px(200.0))
                            .text_sm()
                            .child("Confirm dangerous queries"),
                    )
                    .child(
                        div()
                            .min_w(px(160.0))
                            .child(self.conn_confirm_dangerous_dropdown.clone()),
                    )
                    .child(div().text_xs().text_color(muted).child(format!(
                        "Default: {}",
                        if effective.confirm_dangerous {
                            "On"
                        } else {
                            "Off"
                        }
                    ))),
            )
            // Requires WHERE clause
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(px(4.0))
                    .border_2()
                    .when(
                        show_focus && focus == FormFocus::SettingsRequiresWhere,
                        |d| d.border_color(ring_color),
                    )
                    .when(
                        !(show_focus && focus == FormFocus::SettingsRequiresWhere),
                        |d| d.border_color(gpui::transparent_black()),
                    )
                    .p(px(2.0))
                    .child(div().w(px(200.0)).text_sm().child("Requires WHERE clause"))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .child(self.conn_requires_where_dropdown.clone()),
                    )
                    .child(div().text_xs().text_color(muted).child(format!(
                        "Default: {}",
                        if effective.requires_where {
                            "On"
                        } else {
                            "Off"
                        }
                    ))),
            )
            // Requires preview
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(px(4.0))
                    .border_2()
                    .when(
                        show_focus && focus == FormFocus::SettingsRequiresPreview,
                        |d| d.border_color(ring_color),
                    )
                    .when(
                        !(show_focus && focus == FormFocus::SettingsRequiresPreview),
                        |d| d.border_color(gpui::transparent_black()),
                    )
                    .p(px(2.0))
                    .child(div().w(px(200.0)).text_sm().child("Requires preview"))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .child(self.conn_requires_preview_dropdown.clone()),
                    )
                    .child(div().text_xs().text_color(muted).child(format!(
                        "Default: {}",
                        if effective.requires_preview {
                            "On"
                        } else {
                            "Off"
                        }
                    ))),
            );

        sections.push(
            self.render_section("Connection Overrides", override_rows, &theme)
                .into_any_element(),
        );

        // --- Driver Schema Section ---
        if let Some(driver) = &self.selected_driver
            && let Some(schema) = driver.settings_schema()
        {
            let mut field_idx: u8 = 0;

            let schema_fields = div().flex().flex_col().gap_2().children(
                schema
                    .tabs
                    .iter()
                    .flat_map(|tab| tab.sections.iter())
                    .flat_map(|section| section.fields.iter())
                    .filter_map(|field| {
                        let current_idx = field_idx;
                        field_idx += 1;
                        let field_focused =
                            show_focus && focus == FormFocus::SettingsDriverField(current_idx);
                        let enabled = form_renderer::is_field_enabled(
                            field,
                            &self.conn_form_state.checkboxes,
                        );

                        match &field.kind {
                            FormFieldKind::Checkbox => {
                                let checked = self
                                    .conn_form_state
                                    .checkboxes
                                    .get(&field.id)
                                    .copied()
                                    .unwrap_or(false);
                                let field_id = field.id.clone();
                                let default_val = effective
                                    .driver_values
                                    .get(&field.id)
                                    .map(|v| if v == "true" { "On" } else { "Off" })
                                    .unwrap_or("Off");

                                Some(
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap_3()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(field_focused, |d| d.border_color(ring_color))
                                        .when(!field_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .opacity(if enabled { 1.0 } else { 0.6 })
                                        .child(
                                            Checkbox::new(SharedString::from(format!(
                                                "conn-schema-{}",
                                                field.id
                                            )))
                                            .checked(checked)
                                            .label(field.label.as_str())
                                            .on_click(cx.listener(
                                                move |this, checked: &bool, _, cx| {
                                                    if !enabled {
                                                        return;
                                                    }
                                                    this.conn_form_state
                                                        .checkboxes
                                                        .insert(field_id.clone(), *checked);
                                                    cx.notify();
                                                },
                                            )),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(muted)
                                                .child(format!("Default: {}", default_val)),
                                        )
                                        .into_any_element(),
                                )
                            }

                            FormFieldKind::Select { .. } => {
                                let dropdown =
                                    self.conn_form_state.dropdowns.get(&field.id)?.clone();
                                let default_val = effective
                                    .driver_values
                                    .get(&field.id)
                                    .cloned()
                                    .unwrap_or_else(|| field.default_value.clone());

                                Some(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(field_focused, |d| d.border_color(ring_color))
                                        .when(!field_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .opacity(if enabled { 1.0 } else { 0.6 })
                                        .child(
                                            div()
                                                .flex()
                                                .items_center()
                                                .gap_2()
                                                .child(div().text_sm().child(field.label.clone()))
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(muted)
                                                        .child(format!("Default: {}", default_val)),
                                                ),
                                        )
                                        .child(div().w(px(240.0)).child(dropdown))
                                        .into_any_element(),
                                )
                            }

                            _ => {
                                let input = self.conn_form_state.inputs.get(&field.id)?.clone();
                                let default_val = effective
                                    .driver_values
                                    .get(&field.id)
                                    .cloned()
                                    .unwrap_or_else(|| field.default_value.clone());

                                Some(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(field_focused, |d| d.border_color(ring_color))
                                        .when(!field_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .child(
                                            div()
                                                .flex()
                                                .items_center()
                                                .gap_2()
                                                .child(div().text_sm().child(field.label.clone()))
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(muted)
                                                        .child(format!("Default: {}", default_val)),
                                                ),
                                        )
                                        .child(Input::new(&input).small().disabled(!enabled))
                                        .into_any_element(),
                                )
                            }
                        }
                    }),
            );

            sections.push(
                self.render_section("Driver Settings", schema_fields, &theme)
                    .into_any_element(),
            );
        }

        if sections.len() == 1 {
            sections.push(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("This driver has no custom settings.")
                    .into_any_element(),
            );
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

    pub(super) fn render_ssh_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let ssh_enabled = self.ssh_enabled;
        let ssh_auth_method = self.ssh_auth_method;
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let save_ssh_secret = self.form_save_ssh_secret;
        let ssh_tunnels = self.app_state.read(cx).ssh_tunnels().to_vec();
        let selected_tunnel_id = self.selected_ssh_tunnel_id;

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

    pub(super) fn render_form(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
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

        let show_focus = self.edit_state == EditState::Navigating;
        let focus = self.form_focus;
        let test_focused = show_focus && focus == FormFocus::TestConnection;
        let save_focused = show_focus && focus == FormFocus::Save;

        let tab_bar = self.render_tab_bar(supports_ssh, cx).into_any_element();

        let tab_content: Vec<AnyElement> = match self.active_tab {
            ActiveTab::Main => self.render_main_tab(cx),
            ActiveTab::Ssh if supports_ssh => self.render_ssh_tab(cx),
            ActiveTab::Ssh => self.render_main_tab(cx),
            ActiveTab::Settings => self.render_settings_tab(cx),
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
            .child(tab_bar)
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

    fn render_form_field(
        &self,
        field_def: &FormFieldDef,
        is_ssh_tab: bool,
        show_focus: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let field_focus = Self::field_id_to_focus(&field_def.id, is_ssh_tab);
        let focused = show_focus && field_focus == Some(self.form_focus);

        match &field_def.kind {
            FormFieldKind::Text
            | FormFieldKind::Password
            | FormFieldKind::Number
            | FormFieldKind::FilePath => {
                let Some(input_state) = self.input_state_for_field(&field_def.id) else {
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
                                    .child(field_def.label.clone()),
                            )
                            .when(field_def.required && field_enabled, |d| {
                                d.child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*"))
                            }),
                    )
                    .child(Input::new(input_state).disabled(!field_enabled))
                    .into_any_element()
            }

            FormFieldKind::Checkbox => {
                let field_id = field_def.id.clone();
                let is_checked = if field_id == "ssh_enabled" {
                    self.ssh_enabled
                } else {
                    self.checkbox_states
                        .get(&field_id)
                        .copied()
                        .unwrap_or(false)
                };

                let checkbox_id = gpui::SharedString::from(field_id.clone());
                div()
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .child(
                        Checkbox::new(checkbox_id)
                            .checked(is_checked)
                            .label(field_def.label.as_str())
                            .on_click(cx.listener(move |this, checked: &bool, window, cx| {
                                if field_id == "ssh_enabled" {
                                    this.ssh_enabled = *checked;
                                } else {
                                    this.checkbox_states.insert(field_id.clone(), *checked);
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
                                .child(field_def.label.clone()),
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
                                        .child(div().text_sm().child(opt.label.clone()))
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

        for section in &tab.sections {
            let fields: Vec<&FormFieldDef> = section
                .fields
                .iter()
                .filter(|field| field.id != "password" || is_ssh_tab)
                .collect();

            if fields.is_empty() {
                continue;
            }

            let mut field_elements: Vec<AnyElement> = Vec::new();
            let mut i = 0;
            while i < fields.len() {
                let field = fields[i];

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
    pub(super) fn form_field_input(
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
                .ssh_tunnels()
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

        div()
            .id("connection-manager")
            .key_context(ContextId::ConnectionManager.as_gpui_context())
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
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
