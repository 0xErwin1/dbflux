use crate::ui::components::form_renderer;
use crate::ui::icons::AppIcon;
use dbflux_components::controls::Input;
use dbflux_components::primitives::{Label, Text};
use dbflux_core::FormFieldKind;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::checkbox::Checkbox;

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus};

impl ConnectionManagerWindow {
    pub(super) fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active_tab = self.active_tab;
        let show_access_tab = !self.uses_file_form();

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
            .when(show_access_tab, |d| {
                d.child(
                    div()
                        .id("tab-access")
                        .px_4()
                        .py_2()
                        .cursor_pointer()
                        .border_b_2()
                        .when(active_tab == ActiveTab::Access, |dd| {
                            dd.border_color(theme.primary).text_color(theme.foreground)
                        })
                        .when(active_tab != ActiveTab::Access, |dd| {
                            dd.border_color(gpui::transparent_black())
                                .text_color(theme.muted_foreground)
                        })
                        .hover(|dd| dd.bg(theme.secondary))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.active_tab = ActiveTab::Access;
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
                                        .text_color(if active_tab == ActiveTab::Access {
                                            theme.foreground
                                        } else {
                                            theme.muted_foreground
                                        }),
                                )
                                .child(div().text_sm().child("Access")),
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
            .child(
                div()
                    .id("tab-mcp")
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(active_tab == ActiveTab::Mcp, |d| {
                        d.border_color(theme.primary).text_color(theme.foreground)
                    })
                    .when(active_tab != ActiveTab::Mcp, |d| {
                        d.border_color(gpui::transparent_black())
                            .text_color(theme.muted_foreground)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = ActiveTab::Mcp;
                        cx.notify();
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(svg().path(AppIcon::Lock.path()).size_4().text_color(
                                if active_tab == ActiveTab::Mcp {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(div().text_sm().child("MCP")),
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

        let ring_color = cx.theme().ring;

        let form_def = driver.form_definition();
        let Some(main_tab) = form_def.main_tab().cloned() else {
            return Vec::new();
        };

        let mut sections = Vec::new();

        // Driver-specific form fields
        sections.extend(self.render_form_tab(&main_tab, false, show_focus, ring_color, cx));

        if requires_password {
            let password_field = self.render_password_field(
                show_focus,
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
                    .child(Text::caption(format!(
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
                    .child(Text::caption(format!(
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
                    .child(Text::caption(format!(
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

        let hooks_rows = self.render_hooks_rows(muted, cx);

        sections.push(
            self.render_section("Connection Hooks", hooks_rows, &theme)
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
                Text::muted("This driver has no custom settings.")
                    .into_any_element(),
            );
        }

        sections
    }

    pub(super) fn render_mcp_tab(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let enabled = self.conn_mcp_enabled;
        let opacity = if enabled { 1.0 } else { 0.5 };

        let actor_label = self
            .conn_mcp_actor_dropdown
            .read(cx)
            .selected_label()
            .map(|l| l.to_string())
            .unwrap_or_default();
        let role_label = self
            .conn_mcp_role_dropdown
            .read(cx)
            .selected_value()
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string());
        let policy_label = self
            .conn_mcp_policy_dropdown
            .read(cx)
            .selected_value()
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string());

        let preview_text = if !enabled {
            "MCP disabled for this connection".to_string()
        } else if actor_label.is_empty() {
            "MCP enabled — select a trusted client to bind".to_string()
        } else {
            format!(
                "Actor '{}' | role: {} | policy: {}",
                actor_label, role_label, policy_label
            )
        };

        let content = div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(Checkbox::new("conn-mcp-enabled").checked(enabled).on_click(
                        cx.listener(|this, checked: &bool, _, cx| {
                            this.conn_mcp_enabled = *checked;
                            cx.notify();
                        }),
                    ))
                    .child(div().text_sm().child("Enable MCP for this connection")),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .opacity(opacity)
                    .child(Label::new("Trusted Client (Actor)"))
                    .child(Text::caption("AI agent identity — configure in Settings → MCP"))
                    .child(self.conn_mcp_actor_dropdown.clone()),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .opacity(opacity)
                    .child(Label::new("Role"))
                    .child(Text::caption("Configure roles in Settings → MCP → Roles"))
                    .child(self.conn_mcp_role_dropdown.clone())
                    .child(Text::caption("Additional roles (optional)"))
                    .child(self.conn_mcp_role_multi_select.clone()),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .opacity(opacity)
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Policy"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Configure policies in Settings → MCP → Policies"),
                    )
                    .child(self.conn_mcp_policy_dropdown.clone())
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Additional policies (optional)"),
                    )
                    .child(self.conn_mcp_policy_multi_select.clone()),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Scope/policy assignment preview")
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(preview_text),
                    ),
            );

        vec![
            self.render_section("MCP Governance", content, &theme)
                .into_any_element(),
        ]
    }
}
