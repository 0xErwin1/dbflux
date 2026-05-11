use crate::ui::components::form_renderer;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::Radii;
use dbflux_components::controls::Input;
use dbflux_components::primitives::{
    FilePicker, Icon as AppIconElement, Label, SegmentedControl, SegmentedItem, Text,
};
use dbflux_core::FormFieldKind;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::checkbox::Checkbox;

use dbflux_components::typography::SubSectionLabel;

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus};

impl ConnectionManagerWindow {
    pub(super) fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let border_color = cx.theme().border;
        let active_tab = self.active_tab;
        let show_access_tab = !self.uses_file_form();

        div()
            .flex()
            .items_center()
            .border_b_1()
            .border_color(border_color)
            .child(self.render_tab_trigger(
                "tab-main",
                "Main",
                AppIcon::Plug,
                ActiveTab::Main,
                active_tab == ActiveTab::Main,
                cx,
            ))
            .when(show_access_tab, |d| {
                d.child(self.render_tab_trigger(
                    "tab-access",
                    "Access",
                    AppIcon::FingerprintPattern,
                    ActiveTab::Access,
                    active_tab == ActiveTab::Access,
                    cx,
                ))
            })
            .child(self.render_tab_trigger(
                "tab-settings",
                "Settings",
                AppIcon::Settings,
                ActiveTab::Settings,
                active_tab == ActiveTab::Settings,
                cx,
            ))
            .child(self.render_tab_trigger(
                "tab-mcp",
                "MCP",
                AppIcon::Lock,
                ActiveTab::Mcp,
                active_tab == ActiveTab::Mcp,
                cx,
            ))
    }

    fn render_tab_trigger(
        &self,
        id: &'static str,
        label: &'static str,
        icon: AppIcon,
        tab: ActiveTab,
        is_active: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let color = if is_active {
            theme.foreground
        } else {
            theme.muted_foreground
        };

        div()
            .id(id)
            .px_4()
            .py_2()
            .cursor_pointer()
            .border_b_2()
            .border_color(if is_active {
                theme.primary
            } else {
                gpui::transparent_black()
            })
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.active_tab = tab;
                cx.notify();
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .child(AppIconElement::new(icon).small().color(color))
                    .child(Text::caption(label).color(color)),
            )
    }

    pub(super) fn render_main_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let Some(driver) = &self.selected_driver else {
            return Vec::new();
        };

        let keyring_available = self.app_state.read(cx).secret_store_available();
        let requires_password = driver.requires_password();
        let save_password = self.form_save_password;
        let ssl_modes = driver.metadata().ssl_modes;

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Main;

        let ring_color = cx.theme().ring;

        let form_def = driver.form_definition();
        let Some(main_tab) = form_def.main_tab().cloned() else {
            return Vec::new();
        };

        // Extract the help text from the driver's password field definition, if any.
        let password_help = main_tab
            .sections
            .iter()
            .flat_map(|s| s.fields.iter())
            .find(|f| f.id == "password")
            .and_then(|f| f.help.clone());

        let mut sections = Vec::new();

        // Driver-specific form fields
        sections.extend(self.render_form_tab(&main_tab, false, show_focus, ring_color, cx));

        if requires_password {
            let password_field = self.render_password_field(
                show_focus,
                keyring_available,
                save_password,
                ring_color,
                password_help,
                cx,
            );

            sections.push(password_field);
        }

        // TRANSPORT section — SSL mode + SSH tunnel (only when the driver supports SSL).
        if let Some(modes) = ssl_modes {
            let transport_section = self.render_transport_section(modes, cx);
            sections.push(transport_section);
        }

        sections
    }

    fn render_transport_section(
        &mut self,
        ssl_modes: &'static [dbflux_core::SslModeOption],
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let current_ssl_mode = self.selected_ssl_mode.clone();

        let ssl_items: Vec<SegmentedItem> = ssl_modes
            .iter()
            .map(|m| SegmentedItem::new(m.id, m.label))
            .collect();

        let entity = cx.entity().clone();

        let ssl_control = SegmentedControl::new(
            ssl_items,
            current_ssl_mode.clone(),
            move |selected: &SharedString, _window, cx| {
                let mode = selected.to_string();
                entity.update(cx, |this, cx| {
                    this.selected_ssl_mode = mode;
                    cx.notify();
                });
            },
        );

        // Wrap the segmented control in a content-width row with a trailing flex filler so
        // its segments hug their labels instead of stretching to fill the field column.
        let ssl_control_row = div()
            .flex()
            .items_center()
            .child(ssl_control)
            .child(div().flex_1());

        let ssl_row = Self::field_row_cm("SSL mode", false, ssl_control_row, None::<&str>, cx);

        let mut section = div()
            .flex()
            .flex_col()
            .gap_2()
            .child(SubSectionLabel::new("TRANSPORT"))
            .child(ssl_row);

        // Cert path inputs — shown only when the driver declares ssl_cert_fields and the
        // selected mode requires certificate verification.
        if let Some(driver) = &self.selected_driver {
            let metadata = driver.metadata();
            if let Some(cert_fields) = &metadata.ssl_cert_fields {
                let mode_requires_root =
                    dbflux_core::ssl_mode_id_requires_root_cert(&current_ssl_mode);

                if mode_requires_root {
                    let ca_row = self.render_ssl_cert_picker_row(
                        "CA certificate",
                        super::SslCertSlot::CaCert,
                        cx,
                    );
                    section = section.child(ca_row);
                }

                if cert_fields.client_cert {
                    let mode_is_cert_active =
                        dbflux_core::ssl_mode_id_is_cert_active(&current_ssl_mode);

                    if mode_is_cert_active {
                        let cert_row = self.render_ssl_cert_picker_row(
                            "Client cert",
                            super::SslCertSlot::ClientCert,
                            cx,
                        );
                        let key_row = self.render_ssl_cert_picker_row(
                            "Client key",
                            super::SslCertSlot::ClientKey,
                            cx,
                        );
                        section = section.child(cert_row).child(key_row);
                    }
                }
            }
        }

        section.into_any_element()
    }

    /// Render an SSL cert-path row as a file-picker button (folder icon + filename or
    /// "Browse…" placeholder, with a trailing clear button when a value is set).
    /// The whole control is keyboard-focusable: Enter/Space opens the picker,
    /// Backspace clears the selection.
    fn render_ssl_cert_picker_row(
        &self,
        label: &'static str,
        slot: super::SslCertSlot,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let input_entity = match slot {
            super::SslCertSlot::CaCert => &self.ssl_ca_cert_input,
            super::SslCertSlot::ClientCert => &self.ssl_client_cert_input,
            super::SslCertSlot::ClientKey => &self.ssl_client_key_input,
        };

        let current_value = input_entity.read(cx).value().to_string();
        let has_value = !current_value.trim().is_empty();
        let current_value_for_browse = has_value.then(|| current_value.clone());

        let button_id: SharedString = match slot {
            super::SslCertSlot::CaCert => "ssl-cert-picker-ca".into(),
            super::SslCertSlot::ClientCert => "ssl-cert-picker-client-cert".into(),
            super::SslCertSlot::ClientKey => "ssl-cert-picker-client-key".into(),
        };

        let entity = cx.entity().clone();
        let browse_entity = entity.clone();
        let clear_entity = entity;

        let picker = FilePicker::new(
            button_id,
            current_value.clone(),
            AppIcon::Folder,
            AppIcon::X,
        )
        .on_browse(move |_event, window, cx| {
            let starting_value = current_value_for_browse.clone();
            browse_entity.update(cx, |this, cx| {
                this.browse_ssl_cert(slot, starting_value.clone(), window, cx);
            });
        })
        .on_clear(move |_event, window, cx| {
            clear_entity.update(cx, |this, cx| {
                this.clear_ssl_cert(slot, window, cx);
            });
        });

        let control = div()
            .flex()
            .items_center()
            .gap_2()
            .child(picker)
            .child(div().flex_1());

        Self::field_row_cm(label, false, control, None::<&str>, cx).into_any_element()
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
                    .child(div().w(px(160.0)).child(Text::caption("Override Value"))),
            )
            // Refresh policy row
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(Radii::SM)
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
                    .child(Text::caption(format!("Default: {}", policy_label))),
            )
            // Refresh interval row
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(Radii::SM)
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
                    .child(Text::caption(format!(
                        "Default: {}s",
                        effective.refresh_interval_secs
                    ))),
            )
            // Confirm dangerous queries
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .rounded(Radii::SM)
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
                    .rounded(Radii::SM)
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
                    .rounded(Radii::SM)
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
                                        .rounded(Radii::SM)
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
                                        .child(Text::caption(format!("Default: {}", default_val)))
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
                                        .rounded(Radii::SM)
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
                                                .child(Text::caption(format!(
                                                    "Default: {}",
                                                    default_val
                                                ))),
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
                                        .rounded(Radii::SM)
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
                                                .child(Text::caption(format!(
                                                    "Default: {}",
                                                    default_val
                                                ))),
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
            sections.push(Text::muted("This driver has no custom settings.").into_any_element());
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
                    .child(Text::caption(
                        "AI agent identity — configure in Settings → MCP",
                    ))
                    .child(self.conn_mcp_actor_dropdown.clone()),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .opacity(opacity)
                    .child(Label::new("Role"))
                    .child(Text::caption(
                        "Configure roles in Settings \u{2192} MCP \u{2192} Roles",
                    ))
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
                    .child(Text::label("Policy"))
                    .child(Text::caption(
                        "Configure policies in Settings \u{2192} MCP \u{2192} Policies",
                    ))
                    .child(self.conn_mcp_policy_dropdown.clone())
                    .child(Text::caption("Additional policies (optional)"))
                    .child(self.conn_mcp_policy_multi_select.clone()),
            )
            .child(Text::caption("Scope/policy assignment preview").into_any_element())
            .child(Text::body(preview_text));

        vec![
            self.render_section("MCP Governance", content, &theme)
                .into_any_element(),
        ]
    }
}

// The `file_picker_label` helper and its tests moved to
// `dbflux_components::primitives::file_picker` together with the `FilePicker`
// primitive itself.
