use dbflux_components::components::form_renderer;
use dbflux_components::controls::Input;
use dbflux_components::icons::AppIcon;
#[cfg(feature = "mcp")]
use dbflux_components::primitives::Label;
use dbflux_components::primitives::{
    FilePicker, Icon as AppIconElement, SegmentedControl, SegmentedItem, Text,
};
#[cfg(feature = "mcp")]
use dbflux_components::tokens::Spacing;
use dbflux_components::tokens::{Radii, Widths};
use dbflux_core::FormFieldKind;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::checkbox::Checkbox;
#[cfg(feature = "mcp")]
use gpui_component::scroll::ScrollableElement;

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
                dbflux_i18n::t!("connection_manager.tab.main"),
                AppIcon::Plug,
                ActiveTab::Main,
                active_tab == ActiveTab::Main,
                cx,
            ))
            .when(show_access_tab, |d| {
                d.child(self.render_tab_trigger(
                    "tab-access",
                    dbflux_i18n::t!("access.tab_label"),
                    AppIcon::FingerprintPattern,
                    ActiveTab::Access,
                    active_tab == ActiveTab::Access,
                    cx,
                ))
            })
            .child(self.render_tab_trigger(
                "tab-settings",
                dbflux_i18n::t!("connection_manager.tab.settings"),
                AppIcon::Settings,
                ActiveTab::Settings,
                active_tab == ActiveTab::Settings,
                cx,
            ))
            .child(self.render_tab_trigger(
                "tab-mcp",
                dbflux_i18n::t!("connection_manager.tab.mcp"),
                AppIcon::Lock,
                ActiveTab::Mcp,
                active_tab == ActiveTab::Mcp,
                cx,
            ))
    }

    fn render_tab_trigger(
        &self,
        id: &'static str,
        label: impl Into<SharedString>,
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
        // Clone the driver Arc up front so we don't hold a reference into `self`
        // across mutable calls like `render_form_tab` below.
        let Some(driver) = self.form.selected_driver.clone() else {
            return Vec::new();
        };

        let keyring_available = self.app_state.read(cx).secret_store_available();
        let requires_password = driver.requires_password();
        let save_password = self.form.form_save_password;
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
            // Drivers can rename the canonical secret field (e.g. "API Token" for
            // InfluxDB v2). The override depends on current form values so that
            // toggles like a version selector can flip the label live.
            let form_values = self.collect_form_values(driver.form_definition(), cx);
            let secret_label = driver
                .secret_field_label(&form_values)
                .unwrap_or_else(|| dbflux_i18n::t!("connection_manager.placeholder.password"));

            let password_field = self.render_password_field(
                show_focus,
                keyring_available,
                save_password,
                ring_color,
                password_help,
                &secret_label,
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
        let current_ssl_mode = self.form.selected_ssl_mode.clone();

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
                    this.form.selected_ssl_mode = mode;
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

        let ssl_row = Self::field_row_cm(
            dbflux_i18n::t!("connection_manager.field.ssl_mode"),
            false,
            ssl_control_row,
            None::<&str>,
            cx,
        );

        let mut section = div()
            .flex()
            .flex_col()
            .gap_2()
            .child(SubSectionLabel::new(dbflux_i18n::t!(
                "connection_manager.section.transport"
            )))
            .child(ssl_row);

        // Cert path inputs — shown only when the driver declares ssl_cert_fields and the
        // selected mode requires certificate verification.
        if let Some(driver) = &self.form.selected_driver {
            let metadata = driver.metadata();
            if let Some(cert_fields) = &metadata.ssl_cert_fields {
                let mode_requires_root =
                    dbflux_core::ssl_mode_id_requires_root_cert(&current_ssl_mode);

                if mode_requires_root {
                    let ca_row = self.render_ssl_cert_picker_row(
                        dbflux_i18n::t!("connection_manager.field.ca_certificate"),
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
                            dbflux_i18n::t!("connection_manager.field.client_cert"),
                            super::SslCertSlot::ClientCert,
                            cx,
                        );
                        let key_row = self.render_ssl_cert_picker_row(
                            dbflux_i18n::t!("connection_manager.field.client_key"),
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
        label: impl Into<SharedString>,
        slot: super::SslCertSlot,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let input_entity = match slot {
            super::SslCertSlot::CaCert => &self.form.ssl_ca_cert_input,
            super::SslCertSlot::ClientCert => &self.form.ssl_client_cert_input,
            super::SslCertSlot::ClientKey => &self.form.ssl_client_key_input,
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
            dbflux_core::RefreshPolicySetting::Manual => {
                dbflux_i18n::t!("settings.general.refresh_policy.option.manual")
            }
            dbflux_core::RefreshPolicySetting::Interval => {
                dbflux_i18n::t!("settings.general.refresh_policy.option.interval")
            }
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
                    .child(div().w(px(160.0)).child(Text::caption(dbflux_i18n::t!(
                        "settings.general.override_value_header"
                    )))),
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
                            .checked(self.settings_tab.conn_override_refresh_policy)
                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                this.settings_tab.conn_override_refresh_policy = *checked;
                                cx.notify();
                            })),
                    )
                    .child(div().w(px(180.0)).text_sm().child(dbflux_i18n::t!(
                        "connection_manager.overrides.refresh_policy"
                    )))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .relative()
                            .opacity(if self.settings_tab.conn_override_refresh_policy {
                                1.0
                            } else {
                                0.6
                            })
                            .child(self.settings_tab.conn_refresh_policy_dropdown.clone())
                            .when(!self.settings_tab.conn_override_refresh_policy, |d| {
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
                    .child(Text::caption(crate::labels::override_default_caption(
                        &policy_label,
                    ))),
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
                            .checked(self.settings_tab.conn_override_refresh_interval)
                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                this.settings_tab.conn_override_refresh_interval = *checked;
                                cx.notify();
                            })),
                    )
                    .child(div().w(px(180.0)).text_sm().child(dbflux_i18n::t!(
                        "connection_manager.overrides.refresh_interval"
                    )))
                    .child(
                        div()
                            .w(px(100.0))
                            .opacity(if self.settings_tab.conn_override_refresh_interval {
                                1.0
                            } else {
                                0.6
                            })
                            .child(
                                Input::new(&self.settings_tab.conn_refresh_interval_input)
                                    .small()
                                    .disabled(!self.settings_tab.conn_override_refresh_interval),
                            ),
                    )
                    .child(Text::caption(
                        crate::labels::override_default_seconds_caption(
                            effective.refresh_interval_secs,
                        ),
                    )),
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
                    .child(div().w(px(200.0)).text_sm().child(dbflux_i18n::t!(
                        "connection_manager.overrides.confirm_dangerous"
                    )))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .child(self.settings_tab.conn_confirm_dangerous_dropdown.clone()),
                    )
                    .child(Text::caption(crate::labels::override_default_caption(
                        &if effective.confirm_dangerous {
                            dbflux_i18n::t!("connection_manager.overrides.on")
                        } else {
                            dbflux_i18n::t!("connection_manager.overrides.off")
                        },
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
                    .child(div().w(px(200.0)).text_sm().child(dbflux_i18n::t!(
                        "connection_manager.overrides.requires_where"
                    )))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .child(self.settings_tab.conn_requires_where_dropdown.clone()),
                    )
                    .child(Text::caption(crate::labels::override_default_caption(
                        &if effective.requires_where {
                            dbflux_i18n::t!("connection_manager.overrides.on")
                        } else {
                            dbflux_i18n::t!("connection_manager.overrides.off")
                        },
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
                    .child(div().w(px(200.0)).text_sm().child(dbflux_i18n::t!(
                        "connection_manager.overrides.requires_preview"
                    )))
                    .child(
                        div()
                            .min_w(px(160.0))
                            .child(self.settings_tab.conn_requires_preview_dropdown.clone()),
                    )
                    .child(Text::caption(crate::labels::override_default_caption(
                        &if effective.requires_preview {
                            dbflux_i18n::t!("connection_manager.overrides.on")
                        } else {
                            dbflux_i18n::t!("connection_manager.overrides.off")
                        },
                    ))),
            );

        sections.push(
            self.render_section(
                dbflux_i18n::t!("connection_manager.connection_overrides_title").as_str(),
                override_rows,
                &theme,
            )
            .into_any_element(),
        );

        let hooks_rows = self.render_hooks_rows(muted, cx);

        sections.push(
            self.render_section(
                dbflux_i18n::t!("connection_manager.connection_hooks_title").as_str(),
                hooks_rows,
                &theme,
            )
            .into_any_element(),
        );

        // --- Driver Schema Section ---
        if let Some(driver) = &self.form.selected_driver
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
                            &self.settings_tab.conn_form_state.checkboxes,
                            &form_renderer::select_values(&self.settings_tab.conn_form_state, cx),
                        );

                        match &field.kind {
                            FormFieldKind::Checkbox => {
                                let checked = self
                                    .settings_tab
                                    .conn_form_state
                                    .checkboxes
                                    .get(&field.id)
                                    .copied()
                                    .unwrap_or(false);
                                let field_id = field.id.clone();
                                let default_val = effective
                                    .driver_values
                                    .get(&field.id)
                                    .map(|v| {
                                        if v == "true" {
                                            dbflux_i18n::t!("connection_manager.overrides.on")
                                        } else {
                                            dbflux_i18n::t!("connection_manager.overrides.off")
                                        }
                                    })
                                    .unwrap_or_else(|| {
                                        dbflux_i18n::t!("connection_manager.overrides.off")
                                    });

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
                                                    this.settings_tab
                                                        .conn_form_state
                                                        .checkboxes
                                                        .insert(field_id.clone(), *checked);
                                                    cx.notify();
                                                },
                                            )),
                                        )
                                        .child(Text::caption(
                                            crate::labels::override_default_caption(&default_val),
                                        ))
                                        .into_any_element(),
                                )
                            }

                            FormFieldKind::Select { .. } => {
                                let dropdown = self
                                    .settings_tab
                                    .conn_form_state
                                    .dropdowns
                                    .get(&field.id)?
                                    .clone();
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
                                                .child(Text::caption(
                                                    crate::labels::override_default_caption(
                                                        &default_val,
                                                    ),
                                                )),
                                        )
                                        .child(div().w(Widths::CM_FORM_DROPDOWN).child(dropdown))
                                        .into_any_element(),
                                )
                            }

                            _ => {
                                let input = self
                                    .settings_tab
                                    .conn_form_state
                                    .inputs
                                    .get(&field.id)?
                                    .clone();
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
                                                .child(Text::caption(
                                                    crate::labels::override_default_caption(
                                                        &default_val,
                                                    ),
                                                )),
                                        )
                                        .child(Input::new(&input).small().disabled(!enabled))
                                        .into_any_element(),
                                )
                            }
                        }
                    }),
            );

            sections.push(
                self.render_section(
                    &dbflux_i18n::t!("connection_manager.driver_settings_title"),
                    schema_fields,
                    &theme,
                )
                .into_any_element(),
            );
        }

        if sections.len() == 1 {
            sections.push(
                Text::muted(dbflux_i18n::t!(
                    "connection_manager.driver_no_custom_settings"
                ))
                .into_any_element(),
            );
        }

        sections
    }

    fn render_mcp_enabled_checkbox(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .child(
                Checkbox::new("conn-mcp-enabled")
                    .checked(self.mcp_tab.conn_mcp_enabled)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.mcp_tab.conn_mcp_enabled = *checked;
                        cx.notify();
                    })),
            )
            .child(
                div()
                    .text_sm()
                    .child(dbflux_i18n::t!("connection_manager.enable_mcp")),
            )
    }

    #[cfg(feature = "mcp")]
    pub(super) fn render_mcp_tab(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let enabled = self.mcp_tab.conn_mcp_enabled;
        let opacity = if enabled { 1.0 } else { 0.5 };

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

        let filter_query = self
            .mcp_tab
            .conn_mcp_client_filter_input
            .read(cx)
            .value()
            .to_string();
        let filtered_clients = super::mcp_bindings::filter_clients(&clients, &filter_query);

        let bindings = self.mcp_tab.bindings.clone();
        let selected_actor_id = self.mcp_tab.selected_actor_id.clone();

        let known_actor_ids: Vec<String> = clients.iter().map(|c| c.id.clone()).collect();
        let orphan_count = super::mcp_bindings::orphan_binding_count(&bindings, &known_actor_ids);

        let ids: Vec<String> = filtered_clients.iter().map(|c| c.id.clone()).collect();
        let items: Vec<dbflux_components::composites::MasterDetailItem> = filtered_clients
            .iter()
            .map(|client| {
                let has_binding = bindings.iter().any(|b| b.actor_id == client.id);
                let is_selected = selected_actor_id.as_deref() == Some(client.id.as_str());

                dbflux_components::composites::MasterDetailItem {
                    id: SharedString::from(client.id.clone()),
                    label: SharedString::from(client.name.clone()),
                    detail: Some(SharedString::from(client.id.clone())),
                    badge: Some(if has_binding {
                        (
                            SharedString::from(dbflux_i18n::t!(
                                "connection_manager.mcp_badge_granted"
                            )),
                            dbflux_components::composites::BadgeTone::Success,
                        )
                    } else {
                        (
                            SharedString::from(dbflux_i18n::t!(
                                "connection_manager.mcp_badge_no_access"
                            )),
                            dbflux_components::composites::BadgeTone::Neutral,
                        )
                    }),
                    selected: is_selected,
                    focused: false,
                }
            })
            .collect();

        let list_config = dbflux_components::composites::MasterDetailListConfig {
            id: SharedString::from("connection-mcp-clients-list"),
            width: Widths::CONNECTION_MCP_LIST_PANEL,
            new_action: None,
            secondary_action: None,
            empty_message: Some(SharedString::from(dbflux_i18n::t!(
                "connection_manager.mcp_empty_clients"
            ))),
        };

        let entity = cx.entity();
        let list = dbflux_components::composites::render_master_detail_list(
            &list_config,
            &items,
            &self.mcp_tab.conn_mcp_client_list_scroll_handle,
            move |index: usize, window: &mut Window, cx: &mut App| {
                let Some(id) = ids.get(index).cloned() else {
                    return;
                };
                entity.update(cx, |this, cx| this.select_mcp_client(id, window, cx));
            },
            |_kind, _window, _cx| {},
            cx,
        );

        let list_column = div()
            .flex()
            .flex_col()
            .h_full()
            .opacity(opacity)
            .child(
                div()
                    .p(Spacing::SM)
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            this.begin_inline_editor_interaction(cx);
                            this.mcp_tab
                                .conn_mcp_client_filter_input
                                .update(cx, |state, cx| state.focus(window, cx));
                        }),
                    )
                    .child(Input::new(&self.mcp_tab.conn_mcp_client_filter_input)),
            )
            .child(div().flex_1().min_h_0().child(list));

        let detail = self.render_mcp_client_detail(
            selected_actor_id.as_deref(),
            &bindings,
            &roles,
            &policies,
            opacity,
            cx,
        );

        let split = div()
            .flex()
            .h(px(340.0))
            .overflow_hidden()
            .child(list_column)
            .child(detail);

        let mut content = div()
            .flex()
            .flex_col()
            .gap_3()
            .child(self.render_mcp_enabled_checkbox(cx))
            .child(split);

        if orphan_count > 0 {
            content = content.child(Text::caption(crate::labels::mcp_orphan_bindings_caption(
                orphan_count,
            )));
        }

        vec![
            self.render_section(
                &dbflux_i18n::t!("connection_manager.mcp_governance_title"),
                content,
                &theme,
            )
            .into_any_element(),
        ]
    }

    #[cfg(feature = "mcp")]
    fn render_mcp_client_detail(
        &self,
        selected_actor_id: Option<&str>,
        bindings: &[dbflux_core::ConnectionMcpPolicyBinding],
        roles: &[dbflux_mcp::PolicyRoleDto],
        policies: &[dbflux_mcp::ToolPolicyDto],
        opacity: f32,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(actor_id) = selected_actor_id else {
            return div()
                .flex_1()
                .h_full()
                .p(Spacing::SM)
                .opacity(opacity)
                .child(Text::caption(dbflux_i18n::t!(
                    "connection_manager.mcp_select_client"
                )))
                .into_any_element();
        };

        let binding = bindings.iter().find(|b| b.actor_id == actor_id).cloned();
        let has_binding = binding.is_some();
        let actor_id_for_checkbox = actor_id.to_string();

        let mut column = div()
            .id("connection-mcp-client-detail")
            .flex_1()
            .h_full()
            .track_scroll(&self.mcp_tab.conn_mcp_detail_scroll_handle)
            .overflow_y_scrollbar()
            .flex()
            .flex_col()
            .gap_3()
            .p(Spacing::SM)
            .opacity(opacity)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        Checkbox::new("conn-mcp-client-allowed")
                            .checked(has_binding)
                            .on_click(cx.listener(move |this, checked: &bool, window, cx| {
                                this.set_mcp_client_allowed(
                                    actor_id_for_checkbox.clone(),
                                    *checked,
                                    window,
                                    cx,
                                );
                            })),
                    )
                    .child(
                        div()
                            .text_sm()
                            .child(dbflux_i18n::t!("connection_manager.mcp_allow_client")),
                    ),
            );

        column = if let Some(binding) = &binding {
            let effective = super::mcp_bindings::effective_permissions(binding, roles, policies);
            let tools_text = if effective.tools.is_empty() {
                dbflux_i18n::t!("connection_manager.mcp_effective_none")
            } else {
                effective.tools.join(", ")
            };
            let classes_text = if effective.classes.is_empty() {
                dbflux_i18n::t!("connection_manager.mcp_effective_none")
            } else {
                effective.classes.join(", ")
            };

            column
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Label::new(dbflux_i18n::t!("connection_manager.role_label")))
                        .child(Text::caption(dbflux_i18n::t!(
                            "connection_manager.mcp_role_hint"
                        )))
                        .child(self.mcp_tab.conn_mcp_role_dropdown.clone())
                        .child(Text::caption(dbflux_i18n::t!(
                            "connection_manager.additional_roles_optional"
                        )))
                        .child(self.mcp_tab.conn_mcp_role_multi_select.clone()),
                )
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Text::label(dbflux_i18n::t!(
                            "connection_manager.policy_label"
                        )))
                        .child(Text::caption(dbflux_i18n::t!(
                            "connection_manager.mcp_policy_hint"
                        )))
                        .child(self.mcp_tab.conn_mcp_policy_dropdown.clone())
                        .child(Text::caption(dbflux_i18n::t!(
                            "connection_manager.additional_policies_optional"
                        )))
                        .child(self.mcp_tab.conn_mcp_policy_multi_select.clone()),
                )
                .child(Text::caption(crate::labels::mcp_effective_tools_line(
                    &tools_text,
                )))
                .child(Text::caption(crate::labels::mcp_effective_classes_line(
                    &classes_text,
                )))
        } else {
            column.child(Text::caption(dbflux_i18n::t!(
                "connection_manager.mcp_client_denied"
            )))
        };

        column.into_any_element()
    }

    #[cfg(not(feature = "mcp"))]
    pub(super) fn render_mcp_tab(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme().clone();

        let content = div()
            .flex()
            .flex_col()
            .gap_3()
            .child(self.render_mcp_enabled_checkbox(cx))
            .child(Text::caption(dbflux_i18n::t!(
                "connection_manager.mcp_not_compiled"
            )));

        vec![
            self.render_section(
                &dbflux_i18n::t!("connection_manager.mcp_governance_title"),
                content,
                &theme,
            )
            .into_any_element(),
        ]
    }
}

// The `file_picker_label` helper and its tests moved to
// `dbflux_components::primitives::file_picker` together with the `FilePicker`

#[cfg(test)]
mod connection_overrides_i18n_tests {
    const CONNECTION_OVERRIDES_KEYS: &[&str] = &[
        "connection_manager.connection_overrides_title",
        "connection_manager.connection_hooks_title",
        "connection_manager.overrides.on",
        "connection_manager.overrides.off",
        "connection_manager.placeholder.extra_hook_ids",
        "connection_manager.placeholder.use_connection_auth_profile",
        "settings.general.override_value_header",
    ];

    #[test]
    fn connection_overrides_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in CONNECTION_OVERRIDES_KEYS {
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
    fn connection_overrides_title_differs_between_locales() {
        let en = dbflux_i18n::t!(
            "connection_manager.connection_overrides_title",
            locale = "en"
        );
        let es = dbflux_i18n::t!(
            "connection_manager.connection_overrides_title",
            locale = "es"
        );

        assert_ne!(
            en, es,
            "connection_manager.connection_overrides_title should differ between en and es"
        );
    }

    #[test]
    fn connection_overrides_keep_their_own_row_labels() {
        assert_eq!(
            dbflux_i18n::t!("connection_manager.overrides.refresh_policy", locale = "en"),
            "Refresh policy"
        );
        assert_eq!(
            dbflux_i18n::t!(
                "connection_manager.overrides.refresh_interval",
                locale = "en"
            ),
            "Refresh interval (s)"
        );
        assert_eq!(
            dbflux_i18n::t!(
                "connection_manager.overrides.confirm_dangerous",
                locale = "en"
            ),
            "Confirm dangerous queries"
        );
        assert_eq!(
            dbflux_i18n::t!("connection_manager.overrides.requires_where", locale = "en"),
            "Requires WHERE clause"
        );
        assert_eq!(
            dbflux_i18n::t!(
                "connection_manager.overrides.requires_preview",
                locale = "en"
            ),
            "Requires preview"
        );
        assert_ne!(
            dbflux_i18n::t!("connection_manager.overrides.requires_where", locale = "en"),
            dbflux_i18n::t!("connection_manager.overrides.requires_where", locale = "es")
        );
    }

    #[test]
    fn overrides_on_off_have_expected_english_text() {
        assert_eq!(
            dbflux_i18n::t!("connection_manager.overrides.on", locale = "en"),
            "On"
        );
        assert_eq!(
            dbflux_i18n::t!("connection_manager.overrides.off", locale = "en"),
            "Off"
        );
    }
}

#[cfg(test)]
mod driver_settings_and_mcp_governance_i18n_tests {
    const DRIVER_SETTINGS_AND_MCP_KEYS: &[&str] = &[
        "connection_manager.driver_settings_title",
        "connection_manager.driver_no_custom_settings",
        "connection_manager.mcp_disabled",
        "connection_manager.enable_mcp",
        "connection_manager.role_label",
        "connection_manager.additional_roles_optional",
        "connection_manager.policy_label",
        "connection_manager.additional_policies_optional",
        "connection_manager.mcp_governance_title",
        "connection_manager.mcp_role_hint",
        "connection_manager.mcp_policy_hint",
        "connection_manager.mcp_not_compiled",
        "connection_manager.placeholder.filter_trusted_clients",
        "connection_manager.mcp_badge_granted",
        "connection_manager.mcp_badge_no_access",
        "connection_manager.mcp_empty_clients",
        "connection_manager.mcp_allow_client",
        "connection_manager.mcp_client_denied",
        "connection_manager.mcp_select_client",
        "connection_manager.mcp_effective_tools",
        "connection_manager.mcp_effective_classes",
        "connection_manager.mcp_effective_none",
        "connection_manager.mcp_orphan_bindings.one",
        "connection_manager.mcp_orphan_bindings.many",
    ];

    #[test]
    fn driver_settings_and_mcp_governance_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in DRIVER_SETTINGS_AND_MCP_KEYS {
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
    fn connection_manager_mcp_governance_title_differs_between_locales() {
        let en = dbflux_i18n::t!("connection_manager.mcp_governance_title", locale = "en");
        let es = dbflux_i18n::t!("connection_manager.mcp_governance_title", locale = "es");

        assert_ne!(
            en, es,
            "connection_manager.mcp_governance_title should differ between en and es"
        );
    }

    #[test]
    fn connection_manager_driver_settings_title_exact_english_value() {
        let en = dbflux_i18n::t!("connection_manager.driver_settings_title", locale = "en");

        assert_eq!(en, "Driver Settings");
    }
}
// primitive itself.
