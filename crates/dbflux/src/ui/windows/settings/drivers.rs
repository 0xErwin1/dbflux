use super::{DriverSettingsEntry, SettingsFocus, SettingsWindow};
use crate::ui::components::form_renderer;
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use dbflux_core::{
    AppConfig, AppConfigStore, DriverCapabilities, FormFieldKind, FormValues, GlobalOverrides,
    RefreshPolicySetting,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputEvent};
use gpui_component::scroll::ScrollableElement;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;

const CAPABILITY_CATALOG: &[(DriverCapabilities, &str)] = &[
    (DriverCapabilities::MULTIPLE_DATABASES, "Multiple Databases"),
    (DriverCapabilities::SCHEMAS, "Schemas"),
    (DriverCapabilities::SSH_TUNNEL, "SSH Tunnel"),
    (DriverCapabilities::SSL, "SSL/TLS"),
    (DriverCapabilities::AUTHENTICATION, "Authentication"),
    (DriverCapabilities::QUERY_CANCELLATION, "Query Cancellation"),
    (DriverCapabilities::QUERY_TIMEOUT, "Query Timeout"),
    (DriverCapabilities::TRANSACTIONS, "Transactions"),
    (
        DriverCapabilities::PREPARED_STATEMENTS,
        "Prepared Statements",
    ),
    (DriverCapabilities::VIEWS, "Views"),
    (DriverCapabilities::FOREIGN_KEYS, "Foreign Keys"),
    (DriverCapabilities::INDEXES, "Indexes"),
    (DriverCapabilities::CUSTOM_TYPES, "Custom Types"),
    (DriverCapabilities::INSERT, "Insert"),
    (DriverCapabilities::UPDATE, "Update"),
    (DriverCapabilities::DELETE, "Delete"),
    (DriverCapabilities::PAGINATION, "Pagination"),
    (DriverCapabilities::SORTING, "Sorting"),
    (DriverCapabilities::FILTERING, "Filtering"),
    (DriverCapabilities::EXPORT_CSV, "Export CSV"),
    (DriverCapabilities::EXPORT_JSON, "Export JSON"),
    (DriverCapabilities::NESTED_DOCUMENTS, "Nested Documents"),
    (DriverCapabilities::ARRAYS, "Arrays"),
    (DriverCapabilities::AGGREGATION, "Aggregation"),
    (DriverCapabilities::KV_SCAN, "KV Scan"),
    (DriverCapabilities::KV_GET, "KV Get"),
    (DriverCapabilities::KV_SET, "KV Set"),
    (DriverCapabilities::KV_DELETE, "KV Delete"),
    (DriverCapabilities::KV_EXISTS, "KV Exists"),
    (DriverCapabilities::KV_TTL, "KV TTL"),
    (DriverCapabilities::KV_KEY_TYPES, "KV Key Types"),
    (DriverCapabilities::KV_VALUE_SIZE, "KV Value Size"),
    (DriverCapabilities::KV_RENAME, "KV Rename"),
    (DriverCapabilities::KV_BULK_GET, "KV Bulk Get"),
    (DriverCapabilities::KV_STREAM_RANGE, "KV Stream Range"),
    (DriverCapabilities::KV_STREAM_ADD, "KV Stream Add"),
    (DriverCapabilities::KV_STREAM_DELETE, "KV Stream Delete"),
    (DriverCapabilities::PUBSUB, "Pub/Sub"),
    (DriverCapabilities::GRAPH_TRAVERSAL, "Graph Traversal"),
    (DriverCapabilities::EDGE_PROPERTIES, "Edge Properties"),
];

fn policy_label(policy: RefreshPolicySetting) -> &'static str {
    match policy {
        RefreshPolicySetting::Manual => "Manual",
        RefreshPolicySetting::Interval => "Interval",
    }
}

fn bool_label(value: bool) -> &'static str {
    if value {
        "On"
    } else {
        "Off"
    }
}

fn bool_override_index(value: Option<bool>) -> usize {
    match value {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    }
}

impl SettingsWindow {
    pub(super) fn has_unsaved_driver_changes(&self) -> bool {
        self.drv_dirty
    }

    pub(super) fn drv_load_entries(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let selected_key = self
            .drv_selected_idx
            .and_then(|idx| self.drv_entries.get(idx))
            .map(|entry| entry.driver_key.clone());

        let mut entries: Vec<DriverSettingsEntry> = self
            .app_state
            .read(cx)
            .drivers()
            .values()
            .map(|driver| DriverSettingsEntry {
                driver_key: driver.driver_key(),
                metadata: driver.metadata().clone(),
                settings_schema: driver.settings_schema(),
            })
            .collect();

        entries.sort_by(|left, right| {
            left.metadata
                .display_name
                .cmp(&right.metadata.display_name)
                .then_with(|| left.driver_key.cmp(&right.driver_key))
        });

        self.drv_entries = entries;

        self.drv_selected_idx = selected_key.as_ref().and_then(|key| {
            self.drv_entries
                .iter()
                .position(|entry| &entry.driver_key == key)
        });

        if self.drv_selected_idx.is_none() && !self.drv_entries.is_empty() {
            self.drv_selected_idx = Some(0);
        }

        self.drv_load_selected_editor(window, cx);
    }

    fn drv_selected_entry(&self) -> Option<&DriverSettingsEntry> {
        self.drv_selected_idx
            .and_then(|idx| self.drv_entries.get(idx))
    }

    pub(super) fn drv_select_driver(
        &mut self,
        idx: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let _ = self.drv_sync_selected_editor(cx, false);

        self.drv_selected_idx = Some(idx);
        self.drv_load_selected_editor(window, cx);
        self.focus_area = SettingsFocus::Content;
        cx.notify();
    }

    fn drv_load_selected_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.drv_form_subscriptions.clear();
        self.drv_form_state.clear();

        let Some(entry) = self.drv_selected_entry().cloned() else {
            return;
        };

        let overrides = self
            .drv_overrides
            .get(&entry.driver_key)
            .cloned()
            .unwrap_or_default();

        let global = &self.gen_settings;

        self.drv_override_refresh_policy = overrides.refresh_policy.is_some();
        self.drv_override_refresh_interval = overrides.refresh_interval_secs.is_some();

        let selected_policy = overrides
            .refresh_policy
            .unwrap_or(global.default_refresh_policy);
        let selected_policy_index = match selected_policy {
            RefreshPolicySetting::Manual => 0,
            RefreshPolicySetting::Interval => 1,
        };

        self.drv_refresh_policy_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(selected_policy_index), cx);
        });

        let refresh_interval = overrides
            .refresh_interval_secs
            .unwrap_or(global.default_refresh_interval_secs);
        self.drv_refresh_interval_input.update(cx, |input, cx| {
            input.set_value(refresh_interval.to_string(), window, cx);
        });

        self.drv_confirm_dangerous_dropdown
            .update(cx, |dropdown, cx| {
                dropdown
                    .set_selected_index(Some(bool_override_index(overrides.confirm_dangerous)), cx);
            });

        self.drv_requires_where_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(bool_override_index(overrides.requires_where)), cx);
        });

        self.drv_requires_preview_dropdown
            .update(cx, |dropdown, cx| {
                dropdown
                    .set_selected_index(Some(bool_override_index(overrides.requires_preview)), cx);
            });

        if let Some(schema) = entry.settings_schema {
            let values = self
                .drv_settings
                .get(&entry.driver_key)
                .cloned()
                .unwrap_or_default();

            self.drv_form_state = form_renderer::create_inputs(&schema, &values, window, cx);

            let mut subscriptions = Vec::new();
            for input in self.drv_form_state.inputs.values() {
                subscriptions.push(cx.subscribe_in(
                    input,
                    window,
                    |this, _, event: &InputEvent, _window, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.drv_dirty = true;
                            cx.notify();
                        }
                    },
                ));
            }

            for dropdown in self.drv_form_state.dropdowns.values() {
                subscriptions.push(cx.subscribe_in(
                    dropdown,
                    window,
                    |this, _, _: &crate::ui::dropdown::DropdownSelectionChanged, _window, cx| {
                        this.drv_dirty = true;
                        cx.notify();
                    },
                ));
            }

            self.drv_form_subscriptions = subscriptions;
        }
    }

    fn drv_sync_selected_editor(&mut self, cx: &App, strict: bool) -> Result<(), String> {
        let Some(entry) = self.drv_selected_entry().cloned() else {
            return Ok(());
        };

        let mut overrides = GlobalOverrides::default();

        if self.drv_override_refresh_policy {
            let selected = self
                .drv_refresh_policy_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "manual".to_string());

            overrides.refresh_policy = Some(if selected == "interval" {
                RefreshPolicySetting::Interval
            } else {
                RefreshPolicySetting::Manual
            });
        }

        if self.drv_override_refresh_interval {
            let raw = self
                .drv_refresh_interval_input
                .read(cx)
                .value()
                .trim()
                .to_string();

            if raw.is_empty() {
                if strict {
                    return Err("Refresh interval override must not be empty".to_string());
                }
            } else {
                match raw.parse::<u32>() {
                    Ok(value) if value > 0 => {
                        overrides.refresh_interval_secs = Some(value);
                    }
                    _ if strict => {
                        return Err(
                            "Refresh interval override must be a number greater than 0".to_string()
                        );
                    }
                    _ => {}
                }
            }
        }

        let parse_boolean_override = |selection: Option<SharedString>| match selection
            .as_ref()
            .map(|value| value.as_ref())
        {
            Some("true") => Some(true),
            Some("false") => Some(false),
            _ => None,
        };

        overrides.confirm_dangerous = parse_boolean_override(
            self.drv_confirm_dangerous_dropdown
                .read(cx)
                .selected_value(),
        );

        overrides.requires_where =
            parse_boolean_override(self.drv_requires_where_dropdown.read(cx).selected_value());

        overrides.requires_preview =
            parse_boolean_override(self.drv_requires_preview_dropdown.read(cx).selected_value());

        if overrides.is_empty() {
            self.drv_overrides.remove(&entry.driver_key);
        } else {
            self.drv_overrides
                .insert(entry.driver_key.clone(), overrides);
        }

        if let Some(schema) = entry.settings_schema {
            let collected = form_renderer::collect_values(
                &schema,
                &self.drv_form_state.inputs,
                &self.drv_form_state.checkboxes,
                &self.drv_form_state.dropdowns,
                cx,
            );

            let mut merged = self
                .drv_settings
                .get(&entry.driver_key)
                .cloned()
                .unwrap_or_default();

            for tab in &schema.tabs {
                for section in &tab.sections {
                    for field in &section.fields {
                        merged.remove(&field.id);
                    }
                }
            }

            for (field_id, value) in collected {
                merged.insert(field_id, value);
            }

            merged.retain(|_, value| !value.is_empty());

            if merged.is_empty() {
                self.drv_settings.remove(&entry.driver_key);
            } else {
                self.drv_settings.insert(entry.driver_key.clone(), merged);
            }
        }

        Ok(())
    }

    pub(super) fn save_driver_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Err(message) = self.drv_sync_selected_editor(cx, true) {
            cx.toast_error(message, window);
            return;
        }

        self.drv_overrides
            .retain(|_, overrides| !overrides.is_empty());
        self.drv_settings.retain(|_, values| !values.is_empty());

        let store = match AppConfigStore::new() {
            Ok(store) => store,
            Err(error) => {
                cx.toast_error(format!("Cannot save: {}", error), window);
                return;
            }
        };

        let mut config = match store.load() {
            Ok(config) => config,
            Err(error) => {
                log::error!(
                    "Failed to load config before driver settings save: {}",
                    error
                );
                AppConfig::default()
            }
        };

        config.driver_overrides = self.drv_overrides.clone();
        config.driver_settings = self.drv_settings.clone();

        if let Err(error) = store.save(&config) {
            log::error!("Failed to save driver settings: {}", error);
            cx.toast_error(format!("Failed to save: {}", error), window);
            return;
        }

        let overrides_for_state = self.drv_overrides.clone();
        let settings_for_state = self.drv_settings.clone();

        self.app_state.update(cx, move |state, _cx| {
            let existing_override_keys: Vec<String> =
                state.driver_overrides().keys().cloned().collect();
            for key in existing_override_keys {
                if !overrides_for_state.contains_key(&key) {
                    state.update_driver_overrides(key, GlobalOverrides::default());
                }
            }

            for (key, overrides) in &overrides_for_state {
                state.update_driver_overrides(key.clone(), overrides.clone());
            }

            let existing_setting_keys: Vec<String> =
                state.driver_settings().keys().cloned().collect();
            for key in existing_setting_keys {
                if !settings_for_state.contains_key(&key) {
                    state.update_driver_settings(key, FormValues::new());
                }
            }

            for (key, values) in &settings_for_state {
                state.update_driver_settings(key.clone(), values.clone());
            }
        });

        self.drv_dirty = false;
        cx.toast_success("Driver settings saved.", window);
    }

    pub(super) fn render_drivers_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

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
                            .child("Drivers"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Configure per-driver overrides and driver-defined settings"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_driver_list(cx))
                    .child(self.render_driver_editor(cx)),
            )
    }

    fn render_driver_list(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let list_focused = self.focus_area == SettingsFocus::Content;

        div()
            .w(px(300.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .p_2()
            .flex()
            .flex_col()
            .gap_1()
            .when(self.drv_entries.is_empty(), |d| {
                d.child(
                    div()
                        .p_3()
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("No registered drivers"),
                )
            })
            .children(self.drv_entries.iter().enumerate().map(|(idx, entry)| {
                let selected = self.drv_selected_idx == Some(idx);
                let focused = list_focused && selected;

                div()
                    .id(SharedString::from(format!(
                        "settings-driver-{}",
                        entry.driver_key
                    )))
                    .px_3()
                    .py_2()
                    .rounded(px(4.0))
                    .cursor_pointer()
                    .border_1()
                    .border_color(if focused && !selected {
                        theme.primary
                    } else {
                        gpui::transparent_black()
                    })
                    .when(selected, |d| d.bg(theme.secondary))
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.drv_select_driver(idx, window, cx);
                    }))
                    .child(
                        div()
                            .flex()
                            .items_start()
                            .gap_2()
                            .child(
                                svg()
                                    .path(AppIcon::from_icon(entry.metadata.icon).path())
                                    .size_4()
                                    .mt(px(2.0))
                                    .text_color(theme.muted_foreground),
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
                                            .child(entry.metadata.display_name.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(entry.driver_key.clone()),
                                    ),
                            ),
                    )
            }))
    }

    fn render_driver_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let Some(entry) = self.drv_selected_entry() else {
            return div()
                .flex_1()
                .h_full()
                .flex()
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("Select a driver to configure settings"),
                );
        };

        let global = &self.gen_settings;

        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div().p_4().border_b_1().border_color(theme.border).child(
                    div()
                        .flex()
                        .items_start()
                        .justify_between()
                        .gap_4()
                        .child(
                            div()
                                .flex()
                                .items_start()
                                .gap_3()
                                .child(
                                    svg()
                                        .path(AppIcon::from_icon(entry.metadata.icon).path())
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
                                                .text_base()
                                                .font_weight(FontWeight::SEMIBOLD)
                                                .child(entry.metadata.display_name.clone()),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child(entry.driver_key.clone()),
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.muted_foreground)
                                                .child(entry.metadata.description.clone()),
                                        ),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(
                                    div()
                                        .text_xs()
                                        .px_2()
                                        .py_1()
                                        .rounded(px(4.0))
                                        .bg(theme.secondary)
                                        .child(entry.metadata.category.display_name()),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .px_2()
                                        .py_1()
                                        .rounded(px(4.0))
                                        .bg(theme.secondary)
                                        .child(
                                            entry
                                                .metadata
                                                .query_language
                                                .display_name()
                                                .to_string(),
                                        ),
                                ),
                        ),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_5()
                    .child(self.render_capabilities(entry, cx))
                    .child(self.render_global_overrides(global, cx))
                    .child(self.render_driver_schema(entry, cx)),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .justify_end()
                    .child(
                        Button::new("save-driver-settings")
                            .label("Save")
                            .small()
                            .primary()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_driver_settings(window, cx);
                            })),
                    ),
            )
    }

    fn render_capabilities(
        &self,
        entry: &DriverSettingsEntry,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let caps = entry.metadata.capabilities;

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("Capabilities"),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .children(CAPABILITY_CATALOG.iter().map(|(capability, label)| {
                        let supported = caps.contains(*capability);
                        div()
                            .px_2()
                            .py_1()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(theme.border)
                            .bg(if supported {
                                theme.secondary
                            } else {
                                gpui::transparent_black()
                            })
                            .text_xs()
                            .child(format!("{} {}", if supported { "✓" } else { "-" }, label))
                    })),
            )
    }

    fn render_global_overrides(
        &self,
        global: &dbflux_core::GeneralSettings,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("Global Overrides"),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Enable override to replace the global default for this driver."),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(px(220.0)))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child("Override Value"),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                Checkbox::new("drv-override-refresh-policy")
                                    .checked(self.drv_override_refresh_policy)
                                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                        this.drv_override_refresh_policy = *checked;
                                        this.drv_dirty = true;
                                        cx.notify();
                                    })),
                            )
                            .child(div().w(px(220.0)).text_sm().child("Refresh policy"))
                            .child(
                                div()
                                    .min_w(px(160.0))
                                    .opacity(if self.drv_override_refresh_policy {
                                        1.0
                                    } else {
                                        0.6
                                    })
                                    .child(self.drv_refresh_policy_dropdown.clone()),
                            )
                            .child(div().text_xs().text_color(theme.muted_foreground).child(
                                format!("Default: {}", policy_label(global.default_refresh_policy)),
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                Checkbox::new("drv-override-refresh-interval")
                                    .checked(self.drv_override_refresh_interval)
                                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                        this.drv_override_refresh_interval = *checked;
                                        this.drv_dirty = true;
                                        cx.notify();
                                    })),
                            )
                            .child(div().w(px(220.0)).text_sm().child("Refresh interval (sec)"))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .opacity(if self.drv_override_refresh_interval {
                                        1.0
                                    } else {
                                        0.6
                                    })
                                    .child(Input::new(&self.drv_refresh_interval_input).small()),
                            )
                            .child(div().text_xs().text_color(theme.muted_foreground).child(
                                format!("Default: {}", global.default_refresh_interval_secs),
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                div()
                                    .w(px(220.0))
                                    .text_sm()
                                    .child("Confirm dangerous queries"),
                            )
                            .child(
                                div()
                                    .w(px(160.0))
                                    .child(self.drv_confirm_dangerous_dropdown.clone()),
                            )
                            .child(div().text_xs().text_color(theme.muted_foreground).child(
                                format!(
                                    "Default: {}",
                                    bool_label(global.confirm_dangerous_queries)
                                ),
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(px(220.0)).text_sm().child("Require WHERE"))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .child(self.drv_requires_where_dropdown.clone()),
                            )
                            .child(div().text_xs().text_color(theme.muted_foreground).child(
                                format!("Default: {}", bool_label(global.dangerous_requires_where)),
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(px(220.0)).text_sm().child("Require preview"))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .child(self.drv_requires_preview_dropdown.clone()),
                            )
                            .child(div().text_xs().text_color(theme.muted_foreground).child(
                                format!(
                                    "Default: {}",
                                    bool_label(global.dangerous_requires_preview)
                                ),
                            )),
                    ),
            )
    }

    fn render_driver_schema(
        &self,
        entry: &DriverSettingsEntry,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let Some(schema) = &entry.settings_schema else {
            return div()
                .flex()
                .flex_col()
                .gap_2()
                .child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child("Driver Settings"),
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("No custom settings for this driver."),
                );
        };

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("Driver Settings"),
            )
            .children(
                schema
                    .tabs
                    .iter()
                    .flat_map(|tab| tab.sections.iter())
                    .map(|section| {
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
                            .children(section.fields.iter().filter_map(|field| {
                                let enabled = form_renderer::is_field_enabled(
                                    field,
                                    &self.drv_form_state.checkboxes,
                                );

                                match &field.kind {
                                    FormFieldKind::Checkbox => {
                                        let checked = self
                                            .drv_form_state
                                            .checkboxes
                                            .get(&field.id)
                                            .copied()
                                            .unwrap_or(false);

                                        Some(
                                            div()
                                                .px_2()
                                                .py_1()
                                                .rounded(px(4.0))
                                                .opacity(if enabled { 1.0 } else { 0.6 })
                                                .child(
                                                    Checkbox::new(SharedString::from(format!(
                                                        "drv-schema-{}",
                                                        field.id
                                                    )))
                                                    .checked(checked)
                                                    .label(field.label.as_str())
                                                    .on_click(cx.listener({
                                                        let field_id = field.id.clone();
                                                        move |this, checked: &bool, _, cx| {
                                                            if !enabled {
                                                                return;
                                                            }

                                                            this.drv_form_state
                                                                .checkboxes
                                                                .insert(field_id.clone(), *checked);
                                                            this.drv_dirty = true;
                                                            cx.notify();
                                                        }
                                                    })),
                                                )
                                                .into_any_element(),
                                        )
                                    }
                                    FormFieldKind::Select { .. } => {
                                        let dropdown =
                                            self.drv_form_state.dropdowns.get(&field.id)?.clone();
                                        Some(
                                            div()
                                                .flex()
                                                .flex_col()
                                                .gap_1()
                                                .opacity(if enabled { 1.0 } else { 0.6 })
                                                .child(div().text_sm().child(field.label.clone()))
                                                .child(div().w(px(240.0)).child(dropdown))
                                                .into_any_element(),
                                        )
                                    }
                                    _ => {
                                        let input =
                                            self.drv_form_state.inputs.get(&field.id)?.clone();
                                        Some(
                                            div()
                                                .flex()
                                                .flex_col()
                                                .gap_1()
                                                .child(div().text_sm().child(field.label.clone()))
                                                .child(
                                                    Input::new(&input).small().disabled(!enabled),
                                                )
                                                .into_any_element(),
                                        )
                                    }
                                }
                            }))
                    }),
            )
    }
}
