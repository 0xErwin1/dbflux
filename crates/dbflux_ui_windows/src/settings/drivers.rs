use super::drivers_section::{
    DriverEditorField, DriverSettingsEntry, DriversFocus, DriversSection,
};
use super::form_section::FormSection;
use super::layout;
use super::section_trait::SectionFocusEvent;
use crate::labels::{override_default_caption, override_default_seconds_caption};
use dbflux_components::components::form_renderer;
use dbflux_components::controls::InputEvent;
use dbflux_components::controls::{Button, Checkbox, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeVariant, Icon, Label};
use dbflux_components::tokens::{Heights, Radii, Widths};
use dbflux_components::typography::{
    Body, FieldLabel, MonoCaption, MonoLabel, MonoMeta, PanelTitle, SubSectionLabel,
};
use dbflux_core::{
    DriverCapabilities, FormFieldKind, FormValues, GlobalOverrides, RefreshPolicySetting,
};
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

/// Capability ids in their stable display order. Each id doubles as the
/// catalog key segment for its translated name: `settings.drivers.capability.<id>`.
const CAPABILITY_IDS: &[(DriverCapabilities, &str)] = &[
    (DriverCapabilities::MULTIPLE_DATABASES, "multiple_databases"),
    (DriverCapabilities::SCHEMAS, "schemas"),
    (DriverCapabilities::SSH_TUNNEL, "ssh_tunnel"),
    (DriverCapabilities::SSL, "ssl_tls"),
    (DriverCapabilities::AUTHENTICATION, "authentication"),
    (DriverCapabilities::QUERY_CANCELLATION, "query_cancellation"),
    (DriverCapabilities::QUERY_TIMEOUT, "query_timeout"),
    (DriverCapabilities::TRANSACTIONS, "transactions"),
    (
        DriverCapabilities::PREPARED_STATEMENTS,
        "prepared_statements",
    ),
    (DriverCapabilities::VIEWS, "views"),
    (DriverCapabilities::FOREIGN_KEYS, "foreign_keys"),
    (DriverCapabilities::INDEXES, "indexes"),
    (DriverCapabilities::CUSTOM_TYPES, "custom_types"),
    (DriverCapabilities::INSERT, "insert"),
    (DriverCapabilities::UPDATE, "update"),
    (DriverCapabilities::DELETE, "delete"),
    (DriverCapabilities::PAGINATION, "pagination"),
    (DriverCapabilities::SORTING, "sorting"),
    (DriverCapabilities::FILTERING, "filtering"),
    (DriverCapabilities::EXPORT_CSV, "export_csv"),
    (DriverCapabilities::EXPORT_JSON, "export_json"),
    (DriverCapabilities::NESTED_DOCUMENTS, "nested_documents"),
    (DriverCapabilities::ARRAYS, "arrays"),
    (DriverCapabilities::AGGREGATION, "aggregation"),
    (DriverCapabilities::KV_SCAN, "kv_scan"),
    (DriverCapabilities::KV_GET, "kv_get"),
    (DriverCapabilities::KV_SET, "kv_set"),
    (DriverCapabilities::KV_DELETE, "kv_delete"),
    (DriverCapabilities::KV_EXISTS, "kv_exists"),
    (DriverCapabilities::KV_TTL, "kv_ttl"),
    (DriverCapabilities::KV_KEY_TYPES, "kv_key_types"),
    (DriverCapabilities::KV_VALUE_SIZE, "kv_value_size"),
    (DriverCapabilities::KV_RENAME, "kv_rename"),
    (DriverCapabilities::KV_BULK_GET, "kv_bulk_get"),
    (DriverCapabilities::KV_STREAM_RANGE, "kv_stream_range"),
    (DriverCapabilities::KV_STREAM_ADD, "kv_stream_add"),
    (DriverCapabilities::KV_STREAM_DELETE, "kv_stream_delete"),
    (DriverCapabilities::PUBSUB, "pub_sub"),
    (DriverCapabilities::GRAPH_TRAVERSAL, "graph_traversal"),
    (DriverCapabilities::EDGE_PROPERTIES, "edge_properties"),
];

/// Resolves the capability display catalog for the active locale: (flag,
/// translated name). Call once per render and reuse across capability chip
/// rows instead of re-resolving per row.
fn capability_catalog() -> Vec<(DriverCapabilities, String)> {
    CAPABILITY_IDS
        .iter()
        .map(|&(capability, id)| {
            (
                capability,
                dbflux_i18n::t!(&format!("settings.drivers.capability.{id}")),
            )
        })
        .collect()
}

fn policy_label(policy: RefreshPolicySetting) -> String {
    match policy {
        RefreshPolicySetting::Manual => {
            dbflux_i18n::t!("settings.general.refresh_policy.option.manual")
        }
        RefreshPolicySetting::Interval => {
            dbflux_i18n::t!("settings.general.refresh_policy.option.interval")
        }
    }
}

fn bool_override_caption(value: bool) -> String {
    if value {
        dbflux_i18n::t!("connection_manager.overrides.on")
    } else {
        dbflux_i18n::t!("connection_manager.overrides.off")
    }
}

fn bool_override_index(value: Option<bool>) -> usize {
    match value {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    }
}

fn driver_entry_name_text(text: impl Into<SharedString>) -> MonoLabel {
    MonoLabel::new(text)
}

fn driver_entry_key_text(text: impl Into<SharedString>) -> MonoMeta {
    MonoMeta::new(text)
}

impl DriversSection {
    pub(super) fn drv_move_editor_down(&mut self) {
        self.drv_editor_field = match self.drv_editor_field {
            DriverEditorField::OverrideRefreshPolicy | DriverEditorField::RefreshPolicy => {
                DriverEditorField::OverrideRefreshInterval
            }
            DriverEditorField::OverrideRefreshInterval | DriverEditorField::RefreshInterval => {
                DriverEditorField::ConfirmDangerous
            }
            DriverEditorField::ConfirmDangerous => DriverEditorField::RequiresWhere,
            DriverEditorField::RequiresWhere => DriverEditorField::RequiresPreview,
            DriverEditorField::RequiresPreview => DriverEditorField::Save,
            DriverEditorField::Save => DriverEditorField::Save,
        };
    }

    pub(super) fn drv_move_editor_up(&mut self) {
        self.drv_editor_field = match self.drv_editor_field {
            DriverEditorField::OverrideRefreshPolicy => DriverEditorField::OverrideRefreshPolicy,
            DriverEditorField::RefreshPolicy => DriverEditorField::OverrideRefreshPolicy,
            DriverEditorField::OverrideRefreshInterval => DriverEditorField::OverrideRefreshPolicy,
            DriverEditorField::RefreshInterval => DriverEditorField::OverrideRefreshInterval,
            DriverEditorField::ConfirmDangerous => DriverEditorField::OverrideRefreshInterval,
            DriverEditorField::RequiresWhere => DriverEditorField::ConfirmDangerous,
            DriverEditorField::RequiresPreview => DriverEditorField::RequiresWhere,
            DriverEditorField::Save => DriverEditorField::RequiresPreview,
        };
    }

    pub(super) fn drv_activate_editor_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.drv_editor_field {
            DriverEditorField::OverrideRefreshPolicy => {
                self.drv_override_refresh_policy = !self.drv_override_refresh_policy;
                self.drv_editor_dirty = true;
                self.validate_form_field();
                cx.notify();
            }
            DriverEditorField::RefreshPolicy => {
                if !self.drv_override_refresh_policy {
                    return;
                }

                self.drv_refresh_policy_dropdown.update(cx, |dropdown, cx| {
                    dropdown.open(cx);
                });
            }
            DriverEditorField::OverrideRefreshInterval => {
                self.drv_override_refresh_interval = !self.drv_override_refresh_interval;
                self.drv_editor_dirty = true;
                self.validate_form_field();

                if !self.drv_override_refresh_interval {
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }

                cx.notify();
            }
            DriverEditorField::RefreshInterval => {
                if !self.drv_override_refresh_interval {
                    return;
                }

                self.focus_current_field(window, cx);
            }
            DriverEditorField::ConfirmDangerous => {
                self.drv_confirm_dangerous_dropdown
                    .update(cx, |dropdown, cx| dropdown.open(cx));
            }
            DriverEditorField::RequiresWhere => {
                self.drv_requires_where_dropdown
                    .update(cx, |dropdown, cx| dropdown.open(cx));
            }
            DriverEditorField::RequiresPreview => {
                self.drv_requires_preview_dropdown
                    .update(cx, |dropdown, cx| dropdown.open(cx));
            }
            DriverEditorField::Save => {
                self.save_driver_settings(window, cx);
            }
        }
    }

    /// Deterministic dirty check: compare the working driver overrides and
    /// settings (including the currently-open editor) against what is persisted
    /// in AppState.  This avoids false positives from transient UI events.
    pub(super) fn has_unsaved_driver_changes(&self, cx: &App) -> bool {
        let state = self.app_state.read(cx);
        let saved_overrides = state.driver_overrides();
        let saved_settings = state.driver_settings();

        let mut working_overrides = self.drv_overrides.clone();
        let mut working_settings = self.drv_settings.clone();

        if let Some(entry) = self.drv_selected_entry() {
            let editor_overrides = self.drv_read_editor_overrides(cx);

            if editor_overrides.is_empty() {
                working_overrides.remove(&entry.driver_key);
            } else {
                working_overrides.insert(entry.driver_key.clone(), editor_overrides);
            }

            if let Some(schema) = &entry.settings_schema {
                let collected = form_renderer::collect_values(
                    schema,
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
                    working_settings.remove(&entry.driver_key);
                } else {
                    working_settings.insert(entry.driver_key.clone(), merged);
                }
            }
        }

        dbflux_core::driver_maps_differ(
            &mut working_overrides,
            &mut working_settings,
            saved_overrides,
            saved_settings,
        )
    }

    /// Read the current editor's override widgets into a `GlobalOverrides`
    /// without mutating `self`.
    fn drv_read_editor_overrides(&self, cx: &App) -> GlobalOverrides {
        let mut overrides = GlobalOverrides::default();

        if self.drv_override_refresh_policy {
            let selected = self
                .drv_refresh_policy_dropdown
                .read(cx)
                .selected_value()
                .map(|v| v.to_string())
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

            if let Ok(value) = raw.parse::<u32>()
                && value > 0
            {
                overrides.refresh_interval_secs = Some(value);
            }
        }

        let parse_boolean_override =
            |selection: Option<SharedString>| match selection.as_ref().map(|v| v.as_ref()) {
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

        overrides
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
        if self.drv_editor_dirty {
            let _ = self.drv_sync_selected_editor(cx, false);
        }

        self.drv_selected_idx = Some(idx);
        self.drv_pending_scroll_idx = Some(idx);
        self.drv_load_selected_editor(window, cx);
        self.content_focused = true;
        cx.notify();
    }

    fn drv_load_selected_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.drv_loading_selected_editor = true;
        self.drv_form_subscriptions.clear();
        self.drv_form_state.clear();

        let Some(entry) = self.drv_selected_entry().cloned() else {
            self.drv_loading_selected_editor = false;
            self.drv_editor_dirty = false;
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
                            if this.drv_loading_selected_editor {
                                return;
                            }

                            this.drv_editor_dirty = true;
                            cx.notify();
                        }
                    },
                ));
            }

            for dropdown in self.drv_form_state.dropdowns.values() {
                subscriptions.push(cx.subscribe_in(
                    dropdown,
                    window,
                    |this,
                     _,
                     _: &dbflux_components::controls::DropdownSelectionChanged,
                     _window,
                     cx| {
                        if this.drv_loading_selected_editor {
                            return;
                        }

                        this.drv_editor_dirty = true;
                        cx.notify();
                    },
                ));
            }

            self.drv_form_subscriptions = subscriptions;
        }

        self.drv_loading_selected_editor = false;
        self.drv_editor_dirty = false;
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
                    return Err(dbflux_i18n::t!(
                        "settings.drivers.error.refresh_interval_empty"
                    ));
                }
            } else {
                match raw.parse::<u32>() {
                    Ok(value) if value > 0 => {
                        overrides.refresh_interval_secs = Some(value);
                    }
                    _ if strict => {
                        return Err(dbflux_i18n::t!(
                            "settings.drivers.error.refresh_interval_invalid"
                        ));
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

        self.drv_editor_dirty = false;

        Ok(())
    }

    pub(super) fn save_driver_settings(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.drv_editor_dirty
            && let Err(message) = self.drv_sync_selected_editor(cx, true)
        {
            let toast_msg = message.to_string();
            Toast::error(toast_msg.clone())
                .meta_right(now_hms())
                .action(copy_action(toast_msg))
                .push(cx);
            return;
        }

        self.drv_overrides
            .retain(|_, overrides| !overrides.is_empty());
        self.drv_settings.retain(|_, values| !values.is_empty());

        let runtime = self.app_state.read(cx).storage_runtime();
        if let Err(e) = dbflux_app::config_loader::save_driver_settings(
            runtime,
            &self.drv_overrides,
            &self.drv_settings,
        ) {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.drivers.error.save_failed", error = e),
                ),
                cx,
            );
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

        self.drv_editor_dirty = false;

        let mut all_warnings = Vec::new();
        for entry in &self.drv_entries {
            if let Some(schema) = &entry.settings_schema
                && let Some(values) = self.drv_settings.get(&entry.driver_key)
            {
                let warnings = form_renderer::validate_values(schema, values);
                for warning in warnings {
                    all_warnings.push(format!("{}: {}", entry.metadata.display_name, warning));
                }
            }
        }

        if all_warnings.is_empty() {
            Toast::success(dbflux_i18n::t!("settings.drivers.toast.saved"))
                .meta_right(now_hms())
                .push(cx);
        } else {
            let body = all_warnings.join("\n");
            Toast::warning(dbflux_i18n::t!("settings.drivers.toast.saved_warnings"))
                .meta_right(now_hms())
                .body(body)
                .collapsible()
                .push(cx);
        }
    }

    pub(super) fn render_drivers_section(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        layout::section_container(layout::split_section_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.drivers.section_title"),
                dbflux_i18n::t!("settings.drivers.section_description"),
                cx,
            ),
            self.render_driver_list(cx),
            self.render_driver_editor(cx),
        ))
    }

    fn render_driver_list(&mut self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let list_focused = self.content_focused && self.drv_focus == DriversFocus::List;

        if let Some(scroll_idx) = self.drv_pending_scroll_idx.take() {
            self.drv_list_scroll_handle.scroll_to_item(scroll_idx);
        }

        div()
            .w(Widths::SETTINGS_LIST_PANEL)
            .h_full()
            .min_h_0()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .id("drivers-list-scroll")
                    .p_2()
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.drv_list_scroll_handle)
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(self.drv_entries.is_empty(), |d| {
                        d.child(
                            div().p_3().child(
                                Body::new(dbflux_i18n::t!("settings.drivers.empty"))
                                    .color(theme.muted_foreground),
                            ),
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
                            .rounded(Radii::SM)
                            .bg(theme.list_even)
                            .cursor_pointer()
                            .border_1()
                            .border_color(if focused {
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
                                        div().mt(px(2.0)).child(
                                            Icon::new(AppIcon::for_driver(
                                                entry.metadata.icon,
                                                entry.metadata.category,
                                            ))
                                            .size(Heights::ICON_SM)
                                            .muted(),
                                        ),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .flex_1()
                                            .child(driver_entry_name_text(
                                                entry.metadata.display_name.clone(),
                                            ))
                                            .child(driver_entry_key_text(entry.driver_key.clone())),
                                    )
                                    .when_some(entry.metadata.deployment_class, |row, class| {
                                        row.child(Badge::new(
                                            class.display_name().to_uppercase(),
                                            BadgeVariant::Neutral,
                                        ))
                                    }),
                            )
                    })),
            )
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
                    Body::new(dbflux_i18n::t!("settings.drivers.select_hint"))
                        .color(theme.muted_foreground),
                );
        };

        let global = &self.gen_settings;

        let header = div()
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
                        Icon::new(AppIcon::for_driver(
                            entry.metadata.icon,
                            entry.metadata.category,
                        ))
                        .size(px(32.0))
                        .color(theme.foreground),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(PanelTitle::new(entry.metadata.display_name.clone()))
                            .child(MonoMeta::new(entry.driver_key.clone()))
                            .child(
                                Body::new(entry.metadata.description.clone())
                                    .color(theme.muted_foreground),
                            ),
                    ),
            )
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .rounded(Radii::SM)
                            .bg(theme.secondary)
                            .child(MonoCaption::new(entry.metadata.category.display_name())),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .rounded(Radii::SM)
                            .bg(theme.secondary)
                            .child(MonoCaption::new(
                                entry.metadata.query_language.display_name().to_string(),
                            )),
                    ),
            );

        let body = div()
            .flex()
            .flex_col()
            .gap_5()
            .child(self.render_capabilities(entry, cx))
            .child(self.render_global_overrides(global, cx))
            .child(self.render_driver_schema(entry, cx));

        layout::sticky_form_shell(header, body, None, &theme)
    }

    pub(super) fn render_driver_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let editor_focused = self.content_focused && self.drv_focus == DriversFocus::Editor;

        div()
            .flex()
            .items_center()
            .gap_3()
            .child(layout::footer_action_frame(
                editor_focused && self.drv_editor_field == DriverEditorField::Save,
                cx.theme().primary,
                Button::new(
                    "save-driver-settings",
                    dbflux_i18n::t!("settings.drivers.action.save"),
                )
                .small()
                .primary()
                .w_full()
                .on_click(cx.listener(|this, _, window, cx| {
                    this.save_driver_settings(window, cx);
                })),
            ))
            .into_any_element()
    }

    fn render_capabilities(
        &self,
        entry: &DriverSettingsEntry,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let caps = entry.metadata.capabilities;
        let relevant = entry.metadata.category.relevant_capabilities();
        let catalog = capability_catalog();

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(FieldLabel::new(dbflux_i18n::t!(
                "settings.drivers.field.capabilities"
            )))
            .child(
                div().flex().flex_wrap().gap_2().children(
                    catalog
                        .into_iter()
                        .filter(|(capability, _)| relevant.contains(*capability))
                        .map(|(capability, label)| {
                            let supported = caps.contains(capability);
                            div()
                                .px_2()
                                .py_1()
                                .rounded(Radii::SM)
                                .border_1()
                                .border_color(theme.border)
                                .bg(if supported {
                                    theme.secondary
                                } else {
                                    gpui::transparent_black()
                                })
                                .child(Body::new(format!(
                                    "{} {}",
                                    if supported { "✓" } else { "-" },
                                    label
                                )))
                        }),
                ),
            )
    }

    fn render_global_overrides(
        &self,
        global: &dbflux_core::GeneralSettings,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let editor_focused = self.content_focused && self.drv_focus == DriversFocus::Editor;

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(FieldLabel::new(dbflux_i18n::t!(
                "settings.drivers.field.global_overrides"
            )))
            .child(
                Body::new(dbflux_i18n::t!("settings.drivers.global_overrides_hint"))
                    .color(theme.muted_foreground),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(Widths::SETTINGS_FORM_LABEL))
                            .child(div().w(px(160.0)).child(FieldLabel::new(dbflux_i18n::t!(
                                "settings.general.override_value_header"
                            )))),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                div()
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::OverrideRefreshPolicy
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::OverrideRefreshPolicy;
                                            cx.notify();
                                        }),
                                    )
                                    .child(
                                        Checkbox::new("drv-override-refresh-policy")
                                            .checked(self.drv_override_refresh_policy)
                                            .on_click(cx.listener(
                                                |this, checked: &bool, _, cx| {
                                                    this.drv_override_refresh_policy = *checked;
                                                    this.drv_editor_dirty = true;

                                                    if !*checked {
                                                        cx.emit(
                                                            SectionFocusEvent::RequestFocusReturn,
                                                        );
                                                    }

                                                    cx.notify();
                                                },
                                            )),
                                    ),
                            )
                            .child(div().w(Widths::SETTINGS_FORM_LABEL).child(Label::new(
                                dbflux_i18n::t!("settings.general.refresh_policy.label"),
                            )))
                            .child(
                                div()
                                    .min_w(px(160.0))
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::RefreshPolicy
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .opacity(if self.drv_override_refresh_policy {
                                        1.0
                                    } else {
                                        0.6
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::RefreshPolicy;
                                            cx.notify();
                                        }),
                                    )
                                    .child(self.drv_refresh_policy_dropdown.clone()),
                            )
                            .child(MonoCaption::new(override_default_caption(&policy_label(
                                global.default_refresh_policy,
                            )))),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                div()
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::OverrideRefreshInterval
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::OverrideRefreshInterval;
                                            cx.notify();
                                        }),
                                    )
                                    .child(
                                        Checkbox::new("drv-override-refresh-interval")
                                            .checked(self.drv_override_refresh_interval)
                                            .on_click(cx.listener(
                                                |this, checked: &bool, _, cx| {
                                                    this.drv_override_refresh_interval = *checked;
                                                    this.drv_editor_dirty = true;

                                                    if !*checked {
                                                        cx.emit(
                                                            SectionFocusEvent::RequestFocusReturn,
                                                        );
                                                    }

                                                    cx.notify();
                                                },
                                            )),
                                    ),
                            )
                            .child(div().w(Widths::SETTINGS_FORM_LABEL).child(Label::new(
                                dbflux_i18n::t!("settings.general.refresh_interval.label"),
                            )))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::RefreshInterval
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .opacity(if self.drv_override_refresh_interval {
                                        1.0
                                    } else {
                                        0.6
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::RefreshInterval;
                                            cx.notify();
                                        }),
                                    )
                                    .child(
                                        Input::new(&self.drv_refresh_interval_input)
                                            .small()
                                            .disabled(!self.drv_override_refresh_interval),
                                    ),
                            )
                            .child(MonoCaption::new(override_default_seconds_caption(
                                global.default_refresh_interval_secs,
                            ))),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(Widths::SETTINGS_FORM_LABEL).child(Label::new(
                                dbflux_i18n::t!("settings.general.confirm_dangerous.label"),
                            )))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::ConfirmDangerous
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::ConfirmDangerous;
                                            cx.notify();
                                        }),
                                    )
                                    .child(self.drv_confirm_dangerous_dropdown.clone()),
                            )
                            .child(MonoCaption::new(override_default_caption(
                                &bool_override_caption(global.confirm_dangerous_queries),
                            ))),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(Widths::SETTINGS_FORM_LABEL).child(Label::new(
                                dbflux_i18n::t!("settings.general.requires_where.label"),
                            )))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::RequiresWhere
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::RequiresWhere;
                                            cx.notify();
                                        }),
                                    )
                                    .child(self.drv_requires_where_dropdown.clone()),
                            )
                            .child(MonoCaption::new(override_default_caption(
                                &bool_override_caption(global.dangerous_requires_where),
                            ))),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(div().w(Widths::SETTINGS_FORM_LABEL).child(Label::new(
                                dbflux_i18n::t!("settings.general.requires_preview.label"),
                            )))
                            .child(
                                div()
                                    .w(px(160.0))
                                    .rounded(Radii::SM)
                                    .border_1()
                                    .border_color(
                                        if editor_focused
                                            && self.drv_editor_field
                                                == DriverEditorField::RequiresPreview
                                        {
                                            theme.primary
                                        } else {
                                            gpui::transparent_black()
                                        },
                                    )
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.switching_input = true;
                                            this.drv_focus = DriversFocus::Editor;
                                            this.drv_editor_field =
                                                DriverEditorField::RequiresPreview;
                                            cx.notify();
                                        }),
                                    )
                                    .child(self.drv_requires_preview_dropdown.clone()),
                            )
                            .child(MonoCaption::new(override_default_caption(
                                &bool_override_caption(global.dangerous_requires_preview),
                            ))),
                    ),
            )
    }

    fn render_driver_schema(
        &self,
        entry: &DriverSettingsEntry,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let Some(schema) = &entry.settings_schema else {
            return div()
                .flex()
                .flex_col()
                .gap_2()
                .child(FieldLabel::new(dbflux_i18n::t!(
                    "settings.drivers.field.driver_settings"
                )))
                .child(
                    Body::new(dbflux_i18n::t!("settings.drivers.no_custom_settings"))
                        .color(cx.theme().muted_foreground),
                );
        };

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(FieldLabel::new(dbflux_i18n::t!(
                "settings.drivers.field.driver_settings"
            )))
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
                            .child(SubSectionLabel::new(section.title.to_uppercase()))
                            .children(section.fields.iter().filter_map(|field| {
                                let enabled = form_renderer::is_field_enabled(
                                    field,
                                    &self.drv_form_state.checkboxes,
                                    &form_renderer::select_values(&self.drv_form_state, cx),
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
                                                .rounded(Radii::SM)
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
                                                            this.drv_editor_dirty = true;
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
                                                .child(Label::new(field.label.clone()))
                                                .child(
                                                    div()
                                                        .w(Widths::CM_FORM_DROPDOWN)
                                                        .child(dropdown),
                                                )
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
                                                .child(Label::new(field.label.clone()))
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

#[cfg(test)]
mod tests {
    use super::{
        CAPABILITY_IDS, bool_override_caption, capability_catalog, driver_entry_key_text,
        driver_entry_name_text, policy_label,
    };
    use dbflux_components::tokens::FontSizes;
    use dbflux_components::typography::{AppFonts, MonoColorSelection, MonoDefaultColor};
    use dbflux_core::{DriverCapabilities, RefreshPolicySetting};

    const CHROME_KEYS: &[&str] = &[
        "settings.drivers.section_title",
        "settings.drivers.section_description",
        "settings.drivers.empty",
        "settings.drivers.select_hint",
        "settings.drivers.action.save",
        "settings.drivers.field.capabilities",
        "settings.drivers.field.global_overrides",
        "settings.drivers.global_overrides_hint",
        "settings.drivers.no_custom_settings",
        "settings.drivers.field.driver_settings",
        "settings.drivers.use_global",
        "settings.drivers.error.refresh_interval_empty",
        "settings.drivers.error.refresh_interval_invalid",
        "settings.drivers.toast.saved",
        "settings.drivers.toast.saved_warnings",
    ];

    #[test]
    fn drivers_chrome_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in CHROME_KEYS {
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
    fn drivers_section_description_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.drivers.section_description", locale = "en");
        let spanish = dbflux_i18n::t!("settings.drivers.section_description", locale = "es");

        assert_eq!(
            english,
            "Configure per-driver overrides and driver-defined settings"
        );
        assert_eq!(
            spanish,
            "Configura anulaciones por driver y ajustes definidos por el driver"
        );
        assert_ne!(english, spanish);
    }

    #[test]
    fn capability_names_cover_every_flag() {
        for &(_, id) in CAPABILITY_IDS {
            for locale in ["en", "es"] {
                let key = format!("settings.drivers.capability.{id}");
                let value = dbflux_i18n::t!(&key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn capability_catalog_flags_unchanged() {
        let catalog = capability_catalog();
        let actual_flags: Vec<DriverCapabilities> =
            catalog.iter().map(|(capability, _)| *capability).collect();
        let expected_flags: Vec<DriverCapabilities> = CAPABILITY_IDS
            .iter()
            .map(|&(capability, _)| capability)
            .collect();

        assert_eq!(actual_flags, expected_flags);
    }

    #[test]
    fn capability_name_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.drivers.capability.ssh_tunnel", locale = "en");
        let spanish = dbflux_i18n::t!("settings.drivers.capability.ssh_tunnel", locale = "es");

        assert_eq!(english, "SSH Tunnel");
        assert_eq!(spanish, "Túnel SSH");
        assert_ne!(english, spanish);
    }

    #[test]
    fn policy_label_differs_between_locales() {
        assert_ne!(
            dbflux_i18n::t!(
                "settings.general.refresh_policy.option.interval",
                locale = "en"
            ),
            dbflux_i18n::t!(
                "settings.general.refresh_policy.option.interval",
                locale = "es"
            )
        );
        assert!(!policy_label(RefreshPolicySetting::Manual).is_empty());
    }

    #[test]
    fn bool_override_caption_resolves_on_and_off() {
        assert!(!bool_override_caption(true).is_empty());
        assert!(!bool_override_caption(false).is_empty());
        assert_ne!(bool_override_caption(true), bool_override_caption(false));
    }

    #[test]
    fn driver_list_preserves_prominent_names_and_deemphasized_keys() {
        let name = driver_entry_name_text("PostgreSQL").inspect();
        let key = driver_entry_key_text("postgres").inspect();

        assert_eq!(name.family, Some(AppFonts::MONO));
        assert_eq!(name.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(name.size_override, Some(FontSizes::BASE));
        assert_eq!(name.weight_override, None);
        assert_eq!(
            name.color_selection,
            MonoColorSelection::RoleDefault(MonoDefaultColor::Foreground)
        );
        assert!(name.uses_role_default_color);
        assert!(!name.has_custom_color_override);

        assert_eq!(key.family, Some(AppFonts::MONO));
        assert_eq!(key.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(key.size_override, Some(FontSizes::SM));
        assert_eq!(key.weight_override, None);
        assert_eq!(key.color_selection, MonoColorSelection::MutedForeground);
        assert!(key.uses_muted_foreground_override);
        assert!(!key.has_custom_color_override);
    }
}
