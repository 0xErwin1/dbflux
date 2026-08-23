use super::SettingsSection;
use super::SettingsSectionId;
use super::section_trait::SectionFocusEvent;
use crate::labels::audit_save_failed_copy;
use crate::settings::layout;
use dbflux_app::keymap::Modifiers;
use dbflux_components::controls::{
    Dropdown, DropdownItem, DropdownSelectionChanged, GpuiInput as Input, InputEvent, InputState,
};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Radii;
use dbflux_components::typography::{FieldLabel, SubSectionLabel};
use dbflux_core::observability::EventSeverity;
use dbflux_storage::repositories::audit_settings::AuditSettingsDto;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use gpui::prelude::*;
use gpui::*;
use gpui_component::checkbox::Checkbox;
use gpui_component::scroll::ScrollableElement;
use gpui_component::{ActiveTheme, Sizable};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum AuditFormRow {
    StatusIndicator,
    EnableAudit,
    RetentionDays,
    CaptureUserActions,
    CaptureSystemEvents,
    CaptureQueryText,
    CaptureHookOutputMetadata,
    RedactSensitiveValues,
    MaxDetailBytes,
    PurgeOnStartup,
    BackgroundPurgeInterval,
    LogCaptureMinLevel,
    SaveButton,
}

#[allow(dead_code)]
pub(super) struct AuditSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) settings: AuditSettingsDto,
    pub(super) original_settings: AuditSettingsDto,
    pub(super) audit_form_cursor: usize,
    pub(super) audit_editing_field: bool,
    pub(super) input_retention_days: Entity<InputState>,
    pub(super) input_max_detail_bytes: Entity<InputState>,
    pub(super) input_background_purge_interval: Entity<InputState>,
    pub(super) dropdown_log_level: Entity<Dropdown>,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    pub(super) event_count: Option<u64>,
    pub(super) pending_save_result: Option<Result<(), String>>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for AuditSection {}

impl AuditSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let settings = Self::load_settings(app_state.clone(), cx);
        let original_settings = settings.clone();

        let retention_days = settings.retention_days.to_string();
        let max_detail_bytes = settings.max_detail_bytes.to_string();
        let background_purge_interval = settings.background_purge_interval_minutes.to_string();

        let input_retention_days = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("30")
                .default_value(retention_days.clone())
        });
        let input_max_detail_bytes = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("65536")
                .default_value(max_detail_bytes.clone())
        });
        let input_background_purge_interval = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("360")
                .default_value(background_purge_interval.clone())
        });

        let log_level_index = Self::log_level_index(&settings.log_capture_min_level);
        let dropdown_log_level = cx.new(move |_cx| {
            Dropdown::new("audit-log-capture-level")
                .placeholder(dbflux_i18n::t!("settings.audit.placeholder_level"))
                .items(Self::log_level_items())
                .selected_index(Some(log_level_index))
        });

        let subscription = cx.subscribe(
            &app_state,
            |this, _, _: &dbflux_ui_base::AppStateChanged, cx| {
                this.content_focused = false;
                this.audit_editing_field = false;
                cx.notify();
            },
        );

        let blur_retention =
            cx.subscribe(&input_retention_days, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        let blur_max_detail = cx.subscribe(
            &input_max_detail_bytes,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        let blur_purge_interval = cx.subscribe(
            &input_background_purge_interval,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        let log_level_subscription = cx.subscribe(
            &dropdown_log_level,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.settings.log_capture_min_level =
                    Self::log_level_for_index(event.index).to_owned();
                cx.notify();
            },
        );

        Self {
            app_state,
            settings,
            original_settings,
            audit_form_cursor: 0,
            audit_editing_field: false,
            input_retention_days,
            input_max_detail_bytes,
            input_background_purge_interval,
            dropdown_log_level,
            content_focused: false,
            switching_input: false,
            event_count: None,
            pending_save_result: None,
            _subscriptions: vec![
                subscription,
                blur_retention,
                blur_max_detail,
                blur_purge_interval,
                log_level_subscription,
            ],
        }
    }

    fn load_settings(
        app_state: Entity<AppStateEntity>,
        cx: &mut Context<Self>,
    ) -> AuditSettingsDto {
        let runtime = app_state.read(cx).storage_runtime();
        let repo = runtime.audit_settings();
        repo.get().ok().flatten().unwrap_or_default()
    }

    fn audit_form_rows(&self) -> Vec<AuditFormRow> {
        vec![
            AuditFormRow::StatusIndicator,
            AuditFormRow::EnableAudit,
            AuditFormRow::RetentionDays,
            AuditFormRow::CaptureUserActions,
            AuditFormRow::CaptureSystemEvents,
            AuditFormRow::CaptureQueryText,
            AuditFormRow::CaptureHookOutputMetadata,
            AuditFormRow::RedactSensitiveValues,
            AuditFormRow::MaxDetailBytes,
            AuditFormRow::PurgeOnStartup,
            AuditFormRow::BackgroundPurgeInterval,
            AuditFormRow::LogCaptureMinLevel,
            AuditFormRow::SaveButton,
        ]
    }

    fn audit_current_row(&self) -> Option<AuditFormRow> {
        self.audit_form_rows().get(self.audit_form_cursor).copied()
    }

    pub(super) fn audit_move_down(&mut self) {
        let count = self.audit_form_rows().len();
        if self.audit_form_cursor + 1 < count {
            self.audit_form_cursor += 1;
        }
    }

    pub(super) fn audit_move_up(&mut self) {
        if self.audit_form_cursor > 0 {
            self.audit_form_cursor -= 1;
        }
    }

    fn audit_move_first(&mut self) {
        self.audit_form_cursor = 0;
    }

    fn audit_move_last(&mut self) {
        self.audit_form_cursor = self.audit_form_rows().len().saturating_sub(1);
    }

    pub(super) fn audit_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.audit_current_row() {
            Some(AuditFormRow::EnableAudit) => {
                self.settings.enabled = !self.settings.enabled;
                cx.notify();
            }
            Some(AuditFormRow::RetentionDays) => {
                self.audit_focus_current_input(window, cx);
            }
            // capture_user_actions, capture_system_events, capture_hook_output_metadata
            // are stored but NOT yet wired to AuditService runtime behavior.
            // They are marked as non-interactive in render_audit_section.
            Some(AuditFormRow::CaptureUserActions)
            | Some(AuditFormRow::CaptureSystemEvents)
            | Some(AuditFormRow::CaptureHookOutputMetadata) => {}
            Some(AuditFormRow::CaptureQueryText) => {
                self.settings.capture_query_text = !self.settings.capture_query_text;
                cx.notify();
            }
            Some(AuditFormRow::RedactSensitiveValues) => {
                self.settings.redact_sensitive_values = !self.settings.redact_sensitive_values;
                cx.notify();
            }
            Some(AuditFormRow::MaxDetailBytes) => {
                self.audit_focus_current_input(window, cx);
            }
            Some(AuditFormRow::PurgeOnStartup) => {
                self.settings.purge_on_startup = !self.settings.purge_on_startup;
                cx.notify();
            }
            // background_purge_interval_minutes controls the periodic purge timer
            // in Workspace. The input is kept active so users can set it, but
            // the timer itself is controlled by Workspace's purge scheduling.
            Some(AuditFormRow::BackgroundPurgeInterval) => {
                self.audit_focus_current_input(window, cx);
            }
            Some(AuditFormRow::LogCaptureMinLevel) => {
                // Dropdown is self-contained; keyboard activation is a no-op here.
            }
            Some(AuditFormRow::SaveButton) => {
                self.save_audit_settings(window, cx);
            }
            Some(AuditFormRow::StatusIndicator) | None => {}
        }
    }

    fn audit_focus_current_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.audit_editing_field = true;

        match self.audit_current_row() {
            Some(AuditFormRow::RetentionDays) => {
                self.input_retention_days
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(AuditFormRow::MaxDetailBytes) => {
                self.input_max_detail_bytes
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(AuditFormRow::BackgroundPurgeInterval) => {
                self.input_background_purge_interval
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            _ => {
                self.audit_editing_field = false;
            }
        }
    }

    pub(super) fn has_unsaved_audit_changes(&self, _cx: &App) -> bool {
        self.settings.enabled != self.original_settings.enabled
            || self.settings.retention_days != self.original_settings.retention_days
            || self.settings.capture_user_actions != self.original_settings.capture_user_actions
            || self.settings.capture_system_events != self.original_settings.capture_system_events
            || self.settings.capture_query_text != self.original_settings.capture_query_text
            || self.settings.capture_hook_output_metadata
                != self.original_settings.capture_hook_output_metadata
            || self.settings.redact_sensitive_values
                != self.original_settings.redact_sensitive_values
            || self.settings.max_detail_bytes != self.original_settings.max_detail_bytes
            || self.settings.purge_on_startup != self.original_settings.purge_on_startup
            || self.settings.background_purge_interval_minutes
                != self.original_settings.background_purge_interval_minutes
            || self.settings.log_capture_min_level != self.original_settings.log_capture_min_level
    }

    pub(super) fn save_audit_settings(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let retention_str = self
            .input_retention_days
            .read(cx)
            .value()
            .trim()
            .to_string();
        let retention_days = match retention_str.parse::<u32>() {
            Ok(value) if value >= 1 => value,
            _ => {
                let msg = dbflux_i18n::t!("settings.audit.error.retention_days_invalid");
                Toast::error(msg.clone())
                    .meta_right(now_hms())
                    .action(copy_action(msg))
                    .push(cx);
                return;
            }
        };

        let max_detail_str = self
            .input_max_detail_bytes
            .read(cx)
            .value()
            .trim()
            .to_string();
        let max_detail_bytes = match max_detail_str.parse::<usize>() {
            Ok(value) if value >= 1024 => value,
            _ => {
                let msg = dbflux_i18n::t!("settings.audit.error.max_detail_bytes_invalid");
                Toast::error(msg.clone())
                    .meta_right(now_hms())
                    .action(copy_action(msg))
                    .push(cx);
                return;
            }
        };

        let purge_interval_str = self
            .input_background_purge_interval
            .read(cx)
            .value()
            .trim()
            .to_string();
        let purge_interval = match purge_interval_str.parse::<u32>() {
            Ok(value) => value,
            _ => {
                let msg = dbflux_i18n::t!("settings.audit.error.purge_interval_invalid");
                Toast::error(msg.clone())
                    .meta_right(now_hms())
                    .action(copy_action(msg))
                    .push(cx);
                return;
            }
        };

        self.settings.retention_days = retention_days;
        self.settings.max_detail_bytes = max_detail_bytes;
        self.settings.background_purge_interval_minutes = purge_interval;

        let app_state = self.app_state.read(cx);
        let runtime = app_state.storage_runtime();
        let repo = runtime.audit_settings();

        // Check degraded state BEFORE writing. If the audit service is in degraded state
        // (real DB could not be opened), do not allow enabling it. This avoids the
        // write-then-correct pattern that could leave bad persisted state on crash.
        if app_state.is_audit_degraded() && self.settings.enabled {
            Toast::error(dbflux_i18n::t!("settings.audit.error.cannot_enable"))
                .meta_right(now_hms())
                .body(dbflux_i18n::t!("settings.audit.error.cannot_enable_body"))
                .action(copy_action(dbflux_i18n::t!(
                    "settings.audit.error.cannot_enable_copy"
                )))
                .push(cx);
            // Revert to disabled in-memory only; do NOT write — user must uncheck
            // the enabled checkbox and save again to persist a disabled state.
            self.settings.enabled = false;
            return;
        }

        if let Err(e) = repo.upsert(&self.settings) {
            let body = e.to_string();
            Toast::error(dbflux_i18n::t!("settings.audit.error.save_failed"))
                .meta_right(now_hms())
                .body(body.clone())
                .action(copy_action(audit_save_failed_copy(&body)))
                .push(cx);
            return;
        }

        let audit_service = app_state.audit_service();
        audit_service.set_enabled(self.settings.enabled);
        audit_service.set_redact_sensitive(self.settings.redact_sensitive_values);
        audit_service.set_capture_query_text(self.settings.capture_query_text);
        audit_service.set_max_detail_bytes(self.settings.max_detail_bytes);

        if let Some(level) = EventSeverity::from_str_repr(&self.settings.log_capture_min_level)
            && let Err(e) = audit_service.set_log_capture_min_level(level)
        {
            log::warn!("Failed to apply log capture min level: {e}");
        }

        self.original_settings = self.settings.clone();

        Toast::success(dbflux_i18n::t!("settings.audit.toast.saved"))
            .meta_right(now_hms())
            .push(cx);
    }
}

impl SettingsSection for AuditSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Audit
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.audit_editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_audit_changes(cx)
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_audit_footer_actions(cx))
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let chord = key_chord_from_gpui(&event.keystroke);

        if self.audit_editing_field {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.audit_editing_field = false;
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                    cx.notify();
                }
                ("enter", modifiers) if modifiers == Modifiers::none() => {
                    self.audit_editing_field = false;
                    self.audit_move_down();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.audit_editing_field = false;
                    self.audit_move_down();
                    self.audit_focus_current_input(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.audit_editing_field = false;
                    self.audit_move_up();
                    self.audit_focus_current_input(window, cx);
                    cx.notify();
                }
                _ => {}
            }

            return;
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                self.audit_move_down();
                cx.notify();
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                self.audit_move_up();
                cx.notify();
            }
            ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                if modifiers == Modifiers::none() =>
            {
                self.audit_activate_current_field(window, cx);
            }
            ("tab", modifiers) if modifiers == Modifiers::none() => {
                self.audit_move_down();
                cx.notify();
            }
            ("tab", modifiers) if modifiers == Modifiers::shift() => {
                self.audit_move_up();
                cx.notify();
            }
            ("g", modifiers) if modifiers == Modifiers::none() => {
                self.audit_move_first();
                cx.notify();
            }
            ("G", modifiers) if modifiers == Modifiers::none() => {
                self.audit_move_last();
                cx.notify();
            }
            _ => {}
        }
    }
}

impl Render for AuditSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_audit_section(cx)
    }
}

impl AuditSection {
    pub(super) fn render_audit_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;
        let muted_fg = theme.muted_foreground;
        let is_focused = self.content_focused;
        let cursor = self.audit_form_cursor;
        let rows = self.audit_form_rows();

        let is_at =
            |row: AuditFormRow| -> bool { is_focused && rows.get(cursor).copied() == Some(row) };

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.audit.section_title"),
                dbflux_i18n::t!("settings.audit.section_description"),
                cx,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_5()
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.status"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_status_indicator(cx))
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.enable_disable"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_checkbox(
                        "audit-enabled",
                        dbflux_i18n::t!("settings.audit.field.enable_global"),
                        self.settings.enabled,
                        is_at(AuditFormRow::EnableAudit),
                        AuditFormRow::EnableAudit,
                        |this, value| this.settings.enabled = value,
                        cx,
                    ))
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.capture_settings"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_unsupported_checkbox(
                        "capture-user-actions",
                        dbflux_i18n::t!("settings.audit.field.capture_user_actions"),
                        self.settings.capture_user_actions,
                        is_at(AuditFormRow::CaptureUserActions),
                        cx,
                    ))
                    .child(self.render_audit_unsupported_checkbox(
                        "capture-system-events",
                        dbflux_i18n::t!("settings.audit.field.capture_system_events"),
                        self.settings.capture_system_events,
                        is_at(AuditFormRow::CaptureSystemEvents),
                        cx,
                    ))
                    .child(self.render_audit_checkbox(
                        "capture-query-text",
                        dbflux_i18n::t!("settings.audit.field.capture_full_query_text"),
                        self.settings.capture_query_text,
                        is_at(AuditFormRow::CaptureQueryText),
                        AuditFormRow::CaptureQueryText,
                        |this, value| this.settings.capture_query_text = value,
                        cx,
                    ))
                    .child(self.render_audit_unsupported_checkbox(
                        "capture-hook-output",
                        dbflux_i18n::t!("settings.audit.field.capture_hook_output"),
                        self.settings.capture_hook_output_metadata,
                        is_at(AuditFormRow::CaptureHookOutputMetadata),
                        cx,
                    ))
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.privacy"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_checkbox(
                        "redact-sensitive",
                        dbflux_i18n::t!("settings.audit.field.redact_sensitive"),
                        self.settings.redact_sensitive_values,
                        is_at(AuditFormRow::RedactSensitiveValues),
                        AuditFormRow::RedactSensitiveValues,
                        |this, value| this.settings.redact_sensitive_values = value,
                        cx,
                    ))
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.retention"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_input_field(
                        &dbflux_i18n::t!("settings.audit.field.retention_days"),
                        &self.input_retention_days,
                        is_at(AuditFormRow::RetentionDays),
                        primary,
                        AuditFormRow::RetentionDays,
                        cx,
                    ))
                    .child(self.render_audit_input_field(
                        &dbflux_i18n::t!("settings.audit.field.max_detail_bytes"),
                        &self.input_max_detail_bytes,
                        is_at(AuditFormRow::MaxDetailBytes),
                        primary,
                        AuditFormRow::MaxDetailBytes,
                        cx,
                    ))
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.purge"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_checkbox(
                        "purge-on-startup",
                        dbflux_i18n::t!("settings.audit.field.purge_on_startup"),
                        self.settings.purge_on_startup,
                        is_at(AuditFormRow::PurgeOnStartup),
                        AuditFormRow::PurgeOnStartup,
                        |this, value| this.settings.purge_on_startup = value,
                        cx,
                    ))
                    .child(self.render_audit_input_field(
                        &dbflux_i18n::t!("settings.audit.field.purge_interval_minutes"),
                        &self.input_background_purge_interval,
                        is_at(AuditFormRow::BackgroundPurgeInterval),
                        primary,
                        AuditFormRow::BackgroundPurgeInterval,
                        cx,
                    ))
                    .child(self.render_audit_group_header(
                        dbflux_i18n::t!("settings.audit.group.log_capture"),
                        border,
                        muted_fg,
                    ))
                    .child(self.render_audit_dropdown(
                        &dbflux_i18n::t!("settings.audit.field.min_log_level"),
                        &self.dropdown_log_level,
                        is_at(AuditFormRow::LogCaptureMinLevel),
                        primary,
                        AuditFormRow::LogCaptureMinLevel,
                        cx,
                    )),
            )
    }

    fn render_audit_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_save_focused = self.content_focused
            && self.audit_form_rows().get(self.audit_form_cursor).copied()
                == Some(AuditFormRow::SaveButton);

        div()
            .flex()
            .items_center()
            .gap_3()
            .child(layout::footer_action_frame(
                is_save_focused,
                cx.theme().primary,
                dbflux_components::controls::Button::new(
                    "save-audit",
                    dbflux_i18n::t!("settings.audit.action.save"),
                )
                .small()
                .primary()
                .w_full()
                .on_click(cx.listener(|this, _, window, cx| {
                    this.content_focused = true;
                    this.audit_form_cursor = this
                        .audit_form_rows()
                        .iter()
                        .position(|row| *row == AuditFormRow::SaveButton)
                        .unwrap_or_default();
                    this.save_audit_settings(window, cx);
                })),
            ))
            .into_any_element()
    }

    fn log_level_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new("trace"),
            DropdownItem::new("debug"),
            DropdownItem::new("info"),
            DropdownItem::new("warn"),
            DropdownItem::new("error"),
        ]
    }

    fn log_level_index(level: &str) -> usize {
        match level {
            "trace" => 0,
            "debug" => 1,
            "info" => 2,
            "warn" => 3,
            "error" | "fatal" => 4,
            _ => 2,
        }
    }

    fn log_level_for_index(index: usize) -> &'static str {
        match index {
            0 => "trace",
            1 => "debug",
            2 => "info",
            3 => "warn",
            4 => "error",
            _ => "info",
        }
    }

    fn render_audit_dropdown(
        &self,
        label: &str,
        dropdown: &Entity<Dropdown>,
        is_focused: bool,
        primary: Hsla,
        row: AuditFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .justify_between()
            .px_2()
            .py_1()
            .rounded(Radii::SM)
            .border_1()
            .border_color(if is_focused {
                primary
            } else {
                gpui::transparent_black()
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.content_focused = true;
                    if let Some(position) = this
                        .audit_form_rows()
                        .iter()
                        .position(|candidate| *candidate == row)
                    {
                        this.audit_form_cursor = position;
                    }
                    cx.notify();
                }),
            )
            .child(FieldLabel::new(label.to_string()))
            .child(div().min_w(px(120.0)).child(dropdown.clone()))
    }

    fn render_audit_group_header(
        &self,
        label: impl Into<SharedString>,
        border: Hsla,
        _muted_fg: Hsla,
    ) -> impl IntoElement {
        div()
            .pt_2()
            .pb_1()
            .border_b_1()
            .border_color(border)
            .child(SubSectionLabel::new(label))
    }

    fn render_audit_status_indicator(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_degraded = self.app_state.read(cx).is_audit_degraded();
        // When degraded, the service is disabled regardless of the persisted setting.
        // Show this honestly so the user understands why no events appear.
        let is_enabled = !is_degraded && self.settings.enabled;

        div()
            .flex()
            .items_center()
            .gap_2()
            .px_2()
            .py_1()
            .rounded(Radii::SM)
            .border_1()
            .border_color(gpui::transparent_black())
            .child(div().size_2().rounded_full().bg(if is_enabled {
                theme.success
            } else {
                theme.muted_foreground
            }))
            .child(div().text_sm().child(if is_degraded {
                dbflux_i18n::t!("settings.audit.status.degraded")
            } else if is_enabled {
                dbflux_i18n::t!("settings.audit.status.enabled")
            } else {
                dbflux_i18n::t!("settings.audit.status.disabled")
            }))
    }

    #[allow(clippy::too_many_arguments)]
    fn render_audit_checkbox(
        &self,
        id: &'static str,
        label: impl Into<SharedString>,
        checked: bool,
        is_focused: bool,
        row: AuditFormRow,
        setter: fn(&mut Self, bool),
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let primary = cx.theme().primary;

        div()
            .flex()
            .items_center()
            .gap_2()
            .px_2()
            .py_1()
            .rounded(Radii::SM)
            .border_1()
            .border_color(if is_focused {
                primary
            } else {
                gpui::transparent_black()
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.content_focused = true;
                    if let Some(position) = this
                        .audit_form_rows()
                        .iter()
                        .position(|candidate| *candidate == row)
                    {
                        this.audit_form_cursor = position;
                    }
                    cx.notify();
                }),
            )
            .child(Checkbox::new(id).checked(checked).on_click(cx.listener(
                move |this, value: &bool, _, cx| {
                    setter(this, *value);
                    cx.notify();
                },
            )))
            .child(div().text_sm().child(label.into()))
    }

    fn render_audit_input_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        row: AuditFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        self.render_audit_input_field_impl(label, input, is_focused, primary, row, cx, false)
    }

    fn render_audit_unsupported_checkbox(
        &self,
        id: &'static str,
        label: impl Into<SharedString>,
        checked: bool,
        is_focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        // Row is non-interactive: no cursor movement on activation,
        // checkbox cannot be toggled. Only visual focus state is shown.
        div()
            .flex()
            .items_center()
            .gap_2()
            .px_2()
            .py_1()
            .rounded(Radii::SM)
            .border_1()
            .border_color(if is_focused {
                primary
            } else {
                gpui::transparent_black()
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.content_focused = true;
                    cx.notify();
                }),
            )
            .child(Checkbox::new(id).checked(checked))
            .child(Text::muted(label))
            .child(div().italic().child(Text::dim_secondary(dbflux_i18n::t!(
                "settings.audit.field.not_wired"
            ))))
    }

    /// Internal implementation for input fields; `unsupported` dims the label
    /// and removes the on_mouse_down focus/input-switching behavior.
    #[allow(clippy::too_many_arguments)]
    fn render_audit_input_field_impl(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        row: AuditFormRow,
        cx: &mut Context<Self>,
        unsupported: bool,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(if unsupported {
                        Text::label_sm(label.to_string()).muted_foreground()
                    } else {
                        Text::label_sm(label.to_string())
                    })
                    .when(unsupported, |this| {
                        this.child(div().italic().child(Text::dim_secondary(dbflux_i18n::t!(
                            "settings.audit.field.not_wired"
                        ))))
                    }),
            )
            .child(
                div()
                    .w(px(200.0))
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(if is_focused {
                        primary
                    } else {
                        gpui::transparent_black()
                    })
                    .when(!unsupported, |this| {
                        this.on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                this.switching_input = true;
                                this.content_focused = true;
                                if let Some(position) = this
                                    .audit_form_rows()
                                    .iter()
                                    .position(|candidate| *candidate == row)
                                {
                                    this.audit_form_cursor = position;
                                }
                                this.audit_focus_current_input(window, cx);
                                cx.notify();
                            }),
                        )
                    })
                    .child(Input::new(input).small().disabled(unsupported)),
            )
    }
}

#[cfg(test)]
mod tests {
    const AUDIT_SECTION_KEYS: &[&str] = &[
        "settings.audit.placeholder_level",
        "settings.audit.section_title",
        "settings.audit.section_description",
        "settings.audit.group.status",
        "settings.audit.group.enable_disable",
        "settings.audit.group.capture_settings",
        "settings.audit.group.privacy",
        "settings.audit.group.retention",
        "settings.audit.group.purge",
        "settings.audit.group.log_capture",
        "settings.audit.field.enable_global",
        "settings.audit.field.capture_user_actions",
        "settings.audit.field.capture_system_events",
        "settings.audit.field.capture_full_query_text",
        "settings.audit.field.capture_hook_output",
        "settings.audit.field.redact_sensitive",
        "settings.audit.field.retention_days",
        "settings.audit.field.max_detail_bytes",
        "settings.audit.field.purge_on_startup",
        "settings.audit.field.purge_interval_minutes",
        "settings.audit.field.min_log_level",
        "settings.audit.field.not_wired",
        "settings.audit.action.save",
        "settings.audit.status.degraded",
        "settings.audit.status.enabled",
        "settings.audit.status.disabled",
        "settings.audit.error.retention_days_invalid",
        "settings.audit.error.max_detail_bytes_invalid",
        "settings.audit.error.purge_interval_invalid",
        "settings.audit.error.cannot_enable",
        "settings.audit.error.cannot_enable_body",
        "settings.audit.error.cannot_enable_copy",
        "settings.audit.error.save_failed",
        "settings.audit.error.save_failed_copy",
        "settings.audit.toast.saved",
    ];

    #[test]
    fn audit_settings_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in AUDIT_SECTION_KEYS {
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
    fn audit_section_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.audit.section_title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.audit.section_title", locale = "es");

        assert_eq!(english, "Audit");
        assert_eq!(spanish, "Auditoría");
        assert_ne!(english, spanish);
    }

    #[test]
    fn audit_status_labels_differ_between_locales() {
        let degraded_en = dbflux_i18n::t!("settings.audit.status.degraded", locale = "en");
        let degraded_es = dbflux_i18n::t!("settings.audit.status.degraded", locale = "es");
        let enabled_en = dbflux_i18n::t!("settings.audit.status.enabled", locale = "en");
        let enabled_es = dbflux_i18n::t!("settings.audit.status.enabled", locale = "es");
        let disabled_en = dbflux_i18n::t!("settings.audit.status.disabled", locale = "en");
        let disabled_es = dbflux_i18n::t!("settings.audit.status.disabled", locale = "es");

        assert_eq!(degraded_en, "Audit is degraded (restart required)");
        assert_eq!(enabled_en, "Audit is enabled");
        assert_eq!(disabled_en, "Audit is disabled");
        assert_ne!(degraded_en, degraded_es);
        assert_ne!(enabled_en, enabled_es);
        assert_ne!(disabled_en, disabled_es);
    }

    #[test]
    fn audit_group_headers_differ_between_locales() {
        let group_keys = [
            "settings.audit.group.status",
            "settings.audit.group.enable_disable",
            "settings.audit.group.capture_settings",
            "settings.audit.group.privacy",
            "settings.audit.group.retention",
            "settings.audit.group.purge",
            "settings.audit.group.log_capture",
        ];

        for key in group_keys {
            let english = dbflux_i18n::t!(key, locale = "en");
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert_ne!(english, spanish, "group header {key} did not diverge");
        }
    }
}
