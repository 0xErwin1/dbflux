use super::SettingsSection;
use super::SettingsSectionId;
use super::layout;
use super::section_trait::SectionFocusEvent;
use crate::app::AppStateEntity;
use crate::keymap::{Modifiers, key_chord_from_gpui};
use crate::ui::components::toast::ToastExt;
use dbflux_components::primitives::Text;
use dbflux_storage::repositories::audit_settings::AuditSettingsDto;
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputEvent, InputState};
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

        let subscription = cx.subscribe(
            &app_state,
            |this, _, _: &crate::app::AppStateChanged, cx| {
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

        Self {
            app_state,
            settings,
            original_settings,
            audit_form_cursor: 0,
            audit_editing_field: false,
            input_retention_days,
            input_max_detail_bytes,
            input_background_purge_interval,
            content_focused: false,
            switching_input: false,
            event_count: None,
            pending_save_result: None,
            _subscriptions: vec![
                subscription,
                blur_retention,
                blur_max_detail,
                blur_purge_interval,
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
    }

    pub(super) fn save_audit_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let retention_str = self
            .input_retention_days
            .read(cx)
            .value()
            .trim()
            .to_string();
        let retention_days = match retention_str.parse::<u32>() {
            Ok(value) if value >= 1 => value,
            _ => {
                cx.toast_error("Retention days must be a number >= 1", window);
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
                cx.toast_error("Max detail bytes must be >= 1024", window);
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
                cx.toast_error("Background purge interval must be a number", window);
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
            cx.toast_error(
                "Audit cannot be enabled: the audit database could not be opened. \
                 Please restart the application. If the problem persists, check disk space \
                 and file permissions for the dbflux data directory.",
                window,
            );
            // Revert to disabled in-memory only; do NOT write — user must uncheck
            // the enabled checkbox and save again to persist a disabled state.
            self.settings.enabled = false;
            return;
        }

        if let Err(e) = repo.upsert(&self.settings) {
            cx.toast_error(format!("Failed to save: {}", e), window);
            return;
        }

        let audit_service = app_state.audit_service();
        audit_service.set_enabled(self.settings.enabled);
        audit_service.set_redact_sensitive(self.settings.redact_sensitive_values);
        audit_service.set_capture_query_text(self.settings.capture_query_text);
        audit_service.set_max_detail_bytes(self.settings.max_detail_bytes);

        self.original_settings = self.settings.clone();

        cx.toast_success("Audit settings saved.", window);
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
            .child(layout::section_header(
                "Audit",
                "Configure global audit event capture and retention",
                theme,
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
                    .child(self.render_audit_group_header("Status", border, muted_fg))
                    .child(self.render_audit_status_indicator(cx))
                    .child(self.render_audit_group_header("Enable/Disable", border, muted_fg))
                    .child(self.render_audit_checkbox(
                        "audit-enabled",
                        "Enable global audit",
                        self.settings.enabled,
                        is_at(AuditFormRow::EnableAudit),
                        AuditFormRow::EnableAudit,
                        |this, value| this.settings.enabled = value,
                        cx,
                    ))
                    .child(self.render_audit_group_header("Capture Settings", border, muted_fg))
                    .child(self.render_audit_unsupported_checkbox(
                        "capture-user-actions",
                        "Capture user actions",
                        self.settings.capture_user_actions,
                        is_at(AuditFormRow::CaptureUserActions),
                        cx,
                    ))
                    .child(self.render_audit_unsupported_checkbox(
                        "capture-system-events",
                        "Capture system events",
                        self.settings.capture_system_events,
                        is_at(AuditFormRow::CaptureSystemEvents),
                        cx,
                    ))
                    .child(self.render_audit_checkbox(
                        "capture-query-text",
                        "Capture full query text (disable for fingerprints only)",
                        self.settings.capture_query_text,
                        is_at(AuditFormRow::CaptureQueryText),
                        AuditFormRow::CaptureQueryText,
                        |this, value| this.settings.capture_query_text = value,
                        cx,
                    ))
                    .child(self.render_audit_unsupported_checkbox(
                        "capture-hook-output",
                        "Capture hook/script output metadata",
                        self.settings.capture_hook_output_metadata,
                        is_at(AuditFormRow::CaptureHookOutputMetadata),
                        cx,
                    ))
                    .child(self.render_audit_group_header("Privacy", border, muted_fg))
                    .child(self.render_audit_checkbox(
                        "redact-sensitive",
                        "Redact sensitive values (passwords, tokens, keys)",
                        self.settings.redact_sensitive_values,
                        is_at(AuditFormRow::RedactSensitiveValues),
                        AuditFormRow::RedactSensitiveValues,
                        |this, value| this.settings.redact_sensitive_values = value,
                        cx,
                    ))
                    .child(self.render_audit_group_header("Retention", border, muted_fg))
                    .child(self.render_audit_input_field(
                        "Retention days",
                        &self.input_retention_days,
                        is_at(AuditFormRow::RetentionDays),
                        primary,
                        AuditFormRow::RetentionDays,
                        cx,
                    ))
                    .child(self.render_audit_input_field(
                        "Max detail bytes",
                        &self.input_max_detail_bytes,
                        is_at(AuditFormRow::MaxDetailBytes),
                        primary,
                        AuditFormRow::MaxDetailBytes,
                        cx,
                    ))
                    .child(self.render_audit_group_header("Purge", border, muted_fg))
                    .child(self.render_audit_checkbox(
                        "purge-on-startup",
                        "Purge old events on startup",
                        self.settings.purge_on_startup,
                        is_at(AuditFormRow::PurgeOnStartup),
                        AuditFormRow::PurgeOnStartup,
                        |this, value| this.settings.purge_on_startup = value,
                        cx,
                    ))
                    .child(self.render_audit_input_field(
                        "Background purge interval (minutes)",
                        &self.input_background_purge_interval,
                        is_at(AuditFormRow::BackgroundPurgeInterval),
                        primary,
                        AuditFormRow::BackgroundPurgeInterval,
                        cx,
                    )),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .p_4()
                    .border_t_1()
                    .border_color(border)
                    .flex()
                    .justify_end()
                    .child({
                        let is_save_focused = is_at(AuditFormRow::SaveButton);

                        div()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_save_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .child(
                                Button::new("save-audit")
                                    .label("Save")
                                    .small()
                                    .primary()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.content_focused = true;
                                        this.audit_form_cursor = this
                                            .audit_form_rows()
                                            .iter()
                                            .position(|row| *row == AuditFormRow::SaveButton)
                                            .unwrap_or_default();
                                        this.save_audit_settings(window, cx);
                                    })),
                            )
                    }),
            )
    }

    fn render_audit_group_header(
        &self,
        label: &str,
        border: Hsla,
        _muted_fg: Hsla,
    ) -> impl IntoElement {
        div().pt_2().pb_1().border_b_1().border_color(border).child(
            Text::body(label.to_string())
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(_muted_fg),
        )
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
            .rounded(px(4.0))
            .border_1()
            .border_color(gpui::transparent_black())
            .child(div().size_2().rounded_full().bg(if is_enabled {
                theme.success
            } else {
                theme.muted_foreground
            }))
            .child(div().text_sm().child(if is_degraded {
                "Audit is degraded (restart required)"
            } else if is_enabled {
                "Audit is enabled"
            } else {
                "Audit is disabled"
            }))
    }

    #[allow(clippy::too_many_arguments)]
    fn render_audit_checkbox(
        &self,
        id: &'static str,
        label: &'static str,
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
            .rounded(px(4.0))
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
            .child(div().text_sm().child(label))
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
        label: &'static str,
        checked: bool,
        is_focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let muted_fg = theme.muted_foreground;

        // Row is non-interactive: no cursor movement on activation,
        // checkbox cannot be toggled. Only visual focus state is shown.
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
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.content_focused = true;
                    cx.notify();
                }),
            )
            .child(Checkbox::new(id).checked(checked))
            .child(div().text_sm().text_color(muted_fg).child(label))
            .child(
                div()
                    .text_xs()
                    .italic()
                    .text_color(muted_fg.opacity(0.7))
                    .child("(not yet wired)"),
            )
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
        let theme = cx.theme();
        let muted_fg = theme.muted_foreground;

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(if unsupported {
                        muted_fg
                    } else {
                        theme.foreground
                    })
                    .child(label.to_string())
                    .when(unsupported, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .italic()
                                .text_color(muted_fg.opacity(0.7))
                                .child("(not yet wired)"),
                        )
                    }),
            )
            .child(
                div()
                    .w(px(200.0))
                    .rounded(px(4.0))
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
