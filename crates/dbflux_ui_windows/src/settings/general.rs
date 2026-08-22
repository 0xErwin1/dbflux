use dbflux_app::keymap::{KeyChord, Modifiers};
use dbflux_components::controls::Button as FluxButton;
use dbflux_components::controls::Dropdown;
use dbflux_components::controls::{GpuiInput as Input, InputState};
use dbflux_components::tokens::Radii;
use dbflux_components::typography::{Body, FieldLabel, SubSectionLabel};
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::checkbox::Checkbox;

use super::general_section::{GeneralFormRow, GeneralSection};
use super::layout;
use super::section_trait::SectionFocusEvent;

impl GeneralSection {
    pub(super) fn has_unsaved_general_changes(&self, cx: &App) -> bool {
        let saved = self.app_state.read(cx).general_settings();

        if self.gen_settings.theme != saved.theme
            || self.gen_settings.style != saved.style
            || self.gen_settings.restore_session_on_startup != saved.restore_session_on_startup
            || self.gen_settings.reopen_last_connections != saved.reopen_last_connections
            || self.gen_settings.default_focus_on_startup != saved.default_focus_on_startup
            || self.gen_settings.default_refresh_policy != saved.default_refresh_policy
            || self.gen_settings.auto_refresh_pause_on_error != saved.auto_refresh_pause_on_error
            || self.gen_settings.auto_refresh_only_if_visible != saved.auto_refresh_only_if_visible
            || self.gen_settings.confirm_dangerous_queries != saved.confirm_dangerous_queries
            || self.gen_settings.dangerous_requires_where != saved.dangerous_requires_where
            || self.gen_settings.dangerous_requires_preview != saved.dangerous_requires_preview
        {
            return true;
        }

        if self.input_max_history.read(cx).value().trim() != saved.max_history_entries.to_string() {
            return true;
        }

        if self.input_auto_save.read(cx).value().trim() != saved.auto_save_interval_ms.to_string() {
            return true;
        }

        if self.input_refresh_interval.read(cx).value().trim()
            != saved.default_refresh_interval_secs.to_string()
        {
            return true;
        }

        if self.input_max_bg_tasks.read(cx).value().trim()
            != saved.max_concurrent_background_tasks.to_string()
        {
            return true;
        }

        self.input_object_preview_limit.read(cx).value().trim()
            != saved.object_preview_size_limit_mib.to_string()
    }

    pub(super) fn gen_form_rows(&self) -> Vec<GeneralFormRow> {
        let mut rows = vec![
            GeneralFormRow::Theme,
            GeneralFormRow::Style,
            GeneralFormRow::Language,
            GeneralFormRow::RestoreSession,
            GeneralFormRow::ReopenConnections,
            GeneralFormRow::DefaultFocus,
            GeneralFormRow::MaxHistory,
            GeneralFormRow::AutoSaveInterval,
            GeneralFormRow::DefaultRefreshPolicy,
            GeneralFormRow::DefaultRefreshInterval,
            GeneralFormRow::MaxBackgroundTasks,
            GeneralFormRow::PauseRefreshOnError,
            GeneralFormRow::RefreshOnlyIfVisible,
            GeneralFormRow::ConfirmDangerous,
            GeneralFormRow::RequiresWhere,
            GeneralFormRow::RequiresPreview,
            GeneralFormRow::ObjectPreviewLimit,
        ];

        // The shared-database toggle only makes sense on nightly, which is the
        // only channel that uses a separate database by default.
        if Self::is_nightly() {
            rows.push(GeneralFormRow::ShareStableDb);
        }

        rows.push(GeneralFormRow::SaveButton);
        rows
    }

    fn is_nightly() -> bool {
        dbflux_core::ReleaseChannel::current() == dbflux_core::ReleaseChannel::Nightly
    }

    /// Toggles whether this nightly build shares the stable database. The change
    /// is persisted to the pre-database marker immediately and applies on the
    /// next launch; a write failure is surfaced to the user and leaves the toggle
    /// unchanged.
    fn set_share_stable_db(&mut self, value: bool, cx: &mut Context<Self>) {
        match dbflux_storage::paths::set_nightly_shares_stable_db(value) {
            Ok(()) => self.gen_share_stable_db = value,
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Config,
                        dbflux_i18n::t!("settings.general.share_stable_db.error"),
                    )
                    .with_cause(format!("{error}")),
                    cx,
                );
            }
        }
    }

    fn gen_current_row(&self) -> Option<GeneralFormRow> {
        self.gen_form_rows().get(self.gen_form_cursor).copied()
    }

    pub(super) fn gen_move_down(&mut self) {
        let count = self.gen_form_rows().len();
        if self.gen_form_cursor + 1 < count {
            self.gen_form_cursor += 1;
        }
    }

    pub(super) fn gen_move_up(&mut self) {
        if self.gen_form_cursor > 0 {
            self.gen_form_cursor -= 1;
        }
    }

    fn gen_move_first(&mut self) {
        self.gen_form_cursor = 0;
    }

    fn gen_move_last(&mut self) {
        self.gen_form_cursor = self.gen_form_rows().len().saturating_sub(1);
    }

    pub(super) fn gen_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.gen_current_row() {
            Some(GeneralFormRow::Theme) => {
                self.dropdown_theme
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::Style) => {
                self.dropdown_style
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::Language) => {
                self.dropdown_language
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::RestoreSession) => {
                self.gen_settings.restore_session_on_startup =
                    !self.gen_settings.restore_session_on_startup;
                cx.notify();
            }
            Some(GeneralFormRow::ReopenConnections) => {
                self.gen_settings.reopen_last_connections =
                    !self.gen_settings.reopen_last_connections;
                cx.notify();
            }
            Some(GeneralFormRow::DefaultFocus) => {
                self.dropdown_default_focus
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::DefaultRefreshPolicy) => {
                self.dropdown_refresh_policy
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::PauseRefreshOnError) => {
                self.gen_settings.auto_refresh_pause_on_error =
                    !self.gen_settings.auto_refresh_pause_on_error;
                cx.notify();
            }
            Some(GeneralFormRow::RefreshOnlyIfVisible) => {
                self.gen_settings.auto_refresh_only_if_visible =
                    !self.gen_settings.auto_refresh_only_if_visible;
                cx.notify();
            }
            Some(GeneralFormRow::ConfirmDangerous) => {
                self.gen_settings.confirm_dangerous_queries =
                    !self.gen_settings.confirm_dangerous_queries;
                cx.notify();
            }
            Some(GeneralFormRow::RequiresWhere) => {
                self.gen_settings.dangerous_requires_where =
                    !self.gen_settings.dangerous_requires_where;
                cx.notify();
            }
            Some(GeneralFormRow::RequiresPreview) => {
                self.gen_settings.dangerous_requires_preview =
                    !self.gen_settings.dangerous_requires_preview;
                cx.notify();
            }
            Some(GeneralFormRow::ShareStableDb) => {
                self.set_share_stable_db(!self.gen_share_stable_db, cx);
                cx.notify();
            }
            Some(GeneralFormRow::MaxHistory)
            | Some(GeneralFormRow::AutoSaveInterval)
            | Some(GeneralFormRow::DefaultRefreshInterval)
            | Some(GeneralFormRow::MaxBackgroundTasks)
            | Some(GeneralFormRow::ObjectPreviewLimit) => {
                self.gen_focus_current_input(window, cx);
            }
            Some(GeneralFormRow::SaveButton) => {
                self.save_general_settings(window, cx);
            }
            None => {}
        }
    }

    fn gen_focus_current_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.gen_editing_field = true;

        match self.gen_current_row() {
            Some(GeneralFormRow::MaxHistory) => {
                self.input_max_history
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::AutoSaveInterval) => {
                self.input_auto_save
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::DefaultRefreshInterval) => {
                self.input_refresh_interval
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::MaxBackgroundTasks) => {
                self.input_max_bg_tasks
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::ObjectPreviewLimit) => {
                self.input_object_preview_limit
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            _ => {
                self.gen_editing_field = false;
            }
        }
    }

    pub(super) fn close_open_dropdown(&mut self, cx: &mut Context<Self>) {
        if let Some(dropdown) = self.current_dropdown() {
            dropdown.update(cx, |dropdown, cx| {
                if dropdown.is_open() {
                    dropdown.close(cx);
                }
            });
        }
    }

    fn current_dropdown(&self) -> Option<&Entity<Dropdown>> {
        match self.gen_current_row() {
            Some(GeneralFormRow::Theme) => Some(&self.dropdown_theme),
            Some(GeneralFormRow::Style) => Some(&self.dropdown_style),
            Some(GeneralFormRow::Language) => Some(&self.dropdown_language),
            Some(GeneralFormRow::DefaultFocus) => Some(&self.dropdown_default_focus),
            Some(GeneralFormRow::DefaultRefreshPolicy) => Some(&self.dropdown_refresh_policy),
            _ => None,
        }
    }

    fn handle_open_dropdown(
        &mut self,
        chord: &KeyChord,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(dropdown_entity) = self.current_dropdown().cloned() else {
            return false;
        };

        if !dropdown_entity.read(cx).is_open() {
            return false;
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.select_next_item(cx));
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.select_prev_item(cx));
            }
            ("enter", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.accept_selection(cx));
            }
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.close(cx));
            }
            ("tab", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.accept_selection(cx));
                self.gen_move_down();
                self.gen_focus_current_input(window, cx);
            }
            ("tab", modifiers) if modifiers == Modifiers::shift() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.accept_selection(cx));
                self.gen_move_up();
                self.gen_focus_current_input(window, cx);
            }
            _ => return false,
        }

        cx.notify();
        true
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let chord = key_chord_from_gpui(&event.keystroke);

        if self.gen_editing_field {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.gen_editing_field = false;
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                    cx.notify();
                }
                ("enter", modifiers) if modifiers == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.gen_move_down();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.gen_move_down();
                    self.gen_focus_current_input(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.gen_editing_field = false;
                    self.gen_move_up();
                    self.gen_focus_current_input(window, cx);
                    cx.notify();
                }
                _ => {}
            }

            return;
        }

        if self.handle_open_dropdown(&chord, window, cx) {
            return;
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                self.gen_move_down();
                cx.notify();
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                self.gen_move_up();
                cx.notify();
            }
            ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                if modifiers == Modifiers::none() =>
            {
                self.gen_activate_current_field(window, cx);
            }
            ("tab", modifiers) if modifiers == Modifiers::none() => {
                self.gen_move_down();
                cx.notify();
            }
            ("tab", modifiers) if modifiers == Modifiers::shift() => {
                self.gen_move_up();
                cx.notify();
            }
            ("g", modifiers) if modifiers == Modifiers::none() => {
                self.gen_move_first();
                cx.notify();
            }
            ("G", modifiers) if modifiers == Modifiers::none() => {
                self.gen_move_last();
                cx.notify();
            }
            _ => {}
        }
    }

    pub(super) fn save_general_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let max_history_str = self.input_max_history.read(cx).value().trim().to_string();
        let max_history = match max_history_str.parse::<usize>() {
            Ok(value) if value >= 10 => value,
            _ => {
                let message = dbflux_i18n::t!("settings.general.max_history.error");
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
                    .push(cx);
                return;
            }
        };

        let auto_save_str = self.input_auto_save.read(cx).value().trim().to_string();
        let auto_save_ms = match auto_save_str.parse::<u64>() {
            Ok(value) if value >= 500 => value,
            _ => {
                let message = dbflux_i18n::t!("settings.general.auto_save_interval.error");
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
                    .push(cx);
                return;
            }
        };

        let refresh_interval_str = self
            .input_refresh_interval
            .read(cx)
            .value()
            .trim()
            .to_string();
        let refresh_interval = match refresh_interval_str.parse::<u32>() {
            Ok(value) if value >= 1 => value,
            _ => {
                let message = dbflux_i18n::t!("settings.general.refresh_interval.error");
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
                    .push(cx);
                return;
            }
        };

        let max_bg_str = self.input_max_bg_tasks.read(cx).value().trim().to_string();
        let max_bg_tasks = match max_bg_str.parse::<usize>() {
            Ok(value) if value >= 1 => value,
            _ => {
                let message = dbflux_i18n::t!("settings.general.max_background_tasks.error");
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
                    .push(cx);
                return;
            }
        };

        let preview_limit_str = self
            .input_object_preview_limit
            .read(cx)
            .value()
            .trim()
            .to_string();
        let object_preview_limit = match preview_limit_str.parse::<u64>() {
            Ok(value) if value >= 1 => value,
            _ => {
                let message = dbflux_i18n::t!("settings.general.object_preview_limit.error");
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
                    .push(cx);
                return;
            }
        };

        self.gen_settings.max_history_entries = max_history;
        self.gen_settings.auto_save_interval_ms = auto_save_ms;
        self.gen_settings.default_refresh_interval_secs = refresh_interval;
        self.gen_settings.max_concurrent_background_tasks = max_bg_tasks;
        self.gen_settings.object_preview_size_limit_mib = object_preview_limit;

        let runtime = self.app_state.read(cx).storage_runtime();
        if let Err(e) =
            dbflux_app::config_loader::save_general_settings(runtime, &self.gen_settings)
        {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.general.save.error", error = e),
                ),
                cx,
            );
            return;
        }

        self.app_state.update(cx, |state, _cx| {
            state.update_general_settings(self.gen_settings.clone());
        });

        // Update the density global so cx-based accessors reflect the new style immediately.
        dbflux_components::density::set_style(cx, self.gen_settings.style);

        dbflux_components::theme::apply_theme(
            self.gen_settings.theme,
            self.gen_settings.style,
            Some(window),
            cx,
        );

        Toast::success(dbflux_i18n::t!("settings.general.save.success"))
            .meta_right(now_hms())
            .push(cx);
    }

    pub(super) fn render_general_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;
        let muted_fg = theme.muted_foreground;
        let is_focused = self.content_focused;
        let cursor = self.gen_form_cursor;
        let rows = self.gen_form_rows();

        let is_at =
            |row: GeneralFormRow| -> bool { is_focused && rows.get(cursor).copied() == Some(row) };

        layout::single_form_section_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.header.title"),
                dbflux_i18n::t!("settings.general.header.subtitle"),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .gap_6()
                .child(self.render_gen_group_header(
                    dbflux_i18n::t!("settings.general.appearance.group"),
                    border,
                    muted_fg,
                ))
                .child(self.render_gen_dropdown(
                    dbflux_i18n::t!("settings.general.theme.label"),
                    &self.dropdown_theme,
                    is_at(GeneralFormRow::Theme),
                    primary,
                    GeneralFormRow::Theme,
                    cx,
                ))
                .child(self.render_gen_dropdown(
                    dbflux_i18n::t!("settings.general.style.label"),
                    &self.dropdown_style,
                    is_at(GeneralFormRow::Style),
                    primary,
                    GeneralFormRow::Style,
                    cx,
                ))
                .child(self.render_gen_dropdown(
                    dbflux_i18n::t!("settings.general.language.label"),
                    &self.dropdown_language,
                    is_at(GeneralFormRow::Language),
                    primary,
                    GeneralFormRow::Language,
                    cx,
                ))
                .child(div().px_2().child(
                    Body::new(dbflux_i18n::t!("settings.general.language.notice")).color(muted_fg),
                ))
                .child(self.render_gen_group_header(
                    dbflux_i18n::t!("settings.general.startup.group"),
                    border,
                    muted_fg,
                ))
                .child(self.render_gen_checkbox(
                    "restore-session",
                    dbflux_i18n::t!("settings.general.restore_session.label"),
                    self.gen_settings.restore_session_on_startup,
                    is_at(GeneralFormRow::RestoreSession),
                    GeneralFormRow::RestoreSession,
                    |this, value, _cx| this.gen_settings.restore_session_on_startup = value,
                    cx,
                ))
                .child(self.render_gen_checkbox(
                    "reopen-conns",
                    dbflux_i18n::t!("settings.general.reopen_connections.label"),
                    self.gen_settings.reopen_last_connections,
                    is_at(GeneralFormRow::ReopenConnections),
                    GeneralFormRow::ReopenConnections,
                    |this, value, _cx| this.gen_settings.reopen_last_connections = value,
                    cx,
                ))
                .child(self.render_gen_dropdown(
                    dbflux_i18n::t!("settings.general.default_focus.label"),
                    &self.dropdown_default_focus,
                    is_at(GeneralFormRow::DefaultFocus),
                    primary,
                    GeneralFormRow::DefaultFocus,
                    cx,
                ))
                .child(self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.max_history.label"),
                    &self.input_max_history,
                    is_at(GeneralFormRow::MaxHistory),
                    primary,
                    GeneralFormRow::MaxHistory,
                    cx,
                ))
                .child(self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.auto_save_interval.label"),
                    &self.input_auto_save,
                    is_at(GeneralFormRow::AutoSaveInterval),
                    primary,
                    GeneralFormRow::AutoSaveInterval,
                    cx,
                ))
                .child(self.render_gen_group_header(
                    dbflux_i18n::t!("settings.general.refresh.group"),
                    border,
                    muted_fg,
                ))
                .child(self.render_gen_dropdown(
                    dbflux_i18n::t!("settings.general.refresh_policy.label"),
                    &self.dropdown_refresh_policy,
                    is_at(GeneralFormRow::DefaultRefreshPolicy),
                    primary,
                    GeneralFormRow::DefaultRefreshPolicy,
                    cx,
                ))
                .child(self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.refresh_interval.label"),
                    &self.input_refresh_interval,
                    is_at(GeneralFormRow::DefaultRefreshInterval),
                    primary,
                    GeneralFormRow::DefaultRefreshInterval,
                    cx,
                ))
                .child(self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.max_background_tasks.label"),
                    &self.input_max_bg_tasks,
                    is_at(GeneralFormRow::MaxBackgroundTasks),
                    primary,
                    GeneralFormRow::MaxBackgroundTasks,
                    cx,
                ))
                .child(self.render_gen_checkbox(
                    "pause-on-error",
                    dbflux_i18n::t!("settings.general.pause_refresh_on_error.label"),
                    self.gen_settings.auto_refresh_pause_on_error,
                    is_at(GeneralFormRow::PauseRefreshOnError),
                    GeneralFormRow::PauseRefreshOnError,
                    |this, value, _cx| this.gen_settings.auto_refresh_pause_on_error = value,
                    cx,
                ))
                .child(self.render_gen_checkbox(
                    "refresh-visible",
                    dbflux_i18n::t!("settings.general.refresh_only_if_visible.label"),
                    self.gen_settings.auto_refresh_only_if_visible,
                    is_at(GeneralFormRow::RefreshOnlyIfVisible),
                    GeneralFormRow::RefreshOnlyIfVisible,
                    |this, value, _cx| this.gen_settings.auto_refresh_only_if_visible = value,
                    cx,
                ))
                .child(self.render_gen_group_header(
                    dbflux_i18n::t!("settings.general.safety.group"),
                    border,
                    muted_fg,
                ))
                .child(self.render_gen_checkbox(
                    "confirm-dangerous",
                    dbflux_i18n::t!("settings.general.confirm_dangerous.label"),
                    self.gen_settings.confirm_dangerous_queries,
                    is_at(GeneralFormRow::ConfirmDangerous),
                    GeneralFormRow::ConfirmDangerous,
                    |this, value, _cx| this.gen_settings.confirm_dangerous_queries = value,
                    cx,
                ))
                .child(self.render_gen_checkbox(
                    "requires-where",
                    dbflux_i18n::t!("settings.general.requires_where.label"),
                    self.gen_settings.dangerous_requires_where,
                    is_at(GeneralFormRow::RequiresWhere),
                    GeneralFormRow::RequiresWhere,
                    |this, value, _cx| this.gen_settings.dangerous_requires_where = value,
                    cx,
                ))
                .child(self.render_gen_checkbox(
                    "requires-preview",
                    dbflux_i18n::t!("settings.general.requires_preview.label"),
                    self.gen_settings.dangerous_requires_preview,
                    is_at(GeneralFormRow::RequiresPreview),
                    GeneralFormRow::RequiresPreview,
                    |this, value, _cx| this.gen_settings.dangerous_requires_preview = value,
                    cx,
                ))
                .child(self.render_gen_group_header(
                    dbflux_i18n::t!("settings.general.object_storage.group"),
                    border,
                    muted_fg,
                ))
                .child(self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.object_preview_limit.label"),
                    &self.input_object_preview_limit,
                    is_at(GeneralFormRow::ObjectPreviewLimit),
                    primary,
                    GeneralFormRow::ObjectPreviewLimit,
                    cx,
                ))
                .child(
                    div().px_2().child(
                        Body::new(dbflux_i18n::t!(
                            "settings.general.object_preview_hint.label"
                        ))
                        .color(muted_fg),
                    ),
                )
                .when(Self::is_nightly(), |column| {
                    column
                        .child(self.render_gen_group_header(
                            dbflux_i18n::t!("settings.general.storage.group"),
                            border,
                            muted_fg,
                        ))
                        .child(self.render_gen_checkbox(
                            "share-stable-db",
                            dbflux_i18n::t!("settings.general.share_stable_db.label"),
                            self.gen_share_stable_db,
                            is_at(GeneralFormRow::ShareStableDb),
                            GeneralFormRow::ShareStableDb,
                            |this, value, cx| this.set_share_stable_db(value, cx),
                            cx,
                        ))
                        .child(
                            div().px_2().child(
                                Body::new(dbflux_i18n::t!("settings.general.share_stable_db.hint"))
                                    .color(muted_fg),
                            ),
                        )
                }),
        )
    }

    pub(super) fn render_general_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_save_focused = self.content_focused
            && self.gen_form_rows().get(self.gen_form_cursor).copied()
                == Some(GeneralFormRow::SaveButton);

        div()
            .flex()
            .items_center()
            .gap_3()
            .child(layout::footer_action_frame(
                is_save_focused,
                cx.theme().primary,
                FluxButton::new(
                    "save-general",
                    dbflux_i18n::t!("settings.general.save.button"),
                )
                .small()
                .primary()
                .w_full()
                .on_click(cx.listener(|this, _, window, cx| {
                    this.content_focused = true;
                    this.gen_form_cursor = this
                        .gen_form_rows()
                        .iter()
                        .position(|row| *row == GeneralFormRow::SaveButton)
                        .unwrap_or_default();
                    this.save_general_settings(window, cx);
                })),
            ))
            .into_any_element()
    }

    fn render_gen_group_header(
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

    #[allow(clippy::too_many_arguments)]
    fn render_gen_checkbox(
        &self,
        id: &'static str,
        label: impl Into<SharedString>,
        checked: bool,
        is_focused: bool,
        row: GeneralFormRow,
        setter: fn(&mut Self, bool, &mut Context<Self>),
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
                        .gen_form_rows()
                        .iter()
                        .position(|candidate| *candidate == row)
                    {
                        this.gen_form_cursor = position;
                    }
                    cx.notify();
                }),
            )
            .child(Checkbox::new(id).checked(checked).on_click(cx.listener(
                move |this, value: &bool, _, cx| {
                    setter(this, *value, cx);
                    cx.notify();
                },
            )))
            .child(Body::new(label))
    }

    fn render_gen_dropdown(
        &self,
        label: impl Into<SharedString>,
        dropdown: &Entity<Dropdown>,
        is_focused: bool,
        primary: Hsla,
        row: GeneralFormRow,
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
                        .gen_form_rows()
                        .iter()
                        .position(|candidate| *candidate == row)
                    {
                        this.gen_form_cursor = position;
                    }
                    cx.notify();
                }),
            )
            .child(FieldLabel::new(label))
            .child(div().min_w(px(140.0)).child(dropdown.clone()))
    }

    fn render_gen_input_field(
        &self,
        label: impl Into<SharedString>,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div().flex().child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .gap_1()
                .child(FieldLabel::new(label))
                .child(
                    div()
                        .w_full()
                        .rounded(Radii::SM)
                        .border_1()
                        .border_color(if is_focused {
                            primary
                        } else {
                            gpui::transparent_black()
                        })
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                this.switching_input = true;
                                this.content_focused = true;
                                if let Some(position) = this
                                    .gen_form_rows()
                                    .iter()
                                    .position(|candidate| *candidate == row)
                                {
                                    this.gen_form_cursor = position;
                                }
                                this.gen_focus_current_input(window, cx);
                                cx.notify();
                            }),
                        )
                        .child(Input::new(input).small().w_full()),
                ),
        )
    }
}
