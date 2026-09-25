use crate::tokens::SettingsMetrics;
use dbflux_app::keymap::{KeyChord, Modifiers};
use dbflux_components::controls::Button as FluxButton;
use dbflux_components::controls::{Checkbox, Dropdown, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::typography::AppFonts;
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;

use super::general_section::{GeneralFormRow, GeneralSection};
use super::layout;
use super::section_trait::SectionFocusEvent;

impl GeneralSection {
    pub(super) fn has_unsaved_general_changes(&self, cx: &App) -> bool {
        self.general_change_count(cx) > 0
    }

    /// Number of settings that differ from the saved ones.
    pub(super) fn general_change_count(&self, cx: &App) -> usize {
        let saved = self.app_state.read(cx).general_settings();
        let current = &self.gen_settings;

        let setting_changes = [
            current.theme != saved.theme,
            current.style != saved.style,
            current.language != saved.language,
            current.restore_session_on_startup != saved.restore_session_on_startup,
            current.reopen_last_connections != saved.reopen_last_connections,
            current.default_focus_on_startup != saved.default_focus_on_startup,
            current.default_refresh_policy != saved.default_refresh_policy,
            current.auto_refresh_pause_on_error != saved.auto_refresh_pause_on_error,
            current.auto_refresh_only_if_visible != saved.auto_refresh_only_if_visible,
            current.confirm_dangerous_queries != saved.confirm_dangerous_queries,
            current.dangerous_requires_where != saved.dangerous_requires_where,
            current.dangerous_requires_preview != saved.dangerous_requires_preview,
            current.vim_mode != saved.vim_mode,
        ];

        let input_changes = [
            (
                &self.input_max_history,
                saved.max_history_entries.to_string(),
            ),
            (
                &self.input_auto_save,
                saved.auto_save_interval_ms.to_string(),
            ),
            (
                &self.input_refresh_interval,
                saved.default_refresh_interval_secs.to_string(),
            ),
            (
                &self.input_max_bg_tasks,
                saved.max_concurrent_background_tasks.to_string(),
            ),
            (
                &self.input_editor_row_limit,
                saved.editor_row_limit.to_string(),
            ),
            (
                &self.input_object_preview_limit,
                saved.object_preview_size_limit_mib.to_string(),
            ),
            (
                &self.input_key_value_size_limit,
                saved.key_value_size_limit_mib.to_string(),
            ),
        ]
        .into_iter()
        .filter(|(input, saved_value)| input.read(cx).value().trim() != saved_value.as_str())
        .count();

        setting_changes
            .into_iter()
            .filter(|changed| *changed)
            .count()
            + input_changes
    }

    /// Parses the editor row limit input. Accepts a whole number of at least 1
    /// that also fits the signed 64-bit storage column; anything else is `None`.
    pub(super) fn parse_editor_row_limit(value: &str) -> Option<usize> {
        let limit = value.trim().parse::<usize>().ok()?;

        (limit >= 1 && i64::try_from(limit).is_ok()).then_some(limit)
    }

    pub(super) fn gen_form_rows(&self) -> Vec<GeneralFormRow> {
        let mut rows = vec![
            GeneralFormRow::Theme,
            GeneralFormRow::Style,
            GeneralFormRow::Language,
            GeneralFormRow::VimMode,
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
            GeneralFormRow::EditorRowLimit,
            GeneralFormRow::ObjectPreviewLimit,
            GeneralFormRow::KeyValueSizeLimit,
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
                let next = (Self::theme_index(self.gen_settings.theme) + 1) % 3;
                self.gen_settings.theme = Self::theme_for_index(next);
                cx.notify();
            }
            Some(GeneralFormRow::Style) => {
                let next = (Self::style_index(self.gen_settings.style) + 1) % 2;
                self.gen_settings.style = Self::style_for_index(next);
                cx.notify();
            }
            Some(GeneralFormRow::Language) => {
                self.dropdown_language
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::VimMode) => {
                self.gen_settings.vim_mode = !self.gen_settings.vim_mode;
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
                let next =
                    (Self::startup_focus_index(self.gen_settings.default_focus_on_startup) + 1) % 2;
                self.gen_settings.default_focus_on_startup = Self::startup_focus_for_index(next);
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
            | Some(GeneralFormRow::EditorRowLimit)
            | Some(GeneralFormRow::ObjectPreviewLimit)
            | Some(GeneralFormRow::KeyValueSizeLimit) => {
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
            Some(GeneralFormRow::EditorRowLimit) => {
                self.input_editor_row_limit
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::ObjectPreviewLimit) => {
                self.input_object_preview_limit
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::KeyValueSizeLimit) => {
                self.input_key_value_size_limit
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
            Some(GeneralFormRow::Language) => Some(&self.dropdown_language),
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

        let editor_row_limit_str = self.input_editor_row_limit.read(cx).value().to_string();
        let Some(editor_row_limit) = Self::parse_editor_row_limit(&editor_row_limit_str) else {
            let message = dbflux_i18n::t!("settings.general.editor_row_limit.error");
            Toast::error(message.clone())
                .meta_right(now_hms())
                .action(copy_action(message))
                .push(cx);
            return;
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

        let kv_size_limit_str = self
            .input_key_value_size_limit
            .read(cx)
            .value()
            .trim()
            .to_string();
        let key_value_size_limit = match kv_size_limit_str.parse::<u64>() {
            Ok(value) if value >= 1 => value,
            _ => {
                let message = dbflux_i18n::t!("settings.general.key_value_size_limit.error");
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
        self.gen_settings.editor_row_limit = editor_row_limit;
        self.gen_settings.object_preview_size_limit_mib = object_preview_limit;
        self.gen_settings.key_value_size_limit_mib = key_value_size_limit;

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

        // Open code editors pick up settings such as Vim mode from this event.
        self.app_state.update(cx, |state, cx| {
            state.update_general_settings(self.gen_settings.clone());
            cx.emit(AppStateChanged);
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
        let theme_index = Self::theme_index(self.gen_settings.theme);
        let style_index = Self::style_index(self.gen_settings.style);
        let focus_index = Self::startup_focus_index(self.gen_settings.default_focus_on_startup);

        let appearance = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.appearance.group"),
                Some(AppIcon::Eye.into()),
                cx,
            ))
            .child(self.render_gen_segmented(
                dbflux_i18n::t!("settings.general.theme.label"),
                Some(dbflux_i18n::t!("settings.general.theme.help")),
                Self::theme_items(),
                theme_index,
                GeneralFormRow::Theme,
                |this, index| this.gen_settings.theme = Self::theme_for_index(index),
                cx,
            ))
            .child(self.render_gen_segmented(
                dbflux_i18n::t!("settings.general.style.label"),
                Some(dbflux_i18n::t!("settings.general.style.help")),
                Self::style_items(),
                style_index,
                GeneralFormRow::Style,
                |this, index| this.gen_settings.style = Self::style_for_index(index),
                cx,
            ))
            .child(self.render_gen_dropdown(
                dbflux_i18n::t!("settings.general.language.label"),
                Some(dbflux_i18n::t!("settings.general.language.notice")),
                &self.dropdown_language,
                GeneralFormRow::Language,
                cx,
            ));

        let editor = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.editor.group"),
                Some(AppIcon::Code.into()),
                cx,
            ))
            .child(self.render_gen_checkbox(
                "vim-mode",
                dbflux_i18n::t!("settings.general.vim_mode.label"),
                Some(dbflux_i18n::t!("settings.general.vim_mode.hint")),
                self.gen_settings.vim_mode,
                GeneralFormRow::VimMode,
                |this, value, _cx| this.gen_settings.vim_mode = value,
                cx,
            ));

        let startup = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.startup.group"),
                Some(AppIcon::Play.into()),
                cx,
            ))
            .child(self.render_gen_checkbox(
                "restore-session",
                dbflux_i18n::t!("settings.general.restore_session.label"),
                None,
                self.gen_settings.restore_session_on_startup,
                GeneralFormRow::RestoreSession,
                |this, value, _cx| this.gen_settings.restore_session_on_startup = value,
                cx,
            ))
            .child(self.render_gen_checkbox(
                "reopen-conns",
                dbflux_i18n::t!("settings.general.reopen_connections.label"),
                None,
                self.gen_settings.reopen_last_connections,
                GeneralFormRow::ReopenConnections,
                |this, value, _cx| this.gen_settings.reopen_last_connections = value,
                cx,
            ))
            .child(self.render_gen_segmented(
                dbflux_i18n::t!("settings.general.default_focus.label"),
                None,
                Self::startup_focus_items(),
                focus_index,
                GeneralFormRow::DefaultFocus,
                |this, index| {
                    this.gen_settings.default_focus_on_startup =
                        Self::startup_focus_for_index(index)
                },
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.max_history.label"),
                &self.input_max_history,
                None,
                GeneralFormRow::MaxHistory,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.auto_save_interval.label"),
                &self.input_auto_save,
                Some(dbflux_i18n::t!("settings.general.unit.milliseconds")),
                GeneralFormRow::AutoSaveInterval,
                cx,
            ));

        let refresh = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.refresh.group"),
                Some(AppIcon::RefreshCcw.into()),
                cx,
            ))
            .child(self.render_gen_dropdown(
                dbflux_i18n::t!("settings.general.refresh_policy.label"),
                None,
                &self.dropdown_refresh_policy,
                GeneralFormRow::DefaultRefreshPolicy,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.refresh_interval.label"),
                &self.input_refresh_interval,
                Some(dbflux_i18n::t!("settings.general.unit.seconds")),
                GeneralFormRow::DefaultRefreshInterval,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.max_background_tasks.label"),
                &self.input_max_bg_tasks,
                None,
                GeneralFormRow::MaxBackgroundTasks,
                cx,
            ))
            .child(self.render_gen_checkbox(
                "pause-on-error",
                dbflux_i18n::t!("settings.general.pause_refresh_on_error.label"),
                None,
                self.gen_settings.auto_refresh_pause_on_error,
                GeneralFormRow::PauseRefreshOnError,
                |this, value, _cx| this.gen_settings.auto_refresh_pause_on_error = value,
                cx,
            ))
            .child(self.render_gen_checkbox(
                "refresh-visible",
                dbflux_i18n::t!("settings.general.refresh_only_if_visible.label"),
                None,
                self.gen_settings.auto_refresh_only_if_visible,
                GeneralFormRow::RefreshOnlyIfVisible,
                |this, value, _cx| this.gen_settings.auto_refresh_only_if_visible = value,
                cx,
            ));

        let safety = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.safety.group"),
                Some(AppIcon::TriangleAlert.into()),
                cx,
            ))
            .child(self.render_gen_checkbox(
                "confirm-dangerous",
                dbflux_i18n::t!("settings.general.confirm_dangerous.label"),
                Some(dbflux_i18n::t!("settings.general.confirm_dangerous.hint")),
                self.gen_settings.confirm_dangerous_queries,
                GeneralFormRow::ConfirmDangerous,
                |this, value, _cx| this.gen_settings.confirm_dangerous_queries = value,
                cx,
            ))
            .child(self.render_gen_checkbox(
                "requires-where",
                dbflux_i18n::t!("settings.general.requires_where.label"),
                None,
                self.gen_settings.dangerous_requires_where,
                GeneralFormRow::RequiresWhere,
                |this, value, _cx| this.gen_settings.dangerous_requires_where = value,
                cx,
            ))
            .child(self.render_gen_checkbox(
                "requires-preview",
                dbflux_i18n::t!("settings.general.requires_preview.label"),
                None,
                self.gen_settings.dangerous_requires_preview,
                GeneralFormRow::RequiresPreview,
                |this, value, _cx| this.gen_settings.dangerous_requires_preview = value,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.editor_row_limit.label"),
                &self.input_editor_row_limit,
                None,
                GeneralFormRow::EditorRowLimit,
                cx,
            ));

        let object_storage = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.object_storage.group"),
                Some(AppIcon::Boxes.into()),
                cx,
            ))
            .child(
                self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.object_preview_limit.label"),
                    &self.input_object_preview_limit,
                    Some(dbflux_i18n::t!("settings.general.unit.mebibytes")),
                    GeneralFormRow::ObjectPreviewLimit,
                    cx,
                )
                .child(layout::help_text(dbflux_i18n::t!(
                    "settings.general.object_preview_hint.label"
                ))),
            );

        let key_value = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.key_value.group"),
                Some(AppIcon::KeyRound.into()),
                cx,
            ))
            .child(
                self.render_gen_input_field(
                    dbflux_i18n::t!("settings.general.key_value_size_limit.label"),
                    &self.input_key_value_size_limit,
                    Some(dbflux_i18n::t!("settings.general.unit.mebibytes")),
                    GeneralFormRow::KeyValueSizeLimit,
                    cx,
                )
                .child(layout::help_text(dbflux_i18n::t!(
                    "settings.general.key_value_size_limit_hint.label"
                ))),
            );

        let nightly_storage = Self::is_nightly().then(|| {
            div()
                .flex()
                .flex_col()
                .child(dbflux_components::composites::section_header(
                    dbflux_i18n::t!("settings.general.storage.group"),
                    Some(AppIcon::HardDrive.into()),
                    cx,
                ))
                .child(self.render_gen_checkbox(
                    "share-stable-db",
                    dbflux_i18n::t!("settings.general.share_stable_db.label"),
                    Some(dbflux_i18n::t!("settings.general.share_stable_db.hint")),
                    self.gen_share_stable_db,
                    GeneralFormRow::ShareStableDb,
                    |this, value, cx| this.set_share_stable_db(value, cx),
                    cx,
                ))
        });

        layout::single_form_section_shell(
            dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.general.header.title"),
                dbflux_i18n::t!("settings.general.header.subtitle"),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .child(appearance)
                .child(editor)
                .child(startup)
                .child(refresh)
                .child(safety)
                .child(object_storage)
                .child(key_value)
                .children(nightly_storage),
        )
    }

    pub(super) fn render_general_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_save_focused = self.is_at(GeneralFormRow::SaveButton);

        FluxButton::new(
            "save-general",
            dbflux_i18n::t!("settings.general.save.button"),
        )
        .small()
        .primary()
        .icon(AppIcon::Save)
        .kbd("Ctrl S")
        .focused(is_save_focused)
        .on_click(cx.listener(|this, _, window, cx| {
            this.select_row(GeneralFormRow::SaveButton);
            this.save_general_settings(window, cx);
        }))
        .into_any_element()
    }

    /// Whether the keyboard cursor is on `row` while the page holds focus.
    fn is_at(&self, row: GeneralFormRow) -> bool {
        self.content_focused && self.gen_form_rows().get(self.gen_form_cursor).copied() == Some(row)
    }

    /// Moves the keyboard cursor to `row` and gives the page focus.
    fn select_row(&mut self, row: GeneralFormRow) {
        self.content_focused = true;

        if let Some(position) = self
            .gen_form_rows()
            .iter()
            .position(|candidate| *candidate == row)
        {
            self.gen_form_cursor = position;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn render_gen_checkbox(
        &self,
        id: &'static str,
        label: String,
        description: Option<String>,
        checked: bool,
        row: GeneralFormRow,
        setter: fn(&mut Self, bool, &mut Context<Self>),
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let checkbox = Checkbox::new(id)
            .checked(checked)
            .label(label)
            .on_click(cx.listener(move |this, value: &bool, _, cx| {
                this.select_row(row);
                setter(this, *value, cx);
                cx.notify();
            }));

        layout::cursor_ring(
            self.is_at(row),
            layout::check_row(checkbox, description.map(SharedString::from)),
            cx,
        )
        .on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, _, cx| {
                this.select_row(row);
                cx.notify();
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_gen_segmented(
        &self,
        label: String,
        help: Option<String>,
        items: Vec<SegmentedItem>,
        active_index: usize,
        row: GeneralFormRow,
        setter: fn(&mut Self, usize),
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let entity = cx.entity();
        let control = SegmentedControl::new(
            items,
            SharedString::from(active_index.to_string()),
            move |selected: &SharedString, _window, cx| {
                let Ok(index) = selected.parse::<usize>() else {
                    return;
                };

                entity.update(cx, |this, cx| {
                    this.select_row(row);
                    setter(this, index);
                    cx.notify();
                });
            },
        );

        layout::form_row(
            label,
            div()
                .flex()
                .child(layout::cursor_ring(self.is_at(row), control, cx)),
            help.map(SharedString::from),
        )
    }

    fn render_gen_dropdown(
        &self,
        label: String,
        help: Option<String>,
        dropdown: &Entity<Dropdown>,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        layout::form_row(
            label,
            layout::cursor_ring(
                self.is_at(row),
                div()
                    .w(SettingsMetrics::SELECT_WIDTH)
                    .child(dropdown.clone()),
                cx,
            )
            .w(SettingsMetrics::SELECT_WIDTH)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.select_row(row);
                    cx.notify();
                }),
            ),
            help.map(SharedString::from),
        )
    }

    /// Stable element id for inputs that UI automation addresses by name.
    fn input_element_id(row: GeneralFormRow) -> &'static str {
        match row {
            GeneralFormRow::MaxHistory => "general-max-history",
            GeneralFormRow::AutoSaveInterval => "general-auto-save",
            GeneralFormRow::DefaultRefreshInterval => "general-refresh-interval",
            GeneralFormRow::MaxBackgroundTasks => "general-max-background-tasks",
            GeneralFormRow::ObjectPreviewLimit => "general-object-preview-limit",
            GeneralFormRow::KeyValueSizeLimit => "general-key-value-size-limit",
            _ => "editor-row-limit",
        }
    }

    /// Numeric form row: a short mono field with an optional unit inside it.
    fn render_gen_input_field(
        &self,
        label: String,
        input: &Entity<InputState>,
        unit: Option<String>,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> Div {
        let field = Input::new(input)
            .id(Self::input_element_id(row))
            .aria_label(label.clone())
            .when_some(unit, |field, unit| {
                field.suffix(Text::code(unit).muted_foreground())
            });

        layout::form_row(
            label,
            layout::cursor_ring(
                self.is_at(row) && !self.gen_editing_field,
                div()
                    .w(SettingsMetrics::NUMBER_FIELD_WIDTH)
                    .font_family(AppFonts::MONO)
                    .child(field),
                cx,
            )
            .w(SettingsMetrics::NUMBER_FIELD_WIDTH)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, window, cx| {
                    this.switching_input = true;
                    this.select_row(row);
                    this.gen_focus_current_input(window, cx);
                    cx.notify();
                }),
            ),
            None,
        )
    }
}
