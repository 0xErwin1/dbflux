use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_app::keymap::{KeyChord, Modifiers};
use dbflux_components::controls::Button as FluxButton;
use dbflux_components::controls::{Checkbox, Dropdown, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::Fields;
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::select::Select;

use super::general_section::{FontFamilyField, FontFamilySelect, GeneralFormRow, GeneralSection};
use super::layout;
use super::section_trait::SectionFocusEvent;

/// Frames a reveal may take before it gives up, should the layout keep
/// moving the field.
const MAX_REVEAL_ATTEMPTS: u8 = 4;

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
            current.ui_font_family != saved.ui_font_family,
            current.editor_font_family != saved.editor_font_family,
            current.grid_font_family != saved.grid_font_family,
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
            current.vim_leader != saved.vim_leader,
        ];

        let input_changes = [
            (&self.input_ui_font_size, saved.ui_font_size.to_string()),
            (
                &self.input_editor_font_size,
                saved.editor_font_size.to_string(),
            ),
            (&self.input_grid_font_size, saved.grid_font_size.to_string()),
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
            GeneralFormRow::UiFontFamily,
            GeneralFormRow::UiFontSize,
            GeneralFormRow::EditorFontFamily,
            GeneralFormRow::EditorFontSize,
            GeneralFormRow::GridFontFamily,
            GeneralFormRow::GridFontSize,
            GeneralFormRow::VimMode,
            GeneralFormRow::VimLeader,
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

    /// Left and Right on a segmented row (theme, style, startup focus) move
    /// its choice, stopping at either end. Returns whether the cursor was on
    /// such a row.
    pub(super) fn gen_step_segmented(&mut self, step: isize) -> bool {
        let stepped = |current: usize, count: usize| {
            current
                .saturating_add_signed(step)
                .min(count.saturating_sub(1))
        };

        match self.gen_current_row() {
            Some(GeneralFormRow::Theme) => {
                let next = stepped(Self::theme_index(self.gen_settings.theme), 3);
                self.gen_settings.theme = Self::theme_for_index(next);
                true
            }
            Some(GeneralFormRow::Style) => {
                let next = stepped(Self::style_index(self.gen_settings.style), 2);
                self.gen_settings.style = Self::style_for_index(next);
                true
            }
            Some(GeneralFormRow::DefaultFocus) => {
                let next = stepped(
                    Self::startup_focus_index(self.gen_settings.default_focus_on_startup),
                    2,
                );
                self.gen_settings.default_focus_on_startup = Self::startup_focus_for_index(next);
                true
            }
            _ => false,
        }
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
            Some(GeneralFormRow::VimLeader) => {
                self.dropdown_vim_leader
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
            Some(GeneralFormRow::UiFontFamily)
            | Some(GeneralFormRow::EditorFontFamily)
            | Some(GeneralFormRow::GridFontFamily)
            | Some(GeneralFormRow::UiFontSize)
            | Some(GeneralFormRow::EditorFontSize)
            | Some(GeneralFormRow::GridFontSize)
            | Some(GeneralFormRow::MaxHistory)
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
            Some(GeneralFormRow::UiFontFamily) => {
                self.select_ui_font_family
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::EditorFontFamily) => {
                self.select_editor_font_family
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::GridFontFamily) => {
                self.select_grid_font_family
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::UiFontSize) => {
                self.input_ui_font_size
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::EditorFontSize) => {
                self.input_editor_font_size
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            Some(GeneralFormRow::GridFontSize) => {
                self.input_grid_font_size
                    .update(cx, |state, cx| state.focus(window, cx));
            }
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

    /// Whether a font family select, or its open search list, holds focus.
    /// A select opened with the mouse takes typed keys without the row being
    /// in edit mode, so the page must not treat them as navigation.
    fn font_family_select_focused(&self, window: &Window, cx: &App) -> bool {
        [
            &self.select_ui_font_family,
            &self.select_editor_font_family,
            &self.select_grid_font_family,
        ]
        .into_iter()
        .any(|select| select.focus_handle(cx).contains_focused(window, cx))
    }

    /// Whether the cursor is on a font family row, whose select opens and
    /// confirms with Enter itself.
    fn on_font_family_row(&self) -> bool {
        matches!(
            self.gen_current_row(),
            Some(GeneralFormRow::UiFontFamily)
                | Some(GeneralFormRow::EditorFontFamily)
                | Some(GeneralFormRow::GridFontFamily)
        )
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
            Some(GeneralFormRow::VimLeader) => Some(&self.dropdown_vim_leader),
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

        if self.gen_editing_field || self.font_family_select_focused(window, cx) {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.gen_editing_field = false;
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                    cx.notify();
                }
                ("enter", modifiers)
                    if modifiers == Modifiers::none() && !self.on_font_family_row() =>
                {
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
            ("left", modifiers)
                if modifiers == Modifiers::none() && self.gen_step_segmented(-1) =>
            {
                cx.notify();
            }
            ("right", modifiers) if modifiers == Modifiers::none() => {
                if self.gen_step_segmented(1) {
                    cx.notify();
                } else {
                    self.gen_activate_current_field(window, cx);
                }
            }
            ("l", modifiers) | ("enter", modifiers) if modifiers == Modifiers::none() => {
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

    /// Reports an invalid field under the field itself, moves the form
    /// cursor and focus to it, and scrolls it into view on the next render.
    /// Toasts show in the main window, away from the form the user is
    /// editing.
    fn reject_field(
        &mut self,
        row: GeneralFormRow,
        message: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.gen_field_error = Some((row, message));
        self.select_row(row);
        self.gen_focus_current_input(window, cx);
        self.pending_reveal = Some(row);
        self.reveal_attempts = 0;
        cx.notify();
    }

    /// Scrolls the form so the field of a rejected save is in view, just
    /// below the page head, then checks the result on the next frame.
    ///
    /// The field's last drawn bounds are taken back to the scroll offset they
    /// were drawn at. Rows above it can still change height between frames
    /// (an error line appears or goes), so the reveal only ends once a frame
    /// drawn at the current offset shows the field in view.
    pub(super) fn reveal_pending_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(row) = self.pending_reveal else {
            return;
        };

        let Some((field, painted_offset)) = self.field_bounds.borrow().get(&row).copied() else {
            Self::reveal_next_frame(window, cx);
            return;
        };

        let offset = self.form_scroll.offset();
        let content_top = field.top() - painted_offset;
        let top = content_top + offset.y;
        let bottom = top + field.size.height;

        let viewport = self.form_viewport.get();
        let margin = FormMetrics::ROW_GAP;
        let in_view = top >= viewport.top() + margin && bottom <= viewport.bottom();

        if in_view && painted_offset == offset.y {
            self.pending_reveal = None;
            self.reveal_attempts = 0;
            return;
        }

        self.reveal_attempts += 1;
        if self.reveal_attempts > MAX_REVEAL_ATTEMPTS {
            self.pending_reveal = None;
            self.reveal_attempts = 0;
            return;
        }

        if !in_view {
            let max_scroll = self.form_scroll.max_offset().y;
            let target = viewport.top() + margin - content_top;

            self.form_scroll
                .set_offset(point(offset.x, target.clamp(-max_scroll, Pixels::ZERO)));
        }

        Self::reveal_next_frame(window, cx);
    }

    /// Renders the section again on the next frame, to check a reveal.
    fn reveal_next_frame(window: &mut Window, cx: &mut Context<Self>) {
        cx.on_next_frame(window, |_, _, cx| cx.notify());
    }

    /// Reads a font size input, or reports `error_key` on `row` and returns
    /// `None`.
    fn read_font_size(
        &mut self,
        input: &Entity<InputState>,
        row: GeneralFormRow,
        error_key: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<f32> {
        let value = input.read(cx).value().to_string();

        if let Some(size) = Self::parse_font_size(&value) {
            return Some(size);
        }

        let message = dbflux_i18n::t!(
            error_key,
            min = dbflux_core::GeneralSettings::MIN_FONT_SIZE,
            max = dbflux_core::GeneralSettings::MAX_FONT_SIZE
        );
        self.reject_field(row, message, window, cx);

        None
    }

    pub(super) fn save_general_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.gen_field_error = None;

        let Some(ui_font_size) = self.read_font_size(
            &self.input_ui_font_size.clone(),
            GeneralFormRow::UiFontSize,
            "settings.general.ui_font_size.error",
            window,
            cx,
        ) else {
            return;
        };
        let Some(editor_font_size) = self.read_font_size(
            &self.input_editor_font_size.clone(),
            GeneralFormRow::EditorFontSize,
            "settings.general.editor_font_size.error",
            window,
            cx,
        ) else {
            return;
        };
        let Some(grid_font_size) = self.read_font_size(
            &self.input_grid_font_size.clone(),
            GeneralFormRow::GridFontSize,
            "settings.general.grid_font_size.error",
            window,
            cx,
        ) else {
            return;
        };

        let max_history_str = self.input_max_history.read(cx).value().trim().to_string();
        let max_history = match max_history_str.parse::<usize>() {
            Ok(value) if value >= 10 => value,
            _ => {
                self.reject_field(
                    GeneralFormRow::MaxHistory,
                    dbflux_i18n::t!("settings.general.max_history.error"),
                    window,
                    cx,
                );
                return;
            }
        };

        let auto_save_str = self.input_auto_save.read(cx).value().trim().to_string();
        let auto_save_ms = match auto_save_str.parse::<u64>() {
            Ok(value) if value >= 500 => value,
            _ => {
                self.reject_field(
                    GeneralFormRow::AutoSaveInterval,
                    dbflux_i18n::t!("settings.general.auto_save_interval.error"),
                    window,
                    cx,
                );
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
                self.reject_field(
                    GeneralFormRow::DefaultRefreshInterval,
                    dbflux_i18n::t!("settings.general.refresh_interval.error"),
                    window,
                    cx,
                );
                return;
            }
        };

        let max_bg_str = self.input_max_bg_tasks.read(cx).value().trim().to_string();
        let max_bg_tasks = match max_bg_str.parse::<usize>() {
            Ok(value) if value >= 1 => value,
            _ => {
                self.reject_field(
                    GeneralFormRow::MaxBackgroundTasks,
                    dbflux_i18n::t!("settings.general.max_background_tasks.error"),
                    window,
                    cx,
                );
                return;
            }
        };

        let editor_row_limit_str = self.input_editor_row_limit.read(cx).value().to_string();
        let Some(editor_row_limit) = Self::parse_editor_row_limit(&editor_row_limit_str) else {
            self.reject_field(
                GeneralFormRow::EditorRowLimit,
                dbflux_i18n::t!("settings.general.editor_row_limit.error"),
                window,
                cx,
            );
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
                self.reject_field(
                    GeneralFormRow::ObjectPreviewLimit,
                    dbflux_i18n::t!("settings.general.object_preview_limit.error"),
                    window,
                    cx,
                );
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
                self.reject_field(
                    GeneralFormRow::KeyValueSizeLimit,
                    dbflux_i18n::t!("settings.general.key_value_size_limit.error"),
                    window,
                    cx,
                );
                return;
            }
        };

        self.gen_settings.ui_font_size = ui_font_size;
        self.gen_settings.editor_font_size = editor_font_size;
        self.gen_settings.grid_font_size = grid_font_size;
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
                "theme",
                |this, index| this.gen_settings.theme = Self::theme_for_index(index),
                cx,
            ))
            .child(self.render_gen_segmented(
                dbflux_i18n::t!("settings.general.style.label"),
                Some(dbflux_i18n::t!("settings.general.style.help")),
                Self::style_items(),
                style_index,
                GeneralFormRow::Style,
                "density",
                |this, index| this.gen_settings.style = Self::style_for_index(index),
                cx,
            ))
            .child(self.render_gen_dropdown(
                dbflux_i18n::t!("settings.general.language.label"),
                Some(dbflux_i18n::t!("settings.general.language.notice")),
                self.dropdown_language.clone(),
                GeneralFormRow::Language,
                cx,
            ))
            .child(self.render_gen_font_family(
                FontFamilyField::Ui,
                &self.select_ui_font_family,
                GeneralFormRow::UiFontFamily,
                cx,
            ))
            .child(self.render_gen_font_size(
                dbflux_i18n::t!("settings.general.ui_font_size.label"),
                dbflux_i18n::t!("settings.general.ui_font_size.help"),
                &self.input_ui_font_size,
                GeneralFormRow::UiFontSize,
                cx,
            ))
            .child(self.render_gen_font_family(
                FontFamilyField::Editor,
                &self.select_editor_font_family,
                GeneralFormRow::EditorFontFamily,
                cx,
            ))
            .child(self.render_gen_font_size(
                dbflux_i18n::t!("settings.general.editor_font_size.label"),
                dbflux_i18n::t!("settings.general.editor_font_size.help"),
                &self.input_editor_font_size,
                GeneralFormRow::EditorFontSize,
                cx,
            ))
            .child(self.render_gen_font_family(
                FontFamilyField::Grid,
                &self.select_grid_font_family,
                GeneralFormRow::GridFontFamily,
                cx,
            ))
            .child(self.render_gen_font_size(
                dbflux_i18n::t!("settings.general.grid_font_size.label"),
                dbflux_i18n::t!("settings.general.grid_font_size.help"),
                &self.input_grid_font_size,
                GeneralFormRow::GridFontSize,
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
            ))
            .child(self.render_gen_dropdown(
                dbflux_i18n::t!("settings.general.vim_leader.label"),
                Some(dbflux_i18n::t!("settings.general.vim_leader.hint")),
                self.dropdown_vim_leader.clone(),
                GeneralFormRow::VimLeader,
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
                "focus-on-launch",
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
                None,
                GeneralFormRow::MaxHistory,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.auto_save_interval.label"),
                &self.input_auto_save,
                Some(dbflux_i18n::t!("settings.general.unit.milliseconds")),
                None,
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
                self.dropdown_refresh_policy.clone(),
                GeneralFormRow::DefaultRefreshPolicy,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.refresh_interval.label"),
                &self.input_refresh_interval,
                Some(dbflux_i18n::t!("settings.general.unit.seconds")),
                None,
                GeneralFormRow::DefaultRefreshInterval,
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.max_background_tasks.label"),
                &self.input_max_bg_tasks,
                None,
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
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.object_preview_limit.label"),
                &self.input_object_preview_limit,
                Some(dbflux_i18n::t!("settings.general.unit.mebibytes")),
                Some(dbflux_i18n::t!(
                    "settings.general.object_preview_hint.label"
                )),
                GeneralFormRow::ObjectPreviewLimit,
                cx,
            ));

        let key_value = div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.general.key_value.group"),
                Some(AppIcon::KeyRound.into()),
                cx,
            ))
            .child(self.render_gen_input_field(
                dbflux_i18n::t!("settings.general.key_value_size_limit.label"),
                &self.input_key_value_size_limit,
                Some(dbflux_i18n::t!("settings.general.unit.mebibytes")),
                Some(dbflux_i18n::t!(
                    "settings.general.key_value_size_limit_hint.label"
                )),
                GeneralFormRow::KeyValueSizeLimit,
                cx,
            ));

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

        layout::scrolled_form_section_shell(
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
            &self.form_scroll,
            self.form_viewport.clone(),
        )
    }

    pub(super) fn render_general_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_save_focused = self.is_at(GeneralFormRow::SaveButton);

        FluxButton::new(
            "save-general",
            dbflux_i18n::t!("settings.general.save.button"),
        )
        .primary()
        .icon(AppIcon::Save)
        .when_some(
            crate::settings::save_shortcut(),
            dbflux_components::controls::Button::kbd,
        )
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
        group: &'static str,
        setter: fn(&mut Self, usize),
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let entity = cx.entity();
        let item_ids: Vec<SharedString> = items.iter().map(|item| item.id.clone()).collect();
        let active_id = item_ids.get(active_index).cloned().unwrap_or_default();

        let control = SegmentedControl::new(
            items,
            active_id,
            move |selected: &SharedString, _window, cx| {
                let Some(index) = item_ids.iter().position(|id| id == selected) else {
                    return;
                };

                entity.update(cx, |this, cx| {
                    this.select_row(row);
                    setter(this, index);
                    cx.notify();
                });
            },
        )
        .group(group);

        layout::form_row(
            label,
            div().flex().child(control.focused(self.is_at(row))),
            help.map(SharedString::from),
        )
    }

    /// Select-width form row around `control`, a dropdown or select.
    fn render_gen_dropdown(
        &self,
        label: String,
        help: Option<String>,
        control: impl IntoElement,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        layout::form_row(
            label,
            layout::cursor_ring(
                self.is_at(row),
                div().w(SettingsMetrics::SELECT_WIDTH).child(control),
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

    /// Font family row: a searchable select of the installed families.
    fn render_gen_font_family(
        &self,
        field: FontFamilyField,
        select: &FontFamilySelect,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let (id, label, help) = match field {
            FontFamilyField::Ui => (
                "general-ui-font-family",
                dbflux_i18n::t!("settings.general.ui_font_family.label"),
                dbflux_i18n::t!("settings.general.ui_font_family.help"),
            ),
            FontFamilyField::Editor => (
                "general-editor-font-family",
                dbflux_i18n::t!("settings.general.editor_font_family.label"),
                dbflux_i18n::t!("settings.general.editor_font_family.help"),
            ),
            FontFamilyField::Grid => (
                "general-grid-font-family",
                dbflux_i18n::t!("settings.general.grid_font_family.label"),
                dbflux_i18n::t!("settings.general.grid_font_family.help"),
            ),
        };

        let select = Select::new(select)
            .id(id)
            .appearance(false)
            .font_family(dbflux_components::fonts::ui_family(cx))
            .text_size(Fields::TEXT)
            .icon(AppIcon::ChevronDown)
            .accessibility_label(label.clone())
            .search_placeholder(dbflux_i18n::t!(
                "settings.general.font_family.search_placeholder"
            ))
            .empty(|_, _| {
                div()
                    .p(Fields::PADDING_X)
                    .child(Text::caption(dbflux_i18n::t!(
                        "settings.general.font_family.no_matches"
                    )))
            });
        let control = dbflux_components::composites::control_shell(select, cx);

        self.render_gen_dropdown(label, Some(help), control, row, cx)
    }

    /// Font size row: a numeric field with the pixel unit and a help line.
    fn render_gen_font_size(
        &self,
        label: String,
        help: String,
        input: &Entity<InputState>,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> Div {
        self.render_gen_input_field(
            label,
            input,
            Some(dbflux_i18n::t!("settings.general.unit.pixels")),
            Some(help),
            row,
            cx,
        )
    }

    /// Stable element id for inputs that UI automation addresses by name.
    fn input_element_id(row: GeneralFormRow) -> &'static str {
        match row {
            GeneralFormRow::UiFontSize => "general-ui-font-size",
            GeneralFormRow::EditorFontSize => "general-editor-font-size",
            GeneralFormRow::GridFontSize => "general-grid-font-size",
            GeneralFormRow::MaxHistory => "general-max-history",
            GeneralFormRow::AutoSaveInterval => "general-auto-save",
            GeneralFormRow::DefaultRefreshInterval => "general-refresh-interval",
            GeneralFormRow::MaxBackgroundTasks => "general-max-background-tasks",
            GeneralFormRow::ObjectPreviewLimit => "general-object-preview-limit",
            GeneralFormRow::KeyValueSizeLimit => "general-key-value-size-limit",
            _ => "editor-row-limit",
        }
    }

    /// Element id of the validation error shown under the input of `row`.
    fn input_error_id(row: GeneralFormRow) -> SharedString {
        SharedString::from(format!("{}-error", Self::input_element_id(row)))
    }

    /// Numeric form row: a short mono field with an optional unit inside it,
    /// its validation error when the last save rejected it, and an optional
    /// helper line under it.
    fn render_gen_input_field(
        &self,
        label: String,
        input: &Entity<InputState>,
        unit: Option<String>,
        help: Option<String>,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> Div {
        let element_id = Self::input_element_id(row);
        let error = self
            .gen_field_error
            .as_ref()
            .filter(|(error_row, _)| *error_row == row)
            .map(|(_, message)| message.clone());

        let field = Input::new(input)
            .id(element_id)
            .aria_label(label.clone())
            .when_some(unit, |field, unit| {
                field.suffix(Text::code(unit).muted_foreground())
            });

        let field_bounds = self.field_bounds.clone();
        let form_scroll = self.form_scroll.clone();
        let bounds_recorder = canvas(
            move |bounds, _, _| {
                field_bounds
                    .borrow_mut()
                    .insert(row, (bounds, form_scroll.offset().y));
            },
            |_, _, _, _| {},
        )
        .absolute()
        .top_0()
        .left_0()
        .size_full();

        let control = layout::cursor_ring(
            self.is_at(row) && !self.gen_editing_field,
            div()
                .relative()
                .w(SettingsMetrics::NUMBER_FIELD_WIDTH)
                .font_family(dbflux_components::fonts::editor_family(cx))
                .child(field)
                .child(bounds_recorder),
            cx,
        )
        .w(SettingsMetrics::NUMBER_FIELD_WIDTH)
        .debug_selector(move || format!("{element_id}-control"))
        .on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, window, cx| {
                this.switching_input = true;
                this.select_row(row);
                this.gen_focus_current_input(window, cx);
                cx.notify();
            }),
        );

        layout::form_row(
            label,
            div()
                .flex()
                .flex_col()
                .gap(FormMetrics::HELP_GAP)
                .child(control)
                .when_some(error, |column, error| {
                    let error_id = Self::input_error_id(row);

                    column.child(
                        div()
                            .id(error_id.clone())
                            .debug_selector(move || error_id.to_string())
                            .child(layout::help_text(error).danger()),
                    )
                }),
            help.map(SharedString::from),
        )
        .debug_selector(move || format!("{element_id}-row"))
    }
}
