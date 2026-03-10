use crate::keymap::{KeyChord, Modifiers};
use crate::ui::components::dropdown::Dropdown;
use crate::ui::components::toast::ToastExt;
use dbflux_core::{AppConfig, AppConfigStore};
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::scroll::ScrollableElement;

use super::general_section::{GeneralFormRow, GeneralSection};

impl GeneralSection {
    pub(super) fn has_unsaved_general_changes(&self, cx: &App) -> bool {
        let saved = self.app_state.read(cx).general_settings();

        if self.gen_settings.theme != saved.theme
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

        self.input_max_bg_tasks.read(cx).value().trim()
            != saved.max_concurrent_background_tasks.to_string()
    }

    pub(super) fn gen_form_rows(&self) -> Vec<GeneralFormRow> {
        vec![
            GeneralFormRow::Theme,
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
            GeneralFormRow::SaveButton,
        ]
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
            Some(GeneralFormRow::MaxHistory)
            | Some(GeneralFormRow::AutoSaveInterval)
            | Some(GeneralFormRow::DefaultRefreshInterval)
            | Some(GeneralFormRow::MaxBackgroundTasks) => {
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
        let chord = KeyChord::from_gpui(&event.keystroke);

        if self.gen_editing_field {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.gen_editing_field = false;
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
                cx.toast_error("Max history entries must be a number >= 10", window);
                return;
            }
        };

        let auto_save_str = self.input_auto_save.read(cx).value().trim().to_string();
        let auto_save_ms = match auto_save_str.parse::<u64>() {
            Ok(value) if value >= 500 => value,
            _ => {
                cx.toast_error("Auto-save interval must be >= 500 ms", window);
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
                cx.toast_error("Refresh interval must be >= 1 second", window);
                return;
            }
        };

        let max_bg_str = self.input_max_bg_tasks.read(cx).value().trim().to_string();
        let max_bg_tasks = match max_bg_str.parse::<usize>() {
            Ok(value) if value >= 1 => value,
            _ => {
                cx.toast_error("Max background tasks must be >= 1", window);
                return;
            }
        };

        self.gen_settings.max_history_entries = max_history;
        self.gen_settings.auto_save_interval_ms = auto_save_ms;
        self.gen_settings.default_refresh_interval_secs = refresh_interval;
        self.gen_settings.max_concurrent_background_tasks = max_bg_tasks;

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
                log::error!("Failed to load config before save: {}", error);
                AppConfig::default()
            }
        };

        config.general = self.gen_settings.clone();

        if let Err(error) = store.save(&config) {
            log::error!("Failed to save config: {}", error);
            cx.toast_error(format!("Failed to save: {}", error), window);
            return;
        }

        self.app_state.update(cx, |state, _cx| {
            state.update_general_settings(self.gen_settings.clone());
        });

        crate::ui::theme::apply_theme(self.gen_settings.theme, Some(window), cx);

        cx.toast_success(
            "Settings saved. Some changes apply on next startup.",
            window,
        );
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

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("General"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(muted_fg)
                            .child("Configure startup, session, refresh, and safety behavior"),
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
                    .gap_6()
                    .child(self.render_gen_group_header("Appearance", border, muted_fg))
                    .child(self.render_gen_dropdown(
                        "Theme",
                        &self.dropdown_theme,
                        is_at(GeneralFormRow::Theme),
                        primary,
                        GeneralFormRow::Theme,
                        cx,
                    ))
                    .child(self.render_gen_group_header("Startup & Session", border, muted_fg))
                    .child(self.render_gen_checkbox(
                        "restore-session",
                        "Restore session on startup",
                        self.gen_settings.restore_session_on_startup,
                        is_at(GeneralFormRow::RestoreSession),
                        GeneralFormRow::RestoreSession,
                        |this, value| this.gen_settings.restore_session_on_startup = value,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "reopen-conns",
                        "Reopen last connections",
                        self.gen_settings.reopen_last_connections,
                        is_at(GeneralFormRow::ReopenConnections),
                        GeneralFormRow::ReopenConnections,
                        |this, value| this.gen_settings.reopen_last_connections = value,
                        cx,
                    ))
                    .child(self.render_gen_dropdown(
                        "Default focus",
                        &self.dropdown_default_focus,
                        is_at(GeneralFormRow::DefaultFocus),
                        primary,
                        GeneralFormRow::DefaultFocus,
                        cx,
                    ))
                    .child(self.render_gen_input_field(
                        "Max history entries",
                        &self.input_max_history,
                        is_at(GeneralFormRow::MaxHistory),
                        primary,
                        GeneralFormRow::MaxHistory,
                        cx,
                    ))
                    .child(self.render_gen_input_field(
                        "Auto-save interval (ms)",
                        &self.input_auto_save,
                        is_at(GeneralFormRow::AutoSaveInterval),
                        primary,
                        GeneralFormRow::AutoSaveInterval,
                        cx,
                    ))
                    .child(self.render_gen_group_header("Refresh & Background", border, muted_fg))
                    .child(self.render_gen_dropdown(
                        "Default refresh policy",
                        &self.dropdown_refresh_policy,
                        is_at(GeneralFormRow::DefaultRefreshPolicy),
                        primary,
                        GeneralFormRow::DefaultRefreshPolicy,
                        cx,
                    ))
                    .child(self.render_gen_input_field(
                        "Default refresh interval (seconds)",
                        &self.input_refresh_interval,
                        is_at(GeneralFormRow::DefaultRefreshInterval),
                        primary,
                        GeneralFormRow::DefaultRefreshInterval,
                        cx,
                    ))
                    .child(self.render_gen_input_field(
                        "Max concurrent background tasks",
                        &self.input_max_bg_tasks,
                        is_at(GeneralFormRow::MaxBackgroundTasks),
                        primary,
                        GeneralFormRow::MaxBackgroundTasks,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "pause-on-error",
                        "Pause auto-refresh on error",
                        self.gen_settings.auto_refresh_pause_on_error,
                        is_at(GeneralFormRow::PauseRefreshOnError),
                        GeneralFormRow::PauseRefreshOnError,
                        |this, value| this.gen_settings.auto_refresh_pause_on_error = value,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "refresh-visible",
                        "Auto-refresh only if tab is visible",
                        self.gen_settings.auto_refresh_only_if_visible,
                        is_at(GeneralFormRow::RefreshOnlyIfVisible),
                        GeneralFormRow::RefreshOnlyIfVisible,
                        |this, value| this.gen_settings.auto_refresh_only_if_visible = value,
                        cx,
                    ))
                    .child(self.render_gen_group_header("Execution Safety", border, muted_fg))
                    .child(self.render_gen_checkbox(
                        "confirm-dangerous",
                        "Confirm dangerous queries",
                        self.gen_settings.confirm_dangerous_queries,
                        is_at(GeneralFormRow::ConfirmDangerous),
                        GeneralFormRow::ConfirmDangerous,
                        |this, value| this.gen_settings.confirm_dangerous_queries = value,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "requires-where",
                        "Require WHERE for DELETE/UPDATE",
                        self.gen_settings.dangerous_requires_where,
                        is_at(GeneralFormRow::RequiresWhere),
                        GeneralFormRow::RequiresWhere,
                        |this, value| this.gen_settings.dangerous_requires_where = value,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "requires-preview",
                        "Always require preview (ignore suppressions)",
                        self.gen_settings.dangerous_requires_preview,
                        is_at(GeneralFormRow::RequiresPreview),
                        GeneralFormRow::RequiresPreview,
                        |this, value| this.gen_settings.dangerous_requires_preview = value,
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
                        let is_save_focused = is_at(GeneralFormRow::SaveButton);

                        div()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_save_focused {
                                primary
                            } else {
                                gpui::transparent_black()
                            })
                            .child(
                                Button::new("save-general")
                                    .label("Save")
                                    .small()
                                    .primary()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.content_focused = true;
                                        this.gen_form_cursor = this
                                            .gen_form_rows()
                                            .iter()
                                            .position(|row| *row == GeneralFormRow::SaveButton)
                                            .unwrap_or_default();
                                        this.save_general_settings(window, cx);
                                    })),
                            )
                    }),
            )
    }

    fn render_gen_group_header(
        &self,
        label: &str,
        border: Hsla,
        muted_fg: Hsla,
    ) -> impl IntoElement {
        div().pt_2().pb_1().border_b_1().border_color(border).child(
            div()
                .text_sm()
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(muted_fg)
                .child(label.to_string()),
        )
    }

    fn render_gen_checkbox(
        &self,
        id: &'static str,
        label: &'static str,
        checked: bool,
        is_focused: bool,
        row: GeneralFormRow,
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
                    setter(this, *value);
                    cx.notify();
                },
            )))
            .child(div().text_sm().child(label))
    }

    fn render_gen_dropdown(
        &self,
        label: &str,
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
                        .gen_form_rows()
                        .iter()
                        .position(|candidate| *candidate == row)
                    {
                        this.gen_form_cursor = position;
                    }
                    cx.notify();
                }),
            )
            .child(div().text_sm().child(label.to_string()))
            .child(div().min_w(px(140.0)).child(dropdown.clone()))
    }

    fn render_gen_input_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        row: GeneralFormRow,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(label.to_string()),
            )
            .child(
                div()
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(if is_focused {
                        primary
                    } else {
                        gpui::transparent_black()
                    })
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
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
                    .child(Input::new(input).small()),
            )
    }
}
