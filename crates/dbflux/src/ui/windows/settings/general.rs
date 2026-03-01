use crate::ui::dropdown::Dropdown;
use crate::ui::toast::ToastExt;
use dbflux_core::{AppConfig, AppConfigStore};
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::scroll::ScrollableElement;

use super::{GeneralFormRow, SettingsFocus, SettingsSection, SettingsWindow};

impl SettingsWindow {
    // -- Form row list --

    pub(super) fn gen_form_rows(&self) -> Vec<GeneralFormRow> {
        vec![
            // Appearance
            GeneralFormRow::Theme,
            // Startup & Session
            GeneralFormRow::RestoreSession,
            GeneralFormRow::ReopenConnections,
            GeneralFormRow::DefaultFocus,
            GeneralFormRow::MaxHistory,
            GeneralFormRow::AutoSaveInterval,
            // Refresh & Background
            GeneralFormRow::DefaultRefreshPolicy,
            GeneralFormRow::DefaultRefreshInterval,
            GeneralFormRow::MaxBackgroundTasks,
            GeneralFormRow::PauseRefreshOnError,
            GeneralFormRow::RefreshOnlyIfVisible,
            // Execution Safety
            GeneralFormRow::ConfirmDangerous,
            GeneralFormRow::RequiresWhere,
            GeneralFormRow::RequiresPreview,
            // Actions
            GeneralFormRow::SaveButton,
        ]
    }

    fn gen_current_row(&self) -> Option<GeneralFormRow> {
        self.gen_form_rows().get(self.gen_form_cursor).copied()
    }

    // -- Navigation --

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

    pub(super) fn gen_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.gen_current_row() {
            Some(GeneralFormRow::Theme) => {
                self.dropdown_theme.update(cx, |dd, cx| dd.toggle_open(cx));
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
                    .update(cx, |dd, cx| dd.toggle_open(cx));
                cx.notify();
            }
            Some(GeneralFormRow::DefaultRefreshPolicy) => {
                self.dropdown_refresh_policy
                    .update(cx, |dd, cx| dd.toggle_open(cx));
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
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            Some(GeneralFormRow::AutoSaveInterval) => {
                self.input_auto_save.update(cx, |s, cx| s.focus(window, cx));
            }
            Some(GeneralFormRow::DefaultRefreshInterval) => {
                self.input_refresh_interval
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            Some(GeneralFormRow::MaxBackgroundTasks) => {
                self.input_max_bg_tasks
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            _ => {
                self.gen_editing_field = false;
            }
        }
    }

    // -- Save --

    pub(super) fn save_general_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let max_history_str = self.input_max_history.read(cx).value().trim().to_string();
        let max_history = match max_history_str.parse::<usize>() {
            Ok(v) if v >= 10 => v,
            _ => {
                cx.toast_error("Max history entries must be a number >= 10", window);
                return;
            }
        };

        let auto_save_str = self.input_auto_save.read(cx).value().trim().to_string();
        let auto_save_ms = match auto_save_str.parse::<u64>() {
            Ok(v) if v >= 500 => v,
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
            Ok(v) if v >= 1 => v,
            _ => {
                cx.toast_error("Refresh interval must be >= 1 second", window);
                return;
            }
        };

        let max_bg_str = self.input_max_bg_tasks.read(cx).value().trim().to_string();
        let max_bg_tasks = match max_bg_str.parse::<usize>() {
            Ok(v) if v >= 1 => v,
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
            Ok(s) => s,
            Err(e) => {
                cx.toast_error(format!("Cannot save: {}", e), window);
                return;
            }
        };

        let mut config = match store.load() {
            Ok(c) => c,
            Err(e) => {
                log::error!("Failed to load config before save: {}", e);
                AppConfig::default()
            }
        };

        config.general = self.gen_settings.clone();

        if let Err(e) = store.save(&config) {
            log::error!("Failed to save config: {}", e);
            cx.toast_error(format!("Failed to save: {}", e), window);
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

    // -- Rendering --

    pub(super) fn render_general_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;
        let muted_fg = theme.muted_foreground;
        let is_focused = self.focus_area == SettingsFocus::Content
            && self.active_section == SettingsSection::General;
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
                    // -- Appearance --
                    .child(self.render_gen_group_header("Appearance", border, muted_fg))
                    .child(self.render_gen_dropdown(
                        "Theme",
                        &self.dropdown_theme,
                        is_at(GeneralFormRow::Theme),
                        primary,
                    ))
                    // -- Startup & Session --
                    .child(self.render_gen_group_header("Startup & Session", border, muted_fg))
                    .child(self.render_gen_checkbox(
                        "restore-session",
                        "Restore session on startup",
                        self.gen_settings.restore_session_on_startup,
                        is_at(GeneralFormRow::RestoreSession),
                        |this, val| this.gen_settings.restore_session_on_startup = val,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "reopen-conns",
                        "Reopen last connections",
                        self.gen_settings.reopen_last_connections,
                        is_at(GeneralFormRow::ReopenConnections),
                        |this, val| this.gen_settings.reopen_last_connections = val,
                        cx,
                    ))
                    .child(self.render_gen_dropdown(
                        "Default focus",
                        &self.dropdown_default_focus,
                        is_at(GeneralFormRow::DefaultFocus),
                        primary,
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
                    // -- Refresh & Background --
                    .child(self.render_gen_group_header("Refresh & Background", border, muted_fg))
                    .child(self.render_gen_dropdown(
                        "Default refresh policy",
                        &self.dropdown_refresh_policy,
                        is_at(GeneralFormRow::DefaultRefreshPolicy),
                        primary,
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
                        |this, val| this.gen_settings.auto_refresh_pause_on_error = val,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "refresh-visible",
                        "Auto-refresh only if tab is visible",
                        self.gen_settings.auto_refresh_only_if_visible,
                        is_at(GeneralFormRow::RefreshOnlyIfVisible),
                        |this, val| this.gen_settings.auto_refresh_only_if_visible = val,
                        cx,
                    ))
                    // -- Execution Safety --
                    .child(self.render_gen_group_header("Execution Safety", border, muted_fg))
                    .child(self.render_gen_checkbox(
                        "confirm-dangerous",
                        "Confirm dangerous queries",
                        self.gen_settings.confirm_dangerous_queries,
                        is_at(GeneralFormRow::ConfirmDangerous),
                        |this, val| this.gen_settings.confirm_dangerous_queries = val,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "requires-where",
                        "Require WHERE for DELETE/UPDATE",
                        self.gen_settings.dangerous_requires_where,
                        is_at(GeneralFormRow::RequiresWhere),
                        |this, val| this.gen_settings.dangerous_requires_where = val,
                        cx,
                    ))
                    .child(self.render_gen_checkbox(
                        "requires-preview",
                        "Always require preview (ignore suppressions)",
                        self.gen_settings.dangerous_requires_preview,
                        is_at(GeneralFormRow::RequiresPreview),
                        |this, val| this.gen_settings.dangerous_requires_preview = val,
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
            .child(Checkbox::new(id).checked(checked).on_click(cx.listener(
                move |this, val: &bool, _, cx| {
                    setter(this, *val);
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
                            this.focus_area = SettingsFocus::Content;
                            let rows = this.gen_form_rows();
                            if let Some(pos) = rows.iter().position(|r| *r == row) {
                                this.gen_form_cursor = pos;
                            }
                            this.gen_focus_current_input(window, cx);
                            cx.notify();
                        }),
                    )
                    .child(Input::new(input).small()),
            )
    }
}
