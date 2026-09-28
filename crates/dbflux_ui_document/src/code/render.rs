use super::*;
use dbflux_components::composites::{EmptyState, SplitButton, result_tab, result_tab_bar};
use dbflux_components::controls::Checkbox;
use dbflux_components::controls::{Button, ButtonVariant};
use dbflux_components::helpers::text_color_for_active;
use dbflux_components::modals::modal::{Modal, ModalVariant};
use dbflux_components::modals::{modal_code, modal_lead};
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Icon, Kbd, Text,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, EditorMetrics, Fields, ModalMetrics, TableViewMetrics,
};
use dbflux_components::typography::AppFonts;
use dbflux_ui_base::keymap::{CODE_EDITOR_KEY_CONTEXT, RunCommand, last_keystroke};
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use gpui::KeyContext;
use gpui_component::scroll::ScrollableElement;

/// Vertical line between two groups of the toolbar.
fn toolbar_divider(theme: &gpui_component::theme::Theme) -> impl IntoElement {
    div()
        .w(px(1.0))
        .h(TableViewMetrics::DIVIDER_HEIGHT)
        .mx(EditorMetrics::TOOLBAR_DIVIDER_MARGIN_X)
        .bg(theme.border)
}

impl CodeDocument {
    /// The toolbar as pane actions (see [`crate::pane::PaneAction`]): the
    /// same buttons, under the same conditions, so the whole toolbar is
    /// reachable from the pane-actions menu. `entity` is this document, for
    /// the entries that call back into it.
    pub(crate) fn pane_actions(&self, entity: &Entity<Self>) -> Vec<crate::pane::PaneAction> {
        use crate::pane::PaneAction;

        if self.read_only {
            return Vec::new();
        }

        let is_executing = self.state == DocumentState::Executing;
        let is_db_language = self.supports_connection_context();
        let context = ContextId::Editor;
        let mut actions = Vec::new();

        if is_executing {
            actions.push(
                PaneAction::command(
                    "run",
                    dbflux_i18n::t!("document.code.toolbar.cancel"),
                    Command::CancelQuery,
                    context,
                )
                .icon(AppIcon::X),
            );
        } else {
            actions.push(
                PaneAction::command(
                    "run",
                    dbflux_i18n::t!("document.code.toolbar.run"),
                    Command::RunQuery,
                    context,
                )
                .icon(AppIcon::Play)
                .enabled(!self.drift.preflight_running),
            );
        }

        if is_db_language && !is_executing {
            actions.push(
                PaneAction::command(
                    "run-in-new-tab",
                    dbflux_i18n::t!("document.code.toolbar.run_in_new_tab"),
                    Command::RunQueryInNewTab,
                    context,
                )
                .icon(AppIcon::SquarePlay),
            );
        }

        actions.push(
            PaneAction::command(
                "save",
                dbflux_i18n::t!("document.code.toolbar.save"),
                Command::SaveQuery,
                context,
            )
            .icon(AppIcon::Save),
        );

        actions.push(
            PaneAction::callback(
                "format",
                dbflux_i18n::t!("document.code.toolbar.formatter_unavailable"),
                |_window, _cx| {},
            )
            .icon(AppIcon::Zap)
            .enabled(false),
        );

        actions.push(
            PaneAction::command(
                "history",
                dbflux_i18n::t!("document.code.toolbar.query_history"),
                Command::ToggleHistoryDropdown,
                context,
            )
            .icon(AppIcon::History),
        );

        if is_db_language {
            let document = entity.clone();
            actions.push(
                PaneAction::callback(
                    "explain",
                    dbflux_i18n::t!("document.code.toolbar.explain_query"),
                    move |window, cx| {
                        document.update(cx, |document, cx| document.run_explain(window, cx));
                    },
                )
                .icon(AppIcon::Info),
            );
        }

        let document = entity.clone();
        actions.push(
            PaneAction::callback(
                "chart",
                dbflux_i18n::t!("document.code.toolbar.open_in_chart"),
                move |_window, cx| {
                    document.update(cx, |document, cx| document.emit_chart_this_query(cx));
                },
            )
            .icon(AppIcon::ChartColumnBig),
        );

        if is_db_language {
            let refresh_command = if self.runner.is_primary_active() {
                Command::CancelQuery
            } else {
                Command::RunQuery
            };

            actions.push(
                PaneAction::command(
                    "refresh",
                    dbflux_i18n::t!("document.code.toolbar.refresh"),
                    refresh_command,
                    context,
                )
                .icon(AppIcon::RefreshCcw),
            );

            let dropdown = self.refresh.refresh_dropdown.clone();
            actions.push(
                PaneAction::callback(
                    "auto-refresh",
                    dbflux_i18n::t!("document.code.toolbar.auto_refresh_interval"),
                    move |window, cx| {
                        dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
                    },
                )
                .icon(AppIcon::Clock),
            );
        }

        actions.extend(self.results_pane_actions());

        actions
    }

    /// The results header as pane actions: the result tabs (switch, close)
    /// and the maximize and hide buttons, while there are results to show.
    fn results_pane_actions(&self) -> Vec<crate::pane::PaneAction> {
        use crate::pane::PaneAction;

        let mut actions = Vec::new();
        let tab_count = self.result_tabs.result_tabs.len();

        if tab_count > 0 {
            actions.push(
                PaneAction::command(
                    "next-result-tab",
                    dbflux_i18n::t!("document.code.toolbar.next_result_tab"),
                    Command::NextResultTab,
                    ContextId::Results,
                )
                .icon(AppIcon::ChevronRight)
                .enabled(tab_count > 1),
            );
            actions.push(
                PaneAction::command(
                    "previous-result-tab",
                    dbflux_i18n::t!("document.code.toolbar.previous_result_tab"),
                    Command::PrevResultTab,
                    ContextId::Results,
                )
                .icon(AppIcon::ChevronLeft)
                .enabled(tab_count > 1),
            );
            actions.push(
                PaneAction::command(
                    "close-result-tab",
                    dbflux_i18n::t!("document.code.toolbar.close_result_tab"),
                    Command::CloseResultTab,
                    ContextId::Results,
                )
                .icon(AppIcon::CircleX),
            );
        }

        if tab_count == 0 && self.execution.live_output.is_none() {
            return actions;
        }

        let (maximize_label, maximize_icon) = if self.results_maximized {
            (
                dbflux_i18n::t!("document.code.toolbar.restore_results"),
                AppIcon::Minimize2,
            )
        } else {
            (
                dbflux_i18n::t!("document.code.toolbar.maximize_results"),
                AppIcon::Maximize2,
            )
        };
        actions.push(
            PaneAction::command(
                "maximize-results",
                maximize_label,
                Command::ToggleResults,
                ContextId::Editor,
            )
            .icon(maximize_icon),
        );

        let hidden = self.layout == SqlQueryLayout::EditorOnly;
        let (hide_id, hide_label, hide_icon) = if hidden {
            (
                "show-results",
                dbflux_i18n::t!("document.code.toolbar.show_results"),
                AppIcon::PanelBottomOpen,
            )
        } else {
            (
                "hide-results",
                dbflux_i18n::t!("document.code.toolbar.hide_results"),
                AppIcon::PanelBottomClose,
            )
        };
        actions.push(
            PaneAction::command(
                hide_id,
                hide_label,
                Command::ToggleEditor,
                ContextId::Editor,
            )
            .icon(hide_icon),
        );

        actions
    }

    fn render_toolbar(&self, window: &Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_executing = self.state == DocumentState::Executing;
        let is_preflight = self.drift.preflight_running;
        let is_db_language = self.supports_connection_context();
        let is_read_only = self.read_only;

        let auto_refresh_enabled = self.refresh.refresh_policy.is_auto();
        let refresh_label = if auto_refresh_enabled {
            crate::labels::refresh_policy_label(self.refresh.refresh_policy)
        } else {
            dbflux_i18n::t!("document.code.toolbar.refresh")
        };
        let refresh_icon = if is_executing {
            AppIcon::Loader
        } else if auto_refresh_enabled {
            AppIcon::Clock
        } else {
            AppIcon::RefreshCcw
        };

        let (run_icon, run_label, run_enabled) = if is_executing {
            (
                AppIcon::X,
                dbflux_i18n::t!("document.code.toolbar.cancel"),
                true,
            )
        } else if is_preflight {
            (
                AppIcon::Loader,
                dbflux_i18n::t!("document.code.toolbar.checking"),
                false,
            )
        } else {
            (
                AppIcon::Play,
                dbflux_i18n::t!("document.code.toolbar.run"),
                true,
            )
        };

        let execution_time = self
            .execution
            .active_execution_index
            .and_then(|i| self.execution.execution_history.get(i))
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.duration_since(r.started_at))
            });

        // Keep this shortcut in sync with the RunQuery binding (Cmd+Enter on
        // macOS, Ctrl+Enter elsewhere) registered in `keymap::defaults`.
        #[cfg(target_os = "macos")]
        let run_shortcut = "Cmd \u{21B5}";
        #[cfg(not(target_os = "macos"))]
        let run_shortcut = "Ctrl \u{21B5}";

        let show_run_group = !is_read_only && is_db_language && !is_executing;
        let refresh_menu_focused = self.refresh.refresh_dropdown.read(cx).is_focused(window);

        let run_summary = super::statements::run_summary_label(
            self.statement_count().filter(|_| is_db_language),
            execution_time.map(|duration| duration.as_secs_f64()),
        );

        div()
            .id("sql-toolbar")
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(EditorMetrics::TOOLBAR_GAP)
            .h(EditorMetrics::BAR_HEIGHT)
            .px(EditorMetrics::BAR_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.popover)
            .when(!is_read_only, |el| {
                el.child(
                    Button::new("run-query-btn", run_label)
                        .icon(run_icon)
                        .variant(if is_executing {
                            ButtonVariant::Danger
                        } else {
                            ButtonVariant::Primary
                        })
                        .disabled(!run_enabled)
                        .when(!is_executing, |button| button.kbd(run_shortcut))
                        .on_click(cx.listener(move |this, _, window, cx| {
                            if this.state == DocumentState::Executing {
                                this.cancel_query(cx);
                            } else {
                                this.run_query(window, cx);
                            }
                        })),
                )
            })
            .when(show_run_group, |el| {
                el.child(
                    Button::new(
                        "run-in-new-tab-btn",
                        dbflux_i18n::t!("document.code.toolbar.new_tab"),
                    )
                    .icon(AppIcon::SquarePlay)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.run_query_in_new_tab(window, cx);
                    })),
                )
            })
            .when(is_read_only, |el| {
                el.child(
                    Text::caption(dbflux_i18n::t!("document.code.toolbar.read_only"))
                        .muted_foreground(),
                )
            })
            .when(!is_read_only, |el| {
                el.child(toolbar_divider(&theme))
                    .child(self.render_secondary_actions(is_read_only, cx))
            })
            .when(!is_read_only && is_db_language, |el| {
                el.child(toolbar_divider(&theme)).child(
                    SplitButton::new(
                        "sql-refresh-split",
                        Button::new("sql-refresh-action", refresh_label)
                            .icon(refresh_icon)
                            .on_click(cx.listener(|this, _, window, cx| {
                                if this.runner.is_primary_active() {
                                    this.cancel_query(cx);
                                } else {
                                    this.run_query(window, cx);
                                }
                            })),
                        self.refresh.refresh_dropdown.clone(),
                    )
                    .menu_focused(refresh_menu_focused),
                )
            })
            .child(div().flex_1())
            .when_some(run_summary, |el, summary| {
                el.child(
                    div()
                        .id("toolbar-run-summary")
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .gap(EditorMetrics::LAST_RUN_GAP)
                        .font_family(AppFonts::MONO)
                        .text_size(EditorMetrics::LAST_RUN_FONT)
                        .text_color(theme.muted_foreground)
                        .child(
                            Icon::new(AppIcon::History)
                                .size(EditorMetrics::LAST_RUN_ICON)
                                .color(theme.muted_foreground),
                        )
                        .child(summary),
                )
            })
            // A query buffer shows its file state in the context bar.
            .when(self.session.show_saved_label && !is_db_language, |el| {
                el.child(
                    div()
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .gap(EditorMetrics::LAST_RUN_GAP)
                        .font_family(AppFonts::MONO)
                        .text_size(EditorMetrics::LAST_RUN_FONT)
                        .text_color(theme.success)
                        .child(
                            Icon::new(AppIcon::Check)
                                .size(EditorMetrics::LAST_RUN_ICON)
                                .color(theme.success),
                        )
                        .child(dbflux_i18n::t!("document.code.toolbar.saved")),
                )
            })
    }

    /// Renders the icon group: Save, Format, History, Explain, Chart.
    ///
    /// All mutating or execution buttons are hidden when `is_read_only` is true.
    fn render_secondary_actions(
        &self,
        is_read_only: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let history_open = self.history.history_panel.read(cx).is_visible();
        let is_db_language = self.supports_connection_context();

        div()
            .flex()
            .items_center()
            .gap(EditorMetrics::TOOLBAR_GAP)
            // Save button — hidden for read-only documents
            .when(!is_read_only, |el| {
                el.child(
                    Button::new(
                        "toolbar-save-btn",
                        dbflux_i18n::t!("document.code.toolbar.save"),
                    )
                    .icon(AppIcon::Save)
                    .icon_only()
                    .on_click(cx.listener(|this, _, window, cx| {
                        if this.is_file_backed() {
                            this.save_file(window, cx);
                        } else {
                            this.save_file_as(window, cx);
                        }
                    })),
                )
            })
            // Format button — hidden for read-only documents (no formatter available)
            .when(!is_read_only, |el| {
                el.child(
                    Button::new(
                        "toolbar-format-btn",
                        dbflux_i18n::t!("document.code.toolbar.formatter_unavailable"),
                    )
                    .icon(AppIcon::Zap)
                    .icon_only()
                    .disabled(true),
                )
            })
            // History button — hidden for read-only documents
            .when(!is_read_only, |el| {
                el.child(
                    Button::new(
                        "toolbar-history-btn",
                        dbflux_i18n::t!("document.code.toolbar.query_history"),
                    )
                    .icon(AppIcon::History)
                    .icon_only()
                    .selected(history_open)
                    .on_click(cx.listener(|this, _, window, cx| {
                        let is_open = this.history.history_panel.read(cx).is_visible();
                        if is_open {
                            this.history
                                .history_panel
                                .update(cx, |panel, cx| panel.close(cx));
                        } else {
                            this.history
                                .history_panel
                                .update(cx, |panel, cx| panel.open(window, cx));
                        }
                    })),
                )
            })
            // Explain button — hidden for read-only documents
            .when(!is_read_only && is_db_language, |el| {
                el.child(
                    Button::new(
                        "toolbar-explain-btn",
                        dbflux_i18n::t!("document.code.toolbar.explain_query"),
                    )
                    .icon(AppIcon::Info)
                    .icon_only()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.run_explain(window, cx);
                    })),
                )
            })
            // Chart button — hidden for read-only documents
            .when(!is_read_only, |el| {
                el.child(
                    Button::new(
                        "toolbar-chart-btn",
                        dbflux_i18n::t!("document.code.toolbar.open_in_chart"),
                    )
                    .icon(AppIcon::ChartColumnBig)
                    .icon_only()
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.emit_chart_this_query(cx);
                    })),
                )
            })
    }

    fn render_editor(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let bg = cx.theme().background;

        // Focus inside the editor shows through its caret; the pane draws no
        // ring of its own.
        let mut key_context = KeyContext::default();
        key_context.add(CODE_EDITOR_KEY_CONTEXT);
        for (key, value) in self.key_context_entries(cx) {
            key_context.set(key, value);
        }

        div()
            .size_full()
            .flex()
            .flex_col()
            .min_h_0()
            .bg(bg)
            .key_context(key_context)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.enter_editor_mode(cx);
                    this.editor
                        .input_state
                        .update(cx, |state, cx| state.focus(window, cx));
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .capture_action(
                cx.listener(|this, _: &gpui_component::input::Escape, window, cx| {
                    this.clear_vim_count_and_notify(cx);
                    if this.handle_vim_escape_action(window, cx) {
                        cx.stop_propagation();
                    }
                }),
            )
            .capture_action(
                cx.listener(|this, _: &gpui_component::input::Undo, window, cx| {
                    this.clear_vim_count_and_notify(cx);
                    if this.handle_vim_history_action(vim::HistoryStep::Undo, window, cx) {
                        cx.stop_propagation();
                    }
                }),
            )
            .capture_action(
                cx.listener(|this, _: &gpui_component::input::Redo, window, cx| {
                    this.clear_vim_count_and_notify(cx);
                    if this.handle_vim_history_action(vim::HistoryStep::Redo, window, cx) {
                        cx.stop_propagation();
                    }
                }),
            )
            // A keymap binding runs before the key listeners below. Vim takes
            // its own keys ahead of a default binding, as it did when the
            // workspace resolved keys after them; a binding the user made
            // wins over Vim. Any other command drops a half-typed count or
            // operator, and Cancel keeps the editor focused, as the Escape
            // key listener does when no binding takes the key.
            .capture_action(cx.listener(|this, action: &RunCommand, window, cx| {
                if let Some(command) = Command::from_action_id(&action.command)
                    && this.handle_editor_overlay_pane_move(command, window, cx)
                {
                    cx.stop_propagation();
                    return;
                }

                if !action.from_user_binding
                    && let Some(keystroke) = last_keystroke(cx)
                    && this.handle_vim_key_down(
                        &gpui::KeyDownEvent {
                            keystroke,
                            is_held: false,
                            prefer_character_input: false,
                        },
                        window,
                        cx,
                    )
                {
                    cx.stop_propagation();
                    return;
                }

                this.clear_vim_count_and_notify(cx);
                if Command::from_action_id(&action.command) == Some(Command::Cancel) {
                    this.schedule_editor_refocus(window, cx);
                }
            }))
            .capture_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, window, cx| {
                if this.handle_vim_key_down(event, window, cx) {
                    cx.stop_propagation();
                }
            }))
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, window, cx| {
                if event.keystroke.key != "escape"
                    || event.keystroke.modifiers.alt
                    || event.keystroke.modifiers.control
                    || event.keystroke.modifiers.shift
                    || event.keystroke.modifiers.platform
                    || event.keystroke.modifiers.function
                {
                    return;
                }
                this.schedule_editor_refocus(window, cx);
            }))
            .child(
                div().flex_1().min_h_0().overflow_hidden().child(
                    gpui_component::input::Editor::new(&self.editor.input_state)
                        .appearance(false)
                        .readonly(self.editor_input_locked())
                        .text_size(EditorMetrics::CODE_FONT)
                        .line_height(EditorMetrics::CODE_LINE_HEIGHT)
                        .w_full()
                        .h_full(),
                ),
            )
            .when_some(self.vim_mode(), |el, mode| {
                el.child(self.render_vim_mode_indicator(mode, cx))
            })
    }

    fn render_vim_mode_indicator(&self, mode: VimMode, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .id("vim-mode-indicator")
            .flex()
            .flex_none()
            .items_center()
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(Text::caption(crate::labels::vim_mode_label(mode)))
            .when(!self.vim.pending_keys.is_empty(), |el| {
                el.child(
                    div()
                        .id("vim-pending-command")
                        .ml(Spacing::SM)
                        .child(self.vim.pending_keys.clone()),
                )
            })
    }

    fn render_results(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let bg = cx.theme().background;
        let is_executing = self.state == DocumentState::Executing;

        let error = self
            .execution
            .active_execution_index
            .and_then(|i| self.execution.execution_history.get(i))
            .and_then(|r| r.error.clone());

        let has_error = error.is_some();
        let has_live_output = self.execution.live_output.is_some() && !has_error;
        let active_panel = self.active_result_panel();
        let has_panel = active_panel.is_some();
        let has_tabs = !has_live_output && !self.result_tabs.result_tabs.is_empty();

        // The results show focus through their own grid cursor and tabs.
        div()
            .size_full()
            .flex()
            .flex_col()
            .min_h_0()
            .bg(bg)
            .when(has_tabs, |el| el.child(self.render_results_header(cx)))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .when_some(error, |el, err| el.child(self.render_error_state(&err, cx)))
                    .when(has_live_output, |el| el.child(self.render_live_output(cx)))
                    .when(!has_live_output, |el| {
                        el.when_some(active_panel, |el, panel| el.child(panel))
                    })
                    .when(
                        !has_live_output && !has_panel && !has_error && is_executing,
                        |el| el.child(self.render_loading_results(cx)),
                    )
                    .when(
                        !has_live_output && !has_panel && !has_error && !is_executing,
                        |el| el.child(self.render_empty_results(cx)),
                    ),
            )
    }

    fn render_live_output(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let live_output = self
            .execution
            .live_output
            .as_ref()
            .expect("live output state should exist when rendering");

        let status = if self.state == DocumentState::Executing {
            dbflux_i18n::t!("document.code.output.running")
        } else if live_output.is_finished() {
            dbflux_i18n::t!("document.code.output.stopped")
        } else {
            dbflux_i18n::t!("document.code.output.output")
        };

        let text = SharedString::from(live_output.render_text());
        let line_count_label = crate::labels::live_output_lines_label(live_output.line_count());

        div()
            .id("script-live-output")
            .size_full()
            .flex()
            .flex_col()
            .bg(theme.background)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .px(Spacing::MD)
                    .py(Spacing::SM)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(Text::body(status))
                    .child(Text::caption(line_count_label))
                    .when(live_output.has_stderr(), |el| {
                        el.child(Badge::new("stderr", BadgeTone::Warning))
                    }),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .p(Spacing::MD)
                    .child(div().whitespace_nowrap().child(Text::code(text))),
            )
            .when(live_output.is_truncated(), |el| {
                el.child(div().px(Spacing::MD).pb(Spacing::SM).child(Text::caption(
                    crate::labels::live_output_truncated_label(LiveOutputState::MAX_LINES),
                )))
            })
    }

    /// Result tabs strip (AppByzEditor): one tab per result with its row
    /// count and a close button, then the maximize and hide controls.
    fn render_results_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let active_index = self.result_tabs.active_result_index;
        let muted = cx.theme().muted_foreground;

        let tabs: Vec<AnyElement> = self
            .result_tabs
            .result_tabs
            .iter()
            .enumerate()
            .map(|(i, tab)| {
                let is_active = active_index == Some(i);
                let tab_id = tab.id;
                let row_count = tab.grid.read(cx).result().row_count();

                result_tab(
                    ElementId::Name(format!("result-tab-{}", tab.id).into()),
                    tab.title.clone(),
                    Some(crate::labels::row_count_label(row_count).into()),
                    is_active,
                    cx,
                )
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.activate_result_tab(i, cx);
                }))
                .child(
                    div()
                        .id(ElementId::Name(
                            format!("close-result-tab-{}", tab.id).into(),
                        ))
                        .flex()
                        .items_center()
                        .justify_center()
                        .cursor_pointer()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.close_result_tab(tab_id, cx);
                        }))
                        .child(
                            Icon::new(AppIcon::CircleX)
                                .size(Fields::CHEVRON)
                                .color(muted),
                        ),
                )
                .into_any_element()
            })
            .collect();

        result_tab_bar(cx)
            .id("results-header")
            .child(
                div()
                    .flex()
                    .items_end()
                    .h_full()
                    .overflow_x_hidden()
                    .flex_1()
                    .children(tabs),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .h_full()
                    .child(self.render_results_controls(cx)),
            )
    }

    fn render_results_controls(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_maximized = self.results_maximized;

        div()
            .flex()
            .items_center()
            .gap_1()
            .child(
                div()
                    .id("toggle-maximize-results")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size_6()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle_maximize_results(cx);
                    }))
                    .child(
                        Icon::new(if is_maximized {
                            AppIcon::Minimize2
                        } else {
                            AppIcon::Maximize2
                        })
                        .size(px(14.0))
                        .muted(),
                    ),
            )
            .child(
                div()
                    .id("hide-results-panel")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size_6()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.hide_results(cx);
                    }))
                    .child(Icon::new(AppIcon::PanelBottomClose).size(px(14.0)).muted()),
            )
    }

    fn render_collapsed_results_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tab_count = self.result_tabs.result_tabs.len();

        div()
            .id("collapsed-results-bar")
            .flex()
            .items_center()
            .h(Heights::TAB)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(div().flex().items_center().gap_1().child(Text::caption(
                crate::labels::result_tab_count_label(tab_count),
            )))
            .child(div().flex_1())
            .child(
                div()
                    .id("expand-results-panel")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size_6()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.layout = SqlQueryLayout::Split;
                        cx.notify();
                    }))
                    .child(Icon::new(AppIcon::PanelBottomOpen).size(px(14.0)).muted()),
            )
    }

    fn render_loading_results(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        let icon = Icon::new(AppIcon::Loader).size(px(12.0)); // guardrail-allow: 12px icon size, no ICON_XS token
        div().p(Spacing::MD).size_full().child(
            BannerBlock::new(
                BannerVariant::Info,
                dbflux_i18n::t!("document.code.result.loading.title"),
            )
            .with_icon(icon)
            .with_body(dbflux_i18n::t!("document.code.result.loading.body")),
        )
    }

    fn render_error_state(&self, error: &str, _cx: &mut Context<Self>) -> impl IntoElement {
        let icon = Icon::new(AppIcon::CircleX).size(Heights::ICON_SM);
        div().p(Spacing::MD).size_full().overflow_y_hidden().child(
            BannerBlock::new(
                BannerVariant::Danger,
                dbflux_i18n::t!("document.code.result.error.title"),
            )
            .with_icon(icon)
            .with_pre(error.to_string()),
        )
    }

    fn render_empty_results(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        EmptyState::new(
            AppIcon::Table,
            dbflux_i18n::t!("document.code.result.empty"),
        )
    }

    /// Placeholder shown for a routine document when no connection is active for
    /// its profile.  The definition will be fetched automatically on connect.
    fn render_awaiting_connection(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        EmptyState::new(
            AppIcon::Plug,
            dbflux_i18n::t!("document.code.result.awaiting_connection"),
        )
    }

    fn render_script_confirm_modal(&self, cx: &mut Context<Self>) -> impl IntoElement {
        // Capture entity clones for each callback before building the footer.
        let entity_cancel = cx.entity().clone();
        let entity_run = cx.entity().clone();
        let entity_close = cx.entity().clone();
        let entity_confirm = cx.entity().clone();

        let statement_count = self
            .pending
            .script_confirm
            .as_ref()
            .map(|p| p.statement_count)
            .unwrap_or(0);
        let message = crate::labels::script_confirm_message_label(statement_count);

        let body = Text::caption(message).into_any_element();

        let footer = div()
            .flex()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "script-confirm-cancel-btn",
                    dbflux_i18n::t!("document.code.script_confirm.cancel"),
                )
                .on_click(move |_, window, cx| {
                    entity_cancel.update(cx, |doc, cx| {
                        doc.cancel_script_query(window, cx);
                    });
                }),
            )
            .child(
                Button::new(
                    "script-confirm-run-btn",
                    dbflux_i18n::t!("document.code.script_confirm.run"),
                )
                .primary()
                .on_click(move |_, window, cx| {
                    entity_run.update(cx, |doc, cx| {
                        doc.confirm_script_query(window, cx);
                    });
                }),
            )
            .into_any_element();

        Modal::new(dbflux_i18n::t!("document.code.script_confirm.title"))
            .body(body)
            .footer(footer)
            .icon(AppIcon::Play)
            .width(px(460.0))
            .focus_handle(self.script_confirm_focus.handle())
            .on_close(move |window, cx| {
                entity_close.update(cx, |doc, cx| {
                    doc.cancel_script_query(window, cx);
                });
            })
            .on_confirm(move |window, cx| {
                entity_confirm.update(cx, |doc, cx| {
                    doc.confirm_script_query(window, cx);
                });
            })
    }

    fn render_dangerous_query_modal(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let entity_run = cx.entity().clone();
        let entity_cancel = cx.entity().clone();
        let entity_toggle = cx.entity().clone();
        let entity_close = cx.entity().clone();
        let entity_confirm = cx.entity().clone();

        let pending = self.pending.dangerous_query.as_ref();
        let suppress = pending.is_some_and(|pending| pending.suppress);
        let query = pending.map(|pending| pending.query.clone());

        let (title, message) = pending
            .map(|p| {
                (
                    crate::labels::dangerous_query_title(p.kind),
                    crate::labels::dangerous_query_body(p.kind),
                )
            })
            .unwrap_or_else(|| {
                (
                    dbflux_i18n::t!("document.code.dangerous_query.fallback.title"),
                    dbflux_i18n::t!("document.code.dangerous_query.fallback.body"),
                )
            });

        let body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(message, cx))
            .when_some(query, |body, query| {
                body.child(modal_code(query.trim().to_string(), cx))
            })
            .child(
                Checkbox::new("dangerous-dont-ask-again")
                    .checked(suppress)
                    .label(dbflux_i18n::t!(
                        "document.code.dangerous_query.dont_ask_again"
                    ))
                    .on_click(move |checked: &bool, _, cx| {
                        let checked = *checked;
                        entity_toggle.update(cx, |doc, cx| {
                            if let Some(pending) = doc.pending.dangerous_query.as_mut() {
                                pending.suppress = checked;
                                cx.notify();
                            }
                        });
                    }),
            );

        let cancel_btn = Button::new(
            "dangerous-cancel-btn",
            dbflux_i18n::t!("document.code.dangerous_query.cancel"),
        )
        .when_some(
            dbflux_ui_base::keymap::shortcut_label(ContextId::ConfirmModal, Command::Cancel),
            Button::kbd,
        )
        .on_click(move |_, window, cx| {
            entity_cancel.update(cx, |doc, cx| {
                doc.cancel_dangerous_query(window, cx);
            });
        });

        let run_anyway_btn = Button::new(
            "dangerous-confirm-btn",
            dbflux_i18n::t!("document.code.dangerous_query.run_anyway"),
        )
        .danger()
        .icon(AppIcon::Play)
        .when_some(
            dbflux_ui_base::keymap::shortcut_label(ContextId::ConfirmModal, Command::Execute),
            Button::kbd,
        )
        .on_click(move |_, window, cx| {
            entity_run.update(cx, |doc, cx| {
                doc.confirm_dangerous_query(suppress, window, cx);
            });
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(cancel_btn)
            .child(run_anyway_btn)
            .into_any_element();

        Modal::new(title)
            .body(body)
            .footer(footer)
            .icon(AppIcon::TriangleAlert)
            .width(ModalMetrics::WIDTH)
            .variant(ModalVariant::Danger)
            .focus_handle(self.dangerous_query_focus.handle())
            .on_close(move |window, cx| {
                entity_close.update(cx, |doc, cx| {
                    doc.cancel_dangerous_query(window, cx);
                });
            })
            .on_confirm(move |window, cx| {
                entity_confirm.update(cx, |doc, cx| {
                    doc.confirm_dangerous_query(suppress, window, cx);
                });
            })
    }
}

impl Render for CodeDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.sync_statement_gutter_style(cx);

        self.process_pending_result(window, cx);

        self.process_pending_set_query(window, cx);

        self.process_pending_auto_refresh(window, cx);

        if std::mem::take(&mut self.pending.history_focus_restore) {
            self.focus(window, cx);
        }

        self.process_pending_drift_continue(window, cx);

        if let Some((start_value, end_value)) = self.pending.source_input_values.take() {
            // Each `set_value` emits an `InputEvent::Change`; mark both as
            // seed-originated so the subscription handler skips them.
            self.source.source_seed_suppress = self.source.source_seed_suppress.saturating_add(2);

            self.source
                .source_start_input
                .update(cx, |state, cx| state.set_value(&start_value, window, cx));
            self.source
                .source_end_input
                .update(cx, |state, cx| state.set_value(&end_value, window, cx));
        }

        // Lazily create the source-context time-range panel the first time a
        // connection with labelled start/end inputs is active.  Panel creation
        // requires a Window reference (for DatePickerState), so it is deferred
        // here from sync_source_controls which runs in a subscription context.
        if self.source.source_time_range_panel.is_none() && self.should_show_source_controls(cx) {
            let spec = self.current_source_context_spec(cx);
            if spec.is_some_and(|s| !s.start_label.is_empty() && !s.end_label.is_empty()) {
                let panel = cx.new(|cx| {
                    // Index 3 = Last24Hours (24h is the sensible default for time-series sources).
                    TimeRangePanel::new(
                        TimeRangePanel::preset_label(TimeRange::Last24Hours),
                        Some(3),
                        window,
                        cx,
                    )
                });
                let sub = cx.subscribe(&panel, |this, _panel, event: &TimeRangeChanged, cx| {
                    this.on_source_time_range_panel_changed(event.start_ms, event.end_ms, cx);
                });
                self.source.source_time_range_panel = Some(panel.clone());
                self.source._source_time_range_sub = Some(sub);

                // Wire the panel into the active result grid so the chart
                // toolbar's RANGE chips can drive it.
                if let Some(grid) = self
                    .result_tabs
                    .active_result_index
                    .and_then(|i| self.result_tabs.result_tabs.get(i))
                    .map(|t| t.grid.clone())
                {
                    grid.update(cx, |g, cx| {
                        g.set_chart_time_range_panel(Some(panel.clone()), cx);
                    });
                }

                // Seed the initial window for the default preset only when no
                // user-selected window already exists. On panel *recreation*
                // (e.g. after a transient teardown / connection reload) the
                // existing exec_ctx.source carries the user's current selection
                // and must not be overwritten by Last-24h.
                if self.source.exec_ctx.source.is_none() {
                    panel.update(cx, |panel, cx| panel.emit_initial(cx));
                }
            }
        }

        if std::mem::take(&mut self.pending.chart_reexecute)
            && !self.result_tabs.result_tabs.is_empty()
        {
            self.run_query(window, cx);
        }

        if let Some(error) = self.pending.error.take() {
            let toast_msg = error.to_string();
            Toast::error(toast_msg.clone())
                .meta_right(now_hms())
                .action(copy_action(toast_msg))
                .push(cx);
        }

        // Apply a pending routine definition fetched from a background task.
        // `set_content` requires a `Window` reference, so it is deferred here.
        if let Some(body) = self.pending.routine_definition.take() {
            self.set_content(&body, window, cx);
        }

        let context_bar = self.render_context_bar(cx).into_any_element();
        let production_banner = self.render_production_banner(cx);
        let toolbar = self.render_toolbar(window, cx).into_any_element();

        let editor_view = if self.routine_definition_pending {
            self.render_awaiting_connection(cx).into_any_element()
        } else {
            self.render_editor(window, cx).into_any_element()
        };
        let results_view = self.render_results(window, cx).into_any_element();

        let bg = cx.theme().background;
        let has_collapsed_results =
            self.layout == SqlQueryLayout::EditorOnly && !self.result_tabs.result_tabs.is_empty();
        let drift_modal_visible = self.drift.schema_drift_modal.read(cx).is_visible();

        div()
            .id(ElementId::Name(format!("sql-doc-{}", self.id.0).into()))
            .size_full()
            .flex()
            .flex_col()
            .min_h_0()
            .bg(bg)
            .track_focus(&self.focus_handle)
            .child(context_bar)
            .when_some(production_banner, |el, banner| el.child(banner))
            .child(toolbar)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(match self.layout {
                        SqlQueryLayout::Split => {
                            v_resizable(SharedString::from(format!("sql-split-{}", self.id.0)))
                                .child(
                                    resizable_panel()
                                        .size(px(200.0))
                                        .size_range(px(100.0)..px(1000.0))
                                        .child(editor_view),
                                )
                                .child(
                                    resizable_panel()
                                        .size(px(200.0))
                                        .size_range(EditorMetrics::RESULTS_MIN_HEIGHT..px(1000.0))
                                        .child(results_view),
                                )
                                .into_any_element()
                        }

                        SqlQueryLayout::EditorOnly => editor_view,

                        SqlQueryLayout::ResultsOnly => results_view,
                    }),
            )
            .when(has_collapsed_results, |el| {
                el.child(self.render_collapsed_results_bar(cx))
            })
            .when(self.pending.dangerous_query.is_some(), |el| {
                el.child(self.render_dangerous_query_modal(cx))
            })
            .when(self.pending.script_confirm.is_some(), |el| {
                el.child(self.render_script_confirm_modal(cx))
            })
            .when(drift_modal_visible, |el| {
                el.child(self.drift.schema_drift_modal.clone())
            })
    }
}

#[cfg(test)]
mod tests {
    /// The editor and results panes draw no focus ring around themselves:
    /// focus inside a document shows through its own caret, grid cursor and
    /// controls.
    #[test]
    fn code_panes_draw_no_ring_around_the_document() {
        let source = include_str!("render.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("render.rs has production code before its tests");

        for pane in ["fn render_editor(", "fn render_results("] {
            let start = production.find(pane).expect("pane renderer");
            let body = &production[start..start + 600];

            assert!(
                !body.contains("focus_ring("),
                "{pane} wraps itself in a focus ring"
            );
        }
    }
}
