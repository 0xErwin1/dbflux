use super::*;
use crate::chrome::{ToolbarButton, ToolbarButtonVariant, compact_top_bar};
use dbflux_components::composites::split_toolbar_action;
use dbflux_components::controls::Button;
use dbflux_components::helpers::text_color_for_active;
use dbflux_components::modals::shell::{ModalShell, ModalVariant};
use dbflux_components::primitives::{
    Badge, BadgeVariant, BannerBlock, BannerVariant, Icon, Text, focus_frame,
};
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use gpui_component::scroll::ScrollableElement;

fn code_pane_is_focused(focus_mode: SqlQueryFocus, pane: SqlQueryFocus) -> bool {
    focus_mode == pane
}

impl CodeDocument {
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_executing = self.state == DocumentState::Executing;
        let is_preflight = self.drift_preflight_running;
        let is_db_language = self.query_language.supports_connection_context();
        let is_read_only = self.read_only;

        let auto_refresh_enabled = self.refresh_policy.is_auto();
        let refresh_label = if auto_refresh_enabled {
            self.refresh_policy.label()
        } else {
            "Refresh"
        };
        let refresh_icon = if is_executing {
            AppIcon::Loader
        } else if auto_refresh_enabled {
            AppIcon::Clock
        } else {
            AppIcon::RefreshCcw
        };

        let (run_icon, run_label, run_enabled) = if is_executing {
            (AppIcon::X, "Cancel", true)
        } else if is_preflight {
            (AppIcon::Loader, "Checking…", false)
        } else {
            (AppIcon::Play, "Run", true)
        };

        let accent = theme.accent;
        let fg = theme.foreground;

        let execution_time = self
            .execution.active_execution_index
            .and_then(|i| self.execution.execution_history.get(i))
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.duration_since(r.started_at))
            });

        // Keep this label in sync with the RunQuery binding (Cmd+Enter on
        // macOS, Ctrl+Enter elsewhere) registered in `keymap::defaults`.
        #[cfg(target_os = "macos")]
        let shortcut_hint = if is_db_language {
            "Cmd+Enter (selection/full)"
        } else {
            "Cmd+Enter"
        };
        #[cfg(not(target_os = "macos"))]
        let shortcut_hint = if is_db_language {
            "Ctrl+Enter (selection/full)"
        } else {
            "Ctrl+Enter"
        };

        compact_top_bar(&theme, std::iter::empty::<AnyElement>())
            .id("sql-toolbar")
            .when(!is_read_only, |el| {
                el.child(
                    ToolbarButton::new("run-query-btn")
                        .icon(run_icon)
                        .label(run_label)
                        .variant(if is_executing {
                            ToolbarButtonVariant::Danger
                        } else {
                            ToolbarButtonVariant::Primary
                        })
                        .disabled(!run_enabled)
                        .on_click(cx.listener(move |this, _, window, cx| {
                            if this.state == DocumentState::Executing {
                                this.cancel_query(cx);
                            } else {
                                this.run_query(window, cx);
                            }
                        })),
                )
            })
            .when(!is_read_only && is_db_language && !is_executing, |el| {
                el.child(
                    ToolbarButton::new("run-in-new-tab-btn")
                        .icon(AppIcon::SquarePlay)
                        .label("New tab")
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_query_in_new_tab(window, cx);
                        })),
                )
                .child(
                    ToolbarButton::new("run-selection-btn")
                        .icon(AppIcon::ScrollText)
                        .label("Selection")
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_selected_query(window, cx);
                        })),
                )
            })
            .when(!is_read_only, |el| el.child(Text::caption(shortcut_hint)))
            .when(is_read_only, |el| {
                el.child(Text::caption("Read-only").muted_foreground())
            })
            .child(self.render_secondary_actions(is_read_only, cx))
            .when(!is_read_only && is_db_language, |el| {
                el.child(split_toolbar_action(
                    div()
                        .id("sql-refresh-action")
                        .h_full()
                        .px(Spacing::SM)
                        .flex()
                        .items_center()
                        .gap_1()
                        .cursor_pointer()
                        .hover(|d| d.bg(accent.opacity(0.08)))
                        .on_click(cx.listener(|this, _, window, cx| {
                            if this.runner.is_primary_active() {
                                this.cancel_query(cx);
                            } else {
                                this.run_query(window, cx);
                            }
                        }))
                        .child(Icon::new(refresh_icon).small().color(fg))
                        .child(Text::caption(refresh_label)),
                    div()
                        .id("sql-refresh-control")
                        .w(px(28.0)) // guardrail-allow: dropdown control width, not a height token
                        .h_full()
                        .child(self.refresh_dropdown.clone()),
                    cx,
                ))
            })
            .child(div().flex_1())
            .when_some(execution_time, |el, duration| {
                el.child(Text::caption(format!("{:.2}s", duration.as_secs_f64())))
            })
            .when(self.show_saved_label, |el| el.child(Text::caption("Saved")))
    }

    /// Renders the secondary action buttons: Save, Format, History, Explain, Chart.
    ///
    /// All mutating or execution buttons are hidden when `is_read_only` is true.
    fn render_secondary_actions(
        &self,
        is_read_only: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_db_language = self.query_language.supports_connection_context();

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            // Save button — hidden for read-only documents
            .when(!is_read_only, |el| {
                el.child(
                    ToolbarButton::new("toolbar-save-btn")
                        .icon(AppIcon::Save)
                        .tooltip("Save")
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
                    ToolbarButton::new("toolbar-format-btn")
                        .icon(AppIcon::Zap)
                        .tooltip("Formatter unavailable")
                        .disabled(true),
                )
            })
            // History button — hidden for read-only documents
            .when(!is_read_only, |el| {
                el.child(
                    ToolbarButton::new("toolbar-history-btn")
                        .icon(AppIcon::History)
                        .tooltip("Query history")
                        .on_click(cx.listener(|this, _, window, cx| {
                            let is_open = this.history_modal.read(cx).is_visible();
                            if is_open {
                                this.history_modal.update(cx, |modal, cx| modal.close(cx));
                            } else {
                                this.history_modal
                                    .update(cx, |modal, cx| modal.open(window, cx));
                            }
                        })),
                )
            })
            // Explain button — hidden for read-only documents
            .when(!is_read_only && is_db_language, |el| {
                el.child(
                    ToolbarButton::new("toolbar-explain-btn")
                        .icon(AppIcon::Info)
                        .tooltip("Explain query")
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_explain(window, cx);
                        })),
                )
            })
            // Chart button — hidden for read-only documents
            .when(!is_read_only, |el| {
                el.child(
                    ToolbarButton::new("toolbar-chart-btn")
                        .icon(AppIcon::ChartSpline)
                        .tooltip("Open current query in a chart document")
                        .on_click(cx.listener(|this, _, _window, cx| {
                            this.emit_chart_this_query(cx);
                        })),
                )
            })
    }

    fn render_editor(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = code_pane_is_focused(self.focus_mode, SqlQueryFocus::Editor);
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        focus_frame(
            is_focused,
            Some(accent.opacity(0.3)),
            div()
                .size_full()
                .flex()
                .flex_col()
                .min_h_0()
                .bg(bg)
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| {
                        this.enter_editor_mode(cx);
                        this.input_state
                            .update(cx, |state, cx| state.focus(window, cx));
                        cx.emit(DocumentEvent::RequestFocus);
                    }),
                )
                // gpui-component's completion menu hides itself on Esc via
                // InputState::escape but never restores focus to the editor
                // input. Synchronously the input still owns focus when we
                // observe Esc, but the menu's cx.notify() + the resulting
                // re-render reset window.focus before the next paint — so we
                // refocus on the next tick rather than inline.
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
                    if this.focus_mode != SqlQueryFocus::Editor {
                        return;
                    }
                    let input = this.input_state.clone();
                    cx.spawn_in(window, async move |_this, cx| {
                        cx.update(|window, cx| {
                            input.update(cx, |state, cx| state.focus(window, cx));
                        })
                        .ok();
                    })
                    .detach();
                }))
                .child(
                    div().flex_1().min_h_0().overflow_hidden().child(
                        Input::new(&self.input_state)
                            .appearance(false)
                            .w_full()
                            .h_full()
                            // Propagate read-only into the Input component so the
                            // underlying InputState has its `disabled` flag set
                            // during render. This prevents all text-mutating actions
                            // (backspace, delete, paste, undo, redo) from being
                            // bound while keeping selection and copy fully functional.
                            .disabled(self.read_only),
                    ),
                ),
            cx,
        )
        .size_full()
    }

    fn render_results(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = code_pane_is_focused(self.focus_mode, SqlQueryFocus::Results);
        let bg = cx.theme().background;
        let accent = cx.theme().accent;
        let is_executing = self.state == DocumentState::Executing;

        let error = self
            .execution.active_execution_index
            .and_then(|i| self.execution.execution_history.get(i))
            .and_then(|r| r.error.clone());

        let has_error = error.is_some();
        let has_live_output = self.execution.live_output.is_some() && !has_error;
        let active_panel = self.active_result_panel();
        let has_panel = active_panel.is_some();
        let has_tabs = !has_live_output && !self.result_tabs.result_tabs.is_empty();

        focus_frame(
            is_focused,
            Some(accent.opacity(0.3)),
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
                ),
            cx,
        )
        .size_full()
    }

    fn render_live_output(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let live_output = self
            .execution.live_output
            .as_ref()
            .expect("live output state should exist when rendering");

        let status = if self.state == DocumentState::Executing {
            "Running..."
        } else if live_output.is_finished() {
            "Stopped"
        } else {
            "Output"
        };

        let text = SharedString::from(live_output.render_text());
        let line_count = live_output.line_count();

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
                    .child(Text::label(status))
                    .child(Text::caption(format!("{} lines", line_count)))
                    .when(live_output.has_stderr(), |el| {
                        el.child(Badge::new("stderr", BadgeVariant::Warning))
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
                el.child(
                    div()
                        .px(Spacing::MD)
                        .pb(Spacing::SM)
                        .child(Text::caption("(truncated at 5000 lines)")),
                )
            })
    }

    fn render_results_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active_index = self.result_tabs.active_result_index;

        div()
            .id("results-header")
            .flex()
            .items_center()
            .h(Heights::TAB)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .overflow_x_hidden()
                    .flex_1()
                    .children(self.result_tabs.result_tabs.iter().enumerate().map(|(i, tab)| {
                        let is_active = active_index == Some(i);
                        let tab_id = tab.id;

                        div()
                            .id(ElementId::Name(format!("result-tab-{}", tab.id).into()))
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .when(is_active, |el| el.bg(theme.secondary))
                            .when(!is_active, |el| {
                                el.hover(|d| d.bg(theme.secondary.opacity(0.5)))
                            })
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.activate_result_tab(i, cx);
                            }))
                            .child(
                                Text::caption(tab.title.clone())
                                    .color(text_color_for_active(is_active, theme)),
                            )
                            .child(
                                div()
                                    .id(ElementId::Name(
                                        format!("close-result-tab-{}", tab.id).into(),
                                    ))
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .size_4()
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.danger.opacity(0.2)))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.close_result_tab(tab_id, cx);
                                    }))
                                    .child(Icon::new(AppIcon::X).size(px(12.0)).muted()), // guardrail-allow: 12px icon size, no ICON_XS token
                            )
                    })),
            )
            .child(div().flex_1())
            .child(self.render_results_controls(cx))
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
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .child(Text::caption(format!(
                        "{} result{}",
                        tab_count,
                        if tab_count == 1 { "" } else { "s" }
                    ))),
            )
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
            BannerBlock::new(BannerVariant::Info, "Running…")
                .with_icon(icon)
                .with_body("Query in progress"),
        )
    }

    fn render_error_state(&self, error: &str, _cx: &mut Context<Self>) -> impl IntoElement {
        let icon = Icon::new(AppIcon::CircleX).size(Heights::ICON_SM);
        div().p(Spacing::MD).size_full().overflow_y_hidden().child(
            BannerBlock::new(BannerVariant::Danger, "Query Error")
                .with_icon(icon)
                .with_pre(error.to_string()),
        )
    }

    fn render_empty_results(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .child(Text::muted("Run a query to see results"))
    }

    /// Placeholder shown for a routine document when no connection is active for
    /// its profile.  The definition will be fetched automatically on connect.
    fn render_awaiting_connection(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .child(Text::muted(
                "Connect to this database to view the routine definition.",
            ))
    }

    fn render_script_confirm_modal(&self, cx: &mut Context<Self>) -> impl IntoElement {
        // Capture entity clones for each callback before building the footer.
        let entity_cancel = cx.entity().clone();
        let entity_run = cx.entity().clone();
        let entity_close = cx.entity().clone();

        let statement_count = self
            .pending.script_confirm
            .as_ref()
            .map(|p| p.statement_count)
            .unwrap_or(0);
        let message = format!(
            "No text is selected, so the entire script will run as {} statements in order. Continue?",
            statement_count
        );

        let body = Text::caption(message).into_any_element();

        let footer = div()
            .flex()
            .gap(Spacing::SM)
            .child(
                Button::new("script-confirm-cancel-btn", "Cancel").on_click(move |_, _, cx| {
                    entity_cancel.update(cx, |doc, cx| {
                        doc.cancel_script_query(cx);
                    });
                }),
            )
            .child(
                Button::new("script-confirm-run-btn", "Run Script").on_click(
                    move |_, window, cx| {
                        entity_run.update(cx, |doc, cx| {
                            doc.confirm_script_query(window, cx);
                        });
                    },
                ),
            )
            .into_any_element();

        ModalShell::new("Run entire script", body, footer)
            .width(px(460.0))
            .on_close(move |_, cx| {
                entity_close.update(cx, |doc, cx| {
                    doc.cancel_script_query(cx);
                });
            })
    }

    fn render_dangerous_query_modal(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        // Capture entity clones for each callback before building the footer.
        let entity_run = cx.entity().clone();
        let entity_cancel = cx.entity().clone();
        let entity_suppress = cx.entity().clone();
        let entity_close = cx.entity().clone();

        let (title, message) = self
            .pending.dangerous_query
            .as_ref()
            .map(|p| {
                let title = match p.kind {
                    DangerousQueryKind::DeleteNoWhere => "DELETE without WHERE",
                    DangerousQueryKind::UpdateNoWhere => "UPDATE without WHERE",
                    DangerousQueryKind::Truncate => "TRUNCATE",
                    DangerousQueryKind::Drop => "DROP",
                    DangerousQueryKind::Alter => "ALTER",
                    DangerousQueryKind::Script => "Dangerous Script",
                    DangerousQueryKind::MongoDeleteMany => "deleteMany with empty filter",
                    DangerousQueryKind::MongoUpdateMany => "updateMany with empty filter",
                    DangerousQueryKind::MongoDropCollection => "drop() collection",
                    DangerousQueryKind::MongoDropDatabase => "dropDatabase()",
                    DangerousQueryKind::RedisFlushAll => "FLUSHALL",
                    DangerousQueryKind::RedisFlushDb => "FLUSHDB",
                    DangerousQueryKind::RedisMultiDelete => "DEL (multiple keys)",
                    DangerousQueryKind::RedisKeysPattern => "KEYS pattern",
                    DangerousQueryKind::RawExpressionInSet => "Raw SQL expression in SET",
                };
                (title, p.kind.message())
            })
            .unwrap_or(("Warning", "This query may be dangerous."));

        let body = Text::caption(message).into_any_element();

        let dont_ask_chip = div()
            .id("dont-ask-again-btn")
            .flex()
            .items_center()
            .gap_1()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary))
            .on_click(move |_, window, cx| {
                entity_suppress.update(cx, |doc, cx| {
                    doc.confirm_dangerous_query(true, window, cx);
                });
            })
            .child(Text::caption("Don't ask again"));

        let cancel_btn = Button::new("dangerous-cancel-btn", "Cancel").on_click(move |_, _, cx| {
            entity_cancel.update(cx, |doc, cx| {
                doc.cancel_dangerous_query(cx);
            });
        });

        let run_anyway_btn = Button::new("dangerous-confirm-btn", "Run Anyway")
            .danger()
            .on_click(move |_, window, cx| {
                entity_run.update(cx, |doc, cx| {
                    doc.confirm_dangerous_query(false, window, cx);
                });
            });

        // Footer: "Don't ask again" left-aligned, Cancel + Run Anyway right-aligned.
        // The flex_1 spacer pushes the buttons to the right within the single footer AnyElement.
        let footer = div()
            .flex()
            .items_center()
            .child(dont_ask_chip)
            .child(div().flex_1())
            .child(
                div()
                    .flex()
                    .gap(Spacing::SM)
                    .child(cancel_btn)
                    .child(run_anyway_btn),
            )
            .into_any_element();

        ModalShell::new(title, body, footer)
            .width(px(460.0))
            .variant(ModalVariant::Danger)
            .on_close(move |_, cx| {
                entity_close.update(cx, |doc, cx| {
                    doc.cancel_dangerous_query(cx);
                });
            })
    }
}

impl Render for CodeDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
            self.source.source_seed_suppress =
                self.source.source_seed_suppress.saturating_add(2);

            self.source.source_start_input
                .update(cx, |state, cx| state.set_value(&start_value, window, cx));
            self.source.source_end_input
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
                    TimeRangePanel::new("24h", Some(3), window, cx)
                });
                let sub = cx.subscribe(&panel, |this, _panel, event: &TimeRangeChanged, cx| {
                    this.on_source_time_range_panel_changed(event.start_ms, event.end_ms, cx);
                });
                self.source.source_time_range_panel = Some(panel.clone());
                self.source._source_time_range_sub = Some(sub);

                // Wire the panel into the active result grid so the chart
                // toolbar's RANGE chips can drive it.
                if let Some(grid) = self
                    .result_tabs.active_result_index
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

        if std::mem::take(&mut self.pending.chart_reexecute) && !self.result_tabs.result_tabs.is_empty() {
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
        let toolbar = self.render_toolbar(cx).into_any_element();

        let editor_view = if self.routine_definition_pending {
            self.render_awaiting_connection(cx).into_any_element()
        } else {
            self.render_editor(window, cx).into_any_element()
        };
        let results_view = self.render_results(window, cx).into_any_element();

        let bg = cx.theme().background;
        let has_collapsed_results =
            self.layout == SqlQueryLayout::EditorOnly && !self.result_tabs.result_tabs.is_empty();
        let drift_modal_visible = self.schema_drift_modal.read(cx).is_visible();

        div()
            .id(ElementId::Name(format!("sql-doc-{}", self.id.0).into()))
            .size_full()
            .flex()
            .flex_col()
            .min_h_0()
            .bg(bg)
            .track_focus(&self.focus_handle)
            .child(context_bar)
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
                                        .size_range(px(100.0)..px(1000.0))
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
            .child(self.history_modal.clone())
            .when(self.pending.dangerous_query.is_some(), |el| {
                el.child(self.render_dangerous_query_modal(cx))
            })
            .when(self.pending.script_confirm.is_some(), |el| {
                el.child(self.render_script_confirm_modal(cx))
            })
            .when(drift_modal_visible, |el| {
                el.child(self.schema_drift_modal.clone())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::code_pane_is_focused;
    use crate::code::SqlQueryFocus;

    #[test]
    fn editor_focus_shell_tracks_editor_mode_only() {
        assert!(code_pane_is_focused(
            SqlQueryFocus::Editor,
            SqlQueryFocus::Editor,
        ));
        assert!(!code_pane_is_focused(
            SqlQueryFocus::Results,
            SqlQueryFocus::Editor,
        ));
    }

    #[test]
    fn results_focus_shell_tracks_results_mode_only() {
        assert!(code_pane_is_focused(
            SqlQueryFocus::Results,
            SqlQueryFocus::Results,
        ));
        assert!(!code_pane_is_focused(
            SqlQueryFocus::ContextBar,
            SqlQueryFocus::Results,
        ));
    }
}
