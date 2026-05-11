use super::*;
use crate::ui::components::toast::{Toast, copy_action, now_hms};
use dbflux_components::composites::split_toolbar_action;
use dbflux_components::controls::Button;
use dbflux_components::helpers::text_color_for_active;
use dbflux_components::primitives::{
    Badge, BadgeVariant, BannerBlock, BannerVariant, Icon, Text, focus_frame, overlay_bg,
    surface_panel,
};
use gpui_component::scroll::ScrollableElement;
use gpui_component::tooltip::Tooltip;

fn code_pane_is_focused(focus_mode: SqlQueryFocus, pane: SqlQueryFocus) -> bool {
    focus_mode == pane
}

impl CodeDocument {
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_executing = self.state == DocumentState::Executing;
        let is_preflight = self.drift_preflight_running;
        let is_db_language = self.query_language.supports_connection_context();

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

        let btn_bg = theme.secondary;
        let secondary_hover = theme.secondary_hover;
        let primary = theme.primary;
        let accent = theme.accent;
        let fg = theme.foreground;
        let bg = theme.background;
        let muted_fg = theme.muted_foreground;
        let danger = theme.danger;
        let border = theme.border;
        let tab_bar = theme.tab_bar;

        let execution_time = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.duration_since(r.started_at))
            });

        let shortcut_hint = if is_db_language {
            "Ctrl+Enter (selection/full)"
        } else {
            "Ctrl+Enter"
        };

        div()
            .id("sql-toolbar")
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(border)
            .bg(tab_bar)
            .child(
                div()
                    .id("run-query-btn")
                    .flex()
                    .items_center()
                    .gap_1()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .when(run_enabled, |el| {
                        el.bg(if is_executing { danger } else { primary })
                            .hover(|d| d.opacity(0.9))
                    })
                    .when(!run_enabled, |el| el.bg(btn_bg).cursor_not_allowed())
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if this.state == DocumentState::Executing {
                            this.cancel_query(cx);
                        } else {
                            this.run_query(window, cx);
                        }
                    }))
                    .child(Icon::new(run_icon).size(px(12.0)).color(if run_enabled {
                        bg
                    } else {
                        muted_fg
                    }))
                    .child(if run_enabled {
                        Text::caption(run_label).color(bg)
                    } else {
                        Text::caption(run_label).muted_foreground()
                    }),
            )
            .when(is_db_language && !is_executing, |el| {
                el.child(
                    div()
                        .id("run-in-new-tab-btn")
                        .flex()
                        .items_center()
                        .gap_1()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .bg(btn_bg)
                        .hover(|d| d.bg(secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_query_in_new_tab(window, cx);
                        }))
                        .child(Icon::new(AppIcon::SquarePlay).size(px(12.0)).color(fg))
                        .child(Text::body("New tab").font_size(FontSizes::SM)),
                )
                .child(
                    div()
                        .id("run-selection-btn")
                        .flex()
                        .items_center()
                        .gap_1()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .bg(btn_bg)
                        .hover(|d| d.bg(secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_selected_query(window, cx);
                        }))
                        .child(Icon::new(AppIcon::ScrollText).size(px(12.0)).color(fg))
                        .child(Text::body("Selection").font_size(FontSizes::SM)),
                )
            })
            .child(Text::caption(shortcut_hint))
            .child(self.render_secondary_actions(cx))
            .when(is_db_language, |el| {
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
                        .child(Icon::new(refresh_icon).size(px(12.0)).color(fg))
                        .child(Text::body(refresh_label)),
                    div()
                        .id("sql-refresh-control")
                        .w(px(28.0))
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

    /// Renders the secondary action buttons: Save, Format, History, Explain.
    fn render_secondary_actions(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let secondary = theme.secondary;
        let secondary_hover = theme.secondary_hover;
        let muted_fg = theme.muted_foreground;
        let fg = theme.foreground;

        let is_file_backed = self.is_file_backed();
        let is_db_language = self.query_language.supports_connection_context();

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            // Save button
            .child(
                div()
                    .id("toolbar-save-btn")
                    .h(Heights::BUTTON)
                    .flex()
                    .items_center()
                    .gap_1()
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .bg(secondary)
                    .hover(|d| d.bg(secondary_hover))
                    .on_click(cx.listener(|this, _, window, cx| {
                        if this.is_file_backed() {
                            this.save_file(window, cx);
                        } else {
                            this.save_file_as(window, cx);
                        }
                    }))
                    .tooltip(|window, cx| Tooltip::new("Save").build(window, cx))
                    .child(
                        Icon::new(AppIcon::Save)
                            .size(px(12.0))
                            .color(if is_file_backed { fg } else { muted_fg }),
                    ),
            )
            // Format button — no SQL formatter available; disabled with tooltip
            .child(
                div()
                    .id("toolbar-format-btn")
                    .h(Heights::BUTTON)
                    .flex()
                    .items_center()
                    .gap_1()
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_not_allowed()
                    .bg(secondary)
                    .tooltip(|window, cx| Tooltip::new("Formatter unavailable").build(window, cx))
                    .child(Icon::new(AppIcon::Zap).size(px(12.0)).color(muted_fg)),
            )
            // History button — opens history modal
            .child(
                div()
                    .id("toolbar-history-btn")
                    .h(Heights::BUTTON)
                    .flex()
                    .items_center()
                    .gap_1()
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .bg(secondary)
                    .hover(|d| d.bg(secondary_hover))
                    .on_click(cx.listener(|this, _, window, cx| {
                        let is_open = this.history_modal.read(cx).is_visible();
                        if is_open {
                            this.history_modal.update(cx, |modal, cx| modal.close(cx));
                        } else {
                            this.history_modal
                                .update(cx, |modal, cx| modal.open(window, cx));
                        }
                    }))
                    .tooltip(|window, cx| Tooltip::new("Query history").build(window, cx))
                    .child(Icon::new(AppIcon::History).size(px(12.0)).color(fg)),
            )
            // Explain button — prepends EXPLAIN to the query and runs it (DB languages only)
            .when(is_db_language, |el| {
                el.child(
                    div()
                        .id("toolbar-explain-btn")
                        .h(Heights::BUTTON)
                        .flex()
                        .items_center()
                        .gap_1()
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .bg(secondary)
                        .hover(|d| d.bg(secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_explain(window, cx);
                        }))
                        .tooltip(|window, cx| Tooltip::new("Explain query").build(window, cx))
                        .child(Icon::new(AppIcon::Info).size(px(12.0)).color(fg)),
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
                .child(
                    div().flex_1().min_h_0().overflow_hidden().child(
                        Input::new(&self.input_state)
                            .appearance(false)
                            .w_full()
                            .h_full(),
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
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| r.error.clone());

        let has_error = error.is_some();
        let has_live_output = self.live_output.is_some() && !has_error;
        let active_grid = self.active_result_grid();
        let has_grid = active_grid.is_some();
        let has_tabs = !has_live_output && !self.result_tabs.is_empty();

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
                            el.when_some(active_grid, |el, grid| el.child(grid))
                        })
                        .when(
                            !has_live_output && !has_grid && !has_error && is_executing,
                            |el| el.child(self.render_loading_results(cx)),
                        )
                        .when(
                            !has_live_output && !has_grid && !has_error && !is_executing,
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
            .live_output
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
        let active_index = self.active_result_index;

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
                    .children(self.result_tabs.iter().enumerate().map(|(i, tab)| {
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
                                    .child(Icon::new(AppIcon::X).size(px(12.0)).muted()),
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
        let tab_count = self.result_tabs.len();

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
        let icon = Icon::new(AppIcon::Loader).size(px(12.0));
        div().p(Spacing::MD).size_full().child(
            BannerBlock::new(BannerVariant::Info, "Running…")
                .with_icon(icon)
                .with_body("Query in progress"),
        )
    }

    fn render_error_state(&self, error: &str, _cx: &mut Context<Self>) -> impl IntoElement {
        let icon = Icon::new(AppIcon::CircleX).size(px(16.0));
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

    fn render_dangerous_query_modal(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let entity = cx.entity().clone();
        let entity_cancel = cx.entity().clone();
        let entity_suppress = cx.entity().clone();

        let (title, message) = self
            .pending_dangerous_query
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
                };
                (title, p.kind.message())
            })
            .unwrap_or(("Warning", "This query may be dangerous."));

        div()
            .id("dangerous-query-modal-overlay")
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                surface_panel(cx)
                    .rounded(Radii::MD)
                    .min_w(px(350.0))
                    .max_w(px(500.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .p(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(Icon::new(AppIcon::TriangleAlert).size(px(20.0)).warning())
                            .child(Text::heading(title)),
                    )
                    .child(Text::caption(message))
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
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
                                    .child(Text::caption("Don't ask again")),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap(Spacing::SM)
                                    .child(Button::new("dangerous-cancel-btn", "Cancel").on_click(
                                        move |_, _, cx| {
                                            entity_cancel.update(cx, |doc, cx| {
                                                doc.cancel_dangerous_query(cx);
                                            });
                                        },
                                    ))
                                    .child(
                                        Button::new("dangerous-confirm-btn", "Run Anyway")
                                            .danger()
                                            .on_click(move |_, window, cx| {
                                                entity.update(cx, |doc, cx| {
                                                    doc.confirm_dangerous_query(false, window, cx);
                                                });
                                            }),
                                    ),
                            ),
                    ),
            )
    }
}

impl Render for CodeDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.process_pending_result(window, cx);

        self.process_pending_set_query(window, cx);

        self.process_pending_auto_refresh(window, cx);

        if std::mem::take(&mut self.pending_history_focus_restore) {
            self.focus(window, cx);
        }

        self.process_pending_drift_continue(window, cx);

        if let Some((start_value, end_value)) = self.pending_source_input_values.take() {
            self.source_start_input
                .update(cx, |state, cx| state.set_value(&start_value, window, cx));
            self.source_end_input
                .update(cx, |state, cx| state.set_value(&end_value, window, cx));
        }

        if let Some(error) = self.pending_error.take() {
            let toast_msg = error.to_string();
            Toast::error(toast_msg.clone())
                .meta_right(now_hms())
                .action(copy_action(toast_msg))
                .push(cx);
        }

        let context_bar = self.render_context_bar(cx).into_any_element();
        let toolbar = self.render_toolbar(cx).into_any_element();
        let editor_view = self.render_editor(window, cx).into_any_element();
        let results_view = self.render_results(window, cx).into_any_element();

        let bg = cx.theme().background;
        let has_collapsed_results =
            self.layout == SqlQueryLayout::EditorOnly && !self.result_tabs.is_empty();
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
            .when(self.pending_dangerous_query.is_some(), |el| {
                el.child(self.render_dangerous_query_modal(cx))
            })
            .when(drift_modal_visible, |el| {
                el.child(self.schema_drift_modal.clone())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::code_pane_is_focused;
    use crate::ui::document::code::SqlQueryFocus;

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
