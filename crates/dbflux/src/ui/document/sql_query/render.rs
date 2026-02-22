use super::*;

impl SqlQueryDocument {
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_executing = self.state == DocumentState::Executing;
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
        } else {
            (AppIcon::Play, "Run", true)
        };

        let btn_bg = theme.secondary;
        let primary = theme.primary;

        let execution_time = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.duration_since(r.started_at))
            });

        div()
            .id("sql-toolbar")
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
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
                    .text_xs()
                    .when(run_enabled, |el| {
                        el.bg(if is_executing { theme.danger } else { primary })
                            .text_color(theme.background)
                            .hover(|d| d.opacity(0.9))
                    })
                    .when(!run_enabled, |el| {
                        el.bg(btn_bg)
                            .text_color(theme.muted_foreground)
                            .cursor_not_allowed()
                    })
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if this.state == DocumentState::Executing {
                            this.cancel_query(cx);
                        } else {
                            this.run_query(window, cx);
                        }
                    }))
                    .child(
                        svg()
                            .path(run_icon.path())
                            .size_3()
                            .text_color(if run_enabled {
                                theme.background
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .child(run_label),
            )
            .when(!is_executing, |el| {
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
                        .text_xs()
                        .bg(btn_bg)
                        .text_color(theme.foreground)
                        .hover(|d| d.bg(theme.secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_query_in_new_tab(window, cx);
                        }))
                        .child(
                            svg()
                                .path(AppIcon::SquarePlay.path())
                                .size_3()
                                .text_color(theme.foreground),
                        )
                        .child("New tab"),
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
                        .text_xs()
                        .bg(btn_bg)
                        .text_color(theme.foreground)
                        .hover(|d| d.bg(theme.secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_selected_query(window, cx);
                        }))
                        .child(
                            svg()
                                .path(AppIcon::ScrollText.path())
                                .size_3()
                                .text_color(theme.foreground),
                        )
                        .child("Selection"),
                )
            })
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Ctrl+Enter (selection/full)"),
            )
            .child(
                div()
                    .id("sql-refresh-control")
                    .flex()
                    .items_center()
                    .gap_0()
                    .h(Heights::BUTTON)
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.input)
                    .rounded(Radii::SM)
                    .child(
                        div()
                            .id("sql-refresh-action")
                            .h_full()
                            .px(Spacing::SM)
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_sm()
                            .cursor_pointer()
                            .text_color(theme.foreground)
                            .hover(|d| d.bg(theme.accent.opacity(0.08)))
                            .on_click(cx.listener(|this, _, window, cx| {
                                if this.runner.is_primary_active() {
                                    this.cancel_query(cx);
                                } else {
                                    this.run_query(window, cx);
                                }
                            }))
                            .child(
                                svg()
                                    .path(refresh_icon.path())
                                    .size_3()
                                    .text_color(theme.foreground),
                            )
                            .child(refresh_label),
                    )
                    .child(div().w(px(1.0)).h_full().bg(theme.input))
                    .child(
                        div()
                            .w(px(28.0))
                            .h_full()
                            .child(self.refresh_dropdown.clone()),
                    ),
            )
            .child(div().flex_1())
            .when_some(execution_time, |el, duration| {
                el.child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(format!("{:.2}s", duration.as_secs_f64())),
                )
            })
    }

    fn render_editor(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = self.focus_mode == SqlQueryFocus::Editor;
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        div()
            .size_full()
            .flex()
            .flex_col()
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
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .child(
                div().flex_1().overflow_hidden().child(
                    Input::new(&self.input_state)
                        .appearance(false)
                        .w_full()
                        .h_full(),
                ),
            )
    }

    fn render_results(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = self.focus_mode == SqlQueryFocus::Results;
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        let error = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| r.error.clone());

        let has_error = error.is_some();
        let active_grid = self.active_result_grid();
        let has_grid = active_grid.is_some();
        let has_tabs = !self.result_tabs.is_empty();

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .when(has_tabs, |el| el.child(self.render_results_header(cx)))
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .when_some(error, |el, err| el.child(self.render_error_state(&err, cx)))
                    .when_some(active_grid, |el, grid| el.child(grid))
                    .when(!has_grid && !has_error, |el| {
                        el.child(self.render_empty_results(cx))
                    }),
            )
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
                            .text_xs()
                            .when(is_active, |el| {
                                el.bg(theme.secondary).text_color(theme.foreground)
                            })
                            .when(!is_active, |el| {
                                el.text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary.opacity(0.5)))
                            })
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.activate_result_tab(i, cx);
                            }))
                            .child(tab.title.clone())
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
                                    .child(
                                        svg()
                                            .path(AppIcon::X.path())
                                            .size_3()
                                            .text_color(theme.muted_foreground),
                                    ),
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
                        svg()
                            .path(if is_maximized {
                                AppIcon::Minimize2.path()
                            } else {
                                AppIcon::Maximize2.path()
                            })
                            .size_3p5()
                            .text_color(theme.muted_foreground),
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
                    .child(
                        svg()
                            .path(AppIcon::PanelBottomClose.path())
                            .size_3p5()
                            .text_color(theme.muted_foreground),
                    ),
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
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!(
                        "{} result{}",
                        tab_count,
                        if tab_count == 1 { "" } else { "s" }
                    )),
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
                    .child(
                        svg()
                            .path(AppIcon::PanelBottomOpen.path())
                            .size_3p5()
                            .text_color(theme.muted_foreground),
                    ),
            )
    }

    fn render_error_state(&self, error: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let error_color = cx.theme().danger;
        let muted_fg = cx.theme().muted_foreground;

        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap_2()
            .child(
                div()
                    .text_color(error_color)
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Query Error"),
            )
            .child(
                div()
                    .text_color(muted_fg)
                    .text_sm()
                    .max_w(px(500.0))
                    .text_center()
                    .child(error.to_string()),
            )
    }

    fn render_empty_results(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let muted_fg = cx.theme().muted_foreground;

        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .child(
                div()
                    .text_color(muted_fg)
                    .child("Run a query to see results"),
            )
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

        let btn_hover = theme.muted;

        div()
            .id("dangerous-query-modal-overlay")
            .absolute()
            .inset_0()
            .bg(gpui::hsla(0.0, 0.0, 0.0, 0.5))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                div()
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::MD)
                    .p(Spacing::MD)
                    .min_w(px(350.0))
                    .max_w(px(500.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                svg()
                                    .path(AppIcon::TriangleAlert.path())
                                    .size_5()
                                    .text_color(theme.warning),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.foreground)
                                    .child(title),
                            ),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child(message),
                    )
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
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(move |_, window, cx| {
                                        entity_suppress.update(cx, |doc, cx| {
                                            doc.confirm_dangerous_query(true, window, cx);
                                        });
                                    })
                                    .child("Don't ask again"),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap(Spacing::SM)
                                    .child(
                                        div()
                                            .id("dangerous-cancel-btn")
                                            .flex()
                                            .items_center()
                                            .gap_1()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .cursor_pointer()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.muted_foreground)
                                            .bg(theme.secondary)
                                            .hover(|d| d.bg(btn_hover))
                                            .on_click(move |_, _, cx| {
                                                entity_cancel.update(cx, |doc, cx| {
                                                    doc.cancel_dangerous_query(cx);
                                                });
                                            })
                                            .child("Cancel"),
                                    )
                                    .child(
                                        div()
                                            .id("dangerous-confirm-btn")
                                            .flex()
                                            .items_center()
                                            .gap_1()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .cursor_pointer()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.background)
                                            .bg(theme.warning)
                                            .hover(|d| d.opacity(0.9))
                                            .on_click(move |_, window, cx| {
                                                entity.update(cx, |doc, cx| {
                                                    doc.confirm_dangerous_query(false, window, cx);
                                                });
                                            })
                                            .child("Run Anyway"),
                                    ),
                            ),
                    ),
            )
    }
}

impl Render for SqlQueryDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.process_pending_result(window, cx);

        self.process_pending_set_query(window, cx);

        self.process_pending_auto_refresh(window, cx);

        let toolbar = self.render_toolbar(cx).into_any_element();
        let editor_view = self.render_editor(window, cx).into_any_element();
        let results_view = self.render_results(window, cx).into_any_element();

        let bg = cx.theme().background;
        let has_collapsed_results =
            self.layout == SqlQueryLayout::EditorOnly && !self.result_tabs.is_empty();

        div()
            .id(ElementId::Name(format!("sql-doc-{}", self.id.0).into()))
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .track_focus(&self.focus_handle)
            .child(toolbar)
            .child(
                div().flex_1().overflow_hidden().child(match self.layout {
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
    }
}
