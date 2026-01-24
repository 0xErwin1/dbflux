use crate::app::{AppState, AppStateChanged};
use crate::ui::results::ResultsPane;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{CancelToken, HistoryEntry, QueryRequest, TaskId, TaskKind};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants, DropdownButton};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::menu::PopupMenuItem;
use gpui_component::{ActiveTheme, InteractiveElementExt, Sizable};
use log::info;
use uuid::Uuid;

pub struct EditorPane {
    app_state: Entity<AppState>,
    results_pane: Entity<ResultsPane>,
    tabs: Vec<QueryTab>,
    active_tab: usize,
    next_tab_number: usize,
    renaming_tab: Option<usize>,
    rename_input: Entity<InputState>,
    pending_error: Option<String>,
    running_query: Option<RunningQuery>,
}

struct RunningQuery {
    task_id: TaskId,
    cancel_token: CancelToken,
}

struct QueryTab {
    #[allow(dead_code)]
    id: Uuid,
    title: String,
    input_state: Entity<InputState>,
    original_content: String,
}

impl EditorPane {
    pub fn new(
        app_state: Entity<AppState>,
        results_pane: Entity<ResultsPane>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("sql")
                .line_number(true)
                .soft_wrap(false)
                .placeholder("-- Enter SQL here...")
        });

        let rename_input = cx.new(|cx| InputState::new(window, cx));

        cx.subscribe_in(
            &rename_input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } => {
                    this.finish_rename(window, cx);
                }
                InputEvent::Blur => {
                    this.cancel_rename(cx);
                }
                _ => {}
            },
        )
        .detach();

        Self {
            app_state,
            results_pane,
            tabs: vec![QueryTab {
                id: Uuid::new_v4(),
                title: "Query 1".to_string(),
                input_state,
                original_content: String::new(),
            }],
            active_tab: 0,
            next_tab_number: 2,
            renaming_tab: None,
            rename_input,
            pending_error: None,
            running_query: None,
        }
    }

    pub fn add_new_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("sql")
                .line_number(true)
                .soft_wrap(false)
                .placeholder("-- Enter SQL here...")
        });

        self.tabs.push(QueryTab {
            id: Uuid::new_v4(),
            title: format!("Query {}", self.next_tab_number),
            input_state,
            original_content: String::new(),
        });
        self.active_tab = self.tabs.len() - 1;
        self.next_tab_number += 1;
        cx.notify();
    }

    fn switch_tab(&mut self, idx: usize, cx: &mut Context<Self>) {
        if idx < self.tabs.len() && self.renaming_tab.is_none() {
            self.active_tab = idx;
            cx.notify();
        }
    }

    fn close_tab(&mut self, idx: usize, cx: &mut Context<Self>) {
        if self.tabs.len() <= 1 || self.renaming_tab.is_some() {
            return;
        }

        self.tabs.remove(idx);

        if self.active_tab >= self.tabs.len() {
            self.active_tab = self.tabs.len() - 1;
        } else if self.active_tab > idx {
            self.active_tab -= 1;
        }

        cx.notify();
    }

    pub fn next_tab(&mut self, cx: &mut Context<Self>) {
        if self.tabs.is_empty() {
            return;
        }
        let next = (self.active_tab + 1) % self.tabs.len();
        self.switch_tab(next, cx);
    }

    pub fn prev_tab(&mut self, cx: &mut Context<Self>) {
        if self.tabs.is_empty() {
            return;
        }
        let prev = if self.active_tab == 0 {
            self.tabs.len() - 1
        } else {
            self.active_tab - 1
        };
        self.switch_tab(prev, cx);
    }

    pub fn switch_to_tab(&mut self, n: usize, cx: &mut Context<Self>) {
        if n >= 1 && n <= self.tabs.len() {
            self.switch_tab(n - 1, cx);
        }
    }

    pub fn close_current_tab(&mut self, cx: &mut Context<Self>) {
        self.close_tab(self.active_tab, cx);
    }

    fn start_rename(&mut self, idx: usize, window: &mut Window, cx: &mut Context<Self>) {
        if idx >= self.tabs.len() {
            return;
        }

        let current_title = self.tabs[idx].title.clone();
        self.rename_input.update(cx, |state, cx| {
            state.set_value(&current_title, window, cx);
        });
        self.renaming_tab = Some(idx);
        cx.notify();
    }

    fn finish_rename(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(idx) = self.renaming_tab {
            let new_title = self.rename_input.read(cx).value();
            if !new_title.trim().is_empty() && idx < self.tabs.len() {
                self.tabs[idx].title = new_title.to_string();
            }
        }
        self.renaming_tab = None;
        cx.notify();
    }

    fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.renaming_tab = None;
        cx.notify();
    }

    fn is_tab_dirty(&self, idx: usize, cx: &Context<Self>) -> bool {
        if let Some(tab) = self.tabs.get(idx) {
            let current = tab.input_state.read(cx).value();
            current != tab.original_content
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn set_query(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        let sql = sql.to_string();
        self.tabs[self.active_tab]
            .input_state
            .update(cx, |state, cx| {
                state.set_value(&sql, window, cx);
            });
        cx.notify();
    }

    pub fn focus_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.tabs[self.active_tab]
            .input_state
            .update(cx, |state, cx| {
                state.focus(window, cx);
            });
    }

    pub fn run_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        if self.running_query.is_some() {
            cx.toast_warning("A query is already running", window);
            return;
        }

        let sql = self.tabs[self.active_tab].input_state.read(cx).value();

        if sql.trim().is_empty() {
            cx.toast_warning("Enter a query to run", window);
            return;
        }

        info!("Running query: {}", sql);

        let (conn, database, connection_name) = {
            let state = self.app_state.read(cx);
            let active = state.active_connection();
            (
                active.map(|c| c.connection.clone()),
                active.and_then(|c| c.schema.as_ref().and_then(|s| s.current_database.clone())),
                active.map(|c| c.profile.name.clone()),
            )
        };

        let Some(conn) = conn else {
            cx.toast_error("No active connection", window);
            return;
        };

        let sql_owned = sql.to_string();
        let sql_preview = if sql_owned.len() > 50 {
            format!("{}...", &sql_owned[..50])
        } else {
            sql_owned.clone()
        };

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result = state.start_task(TaskKind::Query, format!("Query: {}", sql_preview));
            cx.emit(AppStateChanged);
            result
        });

        self.running_query = Some(RunningQuery {
            task_id,
            cancel_token: cancel_token.clone(),
        });
        cx.notify();

        let request = QueryRequest::new(sql_owned.clone());
        let app_state = self.app_state.clone();
        let results_pane = self.results_pane.clone();
        let editor_entity = cx.entity().clone();
        let conn_for_cleanup = conn.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                let was_cancelled = cancel_token.is_cancelled();

                editor_entity.update(cx, |editor, cx| {
                    editor.running_query = None;
                    cx.notify();
                });

                if was_cancelled {
                    if let Err(e) = conn_for_cleanup.cleanup_after_cancel() {
                        log::warn!("Cleanup after cancel failed: {}", e);
                    }
                    app_state.update(cx, |_, cx| {
                        cx.emit(AppStateChanged);
                    });
                    return;
                }

                match &result {
                    Ok(query_result) => {
                        info!(
                            "Query returned {} rows in {:?}",
                            query_result.row_count(),
                            query_result.execution_time
                        );

                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });

                        let entry = HistoryEntry::new(
                            sql_owned,
                            database,
                            connection_name,
                            query_result.execution_time,
                            Some(query_result.row_count()),
                        );
                        app_state.update(cx, |state, _cx| {
                            state.add_history_entry(entry);
                        });

                        results_pane.update(cx, |pane, cx| {
                            pane.set_query_result_async(query_result.clone(), cx);
                        });
                    }
                    Err(e) => {
                        log::error!("Query failed: {}", e);

                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.to_string());
                        });

                        editor_entity.update(cx, |editor, cx| {
                            editor.pending_error = Some(format!("Query failed: {}", e));
                            cx.notify();
                        });
                    }
                }

                app_state.update(cx, |_, cx| {
                    cx.emit(AppStateChanged);
                });
            })
            .ok();
        })
        .detach();
    }

    fn cancel_query(&mut self, cx: &mut Context<Self>) {
        if let Some(running) = self.running_query.take() {
            running.cancel_token.cancel();

            let conn = self
                .app_state
                .read(cx)
                .active_connection()
                .map(|c| c.connection.clone());

            if let Some(conn) = conn
                && let Err(e) = conn.cancel_active()
            {
                log::warn!("Failed to send cancel to database: {}", e);
            }

            self.app_state.update(cx, |state, cx| {
                state.cancel_task(running.task_id);
                cx.emit(AppStateChanged);
            });

            cx.notify();
            info!("Query cancelled");
        }
    }

    fn is_query_running(&self) -> bool {
        self.running_query.is_some()
    }

    fn build_connection_menu_items(&self, cx: &Context<Self>) -> Vec<(Uuid, String, bool)> {
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id;

        state
            .profiles
            .iter()
            .filter(|p| state.connections.contains_key(&p.id))
            .map(|p| (p.id, p.name.clone(), Some(p.id) == active_id))
            .collect()
    }
}

impl Render for EditorPane {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(error) = self.pending_error.take() {
            use crate::ui::toast::ToastExt;
            cx.toast_error(error, window);
        }

        let theme = cx.theme();
        let active_input = self.tabs[self.active_tab].input_state.clone();
        let active_tab_idx = self.active_tab;
        let tab_count = self.tabs.len();
        let renaming_tab = self.renaming_tab;
        let rename_input = self.rename_input.clone();

        let state = self.app_state.read(cx);
        let active_conn = state.active_connection();
        let is_connected = active_conn.is_some();

        let connection_name = active_conn
            .map(|c| c.profile.name.clone())
            .unwrap_or_default();
        let current_db = active_conn
            .and_then(|c| c.schema.as_ref())
            .and_then(|s| s.current_database.clone());
        let has_multiple_connections = state.connections.len() > 1;

        let connection_items = self.build_connection_menu_items(cx);
        let app_state = self.app_state.clone();
        let is_query_running = self.is_query_running();

        let tab_dirty_states: Vec<bool> = (0..self.tabs.len())
            .map(|i| self.is_tab_dirty(i, cx))
            .collect();

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(theme.sidebar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(Heights::HEADER)
                    .px(Spacing::MD)
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .when(!is_connected, |d| {
                        d.child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .child(
                                    div()
                                        .w(Spacing::SM)
                                        .h(Spacing::SM)
                                        .rounded_full()
                                        .bg(theme.muted_foreground),
                                )
                                .child(
                                    div()
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .child("No connection"),
                                ),
                        )
                    })
                    .when(is_connected, |d| {
                        d.child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::MD)
                                .bg(theme.secondary)
                                .child(
                                    div()
                                        .w(Spacing::SM)
                                        .h(Spacing::SM)
                                        .rounded_full()
                                        .bg(gpui::rgb(0x22C55E)),
                                )
                                .when(!has_multiple_connections, |d| {
                                    d.child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::XS)
                                            .child(
                                                div()
                                                    .text_size(FontSizes::SM)
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .text_color(theme.foreground)
                                                    .child(connection_name.clone()),
                                            )
                                            .when_some(current_db.clone(), |d, db| {
                                                d.child(
                                                    div()
                                                        .text_size(FontSizes::SM)
                                                        .text_color(theme.muted_foreground)
                                                        .child("/"),
                                                )
                                                .child(
                                                    div()
                                                        .text_size(FontSizes::SM)
                                                        .text_color(theme.foreground)
                                                        .child(db),
                                                )
                                            }),
                                    )
                                })
                                .when(has_multiple_connections, |d| {
                                    d.child(
                                        DropdownButton::new("connection-selector")
                                            .small()
                                            .button(Button::new("conn-btn").ghost().small().label(
                                                if let Some(ref db) = current_db {
                                                    format!("{} / {}", connection_name, db)
                                                } else {
                                                    connection_name.clone()
                                                },
                                            ))
                                            .dropdown_menu(move |menu, _window, _cx| {
                                                let mut menu = menu;
                                                for (profile_id, name, is_active) in
                                                    &connection_items
                                                {
                                                    let pid = *profile_id;
                                                    let app_state = app_state.clone();
                                                    menu = menu.item(
                                                        PopupMenuItem::new(name.clone())
                                                            .checked(*is_active)
                                                            .on_click(move |_, _, cx| {
                                                                app_state.update(
                                                                    cx,
                                                                    |state, cx| {
                                                                        state
                                                                            .set_active_connection(
                                                                                pid,
                                                                            );
                                                                        cx.notify();
                                                                    },
                                                                );
                                                            }),
                                                    );
                                                }
                                                menu
                                            }),
                                    )
                                }),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("run-query")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .px(Spacing::MD)
                                    .h(Heights::BUTTON)
                                    .rounded(Radii::MD)
                                    .border_1()
                                    .when(is_connected && !is_query_running, |d| {
                                        d.border_color(theme.border)
                                            .bg(theme.background)
                                            .text_color(theme.foreground)
                                            .cursor_pointer()
                                            .hover(|s| {
                                                s.bg(theme.secondary).border_color(theme.primary)
                                            })
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.run_query(window, cx);
                                            }))
                                    })
                                    .when(!is_connected || is_query_running, |d| {
                                        d.border_color(theme.border)
                                            .bg(theme.secondary)
                                            .text_color(theme.muted_foreground)
                                            .cursor_not_allowed()
                                    })
                                    .text_size(FontSizes::SM)
                                    .child("▶")
                                    .child("Run"),
                            )
                            .when(is_query_running, |d| {
                                d.child(
                                    div()
                                        .id("cancel-query")
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::SM)
                                        .px(Spacing::MD)
                                        .h(Heights::BUTTON)
                                        .rounded(Radii::MD)
                                        .border_1()
                                        .border_color(gpui::rgb(0xDC2626))
                                        .bg(theme.background)
                                        .text_color(gpui::rgb(0xDC2626))
                                        .cursor_pointer()
                                        .hover(|s| {
                                            s.bg(gpui::rgb(0xDC2626)).text_color(gpui::white())
                                        })
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.cancel_query(cx);
                                        }))
                                        .text_size(FontSizes::SM)
                                        .child("■")
                                        .child("Cancel"),
                                )
                            }),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .h(Heights::TAB)
                    .px(Spacing::XS)
                    .gap(Spacing::XS)
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .children(self.tabs.iter().enumerate().map(|(idx, tab)| {
                        let is_active = idx == active_tab_idx;
                        let is_renaming = renaming_tab == Some(idx);
                        let is_dirty = tab_dirty_states.get(idx).copied().unwrap_or(false);
                        let tab_title = if is_dirty {
                            format!("{}*", tab.title)
                        } else {
                            tab.title.clone()
                        };

                        div()
                            .id(("tab", idx))
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .text_size(FontSizes::SM)
                            .rounded_t(Radii::SM)
                            .cursor_pointer()
                            .when(is_active, |d| {
                                d.bg(theme.background).text_color(theme.foreground)
                            })
                            .when(!is_active, |d| {
                                d.text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary))
                            })
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.switch_tab(idx, cx);
                            }))
                            .on_double_click(cx.listener(move |this, _, window, cx| {
                                this.start_rename(idx, window, cx);
                            }))
                            .when(is_renaming, |d| {
                                d.child(div().w(px(100.0)).child(Input::new(&rename_input).small()))
                            })
                            .when(!is_renaming, |d| d.child(tab_title))
                            .when(tab_count > 1 && !is_renaming, |d| {
                                d.child(
                                    div()
                                        .id(("close-tab", idx))
                                        .ml(Spacing::XS)
                                        .px(Spacing::XS)
                                        .rounded(Radii::SM)
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .hover(|d| {
                                            d.bg(theme.secondary).text_color(theme.foreground)
                                        })
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            this.close_tab(idx, cx);
                                        }))
                                        .child("×"),
                                )
                            })
                    }))
                    .child(
                        div()
                            .id("new-tab")
                            .w(Heights::ICON_MD)
                            .h(Heights::ICON_MD)
                            .flex()
                            .items_center()
                            .justify_center()
                            .rounded(Radii::SM)
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.add_new_tab(window, cx);
                            }))
                            .child("+"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .p_2()
                    .child(Input::new(&active_input).h_full()),
            )
    }
}
