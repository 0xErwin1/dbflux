use crate::app::AppState;
use dbflux_core::{QueryRequest, QueryResult};
use gpui::prelude::FluentBuilder;
use gpui::*;

use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::table::{Column, Table, TableDelegate, TableState};
use gpui_component::{ActiveTheme, Sizable};
use log::info;

enum ResultSource {
    Query,
    TableView { table_name: String },
}

struct ResultTab {
    #[allow(dead_code)]
    id: usize,
    title: String,
    source: ResultSource,
    result: QueryResult,
    table_state: Entity<TableState<ResultsTableDelegate>>,
}

struct PendingTableResult {
    table_name: String,
    result: QueryResult,
}

pub struct ResultsPane {
    app_state: Entity<AppState>,
    tabs: Vec<ResultTab>,
    active_tab: usize,
    next_tab_id: usize,

    filter_input: Entity<InputState>,
    limit_input: Entity<InputState>,
    pending_result: Option<QueryResult>,
    pending_table_result: Option<PendingTableResult>,
    pending_error: Option<String>,
}

impl ResultsPane {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let filter_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder("e.g. id > 10 AND name LIKE '%test%'")
        });

        let limit_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("100");
            state.set_value("100", window, cx);
            state
        });

        let _ = cx.subscribe_in(
            &filter_input,
            window,
            |this, _, event: &InputEvent, window, cx| {
                if let InputEvent::PressEnter { secondary: false } = event {
                    this.run_table_query(window, cx);
                }
            },
        );

        let _ = cx.subscribe_in(
            &limit_input,
            window,
            |this, _, event: &InputEvent, window, cx| {
                if let InputEvent::PressEnter { secondary: false } = event {
                    this.run_table_query(window, cx);
                }
            },
        );

        Self {
            app_state,
            tabs: Vec::new(),
            active_tab: 0,
            next_tab_id: 1,
            filter_input,
            limit_input,
            pending_result: None,
            pending_table_result: None,
            pending_error: None,
        }
    }

    pub fn set_query_result(
        &mut self,
        result: QueryResult,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let delegate = ResultsTableDelegate::new(result.clone());
        let table_state = cx.new(|cx| TableState::new(delegate, window, cx));

        let tab = ResultTab {
            id: self.next_tab_id,
            title: format!("Result {}", self.next_tab_id),
            source: ResultSource::Query,
            result,
            table_state,
        };

        self.tabs.push(tab);
        self.active_tab = self.tabs.len() - 1;
        self.next_tab_id += 1;
        cx.notify();
    }

    pub fn set_query_result_async(&mut self, result: QueryResult, cx: &mut Context<Self>) {
        self.pending_result = Some(result);
        cx.notify();
    }

    fn apply_table_result(
        &mut self,
        table_name: String,
        result: QueryResult,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let delegate = ResultsTableDelegate::new(result.clone());
        let table_state = cx.new(|cx| TableState::new(delegate, window, cx));

        if let Some(idx) = self.tabs.iter().position(|t| {
            matches!(&t.source, ResultSource::TableView { table_name: n } if *n == table_name)
        }) {
            self.tabs[idx].result = result;
            self.tabs[idx].table_state = table_state;
            self.active_tab = idx;
        } else {
            let tab = ResultTab {
                id: self.next_tab_id,
                title: table_name.clone(),
                source: ResultSource::TableView { table_name },
                result,
                table_state,
            };
            self.tabs.push(tab);
            self.active_tab = self.tabs.len() - 1;
            self.next_tab_id += 1;
        }

        cx.notify();
    }

    pub fn view_table(&mut self, table_name: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(idx) = self.tabs.iter().position(
            |t| matches!(&t.source, ResultSource::TableView { table_name: n } if n == table_name),
        ) {
            self.active_tab = idx;
            cx.notify();
            return;
        }

        self.filter_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        self.run_table_query_for(table_name, window, cx);
    }

    fn run_table_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tabs.get(self.active_tab) else {
            return;
        };

        let table_name = match &tab.source {
            ResultSource::TableView { table_name } => table_name.clone(),
            _ => return,
        };

        self.run_table_query_for(&table_name, window, cx);
    }

    fn run_table_query_for(
        &mut self,
        table_name: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        let filter = self.filter_input.read(cx).value();
        let limit_str = self.limit_input.read(cx).value();
        let limit: u32 = limit_str
            .parse()
            .ok()
            .filter(|&n| n > 0 && n <= 10000)
            .unwrap_or(100);

        let Some(quoted_table) = Self::quote_table_identifier(table_name) else {
            cx.toast_error(format!("Invalid table identifier: {}", table_name), window);
            return;
        };

        let sql = if filter.trim().is_empty() {
            format!("SELECT * FROM {} LIMIT {}", quoted_table, limit)
        } else {
            format!(
                "SELECT * FROM {} WHERE {} LIMIT {}",
                quoted_table, filter, limit
            )
        };

        info!("Running table query: {}", sql);

        let conn = {
            let state = self.app_state.read(cx);
            state.active_connection().map(|c| c.connection.clone())
        };

        let Some(conn) = conn else {
            cx.toast_error("No active connection", window);
            return;
        };

        let request = QueryRequest::new(sql);
        let table_name_owned = table_name.to_string();
        let results_entity = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            conn.execute(&request)
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                match result {
                    Ok(result) => {
                        info!(
                            "Query returned {} rows in {:?}",
                            result.row_count(),
                            result.execution_time
                        );

                        results_entity.update(cx, |pane, cx| {
                            pane.pending_table_result = Some(PendingTableResult {
                                table_name: table_name_owned,
                                result,
                            });
                            cx.notify();
                        });
                    }
                    Err(e) => {
                        log::error!("Table query failed: {}", e);
                        results_entity.update(cx, |pane, cx| {
                            pane.pending_error = Some(format!("Query failed: {}", e));
                            cx.notify();
                        });
                    }
                }
            })
            .ok();
        })
        .detach();
    }

    fn close_tab(&mut self, idx: usize, cx: &mut Context<Self>) {
        if idx >= self.tabs.len() {
            return;
        }

        self.tabs.remove(idx);

        if self.tabs.is_empty() {
            self.active_tab = 0;
        } else if self.active_tab >= self.tabs.len() {
            self.active_tab = self.tabs.len() - 1;
        } else if self.active_tab > idx {
            self.active_tab -= 1;
        }

        cx.notify();
    }

    fn switch_tab(&mut self, idx: usize, cx: &mut Context<Self>) {
        if idx < self.tabs.len() {
            self.active_tab = idx;
            cx.notify();
        }
    }

    fn active_tab(&self) -> Option<&ResultTab> {
        self.tabs.get(self.active_tab)
    }

    fn is_table_view_mode(&self) -> bool {
        self.active_tab()
            .map(|t| matches!(t.source, ResultSource::TableView { .. }))
            .unwrap_or(false)
    }

    fn current_table_name(&self) -> Option<&str> {
        self.active_tab().and_then(|t| match &t.source {
            ResultSource::TableView { table_name } => Some(table_name.as_str()),
            _ => None,
        })
    }

    #[allow(dead_code)]
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.tabs.clear();
        self.active_tab = 0;
        cx.notify();
    }

    fn is_valid_identifier(s: &str) -> bool {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            && !s.chars().next().unwrap().is_ascii_digit()
    }

    fn quote_table_identifier(table_name: &str) -> Option<String> {
        if table_name.contains('.') {
            let parts: Vec<&str> = table_name.splitn(2, '.').collect();
            if parts.len() == 2
                && Self::is_valid_identifier(parts[0])
                && Self::is_valid_identifier(parts[1])
            {
                Some(format!("\"{}\".\"{}\"", parts[0], parts[1]))
            } else {
                None
            }
        } else if Self::is_valid_identifier(table_name) {
            Some(format!("\"{}\"", table_name))
        } else {
            None
        }
    }
}

impl Render for ResultsPane {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(result) = self.pending_result.take() {
            self.set_query_result(result, window, cx);
        }

        if let Some(pending) = self.pending_table_result.take() {
            self.apply_table_result(pending.table_name, pending.result, window, cx);
        }

        if let Some(error) = self.pending_error.take() {
            use crate::ui::toast::ToastExt;
            cx.toast_error(error, window);
        }

        let theme = cx.theme();

        let (row_count, exec_time) = self
            .active_tab()
            .map(|t| {
                let time_ms = t.result.execution_time.as_millis();
                (t.result.row_count(), format!("{}ms", time_ms))
            })
            .unwrap_or((0, "-".to_string()));

        let is_table_view = self.is_table_view_mode();
        let table_name = self.current_table_name().map(|s| s.to_string());
        let filter_input = self.filter_input.clone();
        let limit_input = self.limit_input.clone();
        let active_tab_idx = self.active_tab;
        let tab_count = self.tabs.len();

        div()
            .flex()
            .flex_col()
            .flex_1()
            .size_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .h(px(26.0))
                    .px_1()
                    .gap(px(2.0))
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .when(self.tabs.is_empty(), |d| {
                        d.child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("Results"),
                        )
                    })
                    .children(self.tabs.iter().enumerate().map(|(idx, tab)| {
                        let is_active = idx == active_tab_idx;
                        let tab_title = tab.title.clone();
                        let is_table = matches!(tab.source, ResultSource::TableView { .. });

                        div()
                            .id(("result-tab", idx))
                            .flex()
                            .items_center()
                            .gap(px(2.0))
                            .px_2()
                            .py(px(2.0))
                            .text_xs()
                            .rounded_t(px(3.0))
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
                            .when(is_table, |d| {
                                d.child(div().text_xs().text_color(gpui::rgb(0x4EC9B0)).child("▦ "))
                            })
                            .child(tab_title)
                            .child(
                                div()
                                    .id(("close-result-tab", idx))
                                    .ml(px(2.0))
                                    .px(px(2.0))
                                    .rounded(px(2.0))
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.close_tab(idx, cx);
                                    }))
                                    .child("×"),
                            )
                    })),
            )
            .when(is_table_view, |d| {
                let table_name = table_name.clone().unwrap_or_default();
                d.child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .h(px(28.0))
                        .px_2()
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.secondary)
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground)
                                        .child("SELECT * FROM"),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .font_weight(FontWeight::MEDIUM)
                                        .text_color(theme.foreground)
                                        .child(table_name),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground)
                                        .child("WHERE"),
                                )
                                .child(
                                    div()
                                        .w(px(220.0))
                                        .child(Input::new(&filter_input).small().cleanable(true)),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground)
                                        .child("LIMIT"),
                                )
                                .child(div().w(px(50.0)).child(Input::new(&limit_input).small())),
                        )
                        .child(
                            div()
                                .id("refresh-table")
                                .w(px(22.0))
                                .h(px(22.0))
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded(px(3.0))
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.run_table_query(window, cx);
                                }))
                                .child("↻"),
                        ),
                )
            })
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .when(tab_count == 0, |d| {
                        d.flex().items_center().justify_center().child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("Run a query to see results"),
                        )
                    })
                    .when_some(
                        self.active_tab().map(|t| t.table_state.clone()),
                        |d, table_state| {
                            d.child(Table::new(&table_state).stripe(true).bordered(true))
                        },
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(px(20.0))
                    .px_2()
                    .border_t_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("{} rows", row_count)),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(exec_time),
                    ),
            )
    }
}

struct ResultsTableDelegate {
    result: QueryResult,
    columns: Vec<Column>,
}

impl ResultsTableDelegate {
    fn new(result: QueryResult) -> Self {
        let columns = result
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                Column::new(format!("col_{}", i), &col.name)
                    .width(120.)
                    .resizable(true)
            })
            .collect();

        Self { result, columns }
    }
}

impl TableDelegate for ResultsTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.result.rows.len()
    }

    fn column(&self, col_ix: usize, _cx: &App) -> &Column {
        &self.columns[col_ix]
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let value = self
            .result
            .rows
            .get(row_ix)
            .and_then(|row| row.get(col_ix))
            .map(|v| v.as_display_string_truncated(200))
            .unwrap_or_default();

        div()
            .text_xs()
            .overflow_hidden()
            .text_ellipsis()
            .whitespace_nowrap()
            .child(value)
    }
}
