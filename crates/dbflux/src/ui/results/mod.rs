use crate::app::AppState;
use dbflux_core::{QueryRequest, QueryResult};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::table::{Column, Table, TableDelegate, TableState};
use gpui_component::{ActiveTheme, Sizable};
use log::{error, info};

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

pub struct ResultsPane {
    app_state: Entity<AppState>,
    tabs: Vec<ResultTab>,
    active_tab: usize,
    next_tab_id: usize,

    filter_input: Entity<InputState>,
    limit_input: Entity<InputState>,
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
        let filter = self.filter_input.read(cx).value();
        let limit_str = self.limit_input.read(cx).value();
        let limit: u32 = limit_str.parse().unwrap_or(100);

        let sql = if filter.trim().is_empty() {
            format!("SELECT * FROM {} LIMIT {}", table_name, limit)
        } else {
            format!(
                "SELECT * FROM {} WHERE {} LIMIT {}",
                table_name, filter, limit
            )
        };

        info!("Running table query: {}", sql);

        let conn = {
            let state = self.app_state.read(cx);
            state.active_connection().map(|c| c.connection.clone())
        };

        let Some(conn) = conn else {
            error!("No active connection");
            return;
        };

        let request = QueryRequest::new(sql);
        match conn.execute(&request) {
            Ok(result) => {
                info!(
                    "Query returned {} rows in {:?}",
                    result.row_count(),
                    result.execution_time
                );

                let delegate = ResultsTableDelegate::new(result.clone());
                let table_state = cx.new(|cx| TableState::new(delegate, window, cx));

                if let Some(idx) = self.tabs.iter().position(|t| {
                    matches!(&t.source, ResultSource::TableView { table_name: n } if n == table_name)
                }) {
                    self.tabs[idx].result = result;
                    self.tabs[idx].table_state = table_state;
                    self.active_tab = idx;
                } else {
                    let tab = ResultTab {
                        id: self.next_tab_id,
                        title: table_name.to_string(),
                        source: ResultSource::TableView {
                            table_name: table_name.to_string(),
                        },
                        result,
                        table_state,
                    };
                    self.tabs.push(tab);
                    self.active_tab = self.tabs.len() - 1;
                    self.next_tab_id += 1;
                }

                cx.notify();
            }
            Err(e) => {
                error!("Query failed: {:?}", e);
            }
        }
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
}

impl Render for ResultsPane {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
                    .h(px(32.0))
                    .px_2()
                    .gap_1()
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .when(self.tabs.is_empty(), |d| {
                        d.child(
                            div()
                                .text_sm()
                                .text_color(theme.foreground)
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
                            .gap_1()
                            .pl_2()
                            .pr_1()
                            .py_1()
                            .text_sm()
                            .rounded_t(px(4.0))
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
                                    .ml_1()
                                    .px_1()
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
                        .gap_3()
                        .h(px(36.0))
                        .px_3()
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
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("SELECT * FROM"),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::MEDIUM)
                                        .text_color(theme.foreground)
                                        .child(table_name),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("WHERE"),
                                )
                                .child(
                                    div()
                                        .w(px(280.0))
                                        .child(Input::new(&filter_input).small().cleanable(true)),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("LIMIT"),
                                )
                                .child(div().w(px(70.0)).child(Input::new(&limit_input).small())),
                        )
                        .child(
                            Button::new("refresh-table")
                                .ghost()
                                .compact()
                                .label("↻")
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.run_table_query(window, cx);
                                })),
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
                    .h(px(24.0))
                    .px_2()
                    .border_t_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("Rows: {}", row_count)),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("Time: {}", exec_time)),
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
            .map(|v| v.as_display_string())
            .unwrap_or_default();

        div().text_sm().child(value)
    }
}
