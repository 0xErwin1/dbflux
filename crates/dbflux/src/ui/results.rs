use crate::app::{AppState, AppStateChanged};
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    CancelToken, Pagination, QueryRequest, QueryResult, TableBrowseRequest, TableRef, TaskId,
    TaskKind,
};
use dbflux_export::{CsvExporter, Exporter};
use gpui::prelude::FluentBuilder;
use gpui::*;

use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::table::{Column, Table, TableDelegate, TableState};
use gpui_component::{ActiveTheme, Sizable};
use log::info;
use std::fs::File;
use std::io::BufWriter;

pub struct ResultsReceived;

impl EventEmitter<ResultsReceived> for ResultsPane {}

enum ResultSource {
    Query,
    TableView {
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<String>,
        total_rows: Option<u64>,
    },
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
    table: TableRef,
    pagination: Pagination,
    order_by: Vec<String>,
    total_rows: Option<u64>,
    result: QueryResult,
}

struct PendingTotalCount {
    table_qualified: String,
    total: u64,
}

#[allow(dead_code)]
struct RunningTableQuery {
    task_id: TaskId,
    cancel_token: CancelToken,
}

struct PendingToast {
    message: String,
    is_error: bool,
}

#[derive(Clone, Copy, PartialEq, Default)]
pub enum FocusMode {
    #[default]
    Table,
    Toolbar,
}

#[derive(Clone, Copy, PartialEq, Default)]
pub enum ToolbarFocus {
    #[default]
    Filter,
    Limit,
    Refresh,
}

impl ToolbarFocus {
    fn left(self) -> Self {
        match self {
            ToolbarFocus::Filter => ToolbarFocus::Filter,
            ToolbarFocus::Limit => ToolbarFocus::Filter,
            ToolbarFocus::Refresh => ToolbarFocus::Limit,
        }
    }

    fn right(self) -> Self {
        match self {
            ToolbarFocus::Filter => ToolbarFocus::Limit,
            ToolbarFocus::Limit => ToolbarFocus::Refresh,
            ToolbarFocus::Refresh => ToolbarFocus::Refresh,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Default)]
pub enum EditState {
    #[default]
    Navigating,
    Editing,
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
    pending_total_count: Option<PendingTotalCount>,
    pending_error: Option<String>,
    running_table_query: Option<RunningTableQuery>,
    pending_toast: Option<PendingToast>,

    focus_mode: FocusMode,
    toolbar_focus: ToolbarFocus,
    edit_state: EditState,
    focus_handle: FocusHandle,
    switching_input: bool,
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

        cx.subscribe_in(
            &filter_input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { secondary: false } => {
                    this.run_table_query(window, cx);
                    this.focus_table(window, cx);
                }
                InputEvent::Blur => {
                    this.exit_edit_mode(window, cx);
                }
                _ => {}
            },
        )
        .detach();

        cx.subscribe_in(
            &limit_input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { secondary: false } => {
                    this.run_table_query(window, cx);
                    this.focus_table(window, cx);
                }
                InputEvent::Blur => {
                    this.exit_edit_mode(window, cx);
                }
                _ => {}
            },
        )
        .detach();

        let focus_handle = cx.focus_handle();

        Self {
            app_state,
            tabs: Vec::new(),
            active_tab: 0,
            next_tab_id: 1,
            filter_input,
            limit_input,
            pending_result: None,
            pending_table_result: None,
            pending_total_count: None,
            pending_error: None,
            running_table_query: None,
            pending_toast: None,
            focus_mode: FocusMode::default(),
            toolbar_focus: ToolbarFocus::default(),
            edit_state: EditState::default(),
            focus_handle,
            switching_input: false,
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
        cx.emit(ResultsReceived);
        cx.notify();
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_table_result(
        &mut self,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<String>,
        total_rows: Option<u64>,
        result: QueryResult,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let delegate = ResultsTableDelegate::new(result.clone());
        let table_state = cx.new(|cx| TableState::new(delegate, window, cx));

        let qualified = table.qualified_name();

        if let Some(idx) = self.tabs.iter().position(
            |t| matches!(&t.source, ResultSource::TableView { table: tbl, .. } if tbl.qualified_name() == qualified),
        ) {
            let existing_total = match &self.tabs[idx].source {
                ResultSource::TableView { total_rows, .. } => *total_rows,
                _ => None,
            };

            self.tabs[idx].result = result;
            self.tabs[idx].table_state = table_state;
            self.tabs[idx].source = ResultSource::TableView {
                table,
                pagination,
                order_by,
                total_rows: total_rows.or(existing_total),
            };
            self.active_tab = idx;
        } else {
            let tab = ResultTab {
                id: self.next_tab_id,
                title: table.name.clone(),
                source: ResultSource::TableView {
                    table,
                    pagination,
                    order_by,
                    total_rows,
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

    fn apply_total_count(&mut self, table_qualified: String, total: u64, cx: &mut Context<Self>) {
        for tab in &mut self.tabs {
            if let ResultSource::TableView {
                table, total_rows, ..
            } = &mut tab.source
                && table.qualified_name() == table_qualified
            {
                *total_rows = Some(total);
                cx.notify();
                return;
            }
        }
    }

    pub fn view_table(&mut self, table_name: &str, window: &mut Window, cx: &mut Context<Self>) {
        let table = TableRef::from_qualified(table_name);
        let qualified = table.qualified_name();

        if let Some(idx) = self.tabs.iter().position(
            |t| matches!(&t.source, ResultSource::TableView { table: tbl, .. } if tbl.qualified_name() == qualified),
        ) {
            self.active_tab = idx;
            cx.notify();
            return;
        }

        self.filter_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        let order_by = self.get_primary_key_columns(&table, cx);
        let pagination = Pagination::default();

        self.run_table_query_internal(table.clone(), pagination, order_by, None, None, window, cx);
        self.fetch_total_count(table, None, cx);
    }

    fn fetch_total_count(
        &mut self,
        table: TableRef,
        filter: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let conn = {
            let state = self.app_state.read(cx);
            state.active_connection().map(|c| c.connection.clone())
        };

        let Some(conn) = conn else {
            return;
        };

        let sql = if let Some(ref f) = filter {
            let trimmed = f.trim();
            if trimmed.is_empty() {
                format!("SELECT COUNT(*) FROM {}", table.quoted())
            } else {
                format!("SELECT COUNT(*) FROM {} WHERE {}", table.quoted(), trimmed)
            }
        } else {
            format!("SELECT COUNT(*) FROM {}", table.quoted())
        };

        let request = QueryRequest::new(sql);
        let results_entity = cx.entity().clone();
        let qualified = table.qualified_name();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if let Ok(query_result) = result
                    && let Some(row) = query_result.rows.first()
                    && let Some(dbflux_core::Value::Int(count)) = row.first()
                {
                    let total = *count as u64;
                    results_entity.update(cx, |pane, cx| {
                        pane.pending_total_count = Some(PendingTotalCount {
                            table_qualified: qualified,
                            total,
                        });
                        cx.notify();
                    });
                }
            })
            .ok();
        })
        .detach();
    }

    fn run_table_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tabs.get(self.active_tab) else {
            return;
        };

        let (table, pagination, order_by, total_rows) = match &tab.source {
            ResultSource::TableView {
                table,
                pagination,
                order_by,
                total_rows,
            } => (
                table.clone(),
                pagination.clone(),
                order_by.clone(),
                *total_rows,
            ),
            _ => return,
        };

        let filter_value = self.filter_input.read(cx).value();
        let filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        let limit_value = self.limit_input.read(cx).value();
        let limit_str = limit_value.trim();
        let pagination = match limit_str.parse::<u32>() {
            Ok(0) => {
                use crate::ui::toast::ToastExt;
                cx.toast_warning("Limit must be greater than 0", window);
                pagination
            }
            Ok(limit) if limit != pagination.limit() => pagination.with_limit(limit).reset_offset(),
            Ok(_) => pagination,
            Err(_) if !limit_str.is_empty() => {
                use crate::ui::toast::ToastExt;
                cx.toast_warning("Invalid limit value", window);
                pagination
            }
            Err(_) => pagination,
        };

        self.run_table_query_internal(
            table.clone(),
            pagination,
            order_by,
            filter.clone(),
            total_rows,
            window,
            cx,
        );

        if total_rows.is_none() {
            self.fetch_total_count(table, filter, cx);
        }
    }

    fn get_primary_key_columns(&self, table: &TableRef, cx: &Context<Self>) -> Vec<String> {
        let state = self.app_state.read(cx);
        let Some(connected) = state.active_connection() else {
            return Vec::new();
        };
        let Some(schema) = &connected.schema else {
            return Vec::new();
        };

        for db_schema in &schema.schemas {
            if table.schema.as_deref() == Some(&db_schema.name) || table.schema.is_none() {
                for t in &db_schema.tables {
                    if t.name == table.name {
                        return t
                            .columns
                            .iter()
                            .filter(|c| c.is_primary_key)
                            .map(|c| c.name.clone())
                            .collect();
                    }
                }
            }
        }

        Vec::new()
    }

    #[allow(clippy::too_many_arguments)]
    fn run_table_query_internal(
        &mut self,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<String>,
        filter: Option<String>,
        total_rows: Option<u64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        if self.running_table_query.is_some() {
            cx.toast_error("A table query is already running", window);
            return;
        }

        let mut request = TableBrowseRequest::new(table.clone())
            .with_pagination(pagination.clone())
            .with_order_by(order_by.clone());

        if let Some(ref f) = filter {
            request = request.with_filter(f.clone());
        }

        let sql = request.build_sql();
        info!("Running table query: {}", sql);

        let conn = {
            let state = self.app_state.read(cx);
            state.active_connection().map(|c| c.connection.clone())
        };

        let Some(conn) = conn else {
            cx.toast_error("No active connection", window);
            return;
        };

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result = state.start_task(
                TaskKind::Query,
                format!("SELECT * FROM {}", table.qualified_name()),
            );
            cx.emit(AppStateChanged);
            result
        });

        self.running_table_query = Some(RunningTableQuery {
            task_id,
            cancel_token: cancel_token.clone(),
        });

        let query_request = QueryRequest::new(sql);
        let results_entity = cx.entity().clone();
        let app_state = self.app_state.clone();

        let conn_for_cleanup = conn.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&query_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                results_entity.update(cx, |pane, _cx| {
                    pane.running_table_query = None;
                });

                if cancel_token.is_cancelled() {
                    log::info!("Table query was cancelled, discarding result");
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

                        results_entity.update(cx, |pane, cx| {
                            pane.pending_table_result = Some(PendingTableResult {
                                table,
                                pagination,
                                order_by,
                                total_rows,
                                result: query_result.clone(),
                            });
                            cx.notify();
                        });
                    }
                    Err(e) => {
                        log::error!("Table query failed: {}", e);

                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.to_string());
                        });

                        results_entity.update(cx, |pane, cx| {
                            pane.pending_error = Some(format!("Query failed: {}", e));
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

    pub fn go_to_next_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tabs.get(self.active_tab) else {
            return;
        };

        let (table, pagination, order_by, total_rows) = match &tab.source {
            ResultSource::TableView {
                table,
                pagination,
                order_by,
                total_rows,
            } => (
                table.clone(),
                pagination.next_page(),
                order_by.clone(),
                *total_rows,
            ),
            _ => return,
        };

        let filter_value = self.filter_input.read(cx).value();
        let filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        self.run_table_query_internal(table, pagination, order_by, filter, total_rows, window, cx);
    }

    pub fn go_to_prev_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tabs.get(self.active_tab) else {
            return;
        };

        let (table, pagination, order_by, total_rows) = match &tab.source {
            ResultSource::TableView {
                table,
                pagination,
                order_by,
                total_rows,
            } => {
                let Some(prev) = pagination.prev_page() else {
                    return;
                };
                (table.clone(), prev, order_by.clone(), *total_rows)
            }
            _ => return,
        };

        let filter_value = self.filter_input.read(cx).value();
        let filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        self.run_table_query_internal(table, pagination, order_by, filter, total_rows, window, cx);
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

    fn current_table_ref(&self) -> Option<&TableRef> {
        self.active_tab().and_then(|t| match &t.source {
            ResultSource::TableView { table, .. } => Some(table),
            _ => None,
        })
    }

    fn current_pagination(&self) -> Option<&Pagination> {
        self.active_tab().and_then(|t| match &t.source {
            ResultSource::TableView { pagination, .. } => Some(pagination),
            _ => None,
        })
    }

    fn can_go_prev(&self) -> bool {
        self.current_pagination()
            .map(|p| !p.is_first_page())
            .unwrap_or(false)
    }

    fn can_go_next(&self) -> bool {
        let Some(tab) = self.active_tab() else {
            return false;
        };
        let Some(pagination) = self.current_pagination() else {
            return false;
        };

        if let Some(total) = self.current_total_rows() {
            let next_offset = pagination.offset() + pagination.limit() as u64;
            return next_offset < total;
        }

        tab.result.row_count() >= pagination.limit() as usize
    }

    fn current_total_rows(&self) -> Option<u64> {
        self.active_tab().and_then(|t| match &t.source {
            ResultSource::TableView { total_rows, .. } => *total_rows,
            _ => None,
        })
    }

    fn total_pages(&self) -> Option<u64> {
        let pagination = self.current_pagination()?;
        let total = self.current_total_rows()?;
        let limit = pagination.limit() as u64;
        if limit == 0 {
            return Some(1);
        }
        Some(total.div_ceil(limit))
    }

    #[allow(dead_code)]
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.tabs.clear();
        self.active_tab = 0;
        cx.notify();
    }

    #[allow(dead_code)]
    fn is_table_query_running(&self) -> bool {
        self.running_table_query.is_some()
    }

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        let row_count = tab.result.rows.len();
        if row_count == 0 {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            let next = match state.selected_row() {
                Some(current) => (current + 1).min(row_count.saturating_sub(1)),
                None => 0,
            };
            state.set_selected_row(next, cx);
            state.scroll_to_row(next, cx);
        });
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        let row_count = tab.result.rows.len();
        if row_count == 0 {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            let prev = match state.selected_row() {
                Some(current) => current.saturating_sub(1),
                None => row_count.saturating_sub(1),
            };
            state.set_selected_row(prev, cx);
            state.scroll_to_row(prev, cx);
        });
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        if tab.result.rows.is_empty() {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.set_selected_row(0, cx);
            state.scroll_to_row(0, cx);
        });
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        let row_count = tab.result.rows.len();
        if row_count == 0 {
            return;
        }

        let last = row_count.saturating_sub(1);
        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.set_selected_row(last, cx);
            state.scroll_to_row(last, cx);
        });
    }

    pub fn column_left(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        let col_count = tab.result.columns.len();
        if col_count == 0 {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            let current = state.selected_col().unwrap_or(0);
            let prev = current.saturating_sub(1);
            state.set_selected_col(prev, cx);
            state.scroll_to_col(prev, cx);
        });
    }

    pub fn column_right(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        let col_count = tab.result.columns.len();
        if col_count == 0 {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            let current = state.selected_col().unwrap_or(0);
            let next = (current + 1).min(col_count.saturating_sub(1));
            state.set_selected_col(next, cx);
            state.scroll_to_col(next, cx);
        });
    }

    // === Focus Mode / Toolbar Navigation ===

    pub fn focus_mode(&self) -> FocusMode {
        self.focus_mode
    }

    pub fn edit_state(&self) -> EditState {
        self.edit_state
    }

    pub fn focus_toolbar(&mut self, cx: &mut Context<Self>) {
        if self.tabs.is_empty() || !self.is_table_view_mode() {
            return;
        }
        self.focus_mode = FocusMode::Toolbar;
        self.toolbar_focus = ToolbarFocus::Filter;
        self.edit_state = EditState::Navigating;
        cx.notify();
    }

    pub fn focus_table(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = FocusMode::Table;
        self.edit_state = EditState::Navigating;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    pub fn toolbar_left(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != FocusMode::Toolbar {
            return;
        }
        self.toolbar_focus = self.toolbar_focus.left();
        cx.notify();
    }

    pub fn toolbar_right(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != FocusMode::Toolbar {
            return;
        }
        self.toolbar_focus = self.toolbar_focus.right();
        cx.notify();
    }

    pub fn toolbar_execute(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_mode != FocusMode::Toolbar {
            return;
        }

        match self.toolbar_focus {
            ToolbarFocus::Filter => {
                self.edit_state = EditState::Editing;
                self.filter_input.update(cx, |input, cx| {
                    input.focus(window, cx);
                });
                cx.notify();
            }
            ToolbarFocus::Limit => {
                self.edit_state = EditState::Editing;
                self.limit_input.update(cx, |input, cx| {
                    input.focus(window, cx);
                });
                cx.notify();
            }
            ToolbarFocus::Refresh => {
                self.run_table_query(window, cx);
                self.focus_table(window, cx);
            }
        }
    }

    pub fn exit_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.switching_input {
            self.switching_input = false;
            return;
        }

        if self.edit_state == EditState::Editing {
            self.edit_state = EditState::Navigating;
            window.focus(&self.focus_handle);
            cx.notify();
        }
    }

    pub fn export_results(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        let Some(tab) = self.active_tab() else {
            cx.toast_error("No results to export", window);
            return;
        };

        let result = tab.result.clone();
        let suggested_name = match &tab.source {
            ResultSource::TableView { table, .. } => format!("{}.csv", table.name),
            ResultSource::Query => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                format!("result_{}.csv", timestamp)
            }
        };

        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let file_handle = rfd::AsyncFileDialog::new()
                .set_title("Export as CSV")
                .set_file_name(&suggested_name)
                .add_filter("CSV", &["csv"])
                .save_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();

            let export_result = (|| {
                let file = File::create(&path)?;
                let mut writer = BufWriter::new(file);
                CsvExporter.export(&result, &mut writer)?;
                Ok::<_, dbflux_export::ExportError>(())
            })();

            let message = match &export_result {
                Ok(()) => format!("Exported to {}", path.display()),
                Err(e) => format!("Export failed: {}", e),
            };
            let is_error = export_result.is_err();

            cx.update(|cx| {
                entity.update(cx, |pane, cx| {
                    pane.pending_toast = Some(PendingToast { message, is_error });
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }
}

impl Render for ResultsPane {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(result) = self.pending_result.take() {
            self.set_query_result(result, window, cx);
        }

        if let Some(pending) = self.pending_table_result.take() {
            self.apply_table_result(
                pending.table,
                pending.pagination,
                pending.order_by,
                pending.total_rows,
                pending.result,
                window,
                cx,
            );
        }

        if let Some(pending) = self.pending_total_count.take() {
            self.apply_total_count(pending.table_qualified, pending.total, cx);
        }

        if let Some(error) = self.pending_error.take() {
            use crate::ui::toast::ToastExt;
            cx.toast_error(error, window);
        }

        if let Some(toast) = self.pending_toast.take() {
            use crate::ui::toast::ToastExt;
            if toast.is_error {
                cx.toast_error(toast.message, window);
            } else {
                cx.toast_success(toast.message, window);
            }
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
        let table_name = self.current_table_ref().map(|t| t.qualified_name());
        let filter_input = self.filter_input.clone();
        let filter_has_value = !self.filter_input.read(cx).value().is_empty();
        let limit_input = self.limit_input.clone();
        let active_tab_idx = self.active_tab;
        let tab_count = self.tabs.len();

        let pagination_info = self.current_pagination().cloned();
        let total_pages = self.total_pages();
        let can_prev = self.can_go_prev();
        let can_next = self.can_go_next();

        let focus_mode = self.focus_mode;
        let toolbar_focus = self.toolbar_focus;
        let edit_state = self.edit_state;
        let show_toolbar_focus =
            focus_mode == FocusMode::Toolbar && edit_state == EditState::Navigating;
        let focus_handle = self.focus_handle.clone();

        div()
            .track_focus(&focus_handle)
            .flex()
            .flex_col()
            .flex_1()
            .size_full()
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
                    .when(self.tabs.is_empty(), |d| {
                        d.child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .child("Results"),
                        )
                    })
                    .children(self.tabs.iter().enumerate().map(|(idx, tab)| {
                        let is_active = idx == active_tab_idx;
                        let tab_title = match &tab.source {
                            ResultSource::TableView { table, .. } => table.qualified_name(),
                            _ => tab.title.clone(),
                        };
                        let is_table = matches!(tab.source, ResultSource::TableView { .. });

                        div()
                            .id(("result-tab", idx))
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
                            .when(is_table, |d| {
                                d.child(
                                    div()
                                        .text_size(FontSizes::SM)
                                        .text_color(gpui::rgb(0x4EC9B0))
                                        .child("▦ "),
                                )
                            })
                            .child(tab_title)
                            .child(
                                div()
                                    .id(("close-result-tab", idx))
                                    .ml(Spacing::XS)
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::SM)
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
                        .gap(Spacing::SM)
                        .h(Heights::TOOLBAR)
                        .px(Spacing::SM)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.secondary)
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .child(
                                    div()
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .child("SELECT * FROM"),
                                )
                                .child(
                                    div()
                                        .text_size(FontSizes::SM)
                                        .font_weight(FontWeight::MEDIUM)
                                        .text_color(theme.foreground)
                                        .child(table_name),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .child(
                                    div()
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .child("WHERE"),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .items_center()
                                        .w(px(280.0))
                                        .rounded(Radii::SM)
                                        .when(
                                            show_toolbar_focus
                                                && toolbar_focus == ToolbarFocus::Filter,
                                            |d| d.border_1().border_color(theme.ring),
                                        )
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, _, cx| {
                                                this.switching_input = true;
                                                this.focus_mode = FocusMode::Toolbar;
                                                this.toolbar_focus = ToolbarFocus::Filter;
                                                this.edit_state = EditState::Editing;
                                                cx.notify();
                                            }),
                                        )
                                        .child(
                                            div().flex_1().child(Input::new(&filter_input).small()),
                                        )
                                        .when(filter_has_value, |d| {
                                            d.child(
                                                div()
                                                    .id("clear-filter")
                                                    .w(px(20.0))
                                                    .h(px(20.0))
                                                    .mr(Spacing::XS)
                                                    .flex()
                                                    .items_center()
                                                    .justify_center()
                                                    .rounded(Radii::SM)
                                                    .text_size(FontSizes::SM)
                                                    .text_color(theme.muted_foreground)
                                                    .cursor_pointer()
                                                    .hover(|d| {
                                                        d.bg(theme.secondary)
                                                            .text_color(theme.foreground)
                                                    })
                                                    .on_click(cx.listener(|this, _, window, cx| {
                                                        this.filter_input.update(cx, |input, cx| {
                                                            input.set_value("", window, cx);
                                                        });
                                                        cx.notify();
                                                    }))
                                                    .child("×"),
                                            )
                                        }),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .child(
                                    div()
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .child("LIMIT"),
                                )
                                .child(
                                    div()
                                        .w(px(60.0))
                                        .rounded(Radii::SM)
                                        .when(
                                            show_toolbar_focus
                                                && toolbar_focus == ToolbarFocus::Limit,
                                            |d| d.border_1().border_color(theme.ring),
                                        )
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, _, cx| {
                                                this.switching_input = true;
                                                this.focus_mode = FocusMode::Toolbar;
                                                this.toolbar_focus = ToolbarFocus::Limit;
                                                this.edit_state = EditState::Editing;
                                                cx.notify();
                                            }),
                                        )
                                        .child(Input::new(&limit_input).small()),
                                ),
                        )
                        .child(
                            div()
                                .id("refresh-table")
                                .w(Heights::ICON_MD)
                                .h(Heights::ICON_MD)
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded(Radii::SM)
                                .text_size(FontSizes::BASE)
                                .text_color(theme.muted_foreground)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                                .when(
                                    show_toolbar_focus && toolbar_focus == ToolbarFocus::Refresh,
                                    |d| d.border_1().border_color(theme.ring),
                                )
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.run_table_query(window, cx);
                                    this.focus_table(window, cx);
                                }))
                                .child("↻"),
                        ),
                )
            })
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            if this.focus_mode != FocusMode::Table {
                                this.focus_table(window, cx);
                            }
                        }),
                    )
                    .when(tab_count == 0, |d| {
                        d.flex().items_center().justify_center().child(
                            div()
                                .text_size(FontSizes::BASE)
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
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .border_t_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(format!("{} rows", row_count)),
                    )
                    .child(div().flex().items_center().gap(Spacing::SM).when(
                        is_table_view && pagination_info.is_some(),
                        |d| {
                            let pagination = pagination_info.clone().unwrap();
                            let page = pagination.current_page();
                            let offset = pagination.offset();
                            let start = offset + 1;
                            let end = offset + row_count as u64;

                            d.child(
                                div()
                                    .id("prev-page")
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::XS)
                                    .when(can_prev, |d| {
                                        d.cursor_pointer()
                                            .text_color(theme.foreground)
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.go_to_prev_page(window, cx);
                                            }))
                                    })
                                    .when(!can_prev, |d| {
                                        d.text_color(theme.muted_foreground).opacity(0.5)
                                    })
                                    .child("← Prev"),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(if let Some(total) = total_pages {
                                        format!("Page {}/{} ({}-{})", page, total, start, end)
                                    } else {
                                        format!("Page {} ({}-{})", page, start, end)
                                    }),
                            )
                            .child(
                                div()
                                    .id("next-page")
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::XS)
                                    .when(can_next, |d| {
                                        d.cursor_pointer()
                                            .text_color(theme.foreground)
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.go_to_next_page(window, cx);
                                            }))
                                    })
                                    .when(!can_next, |d| {
                                        d.text_color(theme.muted_foreground).opacity(0.5)
                                    })
                                    .child("Next →"),
                            )
                        },
                    ))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .when(tab_count > 0, |d| {
                                d.child(
                                    div()
                                        .id("export-csv")
                                        .px(Spacing::XS)
                                        .rounded(Radii::SM)
                                        .text_size(FontSizes::XS)
                                        .cursor_pointer()
                                        .text_color(theme.muted_foreground)
                                        .hover(|d| {
                                            d.bg(theme.secondary).text_color(theme.foreground)
                                        })
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.export_results(window, cx);
                                        }))
                                        .child("Export CSV"),
                                )
                            })
                            .child({
                                let mut muted = theme.muted_foreground;
                                muted.a = 0.5;
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(muted)
                                    .child(exec_time)
                            }),
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
