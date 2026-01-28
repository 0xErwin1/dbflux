use crate::app::{AppState, AppStateChanged};
use crate::ui::components::data_table::{
    DataTable, DataTableEvent, DataTableState, Direction, Edge, SortState as TableSortState,
    TableModel,
};
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    CancelToken, DbKind, OrderByColumn, Pagination, QueryRequest, QueryResult, SortDirection,
    TableBrowseRequest, TableRef, TaskId, TaskKind,
};
use dbflux_export::{CsvExporter, Exporter};
use gpui::prelude::FluentBuilder;
use gpui::{Subscription, *};

use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{ActiveTheme, Sizable};
use log::info;
use std::cmp::Ordering;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;
use uuid::Uuid;

pub struct ResultsReceived;

impl EventEmitter<ResultsReceived> for ResultsPane {}

enum ResultSource {
    Query,
    TableView {
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
    },
}

/// Sort state for a result tab (used for in-memory sorting of Query results).
#[derive(Clone, Copy)]
struct SortState {
    column_ix: usize,
    direction: SortDirection,
}

struct ResultTab {
    id: usize,
    title: String,
    source: ResultSource,
    result: QueryResult,
    table_state: Entity<DataTableState>,
    data_table: Entity<DataTable>,
    /// Sort state for Query tabs (in-memory sort).
    sort_state: Option<SortState>,
    /// Original row order for restoring after sort clear (indices into current rows).
    original_row_order: Option<Vec<usize>>,
    /// Subscription to table events (kept alive to receive events).
    subscription: Subscription,
}

struct PendingTableResult {
    profile_id: Uuid,
    table: TableRef,
    pagination: Pagination,
    order_by: Vec<OrderByColumn>,
    total_rows: Option<u64>,
    result: QueryResult,
}

/// Pending request to re-run a table query (triggered by sort change).
struct PendingTableRequery {
    profile_id: Uuid,
    table: TableRef,
    pagination: Pagination,
    order_by: Vec<OrderByColumn>,
    filter: Option<String>,
    total_rows: Option<u64>,
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

    /// Pending re-query triggered by sort change on TableView.
    pending_table_requery: Option<PendingTableRequery>,
    /// Tab index to rebuild after in-memory sort.
    pending_table_rebuild: Option<usize>,

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
            pending_table_requery: None,
            pending_table_rebuild: None,
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
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let tab_id = self.next_tab_id;
        let table_model = Arc::new(TableModel::from(&result));
        let table_state = cx.new(|cx| DataTableState::new(table_model, cx));
        let data_table = cx.new(|cx| DataTable::new("results-table", table_state.clone(), cx));

        let subscription = self.subscribe_to_table_events(&table_state, tab_id, cx);

        let tab = ResultTab {
            id: tab_id,
            title: format!("Result {}", tab_id),
            source: ResultSource::Query,
            result,
            table_state,
            data_table,
            sort_state: None,
            original_row_order: None,
            subscription: subscription,
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

    fn subscribe_to_table_events(
        &self,
        table_state: &Entity<DataTableState>,
        tab_id: usize,
        cx: &mut Context<Self>,
    ) -> Subscription {
        cx.subscribe(
            table_state,
            move |this, _state, event: &DataTableEvent, cx| {
                if let DataTableEvent::SortChanged(sort) = event {
                    match sort {
                        Some(sort_state) => {
                            this.handle_sort_request(
                                tab_id,
                                sort_state.column_ix,
                                sort_state.direction,
                                cx,
                            );
                        }
                        None => {
                            this.handle_sort_clear(tab_id, cx);
                        }
                    }
                }
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_table_result(
        &mut self,
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
        result: QueryResult,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let qualified = table.qualified_name();

        info!(
            "apply_table_result: table={}, order_by={:?}, result_columns={:?}",
            qualified,
            order_by
                .iter()
                .map(|c| format!("{} {:?}", c.name, c.direction))
                .collect::<Vec<_>>(),
            result.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );

        // Determine sort state from order_by for visual indicator
        let initial_sort = order_by.first().and_then(|col| {
            let pos = result.columns.iter().position(|c| c.name == col.name);
            info!(
                "sort_state calculation: looking for '{}' in columns, found at {:?}",
                col.name, pos
            );
            pos.map(|column_ix| TableSortState::new(column_ix, col.direction))
        });
        info!(
            "sort_state for DataTable: {:?}",
            initial_sort.map(|s| (s.column_ix, s.direction))
        );

        // Find or create tab
        let (tab_id, tab_idx) = if let Some(idx) = self.tabs.iter().position(
            |t| matches!(&t.source, ResultSource::TableView { table: tbl, .. } if tbl.qualified_name() == qualified),
        ) {
            (self.tabs[idx].id, Some(idx))
        } else {
            let id = self.next_tab_id;
            self.next_tab_id += 1;
            (id, None)
        };

        let table_model = Arc::new(TableModel::from(&result));
        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            if let Some(sort) = initial_sort {
                state.set_sort_without_emit(sort);
            }
            state
        });
        let data_table = cx.new(|cx| DataTable::new("results-table", table_state.clone(), cx));

        let subscription = self.subscribe_to_table_events(&table_state, tab_id, cx);

        if let Some(idx) = tab_idx {
            let existing_total = match &self.tabs[idx].source {
                ResultSource::TableView { total_rows, .. } => *total_rows,
                _ => None,
            };

            self.tabs[idx].result = result;
            self.tabs[idx].table_state = table_state;
            self.tabs[idx].data_table = data_table;
            self.tabs[idx].source = ResultSource::TableView {
                profile_id,
                table,
                pagination,
                order_by,
                total_rows: total_rows.or(existing_total),
            };
            self.tabs[idx].sort_state = None; // TableView uses server-side sort
            self.tabs[idx].original_row_order = None;
            self.tabs[idx].subscription = subscription;
            self.active_tab = idx;
        } else {
            let tab = ResultTab {
                id: tab_id,
                title: table.name.clone(),
                source: ResultSource::TableView {
                    profile_id,
                    table,
                    pagination,
                    order_by,
                    total_rows,
                },
                result,
                table_state,
                data_table,
                sort_state: None,
                original_row_order: None,
                subscription,
            };
            self.tabs.push(tab);
            self.active_tab = self.tabs.len() - 1;
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

    pub fn view_table_for_connection(
        &mut self,
        profile_id: Uuid,
        table_name: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let table = TableRef::from_qualified(table_name);
        let qualified = table.qualified_name();

        if let Some(idx) = self.tabs.iter().position(
            |t| matches!(&t.source, ResultSource::TableView { table: tbl, .. } if tbl.qualified_name() == qualified),
        ) {
            self.active_tab = idx;
            cx.notify();
            return;
        }

        let db_kind = {
            let state = self.app_state.read(cx);
            state
                .connections
                .get(&profile_id)
                .map(|c| c.connection.kind())
                .unwrap_or(DbKind::Postgres)
        };

        self.filter_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        let order_by = self.get_primary_key_columns_for_connection(profile_id, &table, cx);
        let pagination = Pagination::default();

        self.run_table_query_for_connection(
            profile_id,
            table.clone(),
            pagination,
            order_by,
            None,
            None,
            window,
            cx,
        );
        self.fetch_total_count_for_connection(profile_id, table, None, db_kind, cx);
    }

    fn fetch_total_count_for_connection(
        &mut self,
        profile_id: Uuid,
        table: TableRef,
        filter: Option<String>,
        db_kind: DbKind,
        cx: &mut Context<Self>,
    ) {
        let (conn, active_database) = {
            let state = self.app_state.read(cx);
            match state.connections.get(&profile_id) {
                Some(c) => (Some(c.connection.clone()), c.active_database.clone()),
                None => (None, None),
            }
        };

        let Some(conn) = conn else {
            return;
        };

        let quoted_table = table.quoted_for_kind(db_kind);
        let sql = if let Some(ref f) = filter {
            let trimmed = f.trim();
            if trimmed.is_empty() {
                format!("SELECT COUNT(*) FROM {}", quoted_table)
            } else {
                format!("SELECT COUNT(*) FROM {} WHERE {}", quoted_table, trimmed)
            }
        } else {
            format!("SELECT COUNT(*) FROM {}", quoted_table)
        };

        let request = QueryRequest::new(sql).with_database(active_database);
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

        let (profile_id, table, pagination, order_by, total_rows) = match &tab.source {
            ResultSource::TableView {
                profile_id,
                table,
                pagination,
                order_by,
                total_rows,
            } => (
                *profile_id,
                table.clone(),
                pagination.clone(),
                order_by.clone(),
                *total_rows,
            ),
            _ => return,
        };

        let db_kind = {
            let state = self.app_state.read(cx);
            state
                .connections
                .get(&profile_id)
                .map(|c| c.connection.kind())
                .unwrap_or(DbKind::Postgres)
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

        self.run_table_query_for_connection(
            profile_id,
            table.clone(),
            pagination,
            order_by,
            filter.clone(),
            total_rows,
            window,
            cx,
        );

        if total_rows.is_none() {
            self.fetch_total_count_for_connection(profile_id, table, filter, db_kind, cx);
        }
    }

    fn get_primary_key_columns_for_connection(
        &self,
        profile_id: Uuid,
        table: &TableRef,
        cx: &Context<Self>,
    ) -> Vec<OrderByColumn> {
        let state = self.app_state.read(cx);
        let Some(connected) = state.connections.get(&profile_id) else {
            info!("get_primary_key_columns: connection not found");
            return Vec::new();
        };

        // Check database_schemas first (for MySQL/MariaDB lazy loading)
        if let Some(schema_name) = &table.schema
            && let Some(db_schema) = connected.database_schemas.get(schema_name)
        {
            for t in &db_schema.tables {
                if t.name == table.name {
                    return t
                        .columns
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .filter(|c| c.is_primary_key)
                        .map(|c| OrderByColumn::asc(&c.name))
                        .collect();
                }
            }
        }

        // Fall back to schema.schemas (for PostgreSQL/SQLite)
        let Some(schema) = &connected.schema else {
            return Vec::new();
        };

        for db_schema in &schema.schemas {
            if table.schema.as_deref() == Some(&db_schema.name) || table.schema.is_none() {
                for t in &db_schema.tables {
                    if t.name == table.name {
                        return t
                            .columns
                            .as_deref()
                            .unwrap_or(&[])
                            .iter()
                            .filter(|c| c.is_primary_key)
                            .map(|c| OrderByColumn::asc(&c.name))
                            .collect();
                    }
                }
            }
        }

        Vec::new()
    }

    #[allow(clippy::too_many_arguments)]
    fn run_table_query_for_connection(
        &mut self,
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
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

        let (conn, db_kind, active_database) = {
            let state = self.app_state.read(cx);
            match state.connections.get(&profile_id) {
                Some(c) => (
                    Some(c.connection.clone()),
                    c.connection.kind(),
                    c.active_database.clone(),
                ),
                None => {
                    cx.toast_error("Connection not found", window);
                    return;
                }
            }
        };

        let Some(conn) = conn else {
            cx.toast_error("Connection not available", window);
            return;
        };

        let sql = request.build_sql_for_kind(db_kind);
        info!("Running table query: {}", sql);

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

        let query_request = QueryRequest::new(sql).with_database(active_database);
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
                                profile_id,
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

        let (profile_id, table, pagination, order_by, total_rows) = match &tab.source {
            ResultSource::TableView {
                profile_id,
                table,
                pagination,
                order_by,
                total_rows,
            } => (
                *profile_id,
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

        self.run_table_query_for_connection(
            profile_id, table, pagination, order_by, filter, total_rows, window, cx,
        );
    }

    pub fn go_to_prev_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tabs.get(self.active_tab) else {
            return;
        };

        let (profile_id, table, pagination, order_by, total_rows) = match &tab.source {
            ResultSource::TableView {
                profile_id,
                table,
                pagination,
                order_by,
                total_rows,
            } => {
                let Some(prev) = pagination.prev_page() else {
                    return;
                };
                (
                    *profile_id,
                    table.clone(),
                    prev,
                    order_by.clone(),
                    *total_rows,
                )
            }
            _ => return,
        };

        let filter_value = self.filter_input.read(cx).value();
        let filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        self.run_table_query_for_connection(
            profile_id, table, pagination, order_by, filter, total_rows, window, cx,
        );
    }

    /// Handle sort request from table delegate.
    fn handle_sort_request(
        &mut self,
        tab_id: usize,
        col_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        let Some(tab_idx) = self.tabs.iter().position(|t| t.id == tab_id) else {
            return;
        };

        // Extract info we need from the tab first
        let tab = &self.tabs[tab_idx];
        let col_name = tab
            .result
            .columns
            .get(col_ix)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        let source_info = match &tab.source {
            ResultSource::TableView {
                profile_id,
                table,
                pagination,
                total_rows,
                ..
            } => Some((
                *profile_id,
                table.clone(),
                pagination.reset_offset(),
                *total_rows,
            )),
            ResultSource::Query => None,
        };

        if let Some((profile_id, table, new_pagination, total_rows)) = source_info {
            // Server-side sort: update source and queue re-query
            let new_order_by = vec![OrderByColumn {
                name: col_name,
                direction,
            }];

            let filter_value = self.filter_input.read(cx).value();
            let filter = if filter_value.trim().is_empty() {
                None
            } else {
                Some(filter_value.to_string())
            };

            // Update source immediately for UI consistency
            self.tabs[tab_idx].source = ResultSource::TableView {
                profile_id,
                table: table.clone(),
                pagination: new_pagination.clone(),
                order_by: new_order_by.clone(),
                total_rows,
            };

            // Queue re-query
            self.pending_table_requery = Some(PendingTableRequery {
                profile_id,
                table,
                pagination: new_pagination,
                order_by: new_order_by,
                filter,
                total_rows,
            });

            cx.notify();
        } else {
            // Client-side sort: sort in memory
            self.apply_local_sort(tab_idx, col_ix, direction, cx);
        }
    }

    /// Handle sort clear (restore original order).
    fn handle_sort_clear(&mut self, tab_id: usize, cx: &mut Context<Self>) {
        let Some(tab_idx) = self.tabs.iter().position(|t| t.id == tab_id) else {
            return;
        };

        // Extract info we need from the tab first
        let source_info = match &self.tabs[tab_idx].source {
            ResultSource::TableView {
                profile_id,
                table,
                pagination,
                total_rows,
                ..
            } => Some((
                *profile_id,
                table.clone(),
                pagination.reset_offset(),
                *total_rows,
            )),
            ResultSource::Query => None,
        };

        if let Some((profile_id, table, new_pagination, total_rows)) = source_info {
            // Restore to PK order
            let pk_order = self.get_primary_key_columns_for_connection(profile_id, &table, cx);

            let filter_value = self.filter_input.read(cx).value();
            let filter = if filter_value.trim().is_empty() {
                None
            } else {
                Some(filter_value.to_string())
            };

            // Update source
            self.tabs[tab_idx].source = ResultSource::TableView {
                profile_id,
                table: table.clone(),
                pagination: new_pagination.clone(),
                order_by: pk_order.clone(),
                total_rows,
            };

            // Queue re-query
            self.pending_table_requery = Some(PendingTableRequery {
                profile_id,
                table,
                pagination: new_pagination,
                order_by: pk_order,
                filter,
                total_rows,
            });

            cx.notify();
        } else {
            // Restore original row order for Query tab
            let tab = &mut self.tabs[tab_idx];

            if let Some(original_order) = tab.original_row_order.take() {
                // Create mapping from current position to original position
                let mut restore_indices: Vec<(usize, usize)> = original_order
                    .iter()
                    .enumerate()
                    .map(|(current, &original)| (original, current))
                    .collect();
                restore_indices.sort_by_key(|(orig, _)| *orig);

                let rows = std::mem::take(&mut tab.result.rows);
                tab.result.rows = restore_indices
                    .into_iter()
                    .map(|(_, current)| rows[current].clone())
                    .collect();
            }

            tab.sort_state = None;
            self.pending_table_rebuild = Some(tab_idx);
            cx.notify();
        }
    }

    /// Apply in-memory sort to a Query tab.
    fn apply_local_sort(
        &mut self,
        tab_idx: usize,
        col_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        let tab = &mut self.tabs[tab_idx];

        // Save original order if this is the first sort
        if tab.original_row_order.is_none() {
            tab.original_row_order = Some((0..tab.result.rows.len()).collect());
        }

        // Sort using indices for tracking
        let mut indices: Vec<usize> = (0..tab.result.rows.len()).collect();
        indices.sort_by(|&a, &b| {
            let val_a = tab.result.rows[a].get(col_ix);
            let val_b = tab.result.rows[b].get(col_ix);

            let cmp = match (val_a, val_b) {
                (Some(a), Some(b)) => a.cmp(b),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };

            match direction {
                SortDirection::Ascending => cmp,
                SortDirection::Descending => cmp.reverse(),
            }
        });

        // Reorder rows according to sorted indices
        let sorted_rows: Vec<_> = indices
            .iter()
            .map(|&i| tab.result.rows[i].clone())
            .collect();
        tab.result.rows = sorted_rows;

        // Update original_row_order to map new order -> original
        if let Some(ref mut orig) = tab.original_row_order {
            *orig = indices.iter().map(|&i| orig[i]).collect();
        }

        tab.sort_state = Some(SortState {
            column_ix: col_ix,
            direction,
        });
        self.pending_table_rebuild = Some(tab_idx);
        cx.notify();
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

    /// Get current sort info for display: (column_name, direction, is_server_sort)
    fn current_sort_info(&self) -> Option<(String, SortDirection, bool)> {
        let tab = self.active_tab()?;

        match &tab.source {
            ResultSource::TableView { order_by, .. } => {
                // Server-side sort from order_by
                let result = order_by
                    .first()
                    .map(|col| (col.name.clone(), col.direction, true));
                log::debug!(
                    "current_sort_info TableView: order_by.len()={}, result={:?}",
                    order_by.len(),
                    result
                );
                result
            }
            ResultSource::Query => {
                // Client-side sort from sort_state
                tab.sort_state.and_then(|state| {
                    tab.result
                        .columns
                        .get(state.column_ix)
                        .map(|col| (col.name.clone(), state.direction, false))
                })
            }
        }
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
        if tab.result.rows.is_empty() {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.move_active(Direction::Down, false, cx);
        });
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        if tab.result.rows.is_empty() {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.move_active(Direction::Up, false, cx);
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
            state.move_to_edge(Edge::Home, false, cx);
        });
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        if tab.result.rows.is_empty() {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.move_to_edge(Edge::End, false, cx);
        });
    }

    pub fn column_left(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        if tab.result.columns.is_empty() {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.move_active(Direction::Left, false, cx);
        });
    }

    pub fn column_right(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            return;
        };
        if tab.result.columns.is_empty() {
            return;
        }

        let table_state = tab.table_state.clone();
        table_state.update(cx, |state, cx| {
            state.move_active(Direction::Right, false, cx);
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
                pending.profile_id,
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

        // Process pending table re-query (triggered by sort change on TableView)
        if let Some(requery) = self.pending_table_requery.take() {
            self.run_table_query_for_connection(
                requery.profile_id,
                requery.table,
                requery.pagination,
                requery.order_by,
                requery.filter,
                requery.total_rows,
                window,
                cx,
            );
        }

        // Rebuild table state after in-memory sort
        if let Some(tab_idx) = self.pending_table_rebuild.take()
            && let Some(tab) = self.tabs.get(tab_idx)
        {
            let tab_id = tab.id;
            let result = tab.result.clone();
            let sort_state = tab.sort_state;

            let table_model = Arc::new(TableModel::from(&result));
            let table_state = cx.new(|cx| {
                let mut state = DataTableState::new(table_model, cx);
                if let Some(local_sort) = sort_state {
                    state.set_sort_without_emit(TableSortState::new(
                        local_sort.column_ix,
                        local_sort.direction,
                    ));
                }
                state
            });
            let data_table = cx.new(|cx| DataTable::new("results-table", table_state.clone(), cx));

            let subscription = self.subscribe_to_table_events(&table_state, tab_id, cx);

            if let Some(tab) = self.tabs.get_mut(tab_idx) {
                tab.table_state = table_state;
                tab.data_table = data_table;
                tab.subscription = subscription;
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
        let sort_info = self.current_sort_info();

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
                                                        this.filter_input.update(
                                                            cx,
                                                            |input, cx| {
                                                                input.set_value("", window, cx);
                                                            },
                                                        );
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
                        self.active_tab().map(|t| t.data_table.clone()),
                        |d, data_table| d.child(data_table),
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
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(format!("{} rows", row_count)),
                            )
                            .when_some(sort_info, |d, (col_name, direction, is_server)| {
                                let arrow = match direction {
                                    SortDirection::Ascending => "↑",
                                    SortDirection::Descending => "↓",
                                };
                                let mode = if is_server { "db" } else { "local" };
                                d.child(
                                    div()
                                        .text_size(FontSizes::XS)
                                        .text_color(theme.muted_foreground)
                                        .child(format!("{} {} ({})", arrow, col_name, mode)),
                                )
                            }),
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
