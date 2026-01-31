use crate::app::AppState;
use crate::keymap::Command;
use crate::ui::components::data_table::{
    DataTable, DataTableEvent, DataTableState, Direction, Edge, SortState as TableSortState,
    TableModel,
};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
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

/// Source of data for the grid panel.
#[derive(Clone)]
pub enum DataSource {
    /// Table with server-side pagination and sorting.
    Table {
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
    },
    /// Static query result (in-memory sorting only).
    QueryResult {
        #[allow(dead_code)]
        result: Arc<QueryResult>,
        #[allow(dead_code)]
        original_query: String,
    },
}

impl DataSource {
    pub fn is_table(&self) -> bool {
        matches!(self, DataSource::Table { .. })
    }

    pub fn table_ref(&self) -> Option<&TableRef> {
        match self {
            DataSource::Table { table, .. } => Some(table),
            DataSource::QueryResult { .. } => None,
        }
    }

    pub fn pagination(&self) -> Option<&Pagination> {
        match self {
            DataSource::Table { pagination, .. } => Some(pagination),
            DataSource::QueryResult { .. } => None,
        }
    }

    pub fn total_rows(&self) -> Option<u64> {
        match self {
            DataSource::Table { total_rows, .. } => *total_rows,
            DataSource::QueryResult { .. } => None,
        }
    }
}

/// Events emitted by DataGridPanel.
#[derive(Clone, Debug)]
pub enum DataGridEvent {
    /// Request to hide the results panel.
    RequestHide,
    /// Request to maximize/restore the results panel.
    RequestToggleMaximize,
}

/// Internal state for grid loading/ready/error.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum GridState {
    #[default]
    Ready,
    Loading,
    Error,
}

/// Focus mode within the panel.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum GridFocusMode {
    #[default]
    Table,
    Toolbar,
}

/// Which toolbar element is focused.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum ToolbarFocus {
    #[default]
    Filter,
    Limit,
    Refresh,
}

impl ToolbarFocus {
    pub fn left(self) -> Self {
        match self {
            ToolbarFocus::Filter => ToolbarFocus::Filter,
            ToolbarFocus::Limit => ToolbarFocus::Filter,
            ToolbarFocus::Refresh => ToolbarFocus::Limit,
        }
    }

    pub fn right(self) -> Self {
        match self {
            ToolbarFocus::Filter => ToolbarFocus::Limit,
            ToolbarFocus::Limit => ToolbarFocus::Refresh,
            ToolbarFocus::Refresh => ToolbarFocus::Refresh,
        }
    }
}

/// Edit state for toolbar inputs.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum EditState {
    #[default]
    Navigating,
    Editing,
}

/// Sort state for in-memory sorting (QueryResult source only).
#[derive(Clone, Copy)]
struct LocalSortState {
    column_ix: usize,
    direction: SortDirection,
}

struct RunningQuery {
    #[allow(dead_code)]
    task_id: TaskId,
    #[allow(dead_code)]
    cancel_token: CancelToken,
}

struct PendingRequery {
    profile_id: Uuid,
    table: TableRef,
    pagination: Pagination,
    order_by: Vec<OrderByColumn>,
    #[allow(dead_code)]
    filter: Option<String>,
    total_rows: Option<u64>,
}

struct PendingTotalCount {
    table_qualified: String,
    total: u64,
}

struct PendingToast {
    message: String,
    is_error: bool,
}

/// Reusable data grid panel with filter bar, grid, toolbar, and status bar.
/// Used both embedded in ScriptDocument and as standalone DataDocument.
pub struct DataGridPanel {
    source: DataSource,
    app_state: Entity<AppState>,

    // Current result data
    result: QueryResult,
    data_table: Option<Entity<DataTable>>,
    table_state: Option<Entity<DataTableState>>,
    table_subscription: Option<Subscription>,

    // Filter & limit inputs
    filter_input: Entity<InputState>,
    limit_input: Entity<InputState>,

    // In-memory sort state (for QueryResult source)
    local_sort_state: Option<LocalSortState>,
    original_row_order: Option<Vec<usize>>,

    // Async state
    state: GridState,
    running_query: Option<RunningQuery>,
    pending_requery: Option<PendingRequery>,
    pending_total_count: Option<PendingTotalCount>,
    pending_rebuild: bool,
    pending_toast: Option<PendingToast>,

    // Focus
    focus_handle: FocusHandle,
    focus_mode: GridFocusMode,
    toolbar_focus: ToolbarFocus,
    edit_state: EditState,
    switching_input: bool,

    // Panel controls (shown when embedded in SqlQueryDocument)
    show_panel_controls: bool,
    is_maximized: bool,
}

impl DataGridPanel {
    /// Create a new panel for browsing a table (server-side pagination).
    pub fn new_for_table(
        profile_id: Uuid,
        table: TableRef,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let order_by = Self::get_primary_key_columns(&app_state, profile_id, &table, cx);
        let pagination = Pagination::default();

        let source = DataSource::Table {
            profile_id,
            table: table.clone(),
            pagination,
            order_by,
            total_rows: None,
        };

        let mut panel = Self::new_internal(source, app_state, window, cx);
        panel.refresh(window, cx);
        panel
    }

    /// Create a new panel for displaying a query result (in-memory sorting).
    pub fn new_for_result(
        result: Arc<QueryResult>,
        original_query: String,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let source = DataSource::QueryResult {
            result: result.clone(),
            original_query,
        };

        let mut panel = Self::new_internal(source, app_state, window, cx);
        panel.set_result((*result).clone(), cx);
        panel
    }

    fn new_internal(
        source: DataSource,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
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
                    this.refresh(window, cx);
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
                    this.refresh(window, cx);
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
            source,
            app_state,
            result: QueryResult::empty(),
            data_table: None,
            table_state: None,
            table_subscription: None,
            filter_input,
            limit_input,
            local_sort_state: None,
            original_row_order: None,
            state: GridState::Ready,
            running_query: None,
            pending_requery: None,
            pending_total_count: None,
            pending_rebuild: false,
            pending_toast: None,
            focus_handle,
            focus_mode: GridFocusMode::default(),
            toolbar_focus: ToolbarFocus::default(),
            edit_state: EditState::default(),
            switching_input: false,
            show_panel_controls: false,
            is_maximized: false,
        }
    }

    /// Enable panel control buttons (hide, maximize) for embedded panels.
    #[allow(dead_code)]
    pub fn with_panel_controls(mut self) -> Self {
        self.show_panel_controls = true;
        self
    }

    /// Update the maximized state (called by parent).
    pub fn set_maximized(&mut self, maximized: bool, cx: &mut Context<Self>) {
        self.is_maximized = maximized;
        cx.notify();
    }

    /// Update the result data (for QueryResult source or after table fetch).
    pub fn set_result(&mut self, result: QueryResult, cx: &mut Context<Self>) {
        self.result = result;
        self.rebuild_table(None, cx);
        self.state = GridState::Ready;
        cx.notify();
    }

    /// Update source to a new query result (used by ScriptDocument).
    pub fn set_query_result(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        cx: &mut Context<Self>,
    ) {
        self.source = DataSource::QueryResult {
            result: result.clone(),
            original_query: query,
        };
        self.local_sort_state = None;
        self.original_row_order = None;
        self.set_result((*result).clone(), cx);
    }

    fn rebuild_table(&mut self, initial_sort: Option<TableSortState>, cx: &mut Context<Self>) {
        let table_model = Arc::new(TableModel::from(&self.result));
        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            if let Some(sort) = initial_sort {
                state.set_sort_without_emit(sort);
            }
            state
        });
        let data_table = cx.new(|cx| DataTable::new("data-grid-table", table_state.clone(), cx));

        let subscription =
            cx.subscribe(&table_state, |this, _state, event: &DataTableEvent, cx| {
                if let DataTableEvent::SortChanged(sort) = event {
                    match sort {
                        Some(sort_state) => {
                            this.handle_sort_request(
                                sort_state.column_ix,
                                sort_state.direction,
                                cx,
                            );
                        }
                        None => {
                            this.handle_sort_clear(cx);
                        }
                    }
                }
            });

        self.table_state = Some(table_state);
        self.data_table = Some(data_table);
        self.table_subscription = Some(subscription);
    }

    // === Refresh / Query Execution ===

    /// Refresh data from source.
    pub fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::Table {
                profile_id,
                table,
                pagination,
                order_by,
                total_rows,
            } => {
                self.run_table_query(
                    *profile_id,
                    table.clone(),
                    pagination.clone(),
                    order_by.clone(),
                    *total_rows,
                    window,
                    cx,
                );
            }
            DataSource::QueryResult { .. } => {
                // QueryResult is static, nothing to refresh
            }
        }
    }

    fn run_table_query(
        &mut self,
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.running_query.is_some() {
            cx.toast_error("A query is already running", window);
            return;
        }

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
                cx.toast_warning("Limit must be greater than 0", window);
                pagination
            }
            Ok(limit) if limit != pagination.limit() => pagination.with_limit(limit).reset_offset(),
            Ok(_) => pagination,
            Err(_) if !limit_str.is_empty() => {
                cx.toast_warning("Invalid limit value", window);
                pagination
            }
            Err(_) => pagination,
        };

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

        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            state.start_task(
                TaskKind::Query,
                format!("SELECT * FROM {}", table.qualified_name()),
            )
        });

        self.running_query = Some(RunningQuery {
            task_id,
            cancel_token: cancel_token.clone(),
        });
        self.state = GridState::Loading;
        cx.notify();

        let query_request = QueryRequest::new(sql).with_database(active_database);
        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();
        let conn_for_cleanup = conn.clone();

        // Clone for use in spawn closure
        let table_for_spawn = table.clone();
        let pagination_for_spawn = pagination.clone();
        let order_by_for_spawn = order_by.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&query_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |panel, _cx| {
                    panel.running_query = None;
                });

                if cancel_token.is_cancelled() {
                    log::info!("Query was cancelled, discarding result");
                    if let Err(e) = conn_for_cleanup.cleanup_after_cancel() {
                        log::warn!("Cleanup after cancel failed: {}", e);
                    }
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

                        entity.update(cx, |panel, cx| {
                            panel.apply_table_result(
                                profile_id,
                                table_for_spawn,
                                pagination_for_spawn,
                                order_by_for_spawn,
                                total_rows,
                                query_result.clone(),
                                cx,
                            );
                        });
                    }
                    Err(e) => {
                        log::error!("Query failed: {}", e);

                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.to_string());
                        });

                        entity.update(cx, |panel, cx| {
                            panel.state = GridState::Error;
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Query failed: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }
            })
            .ok();
        })
        .detach();

        // Fetch total count if not known
        if total_rows.is_none() {
            self.fetch_total_count(profile_id, table, filter, db_kind, cx);
        }
    }

    fn apply_table_result(
        &mut self,
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
        result: QueryResult,
        cx: &mut Context<Self>,
    ) {
        // Determine sort state from order_by for visual indicator
        let initial_sort = order_by.first().and_then(|col| {
            let pos = result.columns.iter().position(|c| c.name == col.name);
            pos.map(|column_ix| TableSortState::new(column_ix, col.direction))
        });

        // Preserve existing total_rows if not provided
        let existing_total = match &self.source {
            DataSource::Table { total_rows, .. } => *total_rows,
            _ => None,
        };

        self.source = DataSource::Table {
            profile_id,
            table,
            pagination,
            order_by,
            total_rows: total_rows.or(existing_total),
        };

        self.result = result;
        self.local_sort_state = None;
        self.original_row_order = None;
        self.rebuild_table(initial_sort, cx);
        self.state = GridState::Ready;
        cx.notify();
    }

    fn fetch_total_count(
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
        let entity = cx.entity().clone();
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
                    entity.update(cx, |panel, cx| {
                        panel.pending_total_count = Some(PendingTotalCount {
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

    fn apply_total_count(&mut self, table_qualified: String, total: u64, cx: &mut Context<Self>) {
        if let DataSource::Table {
            table, total_rows, ..
        } = &mut self.source
        {
            if table.qualified_name() == table_qualified {
                *total_rows = Some(total);
                cx.notify();
            }
        }
    }

    // === Sorting ===

    fn handle_sort_request(
        &mut self,
        col_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        let col_name = self
            .result
            .columns
            .get(col_ix)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        // Extract values before mutating self.source
        let table_info = match &self.source {
            DataSource::Table {
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
            DataSource::QueryResult { .. } => None,
        };

        if let Some((profile_id, table, new_pagination, total_rows)) = table_info {
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
            self.source = DataSource::Table {
                profile_id,
                table: table.clone(),
                pagination: new_pagination.clone(),
                order_by: new_order_by.clone(),
                total_rows,
            };

            // Queue re-query
            self.pending_requery = Some(PendingRequery {
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
            self.apply_local_sort(col_ix, direction, cx);
        }
    }

    fn handle_sort_clear(&mut self, cx: &mut Context<Self>) {
        // Extract values before mutating self.source
        let table_info = match &self.source {
            DataSource::Table {
                profile_id,
                table,
                pagination,
                total_rows,
                ..
            } => {
                let pk_order =
                    Self::get_primary_key_columns(&self.app_state, *profile_id, table, cx);
                Some((
                    *profile_id,
                    table.clone(),
                    pagination.reset_offset(),
                    *total_rows,
                    pk_order,
                ))
            }
            DataSource::QueryResult { .. } => None,
        };

        if let Some((profile_id, table, new_pagination, total_rows, pk_order)) = table_info {
            let filter_value = self.filter_input.read(cx).value();
            let filter = if filter_value.trim().is_empty() {
                None
            } else {
                Some(filter_value.to_string())
            };

            self.source = DataSource::Table {
                profile_id,
                table: table.clone(),
                pagination: new_pagination.clone(),
                order_by: pk_order.clone(),
                total_rows,
            };

            self.pending_requery = Some(PendingRequery {
                profile_id,
                table,
                pagination: new_pagination,
                order_by: pk_order,
                filter,
                total_rows,
            });

            cx.notify();
        } else {
            // Restore original row order
            if let Some(original_order) = self.original_row_order.take() {
                let mut restore_indices: Vec<(usize, usize)> = original_order
                    .iter()
                    .enumerate()
                    .map(|(current, &original)| (original, current))
                    .collect();
                restore_indices.sort_by_key(|(orig, _)| *orig);

                let rows = std::mem::take(&mut self.result.rows);
                self.result.rows = restore_indices
                    .into_iter()
                    .map(|(_, current)| rows[current].clone())
                    .collect();
            }

            self.local_sort_state = None;
            self.pending_rebuild = true;
            cx.notify();
        }
    }

    fn apply_local_sort(
        &mut self,
        col_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        // Save original order if this is the first sort
        if self.original_row_order.is_none() {
            self.original_row_order = Some((0..self.result.rows.len()).collect());
        }

        // Sort using indices for tracking
        let mut indices: Vec<usize> = (0..self.result.rows.len()).collect();
        indices.sort_by(|&a, &b| {
            let val_a = self.result.rows[a].get(col_ix);
            let val_b = self.result.rows[b].get(col_ix);

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
            .map(|&i| self.result.rows[i].clone())
            .collect();
        self.result.rows = sorted_rows;

        // Update original_row_order to map new order -> original
        if let Some(ref mut orig) = self.original_row_order {
            *orig = indices.iter().map(|&i| orig[i]).collect();
        }

        self.local_sort_state = Some(LocalSortState {
            column_ix: col_ix,
            direction,
        });
        self.pending_rebuild = true;
        cx.notify();
    }

    // === Pagination ===

    pub fn go_to_next_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let DataSource::Table {
            profile_id,
            table,
            pagination,
            order_by,
            total_rows,
        } = &self.source
        else {
            return;
        };

        let filter_value = self.filter_input.read(cx).value();
        let _filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        self.run_table_query(
            *profile_id,
            table.clone(),
            pagination.next_page(),
            order_by.clone(),
            *total_rows,
            window,
            cx,
        );
    }

    pub fn go_to_prev_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let DataSource::Table {
            profile_id,
            table,
            pagination,
            order_by,
            total_rows,
        } = &self.source
        else {
            return;
        };

        let Some(prev) = pagination.prev_page() else {
            return;
        };

        let filter_value = self.filter_input.read(cx).value();
        let _filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        self.run_table_query(
            *profile_id,
            table.clone(),
            prev,
            order_by.clone(),
            *total_rows,
            window,
            cx,
        );
    }

    fn can_go_prev(&self) -> bool {
        self.source
            .pagination()
            .map(|p| !p.is_first_page())
            .unwrap_or(false)
    }

    fn can_go_next(&self) -> bool {
        let Some(pagination) = self.source.pagination() else {
            return false;
        };

        if let Some(total) = self.source.total_rows() {
            let next_offset = pagination.offset() + pagination.limit() as u64;
            return next_offset < total;
        }

        self.result.row_count() >= pagination.limit() as usize
    }

    fn total_pages(&self) -> Option<u64> {
        let pagination = self.source.pagination()?;
        let total = self.source.total_rows()?;
        let limit = pagination.limit() as u64;
        if limit == 0 {
            return Some(1);
        }
        Some(total.div_ceil(limit))
    }

    // === Navigation ===

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Down, false, cx);
            });
        }
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Up, false, cx);
            });
        }
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_to_edge(Edge::Home, false, cx);
            });
        }
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_to_edge(Edge::End, false, cx);
            });
        }
    }

    pub fn column_left(&mut self, cx: &mut Context<Self>) {
        if self.result.columns.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Left, false, cx);
            });
        }
    }

    pub fn column_right(&mut self, cx: &mut Context<Self>) {
        if self.result.columns.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Right, false, cx);
            });
        }
    }

    // === Focus Management ===

    #[allow(dead_code)]
    pub fn focus_mode(&self) -> GridFocusMode {
        self.focus_mode
    }

    pub fn focus_toolbar(&mut self, cx: &mut Context<Self>) {
        if !self.source.is_table() {
            return;
        }
        self.focus_mode = GridFocusMode::Toolbar;
        self.toolbar_focus = ToolbarFocus::Filter;
        self.edit_state = EditState::Navigating;
        cx.notify();
    }

    pub fn focus_table(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = GridFocusMode::Table;
        self.edit_state = EditState::Navigating;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    pub fn toolbar_left(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != GridFocusMode::Toolbar {
            return;
        }
        self.toolbar_focus = self.toolbar_focus.left();
        cx.notify();
    }

    pub fn toolbar_right(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != GridFocusMode::Toolbar {
            return;
        }
        self.toolbar_focus = self.toolbar_focus.right();
        cx.notify();
    }

    pub fn toolbar_execute(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_mode != GridFocusMode::Toolbar {
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
                self.refresh(window, cx);
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

    // === Command Dispatch ===

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // Handle toolbar mode commands
        if self.focus_mode == GridFocusMode::Toolbar {
            match cmd {
                Command::Cancel => {
                    self.focus_table(window, cx);
                    return true;
                }
                Command::FocusLeft | Command::ColumnLeft => {
                    self.toolbar_left(cx);
                    return true;
                }
                Command::FocusRight | Command::ColumnRight => {
                    self.toolbar_right(cx);
                    return true;
                }
                Command::Execute => {
                    self.toolbar_execute(window, cx);
                    return true;
                }
                _ => {}
            }
        }

        // Handle table mode commands
        match cmd {
            Command::FocusToolbar => {
                self.focus_toolbar(cx);
                true
            }
            Command::SelectNext | Command::FocusDown => {
                self.select_next(cx);
                true
            }
            Command::SelectPrev | Command::FocusUp => {
                self.select_prev(cx);
                true
            }
            Command::SelectFirst => {
                self.select_first(cx);
                true
            }
            Command::SelectLast => {
                self.select_last(cx);
                true
            }
            Command::ColumnLeft | Command::FocusLeft => {
                self.column_left(cx);
                true
            }
            Command::ColumnRight | Command::FocusRight => {
                self.column_right(cx);
                true
            }
            Command::ResultsNextPage | Command::PageDown => {
                self.go_to_next_page(window, cx);
                true
            }
            Command::ResultsPrevPage | Command::PageUp => {
                self.go_to_prev_page(window, cx);
                true
            }
            Command::RefreshSchema => {
                self.refresh(window, cx);
                true
            }
            Command::ExportResults => {
                self.export_results(window, cx);
                true
            }
            _ => false,
        }
    }

    // === Export ===

    pub fn export_results(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            cx.toast_error("No results to export", window);
            return;
        }

        let result = self.result.clone();
        let suggested_name = match &self.source {
            DataSource::Table { table, .. } => format!("{}.csv", table.name),
            DataSource::QueryResult { .. } => {
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
                entity.update(cx, |panel, cx| {
                    panel.pending_toast = Some(PendingToast { message, is_error });
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub fn request_hide(&mut self, cx: &mut Context<Self>) {
        cx.emit(DataGridEvent::RequestHide);
    }

    pub fn request_toggle_maximize(&mut self, cx: &mut Context<Self>) {
        cx.emit(DataGridEvent::RequestToggleMaximize);
    }

    // === Helpers ===

    fn get_primary_key_columns(
        app_state: &Entity<AppState>,
        profile_id: Uuid,
        table: &TableRef,
        cx: &Context<Self>,
    ) -> Vec<OrderByColumn> {
        let state = app_state.read(cx);
        let Some(connected) = state.connections.get(&profile_id) else {
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

    fn current_sort_info(&self) -> Option<(String, SortDirection, bool)> {
        match &self.source {
            DataSource::Table { order_by, .. } => order_by
                .first()
                .map(|col| (col.name.clone(), col.direction, true)),
            DataSource::QueryResult { .. } => self.local_sort_state.and_then(|state| {
                self.result
                    .columns
                    .get(state.column_ix)
                    .map(|col| (col.name.clone(), state.direction, false))
            }),
        }
    }

    #[allow(dead_code)]
    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    #[allow(dead_code)]
    pub fn result(&self) -> &QueryResult {
        &self.result
    }

    pub fn source(&self) -> &DataSource {
        &self.source
    }
}

impl EventEmitter<DataGridEvent> for DataGridPanel {}

impl Render for DataGridPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Process pending state
        if let Some(pending) = self.pending_total_count.take() {
            self.apply_total_count(pending.table_qualified, pending.total, cx);
        }

        if let Some(toast) = self.pending_toast.take() {
            if toast.is_error {
                cx.toast_error(toast.message, window);
            } else {
                cx.toast_success(toast.message, window);
            }
        }

        if let Some(requery) = self.pending_requery.take() {
            self.run_table_query(
                requery.profile_id,
                requery.table,
                requery.pagination,
                requery.order_by,
                requery.total_rows,
                window,
                cx,
            );
        }

        if self.pending_rebuild {
            self.pending_rebuild = false;
            let sort = self
                .local_sort_state
                .map(|s| TableSortState::new(s.column_ix, s.direction));
            self.rebuild_table(sort, cx);
        }

        // Clone theme colors to avoid borrow conflicts with cx
        let theme = cx.theme().clone();

        let row_count = self.result.row_count();
        let exec_time = format!("{}ms", self.result.execution_time.as_millis());

        let is_table_view = self.source.is_table();
        let table_name = self.source.table_ref().map(|t| t.qualified_name());
        let filter_input = self.filter_input.clone();
        let filter_has_value = !self.filter_input.read(cx).value().is_empty();
        let limit_input = self.limit_input.clone();

        let pagination_info = self.source.pagination().cloned();
        let total_pages = self.total_pages();
        let can_prev = self.can_go_prev();
        let can_next = self.can_go_next();
        let sort_info = self.current_sort_info();

        let focus_mode = self.focus_mode;
        let toolbar_focus = self.toolbar_focus;
        let edit_state = self.edit_state;
        let show_toolbar_focus =
            focus_mode == GridFocusMode::Toolbar && edit_state == EditState::Navigating;
        let focus_handle = self.focus_handle.clone();

        let has_data = !self.result.rows.is_empty();
        let is_loading = self.state == GridState::Loading;
        let muted_fg = theme.muted_foreground;

        let show_panel_controls = self.show_panel_controls;
        let is_maximized = self.is_maximized;

        div()
            .track_focus(&focus_handle)
            .flex()
            .flex_col()
            .size_full()
            // Toolbar (only for Table source)
            .when(is_table_view, |d| {
                let table_name = table_name.clone().unwrap_or_default();
                d.child(self.render_toolbar(
                    &table_name,
                    &filter_input,
                    filter_has_value,
                    &limit_input,
                    show_toolbar_focus,
                    toolbar_focus,
                    &theme,
                    cx,
                ))
            })
            // Header bar with panel controls (only when embedded)
            .when(show_panel_controls && has_data, |d| {
                d.child(
                    div()
                        .flex()
                        .items_center()
                        .justify_end()
                        .h(Heights::ROW_COMPACT)
                        .px(Spacing::SM)
                        .border_b_1()
                        .border_color(theme.border)
                        .child(
                            div()
                                .id("toggle-maximize")
                                .flex()
                                .items_center()
                                .justify_center()
                                .w(px(24.0))
                                .h(px(24.0))
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.request_toggle_maximize(cx);
                                }))
                                .child(
                                    svg()
                                        .path(if is_maximized {
                                            AppIcon::Minimize2.path()
                                        } else {
                                            AppIcon::Maximize2.path()
                                        })
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                ),
                        )
                        .child(
                            div()
                                .id("hide-panel")
                                .flex()
                                .items_center()
                                .justify_center()
                                .w(px(24.0))
                                .h(px(24.0))
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.request_hide(cx);
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::PanelBottomClose.path())
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                ),
                        ),
                )
            })
            // Grid
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            if this.focus_mode != GridFocusMode::Table {
                                this.focus_table(window, cx);
                            }
                        }),
                    )
                    .when(!has_data, |d| {
                        d.flex().items_center().justify_center().child(
                            div()
                                .text_size(FontSizes::BASE)
                                .text_color(muted_fg)
                                .child(if is_loading { "Loading..." } else { "No data" }),
                        )
                    })
                    .when_some(self.data_table.clone(), |d, data_table| d.child(data_table)),
            )
            // Status bar
            .child(self.render_status_bar(
                row_count,
                &exec_time,
                is_table_view,
                pagination_info,
                total_pages,
                can_prev,
                can_next,
                sort_info,
                has_data,
                &theme,
                cx,
            ))
    }
}

impl DataGridPanel {
    fn render_toolbar(
        &self,
        table_name: &str,
        filter_input: &Entity<InputState>,
        filter_has_value: bool,
        limit_input: &Entity<InputState>,
        show_toolbar_focus: bool,
        toolbar_focus: ToolbarFocus,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
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
                            .child(table_name.to_string()),
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
                                show_toolbar_focus && toolbar_focus == ToolbarFocus::Filter,
                                |d| d.border_1().border_color(theme.ring),
                            )
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.switching_input = true;
                                    this.focus_mode = GridFocusMode::Toolbar;
                                    this.toolbar_focus = ToolbarFocus::Filter;
                                    this.edit_state = EditState::Editing;
                                    cx.notify();
                                }),
                            )
                            .child(div().flex_1().child(Input::new(filter_input).small()))
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
                                            d.bg(theme.secondary).text_color(theme.foreground)
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
                                show_toolbar_focus && toolbar_focus == ToolbarFocus::Limit,
                                |d| d.border_1().border_color(theme.ring),
                            )
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.switching_input = true;
                                    this.focus_mode = GridFocusMode::Toolbar;
                                    this.toolbar_focus = ToolbarFocus::Limit;
                                    this.edit_state = EditState::Editing;
                                    cx.notify();
                                }),
                            )
                            .child(Input::new(limit_input).small()),
                    ),
            )
            .child(
                div()
                    .id("refresh-btn")
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
                        this.refresh(window, cx);
                        this.focus_table(window, cx);
                    }))
                    .child(
                        svg()
                            .path(AppIcon::RefreshCcw.path())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    ),
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_status_bar(
        &self,
        row_count: usize,
        exec_time: &str,
        is_table_view: bool,
        pagination_info: Option<Pagination>,
        total_pages: Option<u64>,
        can_prev: bool,
        can_next: bool,
        sort_info: Option<(String, SortDirection, bool)>,
        has_data: bool,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .justify_between()
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            // Left: row count and sort info
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(
                                svg()
                                    .path(AppIcon::Rows3.path())
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(format!("{} rows", row_count)),
                    )
                    .when_some(sort_info, |d, (col_name, direction, is_server)| {
                        let arrow_icon = match direction {
                            SortDirection::Ascending => AppIcon::ArrowUp,
                            SortDirection::Descending => AppIcon::ArrowDown,
                        };
                        let mode = if is_server { "db" } else { "local" };
                        d.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(
                                    svg()
                                        .path(arrow_icon.path())
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                )
                                .child(format!("{} ({})", col_name, mode)),
                        )
                    }),
            )
            // Center: pagination (only for Table source)
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
                            .flex()
                            .items_center()
                            .gap_1()
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
                            .child(svg().path(AppIcon::ChevronLeft.path()).size_3().text_color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child("Prev"),
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
                            .flex()
                            .items_center()
                            .gap_1()
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
                            .child("Next")
                            .child(
                                svg()
                                    .path(AppIcon::ChevronRight.path())
                                    .size_3()
                                    .text_color(if can_next {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            ),
                    )
                },
            ))
            // Right: export and execution time
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .when(has_data, |d| {
                        d.child(
                            div()
                                .id("export-csv")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .text_size(FontSizes::XS)
                                .cursor_pointer()
                                .text_color(theme.muted_foreground)
                                .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.export_results(window, cx);
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::FileSpreadsheet.path())
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                )
                                .child("Export CSV"),
                        )
                    })
                    .child({
                        let mut muted = theme.muted_foreground;
                        muted.a = 0.5;
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(muted)
                            .child(exec_time.to_string())
                    }),
            )
    }
}
