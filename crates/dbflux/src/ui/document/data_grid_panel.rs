use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::cell_editor_modal::{CellEditorModal, CellEditorSaveEvent};
use crate::ui::components::data_table::{
    ContextMenuAction, DataTable, DataTableEvent, DataTableState, Direction, Edge, HEADER_HEIGHT,
    ROW_HEIGHT, SortState as TableSortState, TableModel,
};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    CancelToken, DbKind, OrderByColumn, Pagination, QueryRequest, QueryResult, RowDelete,
    RowIdentity, RowInsert, RowPatch, RowState, SortDirection, TableBrowseRequest, TableRef,
    TaskId, TaskKind, Value,
};
use dbflux_export::{CsvExporter, Exporter};
use gpui::prelude::FluentBuilder;
use gpui::{Subscription, deferred, *};
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
    /// The data grid received focus (user clicked on it).
    Focused,
    /// Request to show SQL preview modal.
    RequestSqlPreview {
        profile_id: Uuid,
        schema_name: Option<String>,
        table_name: String,
        column_names: Vec<String>,
        row_values: Vec<String>,
        pk_indices: Vec<usize>,
        generation_type: crate::ui::sql_preview_modal::SqlGenerationType,
    },
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

struct PendingModalOpen {
    row: usize,
    col: usize,
    value: String,
    is_json: bool,
}

/// Context menu state for right-click operations.
struct TableContextMenu {
    /// Row index of the clicked cell.
    row: usize,
    /// Column index of the clicked cell.
    col: usize,
    /// Screen position where the menu should appear.
    position: Point<Pixels>,
    /// Whether the SQL generation submenu is open.
    sql_submenu_open: bool,
    /// Currently selected menu item index (for keyboard navigation).
    selected_index: usize,
    /// Selected index within the SQL submenu (0-3).
    submenu_selected_index: usize,
}

/// A single item in the context menu.
struct ContextMenuItem {
    label: &'static str,
    action: Option<ContextMenuAction>,
    icon: Option<AppIcon>,
    is_separator: bool,
    is_danger: bool,
}

/// Kind of SQL statement to generate from row data.
#[derive(Debug, Clone, Copy)]
enum SqlGenerateKind {
    SelectWhere,
    Insert,
    Update,
    Delete,
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

    // Primary key columns for row editing
    pk_columns: Vec<String>,

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

    // Context menu
    context_menu: Option<TableContextMenu>,
    context_menu_focus: FocusHandle,

    // Modal editor for JSON/long text
    cell_editor: Entity<CellEditorModal>,
    pending_modal_open: Option<PendingModalOpen>,

    // Panel origin in window coordinates (for context menu positioning)
    panel_origin: Point<Pixels>,
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
        let pk_columns: Vec<String> = order_by.iter().map(|c| c.name.clone()).collect();
        let pagination = Pagination::default();

        let source = DataSource::Table {
            profile_id,
            table: table.clone(),
            pagination,
            order_by,
            total_rows: None,
        };

        let mut panel =
            Self::new_internal(source, app_state.clone(), pk_columns.clone(), window, cx);
        panel.refresh(window, cx);

        // If pk_columns is empty, fetch table details to get PK info
        if pk_columns.is_empty() {
            panel.fetch_table_details_for_pk(profile_id, &table, cx);
        }

        panel
    }

    /// Fetch table details to get PK columns if not already cached.
    fn fetch_table_details_for_pk(
        &mut self,
        profile_id: Uuid,
        table: &TableRef,
        cx: &mut Context<Self>,
    ) {
        let database = {
            let state = self.app_state.read(cx);
            state
                .connections
                .get(&profile_id)
                .and_then(|c| c.active_database.clone())
                .unwrap_or_else(|| "default".to_string())
        };

        let params = match self.app_state.read(cx).prepare_fetch_table_details(
            profile_id,
            &database,
            &table.name,
        ) {
            Ok(p) => p,
            Err(_) => return,
        };

        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { params.execute() })
                .await;

            cx.update(|cx| {
                let Ok(fetch_result) = result else { return };

                // Store in cache
                app_state.update(cx, |state, _| {
                    state.set_table_details(
                        fetch_result.profile_id,
                        fetch_result.database,
                        fetch_result.table,
                        fetch_result.details.clone(),
                    );
                });

                // Extract PK columns and update panel
                let pk_names: Vec<String> = fetch_result
                    .details
                    .columns
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| c.name.clone())
                    .collect();

                if !pk_names.is_empty() {
                    entity.update(cx, |panel, cx| {
                        panel.pk_columns = pk_names;
                        panel.pending_rebuild = true;
                        cx.notify();
                    });
                }
            })
            .ok();
        })
        .detach();
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

        // Query results are not editable (no PK info)
        let mut panel = Self::new_internal(source, app_state, Vec::new(), window, cx);
        panel.set_result((*result).clone(), cx);
        panel
    }

    fn new_internal(
        source: DataSource,
        app_state: Entity<AppState>,
        pk_columns: Vec<String>,
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
        let context_menu_focus = cx.focus_handle();

        let cell_editor = cx.new(|cx| CellEditorModal::new(window, cx));

        cx.subscribe_in(
            &cell_editor,
            window,
            |this, _, event: &CellEditorSaveEvent, window, cx| {
                this.handle_cell_editor_save(event.row, event.col, &event.value, window, cx);
            },
        )
        .detach();

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
            pk_columns,
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
            context_menu: None,
            context_menu_focus,
            cell_editor,
            pending_modal_open: None,
            panel_origin: Point::default(),
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
        // Find PK column indices in result columns
        let pk_indices: Vec<usize> = self
            .pk_columns
            .iter()
            .filter_map(|pk_name| self.result.columns.iter().position(|c| c.name == *pk_name))
            .collect();

        log::debug!(
            "[EDIT] rebuild_table: pk_columns={:?}, pk_indices={:?}, result_columns={:?}",
            self.pk_columns,
            pk_indices,
            self.result
                .columns
                .iter()
                .map(|c| &c.name)
                .collect::<Vec<_>>()
        );

        let table_model = Arc::new(TableModel::from(&self.result));
        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            if let Some(sort) = initial_sort {
                state.set_sort_without_emit(sort);
            }
            state.set_pk_columns(pk_indices.clone());
            state
        });
        let data_table = cx.new(|cx| DataTable::new("data-grid-table", table_state.clone(), cx));

        let subscription =
            cx.subscribe(&table_state, |this, _state, event: &DataTableEvent, cx| {
                match event {
                    DataTableEvent::SortChanged(sort) => match sort {
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
                    },
                    DataTableEvent::Focused => {
                        cx.emit(DataGridEvent::Focused);
                    }
                    DataTableEvent::SelectionChanged(_) => {}
                    DataTableEvent::SaveRowRequested(row_idx) => {
                        this.handle_save_row(*row_idx, cx);
                    }
                    DataTableEvent::ContextMenuRequested { row, col, position } => {
                        this.context_menu = Some(TableContextMenu {
                            row: *row,
                            col: *col,
                            position: *position,
                            sql_submenu_open: false,
                            selected_index: 0,
                            submenu_selected_index: 0,
                        });
                        cx.notify();
                    }
                    // Keyboard-triggered row operations
                    DataTableEvent::DeleteRowRequested(row) => {
                        this.handle_delete_row(*row, cx);
                    }
                    DataTableEvent::AddRowRequested(row) => {
                        this.handle_add_row(*row, cx);
                    }
                    DataTableEvent::DuplicateRowRequested(row) => {
                        this.handle_duplicate_row(*row, cx);
                    }
                    DataTableEvent::SetNullRequested { row, col } => {
                        this.handle_set_null(*row, *col, cx);
                    }
                    DataTableEvent::CopyRowRequested(row) => {
                        this.handle_copy_row(*row, cx);
                    }
                    DataTableEvent::ModalEditRequested {
                        row,
                        col,
                        value,
                        is_json,
                    } => {
                        this.pending_modal_open = Some(PendingModalOpen {
                            row: *row,
                            col: *col,
                            value: value.clone(),
                            is_json: *is_json,
                        });
                        cx.notify();
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

    #[allow(clippy::too_many_arguments)]
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

    #[allow(clippy::too_many_arguments)]
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
            && table.qualified_name() == table_qualified
        {
            *total_rows = Some(total);
            cx.notify();
        }
    }

    // === Row Editing ===

    fn handle_save_row(&mut self, row_idx: usize, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);

        if !state.is_editable() {
            return;
        }

        let changes = state.edit_buffer().row_changes(row_idx);
        if changes.is_empty() {
            return;
        }

        let (profile_id, table_ref) = match &self.source {
            DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table.clone()),
            DataSource::QueryResult { .. } => return,
        };

        // Build PK identity from original row values
        let pk_indices = state.pk_columns();
        let model = state.model();

        let mut pk_columns = Vec::with_capacity(pk_indices.len());
        let mut pk_values = Vec::with_capacity(pk_indices.len());

        for &col_idx in pk_indices {
            if let Some(col_spec) = model.columns.get(col_idx) {
                pk_columns.push(col_spec.title.to_string());
            }
            if let Some(cell) = model.cell(row_idx, col_idx) {
                pk_values.push(cell.to_value());
            }
        }

        if pk_columns.len() != pk_indices.len() || pk_values.len() != pk_indices.len() {
            log::error!("[SAVE] Failed to build row identity");
            return;
        }

        let identity = RowIdentity::new(pk_columns, pk_values);

        // Build column changes
        let change_values: Vec<(String, Value)> = changes
            .iter()
            .filter_map(|&(col_idx, cell_value)| {
                model
                    .columns
                    .get(col_idx)
                    .map(|col| (col.title.to_string(), cell_value.to_value()))
            })
            .collect();

        let patch = RowPatch::new(
            identity,
            table_ref.name.clone(),
            table_ref.schema.clone(),
            change_values,
        );

        // Set row state to Saving
        let table_state_for_update = table_state.clone();
        table_state_for_update.update(cx, |state, cx| {
            state
                .edit_buffer_mut()
                .set_row_state(row_idx, RowState::Saving);
            cx.notify();
        });

        // Execute update asynchronously
        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            // Get connection
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        if let Some(table_state) = &panel.table_state {
                            table_state.update(cx, |state, cx| {
                                state.edit_buffer_mut().set_row_state(
                                    row_idx,
                                    RowState::Error("No connection".to_string()),
                                );
                                cx.notify();
                            });
                        }
                    });
                })
                .ok();
                return;
            };

            // Execute on background
            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_row(&patch) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    let Some(table_state) = &panel.table_state else {
                        return;
                    };

                    match result {
                        Ok(crud_result) => {
                            table_state.update(cx, |state, cx| {
                                // Apply returning row if available (server-computed values)
                                if let Some(returning_row) = crud_result.returning_row {
                                    state.apply_returning_row(row_idx, &returning_row);
                                }
                                state.edit_buffer_mut().clear_row(row_idx);
                                cx.notify();
                            });
                            panel.pending_toast = Some(PendingToast {
                                message: "Row saved".to_string(),
                                is_error: false,
                            });
                        }
                        Err(e) => {
                            log::error!("[SAVE] Failed to save row {}: {}", row_idx, e);
                            table_state.update(cx, |state, cx| {
                                state
                                    .edit_buffer_mut()
                                    .set_row_state(row_idx, RowState::Error(e.to_string()));
                                cx.notify();
                            });
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Save failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
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
        // Handle context menu commands when menu is open
        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

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
            Command::OpenContextMenu => {
                self.open_context_menu_at_selection(window, cx);
                true
            }
            _ => false,
        }
    }

    /// Opens context menu at the current selection.
    fn open_context_menu_at_selection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let (row, col, cell_x, horizontal_offset) = {
            let ts = table_state.read(cx);
            let Some(active) = ts.selection().active else {
                return;
            };
            let widths = ts.column_widths();

            // Calculate cell x position: sum of column widths up to col
            let cell_x: f32 = widths.iter().take(active.col).sum();

            (active.row, active.col, cell_x, ts.horizontal_offset())
        };

        // Calculate position in window coordinates:
        // x: panel_origin.x + cell_x - horizontal_scroll + some padding
        // y: panel_origin.y + HEADER_HEIGHT + (row * ROW_HEIGHT) + some padding for toolbar
        let toolbar_height = px(36.0); // Approximate toolbar height
        let position = Point {
            x: self.panel_origin.x + px(cell_x) - horizontal_offset + px(20.0),
            y: self.panel_origin.y + toolbar_height + HEADER_HEIGHT + ROW_HEIGHT * row,
        };

        self.context_menu = Some(TableContextMenu {
            row,
            col,
            position,
            sql_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
        });

        // Focus the context menu to receive keyboard events
        self.context_menu_focus.focus(window);
        cx.notify();
    }

    /// Returns true if the data grid is editable (has primary key info).
    fn check_is_editable(&self, cx: &App) -> bool {
        self.table_state
            .as_ref()
            .map(|ts| ts.read(cx).is_editable())
            .unwrap_or(false)
    }

    /// Returns true if the context menu is currently open.
    pub fn is_context_menu_open(&self) -> bool {
        self.context_menu.is_some()
    }

    /// Returns the active context for keyboard handling.
    pub fn active_context(&self) -> ContextId {
        if self.context_menu.is_some() {
            ContextId::ContextMenu
        } else {
            ContextId::Results
        }
    }

    /// Handles commands when the context menu is open.
    fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let is_editable = self.check_is_editable(cx);

        // Build the menu items list to know the count
        // Items: Copy, Paste*, Edit*, EditModal*, sep, SetDefault*, SetNull*, sep, AddRow*, DupRow*, DelRow*, sep, GenSQL
        // * = requires editable
        let menu_items: Vec<Option<ContextMenuAction>> = if is_editable {
            vec![
                Some(ContextMenuAction::Copy),
                Some(ContextMenuAction::Paste),
                Some(ContextMenuAction::Edit),
                Some(ContextMenuAction::EditInModal),
                None, // separator
                Some(ContextMenuAction::SetDefault),
                Some(ContextMenuAction::SetNull),
                None, // separator
                Some(ContextMenuAction::AddRow),
                Some(ContextMenuAction::DuplicateRow),
                Some(ContextMenuAction::DeleteRow),
                None, // separator (before Generate SQL)
                None, // Generate SQL trigger (special handling)
            ]
        } else {
            vec![
                Some(ContextMenuAction::Copy),
                None, // separator (before Generate SQL)
                None, // Generate SQL trigger
            ]
        };

        let item_count = menu_items.len();
        let submenu_count = 4; // SELECT WHERE, INSERT, UPDATE, DELETE

        match cmd {
            Command::MenuDown => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open {
                        menu.submenu_selected_index =
                            (menu.submenu_selected_index + 1) % submenu_count;
                    } else {
                        menu.selected_index = (menu.selected_index + 1) % item_count;
                        // Skip separators
                        while menu.selected_index < item_count
                            && menu_items[menu.selected_index].is_none()
                            && menu.selected_index != item_count - 1
                        {
                            menu.selected_index = (menu.selected_index + 1) % item_count;
                        }
                    }
                    cx.notify();
                }
                true
            }
            Command::MenuUp => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open {
                        menu.submenu_selected_index = if menu.submenu_selected_index == 0 {
                            submenu_count - 1
                        } else {
                            menu.submenu_selected_index - 1
                        };
                    } else {
                        menu.selected_index = if menu.selected_index == 0 {
                            item_count - 1
                        } else {
                            menu.selected_index - 1
                        };
                        // Skip separators (going backwards)
                        while menu.selected_index > 0
                            && menu_items[menu.selected_index].is_none()
                            && menu.selected_index != item_count - 1
                        {
                            menu.selected_index = if menu.selected_index == 0 {
                                item_count - 1
                            } else {
                                menu.selected_index - 1
                            };
                        }
                    }
                    cx.notify();
                }
                true
            }
            Command::MenuSelect => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open {
                        // Execute submenu action
                        let action = match menu.submenu_selected_index {
                            0 => ContextMenuAction::GenerateSelectWhere,
                            1 => ContextMenuAction::GenerateInsert,
                            2 => ContextMenuAction::GenerateUpdate,
                            _ => ContextMenuAction::GenerateDelete,
                        };
                        self.handle_context_menu_action(action, window, cx);
                    } else if menu.selected_index == item_count - 1 {
                        // Last item is Generate SQL - open submenu
                        menu.sql_submenu_open = true;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    } else if let Some(action) =
                        menu_items.get(menu.selected_index).and_then(|a| *a)
                    {
                        self.handle_context_menu_action(action, window, cx);
                    }
                }
                true
            }
            Command::MenuBack | Command::Cancel => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open {
                        // Close submenu
                        menu.sql_submenu_open = false;
                        cx.notify();
                    } else {
                        // Close menu and restore focus to table
                        self.context_menu = None;
                        self.focus_handle.focus(window);
                        cx.notify();
                    }
                }
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

        let database = connected.active_database.as_deref().unwrap_or("default");

        // Check table_details cache first (populated when table is expanded)
        let cache_key = (database.to_string(), table.name.clone());
        if let Some(table_info) = connected.table_details.get(&cache_key) {
            let columns = table_info.columns.as_deref().unwrap_or(&[]);
            return columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| OrderByColumn::asc(&c.name))
                .collect();
        }

        // Check database_schemas (MySQL/MariaDB lazy loading)
        if let Some(schema_name) = &table.schema
            && let Some(db_schema) = connected.database_schemas.get(schema_name)
        {
            for t in &db_schema.tables {
                if t.name == table.name {
                    let columns = t.columns.as_deref().unwrap_or(&[]);
                    return columns
                        .iter()
                        .filter(|c| c.is_primary_key)
                        .map(|c| OrderByColumn::asc(&c.name))
                        .collect();
                }
            }
        }

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let Some(schema) = &connected.schema else {
            return Vec::new();
        };

        for db_schema in &schema.schemas {
            if table.schema.as_deref() == Some(&db_schema.name) || table.schema.is_none() {
                for t in &db_schema.tables {
                    if t.name == table.name {
                        let columns = t.columns.as_deref().unwrap_or(&[]);
                        return columns
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

        if let Some(modal) = self.pending_modal_open.take() {
            self.cell_editor.update(cx, |editor, cx| {
                editor.open(modal.row, modal.col, modal.value, modal.is_json, window, cx);
            });
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

        // Get edit state from table
        let (is_editable, has_pending_changes, dirty_count, can_undo, can_redo) = self
            .table_state
            .as_ref()
            .map(|ts| {
                let state = ts.read(cx);
                let buffer = state.edit_buffer();

                // Count all pending operations: edits, inserts, deletes
                let edit_count = buffer.dirty_row_count();
                let insert_count = buffer.pending_insert_rows().len();
                let delete_count = buffer.pending_delete_rows().len();
                let total_count = edit_count + insert_count + delete_count;

                (
                    state.is_editable(),
                    total_count > 0,
                    total_count,
                    buffer.can_undo(),
                    buffer.can_redo(),
                )
            })
            .unwrap_or((false, false, 0, false, false));

        // Show PK warning only for table views without PK
        let show_pk_warning = is_table_view && has_data && !is_editable;

        // Always show edit toolbar for editable tables
        let show_edit_toolbar = is_table_view && has_data && is_editable;

        div()
            .track_focus(&focus_handle)
            .flex()
            .flex_col()
            .size_full()
            // Track panel origin for context menu positioning
            .child({
                let this_entity = cx.entity().clone();
                canvas(
                    move |bounds, _, cx| {
                        this_entity.update(cx, |this, _cx| {
                            this.panel_origin = bounds.origin;
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full()
            })
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
            // PK warning banner (when table has no PK)
            .when(show_pk_warning, |d| {
                d.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .h(Heights::ROW_COMPACT)
                        .px(Spacing::SM)
                        .bg(theme.warning.opacity(0.15))
                        .border_b_1()
                        .border_color(theme.warning.opacity(0.3))
                        .child(
                            svg()
                                .path(AppIcon::TriangleAlert.path())
                                .size_4()
                                .text_color(theme.warning),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.warning)
                                .child("This table has no primary key - editing is disabled"),
                        ),
                )
            })
            // Edit toolbar (always visible for editable tables)
            .when(show_edit_toolbar, |d| {
                d.child(self.render_edit_toolbar(
                    dirty_count,
                    has_pending_changes,
                    can_undo,
                    can_redo,
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
                    .when(self.data_table.is_none(), |d| {
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
            // Context menu overlay
            .when_some(self.context_menu.as_ref(), |d, menu| {
                d.child(self.render_context_menu(menu, is_editable, &theme, cx))
            })
            // Cell editor modal overlay
            .when(self.cell_editor.read(cx).is_visible(), |d| {
                d.child(self.cell_editor.clone())
            })
    }
}

impl DataGridPanel {
    #[allow(clippy::too_many_arguments)]
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

    fn render_edit_toolbar(
        &self,
        dirty_count: usize,
        has_changes: bool,
        can_undo: bool,
        can_redo: bool,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .justify_between()
            .h(px(44.0))
            .px(Spacing::MD)
            .border_b_1()
            .border_color(theme.border)
            // Left: status text
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(if has_changes {
                        theme.warning
                    } else {
                        theme.muted_foreground
                    })
                    .child(if has_changes {
                        format!(
                            "{} unsaved change{}",
                            dirty_count,
                            if dirty_count == 1 { "" } else { "s" }
                        )
                    } else {
                        "No unsaved changes".to_string()
                    }),
            )
            // Right: buttons
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    // Undo button
                    .child(
                        div()
                            .id("undo-btn")
                            .flex()
                            .items_center()
                            .justify_center()
                            .size(px(28.0))
                            .rounded(Radii::MD)
                            .border_1()
                            .when(can_undo, |d| {
                                d.border_color(theme.border)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                if state.is_editing() {
                                                    state.stop_editing(false, cx);
                                                }
                                                if state.edit_buffer_mut().undo() {
                                                    let visual_count = state
                                                        .edit_buffer()
                                                        .compute_visual_order()
                                                        .len();
                                                    if let Some(active) = state.selection().active
                                                        && active.row >= visual_count
                                                    {
                                                        state.clear_selection(cx);
                                                    }
                                                    cx.notify();
                                                }
                                            });
                                        }
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!can_undo, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.muted_foreground)
                            })
                            .child(svg().path(AppIcon::Undo.path()).size_4().text_color(
                                if can_undo {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
                    )
                    // Redo button
                    .child(
                        div()
                            .id("redo-btn")
                            .flex()
                            .items_center()
                            .justify_center()
                            .size(px(28.0))
                            .rounded(Radii::MD)
                            .border_1()
                            .when(can_redo, |d| {
                                d.border_color(theme.border)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                if state.is_editing() {
                                                    state.stop_editing(false, cx);
                                                }
                                                if state.edit_buffer_mut().redo() {
                                                    let visual_count = state
                                                        .edit_buffer()
                                                        .compute_visual_order()
                                                        .len();
                                                    if let Some(active) = state.selection().active
                                                        && active.row >= visual_count
                                                    {
                                                        state.clear_selection(cx);
                                                    }
                                                    cx.notify();
                                                }
                                            });
                                        }
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!can_redo, |d| d.border_color(theme.border))
                            .child(svg().path(AppIcon::Redo.path()).size_4().text_color(
                                if can_redo {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
                    )
                    // Save button
                    .child(
                        div()
                            .id("save-btn")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::MD)
                            .h(px(28.0))
                            .rounded(Radii::MD)
                            .text_size(FontSizes::SM)
                            .border_1()
                            .when(has_changes, |d| {
                                d.border_color(theme.primary)
                                    .bg(theme.primary)
                                    .text_color(theme.primary_foreground)
                                    .cursor_pointer()
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                state.request_save_row(cx);
                                            });
                                        }
                                        // Refocus table after button click
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!has_changes, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.muted_foreground)
                            })
                            .child("Save")
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(if has_changes {
                                        theme.primary_foreground.opacity(0.7)
                                    } else {
                                        theme.muted_foreground.opacity(0.5)
                                    })
                                    .child("Ctrl+↵"),
                            ),
                    )
                    // Revert button
                    .child(
                        div()
                            .id("revert-btn")
                            .flex()
                            .items_center()
                            .px(Spacing::MD)
                            .h(px(28.0))
                            .rounded(Radii::MD)
                            .text_size(FontSizes::SM)
                            .border_1()
                            .when(has_changes, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.foreground)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                state.revert_all(cx);
                                            });
                                        }
                                        // Refocus table after button click
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!has_changes, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.muted_foreground)
                            })
                            .child("Revert"),
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

    /// Builds the list of visible context menu items based on editability.
    fn build_context_menu_items(is_editable: bool) -> Vec<ContextMenuItem> {
        let mut items = vec![ContextMenuItem {
            label: "Copy",
            action: Some(ContextMenuAction::Copy),
            icon: Some(AppIcon::Layers),
            is_separator: false,
            is_danger: false,
        }];

        if is_editable {
            items.extend([
                ContextMenuItem {
                    label: "Paste",
                    action: Some(ContextMenuAction::Paste),
                    icon: Some(AppIcon::Download),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Edit",
                    action: Some(ContextMenuAction::Edit),
                    icon: Some(AppIcon::Pencil),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Edit in Modal",
                    action: Some(ContextMenuAction::EditInModal),
                    icon: Some(AppIcon::Maximize2),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Set to Default",
                    action: Some(ContextMenuAction::SetDefault),
                    icon: Some(AppIcon::RotateCcw),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Set to NULL",
                    action: Some(ContextMenuAction::SetNull),
                    icon: Some(AppIcon::X),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Add Row",
                    action: Some(ContextMenuAction::AddRow),
                    icon: Some(AppIcon::Plus),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Duplicate Row",
                    action: Some(ContextMenuAction::DuplicateRow),
                    icon: Some(AppIcon::Layers),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Delete Row",
                    action: Some(ContextMenuAction::DeleteRow),
                    icon: Some(AppIcon::Delete),
                    is_separator: false,
                    is_danger: true,
                },
            ]);
        }

        items
    }

    /// Returns the total number of navigable items in the context menu.
    /// This includes all visible items plus the Generate SQL trigger.
    #[allow(dead_code)]
    fn context_menu_item_count(is_editable: bool) -> usize {
        let base_items = Self::build_context_menu_items(is_editable);
        // Count non-separator items + 1 for Generate SQL
        base_items.iter().filter(|i| !i.is_separator).count() + 1
    }

    fn render_context_menu(
        &self,
        menu: &TableContextMenu,
        is_editable: bool,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let menu_width = px(180.0);

        // Convert window coordinates to panel-relative coordinates
        let menu_x = menu.position.x - self.panel_origin.x;
        let menu_y = menu.position.y - self.panel_origin.y;

        // Build visible menu items list for keyboard navigation
        let visible_items = Self::build_context_menu_items(is_editable);
        let selected_index = menu.selected_index;

        // Build menu items with selection highlighting
        let mut menu_items: Vec<AnyElement> = Vec::new();
        let mut visual_index = 0usize;

        for item in &visible_items {
            if item.is_separator {
                menu_items.push(
                    div()
                        .h(px(1.0))
                        .mx(Spacing::SM)
                        .my(Spacing::XS)
                        .bg(theme.border)
                        .into_any_element(),
                );
                visual_index += 1;
                continue;
            }

            let Some(action) = item.action else {
                visual_index += 1;
                continue;
            };

            let is_selected = visual_index == selected_index;
            let is_danger = item.is_danger;
            let label = item.label;
            let icon = item.icon;
            let current_index = visual_index;

            menu_items.push(
                div()
                    .id(SharedString::from(label))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(if is_danger {
                        theme.danger
                    } else {
                        theme.foreground
                    })
                    .when(is_selected, |d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.accent
                        })
                        .text_color(if is_danger {
                            theme.danger
                        } else {
                            theme.accent_foreground
                        })
                    })
                    .when(!is_selected, |d| {
                        d.hover(|d| {
                            d.bg(if is_danger {
                                theme.danger.opacity(0.1)
                            } else {
                                theme.secondary
                            })
                        })
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != current_index
                        {
                            menu.selected_index = current_index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.handle_context_menu_action(action, window, cx);
                    }))
                    .when_some(icon, |d, icon| {
                        d.child(svg().path(icon.path()).size_4().text_color(if is_danger {
                            theme.danger
                        } else if is_selected {
                            theme.accent_foreground
                        } else {
                            theme.muted_foreground
                        }))
                    })
                    .when(icon.is_none(), |d| d.pl(px(20.0)))
                    .child(label)
                    .into_any_element(),
            );

            visual_index += 1;
        }

        // Add separator before "Generate SQL"
        menu_items.push(
            div()
                .h(px(1.0))
                .mx(Spacing::SM)
                .my(Spacing::XS)
                .bg(theme.border)
                .into_any_element(),
        );
        visual_index += 1; // Separator takes an index slot

        // "Generate SQL" submenu trigger
        let sql_submenu_open = menu.sql_submenu_open;
        let submenu_bg = theme.popover;
        let submenu_border = theme.border;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;
        let gen_sql_index = visual_index; // Index for Generate SQL item
        let gen_sql_selected = selected_index == gen_sql_index;
        let submenu_selected_index = menu.submenu_selected_index;

        menu_items.push(
            div()
                .id("generate-sql-trigger")
                .relative()
                .flex()
                .items_center()
                .justify_between()
                .h(Heights::ROW_COMPACT)
                .px(Spacing::SM)
                .mx(Spacing::XS)
                .rounded(Radii::SM)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(if gen_sql_selected && !sql_submenu_open {
                    theme.accent_foreground
                } else {
                    submenu_fg
                })
                .when(sql_submenu_open, |d| d.bg(submenu_hover))
                .when(gen_sql_selected && !sql_submenu_open, |d| {
                    d.bg(theme.accent)
                })
                .when(!gen_sql_selected && !sql_submenu_open, |d| {
                    d.hover(|d| d.bg(submenu_hover))
                })
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if let Some(ref mut menu) = this.context_menu
                        && menu.selected_index != gen_sql_index
                        && !menu.sql_submenu_open
                    {
                        menu.selected_index = gen_sql_index;
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(|this, _, _, cx| {
                    if let Some(ref mut menu) = this.context_menu {
                        menu.sql_submenu_open = !menu.sql_submenu_open;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    }
                }))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(svg().path(AppIcon::Code.path()).size_4().text_color(
                            if gen_sql_selected && !sql_submenu_open {
                                theme.accent_foreground
                            } else {
                                submenu_fg
                            },
                        ))
                        .child("Generate SQL"),
                )
                .child(
                    svg()
                        .path(AppIcon::ChevronRight.path())
                        .size_4()
                        .text_color(if gen_sql_selected && !sql_submenu_open {
                            theme.accent_foreground
                        } else {
                            theme.muted_foreground
                        }),
                )
                // Submenu appears to the right
                .when(sql_submenu_open, |d: Stateful<Div>| {
                    d.child(
                        div()
                            .absolute()
                            .left(px(172.0)) // menu_width - some padding
                            .top(px(-4.0))
                            .w(px(160.0))
                            .bg(submenu_bg)
                            .border_1()
                            .border_color(submenu_border)
                            .rounded(Radii::MD)
                            .shadow_lg()
                            .py(Spacing::XS)
                            // Capture clicks within submenu bounds (prevents overlay from closing menu)
                            .occlude()
                            // Stop click from bubbling to parent "Generate SQL" trigger
                            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                cx.stop_propagation();
                            })
                            .children(
                                [
                                    ("SELECT WHERE", ContextMenuAction::GenerateSelectWhere),
                                    ("INSERT", ContextMenuAction::GenerateInsert),
                                    ("UPDATE", ContextMenuAction::GenerateUpdate),
                                    ("DELETE", ContextMenuAction::GenerateDelete),
                                ]
                                .into_iter()
                                .enumerate()
                                .map(|(idx, (label, action))| {
                                    let is_submenu_selected = idx == submenu_selected_index;
                                    div()
                                        .id(SharedString::from(label))
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::SM)
                                        .h(Heights::ROW_COMPACT)
                                        .px(Spacing::SM)
                                        .mx(Spacing::XS)
                                        .rounded(Radii::SM)
                                        .cursor_pointer()
                                        .text_size(FontSizes::SM)
                                        .text_color(if is_submenu_selected {
                                            theme.accent_foreground
                                        } else {
                                            submenu_fg
                                        })
                                        .when(is_submenu_selected, |d| d.bg(theme.accent))
                                        .when(!is_submenu_selected, |d| {
                                            d.hover(|d| d.bg(submenu_hover))
                                        })
                                        .on_mouse_move(cx.listener(move |this, _, _, cx| {
                                            if let Some(ref mut menu) = this.context_menu
                                                && menu.submenu_selected_index != idx
                                            {
                                                menu.submenu_selected_index = idx;
                                                cx.notify();
                                            }
                                        }))
                                        .on_click(cx.listener(move |this, _, window, cx| {
                                            this.handle_context_menu_action(action, window, cx);
                                        }))
                                        .child(
                                            svg().path(AppIcon::Code.path()).size_4().text_color(
                                                if is_submenu_selected {
                                                    theme.accent_foreground
                                                } else {
                                                    theme.muted_foreground
                                                },
                                            ),
                                        )
                                        .child(label)
                                })
                                .collect::<Vec<_>>(),
                            ),
                    )
                })
                .into_any_element(),
        );

        // Use deferred() to render at window level for correct positioning
        deferred(
            div()
                .id("context-menu-overlay")
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .track_focus(&self.context_menu_focus)
                .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                    use crate::keymap::{KeyChord, default_keymap};

                    let chord = KeyChord::from_gpui(&event.keystroke);
                    let keymap = default_keymap();

                    if let Some(cmd) = keymap.resolve(ContextId::ContextMenu, &chord)
                        && this.dispatch_menu_command(cmd, window, cx)
                    {
                        cx.stop_propagation();
                    }
                }))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| {
                        this.context_menu = None;
                        this.focus_handle.focus(window);
                        cx.notify();
                    }),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(|this, _, window, cx| {
                        this.context_menu = None;
                        this.focus_handle.focus(window);
                        cx.notify();
                    }),
                )
                .child(
                    div()
                        .id("context-menu")
                        .absolute()
                        .left(menu_x)
                        .top(menu_y)
                        .w(menu_width)
                        .bg(theme.popover)
                        .border_1()
                        .border_color(theme.border)
                        .rounded(Radii::MD)
                        .shadow_lg()
                        .py(Spacing::XS)
                        .occlude()
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .children(menu_items),
                ),
        )
        .with_priority(1)
    }

    fn handle_context_menu_action(
        &mut self,
        action: ContextMenuAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let menu = match self.context_menu.take() {
            Some(m) => m,
            None => return,
        };

        match action {
            ContextMenuAction::Copy => self.handle_copy(window, cx),
            ContextMenuAction::Paste => self.handle_paste(window, cx),
            ContextMenuAction::Edit => self.handle_edit(menu.row, menu.col, window, cx),
            ContextMenuAction::EditInModal => {
                self.handle_edit_in_modal(menu.row, menu.col, cx);
            }
            ContextMenuAction::SetDefault => self.handle_set_default(menu.row, menu.col, cx),
            ContextMenuAction::SetNull => self.handle_set_null(menu.row, menu.col, cx),
            ContextMenuAction::AddRow => self.handle_add_row(menu.row, cx),
            ContextMenuAction::DuplicateRow => self.handle_duplicate_row(menu.row, cx),
            ContextMenuAction::DeleteRow => self.handle_delete_row(menu.row, cx),
            ContextMenuAction::GenerateSelectWhere => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::SelectWhere, cx)
            }
            ContextMenuAction::GenerateInsert => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::Insert, cx)
            }
            ContextMenuAction::GenerateUpdate => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::Update, cx)
            }
            ContextMenuAction::GenerateDelete => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::Delete, cx)
            }
        }

        // Restore focus to table after action
        self.focus_handle.focus(window);
        cx.notify();
    }

    fn handle_copy(&self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            let text = table_state.read(cx).copy_selection();
            if let Some(text) = text {
                cx.write_to_clipboard(ClipboardItem::new_string(text));
            }
        }
    }

    /// Copy entire row as TSV (tab-separated values).
    fn handle_copy_row(&self, row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        // Get row data based on visual row source
        let row_values: Vec<String> = match visual_order.get(row).copied() {
            Some(VisualRowSource::Base(base_idx)) => self
                .result
                .rows
                .get(base_idx)
                .map(|r| {
                    r.iter()
                        .map(|val| {
                            crate::ui::components::data_table::clipboard::format_cell(
                                &crate::ui::components::data_table::model::CellValue::from(val),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| {
                    cells
                        .iter()
                        .map(crate::ui::components::data_table::clipboard::format_cell)
                        .collect()
                })
                .unwrap_or_default(),
            None => return,
        };

        if !row_values.is_empty() {
            let text = row_values.join("\t");
            cx.write_to_clipboard(ClipboardItem::new_string(text));
        }
    }

    fn handle_paste(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let clipboard_text = cx
            .read_from_clipboard()
            .and_then(|item| item.text().map(|s| s.to_string()));

        let Some(text) = clipboard_text else {
            return;
        };

        table_state.update(cx, |state, cx| {
            if let Some(coord) = state.selection().active {
                let cell_value = crate::ui::components::data_table::model::CellValue::text(&text);
                state
                    .edit_buffer_mut()
                    .set_cell(coord.row, coord.col, cell_value);
                cx.notify();
            }
        });
    }

    fn handle_edit(&mut self, row: usize, col: usize, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                let coord = crate::ui::components::data_table::selection::CellCoord::new(row, col);
                state.start_editing(coord, window, cx);
            });
        }
    }

    fn handle_edit_in_modal(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::{ColumnKind, VisualRowSource};

        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);
        if !state.is_editable() {
            return;
        }

        let is_json = state
            .model()
            .columns
            .get(col)
            .map(|c| c.kind == ColumnKind::Json)
            .unwrap_or(false);

        let visual_order = state.edit_buffer().compute_visual_order();
        let null_cell = crate::ui::components::data_table::model::CellValue::null();

        let value = match visual_order.get(row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                let base_cell = state.model().cell(base_idx, col);
                let base = base_cell.unwrap_or(&null_cell);
                let cell = state.edit_buffer().get_cell(base_idx, col, base);
                cell.edit_text()
            }
            Some(VisualRowSource::Insert(insert_idx)) => {
                if let Some(insert_data) = state.edit_buffer().get_pending_insert_by_idx(insert_idx)
                {
                    if col < insert_data.len() {
                        insert_data[col].edit_text()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            }
            None => return,
        };

        self.pending_modal_open = Some(PendingModalOpen {
            row,
            col,
            value,
            is_json,
        });
        cx.notify();
    }

    fn handle_set_default(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        // Get column default value from table details
        let default_value = self.get_column_default(col, cx);

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            let visual_order = buffer.compute_visual_order();

            let cell_value = if let Some(default) = default_value {
                crate::ui::components::data_table::model::CellValue::text(&default)
            } else {
                crate::ui::components::data_table::model::CellValue::null()
            };

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.set_cell(base_idx, col, cell_value);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.set_insert_cell(insert_idx, col, cell_value);
                }
                None => {}
            }

            cx.notify();
        });
    }

    fn handle_set_null(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            let visual_order = buffer.compute_visual_order();
            let cell_value = crate::ui::components::data_table::model::CellValue::null();

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.set_cell(base_idx, col, cell_value);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.set_insert_cell(insert_idx, col, cell_value);
                }
                None => {}
            }

            cx.notify();
        });
    }

    fn handle_cell_editor_save(
        &mut self,
        row: usize,
        col: usize,
        value: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            let visual_order = buffer.compute_visual_order();
            let cell_value = crate::ui::components::data_table::model::CellValue::text(value);

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.set_cell(base_idx, col, cell_value);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.set_insert_cell(insert_idx, col, cell_value);
                }
                None => {}
            }

            cx.notify();
        });

        self.focus_table(window, cx);
    }

    fn handle_add_row(&mut self, after_visual_row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        if !matches!(self.source, DataSource::Table { .. }) {
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };

        // Determine the base row index to insert after
        let insert_after_base = {
            let state = table_state.read(cx);
            let buffer = state.edit_buffer();
            let visual_order = buffer.compute_visual_order();

            match visual_order.get(after_visual_row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    // Insert after this base row
                    base_idx
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    // Insert after the same base row as this pending insert
                    buffer
                        .pending_inserts()
                        .get(insert_idx)
                        .and_then(|pi| pi.insert_after)
                        .unwrap_or(self.result.rows.len().saturating_sub(1))
                }
                None => self.result.rows.len().saturating_sub(1),
            }
        };

        // Get column defaults from table metadata
        let column_defaults = self.get_all_column_defaults(cx);

        // Create a new row with default values or NULL
        let new_row: Vec<crate::ui::components::data_table::model::CellValue> = self
            .result
            .columns
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                if let Some(default_expr) = column_defaults.get(idx).and_then(|d| d.as_ref()) {
                    // Column has a default expression (e.g., nextval(), now())
                    crate::ui::components::data_table::model::CellValue::auto_generated(
                        default_expr,
                    )
                } else {
                    crate::ui::components::data_table::model::CellValue::null()
                }
            })
            .collect();

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            buffer.set_base_row_count(self.result.rows.len());
            buffer.add_pending_insert_after(insert_after_base, new_row);
            cx.notify();
        });
    }

    fn handle_duplicate_row(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        if !matches!(self.source, DataSource::Table { .. }) {
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };

        // Find PK column indices to use default values
        let pk_indices: std::collections::HashSet<usize> = self
            .pk_columns
            .iter()
            .filter_map(|pk_name| self.result.columns.iter().position(|c| c.name == *pk_name))
            .collect();

        // Get column defaults for PK columns
        let column_defaults = self.get_all_column_defaults(cx);

        // Get source row data and determine insert position
        let base_row_count = self.result.rows.len();
        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let (source_values, insert_after_base): (
            Vec<crate::ui::components::data_table::model::CellValue>,
            usize,
        ) = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                // Copying from base model row - insert after this row
                let values = self
                    .result
                    .rows
                    .get(base_idx)
                    .map(|r| {
                        r.iter()
                            .enumerate()
                            .map(|(idx, val)| {
                                // Use default for PK columns, copy value for others
                                if pk_indices.contains(&idx) {
                                    if let Some(default_expr) =
                                        column_defaults.get(idx).and_then(|d| d.as_ref())
                                    {
                                        crate::ui::components::data_table::model::CellValue::auto_generated(default_expr)
                                    } else {
                                        crate::ui::components::data_table::model::CellValue::null()
                                    }
                                } else {
                                    crate::ui::components::data_table::model::CellValue::from(val)
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (values, base_idx)
            }
            Some(VisualRowSource::Insert(insert_idx)) => {
                // Copying from pending insert - insert after same base row
                let insert_after = buffer
                    .pending_inserts()
                    .get(insert_idx)
                    .and_then(|pi| pi.insert_after)
                    .unwrap_or(base_row_count.saturating_sub(1));

                let values = buffer
                    .get_pending_insert_by_idx(insert_idx)
                    .map(|insert_data| {
                        insert_data
                            .iter()
                            .enumerate()
                            .map(|(idx, val)| {
                                if pk_indices.contains(&idx) {
                                    if let Some(default_expr) =
                                        column_defaults.get(idx).and_then(|d| d.as_ref())
                                    {
                                        crate::ui::components::data_table::model::CellValue::auto_generated(default_expr)
                                    } else {
                                        crate::ui::components::data_table::model::CellValue::null()
                                    }
                                } else {
                                    val.clone()
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (values, insert_after)
            }
            None => return,
        };

        if source_values.is_empty() {
            return;
        }

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            buffer.set_base_row_count(base_row_count);
            buffer.add_pending_insert_after(insert_after_base, source_values);
            cx.notify();
        });
    }

    fn handle_delete_row(&mut self, row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        if !matches!(self.source, DataSource::Table { .. }) {
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };

        let base_row_count = self.result.rows.len();

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            buffer.set_base_row_count(base_row_count);

            // Use visual ordering to determine the actual row type
            let visual_order = buffer.compute_visual_order();

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.mark_for_delete(base_idx);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.remove_pending_insert_by_idx(insert_idx);
                }
                None => {}
            }

            cx.notify();
        });
    }

    fn handle_generate_sql(
        &mut self,
        visual_row: usize,
        kind: SqlGenerateKind,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::components::data_table::model::VisualRowSource;
        use crate::ui::sql_preview_modal::SqlGenerationType;

        let (profile_id, table_ref) = match &self.source {
            DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table.clone()),
            DataSource::QueryResult { .. } => return,
        };

        let Some(table_state) = &self.table_state else {
            return;
        };

        // Get column info including primary keys
        let state = self.app_state.read(cx);
        let connected = match state.connections.get(&profile_id) {
            Some(c) => c,
            None => return,
        };

        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = connected.table_details.get(&cache_key);
        let columns_info = table_info.and_then(|t| t.columns.as_deref());

        // Get column names from result
        let col_names: Vec<String> = self.result.columns.iter().map(|c| c.name.clone()).collect();

        // Get row values
        let ts = table_state.read(cx);
        let buffer = ts.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let row_values: Vec<String> = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => self
                .result
                .rows
                .get(base_idx)
                .map(|r| r.iter().map(|v| self.format_value_for_sql(v)).collect())
                .unwrap_or_default(),
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.iter().map(|c| self.format_cell_for_sql(c)).collect())
                .unwrap_or_default(),
            None => return,
        };

        if row_values.is_empty() || col_names.len() != row_values.len() {
            return;
        }

        // Find primary key columns
        let pk_indices: Vec<usize> = if let Some(cols) = columns_info {
            col_names
                .iter()
                .enumerate()
                .filter_map(|(idx, name)| {
                    cols.iter()
                        .find(|c| c.name == *name && c.is_primary_key)
                        .map(|_| idx)
                })
                .collect()
        } else {
            vec![]
        };

        // Convert SqlGenerateKind to SqlGenerationType
        let generation_type = match kind {
            SqlGenerateKind::SelectWhere => SqlGenerationType::SelectWhere,
            SqlGenerateKind::Insert => SqlGenerationType::Insert,
            SqlGenerateKind::Update => SqlGenerationType::Update,
            SqlGenerateKind::Delete => SqlGenerationType::Delete,
        };

        // Emit event for SQL preview modal
        cx.emit(DataGridEvent::RequestSqlPreview {
            profile_id,
            schema_name: table_ref.schema.clone(),
            table_name: table_ref.name.clone(),
            column_names: col_names,
            row_values,
            pk_indices,
            generation_type,
        });
    }

    fn format_value_for_sql(&self, value: &dbflux_core::Value) -> String {
        use dbflux_core::Value;

        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bytes(_) => "'<binary>'".to_string(),
            Value::Json(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Decimal(s) => s.clone(),
            Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S%.f%z")),
            Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
            Value::Time(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
            Value::ObjectId(id) => format!("'{}'", id.replace('\'', "''")),
            Value::Array(arr) => {
                let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
                format!("'{}'", json.replace('\'', "''"))
            }
            Value::Document(doc) => {
                let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
                format!("'{}'", json.replace('\'', "''"))
            }
        }
    }

    fn format_cell_for_sql(
        &self,
        cell: &crate::ui::components::data_table::model::CellValue,
    ) -> String {
        use crate::ui::components::data_table::model::CellKind;

        match &cell.kind {
            CellKind::Null => "NULL".to_string(),
            CellKind::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            CellKind::Int(i) => i.to_string(),
            CellKind::Float(f) => f.to_string(),
            CellKind::Text(s) => format!("'{}'", s.replace('\'', "''")),
            CellKind::Bytes(_) => "'<binary>'".to_string(),
            CellKind::AutoGenerated(expr) => format!("DEFAULT /* {} */", expr),
        }
    }

    fn get_column_default(&self, col: usize, cx: &Context<Self>) -> Option<String> {
        let (profile_id, table_ref) = match &self.source {
            DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table),
            DataSource::QueryResult { .. } => return None,
        };

        let col_name = self.result.columns.get(col)?.name.clone();

        let state = self.app_state.read(cx);
        let connected = state.connections.get(&profile_id)?;
        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = connected.table_details.get(&cache_key)?;
        let columns = table_info.columns.as_deref()?;

        columns
            .iter()
            .find(|c| c.name == col_name)
            .and_then(|c| c.default_value.clone())
    }

    /// Get default values for all columns.
    /// Returns a Vec with Some(default_expr) or None for each column.
    fn get_all_column_defaults(&self, cx: &Context<Self>) -> Vec<Option<String>> {
        let (profile_id, table_ref) = match &self.source {
            DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table),
            DataSource::QueryResult { .. } => {
                return vec![None; self.result.columns.len()];
            }
        };

        let state = self.app_state.read(cx);
        let connected = match state.connections.get(&profile_id) {
            Some(c) => c,
            None => return vec![None; self.result.columns.len()],
        };

        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = match connected.table_details.get(&cache_key) {
            Some(t) => t,
            None => return vec![None; self.result.columns.len()],
        };

        let columns = match table_info.columns.as_deref() {
            Some(c) => c,
            None => return vec![None; self.result.columns.len()],
        };

        // Map result columns to their defaults
        self.result
            .columns
            .iter()
            .map(|col| {
                columns
                    .iter()
                    .find(|c| c.name == col.name)
                    .and_then(|c| c.default_value.clone())
            })
            .collect()
    }
}
