mod context_menu;
mod mutations;
mod navigation;
mod query;
mod render;
mod utils;

use super::result_view::ResultViewMode;
use super::task_runner::DocumentTaskRunner;
use crate::app::AppState;
use crate::ui::cell_editor_modal::{CellEditorClosedEvent, CellEditorModal, CellEditorSaveEvent};
use crate::ui::components::data_table::{
    ContextMenuAction, DataTable, DataTableEvent, DataTableState, SortState as TableSortState,
    TableModel,
};
use crate::ui::components::document_tree::{DocumentTree, DocumentTreeEvent, DocumentTreeState};
use crate::ui::document_preview_modal::{
    DocumentPreviewClosedEvent, DocumentPreviewModal, DocumentPreviewSaveEvent,
};
use crate::ui::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::toast::PendingToast;
use crate::ui::toast::ToastExt;
use dbflux_core::{
    CollectionRef, OrderByColumn, Pagination, QueryResult, RefreshPolicy, SortDirection, TableRef,
    Value,
};
use gpui::*;
use gpui_component::Sizable;
use gpui_component::input::{Input, InputEvent, InputState};
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
    /// Collection (document database) with server-side pagination.
    Collection {
        profile_id: Uuid,
        collection: CollectionRef,
        pagination: Pagination,
        total_docs: Option<u64>,
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

    pub fn is_collection(&self) -> bool {
        matches!(self, DataSource::Collection { .. })
    }

    /// Returns true if this source supports server-side pagination.
    pub fn is_paginated(&self) -> bool {
        matches!(
            self,
            DataSource::Table { .. } | DataSource::Collection { .. }
        )
    }

    pub fn table_ref(&self) -> Option<&TableRef> {
        match self {
            DataSource::Table { table, .. } => Some(table),
            _ => None,
        }
    }

    pub fn collection_ref(&self) -> Option<&CollectionRef> {
        match self {
            DataSource::Collection { collection, .. } => Some(collection),
            _ => None,
        }
    }

    pub fn pagination(&self) -> Option<&Pagination> {
        match self {
            DataSource::Table { pagination, .. } => Some(pagination),
            DataSource::Collection { pagination, .. } => Some(pagination),
            DataSource::QueryResult { .. } => None,
        }
    }

    pub fn total_rows(&self) -> Option<u64> {
        match self {
            DataSource::Table { total_rows, .. } => *total_rows,
            DataSource::Collection { total_docs, .. } => *total_docs,
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
        row_values: Vec<Value>,
        pk_indices: Vec<usize>,
        generation_type: crate::ui::sql_preview_modal::SqlGenerationType,
    },
}

/// Internal state for grid loading/ready/error.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum GridState {
    #[default]
    Ready,
    Loading,
    Error,
}

/// Focus mode within the panel.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum GridFocusMode {
    #[default]
    Table,
    Toolbar,
}

/// Which toolbar element is focused.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum ToolbarFocus {
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
enum EditState {
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
    /// Qualified name of the table or collection (e.g., "public.users" or "mydb.users")
    source_qualified: String,
    total: u64,
}

struct PendingModalOpen {
    row: usize,
    col: usize,
    value: String,
    is_json: bool,
}

struct PendingDeleteConfirm {
    row_idx: usize,
    is_table: bool,
}

struct PendingDocumentPreview {
    doc_index: usize,
    document_json: String,
}

/// Context menu state for right-click operations.
struct TableContextMenu {
    /// Row index of the clicked cell (or document index in document view).
    row: usize,
    /// Column index of the clicked cell (unused in document view).
    col: usize,
    /// Screen position where the menu should appear.
    position: Point<Pixels>,
    /// Whether the SQL generation submenu is open.
    sql_submenu_open: bool,
    /// Currently selected menu item index (for keyboard navigation).
    selected_index: usize,
    /// Selected index within the SQL submenu (0-3).
    submenu_selected_index: usize,
    /// Whether this is a document view context menu (different items shown).
    is_document_view: bool,
}

/// A single item in the context menu.
struct ContextMenuItem {
    label: &'static str,
    action: Option<ContextMenuAction>,
    icon: Option<crate::ui::icons::AppIcon>,
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
    runner: DocumentTaskRunner,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    _refresh_timer: Option<Task<()>>,
    _refresh_subscriptions: Vec<Subscription>,
    state: GridState,
    pending_requery: Option<PendingRequery>,
    pending_total_count: Option<PendingTotalCount>,
    pending_rebuild: bool,
    pending_refresh: bool,
    pending_toast: Option<PendingToast>,
    pending_delete_confirm: Option<PendingDeleteConfirm>,

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
    pending_context_menu_focus: bool,

    // Modal editor for JSON/long text
    cell_editor: Entity<CellEditorModal>,
    pending_modal_open: Option<PendingModalOpen>,

    // Panel origin in window coordinates (for context menu positioning)
    panel_origin: Point<Pixels>,

    // View mode configuration
    view_config: super::data_view::DataViewConfig,

    // Result view mode for QueryResult sources (Text/Json/Raw/Table)
    result_view_mode: ResultViewMode,
    derived_json: Option<String>,
    derived_text: Option<String>,

    // Document tree for MongoDB document view
    document_tree: Option<Entity<DocumentTree>>,
    document_tree_state: Option<Entity<DocumentTreeState>>,
    document_tree_subscription: Option<Subscription>,

    // Document preview modal for viewing/editing full documents
    document_preview_modal: Entity<DocumentPreviewModal>,
    pending_document_preview: Option<PendingDocumentPreview>,
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

    pub fn new_for_collection(
        profile_id: Uuid,
        collection: CollectionRef,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let pagination = Pagination::default();

        let source = DataSource::Collection {
            profile_id,
            collection,
            pagination,
            total_docs: None,
        };

        // Document collections use _id as the primary key
        let pk_columns = vec!["_id".to_string()];

        let mut panel = Self::new_internal(source, app_state, pk_columns, window, cx);
        panel.refresh(window, cx);
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
                .connections()
                .get(&profile_id)
                .and_then(|c| c.active_database.clone())
                .unwrap_or_else(|| "default".to_string())
        };

        log::info!(
            "[PK] Fetching table details for PK columns: {}.{}",
            database,
            table.qualified_name()
        );

        let params = match self.app_state.read(cx).prepare_fetch_table_details(
            profile_id,
            &database,
            &table.name,
        ) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("[PK] Failed to prepare fetch_table_details: {}", e);
                return;
            }
        };

        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { params.execute() })
                .await;

            cx.update(|cx| {
                let fetch_result = match result {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[PK] Failed to fetch table details: {}", e);
                        return;
                    }
                };

                // Extract PK columns
                let columns = fetch_result.details.columns.as_deref().unwrap_or(&[]);

                let pk_names: Vec<String> = columns
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| c.name.clone())
                    .collect();

                // Store in cache
                app_state.update(cx, |state, _| {
                    state.set_table_details(
                        fetch_result.profile_id,
                        fetch_result.database,
                        fetch_result.table,
                        fetch_result.details,
                    );
                });

                // Update panel with PK info
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
        let filter_placeholder = if source.is_collection() {
            r#"e.g. {"name": {"$regex": "test"}}"#
        } else {
            "e.g. id > 10 AND name LIKE '%test%'"
        };

        let filter_input = cx.new(|cx| InputState::new(window, cx).placeholder(filter_placeholder));

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

        cx.subscribe_in(
            &cell_editor,
            window,
            |this, _, _: &CellEditorClosedEvent, window, cx| {
                this.focus_active_view(window, cx);
            },
        )
        .detach();

        let document_preview_modal = cx.new(|cx| DocumentPreviewModal::new(window, cx));

        cx.subscribe_in(
            &document_preview_modal,
            window,
            |this, _, event: &DocumentPreviewSaveEvent, window, cx| {
                this.handle_document_preview_save(
                    event.doc_index,
                    &event.document_json,
                    window,
                    cx,
                );
            },
        )
        .detach();

        cx.subscribe_in(
            &document_preview_modal,
            window,
            |this, _, _: &DocumentPreviewClosedEvent, window, cx| {
                this.focus_active_view(window, cx);
            },
        )
        .detach();

        let view_config = super::data_view::DataViewConfig::for_source(&source);
        let result_view_mode = ResultViewMode::Table;

        let supports_auto_refresh = matches!(
            &source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        );

        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(policy.label()))
                .collect();

            Dropdown::new("data-grid-auto-refresh")
                .items(items)
                .selected_index(Some(RefreshPolicy::Manual.index()))
                .disabled(!supports_auto_refresh)
                .compact_trigger(true)
        });

        let refresh_policy_sub = cx.subscribe_in(
            &refresh_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                let policy = RefreshPolicy::from_index(event.index);

                if policy.is_auto() && !this.supports_auto_refresh() {
                    this.refresh_dropdown.update(cx, |dd, cx| {
                        dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
                    });
                    cx.toast_warning("Auto-refresh not available for query results", window);
                    return;
                }

                this.set_refresh_policy(policy, cx);
            },
        );

        let runner = {
            let mut r = DocumentTaskRunner::new(app_state.clone());

            let pid = match &source {
                DataSource::Table { profile_id, .. } => Some(*profile_id),
                DataSource::Collection { profile_id, .. } => Some(*profile_id),
                DataSource::QueryResult { .. } => None,
            };

            if let Some(pid) = pid {
                r.set_profile_id(pid);
            }

            r
        };

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
            runner,
            refresh_policy: RefreshPolicy::Manual,
            refresh_dropdown,
            _refresh_timer: None,
            _refresh_subscriptions: vec![refresh_policy_sub],
            state: GridState::Ready,
            pending_requery: None,
            pending_total_count: None,
            pending_rebuild: false,
            pending_refresh: false,
            pending_toast: None,
            pending_delete_confirm: None,
            focus_handle,
            focus_mode: GridFocusMode::default(),
            toolbar_focus: ToolbarFocus::default(),
            edit_state: EditState::default(),
            switching_input: false,
            show_panel_controls: false,
            is_maximized: false,
            context_menu: None,
            context_menu_focus,
            pending_context_menu_focus: false,
            cell_editor,
            pending_modal_open: None,
            panel_origin: Point::default(),
            view_config,
            result_view_mode,
            derived_json: None,
            derived_text: None,
            document_tree: None,
            document_tree_state: None,
            document_tree_subscription: None,
            document_preview_modal,
            pending_document_preview: None,
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

    /// Toggle between available view modes for the current data source.
    pub fn toggle_view_mode(&mut self, cx: &mut Context<Self>) {
        use super::data_view::DataViewMode;

        let available = DataViewMode::available_for(&self.source);
        if available.len() <= 1 {
            return;
        }

        let current_idx = available
            .iter()
            .position(|m| *m == self.view_config.mode)
            .unwrap_or(0);

        let next_idx = (current_idx + 1) % available.len();
        self.view_config.mode = available[next_idx];
        cx.notify();
    }

    /// Check if view mode toggle is available for the current source.
    pub fn can_toggle_view(&self) -> bool {
        super::data_view::DataViewMode::available_for(&self.source).len() > 1
    }

    pub fn set_result_view_mode(&mut self, mode: ResultViewMode, cx: &mut Context<Self>) {
        if self.result_view_mode == mode {
            return;
        }

        self.result_view_mode = mode;
        cx.notify();
    }

    fn uses_result_view(&self) -> bool {
        matches!(self.source, DataSource::QueryResult { .. }) && !self.result_view_mode.is_table()
    }

    pub(super) fn derived_text(&mut self) -> &str {
        if self.derived_text.is_none() {
            self.derived_text = Some(self.compute_derived_text());
        }
        self.derived_text.as_deref().unwrap_or("")
    }

    pub(super) fn derived_json(&mut self) -> &str {
        if self.derived_json.is_none() {
            self.derived_json = Some(self.compute_derived_json());
        }
        self.derived_json.as_deref().unwrap_or("")
    }

    fn compute_derived_text(&self) -> String {
        if let Some(body) = &self.result.text_body {
            return body.clone();
        }

        // Fall back to rendering rows as text
        self.result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.as_display_string())
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn compute_derived_json(&self) -> String {
        use utils::value_to_json;

        if let Some(body) = &self.result.text_body {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
                return serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| body.clone());
            }
            return body.clone();
        }

        // Build JSON from rows
        let json_rows: Vec<serde_json::Value> = self
            .result
            .rows
            .iter()
            .map(|row| {
                if self.result.columns.is_empty() {
                    // Single-value rows
                    if row.len() == 1 {
                        value_to_json(&row[0])
                    } else {
                        serde_json::Value::Array(row.iter().map(value_to_json).collect())
                    }
                } else {
                    let obj: serde_json::Map<String, serde_json::Value> = self
                        .result
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(col, val)| (col.name.clone(), value_to_json(val)))
                        .collect();
                    serde_json::Value::Object(obj)
                }
            })
            .collect();

        if json_rows.len() == 1 {
            serde_json::to_string_pretty(&json_rows[0]).unwrap_or_default()
        } else {
            serde_json::to_string_pretty(&json_rows).unwrap_or_default()
        }
    }

    pub fn supports_auto_refresh(&self) -> bool {
        matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        )
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        if self.refresh_policy == policy {
            return;
        }

        self.refresh_policy = policy;
        self.update_refresh_timer(cx);
        cx.notify();
    }

    fn update_refresh_timer(&mut self, cx: &mut Context<Self>) {
        self._refresh_timer = None;

        if !self.supports_auto_refresh() {
            return;
        }

        let Some(duration) = self.refresh_policy.duration() else {
            return;
        };

        self._refresh_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                let _ = cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };

                    entity.update(cx, |panel, cx| {
                        if !panel.refresh_policy.is_auto()
                            || !panel.supports_auto_refresh()
                            || panel.runner.is_primary_active()
                        {
                            return;
                        }

                        panel.pending_refresh = true;
                        cx.notify();
                    });
                });
            }
        }));
    }

    /// Update the result data (for QueryResult source or after table fetch).
    pub fn set_result(&mut self, result: QueryResult, cx: &mut Context<Self>) {
        self.view_config = super::data_view::DataViewConfig::for_source(&self.source);
        self.result_view_mode = ResultViewMode::default_for_shape(&result.shape);
        self.derived_json = None;
        self.derived_text = None;
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
        self.refresh_policy = RefreshPolicy::Manual;
        self._refresh_timer = None;
        self.refresh_dropdown.update(cx, |dd, cx| {
            dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
        });

        self.source = DataSource::QueryResult {
            result: result.clone(),
            original_query: query,
        };
        self.local_sort_state = None;
        self.original_row_order = None;
        self.set_result((*result).clone(), cx);
    }

    pub(super) fn focus_active_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = GridFocusMode::Table;
        self.edit_state = EditState::Navigating;

        if self.view_config.mode == super::data_view::DataViewMode::Document {
            if let Some(tree_state) = &self.document_tree_state {
                tree_state.update(cx, |state, _| state.focus(window));
            } else {
                self.focus_handle.focus(window);
            }
        } else {
            self.focus_handle.focus(window);
        }

        cx.emit(DataGridEvent::Focused);
        cx.notify();
    }

    fn rebuild_table(&mut self, initial_sort: Option<TableSortState>, cx: &mut Context<Self>) {
        // Find PK column indices in result columns
        let pk_indices: Vec<usize> = self
            .pk_columns
            .iter()
            .filter_map(|pk_name| self.result.columns.iter().position(|c| c.name == *pk_name))
            .collect();

        log::debug!(
            "rebuild_table: pk_columns={:?}, pk_indices={:?}",
            self.pk_columns,
            pk_indices,
        );

        let is_insertable = matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        );

        let table_model = Arc::new(TableModel::from(&self.result));
        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            if let Some(sort) = initial_sort {
                state.set_sort_without_emit(sort);
            }
            state.set_pk_columns(pk_indices.clone());
            state.set_insertable(is_insertable);
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
                            is_document_view: false,
                        });
                        this.pending_context_menu_focus = true;
                        cx.emit(DataGridEvent::Focused);
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
                    DataTableEvent::CommitInsertRequested(insert_idx) => {
                        this.handle_commit_insert(*insert_idx, cx);
                    }
                    DataTableEvent::CommitDeleteRequested(row_idx) => {
                        this.handle_commit_delete(*row_idx, cx);
                    }
                }
            });

        self.table_state = Some(table_state);
        self.data_table = Some(data_table);
        self.table_subscription = Some(subscription);

        // Build document tree for collections OR JSON-shaped query results
        let should_build_tree = self.source.is_collection()
            || matches!(&self.source, DataSource::QueryResult { result, .. } if result.shape.is_json());

        if should_build_tree {
            self.rebuild_document_tree(cx);
        }
    }

    fn rebuild_document_tree(&mut self, cx: &mut Context<Self>) {
        let tree_state = cx.new(|cx| {
            let mut state = DocumentTreeState::new(cx);
            state.load_from_result(&self.result, cx);
            state
        });

        let tree = cx.new(|cx| DocumentTree::new("document-tree", tree_state.clone(), cx));

        let subscription = cx.subscribe(
            &tree_state,
            |this, _state, event: &DocumentTreeEvent, cx| match event {
                DocumentTreeEvent::Focused => {
                    cx.emit(DataGridEvent::Focused);
                }
                DocumentTreeEvent::InlineEditCommitted { node_id, new_value } => {
                    this.handle_document_tree_inline_edit(node_id, new_value, cx);
                }
                DocumentTreeEvent::DocumentPreviewRequested {
                    doc_index,
                    document_json,
                } => {
                    this.pending_document_preview = Some(PendingDocumentPreview {
                        doc_index: *doc_index,
                        document_json: document_json.clone(),
                    });
                    cx.notify();
                }
                DocumentTreeEvent::DeleteRequested(node_id) => {
                    if let Some(doc_idx) = node_id.doc_index() {
                        this.pending_delete_confirm = Some(PendingDeleteConfirm {
                            row_idx: doc_idx,
                            is_table: false,
                        });
                        cx.notify();
                    }
                }
                DocumentTreeEvent::ContextMenuRequested {
                    doc_index,
                    position,
                } => {
                    // Set context menu state directly (same pattern as DataTableEvent::ContextMenuRequested)
                    this.context_menu = Some(TableContextMenu {
                        row: *doc_index,
                        col: 0,
                        position: *position,
                        sql_submenu_open: false,
                        selected_index: 0,
                        submenu_selected_index: 0,
                        is_document_view: true,
                    });
                    this.pending_context_menu_focus = true;
                    cx.emit(DataGridEvent::Focused);
                    cx.notify();
                }
                DocumentTreeEvent::CursorMoved
                | DocumentTreeEvent::ExpandToggled
                | DocumentTreeEvent::ViewModeToggled
                | DocumentTreeEvent::SearchOpened
                | DocumentTreeEvent::SearchClosed => {}
            },
        );

        self.document_tree_state = Some(tree_state);
        self.document_tree = Some(tree);
        self.document_tree_subscription = Some(subscription);
    }

    // === Panel Events ===

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
        let Some(connected) = state.connections().get(&profile_id) else {
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

        for db_schema in schema.schemas() {
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
            DataSource::Collection { .. } => None,
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
