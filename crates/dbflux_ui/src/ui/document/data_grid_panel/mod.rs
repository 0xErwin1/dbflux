mod context_menu;
mod mutations;
mod navigation;
mod query;
mod render;
pub mod row_inspector;
mod utils;

use super::result_view::ResultViewMode;
use super::task_runner::DocumentTaskRunner;
use crate::app::AppStateEntity;
use crate::ui::AsyncUpdateResultExt;
use crate::ui::components::data_table::{
    ContextMenuAction, DataTable, DataTableEvent, DataTableState, SortState as TableSortState,
    TableModel,
};
use crate::ui::components::document_tree::{DocumentTree, DocumentTreeEvent, DocumentTreeState};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::toast::PendingToast;
use crate::ui::components::toast::{Toast, copy_action, now_hms};
use crate::ui::overlays::cell_editor_modal::{
    CellEditorClosedEvent, CellEditorModal, CellEditorSaveEvent,
};
use crate::ui::overlays::document_preview_modal::{
    DocumentPreviewClosedEvent, DocumentPreviewModal, DocumentPreviewSaveEvent,
};
use crate::ui::overlays::sql_preview_modal::SqlPreviewContext;
use dbflux_components::chart::{
    ChartDetection, ChartSpec, ChartView, ManualChartSelection, detect_chart_columns,
};
use std::collections::HashSet;
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{
    CollectionRef, DatabaseCategory, OrderByColumn, Pagination, QueryResult, RefreshPolicy,
    SortDirection, TableRef, Value,
};
use gpui::*;
use gpui_component::Sizable;
use std::sync::Arc;
use uuid::Uuid;

/// Source of data for the grid panel.
#[derive(Clone)]
pub enum DataSource {
    /// Table with server-side pagination and sorting.
    Table {
        profile_id: Uuid,
        database: Option<String>,
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
        /// Backing connection profile, when the result came from a host
        /// (CodeDocument, ScriptDocument) that knows which connection was
        /// targeted. Used by category-driven UI gates such as the chart
        /// toggle. `None` for ad-hoc results without an associated connection.
        profile_id: Option<Uuid>,
    },
}

impl DataSource {
    pub fn is_table(&self) -> bool {
        matches!(self, DataSource::Table { .. })
    }

    #[allow(dead_code)]
    pub fn database(&self) -> Option<&str> {
        match self {
            DataSource::Table { database, .. } => database.as_deref(),
            _ => None,
        }
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
        context: Box<SqlPreviewContext>,
        generation_type: crate::ui::overlays::sql_preview_modal::SqlGenerationType,
    },
    /// Request to mount arbitrary content into the workspace-level inspector rail.
    OpenInspector {
        title: SharedString,
        content: AnyView,
    },
}

/// Active tab in the Chart Configure rail.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum ChartRailTab {
    #[default]
    Configure,
    Stats,
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
    database: Option<String>,
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
    row_indices: Vec<usize>,
    is_table: bool,
}

/// Remaining operations in a batch save pipeline.
/// After deletes complete, inserts run one by one, then dirty rows.
/// pending_refresh is only set after all operations finish.
struct PendingBatchRemaining {
    pending_inserts: Vec<usize>,
    dirty_rows: Vec<usize>,
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
    /// Whether the "Copy as Query" submenu is open.
    copy_query_submenu_open: bool,
    /// Whether the "Filter" submenu is open.
    filter_submenu_open: bool,
    /// Whether the "Order" submenu is open.
    order_submenu_open: bool,
    /// Currently selected menu item index (for keyboard navigation).
    selected_index: usize,
    /// Selected index within the active submenu.
    submenu_selected_index: usize,
    /// Whether this is a document view context menu (different items shown).
    is_document_view: bool,
    doc_field_path: Option<Vec<String>>,
    doc_field_value: Option<crate::ui::components::document_tree::NodeValue>,
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
    app_state: Entity<AppStateEntity>,

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
    pending_batch_remaining: Option<PendingBatchRemaining>,
    is_active_tab: bool,

    // Focus
    focus_handle: FocusHandle,
    focus_mode: GridFocusMode,
    toolbar_focus: ToolbarFocus,
    edit_state: EditState,
    switching_input: bool,

    // Panel controls (shown when embedded in CodeDocument)
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

    // Row inspector content entity (workspace owns the chrome/lifecycle).
    row_inspector_content: Option<Entity<row_inspector::RowInspectorContent>>,

    export_menu_open: bool,

    // Chart view (T10-T13)
    /// Auto-detection result for the current result set. `None` means detection
    /// has not run yet or the result is incompatible.
    chart_detection: Option<ChartDetection>,
    /// Built `ChartView` entity. Created on demand when the user switches to
    /// Chart mode or when detection succeeds and the mode is already Chart.
    chart_view: Option<Entity<ChartView>>,
    /// Observation on `chart_view` that re-renders the panel when the chart's
    /// internal state (focused series, hover, etc.) changes, so panel-side
    /// surfaces such as the Stats tab stay in sync with hover-driven focus.
    chart_view_observer: Option<Subscription>,
    /// Manual column selection overriding auto-detection. `None` = use detection.
    chart_manual_selection: Option<ManualChartSelection>,
    /// Index of the currently focused legend series.
    chart_focused_series_idx: usize,
    /// User-controlled legend visibility (REQ-CHART-029). Defaults to `true`;
    /// the engine still hides the legend automatically when series count <= 1.
    chart_legend_visible: bool,

    // Manual column picker state — used when chart_detection is not Ok and the
    // user is choosing columns manually. Reset whenever chart_detection changes.
    /// Index into the result's columns for the picker's selected X column.
    chart_picker_x_col: usize,
    /// Checked state for each Y-candidate column (parallel to the Y-candidate list
    /// built from result columns with kind Float, Integer, or Unknown).
    chart_picker_y_checked: Vec<bool>,

    /// Series indices hidden by the user via the legend. Cleared on set_result and Apply.
    pub(super) chart_hidden_series: HashSet<usize>,

    // Chart Configure rail (feedback round)
    /// Whether the Configure rail is currently visible. Toggled by the gear button.
    pub(super) chart_rail_open: bool,
    /// Active tab inside the rail (Configure or Stats).
    pub(super) chart_rail_tab: ChartRailTab,
    /// Selected X-column index in the rail picker (mirrors the degraded picker).
    pub(super) chart_rail_picker_x_col: usize,
    /// Checked state per Y-candidate column in the rail picker.
    pub(super) chart_rail_picker_y_checked: Vec<bool>,
}

impl DataGridPanel {
    pub fn new_for_table(
        profile_id: Uuid,
        table: TableRef,
        database: Option<String>,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let order_by = Self::get_primary_key_columns(&app_state, profile_id, &table, cx);
        let pk_columns: Vec<String> = order_by.iter().map(|c| c.column.name.clone()).collect();
        let pagination = Pagination::default();

        let source = DataSource::Table {
            profile_id,
            database,
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
        app_state: Entity<AppStateEntity>,
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
        let source_database = match &self.source {
            DataSource::Table { database, .. } => database.clone(),
            _ => None,
        };

        let database = source_database.unwrap_or_else(|| {
            let state = self.app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .and_then(|c| c.active_database.clone())
                .unwrap_or_else(|| "default".to_string())
        });

        log::info!(
            "[PK] Fetching table details for PK columns: {}.{}",
            database,
            table.qualified_name()
        );

        let params = match self.app_state.read(cx).prepare_fetch_table_details(
            profile_id,
            &database,
            table.schema.as_deref(),
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
                        fetch_result.database.clone(),
                        fetch_result.table.clone(),
                        fetch_result.details,
                    );
                    state.set_dependents(
                        fetch_result.profile_id,
                        fetch_result.database,
                        fetch_result.table,
                        fetch_result.dependents,
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
            .log_if_dropped();
        })
        .detach();
    }

    /// Create a new panel for displaying a query result (in-memory sorting).
    pub fn new_for_result(
        result: Arc<QueryResult>,
        original_query: String,
        profile_id: Option<Uuid>,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let source = DataSource::QueryResult {
            result: result.clone(),
            original_query,
            profile_id,
        };

        // Query results are not editable (no PK info)
        let mut panel = Self::new_internal(source, app_state, Vec::new(), window, cx);
        panel.set_result((*result).clone(), cx);
        panel
    }

    fn new_internal(
        source: DataSource,
        app_state: Entity<AppStateEntity>,
        pk_columns: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let filter_placeholder = Self::filter_placeholder_for_source(&source, &app_state, cx);

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

        let connection_id = match &source {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::Collection { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { .. } => None,
        };

        let default_refresh = app_state
            .read(cx)
            .effective_settings_for_connection(connection_id)
            .resolve_refresh_policy();

        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(policy.label()))
                .collect();

            Dropdown::new("data-grid-auto-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
                .disabled(!supports_auto_refresh)
                .compact_trigger(true)
        });

        let refresh_policy_sub = cx.subscribe_in(
            &refresh_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                let policy = RefreshPolicy::from_index(event.index);

                if policy.is_auto() && !this.supports_auto_refresh() {
                    this.refresh_dropdown.update(cx, |dd, cx| {
                        dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
                    });
                    Toast::warning("Auto-refresh not available for query results")
                        .meta_right(now_hms())
                        .push(cx);
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
            refresh_policy: default_refresh,
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
            pending_batch_remaining: None,
            is_active_tab: true,
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
            row_inspector_content: None,
            export_menu_open: false,
            chart_detection: None,
            chart_view: None,
            chart_view_observer: None,
            chart_manual_selection: None,
            chart_focused_series_idx: 0,
            chart_legend_visible: true,
            chart_picker_x_col: 0,
            chart_picker_y_checked: Vec::new(),
            chart_hidden_series: HashSet::new(),
            chart_rail_open: false,
            chart_rail_tab: ChartRailTab::Configure,
            chart_rail_picker_x_col: 0,
            chart_rail_picker_y_checked: Vec::new(),
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

    /// Returns `true` when the current result has a `Timestamp` column and at
    /// least one numeric column — i.e., chart mode is available.
    pub(super) fn chart_available(&self) -> bool {
        matches!(&self.chart_detection, Some(ChartDetection::Ok { .. }))
    }

    /// Build or return the existing `ChartView` entity for the current result.
    ///
    /// Returns `None` when detection failed or the result is incompatible.
    /// Uses the manual selection if set, otherwise auto-detection.
    pub(super) fn ensure_chart_view(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Option<Entity<ChartView>> {
        if self.chart_view.is_some() {
            return self.chart_view.clone();
        }

        let mut spec = if let Some(manual) = &self.chart_manual_selection {
            ChartSpec::from_manual_selection(manual, &self.result.columns, 10_000)
        } else {
            match &self.chart_detection {
                Some(ChartDetection::Ok {
                    time_col,
                    numeric_cols,
                }) => ChartSpec::from_detection(
                    *time_col,
                    numeric_cols.clone(),
                    &self.result.columns,
                    10_000,
                ),
                _ => None,
            }
        }?;

        // Apply the user-controlled legend override: the engine's default rule
        // (`series.len() > 1`) is preserved when the user hasn't toggled the
        // legend off; once toggled off it stays off regardless of series count.
        spec.legend_visible = self.chart_legend_visible && spec.series.len() > 1;

        match ChartView::build(&self.result, spec) {
            Ok(chart_view) => {
                let entity = cx.new(|_cx| chart_view);
                // Re-render the panel whenever the chart entity notifies, so
                // hover-driven focus changes propagate to the Stats tab.
                let observer = cx.observe(&entity, |_this, _chart, cx| cx.notify());
                self.chart_view = Some(entity.clone());
                self.chart_view_observer = Some(observer);
                Some(entity)
            }
            Err(err) => {
                log::warn!("[chart] ChartView::build failed: {}", err);
                None
            }
        }
    }

    /// Toggle the hidden state of a series by index.
    ///
    /// Propagates the updated hidden set to the live `ChartView` entity.
    pub(super) fn toggle_chart_series_hidden(&mut self, idx: usize, cx: &mut Context<Self>) {
        if self.chart_hidden_series.contains(&idx) {
            self.chart_hidden_series.remove(&idx);
        } else {
            self.chart_hidden_series.insert(idx);
        }

        if let Some(chart_entity) = self.chart_view.clone() {
            let hidden = self.chart_hidden_series.clone();
            chart_entity.update(cx, |view, cx| {
                view.set_hidden_series(hidden, cx);
            });
        }

        cx.notify();
    }

    /// Reset the manual picker state for a new result's column list.
    ///
    /// X defaults to the first Timestamp column, or column 0 if none.
    /// Y checkboxes default to `true` for Float and Integer columns.
    pub(super) fn reset_chart_picker(&mut self, columns: &[dbflux_core::ColumnMeta]) {
        use dbflux_core::ColumnKind;

        self.chart_picker_x_col = columns
            .iter()
            .position(|c| c.kind == ColumnKind::Timestamp)
            .unwrap_or(0);

        self.chart_picker_y_checked = columns
            .iter()
            .filter(|c| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|c| matches!(c.kind, ColumnKind::Float | ColumnKind::Integer))
            .collect();
    }

    /// Initialise the Configure rail picker from the current chart spec.
    ///
    /// Called when the rail is toggled open so the controls reflect what is
    /// currently rendered (either auto-detected or manual).
    pub(super) fn prime_chart_rail_picker_from_spec(&mut self) {
        use dbflux_core::ColumnKind;

        let columns = &self.result.columns;

        // Determine effective X and Y columns from current selection or detection.
        let (x_col, y_col_indices) = if let Some(manual) = &self.chart_manual_selection {
            let ys: Vec<usize> = manual.y_cols.clone();
            (manual.x_col, ys)
        } else if let Some(ChartDetection::Ok {
            time_col,
            numeric_cols,
        }) = &self.chart_detection
        {
            (*time_col, numeric_cols.clone())
        } else {
            // No usable spec — fall back to defaults.
            let x = columns
                .iter()
                .position(|c| c.kind == ColumnKind::Timestamp)
                .unwrap_or(0);
            (x, vec![])
        };

        // Map x_col to the rail picker's X index (index into X-candidate list).
        // The rail picker uses the same X-candidates as the degraded picker:
        // Timestamp, Text, or Unknown columns.
        let x_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Text | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        self.chart_rail_picker_x_col = x_candidates.iter().position(|&ci| ci == x_col).unwrap_or(0);

        // Y-candidate list: Float, Integer, or Unknown columns.
        let y_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        self.chart_rail_picker_y_checked = y_candidates
            .iter()
            .map(|ci| y_col_indices.contains(ci))
            .collect();
    }

    /// Apply the current rail Configure picker state as a `ManualChartSelection`.
    ///
    /// Clears the existing `chart_view` so the next render triggers a rebuild.
    /// The rail stays open so the user can see the updated chart.
    pub(super) fn apply_chart_rail_selection(&mut self, cx: &mut Context<Self>) {
        use dbflux_core::ColumnKind;

        let columns = &self.result.columns;

        let x_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Text | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        let x_col = x_candidates
            .get(self.chart_rail_picker_x_col)
            .copied()
            .unwrap_or(0);

        let y_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        let y_cols: Vec<usize> = y_candidates
            .iter()
            .zip(self.chart_rail_picker_y_checked.iter())
            .filter_map(|(&ci, &checked)| if checked { Some(ci) } else { None })
            .collect();

        if y_cols.is_empty() {
            // Nothing to chart; do not trigger a rebuild.
            return;
        }

        self.chart_manual_selection = Some(ManualChartSelection { x_col, y_cols });
        self.chart_view = None;
        self.chart_view_observer = None;
        self.chart_hidden_series = HashSet::new();
        cx.notify();
    }

    /// Reset chart selection to auto-detection, clearing any manual override.
    ///
    /// Disabled (no-op) when detection did not produce an `Ok` result.
    pub(super) fn reset_chart_rail_to_auto(&mut self, cx: &mut Context<Self>) {
        if !matches!(&self.chart_detection, Some(ChartDetection::Ok { .. })) {
            return;
        }
        self.chart_manual_selection = None;
        self.chart_view = None;
        self.chart_view_observer = None;
        // Re-prime the picker to reflect the detection columns.
        self.prime_chart_rail_picker_from_spec();
        cx.notify();
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

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
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

                        let settings = panel.app_state.read(cx).general_settings();

                        if settings.auto_refresh_pause_on_error && panel.state == GridState::Error {
                            return;
                        }

                        if settings.auto_refresh_only_if_visible && !panel.is_active_tab {
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

        // Run chart detection on the new result; invalidate any stale chart view.
        self.chart_detection = Some(detect_chart_columns(&result));
        self.chart_view = None;
        self.chart_view_observer = None;
        self.chart_manual_selection = None;
        self.chart_focused_series_idx = 0;
        self.reset_chart_picker(&result.columns);

        // Reset the Configure rail and hidden-series state for the new result.
        self.chart_hidden_series = HashSet::new();
        self.chart_rail_open = false;
        self.chart_rail_tab = ChartRailTab::Configure;
        self.chart_rail_picker_x_col = 0;
        self.chart_rail_picker_y_checked = Vec::new();

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
        profile_id: Option<Uuid>,
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
            profile_id,
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
        // For collections, update pk_columns from result metadata (is_primary_key flag)
        // This allows DynamoDB and other drivers to use their actual primary keys
        // instead of hardcoded "_id"
        if self.source.is_collection() {
            let pk_columns_from_metadata: Vec<String> = self
                .result
                .columns
                .iter()
                .filter(|col| col.is_primary_key)
                .map(|col| col.name.clone())
                .collect();

            if !pk_columns_from_metadata.is_empty() {
                self.pk_columns = pk_columns_from_metadata;
            }
            // If no columns are marked as PK, keep the existing pk_columns (fallback to "_id" for MongoDB)
        }

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

        let column_details = self.get_column_details(cx);

        // Compute FK column indices before entering the cx.new closure.
        let fk_names = self.get_fk_column_names(cx);
        let fk_indices: std::collections::HashSet<usize> = if fk_names.is_empty() {
            std::collections::HashSet::new()
        } else {
            self.result
                .columns
                .iter()
                .enumerate()
                .filter(|(_, col)| fk_names.contains(&col.name))
                .map(|(ix, _)| ix)
                .collect()
        };

        let table_model = Arc::new(TableModel::from(&self.result));
        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            if let Some(sort) = initial_sort {
                state.set_sort_without_emit(sort);
            }
            state.set_pk_columns(pk_indices.clone());
            state.set_insertable(is_insertable);

            if !fk_indices.is_empty() {
                state.set_fk_columns(fk_indices);
            }

            if let Some(columns) = &column_details {
                for (col_ix, result_col) in self.result.columns.iter().enumerate() {
                    if let Some(info) = columns.iter().find(|c| c.name == result_col.name)
                        && let Some(enum_vals) = &info.enum_values
                    {
                        let mut options = enum_vals.clone();
                        if info.nullable {
                            options.insert(0, DataTableState::NULL_SENTINEL.to_string());
                        }
                        state.set_enum_options(col_ix, options);
                    }
                }
            }

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
                            copy_query_submenu_open: false,
                            filter_submenu_open: false,
                            order_submenu_open: false,
                            selected_index: 0,
                            submenu_selected_index: 0,
                            is_document_view: false,
                            doc_field_path: None,
                            doc_field_value: None,
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
                        this.handle_add_row(*row, false, cx);
                    }
                    DataTableEvent::DuplicateRowRequested(row) => {
                        this.handle_duplicate_row(*row, false, cx);
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
                    DataTableEvent::SaveAllRequested {
                        pending_deletes,
                        pending_inserts,
                        dirty_rows,
                    } => {
                        this.handle_save_all(
                            pending_deletes.clone(),
                            pending_inserts.clone(),
                            dirty_rows.clone(),
                            cx,
                        );
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
                            row_indices: vec![doc_idx],
                            is_table: false,
                        });
                        cx.notify();
                    }
                }
                DocumentTreeEvent::ContextMenuRequested {
                    doc_index,
                    position,
                    node_id,
                    node_value,
                } => {
                    let field_path: Vec<String> = node_id.path[1..].to_vec();

                    this.context_menu = Some(TableContextMenu {
                        row: *doc_index,
                        col: 0,
                        position: *position,
                        sql_submenu_open: false,
                        copy_query_submenu_open: false,
                        filter_submenu_open: false,
                        order_submenu_open: false,
                        selected_index: 0,
                        submenu_selected_index: 0,
                        is_document_view: true,
                        doc_field_path: if field_path.is_empty() {
                            None
                        } else {
                            Some(field_path)
                        },
                        doc_field_value: node_value.clone(),
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
        app_state: &Entity<AppStateEntity>,
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
                .map(|col| (col.column.name.clone(), col.direction, true)),
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

    /// Returns `(inserts, updates, deletes)` counts from the pending edit buffer.
    ///
    /// Returns `(0, 0, 0)` when the table has no edit state or no pending changes.
    pub fn pending_edit_counts(&self, cx: &App) -> (usize, usize, usize) {
        let Some(table_state) = &self.table_state else {
            return (0, 0, 0);
        };

        let state = table_state.read(cx);
        let buffer = state.edit_buffer();

        let inserts = buffer.pending_insert_rows().len();
        let updates = buffer.dirty_row_count();
        let deletes = buffer.pending_delete_rows().len();

        (inserts, updates, deletes)
    }

    /// Short summary of pending edits for the dirty-dot tooltip.
    ///
    /// Returns `None` when no changes are staged.
    pub fn change_summary(&self, cx: &App) -> Option<String> {
        let (inserts, updates, deletes) = self.pending_edit_counts(cx);

        if inserts == 0 && updates == 0 && deletes == 0 {
            None
        } else {
            Some(format!(
                "{} inserts · {} updates · {} deletes",
                inserts, updates, deletes
            ))
        }
    }

    // === Filter bar presentation helpers ===

    /// Resolve the database category for the connection backing this data source.
    ///
    /// `QueryResult` sources carry an optional `profile_id` because the host
    /// (CodeDocument, ScriptDocument) knows which connection produced the
    /// result; this is what allows category-driven UI gates (chart toggle,
    /// filter labels) to work on query results. Returns `None` when the
    /// profile is unknown or no longer registered.
    pub(super) fn connection_category(
        source: &DataSource,
        app_state: &Entity<AppStateEntity>,
        cx: &App,
    ) -> Option<DatabaseCategory> {
        let profile_id = match source {
            DataSource::Table { profile_id, .. } => *profile_id,
            DataSource::Collection { profile_id, .. } => *profile_id,
            DataSource::QueryResult { profile_id, .. } => (*profile_id)?,
        };

        app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.connection.metadata().category)
    }

    /// Filter verb and filter keyword ("SELECT * FROM" / "find" / "FROM") shown
    /// in the toolbar to the left of the source name and to the left of the filter
    /// input, respectively.
    ///
    /// Derived purely from `DatabaseCategory` — no driver-id branching.
    pub(super) fn filter_labels_for_source(
        source: &DataSource,
        app_state: &Entity<AppStateEntity>,
        cx: &App,
    ) -> (&'static str, &'static str) {
        if source.is_table() {
            return ("SELECT * FROM", "WHERE");
        }

        match Self::connection_category(source, app_state, cx) {
            Some(DatabaseCategory::Document) => ("find", "WHERE"),
            Some(DatabaseCategory::TimeSeries) => ("SELECT * FROM", "WHERE"),
            _ => ("SELECT * FROM", "WHERE"),
        }
    }

    /// Filter input placeholder text, derived from `DatabaseCategory`.
    ///
    /// Returns an empty string for `TimeSeries` sources because `browse_collection`
    /// on InfluxDB ignores the filter field — showing a misleading placeholder
    /// would lie to the user.
    fn filter_placeholder_for_source(
        source: &DataSource,
        app_state: &Entity<AppStateEntity>,
        cx: &App,
    ) -> &'static str {
        if source.is_table() {
            return "e.g. id > 10 AND name LIKE '%test%'";
        }

        match Self::connection_category(source, app_state, cx) {
            Some(DatabaseCategory::Document) => r#"e.g. {"name": {"$regex": "test"}}"#,
            Some(DatabaseCategory::TimeSeries) => "",
            _ => "e.g. id > 10 AND name LIKE '%test%'",
        }
    }
}

impl EventEmitter<DataGridEvent> for DataGridPanel {}

#[cfg(test)]
mod tests {
    use super::{DataGridPanel, DataSource};
    use crate::app_state_entity::AppStateEntity;
    use crate::ui::components::toast::{ToastGlobal, ToastHost};
    use crate::ui::theme;
    use dbflux_core::{CollectionRef, ColumnKind, ColumnMeta, Pagination, QueryResult, TableRef};
    use dbflux_storage::bootstrap::StorageRuntime;
    use gpui::{AppContext, TestAppContext};
    use gpui_component::Root;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn isolated_test_app_state(cx: &mut TestAppContext) -> gpui::Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime =
                    StorageRuntime::in_memory().expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
            })
        })
    }

    fn zero_row_columns() -> Vec<ColumnMeta> {
        vec![
            ColumnMeta {
                name: "id".to_string(),
                type_name: "int4".to_string(),
                kind: ColumnKind::Unknown,
                nullable: false,
                is_primary_key: true,
            },
            ColumnMeta {
                name: "name".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Unknown,
                nullable: true,
                is_primary_key: false,
            },
        ]
    }

    fn zero_row_result() -> QueryResult {
        QueryResult::table(zero_row_columns(), Vec::new(), None, Duration::ZERO)
    }

    fn init_test_runtime(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });
    }

    #[test]
    fn table_source_accessors_match_expected_values() {
        let table = TableRef::with_schema("public", "users");
        let pagination = Pagination::Offset {
            limit: 25,
            offset: 50,
        };

        let source = DataSource::Table {
            profile_id: Uuid::new_v4(),
            database: Some("app".to_string()),
            table: table.clone(),
            pagination: pagination.clone(),
            order_by: Vec::new(),
            total_rows: Some(123),
        };

        assert!(source.is_table());
        assert!(!source.is_collection());
        assert!(source.is_paginated());
        assert_eq!(source.database(), Some("app"));
        assert_eq!(source.table_ref(), Some(&table));
        assert_eq!(source.collection_ref(), None);
        assert_eq!(source.pagination(), Some(&pagination));
        assert_eq!(source.total_rows(), Some(123));
    }

    #[test]
    fn collection_source_accessors_match_expected_values() {
        let collection = CollectionRef::new("app", "users");
        let pagination = Pagination::Offset {
            limit: 10,
            offset: 0,
        };

        let source = DataSource::Collection {
            profile_id: Uuid::new_v4(),
            collection: collection.clone(),
            pagination: pagination.clone(),
            total_docs: Some(17),
        };

        assert!(!source.is_table());
        assert!(source.is_collection());
        assert!(source.is_paginated());
        assert_eq!(source.database(), None);
        assert_eq!(source.table_ref(), None);
        assert_eq!(source.collection_ref(), Some(&collection));
        assert_eq!(source.pagination(), Some(&pagination));
        assert_eq!(source.total_rows(), Some(17));
    }

    #[test]
    fn query_result_source_accessors_match_expected_values() {
        let source = DataSource::QueryResult {
            result: Arc::new(QueryResult::text(
                "ok".to_string(),
                std::time::Duration::ZERO,
            )),
            original_query: "PING".to_string(),
            profile_id: None,
        };

        assert!(!source.is_table());
        assert!(!source.is_collection());
        assert!(!source.is_paginated());
        assert_eq!(source.database(), None);
        assert_eq!(source.table_ref(), None);
        assert_eq!(source.collection_ref(), None);
        assert_eq!(source.pagination(), None);
        assert_eq!(source.total_rows(), None);
    }

    #[gpui::test]
    fn filtered_empty_table_runtime_keeps_header_and_active_filter(cx: &mut TestAppContext) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let panel_holder = Rc::new(RefCell::new(None));
        let panel_handle = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id: Uuid::nil(),
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "users"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(0),
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state.clone(),
                    vec!["id".to_string()],
                    window,
                    cx,
                );

                panel.set_result(zero_row_result(), cx);
                panel.filter_input.update(cx, |input, cx| {
                    input.set_value("id = 999", window, cx);
                });

                panel
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let (filter_value, has_table, row_count, col_count) = window.update(|_, app| {
            let panel = panel.read(app);
            let table_state = panel
                .table_state
                .as_ref()
                .expect("filtered empty table should still build table state");
            let table_state = table_state.read(app);

            (
                panel.filter_input.read(app).value().to_string(),
                panel.data_table.is_some(),
                table_state.row_count(),
                table_state.col_count(),
            )
        });

        assert_eq!(filter_value, "id = 999");
        assert!(
            has_table,
            "filtered empty table should keep table content active"
        );
        assert_eq!(
            row_count, 0,
            "filtered empty table should remain visually empty"
        );
        assert_eq!(col_count, 2, "filtered empty table should keep its headers");
    }

    #[gpui::test]
    fn successful_insert_refresh_runtime_keeps_filter_and_can_stay_visually_empty(
        cx: &mut TestAppContext,
    ) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let panel_holder = Rc::new(RefCell::new(None));
        let panel_handle = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id: Uuid::nil(),
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "users"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(0),
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state.clone(),
                    vec!["id".to_string()],
                    window,
                    cx,
                );

                panel.set_result(zero_row_result(), cx);
                panel.filter_input.update(cx, |input, cx| {
                    input.set_value("id = 999", window, cx);
                });

                panel
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let refresh_was_queued = window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.handle_add_row(0, false, cx);
                panel.queue_refresh_after_mutation_success(cx);
                let refresh_was_queued = panel.pending_refresh;
                panel.set_result(zero_row_result(), cx);
                refresh_was_queued
            })
        });

        let (filter_value, pending_inserts) = window.update(|_, app| {
            let panel = panel.read(app);
            let pending_inserts = panel
                .table_state
                .as_ref()
                .map(|state| state.read(app).edit_buffer().pending_insert_rows().len())
                .unwrap_or_default();

            (
                panel.filter_input.read(app).value().to_string(),
                pending_inserts,
            )
        });

        assert_eq!(filter_value, "id = 999");
        assert!(
            refresh_was_queued,
            "successful insert refresh should be queued"
        );
        assert_eq!(
            pending_inserts, 0,
            "refresh result should clear the staged insert row"
        );

        let (row_count, col_count, has_table) = window.update(|_, app| {
            let panel = panel.read(app);
            let table_state = panel
                .table_state
                .as_ref()
                .expect("post-refresh filtered result should still build table state");
            let table_state = table_state.read(app);

            (
                table_state.row_count(),
                table_state.col_count(),
                panel.data_table.is_some(),
            )
        });

        assert!(
            has_table,
            "successful insert refresh should keep table mode active"
        );
        assert_eq!(row_count, 0, "filtered refresh may still be visually empty");
        assert_eq!(col_count, 2, "filtered refresh should keep headers visible");
    }

    #[gpui::test]
    fn pending_edit_counts_empty_buffer_returns_zeros(cx: &mut TestAppContext) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let panel_holder = Rc::new(RefCell::new(None));
        let panel_handle = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id: Uuid::nil(),
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "users"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(0),
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state.clone(),
                    vec!["id".to_string()],
                    window,
                    cx,
                );

                panel.set_result(zero_row_result(), cx);
                panel
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let counts = window.update(|_, app| panel.read(app).pending_edit_counts(app));

        assert_eq!(
            counts,
            (0, 0, 0),
            "fresh panel should have no pending changes"
        );
    }

    #[gpui::test]
    fn pending_edit_counts_only_inserts(cx: &mut TestAppContext) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let panel_holder = Rc::new(RefCell::new(None));
        let panel_handle = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id: Uuid::nil(),
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "users"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(0),
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state.clone(),
                    vec!["id".to_string()],
                    window,
                    cx,
                );

                panel.set_result(zero_row_result(), cx);
                panel
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.handle_add_row(0, false, cx);
            });
        });

        let counts = window.update(|_, app| panel.read(app).pending_edit_counts(app));

        assert_eq!(counts.0, 1, "should have 1 pending insert");
        assert_eq!(counts.1, 0, "should have 0 pending updates");
        assert_eq!(counts.2, 0, "should have 0 pending deletes");
    }
}
