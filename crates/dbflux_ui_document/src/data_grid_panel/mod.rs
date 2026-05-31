mod context_menu;
mod mutations;
mod navigation;
mod query;
mod render;
pub mod row_inspector;
mod utils;

use super::query_builder::{BuilderEvent, QueryBuilderPanel};
use super::result_view::{
    ResultViewMode, default_bindings_for_time_series, should_auto_select_chart_for_time_series,
};
use super::task_runner::DocumentTaskRunner;
use dbflux_components::SqlPreviewContext;
use dbflux_components::chart::{
    ChartDetection, ChartView, DataPointRef, SourceRowRef, detect_chart_columns,
};
use dbflux_components::components::data_table::{
    ContextMenuAction, DataTable, DataTableEvent, DataTableState, SortState as TableSortState,
    TableModel,
};
use dbflux_components::components::document_tree::{
    DocumentTree, DocumentTreeEvent, DocumentTreeState,
};
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::modals::cell_editor::{
    CellEditorClosedEvent, CellEditorModal, CellEditorSaveEvent,
};
use dbflux_components::modals::document_preview::{
    DocumentPreviewClosedEvent, DocumentPreviewModal, DocumentPreviewSaveEvent,
};
use dbflux_core::{
    CollectionRef, DatabaseCategory, OrderByColumn, Pagination, QueryResult, RefreshPolicy,
    SelectQuery, SortDirection, TableRef, Value, VisualQuerySpec,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::AsyncUpdateResultExt;
use dbflux_ui_base::toast::PendingToast;
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
    /// A row-level action (e.g. kill/cancel) was requested for a row.
    ///
    /// Emitted instead of the normal context menu when the panel has a
    /// `row_action_provider` that returns at least one action for the
    /// clicked row.
    RowActionRequested {
        row: usize,
        action_id: String,
        action_label: String,
        is_destructive: bool,
        row_values: Vec<Value>,
        position: Point<Pixels>,
    },
    /// Request to hide the results panel.
    RequestHide,
    /// Request to maximize/restore the results panel.
    RequestToggleMaximize,
    /// The data grid received focus (user clicked on it).
    Focused,
    /// Request to show SQL preview modal.
    RequestSqlPreview {
        context: Box<SqlPreviewContext>,
        generation_type: dbflux_components::SqlGenerationType,
    },
    /// Request to mount arbitrary content into the workspace-level inspector rail.
    OpenInspector {
        title: SharedString,
        content: AnyView,
    },
    /// Request to hide the workspace inspector rail without losing the
    /// panel's cached inspector state (e.g. when switching to another tab).
    CloseInspector,
    /// User requested "Chart this query" from the context menu.
    ChartThisQuery {
        query: String,
        connection_id: Option<Uuid>,
    },
    /// The grid reset its refresh policy internally (e.g. when a new query
    /// result arrives, the policy resets to Manual). The container document
    /// should sync the `ResultPanel`'s dropdown to reflect this.
    RefreshPolicyReset(RefreshPolicy),

    /// The `QueryBuilderPanel` produced an updated spec; the grid should store
    /// it and, on the next Run, re-execute via `generate_select`.
    ///
    /// Boxed because `VisualQuerySpec` is large (>256 bytes).
    ApplyVisualQuery(Box<VisualQuerySpec>),

    /// The builder was reset; restore raw-filter-input chrome and clear the
    /// stored spec so the next query falls back to `TableBrowseRequest`.
    ClearVisualQuery,

    /// The user pressed "Open in Editor" from the builder panel.
    ///
    /// Carries the profile the query should run against and the fully
    /// materialized SQL (literals inlined, no placeholders).
    OpenEditorWithContent { profile_id: Uuid, sql: String },
}

// Re-export the rail tab enum from the chart module so DataGridPanel's render
// code can reference it without a long path.
pub(super) use crate::chart::shell::ChartRailTab;

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
    doc_field_value: Option<dbflux_components::components::document_tree::NodeValue>,
    /// Driver-supplied row-level actions (e.g. Kill, Cancel). When non-empty,
    /// these appear at the bottom of the menu after a separator. Selecting one
    /// emits `DataGridEvent::RowActionRequested`.
    row_actions: Vec<dbflux_core::InspectorRowAction>,
}

/// A single item in the context menu.
struct ContextMenuItem {
    label: &'static str,
    action: Option<ContextMenuAction>,
    icon: Option<dbflux_components::icons::AppIcon>,
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

/// Callback type for providing row-level inspector actions (e.g. kill/cancel).
type RowActionProvider = Arc<dyn Fn(&str) -> Vec<dbflux_core::InspectorRowAction> + Send + Sync>;

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
    /// Refresh-policy dropdown, created at construction time.
    ///
    /// Rendered in the filter bar segment (as the chevron half of the split
    /// button) and also in the chart toolbar. The dropdown's change events are
    /// handled internally via a subscription set up in `new_internal`.
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

    /// Last `(row, col)` opened in the row inspector. `Some` means the inspector
    /// is logically "on" for this panel — it should reappear when the panel's
    /// tab is re-activated, follow the user's cursor on `SelectionChanged`, and
    /// re-snapshot itself after a refresh. Cleared when the user dismisses the
    /// rail explicitly (via [`DataGridPanel::clear_inspector_state`]) or when
    /// the stored row falls outside the new result.
    inspector_row: Option<(usize, usize)>,

    export_menu_open: bool,

    /// Optional provider for row-level kill/cancel actions.
    ///
    /// When set, right-clicking a row emits `DataGridEvent::RowActionRequested`
    /// for the first destructive action the provider returns, instead of opening
    /// the normal context menu. Used by `InspectorPanel` to offer kill actions.
    row_action_provider: Option<RowActionProvider>,

    /// When `true`, the filter/limit/refresh-button toolbar row is suppressed
    /// from `DataGridPanel::render` because it has been moved into the hosting
    /// `ResultPanel`'s chrome row as a `Center` toolbar segment via `ViewHandle`.
    ///
    /// Set by `DataGridPanel::into_view_handle` after the `ViewHandle` is
    /// built. Defaults to `false` (grid renders its own toolbar).
    toolbar_in_chrome_row: bool,

    // Chart subsystem
    /// Lazily-created chart shell entity. Created the first time the result
    /// passes chart detection (or when the user is already in chart mode).
    /// `None` for sources that have never produced a chartable result.
    chart_shell: Option<Entity<crate::chart::ChartShell>>,

    /// Time-range panel from the source-context bar, set by CodeDocument after
    /// the panel is built. Used by the chart toolbar RANGE chips to read/write
    /// the active preset. `None` for non-TimeSeries sources or before the panel
    /// has been created.
    chart_source_time_range_panel:
        Option<Entity<dbflux_components::common::time_range::view::TimeRangePanel>>,

    /// Pending "Save chart from collection" state.
    ///
    /// Present when the user clicked "Save chart" from a Collection-source
    /// DataDocument in chart mode. Holds the input state for the name prompt
    /// overlay. On confirm, the chart is upserted via `app_state.saved_charts`.
    pub(super) pending_collection_chart_save: Option<CollectionChartSaveState>,

    // ---- Visual Query Builder state ----
    /// The spec currently being edited in the `QueryBuilderPanel`.
    ///
    /// Updated on every `SpecChanged` event (i.e. every builder edit). When
    /// `Some`, `run_table_query` delegates to `generate_select` instead of
    /// `TableBrowseRequest`. The name makes clear this is the in-flight draft,
    /// not the last-committed (Run) spec.
    pub(crate) builder_draft_spec: Option<VisualQuerySpec>,

    /// Pre-computed `SelectQuery` for the current `builder_draft_spec`.
    ///
    /// Stored so the query path does not need to re-generate every refresh.
    /// Cleared whenever `builder_draft_spec` changes.
    pub(crate) visual_select: Option<SelectQuery>,

    /// The builder panel entity; kept alive here so inspector close/re-open
    /// preserves state across sessions.
    pub(crate) builder_panel: Option<Entity<QueryBuilderPanel>>,

    /// Subscriptions to `QueryBuilderPanel` events.
    pub(crate) _builder_subscriptions: Vec<Subscription>,

    /// When `true`, the raw filter input row is hidden in the toolbar because
    /// the builder is open and owns query composition for this panel.
    pub(crate) filter_input_hidden: bool,
}

/// State held while the "Save chart" name-prompt overlay is visible for a
/// Collection-source DataDocument.
pub(super) struct CollectionChartSaveState {
    pub(super) name_input: Entity<dbflux_components::controls::InputState>,
    pub(super) chart_spec: dbflux_components::chart::ChartSpec,
    pub(super) bindings: dbflux_components::chart::BindingSpec,
    pub(super) _subscription: gpui::Subscription,
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

        let connection_id = match &source {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::Collection { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { .. } => None,
        };

        let default_refresh = app_state
            .read(cx)
            .effective_settings_for_connection(connection_id)
            .resolve_refresh_policy();

        let supports_auto_refresh = matches!(
            source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        );

        let refresh_dropdown = cx.new(|_cx| {
            let items: Vec<DropdownItem> = RefreshPolicy::ALL
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
                    dbflux_ui_base::toast::Toast::warning(
                        "Auto-refresh not available for query results",
                    )
                    .meta_right(dbflux_ui_base::toast::now_hms())
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
            inspector_row: None,
            export_menu_open: false,
            row_action_provider: None,
            toolbar_in_chrome_row: false,
            chart_shell: None,
            chart_source_time_range_panel: None,
            pending_collection_chart_save: None,
            builder_draft_spec: None,
            visual_select: None,
            builder_panel: None,
            _builder_subscriptions: Vec::new(),
            filter_input_hidden: false,
        }
    }

    /// Attach a row-action provider to this panel.
    ///
    /// When set, right-clicking a row emits `DataGridEvent::RowActionRequested`
    /// for the first action returned by the provider, instead of the normal
    /// context menu. Pass `metric_id` as the key; the provider returns the list
    /// of actions from `InstanceCatalog::row_actions`.
    pub fn set_row_action_provider(&mut self, provider: RowActionProvider) {
        self.row_action_provider = Some(provider);
    }

    /// Returns the metric_id embedded in the `QueryResult` source string, or
    /// `None` for table/collection sources.
    ///
    /// `DataGridPanel::new_for_result` stores the metric_id in `original_query`
    /// when created by `InspectorPanel`. That field is reused here as the key
    /// forwarded to the row-action provider.
    fn row_action_metric_id(&self) -> Option<String> {
        match &self.source {
            DataSource::QueryResult { original_query, .. } => Some(original_query.clone()),
            _ => None,
        }
    }

    /// Collects all cell values for `visual_row` from the current result.
    ///
    /// Returns an empty `Vec` when the row index is out of bounds or no
    /// `table_state` exists.
    fn collect_row_values(&self, visual_row: usize, cx: &App) -> Vec<Value> {
        use dbflux_components::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return Vec::new();
        };

        let ts = table_state.read(cx);
        let buffer = ts.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let base_row = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(idx)) => self.result.rows.get(idx),
            _ => None,
        };

        base_row.cloned().unwrap_or_default()
    }

    /// Enable panel control buttons (hide, maximize) for embedded panels.
    #[allow(dead_code)]
    pub fn with_panel_controls(mut self) -> Self {
        self.show_panel_controls = true;
        self
    }

    // ---- Collection chart save flow ----

    /// Open the name-prompt overlay for saving a chart from a Collection or
    /// QueryResult source.
    ///
    /// Captures the current chart spec and bindings from the shell. No-op when
    /// no chart shell exists or the source has no associated profile.
    pub fn open_collection_chart_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(
            &self.source,
            DataSource::Collection { .. } | DataSource::QueryResult { .. }
        ) {
            return;
        }

        let Some(shell) = &self.chart_shell else {
            return;
        };

        let columns = self.result.columns.clone();
        let spec = shell.read(cx).current_chart_spec(&columns);
        let bindings = shell.read(cx).active_bindings();

        let name_input = cx.new(|cx| {
            dbflux_components::controls::InputState::new(window, cx).placeholder("Chart name")
        });

        let sub = cx.subscribe_in(
            &name_input,
            window,
            |_this: &mut Self,
             _input: &Entity<dbflux_components::controls::InputState>,
             _event: &dbflux_components::controls::InputEvent,
             _window,
             _cx| {},
        );

        self.pending_collection_chart_save = Some(CollectionChartSaveState {
            name_input,
            chart_spec: spec,
            bindings,
            _subscription: sub,
        });

        cx.notify();
    }

    /// Confirm the collection-chart name prompt and persist the chart.
    pub fn confirm_collection_chart_save(&mut self, cx: &mut Context<Self>) {
        let Some(state) = self.pending_collection_chart_save.take() else {
            return;
        };

        let name = state.name_input.read(cx).value().trim().to_string();
        if name.is_empty() {
            // Put it back — user must enter a name.
            self.pending_collection_chart_save = Some(state);
            return;
        }

        let chart = match &self.source {
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                let time_window = self.result.resolved_window.clone();
                dbflux_components::saved_chart::SavedChart::new_collection(
                    name.clone(),
                    *profile_id,
                    collection.clone(),
                    time_window,
                    state.chart_spec,
                    state.bindings,
                )
            }
            DataSource::QueryResult {
                profile_id,
                original_query,
                ..
            } => {
                let Some(profile_id) = profile_id else {
                    self.pending_toast = Some(dbflux_ui_base::toast::PendingToast {
                        message: "Cannot save chart: query has no profile binding".into(),
                        is_error: true,
                    });
                    cx.notify();
                    return;
                };
                dbflux_components::saved_chart::SavedChart::new_query(
                    name.clone(),
                    *profile_id,
                    original_query.clone(),
                    state.chart_spec,
                    state.bindings,
                )
            }
            _ => return,
        };

        let chart_id = chart.id;
        let persist_result = self.app_state.update(cx, |app, _cx| {
            app.saved_charts.upsert(chart).inspect_err(|e| {
                app.record_storage_failure(
                    dbflux_core::observability::actions::CONFIG_CREATE,
                    "saved_chart",
                    chart_id.to_string(),
                    format!("Failed to save chart '{name}'"),
                    e.to_string(),
                );
            })
        });

        self.pending_toast = Some(match persist_result {
            Ok(_) => dbflux_ui_base::toast::PendingToast {
                message: format!("Chart \"{}\" saved", name),
                is_error: false,
            },
            Err(e) => dbflux_ui_base::toast::PendingToast {
                message: format!("Failed to save chart \"{name}\": {e}"),
                is_error: true,
            },
        });

        cx.notify();
    }

    /// Cancel the collection-chart name prompt without saving.
    pub fn cancel_collection_chart_save(&mut self, cx: &mut Context<Self>) {
        self.pending_collection_chart_save = None;
        cx.notify();
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

    pub fn result_view_mode(&self) -> ResultViewMode {
        self.result_view_mode
    }

    /// The mode currently displayed in the result view. Alias of
    /// `result_view_mode` used by `ResultPanel` wiring in `DataDocument`.
    pub fn current_result_view_mode(&self) -> ResultViewMode {
        self.result_view_mode
    }

    /// Modes available for the current result shape and connection category.
    ///
    /// Returns an empty slice for non-QueryResult sources (table/collection
    /// browses have no alternative views). For QueryResult sources, returns
    /// the modes available for the shape, plus Chart when chart detection
    /// succeeded. Independent of the currently active mode — switching to
    /// Chart and back must not change which modes are offered.
    pub fn available_result_view_modes(&self, cx: &App) -> Vec<ResultViewMode> {
        if !matches!(self.source, DataSource::QueryResult { .. }) {
            return vec![];
        }

        let mut modes = ResultViewMode::available_for_shape(&self.result.shape);

        if self.chart_available(cx) && !modes.contains(&ResultViewMode::Chart) {
            // Insert Chart after Table when chart detection succeeded.
            if let Some(pos) = modes.iter().position(|m| *m == ResultViewMode::Table) {
                modes.insert(pos + 1, ResultViewMode::Chart);
            } else {
                modes.insert(0, ResultViewMode::Chart);
            }
        }

        modes
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
    pub(super) fn chart_available(&self, cx: &App) -> bool {
        self.chart_shell
            .as_ref()
            .is_some_and(|s| s.read(cx).chart_available())
    }

    /// Build or return the existing `ChartView` entity for the current result.
    ///
    /// Delegates to `ChartShell::ensure_chart_view`. Returns `None` when no
    /// shell exists or when detection failed.
    pub(super) fn ensure_chart_view(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Option<Entity<ChartView>> {
        let result = self.result.clone();
        self.chart_shell
            .as_ref()?
            .update(cx, |shell, cx| shell.ensure_chart_view(&result, cx))
    }

    /// Toggle the hidden state of a series by index.
    ///
    /// Delegates to `ChartShell::toggle_chart_series_hidden`.
    pub(super) fn toggle_chart_series_hidden(&mut self, idx: usize, cx: &mut Context<Self>) {
        if let Some(shell) = &self.chart_shell {
            shell.update(cx, |s, cx| s.toggle_chart_series_hidden(idx, cx));
        }
    }

    /// Wire the source-context time-range panel into this chart panel.
    ///
    /// Called by `CodeDocument` after it lazily creates the `TimeRangePanel`.
    /// The chart toolbar reads and writes the panel to drive RANGE chip selection.
    pub fn set_chart_time_range_panel(
        &mut self,
        panel: Option<Entity<dbflux_components::common::time_range::view::TimeRangePanel>>,
        cx: &mut Context<Self>,
    ) {
        self.chart_source_time_range_panel = panel;

        let enabled = self.supports_auto_refresh();
        self.refresh_dropdown.update(cx, |dd, cx| {
            dd.set_disabled(!enabled, cx);
        });

        cx.notify();
    }

    /// Prime the rail Configure picker from the current chart spec.
    ///
    /// Called when the rail is toggled open so the controls reflect what is
    /// currently rendered (either auto-detected or manual).
    ///
    /// Only invoked from the (now-dead) Configure rail tab.
    #[allow(dead_code)]
    pub(super) fn prime_chart_rail_picker_from_spec(&mut self, cx: &mut Context<Self>) {
        let result = self.result.clone();
        if let Some(shell) = &self.chart_shell {
            shell.update(cx, |s, _cx| s.prime_rail_picker_from_spec(&result));
        }
    }

    /// Apply the current rail Configure picker state as a `ManualChartSelection`.
    ///
    /// Clears the existing `chart_view` so the next render triggers a rebuild.
    /// Only invoked from the (now-dead) Configure rail tab.
    #[allow(dead_code)]
    pub(super) fn apply_chart_rail_selection(&mut self, cx: &mut Context<Self>) {
        let result = self.result.clone();
        if let Some(shell) = &self.chart_shell {
            shell.update(cx, |s, cx| s.apply_rail_selection(&result, cx));
        }
    }

    /// Reset chart selection to auto-detection, clearing any manual override.
    ///
    /// Disabled (no-op) when detection did not produce an `Ok` result.
    /// Only invoked from the (now-dead) Configure rail tab.
    #[allow(dead_code)]
    pub(super) fn reset_chart_rail_to_auto(&mut self, cx: &mut Context<Self>) {
        let result = self.result.clone();
        if let Some(shell) = &self.chart_shell {
            shell.update(cx, |s, cx| s.reset_rail_to_auto(&result, cx));
        }
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
        ) || matches!(self.source, DataSource::QueryResult { .. })
            && self.chart_source_time_range_panel.is_some()
    }

    pub fn set_active_tab(&mut self, active: bool, cx: &mut Context<Self>) {
        self.is_active_tab = active;

        if active {
            // Re-mount the inspector rail with whichever per-tab content was
            // previously open. Builder takes precedence over the row inspector
            // because both share the same rail and the builder is the more
            // recent intentional surface for the user.
            if let Some(panel) = self.builder_panel.clone() {
                let view: AnyView = AnyView::from(panel);
                cx.emit(DataGridEvent::OpenInspector {
                    title: "Query Builder".into(),
                    content: view,
                });
            } else if let Some((row, col)) = self.inspector_row {
                self.open_row_inspector(row, col, cx);
            }
        } else if self.builder_panel.is_some() || self.inspector_row.is_some() {
            // Hide the rail (without dropping cached state) so the next
            // active tab can take it over.
            cx.emit(DataGridEvent::CloseInspector);
        }
    }

    /// Called by the workspace when the user dismisses the inspector rail
    /// explicitly (× button or ESC fallback). Drops the cached coordinates so
    /// the rail does not re-open on tab activation or refresh.
    pub fn clear_inspector_state(&mut self, _cx: &mut Context<Self>) {
        self.inspector_row = None;
        self.row_inspector_content = None;
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

                        if matches!(panel.source, DataSource::QueryResult { .. }) {
                            if let Some(trp) = panel.chart_source_time_range_panel.clone() {
                                trp.update(cx, |p, cx| p.emit_initial(cx));
                            }
                        } else {
                            panel.pending_refresh = true;
                            cx.notify();
                        }
                    });
                });
            }
        }));
    }

    /// Update the result data (for QueryResult source or after table fetch).
    pub fn set_result(&mut self, result: QueryResult, cx: &mut Context<Self>) {
        let was_chart_mode = matches!(self.result_view_mode, ResultViewMode::Chart);

        self.view_config = super::data_view::DataViewConfig::for_source(&self.source);
        self.derived_json = None;
        self.derived_text = None;

        let detection = detect_chart_columns(&result);
        let detection_ok = matches!(detection, ChartDetection::Ok { .. });

        // Auto-select Chart for TimeSeries Collection sources: fires on every fresh
        // result when detection passes, regardless of previous mode. Non-TimeSeries
        // and non-Collection sources follow the existing was_chart_mode preservation path.
        let is_time_series_collection = matches!(self.source, DataSource::Collection { .. })
            && Self::connection_category(&self.source, &self.app_state, cx)
                == Some(DatabaseCategory::TimeSeries);

        let auto_chart = (is_time_series_collection
            && should_auto_select_chart_for_time_series(&detection))
            || (was_chart_mode && detection_ok);

        self.result_view_mode = if auto_chart {
            ResultViewMode::Chart
        } else {
            ResultViewMode::default_for_shape(&result.shape)
        };

        // Update or create the chart shell for this result.
        if detection_ok || self.chart_shell.is_some() {
            if let Some(shell) = &self.chart_shell {
                let was_chart = was_chart_mode;
                shell.update(cx, |s, cx| s.set_result(&result, was_chart, cx));
            } else {
                // Create the shell for the first chartable result.
                let host = crate::chart::HostAdapter::DataGrid(cx.entity().clone());
                let shell = cx.new(|cx| {
                    let mut shell = crate::chart::ChartShell::new(host, cx);
                    shell.set_result(&result, false, cx);
                    shell
                });
                self.chart_shell = Some(shell);
            }

            // Pre-populate bindings for the first TimeSeries Collection result so the
            // AxisBar shows sensible defaults (time, first numeric, first Text tag).
            // Only applied on the initial load (!was_chart_mode) to avoid clobbering
            // user adjustments made during a refresh.
            if is_time_series_collection
                && !was_chart_mode
                && let ChartDetection::Ok {
                    time_col,
                    ref numeric_cols,
                } = detection
            {
                let bindings =
                    default_bindings_for_time_series(time_col, numeric_cols, &result.columns);
                if let Some(shell) = &self.chart_shell {
                    shell.update(cx, |s, cx| s.apply_bindings(bindings, cx));
                }
            }
        }

        self.result = result;
        self.rebuild_table(None, cx);
        self.state = GridState::Ready;

        // Re-snapshot the row inspector against the fresh data so the rail
        // keeps following the same row position across refreshes.
        if let Some((row, col)) = self.inspector_row {
            self.open_row_inspector(row, col, cx);
        }

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

        cx.emit(DataGridEvent::RefreshPolicyReset(RefreshPolicy::Manual));

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
                    DataTableEvent::SelectionChanged(selection) => {
                        // When the row inspector is active, follow the user's
                        // cursor so click / arrow-key navigation updates the
                        // rail in place.
                        if this.inspector_row.is_some()
                            && let Some(active) = selection.active
                        {
                            this.open_row_inspector(active.row, active.col, cx);
                        }
                    }
                    DataTableEvent::SaveRowRequested(row_idx) => {
                        this.handle_save_row(*row_idx, cx);
                    }
                    DataTableEvent::ContextMenuRequested { row, col, position } => {
                        // Gather any driver-supplied row actions (e.g. Kill, Cancel).
                        // They are injected as extra menu items at the bottom rather
                        // than bypassing the context menu entirely.
                        let row_actions = if let Some(provider) = this.row_action_provider.as_ref()
                        {
                            let metric_id = this.row_action_metric_id();
                            provider(metric_id.as_deref().unwrap_or(""))
                        } else {
                            Vec::new()
                        };

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
                            row_actions,
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
                        row_actions: Vec::new(),
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

    // ---- ChartHost delegation methods ----
    // These are called by `HostAdapter::DataGrid` to implement `ChartHost`
    // without requiring a mutable self-borrow in read contexts.

    /// Returns the original query text for the current `QueryResult` source.
    ///
    /// Returns `None` for `Table` and `Collection` sources that do not expose
    /// a user-authored query string.
    pub(crate) fn chart_host_current_query(&self, _cx: &App) -> Option<String> {
        match &self.source {
            DataSource::QueryResult { original_query, .. } => {
                if original_query.is_empty() {
                    None
                } else {
                    Some(original_query.clone())
                }
            }
            _ => None,
        }
    }

    /// Returns the profile ID for the current source, if any.
    pub(crate) fn chart_host_connection_id(&self, _cx: &App) -> Option<Uuid> {
        match &self.source {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::Collection { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { profile_id, .. } => *profile_id,
        }
    }

    /// Returns the time-range panel wired in by the parent document.
    pub(crate) fn chart_host_time_range_panel(
        &self,
        _cx: &App,
    ) -> Option<Entity<dbflux_components::common::time_range::view::TimeRangePanel>> {
        self.chart_source_time_range_panel.clone()
    }

    /// Returns the refresh-policy dropdown entity.
    ///
    /// The dropdown is created at construction time and lives here for the
    /// panel's lifetime. The chart toolbar uses it so the user can change the
    /// policy while viewing a chart.
    pub(crate) fn chart_host_refresh_dropdown(&self, _cx: &App) -> Option<Entity<Dropdown>> {
        Some(self.refresh_dropdown.clone())
    }

    /// Returns the current result as a shared `Arc<QueryResult>`.
    ///
    /// For `QueryResult` sources the result is already `Arc`-wrapped in the
    /// source; for other sources we wrap the live `result` field in a new
    /// `Arc` (shallow clone, no data copy).
    pub(crate) fn chart_host_current_result(&self, _cx: &App) -> Option<Arc<QueryResult>> {
        match &self.source {
            DataSource::QueryResult { result, .. } => Some(result.clone()),
            DataSource::Table { .. } | DataSource::Collection { .. } => {
                Some(Arc::new(self.result.clone()))
            }
        }
    }

    /// Trigger a re-execution of the current query.
    ///
    /// For `QueryResult` sources this emits the time-range panel's initial
    /// event, which causes `CodeDocument` to re-run the query. For table /
    /// collection sources this calls `refresh`.
    pub(crate) fn chart_host_request_reexecute(&mut self, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::QueryResult { .. } => {
                if let Some(trp) = self.chart_source_time_range_panel.clone() {
                    trp.update(cx, |p, cx| p.emit_initial(cx));
                }
            }
            _ => {
                self.pending_refresh = true;
                cx.notify();
            }
        }
    }

    /// Look up the source row for a decimated chart point.
    ///
    /// Consults the `RenderModel.source_indices` built by `ChartView::build`
    /// when `ChartSpec.track_source_indices` was enabled. Returns `None` when
    /// source tracking is disabled (e.g. CodeDocument-backed charts) or when
    /// the index is out of range.
    pub(crate) fn chart_host_source_for_point(
        &self,
        point: DataPointRef,
        cx: &App,
    ) -> Option<SourceRowRef> {
        let shell = self.chart_shell.as_ref()?.read(cx);
        let chart_entity = shell.chart_view()?.clone();
        let chart = chart_entity.read(cx);

        let src_indices = chart.source_indices()?;
        let series_indices = src_indices.get(point.series_idx)?;
        let row_idx = *series_indices.get(point.point_idx_in_series)?;

        Some(SourceRowRef { row_idx })
    }

    /// Scroll the underlying table view to the given row index.
    ///
    /// Uses `DataTableState::scroll_to_row` when the table state is available.
    /// For document-tree sources this is a no-op (document tree manages its own
    /// scroll via `DocumentTreeState`).
    pub(crate) fn chart_host_scroll_to_row(&self, row_idx: usize, cx: &App) {
        if let Some(table_state) = &self.table_state {
            table_state.read(cx).scroll_to_row(row_idx);
        }
    }

    /// Build a `ViewHandle` that erases the concrete `DataGridPanel` type for
    /// use inside a `ResultPanel`.
    ///
    /// After calling this method, `self.toolbar_in_chrome_row` is set to `true`
    /// on the entity, which suppresses `DataGridPanel::render`'s own toolbar row.
    /// The filter bar is instead exposed as a `Center/0` toolbar segment in the
    /// returned `ViewHandle::toolbar_segments` closure.
    ///
    /// The returned `ViewHandle` captures a clone of `entity`. The entity must
    /// already exist (this is called from `DataDocument::new_with_grid` after
    /// `cx.new(|cx| DataGridPanel::new_for_table(...))`).
    pub fn into_view_handle(
        entity: Entity<Self>,
        cx: &mut App,
    ) -> dbflux_components::result_panel::ViewHandle {
        use dbflux_components::result_panel::{SegmentPosition, ToolbarSegment, ViewHandle};
        use render::render_filter_bar_as_segment;

        // Suppress the grid's own toolbar — it moves to the chrome row.
        entity.update(cx, |this, _| {
            this.toolbar_in_chrome_row = true;
        });

        let e_render = entity.clone();
        let e_focus_get = entity.clone();
        let e_focus_do = entity.clone();
        let e_segs = entity.clone();
        let e_modes = entity.clone();
        let e_current = entity.clone();
        let e_set_mode = entity.clone();

        ViewHandle::builder()
            .render(move |_window, _cx| {
                // Render via the GPUI AnyView path: entity.clone().into_any()
                // produces an AnyElement that delegates to DataGridPanel::render.
                AnyView::from(e_render.clone()).into_any()
            })
            .focus({
                move |window, cx| {
                    e_focus_do.update(cx, |grid, cx| {
                        grid.focus_table(window, cx);
                    });
                }
            })
            .focus_handle(move |cx| e_focus_get.read(cx).focus_handle.clone())
            .toolbar_segments(move |cx| {
                let is_table_or_collection = matches!(
                    e_segs.read(cx).source,
                    DataSource::Table { .. } | DataSource::Collection { .. }
                );

                if !is_table_or_collection {
                    return vec![];
                }

                let grid = e_segs.clone();
                vec![ToolbarSegment {
                    position: SegmentPosition::Center,
                    index: 0,
                    builder: Box::new(move |window, cx| {
                        render_filter_bar_as_segment(&grid, window, cx)
                    }),
                }]
            })
            .available_modes(move |cx| e_modes.read(cx).available_result_view_modes(cx))
            .current_mode(move |cx| e_current.read(cx).current_result_view_mode())
            .set_mode(move |mode, cx| {
                e_set_mode.update(cx, |grid, cx| grid.set_result_view_mode(mode, cx));
            })
            .build()
    }

    // ---- Visual Query Builder integration ----

    /// Stores the given spec and re-computes the cached `SelectQuery`.
    ///
    /// Called when the user presses Run inside the `QueryBuilderPanel`. Does
    /// NOT immediately execute the query; sets `pending_refresh = true` so the
    /// next render tick triggers `run_table_query`, which will find
    /// `visual_select` ready to use.
    pub fn apply_builder_draft_spec(&mut self, spec: VisualQuerySpec, cx: &mut Context<Self>) {
        let generator = self.connection_generator(cx);

        let select = generator.and_then(|qgen| qgen.generate_select(&spec).ok().flatten());

        self.builder_draft_spec = Some(spec);
        self.visual_select = select;
        self.filter_input_hidden = true;
        self.pending_refresh = true;

        cx.notify();
    }

    /// Clears the visual spec and restores the raw filter-input chrome.
    ///
    /// Called by the builder's Reset action. The next query falls back to
    /// the `TableBrowseRequest` path.
    pub fn clear_builder_draft_spec(&mut self, cx: &mut Context<Self>) {
        self.builder_draft_spec = None;
        self.visual_select = None;
        self.filter_input_hidden = false;

        cx.notify();
    }

    /// Returns whether the toolbar's "Open in Builder" button should be shown.
    ///
    /// True only for `DataSource::Table` sources on connections whose driver
    /// uses `QueryLanguage::Sql`.
    pub fn can_open_builder(&self, cx: &App) -> bool {
        if !matches!(self.source, DataSource::Table { .. }) {
            return false;
        }

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return false,
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.metadata().query_language == dbflux_core::QueryLanguage::Sql)
            .unwrap_or(false)
    }

    /// Opens (or re-opens) the `QueryBuilderPanel` inspector for this grid.
    ///
    /// Constructs the panel entity on first open, or re-hydrates it from
    /// `builder_draft_spec` when the inspector is opened again after being closed.
    pub fn open_query_builder(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (profile_id, database, table) = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } => (*profile_id, database.clone(), table.clone()),
            _ => return,
        };

        let source = dbflux_core::SourceTable {
            schema: table.schema.clone(),
            table: table.name.clone(),
            alias: table.name.clone(),
        };

        let initial_spec = self.builder_draft_spec.clone();

        let weak_self = cx.entity().downgrade();

        let connection_arc: Option<std::sync::Arc<dyn dbflux_core::Connection>> = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone());

        let generate_preview: Box<dyn Fn(&VisualQuerySpec) -> String + Send + Sync> =
            if let Some(conn) = connection_arc {
                Box::new(move |spec: &VisualQuerySpec| {
                    conn.query_generator()
                        .and_then(|qgen| qgen.generate_select(spec).ok().flatten())
                        .map(|q| q.sql)
                        .unwrap_or_default()
                })
            } else {
                Box::new(|_spec: &VisualQuerySpec| String::new())
            };

        let panel = if let Some(existing) = &self.builder_panel {
            existing.update(cx, |p, cx| {
                if let Some(spec) = initial_spec.clone() {
                    p.set_spec(spec, cx);
                }
            });
            existing.clone()
        } else {
            let new_panel = cx.new(|cx| {
                QueryBuilderPanel::new(
                    source,
                    initial_spec,
                    Some(weak_self.clone()),
                    generate_preview,
                    window,
                    cx,
                )
            });

            let run_sub = cx.subscribe_in(
                &new_panel,
                window,
                |this, _panel, event: &BuilderEvent, window, cx| {
                    this.handle_builder_event(event, window, cx);
                },
            );

            self._builder_subscriptions = vec![run_sub];
            self.builder_panel = Some(new_panel.clone());
            self.filter_input_hidden = true;
            new_panel
        };

        self.spawn_fk_fetch_for_builder(panel.clone(), profile_id, database, table.schema, cx);

        let view: AnyView = AnyView::from(panel);
        cx.emit(DataGridEvent::OpenInspector {
            title: "Query Builder".into(),
            content: view,
        });
    }

    /// Loads foreign-key metadata for the builder's source table on a
    /// background task, then applies it to the panel. If the connection is
    /// missing or the driver returns an error, the panel transitions to the
    /// `Unavailable` state so the raw-expression fallback banner appears.
    fn spawn_fk_fetch_for_builder(
        &self,
        panel: Entity<QueryBuilderPanel>,
        profile_id: uuid::Uuid,
        database: Option<String>,
        schema: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(database) = database else {
            panel.update(cx, |p, cx| p.mark_fk_unavailable(cx));
            return;
        };

        let Some(conn) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        else {
            panel.update(cx, |p, cx| p.mark_fk_unavailable(cx));
            return;
        };

        let schema_for_task = schema.clone();
        let task = cx
            .background_executor()
            .spawn(async move { conn.schema_foreign_keys(&database, schema_for_task.as_deref()) });

        let panel_weak = panel.downgrade();
        cx.spawn(async move |_this, cx| {
            let result = task.await;
            cx.update(|cx| {
                if let Some(panel) = panel_weak.upgrade() {
                    panel.update(cx, |p, cx| match result {
                        Ok(fks) => p.apply_fk_result(fks, cx),
                        Err(_) => p.mark_fk_unavailable(cx),
                    });
                }
            })
            .ok();
        })
        .detach();
    }

    /// Handles events emitted by the builder panel.
    fn handle_builder_event(
        &mut self,
        event: &BuilderEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            BuilderEvent::RunRequested => {
                if let Some(spec) = self.builder_draft_spec.clone().or_else(|| {
                    self.builder_panel
                        .as_ref()
                        .map(|p| p.read(cx).current_spec().clone())
                }) {
                    self.apply_builder_draft_spec(spec, cx);
                    self.refresh(window, cx);
                }
            }

            BuilderEvent::SpecChanged(spec) => {
                self.visual_select = self
                    .connection_generator(cx)
                    .and_then(|qgen| qgen.generate_select(spec).ok().flatten());
                self.builder_draft_spec = Some(*spec.clone());
            }

            BuilderEvent::ResetRequested => {
                self.clear_builder_draft_spec(cx);
                cx.emit(DataGridEvent::CloseInspector);
                self.builder_panel = None;
                self._builder_subscriptions.clear();
                self.refresh(window, cx);
            }

            BuilderEvent::OpenInEditorRequested => {
                self.open_builder_in_editor(cx);
            }

            BuilderEvent::SaveRequested { name } => {
                self.save_builder_query(name.clone(), cx);
            }

            BuilderEvent::SaveAsRequested { name } => {
                self.save_builder_query(name.clone(), cx);
            }

            BuilderEvent::ImportRequested { source_id } => {
                self.import_builder_query(source_id.clone(), cx);
            }
        }
    }

    /// Produces the editor-ready SQL by inlining literals into the parameterized
    /// query, then opens a new code editor tab with that SQL.
    fn open_builder_in_editor(&mut self, cx: &mut Context<Self>) {
        let Some(select) = &self.visual_select else {
            return;
        };

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let generator = self.connection_generator(cx);
        let sql = generator
            .map(|qgen| qgen.materialize_select_for_editor(select))
            .unwrap_or_else(|| select.sql.clone());

        cx.emit(DataGridEvent::OpenEditorWithContent { profile_id, sql });
    }

    /// Saves the current builder spec under `name` for the panel's profile.
    fn save_builder_query(&mut self, name: String, cx: &mut Context<Self>) {
        let Some(spec) = self.builder_draft_spec.clone() else {
            return;
        };

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => profile_id.to_string(),
            _ => return,
        };

        let result = self.app_state.update(cx, |app, _cx| {
            app.saved_queries.save(&profile_id, &name, &spec)
        });

        match result {
            Ok(summary) => {
                if let Some(panel) = &self.builder_panel {
                    panel.update(cx, |p, _| {
                        p.loaded_id = Some(summary.id);
                    });
                }
                dbflux_ui_base::toast::Toast::success(format!("Saved as \"{}\"", name))
                    .meta_right(dbflux_ui_base::toast::now_hms())
                    .push(cx);
            }
            Err(e) => {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::Storage,
                        format!("A saved query named \"{}\" already exists", name),
                    )
                    .with_cause(e.to_string()),
                    cx,
                );
            }
        }
    }

    /// Imports a saved query from another connection into this panel's profile.
    fn import_builder_query(&mut self, source_id: String, cx: &mut Context<Self>) {
        use dbflux_ui_base::saved_query_manager::ConnectionTableProbe;

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let profile_id_str = profile_id.to_string();

        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::User,
                        "Target connection not available",
                    ),
                    cx,
                );
                return;
            };
            connected.connection.clone()
        };

        let database = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|c| c.active_database.clone())
            .unwrap_or_default();

        let probe = ConnectionTableProbe::new(conn.as_ref(), &database);

        let result = self.app_state.update(cx, |app, _cx| {
            app.saved_queries
                .import_to(&source_id, &profile_id_str, &probe)
        });

        match result {
            Ok(_summary) => {
                dbflux_ui_base::toast::Toast::success("Query imported successfully")
                    .meta_right(dbflux_ui_base::toast::now_hms())
                    .push(cx);
            }
            Err(e) => {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::User,
                        "Import failed: source table not found on target connection",
                    )
                    .with_cause(e.to_string()),
                    cx,
                );
            }
        }
    }

    /// Returns a reference to the driver's `QueryGenerator`, if connected.
    fn connection_generator<'a>(&self, cx: &'a App) -> Option<&'a dyn dbflux_core::QueryGenerator> {
        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return None,
        };

        let state = self.app_state.read(cx);
        let connected = state.connections().get(&profile_id)?;

        connected.connection.query_generator()
    }
}

impl EventEmitter<DataGridEvent> for DataGridPanel {}

#[cfg(test)]
mod tests {
    use super::{DataGridPanel, DataSource};
    use dbflux_components::theme;
    use dbflux_core::{
        CollectionRef, ColumnKind, ColumnMeta, Pagination, Projection, QueryResult, SelectQuery,
        SourceTable, TableRef, VisualQuerySpec,
    };
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
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

    // P1 — Right-click always opens context menu; row actions appear in it

    #[test]
    fn context_menu_row_actions_field_stores_provider_actions() {
        use dbflux_core::InspectorRowAction;
        use std::sync::Arc;

        let actions = vec![
            InspectorRowAction {
                id: "kill".to_string(),
                label: "Kill Connection".to_string(),
                description: None,
                is_destructive: true,
            },
            InspectorRowAction {
                id: "cancel".to_string(),
                label: "Cancel Query".to_string(),
                description: None,
                is_destructive: false,
            },
        ];

        // Simulate what the ContextMenuRequested handler now does: call the
        // provider and store its actions in `row_actions`.
        let actions_clone = actions.clone();
        let provider: super::RowActionProvider = Arc::new(move |_metric_id| actions_clone.clone());
        let row_actions = provider("");

        assert_eq!(
            row_actions.len(),
            2,
            "both actions should be returned by the provider"
        );
        assert_eq!(row_actions[0].id, "kill");
        assert_eq!(row_actions[1].id, "cancel");
        assert!(
            row_actions[0].is_destructive,
            "kill action should be marked destructive"
        );
        assert!(
            !row_actions[1].is_destructive,
            "cancel action should not be marked destructive"
        );
    }

    #[test]
    fn context_menu_row_actions_keyboard_nav_index_range() {
        // Verify that the index range for row actions is calculated correctly.
        // With: base_count=1 (only Copy), no filter/order/gen_sql/copy_query,
        // and 2 row actions:
        //   idx 0: Copy
        //   idx 1: separator (row actions)
        //   idx 2: Kill Connection
        //   idx 3: Cancel Query
        // total_count = 1 + (1+2) = 4
        // row_actions_start = 1 (after_copy_query = 1)
        // Action at selected_index=2: action_idx = 2 - 1 - 1 = 0 → "kill"
        // Action at selected_index=3: action_idx = 3 - 1 - 1 = 1 → "cancel"

        let row_action_count = 2usize;
        let row_actions_start = 1usize; // after_copy_query when no optional sections
        let total_count = row_actions_start + 1 + row_action_count;

        assert_eq!(
            total_count, 4,
            "total_count should include separator + 2 actions"
        );

        // selected_index=2 maps to action_idx=0
        let selected = 2usize;
        let in_range =
            selected > row_actions_start && selected <= row_actions_start + row_action_count;
        assert!(in_range, "index 2 should be in the row action range");
        let action_idx = selected - row_actions_start - 1;
        assert_eq!(action_idx, 0, "index 2 → action slot 0");

        // selected_index=3 maps to action_idx=1
        let selected = 3usize;
        let in_range =
            selected > row_actions_start && selected <= row_actions_start + row_action_count;
        assert!(in_range, "index 3 should be in the row action range");
        let action_idx = selected - row_actions_start - 1;
        assert_eq!(action_idx, 1, "index 3 → action slot 1");

        // The separator itself (index 1) should not be in range
        let selected = 1usize;
        let in_range =
            selected > row_actions_start && selected <= row_actions_start + row_action_count;
        assert!(
            !in_range,
            "separator index should not be in the action range"
        );
    }

    fn make_test_spec() -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: Some("public".to_string()),
                table: "users".to_string(),
                alias: "users".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    #[gpui::test]
    fn apply_builder_draft_spec_sets_filter_input_hidden(cx: &mut TestAppContext) {
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
                    total_rows: None,
                };

                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let spec = make_test_spec();

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                assert!(
                    !panel.filter_input_hidden,
                    "filter input should be visible before builder opens"
                );
                assert!(
                    panel.builder_draft_spec.is_none(),
                    "builder_draft_spec should be None before apply"
                );

                panel.apply_builder_draft_spec(spec.clone(), cx);

                assert!(
                    panel.filter_input_hidden,
                    "filter input should be hidden after apply_builder_draft_spec"
                );
                assert!(
                    panel.builder_draft_spec.is_some(),
                    "builder_draft_spec should be Some after apply"
                );
            });
        });
    }

    #[gpui::test]
    fn clear_builder_draft_spec_restores_filter_input_visible(cx: &mut TestAppContext) {
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
                    total_rows: None,
                };

                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let spec = make_test_spec();

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.apply_builder_draft_spec(spec.clone(), cx);

                assert!(panel.filter_input_hidden, "should be hidden after apply");
                assert!(panel.builder_draft_spec.is_some(), "spec should be stored");

                panel.clear_builder_draft_spec(cx);

                assert!(
                    !panel.filter_input_hidden,
                    "filter input should be visible again after clear"
                );
                assert!(
                    panel.builder_draft_spec.is_none(),
                    "builder_draft_spec should be None after clear"
                );
                assert!(
                    panel.visual_select.is_none(),
                    "visual_select should be None after clear"
                );
            });
        });
    }

    #[gpui::test]
    fn apply_builder_draft_spec_sets_pending_refresh(cx: &mut TestAppContext) {
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
                    total_rows: None,
                };

                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let spec = make_test_spec();

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.apply_builder_draft_spec(spec.clone(), cx);

                assert!(
                    panel.pending_refresh,
                    "apply_builder_draft_spec should queue a refresh"
                );
            });
        });
    }

    #[gpui::test]
    fn can_open_builder_false_for_collection_source(cx: &mut TestAppContext) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let panel_holder = Rc::new(RefCell::new(None));
        let panel_handle = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Collection {
                    profile_id: Uuid::nil(),
                    collection: CollectionRef::new("db", "items"),
                    pagination: Pagination::default(),
                    total_docs: None,
                };

                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let result = window.update(|_, app| panel.read(app).can_open_builder(app));

        assert!(
            !result,
            "can_open_builder should return false for Collection source"
        );
    }

    #[gpui::test]
    fn can_open_builder_true_for_sql_table_source(cx: &mut TestAppContext) {
        use dbflux_core::{
            ConnectedProfile, Connection, DatabaseCategory, DbConfig, DbError, DbKind,
            DriverCapabilities, DriverMetadata, Icon as CoreIcon, QueryLanguage,
            QueryResult as CoreQueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
        };
        use std::path::PathBuf;

        init_test_runtime(cx);

        struct StubSqlConnection;

        impl Connection for StubSqlConnection {
            fn metadata(&self) -> &DriverMetadata {
                // Safety: returning a reference to a static value so the lifetime is valid.
                static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
                META.get_or_init(|| DriverMetadata {
                    id: "stub-sql".to_string(),
                    display_name: "Stub SQL".to_string(),
                    description: "test stub".to_string(),
                    category: DatabaseCategory::Relational,
                    deployment_class: None,
                    query_language: QueryLanguage::Sql,
                    capabilities: DriverCapabilities::empty(),
                    default_port: None,
                    uri_scheme: "stub".to_string(),
                    icon: CoreIcon::Database,
                    syntax: None,
                    query: None,
                    mutation: None,
                    ddl: None,
                    transactions: None,
                    limits: None,
                    ssl_modes: None,
                    ssl_cert_fields: None,
                    classification_override: None,
                })
            }

            fn kind(&self) -> DbKind {
                DbKind::SQLite
            }

            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }

            fn dialect(&self) -> &dyn SqlDialect {
                unimplemented!("StubSqlConnection::dialect not needed for this test")
            }

            fn ping(&self) -> Result<(), DbError> {
                Ok(())
            }

            fn close(&mut self) -> Result<(), DbError> {
                Ok(())
            }

            fn execute(
                &self,
                _req: &dbflux_core::QueryRequest,
            ) -> Result<CoreQueryResult, DbError> {
                Err(DbError::NotSupported("stub".to_string()))
            }

            fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
                Ok(())
            }

            fn schema(&self) -> Result<SchemaSnapshot, DbError> {
                Ok(SchemaSnapshot::default())
            }
        }

        let profile_id = Uuid::new_v4();

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime =
                    StorageRuntime::in_memory().expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
            })
        });

        cx.update(|cx| {
            app_state.update(cx, |app, _cx| {
                let profile = dbflux_core::ConnectionProfile::new(
                    "test",
                    DbConfig::SQLite {
                        path: PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                );
                let connected = ConnectedProfile {
                    profile,
                    connection: Arc::new(StubSqlConnection),
                    schema: None,
                    database_schemas: Default::default(),
                    table_details: Default::default(),
                    collection_children: Default::default(),
                    schema_types: Default::default(),
                    schema_indexes: Default::default(),
                    schema_foreign_keys: Default::default(),
                    schema_routines: Default::default(),
                    dependents_cache: Default::default(),
                    active_database: None,
                    redis_key_cache: Default::default(),
                    database_connections: Default::default(),
                    proxy_tunnel: None,
                };
                app.connections_mut().insert(profile_id, connected);
            });
        });

        let panel_holder = Rc::new(RefCell::new(None));
        let panel_handle = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id,
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "users"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: None,
                };

                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let result = window.update(|_, app| panel.read(app).can_open_builder(app));

        assert!(
            result,
            "can_open_builder should return true for Table source with SQL query language"
        );
    }

    #[gpui::test]
    fn visual_select_caches_precomputed_query(cx: &mut TestAppContext) {
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
                    total_rows: None,
                };

                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });

            panel_handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder
            .borrow()
            .clone()
            .expect("panel should be created");

        let pre_select = SelectQuery {
            sql: "SELECT * FROM public.users LIMIT 100".to_string(),
            params: vec![],
        };

        window.update(|_, app| {
            panel.update(app, |panel, _cx| {
                panel.visual_select = Some(pre_select.clone());
            });
        });

        let stored = window.update(|_, app| panel.read(app).visual_select.clone());

        assert_eq!(
            stored,
            Some(pre_select),
            "visual_select should cache the query"
        );
    }
}
