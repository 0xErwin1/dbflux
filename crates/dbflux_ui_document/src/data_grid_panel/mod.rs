mod builder;
mod chart;
mod chrome;
pub(crate) mod context_menu;
mod document_view;
mod documents;
mod facets;
pub(crate) mod filter_bar;
mod grid;
pub(crate) mod mutation_confirm;
pub(crate) mod mutation_executor;
mod mutations;
mod navigation;
mod nearest_neighbors;
mod primary_key;
mod query;
mod refresh;
mod render;
mod result_search;
pub(crate) mod row_inspector;
pub(crate) mod side_island;
mod table_documents;
mod table_views;
mod utils;
pub(crate) mod value_panel;

use super::query_builder::completion::{
    CompletionMode, FkLink, SchemaCache, SchemaCompletionProvider,
};
use super::query_builder::{BuilderEvent, FkLoadState, QueryBuilderPanel};
use super::result_view::{
    ResultViewMode, default_bindings_for_time_series, result_view_mode_for_fresh_result,
};
use super::task_runner::DocumentTaskRunner;
use dbflux_components::SqlPreviewContext;
use dbflux_components::chart::{
    ChartDetection, ChartView, DataPointRef, SourceRowRef, detect_chart_columns,
};
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::components::data_table::{
    ContextMenuAction, DataTable, DataTableEvent, DataTableState, ModelSwap,
    SortState as TableSortState, TableModel,
};
use dbflux_components::components::document_tree::{
    DocumentTree, DocumentTreeEvent, DocumentTreeState,
};
use dbflux_components::controls::CompletionProvider;
use dbflux_components::controls::{
    ButtonVariant, Dropdown, DropdownItem, DropdownSelectionChanged,
};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::modals::cell_editor::{
    CellEditorClosedEvent, CellEditorModal, CellEditorSaveEvent,
};
use dbflux_components::modals::document_preview::{
    DocumentPreviewClosedEvent, DocumentPreviewModal, DocumentPreviewSaveEvent,
};
use dbflux_components::modals::{
    ModalMutationConfirm, ModalMutationConfirmHard, MutationConfirmOutcome,
};
use dbflux_core::{
    CollectionRef, ColumnMeta, DatabaseCategory, OrderByColumn, Pagination, QueryResult,
    RefreshPolicy, SelectQuery, SortDirection, TableRef, TaskId, Value, VisualQuerySpec,
    WhereOperator,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::AsyncUpdateResultExt;
use dbflux_ui_base::toast::PendingToast;
use gpui::*;
use gpui_component::Sizable;
use gpui_component::input::EditorState;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
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

/// Resolve the single orderable key from a result's column metadata, generically.
///
/// Stores that order on exactly one key (e.g. DynamoDB's sort key) emit their
/// primary-key columns partition-key first and sort-key last, so the trailing
/// primary-key column is the orderable key. Returns `None` when there are fewer
/// than two primary-key columns: a single primary-key column is a partition-only
/// key with nothing to order on. The function reads only generic `ColumnMeta`
/// and never names a driver.
fn resolve_orderable_sort_key(columns: &[ColumnMeta]) -> Option<String> {
    let primary_key_columns: Vec<&ColumnMeta> = columns
        .iter()
        .filter(|column| column.is_primary_key)
        .collect();

    if primary_key_columns.len() < 2 {
        return None;
    }

    primary_key_columns.last().map(|column| column.name.clone())
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
    ///
    /// `content_has_header` tells the rail the content draws its own title
    /// bar (the row inspector), so the rail must not add one.
    OpenInspector {
        title: SharedString,
        content: AnyView,
        content_has_header: bool,
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

    /// A visual-mutation run finished, and whether its edits reached the database.
    ///
    /// `landed: false` covers a failed run, a cancelled run, and a statement the
    /// database matched no rows for — the case a close waiting on the apply has to
    /// tell apart from a successful one.
    MutationFinished { landed: bool },

    /// The panel wants to close itself, because the mutation a close was waiting
    /// on landed.
    RequestClose,

    /// Count the rows of the query behind a result the row limit cut short,
    /// without fetching them. Only emitted when the host offered it through
    /// [`DataGridPanel::set_limited_row_actions`].
    CountRowsRequested,

    /// Run the query behind a result the row limit cut short again, without
    /// the limit. Only emitted when the host offered it.
    LoadAllRowsRequested,
}

/// What the footer offers for a query result the editor row limit cut short.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LimitedRowActions {
    /// The host can count the query's rows without fetching them.
    pub(crate) count: bool,
    /// The host can run the query again without the row limit.
    pub(crate) load_all: bool,
}

/// The total row count of a limited result, as far as it is known.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum LimitedRowTotal {
    #[default]
    Unknown,
    Counting,
    Known(u64),
}

/// Footer state of a query result the row limit cut short. Reset with every
/// new result, because a total belongs to the query that produced it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LimitedRows {
    pub(crate) actions: LimitedRowActions,
    pub(crate) total: LimitedRowTotal,
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
    pub(crate) fn left(self) -> Self {
        match self {
            ToolbarFocus::Filter => ToolbarFocus::Filter,
            ToolbarFocus::Limit => ToolbarFocus::Filter,
            ToolbarFocus::Refresh => ToolbarFocus::Limit,
        }
    }

    pub(crate) fn right(self) -> Self {
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
    /// Whether the "Toolbar" submenu (the results toolbar buttons) is open.
    toolbar_submenu_open: bool,
    /// Currently selected menu item index (for keyboard navigation).
    selected_index: usize,
    /// Selected index within the active submenu.
    submenu_selected_index: usize,
    /// Whether this is a document view context menu (different items shown).
    is_document_view: bool,
    /// Whether this menu was opened by right-clicking a column header, which
    /// scopes it to that column's ordering and filtering.
    is_column_header: bool,
    doc_field_path: Option<Vec<String>>,
    doc_field_value: Option<dbflux_components::components::document_tree::NodeValue>,
    /// Driver-supplied row-level actions (e.g. Kill, Cancel). When non-empty,
    /// these appear at the bottom of the menu after a separator. Selecting one
    /// emits `DataGridEvent::RowActionRequested`.
    row_actions: Vec<dbflux_core::InspectorRowAction>,
}

impl TableContextMenu {
    fn any_submenu_open(&self) -> bool {
        self.sql_submenu_open
            || self.copy_query_submenu_open
            || self.filter_submenu_open
            || self.order_submenu_open
            || self.toolbar_submenu_open
    }

    /// Hovering a plain item closes whatever submenu was open, as native
    /// menus do.
    fn close_submenus(&mut self) {
        self.sql_submenu_open = false;
        self.copy_query_submenu_open = false;
        self.filter_submenu_open = false;
        self.order_submenu_open = false;
        self.toolbar_submenu_open = false;
    }
}

/// A single item in the context menu.
struct ContextMenuItem {
    label: SharedString,
    action: Option<ContextMenuAction>,
    icon: Option<dbflux_components::icons::AppIcon>,
    is_separator: bool,
    is_danger: bool,
    /// A muted second line under the label.
    detail: Option<SharedString>,
    /// Drawn but not activatable; `detail` says why.
    disabled: bool,
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

/// Pending intents drained at the top of each render cycle via `process_pending_actions`.
///
/// Each field is set by a producer and consumed exactly once per cycle.
/// Fields that are mid-flow state machines (`pending_delete_confirm`,
/// `pending_batch_remaining`, `pending_mutation_exec`,
/// `pending_collection_chart_save`) are not included here; they are read
/// mid-render and remain as direct fields on `DataGridPanel`.
#[derive(Default)]
struct PendingActions {
    requery: Option<PendingRequery>,
    total_count: Option<PendingTotalCount>,
    rebuild: bool,
    refresh: bool,
    /// Set with `refresh` by a landed mutation: the reload it issues carries
    /// the edits still staged on other rows over to its result.
    refresh_keeps_edits: bool,
    /// Set with `rebuild` by an in-memory sort: the rebuild carries the staged
    /// edits over to the reordered rows.
    rebuild_keeps_edits: bool,
    toast: Option<PendingToast>,
    modal_open: Option<PendingModalOpen>,
    document_preview: Option<PendingDocumentPreview>,
    context_menu_focus: bool,
    mutation_modal: Option<crate::data_grid_panel::mutation_confirm::PendingMutationModal>,
    /// Cell the value panel should open on. Deferred to render because
    /// building the panel's code editor needs a `Window`.
    value_panel: Option<value_panel::ValuePanelTarget>,
    /// Row action requested from the row inspector's footer, with the row and
    /// column it applies to. Deferred to render because editing needs a
    /// `Window`.
    row_inspector_action: Option<(row_inspector::RowInspectorContentEvent, usize, usize)>,
    /// The JSON view has to be reloaded with the page. Deferred to render
    /// because setting an editor's text needs a `Window`.
    json_reload: bool,
}

/// How the grid should treat the state held by an existing `DataTableState`
/// when a result is applied to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum TableReload {
    /// The result holds the same rows, reordered or re-fetched (sort, refresh,
    /// in-memory re-sort): the cursor stays where it is.
    #[default]
    Preserve,
    /// The result holds a different row set (another page, another filter):
    /// the cursor belongs to rows that are gone, so it is dropped.
    ResetRows,
    /// The result may hold different columns (a new query result): the cursor
    /// and the sort column index both point at the old shape and are dropped.
    NewColumns,
}

impl TableReload {
    fn cursor_swap(self) -> ModelSwap {
        match self {
            TableReload::Preserve => ModelSwap::KeepCursor,
            TableReload::ResetRows | TableReload::NewColumns => ModelSwap::ResetCursor,
        }
    }
}

/// Enum/set choices per result column index, with the NULL sentinel prepended
/// for a nullable column. Indexed the same way in a freshly built state and in
/// one reused across a reload.
fn enum_options_for_result(
    result: &QueryResult,
    column_details: Option<&[dbflux_core::ColumnInfo]>,
) -> Vec<(usize, Vec<String>)> {
    let Some(columns) = column_details else {
        return Vec::new();
    };

    result
        .columns
        .iter()
        .enumerate()
        .filter_map(|(col_ix, result_col)| {
            let info = columns.iter().find(|c| c.name == result_col.name)?;
            let enum_vals = info.enum_values.as_ref()?;

            let mut options = enum_vals.clone();
            if info.nullable {
                options.insert(0, DataTableState::NULL_SENTINEL.to_string());
            }
            Some((col_ix, options))
        })
        .collect()
}

/// The rendered table widget and its in-memory sort state.
///
/// `data_table`, `table_state` and `table_subscription` are created together on
/// the first result and then reused across reloads so that user adjustments to
/// the grid survive them; the two local-sort fields track the `QueryResult`
/// in-memory sort.
struct GridTableState {
    data_table: Option<Entity<DataTable>>,
    table_state: Option<Entity<DataTableState>>,
    table_subscription: Option<Subscription>,
    local_sort_state: Option<LocalSortState>,
    original_row_order: Option<Vec<usize>>,
    /// Read by the next `rebuild_table` and reset to `Preserve` there.
    reload: TableReload,
    /// Whether the rebuild for a reload carries the edits still staged on
    /// other rows over to the reloaded rows by primary key.
    ///
    /// Armed only for the duration of `refresh_keeping_edits`: the query it
    /// issues takes the flag into its request and hands it back just before
    /// its own result is applied, where `rebuild_table` consumes it. A rebuild
    /// for anything else, or a request that fails or is cancelled, never
    /// sees it. An in-memory sort arms it for its own rebuild only.
    keep_edits_on_reload: bool,
}

/// The WHERE/LIMIT inputs and refresh-policy dropdown.
///
/// All four fields are consumed by `render_toolbar`; they are created
/// together at construction
/// time and are never individually swapped out.
struct FilterBarState {
    filter_input: Entity<EditorState>,
    /// Schema cache backing the WHERE filter's autocomplete. `Some` only when
    /// `source` is `DataSource::Table` and a completion provider was wired.
    filter_completion_cache: Option<Rc<RefCell<SchemaCache>>>,
    limit_input: Entity<InputState>,
    /// Refresh-policy dropdown; rendered both in the embedded toolbar and the
    /// chart toolbar. Change events are handled via a subscription wired in
    /// `new_internal`.
    refresh_dropdown: Entity<Dropdown>,
    /// Driver-native text of the running collection browse, on one line.
    ///
    /// Shown in place of the generic source label so a collection reads in the
    /// connection's own query language. `None` when the driver does not
    /// describe its browse query, or for table and query-result sources.
    browse_query_label: Option<String>,
}

/// Auto-refresh policy, timer, and grid load state.
///
/// The four fields are mutated together in `set_refresh_policy` /
/// `update_refresh_timer` and are logically inseparable.
struct RefreshState {
    refresh_policy: RefreshPolicy,
    _refresh_timer: Option<Task<()>>,
    _refresh_subscriptions: Vec<Subscription>,
    state: GridState,
}

/// Document/JSON view widgets: tree entity, tree state, its subscription,
/// the document-preview modal, and the cell-editor modal.
///
/// All five are created at construction time and live for the panel's lifetime.
/// They form the full MongoDB-document presentation path.
struct DocumentViewState {
    document_tree: Option<Entity<DocumentTree>>,
    document_tree_state: Option<Entity<DocumentTreeState>>,
    document_tree_subscription: Option<Subscription>,

    /// Virtualized state for the document-card fallback view (used when no
    /// `document_tree` is built). Cards have variable height, so this relies on
    /// `gpui::list` rather than `uniform_list`. Rebuilt with the row count on
    /// every `rebuild_table`.
    document_card_list: Option<ListState>,

    document_preview_modal: Entity<DocumentPreviewModal>,
    cell_editor: Entity<CellEditorModal>,
}

/// Chart shell and source time-range panel.
///
/// Both are lazy / optional: the shell is created on first chartable result;
/// the time-range panel is injected by `CodeDocument` after construction.
struct ChartState {
    /// Lazily-created chart shell entity. Created the first time the result
    /// passes chart detection (or when the user is already in chart mode).
    /// `None` for sources that have never produced a chartable result.
    chart_shell: Option<Entity<crate::chart::ChartShell>>,

    /// Time-range panel from the source-context bar, set by `CodeDocument`
    /// after the panel is built. `None` for non-TimeSeries sources.
    chart_source_time_range_panel:
        Option<Entity<dbflux_components::common::time_range::view::TimeRangePanel>>,

    /// The source carries a time-series facet. Such a source offers the
    /// Data / Chart / JSON result views and opens as a chart.
    time_series_source: bool,

    /// The column a time-series source is indexed by, when its driver names
    /// it. The chart puts it on the time axis instead of the first timestamp
    /// column.
    time_series_column: Option<String>,

    /// Whether the time-series collection already received its first result.
    /// Only that first result picks the chart view and seeds the axis bindings.
    /// Later refreshes keep what the user chose.
    time_series_result_seen: bool,
}

/// Mutation confirmation modal pair (light + hard variants).
struct MutationConfirmState {
    /// Light variant for small row counts.
    pub(crate) mutation_confirm_light: Entity<dbflux_components::modals::ModalMutationConfirm>,
    /// Hard variant for large row counts / DELETE.
    pub(crate) mutation_confirm_hard: Entity<dbflux_components::modals::ModalMutationConfirmHard>,
}

/// Keyboard and toolbar focus state machine.
///
/// Tracks the current navigation mode, which toolbar element is focused,
/// the edit state, input-switch flag, and the context-menu focus handle.
struct FocusState {
    focus_mode: GridFocusMode,
    toolbar_focus: ToolbarFocus,
    edit_state: EditState,
    switching_input: bool,
    context_menu_focus: FocusHandle,
    /// Holds the keyboard while the export menu is open, so the menu keys
    /// reach the menu rather than the table under it.
    export_menu_focus: FocusHandle,
    /// Holds the keyboard while a document collection's query history menu
    /// is open.
    history_menu_focus: FocusHandle,
    /// The side panel the keyboard moved into with Ctrl+L, if any.
    side_island: Option<side_island::SideIsland>,
    /// Clears `side_island` when focus leaves that panel by another route.
    _side_island_blur: Option<Subscription>,
}

/// Panel chrome flags and result-view text caches.
///
/// Presentation toggles for the embedded-panel shell, the current
/// result-view mode, and the lazily-populated derived text/JSON caches.
struct ChromeState {
    show_panel_controls: bool,
    is_maximized: bool,
    export_menu_open: bool,
    /// Highlighted row of the export menu, an index into `export_menu_entries`.
    export_menu_selected: usize,
    result_view_mode: ResultViewMode,
    /// When `true`, the result area shows the active row as a vertical
    /// name/value record instead of the grid.
    record_mode: bool,
    derived_json: Option<String>,
    derived_text: Option<String>,
}

/// Row inspector rail integration.
///
/// Holds the inspector content entity, the last inspected (row, col), and
/// an optional provider for row-level kill/cancel actions.
struct InspectorState {
    row_inspector_content: Option<Entity<row_inspector::RowInspectorContent>>,

    /// Whether row selection should keep driving the shared inspector rail,
    /// including after switching to a different table tab.
    follow_selection: bool,

    /// Last `(row, col)` opened in the row inspector. `Some` means the inspector
    /// is logically "on" for this panel — it should reappear when the panel's
    /// tab is re-activated, follow the user's cursor on `SelectionChanged`, and
    /// re-snapshot itself after a refresh. Cleared when the user dismisses the
    /// rail explicitly (via `DataGridPanel::clear_inspector_state`) or when the
    /// stored row falls outside the new result.
    inspector_row: Option<(usize, usize)>,

    /// The user pinned the inspected row: selection changes no longer move
    /// the inspector, which keeps showing `inspector_row`.
    pinned: bool,

    /// Subscription to the row inspector's button events.
    _row_inspector_subscription: Option<Subscription>,

    /// Debounced lookup and counting of the inspected row's incoming
    /// references.
    incoming_references: row_inspector::IncomingReferencesLoader,

    /// The Document panel a document collection shows instead of the row
    /// inspector, kept alive so its expanded fields survive row changes.
    document_inspector_content: Option<Entity<documents::inspector::DocumentInspectorContent>>,

    /// Subscription to the Document panel's button events.
    _document_inspector_subscription: Option<Subscription>,

    /// Optional provider for row-level kill/cancel actions.
    ///
    /// When set, right-clicking a row emits `DataGridEvent::RowActionRequested`
    /// for the first destructive action the provider returns, instead of the
    /// normal context menu.
    row_action_provider: Option<RowActionProvider>,

    /// Value panel content. Kept alive across closes so the user's format and
    /// word-wrap choices survive reopening.
    value_panel: Option<Entity<value_panel::ValuePanelContent>>,

    /// Whether the value panel currently owns the shared rail.
    value_panel_open: bool,

    /// Subscription to the value panel's save event.
    _value_panel_subscription: Option<Subscription>,
}

/// Visual Query Builder cluster.
///
/// All nine fields are `pub(crate)` — external crates in the same workspace
/// read or update them (e.g. the builder panel subscription in `mod.rs`).
pub(crate) struct BuilderState {
    /// FK metadata for the current (connection, database, schema).
    pub(crate) fk_cache: FkLoadState,
    /// Current state of the relational filter bar chip and inline error area.
    pub(crate) relational_filter_state: filter_bar::RelationalFilterState,
    /// The spec currently being edited in the `QueryBuilderPanel` (in-flight
    /// draft, not the last-committed spec).
    pub(crate) builder_draft_spec: Option<VisualQuerySpec>,
    /// Pre-computed `SelectQuery` for the current `builder_draft_spec`.
    pub(crate) visual_select: Option<SelectQuery>,
    /// The `VisualQuerySpec` last successfully executed by `run_visual_query`.
    pub(crate) current_visual_spec: Option<VisualQuerySpec>,
    /// Builder panel entity, kept alive here to preserve state across sessions.
    pub(crate) builder_panel: Option<Entity<QueryBuilderPanel>>,
    /// Subscriptions to `QueryBuilderPanel` events.
    pub(crate) _builder_subscriptions: Vec<Subscription>,
    /// When `true`, the raw filter input is hidden because an applied builder
    /// spec owns query composition for this panel.
    pub(crate) filter_input_hidden: bool,
    /// Whether the builder currently owns the inspector rail. The raw filter
    /// input is hidden while it is open and comes back once it is closed.
    pub(crate) builder_open: bool,
    /// Editable-safety binding for the last successfully executed builder SELECT.
    pub(crate) builder_editable_binding: Option<dbflux_core::EditableBinding>,
}

/// Reusable data grid panel with filter bar, grid, toolbar, and status bar.
/// Used both embedded in ScriptDocument and as standalone DataDocument.
pub struct DataGridPanel {
    source: DataSource,
    app_state: gpui::Entity<AppStateEntity>,
    result: QueryResult,
    grid_table: GridTableState,
    filter_bar: FilterBarState,
    refresh: RefreshState,
    document_view: DocumentViewState,
    chart: ChartState,
    mutation_confirm: MutationConfirmState,
    focus: FocusState,
    chrome: ChromeState,
    result_search: result_search::ResultSearch,
    inspector: InspectorState,
    pub(crate) builder: BuilderState,
    /// Document collection presentation (flattened table, query bar, schema,
    /// field edits). Inert for other sources.
    collection: documents::CollectionViewState,
    /// Columns and Partitions views of a table whose connection offers them.
    /// Inert for other sources.
    table_views: table_views::TableViewsState,
    pk_columns: Vec<String>,
    runner: DocumentTaskRunner,
    focus_handle: FocusHandle,
    panel_origin: Point<Pixels>,
    /// Panel size from the same canvas; the context menu is kept inside it.
    panel_size: Size<Pixels>,
    /// A table-details fetch for the primary key is in flight. Until it
    /// answers, the grid is read-only for want of a key it may well have, so
    /// the "no primary key" banner waits rather than flashing on every open.
    pk_details_pending: bool,
    /// The key arrived after the first page and the table is being read again
    /// in key order. The rows on screen were built without the key, so the
    /// banner waits for the reordered page instead of flashing until it lands.
    pk_requery_pending: bool,
    /// The rows on screen are the nearest neighbors of a row, not a page of
    /// the table. Cleared by any other result.
    nearest_neighbors: Option<nearest_neighbors::NearestNeighborSearch>,
    /// The neighbor bar's controls and the search in flight.
    nearest: nearest_neighbors::NearestNeighborsUi,
    view_config: super::data_view::DataViewConfig,
    context_menu: Option<TableContextMenu>,
    is_active_tab: bool,
    /// The hosting document hands this grid's side panels (the chart stats
    /// rail) to the workspace through `side_panels`, so the grid does not
    /// dock them inside itself.
    side_panels_hosted: bool,
    pending: PendingActions,
    pending_delete_confirm: Option<PendingDeleteConfirm>,
    pending_batch_remaining: Option<PendingBatchRemaining>,
    /// A close is waiting on the staged edits being applied.
    ///
    /// Armed by [`DataGridPanel::apply_for_close`] and dropped by the first
    /// operation that fails, or by the batch draining. While it is armed, the
    /// grid owns the close gesture: the tab must not go before the edits land,
    /// and must not go if they did not.
    close_after_apply: bool,
    /// Whether the batch pump has a write in flight.
    ///
    /// Set when `process_next_batch_op` dispatches an operation and cleared when
    /// that operation reports back. `apply_for_close` reads it so a second close
    /// request cannot re-enter the pump and start the held rows concurrently.
    batch_op_in_flight: bool,
    /// Pending "Save chart from collection" state.
    pub(super) pending_collection_chart_save: Option<CollectionChartSaveState>,
    pub(crate) pending_mutation_exec: Option<PendingMutationExec>,
    limited_rows: LimitedRows,
    /// Bumped by every `set_query_result`, so a host's asynchronous answer
    /// about one result is not applied to the next.
    result_generation: u64,
}

/// Pending mutation execution — holds the spec and options while the
/// confirmation modal is open so the confirm handler can dispatch the executor.
pub(crate) struct PendingMutationExec {
    pub(crate) spec: dbflux_core::VisualMutationSpec,
    pub(crate) opts: crate::data_grid_panel::mutation_executor::MutationExecOptions,
    pub(crate) profile_id: uuid::Uuid,
    pub(crate) intent: MutationIntent,
}

/// Why a visual-mutation run was started.
///
/// The run itself does not change: the intent decides what finishing it means. A
/// run started from the apply affordance reports its outcome and stops; a run a
/// close is waiting on has to report back to that close, which is what
/// [`DataGridEvent::RequestClose`] does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationIntent {
    /// Started from the grid's own apply affordance.
    Direct,
    /// A close is waiting: close the tab when this run lands.
    CloseAfterApply,
}

/// One finished-or-running visual mutation, as its completion needs to report it.
///
/// Bundles what the completion path reports under (the task slot, the failure
/// label, the table) with why it was started, so the completion is one call
/// instead of one arm per execution mode.
#[derive(Clone, Debug)]
pub(crate) struct MutationRun {
    pub(crate) task_id: TaskId,
    pub(crate) mode: crate::labels::VisualMutationTaskMode,
    pub(crate) table_name: String,
    pub(crate) intent: MutationIntent,
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
        app_state: gpui::Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let order_by =
            Self::get_primary_key_columns(&app_state, profile_id, database.as_deref(), &table, cx);
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
        panel.init_table_views(window, cx);
        panel.refresh(window, cx);

        // The details carry the primary key when the schema snapshot did not,
        // and the table's facets either way; the fetch reuses cached details.
        panel.fetch_table_details_for_pk(profile_id, &table, cx);

        panel.ensure_filter_source_columns_loaded(cx);
        panel.ensure_fk_cache_loaded(cx);

        panel
    }

    pub fn new_for_collection(
        profile_id: Uuid,
        collection: CollectionRef,
        app_state: gpui::Entity<AppStateEntity>,
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

    /// A table grid showing `result`, with rows identified by `pk_columns`,
    /// that never queries a connection. For tests outside this crate that need
    /// an editable grid.
    #[cfg(any(test, feature = "test-support"))]
    pub fn new_for_test_table(
        result: QueryResult,
        pk_columns: Vec<String>,
        app_state: gpui::Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let source = DataSource::Table {
            profile_id: Uuid::nil(),
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "orders"),
            pagination: Pagination::default(),
            order_by: Vec::new(),
            total_rows: None,
        };

        let mut panel = Self::new_internal(source, app_state, pk_columns, window, cx);
        panel.set_result(result, cx);
        panel
    }

    /// Create a new panel for displaying a query result (in-memory sorting).
    pub fn new_for_result(
        result: Arc<QueryResult>,
        original_query: String,
        profile_id: Option<Uuid>,
        app_state: gpui::Entity<AppStateEntity>,
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
        panel.install_result_search(window, cx);
        panel.set_result((*result).clone(), cx);
        panel
    }

    fn new_internal(
        source: DataSource,
        app_state: gpui::Entity<AppStateEntity>,
        pk_columns: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let filter_placeholder = Self::filter_placeholder_for_source(&source, &app_state, cx);

        let filter_input = cx.new(|cx| {
            crate::completion_support::new_single_line_completion_state(
                window,
                cx,
                filter_placeholder,
            )
        });

        let filter_completion_cache: Option<Rc<RefCell<SchemaCache>>> = if let DataSource::Table {
            profile_id,
            table,
            database,
            ..
        } = &source
        {
            let source_columns: Vec<dbflux_core::ColumnInfo> = {
                let state = app_state.read(cx);
                state
                    .connections()
                    .get(profile_id)
                    .and_then(|conn| {
                        let db = database
                            .clone()
                            .or_else(|| conn.active_database.clone())
                            .or_else(|| table.schema.clone())
                            .unwrap_or_else(|| "default".to_string());
                        conn.table_details
                            .get(&(db, table.schema.clone(), table.name.clone()))
                            .and_then(|info| info.columns.clone())
                    })
                    .unwrap_or_default()
            };

            let filter_cache = Rc::new(RefCell::new(SchemaCache {
                source_table: table.name.clone(),
                source_columns,
                joined_columns: HashMap::new(),
                fk_links: HashMap::new(),
                fetching: HashSet::new(),
                failed: HashSet::new(),
            }));

            let filter_provider: Rc<dyn CompletionProvider> =
                Rc::new(SchemaCompletionProvider::new(
                    app_state.downgrade(),
                    *profile_id,
                    CompletionMode::FilterExpression,
                    filter_cache.clone(),
                ));

            filter_input.update(cx, |state, _| {
                state.lsp_mut().completion_provider = Some(filter_provider);
            });

            Some(filter_cache)
        } else {
            None
        };

        let limit_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("100");
            state.set_value("100", window, cx);
            state
        });

        cx.subscribe_in(
            &filter_input,
            window,
            |this, input, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } if this.is_document_collection(cx) => {
                    this.find_documents(window, cx);
                }
                InputEvent::PressEnter {
                    secondary: false, ..
                } => {
                    if this.reload_blocked_by_pending_edits(cx) {
                        return;
                    }

                    // A new filter selects a different row set.
                    this.grid_table.reload = TableReload::ResetRows;
                    this.refresh(window, cx);
                    this.focus_table(window, cx);
                }
                InputEvent::Blur => {
                    this.exit_edit_mode(window, cx);
                }
                InputEvent::Change => {
                    let text = input.read(cx).value().to_string();
                    if filter_bar::classify_filter_input(&text)
                        == filter_bar::FilterMode::Relational
                    {
                        this.ensure_fk_cache_loaded(cx);
                    }
                    this.ensure_filter_source_columns_loaded(cx);
                    this.ensure_filter_fk_columns_loaded(&text, cx);
                }
                _ => {}
            },
        )
        .detach();

        cx.subscribe_in(
            &limit_input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } if this.is_document_collection(cx) => {
                    this.find_documents(window, cx);
                }
                InputEvent::PressEnter {
                    secondary: false, ..
                } => {
                    if this.reload_blocked_by_pending_edits(cx) {
                        return;
                    }

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

        let mutation_confirm_light =
            cx.new(|_cx| dbflux_components::modals::ModalMutationConfirm::new(window, _cx));
        let mutation_confirm_hard =
            cx.new(|cx| dbflux_components::modals::ModalMutationConfirmHard::new(window, cx));

        cx.subscribe_in(
            &mutation_confirm_light,
            window,
            |this, _, outcome: &dbflux_components::modals::MutationConfirmOutcome, window, cx| {
                this.handle_mutation_confirm_outcome(outcome.clone(), window, cx);
            },
        )
        .detach();

        cx.subscribe_in(
            &mutation_confirm_hard,
            window,
            |this, _, outcome: &dbflux_components::modals::MutationConfirmOutcome, window, cx| {
                this.handle_mutation_confirm_outcome(outcome.clone(), window, cx);
            },
        )
        .detach();

        let source_facets = Self::source_facets(&source, &app_state, cx);
        let time_series = facets::time_series_facet(&source_facets).cloned();
        let time_series_source = time_series.is_some();
        let time_series_column = time_series.and_then(|facet| facet.time_column);
        let view_config = Self::view_config_for(&source, &source_facets);
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
                .map(|policy| DropdownItem::new(crate::labels::refresh_policy_label(*policy)))
                .collect();

            Dropdown::new("data-grid-auto-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
                .disabled(!supports_auto_refresh)
                .chevron_trigger(ButtonVariant::Primary)
        });

        let refresh_policy_sub = cx.subscribe_in(
            &refresh_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                let policy = RefreshPolicy::from_index(event.index);

                if policy.is_auto() && !this.supports_auto_refresh() {
                    this.filter_bar.refresh_dropdown.update(cx, |dd, cx| {
                        dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
                    });
                    dbflux_ui_base::toast::Toast::warning(
                        crate::labels::auto_refresh_unavailable_toast(),
                    )
                    .meta_right(dbflux_ui_base::toast::now_hms())
                    .push(cx);
                    return;
                }

                this.set_refresh_policy(policy, cx);
            },
        );

        let collection = documents::CollectionViewState::new(window, cx);
        let nearest = nearest_neighbors::NearestNeighborsUi::new(window, cx);

        if matches!(source, DataSource::Collection { .. }) && filter_completion_cache.is_none() {
            let provider: Rc<dyn CompletionProvider> =
                Rc::new(documents::completion::DocumentFieldCompletionProvider::new(
                    collection.field_paths.clone(),
                ));
            filter_input.update(cx, |state, _| {
                state.lsp_mut().completion_provider = Some(provider);
            });
        }

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

        let mut panel = Self {
            source,
            app_state,
            result: QueryResult::empty(),
            grid_table: GridTableState {
                data_table: None,
                table_state: None,
                table_subscription: None,
                local_sort_state: None,
                original_row_order: None,
                reload: TableReload::default(),
                keep_edits_on_reload: false,
            },
            filter_bar: FilterBarState {
                filter_input,
                filter_completion_cache,
                limit_input,
                refresh_dropdown,
                browse_query_label: None,
            },
            refresh: RefreshState {
                refresh_policy: default_refresh,
                _refresh_timer: None,
                _refresh_subscriptions: vec![refresh_policy_sub],
                state: GridState::Ready,
            },
            document_view: DocumentViewState {
                document_tree: None,
                document_tree_state: None,
                document_tree_subscription: None,
                document_card_list: None,
                document_preview_modal,
                cell_editor,
            },
            chart: ChartState {
                chart_shell: None,
                chart_source_time_range_panel: None,
                time_series_source,
                time_series_column,
                time_series_result_seen: false,
            },
            mutation_confirm: MutationConfirmState {
                mutation_confirm_light,
                mutation_confirm_hard,
            },
            pk_columns,
            focus: FocusState {
                focus_mode: GridFocusMode::default(),
                toolbar_focus: ToolbarFocus::default(),
                edit_state: EditState::default(),
                switching_input: false,
                context_menu_focus,
                export_menu_focus: cx.focus_handle(),
                history_menu_focus: cx.focus_handle(),
                side_island: None,
                _side_island_blur: None,
            },
            chrome: ChromeState {
                show_panel_controls: false,
                is_maximized: false,
                export_menu_open: false,
                export_menu_selected: 0,
                result_view_mode,
                record_mode: false,
                derived_json: None,
                derived_text: None,
            },
            result_search: result_search::ResultSearch::default(),
            inspector: InspectorState {
                row_inspector_content: None,
                follow_selection: false,
                inspector_row: None,
                pinned: false,
                _row_inspector_subscription: None,
                incoming_references: row_inspector::IncomingReferencesLoader::default(),
                document_inspector_content: None,
                _document_inspector_subscription: None,
                row_action_provider: None,
                value_panel: None,
                value_panel_open: false,
                _value_panel_subscription: None,
            },
            builder: BuilderState {
                fk_cache: FkLoadState::Loading,
                relational_filter_state: filter_bar::RelationalFilterState::Inactive,
                builder_draft_spec: None,
                visual_select: None,
                current_visual_spec: None,
                builder_panel: None,
                _builder_subscriptions: Vec::new(),
                filter_input_hidden: false,
                builder_open: false,
                builder_editable_binding: None,
            },
            collection,
            table_views: table_views::TableViewsState::default(),
            runner,
            focus_handle,
            panel_origin: Point::default(),
            panel_size: Size::default(),
            pk_details_pending: false,
            pk_requery_pending: false,
            nearest_neighbors: None,
            nearest,
            view_config,
            context_menu: None,
            is_active_tab: true,
            side_panels_hosted: false,
            pending: PendingActions::default(),
            pending_delete_confirm: None,
            pending_batch_remaining: None,
            close_after_apply: false,
            batch_op_in_flight: false,
            pending_collection_chart_save: None,
            pending_mutation_exec: None,
            limited_rows: LimitedRows::default(),
            result_generation: 0,
        };

        panel.follow_vim_setting(cx);
        panel
    }

    pub fn set_active_tab(&mut self, active: bool, cx: &mut Context<Self>) {
        self.is_active_tab = active;

        if active {
            // Re-mount the inspector rail with whichever per-tab content was
            // previously open. Builder takes precedence over the row inspector
            // because both share the same rail and the builder is the more
            // recent intentional surface for the user.
            if let Some(panel) = self
                .builder
                .builder_panel
                .clone()
                .filter(|_| self.builder.builder_open)
            {
                let view: AnyView = AnyView::from(panel);
                cx.emit(DataGridEvent::OpenInspector {
                    title: "Query Builder".into(),
                    content: view,
                    content_has_header: true,
                });
            } else if self.remount_document_builder(cx) {
                // The document builder took the rail back.
            } else if self.inspector.value_panel_open {
                // Re-read this grid's own cell. Reusing the cached content
                // would leave the rail showing a value from the table the
                // user just switched away from.
                if !self.mount_value_panel_for_active_cell(cx) {
                    cx.emit(DataGridEvent::CloseInspector);
                }
            } else if self.inspector.follow_selection && self.inspector.pinned {
                if let Some((row, col)) = self.inspector.inspector_row {
                    self.open_row_inspector(row, col, cx);
                }
            } else if self.inspector.follow_selection {
                let active = self
                    .grid_table
                    .table_state
                    .as_ref()
                    .and_then(|state| state.read(cx).selection().active);
                if let Some(coord) = active {
                    self.open_row_inspector(coord.row, coord.col, cx);
                } else if let Some((row, col)) = self.inspector.inspector_row {
                    self.open_row_inspector(row, col, cx);
                }
            } else {
                // This tab owns nothing in the rail. Say so explicitly: the
                // rail is global, so staying silent leaves the previous tab's
                // content on screen.
                cx.emit(DataGridEvent::CloseInspector);
            }
        } else if self.builder.builder_open
            || self.collection.builder.open
            || self.inspector.value_panel_open
        {
            // Hide the rail (without dropping cached state) so the next
            // active tab can take it over.
            cx.emit(DataGridEvent::CloseInspector);
        }
    }

    /// View configuration a source opens with, given its facets.
    ///
    /// A collection opens in the view of its first facet: the document tree
    /// when its driver declares documents first, the grid otherwise. Every
    /// other source keeps its own recommended view.
    fn view_config_for(
        source: &DataSource,
        facets: &[dbflux_core::ObjectFacet],
    ) -> super::data_view::DataViewConfig {
        use super::data_view::{DataViewConfig, DataViewMode};

        match source {
            DataSource::Collection { .. } => DataViewConfig {
                mode: DataViewMode::for_facets(facets)
                    .first()
                    .copied()
                    .unwrap_or_default(),
            },
            _ => DataViewConfig::for_source(source),
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

    /// Build a `ViewHandle` that erases the concrete `DataGridPanel` type for
    /// use inside a `ResultPanel`.
    ///
    /// A table or collection draws its own header, filter row and view
    /// switch (AppByzTable), so for those sources the handle contributes no
    /// chrome-row segments and no mode bar; a query result keeps the
    /// `ResultPanel` mode bar.
    ///
    /// The returned `ViewHandle` captures a clone of `entity`. The entity must
    /// already exist (this is called from `DataDocument::new_with_grid` after
    /// `cx.new(|cx| DataGridPanel::new_for_table(...))`).
    pub fn into_view_handle(
        entity: Entity<Self>,
        _cx: &mut App,
    ) -> dbflux_components::result_panel::ViewHandle {
        use dbflux_components::result_panel::ViewHandle;

        let e_render = entity.clone();
        let e_focus_get = entity.clone();
        let e_focus_do = entity.clone();
        let e_modes = entity.clone();
        let e_current = entity.clone();
        let e_set_mode = entity.clone();
        let e_segments = entity.clone();

        ViewHandle::builder()
            .render(move |_window, _cx| {
                // AnyView is itself an element and delegates to
                // DataGridPanel::render.
                AnyView::from(e_render.clone()).into_any_element()
            })
            .focus({
                move |window, cx| {
                    e_focus_do.update(cx, |grid, cx| {
                        grid.focus_table(window, cx);
                    });
                }
            })
            .focus_handle(move |cx| e_focus_get.read(cx).focus_handle.clone())
            .toolbar_segments(move |cx| Self::result_toolbar_segments(&e_segments, cx))
            .available_modes(move |cx| {
                let grid = e_modes.read(cx);
                if grid.footer_hosts_view_switch() {
                    Vec::new()
                } else {
                    grid.available_result_view_modes(cx)
                }
            })
            .current_mode(move |cx| e_current.read(cx).current_result_view_mode())
            .set_mode(move |mode, cx| {
                e_set_mode.update(cx, |grid, cx| grid.set_result_view_mode(mode, cx));
            })
            .build()
    }

    /// Returns `true` when the currently displayed result rows are from a
    /// grouped (aggregated) query.
    ///
    /// `current_visual_spec` is updated only on successful query completion so
    /// it always describes the rows visible in the grid. On query failure the
    /// previous successful spec is retained, which keeps this method consistent
    /// with what the user can actually see and interact with.
    pub fn is_grouped_result(&self) -> bool {
        self.builder
            .current_visual_spec
            .as_ref()
            .is_some_and(|s| s.is_grouped())
    }
}

impl EventEmitter<DataGridEvent> for DataGridPanel {}

#[cfg(test)]
mod tests;
