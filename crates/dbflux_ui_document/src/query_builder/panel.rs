use std::collections::{HashMap, HashSet};

use dbflux_components::controls::{
    Dropdown, DropdownItem, DropdownSelectionChanged, InputEvent, InputState,
};
use dbflux_core::{
    BoolOp, ColumnKind, Comparator, FilterNode, JoinFilterNode, JoinKind, JoinOn, JoinPredicate,
    JoinStep, LiteralValue, Predicate, PredicateValue, ProjectedColumn, Projection,
    SchemaForeignKeyInfo, SelectQuery, SortEntry, SourceTable, VisualQuerySpec,
    VisualSortDirection,
};
use gpui::{
    App, AppContext, Context, Entity, EventEmitter, FocusHandle, Render, Subscription, WeakEntity,
    Window,
};
use uuid::Uuid;

use crate::data_grid_panel::DataGridPanel;
use crate::query_builder::events::BuilderEvent;
use crate::query_builder::tree_ops::{
    collect_filter_predicate_ids, collect_join_predicate_ids, filter_node_at_path_mut,
    insert_filter_at_path, join_node_at_path_mut, remove_filter_at_path, set_join_predicate_field,
};

/// Default page limit applied when the builder opens without a prior spec.
const DEFAULT_LIMIT: u64 = 100;

/// Maximum nested group depth the UI will allow the user to create.
///
/// The SQL generator accepts any depth; this cap is enforced at the UI layer
/// only so imported saved queries authored before the cap still load correctly.
pub const FILTER_DEPTH_CAP: usize = 6;

/// Load state for foreign-key metadata used by the Joins section.
#[derive(Debug, Clone)]
pub enum FkLoadState {
    /// Background fetch in flight.
    Loading,
    /// Fetch succeeded; dropdowns populated.
    Ready(Vec<SchemaForeignKeyInfo>),
    /// Fetch failed or returned empty; banner shown once per session.
    Unavailable,
}

impl FkLoadState {
    /// Returns `true` if the load is complete (ready or unavailable).
    pub fn is_done(&self) -> bool {
        !matches!(self, FkLoadState::Loading)
    }

    /// Returns `true` if FK metadata is available.
    pub fn is_ready(&self) -> bool {
        matches!(self, FkLoadState::Ready(_))
    }

    /// Returns `true` if FK metadata is unavailable.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, FkLoadState::Unavailable)
    }

    /// Returns the FK list if available.
    pub fn foreign_keys(&self) -> Option<&[SchemaForeignKeyInfo]> {
        match self {
            FkLoadState::Ready(fks) => Some(fks),
            _ => None,
        }
    }
}

/// The panel's internal representation of a single join row.
///
/// Mirrors `JoinStep` but also tracks whether the row is currently in
/// raw-expression edit mode vs FK-dropdown mode.
#[derive(Debug, Clone)]
pub struct JoinRow {
    pub kind: JoinKind,
    pub from_alias: String,
    pub from_column: String,
    pub to_schema: Option<String>,
    pub to_table: String,
    pub to_alias: String,
    pub on: JoinOn,
}

impl JoinRow {
    fn to_join_step(&self) -> JoinStep {
        JoinStep {
            kind: self.kind,
            from_alias: self.from_alias.clone(),
            to_schema: self.to_schema.clone(),
            to_table: self.to_table.clone(),
            to_alias: self.to_alias.clone(),
            on: self.on.clone(),
        }
    }
}

/// A single sort entry as tracked by the panel.
#[derive(Debug, Clone)]
pub struct SortRow {
    pub source_alias: String,
    pub column: String,
    pub direction: VisualSortDirection,
}

impl SortRow {
    fn to_sort_entry(&self) -> SortEntry {
        SortEntry {
            source_alias: self.source_alias.clone(),
            column: self.column.clone(),
            direction: self.direction,
        }
    }
}

/// A column in the projection list as tracked by the panel.
#[derive(Debug, Clone)]
pub struct ProjectionRow {
    pub source_alias: String,
    pub column: String,
    pub alias: Option<String>,
}

impl ProjectionRow {
    fn to_projected_column(&self) -> ProjectedColumn {
        ProjectedColumn {
            source_alias: self.source_alias.clone(),
            column: self.column.clone(),
            alias: self.alias.clone(),
        }
    }
}

/// Whether the projection is "all columns" or an explicit list.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionMode {
    All,
    Explicit,
}

/// Visual Query Builder panel — GPUI entity.
///
/// Owns a `VisualQuerySpec` that starts as a copy of `DataGridPanel.builder_draft_spec`
/// (or a fresh default spec) and accumulates user edits. Emits `BuilderEvent`
/// on every user action that changes the spec or triggers a command.
///
/// The panel does NOT auto-execute; it mirrors its spec to the `DataGridPanel`
/// only when the user explicitly presses Run via `BuilderEvent::RunRequested`.
pub struct QueryBuilderPanel {
    /// The spec the panel is currently editing.
    pub(crate) current_spec: VisualQuerySpec,

    /// Projection mode: All vs Explicit list.
    pub(crate) projection_mode: ProjectionMode,

    /// Explicit column list (used when `projection_mode == Explicit`).
    pub(crate) projection_rows: Vec<ProjectionRow>,

    /// Join rows in display order.
    pub(crate) join_rows: Vec<JoinRow>,

    /// Sort rows in display order.
    pub(crate) sort_rows: Vec<SortRow>,

    /// FK load state for the Joins section.
    pub(crate) fk_state: FkLoadState,

    /// Whether the FK unavailability banner has been dismissed this session.
    pub(crate) fk_banner_dismissed: bool,

    /// Limit input value as string (to allow the user to clear/type freely).
    pub(crate) limit_text: String,

    /// Offset input value as string.
    pub(crate) offset_text: String,

    /// The id of the currently loaded saved query, if any.
    pub(crate) loaded_id: Option<String>,

    /// Weak handle back to the DataGridPanel that owns this builder.
    data_grid: Option<WeakEntity<DataGridPanel>>,

    /// Focus handle for the panel container.
    pub(crate) focus_handle: Option<FocusHandle>,

    /// Current SQL preview text (updated synchronously on spec change).
    pub(crate) sql_preview: String,

    /// Generator function: takes a spec, returns SQL preview text.
    ///
    /// Injected at construction time so the panel stays driver-agnostic.
    /// The closure calls `QueryGenerator::generate_select` on the driver's
    /// generator and materialises the SQL text for display.
    generate_preview: Box<dyn Fn(&VisualQuerySpec) -> String + Send + Sync>,

    /// Read-only code editor state backing the SQL preview widget.
    ///
    /// The editor uses SQL syntax highlighting and is disabled (no user edits).
    /// Synced from `sql_preview` during render via the `pending_preview_sync` flag.
    /// `None` only in unit tests that bypass the GPUI runtime via `make_panel`.
    pub(crate) sql_preview_state: Option<Entity<InputState>>,

    /// Set to `true` whenever `sql_preview` text changes, so the render cycle
    /// can flush the new text into `sql_preview_state` while `Window` is available.
    pub(crate) pending_preview_sync: bool,

    /// InputState backing the Limit field. Subscribed to `InputEvent::Change`.
    pub(crate) limit_input_state: Option<Entity<InputState>>,

    /// InputState backing the Offset field. Subscribed to `InputEvent::Change`.
    pub(crate) offset_input_state: Option<Entity<InputState>>,

    /// Per-join-row InputState pair: (to_table_input, on_expr_input).
    ///
    /// Length always matches `join_rows`. Rebuilt (with subscriptions) whenever
    /// the join row count changes.
    pub(crate) join_input_states: Vec<(Entity<InputState>, Entity<InputState>)>,

    /// All input subscriptions (limit, offset, join rows, filter predicates).
    ///
    /// Held here so they are cancelled when the panel is dropped. Join and filter
    /// subscriptions are rebuilt whenever the corresponding row counts change.
    pub(crate) _input_subs: Vec<Subscription>,

    /// Set to `true` when `join_rows` count changes, so the render cycle
    /// rebuilds `join_input_states` while `Window` is available.
    pub(crate) pending_join_rebuild: bool,

    /// InputState backing the "add column (alias.column)" entry field.
    pub(crate) add_column_input_state: Option<Entity<InputState>>,

    /// InputState backing the "add sort (alias.column)" entry field.
    pub(crate) add_sort_input_state: Option<Entity<InputState>>,

    /// Monotonically increasing counter used to mint stable `node_id` values for
    /// new `Predicate` nodes. The counter only moves forward; no value is reused.
    pub(crate) next_node_id: u64,

    /// Per-predicate value `InputState` keyed by `Predicate::node_id`.
    ///
    /// Created lazily in `render_panel` when a predicate node is first rendered.
    /// Entries are swept after render to remove stale keys (nodes that were deleted).
    pub(crate) predicate_input_states: HashMap<u64, Entity<InputState>>,

    /// Per-predicate column-reference `InputState` keyed by `Predicate::node_id`.
    /// Holds the dotted "alias.column" string editable by the user.
    pub(crate) predicate_column_input_states: HashMap<u64, Entity<InputState>>,

    /// Per-predicate comparator `Dropdown` keyed by `Predicate::node_id`. The
    /// dropdown carries one item per `Comparator` variant; the subscription
    /// applies the selected variant to the predicate.
    pub(crate) predicate_comparator_dropdowns: HashMap<u64, Entity<Dropdown>>,

    /// Per-join-row kind `Dropdown` (parallel to `join_rows`). Rebuilt whenever
    /// the join row count changes, mirroring `join_input_states`.
    pub(crate) join_kind_dropdowns: Vec<Entity<Dropdown>>,

    /// Per-join-condition input/dropdown state keyed by `JoinPredicate::node_id`.
    /// Holds the left/right `alias.column` inputs and the comparator dropdown.
    pub(crate) join_cond_left_inputs: HashMap<u64, Entity<InputState>>,
    pub(crate) join_cond_right_inputs: HashMap<u64, Entity<InputState>>,
    pub(crate) join_cond_op_dropdowns: HashMap<u64, Entity<Dropdown>>,

    /// Names of columns available on the source table, used to render the
    /// per-column checklist when projection is not "All columns". Populated
    /// once at panel construction from the data grid's current result.
    pub(crate) available_columns: Vec<String>,

    /// Set to `true` after any filter mutation so the render cycle sweeps stale
    /// entries from `predicate_input_states`.
    pub(crate) pending_filter_input_sweep: bool,

    /// Set to `true` after any mutation that can orphan join-condition node ids
    /// (remove a join row, remove a node from a join tree, or load a spec that
    /// replaces `join_rows`) so the render cycle sweeps stale entries from the
    /// join-condition HashMaps.
    pub(crate) pending_join_condition_sweep: bool,
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl QueryBuilderPanel {
    /// Creates a new panel for the given source table.
    ///
    /// `initial_spec` should be `DataGridPanel.builder_draft_spec` if the user had
    /// previously run the builder; `None` produces the default spec.
    /// `generate_preview` is a closure that calls the driver's
    /// `QueryGenerator::generate_select` and returns the SQL text.
    pub fn new(
        source: SourceTable,
        initial_spec: Option<VisualQuerySpec>,
        data_grid: Option<WeakEntity<DataGridPanel>>,
        available_columns: Vec<String>,
        generate_preview: Box<dyn Fn(&VisualQuerySpec) -> String + Send + Sync>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut next_node_id = 0u64;
        let mut spec = initial_spec.unwrap_or_else(|| VisualQuerySpec {
            source: source.clone(),
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(DEFAULT_LIMIT),
            offset: 0,
        });
        spec.reassign_node_ids(&mut next_node_id);

        let (projection_mode, projection_rows) = match &spec.projection {
            Projection::All => (ProjectionMode::All, Vec::new()),
            Projection::Explicit(cols) => {
                let rows = cols
                    .iter()
                    .map(|c| ProjectionRow {
                        source_alias: c.source_alias.clone(),
                        column: c.column.clone(),
                        alias: c.alias.clone(),
                    })
                    .collect();
                (ProjectionMode::Explicit, rows)
            }
        };

        let join_rows: Vec<JoinRow> = spec
            .joins
            .iter()
            .map(|j| JoinRow {
                kind: j.kind,
                from_alias: j.from_alias.clone(),
                from_column: match &j.on {
                    JoinOn::FkPath { from_column, .. } => from_column.clone(),
                    JoinOn::RawExpression(_) | JoinOn::Conditions(_) => String::new(),
                },
                to_schema: j.to_schema.clone(),
                to_table: j.to_table.clone(),
                to_alias: j.to_alias.clone(),
                on: j.on.clone(),
            })
            .collect();

        let sort_rows: Vec<SortRow> = spec
            .sort
            .iter()
            .map(|s| SortRow {
                source_alias: s.source_alias.clone(),
                column: s.column.clone(),
                direction: s.direction, // SortEntry.direction is VisualSortDirection
            })
            .collect();

        let limit_text = spec
            .limit
            .map_or_else(|| "0".to_string(), |v| v.to_string());
        let offset_text = spec.offset.to_string();

        let sql_preview = generate_preview(&spec);
        let focus_handle = Some(cx.focus_handle());

        let sql_preview_state = cx.new(|cx| InputState::new(window, cx).code_editor("sql"));

        let limit_val = limit_text.clone();
        let limit_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("100");
            state.set_value(&limit_val, window, cx);
            state
        });

        let offset_val = offset_text.clone();
        let offset_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("0");
            state.set_value(&offset_val, window, cx);
            state
        });

        let add_column_input_state =
            cx.new(|cx| InputState::new(window, cx).placeholder("alias.column"));

        let add_sort_input_state =
            cx.new(|cx| InputState::new(window, cx).placeholder("alias.column"));

        let limit_sub = cx.subscribe_in(
            &limit_input_state,
            window,
            |this, entity, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    let text = entity.read(cx).value().to_string();
                    this.set_limit_text(&text.clone(), cx);
                    let _ = (window, text);
                }
            },
        );

        let offset_sub = cx.subscribe_in(
            &offset_input_state,
            window,
            |this, entity, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    let text = entity.read(cx).value().to_string();
                    this.set_offset_text(&text.clone(), cx);
                    let _ = (window, text);
                }
            },
        );

        Self {
            current_spec: spec,
            projection_mode,
            projection_rows,
            join_rows,
            sort_rows,
            fk_state: FkLoadState::Loading,
            fk_banner_dismissed: false,
            limit_text,
            offset_text,
            loaded_id: None,
            data_grid,
            focus_handle,
            sql_preview,
            generate_preview,
            sql_preview_state: Some(sql_preview_state),
            pending_preview_sync: true,
            limit_input_state: Some(limit_input_state),
            offset_input_state: Some(offset_input_state),
            join_input_states: Vec::new(),
            _input_subs: vec![limit_sub, offset_sub],
            pending_join_rebuild: false,
            add_column_input_state: Some(add_column_input_state),
            add_sort_input_state: Some(add_sort_input_state),
            next_node_id,
            predicate_input_states: HashMap::new(),
            predicate_column_input_states: HashMap::new(),
            predicate_comparator_dropdowns: HashMap::new(),
            join_kind_dropdowns: Vec::new(),
            join_cond_left_inputs: HashMap::new(),
            join_cond_right_inputs: HashMap::new(),
            join_cond_op_dropdowns: HashMap::new(),
            available_columns,
            pending_filter_input_sweep: false,
            pending_join_condition_sweep: false,
        }
    }

    /// Returns the current SQL preview text.
    pub fn sql_preview(&self) -> &str {
        &self.sql_preview
    }

    /// Returns the current spec.
    pub fn current_spec(&self) -> &VisualQuerySpec {
        &self.current_spec
    }

    /// Returns the identifier of the persisted spec that was last loaded into
    /// this panel, if any.
    pub fn loaded_id(&self) -> Option<&str> {
        self.loaded_id.as_deref()
    }

    /// Replaces the panel's spec entirely (e.g. when the inspector re-opens
    /// and needs to re-hydrate from `DataGridPanel.builder_draft_spec`).
    pub fn set_spec(&mut self, spec: VisualQuerySpec, cx: &mut Context<Self>) {
        let (projection_mode, projection_rows) = match &spec.projection {
            Projection::All => (ProjectionMode::All, Vec::new()),
            Projection::Explicit(cols) => {
                let rows = cols
                    .iter()
                    .map(|c| ProjectionRow {
                        source_alias: c.source_alias.clone(),
                        column: c.column.clone(),
                        alias: c.alias.clone(),
                    })
                    .collect();
                (ProjectionMode::Explicit, rows)
            }
        };

        let join_rows: Vec<JoinRow> = spec
            .joins
            .iter()
            .map(|j| JoinRow {
                kind: j.kind,
                from_alias: j.from_alias.clone(),
                from_column: match &j.on {
                    JoinOn::FkPath { from_column, .. } => from_column.clone(),
                    JoinOn::RawExpression(_) | JoinOn::Conditions(_) => String::new(),
                },
                to_schema: j.to_schema.clone(),
                to_table: j.to_table.clone(),
                to_alias: j.to_alias.clone(),
                on: j.on.clone(),
            })
            .collect();

        let sort_rows: Vec<SortRow> = spec
            .sort
            .iter()
            .map(|s| SortRow {
                source_alias: s.source_alias.clone(),
                column: s.column.clone(),
                direction: s.direction, // SortEntry.direction is VisualSortDirection
            })
            .collect();

        self.limit_text = spec
            .limit
            .map_or_else(|| "0".to_string(), |v| v.to_string());
        self.offset_text = spec.offset.to_string();
        self.projection_mode = projection_mode;
        self.projection_rows = projection_rows;
        self.join_rows = join_rows;
        self.pending_join_condition_sweep = true;
        self.sort_rows = sort_rows;
        self.sql_preview = (self.generate_preview)(&spec);
        self.pending_preview_sync = true;
        self.current_spec = spec;
        cx.notify();
    }

    // -----------------------------------------------------------------------
    // Projection mutations
    // -----------------------------------------------------------------------

    /// Enables or disables "all columns (*)" mode.
    ///
    /// Disabling preserves the projection rows that were active before all-
    /// columns was toggled on; if none are saved, it defaults to no columns
    /// (the user must add them).
    pub fn set_all_columns(&mut self, all: bool, cx: &mut Context<Self>) {
        self.projection_mode = if all {
            ProjectionMode::All
        } else {
            ProjectionMode::Explicit
        };
        self.rebuild_spec_and_notify(cx);
    }

    /// Adds a column to the explicit projection list.
    ///
    /// No-op if the same `(source_alias, column)` pair already exists.
    pub fn add_column(&mut self, source_alias: &str, column: &str, cx: &mut Context<Self>) {
        let already_present = self
            .projection_rows
            .iter()
            .any(|r| r.source_alias == source_alias && r.column == column);

        if already_present {
            return;
        }

        self.projection_rows.push(ProjectionRow {
            source_alias: source_alias.to_string(),
            column: column.to_string(),
            alias: None,
        });

        if self.projection_mode == ProjectionMode::All {
            self.projection_mode = ProjectionMode::Explicit;
        }

        self.rebuild_spec_and_notify(cx);
    }

    /// Removes a column from the explicit projection list by its index.
    pub fn remove_column(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.projection_rows.len() {
            self.projection_rows.remove(index);
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Moves the column at `from_index` to `to_index`.
    pub fn reorder_column(&mut self, from_index: usize, to_index: usize, cx: &mut Context<Self>) {
        if from_index < self.projection_rows.len() && to_index < self.projection_rows.len() {
            let row = self.projection_rows.remove(from_index);
            self.projection_rows.insert(to_index, row);
            self.rebuild_spec_and_notify(cx);
        }
    }

    // -----------------------------------------------------------------------
    // Filter mutations
    // -----------------------------------------------------------------------

    /// Replaces the root filter node.
    pub fn set_filter(&mut self, filter: Option<FilterNode>, cx: &mut Context<Self>) {
        self.current_spec.filter = filter;
        self.refresh_preview_and_notify(cx);
    }

    /// Returns `true` if adding a group at the given current depth would
    /// exceed the cap.
    ///
    /// `current_depth` is the depth of the node the user is trying to nest
    /// inside. A predicate at depth 1 (inside the root group) has depth 1.
    pub fn would_exceed_depth_cap(&self, current_depth: usize) -> bool {
        current_depth >= FILTER_DEPTH_CAP
    }

    /// Adds a new empty predicate to the filter tree.
    ///
    /// If there is no root filter, creates a root `AND` group with one empty predicate.
    /// Otherwise, appends to the root group (shallow append; for nested groups the
    /// path-based variant would be used when the UI needs it).
    pub fn add_predicate(
        &mut self,
        parent_path: Vec<usize>,
        source_alias: &str,
        column: &str,
        cx: &mut Context<Self>,
    ) {
        self.next_node_id += 1;
        let new_pred = FilterNode::Predicate(Predicate {
            source_alias: source_alias.to_string(),
            column: column.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Text(String::new())),
            node_id: self.next_node_id,
        });

        match &mut self.current_spec.filter {
            None => {
                self.current_spec.filter = Some(FilterNode::Group {
                    op: BoolOp::And,
                    children: vec![new_pred],
                });
            }
            Some(root) => {
                insert_filter_at_path(root, &parent_path, new_pred);
            }
        }

        self.pending_filter_input_sweep = true;
        self.refresh_preview_and_notify(cx);
    }

    /// Adds a new empty sub-group to the filter tree at `parent_path`.
    pub fn add_group(&mut self, parent_path: Vec<usize>, cx: &mut Context<Self>) {
        let new_group = FilterNode::Group {
            op: BoolOp::And,
            children: Vec::new(),
        };

        match &mut self.current_spec.filter {
            None => {
                self.current_spec.filter = Some(new_group);
            }
            Some(root) => {
                insert_filter_at_path(root, &parent_path, new_group);
            }
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Collects the `node_id` of every `Predicate` in the filter tree.
    ///
    /// Used by the render cycle to sweep stale entries from `predicate_input_states`.
    pub fn collect_predicate_node_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();
        if let Some(root) = &self.current_spec.filter {
            collect_filter_predicate_ids(root, &mut ids);
        }
        ids
    }

    /// Removes the filter node at `path` from its parent group.
    pub fn remove_filter_node(&mut self, path: Vec<usize>, cx: &mut Context<Self>) {
        if path.is_empty() {
            self.current_spec.filter = None;
        } else {
            if let Some(root) = &mut self.current_spec.filter {
                remove_filter_at_path(root, &path);
            }

            if let Some(FilterNode::Group { children, .. }) = &self.current_spec.filter
                && children.is_empty()
            {
                self.current_spec.filter = None;
            }
        }

        self.pending_filter_input_sweep = true;
        self.refresh_preview_and_notify(cx);
    }

    /// Toggles the boolean operator (AND ↔ OR) of the group at `path`.
    pub fn toggle_group_op(&mut self, path: Vec<usize>, cx: &mut Context<Self>) {
        if let Some(root) = &mut self.current_spec.filter
            && let Some(FilterNode::Group { op, .. }) = filter_node_at_path_mut(root, &path)
        {
            *op = match *op {
                BoolOp::And => BoolOp::Or,
                BoolOp::Or => BoolOp::And,
            };
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Cycles the comparator of the predicate at `path` through the operator list.
    pub fn cycle_predicate_comparator(&mut self, path: Vec<usize>, cx: &mut Context<Self>) {
        if let Some(root) = &mut self.current_spec.filter
            && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &path)
        {
            pred.comparator = Self::next_comparator(pred.comparator);
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Updates the text value of the predicate at `path`.
    pub fn set_predicate_value(&mut self, path: Vec<usize>, text: String, cx: &mut Context<Self>) {
        if let Some(root) = &mut self.current_spec.filter
            && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &path)
        {
            pred.value = PredicateValue::Single(LiteralValue::Text(text));
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Ensures a value `InputState` exists for the predicate at `node_id`.
    ///
    /// Creates a new `Entity<InputState>` seeded from `current_value` and subscribes
    /// to `InputEvent::Change` to call `set_predicate_value(path, text)` when the
    /// user types. Idempotent: if an entry already exists for `node_id`, does nothing.
    ///
    /// `path` must be the current path to the predicate node (used by the subscription
    /// to route the value mutation to the correct node in the tree).
    pub fn ensure_predicate_input(
        &mut self,
        node_id: u64,
        path: Vec<usize>,
        current_value: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.predicate_input_states.contains_key(&node_id) {
            return;
        }

        let value = current_value.to_string();
        let state = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("<value>");
            s.set_value(&value, window, cx);
            s
        });

        let sub = cx.subscribe_in(
            &state,
            window,
            move |this, entity, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    let text = entity.read(cx).value().to_string();
                    this.set_predicate_value(path.clone(), text, cx);
                    let _ = window;
                }
            },
        );

        self.predicate_input_states.insert(node_id, state);
        self._input_subs.push(sub);
    }

    /// Returns true when an explicit projection entry exists matching the
    /// given `(alias, column)` pair.
    pub fn is_column_selected(&self, alias: &str, column: &str) -> bool {
        self.projection_rows
            .iter()
            .any(|r| r.source_alias == alias && r.column == column)
    }

    /// Adds or removes a column from the explicit projection. Used by the
    /// per-column checklist so the user can toggle without typing.
    pub fn toggle_column(&mut self, alias: &str, column: &str, cx: &mut Context<Self>) {
        match self
            .projection_rows
            .iter()
            .position(|r| r.source_alias == alias && r.column == column)
        {
            Some(idx) => self.remove_column(idx, cx),
            None => self.add_column(alias, column, cx),
        }
    }

    /// Updates the column reference of the predicate at `path` from a dotted
    /// "alias.column" string. If the input lacks a dot the whole string is
    /// treated as the column and the alias is preserved.
    pub fn set_predicate_column_ref(
        &mut self,
        path: Vec<usize>,
        text: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(root) = &mut self.current_spec.filter
            && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &path)
        {
            match text.split_once('.') {
                Some((alias, column)) => {
                    pred.source_alias = alias.trim().to_string();
                    pred.column = column.trim().to_string();
                }
                None => {
                    pred.column = text.trim().to_string();
                }
            }
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Ensures a column-reference `InputState` exists for the predicate at `node_id`.
    /// Seeded from the current dotted "alias.column" string; subscribes to
    /// `InputEvent::Change` to call `set_predicate_column_ref`.
    pub fn ensure_predicate_column_input(
        &mut self,
        node_id: u64,
        path: Vec<usize>,
        current_text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.predicate_column_input_states.contains_key(&node_id) {
            return;
        }

        let value = current_text.to_string();
        let state = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("alias.column");
            s.set_value(&value, window, cx);
            s
        });

        let sub = cx.subscribe_in(
            &state,
            window,
            move |this, entity, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    let text = entity.read(cx).value().to_string();
                    this.set_predicate_column_ref(path.clone(), text, cx);
                    let _ = window;
                }
            },
        );

        self.predicate_column_input_states.insert(node_id, state);
        self._input_subs.push(sub);
    }

    /// Sets the comparator of the predicate at `path`.
    pub fn set_predicate_comparator(
        &mut self,
        path: Vec<usize>,
        comparator: Comparator,
        cx: &mut Context<Self>,
    ) {
        if let Some(root) = &mut self.current_spec.filter
            && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &path)
        {
            pred.comparator = comparator;
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Ensures a `Dropdown` entity exists for the predicate at `node_id` with
    /// one item per `Comparator` variant. Subscribes to `DropdownSelectionChanged`
    /// to apply the selection.
    pub fn ensure_predicate_comparator_dropdown(
        &mut self,
        node_id: u64,
        path: Vec<usize>,
        current: Comparator,
        cx: &mut Context<Self>,
    ) {
        if self.predicate_comparator_dropdowns.contains_key(&node_id) {
            return;
        }

        let items: Vec<DropdownItem> = COMPARATOR_ORDER
            .iter()
            .map(|c| DropdownItem::with_value(comparator_label(*c), comparator_value(*c)))
            .collect();

        let selected = COMPARATOR_ORDER.iter().position(|c| *c == current);

        let dropdown = cx.new(|_cx| {
            Dropdown::new(("qb-pred-cmp-dd", node_id))
                .items(items)
                .selected_index(selected)
                .toolbar_style(true)
        });

        let path_for_sub = path;
        let sub = cx.subscribe(
            &dropdown,
            move |this, _entity, event: &DropdownSelectionChanged, cx| {
                if let Some(comparator) = COMPARATOR_ORDER.get(event.index).copied() {
                    this.set_predicate_comparator(path_for_sub.clone(), comparator, cx);
                }
            },
        );

        self.predicate_comparator_dropdowns
            .insert(node_id, dropdown);
        self._input_subs.push(sub);
    }

    /// Sweeps `predicate_input_states` to remove entries whose `node_id` is no
    /// longer present in the filter tree. Call after any filter mutation that may
    /// have removed predicates.
    pub fn sweep_stale_predicate_inputs(&mut self) {
        let live_ids = self.collect_predicate_node_ids();
        self.predicate_input_states
            .retain(|id, _| live_ids.contains(id));
        self.predicate_column_input_states
            .retain(|id, _| live_ids.contains(id));
        self.predicate_comparator_dropdowns
            .retain(|id, _| live_ids.contains(id));
    }

    /// Rebuilds `join_input_states` and its subscriptions from the current `join_rows`.
    ///
    /// Call after any operation that adds or removes join rows so the InputState
    /// count matches the row count.
    pub fn rebuild_join_input_states(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let target_len = self.join_rows.len();
        let current_len = self.join_input_states.len();

        if current_len == target_len {
            return;
        }

        // On shrink, we cannot just truncate: the retained `InputState` entities
        // still display the text from the removed-or-shifted ordinal positions,
        // while the subscriptions captured an `idx` that now points to a
        // different `JoinRow`. Result: user sees stale labels but mutations
        // land on the shifted row. Clear and rebuild from scratch instead.
        if target_len < current_len {
            self.join_input_states.clear();
            self.join_kind_dropdowns.clear();
        }

        let start = self.join_input_states.len();
        if target_len > start {
            for i in start..target_len {
                let to_table_val = self.join_rows[i].to_table.clone();
                let on_expr_val = match &self.join_rows[i].on {
                    JoinOn::RawExpression(expr) => expr.clone(),
                    JoinOn::FkPath {
                        from_column,
                        to_column,
                    } => format!(
                        "{}.{} = {}.{}",
                        self.join_rows[i].from_alias,
                        from_column,
                        self.join_rows[i].to_alias,
                        to_column
                    ),
                    // Conditions are edited via dedicated per-predicate inputs
                    // rather than a single raw textbox, so the raw input is
                    // initialised empty when the row uses structured mode.
                    JoinOn::Conditions(_) => String::new(),
                };

                let to_table_state = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("table");
                    state.set_value(&to_table_val, window, cx);
                    state
                });

                let on_expr_state = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("a.id = b.a_id");
                    state.set_value(&on_expr_val, window, cx);
                    state
                });

                let idx = i;
                let to_table_sub = cx.subscribe_in(
                    &to_table_state,
                    window,
                    move |this, entity, event: &InputEvent, window, cx| {
                        if matches!(event, InputEvent::Change) {
                            let text = entity.read(cx).value().to_string();
                            if let Some(row) = this.join_rows.get(idx).cloned() {
                                let updated = JoinRow {
                                    to_table: text,
                                    to_alias: row.to_alias.clone(),
                                    ..row
                                };
                                this.update_join(idx, updated, cx);
                            }
                            let _ = window;
                        }
                    },
                );

                let on_expr_sub = cx.subscribe_in(
                    &on_expr_state,
                    window,
                    move |this, entity, event: &InputEvent, window, cx| {
                        if matches!(event, InputEvent::Change) {
                            let text = entity.read(cx).value().to_string();
                            if let Some(row) = this.join_rows.get(idx).cloned() {
                                let updated = JoinRow {
                                    on: JoinOn::RawExpression(text),
                                    ..row
                                };
                                this.update_join(idx, updated, cx);
                            }
                            let _ = window;
                        }
                    },
                );

                self.join_input_states.push((to_table_state, on_expr_state));
                self._input_subs.push(to_table_sub);
                self._input_subs.push(on_expr_sub);

                let kind_items: Vec<DropdownItem> = JOIN_KIND_ORDER
                    .iter()
                    .map(|k| DropdownItem::with_value(join_kind_label(*k), join_kind_label(*k)))
                    .collect();
                let kind_selected = JOIN_KIND_ORDER
                    .iter()
                    .position(|k| *k == self.join_rows[i].kind);
                let kind_dropdown = cx.new(|_cx| {
                    Dropdown::new(("qb-join-kind-dd", i))
                        .items(kind_items)
                        .selected_index(kind_selected)
                        .toolbar_style(true)
                });

                let idx_for_kind = i;
                let kind_sub = cx.subscribe(
                    &kind_dropdown,
                    move |this, _entity, event: &DropdownSelectionChanged, cx| {
                        if let Some(kind) = JOIN_KIND_ORDER.get(event.index).copied()
                            && let Some(row) = this.join_rows.get(idx_for_kind).cloned()
                        {
                            this.update_join(idx_for_kind, JoinRow { kind, ..row }, cx);
                        }
                    },
                );

                self.join_kind_dropdowns.push(kind_dropdown);
                self._input_subs.push(kind_sub);
            }
        }
    }

    fn next_comparator(current: Comparator) -> Comparator {
        match current {
            Comparator::Eq => Comparator::Neq,
            Comparator::Neq => Comparator::Lt,
            Comparator::Lt => Comparator::Lte,
            Comparator::Lte => Comparator::Gt,
            Comparator::Gt => Comparator::Gte,
            Comparator::Gte => Comparator::Like,
            Comparator::Like => Comparator::ILike,
            Comparator::ILike => Comparator::In,
            Comparator::In => Comparator::IsNull,
            Comparator::IsNull => Comparator::IsNotNull,
            Comparator::IsNotNull => Comparator::Eq,
        }
    }

    // -----------------------------------------------------------------------
    // Join mutations
    // -----------------------------------------------------------------------

    /// Appends a new join row defaulting to a single empty condition under an AND root.
    pub fn add_join(&mut self, from_alias: &str, cx: &mut Context<Self>) {
        self.next_node_id += 1;
        let root_id = self.next_node_id;
        self.next_node_id += 1;
        let first_pred = JoinPredicate {
            node_id: self.next_node_id,
            left: String::new(),
            op: Comparator::Eq,
            right: String::new(),
        };
        self.join_rows.push(JoinRow {
            kind: JoinKind::Inner,
            from_alias: from_alias.to_string(),
            from_column: String::new(),
            to_schema: None,
            to_table: String::new(),
            to_alias: String::new(),
            on: JoinOn::Conditions(JoinFilterNode::Group {
                node_id: root_id,
                op: BoolOp::And,
                children: vec![JoinFilterNode::Predicate(first_pred)],
            }),
        });
        self.pending_join_rebuild = true;
        self.rebuild_spec_and_notify(cx);
    }

    /// Appends a new empty `JoinPredicate` at `path` inside the join tree at `join_idx`.
    pub fn add_join_condition(
        &mut self,
        join_idx: usize,
        path: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let new_id = {
            self.next_node_id += 1;
            self.next_node_id
        };
        let new_pred = JoinFilterNode::Predicate(JoinPredicate {
            node_id: new_id,
            left: String::new(),
            op: Comparator::Eq,
            right: String::new(),
        });
        if let Some(row) = self.join_rows.get_mut(join_idx)
            && let JoinOn::Conditions(root) = &mut row.on
            && let Some(JoinFilterNode::Group { children, .. }) = join_node_at_path_mut(root, &path)
        {
            children.push(new_pred);
        }
        self.rebuild_spec_and_notify(cx);
    }

    /// Appends a new empty AND sub-group at `path` inside the join tree at `join_idx`.
    pub fn add_join_subgroup(&mut self, join_idx: usize, path: Vec<usize>, cx: &mut Context<Self>) {
        let (group_id, pred_id) = {
            self.next_node_id += 1;
            let g = self.next_node_id;
            self.next_node_id += 1;
            (g, self.next_node_id)
        };
        let new_group = JoinFilterNode::Group {
            node_id: group_id,
            op: BoolOp::Or,
            children: vec![JoinFilterNode::Predicate(JoinPredicate {
                node_id: pred_id,
                left: String::new(),
                op: Comparator::Eq,
                right: String::new(),
            })],
        };
        if let Some(row) = self.join_rows.get_mut(join_idx)
            && let JoinOn::Conditions(root) = &mut row.on
            && let Some(JoinFilterNode::Group { children, .. }) = join_node_at_path_mut(root, &path)
        {
            children.push(new_group);
        }
        self.rebuild_spec_and_notify(cx);
    }

    /// Toggles AND ↔ OR for the group at `path` inside join `join_idx`.
    pub fn toggle_join_group_op(
        &mut self,
        join_idx: usize,
        path: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        if let Some(row) = self.join_rows.get_mut(join_idx)
            && let JoinOn::Conditions(root) = &mut row.on
            && let Some(JoinFilterNode::Group { op, .. }) = join_node_at_path_mut(root, &path)
        {
            *op = match op {
                BoolOp::And => BoolOp::Or,
                BoolOp::Or => BoolOp::And,
            };
            self.refresh_preview_and_notify(cx);
        }
    }

    /// Removes the node at `path` from join `join_idx`. Root is never removed
    /// (the join still owns a Conditions root); call `remove_join` to drop the
    /// whole row.
    pub fn remove_join_node(&mut self, join_idx: usize, path: Vec<usize>, cx: &mut Context<Self>) {
        if path.is_empty() {
            return;
        }
        let (parent_path, last) = (&path[..path.len() - 1], path[path.len() - 1]);
        if let Some(row) = self.join_rows.get_mut(join_idx)
            && let JoinOn::Conditions(root) = &mut row.on
            && let Some(JoinFilterNode::Group { children, .. }) =
                join_node_at_path_mut(root, parent_path)
            && last < children.len()
        {
            children.remove(last);
        }
        self.pending_join_condition_sweep = true;
        self.rebuild_spec_and_notify(cx);
    }

    /// Updates the left side of the predicate identified by `node_id` anywhere
    /// in any join tree.
    pub fn set_join_condition_left(&mut self, node_id: u64, text: String, cx: &mut Context<Self>) {
        let mut applied = false;
        let mut setter = |p: &mut JoinPredicate| p.left = text.clone();
        for row in self.join_rows.iter_mut() {
            if let JoinOn::Conditions(root) = &mut row.on
                && set_join_predicate_field(root, node_id, &mut setter)
            {
                applied = true;
                break;
            }
        }
        if applied {
            self.refresh_preview_and_notify(cx);
        }
    }

    /// Updates the right side of the predicate identified by `node_id`.
    pub fn set_join_condition_right(&mut self, node_id: u64, text: String, cx: &mut Context<Self>) {
        let mut applied = false;
        let mut setter = |p: &mut JoinPredicate| p.right = text.clone();
        for row in self.join_rows.iter_mut() {
            if let JoinOn::Conditions(root) = &mut row.on
                && set_join_predicate_field(root, node_id, &mut setter)
            {
                applied = true;
                break;
            }
        }
        if applied {
            self.refresh_preview_and_notify(cx);
        }
    }

    /// Updates the comparator of the predicate identified by `node_id`.
    pub fn set_join_condition_op(&mut self, node_id: u64, op: Comparator, cx: &mut Context<Self>) {
        let mut applied = false;
        let mut setter = |p: &mut JoinPredicate| p.op = op;
        for row in self.join_rows.iter_mut() {
            if let JoinOn::Conditions(root) = &mut row.on
                && set_join_predicate_field(root, node_id, &mut setter)
            {
                applied = true;
                break;
            }
        }
        if applied {
            self.refresh_preview_and_notify(cx);
        }
    }

    /// Ensures input states + comparator dropdown exist for the condition.
    #[allow(clippy::map_entry)]
    pub fn ensure_join_condition_state(
        &mut self,
        node_id: u64,
        left: &str,
        right: &str,
        op: Comparator,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.join_cond_left_inputs.contains_key(&node_id) {
            let left_owned = left.to_string();
            let state = cx.new(|cx| {
                let mut s = InputState::new(window, cx).placeholder("alias.column");
                s.set_value(&left_owned, window, cx);
                s
            });
            let id_for_sub = node_id;
            let sub = cx.subscribe_in(
                &state,
                window,
                move |this, entity, event: &InputEvent, window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        this.set_join_condition_left(id_for_sub, text, cx);
                        let _ = window;
                    }
                },
            );
            self.join_cond_left_inputs.insert(node_id, state);
            self._input_subs.push(sub);
        }

        if !self.join_cond_right_inputs.contains_key(&node_id) {
            let right_owned = right.to_string();
            let state = cx.new(|cx| {
                let mut s = InputState::new(window, cx).placeholder("alias.column");
                s.set_value(&right_owned, window, cx);
                s
            });
            let id_for_sub = node_id;
            let sub = cx.subscribe_in(
                &state,
                window,
                move |this, entity, event: &InputEvent, window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        this.set_join_condition_right(id_for_sub, text, cx);
                        let _ = window;
                    }
                },
            );
            self.join_cond_right_inputs.insert(node_id, state);
            self._input_subs.push(sub);
        }

        if !self.join_cond_op_dropdowns.contains_key(&node_id) {
            let items: Vec<DropdownItem> = COMPARATOR_ORDER
                .iter()
                .map(|c| DropdownItem::with_value(comparator_label(*c), comparator_value(*c)))
                .collect();
            let selected = COMPARATOR_ORDER.iter().position(|c| *c == op);
            let dropdown = cx.new(|_cx| {
                Dropdown::new(("qb-join-cond-op", node_id))
                    .items(items)
                    .selected_index(selected)
                    .toolbar_style(true)
            });
            let id_for_sub = node_id;
            let sub = cx.subscribe(
                &dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    if let Some(op) = COMPARATOR_ORDER.get(event.index).copied() {
                        this.set_join_condition_op(id_for_sub, op, cx);
                    }
                },
            );
            self.join_cond_op_dropdowns.insert(node_id, dropdown);
            self._input_subs.push(sub);
        }
    }

    /// Sweeps stale join-condition state when nodes are removed from any tree.
    pub fn sweep_stale_join_condition_state(&mut self) {
        let mut live: HashSet<u64> = HashSet::new();
        for row in &self.join_rows {
            if let JoinOn::Conditions(root) = &row.on {
                collect_join_predicate_ids(root, &mut live);
            }
        }
        self.join_cond_left_inputs.retain(|id, _| live.contains(id));
        self.join_cond_right_inputs
            .retain(|id, _| live.contains(id));
        self.join_cond_op_dropdowns
            .retain(|id, _| live.contains(id));
    }

    /// Removes a join row by its index.
    pub fn remove_join(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.join_rows.len() {
            self.join_rows.remove(index);
            self.pending_join_rebuild = true;
            self.pending_join_condition_sweep = true;
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Updates the join at `index`.
    pub fn update_join(&mut self, index: usize, row: JoinRow, cx: &mut Context<Self>) {
        if index < self.join_rows.len() {
            self.join_rows[index] = row;
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Applies the result of the FK background fetch.
    pub fn apply_fk_result(
        &mut self,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
        cx: &mut Context<Self>,
    ) {
        self.fk_state = if foreign_keys.is_empty() {
            FkLoadState::Unavailable
        } else {
            FkLoadState::Ready(foreign_keys)
        };
        cx.notify();
    }

    /// Marks FK metadata as unavailable (fetch failed).
    pub fn mark_fk_unavailable(&mut self, cx: &mut Context<Self>) {
        self.fk_state = FkLoadState::Unavailable;
        cx.notify();
    }

    /// Dismisses the FK unavailability banner.
    pub fn dismiss_fk_banner(&mut self, cx: &mut Context<Self>) {
        self.fk_banner_dismissed = true;
        cx.notify();
    }

    // -----------------------------------------------------------------------
    // Sort mutations
    // -----------------------------------------------------------------------

    /// Appends a sort row.
    pub fn add_sort(&mut self, source_alias: &str, column: &str, cx: &mut Context<Self>) {
        self.sort_rows.push(SortRow {
            source_alias: source_alias.to_string(),
            column: column.to_string(),
            direction: VisualSortDirection::Asc,
        });
        self.rebuild_spec_and_notify(cx);
    }

    /// Removes a sort row by index.
    pub fn remove_sort(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.sort_rows.len() {
            self.sort_rows.remove(index);
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Toggles the direction of the sort entry at `index`.
    pub fn toggle_sort_direction(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(row) = self.sort_rows.get_mut(index) {
            row.direction = match row.direction {
                VisualSortDirection::Asc => VisualSortDirection::Desc,
                VisualSortDirection::Desc => VisualSortDirection::Asc,
            };
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Moves the sort row at `from_index` to `to_index`.
    pub fn reorder_sort(&mut self, from_index: usize, to_index: usize, cx: &mut Context<Self>) {
        if from_index < self.sort_rows.len() && to_index < self.sort_rows.len() {
            let row = self.sort_rows.remove(from_index);
            self.sort_rows.insert(to_index, row);
            self.rebuild_spec_and_notify(cx);
        }
    }

    // -----------------------------------------------------------------------
    // Limit / Offset mutations
    // -----------------------------------------------------------------------

    /// Updates the limit text. Accepts only digit characters; ignores non-numeric input.
    pub fn set_limit_text(&mut self, text: &str, cx: &mut Context<Self>) {
        let sanitized: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
        self.limit_text = sanitized;
        self.rebuild_spec_and_notify(cx);
    }

    /// Updates the offset text. Accepts only digit characters.
    pub fn set_offset_text(&mut self, text: &str, cx: &mut Context<Self>) {
        let sanitized: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
        self.offset_text = sanitized;
        self.rebuild_spec_and_notify(cx);
    }

    // -----------------------------------------------------------------------
    // Operator helpers (column-kind gating)
    // -----------------------------------------------------------------------

    /// Returns the set of comparators that are valid for a given `ColumnKind`.
    ///
    /// Used by the filter operator dropdown to show only applicable operators.
    pub fn operators_for_kind(kind: ColumnKind) -> &'static [Comparator] {
        match kind {
            ColumnKind::Timestamp => &[
                Comparator::Eq,
                Comparator::Neq,
                Comparator::Gt,
                Comparator::Lt,
                Comparator::Gte,
                Comparator::Lte,
                Comparator::IsNull,
                Comparator::IsNotNull,
            ],
            ColumnKind::Integer | ColumnKind::Float => &[
                Comparator::Eq,
                Comparator::Neq,
                Comparator::Gt,
                Comparator::Lt,
                Comparator::Gte,
                Comparator::Lte,
                Comparator::In,
                Comparator::IsNull,
                Comparator::IsNotNull,
            ],
            // Text and Unknown both use the text operator set.
            // The wildcard is required because ColumnKind is #[non_exhaustive].
            _ => &[
                Comparator::Eq,
                Comparator::Neq,
                Comparator::Like,
                Comparator::ILike,
                Comparator::In,
                Comparator::IsNull,
                Comparator::IsNotNull,
            ],
        }
    }

    // -----------------------------------------------------------------------
    // Internal spec reconstruction
    // -----------------------------------------------------------------------

    /// Rebuilds `current_spec` from the panel's mutable row data without
    /// interacting with the GPUI context. Called from both the notify path
    /// and from unit tests.
    pub(crate) fn rebuild_spec_pure(&mut self) {
        let projection = match self.projection_mode {
            ProjectionMode::All => Projection::All,
            ProjectionMode::Explicit => Projection::Explicit(
                self.projection_rows
                    .iter()
                    .map(|r| r.to_projected_column())
                    .collect(),
            ),
        };

        let joins: Vec<JoinStep> = self.join_rows.iter().map(|r| r.to_join_step()).collect();
        let sort: Vec<SortEntry> = self.sort_rows.iter().map(|r| r.to_sort_entry()).collect();

        let limit = match self.limit_text.parse::<u64>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n),
        };

        let offset = self.offset_text.parse::<u64>().unwrap_or(0);

        self.current_spec.projection = projection;
        self.current_spec.joins = joins;
        self.current_spec.sort = sort;
        self.current_spec.limit = limit;
        self.current_spec.offset = offset;

        let spec = self.current_spec.clone();
        self.sql_preview = (self.generate_preview)(&spec);
        self.pending_preview_sync = true;
    }

    /// Rebuilds `current_spec` from the panel's mutable row data, then updates
    /// the SQL preview and notifies GPUI.
    fn rebuild_spec_and_notify(&mut self, cx: &mut Context<Self>) {
        self.rebuild_spec_pure();
        cx.emit(BuilderEvent::SpecChanged(Box::new(
            self.current_spec.clone(),
        )));
        cx.notify();
    }

    /// Recomputes the SQL preview from `current_spec` and notifies GPUI.
    fn refresh_preview_and_notify(&mut self, cx: &mut Context<Self>) {
        let spec = self.current_spec.clone();
        self.sql_preview = (self.generate_preview)(&spec);
        self.pending_preview_sync = true;
        cx.emit(BuilderEvent::SpecChanged(Box::new(
            self.current_spec.clone(),
        )));
        cx.notify();
    }

    /// Returns the current limit as a `u64`, or `None` when zero / unparseable.
    pub fn current_limit(&self) -> Option<u64> {
        match self.limit_text.parse::<u64>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n),
        }
    }

    /// Returns the current offset as a `u64`.
    pub fn current_offset(&self) -> u64 {
        self.offset_text.parse::<u64>().unwrap_or(0)
    }

    /// Returns `true` when the panel has a valid spec that can be executed.
    pub fn is_runnable(&self) -> bool {
        self.current_spec.is_runnable().is_ok()
    }

    /// The weak handle to the owning `DataGridPanel`, if one was provided.
    pub fn data_grid(&self) -> Option<&WeakEntity<DataGridPanel>> {
        self.data_grid.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Dropdown helpers
// ---------------------------------------------------------------------------

const COMPARATOR_ORDER: &[Comparator] = &[
    Comparator::Eq,
    Comparator::Neq,
    Comparator::Gt,
    Comparator::Lt,
    Comparator::Gte,
    Comparator::Lte,
    Comparator::Like,
    Comparator::ILike,
    Comparator::In,
    Comparator::IsNull,
    Comparator::IsNotNull,
];

const JOIN_KIND_ORDER: &[JoinKind] = &[
    JoinKind::Inner,
    JoinKind::Left,
    JoinKind::Right,
    JoinKind::Full,
];

fn comparator_label(c: Comparator) -> &'static str {
    match c {
        Comparator::Eq => "=",
        Comparator::Neq => "≠",
        Comparator::Gt => ">",
        Comparator::Lt => "<",
        Comparator::Gte => "≥",
        Comparator::Lte => "≤",
        Comparator::Like => "LIKE",
        Comparator::ILike => "ILIKE",
        Comparator::In => "IN",
        Comparator::IsNull => "IS NULL",
        Comparator::IsNotNull => "IS NOT NULL",
    }
}

fn comparator_value(c: Comparator) -> &'static str {
    // Stable identifier per variant; matches the label except for spaces in
    // multi-word operators, which would otherwise be ambiguous with other UI
    // strings if items were ever compared by label.
    match c {
        Comparator::Eq => "eq",
        Comparator::Neq => "neq",
        Comparator::Gt => "gt",
        Comparator::Lt => "lt",
        Comparator::Gte => "gte",
        Comparator::Lte => "lte",
        Comparator::Like => "like",
        Comparator::ILike => "ilike",
        Comparator::In => "in",
        Comparator::IsNull => "is_null",
        Comparator::IsNotNull => "is_not_null",
    }
}

fn join_kind_label(k: JoinKind) -> &'static str {
    match k {
        JoinKind::Inner => "INNER",
        JoinKind::Left => "LEFT",
        JoinKind::Right => "RIGHT",
        JoinKind::Full => "FULL",
    }
}

// ---------------------------------------------------------------------------
// GPUI integration
// ---------------------------------------------------------------------------

impl EventEmitter<BuilderEvent> for QueryBuilderPanel {}

impl Render for QueryBuilderPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl gpui::IntoElement {
        crate::query_builder::view::render_panel(self, window, cx)
    }
}

// ---------------------------------------------------------------------------
// Tests — pure state-machine only (no GPUI runtime needed)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        BoolOp, Comparator, FilterNode, JoinKind, JoinOn, LiteralValue, Predicate, PredicateValue,
        Projection, SourceTable, VisualQuerySpec, VisualSortDirection,
    };

    // ---- helpers -----------------------------------------------------------

    fn test_source() -> SourceTable {
        SourceTable {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            alias: "users".to_string(),
        }
    }

    fn make_spec(source: SourceTable) -> VisualQuerySpec {
        VisualQuerySpec {
            source,
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    fn no_op_preview(_spec: &VisualQuerySpec) -> String {
        "SELECT * FROM users".to_string()
    }

    /// Builds a panel directly (bypassing GPUI context) for pure unit tests.
    ///
    /// GPUI handle fields (`focus_handle`, `data_grid`) are set to `None`.
    /// Tests MUST only call the `t_*` helpers defined below, which route
    /// through `rebuild_spec_pure()` and never touch those handles.
    fn make_panel(spec: VisualQuerySpec) -> QueryBuilderPanel {
        let projection_rows = Vec::new();
        let join_rows: Vec<JoinRow> = spec
            .joins
            .iter()
            .map(|j| JoinRow {
                kind: j.kind,
                from_alias: j.from_alias.clone(),
                from_column: match &j.on {
                    JoinOn::FkPath { from_column, .. } => from_column.clone(),
                    JoinOn::RawExpression(_) | JoinOn::Conditions(_) => String::new(),
                },
                to_schema: j.to_schema.clone(),
                to_table: j.to_table.clone(),
                to_alias: j.to_alias.clone(),
                on: j.on.clone(),
            })
            .collect();

        let sort_rows: Vec<SortRow> = spec
            .sort
            .iter()
            .map(|s| SortRow {
                source_alias: s.source_alias.clone(),
                column: s.column.clone(),
                direction: s.direction, // SortEntry.direction is VisualSortDirection
            })
            .collect();

        let limit_text = spec
            .limit
            .map_or_else(|| "0".to_string(), |v| v.to_string());
        let offset_text = spec.offset.to_string();
        let sql_preview = no_op_preview(&spec);

        QueryBuilderPanel {
            current_spec: spec,
            projection_mode: ProjectionMode::All,
            projection_rows,
            join_rows,
            sort_rows,
            fk_state: FkLoadState::Loading,
            fk_banner_dismissed: false,
            limit_text,
            offset_text,
            loaded_id: None,
            data_grid: None,
            focus_handle: None,
            sql_preview,
            generate_preview: Box::new(no_op_preview),
            sql_preview_state: None,
            pending_preview_sync: false,
            limit_input_state: None,
            offset_input_state: None,
            join_input_states: Vec::new(),
            _input_subs: Vec::new(),
            pending_join_rebuild: false,
            add_column_input_state: None,
            add_sort_input_state: None,
            next_node_id: 0,
            predicate_input_states: HashMap::new(),
            predicate_column_input_states: HashMap::new(),
            predicate_comparator_dropdowns: HashMap::new(),
            join_kind_dropdowns: Vec::new(),
            join_cond_left_inputs: HashMap::new(),
            join_cond_right_inputs: HashMap::new(),
            join_cond_op_dropdowns: HashMap::new(),
            available_columns: Vec::new(),
            pending_filter_input_sweep: false,
            pending_join_condition_sweep: false,
        }
    }

    /// Test-only extension methods that call `rebuild_spec_pure()` rather than
    /// `rebuild_spec_and_notify(cx)`, avoiding the need for a live GPUI context.
    impl QueryBuilderPanel {
        fn t_add_column(&mut self, source_alias: &str, column: &str) {
            let already = self
                .projection_rows
                .iter()
                .any(|r| r.source_alias == source_alias && r.column == column);
            if already {
                return;
            }
            self.projection_rows.push(ProjectionRow {
                source_alias: source_alias.to_string(),
                column: column.to_string(),
                alias: None,
            });
            if self.projection_mode == ProjectionMode::All {
                self.projection_mode = ProjectionMode::Explicit;
            }
            self.rebuild_spec_pure();
        }

        fn t_remove_column(&mut self, index: usize) {
            if index < self.projection_rows.len() {
                self.projection_rows.remove(index);
                self.rebuild_spec_pure();
            }
        }

        fn t_reorder_column(&mut self, from: usize, to: usize) {
            if from < self.projection_rows.len() && to < self.projection_rows.len() {
                let row = self.projection_rows.remove(from);
                self.projection_rows.insert(to, row);
                self.rebuild_spec_pure();
            }
        }

        fn t_set_all_columns(&mut self, all: bool) {
            self.projection_mode = if all {
                ProjectionMode::All
            } else {
                ProjectionMode::Explicit
            };
            self.rebuild_spec_pure();
        }

        fn t_add_sort(&mut self, source_alias: &str, column: &str) {
            self.sort_rows.push(SortRow {
                source_alias: source_alias.to_string(),
                column: column.to_string(),
                direction: VisualSortDirection::Asc,
            });
            self.rebuild_spec_pure();
        }

        fn t_toggle_sort_direction(&mut self, index: usize) {
            if let Some(row) = self.sort_rows.get_mut(index) {
                row.direction = match row.direction {
                    VisualSortDirection::Asc => VisualSortDirection::Desc,
                    VisualSortDirection::Desc => VisualSortDirection::Asc,
                };
                self.rebuild_spec_pure();
            }
        }

        fn t_remove_sort(&mut self, index: usize) {
            if index < self.sort_rows.len() {
                self.sort_rows.remove(index);
                self.rebuild_spec_pure();
            }
        }

        fn t_reorder_sort(&mut self, from: usize, to: usize) {
            if from < self.sort_rows.len() && to < self.sort_rows.len() {
                let row = self.sort_rows.remove(from);
                self.sort_rows.insert(to, row);
                self.rebuild_spec_pure();
            }
        }

        fn t_add_join(&mut self, from_alias: &str) {
            self.join_rows.push(JoinRow {
                kind: JoinKind::Inner,
                from_alias: from_alias.to_string(),
                from_column: String::new(),
                to_schema: None,
                to_table: String::new(),
                to_alias: String::new(),
                on: JoinOn::RawExpression(String::new()),
            });
            self.rebuild_spec_pure();
        }

        fn t_remove_join(&mut self, index: usize) {
            if index < self.join_rows.len() {
                self.join_rows.remove(index);
                self.rebuild_spec_pure();
            }
        }

        fn t_update_join(&mut self, index: usize, row: JoinRow) {
            if index < self.join_rows.len() {
                self.join_rows[index] = row;
                self.rebuild_spec_pure();
            }
        }

        fn t_apply_fk_result(&mut self, foreign_keys: Vec<SchemaForeignKeyInfo>) {
            self.fk_state = if foreign_keys.is_empty() {
                FkLoadState::Unavailable
            } else {
                FkLoadState::Ready(foreign_keys)
            };
        }

        fn t_mark_fk_unavailable(&mut self) {
            self.fk_state = FkLoadState::Unavailable;
        }

        fn t_dismiss_fk_banner(&mut self) {
            self.fk_banner_dismissed = true;
        }

        fn t_set_limit_text(&mut self, text: &str) {
            let sanitized: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
            self.limit_text = sanitized;
            self.rebuild_spec_pure();
        }

        fn t_set_offset_text(&mut self, text: &str) {
            let sanitized: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
            self.offset_text = sanitized;
            self.rebuild_spec_pure();
        }
    }

    // ---- 4.1: default spec on construction --------------------------------

    #[test]
    fn default_spec_has_all_projection_and_limit_100() {
        let panel = make_panel(make_spec(test_source()));

        assert_eq!(panel.projection_mode, ProjectionMode::All);
        assert_eq!(panel.current_spec.projection, Projection::All);
        assert_eq!(panel.current_limit(), Some(100));
        assert_eq!(panel.current_offset(), 0);
        assert!(panel.current_spec.filter.is_none());
        assert!(panel.current_spec.joins.is_empty());
        assert!(panel.current_spec.sort.is_empty());
    }

    #[test]
    fn is_runnable_with_valid_table() {
        let panel = make_panel(make_spec(test_source()));
        assert!(panel.is_runnable());
    }

    #[test]
    fn is_not_runnable_with_empty_table_name() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: String::new(),
                alias: "t".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        };
        let panel = make_panel(spec);
        assert!(!panel.is_runnable());
    }

    // ---- 4.2: columns section state machine --------------------------------

    #[test]
    fn add_column_switches_to_explicit_mode() {
        let mut panel = make_panel(make_spec(test_source()));
        assert_eq!(panel.projection_mode, ProjectionMode::All);

        panel.t_add_column("users", "email");

        assert_eq!(panel.projection_mode, ProjectionMode::Explicit);
        assert_eq!(panel.projection_rows.len(), 1);
        assert_eq!(panel.projection_rows[0].column, "email");
    }

    #[test]
    fn add_column_is_noop_when_duplicate() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_column("users", "email");
        panel.t_add_column("users", "email");

        assert_eq!(panel.projection_rows.len(), 1);
    }

    #[test]
    fn remove_column_by_index() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_column("users", "email");
        panel.t_add_column("users", "name");

        panel.t_remove_column(0);

        assert_eq!(panel.projection_rows.len(), 1);
        assert_eq!(panel.projection_rows[0].column, "name");
    }

    #[test]
    fn reorder_column_moves_item() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_column("users", "c");
        panel.t_add_column("users", "a");
        panel.t_add_column("users", "b");

        // Move "c" (index 0) to position 2 → order becomes [a, b, c]
        panel.t_reorder_column(0, 2);

        let cols: Vec<&str> = panel
            .projection_rows
            .iter()
            .map(|r| r.column.as_str())
            .collect();
        assert_eq!(cols, ["a", "b", "c"]);
    }

    #[test]
    fn set_all_columns_false_preserves_rows() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_column("users", "id");
        panel.t_add_column("users", "email");

        // Switch to all-columns
        panel.t_set_all_columns(true);
        assert_eq!(panel.projection_mode, ProjectionMode::All);

        // Switch back — rows are preserved
        panel.t_set_all_columns(false);
        assert_eq!(panel.projection_mode, ProjectionMode::Explicit);
        assert_eq!(panel.projection_rows.len(), 2);
    }

    // ---- 4.2: sort section state machine -----------------------------------

    #[test]
    fn add_sort_defaults_to_asc() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_sort("users", "name");

        assert_eq!(panel.sort_rows.len(), 1);
        assert_eq!(panel.sort_rows[0].direction, VisualSortDirection::Asc);
    }

    #[test]
    fn toggle_sort_direction_flips_asc_to_desc() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_sort("users", "name");
        panel.t_toggle_sort_direction(0);

        assert_eq!(panel.sort_rows[0].direction, VisualSortDirection::Desc);
    }

    #[test]
    fn toggle_sort_direction_flips_desc_to_asc() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_sort("users", "name");
        panel.t_toggle_sort_direction(0);
        panel.t_toggle_sort_direction(0);

        assert_eq!(panel.sort_rows[0].direction, VisualSortDirection::Asc);
    }

    #[test]
    fn remove_sort_removes_entry() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_sort("users", "name");
        panel.t_add_sort("users", "created_at");
        panel.t_remove_sort(0);

        assert_eq!(panel.sort_rows.len(), 1);
        assert_eq!(panel.sort_rows[0].column, "created_at");
    }

    #[test]
    fn reorder_sort_moves_entry() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_sort("users", "name");
        panel.t_add_sort("users", "created_at");

        // Move "name" (0) to position 1
        panel.t_reorder_sort(0, 1);

        assert_eq!(panel.sort_rows[0].column, "created_at");
        assert_eq!(panel.sort_rows[1].column, "name");
    }

    // ---- 4.3: filter depth cap enforcement ---------------------------------

    #[test]
    fn would_exceed_depth_cap_at_cap_level() {
        let panel = make_panel(make_spec(test_source()));
        assert!(panel.would_exceed_depth_cap(FILTER_DEPTH_CAP));
    }

    #[test]
    fn would_not_exceed_depth_cap_below_cap() {
        let panel = make_panel(make_spec(test_source()));
        assert!(!panel.would_exceed_depth_cap(FILTER_DEPTH_CAP - 1));
    }

    #[test]
    fn depth_cap_value_is_six() {
        assert_eq!(FILTER_DEPTH_CAP, 6);
    }

    // ---- 4.4: FK state transitions -----------------------------------------

    #[test]
    fn initial_fk_state_is_loading() {
        let panel = make_panel(make_spec(test_source()));
        assert!(matches!(panel.fk_state, FkLoadState::Loading));
    }

    #[test]
    fn apply_fk_result_transitions_to_ready() {
        let mut panel = make_panel(make_spec(test_source()));
        let fk = SchemaForeignKeyInfo {
            name: "fk_users_org".to_string(),
            table_name: "users".to_string(),
            columns: vec!["org_id".to_string()],
            referenced_schema: Some("public".to_string()),
            referenced_table: "organizations".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        };
        panel.t_apply_fk_result(vec![fk.clone()]);

        assert!(panel.fk_state.is_ready());
        assert_eq!(panel.fk_state.foreign_keys().map(|fks| fks.len()), Some(1));
        assert_eq!(
            panel.fk_state.foreign_keys().unwrap()[0].name,
            "fk_users_org"
        );
    }

    #[test]
    fn apply_fk_result_empty_transitions_to_unavailable() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_apply_fk_result(vec![]);
        assert!(panel.fk_state.is_unavailable());
    }

    #[test]
    fn mark_fk_unavailable_transitions_from_loading() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_mark_fk_unavailable();
        assert!(panel.fk_state.is_unavailable());
    }

    #[test]
    fn fk_banner_starts_not_dismissed() {
        let panel = make_panel(make_spec(test_source()));
        assert!(!panel.fk_banner_dismissed);
    }

    #[test]
    fn dismiss_fk_banner_sets_flag() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_mark_fk_unavailable();
        panel.t_dismiss_fk_banner();
        assert!(panel.fk_banner_dismissed);
    }

    // ---- 4.4: join state machine -------------------------------------------

    #[test]
    fn add_join_appends_row() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_join("users");

        assert_eq!(panel.join_rows.len(), 1);
        assert_eq!(panel.join_rows[0].from_alias, "users");
        assert!(matches!(panel.join_rows[0].on, JoinOn::RawExpression(_)));
    }

    #[test]
    fn remove_join_removes_row() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_join("users");
        panel.t_remove_join(0);
        assert!(panel.join_rows.is_empty());
    }

    #[test]
    fn update_join_replaces_row() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_join("users");

        let updated = JoinRow {
            kind: JoinKind::Left,
            from_alias: "users".to_string(),
            from_column: "org_id".to_string(),
            to_schema: None,
            to_table: "organizations".to_string(),
            to_alias: "org".to_string(),
            on: JoinOn::FkPath {
                from_column: "org_id".to_string(),
                to_column: "id".to_string(),
            },
        };
        panel.t_update_join(0, updated.clone());

        assert_eq!(panel.join_rows[0].kind, JoinKind::Left);
        assert_eq!(panel.join_rows[0].to_table, "organizations");
        assert!(matches!(
            &panel.join_rows[0].on,
            JoinOn::FkPath { from_column, to_column }
            if from_column == "org_id" && to_column == "id"
        ));
    }

    // ---- 4.5: limit / offset numeric enforcement ---------------------------

    #[test]
    fn set_limit_text_accepts_digits() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_set_limit_text("50");
        assert_eq!(panel.current_limit(), Some(50));
    }

    #[test]
    fn set_limit_text_rejects_non_numeric() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_set_limit_text("abc");
        // All chars filtered → empty → parses as None
        assert_eq!(panel.current_limit(), None);
    }

    #[test]
    fn set_limit_text_zero_becomes_none() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_set_limit_text("0");
        assert_eq!(panel.current_limit(), None);
    }

    #[test]
    fn set_offset_text_accepts_digits() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_set_offset_text("20");
        assert_eq!(panel.current_offset(), 20);
    }

    #[test]
    fn set_offset_text_rejects_non_numeric() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_set_offset_text("xyz");
        assert_eq!(panel.current_offset(), 0);
    }

    // ---- operators_for_kind ------------------------------------------------

    #[test]
    fn operators_for_text_includes_like_ilike_in() {
        let ops = QueryBuilderPanel::operators_for_kind(ColumnKind::Text);
        assert!(ops.contains(&Comparator::Like));
        assert!(ops.contains(&Comparator::ILike));
        assert!(ops.contains(&Comparator::In));
    }

    #[test]
    fn operators_for_integer_includes_numeric_range() {
        let ops = QueryBuilderPanel::operators_for_kind(ColumnKind::Integer);
        assert!(ops.contains(&Comparator::Gt));
        assert!(ops.contains(&Comparator::Lt));
        assert!(ops.contains(&Comparator::Gte));
        assert!(ops.contains(&Comparator::Lte));
    }

    #[test]
    fn operators_for_timestamp_excludes_like() {
        let ops = QueryBuilderPanel::operators_for_kind(ColumnKind::Timestamp);
        assert!(!ops.contains(&Comparator::Like));
        assert!(!ops.contains(&Comparator::ILike));
    }

    #[test]
    fn operators_for_unknown_falls_back_to_text_operators() {
        let ops = QueryBuilderPanel::operators_for_kind(ColumnKind::Unknown);
        assert!(ops.contains(&Comparator::Like));
    }

    // ---- spec is rebuilt from row data ------------------------------------

    #[test]
    fn rebuilt_spec_reflects_join_rows() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_join("users");
        panel.t_update_join(
            0,
            JoinRow {
                kind: JoinKind::Inner,
                from_alias: "users".to_string(),
                from_column: "org_id".to_string(),
                to_schema: None,
                to_table: "orgs".to_string(),
                to_alias: "orgs".to_string(),
                on: JoinOn::FkPath {
                    from_column: "org_id".to_string(),
                    to_column: "id".to_string(),
                },
            },
        );

        assert_eq!(panel.current_spec.joins.len(), 1);
        assert_eq!(panel.current_spec.joins[0].to_table, "orgs");
    }

    #[test]
    fn rebuilt_spec_has_no_limit_when_zero() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_set_limit_text("0");
        assert!(panel.current_spec.limit.is_none());
    }

    #[test]
    fn rebuilt_spec_has_no_order_by_when_no_sorts() {
        let panel = make_panel(make_spec(test_source()));
        assert!(panel.current_spec.sort.is_empty());
    }

    // ---- Slice 7: node_id + predicate value inputs --------------------------

    fn t_add_predicate(panel: &mut QueryBuilderPanel, parent_path: Vec<usize>, column: &str) {
        panel.next_node_id += 1;
        let new_pred = FilterNode::Predicate(Predicate {
            source_alias: "users".to_string(),
            column: column.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Text(String::new())),
            node_id: panel.next_node_id,
        });
        match &mut panel.current_spec.filter {
            None => {
                panel.current_spec.filter = Some(FilterNode::Group {
                    op: BoolOp::And,
                    children: vec![new_pred],
                });
            }
            Some(root) => {
                insert_filter_at_path(root, &parent_path, new_pred);
            }
        }
        panel.pending_filter_input_sweep = true;
        panel.rebuild_spec_pure();
    }

    #[test]
    fn add_predicate_assigns_nonzero_node_id() {
        let mut panel = make_panel(make_spec(test_source()));
        t_add_predicate(&mut panel, vec![], "email");

        if let Some(FilterNode::Group { children, .. }) = &panel.current_spec.filter {
            if let FilterNode::Predicate(pred) = &children[0] {
                assert_ne!(pred.node_id, 0, "node_id must be non-zero");
            } else {
                panic!("expected predicate");
            }
        } else {
            panic!("expected group at root");
        }
    }

    #[test]
    fn set_predicate_value_updates_correct_node_in_nested_tree() {
        let mut panel = make_panel(make_spec(test_source()));
        t_add_predicate(&mut panel, vec![], "email");
        t_add_predicate(&mut panel, vec![], "name");

        // Update value at path [0] (email predicate).
        if let Some(root) = &mut panel.current_spec.filter {
            if let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &[0]) {
                pred.value = PredicateValue::Single(LiteralValue::Text("%foo%".to_string()));
            }
        }
        panel.rebuild_spec_pure();

        // Verify email predicate has the new value.
        if let Some(FilterNode::Group { children, .. }) = &panel.current_spec.filter {
            if let FilterNode::Predicate(pred) = &children[0] {
                assert_eq!(
                    pred.value,
                    PredicateValue::Single(LiteralValue::Text("%foo%".to_string()))
                );
            } else {
                panic!("expected predicate at [0]");
            }

            if let FilterNode::Predicate(pred) = &children[1] {
                assert_eq!(
                    pred.value,
                    PredicateValue::Single(LiteralValue::Text(String::new())),
                    "name predicate must be unchanged"
                );
            } else {
                panic!("expected predicate at [1]");
            }
        } else {
            panic!("expected AND group at root");
        }
    }

    #[test]
    fn collect_predicate_node_ids_returns_all_leaf_ids() {
        let mut panel = make_panel(make_spec(test_source()));
        t_add_predicate(&mut panel, vec![], "email");
        t_add_predicate(&mut panel, vec![], "name");

        let ids = panel.collect_predicate_node_ids();
        assert_eq!(ids.len(), 2);
        for id in &ids {
            assert_ne!(*id, 0);
        }
    }

    #[test]
    fn collect_predicate_node_ids_returns_empty_when_no_filter() {
        let panel = make_panel(make_spec(test_source()));
        let ids = panel.collect_predicate_node_ids();
        assert!(ids.is_empty());
    }

    #[test]
    fn collect_predicate_node_ids_after_remove_excludes_deleted_node() {
        let mut panel = make_panel(make_spec(test_source()));
        t_add_predicate(&mut panel, vec![], "email");
        t_add_predicate(&mut panel, vec![], "name");

        let before_ids = panel.collect_predicate_node_ids();
        assert_eq!(before_ids.len(), 2);

        // Remove the first predicate (index 0).
        if let Some(root) = &mut panel.current_spec.filter {
            remove_filter_at_path(root, &[0]);
        }
        panel.rebuild_spec_pure();

        let after_ids = panel.collect_predicate_node_ids();
        assert_eq!(after_ids.len(), 1, "only one predicate should remain");

        // The removed id must not be in the live set.
        for removed in before_ids.difference(&after_ids) {
            assert!(
                !after_ids.contains(removed),
                "removed node id should not be in live set"
            );
        }
    }
}
