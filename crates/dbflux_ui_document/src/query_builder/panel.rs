use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dbflux_components::controls::{
    CompletionProvider, Dropdown, DropdownItem, DropdownSelectionChanged, InputEvent, InputState,
};
use dbflux_core::{
    AggFn, BoolOp, ColumnInfo, ColumnKind, Comparator, FilterNode, GroupByEntry, JoinFilterNode,
    JoinKind, JoinOn, JoinPredicate, JoinStep, LiteralValue, Predicate, PredicateValue,
    ProjectedColumn, Projection, SchemaForeignKeyInfo, SelectQuery, SortEntry, SourceTable,
    VisualAggregateSpec, VisualQuerySpec, VisualSortDirection,
};
use gpui::{
    App, AppContext, Context, Entity, EventEmitter, FocusHandle, Render, Subscription, WeakEntity,
    Window,
};
use uuid::Uuid;

use dbflux_ui_base::AppStateEntity;

use crate::data_grid_panel::DataGridPanel;
use crate::query_builder::completion::{
    AliasBinding, CompletionMode, SchemaCache, SchemaCompletionProvider,
};
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

/// Discriminant that routes filter-tree mutations to either the WHERE or
/// HAVING predicate tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterTarget {
    Where,
    Having,
}

/// A single group-by entry as tracked by the builder panel.
#[derive(Debug, Clone)]
pub struct GroupByRow {
    pub source_alias: String,
    pub column: String,
}

impl GroupByRow {
    fn to_group_by_entry(&self) -> GroupByEntry {
        GroupByEntry {
            source_alias: self.source_alias.clone(),
            column: self.column.clone(),
        }
    }
}

/// A single aggregate row as tracked by the builder panel.
#[derive(Debug, Clone)]
pub struct AggregateRow {
    pub function: AggFn,
    pub source_alias: String,
    pub column: String,
    pub alias: String,
}

impl AggregateRow {
    fn to_aggregate_spec(&self) -> VisualAggregateSpec {
        if self.function == AggFn::CountStar {
            VisualAggregateSpec {
                function: self.function,
                source_alias: None,
                column: None,
                alias: self.alias.clone(),
            }
        } else {
            VisualAggregateSpec {
                function: self.function,
                source_alias: Some(self.source_alias.clone()),
                column: Some(self.column.clone()),
                alias: self.alias.clone(),
            }
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

    /// Shared schema cache for completion providers.
    ///
    /// `source_columns` is populated at panel construction. `joined_columns`
    /// is populated lazily via `ensure_joined_columns`. The cache is shared
    /// via `Rc<RefCell<…>>` between the panel (writer) and all attached
    /// `SchemaCompletionProvider` instances (readers). Everything runs on the
    /// foreground thread, so `RefCell` is sufficient.
    pub(crate) schema_cache: Rc<RefCell<SchemaCache>>,

    /// Weak handle to `AppStateEntity`, used inside `ensure_joined_columns`
    /// to reach the active connection without capturing a strong reference.
    pub(crate) app_state_weak: WeakEntity<AppStateEntity>,

    /// Profile ID for this builder panel, used to look up the connection.
    pub(crate) schema_profile_id: Uuid,

    /// Set to `true` after any filter mutation so the render cycle sweeps stale
    /// entries from `predicate_input_states`.
    pub(crate) pending_filter_input_sweep: bool,

    /// Set to `true` after any mutation that can orphan join-condition node ids
    /// (remove a join row, remove a node from a join tree, or load a spec that
    /// replaces `join_rows`) so the render cycle sweeps stale entries from the
    /// join-condition HashMaps.
    pub(crate) pending_join_condition_sweep: bool,

    /// Group-by column rows in display order.
    pub(crate) group_by_rows: Vec<GroupByRow>,

    /// Aggregate rows in display order.
    pub(crate) aggregate_rows: Vec<AggregateRow>,

    /// Per-group-by-row column text input (parallel to `group_by_rows`).
    ///
    /// Each entry holds an `InputState` for the "alias.column" text field.
    /// Rebuilt whenever the group-by row count changes.
    pub(crate) group_by_col_inputs: Vec<Entity<InputState>>,

    /// Per-aggregate-row function `Dropdown` (parallel to `aggregate_rows`).
    pub(crate) agg_fn_dropdowns: Vec<Entity<Dropdown>>,

    /// Per-aggregate-row column text input (parallel to `aggregate_rows`).
    ///
    /// Holds an `InputState` for the "alias.column" reference. Disabled (read-only)
    /// when the selected function is `CountStar`.
    pub(crate) agg_col_inputs: Vec<Entity<InputState>>,

    /// Per-aggregate-row alias text input (parallel to `aggregate_rows`).
    pub(crate) agg_alias_inputs: Vec<Entity<InputState>>,

    /// Set to `true` whenever `group_by_rows` or `aggregate_rows` count changes so
    /// the render cycle can rebuild the per-row entity vectors.
    pub(crate) pending_group_by_rebuild: bool,

    /// Per-predicate value `InputState` for HAVING predicates, keyed by node_id.
    pub(crate) having_predicate_input_states: HashMap<u64, Entity<InputState>>,

    /// Per-predicate column-reference `InputState` for HAVING predicates, keyed by node_id.
    pub(crate) having_predicate_column_input_states: HashMap<u64, Entity<InputState>>,

    /// Per-predicate comparator `Dropdown` for HAVING predicates, keyed by node_id.
    pub(crate) having_predicate_comparator_dropdowns: HashMap<u64, Entity<Dropdown>>,

    /// Set to `true` after any HAVING filter mutation so the render cycle
    /// sweeps stale entries from `having_predicate_*` maps.
    pub(crate) pending_having_input_sweep: bool,

    /// Snapshot of the projection before entering grouped mode.
    ///
    /// Builder UI state only — NOT serialized on VisualQuerySpec.
    /// Captured when the first group-by or aggregate row is added while
    /// `projection == All`. Restored when all group-by and aggregate rows
    /// are removed. If `None`, no snapshot was taken (either the spec was
    /// loaded already grouped, or the user started with `Explicit`).
    pub(crate) pre_group_projection: Option<Projection>,

    /// Non-empty when a sort entry was rejected because the column is not in
    /// the current group-by / aggregate alias set.
    ///
    /// Cleared when the sort input loses focus or the user adds a valid entry.
    pub(crate) sort_validation_error: Option<String>,

    /// Count of aggregate rows that are present in the UI but excluded from
    /// `spec.aggregates` because they are incomplete (empty column for
    /// non-CountStar functions). Surfaced as a footer warning.
    pub(crate) incomplete_aggregate_row_count: usize,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: SourceTable,
        initial_spec: Option<VisualQuerySpec>,
        data_grid: Option<WeakEntity<DataGridPanel>>,
        available_columns: Vec<String>,
        app_state: Entity<AppStateEntity>,
        profile_id: Uuid,
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
            group_by: vec![],
            aggregates: vec![],
            having: None,
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

        let source_columns: Vec<ColumnInfo> = {
            let state = app_state.read(cx);
            let source_table_name = spec.source.table.clone();
            let source_schema = spec.source.schema.clone();

            state
                .connections()
                .get(&profile_id)
                .and_then(|conn| {
                    let db_name = conn
                        .active_database
                        .clone()
                        .or(source_schema)
                        .unwrap_or_else(|| "default".to_string());
                    conn.table_details
                        .get(&(db_name, source_table_name))
                        .and_then(|info| info.columns.clone())
                })
                .unwrap_or_default()
        };

        let schema_cache = Rc::new(RefCell::new(SchemaCache {
            source_table: spec.source.table.clone(),
            source_columns,
            joined_columns: HashMap::new(),
            fk_links: HashMap::new(),
            fetching: HashSet::new(),
            failed: HashSet::new(),
        }));

        if schema_cache.borrow().source_columns.is_empty() {
            Self::spawn_source_columns_fetch(
                schema_cache.clone(),
                app_state.downgrade(),
                profile_id,
                spec.source.schema.clone(),
                spec.source.table.clone(),
                cx,
            );
        }

        let source_alias_binding = AliasBinding {
            alias: spec.source.alias.clone(),
            schema: spec.source.schema.clone(),
            table: spec.source.table.clone(),
            is_source: true,
        };

        let app_state_weak = app_state.downgrade();

        let add_column_input_state =
            cx.new(|cx| InputState::new(window, cx).placeholder("alias.column"));

        let add_sort_input_state =
            cx.new(|cx| InputState::new(window, cx).placeholder("alias.column"));

        let alias_or_column_provider: Rc<dyn CompletionProvider> =
            Rc::new(SchemaCompletionProvider::new(
                app_state_weak.clone(),
                profile_id,
                CompletionMode::AliasOrColumn {
                    aliases: vec![source_alias_binding.clone()],
                },
                schema_cache.clone(),
            ));

        add_column_input_state.update(cx, |state, _| {
            state.lsp.completion_provider = Some(alias_or_column_provider.clone());
        });

        add_sort_input_state.update(cx, |state, _| {
            state.lsp.completion_provider = Some(alias_or_column_provider.clone());
        });

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

        let group_by_rows: Vec<GroupByRow> = spec
            .group_by
            .iter()
            .map(|g| GroupByRow {
                source_alias: g.source_alias.clone(),
                column: g.column.clone(),
            })
            .collect();

        let aggregate_rows: Vec<AggregateRow> = spec
            .aggregates
            .iter()
            .map(|a| AggregateRow {
                function: a.function,
                source_alias: a.source_alias.clone().unwrap_or_default(),
                column: a.column.clone().unwrap_or_default(),
                alias: a.alias.clone(),
            })
            .collect();

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
            schema_cache,
            app_state_weak,
            schema_profile_id: profile_id,
            pending_filter_input_sweep: false,
            pending_join_condition_sweep: false,
            group_by_rows,
            aggregate_rows,
            group_by_col_inputs: Vec::new(),
            agg_fn_dropdowns: Vec::new(),
            agg_col_inputs: Vec::new(),
            agg_alias_inputs: Vec::new(),
            pending_group_by_rebuild: false,
            having_predicate_input_states: HashMap::new(),
            having_predicate_column_input_states: HashMap::new(),
            having_predicate_comparator_dropdowns: HashMap::new(),
            pending_having_input_sweep: false,
            pre_group_projection: None,
            sort_validation_error: None,
            incomplete_aggregate_row_count: 0,
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
        self.set_spec_pure(spec);
        cx.notify();
    }

    /// Pure-state variant of `set_spec`: replaces every mutable row vector
    /// from `spec`, sets the join-rebuild and join-condition-sweep flags so
    /// the next render reconciles per-row entities and orphaned node-id
    /// state, and recomputes the SQL preview. Exposed for unit tests; the
    /// public entry point calls this and then `cx.notify`.
    pub(crate) fn set_spec_pure(&mut self, spec: VisualQuerySpec) {
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
                direction: s.direction,
            })
            .collect();

        let group_by_rows: Vec<GroupByRow> = spec
            .group_by
            .iter()
            .map(|g| GroupByRow {
                source_alias: g.source_alias.clone(),
                column: g.column.clone(),
            })
            .collect();

        let aggregate_rows: Vec<AggregateRow> = spec
            .aggregates
            .iter()
            .map(|a| AggregateRow {
                function: a.function,
                source_alias: a.source_alias.clone().unwrap_or_default(),
                column: a.column.clone().unwrap_or_default(),
                alias: a.alias.clone(),
            })
            .collect();

        self.limit_text = spec
            .limit
            .map_or_else(|| "0".to_string(), |v| v.to_string());
        self.offset_text = spec.offset.to_string();
        self.projection_mode = projection_mode;
        self.projection_rows = projection_rows;
        self.join_rows = join_rows;
        self.group_by_rows = group_by_rows;
        self.aggregate_rows = aggregate_rows;
        // Clear per-row interactive entities; they will be rebuilt on next render.
        self.group_by_col_inputs.clear();
        self.agg_fn_dropdowns.clear();
        self.agg_col_inputs.clear();
        self.agg_alias_inputs.clear();
        self.pending_group_by_rebuild = true;
        // Sweep HAVING input states; they will be recreated on next render.
        self.having_predicate_input_states.clear();
        self.having_predicate_column_input_states.clear();
        self.having_predicate_comparator_dropdowns.clear();
        // Both flags are required: the rebuild flag drives the InputState /
        // Dropdown vec back to the new row count, the sweep flag drops node-id
        // entries that the previous spec owned.
        self.pending_join_rebuild = true;
        self.pending_join_condition_sweep = true;
        self.sort_rows = sort_rows;
        self.sql_preview = (self.generate_preview)(&spec);
        self.pending_preview_sync = true;
        self.pre_group_projection = if spec.is_grouped() {
            Some(Projection::All)
        } else {
            None
        };
        self.current_spec = spec;
        self.sort_validation_error = None;
        self.incomplete_aggregate_row_count = 0;
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

        let predicate_col_provider: Rc<dyn CompletionProvider> =
            Rc::new(SchemaCompletionProvider::new(
                self.app_state_weak.clone(),
                self.schema_profile_id,
                CompletionMode::AliasOrColumn {
                    aliases: self.make_alias_bindings(),
                },
                self.schema_cache.clone(),
            ));
        state.update(cx, |s, _| {
            s.lsp.completion_provider = Some(predicate_col_provider);
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

                let table_names = self
                    .app_state_weak
                    .upgrade()
                    .as_ref()
                    .and_then(|app| {
                        let state = app.read(cx);
                        let conn = state.connections().get(&self.schema_profile_id)?;
                        let schema = conn.schema.as_ref()?;
                        if let dbflux_core::DataStructure::Relational(rel) = &schema.structure {
                            let default_schema = conn
                                .active_database
                                .clone()
                                .or_else(|| rel.schemas.first().map(|s| s.name.clone()))
                                .unwrap_or_default();
                            let names: Vec<String> = rel
                                .schemas
                                .iter()
                                .flat_map(|s| {
                                    let default_schema = default_schema.clone();
                                    s.tables.iter().map(move |t| {
                                        if s.name == default_schema {
                                            t.name.clone()
                                        } else {
                                            format!("{}.{}", s.name, t.name)
                                        }
                                    })
                                })
                                .chain(rel.tables.iter().map(|t| t.name.clone()))
                                .collect();
                            Some(names)
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                let tables_provider: Rc<dyn CompletionProvider> =
                    Rc::new(SchemaCompletionProvider::new(
                        self.app_state_weak.clone(),
                        self.schema_profile_id,
                        CompletionMode::Tables {
                            table_names,
                            default_schema: None,
                        },
                        self.schema_cache.clone(),
                    ));

                to_table_state.update(cx, |state, _| {
                    state.lsp.completion_provider = Some(tables_provider);
                });

                if !to_table_val.is_empty() {
                    let row = &self.join_rows[i];
                    self.ensure_joined_columns(row.to_schema.as_deref(), &to_table_val, cx);
                }

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

        let refreshed_aliases = self.make_alias_bindings();
        let refreshed_provider: Rc<dyn CompletionProvider> =
            Rc::new(SchemaCompletionProvider::new(
                self.app_state_weak.clone(),
                self.schema_profile_id,
                CompletionMode::AliasOrColumn {
                    aliases: refreshed_aliases,
                },
                self.schema_cache.clone(),
            ));

        if let Some(col_input) = &self.add_column_input_state {
            let p = refreshed_provider.clone();
            col_input.update(cx, |s, _| s.lsp.completion_provider = Some(p));
        }

        if let Some(sort_input) = &self.add_sort_input_state {
            let p = refreshed_provider.clone();
            sort_input.update(cx, |s, _| s.lsp.completion_provider = Some(p));
        }
    }

    /// Rebuilds the per-row `InputState` and `Dropdown` entities for the
    /// Group-By and Aggregate sections from the current row vectors.
    ///
    /// Called from the render cycle when `pending_group_by_rebuild` is set.
    /// On any shrink, all per-row entities are cleared and rebuilt from scratch
    /// to avoid stale subscriptions pointing at shifted row indices.
    pub fn rebuild_group_by_input_states(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let gb_target = self.group_by_rows.len();
        let agg_target = self.aggregate_rows.len();

        let gb_needs_rebuild = self.group_by_col_inputs.len() != gb_target;
        let agg_needs_rebuild = self.agg_fn_dropdowns.len() != agg_target;

        if !gb_needs_rebuild && !agg_needs_rebuild {
            return;
        }

        if self.group_by_col_inputs.len() > gb_target {
            self.group_by_col_inputs.clear();
        }

        if self.agg_fn_dropdowns.len() > agg_target {
            self.agg_fn_dropdowns.clear();
            self.agg_col_inputs.clear();
            self.agg_alias_inputs.clear();
        }

        let alias_provider: Rc<dyn CompletionProvider> = Rc::new(SchemaCompletionProvider::new(
            self.app_state_weak.clone(),
            self.schema_profile_id,
            CompletionMode::AliasOrColumn {
                aliases: self.make_alias_bindings(),
            },
            self.schema_cache.clone(),
        ));

        let gb_start = self.group_by_col_inputs.len();
        for i in gb_start..gb_target {
            let col_text = {
                let row = &self.group_by_rows[i];
                if row.source_alias.is_empty() || row.source_alias == row.column {
                    row.column.clone()
                } else {
                    format!("{}.{}", row.source_alias, row.column)
                }
            };

            let col_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("alias.column");
                state.set_value(&col_text, window, cx);
                state
            });

            col_input.update(cx, |s, _| {
                s.lsp.completion_provider = Some(alias_provider.clone());
            });

            let sub = cx.subscribe_in(
                &col_input,
                window,
                move |this, entity, event: &InputEvent, _window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        let (alias, col) = match text.split_once('.') {
                            Some((a, c)) => (a.trim().to_string(), c.trim().to_string()),
                            None => {
                                let sa = this
                                    .group_by_rows
                                    .get(i)
                                    .map(|r| r.source_alias.clone())
                                    .unwrap_or_default();
                                (sa, text.trim().to_string())
                            }
                        };
                        this.set_group_by_column(i, alias, col, cx);
                    }
                },
            );

            self.group_by_col_inputs.push(col_input);
            self._input_subs.push(sub);
        }

        let agg_start = self.agg_fn_dropdowns.len();
        for i in agg_start..agg_target {
            let current_fn = self.aggregate_rows[i].function;

            let fn_items: Vec<DropdownItem> = AGG_FN_ORDER
                .iter()
                .map(|f| DropdownItem::with_value(agg_fn_display(*f), agg_fn_display(*f)))
                .collect();
            let fn_selected = AGG_FN_ORDER.iter().position(|f| *f == current_fn);

            let fn_dropdown = cx.new(|_cx| {
                Dropdown::new(("qb-agg-fn-dd", i))
                    .items(fn_items)
                    .selected_index(fn_selected)
                    .toolbar_style(true)
            });

            let fn_sub = cx.subscribe(
                &fn_dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    if let Some(function) = AGG_FN_ORDER.get(event.index).copied() {
                        this.set_aggregate_function(i, function, cx);
                    }
                },
            );

            let col_text = {
                let row = &self.aggregate_rows[i];
                if row.column.is_empty() {
                    String::new()
                } else if row.source_alias.is_empty() {
                    row.column.clone()
                } else {
                    format!("{}.{}", row.source_alias, row.column)
                }
            };

            let col_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("alias.column");
                state.set_value(&col_text, window, cx);
                state
            });

            col_input.update(cx, |s, _| {
                s.lsp.completion_provider = Some(alias_provider.clone());
            });

            let col_sub = cx.subscribe_in(
                &col_input,
                window,
                move |this, entity, event: &InputEvent, _window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        let (alias, col) = match text.split_once('.') {
                            Some((a, c)) => (a.trim().to_string(), c.trim().to_string()),
                            None => {
                                let sa = this
                                    .aggregate_rows
                                    .get(i)
                                    .map(|r| r.source_alias.clone())
                                    .unwrap_or_default();
                                (sa, text.trim().to_string())
                            }
                        };
                        this.set_aggregate_column(i, alias, col, cx);
                    }
                },
            );

            let alias_text = self.aggregate_rows[i].alias.clone();
            let alias_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("alias");
                state.set_value(&alias_text, window, cx);
                state
            });

            let alias_sub = cx.subscribe_in(
                &alias_input,
                window,
                move |this, entity, event: &InputEvent, _window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        this.set_aggregate_alias(i, text, cx);
                    }
                },
            );

            self.agg_fn_dropdowns.push(fn_dropdown);
            self.agg_col_inputs.push(col_input);
            self.agg_alias_inputs.push(alias_input);
            self._input_subs.push(fn_sub);
            self._input_subs.push(col_sub);
            self._input_subs.push(alias_sub);
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

            let left_provider: Rc<dyn CompletionProvider> = Rc::new(SchemaCompletionProvider::new(
                self.app_state_weak.clone(),
                self.schema_profile_id,
                CompletionMode::AliasOrColumn {
                    aliases: self.make_alias_bindings(),
                },
                self.schema_cache.clone(),
            ));
            state.update(cx, |s, _| {
                s.lsp.completion_provider = Some(left_provider);
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

            let right_provider: Rc<dyn CompletionProvider> =
                Rc::new(SchemaCompletionProvider::new(
                    self.app_state_weak.clone(),
                    self.schema_profile_id,
                    CompletionMode::JoinConditionRight {
                        aliases: self.make_alias_bindings(),
                    },
                    self.schema_cache.clone(),
                ));
            state.update(cx, |s, _| {
                s.lsp.completion_provider = Some(right_provider);
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
    ///
    /// The replacement `row` may carry a different `on` variant than the
    /// previous one (e.g. structured `Conditions` swapped for a raw
    /// expression), which would orphan node-id entries in the join-condition
    /// HashMaps. The sweep flag is set unconditionally so the next render
    /// drops any stale entries regardless of the variant transition.
    pub fn update_join(&mut self, index: usize, row: JoinRow, cx: &mut Context<Self>) {
        if index < self.join_rows.len() {
            self.join_rows[index] = row;
            self.pending_join_condition_sweep = true;
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
    ///
    /// When the spec is grouped, the column must be either a group-by column or
    /// an aggregate alias. If the column is not in the valid set, the entry is
    /// rejected and `sort_validation_error` is set for the view to display.
    pub fn add_sort(&mut self, source_alias: &str, column: &str, cx: &mut Context<Self>) {
        if self.current_spec.is_grouped() {
            let valid: HashSet<String> = self
                .group_by_rows
                .iter()
                .map(|g| g.column.clone())
                .chain(self.aggregate_rows.iter().map(|a| a.alias.clone()))
                .collect();

            if !valid.contains(column) {
                self.sort_validation_error = Some(format!(
                    "\"{}\" is not in the GROUP BY columns or aggregate aliases",
                    column
                ));
                cx.notify();
                return;
            }
        }

        self.sort_validation_error = None;

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
    // Group-by mutations
    // -----------------------------------------------------------------------

    /// Appends a group-by column row.
    ///
    /// Triggers the projection auto-transition when this is the first group-by
    /// or aggregate row.
    pub fn add_group_by_column(
        &mut self,
        source_alias: String,
        column: String,
        cx: &mut Context<Self>,
    ) {
        let was_empty = self.group_by_rows.is_empty() && self.aggregate_rows.is_empty();
        self.group_by_rows.push(GroupByRow {
            source_alias,
            column,
        });
        self.pending_group_by_rebuild = true;
        if was_empty {
            self.enter_grouped_mode();
        }
        self.rebuild_spec_and_notify(cx);
    }

    /// Removes the group-by row at `index`.
    ///
    /// Triggers exit from grouped mode when this removal leaves both group-by
    /// and aggregate rows empty.
    pub fn remove_group_by_row(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.group_by_rows.len() {
            self.group_by_rows.remove(index);
            self.pending_group_by_rebuild = true;
            self.drop_invalid_sort_for_grouped();
            if self.group_by_rows.is_empty() && self.aggregate_rows.is_empty() {
                self.exit_grouped_mode();
            }
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Updates the column of the group-by row at `index`.
    pub fn set_group_by_column(
        &mut self,
        index: usize,
        source_alias: String,
        column: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(row) = self.group_by_rows.get_mut(index) {
            row.source_alias = source_alias;
            row.column = column;
            self.drop_invalid_sort_for_grouped();
            self.rebuild_spec_and_notify(cx);
        }
    }

    // -----------------------------------------------------------------------
    // Aggregate mutations
    // -----------------------------------------------------------------------

    /// Appends an aggregate row with an auto-generated alias.
    ///
    /// Triggers the projection auto-transition when this is the first group-by
    /// or aggregate row.
    pub fn add_aggregate(&mut self, function: AggFn, cx: &mut Context<Self>) {
        let was_empty = self.group_by_rows.is_empty() && self.aggregate_rows.is_empty();
        let alias = self.generate_aggregate_alias(function, "");
        self.aggregate_rows.push(AggregateRow {
            function,
            source_alias: String::new(),
            column: String::new(),
            alias,
        });
        self.pending_group_by_rebuild = true;
        if was_empty {
            self.enter_grouped_mode();
        }
        self.rebuild_spec_and_notify(cx);
    }

    /// Removes the aggregate row at `index`.
    ///
    /// Triggers exit from grouped mode when this removal leaves both group-by
    /// and aggregate rows empty.
    pub fn remove_aggregate_row(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.aggregate_rows.len() {
            self.aggregate_rows.remove(index);
            self.pending_group_by_rebuild = true;
            self.drop_invalid_sort_for_grouped();
            if self.group_by_rows.is_empty() && self.aggregate_rows.is_empty() {
                self.exit_grouped_mode();
            }
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Changes the function of the aggregate row at `index`.
    ///
    /// When the new function is `CountStar`, clears the column (CountStar
    /// requires no column reference). Otherwise preserves the column.
    pub fn set_aggregate_function(
        &mut self,
        index: usize,
        function: AggFn,
        cx: &mut Context<Self>,
    ) {
        if index >= self.aggregate_rows.len() {
            return;
        }

        self.aggregate_rows[index].function = function;
        if function == AggFn::CountStar {
            self.aggregate_rows[index].source_alias = String::new();
            self.aggregate_rows[index].column = String::new();
        }
        let old_alias = self.aggregate_rows[index].alias.clone();
        let col = self.aggregate_rows[index].column.clone();
        if old_alias.is_empty() || self.is_auto_alias(&old_alias) {
            let new_alias = self.generate_aggregate_alias(function, &col);
            self.aggregate_rows[index].alias = new_alias;
        }
        self.rebuild_spec_and_notify(cx);
    }

    /// Updates the column reference of the aggregate row at `index`.
    pub fn set_aggregate_column(
        &mut self,
        index: usize,
        source_alias: String,
        column: String,
        cx: &mut Context<Self>,
    ) {
        if index >= self.aggregate_rows.len() {
            return;
        }

        let function = self.aggregate_rows[index].function;
        let old_alias = self.aggregate_rows[index].alias.clone();

        self.aggregate_rows[index].source_alias = source_alias;
        self.aggregate_rows[index].column = column.clone();

        if old_alias.is_empty() || self.is_auto_alias(&old_alias) {
            let new_alias = self.generate_aggregate_alias(function, &column);
            self.aggregate_rows[index].alias = new_alias;
        }

        self.drop_invalid_sort_for_grouped();
        self.rebuild_spec_and_notify(cx);
    }

    /// Sets the alias of the aggregate row at `index`.
    pub fn set_aggregate_alias(&mut self, index: usize, alias: String, cx: &mut Context<Self>) {
        if let Some(row) = self.aggregate_rows.get_mut(index) {
            row.alias = alias;
            self.drop_invalid_sort_for_grouped();
            self.rebuild_spec_and_notify(cx);
        }
    }

    // -----------------------------------------------------------------------
    // HAVING filter mutations (routed through FilterTarget)
    // -----------------------------------------------------------------------

    /// Replaces the HAVING root node.
    pub fn set_having(&mut self, having: Option<FilterNode>, cx: &mut Context<Self>) {
        self.current_spec.having = having;
        self.refresh_preview_and_notify(cx);
    }

    /// Adds a new empty predicate to the filter tree identified by `target`.
    pub fn add_predicate_for(
        &mut self,
        target: FilterTarget,
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

        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        match tree {
            None => {
                *tree = Some(FilterNode::Group {
                    op: BoolOp::And,
                    children: vec![new_pred],
                });
            }
            Some(root) => {
                insert_filter_at_path(root, &parent_path, new_pred);
            }
        }

        match target {
            FilterTarget::Where => self.pending_filter_input_sweep = true,
            FilterTarget::Having => self.pending_having_input_sweep = true,
        }
        self.refresh_preview_and_notify(cx);
    }

    /// Adds a new empty sub-group to the filter tree identified by `target`.
    pub fn add_group_for(
        &mut self,
        target: FilterTarget,
        parent_path: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let new_group = FilterNode::Group {
            op: BoolOp::And,
            children: Vec::new(),
        };

        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        match tree {
            None => {
                *tree = Some(new_group);
            }
            Some(root) => {
                insert_filter_at_path(root, &parent_path, new_group);
            }
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Removes the node at `path` from the filter tree identified by `target`.
    pub fn remove_filter_node_for(
        &mut self,
        target: FilterTarget,
        path: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        if path.is_empty() {
            *tree = None;
        } else {
            if let Some(root) = tree.as_mut() {
                remove_filter_at_path(root, &path);
            }
            if let Some(FilterNode::Group { children, .. }) = tree.as_ref()
                && children.is_empty()
            {
                *tree = None;
            }
        }

        match target {
            FilterTarget::Where => self.pending_filter_input_sweep = true,
            FilterTarget::Having => self.pending_having_input_sweep = true,
        }
        self.refresh_preview_and_notify(cx);
    }

    /// Toggles the boolean operator (AND ↔ OR) of the group at `path` in the
    /// filter tree identified by `target`.
    pub fn toggle_group_op_for(
        &mut self,
        target: FilterTarget,
        path: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        if let Some(root) = tree.as_mut()
            && let Some(FilterNode::Group { op, .. }) = filter_node_at_path_mut(root, &path)
        {
            *op = match *op {
                BoolOp::And => BoolOp::Or,
                BoolOp::Or => BoolOp::And,
            };
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Sets the predicate value at `path` in the filter tree identified by
    /// `target`.
    pub fn set_predicate_value_for(
        &mut self,
        target: FilterTarget,
        path: Vec<usize>,
        text: String,
        cx: &mut Context<Self>,
    ) {
        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        if let Some(root) = tree.as_mut()
            && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &path)
        {
            pred.value = PredicateValue::Single(LiteralValue::Text(text));
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Updates the column reference of a predicate at `path` in the filter tree
    /// identified by `target`.
    pub fn set_predicate_column_ref_for(
        &mut self,
        target: FilterTarget,
        path: Vec<usize>,
        text: String,
        cx: &mut Context<Self>,
    ) {
        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        if let Some(root) = tree.as_mut()
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

    /// Sets the comparator at `path` in the filter tree identified by `target`.
    pub fn set_predicate_comparator_for(
        &mut self,
        target: FilterTarget,
        path: Vec<usize>,
        comparator: Comparator,
        cx: &mut Context<Self>,
    ) {
        let tree = match target {
            FilterTarget::Where => &mut self.current_spec.filter,
            FilterTarget::Having => &mut self.current_spec.having,
        };

        if let Some(root) = tree.as_mut()
            && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &path)
        {
            pred.comparator = comparator;
        }

        self.refresh_preview_and_notify(cx);
    }

    /// Returns the set of node_ids for all Predicate nodes in the HAVING tree.
    pub fn collect_having_predicate_node_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();
        if let Some(root) = &self.current_spec.having {
            collect_filter_predicate_ids(root, &mut ids);
        }
        ids
    }

    /// Sweeps stale HAVING predicate input state after mutations.
    pub fn sweep_stale_having_predicate_inputs(&mut self) {
        let live_ids = self.collect_having_predicate_node_ids();
        self.having_predicate_input_states
            .retain(|id, _| live_ids.contains(id));
        self.having_predicate_column_input_states
            .retain(|id, _| live_ids.contains(id));
        self.having_predicate_comparator_dropdowns
            .retain(|id, _| live_ids.contains(id));
    }

    /// Ensures a value `InputState` exists for the HAVING predicate at `node_id`.
    pub fn ensure_having_predicate_input(
        &mut self,
        node_id: u64,
        path: Vec<usize>,
        current_value: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.having_predicate_input_states.contains_key(&node_id) {
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
                    this.set_predicate_value_for(FilterTarget::Having, path.clone(), text, cx);
                    let _ = window;
                }
            },
        );

        self.having_predicate_input_states.insert(node_id, state);
        self._input_subs.push(sub);
    }

    /// Ensures a column-reference `InputState` exists for the HAVING predicate
    /// at `node_id`.
    pub fn ensure_having_predicate_column_input(
        &mut self,
        node_id: u64,
        path: Vec<usize>,
        current_text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self
            .having_predicate_column_input_states
            .contains_key(&node_id)
        {
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
                    this.set_predicate_column_ref_for(FilterTarget::Having, path.clone(), text, cx);
                    let _ = window;
                }
            },
        );

        self.having_predicate_column_input_states
            .insert(node_id, state);
        self._input_subs.push(sub);
    }

    /// Ensures a comparator `Dropdown` exists for the HAVING predicate at
    /// `node_id`.
    pub fn ensure_having_predicate_comparator_dropdown(
        &mut self,
        node_id: u64,
        path: Vec<usize>,
        current: Comparator,
        cx: &mut Context<Self>,
    ) {
        if self
            .having_predicate_comparator_dropdowns
            .contains_key(&node_id)
        {
            return;
        }

        let items: Vec<DropdownItem> = COMPARATOR_ORDER
            .iter()
            .map(|c| DropdownItem::with_value(comparator_label(*c), comparator_value(*c)))
            .collect();

        let selected = COMPARATOR_ORDER.iter().position(|c| *c == current);

        let dropdown = cx.new(|_cx| {
            Dropdown::new(("qb-having-pred-cmp-dd", node_id))
                .items(items)
                .selected_index(selected)
                .toolbar_style(true)
        });

        let path_for_sub = path;
        let sub = cx.subscribe(
            &dropdown,
            move |this, _entity, event: &DropdownSelectionChanged, cx| {
                if let Some(comparator) = COMPARATOR_ORDER.get(event.index).copied() {
                    this.set_predicate_comparator_for(
                        FilterTarget::Having,
                        path_for_sub.clone(),
                        comparator,
                        cx,
                    );
                }
            },
        );

        self.having_predicate_comparator_dropdowns
            .insert(node_id, dropdown);
        self._input_subs.push(sub);
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

        let group_by: Vec<GroupByEntry> = self
            .group_by_rows
            .iter()
            .filter(|r| !r.column.is_empty())
            .map(|r| r.to_group_by_entry())
            .collect();

        let aggregates: Vec<VisualAggregateSpec> = self
            .aggregate_rows
            .iter()
            .filter(|r| !r.alias.is_empty())
            .filter(|r| r.function == AggFn::CountStar || !r.column.is_empty())
            .map(|r| r.to_aggregate_spec())
            .collect();

        let limit = match self.limit_text.parse::<u64>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n),
        };

        let offset = self.offset_text.parse::<u64>().unwrap_or(0);

        self.current_spec.projection = projection;
        self.current_spec.joins = joins;
        self.current_spec.sort = sort;
        self.current_spec.group_by = group_by;
        self.current_spec.aggregates = aggregates;

        let incomplete_count = self
            .aggregate_rows
            .iter()
            .filter(|r| {
                r.alias.is_empty() || (r.function != AggFn::CountStar && r.column.is_empty())
            })
            .count();
        self.incomplete_aggregate_row_count = incomplete_count;

        let spec = self.current_spec.clone();
        self.sql_preview = (self.generate_preview)(&spec);
        self.pending_preview_sync = true;
        self.current_spec.limit = limit;
        self.current_spec.offset = offset;
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

    /// Returns `true` when the query is currently grouped (has at least one
    /// group-by or aggregate row).
    pub fn is_grouped(&self) -> bool {
        self.current_spec.is_grouped()
    }

    /// The weak handle to the owning `DataGridPanel`, if one was provided.
    pub fn data_grid(&self) -> Option<&WeakEntity<DataGridPanel>> {
        self.data_grid.as_ref()
    }

    // -----------------------------------------------------------------------
    // Grouped mode transition helpers
    // -----------------------------------------------------------------------

    /// Transitions into grouped mode: snapshots the current projection and
    /// switches projection to `Explicit([])`. Also drops sort entries that
    /// won't survive the GROUP BY validation.
    fn enter_grouped_mode(&mut self) {
        if self.pre_group_projection.is_none() {
            self.pre_group_projection = Some(self.current_spec.projection.clone());
        }
        self.current_spec.projection = Projection::Explicit(Vec::new());
        self.projection_mode = ProjectionMode::Explicit;
        self.projection_rows.clear();
        self.drop_invalid_sort_for_grouped();
    }

    /// Transitions out of grouped mode: restores the pre-group projection
    /// snapshot. Any sort entries that reference aggregate aliases are dropped.
    fn exit_grouped_mode(&mut self) {
        if let Some(prev) = self.pre_group_projection.take() {
            self.current_spec.projection = prev.clone();
            match &prev {
                Projection::All => {
                    self.projection_mode = ProjectionMode::All;
                    self.projection_rows.clear();
                }
                Projection::Explicit(cols) => {
                    self.projection_mode = ProjectionMode::Explicit;
                    self.projection_rows = cols
                        .iter()
                        .map(|c| ProjectionRow {
                            source_alias: c.source_alias.clone(),
                            column: c.column.clone(),
                            alias: c.alias.clone(),
                        })
                        .collect();
                }
            }
        }
        self.drop_invalid_sort_for_ungrouped();
        // Clear any HAVING state since there is nothing to have without grouping.
        self.current_spec.having = None;
        self.having_predicate_input_states.clear();
        self.having_predicate_column_input_states.clear();
        self.having_predicate_comparator_dropdowns.clear();
    }

    /// Drops sort rows whose column is not in the current grouped valid set
    /// (group-by columns union aggregate aliases).
    fn drop_invalid_sort_for_grouped(&mut self) {
        let valid: HashSet<String> = self
            .group_by_rows
            .iter()
            .map(|g| g.column.clone())
            .chain(self.aggregate_rows.iter().map(|a| a.alias.clone()))
            .collect();

        self.sort_rows.retain(|s| valid.contains(&s.column));
        self.current_spec.sort = self.sort_rows.iter().map(|r| r.to_sort_entry()).collect();
    }

    /// Drops sort rows whose (source_alias, column) pair is not present in the
    /// restored projection.
    fn drop_invalid_sort_for_ungrouped(&mut self) {
        let valid: HashSet<(String, String)> = match &self.current_spec.projection {
            Projection::All => {
                self.sort_rows.clear();
                self.current_spec.sort.clear();
                return;
            }
            Projection::Explicit(cols) => cols
                .iter()
                .map(|c| (c.source_alias.clone(), c.column.clone()))
                .collect(),
        };

        self.sort_rows
            .retain(|s| valid.contains(&(s.source_alias.clone(), s.column.clone())));
        self.current_spec.sort = self.sort_rows.iter().map(|r| r.to_sort_entry()).collect();
    }

    /// Generates a default alias for an aggregate row.
    ///
    /// Returns `count_star` for `CountStar`, `fn_col` for others (e.g.
    /// `sum_amount`). When the generated alias collides with an existing alias,
    /// appends `_2`, `_3`, etc. until unique.
    fn generate_aggregate_alias(&self, function: AggFn, column: &str) -> String {
        let fn_name = match function {
            AggFn::Count => "count",
            AggFn::CountStar => "count_star",
            AggFn::CountDistinct => "count_distinct",
            AggFn::Sum => "sum",
            AggFn::Avg => "avg",
            AggFn::Min => "min",
            AggFn::Max => "max",
        };

        let base = if function == AggFn::CountStar || column.is_empty() {
            fn_name.to_string()
        } else {
            let sanitized: String = column
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            format!("{}_{}", fn_name, sanitized)
        };

        let existing: HashSet<&str> = self
            .aggregate_rows
            .iter()
            .map(|r| r.alias.as_str())
            .collect();

        if !existing.contains(base.as_str()) {
            return base;
        }

        let mut counter = 2usize;
        loop {
            let candidate = format!("{}_{}", base, counter);
            if !existing.contains(candidate.as_str()) {
                return candidate;
            }
            counter += 1;
        }
    }

    /// Returns `true` if `alias` looks like an auto-generated default alias
    /// for any aggregate function.
    fn is_auto_alias(&self, alias: &str) -> bool {
        let auto_prefixes = [
            "count_star",
            "count_distinct",
            "count",
            "sum",
            "avg",
            "min",
            "max",
        ];
        auto_prefixes
            .iter()
            .any(|prefix| alias == *prefix || alias.starts_with(&format!("{}_", prefix)))
    }

    // -----------------------------------------------------------------------
    // Completion support
    // -----------------------------------------------------------------------

    /// Builds the alias binding list from the current spec's source and join rows.
    ///
    /// Used when re-attaching providers after the join list changes.
    pub(crate) fn make_alias_bindings(&self) -> Vec<AliasBinding> {
        let mut bindings = vec![AliasBinding {
            alias: self.current_spec.source.alias.clone(),
            schema: self.current_spec.source.schema.clone(),
            table: self.current_spec.source.table.clone(),
            is_source: true,
        }];

        for row in &self.join_rows {
            if !row.to_table.is_empty() {
                bindings.push(AliasBinding {
                    alias: row.to_alias.clone(),
                    schema: row.to_schema.clone(),
                    table: row.to_table.clone(),
                    is_source: false,
                });
            }
        }

        bindings
    }

    /// Fetches column metadata for a joined table in the background and stores
    /// it in `self.schema_cache`.
    ///
    /// Idempotent: returns immediately if the columns are already cached, a
    /// fetch is in flight, or the fetch previously failed. Fetch failures are
    /// silent (the popover shows aliases only for that join) and stored in the
    /// `failed` set to prevent retries.
    /// Background-fetch column metadata for the builder's source table.
    ///
    /// Called from the panel constructor when the source-table columns are
    /// not yet cached in `AppState`. Writes the fetched columns into the
    /// shared `SchemaCache` so attached completion providers see them as
    /// soon as the fetch resolves; failures are silent (autocomplete is not
    /// a user-facing operation).
    fn spawn_source_columns_fetch(
        schema_cache: Rc<RefCell<SchemaCache>>,
        app_state_weak: gpui::WeakEntity<AppStateEntity>,
        profile_id: uuid::Uuid,
        source_schema: Option<String>,
        source_table: String,
        cx: &mut Context<Self>,
    ) {
        use crate::completion_support::normalize_identifier;

        let key = (
            source_schema.as_ref().map(|s| normalize_identifier(s)),
            normalize_identifier(&source_table),
        );

        {
            let cache = schema_cache.borrow();
            if cache.fetching.contains(&key) || cache.failed.contains(&key) {
                return;
            }
        }

        let Some(app) = app_state_weak.upgrade() else {
            return;
        };

        let db_name = app
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|c| c.active_database.clone())
            .or_else(|| source_schema.clone())
            .unwrap_or_else(|| "default".to_string());

        let Some(conn) = app
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection_for_database(&db_name))
        else {
            return;
        };

        schema_cache.borrow_mut().fetching.insert(key.clone());

        let schema_owned = source_schema;
        let table_owned = source_table;
        let db_for_task = db_name;
        let key_for_task = key.clone();
        let db_for_log = db_for_task.clone();
        let table_for_log = table_owned.clone();

        let task = cx.background_executor().spawn(async move {
            conn.table_details(&db_for_task, schema_owned.as_deref(), &table_owned)
        });

        let schema_cache_for_finish = schema_cache.clone();
        cx.spawn(async move |_this, cx| {
            let result = task.await;
            let _ = cx.update(|cx| {
                {
                    let mut cache = schema_cache_for_finish.borrow_mut();
                    cache.fetching.remove(&key_for_task);
                    match result {
                        Ok(info) => {
                            if let Some(cols) = info.columns {
                                cache.source_columns = cols;
                            } else {
                                log::warn!(
                                    "autocomplete: builder source table_details returned no \
                                     columns for {}.{}",
                                    db_for_log,
                                    table_for_log
                                );
                                cache.failed.insert(key_for_task);
                            }
                        }
                        Err(err) => {
                            log::warn!(
                                "autocomplete: failed to fetch builder source columns for \
                                 {}.{}: {}",
                                db_for_log,
                                table_for_log,
                                err
                            );
                            cache.failed.insert(key_for_task);
                        }
                    }
                }
                _this.update(cx, |_panel, cx| cx.notify()).ok();
            });
        })
        .detach();
    }

    pub(crate) fn ensure_joined_columns(
        &self,
        schema: Option<&str>,
        table: &str,
        cx: &mut Context<Self>,
    ) {
        use crate::completion_support::normalize_identifier;

        let key = (
            schema.map(normalize_identifier),
            normalize_identifier(table),
        );

        {
            let cache = self.schema_cache.borrow();
            if cache.joined_columns.contains_key(&key)
                || cache.fetching.contains(&key)
                || cache.failed.contains(&key)
            {
                return;
            }
        }

        self.schema_cache.borrow_mut().fetching.insert(key.clone());

        let db_name = self
            .app_state_weak
            .upgrade()
            .as_ref()
            .and_then(|app| {
                app.read(cx)
                    .connections()
                    .get(&self.schema_profile_id)
                    .and_then(|c| c.active_database.clone())
            })
            .or_else(|| schema.map(|s| s.to_string()))
            .unwrap_or_else(|| "default".to_string());

        let Some(conn) = self.app_state_weak.upgrade().as_ref().and_then(|app| {
            app.read(cx)
                .connections()
                .get(&self.schema_profile_id)
                .map(|c| c.connection_for_database(&db_name))
        }) else {
            self.schema_cache.borrow_mut().fetching.remove(&key);
            self.schema_cache.borrow_mut().failed.insert(key);
            return;
        };

        let schema_owned = schema.map(|s| s.to_string());
        let table_owned = table.to_string();
        let key_for_task = key.clone();
        let db_for_log = db_name.clone();
        let table_for_log = table_owned.clone();

        let task = cx.background_executor().spawn(async move {
            conn.table_details(&db_name, schema_owned.as_deref(), &table_owned)
        });

        let schema_cache = self.schema_cache.clone();
        cx.spawn(async move |_this, cx| {
            let result = task.await;
            cx.update(|cx| {
                {
                    let mut cache = schema_cache.borrow_mut();
                    cache.fetching.remove(&key_for_task);
                    match result {
                        Ok(table_info) => {
                            let cols = table_info.columns.unwrap_or_default();
                            cache.joined_columns.insert(key_for_task, cols);
                        }
                        Err(err) => {
                            log::warn!(
                                "autocomplete: failed to fetch joined-table columns for \
                                 {}.{}: {}",
                                db_for_log,
                                table_for_log,
                                err
                            );
                            cache.failed.insert(key_for_task);
                        }
                    }
                }
                _this.update(cx, |_panel, cx| cx.notify()).ok();
            })
            .ok();
        })
        .detach();
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

pub(crate) const AGG_FN_ORDER: &[AggFn] = &[
    AggFn::CountStar,
    AggFn::Count,
    AggFn::CountDistinct,
    AggFn::Sum,
    AggFn::Avg,
    AggFn::Min,
    AggFn::Max,
];

pub(crate) fn agg_fn_display(f: AggFn) -> &'static str {
    match f {
        AggFn::CountStar => "COUNT(*)",
        AggFn::Count => "COUNT",
        AggFn::CountDistinct => "COUNT DISTINCT",
        AggFn::Sum => "SUM",
        AggFn::Avg => "AVG",
        AggFn::Min => "MIN",
        AggFn::Max => "MAX",
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
        AggFn, BoolOp, Comparator, FilterNode, JoinKind, JoinOn, LiteralValue, Predicate,
        PredicateValue, Projection, SourceTable, VisualQuerySpec, VisualSortDirection,
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
            group_by: vec![],
            aggregates: vec![],
            having: None,
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

        use std::cell::RefCell;
        use std::rc::Rc;
        use uuid::Uuid;

        let group_by_rows: Vec<GroupByRow> = spec
            .group_by
            .iter()
            .map(|g| GroupByRow {
                source_alias: g.source_alias.clone(),
                column: g.column.clone(),
            })
            .collect();

        let aggregate_rows: Vec<AggregateRow> = spec
            .aggregates
            .iter()
            .map(|a| AggregateRow {
                function: a.function,
                source_alias: a.source_alias.clone().unwrap_or_default(),
                column: a.column.clone().unwrap_or_default(),
                alias: a.alias.clone(),
            })
            .collect();

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
            schema_cache: Rc::new(RefCell::new(SchemaCache::default())),
            app_state_weak: WeakEntity::new_invalid(),
            schema_profile_id: Uuid::nil(),
            pending_filter_input_sweep: false,
            pending_join_condition_sweep: false,
            group_by_rows,
            aggregate_rows,
            group_by_col_inputs: Vec::new(),
            agg_fn_dropdowns: Vec::new(),
            agg_col_inputs: Vec::new(),
            agg_alias_inputs: Vec::new(),
            pending_group_by_rebuild: false,
            having_predicate_input_states: HashMap::new(),
            having_predicate_column_input_states: HashMap::new(),
            having_predicate_comparator_dropdowns: HashMap::new(),
            pending_having_input_sweep: false,
            pre_group_projection: None,
            sort_validation_error: None,
            incomplete_aggregate_row_count: 0,
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

        fn add_sort_pure(&mut self, source_alias: &str, column: &str) {
            if self.current_spec.is_grouped() {
                let valid: HashSet<String> = self
                    .group_by_rows
                    .iter()
                    .map(|g| g.column.clone())
                    .chain(self.aggregate_rows.iter().map(|a| a.alias.clone()))
                    .collect();

                if !valid.contains(column) {
                    self.sort_validation_error = Some(format!(
                        "\"{}\" is not in the GROUP BY columns or aggregate aliases",
                        column
                    ));
                    return;
                }
            }

            self.sort_validation_error = None;
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

        fn t_add_group_by_column(&mut self, source_alias: &str, column: &str) {
            let was_empty = self.group_by_rows.is_empty() && self.aggregate_rows.is_empty();
            self.group_by_rows.push(GroupByRow {
                source_alias: source_alias.to_string(),
                column: column.to_string(),
            });
            if was_empty {
                self.enter_grouped_mode();
            }
            self.rebuild_spec_pure();
        }

        fn t_remove_group_by_row(&mut self, index: usize) {
            if index < self.group_by_rows.len() {
                self.group_by_rows.remove(index);
                self.drop_invalid_sort_for_grouped();
                if self.group_by_rows.is_empty() && self.aggregate_rows.is_empty() {
                    self.exit_grouped_mode();
                }
                self.rebuild_spec_pure();
            }
        }

        fn t_add_aggregate(&mut self, function: AggFn) {
            let was_empty = self.group_by_rows.is_empty() && self.aggregate_rows.is_empty();
            let alias = self.generate_aggregate_alias(function, "");
            self.aggregate_rows.push(AggregateRow {
                function,
                source_alias: String::new(),
                column: String::new(),
                alias,
            });
            if was_empty {
                self.enter_grouped_mode();
            }
            self.rebuild_spec_pure();
        }

        fn t_add_aggregate_with_column(
            &mut self,
            function: AggFn,
            source_alias: &str,
            column: &str,
        ) {
            let was_empty = self.group_by_rows.is_empty() && self.aggregate_rows.is_empty();
            let alias = self.generate_aggregate_alias(function, column);
            self.aggregate_rows.push(AggregateRow {
                function,
                source_alias: source_alias.to_string(),
                column: column.to_string(),
                alias,
            });
            if was_empty {
                self.enter_grouped_mode();
            }
            self.rebuild_spec_pure();
        }

        fn t_remove_aggregate_row(&mut self, index: usize) {
            if index < self.aggregate_rows.len() {
                self.aggregate_rows.remove(index);
                self.drop_invalid_sort_for_grouped();
                if self.group_by_rows.is_empty() && self.aggregate_rows.is_empty() {
                    self.exit_grouped_mode();
                }
                self.rebuild_spec_pure();
            }
        }

        fn t_set_aggregate_function(&mut self, index: usize, function: AggFn) {
            if index >= self.aggregate_rows.len() {
                return;
            }
            self.aggregate_rows[index].function = function;
            if function == AggFn::CountStar {
                self.aggregate_rows[index].source_alias = String::new();
                self.aggregate_rows[index].column = String::new();
            }
            let old_alias = self.aggregate_rows[index].alias.clone();
            let col = self.aggregate_rows[index].column.clone();
            if old_alias.is_empty() || self.is_auto_alias(&old_alias) {
                let new_alias = self.generate_aggregate_alias(function, &col);
                self.aggregate_rows[index].alias = new_alias;
            }
            self.rebuild_spec_pure();
        }

        fn t_set_aggregate_column(&mut self, index: usize, source_alias: &str, column: &str) {
            if index >= self.aggregate_rows.len() {
                return;
            }
            let function = self.aggregate_rows[index].function;
            let old_alias = self.aggregate_rows[index].alias.clone();
            self.aggregate_rows[index].source_alias = source_alias.to_string();
            self.aggregate_rows[index].column = column.to_string();
            if old_alias.is_empty() || self.is_auto_alias(&old_alias) {
                let new_alias = self.generate_aggregate_alias(function, column);
                self.aggregate_rows[index].alias = new_alias;
            }
            self.drop_invalid_sort_for_grouped();
            self.rebuild_spec_pure();
        }

        fn t_set_aggregate_alias(&mut self, index: usize, alias: &str) {
            if let Some(row) = self.aggregate_rows.get_mut(index) {
                row.alias = alias.to_string();
                self.drop_invalid_sort_for_grouped();
                self.rebuild_spec_pure();
            }
        }

        fn t_set_group_by_column(&mut self, index: usize, source_alias: &str, column: &str) {
            if let Some(row) = self.group_by_rows.get_mut(index) {
                row.source_alias = source_alias.to_string();
                row.column = column.to_string();
                self.drop_invalid_sort_for_grouped();
                self.rebuild_spec_pure();
            }
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
            group_by: vec![],
            aggregates: vec![],
            having: None,
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
    fn set_spec_flags_drive_join_state_rebuild() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.pending_join_rebuild = false;
        panel.pending_join_condition_sweep = false;

        // A loaded spec with two joins replaces whatever the panel had.
        let mut spec = make_spec(test_source());
        spec.joins = vec![
            JoinStep {
                kind: JoinKind::Inner,
                from_alias: "users".to_string(),
                to_schema: None,
                to_table: "orders".to_string(),
                to_alias: "orders".to_string(),
                on: JoinOn::FkPath {
                    from_column: "id".to_string(),
                    to_column: "user_id".to_string(),
                },
            },
            JoinStep {
                kind: JoinKind::Left,
                from_alias: "orders".to_string(),
                to_schema: None,
                to_table: "items".to_string(),
                to_alias: "items".to_string(),
                on: JoinOn::RawExpression("orders.id = items.order_id".to_string()),
            },
        ];

        panel.set_spec_pure(spec);

        assert_eq!(panel.join_rows.len(), 2);
        assert!(
            panel.pending_join_rebuild,
            "set_spec must set pending_join_rebuild so the next render aligns join_input_states with the new join_rows length"
        );
        assert!(
            panel.pending_join_condition_sweep,
            "set_spec must set pending_join_condition_sweep so the next render drops orphaned node-id entries"
        );
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

    // ---- Slice 2: group-by state machine -----------------------------------

    #[test]
    fn add_group_by_row_enters_grouped_mode() {
        let mut panel = make_panel(make_spec(test_source()));
        assert_eq!(panel.projection_mode, ProjectionMode::All);

        panel.t_add_group_by_column("users", "country");

        assert!(panel.is_grouped());
        assert_eq!(panel.group_by_rows.len(), 1);
        assert_eq!(panel.group_by_rows[0].column, "country");
        // Projection should have transitioned to Explicit
        assert_eq!(panel.projection_mode, ProjectionMode::Explicit);
        // Snapshot should be stored
        assert!(
            panel.pre_group_projection.is_some(),
            "pre_group_projection must be snapshotted on first row"
        );
    }

    #[test]
    fn remove_last_group_by_row_exits_grouped_mode() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_remove_group_by_row(0);

        assert!(!panel.is_grouped());
        assert!(panel.group_by_rows.is_empty());
        assert_eq!(panel.projection_mode, ProjectionMode::All);
        assert!(
            panel.pre_group_projection.is_none(),
            "snapshot should be cleared after exit"
        );
    }

    #[test]
    fn add_aggregate_enters_grouped_mode() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate(AggFn::CountStar);

        assert!(panel.is_grouped());
        assert_eq!(panel.aggregate_rows.len(), 1);
        assert_eq!(panel.aggregate_rows[0].function, AggFn::CountStar);
        assert_eq!(panel.projection_mode, ProjectionMode::Explicit);
    }

    #[test]
    fn remove_last_aggregate_exits_grouped_mode() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate(AggFn::CountStar);
        panel.t_remove_aggregate_row(0);

        assert!(!panel.is_grouped());
        assert_eq!(panel.projection_mode, ProjectionMode::All);
    }

    #[test]
    fn mixed_group_aggregate_stays_grouped_until_both_empty() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");

        // Remove group_by only — still grouped because aggregate remains
        panel.t_remove_group_by_row(0);
        assert!(
            panel.is_grouped(),
            "still grouped due to remaining aggregate"
        );

        // Remove aggregate — now ungrouped
        panel.t_remove_aggregate_row(0);
        assert!(!panel.is_grouped());
    }

    #[test]
    fn projection_auto_transition_truth_table() {
        let mut panel = make_panel(make_spec(test_source()));
        // ([], []) -> not grouped, All
        assert!(!panel.is_grouped());
        assert_eq!(panel.projection_mode, ProjectionMode::All);

        // Add group-by: ([country], []) -> grouped, Explicit
        panel.t_add_group_by_column("users", "country");
        assert!(panel.is_grouped());
        assert_eq!(panel.projection_mode, ProjectionMode::Explicit);

        // Remove group-by: ([], []) -> not grouped, All restored
        panel.t_remove_group_by_row(0);
        assert!(!panel.is_grouped());
        assert_eq!(panel.projection_mode, ProjectionMode::All);

        // Add aggregate only: ([], [sum]) -> grouped, Explicit
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        assert!(panel.is_grouped());
        assert_eq!(panel.projection_mode, ProjectionMode::Explicit);

        // Remove aggregate: ([], []) -> not grouped, All restored
        panel.t_remove_aggregate_row(0);
        assert!(!panel.is_grouped());
        assert_eq!(panel.projection_mode, ProjectionMode::All);
    }

    #[test]
    fn sort_entries_dropped_when_entering_grouped_mode() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_sort("users", "city");
        panel.t_add_sort("users", "country");

        // Enter grouped mode with country only
        panel.t_add_group_by_column("users", "country");

        // city is not in group-by, should be dropped
        assert_eq!(panel.sort_rows.len(), 1);
        assert_eq!(panel.sort_rows[0].column, "country");
    }

    #[test]
    fn sort_entries_referencing_aggregate_alias_dropped_on_exit() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        // Add a sort on aggregate alias
        panel.sort_rows.push(SortRow {
            source_alias: String::new(),
            column: "sum_amount".to_string(),
            direction: VisualSortDirection::Asc,
        });
        panel.rebuild_spec_pure();
        assert_eq!(panel.sort_rows.len(), 1);

        // Exit grouped mode
        panel.t_remove_group_by_row(0);
        panel.t_remove_aggregate_row(0);
        // sum_amount alias no longer valid — should be dropped
        assert!(
            panel.sort_rows.is_empty(),
            "stale sort on aggregate alias must be removed"
        );
    }

    // ---- Slice 2: rebuild_spec_pure round-trips ----------------------------

    #[test]
    fn rebuild_spec_pure_writes_group_by_and_aggregates() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");

        assert_eq!(panel.current_spec.group_by.len(), 1);
        assert_eq!(panel.current_spec.group_by[0].column, "country");
        assert_eq!(panel.current_spec.aggregates.len(), 1);
        assert_eq!(panel.current_spec.aggregates[0].alias, "sum_amount");
    }

    #[test]
    fn rebuild_spec_pure_skips_incomplete_rows() {
        let mut panel = make_panel(make_spec(test_source()));
        // Group-by with empty column should be skipped
        panel.group_by_rows.push(GroupByRow {
            source_alias: String::new(),
            column: String::new(),
        });
        // Aggregate with empty alias should be skipped
        panel.aggregate_rows.push(AggregateRow {
            function: AggFn::Sum,
            source_alias: "users".to_string(),
            column: "amount".to_string(),
            alias: String::new(),
        });
        panel.rebuild_spec_pure();

        assert!(
            panel.current_spec.group_by.is_empty(),
            "empty-column group-by row must be skipped"
        );
        assert!(
            panel.current_spec.aggregates.is_empty(),
            "empty-alias aggregate row must be skipped"
        );
    }

    #[test]
    fn set_spec_pure_round_trips_grouped_spec() {
        use dbflux_core::{AggFn as CoreAggFn, GroupByEntry, VisualAggregateSpec};

        let mut spec = make_spec(test_source());
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![VisualAggregateSpec {
            function: CoreAggFn::Sum,
            source_alias: Some("users".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }];
        spec.having = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![],
        });

        let mut panel = make_panel(make_spec(test_source()));
        panel.set_spec_pure(spec.clone());

        assert_eq!(panel.group_by_rows.len(), 1);
        assert_eq!(panel.group_by_rows[0].column, "country");
        assert_eq!(panel.aggregate_rows.len(), 1);
        assert_eq!(panel.aggregate_rows[0].alias, "total");
        assert!(panel.current_spec.having.is_some());
    }

    #[test]
    fn set_spec_pure_grouped_then_remove_all_restores_all_projection() {
        use dbflux_core::{AggFn as CoreAggFn, GroupByEntry, VisualAggregateSpec};

        let mut spec = make_spec(test_source());
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![VisualAggregateSpec {
            function: CoreAggFn::Sum,
            source_alias: Some("users".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }];

        let mut panel = make_panel(make_spec(test_source()));
        panel.set_spec_pure(spec);

        assert!(
            panel.pre_group_projection.is_some(),
            "set_spec_pure of a grouped spec must seed pre_group_projection"
        );

        panel.t_remove_aggregate_row(0);
        panel.t_remove_group_by_row(0);

        assert!(!panel.is_grouped());
        assert_eq!(
            panel.current_spec.projection,
            Projection::All,
            "removing all aggregates and group-by rows must restore Projection::All"
        );
    }

    // ---- Slice 2: alias auto-generation ------------------------------------

    #[test]
    fn alias_autogenerated_for_count_star() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate(AggFn::CountStar);
        assert_eq!(panel.aggregate_rows[0].alias, "count_star");
    }

    #[test]
    fn alias_autogenerated_for_sum_with_column() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        assert_eq!(panel.aggregate_rows[0].alias, "sum_amount");
    }

    #[test]
    fn alias_autogenerated_with_collision_suffix() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        // Add a second Sum(amount) — should get suffix _2
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        assert_eq!(panel.aggregate_rows[0].alias, "sum_amount");
        assert_eq!(panel.aggregate_rows[1].alias, "sum_amount_2");
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

    // ---- Slice 2: interactive control round-trips ----------------------------

    #[test]
    fn add_group_by_column_sets_spec_entry() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_set_group_by_column(0, "users", "country");

        assert_eq!(panel.current_spec.group_by.len(), 1);
        assert_eq!(panel.current_spec.group_by[0].source_alias, "users");
        assert_eq!(panel.current_spec.group_by[0].column, "country");
    }

    #[test]
    fn change_group_by_column_updates_spec() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_set_group_by_column(0, "users", "region");

        assert_eq!(panel.current_spec.group_by[0].column, "region");
    }

    #[test]
    fn add_aggregate_row_sets_spec_entry() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate(AggFn::CountStar);

        assert_eq!(panel.current_spec.aggregates.len(), 1);
        assert_eq!(panel.current_spec.aggregates[0].function, AggFn::CountStar);
        assert!(panel.current_spec.aggregates[0].source_alias.is_none());
        assert!(panel.current_spec.aggregates[0].column.is_none());
    }

    #[test]
    fn change_aggregate_function_to_count_star_clears_column_in_spec() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        assert_eq!(
            panel.current_spec.aggregates[0].column,
            Some("amount".to_string())
        );

        panel.t_set_aggregate_function(0, AggFn::CountStar);

        assert_eq!(panel.aggregate_rows[0].function, AggFn::CountStar);
        assert!(
            panel.aggregate_rows[0].column.is_empty(),
            "column must be cleared on the row when function is CountStar"
        );
        assert!(
            panel.current_spec.aggregates[0].column.is_none(),
            "spec column must be None for CountStar"
        );
    }

    #[test]
    fn change_aggregate_function_away_from_count_star_keeps_column() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        panel.t_set_aggregate_function(0, AggFn::CountStar);

        // Now switch back to SUM. The column was cleared by CountStar, so
        // the row has function=Sum and column="". The spec filter excludes
        // rows with empty columns for non-CountStar functions, so aggregates
        // will be empty in the spec until the user fills in a column.
        panel.t_set_aggregate_function(0, AggFn::Sum);

        assert_eq!(panel.aggregate_rows[0].function, AggFn::Sum);
        assert!(
            panel.aggregate_rows[0].column.is_empty(),
            "column was cleared by CountStar and not restored"
        );
        assert!(
            panel.current_spec.aggregates.is_empty(),
            "spec excludes incomplete Sum rows (empty column)"
        );
    }

    #[test]
    fn change_aggregate_column_updates_spec_and_auto_alias() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate(AggFn::Sum);
        panel.t_set_aggregate_column(0, "users", "revenue");

        assert_eq!(panel.aggregate_rows[0].column, "revenue");
        assert_eq!(
            panel.current_spec.aggregates[0].column,
            Some("revenue".to_string())
        );
        assert_eq!(
            panel.aggregate_rows[0].alias, "sum_revenue",
            "auto alias must be regenerated from new column"
        );
        assert_eq!(panel.current_spec.aggregates[0].alias, "sum_revenue");
    }

    #[test]
    fn manually_set_alias_is_preserved_when_column_changes() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");

        // Manually set a custom alias — this is not an auto-alias.
        panel.t_set_aggregate_alias(0, "total_revenue");
        assert_eq!(panel.aggregate_rows[0].alias, "total_revenue");

        // Change column — alias must not be overwritten because it was manually edited.
        panel.t_set_aggregate_column(0, "users", "revenue");
        assert_eq!(
            panel.aggregate_rows[0].alias, "total_revenue",
            "manually edited alias must be preserved across column change"
        );
    }

    #[test]
    fn auto_alias_is_regenerated_when_function_changes() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        assert_eq!(panel.aggregate_rows[0].alias, "sum_amount");

        panel.t_set_aggregate_function(0, AggFn::Avg);
        assert_eq!(
            panel.aggregate_rows[0].alias, "avg_amount",
            "auto alias must be regenerated when function changes"
        );
    }

    #[test]
    fn manually_set_alias_is_preserved_when_function_changes() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        panel.t_set_aggregate_alias(0, "my_custom_alias");

        panel.t_set_aggregate_function(0, AggFn::Avg);
        assert_eq!(
            panel.aggregate_rows[0].alias, "my_custom_alias",
            "manually edited alias must survive function change"
        );
    }

    #[test]
    fn set_aggregate_alias_updates_spec() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        panel.t_set_aggregate_alias(0, "grand_total");

        assert_eq!(panel.current_spec.aggregates[0].alias, "grand_total");
    }

    // ---- Slice 3: sort restriction when grouped ----------------------------

    #[test]
    fn add_sort_accepts_group_by_column_when_grouped() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");

        panel.add_sort_pure("users", "country");

        assert_eq!(
            panel.sort_rows.len(),
            1,
            "valid group-by column must be accepted"
        );
        assert!(
            panel.sort_validation_error.is_none(),
            "no error for valid column"
        );
    }

    #[test]
    fn add_sort_accepts_aggregate_alias_when_grouped() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");

        panel.add_sort_pure("users", "sum_amount");

        assert_eq!(panel.sort_rows.len(), 1, "aggregate alias must be accepted");
        assert!(panel.sort_validation_error.is_none());
    }

    #[test]
    fn add_sort_rejects_invalid_column_when_grouped_and_sets_error() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");

        panel.add_sort_pure("users", "city");

        assert_eq!(panel.sort_rows.len(), 0, "invalid column must be rejected");
        assert!(
            panel.sort_validation_error.is_some(),
            "sort_validation_error must be set for invalid column"
        );
    }

    #[test]
    fn add_sort_allows_any_column_when_ungrouped() {
        let mut panel = make_panel(make_spec(test_source()));

        panel.add_sort_pure("users", "any_column");

        assert_eq!(
            panel.sort_rows.len(),
            1,
            "any column must be accepted when ungrouped"
        );
        assert!(panel.sort_validation_error.is_none());
    }

    // ---- Slice 3: incomplete aggregate count --------------------------------

    #[test]
    fn incomplete_aggregate_count_zero_when_no_aggregates() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        assert_eq!(panel.incomplete_aggregate_row_count, 0);
    }

    #[test]
    fn incomplete_aggregate_count_zero_when_all_complete() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate_with_column(AggFn::Sum, "users", "amount");
        assert_eq!(panel.incomplete_aggregate_row_count, 0);
    }

    #[test]
    fn incomplete_aggregate_count_zero_for_count_star_without_column() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate(AggFn::CountStar);
        assert_eq!(
            panel.incomplete_aggregate_row_count, 0,
            "CountStar without column is NOT incomplete"
        );
    }

    #[test]
    fn incomplete_aggregate_count_one_for_sum_without_column() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate(AggFn::Sum);
        assert_eq!(
            panel.incomplete_aggregate_row_count, 1,
            "Sum without column IS incomplete"
        );
    }

    #[test]
    fn incomplete_aggregate_count_includes_empty_alias() {
        let mut panel = make_panel(make_spec(test_source()));
        panel.t_add_group_by_column("users", "country");
        panel.t_add_aggregate(AggFn::CountStar);

        panel.aggregate_rows[0].alias = String::new();
        panel.rebuild_spec_pure();

        assert_eq!(
            panel.incomplete_aggregate_row_count, 1,
            "row with empty alias IS incomplete regardless of function"
        );
    }

    #[test]
    fn generate_alias_sanitizes_dotted_column_name() {
        let panel = make_panel(make_spec(test_source()));

        let alias = panel.generate_aggregate_alias(AggFn::Sum, "users.name");
        assert_eq!(alias, "sum_users_name");

        let alias_nested = panel.generate_aggregate_alias(AggFn::Max, "a.b.c");
        assert_eq!(alias_nested, "max_a_b_c");
    }

    #[test]
    fn generate_alias_sanitizes_other_special_chars() {
        let panel = make_panel(make_spec(test_source()));
        let alias = panel.generate_aggregate_alias(AggFn::Sum, "total-amount");
        assert_eq!(alias, "sum_total_amount");
    }
}
