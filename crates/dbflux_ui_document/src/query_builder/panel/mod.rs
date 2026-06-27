use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dbflux_components::controls::{
    CompletionProvider, Dropdown, DropdownItem, DropdownSelectionChanged, InputEvent, InputState,
};
use dbflux_core::{
    AggFn, BoolOp, ColumnInfo, ColumnKind, Comparator, CountSpec, FilterNode, GroupByEntry,
    JoinFilterNode, JoinKind, JoinOn, JoinPredicate, JoinStep, LiteralValue, Predicate,
    PredicateValue, ProjectedColumn, Projection, SchemaForeignKeyInfo, SelectQuery, SortEntry,
    SourceTable, VisualAggregateSpec, VisualQuerySpec, VisualSortDirection, inline_params,
    render_filter_node_sql,
};
use gpui::{
    App, AppContext, Context, Entity, EventEmitter, FocusHandle, Render, Subscription, Task,
    WeakEntity, Window,
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

/// Builds the pre-execution `SELECT COUNT(*)` for a mutation's target rows.
///
/// Filter values are inlined into the SQL because drivers in this codebase do
/// not bind `QueryRequest.params`; without inlining a filtered count fails with
/// a parameter-count error and the estimate is never produced.
fn build_mutation_count_sql(spec: &CountSpec, dialect: &dyn dbflux_core::SqlDialect) -> String {
    let qualified = dialect.qualified_table(spec.from.schema.as_deref(), &spec.from.name);

    let mut params = Vec::new();
    let mut param_index = 1;
    let where_clause =
        render_filter_node_sql(spec.filter.as_ref(), dialect, &mut params, &mut param_index);

    let sql = match where_clause {
        Some(clause) if !clause.is_empty() => {
            format!("SELECT COUNT(*) FROM {qualified} WHERE {clause}")
        }
        _ => format!("SELECT COUNT(*) FROM {qualified}"),
    };

    inline_params(&sql, &params, dialect)
}

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

    /// Generator function for mutation preview: takes a `VisualMutationSpec`, returns SQL text.
    ///
    /// Injected at construction time alongside `generate_preview`. Returns an
    /// empty string when no generator is available (e.g. no active connection).
    generate_mutation_preview:
        Box<dyn Fn(&dbflux_core::VisualMutationSpec) -> String + Send + Sync>,

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

    /// Semantic kind of each source column, keyed by column name. Used to
    /// promote free-text SET values to a typed `ScalarLiteral` (e.g. integer)
    /// when building the mutation spec, so a numeric column is not assigned a
    /// string literal. Empty when no type information is available, in which
    /// case values stay textual.
    pub(crate) column_kinds: std::collections::HashMap<String, dbflux_core::ColumnKind>,

    /// The single orderable key column for a `SortKeyOnly` driver, resolved once
    /// at panel open from the browse result's key-schema metadata. `None` when
    /// the source exposes no orderable key (e.g. a partition-key-only table).
    ///
    /// Cached deliberately: a builder-generated read can replace the live result
    /// with one that no longer carries key markers (e.g. a PartiQL `SELECT *`),
    /// so the key is captured up front rather than re-derived from whatever the
    /// grid currently shows.
    pub(crate) cached_sort_key_column: Option<String>,

    /// Serialized `(table, filter)` signature of the last mutation row-count
    /// request. Used so the count is recomputed only when the target rows
    /// actually change, not on every render or assignment edit. `None` while in
    /// SELECT mode.
    pub(crate) count_signature: Option<String>,

    /// Debounced background task that runs the pre-execution row count and
    /// writes the result into `mutation_state.count_state`. Replacing it cancels
    /// any in-flight count, so rapid filter edits coalesce to the latest spec.
    pub(crate) _count_debounce: Option<Task<()>>,

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

    /// Current builder mode state. `None` when the panel is in SELECT mode.
    /// `Some(MutationBuilderState)` when in UPDATE or DELETE mode.
    pub(crate) mutation_state: Option<crate::query_builder::mutation_state::MutationBuilderState>,

    /// Per-assignment-row column-name `InputState`, keyed by row index.
    /// Rebuilt whenever `pending_assign_rebuild` is `true`.
    pub(crate) assign_col_inputs: HashMap<usize, Entity<InputState>>,

    /// Per-assignment-row value `InputState`, keyed by row index.
    /// Rebuilt whenever `pending_assign_rebuild` is `true`.
    pub(crate) assign_val_inputs: HashMap<usize, Entity<InputState>>,

    /// Subscriptions that mirror assignment column/value inputs back into
    /// `mutation_state`. Cleared and rebuilt by `rebuild_assign_inputs` whenever
    /// the assignment list changes length. Kept separate from `_input_subs` so
    /// rebuilding assignment inputs never drops limit/offset/predicate subs.
    pub(crate) _assign_input_subs: Vec<Subscription>,

    /// `InputState` for the chunk-size field in the execution section.
    pub(crate) exec_chunk_size_input: Option<Entity<InputState>>,

    /// `InputState` for the lock-timeout field in the execution section.
    pub(crate) exec_lock_timeout_input: Option<Entity<InputState>>,

    /// Set to `true` when the assignment list changes length so the render
    /// cycle can rebuild `assign_col_inputs` / `assign_val_inputs`.
    pub(crate) pending_assign_rebuild: bool,

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
    /// `generate_mutation_preview` is a closure that calls `generate_update_from_spec`
    /// or `generate_delete_from_spec` based on `spec.kind` and returns the SQL text.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: SourceTable,
        initial_spec: Option<VisualQuerySpec>,
        data_grid: Option<WeakEntity<DataGridPanel>>,
        available_columns: Vec<String>,
        app_state: Entity<AppStateEntity>,
        profile_id: Uuid,
        generate_preview: Box<dyn Fn(&VisualQuerySpec) -> String + Send + Sync>,
        generate_mutation_preview: Box<
            dyn Fn(&dbflux_core::VisualMutationSpec) -> String + Send + Sync,
        >,
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
            generate_mutation_preview,
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
            column_kinds: std::collections::HashMap::new(),
            cached_sort_key_column: None,
            count_signature: None,
            _count_debounce: None,
            schema_cache,
            app_state_weak,
            schema_profile_id: profile_id,
            pending_filter_input_sweep: false,
            pending_join_condition_sweep: false,
            mutation_state: None,
            assign_col_inputs: HashMap::new(),
            assign_val_inputs: HashMap::new(),
            _assign_input_subs: Vec::new(),
            exec_chunk_size_input: None,
            exec_lock_timeout_input: None,
            pending_assign_rebuild: false,
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

mod columns_fetch;
mod filters;
mod grouping;
mod joins;
mod mutation;
mod projection;
mod spec;

#[cfg(test)]
mod tests;
