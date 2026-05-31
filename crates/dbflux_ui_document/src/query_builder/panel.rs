use dbflux_core::{
    BoolOp, ColumnKind, Comparator, FilterNode, JoinKind, JoinOn, JoinStep, Predicate,
    PredicateValue, ProjectedColumn, Projection, SchemaForeignKeyInfo, SelectQuery, SortEntry,
    SourceTable, VisualQuerySpec, VisualSortDirection,
};
use gpui::{App, Context, Entity, EventEmitter, FocusHandle, Render, WeakEntity, Window};
use uuid::Uuid;

use crate::data_grid_panel::DataGridPanel;
use crate::query_builder::events::BuilderEvent;

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
/// Owns a `VisualQuerySpec` that starts as a copy of `DataGridPanel.visual_query`
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
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl QueryBuilderPanel {
    /// Creates a new panel for the given source table.
    ///
    /// `initial_spec` should be `DataGridPanel.visual_query` if the user had
    /// previously run the builder; `None` produces the default spec.
    /// `generate_preview` is a closure that calls the driver's
    /// `QueryGenerator::generate_select` and returns the SQL text.
    pub fn new(
        source: SourceTable,
        initial_spec: Option<VisualQuerySpec>,
        data_grid: Option<WeakEntity<DataGridPanel>>,
        generate_preview: Box<dyn Fn(&VisualQuerySpec) -> String + Send + Sync>,
        cx: &mut Context<Self>,
    ) -> Self {
        let spec = initial_spec.unwrap_or_else(|| VisualQuerySpec {
            source: source.clone(),
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(DEFAULT_LIMIT),
            offset: 0,
        });

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
                    JoinOn::RawExpression(_) => String::new(),
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
    /// and needs to re-hydrate from `DataGridPanel.visual_query`).
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
                    JoinOn::RawExpression(_) => String::new(),
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
        self.sort_rows = sort_rows;
        self.sql_preview = (self.generate_preview)(&spec);
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

    // -----------------------------------------------------------------------
    // Join mutations
    // -----------------------------------------------------------------------

    /// Appends a new join row with default raw-expression mode.
    pub fn add_join(&mut self, from_alias: &str, cx: &mut Context<Self>) {
        self.join_rows.push(JoinRow {
            kind: JoinKind::Inner,
            from_alias: from_alias.to_string(),
            from_column: String::new(),
            to_schema: None,
            to_table: String::new(),
            to_alias: String::new(),
            on: JoinOn::RawExpression(String::new()),
        });
        self.rebuild_spec_and_notify(cx);
    }

    /// Removes a join row by its index.
    pub fn remove_join(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.join_rows.len() {
            self.join_rows.remove(index);
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
                    JoinOn::RawExpression(_) => String::new(),
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
}
