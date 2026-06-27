#![allow(clippy::all)]
use super::*;
use std::collections::{HashMap, HashSet};

use dbflux_core::ColumnKind;
use gpui::WeakEntity;

use super::{FILTER_DEPTH_CAP, build_mutation_count_sql};
use crate::query_builder::completion::SchemaCache;
use crate::query_builder::tree_ops::{
    filter_node_at_path_mut, insert_filter_at_path, remove_filter_at_path,
};

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
        generate_mutation_preview: Box::new(|_spec| String::new()),
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
        column_kinds: std::collections::HashMap::new(),
        cached_sort_key_column: None,
        count_signature: None,
        _count_debounce: None,
        schema_cache: Rc::new(RefCell::new(SchemaCache::default())),
        app_state_weak: WeakEntity::new_invalid(),
        schema_profile_id: Uuid::nil(),
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

    fn t_set_sort_key_direction(&mut self, direction: VisualSortDirection, sort_key_column: &str) {
        self.apply_sort_key_direction(direction, sort_key_column.to_string());
        self.rebuild_spec_pure();
    }

    fn t_push_raw_sort_row(&mut self, source_alias: &str, column: &str) {
        self.sort_rows.push(SortRow {
            source_alias: source_alias.to_string(),
            column: column.to_string(),
            direction: VisualSortDirection::Asc,
        });
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

    fn t_switch_builder_mode(&mut self, mode: crate::query_builder::mutation_state::BuilderMode) {
        use crate::query_builder::mutation_state::{BuilderMode, MutationBuilderState};

        let current = self
            .mutation_state
            .as_ref()
            .map(|s| s.mode)
            .unwrap_or(BuilderMode::Select);

        if current == mode {
            return;
        }

        match mode {
            BuilderMode::Select => {
                self.mutation_state = None;
                self.assign_col_inputs.clear();
                self.assign_val_inputs.clear();
            }
            _ => {
                self.mutation_state = Some(MutationBuilderState::new(mode));
                self.assign_col_inputs.clear();
                self.assign_val_inputs.clear();
                self.pending_assign_rebuild = true;
            }
        }

        self.refresh_mutation_preview_pure();
    }

    fn t_set_assignment_column(&mut self, row_ix: usize, text: String) {
        if let Some(state) = self.mutation_state.as_mut()
            && row_ix < state.assignments.len()
        {
            state.assignments[row_ix].assignment.column = text;
        }

        self.refresh_mutation_preview_pure();
    }

    fn t_set_assignment_raw_text(&mut self, row_ix: usize, text: String) {
        if let Some(state) = self.mutation_state.as_mut()
            && row_ix < state.assignments.len()
        {
            let row = &mut state.assignments[row_ix];

            row.raw_text = text.clone();

            row.assignment.value = match &row.assignment.value {
                dbflux_core::AssignmentValue::Literal(_) => {
                    dbflux_core::AssignmentValue::Literal(dbflux_core::ScalarLiteral::Text(text))
                }
                dbflux_core::AssignmentValue::Expression(_) => {
                    dbflux_core::AssignmentValue::Expression(text)
                }
                other => other.clone(),
            };
        }

        self.refresh_mutation_preview_pure();
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

    fn t_add_aggregate_with_column(&mut self, function: AggFn, source_alias: &str, column: &str) {
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

#[test]
fn sort_key_only_seeds_orderable_column() {
    let mut panel = make_panel(make_spec(test_source()));

    panel.t_set_sort_key_direction(VisualSortDirection::Desc, "created_at");

    assert_eq!(panel.sort_rows.len(), 1);
    assert_eq!(panel.sort_rows[0].column, "created_at");
    assert_eq!(panel.sort_rows[0].direction, VisualSortDirection::Desc);

    assert_eq!(panel.current_spec.sort.len(), 1);
    assert_eq!(panel.current_spec.sort[0].column, "created_at");
    assert_eq!(
        panel.current_spec.sort[0].direction,
        VisualSortDirection::Desc
    );
}

#[test]
fn rebuild_spec_filters_empty_column_sort_row() {
    let mut panel = make_panel(make_spec(test_source()));

    panel.t_push_raw_sort_row("users", "");
    panel.t_push_raw_sort_row("users", "created_at");
    panel.rebuild_spec_pure();

    assert_eq!(panel.current_spec.sort.len(), 1);
    assert_eq!(panel.current_spec.sort[0].column, "created_at");
}

#[test]
fn sort_key_only_without_orderable_column_emits_no_sort() {
    let mut panel = make_panel(make_spec(test_source()));

    panel.t_set_sort_key_direction(VisualSortDirection::Asc, "");

    assert_eq!(panel.sort_rows.len(), 1);
    assert!(panel.sort_rows[0].column.is_empty());
    assert!(panel.current_spec.sort.is_empty());
}

#[test]
fn sort_key_column_returns_cached_value() {
    let mut panel = make_panel(make_spec(test_source()));
    assert_eq!(panel.sort_key_column(), None);

    panel.cached_sort_key_column = Some("created_at".to_string());
    assert_eq!(panel.sort_key_column(), Some("created_at".to_string()));
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
    if let Some(root) = &mut panel.current_spec.filter
        && let Some(FilterNode::Predicate(pred)) = filter_node_at_path_mut(root, &[0])
    {
        pred.value = PredicateValue::Single(LiteralValue::Text("%foo%".to_string()));
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

// ---- Slice 8: mutation preview regen -----------------------------------

fn make_panel_with_mutation_preview(
    spec: VisualQuerySpec,
    mutation_preview: impl Fn(&dbflux_core::VisualMutationSpec) -> String + Send + Sync + 'static,
) -> QueryBuilderPanel {
    let mut panel = make_panel(spec);
    panel.generate_mutation_preview = Box::new(mutation_preview);
    panel
}

#[test]
fn switch_to_update_mode_regenerates_sql_preview() {
    use crate::query_builder::mutation_state::BuilderMode;

    let mut panel = make_panel_with_mutation_preview(make_spec(test_source()), |spec| {
        format!("UPDATE {} SET ...", spec.from.name)
    });

    assert_eq!(panel.sql_preview, "SELECT * FROM users");

    panel.t_switch_builder_mode(BuilderMode::Update);

    assert_eq!(
        panel.sql_preview, "UPDATE users SET ...",
        "preview must be regenerated when switching to UPDATE mode"
    );
}

#[test]
fn switch_to_delete_mode_regenerates_sql_preview() {
    use crate::query_builder::mutation_state::BuilderMode;

    let mut panel = make_panel_with_mutation_preview(make_spec(test_source()), |spec| {
        format!("DELETE FROM {}", spec.from.name)
    });

    panel.t_switch_builder_mode(BuilderMode::Delete);

    assert_eq!(
        panel.sql_preview, "DELETE FROM users",
        "preview must be regenerated when switching to DELETE mode"
    );
}

#[test]
fn switch_back_to_select_restores_select_preview() {
    use crate::query_builder::mutation_state::BuilderMode;

    let mut panel = make_panel_with_mutation_preview(make_spec(test_source()), |_spec| {
        "UPDATE users SET ...".to_string()
    });

    panel.t_switch_builder_mode(BuilderMode::Update);
    assert_eq!(panel.sql_preview, "UPDATE users SET ...");

    panel.t_switch_builder_mode(BuilderMode::Select);

    assert_eq!(
        panel.sql_preview, "SELECT * FROM users",
        "preview must revert to SELECT text when switching back to Select mode"
    );
}

#[test]
fn switch_to_same_mode_is_noop_for_preview() {
    use crate::query_builder::mutation_state::BuilderMode;

    let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = call_count.clone();

    let mut panel = make_panel(make_spec(test_source()));
    panel.generate_mutation_preview = Box::new(move |_spec| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        "UPDATE users SET ...".to_string()
    });

    panel.t_switch_builder_mode(BuilderMode::Update);
    let preview_after_first = panel.sql_preview.clone();

    panel.t_switch_builder_mode(BuilderMode::Update);

    assert_eq!(
        panel.sql_preview, preview_after_first,
        "preview must not change when re-selecting the current mode"
    );
    assert_eq!(
        call_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "mutation preview generator must only be called once (first switch)"
    );
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
fn assignment_column_setter_writes_to_mutation_state() {
    use crate::query_builder::mutation_state::{AssignmentRow, BuilderMode};
    use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};

    let mut panel = make_panel(make_spec(test_source()));
    panel.t_switch_builder_mode(BuilderMode::Update);

    panel
        .mutation_state
        .as_mut()
        .unwrap()
        .assignments
        .push(AssignmentRow {
            assignment: Assignment {
                column: String::new(),
                value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
            },
            raw_text: String::new(),
        });

    // Simulate what the subscription fires by calling the test helper directly.
    // A live GPUI context is required for cx.notify(); the t_* helpers bypass that.
    panel.t_set_assignment_column(0, "email".to_string());

    assert_eq!(
        panel.mutation_state.as_ref().unwrap().assignments[0]
            .assignment
            .column,
        "email",
    );
}

#[test]
fn assignment_value_setter_writes_raw_text_and_derives_value() {
    use crate::query_builder::mutation_state::{AssignmentRow, BuilderMode};
    use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};

    let mut panel = make_panel(make_spec(test_source()));
    panel.t_switch_builder_mode(BuilderMode::Update);

    panel
        .mutation_state
        .as_mut()
        .unwrap()
        .assignments
        .push(AssignmentRow {
            assignment: Assignment {
                column: "name".to_string(),
                value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
            },
            raw_text: String::new(),
        });

    panel.t_set_assignment_raw_text(0, "Alice".to_string());

    let row = &panel.mutation_state.as_ref().unwrap().assignments[0];
    assert_eq!(row.raw_text, "Alice");
    assert_eq!(
        row.assignment.value,
        AssignmentValue::Literal(ScalarLiteral::Text("Alice".to_string())),
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
fn adding_assignment_preserves_prior_typed_values() {
    use crate::query_builder::mutation_state::{AssignmentRow, BuilderMode};
    use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};

    let mut panel = make_panel(make_spec(test_source()));
    panel.t_switch_builder_mode(BuilderMode::Update);

    panel
        .mutation_state
        .as_mut()
        .unwrap()
        .assignments
        .push(AssignmentRow {
            assignment: Assignment {
                column: String::new(),
                value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
            },
            raw_text: String::new(),
        });

    // User types into the first assignment row via the setters.
    panel.t_set_assignment_column(0, "email".to_string());
    panel.t_set_assignment_raw_text(0, "alice@example.com".to_string());

    // User clicks "+ Add assignment".
    panel
        .mutation_state
        .as_mut()
        .unwrap()
        .assignments
        .push(AssignmentRow {
            assignment: Assignment {
                column: String::new(),
                value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
            },
            raw_text: String::new(),
        });
    panel.pending_assign_rebuild = true;

    // The render cycle would call rebuild_assign_inputs (requires Window),
    // but the state in mutation_state is what matters for the spec builder.
    // Assert the first row still holds the typed values.
    let first = &panel.mutation_state.as_ref().unwrap().assignments[0];
    assert_eq!(first.assignment.column, "email");
    assert_eq!(first.raw_text, "alice@example.com");
    assert_eq!(
        first.assignment.value,
        AssignmentValue::Literal(ScalarLiteral::Text("alice@example.com".to_string())),
    );
}

#[test]
fn build_mutation_spec_promotes_text_to_integer_for_integer_column() {
    use crate::query_builder::mutation_state::{AssignmentRow, BuilderMode};
    use dbflux_core::{Assignment, AssignmentValue, ColumnKind, MutationKind, ScalarLiteral};

    let mut panel = make_panel(make_spec(test_source()));
    panel
        .column_kinds
        .insert("id".to_string(), ColumnKind::Integer);
    panel.t_switch_builder_mode(BuilderMode::Update);

    panel
        .mutation_state
        .as_mut()
        .unwrap()
        .assignments
        .push(AssignmentRow {
            assignment: Assignment {
                column: String::new(),
                value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
            },
            raw_text: String::new(),
        });

    panel.t_set_assignment_column(0, "id".to_string());
    panel.t_set_assignment_raw_text(0, "12".to_string());

    let (spec, _opts) = panel
        .build_mutation_spec_and_opts()
        .expect("update spec must build");

    match spec.kind {
        MutationKind::Update { assignments } => {
            assert_eq!(
                assignments[0].value,
                AssignmentValue::Literal(ScalarLiteral::Integer(12)),
                "an integer column must receive a typed integer literal, not a string"
            );
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn build_mutation_count_sql_inlines_filter_value() {
    use dbflux_core::{
        Comparator, CountSpec, DefaultSqlDialect, FilterNode, LiteralValue, Predicate,
        PredicateValue, TableRef,
    };

    let spec = CountSpec {
        from: TableRef {
            schema: None,
            name: "obs".to_string(),
        },
        filter: Some(FilterNode::Predicate(Predicate {
            source_alias: "obs".to_string(),
            column: "status".to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Integer(3)),
            node_id: 0,
        })),
    };

    let sql = build_mutation_count_sql(&spec, &DefaultSqlDialect);

    assert!(sql.starts_with("SELECT COUNT(*) FROM"), "got: {sql}");
    assert!(
        sql.contains("WHERE"),
        "filtered count must have WHERE: {sql}"
    );
    assert!(
        !sql.contains('?'),
        "filter value must be inlined, not a placeholder: {sql}"
    );
    assert!(sql.contains('3'), "inlined filter value missing: {sql}");
}

#[test]
fn build_mutation_count_sql_without_filter_has_no_where() {
    use dbflux_core::{CountSpec, DefaultSqlDialect, TableRef};

    let spec = CountSpec {
        from: TableRef {
            schema: None,
            name: "obs".to_string(),
        },
        filter: None,
    };

    let sql = build_mutation_count_sql(&spec, &DefaultSqlDialect);

    assert!(!sql.contains("WHERE"), "got: {sql}");
    assert!(!sql.contains('?'), "got: {sql}");
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
fn mutation_preview_reflects_typed_assignment() {
    use crate::query_builder::mutation_state::{AssignmentRow, BuilderMode};
    use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};

    let mut panel = make_panel_with_mutation_preview(make_spec(test_source()), |spec| {
        use dbflux_core::MutationKind;
        match &spec.kind {
            MutationKind::Update { assignments } => assignments
                .iter()
                .map(|a| format!("{}=?", a.column))
                .collect::<Vec<_>>()
                .join(","),
            MutationKind::Delete => "DELETE".to_string(),
        }
    });

    panel.t_switch_builder_mode(BuilderMode::Update);

    panel
        .mutation_state
        .as_mut()
        .unwrap()
        .assignments
        .push(AssignmentRow {
            assignment: Assignment {
                column: String::new(),
                value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
            },
            raw_text: String::new(),
        });

    panel.t_set_assignment_column(0, "email".to_string());
    panel.t_set_assignment_raw_text(0, "alice@example.com".to_string());

    assert_eq!(
        panel.sql_preview, "email=?",
        "sql_preview must be regenerated when an assignment column is typed",
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
