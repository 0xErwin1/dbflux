//! Keyboard access to the SQL query builder rail.
//!
//! The rail lists its rows for [`rail_command`] in the order the sections
//! draw them: the columns, the WHERE tree, the joins, the grouping, the
//! HAVING tree and the sort and paging fields in SELECT mode; the SET
//! assignments, the WHERE tree and the execution options in UPDATE and
//! DELETE mode. Each row's fields are the controls the pointer reaches on
//! that line, and its row-wide keys call the same panel methods as the
//! row's buttons. Run, save, reset, open in editor, close and the modes are
//! rail-wide entries of the action menu; Run and Save also answer their
//! chords, and Alt+H / Alt+L switch the mode. A run from the keyboard emits
//! the same event the Run button does, so the grid applies the same
//! confirmation and mutation policy to it.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{
    RailMenuEntry, RailOutcome, RailOwner, RailRow, RailTarget, rail_command,
};
use dbflux_core::{
    Assignment, AssignmentValue, Comparator, FilterNode, JoinFilterNode, JoinOn, OrderByMode,
    ScalarLiteral, VisualSortDirection,
};
use dbflux_ui_base::keymap::{chord_display_parts, effective_keymap};
use gpui::{App, Context, FocusHandle, SharedString, Window};

use crate::data_grid_panel::mutation_executor::{CountState, ExecutionMode};
use crate::query_builder::events::BuilderEvent;
use crate::query_builder::mutation_state::{AssignmentRow, BuilderMode};
use crate::query_builder::panel::{
    AGG_FN_ORDER, FILTER_DEPTH_CAP, FilterTarget, FkLoadState, ProjectionMode, QueryBuilderPanel,
};
use crate::query_builder::sections::assignments::cycle_value_kind;

/// Row ids of the rail, shared by the rows and the sections that draw them.
pub(crate) mod row_id {
    use crate::query_builder::panel::FilterTarget;

    fn tree(target: FilterTarget) -> &'static str {
        match target {
            FilterTarget::Where => "where",
            FilterTarget::Having => "having",
        }
    }

    fn path_key(path: &[usize]) -> String {
        path.iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join("-")
    }

    pub fn all_columns() -> String {
        "columns-all".to_string()
    }

    pub fn picked_column(alias: &str, column: &str) -> String {
        format!("columns-picked-{alias}.{column}")
    }

    pub fn column_choice(column: &str) -> String {
        format!("columns-choice-{column}")
    }

    pub fn column_entry() -> String {
        "columns-entry".to_string()
    }

    pub fn filters_empty(target: FilterTarget) -> String {
        format!("{}-empty", tree(target))
    }

    pub fn filter_group(target: FilterTarget, path: &[usize]) -> String {
        format!("{}-group-{}", tree(target), path_key(path))
    }

    pub fn filter_predicate(target: FilterTarget, node_id: u64) -> String {
        format!("{}-predicate-{node_id}", tree(target))
    }

    pub fn fk_banner() -> String {
        "joins-banner".to_string()
    }

    pub fn join(index: usize) -> String {
        format!("join-{index}")
    }

    pub fn join_group(index: usize, path: &[usize]) -> String {
        format!("join-{index}-group-{}", path_key(path))
    }

    pub fn join_condition(index: usize, node_id: u64) -> String {
        format!("join-{index}-condition-{node_id}")
    }

    pub fn join_expression(index: usize) -> String {
        format!("join-{index}-on")
    }

    pub fn join_add() -> String {
        "joins-add".to_string()
    }

    pub fn group_by(index: usize) -> String {
        format!("group-by-{index}")
    }

    pub fn group_by_add() -> String {
        "group-by-add".to_string()
    }

    pub fn aggregate(index: usize) -> String {
        format!("aggregate-{index}")
    }

    pub fn aggregate_add() -> String {
        "aggregate-add".to_string()
    }

    pub fn sort(index: usize) -> String {
        format!("sort-{index}")
    }

    pub fn sort_key() -> String {
        "sort-key".to_string()
    }

    /// The last line of the sort card: adding a sort key, limit and offset.
    pub fn paging() -> String {
        "paging".to_string()
    }

    pub fn assignment(index: usize) -> String {
        format!("set-{index}")
    }

    pub fn assignment_add() -> String {
        "set-add".to_string()
    }

    pub fn execution_mode() -> String {
        "execution-mode".to_string()
    }

    pub fn chunk_size() -> String {
        "execution-chunk".to_string()
    }

    pub fn lock_timeout() -> String {
        "execution-lock".to_string()
    }
}

/// The builder modes in the order of the mode switch.
const MODES: [BuilderMode; 3] = [
    BuilderMode::Select,
    BuilderMode::Update,
    BuilderMode::Delete,
];

/// The execution modes in the order of their buttons.
const EXECUTION_MODES: [ExecutionMode; 3] = [
    ExecutionMode::SingleTransaction,
    ExecutionMode::ChunkedTransaction,
    ExecutionMode::DirectAutocommit,
];

impl QueryBuilderPanel {
    /// The mode the rail edits.
    pub(crate) fn builder_mode(&self) -> BuilderMode {
        self.mutation_state
            .as_ref()
            .map(|state| state.mode)
            .unwrap_or(BuilderMode::Select)
    }

    /// Whether Run can run now, as the Run button's enabled state.
    pub(crate) fn can_run(&self) -> bool {
        match &self.mutation_state {
            Some(state) => !state.is_update_with_no_assignments(),
            None => self.is_runnable(),
        }
    }

    /// Run: the SELECT, or the UPDATE / DELETE with its execution options and
    /// the current row estimate. The grid applies its confirmation and
    /// mutation policy to the event.
    pub(crate) fn request_run(&mut self, cx: &mut Context<Self>) {
        if !self.can_run() {
            return;
        }

        if !self.builder_mode().is_mutation() {
            cx.emit(BuilderEvent::RunRequested);
            return;
        }

        if let Some((spec, opts)) = self.build_mutation_spec_and_opts() {
            let est_rows =
                self.mutation_state
                    .as_ref()
                    .and_then(|state| match &state.count_state {
                        CountState::Done(count) => Some(*count),
                        _ => None,
                    });

            cx.emit(BuilderEvent::MutationRunRequested {
                spec: Box::new(spec),
                opts: Box::new(opts),
                est_rows,
            });
        }
    }

    /// Save, under the loaded query's name or a new untitled one.
    pub(crate) fn request_save(&mut self, cx: &mut Context<Self>) {
        let name = self
            .loaded_id
            .clone()
            .unwrap_or_else(|| dbflux_i18n::t!("document.query_builder.chrome.untitled_query"));
        cx.emit(BuilderEvent::SaveRequested { name });
    }

    pub(crate) fn add_assignment(&mut self, cx: &mut Context<Self>) {
        if let Some(state) = self.mutation_state.as_mut() {
            state.assignments.push(AssignmentRow {
                assignment: Assignment {
                    column: String::new(),
                    value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
                },
                raw_text: String::new(),
            });
            self.pending_assign_rebuild = true;
        }
        self.refresh_mutation_preview_pure();
        cx.notify();
    }

    pub(crate) fn remove_assignment(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(state) = self.mutation_state.as_mut()
            && index < state.assignments.len()
        {
            state.assignments.remove(index);
            self.pending_assign_rebuild = true;
        }
        self.refresh_mutation_preview_pure();
        cx.notify();
    }

    /// Steps the value kind of the assignment at `index`: literal, raw SQL,
    /// NULL, DEFAULT.
    pub(crate) fn cycle_assignment_kind(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(state) = self.mutation_state.as_mut()
            && let Some(row) = state.assignments.get_mut(index)
        {
            row.assignment.value = cycle_value_kind(&row.assignment.value, &row.raw_text);
        }
        self.refresh_mutation_preview_pure();
        cx.notify();
    }

    pub(crate) fn set_execution_mode(&mut self, mode: ExecutionMode, cx: &mut Context<Self>) {
        if let Some(state) = self.mutation_state.as_mut() {
            state.exec_options.mode = mode;
        }
        cx.notify();
    }

    /// Shows the next (or previous) mode, wrapping, when the mode switch is
    /// shown. Returns whether the switch is there.
    fn step_mode(&mut self, forward: bool, cx: &mut Context<Self>) -> bool {
        if !self.shows_mutation_selector(cx) {
            return false;
        }

        let current = MODES
            .iter()
            .position(|mode| *mode == self.builder_mode())
            .unwrap_or(0);
        let next = if forward {
            (current + 1) % MODES.len()
        } else {
            (current + MODES.len() - 1) % MODES.len()
        };

        self.switch_builder_mode(MODES[next], cx);
        self.rail.reset();
        true
    }

    /// Answers a key while the rail holds the keyboard.
    pub fn keyboard_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> RailOutcome {
        if !self.rail.menu_is_open() {
            match command {
                Command::RunQuery => {
                    self.request_run(cx);
                    return RailOutcome::Handled;
                }
                Command::SaveQuery => {
                    self.request_save(cx);
                    return RailOutcome::Handled;
                }
                Command::NextPanelTab | Command::PrevPanelTab => {
                    self.step_mode(command == Command::NextPanelTab, cx);
                    return RailOutcome::Handled;
                }
                _ => {}
            }
        }

        rail_command(self, command, window, cx)
    }

    /// Whether the rail's action menu is open.
    pub fn keyboard_menu_is_open(&self) -> bool {
        self.rail.menu_is_open()
    }

    fn column_rows(&self, rows: &mut Vec<RailRow<Self>>) {
        let all = self.projection_mode == ProjectionMode::All;
        rows.push(
            RailRow::new(row_id::all_columns())
                .field(
                    "all",
                    RailTarget::run(move |this: &mut Self, _, cx| this.set_all_columns(!all, cx)),
                )
                .on_toggle(move |this: &mut Self, _, cx| this.set_all_columns(!all, cx)),
        );

        if all {
            return;
        }

        let open_picker = || {
            RailTarget::run(|this: &mut Self, _, cx| {
                this.column_picker_open = !this.column_picker_open;
                cx.notify();
            })
        };

        for picked in &self.projection_rows {
            let alias = picked.source_alias.clone();
            let column = picked.column.clone();
            let remove = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.toggle_column(&alias, &column, cx)
            };
            rows.push(
                RailRow::new(row_id::picked_column(&picked.source_alias, &picked.column))
                    .field("chip", RailTarget::run(remove.clone()))
                    .on_remove(remove)
                    .on_add(open_picker()),
            );
        }

        if !self.column_picker_open {
            rows.push(
                RailRow::new(row_id::column_entry())
                    .field("add-chip", open_picker())
                    .on_add(open_picker()),
            );
            return;
        }

        let source_alias = self.current_spec.source.alias.clone();
        for column in &self.available_columns {
            let alias = source_alias.clone();
            let name = column.clone();
            let toggle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.toggle_column(&alias, &name, cx)
            };
            rows.push(
                RailRow::new(row_id::column_choice(column))
                    .field("check", RailTarget::run(toggle.clone()))
                    .on_toggle(toggle),
            );
        }

        let mut entry = RailRow::new(row_id::column_entry()).field("add-chip", open_picker());
        if let Some(state) = self.add_column_input_state.clone() {
            entry = entry
                .field("input", RailTarget::text(state))
                .field("add", RailTarget::run(Self::add_column_from_entry));
        }
        rows.push(entry);
    }

    /// The "alias.column" entry's Add button.
    pub(crate) fn add_column_from_entry(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(state) = self.add_column_input_state.clone() else {
            return;
        };

        let text = state.read(cx).value().trim().to_string();
        if text.is_empty() {
            return;
        }

        let (alias, column) = match text.split_once('.') {
            Some((alias, column)) => (alias.trim().to_string(), column.trim().to_string()),
            None => (self.current_spec.source.alias.clone(), text.clone()),
        };
        self.add_column(&alias, &column, cx);
        state.update(cx, |state, cx| state.set_value("", window, cx));
    }

    fn filter_rows(&self, target: FilterTarget, rows: &mut Vec<RailRow<Self>>) {
        let (tree, source_alias) = match target {
            FilterTarget::Where => (
                self.current_spec.filter.as_ref(),
                self.current_spec.source.alias.clone(),
            ),
            FilterTarget::Having => (self.current_spec.having.as_ref(), String::new()),
        };

        let add_predicate = |path: Vec<usize>| {
            let alias = source_alias.clone();
            RailTarget::run(move |this: &mut Self, _, cx| {
                this.add_predicate_for(target, path.clone(), &alias, "", cx)
            })
        };
        let add_group = |path: Vec<usize>| {
            RailTarget::run(move |this: &mut Self, _, cx| {
                this.add_group_for(target, path.clone(), cx)
            })
        };

        let Some(root) = tree else {
            rows.push(
                RailRow::new(row_id::filters_empty(target))
                    .field("add-filter", add_predicate(Vec::new()))
                    .field("add-group", add_group(Vec::new()))
                    .on_add(add_predicate(Vec::new()))
                    .on_add_group(add_group(Vec::new())),
            );
            return;
        };

        self.filter_node_rows(target, root, Vec::new(), &add_predicate, &add_group, rows);
    }

    fn filter_node_rows(
        &self,
        target: FilterTarget,
        node: &FilterNode,
        path: Vec<usize>,
        add_predicate: &dyn Fn(Vec<usize>) -> RailTarget<Self>,
        add_group: &dyn Fn(Vec<usize>) -> RailTarget<Self>,
        rows: &mut Vec<RailRow<Self>>,
    ) {
        let parent = path[..path.len().saturating_sub(1)].to_vec();

        match node {
            FilterNode::Group { children, .. } => {
                let at_cap = path.len() >= FILTER_DEPTH_CAP;
                let toggle_path = path.clone();
                let toggle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                    this.toggle_group_op_for(target, toggle_path.clone(), cx)
                };

                let mut row = RailRow::new(row_id::filter_group(target, &path))
                    .field("op", RailTarget::run(toggle.clone()))
                    .on_toggle(toggle);

                if !at_cap {
                    row = row
                        .field("add-filter", add_predicate(path.clone()))
                        .field("add-group", add_group(path.clone()))
                        .on_add(add_predicate(path.clone()))
                        .on_add_group(add_group(path.clone()));
                }

                if !path.is_empty() {
                    let remove_path = path.clone();
                    row = row.on_remove(move |this: &mut Self, _, cx| {
                        this.remove_filter_node_for(target, remove_path.clone(), cx)
                    });
                }

                rows.push(row);

                for (index, child) in children.iter().enumerate() {
                    let mut child_path = path.clone();
                    child_path.push(index);
                    self.filter_node_rows(
                        target,
                        child,
                        child_path,
                        add_predicate,
                        add_group,
                        rows,
                    );
                }
            }
            FilterNode::Predicate(predicate) => {
                let id = predicate.node_id;
                let (values, columns, comparators) = match target {
                    FilterTarget::Where => (
                        &self.predicate_input_states,
                        &self.predicate_column_input_states,
                        &self.predicate_comparator_dropdowns,
                    ),
                    FilterTarget::Having => (
                        &self.having_predicate_input_states,
                        &self.having_predicate_column_input_states,
                        &self.having_predicate_comparator_dropdowns,
                    ),
                };

                let mut row = RailRow::new(row_id::filter_predicate(target, id));
                if let Some(state) = columns.get(&id) {
                    row = row.field("column", RailTarget::text(state.clone()));
                }
                if let Some(dropdown) = comparators.get(&id) {
                    row = row.field("op", RailTarget::dropdown(dropdown.clone()));
                }
                let needs_value = !matches!(
                    predicate.comparator,
                    Comparator::IsNull | Comparator::IsNotNull
                );
                if needs_value && let Some(state) = values.get(&id) {
                    row = row.field("value", RailTarget::text(state.clone()));
                }

                let remove_path = path.clone();
                rows.push(
                    row.on_remove(move |this: &mut Self, _, cx| {
                        this.remove_filter_node_for(target, remove_path.clone(), cx)
                    })
                    .on_add(add_predicate(parent.clone()))
                    .on_add_group(add_group(parent)),
                );
            }
        }
    }

    fn join_rows_for_rail(&self, rows: &mut Vec<RailRow<Self>>) {
        let source_alias = self.current_spec.source.alias.clone();
        let add_join = || {
            let alias = source_alias.clone();
            RailTarget::run(move |this: &mut Self, _, cx| this.add_join(&alias, cx))
        };

        if matches!(self.fk_state, FkLoadState::Unavailable) && !self.fk_banner_dismissed {
            rows.push(RailRow::new(row_id::fk_banner()).field(
                "dismiss",
                RailTarget::run(|this: &mut Self, _, cx| this.dismiss_fk_banner(cx)),
            ));
        }

        for (index, join) in self.join_rows.iter().enumerate() {
            let mut row = RailRow::new(row_id::join(index));
            if let Some(dropdown) = self.join_kind_dropdowns.get(index) {
                row = row.field("kind", RailTarget::dropdown(dropdown.clone()));
            }
            if let Some((table, _)) = self.join_input_states.get(index) {
                row = row.field("table", RailTarget::text(table.clone()));
            }
            rows.push(
                row.on_remove(move |this: &mut Self, _, cx| this.remove_join(index, cx))
                    .on_add(add_join()),
            );

            match &join.on {
                JoinOn::Conditions(root) => {
                    self.join_node_rows(index, root, Vec::new(), rows);
                }
                JoinOn::RawExpression(_) => {
                    if let Some((_, expression)) = self.join_input_states.get(index) {
                        rows.push(
                            RailRow::new(row_id::join_expression(index))
                                .field("on", RailTarget::text(expression.clone())),
                        );
                    }
                }
                JoinOn::FkPath { .. } => {}
            }
        }

        rows.push(
            RailRow::new(row_id::join_add())
                .field("add", add_join())
                .on_add(add_join()),
        );
    }

    fn join_node_rows(
        &self,
        join: usize,
        node: &JoinFilterNode,
        path: Vec<usize>,
        rows: &mut Vec<RailRow<Self>>,
    ) {
        let add_condition = |path: Vec<usize>| {
            RailTarget::run(move |this: &mut Self, _, cx| {
                this.add_join_condition(join, path.clone(), cx)
            })
        };
        let add_group = |path: Vec<usize>| {
            RailTarget::run(move |this: &mut Self, _, cx| {
                this.add_join_subgroup(join, path.clone(), cx)
            })
        };
        let parent = path[..path.len().saturating_sub(1)].to_vec();

        match node {
            JoinFilterNode::Group { children, .. } => {
                let toggle_path = path.clone();
                let toggle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                    this.toggle_join_group_op(join, toggle_path.clone(), cx)
                };

                let mut row = RailRow::new(row_id::join_group(join, &path))
                    .field("op", RailTarget::run(toggle.clone()))
                    .field("add-condition", add_condition(path.clone()))
                    .field("add-group", add_group(path.clone()))
                    .on_toggle(toggle)
                    .on_add(add_condition(path.clone()))
                    .on_add_group(add_group(path.clone()));

                if !path.is_empty() {
                    let remove_path = path.clone();
                    row = row.on_remove(move |this: &mut Self, _, cx| {
                        this.remove_join_node(join, remove_path.clone(), cx)
                    });
                }
                rows.push(row);

                for (index, child) in children.iter().enumerate() {
                    let mut child_path = path.clone();
                    child_path.push(index);
                    self.join_node_rows(join, child, child_path, rows);
                }
            }
            JoinFilterNode::Predicate(predicate) => {
                let id = predicate.node_id;
                let mut row = RailRow::new(row_id::join_condition(join, id));
                if let Some(state) = self.join_cond_left_inputs.get(&id) {
                    row = row.field("left", RailTarget::text(state.clone()));
                }
                if let Some(dropdown) = self.join_cond_op_dropdowns.get(&id) {
                    row = row.field("op", RailTarget::dropdown(dropdown.clone()));
                }
                if let Some(state) = self.join_cond_right_inputs.get(&id) {
                    row = row.field("right", RailTarget::text(state.clone()));
                }

                let remove_path = path.clone();
                rows.push(
                    row.on_remove(move |this: &mut Self, _, cx| {
                        this.remove_join_node(join, remove_path.clone(), cx)
                    })
                    .on_add(add_condition(parent.clone()))
                    .on_add_group(add_group(parent)),
                );
            }
        }
    }

    fn group_by_rows_for_rail(&self, rows: &mut Vec<RailRow<Self>>) {
        let source_alias = self.current_spec.source.alias.clone();
        let add_column = || {
            let alias = source_alias.clone();
            RailTarget::run(move |this: &mut Self, _, cx| {
                this.add_group_by_column(alias.clone(), String::new(), cx)
            })
        };
        let add_aggregate = |function| {
            RailTarget::run(move |this: &mut Self, _, cx| this.add_aggregate(function, cx))
        };

        for index in 0..self.group_by_rows.len() {
            let mut row = RailRow::new(row_id::group_by(index));
            if let Some(state) = self.group_by_col_inputs.get(index) {
                row = row.field("column", RailTarget::text(state.clone()));
            }
            rows.push(
                row.on_remove(move |this: &mut Self, _, cx| this.remove_group_by_row(index, cx))
                    .on_add(add_column()),
            );
        }

        rows.push(
            RailRow::new(row_id::group_by_add())
                .field("add", add_column())
                .on_add(add_column()),
        );

        for (index, aggregate) in self.aggregate_rows.iter().enumerate() {
            let mut row = RailRow::new(row_id::aggregate(index));
            if let Some(dropdown) = self.agg_fn_dropdowns.get(index) {
                row = row.field("function", RailTarget::dropdown(dropdown.clone()));
            }
            if aggregate.function != dbflux_core::AggFn::CountStar
                && let Some(state) = self.agg_col_inputs.get(index)
            {
                row = row.field("column", RailTarget::text(state.clone()));
            }
            if let Some(state) = self.agg_alias_inputs.get(index) {
                row = row.field("alias", RailTarget::text(state.clone()));
            }
            rows.push(
                row.on_remove(move |this: &mut Self, _, cx| this.remove_aggregate_row(index, cx))
                    .on_add(add_aggregate(AGG_FN_ORDER[0])),
            );
        }

        let mut add_row =
            RailRow::new(row_id::aggregate_add()).on_add(add_aggregate(AGG_FN_ORDER[0]));
        for function in AGG_FN_ORDER {
            add_row = add_row.field(
                format!("add-{}", crate::labels::agg_fn_display(*function)),
                add_aggregate(*function),
            );
        }
        rows.push(add_row);
    }

    fn sort_rows_for_rail(&self, cx: &App, rows: &mut Vec<RailRow<Self>>) {
        let limit = self
            .limit_input_state
            .clone()
            .map(|state| ("limit", RailTarget::text(state)));
        let offset = self
            .offset_input_state
            .clone()
            .map(|state| ("offset", RailTarget::text(state)));
        let add = self.sort_add_dropdown.clone().map(RailTarget::dropdown);

        let mut paging = RailRow::new(row_id::paging());

        match self.order_by_mode(cx) {
            OrderByMode::SortKeyOnly => {
                let flip = |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                    let next = match this.sort_key_direction() {
                        VisualSortDirection::Asc => VisualSortDirection::Desc,
                        VisualSortDirection::Desc => VisualSortDirection::Asc,
                    };
                    this.set_sort_key_direction(next, cx);
                };
                rows.push(
                    RailRow::new(row_id::sort_key())
                        .field("dir", RailTarget::run(flip))
                        .on_toggle(flip),
                );
                paging = paging.fields_from(limit).fields_from(offset);
            }
            OrderByMode::None => {
                paging = paging.fields_from(limit).fields_from(offset);
            }
            OrderByMode::AnyColumns => {
                let mut limit = limit;

                if self.sort_rows.is_empty() {
                    if let Some(add) = add.clone() {
                        paging = paging.field("add", add.clone()).on_add(add);
                    }
                    paging = paging.fields_from(limit.take());
                }

                for index in 0..self.sort_rows.len() {
                    let mut row = RailRow::new(row_id::sort(index));
                    if let Some(dropdown) = self.sort_column_dropdowns.get(index) {
                        row = row.field("column", RailTarget::dropdown(dropdown.clone()));
                    }
                    let flip = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                        this.toggle_sort_direction(index, cx)
                    };
                    row = row.field("dir", RailTarget::run(flip)).on_toggle(flip);
                    if index == 0 {
                        row = row.fields_from(limit.take());
                    }
                    if let Some(add) = add.clone() {
                        row = row.on_add(add);
                    }
                    rows.push(
                        row.on_remove(move |this: &mut Self, _, cx| this.remove_sort(index, cx)),
                    );
                }

                if !self.sort_rows.is_empty()
                    && let Some(add) = add
                {
                    paging = paging.field("add", add.clone()).on_add(add);
                }
                paging = paging.fields_from(offset);
            }
        }

        if !paging.fields.is_empty() {
            rows.push(paging);
        }
    }

    fn assignment_rows(&self, rows: &mut Vec<RailRow<Self>>) {
        let add = || RailTarget::run(|this: &mut Self, _, cx| this.add_assignment(cx));
        let count = self
            .mutation_state
            .as_ref()
            .map(|state| state.assignments.len())
            .unwrap_or(0);

        for index in 0..count {
            let shows_value = self
                .mutation_state
                .as_ref()
                .and_then(|state| state.assignments.get(index))
                .is_some_and(|row| {
                    matches!(
                        row.assignment.value,
                        AssignmentValue::Literal(_) | AssignmentValue::Expression(_)
                    )
                });

            let mut row = RailRow::new(row_id::assignment(index));
            if let Some(state) = self.assign_col_inputs.get(&index) {
                row = row.field("column", RailTarget::text(state.clone()));
            }
            if shows_value && let Some(state) = self.assign_val_inputs.get(&index) {
                row = row.field("value", RailTarget::text(state.clone()));
            }
            let cycle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.cycle_assignment_kind(index, cx)
            };
            rows.push(
                row.field("kind", RailTarget::run(cycle))
                    .on_toggle(cycle)
                    .on_remove(move |this: &mut Self, _, cx| this.remove_assignment(index, cx))
                    .on_add(add()),
            );
        }

        rows.push(
            RailRow::new(row_id::assignment_add())
                .field("add", add())
                .on_add(add()),
        );
    }

    fn execution_rows(&self, rows: &mut Vec<RailRow<Self>>) {
        let Some(state) = self.mutation_state.as_ref() else {
            return;
        };

        let mut modes = RailRow::new(row_id::execution_mode());
        for mode in EXECUTION_MODES {
            modes = modes.field(
                format!("mode-{}", mode as usize),
                RailTarget::run(move |this: &mut Self, _, cx| this.set_execution_mode(mode, cx)),
            );
        }
        let current = state.exec_options.mode;
        rows.push(modes.on_toggle(move |this: &mut Self, _, cx| {
            let index = EXECUTION_MODES
                .iter()
                .position(|mode| *mode == current)
                .unwrap_or(0);
            this.set_execution_mode(EXECUTION_MODES[(index + 1) % EXECUTION_MODES.len()], cx)
        }));

        if current == ExecutionMode::ChunkedTransaction
            && let Some(input) = self.exec_chunk_size_input.clone()
        {
            rows.push(RailRow::new(row_id::chunk_size()).field("input", RailTarget::text(input)));
        }
        if current != ExecutionMode::DirectAutocommit
            && let Some(input) = self.exec_lock_timeout_input.clone()
        {
            rows.push(RailRow::new(row_id::lock_timeout()).field("input", RailTarget::text(input)));
        }
    }
}

/// Adds an optional field, as the builder rows carry fields that exist only
/// for some drivers.
trait OptionalField<T: 'static> {
    fn fields_from(self, field: Option<(&'static str, RailTarget<T>)>) -> Self;
}

impl<T: 'static> OptionalField<T> for RailRow<T> {
    fn fields_from(self, field: Option<(&'static str, RailTarget<T>)>) -> Self {
        match field {
            Some((id, target)) => self.field(id, target),
            None => self,
        }
    }
}

impl RailOwner for QueryBuilderPanel {
    fn rail_nav(&mut self) -> &mut dbflux_components::composites::RailNav<Self> {
        &mut self.rail
    }

    fn rail_rows(&self, cx: &App) -> Vec<RailRow<Self>> {
        let mut rows = Vec::new();

        match self.builder_mode() {
            BuilderMode::Select => {
                if !self.is_grouped() {
                    self.column_rows(&mut rows);
                }
                self.filter_rows(FilterTarget::Where, &mut rows);
                if self.shows_joins_section(cx) {
                    self.join_rows_for_rail(&mut rows);
                }
                if self.shows_group_by_section(cx) {
                    self.group_by_rows_for_rail(&mut rows);
                }
                if self.is_grouped() && self.shows_having_section(cx) {
                    self.filter_rows(FilterTarget::Having, &mut rows);
                }
                self.sort_rows_for_rail(cx, &mut rows);
            }
            BuilderMode::Update => {
                self.assignment_rows(&mut rows);
                self.filter_rows(FilterTarget::Where, &mut rows);
                self.execution_rows(&mut rows);
            }
            BuilderMode::Delete => {
                self.filter_rows(FilterTarget::Where, &mut rows);
                self.execution_rows(&mut rows);
            }
        }

        rows
    }

    fn rail_actions(&self, cx: &App) -> Vec<RailMenuEntry<Self>> {
        let shortcut = |command| builder_shortcut(command);
        let is_mutation = self.builder_mode().is_mutation();

        let mut entries = vec![
            RailMenuEntry::new(
                "run",
                dbflux_i18n::t!("document.query_builder.status.run"),
                RailTarget::run(|this: &mut Self, _, cx| this.request_run(cx)),
            )
            .shortcut(shortcut(Command::RunQuery))
            .enabled(self.can_run()),
        ];

        if !is_mutation {
            entries.push(RailMenuEntry::new(
                "open-in-editor",
                dbflux_i18n::t!("document.query_builder.status.open_in_editor"),
                RailTarget::run(|_this: &mut Self, _, cx| {
                    cx.emit(BuilderEvent::OpenInEditorRequested)
                }),
            ));
        }

        entries.extend([
            RailMenuEntry::new(
                "save",
                dbflux_i18n::t!("document.query_builder.chrome.save"),
                RailTarget::run(|this: &mut Self, _, cx| this.request_save(cx)),
            )
            .shortcut(shortcut(Command::SaveQuery)),
            RailMenuEntry::new(
                "reset",
                dbflux_i18n::t!("document.query_builder.chrome.reset"),
                RailTarget::run(|_this: &mut Self, _, cx| cx.emit(BuilderEvent::ResetRequested)),
            ),
        ]);

        if self.shows_mutation_selector(cx) {
            let current = self.builder_mode();
            for mode in MODES {
                entries.push(
                    RailMenuEntry::new(
                        format!("mode-{}", mode as usize),
                        crate::labels::builder_mode_label(mode),
                        RailTarget::run(move |this: &mut Self, _, cx| {
                            this.switch_builder_mode(mode, cx);
                            this.rail.reset();
                        }),
                    )
                    .enabled(mode != current),
                );
            }
        }

        entries.push(RailMenuEntry::new(
            "close",
            dbflux_i18n::t!("document.query_builder.chrome.close"),
            RailTarget::run(|_this: &mut Self, _, cx| cx.emit(BuilderEvent::CloseRequested)),
        ));

        entries
    }

    fn rail_focus_handle(&self, cx: &App) -> FocusHandle {
        self.focus_handle
            .clone()
            .unwrap_or_else(|| cx.focus_handle())
    }

    fn rail_shortcut(&self, command: Command) -> Option<SharedString> {
        builder_shortcut(command)
    }
}

/// The key the rail's context binds to `command`, as the menu shows it.
fn builder_shortcut(command: Command) -> Option<SharedString> {
    effective_keymap()
        .chord_for_command(ContextId::QueryBuilder, command)
        .map(|chord| chord_display_parts(chord).join(" ").into())
}
