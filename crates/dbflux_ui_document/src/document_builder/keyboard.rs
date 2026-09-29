//! Keyboard access to the document builder rail.
//!
//! The rail lists its rows for [`rail_command`] in the order it draws them:
//! the query name, the saved queries while their list is open, the sync
//! conflict, then the cards of the current mode (the filter groups and
//! conditions, the projection, the sort keys with limit and skip, and the
//! group stage). A field button opens the field picker with its search
//! focused, where typing a path and Enter picks it; an operator opens the
//! operator list, which J / K and Enter drive. Find or Run pipeline, Open in
//! editor, Save, the saved queries, the modes and Close are rail-wide entries
//! of the action menu; Ctrl+Enter runs, Ctrl+S saves and Alt+H / Alt+L
//! switch between Find and Aggregate.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{
    RailMenuEntry, RailNav, RailOutcome, RailOwner, RailRow, RailTarget, rail_command,
};
use dbflux_core::{
    DocumentCombinator, DocumentProjectionMode, DocumentQueryMode, DocumentSortDirection,
};
use dbflux_ui_base::keymap::{chord_display_parts, effective_keymap};
use gpui::{App, Context, FocusHandle, Focusable as _, SharedString, Window};

use super::model::{AccumulatorOp, GroupDraft, NodeDraft, NodeId, Operand, ProblemKind};
use super::panel::{DocumentBuilderPanel, PickTarget};
use super::values::{ScalarKind, ValueEditor, ValueProblem};

/// Row ids of the rail, shared by the rows and the view that draws them.
pub(super) mod row_id {
    use super::NodeId;

    pub fn name() -> String {
        "name".to_string()
    }

    pub fn saved(id: &str) -> String {
        format!("saved-{id}")
    }

    pub fn conflict() -> String {
        "conflict".to_string()
    }

    pub fn match_summary() -> String {
        "match".to_string()
    }

    pub fn group(id: NodeId) -> String {
        format!("group-{id}")
    }

    pub fn condition(id: NodeId) -> String {
        format!("condition-{id}")
    }

    pub fn chip(id: NodeId, index: usize) -> String {
        format!("chip-{id}-{index}")
    }

    pub fn projection_mode() -> String {
        "projection-mode".to_string()
    }

    pub fn projection_field(index: usize) -> String {
        format!("projection-{index}")
    }

    pub fn projection_add() -> String {
        "projection-add".to_string()
    }

    pub fn sort(index: usize) -> String {
        format!("sort-{index}")
    }

    /// Adding a sort key, limit and skip.
    pub fn paging() -> String {
        "paging".to_string()
    }

    pub fn group_stage_add() -> String {
        "group-stage-add".to_string()
    }

    pub fn group_key(index: usize) -> String {
        format!("group-key-{index}")
    }

    pub fn group_key_add() -> String {
        "group-key-add".to_string()
    }

    pub fn accumulator(id: NodeId) -> String {
        format!("accumulator-{id}")
    }

    /// Adding an accumulator and removing the stage.
    pub fn group_stage_actions() -> String {
        "group-stage-actions".to_string()
    }
}

fn pick(target: PickTarget) -> RailTarget<DocumentBuilderPanel> {
    RailTarget::run(move |this: &mut DocumentBuilderPanel, window, cx| {
        this.open_picker(target, window, cx)
    })
}

fn flipped(combinator: DocumentCombinator) -> DocumentCombinator {
    match combinator {
        DocumentCombinator::And => DocumentCombinator::Or,
        DocumentCombinator::Or => DocumentCombinator::And,
    }
}

impl DocumentBuilderPanel {
    /// Answers a key while the rail holds the keyboard.
    pub fn keyboard_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> RailOutcome {
        if self.operator_menu.is_some() {
            match command {
                Command::SelectNext => self.move_operator_highlight(1, cx),
                Command::SelectPrev => self.move_operator_highlight(-1, cx),
                Command::Execute => self.choose_highlighted_operator(window, cx),
                Command::Cancel => self.close_operator_menu(window, cx),
                _ => {}
            }
            return RailOutcome::Handled;
        }

        if command == Command::Cancel && !self.rail.menu_is_open() {
            if self.picker.is_some() {
                self.close_picker(cx);
                self.focus_handle(cx).focus(window, cx);
                return RailOutcome::Handled;
            }
            if self.saved_menu_open && self.focus_handle(cx).is_focused(window) {
                self.toggle_saved_menu(cx);
                return RailOutcome::Handled;
            }
        }

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
                    let next = match self.mode() {
                        DocumentQueryMode::Find => DocumentQueryMode::Aggregate,
                        DocumentQueryMode::Aggregate => DocumentQueryMode::Find,
                    };
                    self.set_mode(next, cx);
                    self.rail.reset();
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

    /// Id of the row the keyboard cursor is on, once a key moved it.
    #[cfg(test)]
    pub fn rail_cursor_for_test(&self) -> Option<String> {
        self.rail.cursor_row().map(|row| row.to_string())
    }

    /// Ids of the rail's rows, in order.
    #[cfg(test)]
    pub fn rail_rows_for_test(&self, cx: &App) -> Vec<String> {
        self.rail_rows(cx)
            .into_iter()
            .map(|row| row.id.to_string())
            .collect()
    }

    fn group_rows(&self, group: &GroupDraft, is_root: bool, rows: &mut Vec<RailRow<Self>>) {
        let id = group.id;
        let next = flipped(group.combinator);
        let toggle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
            this.set_combinator(id, next, cx)
        };
        let add_condition =
            move || RailTarget::run(move |this: &mut Self, _, cx| this.add_condition(id, cx));
        let add_group =
            move || RailTarget::run(move |this: &mut Self, _, cx| this.add_group(id, cx));

        let mut row = RailRow::new(row_id::group(id))
            .field("combinator", RailTarget::run(toggle))
            .field("add-condition", add_condition())
            .field("add-group", add_group())
            .on_toggle(toggle)
            .on_add(add_condition())
            .on_add_group(add_group());
        if !is_root {
            row = row.on_remove(move |this: &mut Self, _, cx| this.remove_node(id, cx));
        }
        rows.push(row);

        for child in &group.children {
            match child {
                NodeDraft::Condition(condition) => {
                    let condition_id = condition.id;
                    let mut row = RailRow::new(row_id::condition(condition_id))
                        .field("field", pick(PickTarget::Condition(condition_id)))
                        .field(
                            "operator",
                            RailTarget::run(move |this: &mut Self, window, cx| {
                                this.toggle_operator_menu(condition_id, window, cx)
                            }),
                        );

                    let input = self
                        .value_inputs
                        .get(&condition_id)
                        .map(|input| input.state.clone());
                    match (condition.editor(), &condition.operand) {
                        (ValueEditor::Toggle, Operand::Toggle(flag)) => {
                            let flag = !*flag;
                            let flip =
                                move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                                    this.set_toggle(condition_id, flag, cx)
                                };
                            row = row.field("value", RailTarget::run(flip)).on_toggle(flip);
                        }
                        (ValueEditor::Nested, _) => {}
                        _ => {
                            if let Some(state) = input {
                                row = row.field("value", RailTarget::text(state));
                            }
                        }
                    }

                    if self.looks_like_object_id(condition_id) {
                        row = row.field(
                            "use-object-id",
                            RailTarget::run(move |this: &mut Self, _, cx| {
                                this.set_kind(condition_id, ScalarKind::ObjectId, cx)
                            }),
                        );
                    }

                    rows.push(
                        row.on_remove(move |this: &mut Self, _, cx| {
                            this.remove_node(condition_id, cx)
                        })
                        .on_add(add_condition())
                        .on_add_group(add_group()),
                    );

                    match &condition.operand {
                        Operand::Chips(items) => {
                            for index in 0..items.len() {
                                let remove = move |this: &mut Self,
                                                   _: &mut Window,
                                                   cx: &mut Context<Self>| {
                                    this.remove_chip(condition_id, index, cx)
                                };
                                rows.push(
                                    RailRow::new(row_id::chip(condition_id, index))
                                        .field("remove", RailTarget::run(remove))
                                        .on_remove(remove),
                                );
                            }
                        }
                        Operand::Nested(nested) => self.group_rows(nested, false, rows),
                        Operand::Text { .. } | Operand::Toggle(_) => {}
                    }
                }
                NodeDraft::Group(nested) => self.group_rows(nested, false, rows),
            }
        }
    }

    /// Whether the value of condition `id` reads like an ObjectId typed as
    /// text, which the row offers to switch.
    pub(super) fn looks_like_object_id(&self, id: NodeId) -> bool {
        self.problems.iter().any(|problem| {
            problem.node == id
                && problem.kind == ProblemKind::Value(ValueProblem::LooksLikeObjectId)
        }) || self.chip_problems.get(&id).copied() == Some(ValueProblem::LooksLikeObjectId)
    }

    fn projection_rows(&self, rows: &mut Vec<RailRow<Self>>) {
        let next = match self.draft.projection.mode {
            DocumentProjectionMode::Include => DocumentProjectionMode::Exclude,
            DocumentProjectionMode::Exclude => DocumentProjectionMode::Include,
        };
        let toggle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
            this.set_projection_mode(next, cx)
        };
        rows.push(
            RailRow::new(row_id::projection_mode())
                .field("mode", RailTarget::run(toggle))
                .on_toggle(toggle),
        );

        for index in 0..self.draft.projection.fields.len() {
            let remove = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.remove_projection_field(index, cx)
            };
            rows.push(
                RailRow::new(row_id::projection_field(index))
                    .field("remove", RailTarget::run(remove))
                    .on_remove(remove)
                    .on_add(pick(PickTarget::Projection)),
            );
        }

        rows.push(
            RailRow::new(row_id::projection_add())
                .field("add", pick(PickTarget::Projection))
                .on_add(pick(PickTarget::Projection)),
        );
    }

    fn sort_rows(&self, rows: &mut Vec<RailRow<Self>>) {
        let count = self.draft.sort.len();

        for (index, key) in self.draft.sort.iter().enumerate() {
            let next = match key.direction {
                DocumentSortDirection::Ascending => DocumentSortDirection::Descending,
                DocumentSortDirection::Descending => DocumentSortDirection::Ascending,
            };
            let flip = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.set_sort_direction(index, next, cx)
            };
            rows.push(
                RailRow::new(row_id::sort(index))
                    .field("direction", RailTarget::run(flip))
                    .on_toggle(flip)
                    .on_remove(move |this: &mut Self, _, cx| this.remove_sort_key(index, cx))
                    .on_add(pick(PickTarget::Sort))
                    .on_move(move |this: &mut Self, delta, _, cx| {
                        let target = index as isize + delta;
                        if (0..count as isize).contains(&target) {
                            this.move_sort_key(index, target as usize, cx);
                        }
                    }),
            );
        }

        rows.push(
            RailRow::new(row_id::paging())
                .field("add", pick(PickTarget::Sort))
                .field("limit", RailTarget::text(self.limit_input.clone()))
                .field("skip", RailTarget::text(self.skip_input.clone()))
                .on_add(pick(PickTarget::Sort)),
        );
    }

    fn group_stage_rows(&self, rows: &mut Vec<RailRow<Self>>) {
        let stage = self
            .draft
            .group
            .as_ref()
            .filter(|_| self.mode() == DocumentQueryMode::Aggregate);

        let Some(stage) = stage else {
            if self.aggregate_available() {
                let add = || RailTarget::run(|this: &mut Self, _, cx| this.add_group_stage(cx));
                rows.push(
                    RailRow::new(row_id::group_stage_add())
                        .field("add", add())
                        .on_add(add()),
                );
            }
            return;
        };

        for index in 0..stage.keys.len() {
            let remove = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.remove_group_key(index, cx)
            };
            rows.push(
                RailRow::new(row_id::group_key(index))
                    .field("remove", RailTarget::run(remove))
                    .on_remove(remove)
                    .on_add(pick(PickTarget::GroupKey)),
            );
        }
        rows.push(
            RailRow::new(row_id::group_key_add())
                .field("add", pick(PickTarget::GroupKey))
                .on_add(pick(PickTarget::GroupKey)),
        );

        let add_accumulator = || RailTarget::run(|this: &mut Self, _, cx| this.add_accumulator(cx));

        for accumulator in &stage.accumulators {
            let id = accumulator.id;
            let position = AccumulatorOp::ALL
                .iter()
                .position(|op| *op == accumulator.op)
                .unwrap_or(0);
            let next = AccumulatorOp::ALL[(position + 1) % AccumulatorOp::ALL.len()];
            let cycle = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.set_accumulator_op(id, next, cx)
            };

            let mut row = RailRow::new(row_id::accumulator(id));
            if let Some(input) = self.accumulator_inputs.get(&id) {
                row = row.field("name", RailTarget::text(input.state.clone()));
            }
            row = row.field("op", RailTarget::run(cycle)).on_toggle(cycle);
            if accumulator.op.takes_field() {
                row = row.field("field", pick(PickTarget::Accumulator(id)));
            }
            rows.push(
                row.on_remove(move |this: &mut Self, _, cx| this.remove_accumulator(id, cx))
                    .on_add(add_accumulator()),
            );
        }

        rows.push(
            RailRow::new(row_id::group_stage_actions())
                .field("add-accumulator", add_accumulator())
                .field(
                    "remove-stage",
                    RailTarget::run(|this: &mut Self, _, cx| this.remove_group_stage(cx)),
                )
                .on_add(add_accumulator()),
        );
    }
}

impl RailOwner for DocumentBuilderPanel {
    fn rail_nav(&mut self) -> &mut RailNav<Self> {
        &mut self.rail
    }

    fn rail_rows(&self, _cx: &App) -> Vec<RailRow<Self>> {
        let mut rows = vec![
            RailRow::new(row_id::name()).field("name", RailTarget::text(self.name_input.clone())),
        ];

        if self.saved_menu_open {
            for entry in &self.saved_queries {
                let open_id = entry.id.clone();
                let delete_id = entry.id.clone();
                let delete = move |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                    this.request_delete_saved(&delete_id, cx)
                };
                rows.push(
                    RailRow::new(row_id::saved(&entry.id))
                        .field(
                            "open",
                            RailTarget::run(move |this: &mut Self, _, cx| {
                                this.request_open_saved(&open_id, cx)
                            }),
                        )
                        .field("delete", RailTarget::run(delete.clone()))
                        .on_remove(delete),
                );
            }
        }

        let aggregate = self.mode() == DocumentQueryMode::Aggregate;

        if !aggregate && self.is_conflicted() {
            let mut row = RailRow::new(row_id::conflict());
            if !self.sync.held().is_empty() {
                row = row.field(
                    "keep",
                    RailTarget::run(|this: &mut Self, _, cx| this.keep_text(cx)),
                );
            }
            if self.problems.is_empty() && self.render_error.is_none() {
                row = row.field(
                    "rewrite",
                    RailTarget::run(|this: &mut Self, _, cx| this.rewrite_from_builder(cx)),
                );
            }
            rows.push(row);
        }

        if aggregate {
            let toggle = |this: &mut Self, _: &mut Window, cx: &mut Context<Self>| {
                this.toggle_filter_expanded(cx)
            };
            rows.push(
                RailRow::new(row_id::match_summary())
                    .field("edit", RailTarget::run(toggle))
                    .on_toggle(toggle),
            );
            if self.filter_expanded {
                self.group_rows(&self.draft.filter, true, &mut rows);
            }
            self.group_stage_rows(&mut rows);
            self.sort_rows(&mut rows);
        } else {
            self.group_rows(&self.draft.filter, true, &mut rows);
            self.projection_rows(&mut rows);
            self.sort_rows(&mut rows);
            self.group_stage_rows(&mut rows);
        }

        rows
    }

    fn rail_actions(&self, cx: &App) -> Vec<RailMenuEntry<Self>> {
        let aggregate = self.mode() == DocumentQueryMode::Aggregate;
        let run_label = if aggregate {
            dbflux_i18n::t!("document.collection.builder.run_pipeline")
        } else {
            dbflux_i18n::t!("document.collection.builder.find")
        };

        vec![
            RailMenuEntry::new(
                "run",
                run_label,
                RailTarget::run(|this: &mut Self, _, cx| this.request_run(cx)),
            )
            .shortcut(builder_shortcut(Command::RunQuery))
            .enabled(self.can_run()),
            RailMenuEntry::new(
                "open-in-editor",
                dbflux_i18n::t!("document.collection.builder.open_in_editor"),
                RailTarget::run(|this: &mut Self, _, cx| this.request_open_in_editor(cx)),
            )
            .enabled(self.problems.is_empty() && self.render_error.is_none()),
            RailMenuEntry::new(
                "save",
                dbflux_i18n::t!("document.collection.builder.saved.save"),
                RailTarget::run(|this: &mut Self, _, cx| this.request_save(cx)),
            )
            .shortcut(builder_shortcut(Command::SaveQuery))
            .enabled(self.can_save(cx)),
            RailMenuEntry::new(
                "saved",
                dbflux_i18n::t!("document.collection.builder.saved.list"),
                RailTarget::run(|this: &mut Self, _, cx| this.toggle_saved_menu(cx)),
            ),
            RailMenuEntry::new(
                "mode-find",
                dbflux_i18n::t!("document.collection.builder.mode.find"),
                RailTarget::run(|this: &mut Self, _, cx| {
                    this.set_mode(DocumentQueryMode::Find, cx);
                    this.rail.reset();
                }),
            )
            .enabled(aggregate),
            RailMenuEntry::new(
                "mode-aggregate",
                dbflux_i18n::t!("document.collection.builder.mode.aggregate"),
                RailTarget::run(|this: &mut Self, _, cx| {
                    this.set_mode(DocumentQueryMode::Aggregate, cx);
                    this.rail.reset();
                }),
            )
            .enabled(!aggregate && self.aggregate_available()),
            RailMenuEntry::new(
                "close",
                dbflux_i18n::t!("document.collection.builder.close"),
                RailTarget::run(|this: &mut Self, _, cx| this.request_close(cx)),
            ),
        ]
    }

    fn rail_focus_handle(&self, cx: &App) -> FocusHandle {
        self.focus_handle(cx)
    }

    fn rail_shortcut(&self, command: Command) -> Option<SharedString> {
        builder_shortcut(command)
    }
}

/// The key the rail's context binds to `command`, as the menu shows it.
fn builder_shortcut(command: Command) -> Option<SharedString> {
    effective_keymap()
        .chord_for_command(ContextId::DocumentBuilder, command)
        .map(|chord| chord_display_parts(chord).join(" ").into())
}
