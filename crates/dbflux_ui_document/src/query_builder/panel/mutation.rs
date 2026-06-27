use super::*;

impl QueryBuilderPanel {
    /// Switches the panel to the given builder mode.
    ///
    /// `Select` drops `mutation_state`. `Update` / `Delete` create a fresh
    /// `MutationBuilderState` if not already in that mode. The filter and
    /// source are preserved; mode-specific state resets (DR-1.6).
    pub fn switch_builder_mode(
        &mut self,
        mode: crate::query_builder::mutation_state::BuilderMode,
        cx: &mut Context<Self>,
    ) {
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
        cx.notify();
    }

    /// Recomputes `sql_preview` from current state without needing a GPUI context.
    ///
    /// In SELECT mode, regenerates from `current_spec` via `generate_preview`.
    /// In UPDATE/DELETE mode, builds the mutation spec and calls
    /// `generate_mutation_preview`; falls back to a placeholder when no valid
    /// spec can be produced (e.g. UPDATE with no table configured yet).
    pub(crate) fn refresh_mutation_preview_pure(&mut self) {
        use crate::query_builder::mutation_state::BuilderMode;

        let in_mutation_mode = self
            .mutation_state
            .as_ref()
            .map(|s| s.mode.is_mutation())
            .unwrap_or(false);

        if in_mutation_mode {
            if let Some((spec, _opts)) = self.build_mutation_spec_and_opts() {
                self.sql_preview = (self.generate_mutation_preview)(&spec);
            } else {
                let kind_label = self
                    .mutation_state
                    .as_ref()
                    .map(|s| match s.mode {
                        BuilderMode::Update => "UPDATE",
                        BuilderMode::Delete => "DELETE",
                        BuilderMode::Select => "SELECT",
                    })
                    .unwrap_or("UPDATE");
                self.sql_preview =
                    format!("-- {kind_label}: configure assignments / filter to preview SQL");
            }
        } else {
            let spec = self.current_spec.clone();
            self.sql_preview = (self.generate_preview)(&spec);
        }

        self.pending_preview_sync = true;
    }

    /// Writes `text` into `mutation_state.assignments[row_ix].assignment.column`
    /// and refreshes the mutation preview. Called by the column input subscription
    /// in `rebuild_assign_inputs`.
    pub fn set_assignment_column(&mut self, row_ix: usize, text: String, cx: &mut Context<Self>) {
        if let Some(state) = self.mutation_state.as_mut()
            && row_ix < state.assignments.len()
        {
            state.assignments[row_ix].assignment.column = text;
        }

        self.refresh_mutation_preview_pure();
        cx.notify();
    }

    /// Writes `text` into `mutation_state.assignments[row_ix].raw_text` and
    /// re-derives the `AssignmentValue` for the `Literal` and `Expression`
    /// variants. `Null` and `Default` are left untouched because their value
    /// inputs are hidden and no text can be entered for them.
    pub fn set_assignment_raw_text(&mut self, row_ix: usize, text: String, cx: &mut Context<Self>) {
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
        cx.notify();
    }

    /// Rebuilds `assign_col_inputs` and `assign_val_inputs` to match the
    /// current assignment count.
    ///
    /// Called from the render cycle when `pending_assign_rebuild` is `true`.
    pub fn rebuild_assign_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self._assign_input_subs.clear();
        self.assign_col_inputs.clear();
        self.assign_val_inputs.clear();

        let count = self
            .mutation_state
            .as_ref()
            .map(|s| s.assignments.len())
            .unwrap_or(0);

        for i in 0..count {
            let col_placeholder = "column";
            let val_placeholder = "value";

            let col_name = self
                .mutation_state
                .as_ref()
                .and_then(|s| s.assignments.get(i))
                .map(|r| r.assignment.column.clone())
                .unwrap_or_default();

            let raw_text = self
                .mutation_state
                .as_ref()
                .and_then(|s| s.assignments.get(i))
                .map(|r| r.raw_text.clone())
                .unwrap_or_default();

            let col_state = cx.new(|cx| {
                let mut s = InputState::new(window, cx).placeholder(col_placeholder);
                s.set_value(&col_name, window, cx);
                s
            });
            let val_state = cx.new(|cx| {
                let mut s = InputState::new(window, cx).placeholder(val_placeholder);
                s.set_value(&raw_text, window, cx);
                s
            });

            let col_sub = cx.subscribe_in(
                &col_state,
                window,
                move |this, entity, event: &InputEvent, _window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        this.set_assignment_column(i, text, cx);
                    }
                },
            );

            let val_sub = cx.subscribe_in(
                &val_state,
                window,
                move |this, entity, event: &InputEvent, _window, cx| {
                    if matches!(event, InputEvent::Change) {
                        let text = entity.read(cx).value().to_string();
                        this.set_assignment_raw_text(i, text, cx);
                    }
                },
            );

            self.assign_col_inputs.insert(i, col_state);
            self.assign_val_inputs.insert(i, val_state);
            self._assign_input_subs.push(col_sub);
            self._assign_input_subs.push(val_sub);
        }
    }

    /// Builds the `VisualMutationSpec` and `MutationExecOptions` from the
    /// current mutation state and spec.
    ///
    /// Returns `None` if the mode is `Select` or if the spec cannot be built.
    pub fn build_mutation_spec_and_opts(
        &self,
    ) -> Option<(
        dbflux_core::VisualMutationSpec,
        crate::data_grid_panel::mutation_executor::MutationExecOptions,
    )> {
        use crate::query_builder::mutation_state::BuilderMode;
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        let state = self.mutation_state.as_ref()?;

        let from = TableRef {
            schema: self.current_spec.source.schema.clone(),
            name: self.current_spec.source.table.clone(),
        };

        let kind = match state.mode {
            BuilderMode::Select => return None,
            BuilderMode::Delete => MutationKind::Delete,
            BuilderMode::Update => {
                let assignments: Vec<dbflux_core::Assignment> = state
                    .assignments
                    .iter()
                    .filter(|r| !r.assignment.column.is_empty())
                    .map(|r| self.typed_assignment(r))
                    .collect();
                MutationKind::Update { assignments }
            }
        };

        let spec = VisualMutationSpec {
            from,
            filter: self.current_spec.filter.clone(),
            kind,
        };

        Some((spec, state.exec_options.clone()))
    }

    /// Clones an assignment row into its spec `Assignment`, promoting a free-text
    /// literal to the target column's typed `ScalarLiteral` when the column kind
    /// is known. Non-literal values (Expression / Null / Default) and columns
    /// with unknown kind are left untouched.
    pub(crate) fn typed_assignment(
        &self,
        row: &crate::query_builder::mutation_state::AssignmentRow,
    ) -> dbflux_core::Assignment {
        use dbflux_core::{AssignmentValue, ScalarLiteral};

        let mut assignment = row.assignment.clone();

        if let AssignmentValue::Literal(ScalarLiteral::Text(_)) = &assignment.value
            && let Some(kind) = self.column_kinds.get(&assignment.column)
        {
            assignment.value =
                AssignmentValue::Literal(ScalarLiteral::from_input_for_kind(&row.raw_text, *kind));
        }

        assignment
    }

    /// Recomputes the pre-execution row count when the target rows change.
    ///
    /// No-op in SELECT mode. The count depends only on `(table, filter)`, so it
    /// is skipped when that signature is unchanged (assignment edits and the
    /// Update/Delete toggle do not retrigger it). The query runs on a debounced
    /// background task with a deadline and its result is written into
    /// `mutation_state.count_state`; filter values are inlined because drivers
    /// do not bind `QueryRequest.params`.
    ///
    /// Called from the render cycle, which fires after every state change.
    pub(crate) fn maybe_refresh_mutation_count(&mut self, cx: &mut Context<Self>) {
        use crate::data_grid_panel::mutation_executor::{CountState, count_with_deadline};
        use std::time::Duration;

        let in_mutation_mode = self
            .mutation_state
            .as_ref()
            .map(|s| s.mode.is_mutation())
            .unwrap_or(false);

        if !in_mutation_mode {
            self.count_signature = None;
            return;
        }

        let count_spec = CountSpec {
            from: dbflux_core::TableRef {
                schema: self.current_spec.source.schema.clone(),
                name: self.current_spec.source.table.clone(),
            },
            filter: self.current_spec.filter.clone(),
        };

        let signature = serde_json::to_string(&count_spec).unwrap_or_default();
        if self.count_signature.as_deref() == Some(signature.as_str()) {
            return;
        }

        let Some(app) = self.app_state_weak.upgrade() else {
            return;
        };
        let Some(connection) = app
            .read(cx)
            .connections()
            .get(&self.schema_profile_id)
            .map(|c| c.connection.clone())
        else {
            return;
        };

        self.count_signature = Some(signature);
        if let Some(state) = self.mutation_state.as_mut() {
            state.count_state = CountState::Counting;
        }

        let count_sql = build_mutation_count_sql(&count_spec, connection.dialect());

        self._count_debounce = Some(cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_millis(300))
                .await;

            let result = cx
                .background_executor()
                .spawn(async move {
                    count_with_deadline(connection, count_sql, Vec::new(), Duration::from_secs(2))
                })
                .await;

            this.update(cx, |panel, cx| {
                if let Some(state) = panel.mutation_state.as_mut() {
                    state.count_state = result;
                }
                cx.notify();
            })
            .ok();
        }));

        cx.notify();
    }

    // -----------------------------------------------------------------------
    // Grouped mode transition helpers
    // -----------------------------------------------------------------------
}
