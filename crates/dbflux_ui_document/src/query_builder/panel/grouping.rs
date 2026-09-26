use super::*;

impl QueryBuilderPanel {
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
                let mut state = crate::completion_support::new_single_line_completion_state(
                    window,
                    cx,
                    "alias.column",
                );
                state.set_value(&col_text, window, cx);
                state
            });

            col_input.update(cx, |s, _| {
                s.lsp_mut().completion_provider = Some(alias_provider.clone());
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
                let mut state = crate::completion_support::new_single_line_completion_state(
                    window,
                    cx,
                    "alias.column",
                );
                state.set_value(&col_text, window, cx);
                state
            });

            col_input.update(cx, |s, _| {
                s.lsp_mut().completion_provider = Some(alias_provider.clone());
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
                let mut state = InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                    "document.query_builder.group_by.alias_placeholder"
                ));
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

    /// Appends a sort row.
    ///
    /// When the spec is grouped, the column must be either a group-by column or
    /// an aggregate alias. If the column is not in the valid set, the entry is
    /// rejected and `sort_validation_error` is set for the view to display.
    pub fn add_sort(&mut self, source_alias: &str, column: &str, cx: &mut Context<Self>) {
        if !self.accepts_sort_column(column) {
            cx.notify();
            return;
        }

        self.sort_rows.push(SortRow {
            source_alias: source_alias.to_string(),
            column: column.to_string(),
            direction: VisualSortDirection::Asc,
        });
        self.rebuild_spec_and_notify(cx);
    }

    /// Points the sort row at `index` to another column, keeping its direction.
    ///
    /// The column is checked the way [`Self::add_sort`] checks it.
    pub fn set_sort_column(
        &mut self,
        index: usize,
        option: SortColumnOption,
        cx: &mut Context<Self>,
    ) {
        if index >= self.sort_rows.len() || !self.accepts_sort_column(&option.column) {
            cx.notify();
            return;
        }

        if let Some(row) = self.sort_rows.get_mut(index) {
            row.source_alias = option.source_alias;
            row.column = option.column;
        }
        self.rebuild_spec_and_notify(cx);
    }

    /// Whether `column` may be sorted on: any column of an ungrouped query, a
    /// group-by column or an aggregate alias of a grouped one. A rejected
    /// column sets `sort_validation_error`; an accepted one clears it.
    fn accepts_sort_column(&mut self, column: &str) -> bool {
        if self.current_spec.is_grouped() {
            let valid: HashSet<String> = self
                .group_by_rows
                .iter()
                .map(|g| g.column.clone())
                .chain(self.aggregate_rows.iter().map(|a| a.alias.clone()))
                .collect();

            if !valid.contains(column) {
                self.sort_validation_error = Some(dbflux_i18n::t!(
                    "document.query_builder.sort.invalid_column",
                    column = column
                ));
                return false;
            }
        }

        self.sort_validation_error = None;
        true
    }

    /// The columns a sort can use: the group-by columns and aggregate aliases
    /// of a grouped query, otherwise the source table's columns followed by
    /// the columns of each joined table whose metadata has loaded.
    pub(crate) fn sort_column_options(&self) -> Vec<SortColumnOption> {
        if self.current_spec.is_grouped() {
            let group_columns = self.group_by_rows.iter().map(|row| SortColumnOption {
                source_alias: row.source_alias.clone(),
                column: row.column.clone(),
            });

            let aggregate_aliases = self
                .aggregate_rows
                .iter()
                .filter(|row| !row.alias.is_empty())
                .map(|row| SortColumnOption {
                    source_alias: String::new(),
                    column: row.alias.clone(),
                });

            return group_columns.chain(aggregate_aliases).collect();
        }

        let source_alias = self.current_spec.source.alias.clone();
        let mut options: Vec<SortColumnOption> = self
            .available_columns
            .iter()
            .map(|column| SortColumnOption {
                source_alias: source_alias.clone(),
                column: column.clone(),
            })
            .collect();

        let cache = self.schema_cache.borrow();
        for join in self.join_rows.iter().filter(|row| !row.to_table.is_empty()) {
            use crate::completion_support::normalize_identifier;

            let key = (
                join.to_schema.as_deref().map(normalize_identifier),
                normalize_identifier(&join.to_table),
            );

            if let Some(columns) = cache.joined_columns.get(&key) {
                options.extend(columns.iter().map(|column| SortColumnOption {
                    source_alias: join.to_alias.clone(),
                    column: column.name.clone(),
                }));
            }
        }

        options
    }

    /// The dropdown label of a sort column: bare for the source table and
    /// aggregate aliases, `alias.column` for a joined table.
    pub(crate) fn sort_option_label(&self, option: &SortColumnOption) -> String {
        if option.source_alias.is_empty() || option.source_alias == self.current_spec.source.alias {
            option.column.clone()
        } else {
            format!("{}.{}", option.source_alias, option.column)
        }
    }

    /// Brings the sort dropdowns in line with `sort_rows` and the columns
    /// on offer: one column dropdown per row, each showing its row's column,
    /// and the dropdown that appends a row.
    ///
    /// Called from render. Dropdowns are rebuilt when the row count changes
    /// and refreshed in place when only the offered columns change.
    pub(crate) fn sync_sort_dropdowns(&mut self, cx: &mut Context<Self>) {
        let options = self.sort_column_options();
        let items: Vec<DropdownItem> = options
            .iter()
            .map(|option| DropdownItem::new(self.sort_option_label(option)))
            .collect();
        let options_changed = options != self.sort_dropdown_options;

        if self.sort_column_dropdowns.len() != self.sort_rows.len()
            || self.sort_add_dropdown.is_none()
        {
            self.sort_column_dropdowns.clear();
            self._sort_dropdown_subs.clear();

            for index in 0..self.sort_rows.len() {
                let dropdown = cx.new(|_cx| {
                    Dropdown::new(("qb-sort-column", index))
                        .items(items.clone())
                        .chevron_trigger(dbflux_components::controls::ButtonVariant::Secondary)
                });

                let subscription = cx.subscribe(
                    &dropdown,
                    move |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                        if let Some(option) = this.sort_dropdown_options.get(event.index).cloned() {
                            this.set_sort_column(index, option, cx);
                        }
                    },
                );

                self.sort_column_dropdowns.push(dropdown);
                self._sort_dropdown_subs.push(subscription);
            }

            let add_dropdown = cx.new(|_cx| {
                Dropdown::new("qb-sort-add")
                    .items(items.clone())
                    .placeholder(dbflux_i18n::t!("document.query_builder.sort.add"))
                    .chevron_trigger(dbflux_components::controls::ButtonVariant::Secondary)
            });

            let subscription = cx.subscribe(
                &add_dropdown,
                |this, dropdown, event: &DropdownSelectionChanged, cx| {
                    if let Some(option) = this.sort_dropdown_options.get(event.index).cloned() {
                        this.add_sort(&option.source_alias, &option.column, cx);
                    }
                    dropdown.update(cx, |dropdown, cx| dropdown.set_selected_index(None, cx));
                },
            );

            self.sort_add_dropdown = Some(add_dropdown);
            self._sort_dropdown_subs.push(subscription);
        } else if options_changed {
            for dropdown in self
                .sort_column_dropdowns
                .iter()
                .chain(self.sort_add_dropdown.iter())
            {
                dropdown.update(cx, |dropdown, cx| dropdown.set_items(items.clone(), cx));
            }
        }

        self.sort_dropdown_options = options;

        for (row, dropdown) in self.sort_rows.iter().zip(&self.sort_column_dropdowns) {
            let selected = self.sort_dropdown_options.iter().position(|option| {
                option.column == row.column
                    && (option.source_alias == row.source_alias || option.source_alias.is_empty())
            });

            let expected_label = selected
                .and_then(|index| items.get(index))
                .map(|item| item.label.clone());

            if dropdown.read(cx).selected_label() != expected_label {
                dropdown.update(cx, |dropdown, cx| dropdown.set_selected_index(selected, cx));
            }
        }
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

    /// Sets the single sort-key direction for sort-key-only drivers.
    ///
    /// Drivers whose `order_by_mode` is `SortKeyOnly` (e.g. DynamoDB) can order
    /// only on the sort key by direction, never by arbitrary columns. This keeps
    /// exactly one sort row carrying the chosen direction so the builder never
    /// offers a multi-column ORDER BY the driver cannot execute.
    pub fn set_sort_key_direction(
        &mut self,
        direction: VisualSortDirection,
        cx: &mut Context<Self>,
    ) {
        let sort_key_column = self.sort_key_column().unwrap_or_default();
        self.apply_sort_key_direction(direction, sort_key_column);
        self.rebuild_spec_and_notify(cx);
    }

    /// Updates the single sort-key row to the given direction, seeding the
    /// orderable column name when the row carries none yet. Pure (no `cx`):
    /// the resolved sort-key column is passed in so the seeding rule can be
    /// unit-tested without a GPUI context.
    pub(crate) fn apply_sort_key_direction(
        &mut self,
        direction: VisualSortDirection,
        sort_key_column: String,
    ) {
        self.sort_validation_error = None;

        let alias = self.current_spec.source.alias.clone();

        match self.sort_rows.first_mut() {
            Some(row) => {
                row.direction = direction;
                if row.column.is_empty() {
                    row.column = sort_key_column;
                    row.source_alias = alias;
                }
                self.sort_rows.truncate(1);
            }
            None => {
                self.sort_rows.push(SortRow {
                    source_alias: alias,
                    column: sort_key_column,
                    direction,
                });
            }
        }
    }

    /// The currently selected sort-key direction (defaults to ascending).
    pub(crate) fn sort_key_direction(&self) -> VisualSortDirection {
        self.sort_rows
            .first()
            .map(|row| row.direction)
            .unwrap_or(VisualSortDirection::Asc)
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
}
