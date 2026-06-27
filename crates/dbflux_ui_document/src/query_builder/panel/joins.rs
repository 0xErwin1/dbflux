use super::*;

impl QueryBuilderPanel {
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

    pub(crate) fn next_comparator(current: Comparator) -> Comparator {
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
}
