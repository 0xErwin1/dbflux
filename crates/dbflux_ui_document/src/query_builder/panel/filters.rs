use super::*;

impl QueryBuilderPanel {
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
}
