use super::*;

impl DataGridPanel {
    /// Collects all cell values for `visual_row` from the current result.
    ///
    /// Returns an empty `Vec` when the row index is out of bounds or no
    /// `table_state` exists.
    pub(super) fn collect_row_values(&self, visual_row: usize, cx: &App) -> Vec<Value> {
        use dbflux_components::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.grid_table.table_state else {
            return Vec::new();
        };

        let ts = table_state.read(cx);
        let buffer = ts.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let base_row = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(idx)) => self.result.rows.get(idx),
            _ => None,
        };

        base_row.cloned().unwrap_or_default()
    }

    /// Update the result data (for QueryResult source or after table fetch).
    pub fn set_result(&mut self, result: QueryResult, cx: &mut Context<Self>) {
        self.nearest_neighbors = None;
        // A table keeps the view the user picked while its facets still offer
        // it (a JSON table stays in Document across a reload).
        let keeps_view = matches!(self.source, DataSource::Table { .. })
            && self.data_view_modes(cx).contains(&self.view_config.mode);
        if !keeps_view {
            let source_facets = self.facets(cx);
            self.view_config = Self::view_config_for(&self.source, &source_facets);
        }
        self.chrome.derived_json = None;
        self.chrome.derived_text = None;

        self.apply_chart_for_result(&result, cx);

        self.result = result;
        // The rows were replaced, so a client-side sort has no order left to
        // apply; drop the order and the sort state together so neither
        // outlives the rows it was built for.
        self.grid_table.local_sort_state = None;
        self.grid_table.original_row_order = None;
        self.reapply_result_search_to_new_rows();
        self.rebuild_table(None, cx);
        self.refresh.state = GridState::Ready;

        // Re-snapshot the row inspector against the fresh data so the rail
        // keeps following the same row position across refreshes. The reload
        // already emitted `SelectionChanged`, whose handler re-snapshots a rail
        // that follows the cursor — this pass covers what that emission cannot:
        // a rail still pointed at its own row while the cursor sits elsewhere,
        // and one that must be dropped because the new result is shorter.
        if !self.reload_refreshed_the_rail(cx)
            && let Some((row, col)) = self.inspector.inspector_row
        {
            self.open_row_inspector(row, col, cx);
        }

        // The panel's cached target came from the pre-refresh result, so
        // re-read it rather than leaving a value the grid no longer holds.
        // A tab that inherited an open panel but has never shown one yet has
        // no cached cell, so it starts from the current selection instead.
        if self.inspector.value_panel_open {
            match self
                .inspector
                .value_panel
                .as_ref()
                .map(|panel| panel.read(cx).target_cell())
            {
                Some((row, col)) => self.request_value_panel(row, col, cx),
                None => {
                    self.mount_value_panel_for_active_cell(cx);
                }
            }
        }

        cx.notify();
    }

    /// Update source to a new query result (used by ScriptDocument).
    /// Offers the footer actions for a result the row limit cut short. The
    /// offer is dropped with the next result, so the host sets it again for
    /// each one.
    pub(crate) fn set_limited_row_actions(
        &mut self,
        actions: LimitedRowActions,
        cx: &mut Context<Self>,
    ) {
        self.limited_rows.actions = actions;
        cx.notify();
    }

    pub(crate) fn set_limited_row_total(&mut self, total: LimitedRowTotal, cx: &mut Context<Self>) {
        self.limited_rows.total = total;
        cx.notify();
    }

    /// Identifies the current query result, for a host to check that an
    /// asynchronous answer still belongs to it.
    pub(crate) fn result_generation(&self) -> u64 {
        self.result_generation
    }

    /// The query text behind a query result, for a host re-running it.
    pub(crate) fn result_query(&self) -> Option<&str> {
        match &self.source {
            DataSource::QueryResult { original_query, .. } => Some(original_query),
            _ => None,
        }
    }

    pub fn set_query_result(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        profile_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        self.refresh.refresh_policy = RefreshPolicy::Manual;
        self.refresh._refresh_timer = None;

        self.filter_bar.refresh_dropdown.update(cx, |dd, cx| {
            dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
        });

        cx.emit(DataGridEvent::RefreshPolicyReset(RefreshPolicy::Manual));

        self.source = DataSource::QueryResult {
            result: result.clone(),
            original_query: query,
            profile_id,
        };
        self.limited_rows = LimitedRows::default();
        self.result_generation += 1;
        self.grid_table.local_sort_state = None;
        self.grid_table.original_row_order = None;
        // The new result may have a different shape, so the sort column index
        // and cursor address the previous result and are dropped.
        self.grid_table.reload = TableReload::NewColumns;
        self.set_result((*result).clone(), cx);
    }

    pub(super) fn rebuild_table(
        &mut self,
        initial_sort: Option<TableSortState>,
        cx: &mut Context<Self>,
    ) {
        // Consumed here so a reload without an explicit reason defaults to
        // keeping the cursor on the next one.
        let reload = std::mem::take(&mut self.grid_table.reload);

        // A new query result addresses other columns, so its cells cannot
        // take edits staged on the previous shape.
        let keep_edits = std::mem::take(&mut self.grid_table.keep_edits_on_reload)
            && reload != TableReload::NewColumns;

        // For collections, update pk_columns from result metadata (is_primary_key flag)
        // This allows DynamoDB and other drivers to use their actual primary keys
        // instead of hardcoded "_id"
        if self.source.is_collection() {
            let pk_columns_from_metadata: Vec<String> = self
                .result
                .columns
                .iter()
                .filter(|col| col.is_primary_key)
                .map(|col| col.name.clone())
                .collect();

            if !pk_columns_from_metadata.is_empty() {
                self.pk_columns = pk_columns_from_metadata;
            }
            // If no columns are marked as PK, keep the existing pk_columns (fallback to "_id" for MongoDB)
        }

        // Find PK column indices in result columns. When mutations are
        // disabled (grouped result or no PK), pass an empty set to the table
        // state so `is_editable` returns false.
        let document_patches = self.commits_document_patches(cx);
        let stepped_into = self.is_stepped_into();

        // Neighbor rows are read-only, but their key columns still are the
        // table's key and keep the key icon.
        let read_only_keys = self.nearest_neighbors.is_some();

        let pk_indices: Vec<usize> = if !self.mutations_enabled() && !read_only_keys {
            Vec::new()
        } else if stepped_into {
            // Rows inside a nested value are addressed by their path in the
            // document, not by a key column; they are editable only when the
            // driver writes field patches.
            if document_patches && !self.result.columns.is_empty() {
                vec![0]
            } else {
                Vec::new()
            }
        } else {
            let indices: Vec<usize> = self
                .pk_columns
                .iter()
                .filter_map(|pk_name| self.result.columns.iter().position(|c| c.name == *pk_name))
                .collect();

            // A save addresses a row by every key column; with part of the
            // key missing from the result it could match several rows.
            if indices.len() == self.pk_columns.len() {
                indices
            } else {
                Vec::new()
            }
        };

        log::debug!(
            "rebuild_table: pk_columns={:?}, pk_indices={:?}",
            self.pk_columns,
            pk_indices,
        );

        // When a builder binding is present, respect its `insertable` flag to
        // prevent INSERT on join results (column origin is ambiguous).
        let binding_insertable = self
            .builder
            .builder_editable_binding
            .as_ref()
            .map(|b| b.insertable)
            .unwrap_or(true);

        let is_insertable = matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        ) && self.mutations_enabled()
            && (self.builder.current_visual_spec.is_none() || binding_insertable)
            && !stepped_into;

        let column_details = self.get_column_details(cx);

        // Compute FK column indices before entering the cx.new closure.
        let fk_names = self.get_fk_column_names(cx);
        let fk_indices: std::collections::HashSet<usize> = if fk_names.is_empty() {
            std::collections::HashSet::new()
        } else {
            self.result
                .columns
                .iter()
                .enumerate()
                .filter(|(_, col)| fk_names.contains(&col.name))
                .map(|(ix, _)| ix)
                .collect()
        };

        // Compute read-only column indices from the builder binding's column_origin map.
        // Columns tagged Joined are blocked from editing while source-table columns remain
        // editable. This mirrors the FK badge marking pattern above.
        let readonly_indices: std::collections::HashSet<usize> =
            if self.collection.raw.is_some() && !document_patches {
                // Without field patches only top-level fields can be saved, through
                // the generic row save.
                self.collection
                    .flat
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| column.path.len() != 1 || stepped_into)
                    .map(|(ix, _)| ix)
                    .collect()
            } else if let Some(binding) = &self.builder.builder_editable_binding {
                use dbflux_core::ColumnOrigin;
                self.result
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, col)| {
                        binding.column_origin.get(&col.name).copied() == Some(ColumnOrigin::Joined)
                    })
                    .map(|(ix, _)| ix)
                    .collect()
            } else {
                std::collections::HashSet::new()
            };

        let table_model = Arc::new(
            self.document_table_model()
                .unwrap_or_else(|| TableModel::from(&self.result)),
        );
        let document_presentation = self.document_presentation();
        let enum_options = enum_options_for_result(&self.result, column_details.as_deref());

        if let Some(table_state) = self.grid_table.table_state.clone() {
            // A reload reuses the state entity, so column widths, sort, scroll
            // and the record-mode flag survive it. Only the state that
            // addresses the rows being replaced is rebuilt here.
            let dropped_rows = table_state.update(cx, |state, cx| {
                let carried_edits = keep_edits.then(|| state.snapshot_pending_edits());

                // `initial_sort` describes the sort the incoming rows carry,
                // so an absent one means "unsorted": leaving the previous sort
                // in place would light up a header arrow the new rows do not
                // honour. This covers `TableReload::NewColumns` too, which
                // arrives with no sort.
                match initial_sort {
                    Some(sort) => state.set_sort_without_emit(sort),
                    None => state.clear_sort_without_emit(),
                }

                state.set_model(table_model, reload.cursor_swap(), cx);
                if reload == TableReload::NewColumns {
                    // The columns are new, so a pixel offset from the previous
                    // projection points at nothing meaningful.
                    state.scroll_columns_to_start();
                }
                if read_only_keys {
                    state.set_read_only_pk_columns(pk_indices);
                } else {
                    state.set_pk_columns(pk_indices);
                }
                state.set_reference_row(read_only_keys);
                state.set_insertable(is_insertable);
                state.set_fk_columns(fk_indices);
                state.set_readonly_columns(readonly_indices);
                state.set_document_presentation(document_presentation, cx);

                for (col_ix, options) in enum_options {
                    state.set_enum_options(col_ix, options);
                }

                carried_edits.map_or(0, |edits| state.restore_pending_edits(edits, cx))
            });

            if dropped_rows > 0 {
                dbflux_ui_base::toast::Toast::warning(crate::labels::grid_edits_dropped_on_reload(
                    dropped_rows,
                ))
                .meta_right(dbflux_ui_base::toast::now_hms())
                .push(cx);
            }

            self.rebuild_result_views(cx);
            return;
        }

        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            if let Some(sort) = initial_sort {
                state.set_sort_without_emit(sort);
            }
            if read_only_keys {
                state.set_read_only_pk_columns(pk_indices.clone());
            } else {
                state.set_pk_columns(pk_indices.clone());
            }
            state.set_reference_row(read_only_keys);
            state.set_insertable(is_insertable);
            state.set_document_presentation(document_presentation, cx);

            if !fk_indices.is_empty() {
                state.set_fk_columns(fk_indices);
            }

            if !readonly_indices.is_empty() {
                state.set_readonly_columns(readonly_indices);
            }

            for (col_ix, options) in enum_options {
                state.set_enum_options(col_ix, options);
            }

            state
        });
        let data_table = cx.new(|cx| DataTable::new("data-grid-table", table_state.clone(), cx));

        let subscription =
            cx.subscribe(&table_state, |this, _state, event: &DataTableEvent, cx| {
                match event {
                    DataTableEvent::SortChanged(sort) => match sort {
                        Some(sort_state) => {
                            this.handle_sort_request(
                                sort_state.column_ix,
                                sort_state.direction,
                                cx,
                            );
                        }
                        None => {
                            this.handle_sort_clear(cx);
                        }
                    },
                    DataTableEvent::Focused => {
                        cx.emit(DataGridEvent::Focused);
                    }
                    DataTableEvent::SelectionChanged(selection) => {
                        // When the row inspector is active, follow the user's
                        // cursor so click / arrow-key navigation updates the
                        // rail in place.
                        if this.inspector.follow_selection
                            && !this.inspector.pinned
                            && let Some(active) = selection.active
                        {
                            this.open_row_inspector(active.row, active.col, cx);
                        }

                        if this.inspector.value_panel_open
                            && let Some(active) = selection.active
                            && this.value_panel_can_follow(active.row, active.col, cx)
                        {
                            this.request_value_panel(active.row, active.col, cx);
                        }
                    }
                    DataTableEvent::SaveRowRequested(row_idx) => {
                        if this.commits_document_patches(cx) {
                            this.commit_document_edits(cx);
                        } else {
                            this.handle_save_row(*row_idx, false, cx);
                        }
                    }
                    DataTableEvent::StepIntoRequested { row, col } => {
                        this.step_into_document_value(*row, *col, cx);
                    }
                    DataTableEvent::ToggleColumnGroupRequested { col } => {
                        this.toggle_document_column_group(*col, cx);
                    }
                    DataTableEvent::StepOutRequested => {
                        this.step_out_of_document_value(cx);
                    }
                    DataTableEvent::ContextMenuRequested {
                        row,
                        col,
                        position,
                        is_column_header,
                    } => {
                        // Gather any driver-supplied row actions (e.g. Kill, Cancel).
                        // They are injected as extra menu items at the bottom rather
                        // than bypassing the context menu entirely.
                        let row_actions = if *is_column_header {
                            Vec::new()
                        } else {
                            this.menu_row_actions()
                        };

                        this.context_menu = Some(TableContextMenu {
                            row: *row,
                            col: *col,
                            position: *position,
                            sql_submenu_open: false,
                            copy_query_submenu_open: false,
                            filter_submenu_open: false,
                            order_submenu_open: false,
                            toolbar_submenu_open: false,
                            selected_index: 0,
                            submenu_selected_index: 0,
                            is_document_view: false,
                            is_column_header: *is_column_header,
                            doc_field_path: None,
                            doc_field_value: None,
                            row_actions,
                        });
                        this.pending.context_menu_focus = true;
                        cx.emit(DataGridEvent::Focused);
                        cx.notify();
                    }
                    // Keyboard-triggered row operations
                    DataTableEvent::DeleteRowRequested(row) => {
                        this.handle_delete_row(*row, cx);
                    }
                    DataTableEvent::AddRowRequested(row) => {
                        this.handle_add_row(*row, false, cx);
                    }
                    DataTableEvent::DuplicateRowRequested(row) => {
                        this.handle_duplicate_row(*row, false, cx);
                    }
                    DataTableEvent::SetNullRequested { row, col } => {
                        this.handle_set_null(*row, *col, cx);
                    }
                    DataTableEvent::CopyRowRequested(row) => {
                        this.handle_copy_row(*row, cx);
                    }
                    DataTableEvent::ModalEditRequested {
                        row,
                        col,
                        value,
                        is_json,
                    } => {
                        this.pending.modal_open = Some(PendingModalOpen {
                            row: *row,
                            col: *col,
                            value: value.clone(),
                            is_json: *is_json,
                        });
                        cx.notify();
                    }
                    DataTableEvent::CommitInsertRequested(insert_idx) => {
                        this.handle_commit_insert(*insert_idx, false, cx);
                    }
                    DataTableEvent::CommitDeleteRequested(row_idx) => {
                        this.handle_commit_delete(*row_idx, cx);
                    }
                    DataTableEvent::SaveAllRequested { .. }
                        if this.commits_document_patches(cx) =>
                    {
                        this.commit_document_edits(cx);
                    }
                    DataTableEvent::SaveAllRequested {
                        pending_deletes,
                        pending_inserts,
                        dirty_rows,
                    } => {
                        this.handle_save_all(
                            pending_deletes.clone(),
                            pending_inserts.clone(),
                            dirty_rows.clone(),
                            cx,
                        );
                    }
                }
            });

        self.grid_table.table_state = Some(table_state);
        self.grid_table.data_table = Some(data_table);
        self.grid_table.table_subscription = Some(subscription);

        self.rebuild_result_views(cx);
    }

    pub(super) fn current_sort_info(&self) -> Option<(String, SortDirection, bool)> {
        match &self.source {
            DataSource::Table { order_by, .. } => order_by
                .first()
                .map(|col| (col.column.name.clone(), col.direction, true)),
            DataSource::Collection { .. } => None,
            DataSource::QueryResult { .. } => self.grid_table.local_sort_state.and_then(|state| {
                self.result
                    .columns
                    .get(state.column_ix)
                    .map(|col| (col.name.clone(), state.direction, false))
            }),
        }
    }

    /// Returns `(inserts, updates, deletes)` counts from the pending edit buffer.
    ///
    /// Returns `(0, 0, 0)` when the table has no edit state or no pending changes.
    pub fn pending_edit_counts(&self, cx: &App) -> (usize, usize, usize) {
        let Some(table_state) = &self.grid_table.table_state else {
            return (0, 0, 0);
        };

        let state = table_state.read(cx);
        let buffer = state.edit_buffer();

        let inserts = buffer.pending_insert_rows().len();
        let updates = buffer.dirty_row_count();
        let deletes = buffer.pending_delete_rows().len();

        (inserts, updates, deletes)
    }

    /// Short summary of pending edits for the dirty-dot tooltip.
    ///
    /// Returns `None` when no changes are staged.
    pub fn change_summary(&self, cx: &App) -> Option<String> {
        let (inserts, updates, deletes) = self.pending_edit_counts(cx);

        crate::labels::pending_edits_summary(inserts, updates, deletes)
    }

    /// Commits the input the grid still holds in an open editor, the inline
    /// cell editor and the cell editor dialog, so it becomes a pending change
    /// as Enter or Save would make it.
    ///
    /// Returns `false` when input could not be committed: a JSON value that
    /// does not parse stays in the dialog with its error shown, and the caller
    /// must not close the grid over it.
    pub fn commit_pending_input(&mut self, cx: &mut Context<Self>) -> bool {
        let dialog_edit = self
            .document_view
            .cell_editor
            .update(cx, |editor, cx| editor.take_unsaved_edit(cx));

        match dialog_edit {
            Ok(Some(edit)) => self.write_cell_value(edit.row, edit.col, &edit.value, cx),
            Ok(None) => {}
            Err(_) => return false,
        }

        if let Some(table_state) = &self.grid_table.table_state {
            table_state.update(cx, |state, cx| state.commit_pending_edit(cx));
        }

        true
    }
}
