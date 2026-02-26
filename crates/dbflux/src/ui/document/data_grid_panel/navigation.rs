use super::{
    DataGridPanel, DataSource, EditState, GridFocusMode, LocalSortState, PendingRequery,
    ToolbarFocus,
};
use crate::keymap::Command;
use crate::ui::components::data_table::{Direction, Edge, SortState as TableSortState};
use dbflux_core::{OrderByColumn, Pagination, SortDirection};
use gpui::*;
use std::cmp::Ordering;

impl DataGridPanel {
    // === Sorting ===

    pub(super) fn handle_sort_request(
        &mut self,
        col_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        let col_name = self
            .result
            .columns
            .get(col_ix)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        // Extract values before mutating self.source
        let table_info = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                pagination,
                total_rows,
                ..
            } => Some((
                *profile_id,
                database.clone(),
                table.clone(),
                pagination.reset_offset(),
                *total_rows,
            )),
            DataSource::Collection { .. } => None,
            DataSource::QueryResult { .. } => None,
        };

        if let Some((profile_id, database, table, new_pagination, total_rows)) = table_info {
            // Server-side sort: update source and queue re-query
            let new_order_by = vec![OrderByColumn {
                name: col_name,
                direction,
            }];

            let filter_value = self.filter_input.read(cx).value();
            let filter = if filter_value.trim().is_empty() {
                None
            } else {
                Some(filter_value.to_string())
            };

            // Update source immediately for UI consistency
            self.source = DataSource::Table {
                profile_id,
                database: database.clone(),
                table: table.clone(),
                pagination: new_pagination.clone(),
                order_by: new_order_by.clone(),
                total_rows,
            };

            // Queue re-query
            self.pending_requery = Some(PendingRequery {
                profile_id,
                database,
                table,
                pagination: new_pagination,
                order_by: new_order_by,
                filter,
                total_rows,
            });

            cx.notify();
        } else {
            // Client-side sort: sort in memory
            self.apply_local_sort(col_ix, direction, cx);
        }
    }

    pub(super) fn handle_sort_clear(&mut self, cx: &mut Context<Self>) {
        // Extract values before mutating self.source
        let table_info = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                pagination,
                total_rows,
                ..
            } => {
                let pk_order =
                    Self::get_primary_key_columns(&self.app_state, *profile_id, table, cx);
                Some((
                    *profile_id,
                    database.clone(),
                    table.clone(),
                    pagination.reset_offset(),
                    *total_rows,
                    pk_order,
                ))
            }
            DataSource::Collection { .. } => None,
            DataSource::QueryResult { .. } => None,
        };

        if let Some((profile_id, database, table, new_pagination, total_rows, pk_order)) =
            table_info
        {
            let filter_value = self.filter_input.read(cx).value();
            let filter = if filter_value.trim().is_empty() {
                None
            } else {
                Some(filter_value.to_string())
            };

            self.source = DataSource::Table {
                profile_id,
                database: database.clone(),
                table: table.clone(),
                pagination: new_pagination.clone(),
                order_by: pk_order.clone(),
                total_rows,
            };

            self.pending_requery = Some(PendingRequery {
                profile_id,
                database,
                table,
                pagination: new_pagination,
                order_by: pk_order,
                filter,
                total_rows,
            });

            cx.notify();
        } else {
            // Restore original row order
            if let Some(original_order) = self.original_row_order.take() {
                let mut restore_indices: Vec<(usize, usize)> = original_order
                    .iter()
                    .enumerate()
                    .map(|(current, &original)| (original, current))
                    .collect();
                restore_indices.sort_by_key(|(orig, _)| *orig);

                let rows = std::mem::take(&mut self.result.rows);
                self.result.rows = restore_indices
                    .into_iter()
                    .map(|(_, current)| rows[current].clone())
                    .collect();
            }

            self.local_sort_state = None;
            self.pending_rebuild = true;
            cx.notify();
        }
    }

    pub(super) fn apply_local_sort(
        &mut self,
        col_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        // Save original order if this is the first sort
        if self.original_row_order.is_none() {
            self.original_row_order = Some((0..self.result.rows.len()).collect());
        }

        // Sort using indices for tracking
        let mut indices: Vec<usize> = (0..self.result.rows.len()).collect();
        indices.sort_by(|&a, &b| {
            let val_a = self.result.rows[a].get(col_ix);
            let val_b = self.result.rows[b].get(col_ix);

            let cmp = match (val_a, val_b) {
                (Some(a), Some(b)) => a.cmp(b),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };

            match direction {
                SortDirection::Ascending => cmp,
                SortDirection::Descending => cmp.reverse(),
            }
        });

        // Reorder rows according to sorted indices
        let sorted_rows: Vec<_> = indices
            .iter()
            .map(|&i| self.result.rows[i].clone())
            .collect();
        self.result.rows = sorted_rows;

        // Update original_row_order to map new order -> original
        if let Some(ref mut orig) = self.original_row_order {
            *orig = indices.iter().map(|&i| orig[i]).collect();
        }

        self.local_sort_state = Some(LocalSortState {
            column_ix: col_ix,
            direction,
        });
        self.pending_rebuild = true;
        cx.notify();
    }

    // === Pagination ===

    pub fn go_to_next_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                pagination,
                order_by,
                total_rows,
            } => {
                self.run_table_query(
                    *profile_id,
                    database.clone(),
                    table.clone(),
                    pagination.next_page(),
                    order_by.clone(),
                    *total_rows,
                    window,
                    cx,
                );
            }
            DataSource::Collection {
                profile_id,
                collection,
                pagination,
                total_docs,
            } => {
                self.run_collection_query(
                    *profile_id,
                    collection.clone(),
                    pagination.next_page(),
                    *total_docs,
                    window,
                    cx,
                );
            }
            DataSource::QueryResult { .. } => {}
        }
    }

    pub fn go_to_prev_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(prev) = self.source.pagination().and_then(|p| p.prev_page()) else {
            return;
        };

        match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                order_by,
                total_rows,
                ..
            } => {
                self.run_table_query(
                    *profile_id,
                    database.clone(),
                    table.clone(),
                    prev,
                    order_by.clone(),
                    *total_rows,
                    window,
                    cx,
                );
            }
            DataSource::Collection {
                profile_id,
                collection,
                total_docs,
                ..
            } => {
                self.run_collection_query(
                    *profile_id,
                    collection.clone(),
                    prev,
                    *total_docs,
                    window,
                    cx,
                );
            }
            DataSource::QueryResult { .. } => {}
        }
    }

    pub(super) fn can_go_prev(&self) -> bool {
        self.source
            .pagination()
            .map(|p| !p.is_first_page())
            .unwrap_or(false)
    }

    pub(super) fn can_go_next(&self) -> bool {
        let Some(pagination) = self.source.pagination() else {
            return false;
        };

        if let Some(total) = self.source.total_rows() {
            let next_offset = pagination.offset() + pagination.limit() as u64;
            return next_offset < total;
        }

        self.result.row_count() >= pagination.limit() as usize
    }

    pub(super) fn total_pages(&self) -> Option<u64> {
        let pagination = self.source.pagination()?;
        let total = self.source.total_rows()?;
        let limit = pagination.limit() as u64;
        if limit == 0 {
            return Some(1);
        }
        Some(total.div_ceil(limit))
    }

    // === Navigation ===

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Down, false, cx);
            });
        }
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Up, false, cx);
            });
        }
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_to_edge(Edge::Home, false, cx);
            });
        }
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_to_edge(Edge::End, false, cx);
            });
        }
    }

    pub fn column_left(&mut self, cx: &mut Context<Self>) {
        if self.result.columns.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Left, false, cx);
            });
        }
    }

    pub fn column_right(&mut self, cx: &mut Context<Self>) {
        if self.result.columns.is_empty() {
            return;
        }
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                state.move_active(Direction::Right, false, cx);
            });
        }
    }

    // === Focus Management ===

    #[allow(dead_code)]
    pub(super) fn focus_mode(&self) -> GridFocusMode {
        self.focus_mode
    }

    pub fn focus_toolbar(&mut self, cx: &mut Context<Self>) {
        if !self.source.is_table() {
            return;
        }
        self.focus_mode = GridFocusMode::Toolbar;
        self.toolbar_focus = ToolbarFocus::Filter;
        self.edit_state = EditState::Navigating;
        cx.notify();
    }

    pub fn focus_table(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = GridFocusMode::Table;
        self.edit_state = EditState::Navigating;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    pub fn toolbar_left(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != GridFocusMode::Toolbar {
            return;
        }
        self.toolbar_focus = self.toolbar_focus.left();
        cx.notify();
    }

    pub fn toolbar_right(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != GridFocusMode::Toolbar {
            return;
        }
        self.toolbar_focus = self.toolbar_focus.right();
        cx.notify();
    }

    pub fn toolbar_execute(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_mode != GridFocusMode::Toolbar {
            return;
        }

        match self.toolbar_focus {
            ToolbarFocus::Filter => {
                self.edit_state = EditState::Editing;
                self.filter_input.update(cx, |input, cx| {
                    input.focus(window, cx);
                });
                cx.notify();
            }
            ToolbarFocus::Limit => {
                self.edit_state = EditState::Editing;
                self.limit_input.update(cx, |input, cx| {
                    input.focus(window, cx);
                });
                cx.notify();
            }
            ToolbarFocus::Refresh => {
                self.refresh(window, cx);
                self.focus_table(window, cx);
            }
        }
    }

    pub fn exit_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.switching_input {
            self.switching_input = false;
            return;
        }

        if self.edit_state == EditState::Editing {
            self.edit_state = EditState::Navigating;
            window.focus(&self.focus_handle);
            cx.notify();
        }
    }

    // === Command Dispatch ===

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // Handle delete confirmation modal
        if self.pending_delete_confirm.is_some() {
            match cmd {
                Command::Cancel => {
                    self.cancel_delete(window, cx);
                    return true;
                }
                Command::Execute => {
                    self.confirm_delete(window, cx);
                    return true;
                }
                _ => return true, // Block other commands while modal is open
            }
        }

        // Handle context menu commands when menu is open
        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        // Handle toolbar mode commands
        if self.focus_mode == GridFocusMode::Toolbar {
            match cmd {
                Command::Cancel | Command::FocusUp => {
                    self.focus_table(window, cx);
                    return true;
                }
                Command::FocusLeft | Command::ColumnLeft => {
                    self.toolbar_left(cx);
                    return true;
                }
                Command::FocusRight | Command::ColumnRight => {
                    self.toolbar_right(cx);
                    return true;
                }
                Command::Execute => {
                    self.toolbar_execute(window, cx);
                    return true;
                }
                _ => {}
            }
        }

        // When an enum dropdown is open, route navigation to the dropdown
        let editing_enum = self
            .table_state
            .as_ref()
            .map(|ts| ts.read(cx).is_editing_enum())
            .unwrap_or(false);

        if editing_enum && let Some(table_state) = &self.table_state {
            match cmd {
                Command::SelectNext | Command::FocusDown => {
                    table_state.update(cx, |state, cx| state.enum_dropdown_next(cx));
                    return true;
                }
                Command::SelectPrev | Command::FocusUp => {
                    table_state.update(cx, |state, cx| state.enum_dropdown_prev(cx));
                    return true;
                }
                Command::Execute => {
                    table_state.update(cx, |state, cx| state.enum_dropdown_accept(cx));
                    return true;
                }
                Command::Cancel => {
                    table_state.update(cx, |state, cx| state.enum_dropdown_cancel(cx));
                    return true;
                }
                _ => {}
            }
        }

        // Handle table mode commands
        match cmd {
            Command::FocusToolbar => {
                self.focus_toolbar(cx);
                true
            }
            Command::Execute => {
                if let Some(table_state) = &self.table_state {
                    table_state.update(cx, |state, cx| {
                        if state.is_editing() {
                            state.stop_editing(true, cx);
                        } else if let Some(coord) = state.selection().active {
                            state.start_editing(coord, window, cx);
                        }
                    });
                }
                true
            }
            Command::Cancel => {
                if let Some(table_state) = &self.table_state {
                    let was_editing = table_state.update(cx, |state, cx| {
                        if state.is_editing() {
                            state.stop_editing(false, cx);
                            true
                        } else {
                            false
                        }
                    });
                    if was_editing {
                        return true;
                    }
                }
                false
            }
            Command::SelectNext | Command::FocusDown => {
                self.select_next(cx);
                true
            }
            Command::SelectPrev | Command::FocusUp => {
                self.select_prev(cx);
                true
            }
            Command::SelectFirst => {
                self.select_first(cx);
                true
            }
            Command::SelectLast => {
                self.select_last(cx);
                true
            }
            Command::ColumnLeft | Command::FocusLeft => {
                self.column_left(cx);
                true
            }
            Command::ColumnRight | Command::FocusRight => {
                self.column_right(cx);
                true
            }
            Command::ResultsNextPage | Command::PageDown => {
                self.go_to_next_page(window, cx);
                true
            }
            Command::ResultsPrevPage | Command::PageUp => {
                self.go_to_prev_page(window, cx);
                true
            }
            Command::RefreshSchema => {
                self.refresh(window, cx);
                true
            }
            Command::ExportResults => {
                self.export_results(window, cx);
                true
            }
            Command::OpenContextMenu => {
                use crate::ui::document::DataViewMode;
                if self.view_config.mode == DataViewMode::Document {
                    self.open_document_context_menu_at_cursor(window, cx);
                } else {
                    self.open_context_menu_at_selection(window, cx);
                }
                true
            }
            _ => false,
        }
    }
}
