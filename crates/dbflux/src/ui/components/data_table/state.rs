use std::sync::Arc;

use gpui::{
    AppContext, Context, Entity, EventEmitter, FocusHandle, Focusable, Pixels, Point, ScrollHandle,
    Size, UniformListScrollHandle, Window, px,
};
use gpui_component::input::{InputEvent, InputState};

use super::clipboard;
use super::events::{DataTableEvent, Direction, Edge, SortState};
use super::model::{EditBuffer, TableModel};
use super::selection::{CellCoord, SelectionState};
use super::theme::{DEFAULT_COLUMN_WIDTH, SCROLLBAR_WIDTH};
use crate::ui::dropdown::{Dropdown, DropdownDismissed, DropdownItem, DropdownSelectionChanged};

/// Main state for the DataTable component.
pub struct DataTableState {
    /// The data model (Arc to avoid cloning).
    model: Arc<TableModel>,

    /// Width of each column.
    column_widths: Vec<f32>,

    /// Prefix sums of column widths for hit-testing: [0, w0, w0+w1, ...].
    column_offsets: Vec<f32>,

    /// Current sort state.
    sort: Option<SortState>,

    /// Viewport size (updated on layout).
    viewport_size: Size<Pixels>,

    /// Selection state.
    selection: SelectionState,

    /// Focus handle for keyboard input.
    focus_handle: FocusHandle,

    /// Scroll handle for vertical scrolling (uniform list).
    vertical_scroll_handle: UniformListScrollHandle,

    /// Scroll handle for horizontal scrolling.
    horizontal_scroll_handle: ScrollHandle,

    /// Cached horizontal scroll offset for header and body positioning.
    /// Updated when scroll handle offset changes to trigger re-renders.
    horizontal_offset: Pixels,

    // --- Edit Mode ---
    /// Cell currently being edited (inline editor is open).
    editing_cell: Option<CellCoord>,

    /// Input state for the inline cell editor.
    cell_input: Option<Entity<InputState>>,

    /// Dropdown for editing enum/set columns inline.
    enum_dropdown: Option<Entity<Dropdown>>,

    /// Buffer for tracking local edits before committing.
    edit_buffer: EditBuffer,

    /// Column indices that form the primary key (for row identification).
    pk_columns: Vec<usize>,

    /// Whether this table is editable (requires PK for row identification).
    is_editable: bool,

    /// Whether this table supports INSERT operations (add/duplicate rows).
    /// True for Table and Collection sources, false for query results.
    is_insertable: bool,

    /// Enum/set options per column index.
    enum_options: std::collections::HashMap<usize, Vec<String>>,
}

impl DataTableState {
    pub const NULL_SENTINEL: &'static str = "\0__NULL__";

    pub fn new(model: Arc<TableModel>, cx: &mut Context<Self>) -> Self {
        let col_count = model.col_count();
        let row_count = model.row_count();
        let column_widths = vec![DEFAULT_COLUMN_WIDTH; col_count];
        let column_offsets = Self::calculate_offsets(&column_widths);

        let mut edit_buffer = EditBuffer::new();
        edit_buffer.set_base_row_count(row_count);

        Self {
            model,
            column_widths,
            column_offsets,
            sort: None,
            viewport_size: Size::default(),
            selection: SelectionState::new(),
            focus_handle: cx.focus_handle(),
            vertical_scroll_handle: UniformListScrollHandle::new(),
            horizontal_scroll_handle: ScrollHandle::new(),
            horizontal_offset: px(0.0),
            editing_cell: None,
            cell_input: None,
            enum_dropdown: None,
            edit_buffer,
            pk_columns: Vec::new(),
            is_editable: false,
            is_insertable: false,
            enum_options: std::collections::HashMap::new(),
        }
    }

    fn calculate_offsets(widths: &[f32]) -> Vec<f32> {
        let mut offsets = vec![0.0];
        let mut sum = 0.0;
        for w in widths {
            sum += w;
            offsets.push(sum);
        }
        offsets
    }

    // --- Model ---

    pub fn model(&self) -> &TableModel {
        &self.model
    }

    pub fn model_arc(&self) -> &Arc<TableModel> {
        &self.model
    }

    pub fn row_count(&self) -> usize {
        // Include pending inserts in the row count
        self.model.row_count() + self.edit_buffer.pending_insert_rows().len()
    }

    /// Get the base row count (excluding pending inserts).
    #[allow(dead_code)]
    pub fn base_row_count(&self) -> usize {
        self.model.row_count()
    }

    pub fn col_count(&self) -> usize {
        self.model.col_count()
    }

    // --- Column Layout ---

    pub fn column_widths(&self) -> &[f32] {
        &self.column_widths
    }

    pub fn set_column_width(&mut self, col: usize, width: f32, cx: &mut Context<Self>) {
        if col < self.column_widths.len() {
            let min_width = super::theme::MIN_COLUMN_WIDTH;
            self.column_widths[col] = width.max(min_width);
            self.column_offsets = Self::calculate_offsets(&self.column_widths);
            cx.notify();
        }
    }

    pub fn total_content_width(&self) -> f32 {
        *self.column_offsets.last().unwrap_or(&0.0)
    }

    // --- Viewport ---

    pub fn viewport_size(&self) -> Size<Pixels> {
        self.viewport_size
    }

    pub fn set_viewport_size(&mut self, size: Size<Pixels>, cx: &mut Context<Self>) {
        if self.viewport_size != size {
            self.viewport_size = size;
            cx.notify();
        }
    }

    // --- Sort ---

    pub fn sort(&self) -> Option<&SortState> {
        self.sort.as_ref()
    }

    pub fn set_sort(&mut self, sort: Option<SortState>, cx: &mut Context<Self>) {
        if self.sort != sort {
            self.sort = sort;
            cx.emit(DataTableEvent::SortChanged(sort));
            cx.notify();
        }
    }

    /// Set sort state without emitting an event (for initial state).
    pub fn set_sort_without_emit(&mut self, sort: SortState) {
        self.sort = Some(sort);
    }

    /// Cycle sort state for a column: none -> asc -> desc -> none
    pub fn cycle_sort(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        let new_sort = next_sort_state(self.sort, col_ix);

        self.set_sort(new_sort, cx);
    }

    // --- Selection ---

    pub fn selection(&self) -> &SelectionState {
        &self.selection
    }

    pub fn select_cell(&mut self, coord: CellCoord, cx: &mut Context<Self>) {
        self.selection.select_cell(coord);
        cx.emit(DataTableEvent::SelectionChanged(self.selection.clone()));
        cx.notify();
    }

    pub fn extend_selection(&mut self, coord: CellCoord, cx: &mut Context<Self>) {
        self.selection.extend_to(coord);
        cx.emit(DataTableEvent::SelectionChanged(self.selection.clone()));
        cx.notify();
    }

    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        if !self.selection.is_empty() {
            self.selection.clear();
            cx.emit(DataTableEvent::SelectionChanged(self.selection.clone()));
            cx.notify();
        }
    }

    pub fn select_all(&mut self, cx: &mut Context<Self>) {
        self.selection
            .select_all(self.row_count(), self.col_count());
        cx.emit(DataTableEvent::SelectionChanged(self.selection.clone()));
        cx.notify();
    }

    // --- Navigation ---

    /// Move active cell in a direction. If extend is true, extend selection instead of moving.
    pub fn move_active(&mut self, direction: Direction, extend: bool, cx: &mut Context<Self>) {
        let row_count = self.row_count();
        let col_count = self.col_count();

        if row_count == 0 || col_count == 0 {
            return;
        }

        // No selection yet - select first cell
        let Some(current) = self.selection.active else {
            self.select_cell(CellCoord::new(0, 0), cx);
            self.scroll_to_cell(0, 0);
            return;
        };

        let new_coord = match direction {
            Direction::Up => CellCoord::new(current.row.saturating_sub(1), current.col),
            Direction::Down => CellCoord::new((current.row + 1).min(row_count - 1), current.col),
            Direction::Left => CellCoord::new(current.row, current.col.saturating_sub(1)),
            Direction::Right => CellCoord::new(current.row, (current.col + 1).min(col_count - 1)),
        };

        if extend {
            self.extend_selection(new_coord, cx);
        } else {
            self.select_cell(new_coord, cx);
        }

        self.scroll_to_cell(new_coord.row, new_coord.col);
    }

    /// Move to an edge of the table.
    pub fn move_to_edge(&mut self, edge: Edge, extend: bool, cx: &mut Context<Self>) {
        let row_count = self.row_count();
        let col_count = self.col_count();

        if row_count == 0 || col_count == 0 {
            return;
        }

        let current = self.selection.active.unwrap_or(CellCoord::new(0, 0));
        let new_coord = match edge {
            Edge::Top => CellCoord::new(0, current.col),
            Edge::Bottom => CellCoord::new(row_count - 1, current.col),
            Edge::Left => CellCoord::new(current.row, 0),
            Edge::Right => CellCoord::new(current.row, col_count - 1),
            Edge::Home => CellCoord::new(0, 0),
            Edge::End => CellCoord::new(row_count - 1, col_count - 1),
        };

        if extend {
            self.extend_selection(new_coord, cx);
        } else {
            self.select_cell(new_coord, cx);
        }

        self.scroll_to_cell(new_coord.row, new_coord.col);
    }

    // --- Clipboard ---

    pub fn copy_selection(&self) -> Option<String> {
        clipboard::copy_selection(&self.model, &self.selection)
    }

    // --- Focus ---

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Focus the table for keyboard navigation and emit Focused event.
    pub fn focus(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
        cx.emit(DataTableEvent::Focused);
    }

    // --- Scroll Handles ---

    pub fn vertical_scroll_handle(&self) -> &UniformListScrollHandle {
        &self.vertical_scroll_handle
    }

    pub fn horizontal_scroll_handle(&self) -> &ScrollHandle {
        &self.horizontal_scroll_handle
    }

    pub fn horizontal_offset(&self) -> Pixels {
        self.horizontal_offset
    }

    /// Sync horizontal offset from scroll handle. Returns true if changed.
    ///
    /// Clamps the offset to the valid range based on the real viewport size,
    /// since the phantom scroller has a 1px viewport which causes the scroll
    /// handle to calculate an incorrect max_offset.
    pub fn sync_horizontal_offset(&mut self, cx: &mut Context<Self>) -> bool {
        // gpui uses negative offsets (scroll right = negative), we store positive
        let handle_offset = -self.horizontal_scroll_handle.offset().x;

        let clamped_offset = if self.viewport_size.width > px(0.0) {
            let content_width = px(self.total_content_width());
            let viewport_width = self.viewport_size.width - SCROLLBAR_WIDTH;
            let max_offset = (content_width - viewport_width).max(px(0.0));

            handle_offset.clamp(px(0.0), max_offset)
        } else {
            handle_offset.max(px(0.0))
        };

        let diff = (self.horizontal_offset - clamped_offset).abs();
        if diff > px(1.0) {
            self.horizontal_offset = clamped_offset;
            cx.notify();
            return true;
        }

        false
    }

    /// Scroll to ensure the given row is visible.
    pub fn scroll_to_row(&self, row: usize) {
        self.vertical_scroll_handle
            .scroll_to_item(row, gpui::ScrollStrategy::Center);
    }

    /// Scroll to ensure the given column is visible.
    pub fn scroll_to_column(&self, col: usize) {
        if col >= self.column_offsets.len() {
            return;
        }

        let col_left = px(self.column_offsets[col]);
        let col_right = px(*self
            .column_offsets
            .get(col + 1)
            .unwrap_or(&self.column_offsets[col]));

        let viewport_width = self.viewport_size.width - SCROLLBAR_WIDTH;
        if viewport_width <= px(0.0) {
            return;
        }

        let current_offset = self.horizontal_offset;
        let visible_left = current_offset;
        let visible_right = current_offset + viewport_width;

        let new_offset = if col_left < visible_left {
            col_left
        } else if col_right > visible_right {
            col_right - viewport_width
        } else {
            return;
        };

        let content_width = px(self.total_content_width());
        let max_offset = (content_width - viewport_width).max(px(0.0));
        let clamped = new_offset.clamp(px(0.0), max_offset);

        self.horizontal_scroll_handle
            .set_offset(Point::new(-clamped, px(0.0)));
    }

    /// Scroll to ensure the given cell is visible (both row and column).
    pub fn scroll_to_cell(&self, row: usize, col: usize) {
        self.scroll_to_row(row);
        self.scroll_to_column(col);
    }

    // --- Edit Mode ---

    /// Check if the table is editable (has primary key columns).
    pub fn is_editable(&self) -> bool {
        self.is_editable
    }

    /// Set the primary key column indices and update editability.
    pub fn set_pk_columns(&mut self, pk_columns: Vec<usize>) {
        self.is_editable = !pk_columns.is_empty();
        self.pk_columns = pk_columns;
    }

    /// Get the primary key column indices.
    pub fn pk_columns(&self) -> &[usize] {
        &self.pk_columns
    }

    /// Check if the table supports INSERT operations (add/duplicate rows).
    pub fn is_insertable(&self) -> bool {
        self.is_insertable
    }

    /// Set whether the table supports INSERT operations.
    pub fn set_insertable(&mut self, insertable: bool) {
        self.is_insertable = insertable;
    }

    pub fn set_enum_options(&mut self, col: usize, options: Vec<String>) {
        self.enum_options.insert(col, options);
    }

    #[allow(dead_code)]
    pub fn enum_options(&self, col: usize) -> Option<&Vec<String>> {
        self.enum_options.get(&col)
    }

    /// Check if a cell is currently being edited.
    pub fn is_editing(&self) -> bool {
        self.editing_cell.is_some()
    }

    /// Get the currently editing cell, if any.
    pub fn editing_cell(&self) -> Option<CellCoord> {
        self.editing_cell
    }

    /// Start editing a cell. Returns false if the table is not editable.
    /// Note: `coord` uses visual row indices (accounting for pending inserts).
    ///
    /// Editing is allowed when:
    /// - `is_editable` is true (can edit any row, requires PK for UPDATE)
    /// - `is_insertable` is true AND the row is a pending insert (can edit new rows)
    pub fn start_editing(
        &mut self,
        coord: CellCoord,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use super::model::{ColumnKind, VisualRowSource};

        let column_kind = self
            .model
            .columns
            .get(coord.col)
            .map(|c| c.kind)
            .unwrap_or(ColumnKind::Unknown);

        // Translate visual row to source (base or pending insert)
        let visual_order = self.edit_buffer.compute_visual_order();
        let null_cell = super::model::CellValue::null();

        let row_source = visual_order.get(coord.row).copied();

        // Check if editing is allowed for this row
        let can_edit = match row_source {
            Some(VisualRowSource::Base(_)) => self.is_editable,
            Some(VisualRowSource::Insert(_)) => self.is_insertable || self.is_editable,
            None => false,
        };

        if !can_edit {
            return false;
        }

        let (initial_value, needs_modal, is_json_cell, is_unsupported_cell) = match row_source {
            Some(VisualRowSource::Base(base_idx)) => {
                let base_cell = self.model.cell(base_idx, coord.col);
                let base = base_cell.unwrap_or(&null_cell);
                let cell = self.edit_buffer.get_cell(base_idx, coord.col, base);

                (
                    cell.edit_text(),
                    cell.needs_modal_editor(),
                    cell.is_json(),
                    cell.is_unsupported(),
                )
            }
            Some(VisualRowSource::Insert(insert_idx)) => {
                if let Some(insert_data) = self.edit_buffer.get_pending_insert_by_idx(insert_idx) {
                    if coord.col < insert_data.len() {
                        let cell = &insert_data[coord.col];
                        (
                            cell.edit_text(),
                            cell.needs_modal_editor(),
                            cell.is_json(),
                            cell.is_unsupported(),
                        )
                    } else {
                        (String::new(), false, false, false)
                    }
                } else {
                    (String::new(), false, false, false)
                }
            }
            None => return false,
        };

        if is_unsupported_cell {
            return false;
        }

        let is_json = column_kind == ColumnKind::Json || is_json_cell;
        if is_json || needs_modal {
            cx.emit(DataTableEvent::ModalEditRequested {
                row: coord.row,
                col: coord.col,
                value: initial_value,
                is_json,
            });
            return true;
        }

        // Enum/set columns: use a dropdown instead of text input
        if let Some(options) = self.enum_options.get(&coord.col).cloned() {
            let items: Vec<DropdownItem> = options
                .iter()
                .map(|v| {
                    if v == Self::NULL_SENTINEL {
                        DropdownItem::with_value("NULL", Self::NULL_SENTINEL)
                    } else {
                        DropdownItem::new(v.clone())
                    }
                })
                .collect();

            let selected_index = if initial_value.is_empty() {
                options.iter().position(|v| v == Self::NULL_SENTINEL)
            } else {
                options.iter().position(|v| v == &initial_value)
            };

            let dropdown = cx.new(|_cx| {
                Dropdown::new(("enum-edit", coord.row * 10000 + coord.col))
                    .items(items)
                    .selected_index(selected_index)
            });

            dropdown.update(cx, |dd, cx| dd.open(cx));

            cx.subscribe(
                &dropdown,
                |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                    let value = event.item.value.to_string();
                    this.apply_enum_selection(&value, cx);
                },
            )
            .detach();

            cx.subscribe(
                &dropdown,
                |this, _dropdown, _event: &DropdownDismissed, cx| {
                    this.cancel_enum_edit(cx);
                },
            )
            .detach();

            self.editing_cell = Some(coord);
            self.enum_dropdown = Some(dropdown);
            self.cell_input = None;
            self.scroll_to_cell(coord.row, coord.col);
            cx.notify();
            return true;
        }

        let input = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_value(&initial_value, window, cx);
            state
        });

        input.update(cx, |state, cx| {
            state.focus(window, cx);
        });

        cx.subscribe(&input, |this, _input, event: &InputEvent, cx| match event {
            InputEvent::PressEnter { .. } => this.stop_editing(true, cx),
            InputEvent::Blur => this.stop_editing(false, cx),
            _ => {}
        })
        .detach();

        self.editing_cell = Some(coord);
        self.cell_input = Some(input);
        self.enum_dropdown = None;
        self.scroll_to_cell(coord.row, coord.col);
        cx.notify();
        true
    }

    /// Get the cell input state if currently editing.
    pub fn cell_input(&self) -> Option<&Entity<InputState>> {
        self.cell_input.as_ref()
    }

    pub fn enum_dropdown(&self) -> Option<&Entity<Dropdown>> {
        self.enum_dropdown.as_ref()
    }

    fn apply_enum_selection(&mut self, value: &str, cx: &mut Context<Self>) {
        use super::model::VisualRowSource;

        let coord = match self.editing_cell.take() {
            Some(c) => c,
            None => return,
        };

        self.enum_dropdown = None;

        let visual_order = self.edit_buffer.compute_visual_order();

        let cell_value = if value == Self::NULL_SENTINEL {
            super::model::CellValue::null()
        } else {
            super::model::CellValue::text(value)
        };

        match visual_order.get(coord.row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                self.edit_buffer.set_cell(base_idx, coord.col, cell_value);
            }
            Some(VisualRowSource::Insert(insert_idx)) => {
                self.edit_buffer
                    .set_insert_cell(insert_idx, coord.col, cell_value);
            }
            None => {}
        }

        cx.notify();
    }

    fn cancel_enum_edit(&mut self, cx: &mut Context<Self>) {
        self.editing_cell = None;
        self.enum_dropdown = None;
        cx.notify();
    }

    pub fn is_editing_enum(&self) -> bool {
        self.enum_dropdown.is_some()
    }

    pub fn enum_dropdown_next(&mut self, cx: &mut Context<Self>) {
        if let Some(dropdown) = &self.enum_dropdown {
            dropdown.update(cx, |dd, cx| dd.select_next_item(cx));
        }
    }

    pub fn enum_dropdown_prev(&mut self, cx: &mut Context<Self>) {
        if let Some(dropdown) = &self.enum_dropdown {
            dropdown.update(cx, |dd, cx| dd.select_prev_item(cx));
        }
    }

    pub fn enum_dropdown_accept(&mut self, cx: &mut Context<Self>) {
        if let Some(dropdown) = &self.enum_dropdown {
            dropdown.update(cx, |dd, cx| dd.accept_selection(cx));
        }
    }

    pub fn enum_dropdown_cancel(&mut self, cx: &mut Context<Self>) {
        self.cancel_enum_edit(cx);
    }

    /// Stop editing and optionally apply the change.
    /// Note: The stored `editing_cell` uses visual row indices.
    pub fn stop_editing(&mut self, apply: bool, cx: &mut Context<Self>) {
        use super::model::VisualRowSource;

        let coord = match self.editing_cell.take() {
            Some(c) => c,
            None => return,
        };

        self.enum_dropdown = None;

        if apply {
            if let Some(input) = self.cell_input.take() {
                let value_str = input.read(cx).value().to_string();

                // Translate visual row to source
                let visual_order = self.edit_buffer.compute_visual_order();

                match visual_order.get(coord.row).copied() {
                    Some(VisualRowSource::Base(base_idx)) => {
                        let original = self
                            .model
                            .cell(base_idx, coord.col)
                            .map(|c| c.display_text().to_string())
                            .unwrap_or_default();

                        if value_str != original {
                            let cell_value = super::model::CellValue::text(&value_str);
                            self.edit_buffer.set_cell(base_idx, coord.col, cell_value);
                        }
                    }
                    Some(VisualRowSource::Insert(insert_idx)) => {
                        // Apply to pending insert (with undo support)
                        let cell_value = super::model::CellValue::text(&value_str);
                        self.edit_buffer
                            .set_insert_cell(insert_idx, coord.col, cell_value);
                    }
                    None => {}
                }
            }
        } else {
            self.cell_input = None;
        }

        cx.notify();
    }

    /// Cancel editing without applying changes.
    #[allow(dead_code)]
    pub fn cancel_editing(&mut self, cx: &mut Context<Self>) {
        if self.editing_cell.is_some() {
            self.editing_cell = None;
            cx.notify();
        }
    }

    /// Get the edit buffer.
    pub fn edit_buffer(&self) -> &EditBuffer {
        &self.edit_buffer
    }

    /// Get mutable access to the edit buffer.
    pub fn edit_buffer_mut(&mut self) -> &mut EditBuffer {
        &mut self.edit_buffer
    }

    /// Check if there are any pending changes.
    #[allow(dead_code)]
    pub fn has_pending_changes(&self) -> bool {
        self.edit_buffer.has_changes()
    }

    /// Request saving the current row's changes.
    /// Emits SaveRowRequested for base row edits, CommitInsertRequested for pending inserts,
    /// CommitDeleteRequested for rows marked for deletion.
    pub fn request_save_row(&mut self, cx: &mut Context<Self>) {
        use super::model::VisualRowSource;

        if let Some(coord) = self.selection.active {
            let visual_order = self.edit_buffer.compute_visual_order();
            match visual_order.get(coord.row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    let row_state = self.edit_buffer.row_state(base_idx);
                    if row_state.is_pending_delete() {
                        cx.emit(DataTableEvent::CommitDeleteRequested(base_idx));
                        return;
                    }
                    if row_state.is_dirty() {
                        cx.emit(DataTableEvent::SaveRowRequested(base_idx));
                        return;
                    }
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    cx.emit(DataTableEvent::CommitInsertRequested(insert_idx));
                    return;
                }
                None => {}
            }
        }

        if let Some(row_idx) = self.edit_buffer.pending_delete_rows().into_iter().next() {
            cx.emit(DataTableEvent::CommitDeleteRequested(row_idx));
            return;
        }

        if let Some(row_idx) = self.edit_buffer.dirty_rows().into_iter().next() {
            cx.emit(DataTableEvent::SaveRowRequested(row_idx));
        }
    }

    /// Revert all changes for a specific row.
    #[allow(dead_code)]
    pub fn revert_row(&mut self, row: usize, cx: &mut Context<Self>) {
        self.edit_buffer.clear_row(row);
        cx.notify();
    }

    /// Revert all pending changes.
    pub fn revert_all(&mut self, cx: &mut Context<Self>) {
        self.edit_buffer.clear_all();
        cx.notify();
    }

    /// Update a row with values returned from the database (e.g., after RETURNING clause).
    ///
    /// This applies server-side computed values (defaults, triggers) to the model.
    pub fn apply_returning_row(&mut self, row_idx: usize, values: &[dbflux_core::Value]) {
        self.model = Arc::new(self.model.with_row_updated(row_idx, values));
    }
}

fn next_sort_state(current: Option<SortState>, col_ix: usize) -> Option<SortState> {
    match current {
        Some(SortState {
            column_ix,
            direction,
        }) if column_ix == col_ix => {
            use dbflux_core::SortDirection::*;
            match direction {
                Ascending => Some(SortState::descending(col_ix)),
                Descending => None,
            }
        }
        _ => Some(SortState::ascending(col_ix)),
    }
}

#[cfg(test)]
mod tests {
    use super::next_sort_state;
    use crate::ui::components::data_table::events::SortState;

    #[test]
    fn next_sort_state_cycles_none_asc_desc_none() {
        let column = 3;

        let step1 = next_sort_state(None, column);
        assert_eq!(step1, Some(SortState::ascending(column)));

        let step2 = next_sort_state(step1, column);
        assert_eq!(step2, Some(SortState::descending(column)));

        let step3 = next_sort_state(step2, column);
        assert_eq!(step3, None);
    }

    #[test]
    fn next_sort_state_switches_to_new_column_ascending() {
        let current = Some(SortState::descending(1));
        let next = next_sort_state(current, 5);
        assert_eq!(next, Some(SortState::ascending(5)));
    }
}

impl EventEmitter<DataTableEvent> for DataTableState {}

impl Focusable for DataTableState {
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
