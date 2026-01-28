use std::sync::Arc;

use gpui::{
    px, Context, EventEmitter, FocusHandle, Focusable, Pixels, ScrollHandle, Size,
    UniformListScrollHandle,
};

use super::clipboard;
use super::events::{DataTableEvent, Direction, Edge, SortState};
use super::model::TableModel;
use super::selection::{CellCoord, SelectionState};
use super::theme::{DEFAULT_COLUMN_WIDTH, SCROLLBAR_WIDTH};

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

    /// Currently hovered cell.
    hovered_cell: Option<CellCoord>,

    /// Currently hovered header column.
    hovered_header: Option<usize>,

    /// Focus handle for keyboard input.
    focus_handle: FocusHandle,

    /// Scroll handle for vertical scrolling (uniform list).
    vertical_scroll_handle: UniformListScrollHandle,

    /// Scroll handle for horizontal scrolling.
    horizontal_scroll_handle: ScrollHandle,

    /// Cached horizontal scroll offset for header and body positioning.
    /// Updated when scroll handle offset changes to trigger re-renders.
    horizontal_offset: Pixels,
}

impl DataTableState {
    pub fn new(model: Arc<TableModel>, cx: &mut Context<Self>) -> Self {
        let col_count = model.col_count();
        let column_widths = vec![DEFAULT_COLUMN_WIDTH; col_count];
        let column_offsets = Self::calculate_offsets(&column_widths);

        Self {
            model,
            column_widths,
            column_offsets,
            sort: None,
            viewport_size: Size::default(),
            selection: SelectionState::new(),
            hovered_cell: None,
            hovered_header: None,
            focus_handle: cx.focus_handle(),
            vertical_scroll_handle: UniformListScrollHandle::new(),
            horizontal_scroll_handle: ScrollHandle::new(),
            horizontal_offset: px(0.0),
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
        self.model.row_count()
    }

    pub fn col_count(&self) -> usize {
        self.model.col_count()
    }

    // --- Column Layout ---

    pub fn column_widths(&self) -> &[f32] {
        &self.column_widths
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
        let new_sort = match self.sort {
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
        };

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

        let current = self.selection.active.unwrap_or(CellCoord::new(0, 0));
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

        self.scroll_to_row(new_coord.row);
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

        self.scroll_to_row(new_coord.row);
    }

    // --- Hover ---

    pub fn hovered_cell(&self) -> Option<CellCoord> {
        self.hovered_cell
    }

    pub fn set_hovered_cell(&mut self, cell: Option<CellCoord>, cx: &mut Context<Self>) {
        if self.hovered_cell != cell {
            self.hovered_cell = cell;
            cx.notify();
        }
    }

    pub fn hovered_header(&self) -> Option<usize> {
        self.hovered_header
    }

    pub fn set_hovered_header(&mut self, col: Option<usize>, cx: &mut Context<Self>) {
        if self.hovered_header != col {
            self.hovered_header = col;
            cx.notify();
        }
    }

    // --- Clipboard ---

    pub fn copy_selection(&self) -> Option<String> {
        clipboard::copy_selection(&self.model, &self.selection)
    }

    // --- Focus ---

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
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

        let diff = self.horizontal_offset - clamped_offset;
        if diff > px(0.5) || diff < px(-0.5) {
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
}

impl EventEmitter<DataTableEvent> for DataTableState {}

impl Focusable for DataTableState {
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
