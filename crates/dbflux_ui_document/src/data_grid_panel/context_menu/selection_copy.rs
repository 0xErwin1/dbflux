use super::DataGridPanel;
use dbflux_components::components::data_table::model::{
    CellKind, CellValue, EditBuffer, VisualRowSource,
};
use dbflux_core::{QueryResult, Value};
use gpui::App;
use std::ops::RangeInclusive;

impl DataGridPanel {
    /// The table view's rows as the grid shows them, for the export menu's
    /// copy rows. A selection of more than one cell yields just those rows
    /// and columns. A single selected cell is only the cursor, so the copy
    /// takes every loaded row; that yields `None` when nothing is pending,
    /// since the loaded result already is what the grid shows. Rows come in
    /// grid order with pending inserts and unsaved edits applied.
    pub(super) fn copied_cells_result(&self, cx: &App) -> Option<QueryResult> {
        if self.view_config.mode != crate::DataViewMode::Table {
            return None;
        }

        let table_state = self.grid_table.table_state.as_ref()?.read(cx);
        let last_column = self.result.columns.len().checked_sub(1)?;
        let buffer = table_state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let (rows, columns) = match table_state.selection().selected_range() {
            Some(range) if range.start != range.end => (
                range.start.row..=range.end.row,
                range.start.col..=range.end.col.min(last_column),
            ),
            _ if table_state.has_pending_operations() => {
                (0..=visual_order.len().checked_sub(1)?, 0..=last_column)
            }
            _ => return None,
        };

        let column_metas = self.result.columns.get(columns.clone())?.to_vec();
        let rows = rows
            .filter_map(|visual_row| visual_order.get(visual_row).copied())
            .filter_map(|source| self.shown_row_values(source, columns.clone(), buffer))
            .collect();

        Some(QueryResult::table(
            column_metas,
            rows,
            None,
            self.result.execution_time,
        ))
    }

    /// One grid row's values over `columns`, with unsaved edits applied.
    fn shown_row_values(
        &self,
        source: VisualRowSource,
        columns: RangeInclusive<usize>,
        buffer: &EditBuffer,
    ) -> Option<Vec<Value>> {
        match source {
            VisualRowSource::Base(row) => {
                let values = self.result.rows.get(row)?;
                let shown = columns
                    .map(|column| {
                        let value = values.get(column).cloned().unwrap_or(Value::Null);
                        if buffer.is_cell_dirty(row, column) {
                            let base = CellValue::from(&value);
                            self.shown_cell_value(buffer.get_cell(row, column, &base))
                        } else {
                            value
                        }
                    })
                    .collect();
                Some(shown)
            }
            VisualRowSource::Insert(index) => {
                let cells = buffer.get_pending_insert_by_idx(index)?;
                let shown = columns
                    .map(|column| {
                        cells
                            .get(column)
                            .map_or(Value::Null, |cell| self.shown_cell_value(cell))
                    })
                    .collect();
                Some(shown)
            }
        }
    }

    /// A pending or edited cell as the copy writes it. JSON stays JSON, and a
    /// binary cell, whose bytes the grid does not hold, is written as the
    /// `<N bytes>` text the grid shows (the TSV copy writes the same);
    /// everything else converts as the row inspector does.
    fn shown_cell_value(&self, cell: &CellValue) -> Value {
        match &cell.kind {
            CellKind::Json(text) => Value::Json(text.to_string()),
            CellKind::Bytes(len) => Value::Text(format!("<{len} bytes>")),
            _ => self.cell_to_value(cell),
        }
    }
}
