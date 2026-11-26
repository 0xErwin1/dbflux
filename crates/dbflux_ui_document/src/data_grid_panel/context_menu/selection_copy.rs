use super::DataGridPanel;
use dbflux_components::components::data_table::model::{CellValue, EditBuffer, VisualRowSource};
use dbflux_core::{QueryResult, Value};
use gpui::App;
use std::ops::RangeInclusive;

impl DataGridPanel {
    /// The table view's selected cells as a result of their own, so the
    /// export menu's copy rows can copy just them. A selection of one cell
    /// is only the cursor and yields `None`: the copy then takes the whole
    /// loaded result. Rows come in grid order and as the grid shows them,
    /// pending inserts and unsaved edits included.
    pub(super) fn selected_cells_result(&self, cx: &App) -> Option<QueryResult> {
        if self.view_config.mode != crate::DataViewMode::Table {
            return None;
        }

        let table_state = self.grid_table.table_state.as_ref()?.read(cx);
        let range = table_state.selection().selected_range()?;
        if range.start == range.end {
            return None;
        }

        let last_column = range.end.col.min(self.result.columns.len().checked_sub(1)?);
        let columns = range.start.col..=last_column;
        let column_metas = self.result.columns.get(columns.clone())?.to_vec();

        let buffer = table_state.edit_buffer();
        let visual_order = buffer.compute_visual_order();
        let rows = (range.start.row..=range.end.row)
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
                            self.cell_to_value(buffer.get_cell(row, column, &base))
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
                            .map_or(Value::Null, |cell| self.cell_to_value(cell))
                    })
                    .collect();
                Some(shown)
            }
        }
    }
}
