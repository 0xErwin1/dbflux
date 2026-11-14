//! Editing of `SpreadsheetDocument`.
//!
//! Cells are edited in the table, which identifies rows by position and
//! stages every typed value as text; a save turns those values into the
//! patcher's edits (see the `input` module). Rows can only be added at the
//! end of a sheet: a row inserted or deleted inside a sheet would move the
//! cells that formulas, charts, merged ranges and tables point at, and the
//! patchers do not rewrite those references. So appending a row adds an
//! empty row after the last one, deleting removes only a row appended and
//! not saved yet, and every other row operation is refused with a message.
//!
//! The document is dirty while the shown table holds a pending edit or
//! another sheet has edits kept from when it was shown. xls has no writer,
//! so its table stays read-only.

use dbflux_components::components::data_table::model::{CellValue, InsertAnchor, VisualRowSource};
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::components::data_table::{DataTableEvent, DataTableState};
use dbflux_components::icons::AppIcon;
use dbflux_spreadsheet::SpreadsheetFormat;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::document::{SheetPhase, SpreadsheetDocument, StoredSheetEdits};
use super::input::{FormulaSyntax, SheetChanges, sheet_changes};
use crate::handle::DocumentEvent;
use crate::pane::PaneAction;

impl SpreadsheetDocument {
    /// Whether any sheet has unsaved edits.
    pub fn is_dirty(&self) -> bool {
        self.loaded().is_some_and(|loaded| loaded.is_dirty)
    }

    /// Whether a save runs.
    pub fn is_saving(&self) -> bool {
        self.saving
    }

    /// Whether the cells of the shown sheet can be edited now: the format
    /// has a writer and no save runs.
    pub(super) fn can_edit(&self) -> bool {
        self.is_editable_format() && !self.saving
    }

    /// Whether a row can be appended now: the cells can be edited and a
    /// sheet with a table is shown whose file row for appended rows is known.
    pub(super) fn can_append_row(&self) -> bool {
        self.can_edit()
            && self
                .shown_sheet()
                .is_some_and(|shown| shown.model.append_row().is_some())
    }

    /// The formula syntax of the workbook. `None` for xls, which has no
    /// writer.
    pub(super) fn formula_syntax(&self) -> Option<FormulaSyntax> {
        match self.loaded()?.format {
            SpreadsheetFormat::Xlsx | SpreadsheetFormat::Xlsm => Some(FormulaSyntax::Excel),
            SpreadsheetFormat::Ods => Some(FormulaSyntax::OpenFormula),
            SpreadsheetFormat::Xls => None,
        }
    }

    /// Works out the dirty state again and tells the tab when it changed.
    pub(super) fn refresh_dirty(&mut self, cx: &mut Context<Self>) {
        let shown_has_edits = self
            .shown_sheet()
            .is_some_and(|shown| shown.table_state.read(cx).has_pending_operations());

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        let is_dirty = shown_has_edits || !loaded.stored_edits.is_empty();

        if loaded.is_dirty != is_dirty {
            loaded.is_dirty = is_dirty;

            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    /// Makes the table editable or read-only as [`Self::can_edit`] says.
    pub(super) fn sync_table_editing(&mut self, cx: &mut Context<Self>) {
        let can_edit = self.can_edit();

        let Some(table_state) = self.shown_sheet().map(|shown| shown.table_state.clone()) else {
            return;
        };

        table_state.update(cx, |state, cx| {
            if state.is_positional_editing() == can_edit && state.is_insertable() == can_edit {
                return;
            }

            state.set_positional_editing(can_edit);
            state.set_insertable(can_edit);
            cx.notify();
        });
    }

    /// Commits the value typed into an open inline editor, as Enter does but
    /// without taking focus back, so a tab close or a shutdown sees it as a
    /// pending change. Always returns `true`: the document has no dialog
    /// whose value could be refused.
    pub fn commit_pending_input(&mut self, cx: &mut Context<Self>) -> bool {
        if let Some(table_state) = self.shown_sheet().map(|shown| shown.table_state.clone()) {
            table_state.update(cx, |state, cx| state.commit_pending_edit(cx));
        }

        self.refresh_dirty(cx);
        true
    }

    /// Stages the value of an open inline editor, as Enter does.
    pub(super) fn commit_inline_edit(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = self.shown_sheet().map(|shown| shown.table_state.clone()) else {
            return;
        };

        table_state.update(cx, |state, cx| {
            if state.is_editing() {
                state.stop_editing(true, cx);
            }
        });

        self.refresh_dirty(cx);
    }

    /// Carries out the row operations and the saves the table asks for.
    pub(super) fn handle_table_event(&mut self, event: &DataTableEvent, cx: &mut Context<Self>) {
        match event {
            DataTableEvent::AddRowRequested(_) => self.append_row(cx),
            DataTableEvent::DuplicateRowRequested(row) => self.duplicate_row(*row, cx),
            DataTableEvent::DeleteRowRequested(row) => self.delete_row(*row, cx),

            DataTableEvent::SetNullRequested { row, col } => self.clear_cell(*row, *col, cx),

            DataTableEvent::SaveAllRequested { .. }
            | DataTableEvent::SaveRowRequested(_)
            | DataTableEvent::CommitInsertRequested(_)
            | DataTableEvent::CommitDeleteRequested(_) => self.save(cx),

            _ => {}
        }
    }

    // -- Rows ----------------------------------------------------------------

    /// The table, while its cells can be edited.
    fn editable_table(&self) -> Option<Entity<DataTableState>> {
        if !self.can_edit() {
            return None;
        }

        self.shown_sheet().map(|shown| shown.table_state.clone())
    }

    /// Adds an empty row after the last row of the sheet, and puts the
    /// cursor on its first cell. Ignored whenever [`Self::can_append_row`]
    /// says no.
    pub fn append_row(&mut self, cx: &mut Context<Self>) {
        if !self.can_append_row() {
            return;
        }

        let Some(table_state) = self.editable_table() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let row = vec![CellValue::text(""); state.col_count()];

            state
                .edit_buffer_mut()
                .add_pending_insert_at(InsertAnchor::End, row);

            let last_row = state.row_count().saturating_sub(1);
            state.select_cell(CellCoord::new(last_row, 0), cx);
            state.scroll_to_row(last_row);
            cx.notify();
        });
    }

    /// Appends a copy of the appended row shown at `visual_row`. A sheet row
    /// is refused: its copy would hold the values its formulas show.
    fn duplicate_row(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.editable_table() else {
            return;
        };

        let source = table_state
            .read(cx)
            .edit_buffer()
            .visual_row_source(visual_row);

        let Some(VisualRowSource::Insert(index)) = source else {
            self.refuse_row_change(cx);
            return;
        };

        table_state.update(cx, |state, cx| {
            let Some(row) = state
                .edit_buffer()
                .get_pending_insert_by_idx(index)
                .cloned()
            else {
                return;
            };

            state
                .edit_buffer_mut()
                .add_pending_insert_at(InsertAnchor::End, row);
            cx.notify();
        });
    }

    /// Removes the appended row shown at `visual_row`. A sheet row is
    /// refused.
    fn delete_row(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.editable_table() else {
            return;
        };

        let source = table_state
            .read(cx)
            .edit_buffer()
            .visual_row_source(visual_row);

        let Some(VisualRowSource::Insert(index)) = source else {
            self.refuse_row_change(cx);
            return;
        };

        table_state.update(cx, |state, cx| {
            state.edit_buffer_mut().remove_pending_insert_by_idx(index);
            cx.notify();
        });
    }

    /// Stages an empty value for a cell, which clears it: a sheet has no
    /// null.
    fn clear_cell(&mut self, visual_row: usize, column: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.editable_table() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            state.stage_cell_value(visual_row, column, CellValue::text(""));
            cx.notify();
        });
    }

    /// Tells the user that rows can only be added at the end of a sheet.
    fn refuse_row_change(&self, cx: &mut Context<Self>) {
        report_error(
            UserFacingError::new(
                ErrorKind::User,
                dbflux_i18n::t!("document.spreadsheet.error.rows_only_at_end"),
            ),
            cx,
        );
    }

    /// Drops every pending edit of every sheet and the undo history of the
    /// shown one. An open inline editor is closed without staging its value.
    /// Does nothing while a save runs.
    pub fn discard_changes(&mut self, cx: &mut Context<Self>) {
        if self.saving {
            return;
        }

        if let Some(loaded) = self.loaded_mut() {
            loaded.stored_edits.clear();
        }

        if let Some(table_state) = self.shown_sheet().map(|shown| shown.table_state.clone()) {
            table_state.update(cx, |state, cx| {
                if state.is_editing() {
                    state.stop_editing(false, cx);
                }

                let base_row_count = state.base_row_count();
                state.edit_buffer_mut().reset_for_base(base_row_count);
                cx.notify();
            });
        }

        self.refresh_dirty(cx);
        cx.notify();
    }

    // -- Sheet switches ------------------------------------------------------

    /// The edits of the shown sheet, turned into the patcher's edits. `None`
    /// when no table is shown or the format has no writer.
    pub(super) fn shown_changes(&self, cx: &App) -> Option<SheetChanges> {
        let syntax = self.formula_syntax()?;
        let shown = self.shown_sheet()?;
        let state = shown.table_state.read(cx);

        Some(sheet_changes(&shown.model, state.edit_buffer(), syntax))
    }

    /// Keeps the pending edits of the shown sheet before it is dropped, both
    /// as the table holds them and as the patcher takes them. A value still
    /// in the inline editor is committed first.
    pub(super) fn store_shown_edits(&mut self, cx: &mut Context<Self>) {
        self.commit_inline_edit(cx);

        let Some(table_state) = self.shown_sheet().map(|shown| shown.table_state.clone()) else {
            return;
        };

        if !table_state.read(cx).has_pending_operations() {
            return;
        }

        let Some(changes) = self.shown_changes(cx) else {
            return;
        };

        let table_edits = table_state.read(cx).snapshot_pending_edits();

        if let Some(loaded) = self.loaded_mut()
            && let Some(index) = loaded.active_sheet
        {
            loaded.stored_edits.insert(
                index,
                StoredSheetEdits {
                    table_edits,
                    changes,
                },
            );
        }
    }

    /// Lays the edits kept for the selected sheet back onto its table, now
    /// that it is shown again.
    pub(super) fn restore_stored_edits(&mut self, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        let Some(index) = loaded.active_sheet else {
            return;
        };

        let SheetPhase::Shown(shown) = &loaded.sheet else {
            return;
        };

        let table_state = shown.table_state.clone();

        let Some(stored) = loaded.stored_edits.remove(&index) else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let dropped_rows = state.restore_pending_edits(stored.table_edits, cx);

            if dropped_rows > 0 {
                log::warn!("{dropped_rows} edited rows could not be restored after a sheet switch");
            }
        });
    }

    /// Whether the sheet at `index` has pending edits.
    pub(super) fn sheet_has_pending_edits(&self, index: usize, cx: &App) -> bool {
        let Some(loaded) = self.loaded() else {
            return false;
        };

        if loaded.stored_edits.contains_key(&index) {
            return true;
        }

        loaded.active_sheet == Some(index)
            && self
                .shown_sheet()
                .is_some_and(|shown| shown.table_state.read(cx).has_pending_operations())
    }

    /// How many formula cells the pending edits of every sheet replace with
    /// a value. A formula replaced by another formula does not count.
    pub(super) fn formula_replacement_count(&self, cx: &App) -> usize {
        let Some(loaded) = self.loaded() else {
            return 0;
        };

        let stored: usize = loaded
            .stored_edits
            .values()
            .map(|stored| stored.changes.replaced_formulas)
            .sum();

        let shown = self
            .shown_changes(cx)
            .map_or(0, |changes| changes.replaced_formulas);

        stored + shown
    }

    /// The append-row entry of the pane actions menu, for a workbook whose
    /// format has a writer, and Save as .xlsx for xls.
    pub(super) fn pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        let target = this.downgrade();

        if self.offers_save_as() {
            return vec![
                PaneAction::callback(
                    "spreadsheet-save-as",
                    dbflux_i18n::t!("document.spreadsheet.save_as.action"),
                    move |_window, cx| {
                        if let Some(document) = target.upgrade() {
                            document.update(cx, |document, cx| document.save_as_xlsx(cx));
                        }
                    },
                )
                .icon(AppIcon::Save)
                .enabled(self.can_save_as_xlsx()),
            ];
        }

        if !self.is_editable_format() {
            return Vec::new();
        }

        vec![
            PaneAction::callback(
                "spreadsheet-append-row",
                dbflux_i18n::t!("document.spreadsheet.action.append_row"),
                move |_window, cx| {
                    if let Some(document) = target.upgrade() {
                        document.update(cx, |document, cx| document.append_row(cx));
                    }
                },
            )
            .icon(AppIcon::Plus)
            .enabled(self.can_append_row()),
        ]
    }
}
