//! Editing and saving of `DelimitedDocument`.
//!
//! The table identifies rows by position. Cell edits, set null, undo and redo
//! run inside the table on its edit buffer. Adding, inserting, duplicating
//! and deleting a row are requested by the table and carried out here on the
//! same buffer, because the anchor of an inserted row is the document's to
//! choose. A file has no null, so set null stages an empty field.
//!
//! A cell over the table's inline limit, or holding a line break, is edited
//! in the modal cell editor the data grid uses, as plain text. Its value is
//! staged as the inline editor stages one, and only while the rows can be
//! edited: the table asks for no editor while they cannot, and a value saved
//! from an editor that was open when the table became read-only is refused
//! and reported.
//!
//! The document is dirty while the table holds a pending edit or the page
//! model holds a column change. A staged edit emits no event, so the dirty
//! state is worked out again whenever the table notifies. The table's save
//! key knows only row edits, so the document takes it first whenever it is
//! dirty, which also saves a change that touches only the columns.
//!
//! A save builds the edit set from the page model and the edit buffer,
//! writes it through the storage layer on the background executor and, once
//! the file holds the edited bytes, reads again as many pages as were loaded
//! with the dialect in effect. One save runs at a time, none runs while the
//! file is read again under another dialect, and the table, undo and redo
//! included, is read-only while a save runs, because the reopened file
//! replaces every row and an edit made meanwhile would be dropped. A save
//! that does not replace the file keeps every pending edit so it can be
//! retried.

use std::sync::Arc;

use dbflux_components::components::data_table::actions::SaveRow;
use dbflux_components::components::data_table::model::{
    CellValue, EditBuffer, InsertAnchor, VisualRowSource,
};
use dbflux_components::components::data_table::{DataTableEvent, DataTableState, ModelSwap};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{CellEditorClosedEvent, CellEditorModal, CellEditorSaveEvent};
use dbflux_core::observability::EventSeverity;
use dbflux_delimited::EditSet;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::document::{
    DelimitedDocument, DelimitedPhase, OpenError, OpenedFile, open_error_to_user_facing,
    pages_to_read_again, reread_pages,
};
use super::page_model::PageModelError;
use super::save::{SaveOutcome, save_edited};
use super::source::StorageError;
use crate::dedup::DelimitedFileKey;
use crate::handle::DocumentEvent;
use crate::object_text::record_save_audit;
use crate::pane::PaneAction;

/// A cell the table asked the modal editor for: its position as the table
/// shows it and the value the cell holds now, edits included.
pub(super) struct CellEditRequest {
    row: usize,
    col: usize,
    value: String,
}

/// The row of the table a cell of the modal editor belongs to, kept by its
/// identity rather than its position, which an inserted or removed row
/// shifts.
#[derive(Debug, Clone, PartialEq)]
enum CellEditRow {
    /// A loaded record, by its index among the records.
    Base(usize),

    /// A pending insert, by its index in the edit buffer, with its anchor
    /// and the text of its cells when the editor opened. A removed insert
    /// shifts the indices after it, so the row is found again only while the
    /// insert at that index is still the one the editor was opened on.
    Insert {
        index: usize,
        anchor: InsertAnchor,
        cells: Vec<String>,
    },
}

/// The cell the open modal editor edits: the visual row it was opened at,
/// which its save event names, the row's identity, the column, and the
/// reader and discard counts of the rows it was opened on.
pub(super) struct CellEditTarget {
    opened_at: usize,
    row: CellEditRow,
    col: usize,
    reader_epoch: u64,
    discard_generation: u64,
}

/// Why a value saved from the modal cell editor was not staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CellEditRefusal {
    /// The file was read again, after a save or a reload, and its rows were
    /// replaced.
    RowsReadAgain,

    /// The pending changes were discarded.
    ChangesDiscarded,

    /// A save or a reread runs, so the rows cannot be edited now.
    ReadOnly,

    /// The row the cell belongs to was removed.
    RowRemoved,
}

impl CellEditRefusal {
    /// What the user is told about the refusal.
    pub(super) fn cause(self) -> String {
        match self {
            Self::RowsReadAgain => {
                dbflux_i18n::t!("document.delimited.error.edit_rows_read_again")
            }
            Self::ChangesDiscarded => {
                dbflux_i18n::t!("document.delimited.error.edit_changes_discarded")
            }
            Self::ReadOnly => dbflux_i18n::t!("document.delimited.error.edit_while_read_only"),
            Self::RowRemoved => dbflux_i18n::t!("document.delimited.error.edit_row_removed"),
        }
    }
}

/// What a background save hands to the foreground.
enum SaveResult {
    /// The file holds the edited bytes. `reopened` is its first page, read
    /// again with the dialect in effect, boxed because a reader is large.
    Saved {
        outcome: SaveOutcome,
        reopened: Box<Result<OpenedFile, OpenError>>,
    },

    /// Nothing replaced the file.
    Refused(StorageError),
}

/// Where an object's save is recorded in the audit log, as the object editor
/// records its saves.
struct SaveAudit {
    service: dbflux_audit::AuditService,
    profile_id: uuid::Uuid,
    bucket: String,
    key: String,
}

impl SaveAudit {
    /// Records one save that reached the storage layer: a success when the
    /// object holds the edited bytes, a failure with the reason otherwise.
    fn record(&self, result: &SaveResult) {
        let error = match result {
            SaveResult::Saved { .. } => None,
            SaveResult::Refused(error) => Some(error.to_string()),
        };

        record_save_audit(
            &self.service,
            self.profile_id,
            &self.bucket,
            &self.key,
            error.as_deref(),
        );
    }
}

impl DelimitedDocument {
    /// Whether the document has unsaved changes: a pending edit in the table,
    /// a column change in the page model, or an edit of the text view's text
    /// that was not applied yet.
    pub fn is_dirty(&self) -> bool {
        self.loaded().is_some_and(|loaded| loaded.is_dirty)
    }

    /// Whether a save runs.
    pub fn is_saving(&self) -> bool {
        self.saving
    }

    /// Whether the rows of the file can be edited now: it is loaded, it can
    /// be saved in place, and neither a reread nor a save runs.
    pub(super) fn can_edit(&self) -> bool {
        self.loaded().is_some_and(|loaded| {
            loaded.can_save_in_place() && !loaded.is_rereading() && !self.saving
        })
    }

    /// Whether the document shows its edit controls: the file is loaded and
    /// can be saved in place.
    pub(super) fn shows_edit_controls(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| loaded.can_save_in_place())
    }

    /// Whether the pending changes can be saved now: there are some, and
    /// neither a save, a reread nor the load of the rest of the file runs.
    /// This is every state in which [`Self::save`] writes.
    pub(super) fn can_save(&self) -> bool {
        self.is_dirty() && !self.saving && !self.is_rereading() && !self.is_loading_rest()
    }

    /// Whether the pending changes can be discarded now: there are some,
    /// neither a save nor a reread runs, and no dialog is open.
    pub(super) fn can_discard(&self) -> bool {
        self.is_dirty() && !self.saving && !self.is_rereading() && !self.has_open_dialog()
    }

    /// Whether a dialog of the document is open or asked for: the modal cell
    /// editor, the column prompt or the offer to load the rest of the file.
    /// What a dialog does belongs to the rows and columns as they were when
    /// it opened, so nothing that changes them runs meanwhile.
    pub(super) fn has_open_dialog(&self) -> bool {
        self.cell_edit_target.is_some()
            || self.pending_cell_edit.is_some()
            || self.column_prompt.is_some()
            || self.pending_column_prompt.is_some()
            || self.load_rest_prompt.is_some()
    }

    /// Whether the rows or the columns can be changed now: they can be
    /// edited, no dialog is open, and the table is shown. Inserting a row,
    /// adding a column and renaming one are the table's: in the text view
    /// the user edits the text instead.
    pub(super) fn can_change_rows(&self) -> bool {
        self.can_edit()
            && !self.has_open_dialog()
            && self.view() == super::text_view::DelimitedView::Table
    }

    /// Works out the dirty state again and tells the tab when it changed.
    pub(super) fn refresh_dirty(&mut self, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded() else {
            return;
        };

        let is_dirty = loaded.table_state.read(cx).has_pending_operations()
            || loaded.page_model.has_column_changes()
            || self.text.edited;

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        if loaded.is_dirty != is_dirty {
            loaded.is_dirty = is_dirty;

            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    /// Stages the value of an open inline editor, as Enter does, and works
    /// out the dirty state again, so a check that follows sees it.
    pub(super) fn commit_active_inline_edit(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = self.loaded().map(|loaded| loaded.table_state.clone()) else {
            return;
        };

        table_state.update(cx, |state, cx| {
            if state.is_editing() {
                state.stop_editing(true, cx);
            }
        });

        self.refresh_dirty(cx);
    }

    /// Makes the table editable or read-only as [`Self::can_edit`] says.
    pub(super) fn sync_table_editing(&mut self, cx: &mut Context<Self>) {
        let can_edit = self.can_edit();

        let Some(table_state) = self.loaded().map(|loaded| loaded.table_state.clone()) else {
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

    /// The table, while its rows can be edited.
    fn editable_table(&self) -> Option<Entity<DataTableState>> {
        if !self.can_edit() {
            return None;
        }

        self.loaded().map(|loaded| loaded.table_state.clone())
    }

    /// The table, while its rows can be changed: inserted, duplicated,
    /// deleted or cleared.
    fn table_for_row_change(&self) -> Option<Entity<DataTableState>> {
        if !self.can_change_rows() {
            return None;
        }

        self.editable_table()
    }

    /// The table, while a row can be added to it. A table without columns
    /// gets none: the row would have no field, which the writer renders as
    /// an empty quoted field and the saved file reads back as no record.
    /// That refusal is reported.
    fn table_taking_a_row(&self, cx: &mut Context<Self>) -> Option<Entity<DataTableState>> {
        let table_state = self.table_for_row_change()?;

        if table_state.read(cx).col_count() == 0 {
            let summary = crate::labels::delimited_add_row_failed_message(&self.title());
            let cause = dbflux_i18n::t!("document.delimited.error.no_columns");

            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(cause),
                cx,
            );

            return None;
        }

        Some(table_state)
    }

    /// Carries out a row operation or a save the table asks for, opens the
    /// modal editor for a cell over the table's inline limit or holding a
    /// line break, and asks for a new name for a column whose header was
    /// right-clicked.
    pub(super) fn handle_table_event(&mut self, event: &DataTableEvent, cx: &mut Context<Self>) {
        match event {
            DataTableEvent::ModalEditRequested {
                row, col, value, ..
            } => self.request_cell_editor(*row, *col, value.clone(), cx),

            DataTableEvent::ContextMenuRequested {
                col,
                is_column_header: true,
                ..
            } => self.request_rename_column(*col, cx),

            DataTableEvent::AddRowRequested(row) => self.add_row_below(*row, cx),
            DataTableEvent::DuplicateRowRequested(row) => self.duplicate_row(*row, cx),
            DataTableEvent::DeleteRowRequested(row) => self.delete_row(*row, cx),

            DataTableEvent::SetNullRequested { row, col } => {
                self.clear_cell(*row, *col, cx);
            }

            DataTableEvent::SaveAllRequested { .. }
            | DataTableEvent::SaveRowRequested(_)
            | DataTableEvent::CommitInsertRequested(_)
            | DataTableEvent::CommitDeleteRequested(_) => self.save(cx),

            _ => {}
        }
    }

    // -- The modal cell editor ------------------------------------------------

    /// The modal cell editor, once a cell was opened in it.
    pub fn cell_editor(&self) -> Option<&Entity<CellEditorModal>> {
        self.cell_editor.as_ref()
    }

    /// Opens the modal editor on the cell shown at `visual_row`, `col`,
    /// holding `value`, on the next render. Ignored while the rows cannot be
    /// edited.
    fn request_cell_editor(
        &mut self,
        visual_row: usize,
        col: usize,
        value: String,
        cx: &mut Context<Self>,
    ) {
        if !self.can_edit() {
            return;
        }

        self.pending_cell_edit = Some(CellEditRequest {
            row: visual_row,
            col,
            value,
        });
        cx.notify();
    }

    /// Opens the cell asked for since the last render in the modal editor,
    /// in its plain-text mode: a delimited field is text. Does nothing when
    /// the rows cannot be edited any more.
    pub(super) fn open_pending_cell_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(request) = self.pending_cell_edit.take() else {
            return;
        };

        if !self.can_edit() {
            return;
        }

        let Some(loaded) = self.loaded() else {
            return;
        };

        let row = cell_edit_row(loaded.table_state.read(cx).edit_buffer(), request.row);

        let Some(row) = row else {
            return;
        };

        let target = CellEditTarget {
            opened_at: request.row,
            row,
            col: request.col,
            reader_epoch: loaded.reader_epoch,
            discard_generation: loaded.discard_generation,
        };

        let editor = self.cell_editor_entity(window, cx);
        self.cell_edit_target = Some(target);

        editor.update(cx, |editor, cx| {
            editor.open(
                request.row,
                request.col,
                request.value,
                false,
                None,
                window,
                cx,
            );
        });
    }

    /// The modal cell editor, built and subscribed to the first time.
    fn cell_editor_entity(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<CellEditorModal> {
        if let Some(editor) = &self.cell_editor {
            return editor.clone();
        }

        let editor = cx.new(|cx| CellEditorModal::new(window, cx));

        cx.subscribe_in(
            &editor,
            window,
            |this, _, event: &CellEditorSaveEvent, _window, cx| {
                this.apply_cell_editor_value(event, cx);
            },
        )
        .detach();

        cx.subscribe_in(
            &editor,
            window,
            |this, _, _: &CellEditorClosedEvent, window, cx| {
                this.cell_edit_target = None;
                this.focus(window, cx);
            },
        )
        .detach();

        self.cell_editor = Some(editor.clone());
        editor
    }

    /// Stages the value the modal editor saved for its cell, as the inline
    /// editor stages one: a value equal to the record's own is no edit. The
    /// row is found again by its identity, so a row inserted or removed
    /// elsewhere meanwhile does not move the value to another row.
    ///
    /// Refused and reported, with its own reason, when the rows were read
    /// again or the changes discarded since the editor opened, when a save or
    /// a reread runs, or when the row is gone. Nothing is staged then.
    fn apply_cell_editor_value(&mut self, event: &CellEditorSaveEvent, cx: &mut Context<Self>) {
        let Some(target) = self.cell_edit_target.take() else {
            return;
        };

        if (target.opened_at, target.col) != (event.row, event.col) {
            return;
        }

        let visual_row = match self.resolve_cell_edit(&target, cx) {
            Ok(visual_row) => visual_row,

            Err(refusal) => {
                let summary = crate::labels::delimited_edit_failed_message(&self.title());

                report_error(
                    UserFacingError::new(ErrorKind::User, summary).with_cause(refusal.cause()),
                    cx,
                );
                return;
            }
        };

        let Some(table_state) = self.editable_table() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            state.stage_cell_value(visual_row, target.col, CellValue::text(&event.value));
            cx.notify();
        });
    }

    /// The visual row the cell of `target` is shown at now, or why its value
    /// cannot be staged.
    pub(super) fn resolve_cell_edit(
        &self,
        target: &CellEditTarget,
        cx: &App,
    ) -> Result<usize, CellEditRefusal> {
        let Some(loaded) = self.loaded() else {
            return Err(CellEditRefusal::RowsReadAgain);
        };

        if loaded.reader_epoch != target.reader_epoch {
            return Err(CellEditRefusal::RowsReadAgain);
        }

        if loaded.discard_generation != target.discard_generation {
            return Err(CellEditRefusal::ChangesDiscarded);
        }

        if !self.can_edit() {
            return Err(CellEditRefusal::ReadOnly);
        }

        let buffer = loaded.table_state.read(cx).edit_buffer();

        visual_row_of(buffer, &target.row).ok_or(CellEditRefusal::RowRemoved)
    }

    // -- Rows ----------------------------------------------------------------

    /// Adds an empty row below the row shown at `visual_row`. Refused for a
    /// table without columns.
    fn add_row_below(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.table_taking_a_row(cx) else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let anchor = anchor_below(state.edit_buffer(), visual_row);
            let row = empty_row(state.col_count());

            state.edit_buffer_mut().add_pending_insert_at(anchor, row);
            cx.notify();
        });
    }

    /// Adds an empty row above the row the cursor is on, or above the first
    /// row when there is no cursor. Refused for a table without columns.
    ///
    /// Above a row that is itself a pending insert, the new row goes next to
    /// it, below it: the edit buffer orders the rows of one anchor by when
    /// they were added.
    pub fn insert_row_above(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = self.table_taking_a_row(cx) else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let visual_row = state.selection().active.map_or(0, |cell| cell.row);
            let anchor = anchor_above(state.edit_buffer(), visual_row);
            let row = empty_row(state.col_count());

            state.edit_buffer_mut().add_pending_insert_at(anchor, row);
            cx.notify();
        });
    }

    /// Adds a copy of the row shown at `visual_row` below it, with the edits
    /// staged in it.
    fn duplicate_row(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.table_for_row_change() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let Some((anchor, row)) = duplicate_of(state, visual_row) else {
                return;
            };

            state.edit_buffer_mut().add_pending_insert_at(anchor, row);
            cx.notify();
        });
    }

    /// Marks the record shown at `visual_row` for deletion, or drops the row
    /// when it is a pending insert.
    fn delete_row(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.table_for_row_change() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();

            match buffer.compute_visual_order().get(visual_row).copied() {
                Some(VisualRowSource::Base(row)) => buffer.mark_for_delete(row),

                Some(VisualRowSource::Insert(insert)) => {
                    buffer.remove_pending_insert_by_idx(insert);
                }

                None => return,
            }

            cx.notify();
        });
    }

    /// Stages an empty field for a cell: a delimited file has no null.
    fn clear_cell(&mut self, visual_row: usize, col: usize, cx: &mut Context<Self>) {
        let Some(table_state) = self.table_for_row_change() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            state.stage_cell_value(visual_row, col, CellValue::text(""));
            cx.notify();
        });
    }

    /// Drops every pending edit, every column change and the undo history.
    /// An open inline editor is closed without staging its value. An edit of
    /// the text view's text is dropped too, without being read, so text that
    /// does not parse never blocks a discard, and the text is rendered again
    /// from the clean state. Does nothing while a save runs or a dialog is
    /// open.
    pub fn discard_changes(&mut self, cx: &mut Context<Self>) {
        if self.saving || self.has_open_dialog() {
            return;
        }

        self.drop_text_edit(cx);

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        let had_column_changes = loaded.page_model.has_column_changes();
        loaded.page_model.discard_column_changes();
        loaded.discard_generation += 1;

        // The columns of the table follow the page model, so a column change
        // needs a new table model. Replacing the model drops the edits too.
        let model = had_column_changes.then(|| Arc::new(loaded.page_model.table_model()));
        let table_state = loaded.table_state.clone();

        table_state.update(cx, |state, cx| {
            if state.is_editing() {
                state.stop_editing(false, cx);
            }

            match model {
                Some(model) => state.set_model(model, ModelSwap::KeepCursor, cx),

                None => {
                    let base_row_count = state.base_row_count();
                    state.edit_buffer_mut().reset_for_base(base_row_count);
                }
            }

            cx.notify();
        });

        self.refresh_dirty(cx);
        cx.notify();
    }

    /// The page model, for tests that make column changes the document has
    /// no control for yet.
    #[cfg(test)]
    pub(super) fn page_model_mut_for_test(&mut self) -> Option<&mut super::page_model::PageModel> {
        self.loaded_mut().map(|loaded| &mut loaded.page_model)
    }

    /// The edit controls as entries of the pane actions menu: insert a row
    /// above the cursor, enabled while the rows can be edited, and discard,
    /// enabled while there are changes to drop, for a file that can be saved
    /// in place; then reload, for every loaded file. Reload stays enabled
    /// while there are changes, and says why it is refused then.
    pub(super) fn edit_pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        if self.loaded().is_none() {
            return Vec::new();
        }

        let run =
            |id: &'static str,
             label: String,
             run: fn(&mut DelimitedDocument, &mut Context<DelimitedDocument>)| {
                let target = this.downgrade();

                PaneAction::callback(id, label, move |_window, cx| {
                    if let Some(document) = target.upgrade() {
                        document.update(cx, run);
                    }
                })
            };

        let mut actions = Vec::new();

        if self.shows_edit_controls() {
            actions.push(
                run(
                    "delimited-insert-above",
                    dbflux_i18n::t!("document.delimited.action.insert_above"),
                    Self::insert_row_above,
                )
                .icon(AppIcon::Plus)
                .enabled(self.can_change_rows()),
            );
            actions.extend(self.column_pane_actions(this));
            actions.push(
                run(
                    "delimited-discard",
                    dbflux_i18n::t!("document.delimited.action.discard"),
                    Self::discard_changes,
                )
                .icon(AppIcon::RotateCcw)
                .enabled(self.can_discard()),
            );
        }

        actions.extend(self.load_rest_pane_actions(this));

        actions.push(
            run(
                "delimited-reload",
                dbflux_i18n::t!("document.delimited.action.reload"),
                Self::reload,
            )
            .icon(AppIcon::RefreshCcw)
            .enabled(!self.has_open_dialog()),
        );

        actions
    }

    // -- Saving --------------------------------------------------------------

    /// Saves the pending changes to the file.
    ///
    /// Ignored while a save runs: that save reports its own outcome. A
    /// document without changes writes nothing and reports a save that
    /// succeeded. Refused and reported while the file is read again under
    /// another dialect, and while the rest of the file is being loaded. A
    /// value typed in an open inline editor is committed first, and an edit
    /// of the text view's text is applied first: a text that cannot be
    /// applied is reported and nothing is written.
    ///
    /// Every outcome is reported once:
    ///
    /// - The file holds the edited bytes: it is read again from its first
    ///   pages, as many as were loaded (up to
    ///   [`super::document::MAX_PAGES_READ_AGAIN`]), with the dialect in
    ///   effect, every pending edit is dropped, the active cell stays where
    ///   the pages read again still have it, and the document is clean. When
    ///   the new version of the file could not be read, that is reported as a
    ///   warning. When the file cannot be read again, the edits are saved,
    ///   the document shows the failure, and that one failure is reported.
    /// - The file changed since it was opened, the file cannot be saved in
    ///   place, or the write failed: the file is untouched and every pending
    ///   edit is kept so the save can be retried.
    /// - A row was inserted after the last loaded record, or a column added
    ///   before the whole file was loaded: nothing is written, and the user is
    ///   told what to load.
    ///
    /// An object is written through the live connection of its profile, and
    /// its save is recorded in the audit log, a success or a failure, even
    /// when the tab is closed while the save runs. A local file's save is not
    /// recorded. A tab closed while its save runs drops the outcome without a
    /// report, and the save itself completes.
    pub fn save(&mut self, cx: &mut Context<Self>) {
        if self.saving {
            return;
        }

        if !self.apply_text(cx) {
            self.report_save_outcome(false, cx);
            return;
        }

        let Some(table_state) = self.loaded().map(|loaded| loaded.table_state.clone()) else {
            self.report_save_outcome(false, cx);
            return;
        };

        table_state.update(cx, |state, cx| {
            if state.is_editing() {
                state.stop_editing(true, cx);
            }
        });
        self.refresh_dirty(cx);

        if !self.is_dirty() {
            self.report_save_outcome(true, cx);
            return;
        }

        let title = self.title();

        // The records the load reads would be dropped by the reopen after
        // the save, and the column prompt it opens would land on a file
        // that changed under it.
        if self.is_loading_rest() {
            let summary = crate::labels::delimited_save_failed_message(&title);
            let cause = dbflux_i18n::t!("document.delimited.error.save_while_loading_rest");

            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(cause),
                cx,
            );
            self.report_save_outcome(false, cx);
            return;
        }

        // The edits belong to rows the running reread replaces, and the
        // reopen after the save would race it.
        if self.is_rereading() {
            let summary = crate::labels::delimited_save_failed_message(&title);
            let cause = dbflux_i18n::t!("document.delimited.error.save_while_rereading");

            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(cause),
                cx,
            );
            self.report_save_outcome(false, cx);
            return;
        }

        let Some(edit_set) = self.edit_set(cx) else {
            self.report_save_outcome(false, cx);
            return;
        };

        let edit_set = match edit_set {
            Ok(edit_set) => edit_set,

            Err(error) => {
                report_error(page_model_save_error(&title, &error), cx);
                self.report_save_outcome(false, cx);
                return;
            }
        };

        let summary = crate::labels::delimited_save_failed_message(&title);

        if self.use_live_connection(summary, cx).is_err() {
            self.report_save_outcome(false, cx);
            return;
        }

        let Some(loaded) = self.loaded() else {
            self.report_save_outcome(false, cx);
            return;
        };

        let captured = loaded.version.clone();
        let dialect = loaded.dialect;
        let pages = pages_to_read_again(&loaded.page_model);
        let location = self.location.clone();
        let reader_options = self.reader_options;
        let audit = self.save_audit(cx);

        self.saving = true;
        self.sync_table_editing(cx);
        cx.notify();

        let task = cx.background_executor().spawn(async move {
            match save_edited(
                &location,
                &captured,
                &dialect,
                &edit_set,
                reader_options.window_size,
            ) {
                Ok(outcome) => SaveResult::Saved {
                    outcome,
                    reopened: Box::new(reread_pages(&location, dialect, reader_options, pages)),
                },

                Err(error) => SaveResult::Refused(error),
            }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            if let Some(audit) = &audit {
                audit.record(&result);
            }

            cx.update(|cx| {
                this.update(cx, |document, cx| document.apply_save_result(result, cx))
                    .ok();
            });
        })
        .detach();
    }

    /// The table's save key, taken before the table sees it. The table asks
    /// for a save only when it holds a row edit, so a dirty document saves
    /// here, which covers a change to the columns alone, and the table never
    /// hears the key. A clean document lets the key through to the table,
    /// which has nothing to save.
    pub(super) fn take_save_key(
        &mut self,
        _: &SaveRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.is_dirty() {
            return;
        }

        cx.stop_propagation();
        self.save(cx);
    }

    /// Saves as part of an interrupted close: the tab closes only once the
    /// file holds the edits, and keeps them otherwise. A save refused while
    /// the rest of the file loads, or because the text view holds text that
    /// cannot be applied, reports a failed save, so the tab stays open with
    /// its edits. Returns whether a save
    /// started, which is false only for a file that is not loaded.
    ///
    /// Asking again while a save runs is intentional: that save then reports
    /// to the close the second request asked for.
    pub fn save_for_close(&mut self, cx: &mut Context<Self>) -> bool {
        if self.loaded().is_none() {
            return false;
        }

        self.close_after_save = true;
        self.save(cx);
        true
    }

    /// The edit set of the pending changes. `None` when the file is not
    /// loaded.
    fn edit_set(&self, cx: &App) -> Option<Result<EditSet, PageModelError>> {
        let loaded = self.loaded()?;
        let edits = loaded.table_state.read(cx).edit_buffer();

        Some(loaded.page_model.edit_set(loaded.source_length, edits))
    }

    /// Where an object's save is audited. `None` for a local file.
    fn save_audit(&self, cx: &App) -> Option<SaveAudit> {
        let DelimitedFileKey::Object {
            profile_id,
            bucket,
            key,
        } = self.file()
        else {
            return None;
        };

        let service = self.app_state.as_ref()?.read(cx).audit_service().clone();

        Some(SaveAudit {
            service,
            profile_id: *profile_id,
            bucket: bucket.clone(),
            key: key.clone(),
        })
    }

    /// Stores the outcome of a background save. This is the first place a
    /// failure of the save is caught, so it is reported here and only here.
    /// A document that was closed meanwhile never gets here.
    fn apply_save_result(&mut self, result: SaveResult, cx: &mut Context<Self>) {
        self.saving = false;

        let title = self.title();

        let succeeded = match result {
            SaveResult::Refused(error) => {
                report_error(storage_save_error(&title, error), cx);
                false
            }

            SaveResult::Saved { outcome, reopened } => {
                // A reopen that failed is reported on its own, and its
                // message already says the file was saved.
                if let SaveOutcome::SavedVersionUnknown(error) = outcome
                    && reopened.is_ok()
                {
                    let summary = crate::labels::delimited_saved_version_unknown_message(&title);

                    report_error(
                        open_error_to_user_facing(&OpenError::Storage(error), summary)
                            .with_severity(EventSeverity::Warn),
                        cx,
                    );
                }

                self.show_saved_file(*reopened, cx);
                self.notify_object_saved(cx);
                true
            }
        };

        self.sync_table_editing(cx);
        self.refresh_dirty(cx);

        cx.emit(DocumentEvent::MetaChanged);
        self.report_save_outcome(succeeded, cx);
        cx.notify();
    }

    /// Shows the file as it was read again after a save. A file that could
    /// not be read again holds the edits all the same, and the document shows
    /// why it cannot show them.
    fn show_saved_file(&mut self, reopened: Result<OpenedFile, OpenError>, cx: &mut Context<Self>) {
        match reopened {
            Ok(saved) => {
                if let Some(loaded) = self.loaded_mut() {
                    loaded.show_saved(saved, cx);
                }
            }

            Err(error) => {
                let summary = crate::labels::delimited_reopen_failed_message(&self.title());
                let cause = error.to_string();

                report_error(open_error_to_user_facing(&error, summary), cx);

                self.phase = DelimitedPhase::Failed(cause);
            }
        }
    }

    /// Tells the opener of an object that a save replaced it. A local file
    /// has no opener to tell.
    fn notify_object_saved(&self, cx: &mut App) {
        let DelimitedFileKey::Object { key, .. } = self.file() else {
            return;
        };

        if let Some(on_saved) = self.on_object_saved.clone() {
            on_saved(key, cx);
        }
    }

    /// Reports a finished save to the workspace.
    ///
    /// Only a save the interrupted-close flow started, and only one that
    /// succeeded, asks for the tab to close. Every other outcome drops that
    /// intent, so a later save cannot close a tab the user kept.
    fn report_save_outcome(&mut self, succeeded: bool, cx: &mut Context<Self>) {
        let close_after_save = std::mem::take(&mut self.close_after_save);

        cx.emit(DocumentEvent::SaveFinished { succeeded });

        if succeeded && close_after_save {
            cx.emit(DocumentEvent::RequestClose);
        }
    }
}

/// Shows the columns and rows of `page_model` in the table of `state`, with
/// every pending edit and its undo history kept: every row keeps its index
/// and every column its position, and a pending insert gets an empty field
/// for each new column. Replacing the rows closes an open inline editor, so
/// its value is committed first instead of being lost.
pub(super) fn install_page_model(
    state: &mut DataTableState,
    page_model: &super::page_model::PageModel,
    cx: &mut Context<DataTableState>,
) {
    let model = Arc::new(page_model.table_model());
    let row_count = model.row_count();
    let column_count = model.col_count();

    if state.is_editing() {
        state.stop_editing(true, cx);
    }

    let pending = state.edit_buffer().clone();

    state.set_model(model, ModelSwap::KeepCursor, cx);

    *state.edit_buffer_mut() = pending;
    state.edit_buffer_mut().set_base_row_count(row_count);
    pad_pending_inserts(state.edit_buffer_mut(), column_count);
    cx.notify();
}

/// Gives every pending insert of `buffer` an empty field for each of the
/// `column_count` columns it has no field for. The edit buffer drops a value
/// typed into a field an inserted row does not have.
pub(super) fn pad_pending_inserts(buffer: &mut EditBuffer, column_count: usize) {
    for insert in 0..buffer.pending_inserts().len() {
        if let Some(cells) = buffer.get_pending_insert_mut_by_idx(insert)
            && cells.len() < column_count
        {
            cells.resize(column_count, CellValue::text(""));
        }
    }
}

/// The identity of the row shown at `visual_row`. `None` when no row is
/// shown there.
fn cell_edit_row(buffer: &EditBuffer, visual_row: usize) -> Option<CellEditRow> {
    match buffer.compute_visual_order().get(visual_row).copied()? {
        VisualRowSource::Base(row) => Some(CellEditRow::Base(row)),

        VisualRowSource::Insert(index) => {
            let insert = buffer.pending_inserts().get(index)?;

            Some(CellEditRow::Insert {
                index,
                anchor: insert.anchor,
                cells: insert.data.iter().map(CellValue::edit_text).collect(),
            })
        }
    }
}

/// The visual row `row` is shown at now. `None` when it is gone: a record
/// past the loaded ones, or an insert that was removed.
fn visual_row_of(buffer: &EditBuffer, row: &CellEditRow) -> Option<usize> {
    let source = match row {
        CellEditRow::Base(row) => VisualRowSource::Base(*row),

        CellEditRow::Insert {
            index,
            anchor,
            cells,
        } => {
            let insert = buffer.pending_inserts().get(*index)?;
            let current: Vec<String> = insert.data.iter().map(CellValue::edit_text).collect();

            if insert.anchor != *anchor || current != *cells {
                return None;
            }

            VisualRowSource::Insert(*index)
        }
    };

    buffer
        .compute_visual_order()
        .iter()
        .position(|shown| *shown == source)
}

/// A row of `column_count` empty fields.
fn empty_row(column_count: usize) -> Vec<CellValue> {
    vec![CellValue::text(""); column_count]
}

/// The anchor of a row added below the row shown at `visual_row`.
///
/// Below a pending insert the new row shares its anchor. With no row there,
/// the new row goes after the last record, and at the end of a table without
/// records.
fn anchor_below(buffer: &EditBuffer, visual_row: usize) -> InsertAnchor {
    match buffer.compute_visual_order().get(visual_row).copied() {
        Some(VisualRowSource::Base(row)) => InsertAnchor::After(row),
        Some(VisualRowSource::Insert(insert)) => insert_anchor(buffer, insert),

        None => match buffer.base_row_count().checked_sub(1) {
            Some(last) => InsertAnchor::After(last),
            None => InsertAnchor::End,
        },
    }
}

/// The anchor of a row inserted above the row shown at `visual_row`: above
/// the first record, or after the record before it.
fn anchor_above(buffer: &EditBuffer, visual_row: usize) -> InsertAnchor {
    match buffer.compute_visual_order().get(visual_row).copied() {
        Some(VisualRowSource::Base(0)) => InsertAnchor::BeforeFirst,
        Some(VisualRowSource::Base(row)) => InsertAnchor::After(row - 1),
        Some(VisualRowSource::Insert(insert)) => insert_anchor(buffer, insert),

        None if buffer.base_row_count() > 0 => InsertAnchor::BeforeFirst,
        None => InsertAnchor::End,
    }
}

fn insert_anchor(buffer: &EditBuffer, insert: usize) -> InsertAnchor {
    buffer
        .pending_inserts()
        .get(insert)
        .map_or(InsertAnchor::End, |pending| pending.anchor)
}

/// The anchor and the cells of a copy of the row shown at `visual_row`,
/// with the edits staged in it. `None` when no row is shown there.
fn duplicate_of(
    state: &DataTableState,
    visual_row: usize,
) -> Option<(InsertAnchor, Vec<CellValue>)> {
    let buffer = state.edit_buffer();
    let column_count = state.col_count();

    match buffer.compute_visual_order().get(visual_row).copied()? {
        VisualRowSource::Base(row) => {
            let absent = CellValue::text("");

            let cells = (0..column_count)
                .map(|column| {
                    let base = state.model().cell(row, column).unwrap_or(&absent);
                    buffer.get_cell(row, column, base).clone()
                })
                .collect();

            Some((InsertAnchor::After(row), cells))
        }

        VisualRowSource::Insert(insert) => {
            let mut cells = buffer.get_pending_insert_by_idx(insert)?.clone();
            cells.resize(column_count, CellValue::text(""));

            Some((insert_anchor(buffer, insert), cells))
        }
    }
}

/// The user-facing error of a save the page model refused: the file named
/// `file_name` was not written, and the user has to load more of it first.
fn page_model_save_error(file_name: &str, error: &PageModelError) -> UserFacingError {
    let summary = crate::labels::delimited_save_failed_message(file_name);

    let cause = match error {
        PageModelError::NextPageRequired => {
            dbflux_i18n::t!("document.delimited.error.next_page_required")
        }

        PageModelError::FullLoadRequired => {
            dbflux_i18n::t!("document.delimited.error.full_load_required")
        }

        other => other.to_string(),
    };

    UserFacingError::new(ErrorKind::User, summary).with_cause(cause)
}

/// The user-facing error of a save that did not replace the file named
/// `file_name`.
fn storage_save_error(file_name: &str, error: StorageError) -> UserFacingError {
    let summary = crate::labels::delimited_save_failed_message(file_name);

    match error {
        StorageError::SourceChanged => UserFacingError::new(ErrorKind::User, summary)
            .with_cause(dbflux_i18n::t!("document.delimited.error.source_changed")),

        StorageError::VersionUnverifiable => UserFacingError::new(ErrorKind::User, summary)
            .with_cause(dbflux_i18n::t!(
                "document.delimited.warning.cannot_save_in_place"
            )),

        other => open_error_to_user_facing(&OpenError::Storage(other), summary),
    }
}
