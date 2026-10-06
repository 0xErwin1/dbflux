//! Saving `SpreadsheetDocument`, and its part in closing and quitting.
//!
//! A save gathers the pending edits of every sheet into one
//! [`SheetEdits`], checks every typed value against the input rules, and
//! writes the workbook through [`crate::file_save::save_file`] with a stage
//! writer that patches the opened package in place: `patch_xlsx` for xlsx
//! and xlsm, `patch_ods` for ods. Every part the edits do not touch is copied
//! byte for byte. The file is checked to be the version that was opened
//! before anything is written, and a value the patcher refuses refuses the
//! whole save: the file is untouched and every pending edit stays.
//!
//! Once the file holds the edits it is read again, showing the sheet that
//! was shown, and the pending edits are dropped. The table is read-only and
//! the sheet tabs take no switch while a save runs.
//!
//! An object is patched into a temporary file and uploaded through the live
//! connection of its profile, after its version is checked twice (see
//! [`crate::file_save::save_file`]). An object downloaded whole is patched
//! from that copy, which is the version checked, and is downloaded again to
//! be read after the save. Every save of an object that reaches the storage
//! layer is recorded in the audit log, and the opener is told of each one
//! that replaced the object.

use std::io::BufWriter;

use dbflux_core::observability::EventSeverity;
use dbflux_spreadsheet::{SheetEdits, SheetWriteError, SpreadsheetFormat, patch_ods, patch_xlsx};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::document::{
    OpenError, OpenedWorkbook, SpreadsheetDocument, open_error_to_user_facing, read_workbook,
};
use super::input::{CellRefusal, InputRefusal};
use crate::dedup::FileDocumentKey;
use crate::file_edit_lifecycle::{local_file_saves_now, quit_disposition};
use crate::file_save::{SaveOutcome, StageWriteError, save_file};
use crate::file_source::{LocationSource, ObjectReads, StorageError, WriteFailure};
use crate::handle::DocumentEvent;
use crate::object_text::record_save_audit;
use crate::pane::QuitDisposition;

/// What a background save hands to the foreground.
enum SaveResult {
    /// The file holds the edits. `reopened` is the workbook read again with
    /// the shown sheet, and `None` for a save that does not read it again.
    Saved {
        outcome: SaveOutcome,
        reopened: Option<Box<Result<OpenedWorkbook, OpenError>>>,
    },

    /// Nothing replaced the file.
    Refused(StorageError),
}

/// Where an object's save is recorded in the audit log, as the object editor
/// and the CSV document record their saves.
struct SaveAudit {
    service: dbflux_audit::AuditService,
    profile_id: uuid::Uuid,
    bucket: String,
    key: String,
}

impl SaveAudit {
    /// Records one save that reached the storage layer: a success when the
    /// object holds the edits, a failure with the reason otherwise.
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

/// A typed value the input rules refuse, and the sheet it is on.
struct EditRefusal {
    sheet: String,
    refusal: CellRefusal,
}

impl SpreadsheetDocument {
    /// Whether the pending edits can be saved now: there are some, and
    /// neither a save nor a sheet read runs.
    pub(super) fn can_save(&self) -> bool {
        self.is_dirty() && !self.saving && !self.is_reading_sheet()
    }

    /// Saves the pending edits of every sheet to the file.
    ///
    /// Ignored for xls and while a save runs: that save reports its own
    /// outcome. A document without edits writes nothing and reports a save
    /// that succeeded. Every other outcome is reported once:
    ///
    /// - The file holds the edits: it is read again, showing the same sheet,
    ///   and the document is clean. When it cannot be read again, the edits
    ///   are saved and the document shows the failure.
    /// - A sheet is being read, a typed value breaks the input rules, the
    ///   patcher refuses a cell, the file changed since it was opened, or
    ///   the profile of an object is not connected: nothing is written and
    ///   every pending edit stays.
    ///
    /// An object's save is recorded in the audit log once it reaches the
    /// storage layer, a success or a failure, even when the tab is closed
    /// while it runs.
    pub fn save(&mut self, cx: &mut Context<Self>) {
        self.start_save(true, cx);
    }

    /// [`Self::save`], reading the saved file again only when `reopen` is
    /// set. Without it the pending edits are dropped once the file holds
    /// them: only the shutdown flush saves that way.
    fn start_save(&mut self, reopen: bool, cx: &mut Context<Self>) {
        if self.saving || !self.is_editable_format() {
            return;
        }

        self.commit_inline_edit(cx);

        if !self.is_dirty() {
            self.report_save_outcome(true, cx);
            return;
        }

        let title = self.title();

        if self.is_reading_sheet() {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    crate::labels::spreadsheet_save_failed_message(&title),
                )
                .with_cause(dbflux_i18n::t!(
                    "document.spreadsheet.error.save_while_reading"
                )),
                cx,
            );
            self.report_save_outcome(false, cx);
            return;
        }

        let edits = match self.collect_edits(cx) {
            Ok(edits) => edits,

            Err(refusal) => {
                report_error(input_refusal_error(&title, &refusal), cx);
                self.report_save_outcome(false, cx);
                return;
            }
        };

        let summary = crate::labels::spreadsheet_save_failed_message(&title);

        if self.use_live_connection(summary, cx).is_err() {
            self.report_save_outcome(false, cx);
            return;
        }

        self.spawn_save(edits, reopen, cx);
    }

    /// The pending edits of every sheet as one [`SheetEdits`]: the edits kept
    /// for the sheets that are not shown and those of the shown sheet. The
    /// first typed value the input rules refuse, in sheet order, refuses
    /// them all.
    fn collect_edits(&self, cx: &App) -> Result<SheetEdits, EditRefusal> {
        let mut edits = SheetEdits::new();

        let Some(loaded) = self.loaded() else {
            return Ok(edits);
        };

        let shown = loaded
            .active_sheet
            .zip(self.shown_changes(cx))
            .filter(|(index, _)| !loaded.stored_edits.contains_key(index));

        let stored = loaded
            .stored_edits
            .iter()
            .map(|(index, stored)| (*index, stored.changes.clone()));

        let mut sheets: Vec<_> = stored.chain(shown).collect();
        sheets.sort_by_key(|(index, _)| *index);

        for (index, changes) in sheets {
            let sheet_name = loaded
                .sheets
                .get(index)
                .map(|sheet| sheet.name.clone())
                .unwrap_or_default();

            let cells = changes.cells.map_err(|refusal| EditRefusal {
                sheet: sheet_name,
                refusal,
            })?;

            for ((row, column), edit) in cells {
                edits.set(index, row, column, edit);
            }
        }

        Ok(edits)
    }

    /// Marks the document as saving and writes `edits` in the background,
    /// reading the file again after it when `reopen` is set. The result is
    /// recorded in the audit log when the save is audited.
    fn spawn_save(&mut self, edits: SheetEdits, reopen: bool, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded() else {
            self.report_save_outcome(false, cx);
            return;
        };

        let captured = loaded.version.clone();
        let format = loaded.format;
        let shown_sheet = loaded.active_sheet;
        let location = self.location.clone();
        let reads = self.reads;
        let downloaded_copy =
            (reads == Some(ObjectReads::Downloaded)).then(|| loaded.source.clone());
        let audit = self.save_audit(cx);

        self.saving = true;
        self.sync_table_editing(cx);
        cx.notify();

        let task = cx.background_executor().spawn(async move {
            let saved = save_file(&location, &captured, |source, sink| {
                let source = downloaded_copy.as_deref().unwrap_or(source);

                write_patched(source, format, &edits, sink)
            });

            match saved {
                Ok(outcome) => SaveResult::Saved {
                    outcome,
                    reopened: reopen
                        .then(|| Box::new(read_workbook(&location, reads, shown_sheet))),
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

    /// Stores the outcome of a background save. This is the first place a
    /// failure of the save is caught, so it is reported here and only here.
    fn apply_save_result(&mut self, result: SaveResult, cx: &mut Context<Self>) {
        self.saving = false;

        let title = self.title();

        let succeeded = match result {
            SaveResult::Refused(error) => {
                report_error(storage_save_error(&title, error), cx);
                false
            }

            SaveResult::Saved { outcome, reopened } => {
                if let SaveOutcome::SavedVersionUnknown(error) = outcome {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            crate::labels::spreadsheet_save_failed_message(&title),
                        )
                        .with_cause(error.to_string())
                        .with_severity(EventSeverity::Warn),
                        cx,
                    );
                }

                match reopened.map(|reopened| *reopened) {
                    Some(Ok(opened)) => self.install_workbook(opened, cx),

                    Some(Err(error)) => {
                        let summary = crate::labels::spreadsheet_open_failed_message(&title);
                        let cause = error.to_string();

                        report_error(open_error_to_user_facing(&error, summary), cx);

                        self.phase = super::document::SpreadsheetPhase::Failed(cause);
                    }

                    None => self.discard_changes(cx),
                }

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

    /// The table's save key, taken before the table sees it: the table asks
    /// for a save only when the shown sheet has an edit, so a document whose
    /// edits are all on other sheets saves here. A clean document lets the
    /// key through.
    pub(super) fn take_save_key(
        &mut self,
        _: &dbflux_components::components::data_table::actions::SaveRow,
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
    /// file holds the edits, and keeps them otherwise. Returns whether a save
    /// started, which is false for a workbook that is not loaded or has no
    /// writer.
    pub fn save_for_close(&mut self, cx: &mut Context<Self>) -> bool {
        if !self.is_editable_format() {
            return false;
        }

        self.lifecycle.close_after_save();
        self.save(cx);
        true
    }

    // -- Quitting ------------------------------------------------------------

    /// What quitting means for the pending edits: a clean document loses
    /// nothing, a local file whose save would go through now is saved by the
    /// shutdown flush, and everything else asks first: an object always,
    /// because an upload while quitting can fail or be cut short. A local
    /// save goes through when neither a save nor a sheet read runs, every
    /// typed value passes the input rules, and the file passes
    /// [`crate::file_edit_lifecycle::local_file_saves_now`].
    pub fn quit_disposition(&self, cx: &App) -> QuitDisposition {
        quit_disposition(self.is_dirty(), || {
            matches!(self.file(), FileDocumentKey::Local { .. })
                && self.local_save_goes_through_now(cx)
        })
    }

    fn local_save_goes_through_now(&self, cx: &App) -> bool {
        let Some(loaded) = self.loaded() else {
            return false;
        };

        if self.saving || self.is_reading_sheet() || !self.is_editable_format() {
            return false;
        }

        self.collect_edits(cx).is_ok()
            && local_file_saves_now(&self.location, &loaded.version, loaded.source_length)
    }

    /// Saves as part of a quit the user confirmed. The tab stays open.
    /// Returns whether a save started.
    pub fn save_for_quit(&mut self, cx: &mut Context<Self>) -> bool {
        if !self.is_editable_format() {
            return false;
        }

        self.lifecycle.keep_open_after_save();
        self.save(cx);
        true
    }

    /// Drops the pending edits for a quit the user confirmed without saving
    /// them, and keeps the shutdown flush from writing them.
    pub fn discard_for_quit(&mut self, cx: &mut Context<Self>) {
        self.lifecycle.discard_for_quit();
        self.discard_changes(cx);
    }

    /// The document's part of the shutdown flush: a local file with pending
    /// edits is saved once, without reading it again. Returns whether a save
    /// is still running. An object's edits are never uploaded while
    /// quitting: a quit from the window asked about them first. They reach
    /// here only from a quit that could not ask (a terminal signal), and are
    /// dropped with one warning in the log naming the object.
    pub fn flush_for_shutdown(&mut self, cx: &mut Context<Self>) -> bool {
        if self
            .lifecycle
            .start_shutdown_flush(self.is_dirty() && !self.saving)
        {
            match self.file() {
                FileDocumentKey::Local { .. } => self.start_save(false, cx),

                FileDocumentKey::Object { bucket, key, .. } => {
                    let object = format!("{bucket}/{key}");

                    log::warn!(
                        "Unsaved changes to {object} were dropped at shutdown: an object is \
                         not uploaded while quitting"
                    );

                    self.dropped_at_shutdown.push(object);
                }
            }
        }

        self.saving
    }

    /// The summary of the tab's dirty-dot tooltip and the unsaved-changes
    /// dialog, while the document has unsaved edits.
    pub fn change_summary(&self) -> Option<String> {
        self.is_dirty()
            .then(|| crate::labels::spreadsheet_unsaved_summary(&self.title()))
    }

    /// Where an object's save is audited. `None` for a local file.
    fn save_audit(&self, cx: &App) -> Option<SaveAudit> {
        let FileDocumentKey::Object {
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

    /// Tells the opener of an object that a save replaced it. A local file
    /// has no opener to tell.
    fn notify_object_saved(&self, cx: &mut App) {
        let FileDocumentKey::Object { key, .. } = self.file() else {
            return;
        };

        if let Some(on_saved) = self.on_object_saved.clone() {
            on_saved(key, cx);
        }
    }

    /// Reports a finished save to the workspace. Only a successful save the
    /// interrupted-close flow started asks for the tab to close.
    fn report_save_outcome(&mut self, succeeded: bool, cx: &mut Context<Self>) {
        let close_after_save = self.lifecycle.take_close_after_save();

        cx.emit(DocumentEvent::SaveFinished { succeeded });

        if succeeded && close_after_save {
            cx.emit(DocumentEvent::RequestClose);
        }
    }
}

/// Writes the opened package `source` with `edits` applied into `sink`.
fn write_patched(
    source: &LocationSource,
    format: SpreadsheetFormat,
    edits: &SheetEdits,
    sink: &mut BufWriter<&std::fs::File>,
) -> Result<(), StageWriteError> {
    let patched = match format {
        SpreadsheetFormat::Ods => patch_ods(source, edits, &mut *sink),
        SpreadsheetFormat::Xlsx | SpreadsheetFormat::Xlsm | SpreadsheetFormat::Xls => {
            patch_xlsx(source, edits, &mut *sink)
        }
    };

    patched.map_err(|error| match error {
        SheetWriteError::Source(error) => StageWriteError::Storage(StorageError::Read(error)),
        SheetWriteError::Sink(error) => StageWriteError::Sink(error),
        other => StageWriteError::Storage(StorageError::Write(WriteFailure::Spreadsheet(other))),
    })
}

/// The user-facing error of a typed value the input rules refuse.
fn input_refusal_error(file_name: &str, refusal: &EditRefusal) -> UserFacingError {
    let cause = match refusal.refusal.refusal {
        InputRefusal::A1ReferenceInOpenFormula => dbflux_i18n::t!(
            "document.spreadsheet.error.input.a1_reference",
            sheet = refusal.sheet,
            cell = refusal.refusal.cell
        ),
    };

    UserFacingError::new(
        ErrorKind::User,
        crate::labels::spreadsheet_save_failed_message(file_name),
    )
    .with_cause(cause)
}

/// The user-facing error of a save that did not replace the file. A changed
/// file and a refused cell are the user's to resolve; the rest are storage
/// failures.
fn storage_save_error(file_name: &str, error: StorageError) -> UserFacingError {
    let is_users = match &error {
        StorageError::SourceChanged | StorageError::VersionUnverifiable => true,

        StorageError::Write(WriteFailure::Spreadsheet(error)) => !matches!(
            error,
            SheetWriteError::Source(_)
                | SheetWriteError::Sink(_)
                | SheetWriteError::Malformed { .. }
        ),

        _ => false,
    };

    let kind = if is_users {
        ErrorKind::User
    } else {
        ErrorKind::Storage
    };

    UserFacingError::new(
        kind,
        crate::labels::spreadsheet_save_failed_message(file_name),
    )
    .with_cause(error.to_string())
}
