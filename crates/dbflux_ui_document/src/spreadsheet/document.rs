//! The spreadsheet file tab: a workbook shown one sheet at a time, editable
//! for xlsx, xlsm and ods and read-only for xls.
//!
//! Opening runs on the background executor: the file is opened, the format
//! is recognized from its content, the sheet list is read, and the first
//! visible worksheet is decoded into the table's model. The document shows a
//! loading notice until that arrives and the error when it fails.
//!
//! The reader decodes a whole sheet at once, so the shown sheet is held in
//! memory in full. Only one sheet is resident: switching to another sheet
//! drops the shown one before the next is read, on the background executor.
//! The workbook moves into that read and comes back with the result, and a
//! switch made meanwhile is read once it is back.
//!
//! The pending edits of a sheet leave with it: switching sheets lifts them
//! off the table and keeps them, already turned into the patcher's edits, and
//! lays them back onto the table when the sheet is shown again. Editing and
//! saving are in the `editing` and `save` modules.
//!
//! An object is read through the live connection of its profile. A store
//! that reads byte ranges is read by range, as a local file is. An object of
//! a store that cannot read a byte range is downloaded whole once, after the
//! user agrees in a prompt that names its size, and every sheet is read from
//! that copy. Declining closes the tab.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use dbflux_byte_source::ByteSource;
use dbflux_components::components::data_table::model::KeyedPendingEdits;
use dbflux_components::components::data_table::{DataTable, DataTableEvent, DataTableState};
use dbflux_components::modals::ModalFocus;
use dbflux_core::Connection;
use dbflux_spreadsheet::{SheetInfo, SheetKind, SpreadsheetError, SpreadsheetFormat, Workbook};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use dbflux_ui_base::{AppStateEntity, SaveTargetProvider};
use gpui::*;

use super::grid_model::{BLANK_SHEET_COLUMNS, FormulaReadout, SheetModel, cell_address};
use super::input::SheetChanges;
use super::save_as::{SaveAsPrompt, SavedAsCallback};
use crate::dedup::FileDocumentKey;
use crate::file_edit_lifecycle::FileEditLifecycle;
use crate::file_source::{
    DOWNLOAD_IN_MEMORY_LIMIT_BYTES, FileLocation, LocationSource, ObjectReads, SourceVersion,
    StorageError, download_whole_object, open_source, read_version,
};
use crate::handle::DocumentEvent;
use crate::pane::ObjectSavedCallback;
use crate::types::{DocumentId, DocumentState};

/// The source a workbook reads through. It is shared because the reader
/// reads an xlsx or ods package again to find where appended rows go, and a
/// save of a downloaded object patches the copy it reads.
pub(super) type SheetSource = Arc<LocationSource>;

pub(super) type OpenWorkbook = Workbook<SheetSource>;

/// Why a spreadsheet could not be opened.
#[derive(Debug)]
pub(super) enum OpenError {
    /// The file could not be reached or read.
    Storage(StorageError),

    /// The bytes are not a workbook DBFlux can read.
    Spreadsheet(SpreadsheetError),
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => fmt::Display::fmt(error, formatter),
            Self::Spreadsheet(error) => {
                formatter.write_str(&crate::labels::spreadsheet_error_cause(error, None))
            }
        }
    }
}

impl From<StorageError> for OpenError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<SpreadsheetError> for OpenError {
    fn from(error: SpreadsheetError) -> Self {
        Self::Spreadsheet(error)
    }
}

/// What the background open hands to the foreground.
pub(super) struct OpenedWorkbook {
    workbook: OpenWorkbook,

    /// The bytes the workbook reads.
    source: SheetSource,

    /// The version of the file that was read, which a save checks.
    version: SourceVersion,

    /// The length of that version.
    source_length: u64,

    /// The first sheet shown and what reading it gave. `None` when the
    /// workbook has no worksheet.
    first_sheet: Option<(usize, Result<SheetModel, SpreadsheetError>)>,
}

/// The sheet the document shows, or why it shows none.
pub(super) enum SheetPhase {
    /// The workbook has no worksheet, only chart or other sheets.
    NoWorksheet,
    Reading,
    Failed(String),
    /// The sheet holds no value or formula, and rows cannot be appended to
    /// it: its format has no writer. An empty sheet that can be edited is
    /// shown as a table of blank columns instead.
    Empty,
    Shown(Box<ShownSheet>),
}

/// The resident sheet and the table that shows it.
pub(super) struct ShownSheet {
    pub(super) model: SheetModel,
    pub(super) table_state: Entity<DataTableState>,
    table: Entity<DataTable>,
    _subscription: Subscription,

    /// A staged edit emits no event, so the dirty state is worked out again
    /// whenever the table notifies.
    _observation: Subscription,
}

/// The pending edits of a sheet that is not shown.
pub(super) struct StoredSheetEdits {
    /// The edits as the table held them, laid back onto it when the sheet
    /// is shown again.
    pub(super) table_edits: KeyedPendingEdits,

    /// The same edits as the patcher takes them.
    pub(super) changes: SheetChanges,
}

/// An opened workbook.
pub(super) struct LoadedWorkbook {
    /// The reader. `None` while a sheet is being read: the background read
    /// has it and hands it back.
    pub(super) workbook: Option<OpenWorkbook>,

    /// The bytes the workbook reads: the file, the object read by range, or
    /// the copy of an object downloaded whole.
    pub(super) source: SheetSource,

    pub(super) sheets: Vec<SheetInfo>,

    pub(super) format: SpreadsheetFormat,

    /// The version of the file the workbook was read from.
    pub(super) version: SourceVersion,

    /// The length of that version.
    pub(super) source_length: u64,

    /// The sheet selected in the tabs. `None` when the workbook has no
    /// worksheet.
    pub(super) active_sheet: Option<usize>,

    pub(super) sheet: SheetPhase,

    /// Counts the sheet switches. A read carries the count it was started
    /// at, and its result is dropped when another sheet was chosen meanwhile.
    read_generation: u64,

    /// The pending edits of the sheets that are not shown, by sheet index.
    pub(super) stored_edits: BTreeMap<usize, StoredSheetEdits>,

    /// Whether any sheet has pending edits.
    pub(super) is_dirty: bool,
}

pub(super) enum SpreadsheetPhase {
    Loading,
    /// An object that is only read whole waits for the user to agree to the
    /// download. Nothing of it was read yet.
    AwaitingDownload,
    Failed(String),
    Loaded(Box<LoadedWorkbook>),
}

/// The open question whether to download an object whole.
pub(super) struct DownloadPrompt {
    /// The object's size, as `head_object` reports it.
    size: u64,

    focus: ModalFocus,
}

impl DownloadPrompt {
    pub(super) fn focus_mut(&mut self) -> &mut ModalFocus {
        &mut self.focus
    }
}

/// A spreadsheet file, one sheet at a time.
pub struct SpreadsheetDocument {
    id: DocumentId,
    focus_handle: FocusHandle,
    file: FileDocumentKey,
    pub(super) location: FileLocation,
    pub(super) phase: SpreadsheetPhase,

    /// The application state the live connection of an object's profile is
    /// resolved from, and whose audit log records an object's saves. `None`
    /// for a local file.
    pub(super) app_state: Option<Entity<AppStateEntity>>,

    /// How an object is read. `None` for a local file.
    pub(super) reads: Option<ObjectReads>,

    /// The question whether to download an object whole, while it is open.
    download_prompt: Option<DownloadPrompt>,

    /// Told the object's key after each save that replaced the object, so
    /// the object browser that opened it refreshes its view of that object.
    /// `None` for a local file and for an object opened without one.
    pub(super) on_object_saved: Option<ObjectSavedCallback>,

    /// The object, as `bucket/key`, whose pending edits the shutdown flush
    /// dropped instead of uploading them.
    pub(super) dropped_at_shutdown: Vec<String>,

    /// Set when a sheet arrives, so the next render hands the keyboard to
    /// the table if a notice held it.
    pending_table_focus: bool,

    /// Whether a save runs.
    pub(super) saving: bool,

    /// What the close and quit flows asked of the next save.
    pub(super) lifecycle: FileEditLifecycle,

    /// The prompt that says what Save as .xlsx does not keep, while it is
    /// open.
    pub(super) save_as_prompt: Option<SaveAsPrompt>,

    /// Whether a Save as .xlsx runs, from the file dialog to the write.
    pub(super) saving_as: bool,

    /// Chooses where Save as .xlsx writes instead of the file dialog. `None`
    /// uses the dialog.
    pub(super) save_target_override: Option<SaveTargetProvider>,

    /// Told the path of each file Save as .xlsx wrote, to open it.
    pub(super) on_saved_as: Option<SavedAsCallback>,
}

impl EventEmitter<DocumentEvent> for SpreadsheetDocument {}

impl SpreadsheetDocument {
    /// The `tab_kind` a local file is recorded under in the workspace session.
    pub const SESSION_TAB_KIND: &'static str = "Spreadsheet";

    /// Opens the local file at `path`.
    pub fn open_local(path: std::path::PathBuf, cx: &mut Context<Self>) -> Self {
        let file = FileDocumentKey::Local { path: path.clone() };
        let location = FileLocation::Local { path };

        Self::open(file, location, None, None, cx)
    }

    /// Opens the object `key` of `bucket` through `connection`, the live
    /// connection of the profile `profile_id` in `app_state`. Saves go
    /// through the connection the profile has when they start.
    ///
    /// When the store cannot read a byte range of an object, only the
    /// object's version is read here, and the user is asked whether to
    /// download it whole. Declining closes the tab.
    pub fn open_object(
        app_state: Entity<AppStateEntity>,
        profile_id: uuid::Uuid,
        connection: Arc<dyn Connection>,
        bucket: String,
        key: String,
        cx: &mut Context<Self>,
    ) -> Self {
        let file = FileDocumentKey::Object {
            profile_id,
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let reads = ObjectReads::of(connection.as_ref());
        let location = FileLocation::Object {
            connection,
            bucket,
            key,
        };

        Self::open(file, location, Some(app_state), Some(reads), cx)
    }

    fn open(
        file: FileDocumentKey,
        location: FileLocation,
        app_state: Option<Entity<AppStateEntity>>,
        reads: Option<ObjectReads>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut document = Self {
            id: DocumentId::new(),
            focus_handle: cx.focus_handle(),
            file,
            location,
            phase: SpreadsheetPhase::Loading,
            app_state,
            reads,
            download_prompt: None,
            on_object_saved: None,
            dropped_at_shutdown: Vec::new(),
            pending_table_focus: false,
            saving: false,
            lifecycle: FileEditLifecycle::default(),
            save_as_prompt: None,
            saving_as: false,
            save_target_override: None,
            on_saved_as: None,
        };

        document.start_open(cx);
        document
    }

    /// Sets what is told the object's key after each save that replaces the
    /// object. Only an object's saves call it.
    pub fn set_on_object_saved(&mut self, on_saved: ObjectSavedCallback) {
        self.on_object_saved = Some(on_saved);
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// The identity this file is deduplicated by.
    pub fn file(&self) -> &FileDocumentKey {
        &self.file
    }

    /// The file name: the last component of the path.
    pub fn title(&self) -> String {
        match &self.file {
            FileDocumentKey::Local { path } => path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string()),

            FileDocumentKey::Object { key, .. } => {
                key.rsplit('/').next().unwrap_or(key).to_string()
            }
        }
    }

    pub fn state(&self) -> DocumentState {
        if self.is_dirty() {
            return DocumentState::Modified;
        }

        match &self.phase {
            SpreadsheetPhase::Loading => DocumentState::Loading,
            SpreadsheetPhase::Failed(_) => DocumentState::Error,
            SpreadsheetPhase::AwaitingDownload | SpreadsheetPhase::Loaded(_) => {
                DocumentState::Clean
            }
        }
    }

    /// The profile whose connection reads the file, and `None` for a local
    /// file.
    pub fn connection_id(&self) -> Option<uuid::Uuid> {
        match &self.file {
            FileDocumentKey::Local { .. } => None,
            FileDocumentKey::Object { profile_id, .. } => Some(*profile_id),
        }
    }

    pub fn refresh_policy(&self) -> dbflux_core::RefreshPolicy {
        dbflux_core::RefreshPolicy::Manual
    }

    pub fn set_refresh_policy(
        &mut self,
        _policy: dbflux_core::RefreshPolicy,
        _cx: &mut Context<Self>,
    ) {
        // A file is read when it is opened and has nothing to refresh on a
        // timer.
    }

    pub fn active_context(&self) -> dbflux_app::keymap::ContextId {
        dbflux_app::keymap::ContextId::Results
    }

    /// Table navigation and cell editing run inside the embedded `DataTable`
    /// through its own key context. The commands of the document are the
    /// next and the previous sheet (`NextResultTab` and `PrevResultTab`),
    /// which skip chart sheets and wrap around, appending a row
    /// (`ResultsAddRow`) and saving (`SaveRow`, `SaveQuery`). For xls, which
    /// has no writer, the save commands and `SaveFileAs` open Save as .xlsx.
    pub fn dispatch_command(
        &mut self,
        command: dbflux_app::keymap::Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use dbflux_app::keymap::Command;

        match command {
            Command::NextResultTab if self.loaded().is_some() => {
                self.step_sheet(true, window, cx);
                true
            }

            Command::PrevResultTab if self.loaded().is_some() => {
                self.step_sheet(false, window, cx);
                true
            }

            Command::SaveRow | Command::SaveQuery | Command::SaveFileAs
                if self.offers_save_as() =>
            {
                self.save_as_xlsx(cx);
                true
            }

            Command::ResultsAddRow if self.is_editable_format() => {
                self.append_row(cx);
                true
            }

            Command::SaveRow | Command::SaveQuery if self.is_editable_format() => {
                self.save(cx);
                true
            }

            _ => false,
        }
    }

    /// Gives the keyboard to the Save as .xlsx or the download prompt while
    /// one is open, to the table of the shown sheet, or to the document while
    /// a notice takes its place.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(prompt) = &mut self.save_as_prompt {
            prompt.focus_mut().focus(None, window, cx);
            return;
        }

        if let Some(prompt) = &mut self.download_prompt {
            prompt.focus.focus(None, window, cx);
            return;
        }

        if let Some(shown) = self.shown_sheet() {
            let handle = shown.table_state.read(cx).focus_handle().clone();
            handle.focus(window, cx);
            return;
        }

        self.focus_handle.focus(window, cx);
    }

    /// Why the file, or the sheet selected, could not be read.
    pub fn failure(&self) -> Option<&str> {
        match &self.phase {
            SpreadsheetPhase::Failed(cause) => Some(cause),
            SpreadsheetPhase::Loaded(loaded) => match &loaded.sheet {
                SheetPhase::Failed(cause) => Some(cause),
                _ => None,
            },
            SpreadsheetPhase::Loading | SpreadsheetPhase::AwaitingDownload => None,
        }
    }

    /// How an object is read, and `None` for a local file.
    pub fn object_reads(&self) -> Option<ObjectReads> {
        self.reads
    }

    /// What the open download prompt asks: the object's name, its size, and
    /// that all of it is downloaded once. `None` while no prompt is open.
    pub fn download_prompt_message(&self) -> Option<String> {
        let prompt = self.download_prompt.as_ref()?;

        Some(crate::labels::parquet_download_body(
            &self.title(),
            prompt.size,
        ))
    }

    pub(super) fn download_prompt_mut(&mut self) -> Option<&mut DownloadPrompt> {
        self.download_prompt.as_mut()
    }

    /// Closes the download prompt and downloads the object whole, then shows
    /// its first sheet. Does nothing when no prompt is open, and reports a
    /// profile that is not connected without downloading.
    pub fn confirm_download(&mut self, cx: &mut Context<Self>) {
        let Some(mut prompt) = self.download_prompt.take() else {
            return;
        };

        prompt.focus.restore(cx);

        let summary = crate::labels::spreadsheet_open_failed_message(&self.title());

        if self.use_live_connection(summary, cx).is_err() {
            self.phase = SpreadsheetPhase::Failed(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
            return;
        }

        self.spawn_open(cx);
    }

    /// Closes the download prompt without downloading. Nothing of the object
    /// was shown, so the tab asks to be closed: declining the download
    /// declines the open.
    pub fn dismiss_download_prompt(&mut self, cx: &mut Context<Self>) {
        let Some(mut prompt) = self.download_prompt.take() else {
            return;
        };

        prompt.focus.restore(cx);

        if matches!(self.phase, SpreadsheetPhase::AwaitingDownload) {
            cx.emit(DocumentEvent::RequestClose);
        }

        cx.notify();
    }

    /// Resolves the live connection of an object's profile and makes the
    /// document's location read through it. A local file has no connection
    /// and passes.
    ///
    /// A profile that is not connected is reported here under `summary`, and
    /// the caller reads and writes nothing.
    pub(super) fn use_live_connection(
        &mut self,
        summary: String,
        cx: &mut Context<Self>,
    ) -> Result<(), ConnectionUnavailable> {
        let FileDocumentKey::Object { profile_id, .. } = &self.file else {
            return Ok(());
        };

        let connection = self.app_state.as_ref().and_then(|app_state| {
            app_state
                .read(cx)
                .connections()
                .get(profile_id)
                .map(|connected| connected.connection.clone())
        });

        let Some(connection) = connection else {
            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(dbflux_i18n::t!(
                    "document.object_browser.error.connection_unavailable"
                )),
                cx,
            );

            return Err(ConnectionUnavailable);
        };

        if let FileLocation::Object {
            connection: opened_with,
            ..
        } = &mut self.location
        {
            *opened_with = connection;
        }

        Ok(())
    }

    /// The sheets of the opened workbook, in workbook order. Empty until the
    /// file is opened.
    pub fn sheets(&self) -> &[SheetInfo] {
        self.loaded().map_or(&[], |loaded| loaded.sheets.as_slice())
    }

    /// The index of the sheet selected in the tabs.
    pub fn active_sheet(&self) -> Option<usize> {
        self.loaded().and_then(|loaded| loaded.active_sheet)
    }

    /// The state of the table of the shown sheet.
    pub fn table_state(&self) -> Option<&Entity<DataTableState>> {
        self.shown_sheet().map(|shown| &shown.table_state)
    }

    /// Whether the selected sheet was read, holds nothing, and is shown as a
    /// notice because rows cannot be appended to it.
    pub fn is_empty_sheet(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| matches!(loaded.sheet, SheetPhase::Empty))
    }

    /// Whether a sheet is being read.
    pub fn is_reading_sheet(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| matches!(loaded.sheet, SheetPhase::Reading))
    }

    /// The address of the selected cell and what the formula readout says
    /// about it. `None` while no sheet is shown or no cell is selected.
    ///
    /// A cell with a staged value shows the formula typed into it, or no
    /// formula when the value replaces one.
    pub(super) fn selected_formula(&self, cx: &App) -> Option<(String, FormulaReadout)> {
        let shown = self.shown_sheet()?;
        let state = shown.table_state.read(cx);
        let active = state.selection().active?;

        let readout = match super::input::staged_text(state.edit_buffer(), active.row, active.col) {
            Some(text) if text.starts_with('=') => FormulaReadout::Formula(text.into()),
            Some(_) => FormulaReadout::NoFormula,
            None => shown.model.formula_at(active.row, active.col),
        };

        Some((cell_address(active.row, active.col), readout))
    }

    /// The sheet count, the shown sheet's size and that it is held in
    /// memory, once the file is opened.
    pub(super) fn summary(&self) -> Option<String> {
        let loaded = self.loaded()?;

        let shown = match &loaded.sheet {
            SheetPhase::Shown(shown) => Some((shown.model.row_count(), shown.model.column_count())),
            _ => None,
        };

        Some(crate::labels::spreadsheet_summary(
            loaded.sheets.len(),
            shown,
        ))
    }

    pub(super) fn phase(&self) -> &SpreadsheetPhase {
        &self.phase
    }

    pub(super) fn sheet_phase(&self) -> Option<&SheetPhase> {
        self.loaded().map(|loaded| &loaded.sheet)
    }

    pub(super) fn table(&self) -> Option<&Entity<DataTable>> {
        self.shown_sheet().map(|shown| &shown.table)
    }

    /// The name of the selected sheet.
    pub(super) fn active_sheet_name(&self) -> Option<&str> {
        let loaded = self.loaded()?;
        let index = loaded.active_sheet?;

        loaded.sheets.get(index).map(|sheet| sheet.name.as_str())
    }

    pub(super) fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Whether a sheet arrived since the last render. Reading it clears it.
    pub(super) fn take_pending_table_focus(&mut self) -> bool {
        std::mem::take(&mut self.pending_table_focus)
    }

    /// Whether the workbook's format has a writer: xlsx, xlsm and ods.
    pub(super) fn is_editable_format(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| loaded.format != SpreadsheetFormat::Xls)
    }

    pub(super) fn loaded(&self) -> Option<&LoadedWorkbook> {
        match &self.phase {
            SpreadsheetPhase::Loaded(loaded) => Some(loaded),
            _ => None,
        }
    }

    pub(super) fn loaded_mut(&mut self) -> Option<&mut LoadedWorkbook> {
        match &mut self.phase {
            SpreadsheetPhase::Loaded(loaded) => Some(loaded),
            _ => None,
        }
    }

    pub(super) fn shown_sheet(&self) -> Option<&ShownSheet> {
        match &self.loaded()?.sheet {
            SheetPhase::Shown(shown) => Some(shown),
            _ => None,
        }
    }

    // -- Opening -------------------------------------------------------------

    /// Shows the loading notice and opens the file on the background
    /// executor. An object that is only read whole has its version read
    /// first, and the user is asked whether to download it.
    fn start_open(&mut self, cx: &mut Context<Self>) {
        if self.reads == Some(ObjectReads::Downloaded) {
            self.phase = SpreadsheetPhase::Loading;
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();

            self.ask_to_download(cx);
            return;
        }

        self.spawn_open(cx);
    }

    /// Reads the object's version on the background executor, for the size
    /// the download prompt names.
    fn ask_to_download(&mut self, cx: &mut Context<Self>) {
        let location = self.location.clone();

        let task = cx
            .background_executor()
            .spawn(async move { read_version(&location) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| document.apply_object_version(result, cx))
                    .ok();
            });
        })
        .detach();
    }

    /// Opens the download prompt with the object's size. This is the first
    /// place a failed version read is caught, so it is reported here and only
    /// here.
    fn apply_object_version(
        &mut self,
        result: Result<SourceVersion, StorageError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(version) => {
                let mut focus = ModalFocus::new(cx);
                focus.focus_on_next_render();

                self.phase = SpreadsheetPhase::AwaitingDownload;
                self.download_prompt = Some(DownloadPrompt {
                    size: version.length(),
                    focus,
                });
            }

            Err(error) => {
                let error = OpenError::from(error);
                let summary = crate::labels::spreadsheet_open_failed_message(&self.title());
                let cause = error.to_string();

                report_error(open_error_to_user_facing(&error, summary), cx);

                self.phase = SpreadsheetPhase::Failed(cause);
            }
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Shows the loading notice and opens the workbook on the background
    /// executor, downloading an object that is only read whole.
    fn spawn_open(&mut self, cx: &mut Context<Self>) {
        self.phase = SpreadsheetPhase::Loading;
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();

        let location = self.location.clone();
        let reads = self.reads;

        let task = cx
            .background_executor()
            .spawn(async move { read_workbook(&location, reads, None) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| document.apply_open_outcome(result, cx))
                    .ok();
            });
        })
        .detach();
    }

    /// Stores the outcome of the background open. This is the first place a
    /// failure of the open, or of the read of the first sheet, is caught, so
    /// it is reported here and only here.
    fn apply_open_outcome(
        &mut self,
        result: Result<OpenedWorkbook, OpenError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(opened) => self.install_workbook(opened, cx),

            Err(error) => {
                let summary = crate::labels::spreadsheet_open_failed_message(&self.title());
                let cause = error.to_string();

                report_error(open_error_to_user_facing(&error, summary), cx);

                self.phase = SpreadsheetPhase::Failed(cause);
            }
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Shows an opened workbook, replacing whatever the document showed,
    /// pending edits included.
    pub(super) fn install_workbook(&mut self, opened: OpenedWorkbook, cx: &mut Context<Self>) {
        let OpenedWorkbook {
            workbook,
            source,
            version,
            source_length,
            first_sheet,
        } = opened;

        let mut loaded = LoadedWorkbook {
            sheets: workbook.sheets().to_vec(),
            format: workbook.format(),
            version,
            source_length,
            workbook: Some(workbook),
            source,
            active_sheet: None,
            sheet: SheetPhase::NoWorksheet,
            read_generation: 0,
            stored_edits: BTreeMap::new(),
            is_dirty: false,
        };

        if let Some((index, sheet)) = first_sheet {
            loaded.active_sheet = Some(index);
            self.phase = SpreadsheetPhase::Loaded(Box::new(loaded));
            self.show_sheet_outcome(sheet, cx);
        } else {
            self.phase = SpreadsheetPhase::Loaded(Box::new(loaded));
        }
    }

    // -- Sheets --------------------------------------------------------------

    /// Shows the worksheet after or before the selected one, skipping chart
    /// and other sheets without cells, and wrapping around.
    fn step_sheet(&mut self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded() else {
            return;
        };

        let worksheets: Vec<usize> = loaded
            .sheets
            .iter()
            .enumerate()
            .filter(|(_, sheet)| sheet.kind == SheetKind::Worksheet)
            .map(|(index, _)| index)
            .collect();

        let Some(current) = loaded
            .active_sheet
            .and_then(|active| worksheets.iter().position(|&index| index == active))
        else {
            return;
        };

        let count = worksheets.len();
        let next = if forward {
            (current + 1) % count
        } else {
            (current + count - 1) % count
        };

        if let Some(&index) = worksheets.get(next) {
            self.select_sheet(index, window, cx);
        }
    }

    /// Shows the sheet at `index`: the pending edits of the shown sheet are
    /// kept, the sheet is dropped and the new one is read on the background
    /// executor. Does nothing for the sheet already selected, for a sheet
    /// without cells, and while a save runs, which reads the file again.
    ///
    /// The keyboard moves to the document while the sheet is read, and to
    /// the new table once it arrives.
    pub fn select_sheet(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        if self.saving {
            return;
        }

        let Some(loaded) = self.loaded() else {
            return;
        };

        let selectable = loaded
            .sheets
            .get(index)
            .is_some_and(|sheet| sheet.kind == SheetKind::Worksheet);

        if !selectable || loaded.active_sheet == Some(index) {
            return;
        }

        self.store_shown_edits(cx);

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        loaded.active_sheet = Some(index);
        loaded.sheet = SheetPhase::Reading;
        loaded.read_generation += 1;

        self.focus_handle.focus(window, cx);
        self.start_pending_read(cx);

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Reads the selected sheet when it is waiting to be read and the
    /// workbook is not out with another read.
    fn start_pending_read(&mut self, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        if !matches!(loaded.sheet, SheetPhase::Reading) {
            return;
        }

        let Some(index) = loaded.active_sheet else {
            return;
        };

        let Some(mut workbook) = loaded.workbook.take() else {
            return;
        };

        let generation = loaded.read_generation;

        let task = cx.background_executor().spawn(async move {
            let result = workbook.read_sheet(index).map(SheetModel::new);

            (workbook, result)
        });

        cx.spawn(async move |this, cx| {
            let (workbook, result) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_sheet_outcome(generation, workbook, result, cx);
                })
                .ok();
            });
        })
        .detach();
    }

    /// Takes the workbook back and shows the sheet read. A sheet of an older
    /// selection is dropped, and the read of the selected one starts instead.
    fn apply_sheet_outcome(
        &mut self,
        generation: u64,
        workbook: OpenWorkbook,
        result: Result<SheetModel, SpreadsheetError>,
        cx: &mut Context<Self>,
    ) {
        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        loaded.workbook = Some(workbook);

        if generation != loaded.read_generation {
            self.start_pending_read(cx);
            return;
        }

        self.show_sheet_outcome(result, cx);

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Shows a sheet read for the selected sheet, or its failure. This is the
    /// first place a failed sheet read is caught, so it is reported here and
    /// only here.
    ///
    /// An empty sheet of a format with a writer is shown as a table of
    /// [`BLANK_SHEET_COLUMNS`] blank columns, so rows can be appended to it.
    /// A sheet that holds values keeps its own width.
    fn show_sheet_outcome(
        &mut self,
        result: Result<SheetModel, SpreadsheetError>,
        cx: &mut Context<Self>,
    ) {
        let title = self.title();
        let sheet_name = self.active_sheet_name().unwrap_or_default().to_string();

        let editable = self.is_editable_format();

        let sheet = match result {
            Ok(model) if model.is_empty() => match model.append_row() {
                Some(append_row) if editable => {
                    self.pending_table_focus = true;
                    let blank =
                        SheetModel::blank(model.row_count(), BLANK_SHEET_COLUMNS, append_row);
                    SheetPhase::Shown(Box::new(Self::build_shown(blank, editable, cx)))
                }

                _ => SheetPhase::Empty,
            },

            Ok(model) => {
                self.pending_table_focus = true;
                SheetPhase::Shown(Box::new(Self::build_shown(model, editable, cx)))
            }

            Err(error) => {
                let summary = crate::labels::spreadsheet_sheet_failed_message(&title, &sheet_name);
                let cause = crate::labels::spreadsheet_error_cause(&error, Some(&sheet_name));

                report_error(
                    spreadsheet_error_to_user_facing(&error, summary, cause.clone()),
                    cx,
                );

                SheetPhase::Failed(cause)
            }
        };

        if let Some(loaded) = self.loaded_mut() {
            loaded.sheet = sheet;
        }

        self.restore_stored_edits(cx);
        self.refresh_dirty(cx);
    }

    /// Builds the table of `model`, editable by position when `editable`.
    fn build_shown(model: SheetModel, editable: bool, cx: &mut Context<Self>) -> ShownSheet {
        let table_model = model.table_model();
        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(table_model, cx);
            state.set_positional_editing(editable);
            state.set_insertable(editable);
            state
        });
        let table = cx.new(|cx| DataTable::new("spreadsheet-table", table_state.clone(), cx));
        let subscription = Self::subscribe_to_table(&table_state, cx);
        let observation = cx.observe(&table_state, |this, _, cx| {
            this.refresh_dirty(cx);
            cx.notify();
        });

        ShownSheet {
            model,
            table_state,
            table,
            _subscription: subscription,
            _observation: observation,
        }
    }

    /// The rows are the sheet's rows in sheet order, which is what their
    /// addresses mean, so sorting them is refused: a header click sets the
    /// sort indicator before it reports the change, and the indicator is
    /// cleared again here. A selection change redraws the formula readout,
    /// and the row operations and saves the table asks for are the
    /// document's.
    fn subscribe_to_table(
        table_state: &Entity<DataTableState>,
        cx: &mut Context<Self>,
    ) -> Subscription {
        cx.subscribe(
            table_state,
            |this, table_state, event: &DataTableEvent, cx| match event {
                DataTableEvent::SortChanged(Some(_)) => {
                    table_state.update(cx, |state, cx| {
                        state.clear_sort_without_emit();
                        cx.notify();
                    });
                }

                DataTableEvent::SelectionChanged(_) => cx.notify(),

                event => this.handle_table_event(event, cx),
            },
        )
    }
}

/// The sheet shown first: the first visible worksheet, or the first
/// worksheet when every worksheet is hidden. `None` without a worksheet.
fn first_sheet(sheets: &[SheetInfo]) -> Option<usize> {
    let is_worksheet = |sheet: &SheetInfo| sheet.kind == SheetKind::Worksheet;

    sheets
        .iter()
        .position(|sheet| is_worksheet(sheet) && sheet.visible)
        .or_else(|| sheets.iter().position(is_worksheet))
}

/// The profile of an object is not connected, which was reported.
#[derive(Debug)]
pub(super) struct ConnectionUnavailable;

/// Opens the workbook at `location` and reads one sheet: the worksheet at
/// `preferred` when there is one, or else the first shown sheet. An object
/// read as `reads` says [`ObjectReads::Downloaded`] is downloaded whole
/// first, into memory or a temporary file. Blocks on file or network I/O and
/// on decoding the sheet.
pub(super) fn read_workbook(
    location: &FileLocation,
    reads: Option<ObjectReads>,
    preferred: Option<usize>,
) -> Result<OpenedWorkbook, OpenError> {
    let (source, version) = match (location, reads) {
        (
            FileLocation::Object {
                connection,
                bucket,
                key,
            },
            Some(ObjectReads::Downloaded),
        ) => download_whole_object(
            connection.as_ref(),
            bucket,
            key,
            DOWNLOAD_IN_MEMORY_LIMIT_BYTES,
        )?,

        _ => open_source(location)?,
    };

    open_workbook_over(source, version, preferred)
}

/// Opens the workbook `source` reads, `version` of it, and reads one sheet
/// as [`read_workbook`] does.
fn open_workbook_over(
    source: LocationSource,
    version: SourceVersion,
    preferred: Option<usize>,
) -> Result<OpenedWorkbook, OpenError> {
    let source_length = source.byte_length().map_err(StorageError::Read)?;
    let source = Arc::new(source);
    let mut workbook = dbflux_spreadsheet::open(source.clone())?;

    let preferred = preferred.filter(|&index| {
        workbook
            .sheets()
            .get(index)
            .is_some_and(|sheet| sheet.kind == SheetKind::Worksheet)
    });

    let first_sheet = preferred
        .or_else(|| first_sheet(workbook.sheets()))
        .map(|index| (index, workbook.read_sheet(index).map(SheetModel::new)));

    Ok(OpenedWorkbook {
        workbook,
        source,
        version,
        source_length,
        first_sheet,
    })
}

/// The user-facing error of a failed open. `summary` names the file.
pub(super) fn open_error_to_user_facing(error: &OpenError, summary: String) -> UserFacingError {
    match error {
        OpenError::Storage(_) => {
            UserFacingError::new(ErrorKind::Storage, summary).with_cause(error.to_string())
        }

        OpenError::Spreadsheet(spreadsheet_error) => {
            spreadsheet_error_to_user_facing(spreadsheet_error, summary, error.to_string())
        }
    }
}

/// The user-facing error of a failed read of a workbook or one of its
/// sheets. A sheet past the size limit is the user's to avoid; everything
/// else is a file that could not be read.
pub(super) fn spreadsheet_error_to_user_facing(
    error: &SpreadsheetError,
    summary: String,
    cause: String,
) -> UserFacingError {
    let kind = match error {
        SpreadsheetError::SheetTooLarge { .. }
        | SpreadsheetError::ChartSheet { .. }
        | SpreadsheetError::Encrypted => ErrorKind::User,
        _ => ErrorKind::Storage,
    };

    UserFacingError::new(kind, summary).with_cause(cause)
}
