//! The delimited file tab: a CSV or TSV file, local or in object storage,
//! shown as a read-only table of the pages loaded so far.
//!
//! Opening reads the file on the background executor and never on the
//! foreground: the source is opened, a leading sample decides the dialect, and
//! the reader returns the first page. The document shows a loading notice
//! until that arrives and the error when it fails.
//!
//! A further page is read on request, also on the background executor. The
//! reader blocks and needs exclusive access, so it moves into the read and
//! comes back with the result. While it is away no other page can be asked
//! for.
//!
//! The dialect detection resolved is kept, and the toolbar overrides parts
//! of it. An override never rewrites the file: the file is read again from
//! its first page under the overridden dialect, on the background executor
//! and through a source of its own, and the table is replaced when that page
//! arrives. The latest override wins. A reread that a later override
//! replaced, and a page read through the reader a reread replaced, are
//! dropped when they come back.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_components::components::data_table::{
    DataTable, DataTableEvent, DataTableState, ModelSwap,
};
use dbflux_components::controls::DropdownSelectionChanged;
use dbflux_core::{Connection, DbError};
use dbflux_delimited::{
    ByteSource, Dialect, DialectOverrides, Encoding, MemorySource, Page, PagedReader, ReadError,
    ReaderOptions, RecordCount, SampleCoverage, SourceError, detect_dialect,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::page_model::{PageModel, PageModelError};
use super::source::{DelimitedLocation, DelimitedSource, SourceVersion, StorageError, open_source};
use super::toolbar::{DELIMITERS, DialectControls, QUOTES};
use crate::dedup::DelimitedFileKey;
use crate::handle::DocumentEvent;
use crate::object_text::db_error_to_user_facing;
use crate::types::{DocumentId, DocumentState};

/// How many leading bytes dialect detection looks at: enough records for the
/// delimiter vote and the encoding guess, and small enough to be one read.
pub(super) const SAMPLE_BYTES: u64 = 64 * 1024;

/// How many records one page holds: a few screens of rows, so the first page
/// of a large file is parsed and laid out without a visible wait.
pub(super) const PAGE_SIZE: NonZeroUsize = match NonZeroUsize::new(500) {
    Some(size) => size,
    None => panic!("the page size must not be zero"),
};

/// How many bytes the reader asks from the source in one read: a page of
/// ordinary records fits in it, so a further page of an object is one range
/// request. Opening an object costs a `head_object` and two range requests,
/// because the detection sample and the reader's first window both start at
/// offset zero.
const FETCH_WINDOW_BYTES: NonZeroU64 = match NonZeroU64::new(1024 * 1024) {
    Some(size) => size,
    None => panic!("the fetch window must not be zero"),
};

/// How the document pages and fetches a file.
pub(super) const READER_OPTIONS: ReaderOptions = ReaderOptions {
    page_size: PAGE_SIZE,
    window_size: FETCH_WINDOW_BYTES,
};

/// A condition of an opened file that the user has to know about and that
/// does not stop the file from being shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimitedWarning {
    /// The reader refuses the detected dialect, so the file is read with
    /// another delimiter and its columns are probably wrong.
    DetectedDelimiterUnreadable,

    /// Decoding replaced a malformed byte sequence in the header or in a
    /// loaded record, so the encoding is probably wrong.
    MalformedText,

    /// The file reports nothing but its length to tell one version from
    /// another, so a save could overwrite a change it cannot see and is
    /// refused.
    CannotSaveInPlace,
}

/// Why a delimited file could not be opened, or a further page of it could
/// not be read.
#[derive(Debug)]
pub(super) enum OpenError {
    /// The file or object could not be reached or read.
    Storage(StorageError),

    /// The object store refused a range read. The driver's error is kept as
    /// it came, without the wrapping of the source it was read through.
    Driver(Box<DbError>),

    /// The reader could not make records of the bytes, or refuses the
    /// dialect.
    Read(ReadError),

    /// The page did not fit the page model.
    PageModel(PageModelError),
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => fmt::Display::fmt(error, formatter),
            Self::Driver(error) => fmt::Display::fmt(error, formatter),
            Self::Read(error) => fmt::Display::fmt(error, formatter),
            Self::PageModel(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl From<StorageError> for OpenError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<SourceError> for OpenError {
    /// An object source wraps the driver's `DbError`, which is taken out
    /// again here. Any other source error is a read the storage layer could
    /// not do.
    fn from(error: SourceError) -> Self {
        match error.into_inner().downcast::<DbError>() {
            Ok(driver_error) => Self::Driver(driver_error),
            Err(other) => Self::Storage(StorageError::Read(SourceError::new(other))),
        }
    }
}

impl From<ReadError> for OpenError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Source(source) => Self::from(source),
            other => Self::Read(other),
        }
    }
}

impl From<PageModelError> for OpenError {
    fn from(error: PageModelError) -> Self {
        Self::PageModel(error)
    }
}

/// What the background open hands to the foreground.
pub(super) struct OpenedFile {
    /// The dialect detection resolved.
    detected: Dialect,

    /// The dialect the first page was read with. It differs from `detected`
    /// when the reader refuses that one or the file was read again under an
    /// override.
    pub(super) dialect: Dialect,
    version: SourceVersion,
    reader: PagedReader<DelimitedSource>,
    page_model: PageModel,
}

/// What a background page read hands to the foreground.
pub(super) struct ReadPage {
    page: Page,

    /// The reader's count after the read, as [`settled_record_count`] left
    /// it.
    record_count: RecordCount,
}

/// A file whose first page is loaded.
struct LoadedFile {
    /// The dialect detection resolved when the file was opened. Every
    /// override is applied on top of it, never on top of another override.
    detected: Dialect,

    /// The dialect the loaded records were read with: `overrides` applied to
    /// `detected`.
    dialect: Dialect,

    /// The overrides in effect.
    overrides: DialectOverrides,

    /// The overrides the user asked for last, which the controls show. They
    /// differ from `overrides` while the file is read again under them.
    requested_overrides: DialectOverrides,

    /// The dialect the file was opened with in place of a detected one the
    /// reader refuses. `None` when the detected dialect was readable.
    fallback: Option<Dialect>,

    /// The reread that is running. Storing it is what cancels it: a later
    /// override replaces it, and a reread that has not started reading by
    /// then never reads.
    reread_task: Option<Task<()>>,

    /// Counts the overrides asked for. A reread carries the count it was
    /// started at, and its result is dropped when a later override was asked
    /// for meanwhile.
    reread_generation: u64,

    /// Counts the readers this file had. A page read carries the count of
    /// the reader it took, and its result is dropped when a reread replaced
    /// that reader meanwhile.
    reader_epoch: u64,

    controls: DialectControls,

    /// The version of the file the reader's byte ranges belong to.
    version: SourceVersion,

    /// The reader the first page came from, kept open over the same source
    /// for the pages that follow it. `None` while a page is being read: the
    /// background read has it and hands it back with the page.
    reader: Option<PagedReader<DelimitedSource>>,

    page_model: PageModel,
    warnings: Vec<DelimitedWarning>,

    /// The text of each warning, formatted when the file is loaded.
    warning_items: Vec<SharedString>,

    table_state: Entity<DataTableState>,
    table: Entity<DataTable>,

    /// The status line, formatted when the page model changes.
    status_items: Vec<SharedString>,
}

impl LoadedFile {
    /// Shows the page model as it is after a page was appended: the table
    /// gets the rows, and the status line and the warnings are formatted
    /// again.
    ///
    /// The table keeps its column widths, its scroll position, the keyboard
    /// and the selected cell, because the rows already shown keep their
    /// indices. It drops pending edits, of which a read-only table has none.
    fn show_page_model(&mut self, cx: &mut App) {
        let model = Arc::new(self.page_model.table_model());

        self.table_state.update(cx, |state, cx| {
            state.set_model(model, ModelSwap::KeepCursor, cx);
        });

        self.status_items = status_items(&self.dialect, &self.page_model);
        self.refresh_warnings();
    }

    /// Works out the warnings of the file as it is read now, and their
    /// text.
    ///
    /// The unreadable-delimiter warning stays while the file is read with
    /// the delimiter and the encoding it was opened with in place of the
    /// detected dialect, whatever the quote and the header flag, and goes
    /// once the user changes either of the two.
    fn refresh_warnings(&mut self) {
        let mut warnings = Vec::new();

        let reads_with_fallback_delimiter = self.fallback.is_some_and(|fallback| {
            fallback.delimiter == self.dialect.delimiter
                && fallback.encoding == self.dialect.encoding
        });

        if reads_with_fallback_delimiter {
            warnings.push(DelimitedWarning::DetectedDelimiterUnreadable);
        }

        if self.page_model.had_replacements() {
            warnings.push(DelimitedWarning::MalformedText);
        }

        if !self.version.detects_same_length_change() {
            warnings.push(DelimitedWarning::CannotSaveInPlace);
        }

        self.warning_items = warnings
            .iter()
            .map(|warning| warning_text(*warning, &self.dialect, &self.detected).into())
            .collect();
        self.warnings = warnings;
    }

    /// The dialect the controls show: the one asked for last.
    fn requested_dialect(&self) -> Dialect {
        self.requested_overrides.apply(self.detected)
    }

    /// Makes the selects show the dialect asked for last.
    fn show_requested_dialect(&self, cx: &mut App) {
        self.controls.show(&self.requested_dialect(), cx);
    }

    /// Replaces the reader, the records and the table rows with the first
    /// page of the file as it was read again under `requested_overrides`.
    ///
    /// The new reader is put in place even while a page read holds the old
    /// one: that read is of a reader this file no longer has, and its result
    /// is dropped by the epoch it carries. The table returns to its first
    /// row, because the rows it showed are gone.
    fn show_reread(&mut self, reread: OpenedFile, cx: &mut App) {
        let OpenedFile {
            detected: _,
            dialect,
            version,
            reader,
            page_model,
        } = reread;

        self.dialect = dialect;
        self.overrides = self.requested_overrides;
        self.reader = Some(reader);
        self.reader_epoch += 1;
        self.reread_task = None;
        self.version = version;
        self.page_model = page_model;

        self.status_items = status_items(&self.dialect, &self.page_model);
        self.refresh_warnings();

        let model = Arc::new(self.page_model.table_model());

        self.table_state.update(cx, |state, cx| {
            state.set_model(model, ModelSwap::ResetCursor, cx);
        });
    }
}

enum DelimitedPhase {
    Loading,
    Failed(String),
    Loaded(Box<LoadedFile>),
}

/// A CSV or TSV file opened as a table.
pub struct DelimitedDocument {
    id: DocumentId,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    file: DelimitedFileKey,
    location: DelimitedLocation,

    /// The application state the live connection of an object's profile is
    /// resolved from for every page. `None` for a local file, which has no
    /// connection.
    app_state: Option<Entity<AppStateEntity>>,

    /// The page and window sizes every reader of this file is opened with.
    reader_options: ReaderOptions,

    phase: DelimitedPhase,

    /// Set when the first page arrives, so the next render hands the keyboard
    /// to the table if the loading notice held it.
    pending_table_focus: bool,

    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for DelimitedDocument {}

impl DelimitedDocument {
    /// Opens the local file at `path`.
    pub fn open_local(path: PathBuf, cx: &mut Context<Self>) -> Self {
        Self::open_local_with(path, READER_OPTIONS, cx)
    }

    /// [`Self::open_local`] with the page and window sizes of
    /// `reader_options`, which tests make small.
    pub(super) fn open_local_with(
        path: PathBuf,
        reader_options: ReaderOptions,
        cx: &mut Context<Self>,
    ) -> Self {
        let file = DelimitedFileKey::Local { path: path.clone() };
        let location = DelimitedLocation::Local { path };

        Self::open(file, location, None, reader_options, cx)
    }

    /// Opens the object `key` of `bucket` through `connection`, the live
    /// connection of the profile `profile_id` in `app_state`. Further pages
    /// are read through the connection the profile has in `app_state` when
    /// they are asked for.
    pub fn open_object(
        app_state: Entity<AppStateEntity>,
        profile_id: uuid::Uuid,
        connection: Arc<dyn Connection>,
        bucket: String,
        key: String,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::open_object_with(
            app_state,
            profile_id,
            connection,
            bucket,
            key,
            READER_OPTIONS,
            cx,
        )
    }

    /// [`Self::open_object`] with the page and window sizes of
    /// `reader_options`, which tests make small.
    pub(super) fn open_object_with(
        app_state: Entity<AppStateEntity>,
        profile_id: uuid::Uuid,
        connection: Arc<dyn Connection>,
        bucket: String,
        key: String,
        reader_options: ReaderOptions,
        cx: &mut Context<Self>,
    ) -> Self {
        let file = DelimitedFileKey::Object {
            profile_id,
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let location = DelimitedLocation::Object {
            connection,
            bucket,
            key,
        };

        Self::open(file, location, Some(app_state), reader_options, cx)
    }

    fn open(
        file: DelimitedFileKey,
        location: DelimitedLocation,
        app_state: Option<Entity<AppStateEntity>>,
        reader_options: ReaderOptions,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut document = Self {
            id: DocumentId::new(),
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            file,
            location,
            app_state,
            reader_options,
            phase: DelimitedPhase::Loading,
            pending_table_focus: false,
            _subscriptions: Vec::new(),
        };

        document.load_first_page(cx);
        document
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// The identity this file is deduplicated by.
    pub fn file(&self) -> &DelimitedFileKey {
        &self.file
    }

    /// The file name: the last component of the local path or of the object
    /// key.
    pub fn title(&self) -> String {
        match &self.file {
            DelimitedFileKey::Local { path } => path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string()),

            DelimitedFileKey::Object { key, .. } => object_leaf(key).to_string(),
        }
    }

    pub fn state(&self) -> DocumentState {
        match &self.phase {
            DelimitedPhase::Loading => DocumentState::Loading,
            DelimitedPhase::Failed(_) => DocumentState::Error,
            DelimitedPhase::Loaded(_) => DocumentState::Clean,
        }
    }

    pub fn can_close(&self) -> bool {
        true
    }

    /// The profile whose connection reads the object, and `None` for a local
    /// file.
    pub fn connection_id(&self) -> Option<uuid::Uuid> {
        match &self.file {
            DelimitedFileKey::Local { .. } => None,
            DelimitedFileKey::Object { profile_id, .. } => Some(*profile_id),
        }
    }

    /// Always `None`: the table is read-only, so the document has no unsaved
    /// changes.
    pub fn change_summary(&self) -> Option<String> {
        None
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

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
    }

    pub fn active_context(&self) -> dbflux_app::keymap::ContextId {
        dbflux_app::keymap::ContextId::Results
    }

    /// Table navigation runs inside the embedded `DataTable` through its own
    /// key context. The one command of the document is the next page, which
    /// a loaded file answers by loading one.
    pub fn dispatch_command(
        &mut self,
        command: dbflux_app::keymap::Command,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            dbflux_app::keymap::Command::ResultsNextPage if self.loaded().is_some() => {
                self.load_more(cx);
                true
            }

            _ => false,
        }
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match &self.phase {
            DelimitedPhase::Loaded(loaded) => {
                let handle = loaded.table_state.read(cx).focus_handle().clone();
                handle.focus(window, cx);
            }

            DelimitedPhase::Loading | DelimitedPhase::Failed(_) => {
                self.focus_handle.focus(window, cx);
            }
        }
    }

    /// Saves as part of an interrupted close. Returns whether a save started.
    ///
    /// The table is read-only, so there is never anything to write and no
    /// save starts.
    pub fn save_for_close(&mut self, _cx: &mut Context<Self>) -> bool {
        false
    }

    /// The dialect the loaded records were read with, once the file is
    /// loaded: the detected one with the overrides in effect.
    pub fn dialect(&self) -> Option<Dialect> {
        self.loaded().map(|loaded| loaded.dialect)
    }

    /// The dialect detected when the file was opened, once it is loaded.
    pub fn detected_dialect(&self) -> Option<Dialect> {
        self.loaded().map(|loaded| loaded.detected)
    }

    /// The overrides asked for last. A field is `None` while it has the
    /// detected value. Empty until the file is loaded.
    pub fn dialect_overrides(&self) -> DialectOverrides {
        self.loaded()
            .map(|loaded| loaded.requested_overrides)
            .unwrap_or_default()
    }

    /// Whether the file is being read again under an override.
    pub fn is_rereading(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| loaded.requested_overrides != loaded.overrides)
    }

    /// Whether any part of the detected dialect is overridden.
    pub(super) fn has_dialect_overrides(&self) -> bool {
        self.dialect_overrides() != DialectOverrides::default()
    }

    /// The dialect the controls show: the detected one with the overrides
    /// asked for last.
    pub(super) fn requested_dialect(&self) -> Option<Dialect> {
        self.loaded().map(LoadedFile::requested_dialect)
    }

    pub(super) fn dialect_controls(&self) -> Option<&DialectControls> {
        self.loaded().map(|loaded| &loaded.controls)
    }

    /// The count a reread started now would carry.
    #[cfg(test)]
    pub(super) fn reread_generation(&self) -> u64 {
        self.loaded().map_or(0, |loaded| loaded.reread_generation)
    }

    /// The version of the file that was opened, once it is loaded.
    pub fn source_version(&self) -> Option<&SourceVersion> {
        self.loaded().map(|loaded| &loaded.version)
    }

    /// How many records the file has, as far as the reader knew after the
    /// last loaded page.
    pub fn record_count(&self) -> Option<RecordCount> {
        self.loaded().map(|loaded| loaded.page_model.record_count())
    }

    /// The state of the table of loaded records, once the file is loaded.
    pub fn table_state(&self) -> Option<&Entity<DataTableState>> {
        self.loaded().map(|loaded| &loaded.table_state)
    }

    /// What the user has to know about the opened file. Empty until the file
    /// is loaded.
    pub fn warnings(&self) -> &[DelimitedWarning] {
        self.loaded()
            .map_or(&[], |loaded| loaded.warnings.as_slice())
    }

    /// The text of each warning, in the order of [`Self::warnings`].
    pub fn warning_items(&self) -> &[SharedString] {
        self.loaded()
            .map_or(&[], |loaded| loaded.warning_items.as_slice())
    }

    /// Whether the file has records that are not loaded, as far as the reader
    /// knows. False until the file is loaded.
    pub fn has_more_records(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| !loaded.page_model.is_fully_loaded())
    }

    /// Whether the next page can be asked for now: the file has more
    /// records, and neither a page nor the whole file is being read.
    ///
    /// A page asked for during a reread would be read through the reader the
    /// reread replaces, and dropped.
    pub fn can_load_more(&self) -> bool {
        self.has_more_records() && !self.is_loading_more() && !self.is_rereading()
    }

    /// What the footer says while the file is read again under an override.
    pub fn progress_item(&self) -> Option<String> {
        self.is_rereading()
            .then(|| dbflux_i18n::t!("document.delimited.footer.rereading"))
    }

    /// Whether a further page is being read.
    pub fn is_loading_more(&self) -> bool {
        self.loaded().is_some_and(|loaded| loaded.reader.is_none())
    }

    /// The status line: the delimiter, the encoding, and the loaded records
    /// against the total. Empty until the file is loaded.
    pub fn status_items(&self) -> &[SharedString] {
        self.loaded()
            .map_or(&[], |loaded| loaded.status_items.as_slice())
    }

    /// Why the file could not be opened, when it could not.
    pub fn failure(&self) -> Option<&str> {
        match &self.phase {
            DelimitedPhase::Failed(cause) => Some(cause),
            DelimitedPhase::Loading | DelimitedPhase::Loaded(_) => None,
        }
    }

    pub(super) fn table(&self) -> Option<&Entity<DataTable>> {
        self.loaded().map(|loaded| &loaded.table)
    }

    pub(super) fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Whether the first page arrived since the last render. Reading it
    /// clears it.
    pub(super) fn take_pending_table_focus(&mut self) -> bool {
        std::mem::take(&mut self.pending_table_focus)
    }

    fn loaded(&self) -> Option<&LoadedFile> {
        match &self.phase {
            DelimitedPhase::Loaded(loaded) => Some(loaded),
            DelimitedPhase::Loading | DelimitedPhase::Failed(_) => None,
        }
    }

    fn loaded_mut(&mut self) -> Option<&mut LoadedFile> {
        match &mut self.phase {
            DelimitedPhase::Loaded(loaded) => Some(loaded),
            DelimitedPhase::Loading | DelimitedPhase::Failed(_) => None,
        }
    }

    /// The extension of the file name, which hints at the delimiter.
    fn extension(&self) -> Option<String> {
        let file_name = match &self.file {
            DelimitedFileKey::Local { path } => path.file_name()?.to_string_lossy().into_owned(),
            DelimitedFileKey::Object { key, .. } => object_leaf(key).to_string(),
        };

        extension_hint(&file_name).map(str::to_string)
    }

    // -- Opening -------------------------------------------------------------

    fn load_first_page(&mut self, cx: &mut Context<Self>) {
        self.phase = DelimitedPhase::Loading;
        cx.notify();

        let location = self.location.clone();
        let extension = self.extension();
        let reader_options = self.reader_options;

        let task = cx
            .background_executor()
            .spawn(async move { open_first_page(&location, extension.as_deref(), reader_options) });

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
    /// failure of the open is caught, so it is reported here and only here.
    fn apply_open_outcome(
        &mut self,
        result: Result<OpenedFile, OpenError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(opened) => {
                self.phase = DelimitedPhase::Loaded(Box::new(self.build_loaded(opened, cx)));
                self.pending_table_focus = true;
            }

            Err(error) => {
                let summary = crate::labels::delimited_open_failed_message(&self.title());
                let cause = error.to_string();

                report_error(open_error_to_user_facing(&error, summary), cx);

                self.phase = DelimitedPhase::Failed(cause);
            }
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    fn build_loaded(&mut self, opened: OpenedFile, cx: &mut Context<Self>) -> LoadedFile {
        let OpenedFile {
            detected,
            dialect,
            version,
            reader,
            page_model,
        } = opened;

        // A detected dialect the reader refuses is opened under another
        // delimiter, which starts as the override in effect.
        let overrides = overrides_on(
            DialectOverrides {
                delimiter: Some(dialect.delimiter),
                ..DialectOverrides::default()
            },
            &detected,
        );
        let fallback = (dialect != detected).then_some(dialect);

        let model = Arc::new(page_model.table_model());
        let table_state = cx.new(|cx| DataTableState::new(model, cx));

        // The rows are the file's records in file order, so the document does
        // not sort them. A header click sets the sort indicator before it
        // reports the change, and the indicator is cleared again here.
        let sort_subscription = cx.subscribe(
            &table_state,
            |_this, table_state, event: &DataTableEvent, cx| {
                if matches!(event, DataTableEvent::SortChanged(Some(_))) {
                    table_state.update(cx, |state, cx| {
                        state.clear_sort_without_emit();
                        cx.notify();
                    });
                }
            },
        );

        let controls = DialectControls::new(&detected, cx);
        controls.show(&dialect, cx);

        let delimiter_subscription = cx.subscribe(
            &controls.delimiter,
            |this, _, event: &DropdownSelectionChanged, cx| {
                if let Some(delimiter) = DELIMITERS.get(event.index) {
                    this.override_delimiter(*delimiter, cx);
                }
            },
        );

        let quote_subscription = cx.subscribe(
            &controls.quote,
            |this, _, event: &DropdownSelectionChanged, cx| {
                if let Some(quote) = QUOTES.get(event.index) {
                    this.override_quote(*quote, cx);
                }
            },
        );

        let encoding_subscription = cx.subscribe(
            &controls.encoding,
            |this, _, event: &DropdownSelectionChanged, cx| {
                let encoding = this
                    .dialect_controls()
                    .and_then(|controls| controls.encoding_at(event.index));

                if let Some(encoding) = encoding {
                    this.override_encoding(encoding, cx);
                }
            },
        );

        self._subscriptions = vec![
            sort_subscription,
            delimiter_subscription,
            quote_subscription,
            encoding_subscription,
        ];

        let table = cx.new(|cx| DataTable::new("delimited-table", table_state.clone(), cx));

        let status_items = status_items(&dialect, &page_model);

        let mut loaded = LoadedFile {
            detected,
            dialect,
            overrides,
            requested_overrides: overrides,
            fallback,
            reread_task: None,
            reread_generation: 0,
            reader_epoch: 0,
            controls,
            version,
            reader: Some(reader),
            page_model,
            warnings: Vec::new(),
            warning_items: Vec::new(),
            table_state,
            table,
            status_items,
        };

        loaded.refresh_warnings();
        loaded
    }

    // -- Further pages -------------------------------------------------------

    /// Reads the next page on the background executor and appends it to the
    /// table. Does nothing while a page is being read, while the file is
    /// read again under an override, and once every record is loaded.
    ///
    /// An object is read through the live connection of its profile, resolved
    /// here for every page: the one the file was opened with is dead after a
    /// disconnect or a reconnect. A profile that is not connected is reported
    /// and nothing is read.
    pub fn load_more(&mut self, cx: &mut Context<Self>) {
        if !self.can_load_more() {
            return;
        }

        let summary = crate::labels::delimited_load_more_failed_message(&self.title());

        let Ok(connection) = self.use_live_connection(summary, cx) else {
            return;
        };

        let page_size = self.reader_options.page_size;

        let Some(loaded) = self.loaded_mut() else {
            return;
        };
        let Some(mut reader) = loaded.reader.take() else {
            return;
        };
        let page_index = loaded.page_model.next_page();
        let reader_epoch = loaded.reader_epoch;

        if let Some(connection) = connection {
            reader.source_mut().use_connection(connection);
        }

        let task = cx.background_executor().spawn(async move {
            let result = read_page(&mut reader, page_index, page_size);

            (reader, result)
        });

        cx.spawn(async move |this, cx| {
            let (reader, result) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_page_outcome(reader_epoch, reader, result, cx);
                })
                .ok();
            });
        })
        .detach();

        cx.notify();
    }

    /// Resolves the live connection of an object's profile and makes the
    /// document's location read through it. `Ok(None)` for a local file,
    /// which has no connection.
    ///
    /// A profile that is not connected is reported here under `summary`, and
    /// the caller reads nothing.
    fn use_live_connection(
        &mut self,
        summary: String,
        cx: &mut Context<Self>,
    ) -> Result<Option<Arc<dyn Connection>>, ConnectionUnavailable> {
        let DelimitedFileKey::Object { profile_id, .. } = &self.file else {
            return Ok(None);
        };

        let Some(connection) = self.live_connection(*profile_id, cx) else {
            let cause = dbflux_i18n::t!("document.object_browser.error.connection_unavailable");

            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(cause),
                cx,
            );

            return Err(ConnectionUnavailable);
        };

        if let DelimitedLocation::Object {
            connection: opened_with,
            ..
        } = &mut self.location
        {
            *opened_with = connection.clone();
        }

        Ok(Some(connection))
    }

    /// The live connection of the profile `profile_id`, resolved from the
    /// application state the way the object editor resolves it for each
    /// operation. `None` when the profile is not connected.
    fn live_connection(
        &self,
        profile_id: uuid::Uuid,
        cx: &Context<Self>,
    ) -> Option<Arc<dyn Connection>> {
        self.app_state
            .as_ref()?
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.connection.clone())
    }

    /// Takes the reader back and stores the outcome of the background page
    /// read. This is the first place a failure of the read is caught, so it
    /// is reported here and only here. A document that was closed meanwhile
    /// never gets here, and its failure is dropped.
    ///
    /// A failed read leaves the loaded pages as they are, and the page can be
    /// asked for again.
    ///
    /// `reader_epoch` is the epoch of the reader the read took. When the
    /// file was read again under another dialect meanwhile, the reader and
    /// its page belong to a dialect that is no longer shown, and both are
    /// dropped without a report.
    fn apply_page_outcome(
        &mut self,
        reader_epoch: u64,
        reader: PagedReader<DelimitedSource>,
        result: Result<ReadPage, OpenError>,
        cx: &mut Context<Self>,
    ) {
        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        if loaded.reader_epoch != reader_epoch {
            return;
        }

        loaded.reader = Some(reader);

        let appended = result.and_then(|read| {
            loaded
                .page_model
                .append_page(read.page, read.record_count)
                .map_err(OpenError::from)
        });

        match appended {
            Ok(_) => loaded.show_page_model(cx),

            Err(error) => {
                let summary = crate::labels::delimited_load_more_failed_message(&title);

                report_error(open_error_to_user_facing(&error, summary), cx);
            }
        }

        cx.notify();
    }

    // -- Dialect overrides ---------------------------------------------------

    /// Reads the file with `delimiter` between fields.
    pub fn override_delimiter(&mut self, delimiter: u8, cx: &mut Context<Self>) {
        let overrides = DialectOverrides {
            delimiter: Some(delimiter),
            ..self.dialect_overrides()
        };

        self.set_dialect_overrides(overrides, cx);
    }

    /// Reads the file with `quote` around fields, or without quoting.
    pub fn override_quote(&mut self, quote: Option<u8>, cx: &mut Context<Self>) {
        let overrides = DialectOverrides {
            quote: Some(quote),
            ..self.dialect_overrides()
        };

        self.set_dialect_overrides(overrides, cx);
    }

    /// Reads the first record of the file as the column names, or as data.
    pub fn override_has_header(&mut self, has_header: bool, cx: &mut Context<Self>) {
        let overrides = DialectOverrides {
            has_header: Some(has_header),
            ..self.dialect_overrides()
        };

        self.set_dialect_overrides(overrides, cx);
    }

    /// Reads the bytes of the file as text in `encoding`.
    pub fn override_encoding(&mut self, encoding: &'static Encoding, cx: &mut Context<Self>) {
        let overrides = DialectOverrides {
            encoding: Some(encoding),
            ..self.dialect_overrides()
        };

        self.set_dialect_overrides(overrides, cx);
    }

    /// Switches the header flag the controls show.
    pub fn toggle_header(&mut self, cx: &mut Context<Self>) {
        if let Some(requested) = self.requested_dialect() {
            self.override_has_header(!requested.has_header, cx);
        }
    }

    /// Drops every override and reads the file with the detected dialect.
    pub fn reset_dialect(&mut self, cx: &mut Context<Self>) {
        self.set_dialect_overrides(DialectOverrides::default(), cx);
    }

    /// Reads the file again from its first page with `overrides` applied to
    /// the detected dialect. Does nothing until the file is loaded.
    ///
    /// The file is never written. The loaded pages are dropped when the
    /// first page of the new reading arrives, and the previous dialect, its
    /// records and its reader stay in effect until then and when the reading
    /// fails. Header detection is not run again: a delimiter or encoding
    /// override keeps the header flag in effect.
    ///
    /// An override equal to the detected value is no override. A dialect
    /// the reader refuses is reported once and changes nothing. That holds
    /// for dropping every override of a file whose detected dialect the
    /// reader refuses: the file was opened under another delimiter, and the
    /// reset is refused and reported like any other refused override.
    ///
    /// The latest call wins. A call never waits for a reread or a page read
    /// that is running: it starts a reread through a source of its own. The
    /// earlier reread is cancelled, and when it or the page read had already
    /// read, its result is dropped when it comes back.
    /// Overrides that are already the ones in effect cancel a running reread
    /// without reading anything.
    ///
    /// An object is read through the live connection of its profile, as a
    /// further page is.
    pub fn set_dialect_overrides(&mut self, overrides: DialectOverrides, cx: &mut Context<Self>) {
        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        let requested = overrides_on(overrides, &loaded.detected);

        if requested == loaded.requested_overrides {
            return;
        }

        if requested == loaded.overrides {
            loaded.requested_overrides = requested;
            loaded.reread_generation += 1;
            loaded.reread_task = None;
            loaded.show_requested_dialect(cx);

            cx.notify();
            return;
        }

        let dialect = requested.apply(loaded.detected);

        if let Some(refusal) = dialect_refusal(dialect, self.reader_options) {
            report_error(refused_dialect_error(&title, &refusal), cx);
            self.show_requested_dialect(cx);
            return;
        }

        let summary = crate::labels::delimited_reread_failed_message(&title);

        if self.use_live_connection(summary, cx).is_err() {
            self.show_requested_dialect(cx);
            return;
        }

        let location = self.location.clone();
        let reader_options = self.reader_options;

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        loaded.requested_overrides = requested;
        loaded.reread_generation += 1;
        loaded.show_requested_dialect(cx);

        let generation = loaded.reread_generation;

        // The read is started from inside the stored task, so a reread that
        // is replaced before it runs starts no read at all. One that is
        // already reading runs to its end, because the read blocks, and its
        // result is then dropped by its generation.
        let reread_task = cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { reread_first_page(&location, dialect, reader_options) })
                .await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_reread_outcome(generation, result, cx);
                })
                .ok();
            });
        });

        // Replacing the stored task drops the reread a previous override
        // started, which stops it before it reads when it has not begun.
        if let Some(loaded) = self.loaded_mut() {
            loaded.reread_task = Some(reread_task);
        }

        cx.notify();
    }

    /// Makes the selects show the dialect asked for last, after a selection
    /// that was not taken.
    fn show_requested_dialect(&mut self, cx: &mut Context<Self>) {
        if let Some(loaded) = self.loaded() {
            loaded.show_requested_dialect(cx);
        }

        cx.notify();
    }

    /// Stores the outcome of a background reread started at `generation`.
    /// This is the first place a failure of the reread is caught, so it is
    /// reported here and only here.
    ///
    /// The outcome of a reread that a later override replaced is dropped,
    /// failure included. A failed reread gives the override up: the controls
    /// return to the dialect the table shows.
    pub(super) fn apply_reread_outcome(
        &mut self,
        generation: u64,
        result: Result<OpenedFile, OpenError>,
        cx: &mut Context<Self>,
    ) {
        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        if loaded.reread_generation != generation {
            return;
        }

        match result {
            Ok(reread) => loaded.show_reread(reread, cx),

            Err(error) => {
                loaded.requested_overrides = loaded.overrides;
                loaded.reread_task = None;
                loaded.show_requested_dialect(cx);

                let summary = crate::labels::delimited_reread_failed_message(&title);

                report_error(open_error_to_user_facing(&error, summary), cx);
            }
        }

        cx.notify();
    }
}

/// The profile of an object is not connected, which was reported.
struct ConnectionUnavailable;

/// `overrides` with every field that names the value of `detected` cleared,
/// so that only a real difference from detection counts as an override.
fn overrides_on(overrides: DialectOverrides, detected: &Dialect) -> DialectOverrides {
    DialectOverrides {
        delimiter: overrides
            .delimiter
            .filter(|delimiter| *delimiter != detected.delimiter),
        quote: overrides.quote.filter(|quote| *quote != detected.quote),
        has_header: overrides
            .has_header
            .filter(|has_header| *has_header != detected.has_header),
        encoding: overrides
            .encoding
            .filter(|encoding| *encoding != detected.encoding),
    }
}

/// Why the reader refuses `dialect`, when it does.
///
/// The reader checks a dialect when it is opened, before it reads anything,
/// so opening it over no bytes answers without touching the file.
fn dialect_refusal(dialect: Dialect, reader_options: ReaderOptions) -> Option<ReadError> {
    PagedReader::open(MemorySource::new(Vec::new()), dialect, reader_options).err()
}

/// The user-facing error of a dialect the reader refuses: the file named
/// `file_name` was read, and the dialect is the user's to change.
pub(super) fn refused_dialect_error(file_name: &str, refusal: &ReadError) -> UserFacingError {
    let summary = crate::labels::delimited_reread_failed_message(file_name);
    let cause = crate::labels::delimited_refused_dialect_cause(refusal)
        .unwrap_or_else(|| refusal.to_string());

    UserFacingError::new(ErrorKind::User, summary).with_cause(cause)
}

/// The last `/`-separated component of an object key.
fn object_leaf(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

/// The extension of `file_name`: the text after its last dot, in the letter
/// case it is written in. A name without a dot, or with only a leading one,
/// has none.
pub(super) fn extension_hint(file_name: &str) -> Option<&str> {
    Path::new(file_name)
        .extension()
        .and_then(|extension| extension.to_str())
}

/// Whether a sample of `sample_length` bytes read from the start of a source
/// of `source_length` bytes is all of it.
pub(super) fn sample_coverage(sample_length: usize, source_length: u64) -> SampleCoverage {
    if sample_length as u64 >= source_length {
        SampleCoverage::WholeFile
    } else {
        SampleCoverage::Prefix
    }
}

/// The text of `warning` for a file detected as `detected` and read with
/// `dialect`.
fn warning_text(warning: DelimitedWarning, dialect: &Dialect, detected: &Dialect) -> String {
    match warning {
        DelimitedWarning::DetectedDelimiterUnreadable => {
            crate::labels::delimited_unreadable_delimiter_warning(
                detected.delimiter,
                dialect.encoding.name(),
                dialect.delimiter,
            )
        }

        DelimitedWarning::MalformedText => crate::labels::delimited_malformed_text_warning(
            dialect.encoding.name(),
            dialect.encoding != detected.encoding,
        ),

        DelimitedWarning::CannotSaveInPlace => {
            dbflux_i18n::t!("document.delimited.warning.cannot_save_in_place")
        }
    }
}

fn status_items(dialect: &Dialect, page_model: &PageModel) -> Vec<SharedString> {
    vec![
        crate::labels::delimited_delimiter_status(dialect.delimiter).into(),
        crate::labels::delimited_encoding_status(dialect.encoding.name()).into(),
        crate::labels::delimited_record_count_status(
            page_model.records().len(),
            page_model.record_count(),
        )
        .into(),
    ]
}

/// Opens the file at `location`, detects its dialect from a leading sample
/// and reads its first page. Blocks on file or network I/O.
pub(super) fn open_first_page(
    location: &DelimitedLocation,
    extension: Option<&str>,
    reader_options: ReaderOptions,
) -> Result<OpenedFile, OpenError> {
    let (source, version) = open_source(location)?;

    let length = source.byte_length()?;
    let sample = source.read_range(0..length.min(SAMPLE_BYTES))?;

    let detected = detect_dialect(&sample, sample_coverage(sample.len(), length), extension);
    let dialect = readable_dialect(detected, reader_options)?;

    let opened = read_first_page(source, version, dialect, reader_options)?;

    Ok(OpenedFile { detected, ..opened })
}

/// `detected` when the reader accepts it. When it refuses it, `detected`
/// with the first delimiter the reader accepts in its place, so that a file
/// whose detected delimiter cannot be scanned in its encoding still opens
/// and its dialect can be overridden.
///
/// # Errors
///
/// The refusal of `detected` when no delimiter makes it readable, which is
/// an encoding the reader cannot scan at all. Detection reports no such
/// encoding.
pub(super) fn readable_dialect(
    detected: Dialect,
    reader_options: ReaderOptions,
) -> Result<Dialect, ReadError> {
    let Some(refusal) = dialect_refusal(detected, reader_options) else {
        return Ok(detected);
    };

    DELIMITERS
        .iter()
        .map(|delimiter| Dialect {
            delimiter: *delimiter,
            ..detected
        })
        .find(|dialect| dialect_refusal(*dialect, reader_options).is_none())
        .ok_or(refusal)
}

/// Opens the file at `location` again and reads its first page with
/// `dialect`. Blocks on file or network I/O.
///
/// The source is a new one, with the version the file has now. The reader
/// that is in use keeps its own source, so a failure here costs nothing.
fn reread_first_page(
    location: &DelimitedLocation,
    dialect: Dialect,
    reader_options: ReaderOptions,
) -> Result<OpenedFile, OpenError> {
    let (source, version) = open_source(location)?;

    read_first_page(source, version, dialect, reader_options)
}

/// Opens a reader over `source` with `dialect` and reads the first page.
/// Blocks on file or network I/O.
pub(super) fn read_first_page(
    source: DelimitedSource,
    version: SourceVersion,
    dialect: Dialect,
    reader_options: ReaderOptions,
) -> Result<OpenedFile, OpenError> {
    let mut reader = PagedReader::open(source, dialect, reader_options)?;

    let first = read_page(&mut reader, 0, reader_options.page_size)?;

    let mut page_model = PageModel::new(reader.header().cloned());
    page_model.append_page(first.page, first.record_count)?;

    Ok(OpenedFile {
        detected: dialect,
        dialect,
        version,
        reader,
        page_model,
    })
}

/// Reads page `page_index` through `reader`, whose pages hold `page_size`
/// records. Blocks on file or network I/O.
pub(super) fn read_page(
    reader: &mut PagedReader<DelimitedSource>,
    page_index: usize,
    page_size: NonZeroUsize,
) -> Result<ReadPage, OpenError> {
    let page = reader.read_page(page_index)?;
    let record_count = settled_record_count(&page, reader.record_count(), page_size);

    Ok(ReadPage { page, record_count })
}

/// The record count to keep with `page`, after whose read the reader
/// reported `reported`.
///
/// A page with fewer records than `page_size` is the last one: the reader
/// found no record after it. The reader reports a total only when its scan
/// stopped at the last byte of the source, so a short or empty page it left
/// open is settled here. The file then counts as fully loaded with the read
/// that found its end, and no further, empty, page is ever asked for.
pub(super) fn settled_record_count(
    page: &Page,
    reported: RecordCount,
    page_size: NonZeroUsize,
) -> RecordCount {
    match reported {
        RecordCount::IndexedSoFar(_) if page.records.len() < page_size.get() => {
            RecordCount::Total(page.first_record.saturating_add(page.records.len() as u64))
        }

        reported => reported,
    }
}

/// The user-facing error of a failed open or page read. `summary` names the
/// file, and the cause is the driver's, the storage layer's or the reader's
/// own message.
///
/// A refusal by the object store, at `head_object` or at a range read, keeps
/// the driver's formatted error. A dialect the reader refuses is the user's
/// to change, because the file itself was read. Everything else is a file
/// that could not be read.
pub(super) fn open_error_to_user_facing(error: &OpenError, summary: String) -> UserFacingError {
    match error {
        OpenError::Storage(StorageError::ObjectStore { source, .. })
        | OpenError::Driver(source) => {
            let driver_error = db_error_to_user_facing(source);
            let cause = match driver_error.cause {
                Some(cause) => format!("{}\n{cause}", driver_error.summary),
                None => driver_error.summary,
            };

            UserFacingError::new(ErrorKind::Driver, summary).with_cause(cause)
        }

        OpenError::Read(
            ReadError::UnsupportedDialect { .. }
            | ReadError::LineBreakInDialect { .. }
            | ReadError::QuoteEqualsDelimiter { .. },
        ) => UserFacingError::new(ErrorKind::User, summary).with_cause(error.to_string()),

        OpenError::Storage(_) | OpenError::Read(_) | OpenError::PageModel(_) => {
            UserFacingError::new(ErrorKind::Storage, summary).with_cause(error.to_string())
        }
    }
}
