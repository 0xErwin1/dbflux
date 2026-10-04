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

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_components::components::data_table::{
    DataTable, DataTableEvent, DataTableState, ModelSwap,
};
use dbflux_core::{Connection, DbError};
use dbflux_delimited::{
    ByteSource, Dialect, Page, PagedReader, ReadError, ReaderOptions, RecordCount, SampleCoverage,
    SourceError, detect_dialect,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::page_model::{PageModel, PageModelError};
use super::source::{DelimitedLocation, DelimitedSource, SourceVersion, StorageError, open_source};
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
    /// The dialect detection resolved when the file was opened.
    dialect: Dialect,

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

        if self.page_model.had_replacements()
            && !self.warnings.contains(&DelimitedWarning::MalformedText)
        {
            let warning = DelimitedWarning::MalformedText;

            self.warnings.insert(0, warning);
            self.warning_items
                .insert(0, warning_text(warning, &self.dialect).into());
        }
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

    /// The dialect detected when the file was opened, once it is loaded.
    pub fn dialect(&self) -> Option<Dialect> {
        self.loaded().map(|loaded| loaded.dialect)
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
            dialect,
            version,
            reader,
            page_model,
        } = opened;

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
        self._subscriptions = vec![sort_subscription];

        let table = cx.new(|cx| DataTable::new("delimited-table", table_state.clone(), cx));

        let mut warnings = Vec::new();

        if page_model.had_replacements() {
            warnings.push(DelimitedWarning::MalformedText);
        }

        if !version.detects_same_length_change() {
            warnings.push(DelimitedWarning::CannotSaveInPlace);
        }

        let warning_items = warnings
            .iter()
            .map(|warning| warning_text(*warning, &dialect).into())
            .collect();

        let status_items = status_items(&dialect, &page_model);

        LoadedFile {
            dialect,
            version,
            reader: Some(reader),
            page_model,
            warnings,
            warning_items,
            table_state,
            table,
            status_items,
        }
    }

    // -- Further pages -------------------------------------------------------

    /// Reads the next page on the background executor and appends it to the
    /// table. Does nothing while a page is being read, and once every record
    /// is loaded.
    ///
    /// An object is read through the live connection of its profile, resolved
    /// here for every page: the one the file was opened with is dead after a
    /// disconnect or a reconnect. A profile that is not connected is reported
    /// and nothing is read.
    pub fn load_more(&mut self, cx: &mut Context<Self>) {
        if !self.has_more_records() || self.is_loading_more() {
            return;
        }

        let connection = match &self.file {
            DelimitedFileKey::Local { .. } => None,

            DelimitedFileKey::Object { profile_id, .. } => {
                let Some(connection) = self.live_connection(*profile_id, cx) else {
                    let summary = crate::labels::delimited_load_more_failed_message(&self.title());
                    let cause =
                        dbflux_i18n::t!("document.object_browser.error.connection_unavailable");

                    report_error(
                        UserFacingError::new(ErrorKind::User, summary).with_cause(cause),
                        cx,
                    );
                    return;
                };

                Some(connection)
            }
        };

        let page_size = self.reader_options.page_size;

        let DelimitedPhase::Loaded(loaded) = &mut self.phase else {
            return;
        };
        let Some(mut reader) = loaded.reader.take() else {
            return;
        };
        let page_index = loaded.page_model.next_page();

        if let Some(connection) = connection {
            reader.source_mut().use_connection(connection.clone());

            if let DelimitedLocation::Object {
                connection: opened_with,
                ..
            } = &mut self.location
            {
                *opened_with = connection;
            }
        }

        let task = cx.background_executor().spawn(async move {
            let result = read_page(&mut reader, page_index, page_size);

            (reader, result)
        });

        cx.spawn(async move |this, cx| {
            let (reader, result) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_page_outcome(reader, result, cx);
                })
                .ok();
            });
        })
        .detach();

        cx.notify();
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
    fn apply_page_outcome(
        &mut self,
        reader: PagedReader<DelimitedSource>,
        result: Result<ReadPage, OpenError>,
        cx: &mut Context<Self>,
    ) {
        let title = self.title();

        let DelimitedPhase::Loaded(loaded) = &mut self.phase else {
            return;
        };

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

fn warning_text(warning: DelimitedWarning, dialect: &Dialect) -> String {
    match warning {
        DelimitedWarning::MalformedText => {
            crate::labels::delimited_malformed_text_warning(dialect.encoding.name())
        }

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

    let dialect = detect_dialect(&sample, sample_coverage(sample.len(), length), extension);

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
