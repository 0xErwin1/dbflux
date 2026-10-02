//! The delimited file tab: a CSV or TSV file, local or in object storage,
//! shown as a read-only table of its first page.
//!
//! Opening reads the file on the background executor and never on the
//! foreground: the source is opened, a leading sample decides the dialect, and
//! the reader returns the first page. The document shows a loading notice
//! until that arrives and the error when it fails.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_components::components::data_table::{DataTable, DataTableEvent, DataTableState};
use dbflux_core::{Connection, DbError};
use dbflux_delimited::{
    ByteSource, Dialect, PagedReader, ReadError, ReaderOptions, RecordCount, SampleCoverage,
    SourceError, detect_dialect,
};
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
pub(super) const PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(500).unwrap();

/// How many bytes the reader asks from the source in one read: a page of
/// ordinary records fits in it, so a further page of an object is one range
/// request. Opening an object costs a `head_object` and two range requests,
/// because the detection sample and the reader's first window both start at
/// offset zero.
const FETCH_WINDOW_BYTES: NonZeroU64 = NonZeroU64::new(1024 * 1024).unwrap();

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

/// Why a delimited file could not be opened.
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

    /// The first page did not fit the page model.
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

/// A file whose first page is loaded.
struct LoadedFile {
    /// The dialect detection resolved when the file was opened.
    dialect: Dialect,

    /// The version of the file the reader's byte ranges belong to.
    version: SourceVersion,

    /// The reader the first page came from, kept open over the same source
    /// for the pages that follow it.
    #[expect(
        dead_code,
        reason = "only the first page is read, and nothing asks for a second one"
    )]
    reader: PagedReader<DelimitedSource>,

    page_model: PageModel,
    warnings: Vec<DelimitedWarning>,

    /// The text of each warning, formatted when the file is loaded.
    warning_items: Vec<SharedString>,

    table_state: Entity<DataTableState>,
    table: Entity<DataTable>,

    /// The status line, formatted when the page model changes.
    status_items: Vec<SharedString>,
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
        let file = DelimitedFileKey::Local { path: path.clone() };
        let location = DelimitedLocation::Local { path };

        Self::open(file, location, cx)
    }

    /// Opens the object `key` of `bucket` through `connection`, the live
    /// connection of the profile `profile_id`.
    pub fn open_object(
        profile_id: uuid::Uuid,
        connection: Arc<dyn Connection>,
        bucket: String,
        key: String,
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

        Self::open(file, location, cx)
    }

    fn open(file: DelimitedFileKey, location: DelimitedLocation, cx: &mut Context<Self>) -> Self {
        let mut document = Self {
            id: DocumentId::new(),
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            file,
            location,
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
    /// key context, and the document has no command of its own.
    pub fn dispatch_command(
        &mut self,
        _command: dbflux_app::keymap::Command,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        false
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

        let task = cx
            .background_executor()
            .spawn(async move { open_first_page(&location, extension.as_deref()) });

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
            reader,
            page_model,
            warnings,
            warning_items,
            table_state,
            table,
            status_items,
        }
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
) -> Result<OpenedFile, OpenError> {
    let (source, version) = open_source(location)?;

    let length = source.byte_length()?;
    let sample = source.read_range(0..length.min(SAMPLE_BYTES))?;

    let dialect = detect_dialect(&sample, sample_coverage(sample.len(), length), extension);

    read_first_page(source, version, dialect)
}

/// Opens a reader over `source` with `dialect` and reads the first page.
/// Blocks on file or network I/O.
pub(super) fn read_first_page(
    source: DelimitedSource,
    version: SourceVersion,
    dialect: Dialect,
) -> Result<OpenedFile, OpenError> {
    let options = ReaderOptions {
        page_size: PAGE_SIZE,
        window_size: FETCH_WINDOW_BYTES,
    };
    let mut reader = PagedReader::open(source, dialect, options)?;

    let page = reader.read_page(0)?;

    let mut page_model = PageModel::new(reader.header().cloned());
    page_model.append_page(page, reader.record_count())?;

    Ok(OpenedFile {
        dialect,
        version,
        reader,
        page_model,
    })
}

/// The user-facing error of a failed open. `summary` names the file, and the
/// cause is the driver's, the storage layer's or the reader's own message.
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
