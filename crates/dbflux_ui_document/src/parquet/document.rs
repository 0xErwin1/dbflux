//! The Parquet file tab: a Parquet file shown read-only as a table of the
//! row windows loaded so far.
//!
//! Opening runs on the background executor: the source is opened, the footer
//! and its statistics are read, the default projection picks the leading
//! columns that fit the read budget of one page, and the first window of rows
//! is decoded. The document shows a loading notice until that arrives and
//! the error when it fails.
//!
//! A further window is read on request, also on the background executor. The
//! source moves into the read and comes back with the result, so no other
//! window can be asked for meanwhile. Before each window the version of the
//! file is read again: rows of another version never join the ones shown.

use std::fmt;
use std::sync::Arc;

use dbflux_byte_source::SourceError;
use dbflux_components::components::data_table::{
    DataTable, DataTableEvent, DataTableState, HeaderAnnotation,
};
use dbflux_core::{
    Connection, DEFAULT_PAGE_ROWS, DEFAULT_PROJECTION_BUDGET_BYTES, DEFAULT_PROJECTION_MAX_COLUMNS,
    DbError, ProfileSource, TableProfile, default_projection,
};
use dbflux_parquet::{
    CellPage, ParquetError, ParquetFile, RowWindow, cells_of, column_statistics, read_estimate,
    read_window,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::page_model::{ParquetPageModel, column_profile, header_annotation};
use crate::dedup::FileDocumentKey;
use crate::file_source::{
    FileLocation, LocationSource, SourceVersion, StorageError, has_changed_since, open_source,
};
use crate::handle::DocumentEvent;
use crate::object_text::db_error_to_user_facing;
use crate::types::{DocumentId, DocumentState};

/// Why a Parquet file could not be opened, or a further window of it could
/// not be read.
#[derive(Debug)]
pub(super) enum OpenError {
    /// The file or object could not be reached or read.
    Storage(StorageError),

    /// The object store refused a range read. The driver's error is kept as
    /// it came, without the wrapping of the source it was read through.
    Driver(Box<DbError>),

    /// The bytes could not be read as Parquet, or this build cannot decode
    /// them.
    Parquet(ParquetError),

    /// The file is not the version that was opened.
    SourceChanged,
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => fmt::Display::fmt(error, formatter),
            Self::Driver(error) => fmt::Display::fmt(error, formatter),
            Self::Parquet(error) => formatter.write_str(&crate::labels::parquet_error_cause(error)),
            Self::SourceChanged => {
                formatter.write_str(&dbflux_i18n::t!("document.parquet.error.source_changed"))
            }
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

impl From<ParquetError> for OpenError {
    fn from(error: ParquetError) -> Self {
        match error {
            ParquetError::Source(source) => Self::from(source),
            other => Self::Parquet(other),
        }
    }
}

/// What the background open hands to the foreground.
pub(super) enum OpenedFile {
    /// A file that holds at least one row, with its first window read.
    Rows(Box<OpenedRows>),

    /// A file without rows.
    Empty,
}

pub(super) struct OpenedRows {
    version: SourceVersion,
    source: LocationSource,
    file: ParquetFile,
    /// The top-level columns shown, in schema order.
    columns: Vec<usize>,
    /// The second header line of each shown column.
    annotations: Vec<Option<HeaderAnnotation>>,
    total_columns: usize,
    page_model: ParquetPageModel,
}

/// A file whose first window is loaded.
pub(super) struct LoadedFile {
    /// The version of the file the shown rows belong to.
    version: SourceVersion,

    /// The source the windows are read through. `None` while a window is
    /// being read: the background read has it and hands it back.
    source: Option<LocationSource>,

    file: ParquetFile,
    columns: Vec<usize>,
    total_columns: usize,
    page_model: ParquetPageModel,

    /// Set when a window found the file changed: no further window is read
    /// until the file is opened again.
    source_changed: bool,

    table_state: Entity<DataTableState>,
    table: Entity<DataTable>,
}

pub(super) enum ParquetPhase {
    Loading,
    Failed(String),
    Empty,
    Loaded(Box<LoadedFile>),
}

/// A Parquet file opened read-only as a table.
pub struct ParquetDocument {
    id: DocumentId,
    focus_handle: FocusHandle,
    file: FileDocumentKey,
    location: FileLocation,

    /// The application state the live connection of an object's profile is
    /// resolved from for every window. `None` for a local file.
    app_state: Option<Entity<AppStateEntity>>,

    phase: ParquetPhase,

    /// Counts the opens of the file. An open carries the count it was started
    /// at, and its result is dropped when a reload started another meanwhile.
    open_generation: u64,

    /// Set when the first window arrives, so the next render hands the
    /// keyboard to the table if the loading notice held it.
    pending_table_focus: bool,

    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for ParquetDocument {}

impl ParquetDocument {
    /// Opens the local file at `path`.
    pub fn open_local(path: std::path::PathBuf, cx: &mut Context<Self>) -> Self {
        let file = FileDocumentKey::Local { path: path.clone() };
        let location = FileLocation::Local { path };

        Self::open(file, location, None, cx)
    }

    /// Opens the object `key` of `bucket` through `connection`, the live
    /// connection of the profile `profile_id` in `app_state`. Further windows
    /// are read through the connection the profile has when they are asked
    /// for.
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
        let location = FileLocation::Object {
            connection,
            bucket,
            key,
        };

        Self::open(file, location, Some(app_state), cx)
    }

    fn open(
        file: FileDocumentKey,
        location: FileLocation,
        app_state: Option<Entity<AppStateEntity>>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut document = Self {
            id: DocumentId::new(),
            focus_handle: cx.focus_handle(),
            file,
            location,
            app_state,
            phase: ParquetPhase::Loading,
            open_generation: 0,
            pending_table_focus: false,
            _subscriptions: Vec::new(),
        };

        document.load_first_window(cx);
        document
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// The identity this file is deduplicated by.
    pub fn file(&self) -> &FileDocumentKey {
        &self.file
    }

    /// The file name: the last component of the local path or of the object
    /// key.
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
        match &self.phase {
            ParquetPhase::Loading => DocumentState::Loading,
            ParquetPhase::Failed(_) => DocumentState::Error,
            ParquetPhase::Empty | ParquetPhase::Loaded(_) => DocumentState::Clean,
        }
    }

    /// The profile whose connection reads the object, and `None` for a local
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

    /// Table navigation runs inside the embedded `DataTable` through its own
    /// key context. The commands of the document are the next window and the
    /// reload (`RefreshSchema`), which opens the file again.
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

            dbflux_app::keymap::Command::RefreshSchema if self.can_reload() => {
                self.reload(cx);
                true
            }

            _ => false,
        }
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match &self.phase {
            ParquetPhase::Loaded(loaded) => {
                let handle = loaded.table_state.read(cx).focus_handle().clone();
                handle.focus(window, cx);
            }

            ParquetPhase::Loading | ParquetPhase::Failed(_) | ParquetPhase::Empty => {
                self.focus_handle.focus(window, cx);
            }
        }
    }

    /// Why the file could not be opened, when it could not.
    pub fn failure(&self) -> Option<&str> {
        match &self.phase {
            ParquetPhase::Failed(cause) => Some(cause),
            _ => None,
        }
    }

    /// Whether the file was opened and holds no rows.
    pub fn is_empty_file(&self) -> bool {
        matches!(self.phase, ParquetPhase::Empty)
    }

    /// The state of the table of loaded rows, once the file is loaded.
    pub fn table_state(&self) -> Option<&Entity<DataTableState>> {
        self.loaded().map(|loaded| &loaded.table_state)
    }

    /// The top-level columns shown, by index in the file's schema order.
    /// Empty until the file is loaded.
    pub fn shown_columns(&self) -> &[usize] {
        self.loaded()
            .map_or(&[], |loaded| loaded.columns.as_slice())
    }

    /// How many rows are loaded and how many the file holds, once loaded.
    pub fn row_counts(&self) -> Option<(u64, u64)> {
        self.loaded().map(|loaded| {
            (
                loaded.page_model.loaded_rows(),
                loaded.page_model.total_rows(),
            )
        })
    }

    /// Whether a window found the file changed since it was opened.
    pub fn source_changed(&self) -> bool {
        self.loaded().is_some_and(|loaded| loaded.source_changed)
    }

    pub fn has_more_rows(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| !loaded.page_model.is_fully_loaded())
    }

    /// Whether a further window is being read.
    pub fn is_loading_more(&self) -> bool {
        self.loaded().is_some_and(|loaded| loaded.source.is_none())
    }

    /// Whether the next window can be asked for now: the file has more rows,
    /// none is being read, and the file was not found changed.
    pub fn can_load_more(&self) -> bool {
        self.has_more_rows() && !self.is_loading_more() && !self.source_changed()
    }

    /// Whether the file can be opened again now: it is not being opened.
    pub fn can_reload(&self) -> bool {
        !matches!(self.phase, ParquetPhase::Loading)
    }

    /// The status line: the rows loaded of the file's total and, when the
    /// default projection left columns out, how many are shown.
    pub fn status_items(&self) -> Vec<SharedString> {
        let Some(loaded) = self.loaded() else {
            return Vec::new();
        };

        let mut items = vec![
            crate::labels::parquet_row_count_status(
                loaded.page_model.loaded_rows(),
                loaded.page_model.total_rows(),
            )
            .into(),
        ];

        if loaded.columns.len() < loaded.total_columns {
            items.push(
                crate::labels::parquet_column_count_status(
                    loaded.columns.len(),
                    loaded.total_columns,
                )
                .into(),
            );
        }

        items
    }

    pub(super) fn phase(&self) -> &ParquetPhase {
        &self.phase
    }

    pub(super) fn table(&self) -> Option<&Entity<DataTable>> {
        self.loaded().map(|loaded| &loaded.table)
    }

    pub(super) fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Whether the first window arrived since the last render. Reading it
    /// clears it.
    pub(super) fn take_pending_table_focus(&mut self) -> bool {
        std::mem::take(&mut self.pending_table_focus)
    }

    fn loaded(&self) -> Option<&LoadedFile> {
        match &self.phase {
            ParquetPhase::Loaded(loaded) => Some(loaded),
            _ => None,
        }
    }

    fn loaded_mut(&mut self) -> Option<&mut LoadedFile> {
        match &mut self.phase {
            ParquetPhase::Loaded(loaded) => Some(loaded),
            _ => None,
        }
    }

    // -- Opening -------------------------------------------------------------

    /// Opens the file again from its first window, dropping the rows shown.
    /// This is how a file changed elsewhere is seen again.
    pub fn reload(&mut self, cx: &mut Context<Self>) {
        if !self.can_reload() {
            return;
        }

        let summary = crate::labels::parquet_open_failed_message(&self.title());

        if self.use_live_connection(summary, cx).is_err() {
            return;
        }

        self.load_first_window(cx);
    }

    fn load_first_window(&mut self, cx: &mut Context<Self>) {
        self.phase = ParquetPhase::Loading;
        self._subscriptions.clear();
        self.open_generation += 1;
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();

        let generation = self.open_generation;
        let location = self.location.clone();

        let task = cx
            .background_executor()
            .spawn(async move { open_first_window(&location, DEFAULT_PAGE_ROWS) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_open_outcome(generation, result, cx)
                })
                .ok();
            });
        })
        .detach();
    }

    /// Stores the outcome of the background open. This is the first place a
    /// failure of the open is caught, so it is reported here and only here.
    fn apply_open_outcome(
        &mut self,
        generation: u64,
        result: Result<OpenedFile, OpenError>,
        cx: &mut Context<Self>,
    ) {
        if generation != self.open_generation {
            return;
        }

        match result {
            Ok(OpenedFile::Rows(opened)) => {
                self.phase = ParquetPhase::Loaded(Box::new(self.build_loaded(*opened, cx)));
                self.pending_table_focus = true;
            }

            Ok(OpenedFile::Empty) => {
                self.phase = ParquetPhase::Empty;
            }

            Err(error) => {
                let summary = crate::labels::parquet_open_failed_message(&self.title());
                let cause = error.to_string();

                report_error(open_error_to_user_facing(&error, summary), cx);

                self.phase = ParquetPhase::Failed(cause);
            }
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    fn build_loaded(&mut self, opened: OpenedRows, cx: &mut Context<Self>) -> LoadedFile {
        let OpenedRows {
            version,
            source,
            file,
            columns,
            annotations,
            total_columns,
            page_model,
        } = opened;

        let model = Arc::new(page_model.table_model());

        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(model, cx);
            state.set_header_annotations(annotations, cx);
            state
        });

        self._subscriptions = vec![Self::subscribe_to_table(&table_state, cx)];

        let table = cx.new(|cx| DataTable::new("parquet-table", table_state.clone(), cx));

        LoadedFile {
            version,
            source: Some(source),
            file,
            columns,
            total_columns,
            page_model,
            source_changed: false,
            table_state,
            table,
        }
    }

    /// The rows are the file's rows in file order and only some of them are
    /// loaded, so sorting them would misrepresent the file. A header click
    /// sets the sort indicator before it reports the change, and the
    /// indicator is cleared again here.
    fn subscribe_to_table(
        table_state: &Entity<DataTableState>,
        cx: &mut Context<Self>,
    ) -> Subscription {
        cx.subscribe(
            table_state,
            |_this, table_state, event: &DataTableEvent, cx| {
                if matches!(event, DataTableEvent::SortChanged(Some(_))) {
                    table_state.update(cx, |state, cx| {
                        state.clear_sort_without_emit();
                        cx.notify();
                    });
                }
            },
        )
    }

    // -- Further windows -----------------------------------------------------

    /// Reads the next window of rows on the background executor and appends
    /// it to the table, after checking that the file is still the version
    /// that was opened. Does nothing while a window is being read, once every
    /// row is loaded, and once the file was found changed.
    ///
    /// An object is read through the live connection of its profile, resolved
    /// here for every window. A profile that is not connected is reported and
    /// nothing is read.
    pub fn load_more(&mut self, cx: &mut Context<Self>) {
        if !self.can_load_more() {
            return;
        }

        let summary = crate::labels::parquet_load_more_failed_message(&self.title());

        let Ok(connection) = self.use_live_connection(summary, cx) else {
            return;
        };

        let location = self.location.clone();
        let generation = self.open_generation;

        let Some(loaded) = self.loaded_mut() else {
            return;
        };
        let Some(mut source) = loaded.source.take() else {
            return;
        };

        if let Some(connection) = connection {
            source.use_connection(connection);
        }

        let version = loaded.version.clone();
        let file = loaded.file.clone();
        let columns = loaded.columns.clone();
        let window = RowWindow::new(loaded.page_model.loaded_rows(), DEFAULT_PAGE_ROWS);

        let task = cx.background_executor().spawn(async move {
            let result = read_next_window(&location, &version, &source, &file, window, &columns);

            (source, result)
        });

        cx.spawn(async move |this, cx| {
            let (source, result) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_window_outcome(generation, source, result, cx);
                })
                .ok();
            });
        })
        .detach();

        cx.notify();
    }

    /// Takes the source back and stores the outcome of the background window
    /// read. This is the first place a failure of the read is caught, so it
    /// is reported here and only here. A failed read leaves the loaded rows as
    /// they are; a file found changed reads no further window until it is
    /// opened again.
    ///
    /// `generation` is the open the window was read for. When the file was
    /// opened again meanwhile, the window belongs to rows no longer shown and
    /// is dropped without a report.
    fn apply_window_outcome(
        &mut self,
        generation: u64,
        source: LocationSource,
        result: Result<CellPage, OpenError>,
        cx: &mut Context<Self>,
    ) {
        if generation != self.open_generation {
            return;
        }

        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        loaded.source = Some(source);

        match result {
            Ok(page) => {
                loaded.page_model.append(page);

                let model = Arc::new(loaded.page_model.table_model());

                loaded.table_state.update(cx, |state, cx| {
                    state.set_model(
                        model,
                        dbflux_components::components::data_table::ModelSwap::KeepCursor,
                        cx,
                    );
                });
            }

            Err(error) => {
                if matches!(error, OpenError::SourceChanged) {
                    loaded.source_changed = true;
                }

                let summary = crate::labels::parquet_load_more_failed_message(&title);

                report_error(open_error_to_user_facing(&error, summary), cx);
            }
        }

        cx.notify();
    }

    /// Resolves the live connection of an object's profile and makes the
    /// document's location read through it. `Ok(None)` for a local file.
    ///
    /// A profile that is not connected is reported here under `summary`, and
    /// the caller reads nothing.
    fn use_live_connection(
        &mut self,
        summary: String,
        cx: &mut Context<Self>,
    ) -> Result<Option<Arc<dyn Connection>>, ConnectionUnavailable> {
        let FileDocumentKey::Object { profile_id, .. } = &self.file else {
            return Ok(None);
        };

        let connection = self.app_state.as_ref().and_then(|app_state| {
            app_state
                .read(cx)
                .connections()
                .get(profile_id)
                .map(|connected| connected.connection.clone())
        });

        let Some(connection) = connection else {
            let cause = dbflux_i18n::t!("document.object_browser.error.connection_unavailable");

            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(cause),
                cx,
            );

            return Err(ConnectionUnavailable);
        };

        if let FileLocation::Object {
            connection: opened_with,
            ..
        } = &mut self.location
        {
            *opened_with = connection.clone();
        }

        Ok(Some(connection))
    }
}

/// The profile of an object is not connected, which was reported.
struct ConnectionUnavailable;

/// Opens the file at `location`, reads its footer and statistics, picks the
/// default projection and reads the first window of `page_rows` rows of it.
/// Blocks on file or network I/O.
pub(super) fn open_first_window(
    location: &FileLocation,
    page_rows: u64,
) -> Result<OpenedFile, OpenError> {
    let (source, version) = open_source(location)?;

    let file = dbflux_parquet::open(&source)?;

    // A file without rows may have no row groups at all, which the window
    // and estimate readers are never asked about.
    if file.row_count() == 0 {
        return Ok(OpenedFile::Empty);
    }

    let total_columns = file.schema().fields().len();

    let statistics = column_statistics(&file)?;
    let profiles: Vec<_> = statistics.iter().map(column_profile).collect();

    let window = RowWindow::new(0, page_rows);
    let columns = default_columns(&file, &profiles, window)?;

    let annotations = columns
        .iter()
        .map(|&column| {
            profiles
                .get(column)
                .map(|profile| header_annotation(profile, file.row_count()))
        })
        .collect();

    let rows = read_window(&file, &source, window, &columns)?;
    let first_page = cells_of(&rows)?;

    let page_model = ParquetPageModel::new(first_page, file.row_count());

    Ok(OpenedFile::Rows(Box::new(OpenedRows {
        version,
        source,
        file,
        columns,
        annotations,
        total_columns,
        page_model,
    })))
}

/// The leading columns in schema order whose bytes for `window` stay within
/// the default projection's budget, as the reader estimates them from the
/// footer.
fn default_columns(
    file: &ParquetFile,
    profiles: &[dbflux_core::ColumnProfile],
    window: RowWindow,
) -> Result<Vec<usize>, ParquetError> {
    let column_count = profiles.len();
    let every_column: Vec<usize> = (0..column_count).collect();

    let estimate = read_estimate(file, window, &every_column)?;

    let mut window_bytes = vec![None; column_count];

    for (column, bytes) in estimate.per_column {
        if let Some(slot) = window_bytes.get_mut(column) {
            *slot = Some(bytes);
        }
    }

    let profile = TableProfile {
        columns: profiles.to_vec(),
        row_count: Some(file.row_count()),
        total_compressed_bytes: None,
        total_uncompressed_bytes: None,
        source_label: ProfileSource::FileFooter,
    };

    let projection = default_projection(
        &profile,
        &window_bytes,
        DEFAULT_PROJECTION_BUDGET_BYTES,
        DEFAULT_PROJECTION_MAX_COLUMNS,
    );

    Ok(projection.selected_indices())
}

/// Checks that the file at `location` is still `version`, then reads
/// `window` of `columns` through `source`. Blocks on file or network I/O.
pub(super) fn read_next_window(
    location: &FileLocation,
    version: &SourceVersion,
    source: &LocationSource,
    file: &ParquetFile,
    window: RowWindow,
    columns: &[usize],
) -> Result<CellPage, OpenError> {
    if has_changed_since(location, version)? {
        return Err(OpenError::SourceChanged);
    }

    let rows = read_window(file, source, window, columns)?;

    Ok(cells_of(&rows)?)
}

/// The user-facing error of a failed open or window read. `summary` names the
/// file.
///
/// A refusal by the object store keeps the driver's formatted error. A file
/// that changed since it was opened is the user's to reload. Everything else
/// is a file that could not be read.
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

        OpenError::SourceChanged => {
            UserFacingError::new(ErrorKind::User, summary).with_cause(error.to_string())
        }

        OpenError::Storage(_) | OpenError::Parquet(_) => {
            UserFacingError::new(ErrorKind::Storage, summary).with_cause(error.to_string())
        }
    }
}
