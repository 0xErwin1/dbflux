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
//!
//! The columns read are the applied projection, which the column picker of
//! the Data view and the eye toggles of the Columns view change. A change
//! reads the file again from its first row with the new columns. Every change
//! counts as a new read generation: a window started under an older one is
//! dropped when it arrives, and the read of the latest projection starts as
//! soon as the source is back.
//!
//! An object of a store that cannot read a byte range is downloaded whole
//! once, after the user agrees in a prompt that names its size, and read from
//! that copy. The copy is a snapshot: its windows are not checked against the
//! store, and a reload compares the object's version with the one recorded at
//! the download, asking again before downloading a changed object.

use std::fmt;
use std::sync::Arc;

use dbflux_byte_source::SourceError;
use dbflux_components::components::column_profile_view::ColumnProfileView;
use dbflux_components::components::column_projection::{ColumnProjectionPicker, ProjectionChanged};
use dbflux_components::components::data_table::{
    DataTable, DataTableEvent, DataTableState, HeaderAnnotation, ModelSwap,
};
use dbflux_components::components::read_estimate_bar::ReadEstimateBar;
use dbflux_components::modals::ModalFocus;
use dbflux_core::{
    ColumnProjection, Connection, DEFAULT_PAGE_ROWS, DEFAULT_PROJECTION_BUDGET_BYTES,
    DEFAULT_PROJECTION_MAX_COLUMNS, DbError, EstimateScope, PartUnit, ProfileSource, ReadEstimate,
    TableProfile, default_projection,
};
use dbflux_parquet::{
    CellPage, ParquetError, ParquetFile, ReadEstimate as FileReadEstimate, RowWindow, cells_of,
    column_statistics, file_statistics, read_estimate, read_window,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::page_model::{ParquetPageModel, column_profile, header_annotation};
use crate::dedup::FileDocumentKey;
use crate::file_source::{
    DOWNLOAD_IN_MEMORY_LIMIT_BYTES, FileLocation, LocationSource, ObjectReads, SourceVersion,
    StorageError, download_whole_object, has_changed_since, open_source, read_version,
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
    /// What the footer says about the file and each of its columns.
    profile: TableProfile,
    /// The top-level columns shown, in schema order.
    columns: Vec<usize>,
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
    profile: Arc<TableProfile>,

    /// The top-level columns the table shows, in schema order.
    columns: Vec<usize>,

    /// The projection the picker and the Columns view show. It differs from
    /// `columns` while the file is read again with it.
    projection: ColumnProjection,

    total_columns: usize,
    page_model: ParquetPageModel,

    /// Counts the projections applied. A window carries the count it was
    /// read at, and is dropped when another projection was applied meanwhile.
    read_generation: u64,

    /// Set when the applied projection still has to be read. The read
    /// starts once the source is back from the window being read.
    reread_pending: bool,

    /// What the next window of the shown columns costs, and its strip.
    /// `None` once every row is loaded.
    estimate: Option<ReadEstimate>,
    estimate_bar: Option<ReadEstimateBar>,

    /// "M columns · R rows · size", formatted once.
    summary: SharedString,

    /// Set when a window found the file changed: no further window is read
    /// until the file is opened again.
    source_changed: bool,

    table_state: Entity<DataTableState>,
    table: Entity<DataTable>,

    /// The column picker and the Columns view. They need a window to be
    /// built, so the first render after the file is loaded builds them.
    picker: Option<Entity<ColumnProjectionPicker>>,
    profile_view: Option<Entity<ColumnProfileView>>,
}

impl LoadedFile {
    /// Works out what the next window of the shown columns costs and formats
    /// its strip. A footer the estimate cannot be read from shows no strip:
    /// the window read reports the same problem when it is asked for.
    fn refresh_estimate(&mut self) {
        self.estimate = match next_window_estimate(&self.file, &self.page_model, &self.columns) {
            Ok(estimate) => estimate,
            Err(error) => {
                log::warn!("Could not estimate the next Parquet window: {error}");
                None
            }
        };

        self.estimate_bar = self
            .estimate
            .as_ref()
            .map(|estimate| ReadEstimateBar::new(estimate, &self.profile));
    }

    /// Shows the projection applied in the picker and the Columns view.
    fn sync_projection_controls(&self, cx: &mut App) {
        if let Some(picker) = &self.picker {
            let projection = self.projection.clone();
            picker.update(cx, |picker, cx| picker.set_applied(projection, cx));
        }

        if let Some(profile_view) = &self.profile_view {
            let projection = self.projection.clone();
            profile_view.update(cx, |view, cx| view.set_applied(projection, cx));
        }
    }

    /// Makes the shown columns the applied projection again, after the read
    /// of another projection failed.
    fn revert_projection(&mut self, cx: &mut App) {
        self.projection = ColumnProjection::from_indices(self.total_columns, &self.columns);
        self.reread_pending = false;
        self.sync_projection_controls(cx);
    }

    /// Replaces the table with the first window of `columns`, dropping the
    /// cursor and the selection, which pointed at the old columns.
    fn show_columns(&mut self, columns: Vec<usize>, page: CellPage, cx: &mut App) {
        self.page_model = ParquetPageModel::new(page, self.page_model.total_rows());

        let annotations = header_annotations(&self.profile, &columns);
        let model = self.page_model.table_model();

        self.table_state.update(cx, |state, cx| {
            state.set_model(model, ModelSwap::ResetCursor, cx);
            state.set_header_annotations(annotations, cx);
        });

        self.columns = columns;
        self.refresh_estimate();
    }
}

/// The view of a loaded file the document shows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ParquetView {
    /// The rows of the shown columns.
    #[default]
    Data,
    /// One row per column with what the footer says about it.
    Columns,
}

pub(super) enum ParquetPhase {
    Loading,
    /// An object that is only read whole waits for the user to agree to the
    /// download. Nothing of it was read yet.
    AwaitingDownload,
    Failed(String),
    Empty,
    Loaded(Box<LoadedFile>),
}

/// The open question whether to download an object whole.
pub(super) struct DownloadPrompt {
    /// The object's size, as `head_object` reports it.
    size: u64,

    /// Whether the object changed since the copy whose rows are shown was
    /// downloaded, so a download replaces them.
    replaces_rows: bool,

    focus: ModalFocus,
}

impl DownloadPrompt {
    pub(super) fn focus_mut(&mut self) -> &mut ModalFocus {
        &mut self.focus
    }
}

/// Why the version of an object read whole is asked for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum VersionCheck {
    /// The document is opened: the answer is the size the prompt names.
    Open,
    /// The user asked to reload: an unchanged object is not downloaded again.
    Reload,
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

    /// How an object is read. `None` for a local file.
    reads: Option<ObjectReads>,

    phase: ParquetPhase,

    /// The question whether to download an object whole, while it is open.
    download_prompt: Option<DownloadPrompt>,

    /// Set while the version of an object read whole is asked for.
    checking_version: bool,

    view: ParquetView,

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
    /// The `tab_kind` a local file is recorded under in the workspace session.
    pub const SESSION_TAB_KIND: &'static str = "Parquet";

    /// Opens the local file at `path`.
    pub fn open_local(path: std::path::PathBuf, cx: &mut Context<Self>) -> Self {
        let file = FileDocumentKey::Local { path: path.clone() };
        let location = FileLocation::Local { path };

        Self::open(file, location, None, None, cx)
    }

    /// Opens the object `key` of `bucket` through `connection`, the live
    /// connection of the profile `profile_id` in `app_state`. Further windows
    /// are read through the connection the profile has when they are asked
    /// for.
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
            app_state,
            reads,
            phase: ParquetPhase::Loading,
            download_prompt: None,
            checking_version: false,
            view: ParquetView::Data,
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
            ParquetPhase::AwaitingDownload | ParquetPhase::Empty | ParquetPhase::Loaded(_) => {
                DocumentState::Clean
            }
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
    /// key context, and the column picker and the Columns view handle the
    /// keys of their lists. The commands of the document are the next window,
    /// the reload (`RefreshSchema`), which opens the file again, the switch
    /// between Data and Columns (`CycleDocumentView`), the column picker
    /// (`FocusToolbar`, in Data), and the sort and the filter of the Columns
    /// view (`CycleResultView` and `FocusSearch`, in Columns).
    pub fn dispatch_command(
        &mut self,
        command: dbflux_app::keymap::Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use dbflux_app::keymap::Command;

        let loaded = self.loaded().is_some();

        match command {
            Command::ResultsNextPage if loaded => {
                self.load_more(cx);
                true
            }

            Command::RefreshSchema if self.can_reload() => {
                self.reload(cx);
                true
            }

            Command::CycleDocumentView if loaded => {
                let next = match self.view {
                    ParquetView::Data => ParquetView::Columns,
                    ParquetView::Columns => ParquetView::Data,
                };

                self.show_view(next, window, cx);
                true
            }

            Command::FocusToolbar if loaded && self.view == ParquetView::Data => {
                self.open_column_picker(window, cx)
            }

            Command::FocusToolbar | Command::FocusSearch
                if loaded && self.view == ParquetView::Columns =>
            {
                self.focus_column_filter(window, cx)
            }

            Command::CycleResultView if loaded && self.view == ParquetView::Columns => {
                let Some(profile_view) = self.column_profile_view().cloned() else {
                    return false;
                };

                profile_view.update(cx, |view, cx| view.cycle_sort(cx));
                true
            }

            _ => false,
        }
    }

    /// Gives the keyboard to the view that is shown: the table, or the rows
    /// of the Columns view.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(prompt) = &mut self.download_prompt {
            prompt.focus.focus(None, window, cx);
            return;
        }

        match &self.phase {
            ParquetPhase::Loaded(loaded) => {
                if self.view == ParquetView::Columns
                    && let Some(profile_view) = loaded.profile_view.clone()
                {
                    profile_view.update(cx, |view, cx| view.focus(window, cx));
                    return;
                }

                let handle = loaded.table_state.read(cx).focus_handle().clone();
                handle.focus(window, cx);
            }

            ParquetPhase::Loading
            | ParquetPhase::AwaitingDownload
            | ParquetPhase::Failed(_)
            | ParquetPhase::Empty => {
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

    /// Whether the file can be opened again now: it is not being opened, its
    /// version is not being asked for, and no download is waiting for an
    /// answer.
    pub fn can_reload(&self) -> bool {
        !matches!(
            self.phase,
            ParquetPhase::Loading | ParquetPhase::AwaitingDownload
        ) && !self.checking_version
            && self.download_prompt.is_none()
    }

    /// How an object is read, and `None` for a local file.
    pub fn object_reads(&self) -> Option<ObjectReads> {
        self.reads
    }

    /// What the open download prompt asks: the object's name, its size, and
    /// that all of it is downloaded once. `None` while no prompt is open.
    pub fn download_prompt_message(&self) -> Option<String> {
        let prompt = self.download_prompt.as_ref()?;
        let title = self.title();

        Some(if prompt.replaces_rows {
            crate::labels::parquet_download_changed_body(&title, prompt.size)
        } else {
            crate::labels::parquet_download_body(&title, prompt.size)
        })
    }

    pub(super) fn download_prompt_mut(&mut self) -> Option<&mut DownloadPrompt> {
        self.download_prompt.as_mut()
    }

    /// Closes the download prompt and downloads the object whole, then shows
    /// its first window. Rows shown from an older copy are dropped. Does
    /// nothing when no prompt is open, and reports a profile that is not
    /// connected without downloading.
    pub fn confirm_download(&mut self, cx: &mut Context<Self>) {
        let Some(mut prompt) = self.download_prompt.take() else {
            return;
        };

        prompt.focus.restore(cx);

        let summary = crate::labels::parquet_open_failed_message(&self.title());

        if self.use_live_connection(summary, cx).is_err() {
            if matches!(self.phase, ParquetPhase::AwaitingDownload) {
                self.phase = ParquetPhase::Failed(dbflux_i18n::t!(
                    "document.object_browser.error.connection_unavailable"
                ));
                cx.emit(DocumentEvent::MetaChanged);
            }

            cx.notify();
            return;
        }

        let FileLocation::Object {
            connection,
            bucket,
            key,
        } = self.location.clone()
        else {
            return;
        };

        self.start_open(
            move || open_downloaded(connection.as_ref(), &bucket, &key, DEFAULT_PAGE_ROWS),
            cx,
        );
    }

    /// Closes the download prompt without downloading. When nothing of the
    /// object was shown yet, the tab asks to be closed: declining the
    /// download declines the open.
    pub fn dismiss_download_prompt(&mut self, cx: &mut Context<Self>) {
        let Some(mut prompt) = self.download_prompt.take() else {
            return;
        };

        prompt.focus.restore(cx);

        if matches!(self.phase, ParquetPhase::AwaitingDownload) {
            cx.emit(DocumentEvent::RequestClose);
        }

        cx.notify();
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

    pub(super) fn view(&self) -> ParquetView {
        self.view
    }

    /// What the footer says about the file, once it is loaded.
    #[cfg(test)]
    pub(super) fn profile(&self) -> Option<&Arc<TableProfile>> {
        self.loaded().map(|loaded| &loaded.profile)
    }

    /// The projection the picker and the Columns view show, once loaded.
    #[cfg(test)]
    pub(super) fn applied_projection(&self) -> Option<&ColumnProjection> {
        self.loaded().map(|loaded| &loaded.projection)
    }

    pub(super) fn projection_picker(&self) -> Option<&Entity<ColumnProjectionPicker>> {
        self.loaded().and_then(|loaded| loaded.picker.as_ref())
    }

    pub(super) fn column_profile_view(&self) -> Option<&Entity<ColumnProfileView>> {
        self.loaded()
            .and_then(|loaded| loaded.profile_view.as_ref())
    }

    /// What the next window of the shown columns costs. `None` once every
    /// row is loaded.
    #[cfg(test)]
    pub(super) fn next_read_estimate(&self) -> Option<&ReadEstimate> {
        self.loaded().and_then(|loaded| loaded.estimate.as_ref())
    }

    pub(super) fn estimate_bar(&self) -> Option<&ReadEstimateBar> {
        self.loaded()
            .and_then(|loaded| loaded.estimate_bar.as_ref())
    }

    /// "M columns · R rows · size" of a loaded file.
    pub(super) fn summary(&self) -> Option<&SharedString> {
        self.loaded().map(|loaded| &loaded.summary)
    }

    /// Shows `view`. Leaving Data throws away a draft of the column picker,
    /// which is not drawn in Columns.
    pub(super) fn show_view(
        &mut self,
        view: ParquetView,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.view == view || self.loaded().is_none() {
            return;
        }

        if let Some(picker) = self.projection_picker().cloned() {
            picker.update(cx, |picker, cx| picker.discard(window, cx));
        }

        self.view = view;
        self.focus(window, cx);
        cx.notify();
    }

    /// Opens the column picker with the keyboard on its list.
    fn open_column_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        let Some(picker) = self.projection_picker().cloned() else {
            return false;
        };

        picker.update(cx, |picker, cx| picker.open(window, cx));
        true
    }

    fn focus_column_filter(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        let Some(profile_view) = self.column_profile_view().cloned() else {
            return false;
        };

        profile_view.update(cx, |view, cx| view.focus_filter(window, cx));
        true
    }

    /// Builds the column picker and the Columns view of a loaded file that
    /// has none yet. They need the window, which the load does not have.
    pub(super) fn ensure_projection_controls(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(loaded) = self.loaded() else {
            return;
        };

        if loaded.picker.is_some() {
            return;
        }

        let profile = loaded.profile.clone();
        let projection = loaded.projection.clone();

        let picker = cx.new(|cx| {
            ColumnProjectionPicker::new(
                "parquet-column-picker",
                profile.clone(),
                projection.clone(),
                window,
                cx,
            )
        });

        let profile_view = cx.new(|cx| {
            let mut view = ColumnProfileView::new(window, cx);
            view.set_profile(profile, projection, cx);
            view
        });

        self._subscriptions.push(cx.subscribe(
            &picker,
            |this, _picker, event: &ProjectionChanged, cx| {
                this.apply_projection(event.0.clone(), cx);
            },
        ));

        self._subscriptions.push(cx.subscribe(
            &profile_view,
            |this, _view, event: &ProjectionChanged, cx| {
                this.apply_projection(event.0.clone(), cx);
            },
        ));

        if let Some(loaded) = self.loaded_mut() {
            loaded.picker = Some(picker);
            loaded.profile_view = Some(profile_view);
        }
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
    ///
    /// An object read whole is not downloaded again when its version is the
    /// one recorded at the download, which is said in a notice. A changed
    /// object, or one whose rows are not shown, is offered for download
    /// again, and the rows shown stay until the user agrees.
    pub fn reload(&mut self, cx: &mut Context<Self>) {
        if !self.can_reload() {
            return;
        }

        let summary = crate::labels::parquet_open_failed_message(&self.title());

        if self.use_live_connection(summary, cx).is_err() {
            return;
        }

        if self.reads == Some(ObjectReads::Downloaded) {
            self.check_object_version(VersionCheck::Reload, cx);
            return;
        }

        self.load_first_window(cx);
    }

    fn load_first_window(&mut self, cx: &mut Context<Self>) {
        if self.reads == Some(ObjectReads::Downloaded) {
            self.check_object_version(VersionCheck::Open, cx);
            return;
        }

        let location = self.location.clone();

        self.start_open(move || open_first_window(&location, DEFAULT_PAGE_ROWS), cx);
    }

    /// Shows the loading notice and runs `open` on the background executor,
    /// storing what it returns. A result of an older open is dropped.
    fn start_open(
        &mut self,
        open: impl FnOnce() -> Result<OpenedFile, OpenError> + Send + 'static,
        cx: &mut Context<Self>,
    ) {
        self.phase = ParquetPhase::Loading;
        self._subscriptions.clear();
        self.open_generation += 1;
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();

        let generation = self.open_generation;

        let task = cx.background_executor().spawn(async move { open() });

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

    /// Reads the version of an object read whole on the background executor:
    /// one `head_object` call, no body. Opening shows the loading notice
    /// meanwhile, and a reload keeps what is shown.
    fn check_object_version(&mut self, purpose: VersionCheck, cx: &mut Context<Self>) {
        if purpose == VersionCheck::Open {
            self.phase = ParquetPhase::Loading;
            self._subscriptions.clear();
            self.open_generation += 1;
            cx.emit(DocumentEvent::MetaChanged);
        }

        self.checking_version = true;
        cx.notify();

        let generation = self.open_generation;
        let location = self.location.clone();

        let task = cx
            .background_executor()
            .spawn(async move { read_version(&location) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_version_check(generation, purpose, result, cx)
                })
                .ok();
            });
        })
        .detach();
    }

    /// Handles a reload that found the object at `version`, as the version
    /// read of an object read whole does.
    #[cfg(test)]
    pub(super) fn apply_reload_version(&mut self, version: SourceVersion, cx: &mut Context<Self>) {
        let generation = self.open_generation;

        self.apply_version_check(generation, VersionCheck::Reload, Ok(version), cx);
    }

    /// Asks whether to download the object, or says that a reload found it
    /// unchanged. This is the first place a failed version read is caught,
    /// so it is reported here and only here.
    fn apply_version_check(
        &mut self,
        generation: u64,
        purpose: VersionCheck,
        result: Result<SourceVersion, StorageError>,
        cx: &mut Context<Self>,
    ) {
        if generation != self.open_generation {
            return;
        }

        self.checking_version = false;

        let title = self.title();

        match result {
            Ok(version) => {
                let shown_version = self.loaded().map(|loaded| loaded.version.clone());
                let unchanged =
                    purpose == VersionCheck::Reload && shown_version.as_ref() == Some(&version);

                if unchanged {
                    Toast::info(crate::labels::parquet_download_unchanged(&title)).push(cx);
                } else {
                    if matches!(self.phase, ParquetPhase::Loading) {
                        self.phase = ParquetPhase::AwaitingDownload;
                    }

                    let mut focus = ModalFocus::new(cx);
                    focus.focus_on_next_render();

                    self.download_prompt = Some(DownloadPrompt {
                        size: version.length(),
                        replaces_rows: shown_version.is_some(),
                        focus,
                    });
                }
            }

            Err(error) => {
                let error = OpenError::from(error);
                let summary = crate::labels::parquet_open_failed_message(&title);
                let cause = error.to_string();

                report_error(open_error_to_user_facing(&error, summary), cx);

                if matches!(self.phase, ParquetPhase::Loading) {
                    self.phase = ParquetPhase::Failed(cause);
                }
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
            profile,
            columns,
            page_model,
        } = opened;

        let total_columns = profile.columns.len();
        let projection = ColumnProjection::from_indices(total_columns, &columns);
        let summary = crate::labels::parquet_summary(
            total_columns,
            page_model.total_rows(),
            profile.total_compressed_bytes,
            self.reads,
        )
        .into();
        let profile = Arc::new(profile);
        let annotations = header_annotations(&profile, &columns);

        let model = page_model.table_model();

        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(model, cx);
            state.set_header_annotations(annotations, cx);
            state
        });

        self._subscriptions = vec![Self::subscribe_to_table(&table_state, cx)];

        let table = cx.new(|cx| DataTable::new("parquet-table", table_state.clone(), cx));

        let mut loaded = LoadedFile {
            version,
            source: Some(source),
            file,
            profile,
            columns,
            projection,
            total_columns,
            page_model,
            read_generation: 0,
            reread_pending: false,
            estimate: None,
            estimate_bar: None,
            summary,
            source_changed: false,
            table_state,
            table,
            picker: None,
            profile_view: None,
        };

        loaded.refresh_estimate();
        loaded
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

        let Ok(connection) = self.connection_for_windows(summary, cx) else {
            return;
        };

        let recheck = self.version_recheck();
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
        let read_generation = loaded.read_generation;
        let window = RowWindow::new(loaded.page_model.loaded_rows(), DEFAULT_PAGE_ROWS);

        let task = cx.background_executor().spawn(async move {
            let result =
                read_next_window(recheck.as_ref(), &version, &source, &file, window, &columns);

            (source, result)
        });

        cx.spawn(async move |this, cx| {
            let (source, result) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_window_outcome(
                        WindowGeneration {
                            open: generation,
                            read: read_generation,
                        },
                        source,
                        result,
                        cx,
                    );
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
    /// `generation` is the open and the projection the window was read for.
    /// When the file was opened again, or another projection was applied
    /// meanwhile, the window belongs to rows no longer shown and is dropped
    /// without a report; the read of the applied projection starts instead.
    fn apply_window_outcome(
        &mut self,
        generation: WindowGeneration,
        source: LocationSource,
        result: Result<CellPage, OpenError>,
        cx: &mut Context<Self>,
    ) {
        if generation.open != self.open_generation {
            return;
        }

        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        loaded.source = Some(source);

        if generation.read != loaded.read_generation {
            self.start_pending_reread(cx);
            cx.notify();
            return;
        }

        match result {
            Ok(page) => {
                loaded.page_model.append(page);

                let model = loaded.page_model.table_model();

                loaded.table_state.update(cx, |state, cx| {
                    state.set_model(model, ModelSwap::KeepCursor, cx);
                });

                loaded.refresh_estimate();
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

    // -- Projection ----------------------------------------------------------

    /// Makes `projection` the applied one and reads the file again from its
    /// first row with its columns, which the reader returns in schema order.
    /// Does nothing for a projection without columns, of another file, or
    /// equal to the applied one. Once the file was found changed nothing is
    /// read until it is opened again: the picker and the Columns view go back
    /// to the shown columns.
    ///
    /// A window being read meanwhile is dropped when it arrives, and the read
    /// starts then, with the source it hands back.
    pub fn apply_projection(&mut self, projection: ColumnProjection, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        if !projection.is_applicable()
            || projection.total_count() != loaded.total_columns
            || projection == loaded.projection
        {
            return;
        }

        if loaded.source_changed {
            loaded.sync_projection_controls(cx);
            cx.notify();
            return;
        }

        loaded.projection = projection;
        loaded.read_generation += 1;
        loaded.reread_pending = true;
        loaded.sync_projection_controls(cx);

        self.start_pending_reread(cx);
        cx.notify();
    }

    /// Starts the read of the applied projection when one is pending and the
    /// source is not out with another read.
    fn start_pending_reread(&mut self, cx: &mut Context<Self>) {
        let ready = self
            .loaded()
            .is_some_and(|loaded| loaded.reread_pending && loaded.source.is_some());

        if !ready {
            return;
        }

        let summary = crate::labels::parquet_projection_failed_message(&self.title());

        let Ok(connection) = self.connection_for_windows(summary, cx) else {
            if let Some(loaded) = self.loaded_mut() {
                loaded.revert_projection(cx);
            }

            return;
        };

        let recheck = self.version_recheck();
        let open_generation = self.open_generation;

        let Some(loaded) = self.loaded_mut() else {
            return;
        };
        let Some(mut source) = loaded.source.take() else {
            return;
        };

        loaded.reread_pending = false;

        if let Some(connection) = connection {
            source.use_connection(connection);
        }

        let generation = WindowGeneration {
            open: open_generation,
            read: loaded.read_generation,
        };
        let version = loaded.version.clone();
        let file = loaded.file.clone();
        let columns = loaded.projection.selected_indices();

        let task = cx.background_executor().spawn(async move {
            let window = RowWindow::new(0, DEFAULT_PAGE_ROWS);
            let result =
                read_next_window(recheck.as_ref(), &version, &source, &file, window, &columns);

            (source, columns, result)
        });

        cx.spawn(async move |this, cx| {
            let (source, columns, result) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_reread_outcome(generation, source, columns, result, cx);
                })
                .ok();
            });
        })
        .detach();
    }

    /// Takes the source back and shows the first window of `columns`. This
    /// is the first place a failure of the read is caught, so it is reported
    /// here and only here; the table keeps the columns it showed and the
    /// projection goes back to them.
    ///
    /// A read of a projection replaced meanwhile is dropped without a report,
    /// and the read of the applied one starts instead.
    fn apply_reread_outcome(
        &mut self,
        generation: WindowGeneration,
        source: LocationSource,
        columns: Vec<usize>,
        result: Result<CellPage, OpenError>,
        cx: &mut Context<Self>,
    ) {
        if generation.open != self.open_generation {
            return;
        }

        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        loaded.source = Some(source);

        if generation.read != loaded.read_generation {
            self.start_pending_reread(cx);
            cx.notify();
            return;
        }

        match result {
            Ok(page) => loaded.show_columns(columns, page, cx),

            Err(error) => {
                if matches!(error, OpenError::SourceChanged) {
                    loaded.source_changed = true;
                }

                loaded.revert_projection(cx);

                let summary = crate::labels::parquet_projection_failed_message(&title);

                report_error(open_error_to_user_facing(&error, summary), cx);
            }
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// The connection a further window is read through: `Ok(None)` for a
    /// local file and for a downloaded copy, which needs no connection, and
    /// otherwise the live connection of the object's profile.
    fn connection_for_windows(
        &mut self,
        summary: String,
        cx: &mut Context<Self>,
    ) -> Result<Option<Arc<dyn Connection>>, ConnectionUnavailable> {
        if self.reads == Some(ObjectReads::Downloaded) {
            return Ok(None);
        }

        self.use_live_connection(summary, cx)
    }

    /// Where the version is read again before each window, and `None` for a
    /// downloaded copy, which is a snapshot that does not change.
    fn version_recheck(&self) -> Option<FileLocation> {
        (self.reads != Some(ObjectReads::Downloaded)).then(|| self.location.clone())
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

/// The open and the projection a window was read for.
#[derive(Clone, Copy)]
struct WindowGeneration {
    open: u64,
    read: u64,
}

/// Opens the file at `location`, reads its footer and statistics, picks the
/// default projection and reads the first window of `page_rows` rows of it.
/// Blocks on file or network I/O.
pub(super) fn open_first_window(
    location: &FileLocation,
    page_rows: u64,
) -> Result<OpenedFile, OpenError> {
    let (source, version) = open_source(location)?;

    open_rows(source, version, page_rows)
}

/// Downloads the object `key` of `bucket` whole through `connection` and
/// opens it like [`open_first_window`]. Blocks on network and file I/O.
fn open_downloaded(
    connection: &dyn Connection,
    bucket: &str,
    key: &str,
    page_rows: u64,
) -> Result<OpenedFile, OpenError> {
    let (source, version) =
        download_whole_object(connection, bucket, key, DOWNLOAD_IN_MEMORY_LIMIT_BYTES)?;

    open_rows(source, version, page_rows)
}

/// Reads the footer and statistics of the file `source` reads, `version` of
/// it, picks the default projection and reads the first window of
/// `page_rows` rows of it.
fn open_rows(
    source: LocationSource,
    version: SourceVersion,
    page_rows: u64,
) -> Result<OpenedFile, OpenError> {
    let file = dbflux_parquet::open(&source)?;

    // A file without rows may have no row groups at all, which the window
    // and estimate readers are never asked about.
    if file.row_count() == 0 {
        return Ok(OpenedFile::Empty);
    }

    let profile = table_profile(&file)?;

    let window = RowWindow::new(0, page_rows);
    let columns = default_columns(&file, &profile, window)?;

    let rows = read_window(&file, &source, window, &columns)?;
    let first_page = cells_of(&rows)?;

    let page_model = ParquetPageModel::new(first_page, file.row_count());

    Ok(OpenedFile::Rows(Box::new(OpenedRows {
        version,
        source,
        file,
        profile,
        columns,
        page_model,
    })))
}

/// The profile of `file` from its footer: each top-level column in schema
/// order, the row count and the sizes of the whole file.
fn table_profile(file: &ParquetFile) -> Result<TableProfile, ParquetError> {
    let columns = column_statistics(file)?
        .iter()
        .map(column_profile)
        .collect();
    let totals = file_statistics(file)?;

    Ok(TableProfile {
        columns,
        row_count: Some(totals.row_count),
        total_compressed_bytes: Some(totals.compressed_bytes),
        total_uncompressed_bytes: Some(totals.uncompressed_bytes),
        source_label: ProfileSource::FileFooter,
    })
}

/// The second header line of each of `columns`, in the order given.
fn header_annotations(profile: &TableProfile, columns: &[usize]) -> Vec<Option<HeaderAnnotation>> {
    let row_count = profile.row_count.unwrap_or(0);

    columns
        .iter()
        .map(|&column| {
            profile
                .columns
                .get(column)
                .map(|column| header_annotation(column, row_count))
        })
        .collect()
}

/// What reading the window after the rows `page_model` holds costs, for
/// `columns`. `None` once every row is loaded.
fn next_window_estimate(
    file: &ParquetFile,
    page_model: &ParquetPageModel,
    columns: &[usize],
) -> Result<Option<ReadEstimate>, ParquetError> {
    if page_model.is_fully_loaded() {
        return Ok(None);
    }

    let window = RowWindow::new(page_model.loaded_rows(), DEFAULT_PAGE_ROWS);
    let estimate = read_estimate(file, window, columns)?;

    Ok(Some(core_estimate(estimate)))
}

/// The reader's estimate as the generic model the estimate strip shows.
fn core_estimate(estimate: FileReadEstimate) -> ReadEstimate {
    ReadEstimate {
        total_bytes: estimate.total_bytes,
        per_column: estimate.per_column,
        scope: EstimateScope::Parts {
            touched: estimate.row_groups_touched,
            total: estimate.row_group_count,
            unit: PartUnit::RowGroups,
        },
    }
}

/// The leading columns in schema order whose bytes for `window` stay within
/// the default projection's budget, as the reader estimates them from the
/// footer.
fn default_columns(
    file: &ParquetFile,
    profile: &TableProfile,
    window: RowWindow,
) -> Result<Vec<usize>, ParquetError> {
    let column_count = profile.columns.len();
    let every_column: Vec<usize> = (0..column_count).collect();

    let estimate = read_estimate(file, window, &every_column)?;

    let mut window_bytes = vec![None; column_count];

    for (column, bytes) in estimate.per_column {
        if let Some(slot) = window_bytes.get_mut(column) {
            *slot = Some(bytes);
        }
    }

    let projection = default_projection(
        profile,
        &window_bytes,
        DEFAULT_PROJECTION_BUDGET_BYTES,
        DEFAULT_PROJECTION_MAX_COLUMNS,
    );

    Ok(projection.selected_indices())
}

/// Checks that the file at `recheck` is still `version`, then reads
/// `window` of `columns` through `source`. A downloaded copy has no
/// `recheck`: it is a snapshot. Blocks on file or network I/O.
pub(super) fn read_next_window(
    recheck: Option<&FileLocation>,
    version: &SourceVersion,
    source: &LocationSource,
    file: &ParquetFile,
    window: RowWindow,
    columns: &[usize],
) -> Result<CellPage, OpenError> {
    if let Some(location) = recheck
        && has_changed_since(location, version)?
    {
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
