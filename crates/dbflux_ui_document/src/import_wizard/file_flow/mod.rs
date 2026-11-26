//! The wizard's "Import file" entry: pick one CSV, JSON or JSON Lines file,
//! read its columns, and review it as a single-table import through the same
//! configure, confirm and run steps as a folder import.
//!
//! The target table defaults to the file stem. A CSV file offers the delimiter,
//! encoding and NULL text; changing the delimiter or encoding reads the file
//! again, because they decide where the header row splits. Everything else is
//! the folder flow's: `start_import` runs the job this module describes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_components::controls::{Button, Dropdown, Input, InputEvent, InputState};
use dbflux_components::modals::modal_hint;
use dbflux_components::primitives::{Kbd, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{Spacing, ui};
use dbflux_core::keymap_types::{Command, ContextId};
use dbflux_core::{
    CancelToken, Connection, DbError, DbSchemaInfo, DriverCapabilities, RelationalSchema,
    SchemaLoadingStrategy, SchemaSnapshot, TableInfo, TransferColumn,
};
use dbflux_export::{CsvDelimiter, FormatOptions, TextEncoding};
use dbflux_transfer::import::{ImportOptions, ImportOutcome, ImportTablePlan, run_import};
use dbflux_transfer::import_file::{ImportFile, InspectError, run_file_import};
use dbflux_transfer::{FileFormat, TransferError};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;

use super::chrome::error_block;
use super::column_mapping::TableImportConfig;
use super::errors::{ErrorBlock, FileLoadError};
use super::file_options::{
    delimiter_from_segment_id, delimiter_label, delimiter_segment_id, detect_format,
    encoding_from_segment_id, encoding_label, encoding_segment_id, is_sql_script,
    picker_extensions, visible_file_options,
};
use super::keyboard::{ConfigureField, with_cursor_ring};
use super::sheet_picker::{SheetChoice, choose_sheet, uses_sheet_picker};
use super::{ImportWizard, WizardStep};
use crate::export_wizard::notes::{form_row, info_line};

/// Width of the target table and NULL text fields. (220 px at the base font size)
const FILE_INPUT_WIDTH: Rems = ui(220.0);

/// What the wizard imports: a folder written by Export, or one loose file.
pub(super) enum ImportJob {
    Folder(PathBuf),
    File {
        path: PathBuf,
        format: FileFormat,
        options: FormatOptions,
        /// The spreadsheet sheet to read; `None` for every other format.
        sheet: Option<String>,
    },
}

impl ImportJob {
    /// Runs the import. `on_progress` receives `(rows_done, estimated_total)`.
    pub(super) fn run(
        &self,
        connection: &Arc<dyn Connection>,
        plans: &[ImportTablePlan],
        options: &ImportOptions,
        cancel: &CancelToken,
        mut on_progress: impl FnMut(u64, Option<u64>),
    ) -> Result<ImportOutcome, TransferError> {
        match self {
            Self::Folder(directory) => run_import(
                connection,
                directory,
                plans,
                options,
                cancel,
                |_index, rows_done, estimated_total| on_progress(rows_done, estimated_total),
            ),
            Self::File {
                path,
                format,
                options: format_options,
                sheet,
            } => {
                let plan = plans.first().ok_or_else(|| {
                    TransferError::Source("there is no table to import the file into".to_string())
                })?;
                let file = ImportFile::inspect_sheet(
                    path,
                    *format,
                    format_options.clone(),
                    sheet.as_deref(),
                )?;

                run_file_import(connection, &file, plan, options, cancel, on_progress)
            }
        }
    }
}

/// The file picked for import and the options it is read with.
pub(super) struct FileImportState {
    path: PathBuf,
    pub(super) format: FileFormat,
    pub(super) options: FormatOptions,
    /// The visible worksheets of a workbook, in workbook order; empty for a
    /// file with no sheets and until the first read of a workbook finishes.
    pub(super) sheets: Vec<String>,
    /// The worksheet the import reads; set once the workbook was listed.
    pub(super) sheet: Option<String>,
    /// Set when the file could not be read with these options, so the user can
    /// change them; the run stays unavailable until it is read again.
    pub(super) error: Option<ErrorBlock>,
    /// Identifies the latest read, so a slower earlier one cannot overwrite it.
    pub(super) generation: u64,
    /// Continue to the run as soon as the read in flight finishes: the user
    /// pressed Continue with a typed table name that was not applied yet.
    continue_after_read: bool,
}

/// The text fields of the file entry. Created when the wizard opens, because
/// an input needs a window.
pub(super) struct FileInputs {
    pub(super) target_table: Entity<InputState>,
    pub(super) null_text: Entity<InputState>,
    pub(super) sheet_dropdown: Entity<Dropdown>,
    _subscriptions: Vec<Subscription>,
}

impl FileInputs {
    pub(super) fn new(window: &mut Window, cx: &mut Context<ImportWizard>) -> Self {
        let target_table = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.import_wizard.file.target_table_placeholder"
            ))
        });
        let null_text = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.export_wizard.format_options.null_text_placeholder"
            ))
        });

        let target_subscription = cx.subscribe_in(
            &target_table,
            window,
            |this, _input, event: &InputEvent, _window, cx| {
                if let InputEvent::PressEnter {
                    secondary: false, ..
                } = event
                {
                    this.apply_target_table(cx);
                }
            },
        );
        let null_text_subscription = cx.subscribe_in(
            &null_text,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if let InputEvent::Change = event {
                    let typed = input.read(cx).value().to_string();
                    if let Some(state) = this.file_import.as_mut() {
                        state.options.null_text = typed;
                    }
                }
            },
        );

        let (sheet_dropdown, sheet_subscription) = Self::new_sheet_dropdown(cx);

        Self {
            target_table,
            null_text,
            sheet_dropdown,
            _subscriptions: vec![
                target_subscription,
                null_text_subscription,
                sheet_subscription,
            ],
        }
    }
}

/// A file read for import with the table configuration it starts from.
struct LoadedFile {
    config: TableImportConfig,
}

/// What to read: the file, how, the target table name typed so far and, for
/// a workbook, the sheet.
struct FileRequest {
    path: PathBuf,
    format: FileFormat,
    options: FormatOptions,
    target_table: Option<String>,
    sheet: Option<String>,
}

/// The outcome of one read: the sheets a workbook holds, found even when the
/// chosen sheet cannot be imported, and the configuration or why not.
struct FileRead {
    choice: Option<SheetChoice>,
    loaded: Result<LoadedFile, FileLoadError>,
}

/// The target table could not be looked up. Carries the lookup's own error
/// text, which the wizard shows after naming the lookup.
#[derive(Debug)]
struct TargetLookupError(String);

impl std::fmt::Display for TargetLookupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The columns of the target table, `None` when the table is not there.
///
/// A lookup that reports the table as not found is a miss. Drivers hosted
/// over RPC flatten their errors, so for any other failure `listing` (the
/// connection's schema) decides: a table it does not hold is a miss. A table
/// it does hold, a listing that cannot be read, or a listing that cannot say (not a
/// relational schema) returns the original lookup error, because treating it
/// as a miss would offer to create a table over one that exists.
///
/// SQL Server and Redshift answer a lookup of a missing table with no
/// columns instead of an error, so that answer is undecided: a listing
/// without the table makes it a miss, and otherwise it stays an existing
/// table with no columns.
fn existing_table_columns(
    lookup: Result<TableInfo, DbError>,
    table: &str,
    listing: impl FnOnce() -> Option<SchemaSnapshot>,
) -> Result<Option<Vec<TransferColumn>>, TargetLookupError> {
    let error = match lookup {
        Ok(info) => {
            let columns: Vec<TransferColumn> = info
                .columns
                .unwrap_or_default()
                .into_iter()
                .map(|column| TransferColumn {
                    name: column.name,
                    type_name: Some(column.type_name),
                    nullable: column.nullable,
                    is_primary_key: column.is_primary_key,
                    kind: dbflux_core::ColumnKind::Unknown,
                })
                .collect();

            if !columns.is_empty() {
                return Ok(Some(columns));
            }

            return Ok(match listing() {
                Some(snapshot) if !snapshot_may_hold(&snapshot, table) => None,
                _ => Some(columns),
            });
        }
        Err(DbError::ObjectNotFound(_)) => return Ok(None),
        Err(error) => error,
    };

    match listing() {
        Some(snapshot) if !snapshot_may_hold(&snapshot, table) => Ok(None),
        _ => Err(TargetLookupError(error.to_string())),
    }
}

/// Whether the relational schema lists any table or view.
fn lists_objects(relational: &RelationalSchema) -> bool {
    !relational.tables.is_empty()
        || !relational.views.is_empty()
        || relational
            .schemas
            .iter()
            .any(|schema| !schema.tables.is_empty() || !schema.views.is_empty())
}

/// Whether the schema may hold a table or view named `table`, in any schema
/// and ignoring case. A schema that is not relational cannot rule it out. The
/// schema is otherwise taken as an enumeration of the objects, which
/// [`complete_listing`] guarantees, so an empty one rules the table out.
fn snapshot_may_hold(snapshot: &SchemaSnapshot, table: &str) -> bool {
    let Some(relational) = snapshot.as_relational() else {
        return true;
    };
    let named = |name: &str| name.eq_ignore_ascii_case(table);

    relational.tables.iter().any(|entry| named(&entry.name))
        || relational.views.iter().any(|entry| named(&entry.name))
        || relational.schemas.iter().any(|schema| {
            schema.tables.iter().any(|entry| named(&entry.name))
                || schema.views.iter().any(|entry| named(&entry.name))
        })
}

/// The listing a missing table is checked against. `first` is the
/// connection's schema snapshot. MySQL, SQL Server, DuckDB, ClickHouse and
/// lazily loading RPC drivers list only databases there, and tables load per
/// database; so a snapshot with no tables, or any snapshot of a `lazy`
/// connection, is replaced by the target database's listing. A listing that
/// was read enumerates the database, even when it is empty (the first table
/// of an empty database). When it is unreadable nothing can confirm a miss,
/// and the answer is `None`.
fn complete_listing(
    first: Option<SchemaSnapshot>,
    lazy: bool,
    for_database: impl FnOnce() -> Option<DbSchemaInfo>,
) -> Option<SchemaSnapshot> {
    let snapshot = first?;
    let complete = snapshot.as_relational().is_none_or(lists_objects) && !lazy;
    if complete {
        return Some(snapshot);
    }

    let database = for_database()?;
    let mut schema = RelationalSchema::default();
    schema.schemas.push(database);

    Some(SchemaSnapshot::relational(schema))
}

/// Reads the file's columns and looks up the target table, off the UI thread.
fn read_file_config(
    connection: &Arc<dyn Connection>,
    target_database: &str,
    request: FileRequest,
) -> FileRead {
    let choice = if uses_sheet_picker(request.format) {
        match choose_sheet(&request.path, request.sheet.as_deref()) {
            Ok(choice) => Some(choice),
            Err(error) => {
                return FileRead {
                    choice: None,
                    loaded: Err(FileLoadError::Read(error)),
                };
            }
        }
    } else {
        None
    };
    let sheet = choice.as_ref().map(|choice| choice.selected.as_str());
    let loaded = load_file_config(connection, target_database, &request, sheet);

    FileRead { choice, loaded }
}

fn load_file_config(
    connection: &Arc<dyn Connection>,
    target_database: &str,
    request: &FileRequest,
    sheet: Option<&str>,
) -> Result<LoadedFile, FileLoadError> {
    let file = ImportFile::inspect_checked(
        &request.path,
        request.format,
        request.options.clone(),
        sheet,
    )
    .map_err(|error| match error {
        InspectError::Sheet(issue) => FileLoadError::Sheet(issue),
        InspectError::Failed(error) => FileLoadError::Read(error),
    })?;
    let source_table = file.table_name();
    let target_table = request
        .target_table
        .clone()
        .unwrap_or_else(|| source_table.clone());

    let existing_columns = existing_table_columns(
        connection.table_details(target_database, None, &target_table),
        &target_table,
        // A listing that cannot be read leaves the lookup error standing.
        || {
            complete_listing(
                connection.schema().ok(),
                matches!(
                    connection.schema_loading_strategy(),
                    SchemaLoadingStrategy::LazyPerDatabase
                ),
                || connection.schema_for_database(target_database).ok(),
            )
        },
    )
    .map_err(|error| FileLoadError::Lookup(error.0))?;
    let target_exists = existing_columns.is_some();

    let mut manifest_table = file.manifest_table();
    manifest_table.name = target_table;
    let mut config = TableImportConfig::new(
        &manifest_table,
        target_exists,
        existing_columns.unwrap_or_default(),
    );
    config.source_table = source_table;

    Ok(LoadedFile { config })
}

/// The keycap at the right edge of the target table field: Enter applies the
/// name, like Use Table. The label comes from the keymap, so it follows the
/// platform and the user's bindings.
fn enter_hint(cx: &App) -> Div {
    let label = dbflux_components::actions::shortcut_label(
        cx,
        ContextId::Modal.id(),
        Command::Execute.id(),
        "Enter",
    );

    div()
        .debug_selector(|| "import-wizard-target-enter-hint".to_string())
        .children(label.map(Kbd::new))
}

/// The explanation shown for a format with no import options, worded for
/// that format.
fn no_options_key(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Xml => "document.import_wizard.file.xml_no_options",
        FileFormat::Spreadsheet => "document.import_wizard.file.spreadsheet_header",
        _ => "document.import_wizard.file.json_no_options",
    }
}

impl ImportWizard {
    /// The job the run step executes: the picked file, else the picked folder.
    pub(super) fn import_job(&self) -> Option<ImportJob> {
        match &self.file_import {
            Some(state) => Some(ImportJob::File {
                path: state.path.clone(),
                format: state.format,
                options: state.options.clone(),
                sheet: state.sheet.clone(),
            }),
            None => self.manifest_dir.clone().map(ImportJob::Folder),
        }
    }

    /// Whether the file entry has a file that cannot be run yet.
    pub(super) fn file_import_blocked(&self) -> bool {
        self.file_import
            .as_ref()
            .is_some_and(|state| state.error.is_some() || self.loading)
    }

    /// The footer's line for a disabled Continue on a file: the specific
    /// reason the failed read gave, else that the file has not been read.
    pub(super) fn file_blocked_reason(&self) -> Option<String> {
        if !self.file_import_blocked() {
            return None;
        }

        let specific = self
            .file_import
            .as_ref()
            .and_then(|state| state.error.as_ref())
            .and_then(|error| error.footer_reason.clone());

        Some(
            specific.unwrap_or_else(|| {
                dbflux_i18n::t!("document.import_wizard.footer.reason.file_read")
            }),
        )
    }

    pub(super) fn choose_file(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            self.manifest_error = Some(ErrorBlock::no_connection());
            cx.notify();
            return;
        };

        let sql_scripts = Self::sql_scripts_offered(connection.as_ref());

        self.loading = true;
        self.loading_folder = false;
        self.manifest_error = None;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let extensions = picker_extensions(sql_scripts);
            let picked = dbflux_ui_base::file_dialog::pick_existing_file(cx, async {
                rfd::AsyncFileDialog::new()
                    .set_title(dbflux_i18n::t!(
                        "document.import_wizard.pick_file.dialog_title"
                    ))
                    .add_filter(
                        if sql_scripts {
                            dbflux_i18n::t!("document.import_wizard.pick_file.filter_label_sql")
                        } else {
                            dbflux_i18n::t!("document.import_wizard.pick_file.filter_label")
                        },
                        &extensions,
                    )
                    .pick_file()
                    .await
                    .map(|handle| handle.path().to_path_buf())
            })
            .await;

            this.update(cx, |this, cx| match picked {
                None => {
                    this.loading = false;
                    cx.notify();
                }
                Some(path) => this.open_picked_file(connection, path, cx),
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn open_picked_file(
        &mut self,
        connection: Arc<dyn Connection>,
        path: PathBuf,
        cx: &mut Context<Self>,
    ) {
        // A `.sql` file runs as a script. The script flow refuses it with the
        // reason when the connection is not SQL, for a file that got past the
        // dialog's filter.
        if is_sql_script(&path) {
            self.open_script(connection, path, cx);
            return;
        }

        let Some(format) = detect_format(&path) else {
            self.loading = false;
            self.manifest_error = Some(ErrorBlock::unsupported_file(Self::sql_scripts_offered(
                connection.as_ref(),
            )));
            cx.notify();
            return;
        };

        let stem = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.pending_file_inputs = Some(stem);
        self.field_cursor = 0;
        self.read_file(
            connection,
            FileRequest {
                path,
                format,
                options: FormatOptions::default(),
                target_table: None,
                sheet: None,
            },
            cx,
        );
    }

    /// Reads the file again with the current options, sheet and target table
    /// name.
    pub(super) fn reload_file(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            return;
        };
        let Some(state) = self.file_import.as_ref() else {
            return;
        };
        let (path, format, options, sheet) = (
            state.path.clone(),
            state.format,
            state.options.clone(),
            state.sheet.clone(),
        );
        let typed = self
            .file_inputs
            .as_ref()
            .map(|inputs| inputs.target_table.read(cx).value().trim().to_string())
            .filter(|name| !name.is_empty());

        self.read_file(
            connection,
            FileRequest {
                path,
                format,
                options,
                target_table: typed,
                sheet,
            },
            cx,
        );
    }

    /// When the target table field holds a name the table row does not use
    /// yet, reads the file again for that name and continues to the run once
    /// it is read. Returns whether Continue waits for that read.
    pub(super) fn defer_continue_for_typed_table(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(typed) = self
            .file_inputs
            .as_ref()
            .map(|inputs| inputs.target_table.read(cx).value().trim().to_string())
            .filter(|name| !name.is_empty())
        else {
            return false;
        };
        let applied = self
            .rows
            .first()
            .is_some_and(|row| row.config.target_table == typed);
        if self.file_import.is_none() || applied {
            return false;
        }

        self.reload_file(cx);
        if let Some(state) = self.file_import.as_mut() {
            state.continue_after_read = true;
        }
        true
    }

    pub(super) fn apply_target_table(&mut self, cx: &mut Context<Self>) {
        self.reload_file(cx);
    }

    fn read_file(
        &mut self,
        connection: Arc<dyn Connection>,
        request: FileRequest,
        cx: &mut Context<Self>,
    ) {
        let target_database = self.target_database(&connection);
        let supports_truncate = connection.supports(DriverCapabilities::TRUNCATE_TABLE);
        let generation = self.next_file_read_request();

        let sheets = self
            .file_import
            .as_ref()
            .filter(|state| state.path == request.path)
            .map(|state| state.sheets.clone())
            .unwrap_or_default();

        self.loading = true;
        self.loading_folder = false;
        self.file_import = Some(FileImportState {
            path: request.path.clone(),
            format: request.format,
            options: request.options.clone(),
            sheets,
            sheet: request.sheet.clone(),
            error: None,
            generation,
            continue_after_read: false,
        });
        cx.notify();

        cx.spawn(async move |this, cx| {
            let read = cx
                .background_executor()
                .spawn(async move { read_file_config(&connection, &target_database, request) })
                .await;

            this.update(cx, |this, cx| {
                this.apply_sheet_choice(generation, read.choice, cx);
                this.finish_file_read(generation, supports_truncate, read.loaded, cx);
            })
            .ok();
        })
        .detach();
    }

    /// A new read request id. The counter belongs to the wizard and is never
    /// reset, so a read that finishes after the wizard was reopened cannot
    /// match the id of a read of the new session.
    pub(super) fn next_file_read_request(&mut self) -> u64 {
        self.file_read_request += 1;
        self.file_read_request
    }

    fn finish_file_read(
        &mut self,
        generation: u64,
        supports_truncate: bool,
        loaded: Result<LoadedFile, FileLoadError>,
        cx: &mut Context<Self>,
    ) {
        if self
            .file_import
            .as_ref()
            .is_none_or(|state| state.generation != generation)
        {
            return;
        }
        self.loading = false;
        let continue_now = self
            .file_import
            .as_mut()
            .is_some_and(|state| std::mem::take(&mut state.continue_after_read));

        match loaded {
            Ok(LoadedFile { config }) => {
                self.script = None;
                self.supports_truncate = supports_truncate;
                self.build_rows(vec![config], cx);
                self.step = WizardStep::Configure;
                if continue_now {
                    self.continue_from_configure(cx);
                }
            }
            Err(failure) => {
                let format = self
                    .file_import
                    .as_ref()
                    .map_or(FileFormat::Csv, |state| state.format);
                let other_sheets = self
                    .file_import
                    .as_ref()
                    .is_some_and(|state| state.sheets.len() > 1);
                let block = match &failure {
                    FileLoadError::Sheet(issue) => ErrorBlock::sheet_issue(issue, other_sheets),
                    _ => ErrorBlock::file_load_failure(&failure, format),
                };
                report_error(
                    UserFacingError::new(ErrorKind::User, block.message.clone()),
                    cx,
                );

                // Nothing on Configure can fix an unreadable JSON file, and that
                // step cannot choose another file: the picker shows the error,
                // whichever step the read started from.
                let unreadable_without_options =
                    format != FileFormat::Csv && matches!(failure, FileLoadError::Read(_));

                if unreadable_without_options {
                    self.file_import = None;
                    self.rows.clear();
                    self._row_subscriptions.clear();
                    self.step = WizardStep::PickFolder;
                    self.manifest_error = Some(block);
                } else {
                    // A CSV stays picked and the user lands on the configure
                    // step, where the delimiter and encoding can be changed
                    // and the file read again.
                    self.rows.clear();
                    self._row_subscriptions.clear();
                    if let Some(state) = self.file_import.as_mut() {
                        state.error = Some(block);
                    }
                    self.step = WizardStep::Configure;
                }
            }
        }
        cx.notify();
    }

    pub(super) fn select_delimiter(&mut self, segment_id: &str, cx: &mut Context<Self>) {
        let Some(delimiter) = delimiter_from_segment_id(segment_id) else {
            return;
        };
        if let Some(state) = self.file_import.as_mut() {
            state.options.delimiter = delimiter;
        }
        self.reload_file(cx);
    }

    pub(super) fn select_encoding(&mut self, segment_id: &str, cx: &mut Context<Self>) {
        let Some(encoding) = encoding_from_segment_id(segment_id) else {
            return;
        };
        if let Some(state) = self.file_import.as_mut() {
            state.options.encoding = encoding;
        }
        self.reload_file(cx);
    }

    /// Applies the values a fresh pick sets on the text fields. Done while
    /// rendering because setting an input's text needs the window.
    pub(super) fn apply_pending_file_inputs(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(target_table) = self.pending_file_inputs.take() else {
            return;
        };
        let Some(inputs) = &self.file_inputs else {
            return;
        };

        inputs
            .target_table
            .update(cx, |input, cx| input.set_value(target_table, window, cx));
        inputs
            .null_text
            .update(cx, |input, cx| input.set_value("", window, cx));
    }

    /// The file's target table and format options, above the table list.
    pub(super) fn render_file_options(
        &self,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let state = self.file_import.as_ref()?;
        let inputs = self.file_inputs.as_ref()?;
        let visible = visible_file_options(state.format);

        let mut rows = vec![
            Text::caption(dbflux_i18n::t!(
                "document.import_wizard.file.source",
                file = state.path.display().to_string(),
                format = state.format.label()
            ))
            .into_any_element(),
        ];

        // A sheet that cannot be read has its own block, so the header line
        // that tells what a good sheet looks like is left out.
        let spreadsheet_ok = uses_sheet_picker(state.format) && state.error.is_none();
        if (!visible.delimiter_and_encoding && !visible.null_text)
            && (spreadsheet_ok || !uses_sheet_picker(state.format))
        {
            rows.push(info_line(
                dbflux_i18n::t!(no_options_key(state.format)),
                "import-wizard-json-no-options",
                cx,
            ));
        }
        if uses_sheet_picker(state.format) && !state.sheets.is_empty() {
            rows.push(self.render_sheet_row(state, inputs, active, cx));
        }
        rows.push(self.render_target_table_row(inputs, cx));

        if visible.delimiter_and_encoding {
            rows.push(self.render_delimiter_row(state.options.delimiter, active, cx));
            rows.push(self.render_encoding_row(state.options.encoding, active, cx));
        }
        if visible.null_text {
            rows.push(Self::render_null_text_row(inputs, cx));
        }
        if let Some(error) = state.error.as_ref() {
            rows.push(error_block(error, "import-wizard-error", cx));
        }

        Some(
            div()
                .id("import-wizard-file-options")
                .flex()
                .flex_col()
                .gap(Spacing::SM)
                .children(rows)
                .into_any_element(),
        )
    }

    fn render_target_table_row(&self, inputs: &FileInputs, cx: &mut Context<Self>) -> AnyElement {
        form_row(
            dbflux_i18n::t!("document.import_wizard.file.target_table_label"),
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .child(
                    div().w(FILE_INPUT_WIDTH).child(
                        Input::new(&inputs.target_table)
                            .id("import-wizard-file-target-table")
                            .suffix(enter_hint(cx))
                            .w_full()
                            .aria_label(dbflux_i18n::t!(
                                "document.import_wizard.file.target_table_label"
                            )),
                    ),
                )
                .child(
                    Button::new(
                        "import-wizard-file-apply-target",
                        dbflux_i18n::t!("document.import_wizard.file.use_table"),
                    )
                    .ghost()
                    .on_click(cx.listener(|this, _, _, cx| this.apply_target_table(cx))),
                ),
            None,
            cx,
        )
        .into_any_element()
    }

    fn render_delimiter_row(
        &self,
        selected: CsvDelimiter,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entity = cx.entity().downgrade();
        let items: Vec<SegmentedItem> = CsvDelimiter::ALL
            .into_iter()
            .map(|delimiter| {
                SegmentedItem::new(delimiter_segment_id(delimiter), delimiter_label(delimiter))
            })
            .collect();
        let picker = SegmentedControl::new(
            items,
            delimiter_segment_id(selected),
            move |segment_id, _window, cx| {
                entity
                    .update(cx, |this, cx| this.select_delimiter(segment_id, cx))
                    .ok();
            },
        )
        .group("import-file-delimiter");

        form_row(
            dbflux_i18n::t!("document.export_wizard.format_options.delimiter_label"),
            div().flex().child(with_cursor_ring(
                active,
                ConfigureField::Delimiter,
                picker,
                cx,
            )),
            None,
            cx,
        )
        .into_any_element()
    }

    fn render_encoding_row(
        &self,
        selected: TextEncoding,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entity = cx.entity().downgrade();
        let items: Vec<SegmentedItem> = TextEncoding::ALL
            .into_iter()
            .map(|encoding| {
                SegmentedItem::new(encoding_segment_id(encoding), encoding_label(encoding))
            })
            .collect();
        let picker = SegmentedControl::new(
            items,
            encoding_segment_id(selected),
            move |segment_id, _window, cx| {
                entity
                    .update(cx, |this, cx| this.select_encoding(segment_id, cx))
                    .ok();
            },
        )
        .group("import-file-encoding");

        form_row(
            dbflux_i18n::t!("document.export_wizard.format_options.encoding_label"),
            div().flex().child(with_cursor_ring(
                active,
                ConfigureField::Encoding,
                picker,
                cx,
            )),
            None,
            cx,
        )
        .into_any_element()
    }

    fn render_null_text_row(inputs: &FileInputs, cx: &mut Context<Self>) -> AnyElement {
        let hint = modal_hint(
            dbflux_i18n::t!("document.import_wizard.file.null_text_hint"),
            cx,
        );

        form_row(
            dbflux_i18n::t!("document.export_wizard.format_options.null_text_label"),
            div()
                .debug_selector(|| "import-wizard-file-null-text".to_string())
                .w(FILE_INPUT_WIDTH)
                .font_family(dbflux_components::fonts::editor_family(cx))
                .child(
                    Input::new(&inputs.null_text)
                        .id("import-wizard-file-null-text")
                        .w_full()
                        .aria_label(dbflux_i18n::t!(
                            "document.export_wizard.format_options.null_text_label"
                        )),
                ),
            Some(hint),
            cx,
        )
        .into_any_element()
    }
}

#[cfg(test)]
pub(super) mod tests;
