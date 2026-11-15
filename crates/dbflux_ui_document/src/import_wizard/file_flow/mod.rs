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

use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::modals::{modal_form_row, modal_hint};
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{Spacing, ui};
use dbflux_core::{
    CancelToken, Connection, DbError, DbSchemaInfo, DriverCapabilities, RelationalSchema,
    SchemaLoadingStrategy, SchemaSnapshot, TableInfo, TransferColumn,
};
use dbflux_export::{CsvDelimiter, FormatOptions, TextEncoding};
use dbflux_transfer::import::{ImportOptions, ImportOutcome, ImportTablePlan, run_import};
use dbflux_transfer::import_file::{ImportFile, run_file_import};
use dbflux_transfer::{FileFormat, TransferError};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;

use super::column_mapping::TableImportConfig;
use super::file_options::{
    delimiter_from_segment_id, delimiter_label, delimiter_segment_id, detect_format,
    encoding_from_segment_id, encoding_label, encoding_segment_id, picker_extensions,
    visible_file_options,
};
use super::keyboard::ConfigureField;
use super::{ImportWizard, WizardStep};

/// Width of the target table and NULL text fields. (220 px at the base font size)
const FILE_INPUT_WIDTH: Rems = ui(220.0);

/// What the wizard imports: a folder written by Export, or one loose file.
pub(super) enum ImportJob {
    Folder(PathBuf),
    File {
        path: PathBuf,
        format: FileFormat,
        options: FormatOptions,
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
            } => {
                let plan = plans.first().ok_or_else(|| {
                    TransferError::Source("there is no table to import the file into".to_string())
                })?;
                let file = ImportFile::inspect(path, *format, format_options.clone())?;

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
    /// Set when the file could not be read with these options, so the user can
    /// change them; the run stays unavailable until it is read again.
    error: Option<String>,
    /// Identifies the latest read, so a slower earlier one cannot overwrite it.
    generation: u64,
    /// Continue to the run as soon as the read in flight finishes: the user
    /// pressed Continue with a typed table name that was not applied yet.
    continue_after_read: bool,
}

/// The text fields of the file entry. Created when the wizard opens, because
/// an input needs a window.
pub(super) struct FileInputs {
    target_table: Entity<InputState>,
    null_text: Entity<InputState>,
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

        Self {
            target_table,
            null_text,
            _subscriptions: vec![target_subscription, null_text_subscription],
        }
    }
}

/// A file read for import with the table configuration it starts from.
struct LoadedFile {
    config: TableImportConfig,
}

/// The columns of the target table, `None` when the table is not there.
///
/// A lookup that reports the table as not found is a miss. Drivers hosted
/// over RPC flatten their errors, so for any other failure `listing` (the
/// connection's schema) decides: a table it does not hold is a miss. A table
/// it does hold, a listing that cannot be read, or a listing that cannot say (not a
/// relational schema) returns the original lookup error, because treating it
/// as a miss would offer to create a table over one that exists.
fn existing_table_columns(
    lookup: Result<TableInfo, DbError>,
    table: &str,
    listing: impl FnOnce() -> Option<SchemaSnapshot>,
) -> Result<Option<Vec<TransferColumn>>, TransferError> {
    let error = match lookup {
        Ok(info) => {
            return Ok(Some(
                info.columns
                    .unwrap_or_default()
                    .into_iter()
                    .map(|column| TransferColumn {
                        name: column.name,
                        type_name: Some(column.type_name),
                        nullable: column.nullable,
                        is_primary_key: column.is_primary_key,
                    })
                    .collect(),
            ));
        }
        Err(DbError::ObjectNotFound(_)) => return Ok(None),
        Err(error) => error,
    };

    match listing() {
        Some(snapshot) if !snapshot_may_hold(&snapshot, table) => Ok(None),
        _ => Err(TransferError::Source(format!(
            "could not look up the target table: {error}"
        ))),
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
/// and ignoring case. A schema that is not relational, or that lists no
/// tables at all (a snapshot of databases only), cannot rule it out.
fn snapshot_may_hold(snapshot: &SchemaSnapshot, table: &str) -> bool {
    let Some(relational) = snapshot.as_relational() else {
        return true;
    };
    if !lists_objects(relational) {
        return true;
    }
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
/// connection, is replaced by the target database's listing. When that is
/// unreadable or lists no tables either, nothing can confirm a miss and the
/// answer is a listing that rules nothing out.
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
fn load_file_config(
    connection: &Arc<dyn Connection>,
    target_database: &str,
    path: &Path,
    format: FileFormat,
    options: FormatOptions,
    target_table: Option<String>,
) -> Result<LoadedFile, TransferError> {
    let file = ImportFile::inspect(path, format, options)?;
    let source_table = file.table_name();
    let target_table = target_table.unwrap_or_else(|| source_table.clone());

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
    )?;
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

impl ImportWizard {
    /// The job the run step executes: the picked file, else the picked folder.
    pub(super) fn import_job(&self) -> Option<ImportJob> {
        match &self.file_import {
            Some(state) => Some(ImportJob::File {
                path: state.path.clone(),
                format: state.format,
                options: state.options.clone(),
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

    pub(super) fn choose_file(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            self.manifest_error = Some(dbflux_i18n::t!(
                "document.import_wizard.pick_folder.error.no_connection"
            ));
            cx.notify();
            return;
        };

        self.loading = true;
        self.manifest_error = None;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let extensions = picker_extensions();
            let picked = dbflux_ui_base::file_dialog::pick_existing_file(cx, async {
                rfd::AsyncFileDialog::new()
                    .set_title(dbflux_i18n::t!(
                        "document.import_wizard.pick_file.dialog_title"
                    ))
                    .add_filter(
                        dbflux_i18n::t!("document.import_wizard.pick_file.filter_label"),
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
        let Some(format) = detect_format(&path) else {
            self.loading = false;
            self.manifest_error = Some(dbflux_i18n::t!(
                "document.import_wizard.pick_file.error.unsupported"
            ));
            cx.notify();
            return;
        };

        let stem = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.pending_file_inputs = Some(stem);
        self.read_file(connection, path, format, FormatOptions::default(), None, cx);
    }

    /// Reads the file again with the current options and target table name.
    fn reload_file(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            return;
        };
        let Some(state) = self.file_import.as_ref() else {
            return;
        };
        let (path, format, options) = (state.path.clone(), state.format, state.options.clone());
        let typed = self
            .file_inputs
            .as_ref()
            .map(|inputs| inputs.target_table.read(cx).value().trim().to_string())
            .filter(|name| !name.is_empty());

        self.read_file(connection, path, format, options, typed, cx);
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
        path: PathBuf,
        format: FileFormat,
        options: FormatOptions,
        target_table: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let target_database = self.target_database(&connection);
        let supports_truncate = connection.supports(DriverCapabilities::TRUNCATE_TABLE);
        let generation = self.next_file_read_request();

        self.loading = true;
        self.file_import = Some(FileImportState {
            path: path.clone(),
            format,
            options: options.clone(),
            error: None,
            generation,
            continue_after_read: false,
        });
        cx.notify();

        cx.spawn(async move |this, cx| {
            let loaded = cx
                .background_executor()
                .spawn(async move {
                    load_file_config(
                        &connection,
                        &target_database,
                        &path,
                        format,
                        options,
                        target_table,
                    )
                })
                .await;

            this.update(cx, |this, cx| {
                this.finish_file_read(generation, supports_truncate, loaded, cx);
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
        loaded: Result<LoadedFile, TransferError>,
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
                self.supports_truncate = supports_truncate;
                self.build_rows(vec![config], cx);
                self.step = WizardStep::Configure;
                if continue_now {
                    self.continue_from_configure(cx);
                }
            }
            Err(error) => {
                let message = dbflux_i18n::t!(
                    "document.import_wizard.pick_file.error.invalid_file",
                    error = error.to_string()
                );
                report_error(UserFacingError::new(ErrorKind::User, message.clone()), cx);

                let options_can_fix_it = self
                    .file_import
                    .as_ref()
                    .is_some_and(|state| state.format == FileFormat::Csv);

                if self.step == WizardStep::PickFolder && !options_can_fix_it {
                    // Nothing on the configure step could fix this file, and
                    // that step has no way back: the picker shows the error.
                    self.file_import = None;
                    self.manifest_error = Some(message);
                } else {
                    // A CSV stays picked and the user lands on the configure
                    // step, where the delimiter and encoding can be changed
                    // and the file read again.
                    self.rows.clear();
                    self._row_subscriptions.clear();
                    if let Some(state) = self.file_import.as_mut() {
                        state.error = Some(message);
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
    pub(super) fn render_file_options(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
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
            modal_form_row(
                dbflux_i18n::t!("document.import_wizard.file.target_table_label"),
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div().w(FILE_INPUT_WIDTH).child(
                            Input::new(&inputs.target_table)
                                .id("import-wizard-file-target-table")
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
            .into_any_element(),
        ];

        if visible.delimiter_and_encoding {
            rows.push(self.render_delimiter_row(state.options.delimiter, cx));
            rows.push(self.render_encoding_row(state.options.encoding, cx));
        }
        if visible.null_text {
            rows.push(Self::render_null_text_row(inputs, cx));
        }
        if let Some(error) = state.error.clone() {
            rows.push(Text::caption(error).danger().into_any_element());
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

    fn render_delimiter_row(&self, selected: CsvDelimiter, cx: &mut Context<Self>) -> AnyElement {
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

        modal_form_row(
            dbflux_i18n::t!("document.export_wizard.format_options.delimiter_label"),
            div()
                .flex()
                .child(self.with_cursor_ring(ConfigureField::Delimiter, picker, cx)),
            None,
            cx,
        )
        .into_any_element()
    }

    fn render_encoding_row(&self, selected: TextEncoding, cx: &mut Context<Self>) -> AnyElement {
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

        modal_form_row(
            dbflux_i18n::t!("document.export_wizard.format_options.encoding_label"),
            div()
                .flex()
                .child(self.with_cursor_ring(ConfigureField::Encoding, picker, cx)),
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

        modal_form_row(
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
mod tests;
