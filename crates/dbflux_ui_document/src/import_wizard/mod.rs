//! T23 — Import wizard: pick a folder bundle written by Export, review/adjust
//! each table's target-table handling and column mapping (T22), confirm any
//! destructive plan, then run the import via `dbflux_transfer::import::run_import`.
//!
//! Targets the connection the wizard was opened from (T24 wires
//! `SidebarEvent::RequestImportWizard` from the sidebar's connected-profile
//! Import action) — this slice does not offer a separate target-connection
//! picker for Import (that is Migration's job in a later slice); the wizard
//! always imports into the profile/database it was invoked against, which is
//! by definition already connected.

mod column_mapping;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dbflux_components::composites::{
    RailItem, WIZARD_MODAL_HEIGHT_FRACTION, WIZARD_MODAL_WIDTH, render_wizard_progress_bar,
    render_wizard_rail, wizard_progress_fraction,
};
use dbflux_components::controls::{Button, Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{
    CancelToken, Connection, DriverCapabilities, TaskId, TaskKind, TaskStatus, TaskTarget,
};
use dbflux_transfer::import::{
    ImportOptions, ImportOutcome, ImportTablePlan, ImportedTable, run_import,
};
use dbflux_transfer::manifest::read_manifest;
use dbflux_transfer::{TableTransferStatus, TransferError};
use dbflux_ui_base::app_state_entity::{AppStateChanged, AppStateEntity};
use dbflux_ui_base::modal_frame::ModalFrame;
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;

pub use column_mapping::TableImportConfig;
use column_mapping::mapping_mode_options;

#[derive(Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    PickFolder,
    Configure,
    Confirm,
    Running,
    Done,
}

/// `Running` and `Done` collapse onto the same rail index so the terminal
/// steps of the five-variant `WizardStep` state machine share the rail's
/// fourth "Run" entry instead of growing a fifth marker.
fn rail_index(step: WizardStep) -> usize {
    match step {
        WizardStep::PickFolder => 0,
        WizardStep::Configure => 1,
        WizardStep::Confirm => 2,
        WizardStep::Running | WizardStep::Done => 3,
    }
}

/// Maps the wizard's current [`WizardStep`] to the shared rail composite's
/// domain-free [`RailItem`]s, marking every entry before the current one as
/// completed — see [`crate::labels::import_rail_labels`] for why `Confirm`
/// may show completed without the user ever visiting it.
fn import_rail_items(step: WizardStep) -> Vec<RailItem> {
    let current_index = rail_index(step);

    crate::labels::import_rail_labels()
        .into_iter()
        .enumerate()
        .map(|(index, label)| RailItem {
            label: SharedString::from(label),
            completed: index < current_index,
            current: index == current_index,
        })
        .collect()
}

/// The terminal action to apply to the run's task registry entry.
enum RunTaskAction {
    Complete,
    Cancel,
    Fail(String),
}

/// The fully-resolved outcome of an import run: the task action, an optional
/// user-facing error to report, whether to toast success, and the Done
/// screen's summary and status lines. Computed once so the task-finalization
/// path and the UI-reflection path agree even if the wizard entity is dropped
/// between them (mirrors the export wizard's resolution).
struct RunResolution {
    task_action: RunTaskAction,
    report: Option<String>,
    toast_success: bool,
    summary: String,
    warnings: Vec<String>,
}

/// Maps a finished `run_import` call onto its [`RunResolution`]. A cancelled
/// run is neither a failure nor a success: the task ends cancelled, nothing is
/// reported as an error, and the Done screen states how many rows were
/// already written — the engine commits each chunk that was in flight when
/// the cancel arrived, so those rows stay in the target.
fn resolve_run_outcome(result: Result<ImportOutcome, TransferError>) -> RunResolution {
    match result {
        Ok(outcome) if outcome.cancelled => {
            let mut warnings = vec![dbflux_i18n::t!(
                "document.import_wizard.done.cancelled_rows",
                rows = imported_row_count(&outcome.tables)
            )];
            warnings.extend(ImportWizard::itemized_status_lines(
                &outcome.tables,
                &outcome.warnings,
            ));

            RunResolution {
                task_action: RunTaskAction::Cancel,
                report: None,
                toast_success: false,
                summary: dbflux_i18n::t!("document.import_wizard.toast.cancelled"),
                warnings,
            }
        }
        Ok(outcome) => {
            let failed_table = outcome.tables.iter().find_map(|t| match &t.status {
                TableTransferStatus::Failed { error } => {
                    Some((t.source_table.clone(), error.clone()))
                }
                _ => None,
            });

            let summary = ImportWizard::summarize(&outcome);
            let warnings = ImportWizard::itemized_status_lines(&outcome.tables, &outcome.warnings);

            match failed_table {
                Some((table, error)) => RunResolution {
                    task_action: RunTaskAction::Fail(format!("{table}: {error}")),
                    report: Some(dbflux_i18n::t!(
                        "document.import_wizard.toast.table_failed",
                        table = table,
                        error = error
                    )),
                    toast_success: false,
                    summary,
                    warnings,
                },
                None => RunResolution {
                    task_action: RunTaskAction::Complete,
                    report: None,
                    toast_success: true,
                    summary,
                    warnings,
                },
            }
        }
        Err(e) => {
            let message =
                dbflux_i18n::t!("document.import_wizard.toast.failed", error = e.to_string());

            RunResolution {
                task_action: RunTaskAction::Fail(e.to_string()),
                report: Some(message.clone()),
                toast_success: false,
                summary: message,
                warnings: Vec::new(),
            }
        }
    }
}

/// Rows written across every table that reached `Completed`, plus the rows
/// the partially loaded table a cancel stopped (`Cancelled`) kept.
fn imported_row_count(tables: &[ImportedTable]) -> u64 {
    tables
        .iter()
        .map(|t| match &t.status {
            TableTransferStatus::Completed { rows } | TableTransferStatus::Cancelled { rows } => {
                *rows
            }
            _ => 0,
        })
        .sum()
}

/// One table row's live controls, wrapping the pure [`TableImportConfig`]
/// with the `Dropdown` entities the user adjusts it through. Item lists are
/// static (source/target column names don't change once the folder is
/// picked); only selections and `config` mutate.
struct TableImportRow {
    config: TableImportConfig,
    mapping_mode_dropdown: Entity<Dropdown>,
    rebind_target_dropdown: Entity<Dropdown>,
    rebind_source_dropdown: Entity<Dropdown>,
}

pub struct ImportWizard {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    visible: bool,
    profile_id: Option<Uuid>,
    database: Option<String>,
    step: WizardStep,
    manifest_dir: Option<PathBuf>,
    manifest_error: Option<String>,
    rows: Vec<TableImportRow>,
    supports_truncate: bool,
    /// Set only by the Confirm step's "Yes, proceed" click; reset on
    /// open()/cancel/back. This — not a derived `rows.any(is_destructive)` —
    /// is what reaches `ImportOptions::destructive_confirmed`, so the
    /// engine's destructive gate actually requires the explicit confirm
    /// rather than being trivially satisfied by the plan itself (B-003).
    confirmed_destructive: bool,
    loading: bool,
    running: bool,
    progress: Arc<Mutex<(u64, Option<u64>)>>,
    /// The running import's cancel token, shared with its Tasks-panel entry;
    /// `Some` only while a run is in flight.
    cancel_token: Option<CancelToken>,
    active_task_id: Option<TaskId>,
    result_summary: Option<String>,
    result_warnings: Vec<String>,
    _row_subscriptions: Vec<Subscription>,
}

impl ImportWizard {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            focus_handle: cx.focus_handle(),
            visible: false,
            profile_id: None,
            database: None,
            step: WizardStep::PickFolder,
            manifest_dir: None,
            manifest_error: None,
            rows: Vec::new(),
            supports_truncate: false,
            confirmed_destructive: false,
            loading: false,
            running: false,
            progress: Arc::new(Mutex::new((0, None))),
            cancel_token: None,
            active_task_id: None,
            result_summary: None,
            result_warnings: Vec::new(),
            _row_subscriptions: Vec::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.visible = true;
        self.profile_id = Some(profile_id);
        self.database = database;
        self.step = WizardStep::PickFolder;
        self.manifest_dir = None;
        self.manifest_error = None;
        self.rows.clear();
        self._row_subscriptions.clear();
        self.confirmed_destructive = false;
        self.loading = false;
        self.running = false;
        self.cancel_token = None;
        self.active_task_id = None;
        self.result_summary = None;
        self.result_warnings.clear();
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.notify();
    }

    fn resolve_connection(&self, cx: &App) -> Option<Arc<dyn Connection>> {
        let profile_id = self.profile_id?;
        let connected = self.app_state.read(cx).connections().get(&profile_id)?;

        Some(match &self.database {
            Some(db) => connected.connection_for_database(db),
            None => connected.connection.clone(),
        })
    }

    fn target_database(&self, connection: &Arc<dyn Connection>) -> String {
        self.database
            .clone()
            .or_else(|| connection.active_database())
            .unwrap_or_default()
    }

    fn choose_folder(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            self.manifest_error = Some(dbflux_i18n::t!(
                "document.import_wizard.pick_folder.error.no_connection"
            ));
            cx.notify();
            return;
        };
        let target_database = self.target_database(&connection);
        let supports_truncate = connection.supports(DriverCapabilities::TRUNCATE_TABLE);
        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        self.loading = true;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let dir = if dialog_available {
                match rfd::AsyncFileDialog::new()
                    .set_title(dbflux_i18n::t!(
                        "document.import_wizard.pick_folder.dialog_title"
                    ))
                    .pick_folder()
                    .await
                {
                    Some(handle) => handle.path().to_path_buf(),
                    None => {
                        this.update(cx, |this, cx| {
                            this.loading = false;
                            cx.notify();
                        })
                        .ok();
                        return;
                    }
                }
            } else {
                this.update(cx, |this, cx| {
                    this.loading = false;
                    this.manifest_error = Some(dbflux_i18n::t!(
                        "document.import_wizard.pick_folder.error.no_dialog"
                    ));
                    cx.notify();
                })
                .ok();
                return;
            };

            let manifest_path = dir.join("manifest.json");
            let manifest_result = cx
                .background_executor()
                .spawn(async move { read_manifest(&manifest_path) })
                .await;

            let manifest = match manifest_result {
                Ok(manifest) => manifest,
                Err(e) => {
                    this.update(cx, |this, cx| {
                        this.loading = false;
                        this.manifest_error = Some(dbflux_i18n::t!(
                            "document.import_wizard.pick_folder.error.invalid_bundle",
                            error = e.to_string()
                        ));
                        cx.notify();
                    })
                    .ok();
                    return;
                }
            };

            let mut configs = Vec::with_capacity(manifest.tables.len());
            for manifest_table in &manifest.tables {
                let target_exists_and_columns = connection
                    .table_details(
                        &target_database,
                        manifest_table.schema.as_deref(),
                        &manifest_table.name,
                    )
                    .ok()
                    .map(|info| {
                        info.columns
                            .unwrap_or_default()
                            .into_iter()
                            .map(|c| dbflux_core::TransferColumn {
                                name: c.name,
                                type_name: Some(c.type_name),
                                nullable: c.nullable,
                                is_primary_key: c.is_primary_key,
                            })
                            .collect::<Vec<_>>()
                    });
                let target_exists = target_exists_and_columns.is_some();
                let target_columns = target_exists_and_columns.unwrap_or_default();

                configs.push(TableImportConfig::new(
                    manifest_table,
                    target_exists,
                    target_columns,
                ));
            }

            this.update(cx, |this, cx| {
                this.loading = false;
                this.manifest_dir = Some(dir);
                this.supports_truncate = supports_truncate;
                this.build_rows(configs, cx);
                this.step = WizardStep::Configure;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn build_rows(&mut self, configs: Vec<TableImportConfig>, cx: &mut Context<Self>) {
        self.rows.clear();
        self._row_subscriptions.clear();
        let supports_truncate = self.supports_truncate;

        for (table_index, config) in configs.into_iter().enumerate() {
            let mode_options = mapping_mode_options(supports_truncate);
            let selected_mode_index = mode_options
                .iter()
                .position(|(_, mode)| *mode == config.mapping_mode);
            let mode_items: Vec<DropdownItem> = mode_options
                .iter()
                .map(|(label, _)| DropdownItem::new(label.clone()))
                .collect();

            let mapping_mode_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("import-mode-{table_index}")))
                    .items(mode_items)
                    .selected_index(selected_mode_index)
                    .placeholder(dbflux_i18n::t!(
                        "document.import_wizard.configure.mode_placeholder"
                    ))
            });

            let target_items: Vec<DropdownItem> = config
                .target_columns
                .iter()
                .map(|c| DropdownItem::new(c.name.clone()))
                .collect();
            let rebind_target_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("import-target-{table_index}")))
                    .items(target_items)
                    .placeholder(dbflux_i18n::t!(
                        "document.import_wizard.configure.target_placeholder"
                    ))
            });

            let mut source_items = vec![DropdownItem::new(dbflux_i18n::t!(
                "document.import_wizard.configure.source_unset"
            ))];
            source_items.extend(
                config
                    .source_columns
                    .iter()
                    .map(|c| DropdownItem::new(c.name.clone())),
            );
            let rebind_source_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("import-source-{table_index}")))
                    .items(source_items)
                    .placeholder(dbflux_i18n::t!(
                        "document.import_wizard.configure.source_placeholder"
                    ))
            });

            let mode_sub = cx.subscribe(
                &mapping_mode_dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    let mode_options = mapping_mode_options(this.supports_truncate);
                    if let Some((_, mode)) = mode_options.get(event.index)
                        && let Some(row) = this.rows.get_mut(table_index)
                    {
                        row.config.mapping_mode = *mode;
                        cx.notify();
                    }
                },
            );

            self.rows.push(TableImportRow {
                config,
                mapping_mode_dropdown,
                rebind_target_dropdown,
                rebind_source_dropdown,
            });
            self._row_subscriptions.push(mode_sub);
        }
    }

    fn apply_rebind(&mut self, table_index: usize, cx: &mut Context<Self>) {
        let Some(row) = self.rows.get(table_index) else {
            return;
        };

        let Some(target_index) = row
            .rebind_target_dropdown
            .read(cx)
            .selected_value()
            .and_then(|value| {
                row.config
                    .target_columns
                    .iter()
                    .position(|c| c.name.as_str() == value.as_ref())
            })
        else {
            return;
        };

        let source_index = row
            .rebind_source_dropdown
            .read(cx)
            .selected_value()
            .and_then(|value| {
                row.config
                    .source_columns
                    .iter()
                    .position(|c| c.name.as_str() == value.as_ref())
            });

        if let Some(row) = self.rows.get_mut(table_index) {
            row.config.set_binding(target_index, source_index);
        }
        cx.notify();
    }

    fn continue_from_configure(&mut self, cx: &mut Context<Self>) {
        let has_destructive = self.rows.iter().any(|row| row.config.is_destructive());
        if has_destructive {
            self.step = WizardStep::Confirm;
        } else {
            self.start_import(cx);
        }
        cx.notify();
    }

    fn confirm_destructive_and_run(&mut self, cx: &mut Context<Self>) {
        self.confirmed_destructive = true;
        self.start_import(cx);
        cx.notify();
    }

    /// Starts the import run. The wizard enters `Running` only once the run's
    /// task and cancel token exist, so the running step's Cancel always has a
    /// token to trip. When the run cannot start (the connection went away, or
    /// no bundle is loaded) the wizard returns to `Configure` with the
    /// destructive confirmation cleared, as a Back from `Confirm` would.
    fn start_import(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.import_wizard.error.no_connection"),
                ),
                cx,
            );
            self.return_to_configure();
            return;
        };
        let Some(manifest_dir) = self.manifest_dir.clone() else {
            self.return_to_configure();
            return;
        };
        let target_database = self.target_database(&connection);
        let profile_id = self.profile_id;
        let database = self.database.clone();

        let plans: Vec<ImportTablePlan> = self
            .rows
            .iter()
            .map(|row| ImportTablePlan {
                source_table: row.config.source_table.clone(),
                target_schema: row.config.target_schema.clone(),
                target_table: row.config.target_table.clone(),
                mapping_mode: row.config.mapping_mode,
                column_overrides: Some(row.config.to_overrides()),
            })
            .collect();
        let destructive_confirmed = self.confirmed_destructive;

        self.running = true;
        self.result_summary = None;
        self.result_warnings.clear();
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = (0, None);

        let description = crate::labels::import_wizard_task_label(plans.len());
        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Import,
                description,
                profile_id.map(|profile_id| TaskTarget {
                    profile_id,
                    database: database.clone(),
                }),
            );
            cx.emit(AppStateChanged);
            pair
        });
        self.active_task_id = Some(task_id);
        self.cancel_token = Some(cancel_token.clone());
        self.step = WizardStep::Running;

        let app_state = self.app_state.clone();
        let progress = Arc::clone(&self.progress);
        let ticker_progress = Arc::clone(&self.progress);
        let ticker_app_state = app_state.clone();

        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(150))
                    .await;

                let still_running = cx.update(|cx| {
                    ticker_app_state.update(cx, |state, cx| {
                        let Some(snapshot) = state.tasks().get(task_id) else {
                            return false;
                        };
                        if snapshot.status != TaskStatus::Running {
                            return false;
                        }

                        let (rows_done, estimated_total) = *ticker_progress
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if let Some(total) = estimated_total
                            && total > 0
                        {
                            let fraction = (rows_done as f32 / total as f32).clamp(0.0, 1.0);
                            state.tasks_mut().update_progress(task_id, fraction);
                            cx.notify();
                        }

                        true
                    })
                });

                if !still_running {
                    break;
                }

                // The app-state notify above only refreshes the Tasks panel;
                // re-render the wizard so its own row counter and progress
                // bar advance while the run is in flight.
                this.update(cx, |_this, cx| cx.notify()).ok();
            }
        })
        .detach();

        cx.spawn(async move |this, cx| {
            let import_result = cx
                .background_executor()
                .spawn(async move {
                    let options = ImportOptions {
                        segment_size: 500,
                        target_database,
                        destructive_confirmed,
                    };

                    run_import(
                        &connection,
                        &manifest_dir,
                        &plans,
                        &options,
                        &cancel_token,
                        move |_index, rows_done, estimated_total| {
                            if let Ok(mut guard) = progress.lock() {
                                *guard = (rows_done, estimated_total);
                            }
                        },
                    )
                })
                .await;

            let RunResolution {
                task_action,
                report,
                toast_success,
                summary,
                warnings,
            } = resolve_run_outcome(import_result);

            // The task must reach a terminal state and any failure must reach
            // the foreground even if the wizard entity was dropped, so
            // finalize through the app directly, never gated on `this`.
            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    match &task_action {
                        RunTaskAction::Complete => state.complete_task(task_id),
                        RunTaskAction::Cancel => {
                            state.tasks_mut().cancel(task_id);
                        }
                        RunTaskAction::Fail(message) => state.fail_task(task_id, message.clone()),
                    }
                    cx.emit(AppStateChanged);
                });

                if let Some(message) = &report {
                    report_error(UserFacingError::new(ErrorKind::Driver, message.clone()), cx);
                }
                if toast_success {
                    Toast::success(dbflux_i18n::t!("document.import_wizard.toast.completed"))
                        .push(cx);
                }
            });

            this.update(cx, |this, cx| {
                this.running = false;
                this.cancel_token = None;
                this.step = WizardStep::Done;
                this.result_summary = Some(summary);
                this.result_warnings = warnings;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn return_to_configure(&mut self) {
        self.step = WizardStep::Configure;
        self.confirmed_destructive = false;
    }

    /// Requests cancellation of the running import. The engine checks the
    /// token before each chunk, so the chunk already in flight is written and
    /// committed, and no later chunk or table is started.
    fn cancel_run(&mut self, cx: &mut Context<Self>) {
        if let Some(token) = &self.cancel_token {
            token.cancel();
        }
        cx.notify();
    }

    fn summarize(outcome: &ImportOutcome) -> String {
        let completed = outcome
            .tables
            .iter()
            .filter(|t| matches!(t.status, TableTransferStatus::Completed { .. }))
            .count();
        let skipped = outcome
            .tables
            .iter()
            .filter(|t| matches!(t.status, TableTransferStatus::Skipped))
            .count();
        let failed = outcome
            .tables
            .iter()
            .filter(|t| matches!(t.status, TableTransferStatus::Failed { .. }))
            .count();
        let rows = imported_row_count(&outcome.tables);

        crate::labels::import_summary_label(completed, rows, skipped, failed)
    }

    /// Renders one status line per planned table when the run left any table
    /// `Failed`, `Cancelled` or `NotStarted`, so the user sees exactly which
    /// tables succeeded, which one failed with what error, which one a cancel
    /// stopped partway, and which were never attempted — not just the last
    /// error swallowed into a single toast (R4-002/B-007). On a fully
    /// successful/skipped run, only the engine's own warnings are shown,
    /// unchanged.
    fn itemized_status_lines(tables: &[ImportedTable], engine_warnings: &[String]) -> Vec<String> {
        let has_issue = tables.iter().any(|t| {
            matches!(
                t.status,
                TableTransferStatus::Failed { .. }
                    | TableTransferStatus::Cancelled { .. }
                    | TableTransferStatus::NotStarted
            )
        });

        if !has_issue {
            return engine_warnings.to_vec();
        }

        let mut lines: Vec<String> = tables
            .iter()
            .map(crate::labels::import_table_status_line)
            .collect();
        lines.extend(engine_warnings.iter().cloned());
        lines
    }
}

impl Render for ImportWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            close_entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let mut frame = ModalFrame::new("import-wizard", &self.focus_handle, close)
            .title(dbflux_i18n::t!("document.import_wizard.title"))
            .icon(AppIcon::Download)
            .width(WIZARD_MODAL_WIDTH)
            .height_fraction(WIZARD_MODAL_HEIGHT_FRACTION)
            .center_vertically();

        frame = frame.child(self.render_body(cx));
        frame.render(cx).into_any_element()
    }
}

impl ImportWizard {
    /// Rail + phase-area layout shared with the migrate/export wizard chrome.
    /// The rail is display-only (`on_select: None`) — see `import_rail_items`
    /// — so this wrapping is purely presentational and leaves every step's
    /// own logic and buttons (`render_pick_folder`, `render_configure`, ...)
    /// unchanged.
    fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let phase = match self.step {
            WizardStep::PickFolder => self.render_pick_folder(cx),
            WizardStep::Configure => self.render_configure(cx),
            WizardStep::Confirm => self.render_confirm(cx),
            WizardStep::Running => self.render_running(cx),
            WizardStep::Done => self.render_done(cx),
        };

        div()
            .flex()
            .flex_row()
            .flex_1()
            .min_h(px(0.0))
            .child(render_wizard_rail(
                &import_rail_items(self.step),
                None::<fn(usize, &mut Window, &mut App)>,
                cx,
            ))
            .child(div().flex_1().min_w(px(0.0)).flex().flex_col().child(phase))
            .into_any_element()
    }
}

impl ImportWizard {
    fn render_pick_folder(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(dbflux_i18n::t!(
                "document.import_wizard.pick_folder.description"
            )))
            .when_some(self.manifest_error.clone(), |d, error| {
                d.child(Text::caption(error))
            })
            .child(
                Button::new(
                    "import-wizard-choose-folder",
                    if self.loading {
                        dbflux_i18n::t!("document.import_wizard.pick_folder.reading_manifest")
                    } else {
                        dbflux_i18n::t!("document.import_wizard.pick_folder.choose_folder")
                    },
                )
                .disabled(self.loading)
                .on_click(cx.listener(|this, _, _, cx| this.choose_folder(cx))),
            )
            .into_any_element()
    }

    fn render_configure(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self.rows.iter().enumerate().map(|(table_index, row)| {
            let unmatched = row.config.unmatched_source_names();

            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .p(Spacing::SM)
                .border_1()
                .child(Text::body(format!(
                    "{} → {}",
                    row.config.source_table, row.config.target_table
                )))
                .child(
                    div()
                        .flex()
                        .gap(Spacing::SM)
                        .child(row.mapping_mode_dropdown.clone())
                        .child(row.rebind_target_dropdown.clone())
                        .child(row.rebind_source_dropdown.clone())
                        .child(
                            Button::new(
                                SharedString::from(format!("import-apply-mapping-{table_index}")),
                                dbflux_i18n::t!("document.import_wizard.configure.apply_mapping"),
                            )
                            .ghost()
                            .on_click(cx.listener(
                                move |this, _, _, cx| {
                                    this.apply_rebind(table_index, cx);
                                },
                            )),
                        ),
                )
                .when(!unmatched.is_empty(), |d| {
                    d.child(Text::caption(dbflux_i18n::t!(
                        "document.import_wizard.configure.unmatched_source",
                        names = unmatched.join(", ")
                    )))
                })
                .into_any_element()
        });

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h(px(0.0))
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(
                div()
                    .id("import-wizard-tables")
                    .debug_selector(|| "import-wizard-tables".to_string())
                    .flex_1()
                    .min_h(px(0.0))
                    .overflow_y_scroll()
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .children(rows),
            )
            .child(
                Button::new(
                    "import-wizard-continue",
                    dbflux_i18n::t!("document.import_wizard.configure.continue"),
                )
                .on_click(cx.listener(|this, _, _, cx| this.continue_from_configure(cx))),
            )
            .into_any_element()
    }

    fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let destructive_tables: Vec<String> = self
            .rows
            .iter()
            .filter(|row| row.config.is_destructive())
            .map(|row| row.config.target_table.clone())
            .collect();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(dbflux_i18n::t!(
                "document.import_wizard.confirm.body",
                tables = destructive_tables.join(", ")
            )))
            .child(Text::caption(dbflux_i18n::t!(
                "document.import_wizard.confirm.warning"
            )))
            .child(
                div()
                    .flex()
                    .gap(Spacing::SM)
                    .child(
                        Button::new(
                            "import-wizard-cancel-confirm",
                            dbflux_i18n::t!("document.import_wizard.confirm.back"),
                        )
                        .ghost()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.step = WizardStep::Configure;
                            this.confirmed_destructive = false;
                            cx.notify();
                        })),
                    )
                    .child(
                        Button::new(
                            "import-wizard-confirm-destructive",
                            dbflux_i18n::t!("document.import_wizard.confirm.proceed"),
                        )
                        .on_click(
                            cx.listener(|this, _, _, cx| this.confirm_destructive_and_run(cx)),
                        ),
                    ),
            )
            .into_any_element()
    }

    fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let (rows_done, estimated_total) = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let fraction = wizard_progress_fraction(rows_done, estimated_total);
        let cancel_requested = self
            .cancel_token
            .as_ref()
            .is_none_or(CancelToken::is_cancelled);
        let label = match estimated_total {
            Some(total) if total > 0 => dbflux_i18n::t!(
                "document.import_wizard.running.progress.of_total",
                done = rows_done,
                total = total
            ),
            _ => dbflux_i18n::t!(
                "document.import_wizard.running.progress.only",
                done = rows_done
            ),
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(dbflux_i18n::t!(
                "document.import_wizard.running.title"
            )))
            .child(Text::caption(label))
            .when_some(fraction, |d, fraction| {
                d.child(render_wizard_progress_bar(fraction, cx))
            })
            .child(
                div().flex().justify_end().child(
                    Button::new(
                        "import-wizard-cancel-run",
                        dbflux_i18n::t!("document.import_wizard.running.cancel"),
                    )
                    .ghost()
                    .disabled(cancel_requested)
                    .on_click(cx.listener(|this, _, _, cx| this.cancel_run(cx))),
                ),
            )
            .into_any_element()
    }

    fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .when_some(self.result_summary.clone(), |d, summary| {
                d.child(Text::body(summary))
            })
            .when(!self.result_warnings.is_empty(), |d| {
                d.child(Text::caption(self.result_warnings.join("; ")))
            })
            .child(
                Button::new(
                    "import-wizard-close",
                    dbflux_i18n::t!("document.import_wizard.done.close"),
                )
                .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    // `use super::*` would re-glob the parent module's `use gpui::*`, and
    // combining that wildcard with `#[gpui::test]` blows rustc's macro
    // recursion limit in this crate — import only what the tests need.
    use super::{
        AppStateEntity, ImportWizard, RunTaskAction, TableImportConfig, WizardStep,
        resolve_run_outcome,
    };
    use crate::migrate_wizard::source_target::shared_coordinator_tests::{
        PickerFakeConnection, connect_profile,
    };
    use dbflux_core::{CancelToken, SchemaLoadingStrategy};
    use dbflux_transfer::import::{ImportOutcome, ImportedTable};
    use dbflux_transfer::manifest::ManifestTable;
    use dbflux_transfer::{TableMappingMode, TableTransferStatus, TransferError};
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext, Entity, px};
    use uuid::Uuid;

    fn isolated_test_app_state(cx: &mut gpui::TestAppContext) -> Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        })
    }

    fn init_test_runtime(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });
    }

    /// A single-column table plan named `name` that targets an existing
    /// table, so it runs without the destructive confirmation.
    fn table_config(name: &str) -> TableImportConfig {
        let manifest_table = ManifestTable {
            schema: None,
            name: name.to_string(),
            file: format!("{name}.csv"),
            format: "csv".to_string(),
            columns: vec![dbflux_core::TransferColumn {
                name: "id".to_string(),
                type_name: Some("integer".to_string()),
                nullable: false,
                is_primary_key: true,
            }],
            row_count: 0,
            fk_order_index: 0,
        };
        let mut config = TableImportConfig::new(&manifest_table, true, Vec::new());
        config.mapping_mode = TableMappingMode::Existing;
        config
    }

    /// One destructive (`Recreate`) table plan — the shape that must route
    /// through the Confirm step before `confirmed_destructive` may become
    /// `true`.
    fn destructive_table_config() -> TableImportConfig {
        let manifest_table = ManifestTable {
            schema: None,
            name: "users".to_string(),
            file: "users.csv".to_string(),
            format: "csv".to_string(),
            columns: vec![dbflux_core::TransferColumn {
                name: "id".to_string(),
                type_name: Some("integer".to_string()),
                nullable: false,
                is_primary_key: true,
            }],
            row_count: 0,
            fk_order_index: 0,
        };
        let mut config = TableImportConfig::new(&manifest_table, true, Vec::new());
        config.mapping_mode = TableMappingMode::Recreate;
        config
    }

    /// B-003/JD-W2 regression: a destructive plan routes to the Confirm step
    /// — merely reaching it (without clicking "Yes, proceed") must NOT set
    /// `confirmed_destructive`. Before the fix this flag didn't exist and the
    /// engine gate was fed `rows.any(is_destructive)`, which is trivially
    /// true for exactly this scenario — an always-satisfied (inert) gate.
    #[gpui::test]
    fn continue_from_configure_with_a_destructive_plan_leaves_the_confirm_flag_false(
        cx: &mut gpui::TestAppContext,
    ) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let wizard = cx.update(|cx| cx.new(|cx| ImportWizard::new(app_state, cx)));

        wizard.update(cx, |this, cx| {
            this.build_rows(vec![destructive_table_config()], cx);
        });
        wizard.update(cx, |this, cx| this.continue_from_configure(cx));

        let (step, confirmed) =
            cx.update(|cx| (wizard.read(cx).step, wizard.read(cx).confirmed_destructive));

        assert!(
            matches!(step, WizardStep::Confirm),
            "a destructive plan must route to the Confirm step, not start immediately"
        );
        assert!(
            !confirmed,
            "reaching the Confirm step must not itself set the confirm flag"
        );
    }

    /// B-003/JD-W2 regression: only the explicit "Yes, proceed" action may
    /// set `confirmed_destructive` — this is what `start_import` reads into
    /// `ImportOptions::destructive_confirmed`. With a live connection the run
    /// starts, and the wizard enters `Running` holding the run's task and
    /// cancel token.
    #[gpui::test]
    fn confirm_destructive_and_run_sets_the_confirm_flag_and_starts_the_run(
        cx: &mut gpui::TestAppContext,
    ) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &app_state,
            cx,
            profile_id,
            PickerFakeConnection::new(SchemaLoadingStrategy::SingleDatabase),
            None,
        );
        let wizard = cx.update(|cx| cx.new(|cx| ImportWizard::new(app_state, cx)));

        wizard.update(cx, |this, cx| {
            this.profile_id = Some(profile_id);
            this.manifest_dir = Some(std::env::temp_dir().join("dbflux-import-wizard-no-bundle"));
            this.build_rows(vec![destructive_table_config()], cx);
            this.step = WizardStep::Confirm;
        });
        wizard.update(cx, |this, cx| this.confirm_destructive_and_run(cx));

        cx.update(|cx| {
            let wizard = wizard.read(cx);
            assert!(
                wizard.confirmed_destructive,
                "the explicit Yes-proceed action must set the confirm flag"
            );
            assert!(matches!(wizard.step, WizardStep::Running));
            assert!(wizard.cancel_token.is_some());
            assert!(wizard.active_task_id.is_some());
        });
    }

    /// When the connection is gone by the time the run starts, the wizard
    /// never enters `Running` without a task or cancel token (where Cancel
    /// would do nothing): it reports the error and returns to `Configure`.
    #[gpui::test]
    fn continue_from_configure_without_a_connection_returns_to_configure(
        cx: &mut gpui::TestAppContext,
    ) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let wizard = cx.update(|cx| cx.new(|cx| ImportWizard::new(app_state.clone(), cx)));

        wizard.update(cx, |this, cx| {
            this.profile_id = Some(Uuid::new_v4());
            this.manifest_dir = Some(std::env::temp_dir());
            this.build_rows(vec![table_config("users")], cx);
            this.step = WizardStep::Configure;
        });
        wizard.update(cx, |this, cx| this.continue_from_configure(cx));

        cx.update(|cx| {
            let wizard = wizard.read(cx);
            assert!(matches!(wizard.step, WizardStep::Configure));
            assert!(!wizard.running);
            assert!(wizard.cancel_token.is_none());
            assert!(wizard.active_task_id.is_none());
            assert!(
                app_state.read(cx).tasks().recent_tasks(10).is_empty(),
                "no task may be registered for a run that never started"
            );
        });
    }

    /// The destructive path loses its connection the same way: back to
    /// `Configure`, with the confirmation cleared so the next run must be
    /// confirmed again.
    #[gpui::test]
    fn confirm_destructive_and_run_without_a_connection_returns_to_configure(
        cx: &mut gpui::TestAppContext,
    ) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let wizard = cx.update(|cx| cx.new(|cx| ImportWizard::new(app_state, cx)));

        wizard.update(cx, |this, cx| {
            this.profile_id = Some(Uuid::new_v4());
            this.manifest_dir = Some(std::env::temp_dir());
            this.build_rows(vec![destructive_table_config()], cx);
            this.step = WizardStep::Confirm;
        });
        wizard.update(cx, |this, cx| this.confirm_destructive_and_run(cx));

        cx.update(|cx| {
            let wizard = wizard.read(cx);
            assert!(matches!(wizard.step, WizardStep::Configure));
            assert!(!wizard.confirmed_destructive);
            assert!(wizard.cancel_token.is_none());
            assert!(wizard.active_task_id.is_none());
        });
    }

    /// A bundle with more tables than fit in the modal keeps the table list
    /// inside a bounded scroll container that ends inside the window, rather
    /// than growing past the modal with every row.
    #[gpui::test]
    fn configure_step_scrolls_a_long_table_list(cx: &mut gpui::TestAppContext) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);

        let (_root, window) = cx.add_window_view(|window, cx| {
            let wizard = cx.new(|cx| ImportWizard::new(app_state, cx));
            wizard.update(cx, |this, cx| {
                this.open(Uuid::new_v4(), None, window, cx);
                let configs = (0..60)
                    .map(|index| table_config(&format!("table_{index}")))
                    .collect();
                this.build_rows(configs, cx);
                this.step = WizardStep::Configure;
            });
            gpui_component::Root::new(wizard, window, cx)
        });
        window.run_until_parked();

        let viewport = window.update(|window, _| window.viewport_size());
        let list = window
            .debug_bounds("import-wizard-tables")
            .expect("the configure step should render its table list");

        assert!(
            list.bottom() <= viewport.height,
            "the table list {list:?} must end inside the window {viewport:?}"
        );
        assert!(
            list.size.height < px(60.0 * 40.0),
            "sixty table rows cannot fit in {list:?}, so the list must be clipped and scroll"
        );
    }

    fn imported(name: &str, status: TableTransferStatus) -> ImportedTable {
        ImportedTable {
            source_table: name.to_string(),
            target_table: name.to_string(),
            status,
        }
    }

    fn outcome(tables: Vec<ImportedTable>, cancelled: bool) -> ImportOutcome {
        ImportOutcome {
            tables,
            warnings: Vec::new(),
            cancelled,
        }
    }

    /// A cancelled run ends its task as cancelled, never as a failure or a
    /// success, tells the user how many rows were already written, and lists
    /// each table's status with the one the cancel stopped as `Cancelled`.
    #[test]
    fn resolve_run_outcome_cancelled_run_cancels_the_task_and_reports_rows_written() {
        let result = Ok(outcome(
            vec![
                imported("users", TableTransferStatus::Completed { rows: 500 }),
                imported("orders", TableTransferStatus::Cancelled { rows: 20 }),
                imported("line_items", TableTransferStatus::NotStarted),
            ],
            true,
        ));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Cancel));
        assert!(resolution.report.is_none());
        assert!(!resolution.toast_success);
        assert_eq!(resolution.summary, "Import cancelled");
        assert_eq!(
            resolution.warnings,
            vec![
                "520 row(s) were imported before the import stopped.".to_string(),
                "users: completed (500 row(s))".to_string(),
                "orders: cancelled after 20 row(s)".to_string(),
                "line_items: not attempted".to_string(),
            ],
            "the table the cancel stopped must be listed as cancelled, not completed"
        );
    }

    #[test]
    fn resolve_run_outcome_all_success_completes_the_task_and_toasts() {
        let result = Ok(outcome(
            vec![imported(
                "users",
                TableTransferStatus::Completed { rows: 3 },
            )],
            false,
        ));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Complete));
        assert!(resolution.report.is_none());
        assert!(resolution.toast_success);
        assert_eq!(
            resolution.summary,
            "Imported 1 table(s), 3 row(s) total (0 skipped)"
        );
    }

    #[test]
    fn resolve_run_outcome_table_failure_fails_the_task_and_reports_once() {
        let result = Ok(outcome(
            vec![
                imported("users", TableTransferStatus::Completed { rows: 3 }),
                imported(
                    "orders",
                    TableTransferStatus::Failed {
                        error: "constraint violation".to_string(),
                    },
                ),
            ],
            false,
        ));

        let resolution = resolve_run_outcome(result);

        match resolution.task_action {
            RunTaskAction::Fail(ref message) => {
                assert_eq!(message, "orders: constraint violation");
            }
            _ => panic!("a per-table failure must fail the task"),
        }
        assert_eq!(
            resolution.report.as_deref(),
            Some("Import failed on table 'orders': constraint violation")
        );
        assert!(!resolution.toast_success);
    }

    #[test]
    fn resolve_run_outcome_engine_error_fails_the_task_and_reports_once() {
        let result: Result<ImportOutcome, TransferError> =
            Err(TransferError::Sink("disk full".to_string()));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Fail(_)));
        assert_eq!(
            resolution.report.as_deref(),
            Some("Import failed: sink error: disk full")
        );
        assert!(!resolution.toast_success);
    }

    /// The running step's Cancel trips the same token the engine and the
    /// Tasks panel entry share.
    #[gpui::test]
    fn cancel_run_cancels_the_running_import_token(cx: &mut gpui::TestAppContext) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let wizard = cx.update(|cx| cx.new(|cx| ImportWizard::new(app_state, cx)));
        let token = CancelToken::new();

        wizard.update(cx, |this, cx| {
            this.cancel_token = Some(token.clone());
            this.cancel_run(cx);
        });

        assert!(token.is_cancelled());
    }
}
