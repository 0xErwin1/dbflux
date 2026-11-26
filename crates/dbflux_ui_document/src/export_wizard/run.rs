//! Export run execution: builds `ExportTable`s from the wizard's resolved
//! table selection, registers the `TaskManager` entry, drives the streamed
//! `run_export` call on the background executor with the same 150 ms
//! progress ticker and cancellation wiring the sidebar's original inline
//! export action used, and resolves the outcome into task/report/toast
//! actions plus the Done screen's summary and per-table status lines.
//!
//! Reused unchanged: `dbflux_transfer::export::{run_export, ExportOptions}`
//! and the timestamped `dbflux-export-<ts>` folder-bundle + `manifest.json`
//! output they produce. `Connection::table_details` is still called directly
//! per table (not routed through the metadata-cache seam the migrate wizard
//! uses) — that mirrors the sidebar's original behavior exactly.

use std::path::PathBuf;
use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, TaskId, TaskKind, TaskTarget};
use dbflux_export::FormatOptions;
use dbflux_transfer::TableTransferStatus;
use dbflux_transfer::export::{
    ExportOptions, ExportOutcome, ExportTable, ExportedTable, run_export,
};
use dbflux_transfer::{FileFormat, TransferError};
use dbflux_ui_base::app_state_entity::AppStateChanged;
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use crate::export_wizard::ExportWizard;
use crate::export_wizard::notes::{ResultKind, ResultRow};
use crate::export_wizard::phases::{ExportPhase, RunState};
use crate::labels::{
    export_not_attempted_after_failure_line, export_summary_label, export_table_status_line,
    export_wizard_task_label,
};
use crate::progress_forwarding::spawn_task_progress_forwarder;

/// Live counters for the running export: which table (by the wizard's fixed
/// table order) is currently streaming, and its row progress — mirrors the
/// migrate wizard's `RunProgress`.
#[derive(Clone, Copy, Default)]
pub(crate) struct RunProgress {
    pub(crate) table_index: usize,
    pub(crate) rows_done: u64,
    pub(crate) estimated_total: Option<u64>,
}

/// Converts driver-reported column metadata into the transfer engine's
/// column shape — the same mapping the sidebar's original export action used.
fn to_transfer_columns(columns: Vec<dbflux_core::ColumnInfo>) -> Vec<dbflux_core::TransferColumn> {
    columns
        .into_iter()
        .map(|c| dbflux_core::TransferColumn {
            kind: dbflux_core::infer_column_kind(&c.type_name),
            name: c.name,
            type_name: Some(c.type_name),
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
        })
        .collect()
}

/// A `schema.table` (or bare `table`) label for error/status messages.
fn table_label(schema: &Option<String>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    }
}

/// Fetches each selected table's column shape through `Connection::table_details`
/// and assembles the engine's `ExportTable`s. Any single table's fetch
/// failure aborts the whole build and is reported as one error naming the
/// failing table — the same short-circuit the sidebar's original inline
/// export action used.
pub(crate) fn build_export_tables(
    connection: &Arc<dyn Connection>,
    database: Option<&str>,
    table_specs: &[(Option<String>, String)],
) -> Result<Vec<ExportTable>, String> {
    let mut export_tables = Vec::with_capacity(table_specs.len());

    for (schema, name) in table_specs {
        let effective_database = database
            .map(str::to_string)
            .unwrap_or_else(|| schema.clone().unwrap_or_default());

        match connection.table_details(&effective_database, schema.as_deref(), name) {
            Ok(info) => {
                let columns = to_transfer_columns(info.columns.unwrap_or_default());
                export_tables.push(ExportTable {
                    schema: schema.clone(),
                    name: name.clone(),
                    columns,
                    // `TableSource` auto-counts via `SELECT COUNT(*)` when
                    // this is `None`, so the run's progress fraction is real
                    // rather than permanently stuck at 0.
                    estimated_total: None,
                });
            }
            Err(e) => {
                return Err(format!("{}: {e}", table_label(schema, name)));
            }
        }
    }

    Ok(export_tables)
}

/// The engine's options for one run: the wizard's format choices travel with
/// the format and segment size, so the files and the manifest use them.
fn build_export_options<'a>(
    driver_id: &'a str,
    database: &'a str,
    format: FileFormat,
    segment_size: u32,
    format_options: FormatOptions,
) -> ExportOptions<'a> {
    ExportOptions {
        driver_id,
        database,
        format,
        segment_size,
        format_options,
    }
}

/// The owned inputs handed to the off-thread run task.
pub(crate) struct ExportRunContext {
    pub(crate) connection: Arc<dyn Connection>,
    pub(crate) driver_id: String,
    pub(crate) database: String,
    pub(crate) export_tables: Vec<ExportTable>,
    pub(crate) output_dir: PathBuf,
    pub(crate) format: FileFormat,
    pub(crate) segment_size: u32,
    pub(crate) format_options: FormatOptions,
    pub(crate) cancel_token: CancelToken,
}

/// The terminal action to apply to the run's task registry entry.
enum RunTaskAction {
    Complete,
    Cancel,
    Fail(String),
}

/// The fully-resolved outcome of an export run: the task action, an optional
/// user-facing error to report, whether to toast success, and the summary /
/// per-table lines for the Done screen. Computed once so the task-finalization
/// path and the UI-reflection path stay consistent even if the wizard entity
/// is dropped between them.
struct RunResolution {
    task_action: RunTaskAction,
    task_action_details: Option<String>,
    report: Option<String>,
    toast_success: bool,
    summary: String,
    /// One row per planned table, in plan order.
    rows: Vec<ResultRow>,
    /// The engine's own warnings, which belong to no table.
    notes: Vec<String>,
}

fn resolve_run_outcome(result: Result<ExportOutcome, TransferError>) -> RunResolution {
    match result {
        Ok(outcome) if outcome.cancelled => RunResolution {
            task_action: RunTaskAction::Cancel,
            task_action_details: None,
            report: None,
            toast_success: false,
            summary: dbflux_i18n::t!("document.export_wizard.toast.cancelled"),
            rows: result_rows(&outcome.tables),
            notes: outcome.warnings,
        },
        Ok(outcome) => {
            let failed_table = outcome.tables.iter().find_map(|t| match &t.status {
                TableTransferStatus::Failed { error } => Some((t.name.clone(), error.clone())),
                _ => None,
            });

            let summary = summarize(&outcome);
            let rows = result_rows(&outcome.tables);
            let notes = outcome.warnings;

            match failed_table {
                Some((table, error)) => RunResolution {
                    task_action: RunTaskAction::Fail(format!("{table}: {error}")),
                    task_action_details: Some(itemized_table_details(&outcome.tables)),
                    report: Some(dbflux_i18n::t!(
                        "document.export_wizard.toast.table_failed",
                        table = table,
                        error = error
                    )),
                    toast_success: false,
                    summary,
                    rows,
                    notes,
                },
                None => RunResolution {
                    task_action: RunTaskAction::Complete,
                    task_action_details: None,
                    report: None,
                    toast_success: true,
                    summary,
                    rows,
                    notes,
                },
            }
        }
        Err(e) => {
            let message = dbflux_i18n::t!("document.export_wizard.toast.failed", error = e);
            RunResolution {
                task_action: RunTaskAction::Fail(e.to_string()),
                task_action_details: None,
                report: Some(message.clone()),
                toast_success: false,
                summary: message,
                rows: Vec::new(),
                notes: Vec::new(),
            }
        }
    }
}

fn summarize(outcome: &ExportOutcome) -> String {
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
    let rows: u64 = outcome
        .tables
        .iter()
        .map(|t| match &t.status {
            TableTransferStatus::Completed { rows } => *rows,
            _ => 0,
        })
        .sum();

    export_summary_label(completed, rows, skipped, failed)
}

/// One result row per planned table, so the user sees which tables
/// succeeded, which one failed with what error, which one a cancel stopped
/// partway, and which were never attempted. A table never attempted because
/// an earlier one failed says so.
fn result_rows(tables: &[ExportedTable]) -> Vec<ResultRow> {
    let stopped_by_failure = tables
        .iter()
        .any(|table| matches!(table.status, TableTransferStatus::Failed { .. }));

    tables
        .iter()
        .map(|table| {
            let kind = ResultKind::of(&table.status);
            let text = if kind == ResultKind::NotAttempted && stopped_by_failure {
                export_not_attempted_after_failure_line(&table_label(&table.schema, &table.name))
            } else {
                table_status_line(table)
            };

            ResultRow { kind, text }
        })
        .collect()
}

fn table_status_line(table: &ExportedTable) -> String {
    let label = table_label(&table.schema, &table.name);
    export_table_status_line(&label, &table.status)
}

/// Renders the Tasks panel's expandable per-table details for a failed run.
fn itemized_table_details(tables: &[ExportedTable]) -> String {
    tables
        .iter()
        .map(table_status_line)
        .collect::<Vec<_>>()
        .join("\n")
}

impl ExportWizard {
    /// Resolves the connection, fetches the selected tables' column shapes,
    /// registers the run's `TaskManager` entry, and hands off to the
    /// progress ticker and the background `run_export` call. Any table-schema
    /// fetch failure fails the task immediately and never starts the run.
    pub(crate) fn start_export(&mut self, cx: &mut Context<Self>) {
        if self.run_state == RunState::Running {
            return;
        }

        let Some(connection) = self.resolve_connection(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.export_wizard.error.no_connection"),
                ),
                cx,
            );
            return;
        };
        let Some(base_dir) = self.output_dir.clone() else {
            return;
        };

        let profile_id = self.profile_id;
        let database = self.database.clone();
        let profile_label = self.profile_label(cx);
        let driver_id = connection.metadata().id.clone();
        let format = self.selected_format();
        let segment_size = self.segment_size;
        let format_options = self.format_options.clone();
        let table_specs: Vec<(Option<String>, String)> = self
            .tables
            .iter()
            .map(|t| (t.schema.clone(), t.name.clone()))
            .collect();

        self.phase = ExportPhase::Run;
        self.run_state = RunState::Running;
        self.result_summary = None;
        self.result_rows.clear();
        self.result_notes.clear();
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = RunProgress::default();

        let output_dir = base_dir.join(format!(
            "dbflux-export-{}",
            dbflux_core::chrono::Utc::now().format("%Y%m%d-%H%M%S%.3f")
        ));
        let description = export_wizard_task_label(table_specs.len(), &profile_label);

        let app_state = self.app_state.clone();
        let (task_id, cancel_token) = app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Export,
                description,
                profile_id.map(|profile_id| TaskTarget {
                    profile_id,
                    database: database.clone(),
                }),
            );
            cx.emit(AppStateChanged);
            pair
        });
        self.cancel_token = Some(cancel_token.clone());
        cx.notify();

        cx.spawn(async move |this, cx| {
            let export_tables =
                match build_export_tables(&connection, database.as_deref(), &table_specs) {
                    Ok(tables) => tables,
                    Err(err) => {
                        cx.update(|cx| {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(task_id, err.clone());
                                cx.emit(AppStateChanged);
                            });
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    dbflux_i18n::t!(
                                        "document.export_wizard.toast.schema_fetch_failed",
                                        error = err
                                    ),
                                ),
                                cx,
                            );
                        });

                        this.update(cx, |this, cx| {
                            this.run_state = RunState::Done;
                            this.cancel_token = None;
                            this.result_summary = Some(dbflux_i18n::t!(
                                "document.export_wizard.toast.failed",
                                error = err
                            ));
                            cx.notify();
                        })
                        .ok();
                        return;
                    }
                };

            this.update(cx, |this, cx| {
                this.spawn_progress_ticker(task_id, cx);
                this.spawn_run(
                    ExportRunContext {
                        connection,
                        driver_id,
                        database: database.unwrap_or_default(),
                        export_tables,
                        output_dir,
                        format,
                        segment_size,
                        format_options,
                        cancel_token,
                    },
                    task_id,
                    cx,
                );
            })
            .ok();
        })
        .detach();
    }

    fn spawn_progress_ticker(&self, task_id: TaskId, cx: &mut Context<Self>) {
        let ticker_progress = Arc::clone(&self.progress);

        spawn_task_progress_forwarder(
            self.app_state.clone(),
            task_id,
            move || {
                let progress = *ticker_progress
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (progress.rows_done, progress.estimated_total)
            },
            cx,
        );
    }

    fn spawn_run(&self, run: ExportRunContext, task_id: TaskId, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let progress = Arc::clone(&self.progress);

        cx.spawn(async move |this, cx| {
            let ExportRunContext {
                connection,
                driver_id,
                database,
                export_tables,
                output_dir,
                format,
                segment_size,
                format_options,
                cancel_token,
            } = run;
            let output_dir_for_toast = output_dir.clone();

            let export_result = cx
                .background_executor()
                .spawn(async move {
                    let options = build_export_options(
                        &driver_id,
                        &database,
                        format,
                        segment_size,
                        format_options,
                    );

                    run_export(
                        &connection,
                        &export_tables,
                        &output_dir,
                        &options,
                        &cancel_token,
                        move |table_index, rows_done, estimated_total| {
                            if let Ok(mut guard) = progress.lock() {
                                *guard = RunProgress {
                                    table_index,
                                    rows_done,
                                    estimated_total,
                                };
                            }
                        },
                    )
                })
                .await;

            let RunResolution {
                task_action,
                task_action_details,
                report,
                toast_success,
                summary,
                rows,
                notes,
            } = resolve_run_outcome(export_result);

            // The run's task MUST reach a terminal state and any failure MUST
            // reach the foreground even if the wizard entity was dropped
            // (modal closed/reopened) — so finalize the task and report
            // through the app directly, never gated on `this` being alive.
            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    match &task_action {
                        RunTaskAction::Complete => state.complete_task(task_id),
                        RunTaskAction::Cancel => {
                            state.tasks_mut().cancel(task_id);
                        }
                        RunTaskAction::Fail(message) => match &task_action_details {
                            Some(details) => state.fail_task_with_details(
                                task_id,
                                message.clone(),
                                details.clone(),
                            ),
                            None => state.fail_task(task_id, message.clone()),
                        },
                    }
                    cx.emit(AppStateChanged);
                });

                if let Some(message) = &report {
                    report_error(UserFacingError::new(ErrorKind::Driver, message.clone()), cx);
                }
                if toast_success {
                    Toast::success(dbflux_i18n::t!(
                        "document.export_wizard.toast.success",
                        summary = summary,
                        folder = output_dir_for_toast.display().to_string()
                    ))
                    .push(cx);
                }
            });

            // Best-effort UI reflection: if the wizard is gone the run has
            // already been finalized above.
            this.update(cx, |this, cx| {
                this.run_state = RunState::Done;
                this.cancel_token = None;
                this.result_summary = Some(summary);
                this.result_rows = rows;
                this.result_notes = notes;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    pub(crate) fn cancel_run(&mut self, cx: &mut Context<Self>) {
        if let Some(token) = &self.cancel_token {
            token.cancel();
        }
        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    // `use super::*` would re-glob the parent module's `use gpui::*`, which
    // blows rustc's macro recursion limit in this crate — import only what
    // the tests need (see `import_wizard::tests` for the same constraint).
    use super::{
        ExportOutcome, ExportedTable, ResultKind, ResultRow, RunTaskAction, build_export_options,
        resolve_run_outcome, result_rows, summarize, table_status_line, to_transfer_columns,
    };
    use dbflux_export::{CsvDelimiter, DateFormat, FormatOptions, TextEncoding};
    use dbflux_transfer::{FileFormat, TableTransferStatus, TransferError};

    #[test]
    fn schema_columns_carry_the_kind_inferred_from_their_type() {
        let info = |name: &str, type_name: &str| dbflux_core::ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        };

        let columns = to_transfer_columns(vec![
            info("id", "integer"),
            info("price", "numeric(10,2)"),
            info("name", "varchar(80)"),
            info("blob", "bytea"),
        ]);

        let kinds: Vec<_> = columns.iter().map(|column| column.kind).collect();
        assert_eq!(
            kinds,
            [
                dbflux_core::ColumnKind::Integer,
                dbflux_core::ColumnKind::Float,
                dbflux_core::ColumnKind::Text,
                dbflux_core::ColumnKind::Unknown,
            ]
        );
    }

    #[test]
    fn the_chosen_format_options_reach_the_engine_options() {
        let chosen = FormatOptions {
            delimiter: CsvDelimiter::Semicolon,
            encoding: TextEncoding::Windows1252,
            null_text: "\\N".to_string(),
            date_format: DateFormat::Epoch,
            sql_batch_size: 50,
        };

        let options = build_export_options("postgres", "app", FileFormat::Csv, 250, chosen.clone());

        assert_eq!(options.format_options, chosen);
        assert_eq!(options.format, FileFormat::Csv);
        assert_eq!(options.segment_size, 250);
        assert_eq!(options.driver_id, "postgres");
        assert_eq!(options.database, "app");
    }

    #[test]
    fn default_choices_leave_the_engine_options_at_their_defaults() {
        let options = build_export_options(
            "sqlite",
            "main",
            FileFormat::Json,
            500,
            FormatOptions::default(),
        );
        assert!(options.format_options.is_default());
    }

    fn exported(schema: Option<&str>, name: &str, status: TableTransferStatus) -> ExportedTable {
        ExportedTable {
            schema: schema.map(str::to_string),
            name: name.to_string(),
            status,
        }
    }

    fn outcome(
        tables: Vec<ExportedTable>,
        warnings: Vec<String>,
        cancelled: bool,
    ) -> ExportOutcome {
        ExportOutcome {
            manifest: None,
            warnings,
            tables,
            cancelled,
        }
    }

    #[test]
    fn resolve_run_outcome_cancelled_run_skips_task_error_and_toast() {
        let result = Ok(outcome(
            vec![
                exported(None, "users", TableTransferStatus::Completed { rows: 4 }),
                exported(None, "orders", TableTransferStatus::Cancelled { rows: 2 }),
                exported(None, "items", TableTransferStatus::NotStarted),
            ],
            Vec::new(),
            true,
        ));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Cancel));
        assert!(resolution.task_action_details.is_none());
        assert!(resolution.report.is_none());
        assert!(!resolution.toast_success);
        assert_eq!(resolution.summary, "Export cancelled");
        assert_eq!(resolution.rows.len(), 3);
        assert!(resolution.rows[1].text.contains("orders"));
        assert!(resolution.rows[1].text.contains('2'));
    }

    #[test]
    fn resolve_run_outcome_all_success_completes_the_task_and_toasts() {
        let result = Ok(outcome(
            vec![
                exported(None, "users", TableTransferStatus::Completed { rows: 10 }),
                exported(
                    Some("public"),
                    "orders",
                    TableTransferStatus::Completed { rows: 5 },
                ),
            ],
            Vec::new(),
            false,
        ));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Complete));
        assert!(resolution.task_action_details.is_none());
        assert!(resolution.report.is_none());
        assert!(resolution.toast_success);
        assert_eq!(
            resolution.summary,
            "Exported 2 table(s), 15 row(s) total (0 skipped)"
        );
        assert!(resolution.notes.is_empty());
    }

    #[test]
    fn resolve_run_outcome_per_table_failure_fails_the_task_with_details_and_reports_once() {
        let result = Ok(outcome(
            vec![
                exported(None, "users", TableTransferStatus::Completed { rows: 3 }),
                exported(
                    None,
                    "orders",
                    TableTransferStatus::Failed {
                        error: "constraint violation".to_string(),
                    },
                ),
                exported(None, "line_items", TableTransferStatus::NotStarted),
            ],
            Vec::new(),
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
            Some("Export failed on table 'orders': constraint violation")
        );
        assert!(!resolution.toast_success);
        assert_eq!(
            resolution.summary,
            "Exported 1 table(s), 3 row(s) total (0 skipped, 1 failed)"
        );

        let details = resolution
            .task_action_details
            .expect("a failed run must carry itemized per-table details");
        assert!(details.contains("users: completed (3 row(s))"));
        assert!(details.contains("orders: FAILED — constraint violation"));
        assert!(details.contains("line_items: not attempted"));
        assert_eq!(resolution.rows.len(), 3);
    }

    #[test]
    fn resolve_run_outcome_pipeline_error_fails_the_task_and_reports_once() {
        let result: Result<ExportOutcome, TransferError> =
            Err(TransferError::Sink("disk full".to_string()));

        let resolution = resolve_run_outcome(result);

        match resolution.task_action {
            RunTaskAction::Fail(ref message) => assert_eq!(message, "sink error: disk full"),
            _ => panic!("a pipeline error must fail the task"),
        }
        assert!(resolution.task_action_details.is_none());
        assert_eq!(
            resolution.report.as_deref(),
            Some("Export failed: sink error: disk full")
        );
        assert!(!resolution.toast_success);
        assert_eq!(resolution.summary, "Export failed: sink error: disk full");
        assert!(resolution.notes.is_empty());
    }

    #[test]
    fn summarize_reports_completed_skipped_and_failed_counts() {
        let outcome = outcome(
            vec![
                exported(None, "a", TableTransferStatus::Completed { rows: 2 }),
                exported(None, "b", TableTransferStatus::Skipped),
                exported(
                    None,
                    "c",
                    TableTransferStatus::Failed {
                        error: "boom".to_string(),
                    },
                ),
            ],
            Vec::new(),
            false,
        );

        assert_eq!(
            summarize(&outcome),
            "Exported 1 table(s), 2 row(s) total (1 skipped, 1 failed)"
        );
    }

    #[test]
    fn result_rows_list_every_table_even_when_the_run_was_clean() {
        let tables = vec![
            exported(None, "a", TableTransferStatus::Completed { rows: 1 }),
            exported(None, "b", TableTransferStatus::Skipped),
        ];

        let rows = result_rows(&tables);

        assert_eq!(
            rows,
            vec![
                ResultRow {
                    kind: ResultKind::Completed,
                    text: "a: completed (1 row(s))".to_string()
                },
                ResultRow {
                    kind: ResultKind::Skipped,
                    text: "b: skipped".to_string()
                },
            ]
        );
    }

    /// A table the run never reached after a failure says why, with the
    /// board's wording; without a failure (a cancel) it keeps the plain line.
    #[test]
    fn result_rows_explain_a_table_the_failure_stopped_the_run_before() {
        let tables = vec![
            exported(
                Some("public"),
                "a",
                TableTransferStatus::Completed { rows: 1 },
            ),
            exported(
                None,
                "b",
                TableTransferStatus::Failed {
                    error: "timeout".to_string(),
                },
            ),
            exported(Some("public"), "c", TableTransferStatus::NotStarted),
        ];

        let rows = result_rows(&tables);

        assert_eq!(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
            vec![
                ResultKind::Completed,
                ResultKind::Failed,
                ResultKind::NotAttempted
            ]
        );
        assert_eq!(rows[0].text, "public.a: completed (1 row(s))");
        assert_eq!(rows[1].text, "b: FAILED — timeout");
        assert_eq!(
            rows[2].text,
            "public.c: not attempted: the export stopped at the failed table"
        );

        let cancelled = result_rows(&[exported(None, "c", TableTransferStatus::NotStarted)]);
        assert_eq!(cancelled[0].text, "c: not attempted");
    }

    #[test]
    fn table_status_line_covers_every_status_and_qualifies_the_name_with_its_schema() {
        assert_eq!(
            table_status_line(&exported(
                Some("public"),
                "users",
                TableTransferStatus::Completed { rows: 7 }
            )),
            "public.users: completed (7 row(s))"
        );
        assert_eq!(
            table_status_line(&exported(None, "orders", TableTransferStatus::Skipped)),
            "orders: skipped"
        );
        assert_eq!(
            table_status_line(&exported(
                None,
                "line_items",
                TableTransferStatus::Failed {
                    error: "fk violation".to_string()
                }
            )),
            "line_items: FAILED — fk violation"
        );
        assert_eq!(
            table_status_line(&exported(None, "audit", TableTransferStatus::NotStarted)),
            "audit: not attempted"
        );
    }
}
