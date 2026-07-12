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
use std::time::Duration;

use dbflux_core::{CancelToken, Connection, TaskId, TaskKind, TaskStatus, TaskTarget};
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
use crate::export_wizard::phases::{ExportPhase, RunState};

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

/// The owned inputs handed to the off-thread run task.
pub(crate) struct ExportRunContext {
    pub(crate) connection: Arc<dyn Connection>,
    pub(crate) driver_id: String,
    pub(crate) database: String,
    pub(crate) export_tables: Vec<ExportTable>,
    pub(crate) output_dir: PathBuf,
    pub(crate) format: FileFormat,
    pub(crate) segment_size: u32,
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
    warnings: Vec<String>,
}

fn resolve_run_outcome(result: Result<ExportOutcome, TransferError>) -> RunResolution {
    match result {
        Ok(outcome) if outcome.cancelled => RunResolution {
            task_action: RunTaskAction::Cancel,
            task_action_details: None,
            report: None,
            toast_success: false,
            summary: "Export cancelled".to_string(),
            warnings: Vec::new(),
        },
        Ok(outcome) => {
            let failed_table = outcome.tables.iter().find_map(|t| match &t.status {
                TableTransferStatus::Failed { error } => Some((t.name.clone(), error.clone())),
                _ => None,
            });

            let summary = summarize(&outcome);
            let warnings = itemized_status_lines(&outcome.tables, &outcome.warnings);

            match failed_table {
                Some((table, error)) => RunResolution {
                    task_action: RunTaskAction::Fail(format!("{table}: {error}")),
                    task_action_details: Some(itemized_table_details(&outcome.tables)),
                    report: Some(format!("Export failed on table '{table}': {error}")),
                    toast_success: false,
                    summary,
                    warnings,
                },
                None => RunResolution {
                    task_action: RunTaskAction::Complete,
                    task_action_details: None,
                    report: None,
                    toast_success: true,
                    summary,
                    warnings,
                },
            }
        }
        Err(e) => {
            let message = format!("Export failed: {e}");
            RunResolution {
                task_action: RunTaskAction::Fail(e.to_string()),
                task_action_details: None,
                report: Some(message.clone()),
                toast_success: false,
                summary: message,
                warnings: Vec::new(),
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

    if failed > 0 {
        format!(
            "Exported {completed} table(s), {rows} row(s) total ({skipped} skipped, {failed} failed)"
        )
    } else {
        format!("Exported {completed} table(s), {rows} row(s) total ({skipped} skipped)")
    }
}

/// Renders one status line per planned table when the run left any table
/// `Failed` or `NotStarted`, so the user sees exactly which tables succeeded,
/// which one failed with what error, and which were never attempted — not
/// just the last error swallowed into a single toast (R4-002/B-007). On a
/// fully successful/skipped run, only the engine's own warnings are shown.
fn itemized_status_lines(tables: &[ExportedTable], engine_warnings: &[String]) -> Vec<String> {
    let has_issue = tables.iter().any(|t| {
        matches!(
            t.status,
            TableTransferStatus::Failed { .. } | TableTransferStatus::NotStarted
        )
    });

    if !has_issue {
        return engine_warnings.to_vec();
    }

    let mut lines: Vec<String> = tables.iter().map(table_status_line).collect();
    lines.extend(engine_warnings.iter().cloned());
    lines
}

fn table_status_line(table: &ExportedTable) -> String {
    let label = table_label(&table.schema, &table.name);
    match &table.status {
        TableTransferStatus::Completed { rows } => format!("{label}: completed ({rows} row(s))"),
        TableTransferStatus::Skipped => format!("{label}: skipped"),
        TableTransferStatus::Failed { error } => format!("{label}: FAILED — {error}"),
        TableTransferStatus::NotStarted => format!("{label}: not attempted"),
    }
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
                UserFacingError::new(ErrorKind::Storage, "No active connection for this export"),
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
        let table_specs: Vec<(Option<String>, String)> = self
            .tables
            .iter()
            .map(|t| (t.schema.clone(), t.name.clone()))
            .collect();

        self.phase = ExportPhase::Run;
        self.run_state = RunState::Running;
        self.result_summary = None;
        self.result_warnings.clear();
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = RunProgress::default();

        let output_dir = base_dir.join(format!(
            "dbflux-export-{}",
            dbflux_core::chrono::Utc::now().format("%Y%m%d-%H%M%S%.3f")
        ));
        let description = format!("Export {} table(s) from {profile_label}", table_specs.len());

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
                                    format!("Export failed while reading table schema: {err}"),
                                ),
                                cx,
                            );
                        })
                        .ok();

                        this.update(cx, |this, cx| {
                            this.run_state = RunState::Done;
                            this.cancel_token = None;
                            this.result_summary = Some(format!("Export failed: {err}"));
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
        let ticker_app_state = self.app_state.clone();
        let ticker_progress = Arc::clone(&self.progress);

        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(150))
                    .await;

                let still_running = cx
                    .update(|cx| {
                        ticker_app_state.update(cx, |state, cx| {
                            let Some(snapshot) = state.tasks().get(task_id) else {
                                return false;
                            };
                            if snapshot.status != TaskStatus::Running {
                                return false;
                            }

                            let progress = *ticker_progress
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if let Some(total) = progress.estimated_total
                                && total > 0
                            {
                                let fraction =
                                    (progress.rows_done as f32 / total as f32).clamp(0.0, 1.0);
                                state.tasks_mut().update_progress(task_id, fraction);
                                cx.notify();
                            }

                            true
                        })
                    })
                    .unwrap_or(false);

                if !still_running {
                    break;
                }

                // Re-render the wizard every tick so the running phase's
                // per-table position and row counters advance while the run
                // is in flight (the app-state notify above only refreshes
                // the tasks panel, not this modal).
                this.update(cx, |_this, cx| cx.notify()).ok();
            }
        })
        .detach();
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
                cancel_token,
            } = run;
            let output_dir_for_toast = output_dir.clone();

            let export_result = cx
                .background_executor()
                .spawn(async move {
                    let options = ExportOptions {
                        driver_id: &driver_id,
                        database: &database,
                        format,
                        segment_size,
                    };

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
                warnings,
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
                    Toast::success(format!(
                        "{summary}. Saved to {}",
                        output_dir_for_toast.display()
                    ))
                    .push(cx);
                }
            })
            .ok();

            // Best-effort UI reflection: if the wizard is gone the run has
            // already been finalized above.
            this.update(cx, |this, cx| {
                this.run_state = RunState::Done;
                this.cancel_token = None;
                this.result_summary = Some(summary);
                this.result_warnings = warnings;
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
        ExportOutcome, ExportedTable, RunTaskAction, itemized_status_lines, resolve_run_outcome,
        summarize, table_status_line,
    };
    use dbflux_transfer::{TableTransferStatus, TransferError};

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
            vec![exported(None, "users", TableTransferStatus::NotStarted)],
            Vec::new(),
            true,
        ));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Cancel));
        assert!(resolution.task_action_details.is_none());
        assert!(resolution.report.is_none());
        assert!(!resolution.toast_success);
        assert_eq!(resolution.summary, "Export cancelled");
        assert!(resolution.warnings.is_empty());
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
        assert!(resolution.warnings.is_empty());
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
        assert_eq!(resolution.warnings.len(), 3);
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
        assert!(resolution.warnings.is_empty());
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
    fn itemized_status_lines_returns_only_engine_warnings_when_every_table_finished_cleanly() {
        let tables = vec![
            exported(None, "a", TableTransferStatus::Completed { rows: 1 }),
            exported(None, "b", TableTransferStatus::Skipped),
        ];
        let warnings = vec!["heads up".to_string()];

        assert_eq!(itemized_status_lines(&tables, &warnings), warnings);
    }

    #[test]
    fn itemized_status_lines_lists_every_table_when_any_table_failed_or_was_not_started() {
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
            exported(None, "c", TableTransferStatus::NotStarted),
        ];
        let warnings = vec!["engine note".to_string()];

        assert_eq!(
            itemized_status_lines(&tables, &warnings),
            vec![
                "public.a: completed (1 row(s))".to_string(),
                "b: FAILED — timeout".to_string(),
                "c: not attempted".to_string(),
                "engine note".to_string(),
            ]
        );
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
