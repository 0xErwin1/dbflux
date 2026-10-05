//! Confirm and Run phases of the migration wizard, plus the FK-cycle reorder
//! interrupt that can sit in front of the run.
//!
//! The Confirm phase shows the assembled plan — source/target containers and
//! one row per table (source name, target name, mapping mode, destructive
//! flag) — and is where the destructive-confirm gate is finally set. When
//! `topological_order` reported a cycle among the selected tables, an inline
//! reorder panel (not a listed rail phase, see design ADR #1) appears first so
//! the user fixes the load order before the run can start.
//!
//! The Run phase reuses the wizard's existing progress/task wiring verbatim:
//! `start_task_for_target(TaskKind::Migrate, …)`, the shared cancel token, the
//! 150 ms progress ticker, and the same `run_migration` invocation and outcome
//! handling (`summarize` / `itemized_status_lines`) as the pre-redesign wizard,
//! so the produced plan and options stay semantically identical (R9). A failed
//! background run always surfaces to the foreground through `report_error`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dbflux_components::composites::wizard_progress_fraction;
use dbflux_components::controls::{Button, Checkbox};
use dbflux_components::icons::{AppIcon, DriverIconTone};
use dbflux_components::primitives::{
    Badge, BadgeTone, Chamfer, Icon, Spinner, Status, StatusIndicator, Text, environment_label,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, MigrateRunMetrics, ModalMetrics, Spacing,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::{
    CancelToken, Connection, LogErr, OrderResult, TableRef, TaskId, TaskKind, TaskStatus,
    TaskTarget,
};
use dbflux_transfer::TableMappingMode;
use dbflux_transfer::TableTransferStatus;
use dbflux_transfer::migration::{MigrationOutcome, MigrationTablePlan, run_migration};
use dbflux_ui_base::app_state_entity::{AppStateChanged, AppStateEntity};
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

use crate::labels::migrate_wizard_task_label;
use crate::migrate_wizard::MigrateWizard;
use crate::migrate_wizard::column_mapping::TableMigrationConfig;
use crate::migrate_wizard::phases::{ReorderState, RunState};
use crate::migrate_wizard::{build_migration_options, build_migration_table_plans};
use crate::pane::PaneAction;
use dbflux_core::keymap_types::{Command, ContextId};

/// Outcome of resolving the FK load order on the `Options` → `Confirm`
/// transition: either a full order is ready to run, or the selected tables
/// form a cycle and the user must reorder the cyclic subset first.
pub enum OrderDecision {
    Ready(Vec<TableRef>),
    NeedsReorder(ReorderState),
}

/// Maps a raw [`OrderResult`] (computed off-thread by `topological_order`)
/// into the wizard's [`OrderDecision`]: an acyclic graph yields a ready order,
/// a cyclic one seeds the reorder interrupt with the fixed prefix and the
/// cyclic remainder. Pure so the branch is unit-testable without a live run.
pub fn decide_order(result: OrderResult) -> OrderDecision {
    match result {
        OrderResult::Ordered(order) => OrderDecision::Ready(order),
        OrderResult::Cyclic {
            ordered_prefix,
            cycle,
        } => OrderDecision::NeedsReorder(ReorderState::new(ordered_prefix, cycle)),
    }
}

/// The human-readable mode label shown in the Confirm summary — a shorter
/// wording than the mapping grid's mode picker (see
/// [`crate::migrate_wizard::column_mapping::mapping_mode_options`]), resolved
/// through the translation catalog. Exhaustive by construction so a new
/// [`TableMappingMode`] variant fails this crate's build until its
/// `document.migrate_wizard.confirm.mode_label.*` entry is added here.
pub fn mapping_mode_label(mode: TableMappingMode) -> String {
    match mode {
        TableMappingMode::Create => {
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.create")
        }
        TableMappingMode::Existing => {
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.existing")
        }
        TableMappingMode::Recreate => {
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.recreate")
        }
        TableMappingMode::Skip => {
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.skip")
        }
        TableMappingMode::Truncate => {
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.truncate")
        }
    }
}

/// One row of the Confirm plan summary: what will happen to a single table.
pub struct PlanSummaryRow {
    pub source: String,
    pub target: String,
    pub mode_label: String,
    pub destructive: bool,
}

/// The Confirm phase's read-only view of the assembled plan: the two ends of
/// the transfer and one row per table. Built purely from the mapping configs
/// so it can be asserted without rendering.
pub struct PlanSummary {
    pub source_container: String,
    pub target_container: String,
    pub rows: Vec<PlanSummaryRow>,
}

impl PlanSummary {
    pub fn has_destructive(&self) -> bool {
        self.rows.iter().any(|row| row.destructive)
    }
}

/// Assembles the Confirm summary from the mapping configs — the same
/// per-table `is_destructive()` classification the engine's destructive gate
/// uses, so what the user confirms matches what will run.
pub fn build_plan_summary<'a>(
    source_container: String,
    target_container: String,
    configs: impl IntoIterator<Item = &'a TableMigrationConfig>,
) -> PlanSummary {
    let rows = configs
        .into_iter()
        .map(|config| PlanSummaryRow {
            source: config.source_table.qualified_name(),
            target: config.target_table.clone(),
            mode_label: mapping_mode_label(config.mapping_mode),
            destructive: config.is_destructive(),
        })
        .collect();

    PlanSummary {
        source_container,
        target_container,
        rows,
    }
}

/// Everything the Confirm/Run phase needs to render the plan and drive the
/// migration, handed over by the wizard once the earlier phases have resolved
/// the connections, databases, mapping configs, run options, and FK order.
pub struct ConfirmRunInputs {
    pub app_state: Entity<AppStateEntity>,
    pub source_connection: Arc<dyn Connection>,
    pub target_connection: Arc<dyn Connection>,
    pub source_database: String,
    pub target_database: String,
    pub source_profile_id: Uuid,
    pub target_profile_id: Uuid,
    pub source_container_label: String,
    pub target_container_label: String,
    pub segment_size: u32,
    pub disable_referential_integrity: bool,
    pub order: OrderDecision,
    pub configs: Vec<TableMigrationConfig>,
    /// Non-blocking warnings computed on the `Options` → `Confirm` transition
    /// (e.g. a non-destructive same-container source-as-target write), shown on
    /// the Confirm screen so the user sees them before starting the run.
    pub pre_run_warnings: Vec<String>,
}

/// Emitted to the host wizard so it can flip the rail's current phase to `Run`
/// when the migration starts, and close the modal when the user dismisses the
/// finished run.
#[derive(Debug, Clone, Copy)]
pub enum ConfirmRunEvent {
    RunStarted,
    CloseRequested,
}

/// Live counters for the running migration, shared between the run task's
/// progress callback and the render thread. `table_index` is the position in
/// the resolved (post-ordering) load sequence of the table currently being
/// transferred, so it maps into [`ConfirmRunPhase::final_order`] for a
/// per-table live status list.
#[derive(Clone, Copy, Default)]
struct RunProgress {
    table_index: usize,
    rows_done: u64,
    estimated_total: Option<u64>,
}

/// The run's progress plus the rows each finished table moved, so the live
/// table list keeps a finished table's count after the run moves on.
#[derive(Clone, Default)]
struct RunLedger {
    current: RunProgress,
    /// Rows moved by every table before `current.table_index`, by position in
    /// the load order.
    finished_rows: Vec<u64>,
}

impl RunLedger {
    /// Records a progress report from the engine. When the reported table is
    /// past the current one, the current table is finished with its last row
    /// count, and any table the engine skipped over is finished with none.
    fn record(&mut self, table_index: usize, rows_done: u64, estimated_total: Option<u64>) {
        while self.finished_rows.len() < table_index {
            let finished = self.finished_rows.len();

            let rows = if finished == self.current.table_index {
                self.current.rows_done
            } else {
                0
            };

            self.finished_rows.push(rows);
        }

        self.current = RunProgress {
            table_index,
            rows_done,
            estimated_total,
        };
    }

    /// Share of the whole run done, counting each table as an equal part and
    /// the current table by its rows when the engine knows its total.
    fn overall_fraction(&self, total_tables: usize) -> f32 {
        if total_tables == 0 {
            return 0.0;
        }

        let current_fraction =
            wizard_progress_fraction(self.current.rows_done, self.current.estimated_total)
                .unwrap_or(0.0);
        let finished = self.current.table_index.min(total_tables) as f32;

        ((finished + current_fraction) / total_tables as f32).clamp(0.0, 1.0)
    }
}

/// Confirm + Run phase entity: renders the plan summary, the optional reorder
/// interrupt, and the live run (progress + cancel), and owns the migration run
/// itself. Mounted by the wizard once `Options` is complete.
pub struct ConfirmRunPhase {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,

    source_connection: Arc<dyn Connection>,
    target_connection: Arc<dyn Connection>,
    source_database: String,
    target_database: String,
    source_profile_id: Uuid,
    target_profile_id: Uuid,
    segment_size: u32,
    disable_referential_integrity: bool,

    configs: Vec<TableMigrationConfig>,
    summary: PlanSummary,
    pre_run_warnings: Vec<String>,

    reorder: Option<ReorderState>,
    /// Keyboard cursor over the load-order rows of the reorder interrupt.
    reorder_cursor: usize,
    final_order: Option<Vec<TableRef>>,
    confirmed_destructive: bool,
    /// Explicit user acknowledgment required before a destructive plan can be
    /// started; unused (and irrelevant) for non-destructive plans.
    destructive_ack: bool,

    run_state: RunState,
    progress: Arc<Mutex<RunLedger>>,
    /// Wall-clock start of the live run, for the elapsed-time readout; cleared
    /// until a run begins.
    run_started_at: Option<Instant>,
    /// Frozen total run duration, captured when the run reaches `Done`, so the
    /// completed screen keeps showing how long the migration took.
    run_elapsed: Option<Duration>,
    cancel_token: Option<CancelToken>,
    result_summary: Option<String>,
    result_warnings: Vec<String>,
}

impl EventEmitter<ConfirmRunEvent> for ConfirmRunPhase {}

impl ConfirmRunPhase {
    pub fn new(inputs: ConfirmRunInputs, cx: &mut Context<Self>) -> Self {
        let summary = build_plan_summary(
            inputs.source_container_label,
            inputs.target_container_label,
            inputs.configs.iter(),
        );

        let (reorder, final_order) = match inputs.order {
            OrderDecision::Ready(order) => (None, Some(order)),
            OrderDecision::NeedsReorder(state) => (Some(state), None),
        };

        Self {
            app_state: inputs.app_state,
            focus_handle: cx.focus_handle(),
            source_connection: inputs.source_connection,
            target_connection: inputs.target_connection,
            source_database: inputs.source_database,
            target_database: inputs.target_database,
            source_profile_id: inputs.source_profile_id,
            target_profile_id: inputs.target_profile_id,
            segment_size: inputs.segment_size,
            disable_referential_integrity: inputs.disable_referential_integrity,
            configs: inputs.configs,
            summary,
            pre_run_warnings: inputs.pre_run_warnings,
            reorder,
            reorder_cursor: 0,
            final_order,
            confirmed_destructive: false,
            destructive_ack: false,
            run_state: RunState::Idle,
            progress: Arc::new(Mutex::new(RunLedger::default())),
            run_started_at: None,
            run_elapsed: None,
            cancel_token: None,
            result_summary: None,
            result_warnings: Vec::new(),
        }
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    pub fn run_state(&self) -> RunState {
        self.run_state
    }

    /// Whether the run switches referential integrity off on the target.
    pub fn disables_referential_integrity(&self) -> bool {
        self.disable_referential_integrity
    }

    /// Whether the plan may start now: a destructive plan needs the explicit
    /// acknowledgment, and the load order must be settled.
    fn start_enabled(&self) -> bool {
        self.run_state == RunState::Idle
            && self.reorder.is_none()
            && (!self.summary.has_destructive() || self.destructive_ack)
    }

    fn toggle_destructive_ack(&mut self, cx: &mut Context<Self>) {
        if self.summary.has_destructive() {
            self.destructive_ack = !self.destructive_ack;
            cx.notify();
        }
    }

    /// Runs a keymap command on the step. In the load-order interrupt J / K
    /// move the cursor, Shift+J / Shift+K move the row under it and Enter
    /// accepts the order; on the plan, Space checks the destructive
    /// acknowledgment and Ctrl+Enter starts the run; once it is done, Enter
    /// closes the wizard. Returns whether the command applied.
    pub fn handle_command(&mut self, command: Command, cx: &mut Context<Self>) -> bool {
        match self.run_state {
            RunState::Idle if self.reorder.is_some() => self.handle_reorder_command(command, cx),
            RunState::Idle => match command {
                Command::ExpandCollapse => {
                    self.toggle_destructive_ack(cx);
                    true
                }
                Command::RunQuery => {
                    if self.start_enabled() {
                        self.on_start_migration(cx);
                    }
                    true
                }
                _ => false,
            },
            RunState::Running => false,
            RunState::Done => {
                if command == Command::Execute {
                    cx.emit(ConfirmRunEvent::CloseRequested);
                    return true;
                }
                false
            }
        }
    }

    fn handle_reorder_command(&mut self, command: Command, cx: &mut Context<Self>) -> bool {
        let count = self
            .reorder
            .as_ref()
            .map_or(0, |reorder| reorder.list.len());
        let last = count.saturating_sub(1);

        match command {
            Command::SelectNext => {
                self.reorder_cursor = (self.reorder_cursor + 1).min(last);
                cx.notify();
                true
            }
            Command::SelectPrev => {
                self.reorder_cursor = self.reorder_cursor.saturating_sub(1);
                cx.notify();
                true
            }
            Command::MoveSelectedDown if self.reorder_cursor < last => {
                self.move_reorder_row(self.reorder_cursor, 1, cx);
                self.reorder_cursor += 1;
                true
            }
            Command::MoveSelectedUp if self.reorder_cursor > 0 => {
                self.move_reorder_row(self.reorder_cursor, -1, cx);
                self.reorder_cursor -= 1;
                true
            }
            Command::MoveSelectedDown | Command::MoveSelectedUp => true,
            Command::Execute => {
                self.accept_reorder(cx);
                true
            }
            _ => false,
        }
    }

    /// The step's buttons for the wizard's pane actions menu: the load-order
    /// row moves and Accept, the destructive acknowledgment and Start.
    pub fn pane_actions(&self, entity: &Entity<Self>) -> Vec<PaneAction> {
        let context = ContextId::MigrateWizard;
        let mut actions = Vec::new();

        if self.run_state != RunState::Idle {
            return actions;
        }

        if let Some(reorder) = self.reorder.as_ref() {
            let last = reorder.list.len().saturating_sub(1);
            actions.push(
                PaneAction::command(
                    "migrate-reorder-up",
                    dbflux_i18n::t!("document.migrate_wizard.confirm.reorder.up"),
                    Command::MoveSelectedUp,
                    context,
                )
                .enabled(self.reorder_cursor > 0),
            );
            actions.push(
                PaneAction::command(
                    "migrate-reorder-down",
                    dbflux_i18n::t!("document.migrate_wizard.confirm.reorder.down"),
                    Command::MoveSelectedDown,
                    context,
                )
                .enabled(self.reorder_cursor < last),
            );

            let phase = entity.downgrade();
            actions.push(PaneAction::callback(
                "migrate-reorder-accept",
                dbflux_i18n::t!("document.migrate_wizard.confirm.reorder.accept"),
                move |_window, cx| {
                    phase
                        .update(cx, |phase, cx| phase.accept_reorder(cx))
                        .log_err();
                },
            ));
            return actions;
        }

        if self.summary.has_destructive() {
            let phase = entity.downgrade();
            actions.push(PaneAction::callback(
                "migrate-destructive-ack",
                dbflux_i18n::t!("document.migrate_wizard.confirm.destructive_ack"),
                move |_window, cx| {
                    phase
                        .update(cx, |phase, cx| phase.toggle_destructive_ack(cx))
                        .log_err();
                },
            ));
        }

        actions.push(
            PaneAction::command(
                "migrate-start",
                dbflux_i18n::t!("document.migrate_wizard.confirm.start_migration"),
                Command::RunQuery,
                context,
            )
            .icon(AppIcon::Play)
            .enabled(self.start_enabled()),
        );

        actions
    }

    fn move_reorder_row(&mut self, index: usize, delta: isize, cx: &mut Context<Self>) {
        if let Some(reorder) = &mut self.reorder {
            reorder.move_row(index, delta);
            cx.notify();
        }
    }

    /// Accepts the current reorder arrangement: the fixed prefix followed by
    /// the user-ordered cyclic remainder becomes the final load order, and the
    /// interrupt clears so the Confirm summary can offer "Start Migration".
    fn accept_reorder(&mut self, cx: &mut Context<Self>) {
        if let Some(reorder) = self.reorder.take() {
            self.final_order = Some(reorder.resolved_order());
        }
        cx.notify();
    }

    fn on_start_migration(&mut self, cx: &mut Context<Self>) {
        // Never start a second concurrent run from the same phase.
        if self.run_state == RunState::Running {
            return;
        }

        // Only one migration runs at a time, even across wizard tabs.
        if another_migration_running(&self.app_state, cx) {
            Toast::warning(dbflux_i18n::t!(
                "document.migrate_wizard.already_running_in_tasks"
            ))
            .push(cx);
            return;
        }

        // The engine's destructive backstop is satisfied only for a plan that
        // actually contains destructive operations — and only then after the
        // user's explicit acknowledgment (which gates this button). A
        // non-destructive plan leaves the flag `false` and needs no ack.
        self.confirmed_destructive = self.summary.has_destructive();

        cx.emit(ConfirmRunEvent::RunStarted);
        self.start_migration(cx);
    }

    fn start_migration(&mut self, cx: &mut Context<Self>) {
        let source_connection = Arc::clone(&self.source_connection);
        let target_connection = Arc::clone(&self.target_connection);
        let source_database = self.source_database.clone();
        let target_database = self.target_database.clone();
        let manual_order = self.final_order.clone();
        let plans = build_migration_table_plans(self.configs.iter());
        let destructive_confirmed = self.confirmed_destructive;
        let disable_referential_integrity = self.disable_referential_integrity;
        let segment_size = self.segment_size;

        self.run_state = RunState::Running;
        self.result_summary = None;
        self.result_warnings.clear();
        self.run_started_at = Some(Instant::now());
        self.run_elapsed = None;
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = RunLedger::default();

        let description = migrate_wizard_task_label(plans.len());
        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Migrate,
                description,
                Some(TaskTarget {
                    profile_id: self.target_profile_id,
                    database: Some(target_database.clone()),
                }),
            );
            cx.emit(AppStateChanged);
            pair
        });
        self.cancel_token = Some(cancel_token.clone());

        self.spawn_progress_ticker(task_id, cx);
        self.spawn_run(
            RunTaskContext {
                source_connection,
                target_connection,
                source_database,
                target_database,
                plans,
                segment_size,
                destructive_confirmed,
                disable_referential_integrity,
                manual_order,
                cancel_token,
            },
            task_id,
            cx,
        );

        cx.notify();
    }

    fn spawn_progress_ticker(&self, task_id: TaskId, cx: &mut Context<Self>) {
        let ticker_app_state = self.app_state.clone();
        let ticker_progress = Arc::clone(&self.progress);

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

                        let progress = ticker_progress
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .current;
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
                });

                if !still_running {
                    break;
                }

                // Re-render the phase every tick so the elapsed timer, the
                // progress bar, and the per-table live status advance while the
                // run is in flight (the app-state notify above only refreshes
                // the tasks panel, not this modal).
                this.update(cx, |_this, cx| cx.notify()).ok();
            }
        })
        .detach();
    }

    fn spawn_run(&self, run: RunTaskContext, task_id: TaskId, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let progress = Arc::clone(&self.progress);

        cx.spawn(async move |this, cx| {
            let RunTaskContext {
                source_connection,
                target_connection,
                source_database,
                target_database,
                plans,
                segment_size,
                destructive_confirmed,
                disable_referential_integrity,
                manual_order,
                cancel_token,
            } = run;

            let migration_result = cx
                .background_executor()
                .spawn(async move {
                    let options = build_migration_options(
                        segment_size,
                        source_database,
                        target_database,
                        destructive_confirmed,
                        disable_referential_integrity,
                        manual_order,
                    );

                    run_migration(
                        &source_connection,
                        &target_connection,
                        &plans,
                        &options,
                        &cancel_token,
                        move |index, rows_done, estimated_total| {
                            if let Ok(mut guard) = progress.lock() {
                                guard.record(index, rows_done, estimated_total);
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
            } = resolve_run_outcome(migration_result);

            // The run's task MUST reach a terminal state and any failure MUST
            // reach the foreground even if the phase entity was dropped (modal
            // closed/reopened, rail-back attempt) — so finalize the task and
            // report through the app directly, never gated on `this` being
            // alive. `cx.update` only fails if the whole app is gone.
            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    match &task_action {
                        RunTaskAction::Complete => {
                            state.complete_task(task_id);
                        }
                        RunTaskAction::Cancel => {
                            state.tasks_mut().cancel(task_id);
                        }
                        RunTaskAction::Fail(message) => {
                            state.fail_task(task_id, message.clone());
                        }
                    }
                    cx.emit(AppStateChanged);
                });

                if let Some(message) = &report {
                    report_error(UserFacingError::new(ErrorKind::Driver, message.clone()), cx);
                }
                if toast_success {
                    Toast::success(dbflux_i18n::t!("document.migrate_wizard.toast.success"))
                        .push(cx);
                }
            });

            // Best-effort UI reflection: if the phase is gone the run has
            // already been finalized above.
            this.update(cx, |this, cx| {
                this.run_state = RunState::Done;
                this.run_elapsed = this.run_started_at.map(|started| started.elapsed());
                this.cancel_token = None;
                this.result_summary = Some(summary);
                this.result_warnings = warnings;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }
}

/// The terminal action to apply to the run's task registry entry.
enum RunTaskAction {
    Complete,
    Cancel,
    Fail(String),
}

/// The fully-resolved outcome of a migration run: the task action, an optional
/// user-facing error to report, whether to toast success, and the summary /
/// per-table lines for the Done screen. Computed once so the task-finalization
/// path and the UI-reflection path stay consistent even if the phase entity is
/// dropped between them.
struct RunResolution {
    task_action: RunTaskAction,
    report: Option<String>,
    toast_success: bool,
    summary: String,
    warnings: Vec<String>,
}

/// Whether a migration task is running anywhere in the app, whichever wizard
/// tab started it.
fn another_migration_running(app_state: &Entity<AppStateEntity>, cx: &App) -> bool {
    app_state
        .read(cx)
        .running_tasks()
        .iter()
        .any(|task| task.kind == TaskKind::Migrate)
}

fn resolve_run_outcome(
    result: Result<MigrationOutcome, dbflux_transfer::TransferError>,
) -> RunResolution {
    match result {
        Ok(MigrationOutcome::Completed(outcome)) if outcome.cancelled => RunResolution {
            task_action: RunTaskAction::Cancel,
            report: None,
            toast_success: false,
            summary: dbflux_i18n::t!("document.migrate_wizard.status.cancelled"),
            warnings: MigrateWizard::itemized_status_lines(&outcome.tables, &outcome.warnings),
        },
        Ok(MigrationOutcome::Completed(outcome)) => {
            let failed_table = outcome.tables.iter().find_map(|t| match &t.status {
                TableTransferStatus::Failed { error } => {
                    Some((t.source_table.clone(), error.clone()))
                }
                _ => None,
            });

            let summary = MigrateWizard::summarize(&outcome);
            let warnings = MigrateWizard::itemized_status_lines(&outcome.tables, &outcome.warnings);

            match failed_table {
                Some((table, error)) => RunResolution {
                    task_action: RunTaskAction::Fail(format!("{table}: {error}")),
                    report: Some(dbflux_i18n::t!(
                        "document.migrate_wizard.error.table_failed",
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
        Ok(MigrationOutcome::CyclicOrderRequired { .. }) => {
            let message = dbflux_i18n::t!("document.migrate_wizard.error.cyclic_order");
            RunResolution {
                task_action: RunTaskAction::Fail(message.clone()),
                report: Some(message.clone()),
                toast_success: false,
                summary: message,
                warnings: Vec::new(),
            }
        }
        Err(e) => {
            let message = dbflux_i18n::t!("document.migrate_wizard.error.failed", error = e);
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

/// The owned inputs handed to the off-thread run task, bundled so
/// `start_migration` stays under the readable-length limit.
struct RunTaskContext {
    source_connection: Arc<dyn Connection>,
    target_connection: Arc<dyn Connection>,
    source_database: String,
    target_database: String,
    plans: Vec<MigrationTablePlan>,
    segment_size: u32,
    destructive_confirmed: bool,
    disable_referential_integrity: bool,
    manual_order: Option<Vec<TableRef>>,
    cancel_token: CancelToken,
}

impl Render for ConfirmRunPhase {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let keyboard = self.focus_handle.is_focused(window);
        let body = match self.run_state {
            RunState::Idle => self.render_confirm(keyboard, cx),
            RunState::Running => self.render_running(cx),
            RunState::Done => self.render_done(cx),
        };

        div()
            .track_focus(&self.focus_handle)
            .key_context("MigrateConfirmRun")
            .flex()
            .flex_col()
            .gap(MigrateRunMetrics::SECTION_GAP)
            .px(MigrateRunMetrics::CONTENT_PADDING_X)
            .py(MigrateRunMetrics::CONTENT_PADDING_Y)
            .size_full()
            .child(body)
    }
}

impl ConfirmRunPhase {
    fn render_confirm(&self, keyboard: bool, cx: &mut Context<Self>) -> AnyElement {
        let summary = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body(dbflux_i18n::t!(
                "document.migrate_wizard.confirm.review_plan"
            )))
            .child(Text::caption(format!(
                "{} → {}",
                self.summary.source_container, self.summary.target_container
            )))
            .child(self.render_summary_rows(cx));

        let action = match self.reorder.is_some() {
            true => self.render_reorder_interrupt(keyboard, cx),
            false => self.render_start_action(cx),
        };

        let warnings = self.pre_run_warnings.clone();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .size_full()
            .child(summary)
            .when(!warnings.is_empty(), |parent| {
                parent.child(
                    div().flex().flex_col().gap(px(2.0)).children(
                        warnings
                            .into_iter()
                            .map(|warning| Text::caption(warning).warning().into_any_element()),
                    ),
                )
            })
            .child(action)
            .into_any_element()
    }

    fn render_summary_rows(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        let rows = self.summary.rows.iter().map(move |row| {
            let mut container = div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .py(Spacing::XS)
                .px(Spacing::SM)
                .border_b_1()
                .border_color(theme.border);

            if row.destructive {
                container = container.border_1().border_color(theme.danger);
            }

            container
                .child(
                    div()
                        .flex_1()
                        .min_w(px(0.0))
                        .overflow_hidden()
                        .child(Text::body(format!("{} → {}", row.source, row.target))),
                )
                .child(Text::caption(row.mode_label.clone()))
                .when(row.destructive, |el| {
                    el.child(
                        Text::caption(dbflux_i18n::t!(
                            "document.migrate_wizard.confirm.destructive_tag"
                        ))
                        .danger(),
                    )
                })
                .into_any_element()
        });

        div().flex().flex_col().gap(Spacing::XS).children(rows)
    }

    fn render_start_action(&self, cx: &mut Context<Self>) -> AnyElement {
        let has_destructive = self.summary.has_destructive();
        let start_enabled = !has_destructive || self.destructive_ack;

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .when(has_destructive, |parent| {
                parent.child(
                    Checkbox::new("migrate-confirm-destructive-ack")
                        .checked(self.destructive_ack)
                        .label(dbflux_i18n::t!(
                            "document.migrate_wizard.confirm.destructive_ack"
                        ))
                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                            this.destructive_ack = *checked;
                            cx.notify();
                        })),
                )
            })
            .child(
                div().flex().justify_end().child(
                    Button::new(
                        "migrate-confirm-start",
                        dbflux_i18n::t!("document.migrate_wizard.confirm.start_migration"),
                    )
                    .primary()
                    .disabled(!start_enabled)
                    .on_click(cx.listener(|this, _event, _window, cx| this.on_start_migration(cx))),
                ),
            )
            .into_any_element()
    }

    fn render_reorder_interrupt(&self, keyboard: bool, cx: &mut Context<Self>) -> AnyElement {
        let Some(reorder) = self.reorder.as_ref() else {
            return div().into_any_element();
        };
        let cursor_fill = cx.theme().accent;

        let rows = reorder.list.iter().enumerate().map(|(index, table)| {
            let is_last = index + 1 == reorder.list.len();
            div()
                .when(keyboard && index == self.reorder_cursor, |row| {
                    row.bg(cursor_fill)
                })
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .py(Spacing::XS)
                .child(
                    div()
                        .flex_1()
                        .min_w(px(0.0))
                        .child(Text::body(table.qualified_name())),
                )
                .child(
                    Button::new(
                        SharedString::from(format!("migrate-reorder-up-{index}")),
                        dbflux_i18n::t!("document.migrate_wizard.confirm.reorder.up"),
                    )
                    .inline()
                    .ghost()
                    .disabled(index == 0)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.move_reorder_row(index, -1, cx);
                    })),
                )
                .child(
                    Button::new(
                        SharedString::from(format!("migrate-reorder-down-{index}")),
                        dbflux_i18n::t!("document.migrate_wizard.confirm.reorder.down"),
                    )
                    .inline()
                    .ghost()
                    .disabled(is_last)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.move_reorder_row(index, 1, cx);
                    })),
                )
                .into_any_element()
        });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(
                Text::caption(dbflux_i18n::t!(
                    "document.migrate_wizard.confirm.reorder.warning"
                ))
                .warning(),
            )
            .children(rows)
            .child(
                div().flex().justify_end().child(
                    Button::new(
                        "migrate-reorder-accept",
                        dbflux_i18n::t!("document.migrate_wizard.confirm.reorder.accept"),
                    )
                    .primary()
                    .on_click(cx.listener(|this, _event, _window, cx| this.accept_reorder(cx))),
                ),
            )
            .into_any_element()
    }

    /// The tables in their resolved load order, for the live run status list.
    /// Falls back to the summary's source names if the order was never
    /// resolved (which cannot happen once a run has started, but keeps the
    /// renderer total).
    fn ordered_table_names(&self) -> Vec<String> {
        match &self.final_order {
            Some(order) => order.iter().map(|table| table.qualified_name()).collect(),
            None => self
                .summary
                .rows
                .iter()
                .map(|row| row.source.clone())
                .collect(),
        }
    }

    /// One end of the migration (P1Migrate): the driver logo and the mono
    /// `connection / database` label.
    fn render_endpoint(&self, profile_id: Uuid, label: &str, cx: &App) -> Div {
        let theme = cx.theme();
        let state = self.app_state.read(cx);

        let (icon, color) = state
            .profiles()
            .iter()
            .find(|profile| profile.id == profile_id)
            .and_then(|profile| state.drivers().get(&profile.driver_id()))
            .map(|driver| {
                let metadata = driver.metadata();
                (
                    AppIcon::for_driver(metadata.icon, metadata.category),
                    DriverIconTone::for_driver(metadata.icon, metadata.category).resolve(cx),
                )
            })
            .unwrap_or((AppIcon::Database, theme.muted_foreground));

        div()
            .flex()
            .items_center()
            .gap(MigrateRunMetrics::HEADER_GAP)
            .child(
                Icon::new(icon)
                    .size(MigrateRunMetrics::DRIVER_ICON)
                    .color(color),
            )
            .child(
                div()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_color(ChromeColors::strong(theme))
                    .child(label.to_string()),
            )
    }

    fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let ledger = self
            .progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let progress = ledger.current;

        let names = self.ordered_table_names();
        let total_tables = names.len();
        let current_index = progress.table_index.min(total_tables.saturating_sub(1));
        let overall = ledger.overall_fraction(total_tables);

        let target_environment = self
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|profile| profile.id == self.target_profile_id)
            .and_then(|profile| profile.environment());

        let header = div()
            .flex()
            .items_center()
            .gap(MigrateRunMetrics::HEADER_GAP)
            .child(self.render_endpoint(self.source_profile_id, &self.summary.source_container, cx))
            .child(
                Icon::new(AppIcon::ArrowLeftRight)
                    .size(MigrateRunMetrics::ARROW_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(self.render_endpoint(self.target_profile_id, &self.summary.target_container, cx))
            .child(div().flex_1())
            .when_some(target_environment, |header, environment| {
                header.child(Badge::new(
                    environment_label(environment),
                    BadgeTone::for_environment(environment),
                ))
            });

        let caption = [
            crate::labels::migrate_running_position_label(current_index, total_tables),
            crate::labels::migrate_running_rows_label(progress.rows_done, progress.estimated_total),
            format_elapsed(
                self.run_started_at
                    .map(|started| started.elapsed())
                    .unwrap_or_default(),
            ),
        ]
        .join(" \u{00B7} ");

        let summary = div()
            .flex()
            .flex_col()
            .gap(MigrateRunMetrics::SUMMARY_GAP)
            .child(
                div()
                    .flex()
                    .items_baseline()
                    .gap(MigrateRunMetrics::HEADER_GAP)
                    .child(
                        div()
                            .font_family(dbflux_components::fonts::display_family(cx))
                            .font_weight(FontWeight::BLACK)
                            .text_size(MigrateRunMetrics::PERCENT_FONT)
                            .text_color(ChromeColors::strong(theme))
                            .child(percent_label(overall)),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .truncate()
                            .text_color(theme.muted_foreground)
                            .child(caption),
                    ),
            )
            .child(progress_track(
                overall,
                theme.primary,
                MigrateRunMetrics::OVERALL_BAR_HEIGHT,
                None,
                cx,
            ));

        let rows = names.iter().enumerate().map(|(index, name)| {
            let (status, fraction, rows_label) = if index < current_index {
                let rows = ledger.finished_rows.get(index).copied().unwrap_or_default();
                (
                    TableRunStatus::Done,
                    1.0,
                    format!("{} / {}", group_digits(rows), group_digits(rows)),
                )
            } else if index == current_index {
                let fraction =
                    wizard_progress_fraction(progress.rows_done, progress.estimated_total)
                        .unwrap_or(0.0);
                let total = progress
                    .estimated_total
                    .map(group_digits)
                    .unwrap_or_else(|| "\u{2014}".to_string());
                (
                    TableRunStatus::Running,
                    fraction,
                    format!("{} / {}", group_digits(progress.rows_done), total),
                )
            } else {
                (TableRunStatus::Pending, 0.0, "\u{2014}".to_string())
            };

            self.render_table_row(name, status, fraction, rows_label, cx)
        });

        let header_row = table_grid(div())
            .h(MigrateRunMetrics::TABLE_HEADER_HEIGHT)
            .border_b_1()
            .border_color(theme.input)
            .text_size(ModalMetrics::TABLE_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(div().w(MigrateRunMetrics::STATUS_COLUMN))
            .child(div().flex_1().child(dbflux_i18n::t!(
                "document.migrate_wizard.running.table_header"
            )))
            .child(
                div()
                    .w(MigrateRunMetrics::PROGRESS_COLUMN)
                    .child(dbflux_i18n::t!(
                        "document.migrate_wizard.running.progress_header"
                    )),
            )
            .child(
                div()
                    .w(MigrateRunMetrics::ROWS_COLUMN)
                    .child(dbflux_i18n::t!(
                        "document.migrate_wizard.running.rows_header"
                    )),
            );

        let table = div()
            .relative()
            .flex()
            .flex_col()
            .flex_shrink(1.0)
            .min_h_0()
            .overflow_hidden()
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(header_row)
            .child(
                div()
                    .id("migrate-run-steps")
                    .flex()
                    .flex_col()
                    .min_h_0()
                    .overflow_y_scroll()
                    .children(rows),
            );

        div()
            .flex()
            .flex_col()
            .gap(MigrateRunMetrics::SECTION_GAP)
            .size_full()
            .child(header)
            .child(summary)
            .child(table)
            .into_any_element()
    }

    fn render_table_row(
        &self,
        name: &str,
        status: TableRunStatus,
        fraction: f32,
        rows_label: String,
        cx: &App,
    ) -> AnyElement {
        let theme = cx.theme();
        let spinner_frame = self
            .run_started_at
            .map(|started| {
                (started.elapsed().as_millis() / u128::from(Spinner::INTERVAL_MS)) as usize
            })
            .unwrap_or_default();

        let marker = match status {
            TableRunStatus::Done => Icon::new(AppIcon::CircleCheck)
                .size(MigrateRunMetrics::STATUS_ICON)
                .color(theme.success)
                .into_any_element(),
            TableRunStatus::Running => Spinner::new(spinner_frame).into_any_element(),
            TableRunStatus::Pending => StatusIndicator::new(Status::Idle).into_any_element(),
        };

        let bar_color = match status {
            TableRunStatus::Done => theme.success,
            TableRunStatus::Running | TableRunStatus::Pending => theme.primary,
        };

        let name_color = match status {
            TableRunStatus::Running => ChromeColors::strong(theme),
            TableRunStatus::Done | TableRunStatus::Pending => theme.foreground,
        };

        table_grid(div())
            .h(MigrateRunMetrics::TABLE_ROW_HEIGHT)
            .flex_shrink_0()
            .border_b_1()
            .border_color(theme.table_row_border)
            .font_family(dbflux_components::fonts::editor_family(cx))
            .text_size(ModalMetrics::CODE_FONT)
            .child(
                div()
                    .w(MigrateRunMetrics::STATUS_COLUMN)
                    .flex()
                    .items_center()
                    .child(marker),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .text_color(name_color)
                    .child(name.to_string()),
            )
            .child(
                div()
                    .w(MigrateRunMetrics::PROGRESS_COLUMN)
                    .flex()
                    .items_center()
                    .gap(MigrateRunMetrics::BAR_GAP)
                    .child(progress_track(
                        fraction,
                        bar_color,
                        MigrateRunMetrics::TABLE_BAR_HEIGHT,
                        Some(MigrateRunMetrics::TABLE_BAR_WIDTH),
                        cx,
                    ))
                    .child(
                        div()
                            .text_size(MigrateRunMetrics::PERCENT_CAPTION_FONT)
                            .text_color(theme.muted_foreground)
                            .child(percent_label(fraction)),
                    ),
            )
            .child(
                div()
                    .w(MigrateRunMetrics::ROWS_COLUMN)
                    .truncate()
                    .text_color(theme.muted_foreground)
                    .child(rows_label),
            )
            .into_any_element()
    }

    pub fn cancel_run(&mut self, cx: &mut Context<Self>) {
        if let Some(token) = &self.cancel_token {
            token.cancel();
        }
        cx.notify();
    }

    fn render_done(&self, _cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .when_some(self.result_summary.clone(), |el, summary| {
                el.child(Text::body(summary))
            })
            .when_some(self.run_elapsed, |el, elapsed| {
                el.child(
                    Text::caption(dbflux_i18n::t!(
                        "document.migrate_wizard.done.completed_in",
                        elapsed = format_elapsed(elapsed)
                    ))
                    .muted_foreground(),
                )
            })
            .when(!self.result_warnings.is_empty(), |el| {
                el.child(Text::caption(self.result_warnings.join("; ")))
            })
            .into_any_element()
    }
}

/// Where one table of a live run stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableRunStatus {
    Done,
    Running,
    Pending,
}

/// A row of the run table: status, table, progress and rows columns.
fn table_grid(row: Div) -> Div {
    row.flex()
        .items_center()
        .px(MigrateRunMetrics::TABLE_PADDING_X)
}

/// A flat progress track on the raised fill, filled to `fraction` in `fill`.
fn progress_track(
    fraction: f32,
    fill: Hsla,
    height: Pixels,
    width: Option<Pixels>,
    cx: &App,
) -> Div {
    let track = div().h(height).bg(cx.theme().secondary).child(
        div()
            .h_full()
            .w(relative(fraction.clamp(0.0, 1.0)))
            .bg(fill),
    );

    match width {
        Some(width) => track.w(width).flex_shrink_0(),
        None => track.w_full(),
    }
}

/// `fraction` as a whole percentage (`64%`).
fn percent_label(fraction: f32) -> String {
    format!("{}%", (fraction.clamp(0.0, 1.0) * 100.0).round() as u32)
}

/// `value` with thousands separated by commas (`219,277`).
fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }

    grouped
}

/// Formats an elapsed run duration as `M:SS` for the live timer and the
/// completed-run readout.
fn format_elapsed(elapsed: Duration) -> String {
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{minutes}:{seconds:02}")
}

#[cfg(test)]
mod tests {
    use super::{
        OrderDecision, PlanSummary, RunLedger, RunTaskAction, build_plan_summary, decide_order,
        group_digits, mapping_mode_label, percent_label, resolve_run_outcome,
    };
    use crate::migrate_wizard::column_mapping::TableMigrationConfig;
    use dbflux_core::{OrderResult, TableRef, TransferColumn};
    use dbflux_transfer::migration::{MigratedTable, MigrationOutcome, MigrationRunOutcome};
    use dbflux_transfer::{TableMappingMode, TableTransferStatus};

    fn column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    fn config(name: &str, mode: TableMappingMode) -> TableMigrationConfig {
        let mut config = TableMigrationConfig::new(
            TableRef::new(name),
            vec![column("id")],
            true,
            vec![column("id")],
        );
        config.mapping_mode = mode;
        config
    }

    #[test]
    fn ledger_keeps_the_rows_of_each_finished_table() {
        let mut ledger = RunLedger::default();

        ledger.record(0, 500, Some(1_000));
        ledger.record(0, 1_000, Some(1_000));
        ledger.record(1, 10, Some(40));

        assert_eq!(ledger.finished_rows, vec![1_000]);
        assert_eq!(ledger.current.rows_done, 10);

        ledger.record(3, 0, None);

        assert_eq!(ledger.finished_rows, vec![1_000, 10, 0]);
    }

    #[test]
    fn overall_fraction_counts_finished_tables_and_the_current_share() {
        let mut ledger = RunLedger::default();
        ledger.record(2, 50, Some(100));

        assert!((ledger.overall_fraction(5) - 0.5).abs() < f32::EPSILON);
        assert_eq!(ledger.overall_fraction(0), 0.0);
    }

    #[test]
    fn counts_and_percentages_are_formatted_for_the_run_table() {
        assert_eq!(group_digits(219_277), "219,277");
        assert_eq!(group_digits(1_204), "1,204");
        assert_eq!(group_digits(0), "0");
        assert_eq!(percent_label(0.64), "64%");
        assert_eq!(percent_label(1.5), "100%");
    }
    #[test]
    fn decide_order_ready_when_topological_order_is_acyclic() {
        let order = vec![TableRef::new("parent"), TableRef::new("child")];
        let decision = decide_order(OrderResult::Ordered(order.clone()));

        match decision {
            OrderDecision::Ready(resolved) => assert_eq!(resolved, order),
            OrderDecision::NeedsReorder(_) => panic!("acyclic order must be Ready"),
        }
    }

    #[test]
    fn decide_order_needs_reorder_seeds_prefix_and_cyclic_list() {
        let result = OrderResult::Cyclic {
            ordered_prefix: vec![TableRef::new("a")],
            cycle: vec![TableRef::new("b"), TableRef::new("c")],
        };

        match decide_order(result) {
            OrderDecision::NeedsReorder(state) => {
                assert_eq!(state.prefix, vec![TableRef::new("a")]);
                assert_eq!(state.list, vec![TableRef::new("b"), TableRef::new("c")]);
            }
            OrderDecision::Ready(_) => panic!("cyclic order must need reorder"),
        }
    }

    #[test]
    fn accepting_a_reorder_yields_prefix_then_user_order() {
        let result = OrderResult::Cyclic {
            ordered_prefix: vec![TableRef::new("a")],
            cycle: vec![TableRef::new("b"), TableRef::new("c")],
        };

        let OrderDecision::NeedsReorder(mut state) = decide_order(result) else {
            panic!("expected reorder");
        };

        state.move_row(0, 1);
        let resolved: Vec<String> = state.resolved_order().into_iter().map(|t| t.name).collect();

        assert_eq!(resolved, vec!["a", "c", "b"]);
    }

    #[test]
    fn mapping_mode_label_covers_every_mode() {
        assert_eq!(
            mapping_mode_label(TableMappingMode::Create),
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.create")
        );
        assert_eq!(
            mapping_mode_label(TableMappingMode::Existing),
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.existing")
        );
        assert_eq!(
            mapping_mode_label(TableMappingMode::Recreate),
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.recreate")
        );
        assert_eq!(
            mapping_mode_label(TableMappingMode::Skip),
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.skip")
        );
        assert_eq!(
            mapping_mode_label(TableMappingMode::Truncate),
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.truncate")
        );
    }

    /// Every `document.migrate_wizard.confirm.mode_label.*` key resolves to a
    /// non-empty, non-fallback value in both locales, and diverges between
    /// locales — exhaustive over every `TableMappingMode` variant.
    #[test]
    fn mapping_mode_label_resolves_and_differs_in_both_locales() {
        for mode in [
            TableMappingMode::Create,
            TableMappingMode::Existing,
            TableMappingMode::Recreate,
            TableMappingMode::Skip,
            TableMappingMode::Truncate,
        ] {
            let key = match mode {
                TableMappingMode::Create => "document.migrate_wizard.confirm.mode_label.create",
                TableMappingMode::Existing => "document.migrate_wizard.confirm.mode_label.existing",
                TableMappingMode::Recreate => "document.migrate_wizard.confirm.mode_label.recreate",
                TableMappingMode::Skip => "document.migrate_wizard.confirm.mode_label.skip",
                TableMappingMode::Truncate => "document.migrate_wizard.confirm.mode_label.truncate",
            };

            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }

            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert_ne!(en, es, "{key} must differ between en and es");
        }
    }

    #[test]
    fn build_plan_summary_maps_each_config_to_a_row_with_destructive_flag() {
        let configs = [
            config("users", TableMappingMode::Existing),
            config("orders", TableMappingMode::Recreate),
        ];

        let summary: PlanSummary = build_plan_summary(
            "prod / app".to_string(),
            "staging / app".to_string(),
            configs.iter(),
        );

        assert_eq!(summary.source_container, "prod / app");
        assert_eq!(summary.target_container, "staging / app");
        assert_eq!(summary.rows.len(), 2);

        assert_eq!(summary.rows[0].target, "users");
        assert_eq!(
            summary.rows[0].mode_label,
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.existing")
        );
        assert!(!summary.rows[0].destructive);

        assert_eq!(
            summary.rows[1].mode_label,
            dbflux_i18n::t!("document.migrate_wizard.confirm.mode_label.recreate")
        );
        assert!(summary.rows[1].destructive);

        assert!(summary.has_destructive());
    }

    #[test]
    fn build_plan_summary_has_no_destructive_when_all_modes_are_safe() {
        let configs = [
            config("users", TableMappingMode::Existing),
            config("orders", TableMappingMode::Skip),
        ];

        let summary = build_plan_summary("a".to_string(), "b".to_string(), configs.iter());

        assert!(!summary.has_destructive());
    }

    fn migrated(name: &str, status: TableTransferStatus) -> MigratedTable {
        MigratedTable {
            source_table: name.to_string(),
            target_table: name.to_string(),
            status,
        }
    }

    /// A cancel mid-table lists every planned table on the Done screen: the
    /// table the cancel stopped reports the rows it kept, and the tables
    /// after it report that they never started.
    #[test]
    fn resolve_run_outcome_cancelled_run_itemizes_the_cancelled_table() {
        let result = Ok(MigrationOutcome::Completed(MigrationRunOutcome {
            tables: vec![
                migrated("users", TableTransferStatus::Completed { rows: 500 }),
                migrated("orders", TableTransferStatus::Cancelled { rows: 20 }),
                migrated("line_items", TableTransferStatus::NotStarted),
            ],
            warnings: vec!["engine warning".to_string()],
            cancelled: true,
        }));

        let resolution = resolve_run_outcome(result);

        assert!(matches!(resolution.task_action, RunTaskAction::Cancel));
        assert!(resolution.report.is_none());
        assert!(!resolution.toast_success);
        assert_eq!(resolution.summary, "Migration cancelled");
        assert_eq!(
            resolution.warnings,
            vec![
                "users: completed (500 row(s))".to_string(),
                "orders: cancelled after 20 row(s)".to_string(),
                "line_items: not attempted".to_string(),
                "engine warning".to_string(),
            ]
        );
    }
}
