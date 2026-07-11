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

use dbflux_components::controls::{Button, Checkbox};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{
    CancelToken, Connection, OrderResult, TableRef, TaskId, TaskKind, TaskStatus, TaskTarget,
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

use crate::migrate_wizard::MigrateWizard;
use crate::migrate_wizard::column_mapping::TableMigrationConfig;
use crate::migrate_wizard::phases::{ReorderState, RunState};
use crate::migrate_wizard::{build_migration_options, build_migration_table_plans};

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

/// The human-readable mode label shown in the Confirm summary — the same
/// wording used by the mapping grid's mode picker, kept as a small pure map so
/// the summary never has to reach back into the dropdown's option list.
pub fn mapping_mode_label(mode: TableMappingMode) -> &'static str {
    match mode {
        TableMappingMode::Create => "Create",
        TableMappingMode::Existing => "Existing",
        TableMappingMode::Recreate => "Recreate",
        TableMappingMode::Skip => "Skip",
        TableMappingMode::Truncate => "Truncate",
    }
}

/// One row of the Confirm plan summary: what will happen to a single table.
pub struct PlanSummaryRow {
    pub source: String,
    pub target: String,
    pub mode_label: &'static str,
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
    target_profile_id: Uuid,
    segment_size: u32,
    disable_referential_integrity: bool,

    configs: Vec<TableMigrationConfig>,
    summary: PlanSummary,
    pre_run_warnings: Vec<String>,

    reorder: Option<ReorderState>,
    final_order: Option<Vec<TableRef>>,
    confirmed_destructive: bool,
    /// Explicit user acknowledgment required before a destructive plan can be
    /// started; unused (and irrelevant) for non-destructive plans.
    destructive_ack: bool,

    run_state: RunState,
    progress: Arc<Mutex<(u64, Option<u64>)>>,
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
            target_profile_id: inputs.target_profile_id,
            segment_size: inputs.segment_size,
            disable_referential_integrity: inputs.disable_referential_integrity,
            configs: inputs.configs,
            summary,
            pre_run_warnings: inputs.pre_run_warnings,
            reorder,
            final_order,
            confirmed_destructive: false,
            destructive_ack: false,
            run_state: RunState::Idle,
            progress: Arc::new(Mutex::new((0, None))),
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
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = (0, None);

        let description = format!("Migrate {} table(s)", plans.len());
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

        cx.spawn(async move |_this, cx| {
            loop {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(150))
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
                    })
                    .unwrap_or(false);

                if !still_running {
                    break;
                }
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
                    Toast::success("Migration completed").push(cx);
                }
            })
            .ok();

            // Best-effort UI reflection: if the phase is gone the run has
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

fn resolve_run_outcome(
    result: Result<MigrationOutcome, dbflux_transfer::TransferError>,
) -> RunResolution {
    match result {
        Ok(MigrationOutcome::Completed(outcome)) if outcome.cancelled => RunResolution {
            task_action: RunTaskAction::Cancel,
            report: None,
            toast_success: false,
            summary: "Migration cancelled".to_string(),
            warnings: Vec::new(),
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
                    report: Some(format!("Migration failed on table '{table}': {error}")),
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
            let message = "Migration failed: FK order became cyclic mid-run".to_string();
            RunResolution {
                task_action: RunTaskAction::Fail("FK order became cyclic mid-run".to_string()),
                report: Some(message.clone()),
                toast_success: false,
                summary: message,
                warnings: Vec::new(),
            }
        }
        Err(e) => {
            let message = format!("Migration failed: {e}");
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
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let body = match self.run_state {
            RunState::Idle => self.render_confirm(cx),
            RunState::Running => self.render_running(cx),
            RunState::Done => self.render_done(cx),
        };

        div()
            .track_focus(&self.focus_handle)
            .key_context("MigrateConfirmRun")
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .size_full()
            .child(body)
    }
}

impl ConfirmRunPhase {
    fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let summary = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::label("Review migration plan"))
            .child(Text::caption(format!(
                "{} → {}",
                self.summary.source_container, self.summary.target_container
            )))
            .child(self.render_summary_rows(cx));

        let action = match self.reorder.is_some() {
            true => self.render_reorder_interrupt(cx),
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
                .child(Text::caption(row.mode_label))
                .when(row.destructive, |el| {
                    el.child(Text::caption("destructive").danger())
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
                        .label(
                            "I understand this plan will drop or empty existing data in the target.",
                        )
                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                            this.destructive_ack = *checked;
                            cx.notify();
                        })),
                )
            })
            .child(
                Button::new("migrate-confirm-start", "Start Migration")
                    .disabled(!start_enabled)
                    .on_click(cx.listener(|this, _event, _window, cx| this.on_start_migration(cx))),
            )
            .into_any_element()
    }

    fn render_reorder_interrupt(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(reorder) = self.reorder.as_ref() else {
            return div().into_any_element();
        };

        let rows = reorder.list.iter().enumerate().map(|(index, table)| {
            let is_last = index + 1 == reorder.list.len();
            div()
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
                        "Up",
                    )
                    .small()
                    .ghost()
                    .disabled(index == 0)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.move_reorder_row(index, -1, cx);
                    })),
                )
                .child(
                    Button::new(
                        SharedString::from(format!("migrate-reorder-down-{index}")),
                        "Down",
                    )
                    .small()
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
                Text::caption(
                    "These tables reference each other in a cycle and cannot be ordered \
                     automatically. Choose the load order before starting:",
                )
                .warning(),
            )
            .children(rows)
            .child(
                Button::new("migrate-reorder-accept", "Use this order")
                    .on_click(cx.listener(|this, _event, _window, cx| this.accept_reorder(cx))),
            )
            .into_any_element()
    }

    fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let (rows_done, estimated_total) = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let label = match estimated_total {
            Some(total) if total > 0 => format!("{rows_done} / {total} rows"),
            _ => format!("{rows_done} rows"),
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::body("Migrating…"))
            .child(Text::caption(label))
            .child(
                Button::new("migrate-run-cancel", "Cancel")
                    .ghost()
                    .on_click(cx.listener(|this, _event, _window, cx| this.cancel_run(cx))),
            )
            .into_any_element()
    }

    fn cancel_run(&mut self, cx: &mut Context<Self>) {
        if let Some(token) = &self.cancel_token {
            token.cancel();
        }
        cx.notify();
    }

    fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .when_some(self.result_summary.clone(), |el, summary| {
                el.child(Text::body(summary))
            })
            .when(!self.result_warnings.is_empty(), |el| {
                el.child(Text::caption(self.result_warnings.join("; ")))
            })
            .child(Button::new("migrate-run-close", "Close").on_click(
                cx.listener(|_this, _event, _window, cx| cx.emit(ConfirmRunEvent::CloseRequested)),
            ))
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderDecision, PlanSummary, build_plan_summary, decide_order, mapping_mode_label};
    use crate::migrate_wizard::column_mapping::TableMigrationConfig;
    use dbflux_core::{OrderResult, TableRef, TransferColumn};
    use dbflux_transfer::TableMappingMode;

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
        assert_eq!(mapping_mode_label(TableMappingMode::Create), "Create");
        assert_eq!(mapping_mode_label(TableMappingMode::Existing), "Existing");
        assert_eq!(mapping_mode_label(TableMappingMode::Recreate), "Recreate");
        assert_eq!(mapping_mode_label(TableMappingMode::Skip), "Skip");
        assert_eq!(mapping_mode_label(TableMappingMode::Truncate), "Truncate");
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
        assert_eq!(summary.rows[0].mode_label, "Existing");
        assert!(!summary.rows[0].destructive);

        assert_eq!(summary.rows[1].mode_label, "Recreate");
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
}
