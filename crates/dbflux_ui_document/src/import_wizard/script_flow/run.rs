//! Starting, interrupting and finishing a script run. The engine blocks in
//! `Connection::execute`, so it runs on the background executor; the wizard
//! follows it through the shared progress counter, like every other run.

use std::sync::Arc;

use dbflux_core::{
    CancelToken, Connection, TaskId, TaskKind, TaskTarget, observability::EventRecord,
};
use dbflux_transfer::sql_script::{ScriptProgress, run_script};
use dbflux_ui_base::app_state_entity::{AppStateChanged, AppStateEntity};
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::super::{ImportWizard, RunTaskAction, WizardStep};
use super::audit::{AuditTarget, RunFacts, record_events, run_events};
use super::messages::{ScriptResolution, resolve_script_outcome_at};
use crate::progress_forwarding::spawn_task_progress_forwarder;

/// Everything the worker and the finishing step need, captured when the run
/// starts so they do not read the wizard again.
struct RunPlan {
    connection: Arc<dyn Connection>,
    script: Arc<dbflux_transfer::sql_script::SqlScript>,
    options: dbflux_transfer::sql_script::RunOptions,
    transaction_offered: bool,
    snippet: Arc<std::sync::Mutex<String>>,
    audit: AuditTarget,
    facts: RunFacts,
}

impl ImportWizard {
    /// "Run script" on Confirm. The wizard enters `Running` only once the
    /// run's task and cancel token exist, so Cancel Run always has a token to
    /// trip; when the run cannot start it returns to Configure.
    pub(in crate::import_wizard) fn run_script_confirmed(&mut self, cx: &mut Context<Self>) {
        if self.step != WizardStep::Confirm {
            return;
        }
        let Some(plan) = self.script_run_plan(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.import_wizard.error.no_connection"),
                ),
                cx,
            );
            self.return_to_configure();
            cx.notify();
            return;
        };

        let (task_id, cancel_token) = self.begin_script_task(cx);
        self.step = WizardStep::Running;
        self.spawn_script_run(plan, task_id, cancel_token, cx);
        cx.notify();
    }

    fn script_run_plan(&self, cx: &App) -> Option<RunPlan> {
        let connection = self.resolve_connection(cx)?;
        let flow = self.script.as_ref()?;
        let options = flow.options.run_options(&flow.rules);
        let audit = self.audit_target(&connection, cx);

        Some(RunPlan {
            facts: RunFacts {
                file_name: flow.file_name(),
                options,
                statements: flow.summary.statement_count,
                destructive_statements: flow.summary.destructive_count,
            },
            script: Arc::clone(&flow.script),
            transaction_offered: flow.rules.transactions_supported
                && flow.rules.self_managed.is_none(),
            snippet: Arc::clone(&flow.snippet),
            options,
            audit,
            connection,
        })
    }

    fn audit_target(&self, connection: &Arc<dyn Connection>, cx: &App) -> AuditTarget {
        let driver_id = self
            .profile_id
            .and_then(|profile_id| self.app_state.read(cx).connections().get(&profile_id))
            .map(|connected| connected.profile.driver_id())
            .unwrap_or_default();

        AuditTarget {
            connection_id: self
                .profile_id
                .map(|profile_id| profile_id.to_string())
                .unwrap_or_default(),
            database: self.target_database(connection),
            driver_id,
        }
    }

    /// Registers the run in the Tasks panel and resets the progress the
    /// Running step reads.
    fn begin_script_task(&mut self, cx: &mut Context<Self>) -> (TaskId, CancelToken) {
        let profile_id = self.profile_id;
        let database = self.database.clone();

        self.running = true;
        self.result_summary = None;
        self.result_rows.clear();
        self.result_notes.clear();
        if let Some(flow) = self.script.as_mut() {
            flow.done = None;
            if let Ok(mut snippet) = flow.snippet.lock() {
                snippet.clear();
            }
        }
        *self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = (0, None);

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Import,
                dbflux_i18n::t!("document.import_wizard.script.task"),
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

        let ticker_progress = Arc::clone(&self.progress);
        spawn_task_progress_forwarder(
            self.app_state.clone(),
            task_id,
            move || {
                *ticker_progress
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            },
            cx,
        );

        (task_id, cancel_token)
    }

    fn spawn_script_run(
        &mut self,
        plan: RunPlan,
        task_id: TaskId,
        cancel_token: CancelToken,
        cx: &mut Context<Self>,
    ) {
        let app_state = self.app_state.clone();
        let progress = Arc::clone(&self.progress);

        cx.spawn(async move |this, cx| {
            let RunPlan {
                connection,
                script,
                options,
                transaction_offered,
                snippet,
                audit,
                facts,
            } = plan;

            let lines = Arc::clone(&script);
            let result = cx
                .background_executor()
                .spawn(async move {
                    run_script(
                        connection.as_ref(),
                        &script,
                        &options,
                        |step: &ScriptProgress| publish_progress(&progress, &snippet, step),
                        &cancel_token,
                    )
                })
                .await;

            let events = run_events(&audit, &facts, &result, now_millis());
            let implicit_commit_line = result
                .as_ref()
                .ok()
                .and_then(|report| report.first_implicit_commit)
                .and_then(|index| lines.statements().get(index.saturating_sub(1)))
                .map(|statement| statement.line);
            let resolution =
                resolve_script_outcome_at(result, transaction_offered, implicit_commit_line);
            finalize_task(&app_state, task_id, &resolution, events, cx);

            this.update(cx, |this, cx| {
                this.running = false;
                this.cancel_token = None;
                this.step = WizardStep::Done;
                if let Some(flow) = this.script.as_mut() {
                    flow.done = Some(resolution.view);
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Asks the driver to stop the statement in progress, off the UI thread.
    /// The token alone only stops the run between statements. A driver that
    /// cannot interrupt a statement says so, and the statement then finishes
    /// before the run stops.
    pub(in crate::import_wizard) fn interrupt_script_statement(&mut self, cx: &mut Context<Self>) {
        if self.script.is_none() || !self.running {
            return;
        }
        let Some(connection) = self.resolve_connection(cx) else {
            return;
        };

        cx.background_executor()
            .spawn(async move {
                if let Err(error) = connection.cancel_active() {
                    log::warn!("Could not interrupt the running statement: {error}");
                }
            })
            .detach();
    }
}

fn publish_progress(
    progress: &std::sync::Mutex<(u64, Option<u64>)>,
    snippet: &std::sync::Mutex<String>,
    step: &ScriptProgress,
) {
    if let Ok(mut guard) = progress.lock() {
        *guard = (step.index as u64, Some(step.total as u64));
    }
    if let Ok(mut guard) = snippet.lock() {
        guard.clone_from(&step.snippet);
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// Ends the run's task, records its audit events and reports a failure once,
/// through the app, so none of it depends on the wizard still existing.
fn finalize_task(
    app_state: &Entity<AppStateEntity>,
    task_id: TaskId,
    resolution: &ScriptResolution,
    events: Vec<EventRecord>,
    cx: &mut AsyncApp,
) {
    cx.update(|cx| {
        let audit_service = app_state.read(cx).audit_service().clone();
        record_events(&audit_service, events);

        app_state.update(cx, |state, cx| {
            match &resolution.task_action {
                RunTaskAction::Complete => state.complete_task(task_id),
                RunTaskAction::Cancel => {
                    state.tasks_mut().cancel(task_id);
                }
                RunTaskAction::Fail(message) => state.fail_task(task_id, message.clone()),
            }
            cx.emit(AppStateChanged);
        });

        if let Some(message) = &resolution.report {
            report_error(UserFacingError::new(ErrorKind::Driver, message.clone()), cx);
        }
        if resolution.toast_success {
            Toast::success(dbflux_i18n::t!(
                "document.import_wizard.script.toast.completed"
            ))
            .push(cx);
        }
    });
}
