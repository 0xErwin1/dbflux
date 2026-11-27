//! The Plan tab of the code document.
//!
//! Explain asks the connection's driver for the structured plan of the
//! statement under the cursor (or the selection) and shows it in a `Plan`
//! tab beside the result tabs. The query is never run, and the driver call
//! happens on the background executor, so the editor stays responsive while
//! the plan loads; Cancel is the editor's own.
//!
//! The view itself lives in `crate::query_plan`. This file is the glue: what
//! to explain, how to run it, and how the tab takes part in the results
//! header, the keyboard and the inspector rail.

use super::*;
use crate::query_plan::{self, PlanFailure, PlanRunMode, PlanView, PlanViewEvent, PlanViewMode};
use dbflux_core::{DbError, QueryLanguage, QueryPlan, QueryPlanMode, QueryPlanRequest};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, record_error, report_error};

/// The connection a plan is asked of.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum PlanRoute {
    /// The document's current connection and database: the editor Explain.
    Current,
    /// The connection and database a result was produced on, whatever the
    /// document points at now.
    Origin {
        connection_id: Uuid,
        database: Option<String>,
    },
}

impl PlanRoute {
    fn connection_id(&self, current: Option<Uuid>) -> Option<Uuid> {
        match self {
            Self::Current => current,
            Self::Origin { connection_id, .. } => Some(*connection_id),
        }
    }
}

/// The text a plan covers and how the tab names it.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PlanTarget {
    /// Where the plan is asked.
    pub(super) route: PlanRoute,
    pub(super) text: String,
    /// The language `text` is written in, for the guard that decides whether
    /// the statement may be analyzed.
    pub(super) language: QueryLanguage,
    /// "Statement L7–16", "Selection L3–5" or "Whole script · 3 statements".
    pub(super) scope: String,
    /// Statements in `text`, when the language can count them.
    pub(super) statement_count: usize,
}

/// The Plan tab, while the document has one.
pub(super) struct PlanTab {
    pub(super) view: Entity<PlanView>,
    target: PlanTarget,
    /// The plan being produced, if it is still on its way.
    running: Option<dbflux_core::TaskId>,
    /// The connection the plan was asked of, for the messages that name it.
    connection_id: Option<Uuid>,
    _view_subscription: Subscription,
    _task: Option<Task<()>>,
}

/// Plan state of a code document: the tab and whether it is the one shown.
#[derive(Default)]
pub(super) struct PlanSlot {
    pub(super) tab: Option<PlanTab>,
    pub(super) shown: bool,
}

impl PlanSlot {
    pub(super) fn view(&self) -> Option<Entity<PlanView>> {
        self.tab.as_ref().map(|tab| tab.view.clone())
    }
}

impl CodeDocument {
    /// Whether the Plan tab exists and is the tab shown.
    pub(super) fn plan_shown(&self) -> bool {
        self.plan.shown && self.plan.tab.is_some()
    }

    /// Whether the results area has anything to show: a result tab or the
    /// Plan tab.
    pub(super) fn has_result_content(&self) -> bool {
        !self.result_tabs.result_tabs.is_empty() || self.plan.tab.is_some()
    }

    /// The tabs the results header lists: the result tabs, then Plan.
    pub(super) fn result_tab_total(&self) -> usize {
        self.result_tabs.result_tabs.len() + usize::from(self.plan.tab.is_some())
    }

    /// Why Explain is unavailable on this connection, when its driver says it
    /// has no plans. Read from the driver's declared query capabilities, so
    /// no driver is named here.
    pub(super) fn explain_unavailable_reason(&self, cx: &App) -> Option<String> {
        let conn_id = self.connection_id?;
        let app_state = self.app_state.read(cx);
        let connected = app_state.connections().get(&conn_id)?;

        query_plan::declares_no_plans(connected).then(|| {
            dbflux_i18n::t!(
                "document.query_plan.explain.unavailable",
                connection = connected.profile.name
            )
        })
    }

    /// The text Explain covers: the selection, else the statement under the
    /// cursor, else the whole buffer (which the driver refuses when it holds
    /// several statements, and the tab then says so).
    pub(super) fn plan_target(&self, cx: &App) -> Option<PlanTarget> {
        let state = self.editor.input_state.read(cx);
        let buffer = state.value().to_string();
        let language = self.effective_language();

        let selection = state.selected_nonempty_ranges();
        if let (Some(first), Some(last)) = (selection.first(), selection.last())
            && let Some(text) = self.selected_query_text(cx)
        {
            let (start, end) = (
                statements::line_at(&buffer, first.start),
                statements::line_at(&buffer, last.end.saturating_sub(1).max(first.start)),
            );
            return Some(PlanTarget {
                route: PlanRoute::Current,
                statement_count: language.statement_count(&text),
                language: language.clone(),
                text,
                scope: dbflux_i18n::t!(
                    "document.query_plan.scope.selection",
                    start = start,
                    end = end
                ),
            });
        }

        if buffer.trim().is_empty() {
            return None;
        }

        if let Some(range) = statements::cursor_statement(language, &buffer, state.cursor())
            && let Some(statement) = buffer.get(range.clone())
        {
            let start = statements::line_at(&buffer, range.start);
            let end = start + statement.trim_end().matches('\n').count();
            return Some(PlanTarget {
                route: PlanRoute::Current,
                text: statement.to_string(),
                statement_count: 1,
                language: language.clone(),
                scope: dbflux_i18n::t!(
                    "document.code.result.statement_lines",
                    start = start,
                    end = end
                ),
            });
        }

        let statement_count = language.statement_count(&buffer);
        let scope = if statement_count > 1 {
            dbflux_i18n::t!("document.query_plan.scope.script", count = statement_count)
        } else {
            let end = statements::line_at(&buffer, buffer.trim_end().len());
            dbflux_i18n::t!("document.code.result.statement_lines", start = 1, end = end)
        };

        Some(PlanTarget {
            route: PlanRoute::Current,
            text: buffer,
            language: language.clone(),
            scope,
            statement_count,
        })
    }

    /// Explain: shows the plan of the statement under the cursor, or of the
    /// selection, in the Plan tab. Never runs the query.
    pub fn run_explain(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.supports_connection_context() {
            Toast::warning(dbflux_i18n::t!(
                "document.code.execution.toast.explain_unavailable"
            ))
            .meta_right(now_hms())
            .push(cx);
            return;
        }

        let Some(target) = self.plan_target(cx) else {
            Toast::warning(dbflux_i18n::t!(
                "document.code.execution.toast.enter_query_to_explain"
            ))
            .meta_right(now_hms())
            .push(cx);
            return;
        };

        self.start_plan(target, cx);
    }

    /// "Explain this query" from a result tab's cell menu: plans the query
    /// that produced that result, not the buffer, in the same Plan tab
    /// Explain uses. Never runs the query.
    pub(super) fn explain_result_query(
        &mut self,
        tab_id: Uuid,
        query: String,
        cx: &mut Context<Self>,
    ) {
        let Some(tab) = self
            .result_tabs
            .result_tabs
            .iter()
            .find(|tab| tab.id == tab_id)
        else {
            return;
        };

        let language = tab.origin.as_ref().map_or_else(
            || self.effective_language().clone(),
            |origin| origin.language.clone(),
        );
        // A result that carries its origin is explained there, even when the
        // document has moved to another connection or database since.
        let route = tab
            .origin
            .as_ref()
            .map_or(PlanRoute::Current, |origin| PlanRoute::Origin {
                connection_id: origin.connection_id,
                database: origin.context.database.clone(),
            });

        self.start_plan(
            PlanTarget {
                route,
                statement_count: language.statement_count(&query),
                scope: tab.title.clone(),
                text: query,
                language,
            },
            cx,
        );
    }

    /// The connection a plan runs on and the task target it is registered
    /// under. `None` after telling the user why there is none.
    fn resolve_plan_connection(
        &self,
        route: &PlanRoute,
        cx: &mut Context<Self>,
    ) -> Option<(Arc<dyn dbflux_core::Connection>, dbflux_core::TaskTarget)> {
        let Some(conn_id) = route.connection_id(self.connection_id) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.code.execution.toast.no_active_connection"),
                ),
                cx,
            );
            return None;
        };

        let resolved = {
            let connections = self.app_state.read(cx).connections();
            let Some(connected) = connections.get(&conn_id) else {
                let message = dbflux_i18n::t!("document.code.execution.toast.connection_not_found");
                report_error(UserFacingError::new(ErrorKind::User, message), cx);
                return None;
            };

            let database = match route {
                PlanRoute::Current => self.source.exec_ctx.database.clone(),
                PlanRoute::Origin { database, .. } => database.clone(),
            }
            .or_else(|| connected.active_database.clone());

            connected
                .resolve_connection_for_execution(database.as_deref())
                .map(|connection| {
                    (
                        connection,
                        super::execution::task_target_for_execution(
                            conn_id,
                            connected,
                            database.as_deref(),
                        ),
                    )
                })
        };

        match resolved {
            Ok(resolved) => Some(resolved),
            Err(dbflux_core::ConnectionResolutionError::PendingDatabaseConnection { database }) => {
                let message = dbflux_i18n::t!(
                    "document.code.execution.toast.connecting",
                    database = database
                );
                report_error(UserFacingError::new(ErrorKind::User, message), cx);
                None
            }
        }
    }

    /// Why a plan cannot be asked of the database it is meant for: its driver
    /// keeps one session for every database and the session is no longer on
    /// that database, so the plan request (which carries no database) would
    /// be planned against the wrong one. The database is the one a result ran
    /// on, or the one the editor shows.
    fn plan_database_changed(&self, route: &PlanRoute, cx: &App) -> Option<String> {
        let (connection_id, expected, key) = match route {
            PlanRoute::Origin {
                connection_id,
                database: Some(origin),
            } => (
                *connection_id,
                origin,
                "document.query_plan.state.origin_database_changed",
            ),
            PlanRoute::Current => (
                self.connection_id?,
                self.source.exec_ctx.database.as_ref()?,
                "document.query_plan.state.editor_database_changed",
            ),
            PlanRoute::Origin { database: None, .. } => return None,
        };

        let connections = self.app_state.read(cx).connections();
        let connected = connections.get(&connection_id)?;
        if connected.connection.schema_loading_strategy()
            == dbflux_core::SchemaLoadingStrategy::ConnectionPerDatabase
        {
            return None;
        }

        let current = connected.active_database.as_deref()?;
        (current != expected).then(|| dbflux_i18n::t!(key, origin = expected, current = current))
    }

    /// Asks the driver for the plan of `target` and shows the Plan tab,
    /// loading, replacing the one a previous Explain left.
    pub(super) fn start_plan(&mut self, target: PlanTarget, cx: &mut Context<Self>) {
        self.launch_plan(target, QueryPlanMode::Estimate, cx);
    }

    /// Run analyze: asks for the analyzed plan of the statement the tab
    /// covers, on the connection it was planned on, in the same tab. The
    /// estimate stays in it, one click away.
    fn start_analysis(&mut self, cx: &mut Context<Self>) {
        let Some(target) = self.plan.tab.as_ref().map(|tab| tab.target.clone()) else {
            return;
        };

        self.launch_plan(target, QueryPlanMode::Analyze, cx);
    }

    fn launch_plan(&mut self, target: PlanTarget, mode: QueryPlanMode, cx: &mut Context<Self>) {
        if let PlanRoute::Origin { connection_id, .. } = &target.route
            && self
                .app_state
                .read(cx)
                .connections()
                .get(connection_id)
                .is_none()
        {
            self.show_plan_origin_gone(target, cx);
            return;
        }

        if let Some(message) = self.plan_database_changed(&target.route, cx) {
            self.show_plan_refused(target, message, cx);
            return;
        }

        let Some((connection, task_target)) = self.resolve_plan_connection(&target.route, cx)
        else {
            return;
        };
        let connection_id = target.route.connection_id(self.connection_id);

        self.clear_query_error(cx);
        self.clear_live_output();

        let description = dbflux_core::truncate_string_safe(target.text.trim(), 80);
        let (task_id, cancel_token) = self.runner.start_primary_for_target(
            dbflux_core::TaskKind::Query,
            description,
            Some(task_target.clone()),
            cx,
        );
        self.execution.active_query_task = Some(ActiveQueryTask {
            task_id,
            target: task_target,
            uses_isolated_session: false,
        });
        self.state = DocumentState::Executing;

        let view_mode = self
            .plan
            .view()
            .map_or(PlanViewMode::default(), |view| view.read(cx).mode());
        self.hide_plan_inspector(cx);

        let view = match (mode, self.plan.view()) {
            (QueryPlanMode::Analyze, Some(view)) => {
                view.update(cx, |view, cx| view.begin_analysis(cx));
                view
            }
            _ => {
                let offer = self.analyze_offer(&target, cx);
                let view = cx.new(|cx| PlanView::new(target.scope.clone(), view_mode, cx));
                view.update(cx, |view, cx| view.set_analyze_offer(offer, cx));
                view
            }
        };
        let subscription = cx.subscribe(&view, Self::on_plan_event);

        let request = match mode {
            QueryPlanMode::Estimate => QueryPlanRequest::new(target.text.clone()),
            QueryPlanMode::Analyze => QueryPlanRequest::analyzed(target.text.clone()),
        };
        let started = Instant::now();
        let work = cx.background_executor().spawn(async move {
            let result = connection.query_plan(&request);
            (result, started.elapsed())
        });
        let task = cx.spawn(async move |this, cx| {
            let (result, elapsed) = work.await;

            if let Err(error) = this.update(cx, |document, cx| {
                document.finish_plan(
                    task_id,
                    mode,
                    cancel_token.is_cancelled(),
                    result,
                    elapsed,
                    cx,
                );
            }) {
                log::debug!("document closed before its plan arrived: {error}");
            }
        });

        self.plan.tab = Some(PlanTab {
            view,
            target,
            running: Some(task_id),
            connection_id,
            _view_subscription: subscription,
            _task: Some(task),
        });
        self.plan.shown = true;
        if self.layout == SqlQueryLayout::EditorOnly {
            self.layout = SqlQueryLayout::Split;
        }

        cx.emit(DocumentEvent::ExecutionStarted);
        cx.notify();
    }

    /// What the Estimate | Analyze switch offers for `target`: nothing when
    /// the connection's driver cannot analyze, so no driver is named here.
    fn analyze_offer(&self, target: &PlanTarget, cx: &App) -> Option<query_plan::AnalyzeOffer> {
        let connection_id = target.route.connection_id(self.connection_id)?;
        let connected = self.app_state.read(cx).connections().get(&connection_id)?;

        query_plan::offer_for(connected, &target.language, &target.text)
    }

    /// The label after the Plan tab's name: `analyzed` once the query ran.
    pub(super) fn plan_tab_meta(&self, cx: &App) -> Option<SharedString> {
        self.plan
            .view()
            .filter(|view| view.read(cx).is_analyzed())
            .map(|_| dbflux_i18n::t!("document.query_plan.tab.analyzed").into())
    }

    /// The connection a result came from is gone: the Plan tab says so in
    /// its error state. Nothing is asked of any other connection.
    fn show_plan_origin_gone(&mut self, target: PlanTarget, cx: &mut Context<Self>) {
        let message = dbflux_i18n::t!("document.query_plan.state.origin_gone");
        self.show_plan_refused(target, message, cx);
    }

    /// The plan is refused before the driver is asked: the Plan tab shows
    /// `message` in its error state.
    fn show_plan_refused(&mut self, target: PlanTarget, message: String, cx: &mut Context<Self>) {
        self.cancel_running_plan(cx);

        let mode = self
            .plan
            .view()
            .map_or(PlanViewMode::default(), |view| view.read(cx).mode());
        self.hide_plan_inspector(cx);

        let failure =
            self.record_plan_failure(&DbError::connection_failed(message.clone()), message, cx);

        let view = cx.new(|cx| PlanView::new(target.scope.clone(), mode, cx));
        view.update(cx, |view, cx| view.set_failed(failure, cx));
        let subscription = cx.subscribe(&view, Self::on_plan_event);

        self.plan.tab = Some(PlanTab {
            view,
            connection_id: target.route.connection_id(self.connection_id),
            target,
            running: None,
            _view_subscription: subscription,
            _task: None,
        });
        self.plan.shown = true;
        if self.layout == SqlQueryLayout::EditorOnly {
            self.layout = SqlQueryLayout::Split;
        }

        cx.notify();
    }

    /// The driver answered. A plan that is no longer the one on its way (the
    /// user cancelled, or asked again) is dropped.
    fn finish_plan(
        &mut self,
        task_id: dbflux_core::TaskId,
        mode: QueryPlanMode,
        cancelled: bool,
        result: Result<QueryPlan, DbError>,
        elapsed: std::time::Duration,
        cx: &mut Context<Self>,
    ) {
        let Some(tab) = self.plan.tab.as_mut() else {
            return;
        };
        if tab.running != Some(task_id) {
            return;
        }
        tab.running = None;

        let view = tab.view.clone();
        let multiple_statements = tab.target.statement_count > 1;
        let connection_name = tab
            .connection_id
            .and_then(|id| self.app_state.read(cx).connections().get(&id))
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default();

        // A query started since owns the document's state and its completion
        // events; this plan only fills its own tab.
        let owns_document = self
            .execution
            .active_query_task
            .as_ref()
            .is_none_or(|task| task.task_id == task_id);
        if owns_document {
            self.execution.active_query_task = None;
            self.state = DocumentState::Clean;
        }

        if cancelled {
            view.update(cx, |view, cx| view.set_cancelled(cx));
            if owns_document {
                cx.emit(DocumentEvent::ExecutionFinished);
                cx.emit(DocumentEvent::MetaChanged);
            }
            cx.notify();
            return;
        }

        match result {
            Ok(plan) => {
                self.runner.complete_primary(task_id, cx);
                view.update(cx, |view, cx| view.set_ready(&plan, elapsed, cx));
            }
            Err(DbError::NotSupported(_)) if mode == QueryPlanMode::Estimate => {
                self.runner.complete_primary(task_id, cx);
                view.update(cx, |view, cx| view.set_unsupported(connection_name, cx));
            }
            Err(error) => {
                self.runner.fail_primary(task_id, error.to_string(), cx);

                let message = error
                    .formatted()
                    .map(|formatted| formatted.message.clone())
                    .unwrap_or_else(|| error.to_string());

                if multiple_statements && matches!(error, DbError::QueryFailed(_)) {
                    view.update(cx, |view, cx| view.set_multi_statement(message, cx));
                } else {
                    let failure = self.record_plan_failure(&error, message, cx);
                    view.update(cx, |view, cx| view.set_failed(failure, cx));
                }
            }
        }

        if owns_document {
            cx.emit(DocumentEvent::ExecutionFinished);
            cx.emit(DocumentEvent::MetaChanged);
        }
        cx.notify();
    }

    /// Records a failed plan the way every user-facing failure is recorded
    /// (audit row, notification center, error badge) without a toast, since
    /// the tab shows the error in place.
    fn record_plan_failure(
        &self,
        error: &DbError,
        message: String,
        cx: &mut Context<Self>,
    ) -> PlanFailure {
        let user_error = match error.formatted() {
            Some(formatted) => {
                UserFacingError::from_formatted(ErrorKind::Driver, formatted.clone())
            }
            None => UserFacingError::new(ErrorKind::Driver, message.clone()),
        };
        let correlation_id = user_error.correlation_id;
        record_error(user_error, cx);

        PlanFailure {
            summary: message,
            correlation_id,
            clipboard: error.to_string(),
        }
    }

    /// The running plan was cancelled from the editor or the tasks panel:
    /// the tab says so at once instead of waiting for the driver to return.
    pub(super) fn plan_cancelled(&mut self, cx: &mut Context<Self>) {
        let Some(tab) = self.plan.tab.as_mut() else {
            return;
        };
        let Some(task_id) = tab.running.take() else {
            return;
        };

        if self
            .execution
            .active_query_task
            .as_ref()
            .is_some_and(|task| task.task_id == task_id)
        {
            self.execution.active_query_task = None;
        }

        tab._task = None;
        let view = tab.view.clone();
        view.update(cx, |view, cx| view.set_cancelled(cx));
    }

    /// Stops the plan that is still loading, if there is one. The document's
    /// cancellation reaches whatever query is active, so it is only used while
    /// the plan's task is that query; once a newer query has taken over, only
    /// the plan's own loading state is dropped and the query keeps running.
    fn cancel_running_plan(&mut self, cx: &mut Context<Self>) {
        let Some(task_id) = self.plan.tab.as_ref().and_then(|tab| tab.running) else {
            return;
        };

        let owns_active_task = self
            .execution
            .active_query_task
            .as_ref()
            .is_some_and(|task| task.task_id == task_id);

        if owns_active_task {
            self.cancel_query(cx);
        } else {
            self.plan_cancelled(cx);
        }
    }

    fn on_plan_event(
        &mut self,
        _view: Entity<PlanView>,
        event: &PlanViewEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            PlanViewEvent::Focused => {
                self.focus_mode = SqlQueryFocus::Results;
                cx.emit(DocumentEvent::RequestFocus);
                cx.notify();
            }
            PlanViewEvent::OpenInspector { title, content } => {
                cx.emit(DocumentEvent::OpenInspector {
                    title: title.clone(),
                    content: content.clone(),
                    content_has_header: true,
                });
            }
            PlanViewEvent::CloseInspector => cx.emit(DocumentEvent::CloseInspector),
            PlanViewEvent::RetryRequested => {
                let analyzing = self
                    .plan
                    .view()
                    .is_some_and(|view| view.read(cx).run_mode() == PlanRunMode::Analyze);

                if analyzing {
                    self.start_analysis(cx);
                } else if let Some(target) = self.plan.tab.as_ref().map(|tab| tab.target.clone()) {
                    self.start_plan(target, cx);
                }
            }
            PlanViewEvent::AnalyzeRequested => self.start_analysis(cx),
            PlanViewEvent::CancelRequested => self.cancel_running_plan(cx),
        }
    }

    // ── Tab ──────────────────────────────────────────────────────────────────

    /// Shows the Plan tab in place of the result tab shown.
    pub(super) fn activate_plan_tab(&mut self, cx: &mut Context<Self>) {
        if self.plan.tab.is_none() || self.plan.shown {
            return;
        }

        self.plan.shown = true;
        if let Some(view) = self.plan.view() {
            view.update(cx, |view, cx| view.restore_inspector(cx));
        }
        cx.notify();
    }

    /// Leaves the Plan tab for a result tab: the rail it opened goes away.
    pub(super) fn leave_plan_tab(&mut self, cx: &mut Context<Self>) {
        if !self.plan.shown {
            return;
        }

        self.plan.shown = false;
        self.hide_plan_inspector(cx);
    }

    fn hide_plan_inspector(&mut self, cx: &mut Context<Self>) {
        if let Some(view) = self.plan.view() {
            view.update(cx, |view, cx| view.hide_inspector(cx));
        }
    }

    /// Closes the Plan tab, cancelling the plan if it is still loading.
    pub(super) fn close_plan_tab(&mut self, cx: &mut Context<Self>) {
        self.cancel_running_plan(cx);

        let Some(tab) = self.plan.tab.take() else {
            return;
        };
        tab.view.update(cx, |view, cx| view.close_inspector(cx));

        self.plan.shown = false;
        if self.result_tabs.result_tabs.is_empty() {
            self.layout = SqlQueryLayout::EditorOnly;
            self.focus_mode = SqlQueryFocus::Editor;
        }
        cx.notify();
    }

    /// Re-mounts the inspector of the Plan tab when the document tab becomes
    /// active again.
    pub(super) fn restore_plan_inspector(&mut self, cx: &mut Context<Self>) {
        if self.plan_shown()
            && let Some(view) = self.plan.view()
        {
            view.update(cx, |view, cx| view.restore_inspector(cx));
        }
    }

    /// The workspace rail was dismissed from outside the plan.
    pub(super) fn mark_plan_inspector_closed(&mut self, cx: &mut Context<Self>) {
        if let Some(view) = self.plan.view() {
            view.update(cx, |view, _| view.mark_inspector_closed());
        }
    }

    // ── Keyboard ─────────────────────────────────────────────────────────────

    /// Hands a Results-context command to the Plan tab while it is shown.
    pub(super) fn dispatch_plan_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(view) = self.plan.view().filter(|_| self.plan_shown()) else {
            return false;
        };

        view.update(cx, |view, cx| view.dispatch_command(command, window, cx))
    }

    /// Gives the keyboard to the Plan tab.
    pub(super) fn focus_plan(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(view) = self.plan.view() {
            let handle = view.read(cx).focus_handle(cx);
            handle.focus(window, cx);
        }
    }

    /// The pane actions of the Plan tab, while it is shown.
    pub(super) fn plan_pane_actions(&self, cx: &App) -> Vec<crate::pane::PaneAction> {
        match self.plan.view().filter(|_| self.plan_shown()) {
            Some(view) => query_plan::pane_actions(&view, cx),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests;
