use super::*;
use dbflux_core::observability::actions as audit_actions;

fn evaluate_dangerous_with_effective_settings(
    kind: dbflux_core::DangerousQueryKind,
    is_suppressed: bool,
    effective: &dbflux_core::EffectiveSettings,
    allow_redis_flush: bool,
) -> dbflux_core::DangerousAction {
    use dbflux_core::DangerousQueryKind::*;

    if !allow_redis_flush && matches!(kind, RedisFlushAll | RedisFlushDb) {
        return dbflux_core::DangerousAction::Block(
            "FLUSHALL / FLUSHDB is disabled in settings".to_string(),
        );
    }

    if !effective.confirm_dangerous {
        return dbflux_core::DangerousAction::Allow;
    }

    if !effective.requires_where && matches!(kind, DeleteNoWhere | UpdateNoWhere) {
        return dbflux_core::DangerousAction::Allow;
    }

    if effective.requires_preview {
        return dbflux_core::DangerousAction::Confirm(kind);
    }

    if is_suppressed {
        return dbflux_core::DangerousAction::Allow;
    }

    dbflux_core::DangerousAction::Confirm(kind)
}

fn task_target_for_execution(
    profile_id: Uuid,
    connected: &dbflux_core::ConnectedProfile,
    target_db: Option<&str>,
) -> TaskTarget {
    let database = target_db.and_then(|database| {
        (connected.connection.schema_loading_strategy()
            == SchemaLoadingStrategy::ConnectionPerDatabase
            && connected
                .schema
                .as_ref()
                .and_then(|schema| schema.current_database())
                .is_none_or(|current| current != database))
        .then(|| database.to_string())
    });

    TaskTarget {
        profile_id,
        database,
    }
}

impl CodeDocument {
    /// Returns selected text when a non-empty selection exists.
    fn selected_query(&self, window: &mut Window, cx: &mut Context<Self>) -> Option<String> {
        self.input_state.update(cx, |state, cx| {
            let sel = state.selected_text_range(false, window, cx)?;

            if sel.range.is_empty() {
                return None;
            }

            let mut adjusted = None;
            state
                .text_for_range(sel.range, &mut adjusted, window, cx)
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
        })
    }

    /// Returns the selected text if a selection exists, otherwise the full editor content.
    fn selected_or_full_query(&self, window: &mut Window, cx: &mut Context<Self>) -> String {
        self.selected_query(window, cx)
            .unwrap_or_else(|| self.input_state.read(cx).value().to_string())
    }

    fn clear_live_output(&mut self) {
        self.live_output = None;
        self._live_output_drain = None;
    }

    fn start_live_output(&mut self, receiver: OutputReceiver, cx: &mut Context<Self>) {
        self.live_output = Some(LiveOutputState::new(receiver));
        self._live_output_drain = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(150))
                    .await;

                let should_continue = cx
                    .update(|cx| {
                        let Some(entity) = this.upgrade() else {
                            return false;
                        };

                        entity.update(cx, |doc, cx| {
                            let Some(live_output) = doc.live_output.as_mut() else {
                                return false;
                            };

                            let changed = live_output.drain();

                            if changed {
                                cx.notify();
                            }

                            !live_output.is_finished()
                        })
                    })
                    .unwrap_or(false);

                if !should_continue {
                    break;
                }
            }
        }));
    }

    pub fn run_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.query_language.supports_connection_context() {
            self.run_script(window, cx);
            return;
        }
        self.run_query_impl(false, window, cx);
    }

    pub fn run_selected_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(query) = self.selected_query(window, cx) else {
            cx.toast_warning("Select query text to run", window);
            return;
        };

        if !self.query_language.supports_connection_context() {
            self.run_script(window, cx);
            return;
        }

        self.run_query_text(query, false, window, cx);
    }

    fn run_query_impl(&mut self, in_new_tab: bool, window: &mut Window, cx: &mut Context<Self>) {
        let query = self.selected_or_full_query(window, cx);
        self.run_query_text(query, in_new_tab, window, cx);
    }

    fn run_query_text(
        &mut self,
        query: String,
        in_new_tab: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if query.trim().is_empty() {
            cx.toast_warning("Enter a query to run", window);
            return;
        }

        if let Some(kind) = detect_dangerous_query(&query) {
            let is_suppressed = self
                .app_state
                .read(cx)
                .dangerous_query_suppressions()
                .is_suppressed(kind);

            let (effective, allow_redis_flush) = {
                let state = self.app_state.read(cx);
                let effective = state.effective_settings_for_connection(self.connection_id);
                let allow_redis_flush = effective
                    .driver_values
                    .get("allow_flush")
                    .map(|value| value == "true")
                    .unwrap_or(false);

                (effective, allow_redis_flush)
            };

            match evaluate_dangerous_with_effective_settings(
                kind,
                is_suppressed,
                &effective,
                allow_redis_flush,
            ) {
                DangerousAction::Allow => {}
                DangerousAction::Confirm(kind) => {
                    self.pending_dangerous_query = Some(PendingDangerousQuery {
                        query,
                        kind,
                        in_new_tab,
                    });
                    cx.notify();
                    return;
                }
                DangerousAction::Block(msg) => {
                    cx.toast_error(msg, window);
                    return;
                }
            }
        }

        if let Some(conn_id) = self.connection_id
            && let Some(connected) = self.app_state.read(cx).connections().get(&conn_id)
        {
            let lang = connected.connection.language_service();
            match lang.validate(&query) {
                ValidationResult::Valid => {}
                ValidationResult::SyntaxError(diag) => {
                    let msg = match diag.hint {
                        Some(ref hint) => format!("{}\nHint: {}", diag.message, hint),
                        None => diag.message,
                    };
                    cx.toast_error(msg, window);
                    return;
                }
                ValidationResult::WrongLanguage { message, .. } => {
                    cx.toast_error(message, window);
                    return;
                }
            }
        }

        self.execute_query_internal(query, in_new_tab, window, cx);
    }

    fn execute_query_internal(
        &mut self,
        query: String,
        in_new_tab: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(conn_id) = self.connection_id else {
            cx.toast_error("No active connection", window);
            return;
        };

        let (connection, active_database, task_target) = {
            let connections = self.app_state.read(cx).connections();
            let Some(connected) = connections.get(&conn_id) else {
                cx.toast_error("Connection not found", window);
                return;
            };

            let active_database = self
                .exec_ctx
                .database
                .clone()
                .or_else(|| connected.active_database.clone());

            match connected.resolve_connection_for_execution(active_database.as_deref()) {
                Ok(connection) => (
                    connection,
                    active_database.clone(),
                    task_target_for_execution(conn_id, connected, active_database.as_deref()),
                ),
                Err(dbflux_core::ConnectionResolutionError::PendingDatabaseConnection {
                    database,
                }) => {
                    cx.toast_error(
                        format!("Connecting to database '{}', please wait...", database),
                        window,
                    );
                    return;
                }
            }
        };

        self.clear_live_output();
        self.run_in_new_tab = in_new_tab;

        let description = dbflux_core::truncate_string_safe(query.trim(), 80);
        let (task_id, cancel_token) = self.runner.start_primary_for_target(
            dbflux_core::TaskKind::Query,
            description,
            Some(task_target.clone()),
            cx,
        );

        let exec_id = Uuid::new_v4();
        let record = ExecutionRecord {
            id: exec_id,
            started_at: Instant::now(),
            finished_at: None,
            result: None,
            error: None,
            rows_affected: None,
            is_script: false,
        };
        self.execution_history.push(record);
        self.active_execution_index = Some(self.execution_history.len() - 1);
        self.active_query_task = Some(ActiveQueryTask {
            task_id,
            target: task_target.clone(),
        });

        self.state = DocumentState::Executing;
        cx.emit(DocumentEvent::ExecutionStarted);
        cx.notify();

        let request = QueryRequest::new(query.clone()).with_database(active_database);

        // Capture audit_service, task_target, and started_at before spawning so we can emit
        // audit events even if the document is closed before the deferred task runs.
        let audit_service = self.app_state.read(cx).audit_service().clone();
        let task_target_for_audit = task_target.clone();
        let started_at = Instant::now();
        let query_for_cancel = query.clone();

        // Capture honest connection metadata (connection_id string + driver_id) before spawn
        // so we can emit proper fallback events without needing cx in the async block.
        let fallback_conn_id = self.connection_id.map(|id| id.to_string());
        let fallback_driver_id = self
            .connection_id
            .and_then(|id| self.app_state.read(cx).connections().get(&id))
            .map(|c| c.profile.driver_id())
            .unwrap_or_default();

        let task = cx.background_executor().spawn({
            let connection = connection.clone();
            async move { connection.execute(&request) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            if cancel_token.is_cancelled() {
                log::info!("Query was cancelled, discarding result");

                if let Err(error) = connection.cleanup_after_cancel() {
                    log::warn!("Cleanup after cancel failed: {}", error);
                }

                let inner_result = this.update(cx, |doc, cx| {
                    doc.complete_cancelled_query(
                        task_id,
                        exec_id,
                        &task_target_for_audit,
                        Some(query_for_cancel),
                        cx,
                    );
                });
                // Fallback fires if the entity is gone (this.update failed). If this.update
                // succeeded, the entity is alive and process_pending_result will emit via the
                // normal path — no second probe needed. This avoids both double-logging and
                // the overhead of a separate cx.update call.
                if inner_result.is_err() {
                    // Entity is gone; process_pending_result won't run. Emit via fallback so the event
                    // is not silently dropped.
                    let ts_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    let duration_ms = started_at.elapsed().as_millis() as i64;

                    let details_json = serde_json::json!({ "query": query }).to_string();

                    let mut event = EventRecord::new(
                        ts_ms,
                        EventSeverity::Warn,
                        EventCategory::Query,
                        EventOutcome::Cancelled,
                    )
                    .with_typed_action(audit_actions::QUERY_CANCEL)
                    .with_summary("Query cancelled")
                    .with_connection_context(
                        fallback_conn_id.clone().unwrap_or_default(),
                        task_target_for_audit.database.clone().unwrap_or_default(),
                        fallback_driver_id.clone(),
                    )
                    .with_details_json(details_json);
                    event.source_id = EventSourceId::Local;
                    event.actor_type = EventActorType::User;
                    event.duration_ms = Some(duration_ms);
                    if let Err(e) = audit_service.record(event) {
                        log::warn!(
                            "Failed to emit cancelled query audit event via fallback: {}",
                            e
                        );
                    }
                }

                return;
            }

            // Emit fallback when entity is gone AND result delivery fails.
            // When entity is gone, process_pending_result never runs → emit directly.
            //
            // Extract outcome details BEFORE moving result into the closure so we can
            // emit the correct success/failure event when the entity is already gone.
            let (outcome, severity, summary, error_detail, action) = match &result {
                Ok(qr) => {
                    let affected_rows = qr.affected_rows.unwrap_or(qr.rows.len() as u64);
                    let rows_label = if qr.affected_rows.is_some() {
                        "affected"
                    } else {
                        "returned"
                    };
                    let summary = format!(
                        "Query executed successfully: {} rows {}",
                        affected_rows, rows_label
                    );
                    (
                        EventOutcome::Success,
                        EventSeverity::Info,
                        summary,
                        None,
                        audit_actions::QUERY_EXECUTE,
                    )
                }
                Err(e) => {
                    let summary = format!("Query failed: {}", e);
                    (
                        EventOutcome::Failure,
                        EventSeverity::Error,
                        summary,
                        Some(e.to_string()),
                        audit_actions::QUERY_EXECUTE_FAILED,
                    )
                }
            };

            // Compute duration before result is consumed by the closure.
            let duration_ms = started_at.elapsed().as_millis() as i64;

            // Capture query text before it gets moved into PendingQueryResult so we can
            // use it in the fallback event if needed.
            let query_text = query.clone();

            let inner_result = this.update(cx, |doc, cx| {
                doc.pending_result = Some(PendingQueryResult {
                    task_id,
                    exec_id,
                    query,
                    result,
                    is_script: false,
                });
                cx.notify();
            });

            // Fallback fires if the entity is gone (this.update failed). If this.update
            // succeeded, the entity is alive and process_pending_result will emit via the
            // normal path — no second probe needed. This avoids both double-logging and
            // the overhead of a separate cx.update call.
            if inner_result.is_err() {
                // Entity is gone; process_pending_result won't run. Emit via fallback so the event
                // is not silently dropped.
                let ts_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);

                let details_json = serde_json::json!({ "query": query_text }).to_string();

                let mut event = EventRecord::new(ts_ms, severity, EventCategory::Query, outcome)
                    .with_typed_action(action)
                    .with_summary(&summary)
                    .with_connection_context(
                        fallback_conn_id.clone().unwrap_or_default(),
                        task_target_for_audit.database.clone().unwrap_or_default(),
                        fallback_driver_id.clone(),
                    )
                    .with_details_json(details_json);
                event.source_id = EventSourceId::Local;
                event.actor_type = EventActorType::User;
                event.duration_ms = Some(duration_ms);
                if let Some(err) = error_detail {
                    event.error_message = Some(err);
                }
                if let Err(e) = audit_service.record(event) {
                    log::warn!("Failed to emit query audit event via fallback: {}", e);
                }
            }
        })
        .detach();
    }

    pub(super) fn confirm_dangerous_query(
        &mut self,
        suppress: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(pending) = self.pending_dangerous_query.take() else {
            return;
        };

        if suppress {
            self.app_state.update(cx, |state, _| {
                state
                    .dangerous_query_suppressions_mut()
                    .set_suppressed(pending.kind);
            });
        }

        // Emit audit event for dangerous query confirmation
        self.emit_dangerous_query_audit_event(cx, pending.kind);

        self.execute_query_internal(pending.query, pending.in_new_tab, window, cx);
    }

    fn complete_cancelled_query(
        &mut self,
        task_id: dbflux_core::TaskId,
        exec_id: Uuid,
        target: &TaskTarget,
        query: Option<String>,
        cx: &mut Context<Self>,
    ) {
        // Determine if this is a script execution by looking up the record
        let is_script = match self.execution_history.iter().find(|r| r.id == exec_id) {
            Some(r) => r.is_script,
            None => {
                log::warn!(
                    "Execution record not found for exec_id={}, cannot determine is_script, defaulting to false",
                    exec_id
                );
                false
            }
        };

        if let Some(record) = self
            .execution_history
            .iter_mut()
            .find(|record| record.id == exec_id)
        {
            record.finished_at = Some(Instant::now());
        }

        let is_active_task = self
            .active_query_task
            .as_ref()
            .is_some_and(|task| task.task_id == task_id);

        if is_active_task {
            self.runner.clear_primary(task_id);
            self.active_query_task = None;
            self.state = DocumentState::Clean;
        }

        if let Some(database) = target.database.as_deref() {
            self.app_state.update(cx, |state, cx| {
                if state.remove_database_connection(target.profile_id, database) {
                    cx.emit(AppStateChanged);
                }
            });
        }

        if is_active_task {
            cx.emit(DocumentEvent::ExecutionFinished);
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }

        // Emit audit event for cancelled execution with correct category
        let duration_ms = self
            .execution_history
            .iter()
            .find(|r| r.id == exec_id)
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.saturating_duration_since(r.started_at))
            })
            .map(|d| d.as_millis() as i64);

        let summary = if is_script {
            "Script cancelled"
        } else {
            "Query cancelled"
        };

        self.emit_audit_event(
            cx,
            if is_script {
                EventCategory::Script
            } else {
                EventCategory::Query
            },
            audit_actions::QUERY_CANCEL,
            EventOutcome::Cancelled,
            summary.to_string(),
            query.as_deref(),
            duration_ms,
            None,
        );
    }

    pub(super) fn cancel_dangerous_query(&mut self, cx: &mut Context<Self>) {
        self.pending_dangerous_query = None;
        cx.notify();
    }

    /// Process pending query selected from history modal (called from render).
    pub(super) fn process_pending_set_query(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(selected) = self.pending_set_query.take() else {
            return;
        };

        self.input_state
            .update(cx, |state, cx| state.set_value(&selected.sql, window, cx));

        if let Some(name) = selected.name {
            self.title = name;
        }

        self.saved_query_id = selected.saved_query_id;

        self.focus_mode = SqlQueryFocus::Editor;

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    pub(super) fn process_pending_auto_refresh(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.pending_auto_refresh {
            return;
        }

        self.pending_auto_refresh = false;

        if !self.can_auto_refresh(cx) {
            self.refresh_policy = dbflux_core::RefreshPolicy::Manual;
            self._refresh_timer = None;
            self.refresh_dropdown.update(cx, |dd, cx| {
                dd.set_selected_index(Some(dbflux_core::RefreshPolicy::Manual.index()), cx);
            });
            cx.toast_warning("Auto-refresh blocked: query modifies data", window);
            return;
        }

        self.run_query_impl(false, window, cx);
    }

    /// Process pending query result (called from render where we have window access).
    pub(super) fn process_pending_result(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_result.take() else {
            return;
        };

        self.clear_live_output();
        self.state = DocumentState::Clean;

        let Some(record) = self
            .execution_history
            .iter_mut()
            .find(|r| r.id == pending.exec_id)
        else {
            return;
        };

        record.finished_at = Some(Instant::now());

        // Compute duration before we start borrowing self for other operations
        let duration_ms = record
            .finished_at
            .map(|finished| finished.saturating_duration_since(record.started_at))
            .map(|d| d.as_millis() as i64);

        let is_script = pending.is_script;

        match pending.result {
            Ok(qr) => {
                self.runner.complete_primary(pending.task_id, cx);

                // Use affected_rows when available (INSERT/UPDATE/DELETE), otherwise rows.len() (SELECT)
                let affected_rows = qr.affected_rows;
                let row_count = affected_rows.unwrap_or(qr.rows.len() as u64);
                let execution_time = qr.execution_time;
                record.rows_affected = Some(row_count);
                let arc_result = Arc::new(qr);
                record.result = Some(arc_result.clone());

                let (database, connection_name) = self
                    .connection_id
                    .and_then(|id| self.app_state.read(cx).connections().get(&id))
                    .map(|c| {
                        let db = self.exec_ctx.database.clone().or(c.active_database.clone());
                        (db, Some(c.profile.name.clone()))
                    })
                    .unwrap_or((None, None));

                let history_entry = HistoryEntry::new(
                    pending.query.clone(),
                    database,
                    connection_name,
                    execution_time,
                    Some(row_count as usize),
                );
                self.app_state.update(cx, |state, _| {
                    state.add_history_entry(history_entry);
                });

                self.setup_data_grid(arc_result, pending.query.clone(), window, cx);

                if self.layout == SqlQueryLayout::EditorOnly {
                    self.layout = SqlQueryLayout::Split;
                }

                self.focus_mode = SqlQueryFocus::Results;

                // Emit audit event for successful execution (query or script)
                let summary = if is_script {
                    "Script executed successfully".to_string()
                } else {
                    // Distinguish between mutation row counts and SELECT result counts
                    let rows_label = if affected_rows.is_some() {
                        "affected"
                    } else {
                        "returned"
                    };
                    format!(
                        "Query executed successfully: {} rows {}",
                        row_count, rows_label
                    )
                };

                if is_script {
                    self.emit_audit_event(
                        cx,
                        EventCategory::Script,
                        audit_actions::SCRIPT_EXECUTE,
                        EventOutcome::Success,
                        summary,
                        Some(&pending.query),
                        duration_ms,
                        None,
                    );
                } else {
                    self.emit_audit_event(
                        cx,
                        EventCategory::Query,
                        audit_actions::QUERY_EXECUTE,
                        EventOutcome::Success,
                        summary,
                        Some(&pending.query),
                        duration_ms,
                        None,
                    );
                }
            }
            Err(e) => {
                self.runner.fail_primary(pending.task_id, e.to_string(), cx);

                let error_msg = e.to_string();
                record.error = Some(error_msg.clone());
                self.state = DocumentState::Error;
                cx.toast_error(format!("Query failed: {}", error_msg), window);

                // Emit audit event for failed execution
                let summary = if is_script {
                    format!("Script failed: {}", error_msg)
                } else {
                    format!("Query failed: {}", error_msg)
                };

                if is_script {
                    self.emit_audit_event(
                        cx,
                        EventCategory::Script,
                        audit_actions::SCRIPT_EXECUTE_FAILED,
                        EventOutcome::Failure,
                        summary,
                        Some(&pending.query),
                        duration_ms,
                        Some(&error_msg),
                    );
                } else {
                    self.emit_audit_event(
                        cx,
                        EventCategory::Query,
                        audit_actions::QUERY_EXECUTE_FAILED,
                        EventOutcome::Failure,
                        summary,
                        Some(&pending.query),
                        duration_ms,
                        Some(&error_msg),
                    );
                }
            }
        }

        if self
            .active_query_task
            .as_ref()
            .is_some_and(|task| task.task_id == pending.task_id)
        {
            self.active_query_task = None;
        }

        cx.emit(DocumentEvent::ExecutionFinished);
        cx.emit(DocumentEvent::MetaChanged);
    }

    fn setup_data_grid(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let should_create_new_tab = self.run_in_new_tab
            || self.result_tabs.is_empty()
            || self.active_result_index.is_none();

        self.run_in_new_tab = false;

        if should_create_new_tab {
            self.create_result_tab(result, query, window, cx);
        } else if let Some(index) = self.active_result_index
            && let Some(tab) = self.result_tabs.get_mut(index)
        {
            tab.grid
                .update(cx, |g, cx| g.set_query_result(result, query.clone(), cx));
        }
    }

    fn create_result_tab(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.result_tab_counter += 1;
        let tab_id = Uuid::new_v4();
        let title = format!("Result {}", self.result_tab_counter);

        let app_state = self.app_state.clone();
        let grid = cx
            .new(|cx| DataGridPanel::new_for_result(result, query.clone(), app_state, window, cx));

        let subscription = cx.subscribe(
            &grid,
            |this, _grid, event: &DataGridEvent, cx| match event {
                DataGridEvent::RequestHide => {
                    this.hide_results(cx);
                }
                DataGridEvent::RequestToggleMaximize => {
                    this.toggle_maximize_results(cx);
                }
                DataGridEvent::Focused => {
                    this.focus_mode = SqlQueryFocus::Results;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }
                DataGridEvent::RequestSqlPreview {
                    context,
                    generation_type,
                } => {
                    cx.emit(DocumentEvent::RequestSqlPreview {
                        context: context.clone(),
                        generation_type: *generation_type,
                    });
                }
            },
        );

        let tab = ResultTab {
            id: tab_id,
            title,
            grid,
            _subscription: subscription,
        };

        self.result_tabs.push(tab);
        self.active_result_index = Some(self.result_tabs.len() - 1);
    }

    pub fn cancel_query(&mut self, cx: &mut Context<Self>) {
        if self.runner.cancel_primary(cx) {
            if let Some(index) = self.active_execution_index
                && let Some(record) = self.execution_history.get_mut(index)
                && record.finished_at.is_none()
            {
                record.finished_at = Some(Instant::now());
            }

            if let Some(task) = self.active_query_task.as_ref() {
                self.app_state
                    .read(cx)
                    .cancel_query_for_target(&task.target);
            } else if let Some(conn_id) = self.connection_id
                && let Some(connected) = self.app_state.read(cx).connections().get(&conn_id)
            {
                let active_database = self
                    .exec_ctx
                    .database
                    .clone()
                    .or_else(|| connected.active_database.clone());
                let target =
                    task_target_for_execution(conn_id, connected, active_database.as_deref());

                self.app_state.read(cx).cancel_query_for_target(&target);
            }

            self.state = DocumentState::Clean;
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    pub fn hide_results(&mut self, cx: &mut Context<Self>) {
        self.layout = SqlQueryLayout::EditorOnly;
        self.focus_mode = SqlQueryFocus::Editor;
        self.results_maximized = false;
        cx.notify();
    }

    pub fn toggle_maximize_results(&mut self, cx: &mut Context<Self>) {
        if self.results_maximized {
            self.layout = SqlQueryLayout::Split;
            self.results_maximized = false;
        } else {
            self.layout = SqlQueryLayout::ResultsOnly;
            self.results_maximized = true;
        }

        if let Some(grid) = self.active_result_grid() {
            grid.update(cx, |g, cx| g.set_maximized(self.results_maximized, cx));
        }

        cx.notify();
    }

    pub fn run_query_in_new_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.query_language.supports_connection_context() {
            self.run_script(window, cx);
            return;
        }
        self.run_query_impl(true, window, cx);
    }

    pub fn close_result_tab(&mut self, tab_id: Uuid, cx: &mut Context<Self>) {
        let Some(index) = self.result_tabs.iter().position(|t| t.id == tab_id) else {
            return;
        };

        self.result_tabs.remove(index);

        if self.result_tabs.is_empty() {
            self.active_result_index = None;
            self.layout = SqlQueryLayout::EditorOnly;
            self.focus_mode = SqlQueryFocus::Editor;
        } else if let Some(active) = self.active_result_index {
            if active >= self.result_tabs.len() {
                self.active_result_index = Some(self.result_tabs.len() - 1);
            } else if active > index {
                self.active_result_index = Some(active - 1);
            }
        }

        cx.notify();
    }

    pub fn activate_result_tab(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.result_tabs.len() {
            self.active_result_index = Some(index);
            cx.notify();
        }
    }

    pub(super) fn active_result_grid(&self) -> Option<Entity<DataGridPanel>> {
        self.active_result_index
            .and_then(|i| self.result_tabs.get(i))
            .map(|tab| tab.grid.clone())
    }

    fn run_script(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::hook_executor::CompositeExecutor;
        use dbflux_core::{
            CancelToken, ConnectionHook, HookContext, HookExecutionMode, HookExecutor,
            HookFailureMode, HookKind, LuaCapabilities, ScriptLanguage, ScriptSource,
        };

        let content = self.input_state.read(cx).value().to_string();
        if content.trim().is_empty() {
            cx.toast_warning("Enter script content to run", window);
            return;
        }

        let kind = match &self.query_language {
            QueryLanguage::Lua => HookKind::Lua {
                source: ScriptSource::Inline {
                    content: content.clone(),
                },
                capabilities: LuaCapabilities::all_enabled(),
            },
            QueryLanguage::Python => HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::Inline {
                    content: content.clone(),
                },
                interpreter: None,
            },
            QueryLanguage::Bash => HookKind::Script {
                language: ScriptLanguage::Bash,
                source: ScriptSource::Inline {
                    content: content.clone(),
                },
                interpreter: None,
            },
            _ => return,
        };

        let hook = ConnectionHook {
            enabled: true,
            kind,
            cwd: None,
            env: std::collections::HashMap::new(),
            inherit_env: true,
            timeout_ms: Some(30_000),
            execution_mode: HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: HookFailureMode::Warn,
        };

        let context = HookContext {
            profile_id: Uuid::nil(),
            profile_name: "script-runner".to_string(),
            db_kind: "none".to_string(),
            host: None,
            port: None,
            database: None,
            phase: None,
        };

        let description = format!("Run {} script", self.query_language.display_name());
        let (output_sender, output_receiver) = dbflux_core::output_channel();
        let (task_id, cancel_token) =
            self.runner
                .start_primary(dbflux_core::TaskKind::Query, description, cx);

        let exec_id = Uuid::new_v4();
        let record = ExecutionRecord {
            id: exec_id,
            started_at: Instant::now(),
            finished_at: None,
            result: None,
            error: None,
            rows_affected: None,
            is_script: true,
        };
        self.execution_history.push(record);
        self.active_execution_index = Some(self.execution_history.len() - 1);

        self.clear_live_output();
        self.start_live_output(output_receiver, cx);
        self.state = DocumentState::Executing;
        self.run_in_new_tab = false;
        if self.layout == SqlQueryLayout::EditorOnly {
            self.layout = SqlQueryLayout::Split;
        }
        cx.emit(DocumentEvent::ExecutionStarted);
        cx.notify();

        // Capture script start time before spawning so we can compute duration for
        // cancellation fallback even if the document is gone when cancellation is detected.
        let script_started_at = Instant::now();

        let executor = CompositeExecutor::new();
        let bg_cancel = cancel_token.clone();

        let task = cx.background_executor().spawn(async move {
            let started_at = Instant::now();
            let result = executor.execute_hook(
                &hook,
                &context,
                &bg_cancel,
                None,
                Some(&output_sender),
                None,
            );

            match result {
                Ok(hook_result) => {
                    let mut output = String::new();

                    if !hook_result.stdout.is_empty() {
                        output.push_str(&hook_result.stdout);
                    }

                    if !hook_result.stderr.is_empty() {
                        if !output.is_empty() {
                            output.push_str("\n--- stderr ---\n");
                        }
                        output.push_str(&hook_result.stderr);
                    }

                    if hook_result.timed_out {
                        output.push_str("\n[Script timed out]");
                    }

                    let exit_info = match hook_result.exit_code {
                        Some(0) => None,
                        Some(code) => Some(format!("Process exited with code {}", code)),
                        None if hook_result.timed_out => None,
                        None => Some("Process exited without status code".to_string()),
                    };

                    if let Some(info) = exit_info {
                        if !output.is_empty() {
                            output.push('\n');
                        }
                        output.push_str(&info);
                    }

                    if output.is_empty() {
                        output = "(no output)".to_string();
                    }

                    let elapsed = started_at.elapsed();
                    Ok(QueryResult {
                        shape: dbflux_core::QueryResultShape::Text,
                        columns: Vec::new(),
                        rows: Vec::new(),
                        affected_rows: None,
                        execution_time: elapsed,
                        text_body: Some(output),
                        raw_bytes: None,
                        next_page_token: None,
                    })
                }
                Err(error) => Err(DbError::query_failed(error)),
            }
        });

        // Capture audit_service and script_started_at before spawning the deferred task so we can emit
        // audit events even if the document is closed before the task runs.
        let audit_service = self.app_state.read(cx).audit_service().clone();
        let content_for_cancel = content.clone();

        cx.spawn(async move |this, cx| {
            let result = task.await;

            if cancel_token.is_cancelled() {
                // Emit cancellation audit event for script (which has no connection context)
                let dummy_target = TaskTarget {
                    profile_id: Uuid::nil(),
                    database: None,
                };
                let inner_result = this.update(cx, |doc, cx| {
                    doc.complete_cancelled_query(
                        task_id,
                        exec_id,
                        &dummy_target,
                        Some(content_for_cancel),
                        cx,
                    );
                });
                let _outer_result = cx.update(|_| ());
                if inner_result.is_err() {
                    // Entity is gone (outer fails) or inner update failed; emit audit event directly
                    // so it is not silently dropped.
                    let ts_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    let duration_ms = script_started_at.elapsed().as_millis() as i64;
                    let details_json = serde_json::json!({ "query": content }).to_string();
                    let event = EventRecord::new(
                        ts_ms,
                        EventSeverity::Warn,
                        EventCategory::Script,
                        EventOutcome::Cancelled,
                    )
                    .with_typed_action(audit_actions::QUERY_CANCEL)
                    .with_summary("Script cancelled")
                    .with_origin(EventOrigin::script())
                    .with_details_json(details_json)
                    .with_duration_ms(duration_ms);
                    if let Err(e) = audit_service.record(event) {
                        log::warn!(
                            "Failed to emit cancelled script audit event via fallback: {}",
                            e
                        );
                    }
                }
                return;
            }

            // Emit fallback when entity is gone AND result delivery fails.
            // When entity is gone, process_pending_result never runs → emit directly.
            //
            // Extract outcome details and duration BEFORE moving result into the closure so we can
            // emit the correct success/failure event when the entity is already gone.
            let (outcome, severity, summary, error_detail, duration_ms, action) = match &result {
                Ok(qr) => {
                    // Script succeeded (output is in the QueryResult text_body)
                    let summary = "Script executed successfully".to_string();
                    let duration_ms = Some(qr.execution_time.as_millis() as i64);
                    (
                        EventOutcome::Success,
                        EventSeverity::Info,
                        summary,
                        None,
                        duration_ms,
                        audit_actions::SCRIPT_EXECUTE,
                    )
                }
                Err(e) => {
                    let summary = format!("Script failed: {}", e);
                    let duration_ms = Some(script_started_at.elapsed().as_millis() as i64);
                    (
                        EventOutcome::Failure,
                        EventSeverity::Error,
                        summary,
                        Some(e.to_string()),
                        duration_ms,
                        audit_actions::SCRIPT_EXECUTE_FAILED,
                    )
                }
            };

            // Capture script content before it gets moved into PendingQueryResult so we can
            // use it in the fallback event if needed.
            let script_content = content.clone();

            let inner_result = this.update(cx, |doc, cx| {
                doc.pending_result = Some(PendingQueryResult {
                    task_id,
                    exec_id,
                    query: content,
                    result,
                    is_script: true,
                });
                cx.notify();
            });

            // Fallback fires if the entity is gone (inner update failed). We do NOT probe
            // with a second synthetic cx.update call — we use the actual inner update result.
            if inner_result.is_err() {
                // Entity is gone; process_pending_result won't run. Emit via fallback so the event
                // is not silently dropped.
                let ts_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);

                let details_json = serde_json::json!({ "query": script_content }).to_string();

                // Scripts have no connection context; use nil profile_id.
                let mut event = EventRecord::new(ts_ms, severity, EventCategory::Script, outcome)
                    .with_typed_action(action)
                    .with_summary(&summary)
                    .with_origin(EventOrigin::script())
                    .with_details_json(details_json)
                    .with_duration_ms(duration_ms.unwrap_or(0));
                if let Some(err) = error_detail {
                    event.error_message = Some(err);
                }
                if let Err(e) = audit_service.record(event) {
                    log::warn!("Failed to emit script audit event via fallback: {}", e);
                }
            }
        })
        .detach();
    }
}
