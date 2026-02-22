use super::*;

impl SqlQueryDocument {
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

    pub fn run_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.run_query_impl(false, window, cx);
    }

    pub fn run_selected_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(query) = self.selected_query(window, cx) else {
            cx.toast_warning("Select query text to run", window);
            return;
        };

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

            if !is_suppressed {
                self.pending_dangerous_query = Some(PendingDangerousQuery {
                    query,
                    kind,
                    in_new_tab,
                });
                cx.notify();
                return;
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

        let connection = self
            .app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| c.connection.clone());

        let Some(connection) = connection else {
            cx.toast_error("Connection not found", window);
            return;
        };

        self.run_in_new_tab = in_new_tab;

        let description = dbflux_core::truncate_string_safe(query.trim(), 80);
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
        };
        self.execution_history.push(record);
        self.active_execution_index = Some(self.execution_history.len() - 1);

        self.state = DocumentState::Executing;
        cx.emit(DocumentEvent::ExecutionStarted);
        cx.notify();

        let active_database = self
            .app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .and_then(|c| c.active_database.clone());

        let request = QueryRequest::new(query.clone()).with_database(active_database);

        let task = cx.background_executor().spawn({
            let connection = connection.clone();
            async move { connection.execute(&request) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            if cancel_token.is_cancelled() {
                log::info!("Query was cancelled, discarding result");
                return;
            }

            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    doc.pending_result = Some(PendingQueryResult {
                        task_id,
                        exec_id,
                        query,
                        result,
                    });
                    cx.notify();
                })
                .ok();
            })
            .ok();
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

        self.execute_query_internal(pending.query, pending.in_new_tab, window, cx);
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

        self.state = DocumentState::Clean;

        let Some(record) = self
            .execution_history
            .iter_mut()
            .find(|r| r.id == pending.exec_id)
        else {
            return;
        };

        record.finished_at = Some(Instant::now());

        match pending.result {
            Ok(qr) => {
                self.runner.complete_primary(pending.task_id, cx);

                let row_count = qr.rows.len();
                let execution_time = qr.execution_time;
                record.rows_affected = Some(row_count as u64);
                let arc_result = Arc::new(qr);
                record.result = Some(arc_result.clone());

                let (database, connection_name) = self
                    .connection_id
                    .and_then(|id| self.app_state.read(cx).connections().get(&id))
                    .map(|c| (c.active_database.clone(), Some(c.profile.name.clone())))
                    .unwrap_or((None, None));

                let history_entry = HistoryEntry::new(
                    pending.query.clone(),
                    database,
                    connection_name,
                    execution_time,
                    Some(row_count),
                );
                self.app_state.update(cx, |state, _| {
                    state.add_history_entry(history_entry);
                });

                self.setup_data_grid(arc_result, pending.query, window, cx);

                if self.layout == SqlQueryLayout::EditorOnly {
                    self.layout = SqlQueryLayout::Split;
                }

                self.focus_mode = SqlQueryFocus::Results;
            }
            Err(e) => {
                self.runner.fail_primary(pending.task_id, e.to_string(), cx);

                let error_msg = e.to_string();
                record.error = Some(error_msg.clone());
                self.state = DocumentState::Error;
                cx.toast_error(format!("Query failed: {}", error_msg), window);
            }
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
                    profile_id,
                    schema_name,
                    table_name,
                    column_names,
                    row_values,
                    pk_indices,
                    generation_type,
                } => {
                    cx.emit(DocumentEvent::RequestSqlPreview {
                        profile_id: *profile_id,
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        column_names: column_names.clone(),
                        row_values: row_values.clone(),
                        pk_indices: pk_indices.clone(),
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
            if let Some(conn_id) = self.connection_id
                && let Some(connected) = self.app_state.read(cx).connections().get(&conn_id)
            {
                let conn = connected.connection.clone();
                let cancel_handle = conn.cancel_handle();
                if let Err(e) = cancel_handle.cancel() {
                    log::warn!("Failed to send cancel via handle: {}", e);
                }
                if let Err(e) = conn.cancel_active() {
                    log::warn!("Failed to send cancel to database: {}", e);
                }
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
}
