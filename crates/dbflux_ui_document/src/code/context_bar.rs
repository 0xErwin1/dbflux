use super::*;
use crate::result_view::ResultViewMode;
use dbflux_components::composites::control_shell;
use dbflux_components::primitives::{Icon, Text, focus_frame};
use dbflux_ui_base::AsyncUpdateResultExt;

fn context_dropdown_min_width(index: usize) -> Pixels {
    match index {
        0 => px(140.0),
        1 => px(120.0),
        _ => px(100.0),
    }
}

fn context_slot_is_keyboard_focused(
    focus_mode: SqlQueryFocus,
    active_slot: ContextBarSlot,
    slot: ContextBarSlot,
) -> bool {
    focus_mode == SqlQueryFocus::ContextBar && active_slot == slot
}

fn parse_source_datetime_input(value: &str) -> Option<i64> {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return None;
    }

    dbflux_core::chrono::DateTime::parse_from_rfc3339(trimmed)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// Resolve which query-mode value the Syntax dropdown should show when the
/// context bar re-syncs, preserving a user's in-progress choice.
///
/// Precedence: a mode committed in the execution context, then the mode
/// currently shown in the dropdown, then the spec default. The preferred value
/// is only kept when the current spec still offers it (`available`); otherwise
/// it falls back to the spec default. This is what prevents an `AppStateChanged`
/// event from resetting a freshly-picked Flux selection back to the spec default
/// (InfluxQL on InfluxDB v2).
fn resolve_query_mode_selection(
    committed: Option<&str>,
    current_dropdown: Option<&str>,
    available: &[String],
    spec_default: Option<&str>,
) -> Option<String> {
    let preferred = committed.or(current_dropdown);

    preferred
        .filter(|mode| available.iter().any(|candidate| candidate == mode))
        .map(|mode| mode.to_string())
        .or_else(|| spec_default.map(|mode| mode.to_string()))
}

impl CodeDocument {
    // === Context dropdown creation ===

    pub(super) fn create_connection_dropdown(
        app_state: &Entity<AppStateEntity>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let items = Self::connection_items(app_state, cx);

        let selected_index = exec_ctx.connection_id.and_then(|id| {
            let id = id.to_string();
            items.iter().position(|item| item.value.as_ref() == id)
        });

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-connection")
                .items(items)
                .selected_index(selected_index)
                .placeholder("No connection")
                .toolbar_style(true)
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                this.on_connection_changed(&event.item, window, cx);
            },
        );

        (dropdown, sub)
    }

    fn connection_items(app_state: &Entity<AppStateEntity>, cx: &App) -> Vec<DropdownItem> {
        let mut items: Vec<_> = app_state
            .read(cx)
            .connections()
            .values()
            .map(|connected| {
                DropdownItem::with_value(&connected.profile.name, connected.profile.id.to_string())
            })
            .collect();

        items.sort_by(|left, right| left.label.as_ref().cmp(right.label.as_ref()));
        items
    }

    fn default_database_for_connection(
        app_state: &Entity<AppStateEntity>,
        connection_id: Uuid,
        cx: &App,
    ) -> Option<String> {
        let connected = app_state.read(cx).connections().get(&connection_id)?;

        connected.active_database.clone().or_else(|| {
            connected
                .schema
                .as_ref()
                .and_then(|schema| schema.current_database().map(String::from))
        })
    }

    /// Re-bind the editor to the effective query language: completion provider
    /// and syntax highlighter. Called whenever the language can change (Syntax
    /// dropdown, connection change), so switching to e.g. Flux drops the SQL
    /// grammar (and, via run_diagnostics, the SQL squiggles) instead of keeping
    /// the document's initial language.
    fn sync_editor_language(&mut self, cx: &mut Context<Self>) {
        let connection_id = self
            .connection_id
            .filter(|id| self.app_state.read(cx).connections().contains_key(id));

        let query_language = self.effective_query_language(cx);
        let editor_mode = query_language.editor_mode();

        let completion_provider: Rc<dyn CompletionProvider> = Rc::new(
            QueryCompletionProvider::new(query_language, self.app_state.clone(), connection_id),
        );

        self.input_state.update(cx, |state, cx| {
            state.lsp.completion_provider = Some(completion_provider);
            state.set_highlighter(editor_mode, cx);
        });
    }

    pub(super) fn current_source_context_spec(
        &self,
        cx: &App,
    ) -> Option<dbflux_core::SourceContextSpec> {
        let connection_id = self.exec_ctx.connection_id.or(self.connection_id)?;

        self.app_state
            .read(cx)
            .connections()
            .get(&connection_id)
            .and_then(|connected| connected.connection.source_context_spec())
    }

    fn current_source_query_mode_value(&self, cx: &App) -> Option<String> {
        let spec = self.current_source_context_spec(cx)?;

        self.source_query_mode_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .or(spec.default_query_mode)
            .or_else(|| spec.query_modes.first().map(|mode| mode.value.clone()))
    }

    pub(super) fn effective_query_language(&self, cx: &App) -> QueryLanguage {
        let Some(spec) = self.current_source_context_spec(cx) else {
            return self.query_language.clone();
        };

        let selected_mode = self.current_source_query_mode_value(cx);

        spec.query_modes
            .into_iter()
            .find(|mode| Some(mode.value.as_str()) == selected_mode.as_deref())
            .map(|mode| mode.query_language)
            .unwrap_or_else(|| self.query_language.clone())
    }

    pub(super) fn should_show_source_controls(&self, cx: &App) -> bool {
        self.current_source_context_spec(cx).is_some()
    }

    fn source_target_items(&self, cx: &App) -> Vec<DropdownItem> {
        let Some(connection_id) = self.exec_ctx.connection_id.or(self.connection_id) else {
            return Vec::new();
        };

        let Some(connected) = self.app_state.read(cx).connections().get(&connection_id) else {
            return Vec::new();
        };

        let schema = self
            .exec_ctx
            .database
            .as_deref()
            .and_then(|database| connected.schema_for_target_database(database))
            .or(connected.schema.as_ref());

        let Some(schema) = schema else {
            return Vec::new();
        };

        // For time-series databases (e.g. InfluxDB) the source-context
        // dropdown represents the top-level container (bucket for v2,
        // database for v1) rather than individual measurements.  Measurements
        // live inside a bucket and are filter predicates in the query, not
        // things a user switches between in the context bar.
        //
        // `SchemaSnapshot::databases()` returns the accessible buckets/
        // databases enumerated by the driver — no driver-id branching needed.
        let mut items: Vec<DropdownItem> = if schema.is_time_series() {
            schema
                .databases()
                .iter()
                .map(|db| DropdownItem::with_value(&db.name, &db.name))
                .collect()
        } else {
            schema
                .collections()
                .iter()
                .map(|c| DropdownItem::with_value(&c.name, &c.name))
                .collect()
        };

        items.sort_by(|left, right| left.label.as_ref().cmp(right.label.as_ref()));
        items
    }

    fn current_source_targets(&self, cx: &App) -> Vec<String> {
        self.source_targets
            .read(cx)
            .selected_values()
            .iter()
            .map(|value| value.to_string())
            .collect()
    }

    pub(super) fn current_source_context(
        &self,
        cx: &App,
    ) -> Result<ExecutionSourceContext, &'static str> {
        let query_mode = self.current_source_query_mode_value(cx);
        let targets = self.current_source_targets(cx);
        let start_input = self.source_start_input.read(cx).value().to_string();
        let end_input = self.source_end_input.read(cx).value().to_string();

        if start_input.trim().is_empty()
            && end_input.trim().is_empty()
            && let Some(source @ ExecutionSourceContext::CollectionWindow { .. }) =
                self.exec_ctx.source.clone()
        {
            return Ok(source);
        }

        let start_ms = parse_source_datetime_input(&start_input);
        let end_ms = parse_source_datetime_input(&end_input);

        build_source_window_context(query_mode, &targets, start_ms, end_ms)
    }

    fn sync_source_exec_context(&mut self, cx: &mut Context<Self>) {
        if !self.should_show_source_controls(cx) {
            self.exec_ctx.source = None;
            return;
        }

        let start_blank = self.source_start_input.read(cx).value().trim().is_empty();
        let end_blank = self.source_end_input.read(cx).value().trim().is_empty();

        if start_blank && end_blank {
            // Time bounds not entered yet: keep any existing window, but sync its
            // query_mode with the dropdown so switching syntax (e.g. to Flux) is
            // not lost while waiting for a time range. Without this, the stored
            // mode stays stale and the query is routed with the old language.
            let mode = self.current_source_query_mode_value(cx);
            if let Some(ExecutionSourceContext::CollectionWindow { query_mode, .. }) =
                self.exec_ctx.source.as_mut()
            {
                *query_mode = mode;
            }
            return;
        }

        self.exec_ctx.source = self.current_source_context(cx).ok();
    }

    fn sync_source_controls(&mut self, cx: &mut Context<Self>) {
        let should_show = self.should_show_source_controls(cx);
        let items = if should_show {
            self.source_target_items(cx)
        } else {
            Vec::new()
        };

        let source_spec = self.current_source_context_spec(cx);

        // Tear down the time-range panel when the spec no longer declares
        // labelled start/end inputs.  Creation is deferred to render because
        // the DatePickerState constructor requires a Window reference.
        // B.3.1: the canonical site for SourceContextSpec start_label / end_label consumption.
        let wants_panel = source_spec
            .as_ref()
            .is_some_and(|spec| !spec.start_label.is_empty() && !spec.end_label.is_empty());

        // Only tear down when the spec is resolved AND explicitly does not want
        // a panel. A transient `source_spec = None` (e.g. mid-schema-reload on
        // an AppStateChanged) would otherwise destroy the panel and force a
        // re-seed of the default preset on the next render — clobbering any
        // custom window the user just applied via the chart toolbar.
        if !wants_panel && source_spec.is_some() {
            self.source_time_range_panel = None;
            self._source_time_range_sub = None;
        }

        let query_mode_items = source_spec
            .as_ref()
            .map(|spec| {
                spec.query_modes
                    .iter()
                    .map(|mode| DropdownItem::with_value(&mode.label, &mode.value))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Preserve the syntax the user already picked. An AppStateChanged event
        // (schema reload, another connection changing state, etc.) must not
        // silently reset the dropdown to the spec default — which is what
        // happened to Flux on InfluxDB v2, whose default mode is InfluxQL.
        let current_dropdown_mode = self
            .source_query_mode_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string());

        let committed_mode = match self.exec_ctx.source.as_ref() {
            Some(ExecutionSourceContext::CollectionWindow { query_mode, .. }) => query_mode.clone(),
            None => None,
            // MetricQuery sources do not participate in the log-group source controls.
            _ => None,
        };

        let available_modes: Vec<String> = query_mode_items
            .iter()
            .map(|item| item.value.to_string())
            .collect();

        let spec_default = source_spec
            .as_ref()
            .and_then(|spec| spec.default_query_mode.clone());

        let selected_query_mode = resolve_query_mode_selection(
            committed_mode.as_deref(),
            current_dropdown_mode.as_deref(),
            &available_modes,
            spec_default.as_deref(),
        );

        let selected_query_mode_index = selected_query_mode.as_ref().and_then(|selected| {
            query_mode_items
                .iter()
                .position(|item| item.value.as_ref() == selected)
        });

        self.source_query_mode_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(query_mode_items, cx);
            dropdown.set_selected_index(selected_query_mode_index, cx);
        });

        // Derive the initial selection: prefer an explicit exec_ctx source, then fall
        // back to the spec's default target so the driver's connected bucket/database
        // is pre-selected instead of showing a blank "Sources" placeholder.
        let selected_values = match self.exec_ctx.source.as_ref() {
            Some(ExecutionSourceContext::CollectionWindow { targets, .. }) => targets.clone(),
            None => source_spec
                .as_ref()
                .and_then(|spec| spec.default_target.clone())
                .into_iter()
                .collect(),
            // MetricQuery sources do not populate the log-group targets selector.
            _ => Vec::new(),
        };

        let targets_placeholder = source_spec
            .as_ref()
            .map(|spec| spec.targets_placeholder.clone())
            .unwrap_or_else(|| "Sources".to_string());

        self.source_targets.update(cx, |multi_select, cx| {
            multi_select.set_placeholder(targets_placeholder, cx);
            multi_select.set_items(items, cx);
            multi_select.set_selected_values(&selected_values, cx);
        });

        self.sync_source_exec_context(cx);
    }

    pub(super) fn on_source_query_mode_changed(
        &mut self,
        _item: &DropdownItem,
        cx: &mut Context<Self>,
    ) {
        self.sync_source_exec_context(cx);
        self.sync_editor_language(cx);
        self.schedule_diagnostic_refresh(cx);
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    pub(super) fn on_source_targets_changed(
        &mut self,
        _selected_targets: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        self.sync_source_exec_context(cx);
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    pub(super) fn on_source_time_range_changed(&mut self, cx: &mut Context<Self>) {
        self.sync_source_exec_context(cx);
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Called when the embedded `TimeRangePanel` emits `TimeRangeChanged`.
    ///
    /// Updates `exec_ctx.source` with the epoch-ms bounds produced by the
    /// panel, preserving the existing targets and query-mode selections.
    /// Only a preset selection produces a valid (start, end) pair; Custom
    /// mode defers to the user pressing Apply inside the panel.
    pub(super) fn on_source_time_range_panel_changed(
        &mut self,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        cx: &mut Context<Self>,
    ) {
        let query_mode = self.current_source_query_mode_value(cx);
        let targets = self.current_source_targets(cx);

        if let (Some(start_ms), Some(end_ms)) = (start_ms, end_ms) {
            self.exec_ctx.source = Some(ExecutionSourceContext::CollectionWindow {
                targets,
                start_ms,
                end_ms,
                query_mode,
            });

            if !self.result_tabs.is_empty() {
                self.pending_chart_reexecute = true;
            }
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    pub(super) fn sync_context_dropdowns(&mut self, cx: &mut Context<Self>) {
        let mut did_change = false;

        if self.connection_id.is_none()
            && self.exec_ctx.connection_id.is_none()
            && let Some(active_connection_id) = self.app_state.read(cx).active_connection_id()
            && self
                .app_state
                .read(cx)
                .connections()
                .contains_key(&active_connection_id)
        {
            self.connection_id = Some(active_connection_id);
            self.exec_ctx.connection_id = Some(active_connection_id);
            did_change = true;
        }

        let connection_items = Self::connection_items(&self.app_state, cx);
        let selected_connection_index = self.connection_id.and_then(|id| {
            let id = id.to_string();
            connection_items
                .iter()
                .position(|item| item.value.as_ref() == id)
        });

        let has_selected_connection = self
            .connection_id
            .is_some_and(|id| self.app_state.read(cx).connections().contains_key(&id));

        self.connection_dropdown.update(cx, |dd, cx| {
            dd.set_items(connection_items, cx);
            dd.set_selected_index(selected_connection_index, cx);
        });

        if has_selected_connection {
            if let Some(connection_id) = self.connection_id {
                self.runner.set_profile_id(connection_id);

                let database_items =
                    Self::database_items_for_connection(&self.app_state, Some(connection_id), cx);

                if self.exec_ctx.database.is_none() {
                    self.exec_ctx.database =
                        Self::default_database_for_connection(&self.app_state, connection_id, cx);
                    did_change = true;
                }

                if self.exec_ctx.database.as_ref().is_some_and(|database| {
                    !database_items
                        .iter()
                        .any(|item| item.value.as_ref() == database)
                }) {
                    self.exec_ctx.database =
                        Self::default_database_for_connection(&self.app_state, connection_id, cx);
                    did_change = true;
                }

                let selected_database_index =
                    self.exec_ctx.database.as_ref().and_then(|database| {
                        database_items
                            .iter()
                            .position(|item| item.value.as_ref() == database)
                    });

                self.database_dropdown.update(cx, |dd, cx| {
                    dd.set_items(database_items, cx);
                    dd.set_selected_index(selected_database_index, cx);
                });

                let schema_items =
                    Self::schema_items_for_connection(&self.app_state, &self.exec_ctx, cx);
                let selected_schema_index = self.exec_ctx.schema.as_ref().and_then(|schema| {
                    schema_items
                        .iter()
                        .position(|item| item.value.as_ref() == schema)
                });

                let next_schema = if selected_schema_index.is_some() {
                    self.exec_ctx.schema.clone()
                } else if schema_items
                    .iter()
                    .any(|item| item.value.as_ref() == "public")
                {
                    Some("public".to_string())
                } else {
                    None
                };

                if self.exec_ctx.schema != next_schema {
                    self.exec_ctx.schema = next_schema.clone();
                    did_change = true;
                }

                let selected_schema_index = next_schema.as_ref().and_then(|schema| {
                    schema_items
                        .iter()
                        .position(|item| item.value.as_ref() == schema)
                });

                self.schema_dropdown.update(cx, |dd, cx| {
                    dd.set_items(schema_items, cx);
                    dd.set_selected_index(selected_schema_index, cx);
                });
            }
        } else {
            self.runner.clear_profile_id();

            self.database_dropdown.update(cx, |dd, cx| {
                dd.set_items(Vec::new(), cx);
                dd.set_selected_index(None, cx);
            });

            self.schema_dropdown.update(cx, |dd, cx| {
                dd.set_items(Vec::new(), cx);
                dd.set_selected_index(None, cx);
            });
        }

        self.sync_source_controls(cx);
        self.sync_editor_language(cx);

        if did_change {
            cx.emit(DocumentEvent::MetaChanged);
        }

        cx.notify();
    }

    pub(super) fn create_database_dropdown(
        app_state: &Entity<AppStateEntity>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let items = Self::database_items_for_connection(app_state, exec_ctx.connection_id, cx);

        let selected_index = exec_ctx
            .database
            .as_ref()
            .and_then(|db| items.iter().position(|item| item.value.as_ref() == db));

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-database")
                .items(items)
                .selected_index(selected_index)
                .placeholder("Database")
                .toolbar_style(true)
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.on_database_changed(&event.item, cx);
            },
        );

        (dropdown, sub)
    }

    pub(super) fn create_schema_dropdown(
        app_state: &Entity<AppStateEntity>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let items = Self::schema_items_for_connection(app_state, exec_ctx, cx);

        let selected_index = exec_ctx
            .schema
            .as_ref()
            .and_then(|s| items.iter().position(|item| item.value.as_ref() == s));

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-schema")
                .items(items)
                .selected_index(selected_index)
                .placeholder("Schema")
                .toolbar_style(true)
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.on_schema_changed(&event.item, cx);
            },
        );

        (dropdown, sub)
    }

    // === Event handlers for context changes ===

    fn on_connection_changed(
        &mut self,
        item: &DropdownItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Ok(new_conn_id) = Uuid::parse_str(item.value.as_ref()) else {
            log::warn!("Invalid connection id in dropdown: {}", item.value.as_ref());
            return;
        };

        self.exec_ctx.connection_id = Some(new_conn_id);
        self.connection_id = Some(new_conn_id);
        self.exec_ctx.database =
            Self::default_database_for_connection(&self.app_state, new_conn_id, cx);
        self.exec_ctx.schema = None;
        self.exec_ctx.container = None;

        self.sync_context_dropdowns(cx);

        // Re-validate context bar index since dropdown visibility may have changed
        if self.focus_mode == SqlQueryFocus::ContextBar {
            self.revalidate_context_bar_index(window, cx);
        }
    }

    fn on_database_changed(&mut self, item: &DropdownItem, cx: &mut Context<Self>) {
        let db_name = item.value.to_string();

        // Save previous state so we can revert on connection failure.
        let prev_database = self.exec_ctx.database.clone();
        let prev_schema = self.exec_ctx.schema.clone();

        self.exec_ctx.database = Some(db_name.clone());
        self.exec_ctx.schema = None;

        if let Some(conn_id) = self.exec_ctx.connection_id {
            let needs_connection = self
                .app_state
                .read(cx)
                .connections()
                .get(&conn_id)
                .is_some_and(|c| {
                    let strategy = c.connection.schema_loading_strategy();
                    strategy == SchemaLoadingStrategy::ConnectionPerDatabase
                        && c.database_connection(&db_name).is_none()
                        && c.schema
                            .as_ref()
                            .and_then(|s| s.current_database())
                            .is_none_or(|current| current != db_name)
                });

            if needs_connection {
                self.connect_to_database(conn_id, db_name.clone(), prev_database, prev_schema, cx);
            }
        }

        self.refresh_schema_dropdown_with_default(cx);

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    fn on_schema_changed(&mut self, item: &DropdownItem, cx: &mut Context<Self>) {
        self.exec_ctx.schema = Some(item.value.to_string());
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Refresh the schema dropdown and pre-select the default schema ("public" for PG).
    fn refresh_schema_dropdown_with_default(&mut self, cx: &mut Context<Self>) {
        let schema_items = Self::schema_items_for_connection(&self.app_state, &self.exec_ctx, cx);

        let selected_index = self.exec_ctx.schema.as_ref().and_then(|schema| {
            schema_items
                .iter()
                .position(|item| item.value.as_ref() == schema)
        });

        let next_schema = if selected_index.is_some() {
            self.exec_ctx.schema.clone()
        } else if schema_items
            .iter()
            .any(|item| item.value.as_ref() == "public")
        {
            Some("public".to_string())
        } else {
            None
        };

        self.exec_ctx.schema = next_schema.clone();

        let selected_index = next_schema.as_ref().and_then(|schema| {
            schema_items
                .iter()
                .position(|item| item.value.as_ref() == schema)
        });

        self.schema_dropdown.update(cx, |dd, cx| {
            dd.set_items(schema_items, cx);
            dd.set_selected_index(selected_index, cx);
        });
    }

    /// Connect to a specific database. Reverts `exec_ctx` on failure.
    fn connect_to_database(
        &mut self,
        profile_id: Uuid,
        database: String,
        prev_database: Option<String>,
        prev_schema: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let params = match self
            .app_state
            .read(cx)
            .prepare_database_connection(profile_id, &database)
        {
            Ok(p) => p,
            Err(e) => {
                log::warn!("Cannot connect to database {}: {}", database, e);
                self.revert_database_selection(prev_database, prev_schema, cx);
                return;
            }
        };

        let app_state = self.app_state.clone();
        let target_db = database.clone();

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            match result {
                Ok(switch_result) => {
                    cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.add_database_connection(
                                profile_id,
                                target_db.clone(),
                                switch_result.connection,
                                switch_result.schema,
                            );
                            cx.emit(AppStateChanged);
                        });

                        this.update(cx, |doc, cx| {
                            doc.refresh_schema_dropdown_with_default(cx);
                            cx.notify();
                        })
                        .ok();
                    })
                    .log_if_dropped();
                }
                Err(e) => {
                    log::error!("Failed to connect to database {}: {}", target_db, e);
                    cx.update(|cx| {
                        this.update(cx, |doc, cx| {
                            doc.revert_database_selection(prev_database, prev_schema, cx);

                            doc.pending_error = Some(format!(
                                "Failed to connect to database '{}': {}",
                                target_db, e
                            ));
                            cx.notify();
                        })
                        .ok();
                    })
                    .log_if_dropped();
                }
            }
        })
        .detach();
    }

    /// Revert the database dropdown and exec_ctx to the previous state.
    fn revert_database_selection(
        &mut self,
        prev_database: Option<String>,
        prev_schema: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.exec_ctx.database = prev_database.clone();
        self.exec_ctx.schema = prev_schema;

        let db_items =
            Self::database_items_for_connection(&self.app_state, self.exec_ctx.connection_id, cx);

        let db_selected = prev_database
            .as_ref()
            .and_then(|db| db_items.iter().position(|item| item.value.as_ref() == db));

        self.database_dropdown.update(cx, |dd, cx| {
            dd.set_items(db_items, cx);
            dd.set_selected_index(db_selected, cx);
        });

        self.refresh_schema_dropdown_with_default(cx);
    }

    // === Data fetching helpers ===

    fn database_items_for_connection(
        app_state: &Entity<AppStateEntity>,
        connection_id: Option<Uuid>,
        cx: &App,
    ) -> Vec<DropdownItem> {
        let Some(conn_id) = connection_id else {
            return Vec::new();
        };

        let Some(connected) = app_state.read(cx).connections().get(&conn_id) else {
            return Vec::new();
        };

        let Some(schema) = &connected.schema else {
            return Vec::new();
        };

        schema
            .databases()
            .iter()
            .map(|db| DropdownItem::with_value(&db.name, &db.name))
            .collect()
    }

    pub(super) fn schema_items_for_connection(
        app_state: &Entity<AppStateEntity>,
        exec_ctx: &ExecutionContext,
        cx: &App,
    ) -> Vec<DropdownItem> {
        let Some(conn_id) = exec_ctx.connection_id else {
            return Vec::new();
        };

        let Some(connected) = app_state.read(cx).connections().get(&conn_id) else {
            return Vec::new();
        };

        if !connected
            .connection
            .metadata()
            .capabilities
            .contains(DriverCapabilities::SCHEMAS)
        {
            return Vec::new();
        }

        let schema = exec_ctx
            .database
            .as_deref()
            .and_then(|db| connected.schema_for_target_database(db))
            .or(connected.schema.as_ref());

        let Some(schema) = schema else {
            return Vec::new();
        };

        schema
            .schemas()
            .iter()
            .map(|s| DropdownItem::with_value(&s.name, &s.name))
            .collect()
    }

    // === Visibility helpers for render ===

    pub(super) fn should_show_database_dropdown(&self, cx: &App) -> bool {
        if self.should_show_source_controls(cx) {
            return false;
        }

        let Some(conn_id) = self.exec_ctx.connection_id else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| {
                c.connection
                    .metadata()
                    .capabilities
                    .contains(DriverCapabilities::MULTIPLE_DATABASES)
            })
            .unwrap_or(false)
    }

    pub(super) fn should_show_schema_dropdown(&self, cx: &App) -> bool {
        if self.should_show_source_controls(cx) {
            return false;
        }

        let Some(conn_id) = self.exec_ctx.connection_id else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| {
                c.connection
                    .metadata()
                    .capabilities
                    .contains(DriverCapabilities::SCHEMAS)
            })
            .unwrap_or(false)
    }

    // === Context bar keyboard navigation ===

    /// Returns the visible context-bar slots for the current document.
    fn visible_context_bar_slots(&self, cx: &App) -> Vec<ContextBarSlot> {
        if !self.query_language.supports_connection_context() {
            return Vec::new();
        }

        let mut slots = vec![ContextBarSlot::Connection];

        if self.should_show_source_controls(cx) {
            if self
                .current_source_context_spec(cx)
                .is_some_and(|spec| !spec.query_modes.is_empty())
            {
                slots.push(ContextBarSlot::SourceQueryMode);
            }
            slots.push(ContextBarSlot::SourceTargets);
            slots.push(ContextBarSlot::SourceStart);
            slots.push(ContextBarSlot::SourceEnd);
            return slots;
        }

        if self.should_show_database_dropdown(cx) {
            slots.push(ContextBarSlot::Database);
        }
        if self.should_show_schema_dropdown(cx) {
            slots.push(ContextBarSlot::Schema);
        }

        slots
    }

    fn dropdown_for_slot(&self, slot: ContextBarSlot) -> Option<&Entity<Dropdown>> {
        match slot {
            ContextBarSlot::Connection => Some(&self.connection_dropdown),
            ContextBarSlot::Database => Some(&self.database_dropdown),
            ContextBarSlot::Schema => Some(&self.schema_dropdown),
            ContextBarSlot::SourceQueryMode => Some(&self.source_query_mode_dropdown),
            ContextBarSlot::SourceTargets
            | ContextBarSlot::SourceStart
            | ContextBarSlot::SourceEnd => None,
        }
    }

    pub(super) fn enter_context_bar(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let visible = self.visible_context_bar_slots(cx);
        if visible.is_empty() {
            return;
        }

        self.focus_mode = SqlQueryFocus::ContextBar;
        self.context_bar_slot = visible[0];
        self.focus_handle.focus(window);
        self.update_context_bar_focus_rings(cx);
        cx.notify();
    }

    /// Clamp `context_bar_slot` to a visible control after connection changes.
    fn revalidate_context_bar_index(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let visible = self.visible_context_bar_slots(cx);

        if visible.is_empty() {
            self.exit_context_bar(window, cx);
            return;
        }

        if !visible.contains(&self.context_bar_slot) {
            self.context_bar_slot = visible[0];
        }

        self.update_context_bar_focus_rings(cx);
    }

    fn exit_context_bar(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.clear_context_bar_focus_rings(cx);
        self.focus_mode = SqlQueryFocus::Editor;
        self.input_state
            .update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    pub(super) fn dispatch_context_bar_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let visible = self.visible_context_bar_slots(cx);
        if visible.is_empty() {
            self.exit_context_bar(window, cx);
            return true;
        }

        // If a dropdown is open, route j/k/Enter/Escape to it
        if let Some(current_dropdown) = self.dropdown_for_slot(self.context_bar_slot).cloned()
            && current_dropdown.read(cx).is_open()
        {
            match cmd {
                Command::SelectNext => {
                    current_dropdown.update(cx, |dd, cx| dd.select_next_item(cx));
                    return true;
                }
                Command::SelectPrev => {
                    current_dropdown.update(cx, |dd, cx| dd.select_prev_item(cx));
                    return true;
                }
                Command::Execute => {
                    current_dropdown.update(cx, |dd, cx| dd.accept_selection(cx));
                    return true;
                }
                Command::Cancel => {
                    current_dropdown.update(cx, |dd, cx| dd.close(cx));
                    return true;
                }
                _ => {}
            }
        }

        match cmd {
            Command::FocusRight => {
                if let Some(pos) = visible
                    .iter()
                    .position(|&slot| slot == self.context_bar_slot)
                    && pos + 1 < visible.len()
                {
                    self.context_bar_slot = visible[pos + 1];
                    self.update_context_bar_focus_rings(cx);
                    cx.notify();
                }
                true
            }
            Command::FocusLeft => {
                if let Some(pos) = visible
                    .iter()
                    .position(|&slot| slot == self.context_bar_slot)
                    && pos > 0
                {
                    self.context_bar_slot = visible[pos - 1];
                    self.update_context_bar_focus_rings(cx);
                    cx.notify();
                }
                true
            }

            Command::Execute => {
                match self.context_bar_slot {
                    ContextBarSlot::SourceQueryMode => {
                        self.source_query_mode_dropdown
                            .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
                    }
                    ContextBarSlot::SourceTargets => {
                        self.source_targets
                            .update(cx, |multi_select, cx| multi_select.toggle_open(cx));
                    }
                    ContextBarSlot::SourceStart => {
                        self.source_start_input
                            .update(cx, |state, cx| state.focus(window, cx));
                    }
                    ContextBarSlot::SourceEnd => {
                        self.source_end_input
                            .update(cx, |state, cx| state.focus(window, cx));
                    }
                    _ => {
                        if let Some(current_dropdown) =
                            self.dropdown_for_slot(self.context_bar_slot).cloned()
                        {
                            current_dropdown.update(cx, |dd, cx| dd.toggle_open(cx));
                        }
                    }
                }
                true
            }

            Command::FocusDown | Command::Cancel => {
                self.exit_context_bar(window, cx);
                true
            }

            Command::FocusUp => true,

            // Don't exit context bar for unrelated commands (e.g. C-b toggle sidebar)
            _ => false,
        }
    }

    fn update_context_bar_focus_rings(&self, cx: &mut Context<Self>) {
        let theme = cx.theme();
        let active_color = theme.ring;

        for slot in [
            ContextBarSlot::Connection,
            ContextBarSlot::Database,
            ContextBarSlot::Schema,
            ContextBarSlot::SourceQueryMode,
        ] {
            if let Some(dropdown) = self.dropdown_for_slot(slot) {
                let color = if slot == self.context_bar_slot {
                    Some(active_color)
                } else {
                    None
                };
                dropdown.update(cx, |dd, cx| dd.set_focus_ring(color, cx));
            }
        }
    }

    fn clear_context_bar_focus_rings(&self, cx: &mut Context<Self>) {
        for slot in [
            ContextBarSlot::Connection,
            ContextBarSlot::Database,
            ContextBarSlot::Schema,
            ContextBarSlot::SourceQueryMode,
        ] {
            if let Some(dropdown) = self.dropdown_for_slot(slot) {
                dropdown.update(cx, |dd, cx| dd.set_focus_ring(None, cx));
            }
        }
    }

    // === Render the context bar ===

    pub(super) fn render_context_bar(&self, cx: &mut Context<Self>) -> AnyElement {
        if !self.query_language.supports_connection_context() {
            return div().id("exec-context-bar").into_any_element();
        }

        let theme = cx.theme();

        let show_source_controls = self.should_show_source_controls(cx);
        let show_db = self.should_show_database_dropdown(cx);
        let show_schema = self.should_show_schema_dropdown(cx);
        let source_spec = self.current_source_context_spec(cx);

        // When the active result grid is in Chart mode the chart toolbar
        // renders its own RANGE chips; hide the time-range widget here.
        let is_chart_mode = self
            .active_result_index
            .and_then(|i| self.result_tabs.get(i))
            .map(|t| t.grid.read(cx).result_view_mode() == ResultViewMode::Chart)
            .unwrap_or(false);

        // Determine whether the custom date-range picker is active.  When it
        // is, the picker + hour/minute dropdowns + Apply button are rendered
        // on a dedicated second row so they don't overflow the bar width.
        // Hidden in Chart mode (chart toolbar covers the range selection).
        // Tuple shrunk to (panel_entity, can_apply) — the helper owns the
        // individual sub-entity references via render_custom_picker_row.
        let custom_range_info = (!is_chart_mode)
            .then_some(())
            .and(self.source_time_range_panel.as_ref())
            .and_then(|p| {
                let panel = p.read(cx);
                let is_custom = panel.selected_time_range == Some(TimeRange::Custom);
                is_custom.then(|| (p.clone(), panel.can_apply_custom_range(cx)))
            });

        // Build the primary (always-visible) controls row.
        // flex_wrap() allows controls to wrap to the next line on narrow viewports
        // rather than overflowing the bar's right edge.
        let main_row = div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap(Spacing::SM)
            .child(
                // flex_none keeps the label+control pair together on the same wrap line.
                div()
                    .flex()
                    .flex_none()
                    .items_center()
                    .gap_1()
                    .child(Icon::new(AppIcon::Database).size(px(12.0)).muted()) // guardrail-allow: 12px icon size, no ICON_XS token
                    .child(Text::caption("Connection:")),
            )
            .child(
                div()
                    .flex_none()
                    .min_w(context_dropdown_min_width(0))
                    .child(focus_frame(
                        context_slot_is_keyboard_focused(
                            self.focus_mode,
                            self.context_bar_slot,
                            ContextBarSlot::Connection,
                        ),
                        Some(theme.ring),
                        control_shell(self.connection_dropdown.clone(), cx),
                        cx,
                    )),
            )
            .when(show_source_controls, |el| {
                let source_spec = source_spec.as_ref();

                let el = el
                    .when(
                        source_spec.is_some_and(|spec| !spec.query_modes.is_empty()),
                        |el| {
                            el.child(
                                div().flex_none().child(Text::caption(
                                    source_spec
                                        .and_then(|spec| spec.query_mode_label.clone())
                                        .unwrap_or_else(|| "Syntax".to_string()),
                                )),
                            )
                            .child(
                                div().flex_none().min_w(px(180.0)).child(focus_frame(
                                    context_slot_is_keyboard_focused(
                                        self.focus_mode,
                                        self.context_bar_slot,
                                        ContextBarSlot::SourceQueryMode,
                                    ),
                                    Some(theme.ring),
                                    control_shell(self.source_query_mode_dropdown.clone(), cx),
                                    cx,
                                )),
                            )
                        },
                    )
                    // "Source:" is the generic label for the target-selector dropdown
                    // across all drivers.  The driver-specific label (spec.targets_label)
                    // is intentionally not used here — the placeholder already carries
                    // driver-specific phrasing (e.g. "Select bucket...").
                    .child(div().flex_none().child(Text::caption("Source:")))
                    .child(div().flex_none().min_w(px(260.0)).child(focus_frame(
                        context_slot_is_keyboard_focused(
                            self.focus_mode,
                            self.context_bar_slot,
                            ContextBarSlot::SourceTargets,
                        ),
                        Some(theme.ring),
                        control_shell(self.source_targets.clone(), cx),
                        cx,
                    )));

                // Time-range preset dropdown — always on the main row, unless the
                // active result grid is in Chart mode (the chart toolbar has its own
                // RANGE chips in that case).
                // The custom date-range controls are on the second row (below).
                let el = el.when_some(
                    (!is_chart_mode)
                        .then_some(self.source_time_range_panel.as_ref())
                        .flatten()
                        .map(|p| {
                            let panel = p.read(cx);
                            let dropdown = panel.dropdown_time_range.clone();
                            let label = source_spec
                                .map(|s| s.start_label.clone())
                                .unwrap_or_else(|| "Time".to_string());
                            (dropdown, label)
                        }),
                    |el, (dropdown, label)| {
                        el.child(div().flex_none().child(Text::caption(label)))
                            .child(
                                div()
                                    .flex_none()
                                    .min_w(px(220.0))
                                    .child(control_shell(dropdown, cx)),
                            )
                    },
                );

                // Text-input fallback when there is no time-range panel (specs
                // without start/end labels — not InfluxDB but kept for generality).
                // Also hidden in Chart mode (chart toolbar covers this).
                el.when(
                    !is_chart_mode && self.source_time_range_panel.is_none(),
                    |el| {
                        el.child(
                            div().flex_none().child(Text::caption(
                                source_spec
                                    .map(|spec| spec.start_label.clone())
                                    .unwrap_or_else(|| "Start".to_string()),
                            )),
                        )
                        .child(div().flex_none().min_w(px(180.0)).child(focus_frame(
                            context_slot_is_keyboard_focused(
                                self.focus_mode,
                                self.context_bar_slot,
                                ContextBarSlot::SourceStart,
                            ),
                            Some(theme.ring),
                            control_shell(Input::new(&self.source_start_input), cx),
                            cx,
                        )))
                        .child(
                            div().flex_none().child(Text::caption(
                                source_spec
                                    .map(|spec| spec.end_label.clone())
                                    .unwrap_or_else(|| "End".to_string()),
                            )),
                        )
                        .child(div().flex_none().min_w(px(180.0)).child(focus_frame(
                            context_slot_is_keyboard_focused(
                                self.focus_mode,
                                self.context_bar_slot,
                                ContextBarSlot::SourceEnd,
                            ),
                            Some(theme.ring),
                            control_shell(Input::new(&self.source_end_input), cx),
                            cx,
                        )))
                    },
                )
            })
            .when(!show_source_controls && show_db, |el| {
                el.child(div().flex_none().child(Text::caption("Database:")))
                    .child(
                        div()
                            .flex_none()
                            .min_w(context_dropdown_min_width(1))
                            .child(focus_frame(
                                context_slot_is_keyboard_focused(
                                    self.focus_mode,
                                    self.context_bar_slot,
                                    ContextBarSlot::Database,
                                ),
                                Some(theme.ring),
                                control_shell(self.database_dropdown.clone(), cx),
                                cx,
                            )),
                    )
            })
            .when(!show_source_controls && show_schema, |el| {
                el.child(div().flex_none().child(Text::caption("Schema:")))
                    .child(
                        div()
                            .flex_none()
                            .min_w(context_dropdown_min_width(2))
                            .child(focus_frame(
                                context_slot_is_keyboard_focused(
                                    self.focus_mode,
                                    self.context_bar_slot,
                                    ContextBarSlot::Schema,
                                ),
                                Some(theme.ring),
                                control_shell(self.schema_dropdown.clone(), cx),
                                cx,
                            )),
                    )
            })
            .child(div().flex_1())
            .when_some(self.path.as_ref(), |el, path| {
                el.child(
                    div()
                        .overflow_x_hidden()
                        .child(Text::caption(path.display().to_string())),
                )
            });

        // Outer bar: column layout so the custom date-range row can sit below
        // the main controls without stretching the bar's width.
        div()
            .id("exec-context-bar")
            .flex()
            .flex_col()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(main_row)
            // Custom date-range second row — only visible when Custom is active.
            // This avoids overflowing the single-line bar with the date picker,
            // four time dropdowns, and Apply button all pushed onto one row.
            .when_some(custom_range_info, |el, (panel, can_apply)| {
                el.child(
                    div()
                        .flex()
                        .items_center()
                        .gap_1()
                        .pt(Spacing::XS)
                        .child(panel.read(cx).render_custom_picker_row(px(320.0), cx))
                        .child(
                            Button::new("ctx-time-range-apply", "Apply")
                                .small()
                                .disabled(!can_apply)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    panel.update(cx, |p, cx| {
                                        // Ignore the returned bounds — the panel emits
                                        // TimeRangeChanged which is the authoritative signal.
                                        let _ = p.apply_custom_range(cx);
                                    });
                                    this.sync_source_exec_context(cx);
                                    cx.emit(DocumentEvent::MetaChanged);
                                    cx.notify();
                                })),
                        ),
                )
            })
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ContextBarSlot, SqlQueryFocus, build_source_window_context, context_dropdown_min_width,
        context_slot_is_keyboard_focused, parse_source_datetime_input,
        resolve_query_mode_selection,
    };
    use dbflux_core::ExecutionSourceContext;
    use gpui::px;

    #[test]
    fn query_mode_selection_prefers_committed_then_dropdown() {
        let available = vec!["influxql".to_string(), "flux".to_string()];

        // A committed mode wins over the dropdown and the default.
        assert_eq!(
            resolve_query_mode_selection(
                Some("flux"),
                Some("influxql"),
                &available,
                Some("influxql")
            ),
            Some("flux".to_string())
        );

        // No committed mode: the current dropdown choice is preserved over the
        // default — this is the AppStateChanged reset the fix prevents.
        assert_eq!(
            resolve_query_mode_selection(None, Some("flux"), &available, Some("influxql")),
            Some("flux".to_string())
        );
    }

    #[test]
    fn query_mode_selection_falls_back_to_default() {
        let available = vec!["influxql".to_string(), "flux".to_string()];

        // Nothing committed and nothing in the dropdown: use the spec default.
        assert_eq!(
            resolve_query_mode_selection(None, None, &available, Some("influxql")),
            Some("influxql".to_string())
        );

        // A preferred mode the current spec no longer offers falls back to the
        // default rather than selecting an absent value.
        assert_eq!(
            resolve_query_mode_selection(Some("sql"), None, &available, Some("influxql")),
            Some("influxql".to_string())
        );
    }

    #[test]
    fn connection_dropdown_keeps_widest_shell() {
        assert_eq!(context_dropdown_min_width(0), px(140.0));
    }

    #[test]
    fn database_and_schema_dropdown_shells_keep_compact_widths() {
        assert_eq!(context_dropdown_min_width(1), px(120.0));
        assert_eq!(context_dropdown_min_width(2), px(100.0));
    }

    #[test]
    fn only_active_context_bar_dropdown_reports_keyboard_focus() {
        assert!(context_slot_is_keyboard_focused(
            SqlQueryFocus::ContextBar,
            ContextBarSlot::Database,
            ContextBarSlot::Database,
        ));
        assert!(!context_slot_is_keyboard_focused(
            SqlQueryFocus::ContextBar,
            ContextBarSlot::Database,
            ContextBarSlot::Connection,
        ));
        assert!(!context_slot_is_keyboard_focused(
            SqlQueryFocus::Editor,
            ContextBarSlot::Database,
            ContextBarSlot::Database,
        ));
    }

    #[test]
    fn source_datetime_inputs_parse_rfc3339_values() {
        assert!(parse_source_datetime_input("2026-04-24T12:34:56Z").is_some());
        assert!(parse_source_datetime_input("").is_none());
        assert!(parse_source_datetime_input("not-a-date").is_none());
    }

    #[test]
    fn valid_source_context_requires_targets_and_ordered_bounds() {
        let source = build_source_window_context(
            Some("cwli".to_string()),
            &["/aws/lambda/app".to_string()],
            Some(10),
            Some(20),
        )
        .expect("valid source context");

        match source {
            ExecutionSourceContext::CollectionWindow {
                targets,
                start_ms,
                end_ms,
                query_mode,
            } => {
                assert_eq!(targets, vec!["/aws/lambda/app"]);
                assert_eq!(start_ms, 10);
                assert_eq!(end_ms, 20);
                assert_eq!(query_mode.as_deref(), Some("cwli"));
            }
            other => panic!("expected CollectionWindow source context, got: {other:?}"),
        }

        assert_eq!(
            build_source_window_context(Some("cwli".to_string()), &[], Some(10), Some(20))
                .unwrap_err(),
            "Select at least one source"
        );
        assert_eq!(
            build_source_window_context(
                Some("cwli".to_string()),
                &["/aws/lambda/app".to_string()],
                None,
                Some(20),
            )
            .unwrap_err(),
            "Start time is required"
        );
        assert_eq!(
            build_source_window_context(
                Some("cwli".to_string()),
                &["/aws/lambda/app".to_string()],
                Some(20),
                Some(10),
            )
            .unwrap_err(),
            "Start time must be earlier than end time"
        );
    }

    #[test]
    fn sql_source_context_allows_empty_targets() {
        let source = build_source_window_context(Some("sql".to_string()), &[], Some(10), Some(20))
            .expect("sql source context without explicit targets");

        match source {
            ExecutionSourceContext::CollectionWindow { targets, .. } => {
                assert!(targets.is_empty());
            }
            other => panic!("expected CollectionWindow source context, got: {other:?}"),
        }
    }
}
