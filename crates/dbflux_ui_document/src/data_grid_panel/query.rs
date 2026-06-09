use super::filter_bar::{FilterMode, RelationalFilterState, classify_filter_input};
use super::{DataGridPanel, DataSource, GridState, PendingToast, PendingTotalCount};
use dbflux_components::components::data_table::SortState as TableSortState;
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, EditableBinding, OrderByColumn,
    Pagination, QueryRequest, QueryResult, SelectQuery, SourceTable, TableBrowseRequest,
    TableCountRequest, TableRef, TaskKind, TaskTarget, VisualQuerySpec,
};
use dbflux_core::{
    RelationalFilterError, RelationalResolveError, count_query_from_spec, parse_and_resolve,
    project_aggregate_kinds,
};
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use gpui::*;
use log::info;
use uuid::Uuid;

impl DataGridPanel {
    /// Refresh data from source.
    ///
    /// When a `visual_select` is present (i.e., the builder panel has produced
    /// a structured SELECT), the parameterized query takes precedence over the
    /// normal `TableBrowseRequest` path.
    pub fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                pagination,
                order_by,
                total_rows,
            } => {
                let profile_id = *profile_id;
                let database = database.clone();
                let table = table.clone();
                let pagination = pagination.clone();
                let order_by = order_by.clone();
                let total_rows = *total_rows;

                if let Some(select) = self.visual_select.clone() {
                    self.run_visual_query(profile_id, database.clone(), select, window, cx);

                    if let Some(spec) = self.builder_draft_spec.clone()
                        && spec.is_grouped()
                    {
                        self.fetch_grouped_total_count(profile_id, database, spec, cx);
                    }
                } else {
                    self.run_table_query(
                        profile_id, database, table, pagination, order_by, total_rows, window, cx,
                    );
                }
            }
            DataSource::Collection {
                profile_id,
                collection,
                pagination,
                total_docs,
            } => {
                self.run_collection_query(
                    *profile_id,
                    collection.clone(),
                    pagination.clone(),
                    *total_docs,
                    window,
                    cx,
                );
            }
            DataSource::QueryResult { .. } => {
                // QueryResult is static, nothing to refresh
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_table_query(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let filter_value = self.filter_input.read(cx).value();
        let filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        let limit_value = self.limit_input.read(cx).value();
        let limit_str = limit_value.trim();
        let pagination = match limit_str.parse::<u32>() {
            Ok(0) => {
                Toast::warning("Limit must be greater than 0")
                    .meta_right(now_hms())
                    .push(cx);
                pagination
            }
            Ok(limit) if limit != pagination.limit() => pagination.with_limit(limit).reset_offset(),
            Ok(_) => pagination,
            Err(_) if !limit_str.is_empty() => {
                Toast::warning("Invalid limit value")
                    .meta_right(now_hms())
                    .push(cx);
                pagination
            }
            Err(_) => pagination,
        };

        // --- Relational filter gate (FR-GATE-1 to FR-GATE-3) ---
        //
        // Only attempt FK resolution when: the input has an unquoted `.`,
        // the driver is SQL, the source is a Table, and the FK cache is Ready
        // with at least one FK. All other cases fall through to the raw path.
        if let Some(filter_text) = &filter {
            if classify_filter_input(filter_text) == FilterMode::Relational {
                if self.try_relational_filter(
                    profile_id,
                    database.clone(),
                    table.clone(),
                    pagination.clone(),
                    order_by.clone(),
                    filter_text.clone(),
                    _window,
                    cx,
                ) {
                    return;
                }
            } else {
                // FR-GATE-3: no unquoted dot → clear any stale relational state
                if !matches!(
                    self.relational_filter_state,
                    RelationalFilterState::Inactive
                ) {
                    self.relational_filter_state = RelationalFilterState::Inactive;
                    cx.notify();
                }
            }
        } else {
            if !matches!(
                self.relational_filter_state,
                RelationalFilterState::Inactive
            ) {
                self.relational_filter_state = RelationalFilterState::Inactive;
                cx.notify();
            }
        }
        // --- end relational filter gate ---

        let mut request = TableBrowseRequest::new(table.clone())
            .with_pagination(pagination.clone())
            .with_order_by(order_by.clone());

        if let Some(ref f) = filter {
            request = request.with_filter(f.clone());
        }

        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                Toast::error("Connection not found")
                    .meta_right(now_hms())
                    .action(copy_action("Connection not found"))
                    .push(cx);
                return;
            };

            match connected.resolve_connection_for_execution(database.as_deref()) {
                Ok(connection) => connection,
                Err(dbflux_core::ConnectionResolutionError::PendingDatabaseConnection {
                    database,
                }) => {
                    let msg = format!(
                        "No connection to database '{}'. Please expand it in the sidebar first.",
                        database
                    );
                    Toast::error(msg.clone())
                        .meta_right(now_hms())
                        .action(copy_action(msg))
                        .push(cx);
                    return;
                }
            }
        };

        let active_database = {
            let state = self.app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .and_then(|c| c.active_database.clone())
        };

        let mut browse_request = request.clone();
        if browse_request.table.schema.is_none()
            && let Some(ref db) = active_database
        {
            browse_request.table.schema = Some(db.clone());
        }

        info!(
            "Running table browse: {:?}",
            browse_request.table.qualified_name()
        );

        let task_target = TaskTarget {
            profile_id,
            database: database.clone(),
        };

        let (task_id, cancel_token) = self.runner.start_primary_for_target(
            TaskKind::Query,
            format!("SELECT * FROM {}", table.qualified_name()),
            Some(task_target),
            cx,
        );

        self.state = GridState::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let conn_for_cleanup = conn.clone();

        let table_for_spawn = table.clone();
        let pagination_for_spawn = pagination.clone();
        let order_by_for_spawn = order_by.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.browse_table(&browse_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Query was cancelled, discarding result");
                    if let Err(e) = conn_for_cleanup.cleanup_after_cancel() {
                        log::warn!("Cleanup after cancel failed: {}", e);
                    }
                    return;
                }

                match &result {
                    Ok(query_result) => {
                        info!(
                            "Query returned {} rows in {:?}",
                            query_result.row_count(),
                            query_result.execution_time
                        );

                        entity.update(cx, |panel, cx| {
                            panel.runner.complete_primary(task_id, cx);
                            panel.apply_table_result(
                                profile_id,
                                table_for_spawn,
                                pagination_for_spawn,
                                order_by_for_spawn,
                                total_rows,
                                query_result.clone(),
                                cx,
                            );
                        });
                    }
                    Err(e) => {
                        log::error!("Query failed: {}", e);

                        entity.update(cx, |panel, cx| {
                            panel.runner.fail_primary(task_id, e.to_string(), cx);
                            panel.state = GridState::Error;
                            panel.pending.toast = Some(PendingToast {
                                message: format!("Query failed: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }
            }) {
                log::warn!(
                    "Failed to apply table query result to UI state: {:?}",
                    error
                );
            }
        })
        .detach();

        // Fetch total count if not known
        if total_rows.is_none() {
            self.fetch_total_count(profile_id, database, table, filter, cx);
        }
    }

    /// Executes the parameterized SELECT produced by `QueryBuilderPanel`.
    ///
    /// Called by `refresh` when `visual_select` is set. The `SelectQuery` is
    /// already fully formed (pagination baked in by the builder); we execute it
    /// as a raw parameterized query and apply the result to the grid.
    ///
    /// On success, sets `current_visual_spec` from `builder_draft_spec` and
    /// applies `project_aggregate_kinds` so aggregate result columns carry the
    /// correct `ColumnKind` for the chart engine.
    pub(super) fn run_visual_query(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        select: SelectQuery,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                Toast::error("Connection not found")
                    .meta_right(now_hms())
                    .action(copy_action("Connection not found"))
                    .push(cx);
                return;
            };

            match connected.resolve_connection_for_execution(database.as_deref()) {
                Ok(connection) => connection,
                Err(dbflux_core::ConnectionResolutionError::PendingDatabaseConnection {
                    database,
                }) => {
                    let msg = format!(
                        "No connection to database '{}'. Please expand it in the sidebar first.",
                        database
                    );
                    Toast::error(msg.clone())
                        .meta_right(now_hms())
                        .action(copy_action(msg))
                        .push(cx);
                    return;
                }
            }
        };

        let task_target = TaskTarget {
            profile_id,
            database: database.clone(),
        };

        let (task_id, cancel_token) = self.runner.start_primary_for_target(
            TaskKind::Query,
            select.sql.clone(),
            Some(task_target),
            cx,
        );

        self.state = GridState::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let conn_for_cleanup = conn.clone();

        let mut request = QueryRequest::new(select.sql.clone());
        request.params = select.params.clone();
        if let Some(ref db) = database {
            request.database = Some(db.clone());
        }

        let committed_spec: Option<VisualQuerySpec> = self.builder_draft_spec.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Visual query was cancelled, discarding result");
                    if let Err(e) = conn_for_cleanup.cleanup_after_cancel() {
                        log::warn!("Cleanup after cancel failed: {}", e);
                    }
                    return;
                }

                match result {
                    Ok(mut query_result) => {
                        info!(
                            "Visual query returned {} rows in {:?}",
                            query_result.row_count(),
                            query_result.execution_time
                        );

                        if let Some(ref spec) = committed_spec {
                            project_aggregate_kinds(spec, &mut query_result.columns);
                        }

                        entity.update(cx, |panel, cx| {
                            panel.runner.complete_primary(task_id, cx);
                            panel.current_visual_spec = committed_spec.clone();
                            panel.result = query_result;
                            panel.state = GridState::Ready;

                            let binding = panel.compute_builder_binding(
                                committed_spec.as_ref(),
                                profile_id,
                                database.as_deref(),
                                cx,
                            );
                            panel.pk_columns = binding
                                .as_ref()
                                .map(|b| b.pk_columns.clone())
                                .unwrap_or_default();
                            panel.builder_editable_binding = binding;
                            panel.pending.rebuild = true;

                            cx.notify();
                        });
                    }
                    Err(e) => {
                        log::error!("Visual query failed: {}", e);

                        entity.update(cx, |panel, cx| {
                            panel.runner.fail_primary(task_id, e.to_string(), cx);
                            panel.state = GridState::Error;
                            panel.pending.toast = Some(PendingToast {
                                message: format!("Query failed: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }
            }) {
                log::warn!(
                    "Failed to apply visual query result to UI state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    pub(super) fn run_collection_query(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        pagination: Pagination,
        total_docs: Option<u64>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let limit_value = self.limit_input.read(cx).value();
        let limit_str = limit_value.trim();
        let pagination = match limit_str.parse::<u32>() {
            Ok(0) => {
                Toast::warning("Limit must be greater than 0")
                    .meta_right(now_hms())
                    .push(cx);
                pagination
            }
            Ok(limit) if limit != pagination.limit() => pagination.with_limit(limit).reset_offset(),
            Ok(_) => pagination,
            Err(_) if !limit_str.is_empty() => {
                Toast::warning("Invalid limit value")
                    .meta_right(now_hms())
                    .push(cx);
                pagination
            }
            Err(_) => pagination,
        };

        let conn = {
            let state = self.app_state.read(cx);
            match state.connections().get(&profile_id) {
                Some(c) => Some(c.connection.clone()),
                None => {
                    Toast::error("Connection not found")
                        .meta_right(now_hms())
                        .action(copy_action("Connection not found"))
                        .push(cx);
                    return;
                }
            }
        };

        let Some(conn) = conn else {
            Toast::error("Connection not available")
                .meta_right(now_hms())
                .action(copy_action("Connection not available"))
                .push(cx);
            return;
        };

        let filter_value = self.filter_input.read(cx).value();
        let filter_str = filter_value.trim();
        let filter: Option<serde_json::Value> = if filter_str.is_empty() {
            None
        } else {
            match serde_json::from_str(filter_str) {
                Ok(v) => Some(v),
                Err(e) => {
                    let toast_body = e.to_string();
                    Toast::error("Invalid JSON filter")
                        .meta_right(now_hms())
                        .body(toast_body.clone())
                        .action(copy_action(format!("Invalid JSON filter: {}", toast_body)))
                        .push(cx);
                    return;
                }
            }
        };

        let filter_for_count = filter.clone();

        let mut browse_request =
            CollectionBrowseRequest::new(collection.clone()).with_pagination(pagination.clone());
        if let Some(f) = filter {
            browse_request = browse_request.with_filter(f);
        }

        info!(
            "Running collection browse: {}.{}",
            collection.database, collection.name
        );

        let task_target = TaskTarget {
            profile_id,
            database: Some(collection.database.clone()),
        };

        let (task_id, cancel_token) = self.runner.start_primary_for_target(
            TaskKind::Query,
            format!("find {}.{}", collection.database, collection.name),
            Some(task_target),
            cx,
        );

        self.state = GridState::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let conn_for_cleanup = conn.clone();
        let collection_for_spawn = collection.clone();
        let pagination_for_spawn = pagination.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.browse_collection(&browse_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Query was cancelled, discarding result");
                    if let Err(e) = conn_for_cleanup.cleanup_after_cancel() {
                        log::warn!("Cleanup after cancel failed: {}", e);
                    }
                    return;
                }

                match &result {
                    Ok(query_result) => {
                        info!(
                            "Collection query returned {} documents in {:?}",
                            query_result.row_count(),
                            query_result.execution_time
                        );

                        entity.update(cx, |panel, cx| {
                            panel.runner.complete_primary(task_id, cx);
                            panel.apply_collection_result(
                                profile_id,
                                collection_for_spawn,
                                pagination_for_spawn,
                                total_docs,
                                query_result.clone(),
                                cx,
                            );
                        });
                    }
                    Err(e) => {
                        log::error!("Collection query failed: {}", e);

                        entity.update(cx, |panel, cx| {
                            panel.runner.fail_primary(task_id, e.to_string(), cx);
                            panel.state = GridState::Error;
                            panel.pending.toast = Some(PendingToast {
                                message: format!("Query failed: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }
            }) {
                log::warn!(
                    "Failed to apply collection query result to UI state: {:?}",
                    error
                );
            }
        })
        .detach();

        // Fetch total count if not known (always re-fetch when filter changes)
        if total_docs.is_none() {
            self.fetch_collection_count(profile_id, collection, filter_for_count, cx);
        }
    }

    pub(super) fn apply_collection_result(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        pagination: Pagination,
        total_docs: Option<u64>,
        result: QueryResult,
        cx: &mut Context<Self>,
    ) {
        // Preserve existing total_docs if not provided
        let existing_total = match &self.source {
            DataSource::Collection { total_docs, .. } => *total_docs,
            _ => None,
        };

        self.source = DataSource::Collection {
            profile_id,
            collection,
            pagination,
            total_docs: total_docs.or(existing_total),
        };

        self.result = result;
        self.local_sort_state = None;
        self.original_row_order = None;
        self.rebuild_table(None, cx);
        self.state = GridState::Ready;
        cx.notify();
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_table_result(
        &mut self,
        profile_id: Uuid,
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
        result: QueryResult,
        cx: &mut Context<Self>,
    ) {
        // Determine sort state from order_by for visual indicator
        let initial_sort = order_by.first().and_then(|col| {
            let pos = result
                .columns
                .iter()
                .position(|c| c.name == col.column.name);
            pos.map(|column_ix| TableSortState::new(column_ix, col.direction))
        });

        // Preserve existing total_rows and database if not provided
        let (existing_total, existing_database) = match &self.source {
            DataSource::Table {
                total_rows,
                database,
                ..
            } => (*total_rows, database.clone()),
            _ => (None, None),
        };

        self.source = DataSource::Table {
            profile_id,
            database: existing_database,
            table,
            pagination,
            order_by,
            total_rows: total_rows.or(existing_total),
        };

        self.result = result;
        self.local_sort_state = None;
        self.original_row_order = None;
        self.rebuild_table(initial_sort, cx);
        self.state = GridState::Ready;
        cx.notify();
    }

    pub(super) fn fetch_total_count(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table: TableRef,
        filter: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                return;
            };

            match connected.resolve_connection_for_execution(database.as_deref()) {
                Ok(connection) => connection,
                Err(_) => return,
            }
        };

        let mut count_request = TableCountRequest::new(table.clone());
        if let Some(f) = filter {
            count_request = count_request.with_filter(f);
        }

        let entity = cx.entity().clone();
        let qualified = table.qualified_name();

        let task = cx
            .background_executor()
            .spawn(async move { conn.count_table(&count_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if let Ok(total) = result {
                    entity.update(cx, |panel, cx| {
                        panel.pending.total_count = Some(PendingTotalCount {
                            source_qualified: qualified,
                            total,
                        });
                        cx.notify();
                    });
                }
            }) {
                log::warn!(
                    "Failed to apply table count result to UI state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    pub(super) fn apply_total_count(
        &mut self,
        source_qualified: String,
        total: u64,
        cx: &mut Context<Self>,
    ) {
        match &mut self.source {
            DataSource::Table {
                table, total_rows, ..
            } if table.qualified_name() == source_qualified => {
                *total_rows = Some(total);
                cx.notify();
            }
            DataSource::Collection {
                collection,
                total_docs,
                ..
            } if collection.qualified_name() == source_qualified => {
                *total_docs = Some(total);
                cx.notify();
            }
            _ => {}
        }
    }

    pub(super) fn fetch_collection_count(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        filter: Option<serde_json::Value>,
        cx: &mut Context<Self>,
    ) {
        let conn = {
            let state = self.app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .map(|c| c.connection.clone())
        };

        let Some(conn) = conn else {
            return;
        };

        let mut count_request = CollectionCountRequest::new(collection.clone());
        if let Some(f) = filter {
            count_request = count_request.with_filter(f);
        }

        let entity = cx.entity().clone();
        let qualified = collection.qualified_name();

        let task = cx
            .background_executor()
            .spawn(async move { conn.count_collection(&count_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if let Ok(total) = result {
                    entity.update(cx, |panel, cx| {
                        panel.pending.total_count = Some(PendingTotalCount {
                            source_qualified: qualified,
                            total,
                        });
                        cx.notify();
                    });
                }
            }) {
                log::warn!(
                    "Failed to apply collection count result to UI state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    /// Attempt relational lowering for the given filter text.
    ///
    /// Returns `true` if lowering succeeded and query execution was dispatched;
    /// returns `false` on any gate failure or parse error, signalling the caller
    /// to fall through to the raw-filter path.
    ///
    /// Gate conditions (FR-GATE-1):
    /// 1. `metadata.query_language == Sql`
    /// 2. `data_source == Table`
    /// 3. `fk_cache` is `Ready` with at least one FK
    #[allow(clippy::too_many_arguments)]
    fn try_relational_filter(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table: TableRef,
        _pagination: Pagination,
        _order_by: Vec<OrderByColumn>,
        filter_text: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // Gate 1: SQL driver only (FR-GATE-5 — no driver-id branching)
        let is_sql = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.metadata().query_language == dbflux_core::QueryLanguage::Sql)
            .unwrap_or(false);

        if !is_sql {
            return false;
        }

        // Gate 2: Table source (already guaranteed by run_table_query call path)
        // Gate 3: FK cache ready with at least one FK
        let fks = match &self.fk_cache {
            super::FkLoadState::Ready(fks) if !fks.is_empty() => fks.clone(),
            super::FkLoadState::Loading => {
                // FK fetch in flight — show Resolving state, fall through to raw
                self.relational_filter_state = RelationalFilterState::Resolving;
                cx.notify();
                return false;
            }
            _ => {
                // Unavailable or empty — silently fall through (FR-ERR-6)
                return false;
            }
        };

        // Build source descriptor for the resolver
        let source = SourceTable {
            schema: table.schema.clone(),
            table: table.name.clone(),
            alias: table.name.clone(),
        };

        // Resolve using the driver's dialect for identifier case-folding
        let resolve_result = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                return false;
            };
            let dialect = connected.connection.dialect();
            parse_and_resolve(&filter_text, source, &fks, dialect)
        };

        match resolve_result {
            Ok(lowering) => {
                let join_count = lowering.diagnostics.join_count;
                let predicate_count = lowering.diagnostics.relational_predicate_count;

                self.apply_builder_draft_spec(lowering.spec.clone(), cx);

                self.relational_filter_state = RelationalFilterState::Active {
                    join_count,
                    predicate_count,
                };
                cx.notify();

                // Execute via the visual query path
                let profile_id_for_run = profile_id;
                let db_for_run = database.clone();
                if let Some(select) = self.visual_select.clone() {
                    self.run_visual_query(
                        profile_id_for_run,
                        db_for_run.clone(),
                        select.clone(),
                        window,
                        cx,
                    );

                    if let Some(spec) = &self.builder_draft_spec {
                        if spec.is_grouped() {
                            self.fetch_grouped_total_count(profile_id, database, spec.clone(), cx);
                        } else {
                            self.fetch_relational_count(profile_id, database, spec.clone(), cx);
                        }
                    }
                }

                true
            }

            Err(RelationalFilterError::Parse(_)) => {
                // FR-PARSE-7: parse errors silently fall back to raw filter
                if !matches!(
                    self.relational_filter_state,
                    RelationalFilterState::Inactive
                ) {
                    self.relational_filter_state = RelationalFilterState::Inactive;
                    cx.notify();
                }
                false
            }

            Err(RelationalFilterError::Resolve(boxed_err)) => {
                // FR-ERR-1 / FR-ERR-2: resolve errors surface inline
                let (message, partial_spec) = match *boxed_err {
                    RelationalResolveError::Ambiguous {
                        segment,
                        from_table,
                        partial_spec,
                        ..
                    } => (
                        format!(
                            "Ambiguous relation `{}` from `{}`. Multiple FKs match — open in builder to resolve.",
                            segment, from_table
                        ),
                        partial_spec,
                    ),
                    RelationalResolveError::Unknown {
                        segment,
                        from_table,
                        partial_spec,
                        ..
                    } => (
                        format!(
                            "Unknown relation `{}` from `{}`. Open in builder to select a join manually.",
                            segment, from_table
                        ),
                        partial_spec,
                    ),
                };

                self.relational_filter_state = RelationalFilterState::Error {
                    message,
                    partial_spec: Box::new(partial_spec),
                };
                cx.notify();

                // Do NOT execute a query for the error state — let the user act
                false
            }
        }
    }

    /// Execute the grouped total-count subquery for a grouped visual query.
    ///
    /// When the visual spec is grouped, a plain `COUNT(*) FROM table` would
    /// count source rows rather than the number of groups. This method wraps
    /// the full grouped query (without LIMIT/OFFSET) in a
    /// `SELECT COUNT(*) FROM (...) AS _dbflux_count_subq` to get the correct
    /// group count for the pagination footer.
    pub(super) fn fetch_grouped_total_count(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        spec: VisualQuerySpec,
        cx: &mut Context<Self>,
    ) {
        let (conn, count_query) = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                return;
            };

            let conn = match connected.resolve_connection_for_execution(database.as_deref()) {
                Ok(c) => c,
                Err(_) => return,
            };

            let dialect = connected.connection.dialect();
            let count_query = match dbflux_core::build_count_of_grouped_query(&spec, dialect) {
                Ok(q) => q,
                Err(e) => {
                    log::warn!("Failed to build grouped count query: {}", e);
                    return;
                }
            };

            (conn, count_query)
        };

        let table_name = spec.source.table.clone();
        let entity = cx.entity().clone();

        let mut request = dbflux_core::QueryRequest::new(count_query.sql.clone());
        request.params = count_query.params.clone();
        if let Some(ref db) = database {
            request.database = Some(db.clone());
        }

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(e) = cx.update(|cx| {
                if let Ok(query_result) = result
                    && let Some(row) = query_result.rows.first()
                    && let Some(first_value) = row.first()
                {
                    let count_opt: Option<u64> = match first_value {
                        dbflux_core::Value::Int(n) => Some(*n as u64),
                        dbflux_core::Value::Float(f) => Some(*f as u64),
                        dbflux_core::Value::Decimal(s) => s.parse::<u64>().ok(),
                        dbflux_core::Value::Text(s) => s.parse::<u64>().ok(),
                        _ => None,
                    };
                    if let Some(total) = count_opt {
                        entity.update(cx, |panel, cx| {
                            panel.pending.total_count = Some(PendingTotalCount {
                                source_qualified: table_name,
                                total,
                            });
                            cx.notify();
                        });
                    }
                }
            }) {
                log::warn!("Failed to apply grouped count result: {:?}", e);
            }
        })
        .detach();
    }

    /// Execute the count subquery for an active relational filter.
    ///
    /// Uses `SELECT COUNT(*) FROM (<inner SELECT>) AS dbflux_count_subq` instead
    /// of `TableCountRequest`, satisfying FR-COUNT-1 / FR-COUNT-2.
    fn fetch_relational_count(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        spec: dbflux_core::VisualQuerySpec,
        cx: &mut Context<Self>,
    ) {
        let (conn, count_query) = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                return;
            };

            let conn = match connected.resolve_connection_for_execution(database.as_deref()) {
                Ok(c) => c,
                Err(_) => return,
            };

            let dialect = connected.connection.dialect();
            let count_query = count_query_from_spec(&spec, dialect);

            (conn, count_query)
        };

        let table_name = spec.source.table.clone();
        let entity = cx.entity().clone();

        let mut request = dbflux_core::QueryRequest::new(count_query.sql.clone());
        request.params = count_query.params.clone();
        if let Some(ref db) = database {
            request.database = Some(db.clone());
        }

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(e) = cx.update(|cx| {
                if let Ok(query_result) = result
                    && let Some(row) = query_result.rows.first()
                    && let Some(dbflux_core::Value::Int(count)) = row.first()
                {
                    entity.update(cx, |panel, cx| {
                        panel.pending.total_count = Some(PendingTotalCount {
                            source_qualified: table_name,
                            total: *count as u64,
                        });
                        cx.notify();
                    });
                }
            }) {
                log::warn!("Failed to apply relational count result: {:?}", e);
            }
        })
        .detach();
    }
}
