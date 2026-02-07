use super::{DataGridPanel, DataSource, GridState, PendingToast, PendingTotalCount, RunningQuery};
use crate::ui::components::data_table::SortState as TableSortState;
use crate::ui::toast::ToastExt;
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, OrderByColumn, Pagination,
    QueryResult, TableBrowseRequest, TableCountRequest, TableRef, TaskKind,
};
use gpui::*;
use log::info;
use uuid::Uuid;

impl DataGridPanel {
    /// Refresh data from source.
    pub fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::Table {
                profile_id,
                table,
                pagination,
                order_by,
                total_rows,
            } => {
                self.run_table_query(
                    *profile_id,
                    table.clone(),
                    pagination.clone(),
                    order_by.clone(),
                    *total_rows,
                    window,
                    cx,
                );
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
        table: TableRef,
        pagination: Pagination,
        order_by: Vec<OrderByColumn>,
        total_rows: Option<u64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.running_query.is_some() {
            cx.toast_error("A query is already running", window);
            return;
        }

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
                cx.toast_warning("Limit must be greater than 0", window);
                pagination
            }
            Ok(limit) if limit != pagination.limit() => pagination.with_limit(limit).reset_offset(),
            Ok(_) => pagination,
            Err(_) if !limit_str.is_empty() => {
                cx.toast_warning("Invalid limit value", window);
                pagination
            }
            Err(_) => pagination,
        };

        let mut request = TableBrowseRequest::new(table.clone())
            .with_pagination(pagination.clone())
            .with_order_by(order_by.clone());

        if let Some(ref f) = filter {
            request = request.with_filter(f.clone());
        }

        let (conn, active_database) = {
            let state = self.app_state.read(cx);
            match state.connections().get(&profile_id) {
                Some(c) => (Some(c.connection.clone()), c.active_database.clone()),
                None => {
                    cx.toast_error("Connection not found", window);
                    return;
                }
            }
        };

        let Some(conn) = conn else {
            cx.toast_error("Connection not available", window);
            return;
        };

        // Use the database from the active connection if the table doesn't have a schema set
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

        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            state.start_task(
                TaskKind::Query,
                format!("SELECT * FROM {}", table.qualified_name()),
            )
        });

        self.running_query = Some(RunningQuery {
            task_id,
            cancel_token: cancel_token.clone(),
        });
        self.state = GridState::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();
        let conn_for_cleanup = conn.clone();

        let table_for_spawn = table.clone();
        let pagination_for_spawn = pagination.clone();
        let order_by_for_spawn = order_by.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.browse_table(&browse_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |panel, _cx| {
                    panel.running_query = None;
                });

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

                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });

                        entity.update(cx, |panel, cx| {
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

                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.to_string());
                        });

                        entity.update(cx, |panel, cx| {
                            panel.state = GridState::Error;
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Query failed: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }
            })
            .ok();
        })
        .detach();

        // Fetch total count if not known
        if total_rows.is_none() {
            self.fetch_total_count(profile_id, table, filter, cx);
        }
    }

    pub(super) fn run_collection_query(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        pagination: Pagination,
        total_docs: Option<u64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.running_query.is_some() {
            cx.toast_error("A query is already running", window);
            return;
        }

        let limit_value = self.limit_input.read(cx).value();
        let limit_str = limit_value.trim();
        let pagination = match limit_str.parse::<u32>() {
            Ok(0) => {
                cx.toast_warning("Limit must be greater than 0", window);
                pagination
            }
            Ok(limit) if limit != pagination.limit() => pagination.with_limit(limit).reset_offset(),
            Ok(_) => pagination,
            Err(_) if !limit_str.is_empty() => {
                cx.toast_warning("Invalid limit value", window);
                pagination
            }
            Err(_) => pagination,
        };

        let conn = {
            let state = self.app_state.read(cx);
            match state.connections().get(&profile_id) {
                Some(c) => Some(c.connection.clone()),
                None => {
                    cx.toast_error("Connection not found", window);
                    return;
                }
            }
        };

        let Some(conn) = conn else {
            cx.toast_error("Connection not available", window);
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
                    cx.toast_error(format!("Invalid JSON filter: {}", e), window);
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

        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            state.start_task(
                TaskKind::Query,
                format!("find {}.{}", collection.database, collection.name),
            )
        });

        self.running_query = Some(RunningQuery {
            task_id,
            cancel_token: cancel_token.clone(),
        });
        self.state = GridState::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();
        let conn_for_cleanup = conn.clone();
        let collection_for_spawn = collection.clone();
        let pagination_for_spawn = pagination.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.browse_collection(&browse_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |panel, _cx| {
                    panel.running_query = None;
                });

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

                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });

                        entity.update(cx, |panel, cx| {
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

                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.to_string());
                        });

                        entity.update(cx, |panel, cx| {
                            panel.state = GridState::Error;
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Query failed: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }
            })
            .ok();
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
            let pos = result.columns.iter().position(|c| c.name == col.name);
            pos.map(|column_ix| TableSortState::new(column_ix, col.direction))
        });

        // Preserve existing total_rows if not provided
        let existing_total = match &self.source {
            DataSource::Table { total_rows, .. } => *total_rows,
            _ => None,
        };

        self.source = DataSource::Table {
            profile_id,
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
        table: TableRef,
        filter: Option<String>,
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

            cx.update(|cx| {
                if let Ok(total) = result {
                    entity.update(cx, |panel, cx| {
                        panel.pending_total_count = Some(PendingTotalCount {
                            source_qualified: qualified,
                            total,
                        });
                        cx.notify();
                    });
                }
            })
            .ok();
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

            cx.update(|cx| {
                if let Ok(total) = result {
                    entity.update(cx, |panel, cx| {
                        panel.pending_total_count = Some(PendingTotalCount {
                            source_qualified: qualified,
                            total,
                        });
                        cx.notify();
                    });
                }
            })
            .ok();
        })
        .detach();
    }
}
