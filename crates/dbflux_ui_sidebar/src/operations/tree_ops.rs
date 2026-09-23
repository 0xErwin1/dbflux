use super::{
    HeldDatabaseConnection, retain_database_cache_entries, try_close_held_database_connection,
};
use crate::*;
use dbflux_core::{
    CancelToken, Connection, DbSchemaInfo, FetchTableDetailsParams, FetchTableDetailsResult,
    TaskKind, TaskTarget,
};
use std::sync::Arc;

struct HeldSidebarDatabaseRefreshState {
    database: String,
    refresh_guard: dbflux_core::DatabaseRefreshGuard,
    primary_schema: Option<SchemaSnapshot>,
    cached_schema: Option<DbSchemaInfo>,
    table_details: HashMap<(String, Option<String>, String), TableInfo>,
    dependents_cache: HashMap<(String, Option<String>, String), Vec<dbflux_core::RelationRef>>,
    schema_types: HashMap<SchemaCacheKey, Vec<CustomTypeInfo>>,
    schema_indexes: HashMap<SchemaCacheKey, Vec<SchemaIndexInfo>>,
    schema_foreign_keys: HashMap<SchemaCacheKey, Vec<SchemaForeignKeyInfo>>,
    subtree_expansion_overrides: HashMap<String, bool>,
    held_connection: Option<HeldDatabaseConnection>,
}

enum DatabaseRefreshMode {
    LazyPerDatabase,
    ConnectionPerDatabaseCurrent,
    ConnectionPerDatabaseSecondary,
}

enum DatabaseRefreshExecutionOutcome {
    Refreshed {
        schema: Option<SchemaSnapshot>,
        database_schema: Option<DbSchemaInfo>,
    },
    RefreshedSecondary {
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    },
    Failed {
        error: String,
        held_state: HeldSidebarDatabaseRefreshState,
    },
    Cancelled {
        held_state: HeldSidebarDatabaseRefreshState,
    },
}

enum SchemaObjectRefreshResult {
    TableDetails(Box<FetchTableDetailsResult>),
    Views {
        profile_id: Uuid,
        database: String,
        schema_name: String,
        views: Vec<ViewInfo>,
    },
}

struct HeldSidebarObjectRefreshState {
    profile_id: Uuid,
    cache_database: String,
    schema_name: String,
    object_name: String,
    previous_details: Option<TableInfo>,
}

impl Sidebar {
    pub(crate) fn handle_database_click(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        let strategy = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.schema_loading_strategy());

        match strategy {
            Some(SchemaLoadingStrategy::LazyPerDatabase) => {
                self.handle_lazy_database_click(profile_id, &db_name, cx);
            }
            Some(SchemaLoadingStrategy::ConnectionPerDatabase) => {
                self.handle_connection_per_database_click(profile_id, &db_name, cx);
            }
            Some(SchemaLoadingStrategy::SingleDatabase) | None => {
                log::info!("Database click not applicable for this database type");
            }
        }
    }

    pub(crate) fn close_database(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Some(conn) = state.connections_mut().get_mut(&profile_id) {
                conn.database_schemas.remove(&db_name);

                if conn.active_database.as_deref() == Some(db_name.as_str()) {
                    conn.active_database = None;
                }
            }

            // Slot removal goes through the manager seam so the per-target
            // slot revision advances; ownership transfers here so the
            // connection is cancelled and dropped off the UI thread.
            if let Some(db_conn) = state.take_database_connection(profile_id, &db_name) {
                std::thread::spawn(move || {
                    if let Err(error) = db_conn.connection.cancel_active() {
                        log::warn!("Failed to cancel active query while closing database: {error}");
                    }
                    drop(db_conn);
                });
            }
            cx.emit(AppStateChanged);
        });

        // Collapse the database node in the tree
        self.set_expanded(item_id, false, cx);

        self.refresh_tree(cx);
    }

    fn database_root_expanded(&self, item_id: &str, cx: &Context<Self>) -> bool {
        fn find_expanded(items: &[TreeItem], item_id: &str) -> Option<bool> {
            for item in items {
                if item.id.as_ref() == item_id {
                    return Some(item.is_expanded());
                }

                if let Some(expanded) = find_expanded(&item.children, item_id) {
                    return Some(expanded);
                }
            }

            None
        }

        let items = self.build_tree_items_with_overrides(cx);
        find_expanded(&items, item_id).unwrap_or(false)
    }

    fn take_database_refresh_state(
        &mut self,
        profile_id: Uuid,
        database: &str,
        item_id: &str,
        cx: &mut Context<Self>,
    ) -> Result<HeldSidebarDatabaseRefreshState, String> {
        let mut descendant_ids = Vec::new();
        let items = self.build_tree_items_with_overrides(cx);
        let _ = Self::collect_subtree_item_ids(&items, item_id, &mut descendant_ids);

        let subtree_expansion_overrides = descendant_ids
            .iter()
            .filter_map(|descendant_id| {
                self.expansion_overrides
                    .get(descendant_id)
                    .copied()
                    .map(|expanded| (descendant_id.clone(), expanded))
            })
            .collect();

        let held_state = self.app_state.update(cx, |state, _cx| {
            if state.is_operation_pending(profile_id, Some(database)) {
                return Err("Database refresh already pending".to_string());
            }
            let cached_schema = state.invalidate_database_schema_target(profile_id, database);
            let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
                return Err("Profile not connected".to_string());
            };

            let table_details = {
                let existing = std::mem::take(&mut connected.table_details);
                let (removed, kept): (Vec<_>, Vec<_>) = existing
                    .into_iter()
                    .partition(|((cache_db, _, _), _)| cache_db == database);
                connected.table_details = kept.into_iter().collect();
                removed.into_iter().collect()
            };

            let dependents_cache = {
                let existing = std::mem::take(&mut connected.dependents_cache);
                let (removed, kept): (Vec<_>, Vec<_>) = existing
                    .into_iter()
                    .partition(|((cache_db, _, _), _)| cache_db == database);
                connected.dependents_cache = kept.into_iter().collect();
                removed.into_iter().collect()
            };

            let schema_types = retain_database_cache_entries(&mut connected.schema_types, database);
            let schema_indexes =
                retain_database_cache_entries(&mut connected.schema_indexes, database);
            let schema_foreign_keys =
                retain_database_cache_entries(&mut connected.schema_foreign_keys, database);

            let previous_active_database = connected.active_database.clone();
            let slot_present = connected.database_connections.contains_key(database);

            let primary_schema = if !slot_present
                && connected
                    .schema
                    .as_ref()
                    .and_then(|schema| schema.current_database())
                    .is_some_and(|current| current == database)
            {
                connected.schema.take()
            } else {
                None
            };

            // Slot take goes through the manager seam so the per-target slot
            // revision advances; ownership of the entry transfers into the
            // held refresh state.
            let held_connection =
                state
                    .take_database_connection(profile_id, database)
                    .map(|connection| HeldDatabaseConnection {
                        database: database.to_string(),
                        connection,
                        cached_schema: None,
                        previous_active_database: previous_active_database.clone(),
                    });

            let refresh_guard = state
                .capture_database_refresh_guard(profile_id, database)
                .ok_or_else(|| "Database refresh target changed".to_string())?;

            Ok(HeldSidebarDatabaseRefreshState {
                database: database.to_string(),
                refresh_guard,
                primary_schema,
                cached_schema,
                table_details,
                dependents_cache,
                schema_types,
                schema_indexes,
                schema_foreign_keys,
                subtree_expansion_overrides,
                held_connection,
            })
        })?;

        for descendant_id in descendant_ids {
            self.expansion_overrides.remove(&descendant_id);
        }

        Ok(held_state)
    }

    fn restore_database_refresh_state(
        state: &mut AppStateEntity,
        profile_id: Uuid,
        held_state: HeldSidebarDatabaseRefreshState,
    ) {
        if !state.database_refresh_guard_is_current(&held_state.refresh_guard) {
            return;
        }
        let HeldSidebarDatabaseRefreshState {
            database,
            refresh_guard: _,
            primary_schema,
            cached_schema,
            table_details,
            dependents_cache,
            schema_types,
            schema_indexes,
            schema_foreign_keys,
            subtree_expansion_overrides: _,
            held_connection,
        } = held_state;

        let had_held_connection = held_connection.is_some();
        let mut cached_schema = cached_schema;

        if let Some(held_connection) = held_connection {
            // DnD rollback intentionally restores active context; refresh
            // rollback must not undo a newer database selection.
            let restored = state.restore_database_connection(
                profile_id,
                database.clone(),
                held_connection.connection,
            );
            if restored
                && let Some(schema) = cached_schema.take()
                && let Some(connected) = state.connections_mut().get_mut(&profile_id)
            {
                connected
                    .database_schemas
                    .entry(database.clone())
                    .or_insert(schema);
            }
        }

        let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
            log::warn!(
                "Failed to restore sidebar refresh state for profile {}: profile missing",
                profile_id
            );
            return;
        };

        if !had_held_connection {
            if let Some(primary_schema) = primary_schema {
                // A shared fetch may have filled this target while the
                // refresh was pending. Never replace its newer snapshot.
                if connected.schema.is_none() && !connected.database_schemas.contains_key(&database)
                {
                    connected.schema = Some(primary_schema);
                }
            }

            if let Some(cached_schema) = cached_schema {
                connected
                    .database_schemas
                    .entry(database.clone())
                    .or_insert(cached_schema);
            }
        }

        for (key, value) in table_details {
            connected.table_details.entry(key).or_insert(value);
        }
        for (key, value) in dependents_cache {
            connected.dependents_cache.entry(key).or_insert(value);
        }
        for (key, value) in schema_types {
            connected.schema_types.entry(key).or_insert(value);
        }
        for (key, value) in schema_indexes {
            connected.schema_indexes.entry(key).or_insert(value);
        }
        for (key, value) in schema_foreign_keys {
            connected.schema_foreign_keys.entry(key).or_insert(value);
        }
    }

    fn resolve_database_refresh_mode(
        &self,
        profile_id: Uuid,
        database: &str,
        cx: &App,
    ) -> Option<DatabaseRefreshMode> {
        let connected = self.app_state.read(cx).connections().get(&profile_id)?;

        match connected.connection.schema_loading_strategy() {
            SchemaLoadingStrategy::LazyPerDatabase => Some(DatabaseRefreshMode::LazyPerDatabase),
            SchemaLoadingStrategy::ConnectionPerDatabase => {
                if connected.database_connections.contains_key(database) {
                    Some(DatabaseRefreshMode::ConnectionPerDatabaseSecondary)
                } else if connected
                    .schema
                    .as_ref()
                    .and_then(|schema| schema.current_database())
                    .is_some_and(|current| current == database)
                {
                    Some(DatabaseRefreshMode::ConnectionPerDatabaseCurrent)
                } else {
                    None
                }
            }
            SchemaLoadingStrategy::SingleDatabase => None,
        }
    }

    fn start_database_refresh_task(
        &mut self,
        profile_id: Uuid,
        database: &str,
        item_id: &str,
        root_expanded: bool,
        cx: &mut Context<Self>,
    ) -> Option<(TaskId, CancelToken)> {
        let started = self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(database)) {
                return false;
            }

            let started = state.start_pending_operation(profile_id, Some(database));
            if started {
                cx.emit(AppStateChanged);
            }
            started
        });

        if !started {
            return None;
        }

        self.expansion_overrides
            .insert(item_id.to_string(), root_expanded);
        self.loading_items.insert(item_id.to_string());

        let task_target = TaskTarget {
            profile_id,
            database: Some(database.to_string()),
        };

        Some(self.app_state.update(cx, |state, cx| {
            let task = state.start_task_for_target(
                TaskKind::SchemaRefresh,
                crate::labels::refreshing_database_task_label(database),
                Some(task_target),
            );
            cx.emit(AppStateChanged);
            task
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_database_refresh_outcome(
        sidebar: &mut Sidebar,
        app_state: &Entity<AppStateEntity>,
        item_id: &str,
        profile_id: Uuid,
        database: &str,
        root_expanded: bool,
        task_id: TaskId,
        refresh_guard: &dbflux_core::DatabaseRefreshGuard,
        outcome: DatabaseRefreshExecutionOutcome,
        cx: &mut Context<Self>,
    ) {
        sidebar.loading_items.remove(item_id);

        if !app_state
            .read(cx)
            .database_refresh_guard_is_current(refresh_guard)
        {
            app_state.update(cx, |state, cx| {
                state.tasks_mut().cancel(task_id);
                // The pending marker is unique per profile/target in one
                // session: until this operation releases it, a second
                // same-session refresh cannot acquire it. A replacement
                // session may own the same key and must remain untouched.
                if state.database_refresh_guard_is_same_session(refresh_guard) {
                    state.finish_pending_operation(profile_id, Some(database));
                }
                cx.emit(AppStateChanged);
            });
            sidebar.refresh_tree(cx);
            return;
        }

        match outcome {
            DatabaseRefreshExecutionOutcome::Refreshed {
                schema,
                database_schema,
            } => {
                app_state.update(cx, |state, cx| {
                    state.complete_task(task_id);
                    state.finish_pending_operation(profile_id, Some(database));

                    if let Some(database_schema) = database_schema {
                        state.set_database_schema(
                            profile_id,
                            database.to_string(),
                            database_schema,
                        );
                    }

                    if let Some(schema) = schema
                        && let Some(connected) = state.connections_mut().get_mut(&profile_id)
                    {
                        if let Some(database_connection) =
                            connected.database_connections.get_mut(database)
                        {
                            database_connection.schema = Some(schema);
                        } else {
                            connected.schema = Some(schema);
                        }
                    }

                    cx.emit(AppStateChanged);
                });

                sidebar
                    .expansion_overrides
                    .insert(item_id.to_string(), root_expanded);
            }
            DatabaseRefreshExecutionOutcome::RefreshedSecondary { connection, schema } => {
                app_state.update(cx, |state, cx| {
                    state.add_database_connection(
                        profile_id,
                        database.to_string(),
                        connection,
                        schema,
                    );
                    state.complete_task(task_id);
                    state.finish_pending_operation(profile_id, Some(database));
                    cx.emit(AppStateChanged);
                });
                sidebar
                    .expansion_overrides
                    .insert(item_id.to_string(), root_expanded);
            }
            DatabaseRefreshExecutionOutcome::Failed { error, held_state } => {
                let subtree_overrides = held_state.subtree_expansion_overrides.clone();

                app_state.update(cx, |state, cx| {
                    state.fail_task(task_id, error.clone());
                    state.finish_pending_operation(profile_id, Some(database));
                    Self::restore_database_refresh_state(state, profile_id, held_state);
                    cx.emit(AppStateChanged);
                });

                sidebar
                    .expansion_overrides
                    .insert(item_id.to_string(), root_expanded);
                sidebar.expansion_overrides.extend(subtree_overrides);
                sidebar.pending_toast = Some(PendingToast {
                    message: crate::labels::refresh_database_failed_label(&error),
                    is_error: true,
                });
            }
            DatabaseRefreshExecutionOutcome::Cancelled { held_state } => {
                let subtree_overrides = held_state.subtree_expansion_overrides.clone();

                app_state.update(cx, |state, cx| {
                    state.tasks_mut().cancel(task_id);
                    state.finish_pending_operation(profile_id, Some(database));
                    Self::restore_database_refresh_state(state, profile_id, held_state);
                    cx.emit(AppStateChanged);
                });

                sidebar
                    .expansion_overrides
                    .insert(item_id.to_string(), root_expanded);
                sidebar.expansion_overrides.extend(subtree_overrides);
            }
        }

        sidebar.refresh_tree(cx);
    }

    fn refresh_lazy_database(
        &mut self,
        item_id: &str,
        profile_id: Uuid,
        database: String,
        root_expanded: bool,
        held_state: HeldSidebarDatabaseRefreshState,
        cx: &mut Context<Self>,
    ) {
        let params = match self.app_state.update(cx, |state, _cx| {
            state.prepare_fetch_database_schema(profile_id, &database)
        }) {
            Ok(params) => params,
            Err(error) => {
                let subtree_overrides = held_state.subtree_expansion_overrides.clone();
                self.app_state.update(cx, |state, _cx| {
                    Self::restore_database_refresh_state(state, profile_id, held_state)
                });
                self.expansion_overrides
                    .insert(item_id.to_string(), root_expanded);
                self.expansion_overrides.extend(subtree_overrides);
                self.pending_toast = Some(PendingToast {
                    message: crate::labels::refresh_database_failed_label(&error),
                    is_error: true,
                });
                self.refresh_tree(cx);
                return;
            }
        };

        let Some((task_id, cancel_token)) =
            self.start_database_refresh_task(profile_id, &database, item_id, root_expanded, cx)
        else {
            let subtree_overrides = held_state.subtree_expansion_overrides.clone();
            self.app_state.update(cx, |state, _cx| {
                Self::restore_database_refresh_state(state, profile_id, held_state)
            });
            self.expansion_overrides
                .insert(item_id.to_string(), root_expanded);
            self.expansion_overrides.extend(subtree_overrides);
            self.pending_toast = Some(PendingToast {
                message: crate::labels::database_refresh_pending_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            return;
        };

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let item_id = item_id.to_string();

        let refresh_guard = held_state.refresh_guard.clone();
        let operation_task = cx.spawn(async move |_this, cx| {
            let outcome = match cx
                .background_executor()
                .spawn(async move { params.execute() })
                .await
            {
                Ok(_) if cancel_token.is_cancelled() => {
                    DatabaseRefreshExecutionOutcome::Cancelled { held_state }
                }
                Ok(result) => DatabaseRefreshExecutionOutcome::Refreshed {
                    schema: None,
                    database_schema: Some(result.schema),
                },
                Err(error) => DatabaseRefreshExecutionOutcome::Failed { error, held_state },
            };

            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.clear_tracked_operation_task(task_id);
                    Self::apply_database_refresh_outcome(
                        sidebar,
                        &app_state,
                        &item_id,
                        profile_id,
                        &database,
                        root_expanded,
                        task_id,
                        &refresh_guard,
                        outcome,
                        cx,
                    );
                });
            });
        });

        self.track_operation_task(task_id, operation_task);
    }

    fn refresh_secondary_database_connection(
        &mut self,
        item_id: &str,
        profile_id: Uuid,
        database: String,
        root_expanded: bool,
        held_state: HeldSidebarDatabaseRefreshState,
        cx: &mut Context<Self>,
    ) {
        let params = match self.app_state.update(cx, |state, _cx| {
            state.prepare_database_connection(profile_id, &database)
        }) {
            Ok(params) => params,
            Err(error) => {
                let subtree_overrides = held_state.subtree_expansion_overrides.clone();
                self.app_state.update(cx, |state, _cx| {
                    Self::restore_database_refresh_state(state, profile_id, held_state)
                });
                self.expansion_overrides
                    .insert(item_id.to_string(), root_expanded);
                self.expansion_overrides.extend(subtree_overrides);
                self.pending_toast = Some(PendingToast {
                    message: crate::labels::refresh_database_failed_label(&error),
                    is_error: true,
                });
                self.refresh_tree(cx);
                return;
            }
        };

        let Some((task_id, cancel_token)) =
            self.start_database_refresh_task(profile_id, &database, item_id, root_expanded, cx)
        else {
            let subtree_overrides = held_state.subtree_expansion_overrides.clone();
            self.app_state.update(cx, |state, _cx| {
                Self::restore_database_refresh_state(state, profile_id, held_state)
            });
            self.expansion_overrides
                .insert(item_id.to_string(), root_expanded);
            self.expansion_overrides.extend(subtree_overrides);
            self.pending_toast = Some(PendingToast {
                message: crate::labels::database_refresh_pending_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            return;
        };

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let item_id = item_id.to_string();

        let refresh_guard = held_state.refresh_guard.clone();
        let operation_task = cx.spawn(async move |_this, cx| {
            let outcome = cx
                .background_executor()
                .spawn(async move {
                    let mut held_state = held_state;

                    let Some(held_connection) = held_state.held_connection.as_mut() else {
                        return DatabaseRefreshExecutionOutcome::Failed {
                            error: format!(
                                "Database '{}' was not open, cannot refresh it as a per-database connection",
                                held_state.database
                            ),
                            held_state,
                        };
                    };

                    if let Err(error) = try_close_held_database_connection(held_connection) {
                        return DatabaseRefreshExecutionOutcome::Failed { error, held_state };
                    }

                    if cancel_token.is_cancelled() {
                        return DatabaseRefreshExecutionOutcome::Cancelled { held_state };
                    }

                    match params.execute() {
                        Ok(result) => DatabaseRefreshExecutionOutcome::RefreshedSecondary {
                            connection: result.connection,
                            schema: result.schema,
                        },
                        Err(error) => DatabaseRefreshExecutionOutcome::Failed { error, held_state },
                    }
                })
                .await;

            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.clear_tracked_operation_task(task_id);
                    Self::apply_database_refresh_outcome(
                        sidebar,
                        &app_state,
                        &item_id,
                        profile_id,
                        &database,
                        root_expanded,
                        task_id,
                        &refresh_guard,
                        outcome,
                        cx,
                    );
                });
            });
        });

        self.track_operation_task(task_id, operation_task);
    }

    fn refresh_current_database_connection(
        &mut self,
        item_id: &str,
        profile_id: Uuid,
        database: String,
        root_expanded: bool,
        held_state: HeldSidebarDatabaseRefreshState,
        cx: &mut Context<Self>,
    ) {
        let Some((task_id, cancel_token)) =
            self.start_database_refresh_task(profile_id, &database, item_id, root_expanded, cx)
        else {
            let subtree_overrides = held_state.subtree_expansion_overrides.clone();
            self.app_state.update(cx, |state, _cx| {
                Self::restore_database_refresh_state(state, profile_id, held_state)
            });
            self.expansion_overrides
                .insert(item_id.to_string(), root_expanded);
            self.expansion_overrides.extend(subtree_overrides);
            self.pending_toast = Some(PendingToast {
                message: crate::labels::database_refresh_pending_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            return;
        };

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let item_id = item_id.to_string();

        let refresh_guard = held_state.refresh_guard.clone();
        let operation_task = cx.spawn(async move |_this, cx| {
            let connection = match cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|connected| connected.connection.clone())
            }) {
                Some(connection) => connection,
                None => {
                    let outcome = DatabaseRefreshExecutionOutcome::Failed {
                        error: "Profile not connected".to_string(),
                        held_state,
                    };

                    cx.update(|cx| {
                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.clear_tracked_operation_task(task_id);
                            Self::apply_database_refresh_outcome(
                                sidebar,
                                &app_state,
                                &item_id,
                                profile_id,
                                &database,
                                root_expanded,
                                task_id,
                                &refresh_guard,
                                outcome,
                                cx,
                            );
                        });
                    });
                    return;
                }
            };

            let outcome = match cx
                .background_executor()
                .spawn(async move { connection.schema() })
                .await
            {
                Ok(_) if cancel_token.is_cancelled() => {
                    DatabaseRefreshExecutionOutcome::Cancelled { held_state }
                }
                Ok(schema) => DatabaseRefreshExecutionOutcome::Refreshed {
                    schema: Some(schema),
                    database_schema: None,
                },
                Err(error) => DatabaseRefreshExecutionOutcome::Failed {
                    error: error.to_string(),
                    held_state,
                },
            };

            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.clear_tracked_operation_task(task_id);
                    Self::apply_database_refresh_outcome(
                        sidebar,
                        &app_state,
                        &item_id,
                        profile_id,
                        &database,
                        root_expanded,
                        task_id,
                        &refresh_guard,
                        outcome,
                        cx,
                    );
                });
            });
        });

        self.track_operation_task(task_id, operation_task);
    }

    pub(crate) fn handle_lazy_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        self.app_state.update(cx, |state, cx| {
            let key = dbflux_ui_base::object_tree::ObjectTreeRequestKey::DatabaseSchema {
                profile_id,
                database: db_name.to_string(),
            };
            if matches!(
                state.object_tree_outcome(&key),
                Some(
                    dbflux_ui_base::object_tree::ObjectTreeOutcome::Failed(_)
                        | dbflux_ui_base::object_tree::ObjectTreeOutcome::Rejected(_)
                        | dbflux_ui_base::object_tree::ObjectTreeOutcome::Cancelled
                )
            ) {
                return;
            }
            state.object_tree_request(
                dbflux_ui_base::object_tree::ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: db_name.to_string(),
                },
                cx,
            );
        });
        self.rebuild_tree_with_overrides(cx);
    }

    fn handle_connection_per_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let already_connected = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|conn| {
                conn.database_connections.contains_key(db_name)
                    || conn
                        .schema
                        .as_ref()
                        .and_then(|schema| schema.current_database())
                        .is_some_and(|current| current == db_name)
            });

        if already_connected {
            self.app_state.update(cx, |state, cx| {
                if state.get_active_database(profile_id).as_deref() != Some(db_name) {
                    state.set_active_database(profile_id, Some(db_name.to_string()));
                    cx.emit(AppStateChanged);
                }
            });

            self.refresh_tree(cx);
            return;
        }

        let (params, guard) = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let params = state.prepare_database_connection_guarded(profile_id, db_name)?;
            let guard = state
                .capture_database_refresh_guard(profile_id, db_name)
                .ok_or_else(|| "Database connection target changed".to_string())?;
            if !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            Ok((params, guard))
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Database connection skipped: {}", e);
                return;
            }
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                if state.database_refresh_guard_is_current(&guard) {
                    state.finish_pending_operation(profile_id, Some(db_name));
                }
            });
            self.pending_toast = Some(PendingToast {
                message: crate::labels::background_task_limit_toast_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result = state.start_task(
                TaskKind::SwitchDatabase,
                crate::labels::connecting_to_database_task_label(db_name),
            );
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let db_name_owned = db_name.to_string();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if !app_state.read(cx).database_refresh_guard_is_current(&guard) {
                    app_state.update(cx, |state, cx| {
                        state.tasks_mut().cancel(task_id);
                        if state.database_refresh_guard_is_same_session(&guard) {
                            state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        }
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| sidebar.refresh_tree(cx));
                    return;
                }
                if cancel_token.is_cancelled() {
                    log::info!("Database connection task was cancelled, discarding result");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = app_state.update(cx, |state, cx| {
                    let toast = match result {
                        Ok(installed) => match state.apply_guarded_database_connection(installed) {
                            dbflux_core::InstallDatabaseConnectionOutcome::Installed => {
                                state.set_active_database(profile_id, Some(db_name_owned.clone()));
                                state.complete_task(task_id);
                                None
                            }
                            dbflux_core::InstallDatabaseConnectionOutcome::Rejected(_) => {
                                state.tasks_mut().cancel(task_id);
                                None
                            }
                        },
                        Err(error) => {
                            state.fail_task(task_id, error.clone());
                            Some(PendingToast {
                                message: crate::labels::connect_database_failed_label(&error),
                                is_error: true,
                            })
                        }
                    };
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));
                    cx.emit(AppStateChanged);
                    cx.notify();
                    toast
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            });
        })
        .detach();
    }

    pub(crate) fn refresh_schema_database(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        let Some(mode) = self.resolve_database_refresh_mode(profile_id, &db_name, cx) else {
            return;
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.pending_toast = Some(PendingToast {
                message: crate::labels::background_task_limit_toast_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let root_expanded = self.database_root_expanded(item_id, cx);
        let held_state = match self.take_database_refresh_state(profile_id, &db_name, item_id, cx) {
            Ok(held_state) => held_state,
            Err(error) => {
                self.pending_toast = Some(PendingToast {
                    message: crate::labels::refresh_database_failed_label(&error),
                    is_error: true,
                });
                self.refresh_tree(cx);
                cx.notify();
                return;
            }
        };

        match mode {
            DatabaseRefreshMode::LazyPerDatabase => self.refresh_lazy_database(
                item_id,
                profile_id,
                db_name,
                root_expanded,
                held_state,
                cx,
            ),
            DatabaseRefreshMode::ConnectionPerDatabaseSecondary => self
                .refresh_secondary_database_connection(
                    item_id,
                    profile_id,
                    db_name,
                    root_expanded,
                    held_state,
                    cx,
                ),
            DatabaseRefreshMode::ConnectionPerDatabaseCurrent => self
                .refresh_current_database_connection(
                    item_id,
                    profile_id,
                    db_name,
                    root_expanded,
                    held_state,
                    cx,
                ),
        }
    }

    pub(crate) fn refresh_schema_object(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(parts) = parse_node_id(item_id)
            .as_ref()
            .and_then(ItemIdParts::from_node_id)
        else {
            return;
        };

        if self.loading_items.contains(item_id) {
            return;
        }

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.pending_toast = Some(PendingToast {
                message: crate::labels::background_task_limit_toast_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let cache_db = parts.cache_database().to_string();
        let node_id = parse_node_id(item_id);
        let previous_details = self.app_state.update(cx, |state, _cx| {
            state
                .connections_mut()
                .get_mut(&parts.profile_id)
                .and_then(|connected| {
                    connected.table_details.remove(&(
                        cache_db.clone(),
                        Some(parts.schema_name.clone()),
                        parts.object_name.clone(),
                    ))
                })
        });

        let held_state = HeldSidebarObjectRefreshState {
            profile_id: parts.profile_id,
            cache_database: cache_db.clone(),
            schema_name: parts.schema_name.clone(),
            object_name: parts.object_name.clone(),
            previous_details,
        };

        let refresh_target = TaskTarget {
            profile_id: parts.profile_id,
            database: parts.database.clone().or_else(|| Some(cache_db.clone())),
        };

        enum RefreshObjectJob {
            Table(FetchTableDetailsParams),
            View(Arc<dyn Connection>),
        }

        let job = match node_id {
            Some(SchemaNodeId::View { .. }) => self
                .app_state
                .read(cx)
                .connections()
                .get(&parts.profile_id)
                .map(|connected| {
                    RefreshObjectJob::View(connected.connection_for_database(&cache_db))
                }),
            _ => self
                .app_state
                .update(cx, |state, _cx| {
                    state
                        .prepare_fetch_table_details(
                            parts.profile_id,
                            &cache_db,
                            Some(&parts.schema_name),
                            &parts.object_name,
                        )
                        .map(RefreshObjectJob::Table)
                })
                .ok(),
        };

        let Some(job) = job else {
            if let Some(previous_details) = held_state.previous_details.clone() {
                self.app_state.update(cx, |state, _cx| {
                    state.set_table_details(
                        held_state.profile_id,
                        held_state.cache_database.clone(),
                        Some(held_state.schema_name.clone()),
                        held_state.object_name.clone(),
                        previous_details,
                    );
                });
            }

            self.pending_toast = Some(PendingToast {
                message: crate::labels::prepare_schema_object_refresh_failed_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            return;
        };

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let task = state.start_task_for_target(
                TaskKind::SchemaRefresh,
                crate::labels::refreshing_schema_object_task_label(&parts.object_name),
                Some(refresh_target),
            );
            cx.emit(AppStateChanged);
            task
        });

        self.loading_items.insert(item_id.to_string());
        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let item_id = item_id.to_string();
        let schema_name = parts.schema_name.clone();
        let profile_id = parts.profile_id;

        let operation_task = cx.spawn(async move |_this, cx| {
            let result = match job {
                RefreshObjectJob::Table(params) => {
                    cx.background_executor()
                        .spawn(async move {
                            params
                                .execute()
                                .map(|r| SchemaObjectRefreshResult::TableDetails(Box::new(r)))
                                .map_err(|e| e.to_string())
                        })
                        .await
                }
                RefreshObjectJob::View(connection) => {
                    let cache_db = cache_db.clone();
                    let schema_name = schema_name.clone();
                    cx.background_executor()
                        .spawn(async move {
                            connection
                                .schema()
                                .map(|schema| {
                                    let views = schema
                                        .schemas()
                                        .iter()
                                        .find(|db_schema| db_schema.name == schema_name)
                                        .map(|db_schema| db_schema.views.clone())
                                        .unwrap_or_else(|| schema.views().to_vec());

                                    SchemaObjectRefreshResult::Views {
                                        profile_id,
                                        database: cache_db,
                                        schema_name,
                                        views,
                                    }
                                })
                                .map_err(|error| error.to_string())
                        })
                        .await
                }
            };

            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.clear_tracked_operation_task(task_id);
                    sidebar.loading_items.remove(&item_id);

                    if cancel_token.is_cancelled() {
                        app_state.update(cx, |state, cx| {
                            state.tasks_mut().cancel(task_id);
                            if let Some(previous_details) = held_state.previous_details.clone() {
                                state.set_table_details(
                                    held_state.profile_id,
                                    held_state.cache_database.clone(),
                                    Some(held_state.schema_name.clone()),
                                    held_state.object_name.clone(),
                                    previous_details,
                                );
                            }
                            cx.emit(AppStateChanged);
                        });
                        sidebar.refresh_tree(cx);
                        return;
                    }

                    match result {
                        Ok(SchemaObjectRefreshResult::TableDetails(result)) => {
                            app_state.update(cx, |state, cx| {
                                state.complete_task(task_id);
                                state.set_table_details(
                                    result.profile_id,
                                    result.database.clone(),
                                    result.schema.clone(),
                                    result.table.clone(),
                                    result.details,
                                );
                                state.set_dependents(
                                    result.profile_id,
                                    result.database,
                                    result.schema,
                                    result.table,
                                    result.dependents,
                                );
                                cx.emit(AppStateChanged);
                            });
                        }
                        Ok(SchemaObjectRefreshResult::Views {
                            profile_id,
                            database,
                            schema_name,
                            views,
                        }) => {
                            app_state.update(cx, |state, cx| {
                                state.complete_task(task_id);

                                if let Some(connected) =
                                    state.connections_mut().get_mut(&profile_id)
                                {
                                    if let Some(db_schema) =
                                        connected.database_schemas.get_mut(&database)
                                    {
                                        db_schema.views = views.clone();
                                    } else if let Some(db_connection) =
                                        connected.database_connections.get_mut(&database)
                                    {
                                        if let Some(schema) = db_connection.schema.as_mut()
                                            && let dbflux_core::DataStructure::Relational(
                                                relational,
                                            ) = &mut schema.structure
                                        {
                                            if let Some(target_schema) = relational
                                                .schemas
                                                .iter_mut()
                                                .find(|db_schema| db_schema.name == schema_name)
                                            {
                                                target_schema.views = views.clone();
                                            } else {
                                                relational.views = views.clone();
                                            }
                                        }
                                    } else if let Some(schema) = connected.schema.as_mut()
                                        && let dbflux_core::DataStructure::Relational(relational) =
                                            &mut schema.structure
                                    {
                                        if let Some(target_schema) = relational
                                            .schemas
                                            .iter_mut()
                                            .find(|db_schema| db_schema.name == schema_name)
                                        {
                                            target_schema.views = views.clone();
                                        } else {
                                            relational.views = views.clone();
                                        }
                                    }
                                }

                                cx.emit(AppStateChanged);
                            });
                        }
                        Err(error) => {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(task_id, error.clone());
                                if let Some(previous_details) = held_state.previous_details.clone()
                                {
                                    state.set_table_details(
                                        held_state.profile_id,
                                        held_state.cache_database.clone(),
                                        Some(held_state.schema_name.clone()),
                                        held_state.object_name.clone(),
                                        previous_details,
                                    );
                                }
                                cx.emit(AppStateChanged);
                            });

                            sidebar.pending_toast = Some(PendingToast {
                                message: crate::labels::refresh_schema_object_failed_label(&error),
                                is_error: true,
                            });
                        }
                    }

                    sidebar.refresh_tree(cx);
                });
            });
        });

        self.track_operation_task(task_id, operation_task);
    }
}
