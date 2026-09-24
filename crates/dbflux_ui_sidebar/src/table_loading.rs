use super::*;
use dbflux_core::{TaskKind, TaskTarget};
use dbflux_ui_base::object_tree::{
    ObjectTreeEvent, ObjectTreeOutcome, ObjectTreeRequestKey, ObjectTreeRequestStatus,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

const COLLECTION_CHILDREN_PAGE_SIZE: u32 = 50;

impl Sidebar {
    pub(super) fn reconcile_table_details_retry(
        &self,
        state: &dbflux_ui_base::app_state_entity::AppStateEntity,
    ) {
        self.table_details_retry
            .borrow_mut()
            .retain(|_, (key, owner, generation)| {
                if state.profile_session_generation(key.profile_id()) != Some(*generation) {
                    return false;
                }
                let Some(connected) = state.connections().get(&key.profile_id()) else {
                    return false;
                };
                let Some(original) = owner.upgrade() else {
                    return false;
                };
                if !std::sync::Arc::ptr_eq(&original, &connected.connection) {
                    return false;
                }
                let ObjectTreeRequestKey::TableDetails {
                    database,
                    schema,
                    table,
                    ..
                } = key
                else {
                    return false;
                };
                !connected
                    .table_details
                    .get(&(database.clone(), schema.clone(), table.clone()))
                    .is_some_and(|details| {
                        details.columns.is_some() || details.sample_fields.is_some()
                    })
            });
    }

    pub(super) fn find_table_for_item<'a>(
        parts: &ItemIdParts,
        schema: &'a Option<SchemaSnapshot>,
    ) -> Option<&'a TableInfo> {
        let schema = schema.as_ref()?;

        for db_schema in schema.schemas() {
            if db_schema.name == parts.schema_name {
                return db_schema
                    .tables
                    .iter()
                    .find(|t| t.name == parts.object_name);
            }
        }

        // For databases without schemas (fallback)
        schema.tables().iter().find(|t| t.name == parts.object_name)
    }

    pub(super) fn find_view_for_item<'a>(
        parts: &ItemIdParts,
        schema: &'a Option<SchemaSnapshot>,
    ) -> Option<&'a ViewInfo> {
        let schema = schema.as_ref()?;

        for db_schema in schema.schemas() {
            if db_schema.name == parts.schema_name {
                return db_schema.views.iter().find(|v| v.name == parts.object_name);
            }
        }

        // For databases without schemas (fallback)
        schema.views().iter().find(|v| v.name == parts.object_name)
    }

    fn retire_stale_table_refresh(
        &mut self,
        item_id: &str,
        current_generation: Option<u64>,
        cx: &mut Context<Self>,
    ) {
        let stale = self
            .table_details_requests
            .get(item_id)
            .is_some_and(|(_, captured)| Some(*captured) != current_generation);
        if !stale {
            return;
        }
        self.table_details_requests.remove(item_id);
        self.loading_items.remove(item_id);
        let original_action = self.table_details_actions.remove(item_id);
        if self.pending_actions.get(item_id) == original_action.as_ref() {
            self.pending_actions.remove(item_id);
        }
        if let Some((task_id, _)) = self.table_refresh_tasks.remove(item_id) {
            self.app_state.update(cx, |state, cx| {
                state.tasks_mut().cancel(task_id);
                cx.emit(AppStateChanged);
            });
        }
    }

    pub(super) fn refresh_table_details_via_coordinator(
        &mut self,
        item_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(parts) = parse_node_id(item_id)
            .as_ref()
            .and_then(ItemIdParts::from_node_id)
        else {
            return;
        };
        let generation = self
            .app_state
            .read(cx)
            .profile_session_generation(parts.profile_id);
        self.retire_stale_table_refresh(item_id, generation, cx);
        if self.loading_items.contains(item_id) {
            return;
        }
        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.pending_toast = Some(PendingToast {
                message: crate::labels::background_task_limit_toast_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            return;
        }

        let database = parts.cache_database().to_string();
        let key = ObjectTreeRequestKey::TableDetails {
            profile_id: parts.profile_id,
            database: database.clone(),
            schema: Some(parts.schema_name.clone()),
            table: parts.object_name.clone(),
        };
        let Some(generation) = generation else {
            return;
        };
        let previous_attempt_pending = self.app_state.read(cx).object_tree_is_pending(&key);
        if previous_attempt_pending {
            self.superseded_details_cancellations
                .insert((key.clone(), generation));
        }
        let status = self.app_state.update(cx, |state, cx| {
            if !state.invalidate_table_details(
                parts.profile_id,
                &database,
                Some(&parts.schema_name),
                &parts.object_name,
            ) {
                return None;
            }
            Some(state.object_tree_retry(key.clone(), cx))
        });
        if !previous_attempt_pending {
            self.superseded_details_cancellations
                .remove(&(key.clone(), generation));
        }
        match status {
            Some(ObjectTreeRequestStatus::Dispatched)
            | Some(ObjectTreeRequestStatus::Pending)
            | Some(ObjectTreeRequestStatus::WaitingForSlotInstall) => {
                let target = TaskTarget {
                    profile_id: parts.profile_id,
                    database: parts.database.clone().or(Some(database)),
                };
                let task = self.app_state.update(cx, |state, cx| {
                    let task = state.start_task_for_target(
                        TaskKind::SchemaRefresh,
                        crate::labels::refreshing_schema_object_task_label(&parts.object_name),
                        Some(target),
                    );
                    cx.emit(AppStateChanged);
                    task
                });
                self.table_refresh_tasks.insert(item_id.to_string(), task);
                self.table_details_requests
                    .insert(item_id.to_string(), (key, generation));
                self.loading_items.insert(item_id.to_string());
            }
            Some(ObjectTreeRequestStatus::Cached | ObjectTreeRequestStatus::Failed(_)) | None => {}
        }
        self.refresh_tree(cx);
    }

    /// Check if a table has detailed schema (columns/indexes) loaded.
    /// If not, spawns a background task to fetch them and returns `Loading`.
    pub(super) fn ensure_table_details(
        &mut self,
        item_id: &str,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> TableDetailsStatus {
        let Some(parts) = parse_node_id(item_id)
            .as_ref()
            .and_then(ItemIdParts::from_node_id)
        else {
            return TableDetailsStatus::NotFound;
        };

        let key = ObjectTreeRequestKey::TableDetails {
            profile_id: parts.profile_id,
            database: parts.cache_database().to_string(),
            schema: Some(parts.schema_name.clone()),
            table: parts.object_name.clone(),
        };
        let Some(generation) = self
            .app_state
            .read(cx)
            .profile_session_generation(parts.profile_id)
        else {
            return TableDetailsStatus::NotFound;
        };
        self.retire_stale_table_refresh(item_id, Some(generation), cx);
        if self
            .table_details_requests
            .get(item_id)
            .is_some_and(|(request, captured)| request == &key && *captured == generation)
        {
            if matches!(
                pending_action,
                PendingAction::ViewSchema { .. } | PendingAction::GenerateCode { .. }
            ) {
                self.table_details_actions
                    .insert(item_id.to_string(), pending_action.clone());
                self.pending_actions
                    .insert(item_id.to_string(), pending_action);
            }
            return TableDetailsStatus::Loading;
        }
        if self.table_details_requests.remove(item_id).is_some() {
            self.table_details_actions.remove(item_id);
            self.pending_actions.remove(item_id);
        }
        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&parts.profile_id) else {
            return TableDetailsStatus::NotFound;
        };

        let cache_db = parts.cache_database();
        let cache_key = (
            cache_db.to_string(),
            Some(parts.schema_name.clone()),
            parts.object_name.clone(),
        );

        if let Some(details) = conn.table_details.get(&cache_key)
            && (details.columns.is_some() || details.sample_fields.is_some())
        {
            return TableDetailsStatus::Ready;
        }

        let primary_matches = parts.database.is_none()
            || conn.schema.as_ref().is_some_and(|snapshot| {
                snapshot.current_database() == Some(cache_db)
                    || (snapshot.current_database().is_none()
                        && (cache_db.is_empty()
                            || conn.active_database.as_deref() == Some(cache_db)))
            });
        let cached_schema = conn.database_schemas.get(cache_db).or_else(|| {
            parts
                .database
                .is_none()
                .then(|| conn.database_schemas.get(&parts.schema_name))
                .flatten()
        });
        if let Some(db_schema) = cached_schema
            && let Some(table) = db_schema.tables.iter().find(|table| {
                table.name == parts.object_name
                    && table.schema.as_deref().unwrap_or(&parts.schema_name) == parts.schema_name
            })
            && (table.columns.is_some() || table.sample_fields.is_some())
        {
            return TableDetailsStatus::Ready;
        }

        let target_schema = parts
            .database
            .as_deref()
            .and_then(|db| conn.database_connections.get(db))
            .and_then(|dc| dc.schema.as_ref())
            .or_else(|| primary_matches.then_some(conn.schema.as_ref()).flatten());

        if let Some(schema) = target_schema {
            for db_schema in schema.schemas() {
                if db_schema.name == parts.schema_name
                    && let Some(table) = db_schema
                        .tables
                        .iter()
                        .find(|t| t.name == parts.object_name)
                    && (table.columns.is_some() || table.sample_fields.is_some())
                {
                    return TableDetailsStatus::Ready;
                }
            }
        }

        self.reconcile_table_details_retry(self.app_state.read(cx));
        if self
            .table_details_retry
            .borrow()
            .get(item_id)
            .is_some_and(|(retry_key, _, _)| retry_key == &key)
        {
            return TableDetailsStatus::NotFound;
        }
        let stale_pending = self
            .app_state
            .read(cx)
            .object_tree_pending_session_generation(&key)
            .flatten()
            .is_some_and(|pending_generation| pending_generation != generation);
        if stale_pending {
            self.superseded_details_cancellations
                .insert((key.clone(), generation));
        }
        let status = self.app_state.update(cx, |state, cx| {
            if stale_pending {
                // A replaced profile session invalidates this key for every consumer;
                // ordinary same-session requests must never cancel shared work.
                state.object_tree_retry(key.clone(), cx)
            } else {
                state.object_tree_request(key.clone(), cx)
            }
        });
        match status {
            ObjectTreeRequestStatus::Cached => TableDetailsStatus::Ready,
            ObjectTreeRequestStatus::Failed(_) => TableDetailsStatus::NotFound,
            _ => {
                self.table_details_actions
                    .insert(item_id.to_string(), pending_action.clone());
                self.pending_actions
                    .insert(item_id.to_string(), pending_action);
                self.table_details_requests
                    .insert(item_id.to_string(), (key, generation));
                self.rebuild_tree_with_overrides(cx);
                TableDetailsStatus::Loading
            }
        }
    }

    pub(super) fn ensure_collection_children(
        &mut self,
        profile_id: Uuid,
        database: &str,
        collection: &str,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> TableDetailsStatus {
        let item_id = pending_action.item_id().to_string();

        if self.loading_items.contains(&item_id) {
            return TableDetailsStatus::Loading;
        }

        let has_any_page = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|connection| {
                connection
                    .collection_children
                    .contains_key(&(database.to_string(), collection.to_string()))
            });

        if has_any_page {
            return TableDetailsStatus::Ready;
        }

        if self.spawn_fetch_collection_children(
            profile_id,
            database,
            collection,
            pending_action,
            cx,
        ) {
            TableDetailsStatus::Loading
        } else {
            TableDetailsStatus::NotFound
        }
    }

    pub(super) fn spawn_fetch_collection_children(
        &mut self,
        profile_id: Uuid,
        database: &str,
        collection: &str,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.loading_items.contains(pending_action.item_id()) {
            return true;
        }

        let params = match self.app_state.read(cx).prepare_fetch_collection_children(
            profile_id,
            database,
            collection,
            COLLECTION_CHILDREN_PAGE_SIZE,
        ) {
            Ok(params) => params,
            Err(error) => {
                if error != "Collection children already fully cached" {
                    log::warn!("Cannot fetch collection children: {}", error);
                    self.pending_toast = Some(PendingToast {
                        message: crate::labels::collection_load_failed_label(collection, &error),
                        is_error: true,
                    });
                    cx.notify();
                }

                return false;
            }
        };

        let database_name = database.to_string();
        let task_description = crate::labels::loading_event_streams_task_label(collection);
        let load_task_id = self.app_state.update(cx, |state, _| {
            let (task_id, _) = state.start_task_for_profile(
                TaskKind::LoadSchema,
                task_description,
                Some(profile_id),
            );
            task_id
        });

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        let collection_name = collection.to_string();

        self.spawn_fetch_with_result(
            pending_action,
            Some(load_task_id),
            task,
            "Failed to fetch collection children",
            move |error| crate::labels::collection_load_failed_label(&collection_name, error),
            |app_state, res, cx| {
                app_state.update(cx, |state, cx| {
                    state.set_collection_children_page(
                        res.profile_id,
                        res.database,
                        res.collection,
                        res.page,
                    );
                    cx.emit(AppStateChanged);
                });
            },
            move |app_state, cx| {
                app_state.update(cx, |state, state_cx| {
                    state.finish_pending_operation(profile_id, Some(&database_name));
                    state_cx.emit(AppStateChanged);
                });
            },
            cx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_fetch_with_result<R, F, G, M>(
        &mut self,
        pending_action: PendingAction,
        task_id: Option<TaskId>,
        task: Task<Result<R, String>>,
        error_log_prefix: &'static str,
        toast_message: M,
        on_success: F,
        on_finalize: G,
        cx: &mut Context<Self>,
    ) -> bool
    where
        R: Send + 'static,
        F: Fn(&Entity<dbflux_ui_base::app_state_entity::AppStateEntity>, R, &mut App)
            + Send
            + 'static,
        G: Fn(&Entity<dbflux_ui_base::app_state_entity::AppStateEntity>, &mut App) + Send + 'static,
        M: Fn(&str) -> String + Send + 'static,
    {
        let item_id = pending_action.item_id().to_string();
        self.pending_actions.insert(item_id.clone(), pending_action);
        self.loading_items.insert(item_id.clone());

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                match result {
                    Ok(res) => {
                        on_success(&app_state, res, cx);

                        if let Some(task_id) = task_id {
                            app_state.update(cx, |state, _| {
                                state.complete_task(task_id);
                            });
                        }

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.loading_items.remove(&item_id);
                            sidebar.complete_pending_action(&item_id, cx);
                        });
                    }
                    Err(e) => {
                        log::error!("{}: {}", error_log_prefix, e);

                        let message = toast_message(&e);

                        if let Some(task_id) = task_id {
                            app_state.update(cx, |state, _| {
                                state.fail_task_with_details(task_id, e.clone(), message.clone());
                            });
                        }

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.loading_items.remove(&item_id);
                            sidebar.pending_actions.remove(&item_id);
                            sidebar.expansion_overrides.remove(&item_id);
                            sidebar.pending_toast = Some(PendingToast {
                                message,
                                is_error: true,
                            });
                            sidebar.rebuild_tree_with_overrides(cx);
                        });
                    }
                }

                on_finalize(&app_state, cx);
            });
        })
        .detach();

        true
    }

    pub(super) fn settle_details_recovery_schema(
        &mut self,
        event: &ObjectTreeEvent,
        cx: &mut Context<Self>,
    ) {
        let ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database,
        } = &event.key
        else {
            return;
        };
        let items: Vec<_> = self
            .table_details_recovery
            .iter()
            .filter(|(_, (key, _))| {
                key.profile_id() == *profile_id
                    && key.node_key().database() == Some(database.as_str())
            })
            .map(|(item, (key, generation))| (item.clone(), key.clone(), *generation))
            .collect();
        for (item, key, generation) in items {
            self.table_details_recovery.remove(&item);
            if self
                .app_state
                .read(cx)
                .profile_session_generation(*profile_id)
                != Some(generation)
            {
                continue;
            }
            if !matches!(
                event.outcome,
                ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached
            ) {
                continue;
            }
            let (table_exists, authoritative) = {
                let state = self.app_state.read(cx);
                let listed = state.get_database_list(*profile_id);
                let listed_target = listed.is_none_or(|list| {
                    list.iter().any(|entry| &entry.name == database) || database.is_empty()
                });
                let refreshed = state
                    .connections()
                    .get(profile_id)
                    .and_then(|connected| connected.database_schemas.get(database));
                let authoritative = refreshed.is_some();
                let table_exists = listed_target
                    && refreshed.is_some_and(|db_schema| {
                        if let ObjectTreeRequestKey::TableDetails { schema, table, .. } = &key {
                            db_schema.tables.iter().any(|entry| {
                                entry.name == *table
                                    && entry.schema.as_deref().unwrap_or(database)
                                        == schema.as_deref().unwrap_or(database)
                            })
                        } else {
                            false
                        }
                    });
                (table_exists, authoritative)
            };
            if table_exists {
                self.recovered_table_databases
                    .insert((*profile_id, database.clone()), (item.clone(), generation));
                self.table_details_retry.borrow_mut().remove(&item);
                let cached = if let ObjectTreeRequestKey::TableDetails { schema, table, .. } = &key
                {
                    self.app_state
                        .read(cx)
                        .connections()
                        .get(profile_id)
                        .and_then(|connected| {
                            connected.table_details.get(&(
                                database.clone(),
                                schema.clone(),
                                table.clone(),
                            ))
                        })
                        .is_some_and(|details| {
                            details.columns.is_some() || details.sample_fields.is_some()
                        })
                } else {
                    false
                };
                if !cached {
                    self.app_state.update(cx, |state, cx| {
                        state.object_tree_retry(key, cx);
                    });
                }
            } else if authoritative {
                // A successful, cached schema without this typed table is not
                // a failed details request. The database remains refreshable.
                self.table_details_retry.borrow_mut().remove(&item);
                self.recovered_table_databases
                    .remove(&(*profile_id, database.clone()));
            }
        }
        self.rebuild_tree_with_overrides(cx);
    }

    pub(super) fn settle_table_details(&mut self, event: &ObjectTreeEvent, cx: &mut Context<Self>) {
        let generation = self
            .app_state
            .read(cx)
            .profile_session_generation(event.key.profile_id());
        if matches!(event.outcome, ObjectTreeOutcome::Cancelled)
            && generation.is_some_and(|generation| {
                self.superseded_details_cancellations
                    .remove(&(event.key.clone(), generation))
            })
        {
            return;
        }
        if matches!(
            event.outcome,
            ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached | ObjectTreeOutcome::Cancelled
        ) {
            self.table_details_retry
                .borrow_mut()
                .retain(|_, (key, _, _)| key != &event.key);
        }
        let stale_items: Vec<String> = self
            .table_details_requests
            .iter()
            .filter(|(_, (key, captured))| key == &event.key && Some(*captured) != generation)
            .map(|(item, _)| item.clone())
            .collect();
        for item in stale_items {
            self.retire_stale_table_refresh(&item, generation, cx);
        }
        if self.app_state.read(cx).object_tree_is_pending(&event.key) {
            return;
        }
        let items: Vec<String> = self
            .table_details_requests
            .iter()
            .filter(|(_, (key, captured))| key == &event.key && Some(*captured) == generation)
            .map(|(item, _)| item.clone())
            .collect();
        for item in items {
            self.table_details_requests.remove(&item);
            self.loading_items.remove(&item);
            if let Some((task_id, token)) = self.table_refresh_tasks.remove(&item) {
                self.app_state.update(cx, |state, cx| {
                    match &event.outcome {
                        ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached
                            if !token.is_cancelled() =>
                        {
                            state.complete_task(task_id);
                        }
                        ObjectTreeOutcome::Failed(error) if !token.is_cancelled() => {
                            state.fail_task(task_id, error.clone());
                        }
                        _ => {
                            state.tasks_mut().cancel(task_id);
                        }
                    }
                    cx.emit(AppStateChanged);
                });
            }
            let original_action = self.table_details_actions.remove(&item);
            if self.pending_actions.get(&item) != original_action.as_ref() {
                continue;
            }
            if matches!(
                event.outcome,
                ObjectTreeOutcome::Rejected(_) | ObjectTreeOutcome::Failed(_)
            ) {
                let state = self.app_state.read(cx);
                if let (Some(connected), Some(generation)) =
                    (state.connections().get(&event.key.profile_id()), generation)
                {
                    self.table_details_retry.borrow_mut().insert(
                        item.clone(),
                        (
                            event.key.clone(),
                            std::sync::Arc::downgrade(&connected.connection),
                            generation,
                        ),
                    );
                }
            } else {
                self.table_details_retry.borrow_mut().remove(&item);
            }
            if matches!(
                event.outcome,
                ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached
            ) {
                if self.expansion_overrides.get(&item) == Some(&false)
                    && !matches!(
                        original_action,
                        Some(PendingAction::ViewSchema { .. } | PendingAction::GenerateCode { .. })
                    )
                {
                    self.pending_actions.remove(&item);
                } else {
                    self.complete_pending_action(&item, cx);
                }
            } else {
                self.pending_actions.remove(&item);
            }
        }
        self.rebuild_tree_with_overrides(cx);
    }

    /// Returns `true` if the fetch was started, `false` if preparation failed.
    pub(super) fn spawn_fetch_schema_types(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> bool {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_types(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema types already cached" {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Network,
                            crate::labels::cannot_load_schema_types_label(),
                        )
                        .with_cause(e),
                        cx,
                    );
                }
                return false;
            }
        };

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        self.spawn_fetch_with_result(
            pending_action,
            None,
            task,
            "Failed to fetch schema types",
            crate::labels::data_types_load_failed_label,
            |app_state, res, cx| {
                app_state.update(cx, |state, cx| {
                    state.set_schema_types(res.profile_id, res.database, res.schema, res.types);
                    cx.emit(AppStateChanged);
                });
            },
            |_app_state, _cx| {},
            cx,
        )
    }

    /// Returns `true` if the fetch was started, `false` if preparation failed.
    pub(super) fn spawn_fetch_schema_indexes(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> bool {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_indexes(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema indexes already cached" {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Network,
                            crate::labels::cannot_load_schema_indexes_label(),
                        )
                        .with_cause(e),
                        cx,
                    );
                }
                return false;
            }
        };

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        self.spawn_fetch_with_result(
            pending_action,
            None,
            task,
            "Failed to fetch schema indexes",
            crate::labels::indexes_load_failed_label,
            |app_state, res, cx| {
                app_state.update(cx, |state, cx| {
                    state.set_schema_indexes(res.profile_id, res.database, res.schema, res.indexes);
                    cx.emit(AppStateChanged);
                });
            },
            |_app_state, _cx| {},
            cx,
        )
    }

    /// Returns `true` if the fetch was started, `false` if preparation failed.
    pub(super) fn spawn_fetch_schema_foreign_keys(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> bool {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_foreign_keys(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema foreign keys already cached" {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Network,
                            crate::labels::cannot_load_schema_foreign_keys_label(),
                        )
                        .with_cause(e),
                        cx,
                    );
                }
                return false;
            }
        };

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        self.spawn_fetch_with_result(
            pending_action,
            None,
            task,
            "Failed to fetch schema foreign keys",
            crate::labels::foreign_keys_load_failed_label,
            |app_state, res, cx| {
                app_state.update(cx, |state, cx| {
                    state.set_schema_foreign_keys(
                        res.profile_id,
                        res.database,
                        res.schema,
                        res.foreign_keys,
                    );
                    cx.emit(AppStateChanged);
                });
            },
            |_app_state, _cx| {},
            cx,
        )
    }

    pub(super) fn spawn_fetch_schema_routines(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> bool {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_routines(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema routines already cached" {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Network,
                            crate::labels::cannot_load_schema_routines_label(),
                        )
                        .with_cause(e),
                        cx,
                    );
                }
                return false;
            }
        };

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        self.spawn_fetch_with_result(
            pending_action,
            None,
            task,
            "Failed to fetch schema routines",
            crate::labels::routines_load_failed_label,
            |app_state, res, cx| {
                app_state.update(cx, |state, cx| {
                    state.set_schema_routines(
                        res.profile_id,
                        res.database,
                        res.schema,
                        res.routines,
                    );
                    cx.emit(AppStateChanged);
                });
            },
            |_app_state, _cx| {},
            cx,
        )
    }

    /// Execute the stored action for a completed fetch.
    pub(super) fn complete_pending_action(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(action) = self.pending_actions.remove(item_id) else {
            return;
        };

        match action {
            PendingAction::ViewSchema { item_id } => {
                self.view_table_schema(&item_id, cx);
            }
            PendingAction::GenerateCode {
                item_id,
                generator_id,
            } => {
                self.generate_code_impl(&item_id, &generator_id, cx);
            }
            PendingAction::ExpandTypesFolder { item_id }
            | PendingAction::ExpandSchemaIndexesFolder { item_id }
            | PendingAction::ExpandSchemaForeignKeysFolder { item_id }
            | PendingAction::ExpandSchemaRoutinesFolder { item_id }
            | PendingAction::ExpandCollection { item_id } => {
                self.expand_schema_folder(&item_id, cx);
            }
            PendingAction::OpenChildPicker { item_id } => {
                self.pending_child_picker_item = Some(item_id);
            }
        }
    }

    pub(super) fn expand_schema_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.expansion_overrides.insert(item_id.to_string(), true);
        self.rebuild_tree_with_overrides(cx);
    }
}

#[cfg(test)]
pub(crate) mod object_tree_adapter_tests {
    //! Behavioral tests for the T4 sidebar adapter: the sidebar's generic
    //! profile/database/schema/table hierarchy and metadata loading must run
    //! through the shared `dbflux_ui_base::object_tree` projection and the
    //! `AppStateEntity` coordinator, exactly like the migration wizard.
    //!
    //! Boundary note: `dbflux_ui_sidebar` cannot import the wizard's test
    //! fixtures (no dependency on `dbflux_ui_document`), so the "second
    //! consumer" in the dedup tests is an independent requester/subscriber of
    //! the SAME `AppStateEntity` — the shared seam under test, not a
    //! wizard-specific path.

    use crate::{ContextMenuAction, PendingAction, TableDetailsStatus};
    use dbflux_core::{
        CollectionPresentation, DatabaseCategory, DriverCapabilities, QueryLanguage,
        SchemaLoadingStrategy, SchemaNodeId, SchemaNodeKind, TableInfo,
    };
    use dbflux_ui_base::app_state_entity::AppStateEntity;
    use dbflux_ui_base::object_tree::{
        ObjectTreeOutcome, ObjectTreeRejection, ObjectTreeRequestKey, ObjectTreeRequestStatus,
    };
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, Render, TestAppContext, Window, div,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock, Mutex};
    use uuid::Uuid;

    pub(crate) fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
        cx.update(gpui_component::theme::init);
        let state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    dbflux_storage::bootstrap::StorageRuntime::in_memory()
                        .expect("test storage runtime"),
                )
                .expect("test app state")
            })
        });
        cx.update(|cx| {
            let host = cx.new(|_| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
            cx.set_global(dbflux_ui_base::app_state_entity::AppStateGlobal {
                entity: state.clone(),
            });
        });
        state
    }

    // --- fixtures ---

    pub(crate) struct AdapterFakeConnection {
        metadata: dbflux_core::DriverMetadata,
        strategy: SchemaLoadingStrategy,
        pub(crate) databases: Mutex<Vec<dbflux_core::DatabaseInfo>>,
        schemas: Mutex<HashMap<String, dbflux_core::DbSchemaInfo>>,
        schema_failures: Mutex<HashMap<String, usize>>,
        list_calls: AtomicUsize,
        list_failures: AtomicUsize,
        schema_calls: Mutex<Vec<String>>,
        details_calls: AtomicUsize,
        details_failures: AtomicUsize,
        primary_schema_calls: AtomicUsize,
        primary_schema_failures: AtomicUsize,
        primary_snapshot: Mutex<Option<dbflux_core::SchemaSnapshot>>,
        authoritative_primary: AtomicBool,
    }

    impl AdapterFakeConnection {
        pub(crate) fn lazy() -> Arc<Self> {
            Arc::new(Self {
                metadata: fake_metadata(),
                strategy: SchemaLoadingStrategy::LazyPerDatabase,
                databases: Mutex::new(Vec::new()),
                schemas: Mutex::new(HashMap::new()),
                schema_failures: Mutex::new(HashMap::new()),
                list_calls: AtomicUsize::new(0),
                list_failures: AtomicUsize::new(0),
                schema_calls: Mutex::new(Vec::new()),
                details_calls: AtomicUsize::new(0),
                details_failures: AtomicUsize::new(0),
                primary_schema_calls: AtomicUsize::new(0),
                primary_schema_failures: AtomicUsize::new(0),
                primary_snapshot: Mutex::new(None),
                authoritative_primary: AtomicBool::new(false),
            })
        }

        fn single_database() -> Arc<Self> {
            let mut connection = Self::lazy();
            let fake = Arc::get_mut(&mut connection).expect("new fake is unshared");
            fake.strategy = SchemaLoadingStrategy::SingleDatabase;
            fake.authoritative_primary.store(true, Ordering::SeqCst);
            connection
        }

        fn connection_per_database() -> Arc<Self> {
            let mut connection = Self::lazy();
            Arc::get_mut(&mut connection)
                .expect("new fake is unshared")
                .strategy = SchemaLoadingStrategy::ConnectionPerDatabase;
            connection
        }

        fn set_schema(&self, database: &str, schema: dbflux_core::DbSchemaInfo) {
            self.schemas
                .lock()
                .expect("fake schemas")
                .insert(database.to_string(), schema);
        }

        fn fail_schema_once(&self, database: &str) {
            self.schema_failures
                .lock()
                .expect("fake schema failures")
                .insert(database.to_string(), 1);
        }

        fn schema_calls(&self) -> Vec<String> {
            self.schema_calls.lock().expect("fake schema calls").clone()
        }

        fn fail_details_once(&self) {
            self.details_failures.store(1, Ordering::SeqCst);
        }

        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }
    }

    static FAKE_DRIVER_FORM: LazyLock<dbflux_core::DriverFormDef> =
        LazyLock::new(|| dbflux_core::DriverFormDef { tabs: vec![] });

    struct AdapterPerDatabaseDriver {
        metadata: dbflux_core::DriverMetadata,
        connect_calls: Arc<AtomicUsize>,
        fail_next_connect: Arc<AtomicBool>,
    }

    impl dbflux_core::DbDriver for AdapterPerDatabaseDriver {
        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::Postgres
        }

        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn form_definition(&self) -> &dbflux_core::DriverFormDef {
            &FAKE_DRIVER_FORM
        }

        fn driver_key(&self) -> dbflux_core::DriverKey {
            "builtin:sidebar-adapter-test".into()
        }

        fn build_config(
            &self,
            _: &dbflux_core::FormValues,
        ) -> Result<dbflux_core::DbConfig, dbflux_core::DbError> {
            Ok(dbflux_core::DbConfig::default_postgres())
        }

        fn extract_values(&self, _: &dbflux_core::DbConfig) -> dbflux_core::FormValues {
            dbflux_core::FormValues::new()
        }

        fn connect_with_secrets(
            &self,
            _: &dbflux_core::ConnectionProfile,
            _: Option<&secrecy::SecretString>,
            _: Option<&secrecy::SecretString>,
        ) -> Result<Box<dyn dbflux_core::Connection>, dbflux_core::DbError> {
            self.connect_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_next_connect.swap(false, Ordering::SeqCst) {
                return Err(dbflux_core::DbError::NotSupported(
                    "fake connect failure".into(),
                ));
            }
            let mut connection = AdapterFakeConnection::lazy();
            let fake = Arc::get_mut(&mut connection).expect("unique fake");
            fake.strategy = SchemaLoadingStrategy::ConnectionPerDatabase;
            *fake.primary_snapshot.lock().expect("fake snapshot") =
                Some(secondary_snapshot("fresh"));
            match Arc::try_unwrap(connection) {
                Ok(connection) => Ok(Box::new(connection)),
                Err(_) => Err(dbflux_core::DbError::NotSupported(
                    "fake connection shared".into(),
                )),
            }
        }

        fn test_connection(
            &self,
            _: &dbflux_core::ConnectionProfile,
        ) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn with_database(
            &self,
            config: &dbflux_core::DbConfig,
            database: &str,
        ) -> Option<dbflux_core::DbConfig> {
            let mut config = config.clone();
            if let dbflux_core::DbConfig::Postgres {
                database: target, ..
            } = &mut config
            {
                *target = database.into();
                Some(config)
            } else {
                None
            }
        }
    }

    pub(crate) fn register_per_database_driver(
        state: &Entity<AppStateEntity>,
        cx: &mut TestAppContext,
    ) -> (Arc<AtomicUsize>, Arc<AtomicBool>) {
        let connect_calls = Arc::new(AtomicUsize::new(0));
        let fail_next_connect = Arc::new(AtomicBool::new(false));
        state.update(cx, |state, _| {
            state.inner.facade.connections.drivers.insert(
                "postgres".into(),
                Arc::new(AdapterPerDatabaseDriver {
                    metadata: fake_metadata(),
                    connect_calls: connect_calls.clone(),
                    fail_next_connect: fail_next_connect.clone(),
                }),
            );
        });
        (connect_calls, fail_next_connect)
    }

    fn fake_metadata() -> dbflux_core::DriverMetadata {
        dbflux_core::DriverMetadata {
            id: "sidebar-adapter-test".to_string(),
            display_name: "SidebarAdapterTest".to_string(),
            description: "test".to_string(),
            category: DatabaseCategory::Relational,
            transfer_family: dbflux_core::TransferFamily::Sql,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: Some(5432),
            uri_scheme: "test".to_string(),
            icon: dbflux_core::Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        }
    }

    fn fake_table(schema: &str, name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some(schema.to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
        }
    }

    fn fake_db_schema(database: &str, tables: Vec<TableInfo>) -> dbflux_core::DbSchemaInfo {
        dbflux_core::DbSchemaInfo {
            name: database.to_string(),
            tables,
            views: Vec::new(),
            custom_types: None,
        }
    }

    impl dbflux_core::Connection for AdapterFakeConnection {
        fn schema_snapshot_authority(&self) -> dbflux_core::SchemaSnapshotAuthority {
            if self.authoritative_primary.load(Ordering::SeqCst) {
                dbflux_core::SchemaSnapshotAuthority::Authoritative
            } else {
                dbflux_core::SchemaSnapshotAuthority::EnumerationOnly
            }
        }

        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn execute(
            &self,
            _req: &dbflux_core::QueryRequest,
        ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
            Err(dbflux_core::DbError::NotSupported("test".to_string()))
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
            self.primary_schema_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .primary_schema_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(dbflux_core::DbError::NotSupported(
                    "fake view refresh failure".into(),
                ));
            }
            Ok(self
                .primary_snapshot
                .lock()
                .expect("fake snapshot")
                .clone()
                .unwrap_or_default())
        }

        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            self.strategy
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }

        fn list_databases(&self) -> Result<Vec<dbflux_core::DatabaseInfo>, dbflux_core::DbError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .list_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(dbflux_core::DbError::ConnectionFailed(
                    dbflux_core::FormattedError::new("listing failed"),
                ));
            }
            Ok(self.databases.lock().expect("fake databases").clone())
        }

        fn schema_for_database(
            &self,
            database: &str,
        ) -> Result<dbflux_core::DbSchemaInfo, dbflux_core::DbError> {
            self.schema_calls
                .lock()
                .expect("fake schema calls")
                .push(database.to_string());

            let mut failures = self.schema_failures.lock().expect("failures");
            if let Some(remaining) = failures.get_mut(database)
                && *remaining > 0
            {
                *remaining -= 1;
                return Err(dbflux_core::DbError::ConnectionFailed(
                    dbflux_core::FormattedError::new(format!(
                        "introspection for '{database}' failed"
                    )),
                ));
            }
            drop(failures);

            self.schemas
                .lock()
                .expect("fake schemas")
                .get(database)
                .cloned()
                .ok_or_else(|| {
                    dbflux_core::DbError::ConnectionFailed(dbflux_core::FormattedError::new(
                        format!("no fake schema for '{database}'"),
                    ))
                })
        }

        fn table_details(
            &self,
            database: &str,
            schema: Option<&str>,
            table: &str,
        ) -> Result<TableInfo, dbflux_core::DbError> {
            self.details_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .details_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(dbflux_core::DbError::NotSupported(
                    "fake table details failure".into(),
                ));
            }
            Ok(loaded_details(database, schema, table))
        }
    }

    fn loaded_details(database: &str, schema: Option<&str>, table: &str) -> TableInfo {
        let mut details = fake_table(schema.unwrap_or(database), table);
        details.columns = Some(vec![dbflux_core::ColumnInfo {
            name: "id".to_string(),
            type_name: "int4".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        }]);
        details
    }

    pub(crate) fn connect_profile(
        state: &Entity<AppStateEntity>,
        cx: &mut TestAppContext,
        profile_id: Uuid,
        connection: Arc<AdapterFakeConnection>,
        schema: Option<dbflux_core::SchemaSnapshot>,
    ) {
        let mut profile = dbflux_core::ConnectionProfile::new(
            "adapter-test",
            dbflux_core::DbConfig::default_postgres(),
        );
        profile.id = profile_id;
        state.update(cx, |state, _| {
            state.profiles_mut().push(profile.clone());
            state.connection_tree_mut().add_node(
                dbflux_core::ConnectionTreeNode::new_connection_ref(profile_id, None, 1000),
            );
            state.apply_connect_profile(
                profile,
                connection,
                schema,
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
    }

    fn secondary_snapshot(table: &str) -> dbflux_core::SchemaSnapshot {
        dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases: vec![dbflux_core::DatabaseInfo {
                name: "reporting".into(),
                is_current: true,
            }],
            current_database: Some("reporting".into()),
            schemas: vec![fake_db_schema(
                "reporting",
                vec![fake_table("public", table)],
            )],
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    pub(crate) fn snapshot_naming(
        databases: Vec<dbflux_core::DatabaseInfo>,
    ) -> dbflux_core::SchemaSnapshot {
        let current_database = databases
            .iter()
            .find(|db| db.is_current)
            .map(|db| db.name.clone());
        dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases,
            current_database,
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    /// Flattens the built tree into `(id, label, is_expanded)` rows, depth-first.
    fn flatten(items: &[gpui_component::tree::TreeItem]) -> Vec<(String, String, bool)> {
        let mut rows = Vec::new();
        fn walk(items: &[gpui_component::tree::TreeItem], rows: &mut Vec<(String, String, bool)>) {
            for item in items {
                rows.push((
                    item.id.to_string(),
                    item.label.to_string(),
                    item.is_expanded(),
                ));
                walk(&item.children, rows);
            }
        }
        walk(items, &mut rows);
        rows
    }

    fn build_items(
        window: &gpui::WindowHandle<crate::Sidebar>,
        cx: &mut TestAppContext,
    ) -> Vec<(String, String, bool)> {
        let items = window
            .update(cx, |sidebar, _, cx| {
                sidebar.build_tree_items_with_overrides(cx)
            })
            .expect("sidebar window alive");
        flatten(&items)
    }

    struct EventRecorder;

    impl Render for EventRecorder {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }

    #[gpui::test]
    async fn selection_rebuild_tracks_visible_table_identity(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::lazy(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema(
                    "app",
                    vec![fake_table("public", "alpha"), fake_table("public", "beta")],
                ),
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let database_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "beta".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&database_id, true, cx);
                let items = sidebar.build_tree_items_with_overrides(cx);
                let index = crate::Sidebar::find_item_index_in_tree(&items, &table_id, &mut 0)
                    .expect("visible table");
                sidebar
                    .tree_state
                    .update(cx, |tree, cx| tree.set_selected_index(Some(index), cx));
                assert_eq!(
                    sidebar
                        .tree_state
                        .read(cx)
                        .selected_entry()
                        .map(|entry| entry.item().id.as_ref()),
                    Some(table_id.as_str())
                );
            })
            .expect("sidebar alive");

        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema(
                    "app",
                    vec![fake_table("public", "beta"), fake_table("public", "alpha")],
                ),
            );
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.rebuild_tree_with_overrides(cx);
                assert_eq!(
                    sidebar
                        .tree_state
                        .read(cx)
                        .selected_entry()
                        .map(|entry| entry.item().id.as_ref()),
                    Some(table_id.as_str())
                );
            })
            .expect("sidebar alive");

        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema("app", vec![fake_table("public", "alpha")]),
            );
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_tree(cx);
                assert!(
                    sidebar.tree_state.read(cx).selected_entry().is_none(),
                    "removed table must not dispatch its neighbor"
                );
            })
            .expect("sidebar alive");
    }

    // --- RED/GREEN behavioral tests ---

    #[gpui::test]
    async fn authoritative_empty_primary_hides_stale_lazy_tables_and_views(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.authoritative_primary.store(true, Ordering::SeqCst);
        connect_profile(
            &state,
            cx,
            profile_id,
            fake,
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            let mut stale = fake_db_schema(
                "public",
                vec![loaded_details("app", Some("public"), "ghost")],
            );
            stale.views.push(dbflux_core::ViewInfo {
                name: "ghost_view".into(),
                schema: Some("public".into()),
            });
            stale.custom_types = Some(vec![dbflux_core::CustomTypeInfo {
                name: "ghost_type".into(),
                schema: Some("stale_types".into()),
                kind: dbflux_core::CustomTypeKind::Enum,
                enum_values: None,
                base_type: None,
            }]);
            state.set_database_schema(profile_id, "app".into(), stale);
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let rows = build_items(&window, cx);
        for name in ["ghost", "ghost_view", "stale_types", "ghost_type (enum)"] {
            assert!(
                !rows.iter().any(|(_, label, _)| label == name),
                "authoritative empty primary must hide stale {name}: {rows:?}"
            );
        }
    }

    #[gpui::test]
    async fn projected_database_named_single_schema_stays_flat(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::lazy(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            let mut info = fake_db_schema("app", vec![loaded_details("app", Some("app"), "users")]);
            info.views.push(dbflux_core::ViewInfo {
                name: "active_users".into(),
                schema: Some("app".into()),
            });
            state.set_database_schema(profile_id, "app".into(), info);
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let rows = build_items(&window, cx);
        let schema = SchemaNodeId::Schema {
            profile_id,
            database: Some("app".into()),
            name: "app".into(),
        }
        .to_string();
        let table = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "app".into(),
            name: "users".into(),
        }
        .to_string();
        let view = SchemaNodeId::View {
            profile_id,
            database: Some("app".into()),
            schema: "app".into(),
            name: "active_users".into(),
        }
        .to_string();
        assert!(
            !rows.iter().any(|(id, _, _)| id == &schema),
            "redundant schema row: {rows:?}"
        );
        assert!(
            rows.iter().any(|(id, _, _)| id == &table),
            "missing table: {rows:?}"
        );
        assert!(
            rows.iter().any(|(id, _, _)| id == &view),
            "missing view: {rows:?}"
        );
    }

    #[gpui::test]
    async fn projected_empty_database_types_distinguish_loaded_empty_from_unloaded(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::lazy(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            let mut info = fake_db_schema("app", Vec::new());
            info.custom_types = Some(Vec::new());
            state.set_database_schema(profile_id, "app".into(), info);
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let folder = SchemaNodeId::TypesFolder {
            profile_id,
            database: "app".into(),
            schema: "app".into(),
        }
        .to_string();
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, label, _)| id == &folder
                && label == &crate::labels::data_types_folder_label(0)),
            "loaded empty types: {rows:?}"
        );
        state.update(cx, |state, _| {
            state.set_database_schema(profile_id, "app".into(), fake_db_schema("app", Vec::new()))
        });
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, label, _)| id == &folder
                && label == &crate::labels::data_types_folder_label_plain()),
            "unloaded types: {rows:?}"
        );
    }

    #[gpui::test]
    async fn implicit_single_database_preserves_direct_legacy_ids(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let snapshot = dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases: Vec::new(),
            current_database: Some("main".into()),
            schemas: vec![{
                let mut schema = fake_db_schema("main", vec![fake_table("main", "users")]);
                schema.views.push(dbflux_core::ViewInfo {
                    name: "active_users".into(),
                    schema: Some("main".into()),
                });
                schema
            }],
            tables: Vec::new(),
            views: Vec::new(),
        });
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::single_database(),
            Some(snapshot),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let schema = SchemaNodeId::Schema {
            profile_id,
            database: None,
            name: "main".into(),
        }
        .to_string();
        let table_folder = SchemaNodeId::TablesFolder {
            profile_id,
            database: None,
            schema: "main".into(),
        }
        .to_string();
        let table = SchemaNodeId::Table {
            profile_id,
            database: None,
            schema: "main".into(),
            name: "users".into(),
        }
        .to_string();
        let view_folder = SchemaNodeId::ViewsFolder {
            profile_id,
            database: None,
            schema: "main".into(),
        }
        .to_string();
        let view = SchemaNodeId::View {
            profile_id,
            database: None,
            schema: "main".into(),
            name: "active_users".into(),
        }
        .to_string();
        let items = window
            .update(cx, |sidebar, _, cx| {
                sidebar.build_tree_items_with_overrides(cx)
            })
            .expect("sidebar alive");
        let profile = items
            .iter()
            .find(|item| item.id.as_ref() == SchemaNodeId::Profile { profile_id }.to_string())
            .expect("profile");
        assert!(
            profile
                .children
                .iter()
                .any(|child| child.id.as_ref() == schema),
            "direct schema: {:?}",
            flatten(&profile.children)
        );
        assert!(!profile.children.iter().any(|child| matches!(
            crate::parse_node_id(&child.id),
            Some(SchemaNodeId::DatabasesFolder { .. } | SchemaNodeId::Database { .. })
        )));
        let rows = flatten(&items);
        let types = SchemaNodeId::TypesFolder {
            profile_id,
            database: "main".into(),
            schema: "main".into(),
        }
        .to_string();
        assert_eq!(schema, format!("S|{profile_id}|main"));
        assert_eq!(table_folder, format!("TF|{profile_id}|main"));
        assert_eq!(table, format!("T|{profile_id}|main|users"));
        assert_eq!(view_folder, format!("VF|{profile_id}|main"));
        assert_eq!(view, format!("V|{profile_id}|main|active_users"));
        for id in [&schema, &table_folder, &table, &view_folder, &view, &types] {
            assert!(
                rows.iter().any(|row| &row.0 == id),
                "missing legacy {id}: {rows:?}"
            );
        }
        let opened = Arc::new(AtomicUsize::new(0));
        let sidebar_entity = window.entity(cx).expect("sidebar entity");
        let _observer = {
            let opened = opened.clone();
            cx.update(|cx| cx.new(|cx| {
                cx.subscribe(&sidebar_entity, move |_, _, event: &crate::SidebarEvent, _| {
                    if matches!(event, crate::SidebarEvent::OpenTable { profile_id: opened_profile, database: None, table } if *opened_profile == profile_id && table.name == "users") {
                        opened.fetch_add(1, Ordering::SeqCst);
                    }
                }).detach();
                EventRecorder
            }))
        };
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&schema, true, cx);
                sidebar.execute_item(&table, cx);
            })
            .expect("sidebar alive");
        assert_eq!(opened.load(Ordering::SeqCst), 1);
        let reopened = build_items(&window, cx);
        assert_eq!(
            reopened.iter().find(|row| row.0 == schema).map(|row| row.2),
            Some(true)
        );
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&profile_item, true, cx)
            })
            .expect("sidebar alive");
        assert!(state.read_with(cx, |state, _| {
            state.object_tree_is_pending(&ObjectTreeRequestKey::DatabaseList { profile_id })
        }));
        cx.run_until_parked();
        assert!(state.read_with(cx, |state, _| {
            state
                .get_database_list(profile_id)
                .is_some_and(|list| list.is_empty())
        }));
        let items = window
            .update(cx, |sidebar, _, cx| {
                sidebar.build_tree_items_with_overrides(cx)
            })
            .expect("sidebar alive");
        let profile = items
            .iter()
            .find(|item| item.id.as_ref() == profile_item)
            .expect("profile");
        assert!(
            profile
                .children
                .iter()
                .any(|child| child.id.as_ref() == schema),
            "settled list must preserve direct schema: {:?}",
            flatten(&profile.children)
        );
        assert!(
            !profile.children.iter().any(|child| child.id.as_ref()
                == SchemaNodeId::DatabasesFolder { profile_id }.to_string())
        );
        let rows = flatten(&items);
        for id in [&schema, &table_folder, &table, &view_folder, &view, &types] {
            assert!(
                rows.iter().any(|row| &row.0 == id),
                "settled list changed {id}: {rows:?}"
            );
        }
        assert_eq!(
            rows.iter().find(|row| row.0 == schema).map(|row| row.2),
            Some(true)
        );
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&table, cx))
            .expect("sidebar alive");
        assert_eq!(opened.load(Ordering::SeqCst), 2);
    }

    #[gpui::test]
    async fn unnamed_primary_uses_first_schema_for_legacy_cache_context(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let snapshot = dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![fake_db_schema("main", vec![fake_table("main", "users")])],
            tables: Vec::new(),
            views: Vec::new(),
        });
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::single_database(),
            Some(snapshot),
        );
        state.update(cx, |state, _| {
            state.set_schema_types(
                profile_id,
                "main".into(),
                Some("main".into()),
                vec![dbflux_core::CustomTypeInfo {
                    name: "status".into(),
                    schema: Some("main".into()),
                    kind: dbflux_core::CustomTypeKind::Enum,
                    enum_values: None,
                    base_type: None,
                }],
            );
            state.set_schema_indexes(
                profile_id,
                "main".into(),
                Some("main".into()),
                vec![dbflux_core::SchemaIndexInfo {
                    name: "users_idx".into(),
                    table_name: "users".into(),
                    columns: vec!["id".into()],
                    is_unique: false,
                    is_primary: false,
                }],
            );
            state.set_schema_foreign_keys(
                profile_id,
                "main".into(),
                Some("main".into()),
                vec![dbflux_core::SchemaForeignKeyInfo {
                    name: "users_fk".into(),
                    table_name: "users".into(),
                    columns: vec!["id".into()],
                    referenced_schema: None,
                    referenced_table: "teams".into(),
                    referenced_columns: vec!["id".into()],
                    on_delete: None,
                    on_update: None,
                }],
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let rows = build_items(&window, cx);
        let schema = SchemaNodeId::Schema {
            profile_id,
            database: None,
            name: "main".into(),
        }
        .to_string();
        let table = SchemaNodeId::Table {
            profile_id,
            database: None,
            schema: "main".into(),
            name: "users".into(),
        }
        .to_string();
        for id in [
            schema,
            table,
            SchemaNodeId::TypesFolder {
                profile_id,
                database: "main".into(),
                schema: "main".into(),
            }
            .to_string(),
            SchemaNodeId::CustomType {
                profile_id,
                schema: "main".into(),
                name: "status".into(),
            }
            .to_string(),
            SchemaNodeId::SchemaIndexesFolder {
                profile_id,
                database: "main".into(),
                schema: "main".into(),
            }
            .to_string(),
            SchemaNodeId::SchemaIndex {
                profile_id,
                schema: "main".into(),
                name: "users_idx".into(),
            }
            .to_string(),
            SchemaNodeId::SchemaForeignKeysFolder {
                profile_id,
                database: "main".into(),
                schema: "main".into(),
            }
            .to_string(),
        ] {
            assert!(
                rows.iter().any(|row| row.0 == id),
                "missing {id} with legacy cache key main/main: {rows:?}"
            );
        }
        let fk = SchemaNodeId::SchemaForeignKey {
            profile_id,
            schema: "main".into(),
            name: "users_fk".into(),
        }
        .to_string();
        assert!(
            rows.iter().any(|row| row.0 == fk),
            "missing cached FK: {rows:?}"
        );
        let table = SchemaNodeId::Table {
            profile_id,
            database: None,
            schema: "main".into(),
            name: "users".into(),
        }
        .to_string();
        let opened = Arc::new(AtomicUsize::new(0));
        let sidebar_entity = window.entity(cx).expect("sidebar entity");
        let _observer = {
            let opened = opened.clone();
            cx.update(|cx| cx.new(|cx| {
                cx.subscribe(&sidebar_entity, move |_, _, event: &crate::SidebarEvent, _| {
                    if matches!(event, crate::SidebarEvent::OpenTable { profile_id: opened_profile, database: None, table } if *opened_profile == profile_id && table.name == "users") {
                        opened.fetch_add(1, Ordering::SeqCst);
                    }
                }).detach();
                EventRecorder
            }))
        };
        let schema = SchemaNodeId::Schema {
            profile_id,
            database: None,
            name: "main".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&schema, true, cx);
                sidebar.execute_item(&table, cx);
            })
            .expect("sidebar alive");
        assert_eq!(opened.load(Ordering::SeqCst), 1);
        assert_eq!(
            build_items(&window, cx)
                .iter()
                .find(|row| row.0 == schema)
                .map(|row| row.2),
            Some(true)
        );
    }

    #[gpui::test]
    async fn explicitly_listed_main_keeps_database_wrapper_and_qualified_ids(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::lazy(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "main".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "main".into(),
                fake_db_schema("main", vec![fake_table("main", "users")]),
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let items = window
            .update(cx, |sidebar, _, cx| {
                sidebar.build_tree_items_with_overrides(cx)
            })
            .expect("sidebar alive");
        let profile = items
            .iter()
            .find(|item| item.id.as_ref() == SchemaNodeId::Profile { profile_id }.to_string())
            .expect("profile");
        let databases_folder = SchemaNodeId::DatabasesFolder { profile_id }.to_string();
        assert!(
            profile
                .children
                .iter()
                .any(|item| item.id.as_ref() == databases_folder),
            "explicit list must keep wrapper"
        );
        let database = SchemaNodeId::Database {
            profile_id,
            name: "main".into(),
        }
        .to_string();
        let table = SchemaNodeId::Table {
            profile_id,
            database: Some("main".into()),
            schema: "main".into(),
            name: "users".into(),
        }
        .to_string();
        let rows = flatten(&items);
        for id in [&database, &table] {
            assert!(
                rows.iter().any(|row| &row.0 == id),
                "missing qualified {id}: {rows:?}"
            );
        }
        assert!(!rows.iter().any(|row| {
            row.0
                == SchemaNodeId::Table {
                    profile_id,
                    database: None,
                    schema: "main".into(),
                    name: "users".into(),
                }
                .to_string()
        }));
    }

    #[gpui::test]
    async fn projected_schema_groups_keep_database_ids_and_independent_expansion(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::lazy(),
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "analytics".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "archive".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            for database in ["analytics", "archive"] {
                let mut info = fake_db_schema(
                    database,
                    vec![
                        loaded_details(database, Some("sales"), "orders"),
                        loaded_details(database, Some("inventory"), "stock"),
                        loaded_details(database, None, "legacy"),
                    ],
                );
                info.views.push(dbflux_core::ViewInfo {
                    name: "report".into(),
                    schema: Some("sales".into()),
                });
                state.set_database_schema(profile_id, database.into(), info);
            }
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let analytics_schema = SchemaNodeId::Schema {
            profile_id,
            database: Some("analytics".into()),
            name: "sales".into(),
        }
        .to_string();
        let archive_schema = SchemaNodeId::Schema {
            profile_id,
            database: Some("archive".into()),
            name: "sales".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&analytics_schema, false, cx);
                sidebar.set_expanded(&archive_schema, true, cx);
            })
            .expect("sidebar alive");
        let rows = build_items(&window, cx);
        assert_eq!(
            rows.iter()
                .find(|(id, _, _)| id == &analytics_schema)
                .map(|row| row.2),
            Some(false)
        );
        assert_eq!(
            rows.iter()
                .find(|(id, _, _)| id == &archive_schema)
                .map(|row| row.2),
            Some(true)
        );
        for database in ["analytics", "archive"] {
            for schema in ["sales", "inventory", database] {
                let id = SchemaNodeId::Schema {
                    profile_id,
                    database: Some(database.into()),
                    name: schema.into(),
                }
                .to_string();
                assert!(
                    rows.iter().any(|(row, _, _)| row == &id),
                    "missing {id}: {rows:?}"
                );
            }
            for (schema, table) in [
                ("sales", "orders"),
                ("inventory", "stock"),
                (database, "legacy"),
            ] {
                let id = SchemaNodeId::Table {
                    profile_id,
                    database: Some(database.into()),
                    schema: schema.into(),
                    name: table.into(),
                }
                .to_string();
                assert!(
                    rows.iter().any(|(row, _, _)| row == &id),
                    "missing {id}: {rows:?}"
                );
            }
            let view = SchemaNodeId::View {
                profile_id,
                database: Some(database.into()),
                schema: "sales".into(),
                name: "report".into(),
            }
            .to_string();
            assert!(
                rows.iter().any(|(id, _, _)| id == &view),
                "missing {view}: {rows:?}"
            );
        }
    }

    #[gpui::test]
    async fn lazy_type_only_schema_keeps_its_schema_and_type_ids(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        connect_profile(
            &state,
            cx,
            profile_id,
            AdapterFakeConnection::lazy(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            let mut info = fake_db_schema("app", Vec::new());
            info.custom_types = Some(vec![
                dbflux_core::CustomTypeInfo {
                    name: "status".into(),
                    schema: Some("audit".into()),
                    kind: dbflux_core::CustomTypeKind::Enum,
                    enum_values: Some(vec!["new".into()]),
                    base_type: None,
                },
                dbflux_core::CustomTypeInfo {
                    name: "tier".into(),
                    schema: None,
                    kind: dbflux_core::CustomTypeKind::Domain,
                    enum_values: None,
                    base_type: Some("varchar(32)".into()),
                },
            ]);
            state.set_database_schema(profile_id, "app".into(), info);
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let rows = build_items(&window, cx);
        let schema = SchemaNodeId::Schema {
            profile_id,
            database: Some("app".into()),
            name: "audit".into(),
        }
        .to_string();
        assert!(
            rows.iter().any(|(id, _, _)| id == &schema),
            "missing type-only schema: {rows:?}"
        );
        assert!(
            rows.iter().any(|(_, label, _)| label == "status (enum)"),
            "missing selected type: {rows:?}"
        );
        for (schema, name) in [("audit", "status"), ("app", "tier")] {
            let id = SchemaNodeId::CustomType {
                profile_id,
                schema: schema.into(),
                name: name.into(),
            }
            .to_string();
            assert!(
                rows.iter().any(|(row, _, _)| row == &id),
                "missing {id}: {rows:?}"
            );
        }
        assert!(rows.iter().any(|(_, label, _)| label == "tier (domain)"));
        state.update(cx, |state, _| {
            state.set_schema_types(
                profile_id,
                "app".into(),
                Some("audit".into()),
                vec![dbflux_core::CustomTypeInfo {
                    name: "cached_status".into(),
                    schema: Some("audit".into()),
                    kind: dbflux_core::CustomTypeKind::Enum,
                    enum_values: None,
                    base_type: None,
                }],
            );
        });
        let rows = build_items(&window, cx);
        assert!(
            rows.iter()
                .any(|(_, label, _)| label == "cached_status (enum)")
        );
        assert!(!rows.iter().any(|(_, label, _)| label == "status (enum)"));
    }

    #[gpui::test]
    async fn enumerative_primary_projects_lazy_table_view_and_schemaless_view_ids(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake,
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            let mut loaded =
                fake_db_schema("app", vec![loaded_details("app", Some("public"), "users")]);
            loaded.views.extend([
                dbflux_core::ViewInfo {
                    name: "report".into(),
                    schema: Some("public".into()),
                },
                dbflux_core::ViewInfo {
                    name: "summary".into(),
                    schema: None,
                },
            ]);
            state.set_database_schema(profile_id, "app".into(), loaded);
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let rows = build_items(&window, cx);
        let ids = [
            SchemaNodeId::Table {
                profile_id,
                database: Some("app".into()),
                schema: "public".into(),
                name: "users".into(),
            },
            SchemaNodeId::View {
                profile_id,
                database: Some("app".into()),
                schema: "public".into(),
                name: "report".into(),
            },
            SchemaNodeId::View {
                profile_id,
                database: Some("app".into()),
                schema: "app".into(),
                name: "summary".into(),
            },
        ];
        for id in ids {
            assert!(
                rows.iter().any(|(row, _, _)| row == &id.to_string()),
                "missing {id}: {rows:?}"
            );
        }
    }

    #[gpui::test]
    async fn explicit_database_cannot_use_legacy_namespace_cache_alias(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "public".to_string(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "public".to_string(),
                fake_db_schema(
                    "public",
                    vec![loaded_details("public", Some("public"), "users")],
                ),
            )
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let implicit = SchemaNodeId::Table {
            profile_id,
            database: None,
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let explicit = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(
                    matches!(
                        sidebar.ensure_table_details(
                            &implicit,
                            PendingAction::ViewSchema {
                                item_id: implicit.clone()
                            },
                            cx
                        ),
                        TableDetailsStatus::Ready
                    ),
                    "legacy database-less namespace remains cached"
                );
                assert!(
                    matches!(
                        sidebar.ensure_table_details(
                            &explicit,
                            PendingAction::ViewSchema {
                                item_id: explicit.clone()
                            },
                            cx
                        ),
                        TableDetailsStatus::Loading
                    ),
                    "explicit app cannot borrow a database literally named public"
                );
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.details_calls.load(Ordering::SeqCst), 1);
        assert!(state.read_with(cx, |state, _| {
            state
                .connections()
                .get(&profile_id)
                .expect("connected")
                .table_details
                .contains_key(&(
                    "app".to_string(),
                    Some("public".to_string()),
                    "users".to_string(),
                ))
        }));
    }

    #[gpui::test]
    async fn other_database_cannot_use_primary_table_details_as_ready(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        let primary = dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases: vec![
                dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "analytics".to_string(),
                    is_current: false,
                },
            ],
            current_database: Some("app".to_string()),
            schemas: vec![fake_db_schema(
                "public",
                vec![loaded_details("app", Some("public"), "users")],
            )],
            tables: Vec::new(),
            views: Vec::new(),
        });
        connect_profile(&state, cx, profile_id, fake.clone(), Some(primary));
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let app_table = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let analytics_table = SchemaNodeId::Table {
            profile_id,
            database: Some("analytics".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &app_table,
                        PendingAction::ViewSchema {
                            item_id: app_table.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Ready
                ));
                assert!(
                    matches!(
                        sidebar.ensure_table_details(
                            &analytics_table,
                            PendingAction::ViewSchema {
                                item_id: analytics_table.clone()
                            },
                            cx
                        ),
                        TableDetailsStatus::Loading
                    ),
                    "another database's complete primary table must not make analytics Ready"
                );
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(
            fake.details_calls.load(Ordering::SeqCst),
            1,
            "analytics must use its typed coordinator request"
        );
        assert!(state.read_with(cx, |state, _| {
            state
                .connections()
                .get(&profile_id)
                .expect("connected")
                .table_details
                .contains_key(&(
                    "analytics".to_string(),
                    Some("public".to_string()),
                    "users".to_string(),
                ))
        }));
    }

    #[gpui::test]
    async fn shared_database_list_dedups_between_two_consumers_and_feeds_sidebar_enumeration(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        *fake.databases.lock().expect("databases") = vec![
            dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            },
            dbflux_core::DatabaseInfo {
                name: "other".to_string(),
                is_current: false,
            },
        ];
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );

        // An independent second consumer subscribes to the same shared entity.
        let events: Arc<Mutex<Vec<dbflux_ui_base::object_tree::ObjectTreeEvent>>> =
            Arc::new(Mutex::new(Vec::new()));
        let _recorder = {
            let events = events.clone();
            cx.update(|cx| {
                cx.new(|cx| {
                    cx.subscribe(
                        &state,
                        move |_this,
                              _state,
                              event: &dbflux_ui_base::object_tree::ObjectTreeEvent,
                              _cx| {
                            events.lock().expect("event log").push(event.clone());
                        },
                    )
                    .detach();
                    EventRecorder
                })
            })
        };

        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));

        // Consumer A (the sidebar): expanding the profile requests the shared
        // database list through the coordinator.
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&profile_item, true, cx);
            })
            .expect("sidebar alive");

        let list_key = ObjectTreeRequestKey::DatabaseList { profile_id };
        assert!(
            state.read_with(cx, |state, _| state.object_tree_is_pending(&list_key)),
            "sidebar profile expansion must dispatch the shared database-list request"
        );

        // Consumer B: an independent requester of the same entity dedups into
        // the same in-flight attempt instead of starting driver work.
        state.update(cx, |state, cx| {
            assert_eq!(
                state.object_tree_request(list_key.clone(), cx),
                ObjectTreeRequestStatus::Pending,
                "two consumers must share one in-flight request"
            );
        });

        cx.run_until_parked();

        assert_eq!(
            fake.list_calls(),
            1,
            "one driver request must serve both consumers"
        );
        let matching = events
            .lock()
            .expect("event log")
            .iter()
            .filter(|event| event.key == list_key)
            .count();
        assert_eq!(
            matching, 1,
            "every subscriber of the shared entity is notified of the settle"
        );

        // The shared enumeration feeds the sidebar tree: the list-named
        // database appears alongside the snapshot-named one.
        let rows = build_items(&window, cx);
        let other_id = SchemaNodeId::Database {
            profile_id,
            name: "other".to_string(),
        }
        .to_string();
        assert!(
            rows.iter().any(|(id, _, _)| *id == other_id),
            "the sidebar must render the shared projection's list-named database; rows: {rows:?}"
        );
    }

    #[gpui::test]
    async fn failed_database_list_requires_explicit_profile_retry(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.list_failures.store(1, Ordering::SeqCst);
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&profile_item, true, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        let retry_id = format!("object-retry|{profile_item}");
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id)
        );
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_tree(cx);
                sidebar.set_expanded(&profile_item, false, cx);
                sidebar.set_expanded(&profile_item, true, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.list_calls(), 1);
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&retry_id, cx))
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.list_calls(), 2);
        assert!(
            !build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id)
        );
    }

    #[gpui::test]
    async fn authoritative_empty_database_list_does_not_resurrect_snapshot_database(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "obsolete".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("connected")
                .active_database = Some("other_obsolete".into());
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&profile_item, true, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.list_calls(), 1);
        let rows = build_items(&window, cx);
        for name in ["obsolete", "other_obsolete"] {
            let ghost_id = SchemaNodeId::Database {
                profile_id,
                name: name.into(),
            }
            .to_string();
            assert!(
                !rows.iter().any(|(id, _, _)| id == &ghost_id),
                "empty list excludes selectable ghost {name}: {rows:?}"
            );
        }
        assert!(
            !rows.iter().any(|(_, label, _)| label.contains("obsolete")),
            "empty list excludes ghost root: {rows:?}"
        );
    }

    #[gpui::test]
    async fn failed_schema_load_renders_explicit_retry_and_never_redispatches_on_rebuild(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        *fake.databases.lock().expect("databases") = vec![dbflux_core::DatabaseInfo {
            name: "app".to_string(),
            is_current: true,
        }];
        fake.fail_schema_once("app");
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );

        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));

        let db_item = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, true, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();

        // The failure must settle into an explicit retry row, not a collapse
        // or a forever-Loading placeholder.
        let rows = build_items(&window, cx);
        let retry_id = format!("object-retry|{db_item}");
        assert!(
            rows.iter().any(|(id, _, _)| *id == retry_id),
            "a failed load must render an explicit retry row; rows: {rows:?}"
        );

        // Rebuilding and re-expanding must NOT re-dispatch the failed load:
        // retrying is the user's explicit choice.
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_tree(cx);
                sidebar.set_expanded(&db_item, false, cx);
                sidebar.set_expanded(&db_item, true, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();

        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string()],
            "rebuilds and re-expansions must not re-dispatch a failed request"
        );

        // The explicit retry row re-dispatches through the coordinator.
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.execute_item(&retry_id, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();

        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string(), "app".to_string()],
            "the retry row must re-dispatch exactly once"
        );
        let rows = build_items(&window, cx);
        assert!(
            !state.read_with(cx, |state, _| state.object_tree_is_pending(
                &ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                }
            )),
            "successful explicit retry must release Loading"
        );
        assert!(
            !rows.iter().any(|(id, _, _)| id == &retry_id),
            "successful explicit retry removes the retry row"
        );
        let tables_id = SchemaNodeId::TablesFolder {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
        }
        .to_string();
        assert!(
            rows.iter().any(|(id, _, _)| *id == tables_id),
            "after a successful retry the loaded content must render; rows: {rows:?}"
        );
    }

    #[gpui::test]
    async fn invalidation_during_table_details_flight_rejects_result_and_keeps_cache_clean(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        *fake.databases.lock().expect("databases") = vec![dbflux_core::DatabaseInfo {
            name: "app".to_string(),
            is_current: true,
        }];
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );

        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));

        // Load the database schema through the shared coordinator first.
        state.update(cx, |state, cx| {
            state.object_tree_request(
                ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                },
                cx,
            );
        });
        cx.run_until_parked();
        assert_eq!(fake.schema_calls(), vec!["app".to_string()]);

        let table_item = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();

        // Start the table-details load through the sidebar adapter.
        window
            .update(cx, |sidebar, _, cx| {
                let status = sidebar.ensure_table_details(
                    &table_item,
                    PendingAction::ViewSchema {
                        item_id: table_item.clone(),
                    },
                    cx,
                );
                assert!(matches!(status, TableDetailsStatus::Loading));
            })
            .expect("sidebar alive");

        // Invalidate the database through the manager seam while the details
        // fetch is in flight.
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();

        let details_key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".to_string(),
            schema: Some("public".to_string()),
            table: "users".to_string(),
        };
        assert_eq!(
            state.read_with(cx, |state, _| state
                .object_tree_outcome(&details_key)
                .cloned()),
            Some(ObjectTreeOutcome::Rejected(
                ObjectTreeRejection::RequestInvalidated
            )),
            "an in-flight result fenced by the invalidation must settle as rejected"
        );
        assert!(
            !state.read_with(cx, |state, _| state
                .connections()
                .get(&profile_id)
                .expect("connected")
                .table_details
                .contains_key(&(
                    "app".to_string(),
                    Some("public".to_string()),
                    "users".to_string()
                ))),
            "a fenced result must never reach the details cache"
        );

        window
            .update(cx, |sidebar, _, _cx| {
                assert!(
                    !sidebar.loading_items.contains(&table_item),
                    "the rejected attempt must release the loading state"
                );
                assert!(
                    sidebar.pending_actions.get(&table_item).is_none(),
                    "a fenced attempt must not keep a pending action alive"
                );
            })
            .expect("sidebar alive");

        // The fenced node settles into an explicit retry, never forever-Loading.
        let retry_id = format!("object-retry|{table_item}");
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, _, _)| *id == retry_id),
            "the fenced table must render an explicit retry row; rows: {rows:?}"
        );
    }

    #[gpui::test]
    async fn profile_retry_restores_schema_before_details_and_visible_columns(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        state.update(cx, |state, cx| {
            state.object_tree_request(
                ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                },
                cx,
            );
        });
        cx.run_until_parked();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        // The primary snapshot can be repopulated with names only while the
        // explicit per-database schema is still absent. It is not the source
        // of truth for the subsequent fenced schema fetch.
        state.update(cx, |state, _| {
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("connected")
                .schema = Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }]));
        });
        let retry_id = format!("object-retry|{table_id}");
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id)
        );
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&retry_id, cx))
            .expect("sidebar alive");
        assert!(
            !build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id),
            "retry action must not remain actionable in flight"
        );
        cx.run_until_parked();
        assert_eq!(
            fake.schema_calls(),
            vec!["app", "app"],
            "retry must reload schema before fetching details"
        );
        assert!(fake.details_calls.load(Ordering::SeqCst) >= 2);
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, _, _)| id == &table_id),
            "table must be visible after retry: {rows:?}"
        );
        assert!(
            rows.iter()
                .any(|(id, label, _)| id.starts_with("CL|") && label.starts_with("id: int4")),
            "loaded column must be visible: {rows:?}"
        );
        assert!(
            !rows.iter().any(|(id, _, _)| id == &retry_id),
            "successful retry must remove placeholder: {rows:?}"
        );
    }

    #[gpui::test]
    async fn profile_retry_removed_table_keeps_honest_action_without_fetch(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "sibling".to_string(),
                    is_current: false,
                },
            ])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        state.update(cx, |state, cx| {
            state.object_tree_request(
                ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                },
                cx,
            );
        });
        cx.run_until_parked();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        let original_calls = fake.details_calls.load(Ordering::SeqCst);
        fake.set_schema("app", fake_db_schema("app", Vec::new()));
        let retry_id = format!("object-retry|{table_id}");
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&retry_id, cx))
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.schema_calls(), vec!["app", "app"]);
        assert_eq!(
            fake.details_calls.load(Ordering::SeqCst),
            original_calls,
            "removed table must not be fetched"
        );
        let rows = build_items(&window, cx);
        assert!(
            !rows.iter().any(|(id, _, _)| id == &retry_id),
            "an authoritative schema without this table must retire its details retry: {rows:?}"
        );
        assert!(
            !rows.iter().any(|(id, _, _)| id == &table_id),
            "removed table must not be projected: {rows:?}"
        );
        let database_id = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        let sibling_id = SchemaNodeId::Database {
            profile_id,
            name: "sibling".to_string(),
        }
        .to_string();
        assert!(
            rows.iter().any(|(id, _, _)| id == &sibling_id),
            "unrelated database must survive: {rows:?}"
        );
        assert!(
            rows.iter().any(|(id, _, _)| id == &database_id),
            "the known database must stay navigable after invalidation: {rows:?}"
        );
        window
            .update(cx, |sidebar, _, cx| {
                let actions =
                    sidebar.build_context_menu_items(SchemaNodeKind::Database, &database_id, cx);
                assert!(
                    actions
                        .iter()
                        .any(|item| matches!(item.action, ContextMenuAction::RefreshDatabase)),
                    "the database must keep its targeted Refresh action"
                );
            })
            .expect("sidebar alive");
        assert!(
            state.read_with(cx, |state, _| state
                .connections()
                .get(&profile_id)
                .and_then(|connected| connected.database_schemas.get("app"))
                .is_some_and(|schema| schema.tables.is_empty())),
            "the refreshed database schema is authoritative and empty"
        );
    }

    #[gpui::test]
    async fn externally_repopulated_details_hide_stale_nested_retry(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake,
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let schema_key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".to_string(),
        };
        state.update(cx, |state, cx| {
            state.object_tree_request(schema_key.clone(), cx);
        });
        cx.run_until_parked();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            state.invalidate_database_schema(profile_id, "app");
        });
        cx.run_until_parked();
        state.update(cx, |state, cx| {
            state.object_tree_retry(schema_key, cx);
        });
        cx.run_until_parked();
        state.update(cx, |state, _| {
            state.set_table_details(
                profile_id,
                "app".to_string(),
                Some("public".to_string()),
                "users".to_string(),
                loaded_details("app", Some("public"), "users"),
            );
        });
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, _, _)| id == &table_id),
            "table must be present: {rows:?}"
        );
        assert!(
            rows.iter()
                .any(|(id, label, _)| id.starts_with("CL|") && label.starts_with("id: int4")),
            "cached column must be shown: {rows:?}"
        );
        assert!(
            !rows
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{table_id}")),
            "complete cached details outrank stale retry: {rows:?}"
        );
        state.update(cx, |state, _| {
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("connected")
                .table_details
                .remove(&(
                    "app".to_string(),
                    Some("public".to_string()),
                    "users".to_string(),
                ));
        });
        window
            .update(cx, |sidebar, _, cx| {
                assert!(
                    matches!(
                        sidebar.ensure_table_details(
                            &table_id,
                            PendingAction::ViewSchema {
                                item_id: table_id.clone()
                            },
                            cx
                        ),
                        TableDetailsStatus::Loading
                    ),
                    "a newly absent cache must request fresh details, not resurrect the old retry"
                );
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn same_uuid_reconnect_cannot_reuse_old_details_retry(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let old = AdapterFakeConnection::lazy();
        old.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            old,
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        state.update(cx, |state, cx| {
            state.object_tree_request(
                ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                },
                cx,
            );
        });
        cx.run_until_parked();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{table_id}"))
        );
        let replacement = AdapterFakeConnection::lazy();
        replacement.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                replacement.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        assert!(
            !build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{table_id}")),
            "old session retry must disappear"
        );
        window
            .update(cx, |sidebar, _, cx| {
                assert!(
                    matches!(
                        sidebar.ensure_table_details(
                            &table_id,
                            PendingAction::ViewSchema {
                                item_id: table_id.clone()
                            },
                            cx
                        ),
                        TableDetailsStatus::Loading
                    ),
                    "new connection must issue its own fenced request"
                );
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(replacement.details_calls.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn same_session_second_consumer_keeps_shared_details_attempt(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".to_string(),
            schema: Some("public".to_string()),
            table: "users".to_string(),
        };
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let cancelled = Arc::new(AtomicUsize::new(0));
        let _recorder = {
            let cancelled = cancelled.clone();
            let state = state.clone();
            cx.update(|cx| {
                cx.new(|cx| {
                    cx.subscribe(
                        &state,
                        move |_, _, event: &dbflux_ui_base::object_tree::ObjectTreeEvent, _| {
                            if matches!(event.outcome, ObjectTreeOutcome::Cancelled) {
                                cancelled.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                    )
                    .detach();
                    EventRecorder
                })
            })
        };
        state.update(cx, |state, cx| {
            assert_eq!(
                state.object_tree_request(key.clone(), cx),
                ObjectTreeRequestStatus::Dispatched
            );
        });
        let old_generation = state.read_with(cx, |state, _| {
            state.object_tree_pending_session_generation(&key)
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
            })
            .expect("sidebar alive");
        assert_eq!(
            state.read_with(cx, |state, _| state
                .object_tree_pending_session_generation(&key)),
            old_generation
        );
        cx.run_until_parked();
        assert_eq!(
            cancelled.load(Ordering::SeqCst),
            0,
            "same-session sidebar must not cancel another consumer"
        );
        assert_eq!(
            fake.details_calls.load(Ordering::SeqCst),
            1,
            "one driver execution for both consumers"
        );
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied)
        );
    }

    #[gpui::test]
    async fn in_flight_old_session_key_is_superseded_before_new_intent(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".to_string(),
            schema: Some("public".to_string()),
            table: "users".to_string(),
        };
        let generated = Arc::new(AtomicUsize::new(0));
        let _recorder = {
            let generated = generated.clone();
            cx.update(|cx| {
                cx.new(|cx| {
                    cx.subscribe(
                        &window.entity(cx).expect("sidebar entity"),
                        move |_, _, event: &crate::SidebarEvent, _| {
                            if matches!(event, crate::SidebarEvent::RequestSqlPreview { .. }) {
                                generated.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                    )
                    .detach();
                    EventRecorder
                })
            })
        };
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
            })
            .expect("sidebar alive");
        assert!(
            state.read_with(cx, |state, _| state.object_tree_is_pending(&key)),
            "old session request must be pending before reinstall"
        );
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::GenerateCode {
                            item_id: table_id.clone(),
                            generator_id: "select_where".to_string()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(
            fake.details_calls.load(Ordering::SeqCst),
            1,
            "only the new session may execute after superseding queued old work"
        );
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied)
        );
        assert!(state.read_with(cx, |state, _| {
            state
                .connections()
                .get(&profile_id)
                .expect("connected")
                .table_details
                .contains_key(&(
                    "app".to_string(),
                    Some("public".to_string()),
                    "users".to_string(),
                ))
        }));
        assert_eq!(
            generated.load(Ordering::SeqCst),
            1,
            "only the new GenerateCode intent may execute"
        );
        assert!(
            !build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{table_id}")),
            "obsolete completion must not create a retry"
        );
    }

    #[gpui::test]
    async fn same_arc_reinstall_retires_retry_and_known_database(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        state.update(cx, |state, cx| {
            state.object_tree_request(
                ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                },
                cx,
            );
        });
        cx.run_until_parked();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{table_id}"))
        );
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake.clone(),
                Some(snapshot_naming(Vec::new())),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        let rows = build_items(&window, cx);
        assert!(
            !rows
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{table_id}")),
            "same Arc new session must not retain details retry: {rows:?}"
        );
        assert!(
            !rows.iter().any(|(id, _, _)| id
                == &SchemaNodeId::Database {
                    profile_id,
                    name: "app".to_string()
                }
                .to_string()),
            "same Arc new session must not retain known DB: {rows:?}"
        );
        window
            .update(cx, |sidebar, _, cx| {
                assert!(
                    matches!(
                        sidebar.ensure_table_details(
                            &table_id,
                            PendingAction::ViewSchema {
                                item_id: table_id.clone()
                            },
                            cx
                        ),
                        TableDetailsStatus::Loading
                    ),
                    "new session should request details"
                );
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.details_calls.load(Ordering::SeqCst), 2);
    }

    #[gpui::test]
    async fn removed_known_database_does_not_revive_after_list_cache_churn(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.invalidate_database_cache(profile_id, "app", cx)
            })
            .expect("sidebar alive");
        let db_id = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &db_id)
        );
        *fake.databases.lock().expect("databases") = vec![dbflux_core::DatabaseInfo {
            name: "sibling".to_string(),
            is_current: true,
        }];
        state.update(cx, |state, cx| {
            state.object_tree_request(ObjectTreeRequestKey::DatabaseList { profile_id }, cx);
        });
        cx.run_until_parked();
        window
            .update(cx, |sidebar, _, _| {
                assert!(
                    !sidebar
                        .known_invalidated_databases
                        .borrow()
                        .contains_key(&(profile_id, "app".to_string())),
                    "list settlement must retire the known target before any later cache churn"
                );
            })
            .expect("sidebar alive");
        assert!(
            !build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &db_id),
            "authoritative list removes app"
        );
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake.clone(),
                Some(snapshot_naming(Vec::new())),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        let rows = build_items(&window, cx);
        assert!(
            !rows.iter().any(|(id, _, _)| id == &db_id),
            "removed known target must not revive after list cache clears: {rows:?}"
        );
    }

    #[gpui::test]
    async fn in_flight_details_use_latest_explicit_pending_action(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        state.update(cx, |state, cx| {
            state.object_tree_request(
                ObjectTreeRequestKey::DatabaseSchema {
                    profile_id,
                    database: "app".to_string(),
                },
                cx,
            );
        });
        cx.run_until_parked();
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let generated = Arc::new(AtomicUsize::new(0));
        let _recorder = {
            let generated = generated.clone();
            cx.update(|cx| {
                cx.new(|cx| {
                    cx.subscribe(
                        &window.entity(cx).expect("sidebar entity"),
                        move |_, _, event: &crate::SidebarEvent, _| {
                            if matches!(event, crate::SidebarEvent::RequestSqlPreview { .. }) {
                                generated.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                    )
                    .detach();
                    EventRecorder
                })
            })
        };
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::GenerateCode {
                            item_id: table_id.clone(),
                            generator_id: "select_where".to_string()
                        },
                        cx
                    ),
                    TableDetailsStatus::Loading
                ));
                assert!(
                    matches!(
                        sidebar.pending_actions.get(&table_id),
                        Some(PendingAction::GenerateCode { .. })
                    ),
                    "new explicit action must supersede the old one while deduplicating the fetch"
                );
            })
            .expect("sidebar alive");
        assert_eq!(
            fake.details_calls.load(Ordering::SeqCst),
            0,
            "the request remains in flight"
        );
        cx.run_until_parked();
        assert_eq!(
            fake.details_calls.load(Ordering::SeqCst),
            1,
            "only one shared fetch is needed"
        );
        assert_eq!(
            generated.load(Ordering::SeqCst),
            1,
            "settlement must execute the latest explicit intent"
        );
    }

    #[gpui::test]
    async fn collapse_during_fetch_stays_collapsed_and_never_cancels_shared_work(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        *fake.databases.lock().expect("databases") = vec![dbflux_core::DatabaseInfo {
            name: "app".to_string(),
            is_current: true,
        }];
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );

        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));

        let db_item = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, true, cx);
            })
            .expect("sidebar alive");

        // Collapse while the shared fetch is in flight.
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, false, cx);
            })
            .expect("sidebar alive");

        let schema_key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".to_string(),
        };
        assert!(
            state.read_with(cx, |state, _| state.object_tree_is_pending(&schema_key)),
            "sidebar must request the shared schema key"
        );
        let received = Arc::new(AtomicUsize::new(0));
        let _observer = {
            let received = received.clone();
            let observed_key = schema_key.clone();
            cx.update(|cx| {
                cx.new(|cx| {
                    cx.subscribe(
                        &state,
                        move |_, _, event: &dbflux_ui_base::object_tree::ObjectTreeEvent, _| {
                            if event.key == observed_key {
                                received.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                    )
                    .detach();
                    EventRecorder
                })
            })
        };
        assert!(
            state.update(cx, |state, cx| state
                .object_tree_request(schema_key.clone(), cx))
                == ObjectTreeRequestStatus::Pending,
            "second consumer must join the pending request"
        );
        assert!(
            state.read_with(cx, |state, _| state.object_tree_is_pending(&schema_key)),
            "a sidebar collapse must never cancel another consumer's shared request"
        );

        cx.run_until_parked();
        assert_eq!(
            fake.schema_calls(),
            vec!["app"],
            "two consumers share one fetch"
        );
        assert_eq!(
            received.load(Ordering::SeqCst),
            1,
            "second subscriber receives settlement"
        );

        assert!(
            state.read_with(cx, |state, _| state
                .connections()
                .get(&profile_id)
                .expect("connected")
                .cache_contains(&dbflux_core::CacheKey::database_schema("app"))),
            "the shared request must complete despite the collapse"
        );

        let rows = build_items(&window, cx);
        let collapsed = rows
            .iter()
            .find(|(id, _, _)| *id == db_item)
            .map(|(_, _, expanded)| *expanded);
        assert_eq!(
            collapsed,
            Some(false),
            "a database collapsed during its load must stay collapsed after completion"
        );
    }

    #[gpui::test]
    async fn empty_schema_settles_without_loading_and_keeps_database_identity(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema("", fake_db_schema("", Vec::new()));
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let db_item = SchemaNodeId::Database {
            profile_id,
            name: String::new(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, true, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        let key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: String::new(),
        };
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached)
        ));
        assert_eq!(
            state.read_with(cx, |state, _| {
                dbflux_ui_base::object_tree::project_database_node(state, profile_id, "")
                    .expect("implicit database")
                    .state
            }),
            dbflux_ui_base::object_tree::NodeContent::Empty
        );
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, _, _)| id == &db_item),
            "stable empty database ID: {rows:?}"
        );
        assert!(
            !rows
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{db_item}"))
        );
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, false, cx);
                sidebar.set_expanded(&db_item, true, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(
            fake.schema_calls(),
            vec![""],
            "cached empty schema must not reload"
        );
    }

    #[gpui::test]
    async fn schema_invalidation_rejection_keeps_explicit_retry_reachable(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        *fake.databases.lock().expect("databases") = vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }];
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let db_item = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        let retry_id = format!("object-retry|{db_item}");
        let key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".to_string(),
        };
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, true, cx);
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Rejected(
                ObjectTreeRejection::RequestInvalidated
            ))
        ));
        let rows = build_items(&window, cx);
        assert!(rows.iter().any(|(id, _, _)| id == &profile_item));
        assert_eq!(
            rows.iter().filter(|(id, _, _)| id == &retry_id).count(),
            1,
            "one reachable profile retry when the DB ancestor is gone: {rows:?}"
        );
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_tree(cx);
                sidebar.set_expanded(&profile_item, false, cx);
                sidebar.set_expanded(&profile_item, true, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.schema_calls().len(), 1, "rebuild must not retry");
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&retry_id, cx))
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.schema_calls().len(), 2);
        let rows = build_items(&window, cx);
        assert!(
            rows.iter().any(|(id, _, _)| id == &db_item),
            "recovered DB: {rows:?}"
        );
        assert!(
            !rows.iter().any(|(id, _, _)| id == &retry_id),
            "retry must retire: {rows:?}"
        );
    }

    #[gpui::test]
    async fn document_schema_retry_restores_collection_nodes_after_invalidation(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let mut fake = AdapterFakeConnection::lazy();
        Arc::get_mut(&mut fake)
            .expect("unique fake")
            .metadata
            .category = DatabaseCategory::Document;
        let mut collection = fake_table("app", "users");
        collection.schema = None;
        collection.child_items = Some(vec![dbflux_core::CollectionChildInfo {
            id: "child-1".to_string(),
            label: "child-1".to_string(),
            last_event_ts_ms: None,
            presentation: CollectionPresentation::DataGrid,
        }]);
        fake.set_schema("app", fake_db_schema("app", vec![collection]));
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let database_id = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        let retry_id = format!("object-retry|{database_id}");
        let key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".to_string(),
        };
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&database_id, true, cx);
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Rejected(
                ObjectTreeRejection::RequestInvalidated
            ))
        ));
        assert_eq!(
            build_items(&window, cx)
                .iter()
                .filter(|(id, _, _)| id == &retry_id)
                .count(),
            1
        );
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&retry_id, cx))
            .expect("sidebar alive");
        cx.run_until_parked();
        let rows = build_items(&window, cx);
        let collection_id = SchemaNodeId::Collection {
            profile_id,
            database: "app".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let relational_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "app".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        assert_eq!(
            rows.iter().filter(|(id, _, _)| id == &database_id).count(),
            1,
            "recovered database must not duplicate: {rows:?}"
        );
        assert!(
            rows.iter().any(|(id, _, _)| id == &collection_id),
            "document schema must render collection IDs: {rows:?}"
        );
        assert!(
            !rows
                .iter()
                .any(|(id, _, _)| id == &relational_id || id == &retry_id),
            "no relational table or obsolete retry: {rows:?}"
        );
        let opened = Arc::new(AtomicUsize::new(0));
        let sidebar_entity = window.entity(cx).expect("sidebar entity");
        let _observer = {
            let opened = opened.clone();
            cx.update(|cx| cx.new(|cx| {
                cx.subscribe(&sidebar_entity, move |_, _, event: &crate::SidebarEvent, _| {
                    if matches!(event, crate::SidebarEvent::OpenCollection {
                        profile_id: opened_profile, collection
                    } if *opened_profile == profile_id && collection.database == "app" && collection.name == "users") {
                        opened.fetch_add(1, Ordering::SeqCst);
                    }
                }).detach();
                EventRecorder
            }))
        };
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.execute_item(&collection_id, cx)
            })
            .expect("sidebar alive");
        assert_eq!(
            opened.load(Ordering::SeqCst),
            1,
            "collection action must remain routed"
        );
    }

    #[gpui::test]
    async fn schema_retry_does_not_resurrect_database_removed_by_authoritative_list(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema("app", fake_db_schema("app", Vec::new()));
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let db_item = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, true, cx);
                sidebar.invalidate_database_cache(profile_id, "app", cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        *fake.databases.lock().expect("databases") = vec![dbflux_core::DatabaseInfo {
            name: "other".to_string(),
            is_current: true,
        }];
        state.update(cx, |state, cx| {
            state.object_tree_request(ObjectTreeRequestKey::DatabaseList { profile_id }, cx);
        });
        cx.run_until_parked();
        assert!(state.read_with(cx, |state, _| {
            state
                .get_database_list(profile_id)
                .is_some_and(|list| list.len() == 1 && list[0].name == "other")
        }));
        let rows = build_items(&window, cx);
        assert!(
            !rows
                .iter()
                .any(|(id, _, _)| id == &format!("object-retry|{db_item}")),
            "authoritative removal must hide stale retry: {rows:?}"
        );
        assert!(
            !rows.iter().any(|(id, _, _)| id == &db_item),
            "removed database must not reappear: {rows:?}"
        );
    }

    #[gpui::test]
    async fn same_arc_reconnect_schema_flight_rejects_stale_work_and_allows_explicit_retry(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let db_item = SchemaNodeId::Database {
            profile_id,
            name: "app".to_string(),
        }
        .to_string();
        let key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".to_string(),
        };
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(&db_item, true, cx);
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        cx.run_until_parked();
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Rejected(_))
        ));
        let retry_id = format!("object-retry|{db_item}");
        window
            .update(cx, |sidebar, _, _| {
                assert!(
                    sidebar.pending_actions.get(&db_item).is_none(),
                    "stale schema completion must not restore a selection action"
                );
            })
            .expect("sidebar alive");
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id)
        );
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_tree(cx);
                sidebar.set_expanded(&db_item, false, cx);
                sidebar.set_expanded(&db_item, true, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(
            build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id)
        );
        window
            .update(cx, |sidebar, _, cx| sidebar.execute_item(&retry_id, cx))
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached)
        ));
        assert!(
            !build_items(&window, cx)
                .iter()
                .any(|(id, _, _)| id == &retry_id)
        );
    }

    #[gpui::test]
    async fn explicit_database_refresh_fences_shared_schema_and_preserves_active_context(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "new")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "other".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.set_active_database(profile_id, Some("other".into()));
            let connected = state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("connected");
            connected.populate_dependents(
                "app",
                Some("public".into()),
                "orders",
                vec![dbflux_core::RelationRef {
                    kind: dbflux_core::RelationKind::View,
                    qualified_name: "public.old_view".into(),
                }],
            );
            connected.populate_dependents(
                "other",
                Some("public".into()),
                "orders",
                vec![dbflux_core::RelationRef {
                    kind: dbflux_core::RelationKind::View,
                    qualified_name: "public.sibling_view".into(),
                }],
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".into(),
        };
        state.update(cx, |state, cx| {
            assert_eq!(
                state.object_tree_request(key.clone(), cx),
                ObjectTreeRequestStatus::Dispatched
            );
        });
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Rejected(_))
        ));
        assert_eq!(
            state.read_with(cx, |state, _| state.get_active_database(profile_id)),
            Some("other".into())
        );
        assert!(state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert!(
                connected
                    .dependents("app", Some("public"), "orders")
                    .is_empty()
            );
            assert_eq!(
                connected.dependents("other", Some("public"), "orders")[0].qualified_name,
                "public.sibling_view"
            );
            connected
                .database_schemas
                .get("app")
                .is_some_and(|schema| schema.tables.iter().any(|table| table.name == "new"))
        }));
    }

    #[gpui::test]
    async fn cancelled_refresh_does_not_replace_newer_shared_schema(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "fresh")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake,
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "other".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema("app", vec![fake_table("public", "held")]),
            );
            state.set_database_schema(
                profile_id,
                "other".into(),
                fake_db_schema("other", vec![fake_table("public", "sibling")]),
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            let params = state
                .prepare_fetch_explicit_database_schema(profile_id, "app")
                .expect("shared prepare");
            let fetched = params.execute().expect("shared fetch");
            assert_eq!(
                state.apply_fetch_explicit_database_schema(fetched),
                dbflux_core::ApplyFetchOutcome::Applied
            );
        });
        let task_id = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("refresh task")
                .id
        });
        state.update(cx, |state, _| assert!(state.cancel_task(task_id)));
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert!(
                connected
                    .database_schemas
                    .get("app")
                    .is_some_and(|schema| schema.tables.iter().any(|table| table.name == "fresh"))
            );
            assert!(
                connected
                    .database_schemas
                    .get("other")
                    .is_some_and(|schema| schema
                        .tables
                        .iter()
                        .any(|table| table.name == "sibling"))
            );
            assert!(!state.is_operation_pending(profile_id, Some("app")));
            assert!(
                dbflux_ui_base::object_tree::project_database_node(state, profile_id, "app")
                    .expect("database")
                    .find(&dbflux_ui_base::object_tree::ObjectTreeKey::Table {
                        profile_id,
                        database: "app".into(),
                        schema: Some("public".into()),
                        table: "fresh".into()
                    })
                    .is_some()
            );
        });
    }

    #[gpui::test]
    async fn failed_refresh_preserves_newer_shared_schema(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "fresh")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "other".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema("app", vec![fake_table("public", "held")]),
            );
            state.set_database_schema(
                profile_id,
                "other".into(),
                fake_db_schema("other", vec![fake_table("public", "sibling")]),
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            let params = state
                .prepare_fetch_explicit_database_schema(profile_id, "app")
                .expect("shared prepare");
            let fetched = params.execute().expect("shared fetch");
            assert_eq!(
                state.apply_fetch_explicit_database_schema(fetched),
                dbflux_core::ApplyFetchOutcome::Applied
            );
        });
        fake.fail_schema_once("app");
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert!(
                connected
                    .database_schemas
                    .get("app")
                    .is_some_and(|schema| schema.tables.iter().any(|table| table.name == "fresh"))
            );
            assert!(
                connected
                    .database_schemas
                    .get("other")
                    .is_some_and(|schema| schema
                        .tables
                        .iter()
                        .any(|table| table.name == "sibling"))
            );
            assert!(!state.is_operation_pending(profile_id, Some("app")));
            assert!(
                dbflux_ui_base::object_tree::project_database_node(state, profile_id, "app")
                    .expect("database")
                    .find(&dbflux_ui_base::object_tree::ObjectTreeKey::Table {
                        profile_id,
                        database: "app".into(),
                        schema: Some("public".into()),
                        table: "fresh".into()
                    })
                    .is_some()
            );
        });
        assert_eq!(
            fake.schema_calls().len(),
            2,
            "shared fetch and failed refresh must both execute"
        );
        assert_eq!(
            state.read_with(cx, |state, _| state.unread_error_count),
            0,
            "sidebar-owned error toast must not double-report"
        );
    }

    #[gpui::test]
    async fn failed_explicit_refresh_restores_held_state_without_switching_database(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.fail_schema_once("app");
        connect_profile(
            &state,
            cx,
            profile_id,
            fake,
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "other".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.set_active_database(profile_id, Some("other".into()));
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema("app", vec![fake_table("public", "held")]),
            );
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("session")
                .populate_dependents(
                    "app",
                    Some("public".into()),
                    "orders",
                    vec![dbflux_core::RelationRef {
                        kind: dbflux_core::RelationKind::View,
                        qualified_name: "public.old_view".into(),
                    }],
                );
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("session")
                .populate_dependents(
                    "app",
                    Some("public".into()),
                    "invoices",
                    vec![dbflux_core::RelationRef {
                        kind: dbflux_core::RelationKind::View,
                        qualified_name: "public.held_invoice_view".into(),
                    }],
                );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("session")
                .populate_dependents(
                    "app",
                    Some("public".into()),
                    "orders",
                    vec![dbflux_core::RelationRef {
                        kind: dbflux_core::RelationKind::View,
                        qualified_name: "public.fresh_view".into(),
                    }],
                );
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("session");
            assert_eq!(
                connected.dependents("app", Some("public"), "orders")[0].qualified_name,
                "public.fresh_view"
            );
            assert_eq!(
                connected.dependents("app", Some("public"), "invoices")[0].qualified_name,
                "public.held_invoice_view"
            );
            assert_eq!(connected.active_database.as_deref(), Some("other"));
            assert!(
                connected
                    .database_schemas
                    .get("app")
                    .is_some_and(|schema| schema.tables.iter().any(|table| table.name == "held"))
            );
            assert!(!state.is_operation_pending(profile_id, Some("app")));
        });
    }

    #[gpui::test]
    async fn reconnect_during_explicit_refresh_never_restores_old_cache(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "old")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema("app", vec![fake_table("public", "held")]),
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
            state.set_active_database(profile_id, Some("replacement-active".into()));
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("new session");
            assert_eq!(
                connected.active_database.as_deref(),
                Some("replacement-active")
            );
            assert!(
                !connected.database_schemas.contains_key("app"),
                "old refresh/rollback must not write to new session"
            );
        });
    }

    #[gpui::test]
    async fn current_primary_database_refresh_keeps_active_context(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::connection_per_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state.set_active_database(profile_id, Some("app".into()))
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(
            fake.primary_schema_calls.load(Ordering::SeqCst),
            1,
            "current primary mode must execute schema() on the primary connection"
        );
        state.read_with(cx, |state, _| {
            assert_eq!(
                state.get_active_database(profile_id).as_deref(),
                Some("app")
            );
            assert!(
                state
                    .connections()
                    .get(&profile_id)
                    .expect("connected")
                    .schema
                    .is_some()
            );
            assert!(!state.is_operation_pending(profile_id, Some("app")));
        });
    }

    #[gpui::test]
    async fn cancelled_lazy_refresh_restores_held_schema_and_active_context(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "fresh")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake,
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "other".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.set_active_database(profile_id, Some("other".into()));
            state.set_database_schema(
                profile_id,
                "app".into(),
                fake_db_schema("app", vec![fake_table("public", "held")]),
            );
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "app".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        let task_id = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("refresh task")
                .id
        });
        state.update(cx, |state, _| assert!(state.cancel_task(task_id)));
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert_eq!(connected.active_database.as_deref(), Some("other"));
            assert!(
                connected
                    .database_schemas
                    .get("app")
                    .is_some_and(|schema| schema.tables.iter().any(|table| table.name == "held"))
            );
            assert!(!state.is_operation_pending(profile_id, Some("app")));
        });
    }

    #[gpui::test]
    async fn secondary_database_refresh_replaces_slot_without_activating_target(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let (connect_calls, _) = register_per_database_driver(&state, cx);
        let profile_id = Uuid::new_v4();
        let primary = AdapterFakeConnection::connection_per_database();
        let original = AdapterFakeConnection::connection_per_database();
        let old_slot_address = Arc::as_ptr(&original) as usize;
        connect_profile(
            &state,
            cx,
            profile_id,
            primary,
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "main".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "reporting".into(),
                    is_current: false,
                },
            ])),
        );
        state.update(cx, |state, _| {
            state.add_database_connection(
                profile_id,
                "reporting".into(),
                original.clone(),
                Some(secondary_snapshot("held")),
            );
            state.set_active_database(profile_id, Some("main".into()));
        });
        drop(original);
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "reporting".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(
            connect_calls.load(Ordering::SeqCst),
            1,
            "secondary refresh must prepare and execute a new database connection"
        );
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert_eq!(connected.active_database.as_deref(), Some("main"));
            let slot = connected
                .database_connections
                .get("reporting")
                .expect("new target slot");
            assert_ne!(
                Arc::as_ptr(&slot.connection) as *const () as usize,
                old_slot_address,
                "refresh must replace the original slot"
            );
            let schema = slot.schema.as_ref().expect("new target schema");
            let dbflux_core::DataStructure::Relational(relational) = &schema.structure else {
                panic!("relational schema expected");
            };
            assert!(
                relational
                    .schemas
                    .iter()
                    .any(|schema| schema.tables.iter().any(|table| table.name == "fresh"))
            );
            assert!(
                !relational
                    .schemas
                    .iter()
                    .any(|schema| schema.tables.iter().any(|table| table.name == "held"))
            );
            assert!(!state.is_operation_pending(profile_id, Some("reporting")));
        });
    }

    #[gpui::test]
    async fn secondary_refresh_same_arc_reconnect_preserves_replacement_slot_and_active_context(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let (connect_calls, _) = register_per_database_driver(&state, cx);
        let profile_id = Uuid::new_v4();
        let primary = AdapterFakeConnection::connection_per_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            primary.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "main".into(),
                is_current: true,
            }])),
        );
        let held = AdapterFakeConnection::connection_per_database();
        state.update(cx, |state, _| {
            state.add_database_connection(
                profile_id,
                "reporting".into(),
                held.clone(),
                Some(secondary_snapshot("held")),
            );
            state.set_active_database(profile_id, Some("main".into()));
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("connected")
                .populate_dependents(
                    "reporting",
                    Some("public".into()),
                    "orders",
                    vec![dbflux_core::RelationRef {
                        kind: dbflux_core::RelationKind::View,
                        qualified_name: "public.held_view".into(),
                    }],
                );
        });
        drop(held);
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "reporting".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        let new_slot = AdapterFakeConnection::connection_per_database();
        let new_slot_address = Arc::as_ptr(&new_slot) as usize;
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                primary.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "main".into(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
            state.add_database_connection(
                profile_id,
                "reporting".into(),
                new_slot.clone(),
                Some(secondary_snapshot("replacement")),
            );
            state.set_active_database(profile_id, Some("replacement-active".into()));
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("replacement")
                .populate_dependents(
                    "reporting",
                    Some("public".into()),
                    "orders",
                    vec![dbflux_core::RelationRef {
                        kind: dbflux_core::RelationKind::View,
                        qualified_name: "public.replacement_view".into(),
                    }],
                );
            assert!(
                state.start_pending_operation(profile_id, Some("reporting")),
                "the replacement session must not inherit old pending work"
            );
        });
        drop(new_slot);
        cx.run_until_parked();
        assert_eq!(
            connect_calls.load(Ordering::SeqCst),
            1,
            "queued refresh must have executed the old driver after reconnect"
        );
        state.read_with(cx, |state, _| {
            let connected = state
                .connections()
                .get(&profile_id)
                .expect("replacement session");
            assert_eq!(
                connected.active_database.as_deref(),
                Some("replacement-active")
            );
            assert!(
                state.is_operation_pending(profile_id, Some("reporting")),
                "old completion must not clear replacement pending work"
            );
            assert_eq!(
                connected.dependents("reporting", Some("public"), "orders")[0].qualified_name,
                "public.replacement_view"
            );
            let slot = connected
                .database_connections
                .get("reporting")
                .expect("replacement slot");
            assert_eq!(
                Arc::as_ptr(&slot.connection) as *const () as usize,
                new_slot_address
            );
            let dbflux_core::DataStructure::Relational(relational) =
                &slot.schema.as_ref().expect("replacement schema").structure
            else {
                panic!("relational schema expected");
            };
            assert!(relational.schemas.iter().any(|schema| {
                schema
                    .tables
                    .iter()
                    .any(|table| table.name == "replacement")
            }));
            assert!(!relational.schemas.iter().any(|schema| {
                schema
                    .tables
                    .iter()
                    .any(|table| table.name == "held" || table.name == "fresh")
            }));
        });
        state.update(cx, |state, _| {
            state.finish_pending_operation(profile_id, Some("reporting"));
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        assert!(
            state.read_with(cx, |state, _| state
                .is_operation_pending(profile_id, Some("reporting"))),
            "new session must start a fresh target refresh"
        );
        cx.run_until_parked();
        assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
        state.read_with(cx, |state, _| {
            let connected = state
                .connections()
                .get(&profile_id)
                .expect("replacement session");
            assert_eq!(
                connected.active_database.as_deref(),
                Some("replacement-active")
            );
            assert!(
                connected
                    .dependents("reporting", Some("public"), "orders")
                    .is_empty()
            );
            assert!(!state.is_operation_pending(profile_id, Some("reporting")));
        });
    }

    #[gpui::test]
    async fn stale_click_after_same_arc_reconnect_cannot_install_or_clear_new_pending(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let (connect_calls, _) = register_per_database_driver(&state, cx);
        let profile_id = Uuid::new_v4();
        let primary = AdapterFakeConnection::connection_per_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            primary.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "main".into(),
                is_current: true,
            }])),
        );
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "analytics".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.handle_database_click(&item_id, cx)
            })
            .expect("sidebar alive");
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "new session",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                primary.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "main".into(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
            state.set_active_database(profile_id, Some("main".into()));
            assert!(state.start_pending_operation(profile_id, Some("analytics")));
        });
        cx.run_until_parked();
        assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("new session");
            assert!(connected.database_connections.get("analytics").is_none());
            assert_eq!(connected.active_database.as_deref(), Some("main"));
            assert!(state.is_operation_pending(profile_id, Some("analytics")));
        });
        state.update(cx, |state, _| {
            state.finish_pending_operation(profile_id, Some("analytics"))
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.handle_database_click(&item_id, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("new session");
            assert!(connected.database_connections.get("analytics").is_some());
            assert_eq!(connected.active_database.as_deref(), Some("analytics"));
            assert!(!state.is_operation_pending(profile_id, Some("analytics")));
        });
    }

    #[gpui::test]
    async fn same_session_slot_churn_releases_stale_refresh_pending(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let (connect_calls, _) = register_per_database_driver(&state, cx);
        let profile_id = Uuid::new_v4();
        let primary = AdapterFakeConnection::connection_per_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            primary,
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "main".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "analytics".into(),
                    is_current: false,
                },
                dbflux_core::DatabaseInfo {
                    name: "reporting".into(),
                    is_current: false,
                },
            ])),
        );
        let held = AdapterFakeConnection::connection_per_database();
        state.update(cx, |state, _| {
            state.add_database_connection(
                profile_id,
                "analytics".into(),
                held,
                Some(secondary_snapshot("held")),
            );
            state.set_active_database(profile_id, Some("reporting".into()));
            assert!(state.start_pending_operation(profile_id, Some("reporting")));
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "analytics".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        let replacement = AdapterFakeConnection::connection_per_database();
        let replacement_address = Arc::as_ptr(&replacement) as usize;
        state.update(cx, |state, _| {
            state.add_database_connection(
                profile_id,
                "analytics".into(),
                replacement,
                Some(secondary_snapshot("replacement")),
            )
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert!(
                !state.is_operation_pending(profile_id, Some("analytics")),
                "stale refresh must release its own pending marker"
            );
            assert!(state.is_operation_pending(profile_id, Some("reporting")));
            assert_eq!(connected.active_database.as_deref(), Some("reporting"));
            let slot = connected
                .database_connections
                .get("analytics")
                .expect("replacement");
            assert_eq!(
                Arc::as_ptr(&slot.connection) as *const () as usize,
                replacement_address
            );
            assert!(slot.schema.is_some());
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        assert!(state.read_with(cx, |state, _| {
            state.is_operation_pending(profile_id, Some("analytics"))
        }));
        cx.run_until_parked();
        assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
    }

    #[gpui::test]
    async fn same_session_slot_churn_releases_stale_database_click(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let (connect_calls, _) = register_per_database_driver(&state, cx);
        let profile_id = Uuid::new_v4();
        let primary = AdapterFakeConnection::connection_per_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            primary,
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "main".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state.set_active_database(profile_id, Some("main".into()))
        });
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        let item_id = SchemaNodeId::Database {
            profile_id,
            name: "analytics".into(),
        }
        .to_string();
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.handle_database_click(&item_id, cx)
            })
            .expect("sidebar alive");
        let replacement = AdapterFakeConnection::connection_per_database();
        let replacement_address = Arc::as_ptr(&replacement) as usize;
        state.update(cx, |state, _| {
            state.add_database_connection(
                profile_id,
                "analytics".into(),
                replacement,
                Some(secondary_snapshot("replacement")),
            )
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            assert!(
                !state.is_operation_pending(profile_id, Some("analytics")),
                "stale click must release pending"
            );
            assert_eq!(connected.active_database.as_deref(), Some("main"));
            assert_eq!(
                Arc::as_ptr(
                    &connected
                        .database_connections
                        .get("analytics")
                        .expect("replacement")
                        .connection
                ) as *const () as usize,
                replacement_address
            );
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_database(&item_id, cx)
            })
            .expect("sidebar alive");
        assert!(state.read_with(cx, |state, _| {
            state.is_operation_pending(profile_id, Some("analytics"))
        }));
        cx.run_until_parked();
        assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
    }

    #[gpui::test]
    async fn secondary_refresh_settle_preserves_newer_database_click(cx: &mut TestAppContext) {
        for settle in ["failure", "cancel", "success"] {
            let state = test_app_state(cx);
            let (_, fail_next_connect) = register_per_database_driver(&state, cx);
            let profile_id = Uuid::new_v4();
            let primary = AdapterFakeConnection::connection_per_database();
            let analytics = AdapterFakeConnection::connection_per_database();
            let analytics_address = Arc::as_ptr(&analytics) as usize;
            connect_profile(
                &state,
                cx,
                profile_id,
                primary,
                Some(snapshot_naming(vec![
                    dbflux_core::DatabaseInfo {
                        name: "main".into(),
                        is_current: true,
                    },
                    dbflux_core::DatabaseInfo {
                        name: "analytics".into(),
                        is_current: false,
                    },
                    dbflux_core::DatabaseInfo {
                        name: "reporting".into(),
                        is_current: false,
                    },
                ])),
            );
            state.update(cx, |state, _| {
                state.add_database_connection(
                    profile_id,
                    "analytics".into(),
                    analytics.clone(),
                    Some(secondary_snapshot("held")),
                );
                state.add_database_connection(
                    profile_id,
                    "reporting".into(),
                    AdapterFakeConnection::connection_per_database(),
                    Some(secondary_snapshot("reports")),
                );
                state.set_active_database(profile_id, Some("main".into()));
            });
            drop(analytics);
            let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
            let item_id = SchemaNodeId::Database {
                profile_id,
                name: "analytics".into(),
            }
            .to_string();
            if settle == "failure" {
                fail_next_connect.store(true, Ordering::SeqCst);
            }
            window
                .update(cx, |sidebar, _, cx| {
                    sidebar.refresh_schema_database(&item_id, cx)
                })
                .expect("sidebar alive");
            let task_id = state.read_with(cx, |state, _| {
                state
                    .tasks()
                    .running_tasks()
                    .into_iter()
                    .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                    .expect("refresh task")
                    .id
            });
            window
                .update(cx, |sidebar, _, cx| {
                    sidebar.handle_database_click(
                        &SchemaNodeId::Database {
                            profile_id,
                            name: "reporting".into(),
                        }
                        .to_string(),
                        cx,
                    )
                })
                .expect("sidebar alive");
            state.read_with(cx, |state, _| {
                assert_eq!(
                    state.get_active_database(profile_id).as_deref(),
                    Some("reporting")
                );
                assert!(state.is_operation_pending(profile_id, Some("analytics")));
            });
            if settle == "cancel" {
                state.update(cx, |state, _| assert!(state.cancel_task(task_id)));
            }
            cx.run_until_parked();
            state.read_with(cx, |state, _| {
                let connected = state.connections().get(&profile_id).expect("connected");
                assert_eq!(
                    connected.active_database.as_deref(),
                    Some("reporting"),
                    "{settle}"
                );
                assert!(
                    !state.is_operation_pending(profile_id, Some("analytics")),
                    "{settle}"
                );
                assert!(connected.database_connections.contains_key("reporting"));
                let slot = connected
                    .database_connections
                    .get("analytics")
                    .expect("analytics restored/refreshed");
                if settle != "success" {
                    assert_eq!(
                        Arc::as_ptr(&slot.connection) as *const () as usize,
                        analytics_address,
                        "{settle}"
                    );
                    assert!(slot.schema.is_some());
                }
            });
            assert!(
                build_items(&window, cx)
                    .iter()
                    .any(|(id, _, _)| id == &item_id)
            );
        }
    }

    #[gpui::test]
    async fn secondary_refresh_failure_and_cancel_restore_held_slot(cx: &mut TestAppContext) {
        for cancel in [false, true] {
            let state = test_app_state(cx);
            let (connect_calls, fail_next_connect) = register_per_database_driver(&state, cx);
            let profile_id = Uuid::new_v4();
            let original = AdapterFakeConnection::connection_per_database();
            let original_address = Arc::as_ptr(&original) as usize;
            connect_profile(
                &state,
                cx,
                profile_id,
                AdapterFakeConnection::connection_per_database(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "main".into(),
                    is_current: true,
                }])),
            );
            state.update(cx, |state, _| {
                state.add_database_connection(
                    profile_id,
                    "reporting".into(),
                    original.clone(),
                    Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                        name: "reporting".into(),
                        is_current: true,
                    }])),
                );
                state.set_active_database(profile_id, Some("main".into()));
            });
            drop(original);
            let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
            let item_id = SchemaNodeId::Database {
                profile_id,
                name: "reporting".into(),
            }
            .to_string();
            if !cancel {
                fail_next_connect.store(true, Ordering::SeqCst);
            }
            window
                .update(cx, |sidebar, _, cx| {
                    sidebar.refresh_schema_database(&item_id, cx)
                })
                .expect("sidebar alive");
            if cancel {
                let task_id = state.read_with(cx, |state, _| {
                    state
                        .tasks()
                        .running_tasks()
                        .into_iter()
                        .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                        .expect("refresh task")
                        .id
                });
                state.update(cx, |state, _| assert!(state.cancel_task(task_id)));
            }
            cx.run_until_parked();
            assert_eq!(connect_calls.load(Ordering::SeqCst), usize::from(!cancel));
            state.read_with(cx, |state, _| {
                let connected = state.connections().get(&profile_id).expect("connected");
                assert_eq!(connected.active_database.as_deref(), Some("main"));
                let slot = connected
                    .database_connections
                    .get("reporting")
                    .expect("restored slot");
                assert_eq!(
                    Arc::as_ptr(&slot.connection) as *const () as usize,
                    original_address
                );
                assert!(slot.schema.is_some());
                assert!(!state.is_operation_pending(profile_id, Some("reporting")));
            });
        }
    }

    #[gpui::test]
    async fn cached_reopen_skips_driver_and_same_named_tables_stay_distinct(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::lazy();
        *fake.databases.lock().expect("databases") = vec![
            dbflux_core::DatabaseInfo {
                name: "app".to_string(),
                is_current: true,
            },
            dbflux_core::DatabaseInfo {
                name: "other".to_string(),
                is_current: false,
            },
        ];
        fake.set_schema(
            "app",
            fake_db_schema("app", vec![fake_table("public", "users")]),
        );
        fake.set_schema(
            "other",
            fake_db_schema("other", vec![fake_table("public", "users")]),
        );
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "app".to_string(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "other".to_string(),
                    is_current: false,
                },
            ])),
        );

        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));

        for database in ["app", "other"] {
            let db_item = SchemaNodeId::Database {
                profile_id,
                name: database.to_string(),
            }
            .to_string();
            window
                .update(cx, |sidebar, _, cx| {
                    sidebar.set_expanded(&db_item, true, cx);
                })
                .expect("sidebar alive");
        }
        cx.run_until_parked();

        // Cached reopen: expanding again must not re-run driver work.
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.set_expanded(
                    &SchemaNodeId::Database {
                        profile_id,
                        name: "app".to_string(),
                    }
                    .to_string(),
                    false,
                    cx,
                );
                sidebar.set_expanded(
                    &SchemaNodeId::Database {
                        profile_id,
                        name: "app".to_string(),
                    }
                    .to_string(),
                    true,
                    cx,
                );
            })
            .expect("sidebar alive");
        cx.run_until_parked();

        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string(), "other".to_string()],
            "cached reopen must not re-dispatch loaded databases"
        );

        // Same-named tables in distinct databases keep distinct stable IDs.
        let rows = build_items(&window, cx);
        let app_table = SchemaNodeId::Table {
            profile_id,
            database: Some("app".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        let other_table = SchemaNodeId::Table {
            profile_id,
            database: Some("other".to_string()),
            schema: "public".to_string(),
            name: "users".to_string(),
        }
        .to_string();
        assert!(
            rows.iter().any(|(id, _, _)| *id == app_table)
                && rows.iter().any(|(id, _, _)| *id == other_table),
            "identical table names in distinct databases must both render with distinct stable IDs; rows: {rows:?}"
        );
    }

    #[gpui::test]
    async fn view_refresh_reconnect_preserves_replacement_views(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let old = AdapterFakeConnection::single_database();
        let replacement = AdapterFakeConnection::single_database();
        let view = |name: &str| dbflux_core::ViewInfo {
            name: name.into(),
            schema: Some("public".into()),
        };
        let snapshot = |name: &str| {
            let mut snapshot = snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }]);
            if let dbflux_core::DataStructure::Relational(relational) = &mut snapshot.structure {
                let mut schema = fake_db_schema("public", Vec::new());
                schema.views.push(view(name));
                relational.schemas.push(schema);
            }
            snapshot
        };
        *old.primary_snapshot.lock().expect("old snapshot") = Some(snapshot("old_view"));
        connect_profile(&state, cx, profile_id, old, Some(snapshot("old_view")));
        let view_id = SchemaNodeId::View {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "old_view".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let task_id = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("view task")
                .id
        });
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                replacement,
                Some(snapshot("new_view")),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let connected = state
                .connections()
                .get(&profile_id)
                .expect("replacement session");
            let schema = connected.schema.as_ref().expect("replacement schema");
            assert_eq!(schema.schemas()[0].views.len(), 1);
            assert_eq!(schema.schemas()[0].views[0].name, "new_view");
            assert_eq!(
                state.tasks().get(task_id).map(|task| task.status),
                Some(dbflux_core::TaskStatus::Cancelled)
            );
        });
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&view_id));
                assert!(!sidebar.view_refresh_tasks.contains_key(&view_id));
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn view_refresh_slot_churn_preserves_replacement_views(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let primary = AdapterFakeConnection::connection_per_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            primary,
            Some(snapshot_naming(vec![
                dbflux_core::DatabaseInfo {
                    name: "main".into(),
                    is_current: true,
                },
                dbflux_core::DatabaseInfo {
                    name: "analytics".into(),
                    is_current: false,
                },
            ])),
        );
        let old = AdapterFakeConnection::connection_per_database();
        old.primary_schema_failures.store(1, Ordering::SeqCst);
        let mut old_snapshot = secondary_snapshot("old");
        if let dbflux_core::DataStructure::Relational(relational) = &mut old_snapshot.structure {
            relational
                .schemas
                .push(fake_db_schema("public", Vec::new()));
            relational.schemas[0].views.push(dbflux_core::ViewInfo {
                name: "old_view".into(),
                schema: Some("public".into()),
            });
        }
        state.update(cx, |state, _| {
            state.add_database_connection(profile_id, "analytics".into(), old, Some(old_snapshot))
        });
        let view_id = SchemaNodeId::View {
            profile_id,
            database: Some("analytics".into()),
            schema: "public".into(),
            name: "old_view".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let task_id = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("view task")
                .id
        });
        let replacement = AdapterFakeConnection::connection_per_database();
        let mut replacement_snapshot = secondary_snapshot("replacement");
        if let dbflux_core::DataStructure::Relational(relational) =
            &mut replacement_snapshot.structure
        {
            relational
                .schemas
                .push(fake_db_schema("public", Vec::new()));
            relational.schemas[0].views.push(dbflux_core::ViewInfo {
                name: "new_view".into(),
                schema: Some("public".into()),
            });
        }
        *replacement
            .primary_snapshot
            .lock()
            .expect("replacement snapshot") = Some(replacement_snapshot.clone());
        state.update(cx, |state, _| {
            state.add_database_connection(
                profile_id,
                "analytics".into(),
                replacement,
                Some(replacement_snapshot),
            )
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let retry_task = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| {
                    task.kind == dbflux_core::TaskKind::SchemaRefresh && task.id != task_id
                })
                .expect("replacement refresh must start without waiting for the stale driver")
                .id
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            let slot = state
                .connections()
                .get(&profile_id)
                .expect("connected")
                .database_connections
                .get("analytics")
                .expect("replacement slot");
            assert_eq!(
                slot.schema.as_ref().expect("schema").schemas()[0].views[0].name,
                "new_view"
            );
            assert_eq!(
                state.tasks().get(task_id).map(|task| task.status),
                Some(dbflux_core::TaskStatus::Cancelled)
            );
            assert_eq!(
                state.tasks().get(retry_task).map(|task| task.status),
                Some(dbflux_core::TaskStatus::Completed)
            );
        });
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&view_id));
                assert!(!sidebar.view_refresh_tasks.contains_key(&view_id));
                assert!(sidebar.pending_toast.is_none());
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn view_refresh_cancelled_task_retries_immediately(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        let mut snapshot = snapshot_naming(vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }]);
        if let dbflux_core::DataStructure::Relational(relational) = &mut snapshot.structure {
            let mut schema = fake_db_schema("public", Vec::new());
            schema.views.push(dbflux_core::ViewInfo {
                name: "report".into(),
                schema: Some("public".into()),
            });
            relational.schemas.push(schema);
        }
        *fake.primary_snapshot.lock().expect("snapshot") = Some(snapshot.clone());
        connect_profile(&state, cx, profile_id, fake, Some(snapshot));
        let view_id = SchemaNodeId::View {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "report".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let old_task = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("old view task")
                .id
        });
        state.update(cx, |state, _| {
            assert!(state.tasks_mut().cancel(old_task));
        });
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let retry_task = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| {
                    task.kind == dbflux_core::TaskKind::SchemaRefresh && task.id != old_task
                })
                .expect("cancelled view request must not block retry")
                .id
        });
        cx.run_until_parked();
        state.read_with(cx, |state, _| {
            assert_eq!(
                state.tasks().get(old_task).map(|task| task.status),
                Some(dbflux_core::TaskStatus::Cancelled)
            );
            assert_eq!(
                state.tasks().get(retry_task).map(|task| task.status),
                Some(dbflux_core::TaskStatus::Completed)
            );
        });
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&view_id));
                assert!(!sidebar.view_refresh_tasks.contains_key(&view_id));
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn view_refresh_failure_retry_tracks_task(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        let mut snapshot = snapshot_naming(vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }]);
        if let dbflux_core::DataStructure::Relational(relational) = &mut snapshot.structure {
            let mut schema = fake_db_schema("public", Vec::new());
            schema.views.push(dbflux_core::ViewInfo {
                name: "old_view".into(),
                schema: Some("public".into()),
            });
            relational.schemas.push(schema);
        }
        *fake.primary_snapshot.lock().expect("snapshot") = Some(snapshot.clone());
        connect_profile(&state, cx, profile_id, fake.clone(), Some(snapshot));
        let view_id = SchemaNodeId::View {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "old_view".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        fake.primary_schema_failures.store(1, Ordering::SeqCst);
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let failed_task = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("view task")
                .id
        });
        cx.run_until_parked();
        assert!(matches!(
            state.read_with(cx, |state, _| state
                .tasks()
                .get(failed_task)
                .map(|task| task.status)),
            Some(dbflux_core::TaskStatus::Failed(_))
        ));
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&view_id));
                assert!(!sidebar.view_refresh_tasks.contains_key(&view_id));
            })
            .expect("sidebar alive");
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&view_id, cx)
            })
            .expect("sidebar alive");
        let retry_task = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("retry task")
                .id
        });
        cx.run_until_parked();
        assert_eq!(
            state.read_with(cx, |state, _| state
                .tasks()
                .get(retry_task)
                .map(|task| task.status)),
            Some(dbflux_core::TaskStatus::Completed)
        );
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&view_id));
                assert!(!sidebar.view_refresh_tasks.contains_key(&view_id));
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn table_refresh_uses_shared_coordinator_after_cached_details(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        state.update(cx, |state, _| {
            state.set_table_details(
                profile_id,
                "app".into(),
                Some("public".into()),
                "users".into(),
                loaded_details("app", Some("public"), "users"),
            );
            state.set_dependents(
                profile_id,
                "app".into(),
                Some("public".into()),
                "users".into(),
                vec![dbflux_core::RelationRef {
                    kind: dbflux_core::RelationKind::View,
                    qualified_name: "public.old_view".into(),
                }],
            );
            state.set_table_details(
                profile_id,
                "app".into(),
                Some("public".into()),
                "orders".into(),
                loaded_details("app", Some("public"), "orders"),
            );
        });
        let key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".into(),
            schema: Some("public".into()),
            table: "users".into(),
        };
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "users".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&table_id, cx);
            })
            .expect("sidebar alive");
        let task_id = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("table refresh must appear in the Tasks panel")
                .id
        });
        cx.run_until_parked();

        assert_eq!(fake.details_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied),
            "table refresh must settle through the shared coordinator"
        );
        assert_eq!(
            state.read_with(cx, |state, _| state
                .tasks()
                .get(task_id)
                .map(|task| task.status)),
            Some(dbflux_core::TaskStatus::Completed)
        );
        state.read_with(cx, |state, _| {
            let connected = state.connections().get(&profile_id).expect("connected");
            let refreshed = (
                "app".to_string(),
                Some("public".to_string()),
                "users".to_string(),
            );
            let sibling = (
                "app".to_string(),
                Some("public".to_string()),
                "orders".to_string(),
            );
            assert!(
                connected
                    .table_details
                    .get(&refreshed)
                    .is_some_and(|details| details.columns.is_some())
            );
            assert!(connected.table_details.contains_key(&sibling));
            assert!(
                connected
                    .dependents_cache
                    .get(&refreshed)
                    .is_none_or(Vec::is_empty)
            );
        });
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&table_id));
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn table_refresh_does_not_cancel_sibling_details_request(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        let sibling_key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".into(),
            schema: Some("public".into()),
            table: "orders".into(),
        };
        state.update(cx, |state, cx| {
            assert_eq!(
                state.object_tree_request(sibling_key.clone(), cx),
                ObjectTreeRequestStatus::Dispatched
            );
        });
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "users".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&table_id, cx)
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.details_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            state.read_with(cx, |state, _| state
                .object_tree_outcome(&sibling_key)
                .cloned()),
            Some(ObjectTreeOutcome::Applied)
        );
    }

    #[gpui::test]
    async fn failed_table_refresh_retries_without_stuck_loading(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        fake.fail_details_once();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        let key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".into(),
            schema: Some("public".into()),
            table: "users".into(),
        };
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "users".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&table_id, cx)
            })
            .expect("sidebar alive");
        let task_id = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("table refresh task")
                .id
        });
        cx.run_until_parked();
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Failed(_))
        ));
        assert!(matches!(
            state.read_with(cx, |state, _| state
                .tasks()
                .get(task_id)
                .map(|task| task.status)),
            Some(dbflux_core::TaskStatus::Failed(_))
        ));
        window
            .update(cx, |sidebar, _, cx| {
                assert!(!sidebar.loading_items.contains(&table_id));
                sidebar.refresh_schema_object(&table_id, cx);
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(fake.details_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied)
        );
    }

    #[gpui::test]
    async fn table_refresh_reconnect_releases_old_loading_and_starts_new_request(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "users".into(),
        }
        .to_string();
        let key = ObjectTreeRequestKey::TableDetails {
            profile_id,
            database: "app".into(),
            schema: Some("public".into()),
            table: "users".into(),
        };
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&table_id, cx)
            })
            .expect("sidebar alive");
        assert!(state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake.clone(),
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
        });
        cx.run_until_parked();
        window
            .update(cx, |sidebar, _, cx| {
                assert!(!sidebar.loading_items.contains(&table_id));
                sidebar.refresh_schema_object(&table_id, cx);
            })
            .expect("sidebar alive");
        assert!(state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
        cx.run_until_parked();
        assert!(fake.details_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied)
        );
        window
            .update(cx, |sidebar, _, _| {
                assert!(!sidebar.loading_items.contains(&table_id))
            })
            .expect("sidebar alive");
    }

    #[gpui::test]
    async fn cached_details_after_reconnect_retire_old_refresh_task(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = AdapterFakeConnection::single_database();
        connect_profile(
            &state,
            cx,
            profile_id,
            fake.clone(),
            Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }])),
        );
        let table_id = SchemaNodeId::Table {
            profile_id,
            database: Some("app".into()),
            schema: "public".into(),
            name: "users".into(),
        }
        .to_string();
        let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
        window
            .update(cx, |sidebar, _, cx| {
                sidebar.refresh_schema_object(&table_id, cx)
            })
            .expect("sidebar alive");
        let old_task = state.read_with(cx, |state, _| {
            state
                .tasks()
                .running_tasks()
                .into_iter()
                .find(|task| task.kind == dbflux_core::TaskKind::SchemaRefresh)
                .expect("old refresh task")
                .id
        });
        state.update(cx, |state, _| {
            let mut profile = dbflux_core::ConnectionProfile::new(
                "replacement",
                dbflux_core::DbConfig::default_postgres(),
            );
            profile.id = profile_id;
            state.apply_connect_profile(
                profile,
                fake,
                Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                }])),
                None,
                false,
                dbflux_core::WritePrivilege::Unknown,
            );
            state.set_table_details(
                profile_id,
                "app".into(),
                Some("public".into()),
                "users".into(),
                loaded_details("app", Some("public"), "users"),
            );
        });
        window
            .update(cx, |sidebar, _, cx| {
                assert!(matches!(
                    sidebar.ensure_table_details(
                        &table_id,
                        PendingAction::ViewSchema {
                            item_id: table_id.clone(),
                        },
                        cx,
                    ),
                    TableDetailsStatus::Ready
                ));
                assert!(!sidebar.loading_items.contains(&table_id));
                assert!(!sidebar.table_refresh_tasks.contains_key(&table_id));
            })
            .expect("sidebar alive");
        cx.run_until_parked();
        assert_eq!(
            state.read_with(cx, |state, _| state
                .tasks()
                .get(old_task)
                .map(|task| task.status)),
            Some(dbflux_core::TaskStatus::Cancelled)
        );
    }
}
