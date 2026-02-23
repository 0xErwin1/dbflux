use super::*;

impl Sidebar {
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

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&parts.profile_id) else {
            return TableDetailsStatus::NotFound;
        };

        let cache_db = parts.cache_database();
        let cache_key = (cache_db.to_string(), parts.object_name.clone());

        if conn.table_details.contains_key(&cache_key) {
            return TableDetailsStatus::Ready;
        }

        if let Some(db_schema) = conn.database_schemas.get(&parts.schema_name)
            && let Some(table) = db_schema
                .tables
                .iter()
                .find(|t| t.name == parts.object_name)
            && table.columns.is_some()
        {
            return TableDetailsStatus::Ready;
        }

        let target_schema = parts
            .database
            .as_deref()
            .and_then(|db| conn.database_connections.get(db))
            .and_then(|dc| dc.schema.as_ref())
            .or(conn.schema.as_ref());

        if let Some(schema) = target_schema {
            for db_schema in schema.schemas() {
                if db_schema.name == parts.schema_name
                    && let Some(table) = db_schema
                        .tables
                        .iter()
                        .find(|t| t.name == parts.object_name)
                    && table.columns.is_some()
                {
                    return TableDetailsStatus::Ready;
                }
            }
        }

        // Table needs details fetched - spawn async task
        self.spawn_fetch_table_details(&parts, pending_action, cx);
        TableDetailsStatus::Loading
    }

    /// Spawn a background task to fetch table details (columns, indexes).
    fn spawn_fetch_table_details(
        &mut self,
        parts: &ItemIdParts,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) {
        let cache_db = parts.cache_database().to_string();

        let params = match self.app_state.read(cx).prepare_fetch_table_details(
            parts.profile_id,
            &cache_db,
            Some(&parts.schema_name),
            &parts.object_name,
        ) {
            Ok(p) => p,
            Err(e) => {
                if e != "Table details already cached" {
                    log::warn!("Cannot fetch table details: {}", e);
                    self.pending_toast = Some(PendingToast {
                        message: format!("Cannot load table schema: {}", e),
                        is_error: true,
                    });
                    cx.notify();
                }
                return;
            }
        };

        self.pending_action = Some(pending_action);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let profile_id = parts.profile_id;
        let db_name = cache_db.clone();
        let table_name = parts.object_name.clone();

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                match result {
                    Ok(res) => {
                        app_state.update(cx, |state, cx| {
                            state.set_table_details(
                                res.profile_id,
                                res.database,
                                res.table,
                                res.details,
                            );
                            cx.emit(AppStateChanged);
                        });

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.complete_pending_action(cx);
                        });
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to fetch table details for {}.{}: {}",
                            db_name,
                            table_name,
                            e
                        );

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_action = None;
                            sidebar.pending_toast = Some(PendingToast {
                                message: format!("Failed to load table schema: {}", e),
                                is_error: true,
                            });
                            cx.notify();
                        });
                    }
                }

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name));
                    cx.emit(AppStateChanged);
                });
            })
            .ok();
        })
        .detach();
    }

    /// Spawn a background task to fetch custom types for a schema.
    pub(super) fn spawn_fetch_schema_types(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_types(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema types already cached" {
                    log::warn!("Cannot fetch schema types: {}", e);
                }
                return;
            }
        };

        self.pending_action = Some(pending_action);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let db_name = database.to_string();
        let schema_name = schema.map(String::from);

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| match result {
                Ok(res) => {
                    app_state.update(cx, |state, cx| {
                        state.set_schema_types(res.profile_id, res.database, res.schema, res.types);
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.complete_pending_action(cx);
                    });
                }
                Err(e) => {
                    log::error!(
                        "Failed to fetch schema types for {}.{:?}: {}",
                        db_name,
                        schema_name,
                        e
                    );

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_action = None;
                        cx.notify();
                    });
                }
            })
            .ok();
        })
        .detach();
    }

    /// Spawn a background task to fetch indexes for a schema.
    pub(super) fn spawn_fetch_schema_indexes(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_indexes(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema indexes already cached" {
                    log::warn!("Cannot fetch schema indexes: {}", e);
                }
                return;
            }
        };

        self.pending_action = Some(pending_action);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| match result {
                Ok(res) => {
                    app_state.update(cx, |state, cx| {
                        state.set_schema_indexes(
                            res.profile_id,
                            res.database,
                            res.schema,
                            res.indexes,
                        );
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.complete_pending_action(cx);
                    });
                }
                Err(e) => {
                    log::error!("Failed to fetch schema indexes: {}", e);
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_action = None;
                        cx.notify();
                    });
                }
            })
            .ok();
        })
        .detach();
    }

    /// Spawn a background task to fetch foreign keys for a schema.
    pub(super) fn spawn_fetch_schema_foreign_keys(
        &mut self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) {
        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_schema_foreign_keys(profile_id, database, schema)
        {
            Ok(p) => p,
            Err(e) => {
                if e != "Schema foreign keys already cached" {
                    log::warn!("Cannot fetch schema foreign keys: {}", e);
                }
                return;
            }
        };

        self.pending_action = Some(pending_action);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| match result {
                Ok(res) => {
                    app_state.update(cx, |state, cx| {
                        state.set_schema_foreign_keys(
                            res.profile_id,
                            res.database,
                            res.schema,
                            res.foreign_keys,
                        );
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.complete_pending_action(cx);
                    });
                }
                Err(e) => {
                    log::error!("Failed to fetch schema foreign keys: {}", e);
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_action = None;
                        cx.notify();
                    });
                }
            })
            .ok();
        })
        .detach();
    }

    /// Called when table/type details finish loading to execute the stored action.
    pub(super) fn complete_pending_action(&mut self, cx: &mut Context<Self>) {
        let Some(action) = self.pending_action.take() else {
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
            | PendingAction::ExpandSchemaForeignKeysFolder { item_id } => {
                self.expand_schema_folder(&item_id, cx);
            }
        }
    }

    pub(super) fn expand_schema_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.expansion_overrides.insert(item_id.to_string(), true);
        self.rebuild_tree_with_overrides(cx);
    }
}
