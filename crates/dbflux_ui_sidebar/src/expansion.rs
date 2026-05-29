use super::*;
use dbflux_core::{DbSchemaInfo, SchemaDropTarget, SchemaObjectKind};
use dbflux_ui_base::AsyncUpdateResultExt;

impl Sidebar {
    fn remove_database_from_snapshot(snapshot: &mut SchemaSnapshot, database: &str) {
        match &mut snapshot.structure {
            dbflux_core::DataStructure::Relational(schema) => {
                schema.databases.retain(|entry| entry.name != database);

                if schema.current_database.as_deref() == Some(database) {
                    schema.current_database = None;
                    schema.schemas.clear();
                    schema.tables.clear();
                    schema.views.clear();
                }
            }
            dbflux_core::DataStructure::Document(schema) => {
                schema.databases.retain(|entry| entry.name != database);

                if schema.current_database.as_deref() == Some(database) {
                    schema.current_database = None;
                    schema.collections.clear();
                }
            }
            dbflux_core::DataStructure::Graph(schema) => {
                schema.databases.retain(|entry| entry.name != database);

                if schema.current_database.as_deref() == Some(database) {
                    schema.current_database = None;
                    schema.node_labels.clear();
                    schema.relationship_types.clear();
                    schema.property_keys.clear();
                }
            }
            dbflux_core::DataStructure::TimeSeries(schema) => {
                schema.databases.retain(|entry| entry.name != database);

                if schema.current_database.as_deref() == Some(database) {
                    schema.current_database = None;
                    schema.measurements.clear();
                    schema.retention_policies.clear();
                }
            }
            dbflux_core::DataStructure::Vector(schema) => {
                schema.databases.retain(|entry| entry.name != database);

                if schema.current_database.as_deref() == Some(database) {
                    schema.current_database = None;
                    schema.collections.clear();
                }
            }
            dbflux_core::DataStructure::MultiModel(schema) => {
                schema.databases.retain(|entry| entry.name != database);

                if schema.current_database.as_deref() == Some(database) {
                    schema.current_database = None;
                    schema.tables.clear();
                    schema.collections.clear();
                    schema.graphs.clear();
                }
            }
            dbflux_core::DataStructure::KeyValue(_)
            | dbflux_core::DataStructure::WideColumn(_)
            | dbflux_core::DataStructure::Search(_) => {}
        }
    }

    fn remove_object_from_db_schema(db_schema: &mut DbSchemaInfo, target: &SchemaDropTarget) {
        match target.kind {
            SchemaObjectKind::Table | SchemaObjectKind::Collection => {
                db_schema.tables.retain(|entry| entry.name != target.name);
            }
            SchemaObjectKind::View => {
                db_schema.views.retain(|entry| entry.name != target.name);
            }
            SchemaObjectKind::Database => {}
        }
    }

    fn remove_object_from_snapshot(snapshot: &mut SchemaSnapshot, target: &SchemaDropTarget) {
        match &mut snapshot.structure {
            dbflux_core::DataStructure::Relational(schema) => {
                let matches_current_database = target
                    .database
                    .as_deref()
                    .is_none_or(|database| schema.current_database.as_deref() == Some(database));

                if !matches_current_database {
                    return;
                }

                if let Some(schema_name) = target.schema.as_deref() {
                    if let Some(db_schema) = schema
                        .schemas
                        .iter_mut()
                        .find(|entry| entry.name == schema_name)
                    {
                        Self::remove_object_from_db_schema(db_schema, target);
                    }
                    return;
                }

                match target.kind {
                    SchemaObjectKind::Table => {
                        schema.tables.retain(|entry| entry.name != target.name);
                    }
                    SchemaObjectKind::View => {
                        schema.views.retain(|entry| entry.name != target.name);
                    }
                    SchemaObjectKind::Collection | SchemaObjectKind::Database => {}
                }
            }
            dbflux_core::DataStructure::Document(schema) => {
                let matches_current_database = target
                    .database
                    .as_deref()
                    .is_none_or(|database| schema.current_database.as_deref() == Some(database));

                if matches_current_database && target.kind == SchemaObjectKind::Collection {
                    schema.collections.retain(|entry| entry.name != target.name);
                }
            }
            dbflux_core::DataStructure::MultiModel(schema) => {
                let matches_current_database = target
                    .database
                    .as_deref()
                    .is_none_or(|database| schema.current_database.as_deref() == Some(database));

                if !matches_current_database {
                    return;
                }

                match target.kind {
                    SchemaObjectKind::Table => {
                        schema.tables.retain(|entry| entry.name != target.name);
                    }
                    SchemaObjectKind::Collection => {
                        schema.collections.retain(|entry| entry.name != target.name);
                    }
                    SchemaObjectKind::View | SchemaObjectKind::Database => {}
                }
            }
            dbflux_core::DataStructure::KeyValue(_)
            | dbflux_core::DataStructure::Graph(_)
            | dbflux_core::DataStructure::WideColumn(_)
            | dbflux_core::DataStructure::TimeSeries(_)
            | dbflux_core::DataStructure::Search(_)
            | dbflux_core::DataStructure::Vector(_) => {}
        }
    }

    pub fn expand_collapse(&mut self, cx: &mut Context<Self>) {
        let tree = self.active_tree_state().clone();
        let entry = tree.read(cx).selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_folder()
        {
            let item_id = entry.item().id.to_string();
            let currently_expanded = entry.is_expanded();
            self.set_expanded(&item_id, !currently_expanded, cx);
        }
    }

    pub fn collapse(&mut self, cx: &mut Context<Self>) {
        let tree = self.active_tree_state().clone();
        let entry = tree.read(cx).selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_folder()
            && entry.is_expanded()
        {
            let item_id = entry.item().id.to_string();
            self.set_expanded(&item_id, false, cx);
        }
    }

    pub fn expand(&mut self, cx: &mut Context<Self>) {
        let tree = self.active_tree_state().clone();
        let entry = tree.read(cx).selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_folder()
            && !entry.is_expanded()
        {
            let item_id = entry.item().id.to_string();
            self.set_expanded(&item_id, true, cx);
        }
    }

    pub(super) fn set_expanded(&mut self, item_id: &str, expanded: bool, cx: &mut Context<Self>) {
        if expanded && !self.trigger_expansion_fetch(item_id, cx) {
            return;
        }

        if let Some(SchemaNodeId::ConnectionFolder { node_id }) = parse_node_id(item_id) {
            self.app_state.update(cx, |state, _cx| {
                state.set_folder_collapsed(node_id, !expanded);
            });
        }

        self.expansion_overrides
            .insert(item_id.to_string(), expanded);
        self.rebuild_tree_with_overrides(cx);
    }

    /// Starts any background fetches required when a node is expanded.
    /// Returns `false` if expansion should be blocked (e.g. fetch preparation failed).
    /// Does not modify `expansion_overrides` or rebuild the tree.
    pub(super) fn trigger_expansion_fetch(
        &mut self,
        item_id: &str,
        cx: &mut Context<Self>,
    ) -> bool {
        let parsed = parse_node_id(item_id);

        if matches!(parsed, Some(SchemaNodeId::Table { .. })) {
            let pending = PendingAction::ViewSchema {
                item_id: item_id.to_string(),
            };
            if matches!(
                self.ensure_table_details(item_id, pending, cx),
                TableDetailsStatus::NotFound
            ) {
                return false;
            }
        }

        if let Some(SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        }) = &parsed
        {
            let pending = PendingAction::ExpandCollection {
                item_id: item_id.to_string(),
            };

            if self.collection_node_is_event_stream(*profile_id, database, name, cx) {
                if matches!(
                    self.ensure_collection_children(*profile_id, database, name, pending, cx),
                    TableDetailsStatus::NotFound
                ) {
                    return false;
                }
            } else if matches!(
                self.ensure_table_details(item_id, pending, cx),
                TableDetailsStatus::NotFound
            ) {
                return false;
            }
        }

        if let Some(SchemaNodeId::TypesFolder {
            profile_id,
            database,
            schema,
        }) = &parsed
        {
            let needs_fetch =
                self.app_state
                    .read(cx)
                    .needs_schema_types(*profile_id, database, Some(schema));

            if needs_fetch {
                let pending = PendingAction::ExpandTypesFolder {
                    item_id: item_id.to_string(),
                };
                if !self.spawn_fetch_schema_types(*profile_id, database, Some(schema), pending, cx)
                {
                    return false;
                }
            }
        }

        if let Some(SchemaNodeId::SchemaIndexesFolder {
            profile_id,
            database,
            schema,
        }) = &parsed
        {
            let needs_fetch =
                self.app_state
                    .read(cx)
                    .needs_schema_indexes(*profile_id, database, Some(schema));

            if needs_fetch {
                let pending = PendingAction::ExpandSchemaIndexesFolder {
                    item_id: item_id.to_string(),
                };
                if !self.spawn_fetch_schema_indexes(
                    *profile_id,
                    database,
                    Some(schema),
                    pending,
                    cx,
                ) {
                    return false;
                }
            }
        }

        if let Some(SchemaNodeId::SchemaForeignKeysFolder {
            profile_id,
            database,
            schema,
        }) = &parsed
        {
            let needs_fetch = self.app_state.read(cx).needs_schema_foreign_keys(
                *profile_id,
                database,
                Some(schema),
            );

            if needs_fetch {
                let pending = PendingAction::ExpandSchemaForeignKeysFolder {
                    item_id: item_id.to_string(),
                };
                if !self.spawn_fetch_schema_foreign_keys(
                    *profile_id,
                    database,
                    Some(schema),
                    pending,
                    cx,
                ) {
                    return false;
                }
            }
        }

        if let Some(SchemaNodeId::RoutinesFolder {
            profile_id,
            database,
            schema,
        }) = &parsed
        {
            let needs_fetch =
                self.app_state
                    .read(cx)
                    .needs_schema_routines(*profile_id, database, Some(schema));

            if needs_fetch {
                let pending = PendingAction::ExpandSchemaRoutinesFolder {
                    item_id: item_id.to_string(),
                };
                if !self.spawn_fetch_schema_routines(
                    *profile_id,
                    database,
                    Some(schema),
                    pending,
                    cx,
                ) {
                    return false;
                }
            }
        }

        if let Some(SchemaNodeId::MetricsFolder {
            profile_id,
            database,
        }) = &parsed
        {
            self.spawn_fetch_metric_namespaces(*profile_id, database, cx);
        }

        if let Some(SchemaNodeId::MetricNamespaceFolder {
            profile_id,
            database,
            namespace,
        }) = &parsed
        {
            self.spawn_fetch_metrics(*profile_id, database, namespace, cx);
        }

        if let Some(SchemaNodeId::RemoteDashboardsFolder { profile_id }) = &parsed {
            self.spawn_fetch_remote_dashboards(*profile_id, cx);
        }

        if matches!(parsed, Some(SchemaNodeId::Database { .. })) {
            self.handle_database_click(item_id, cx);
        }

        true
    }

    /// Fetch metric namespaces for a connection if the cache doesn't have them yet.
    ///
    /// Peeks the cache first; only spawns a background task on a miss. The task
    /// writes through the cache and calls `cx.notify()` on completion so the tree
    /// rebuilds and shows the namespace children.
    pub(super) fn spawn_fetch_metric_namespaces(
        &mut self,
        profile_id: Uuid,
        database: &str,
        cx: &mut Context<Self>,
    ) {
        let cache = self.app_state.read(cx).metric_catalog_cache().clone();

        // Cache hit — no fetch needed.
        if cache.peek_namespaces(profile_id).is_some() {
            return;
        }

        // Deduplicate in-flight fetches: don't spawn a second task if one is running.
        if self
            .pending_metric_namespace_fetches
            .contains_key(&profile_id)
        {
            return;
        }

        let connection = match self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        {
            Some(c) => c,
            None => return,
        };

        let parent_id = SchemaNodeId::MetricsFolder {
            profile_id,
            database: database.to_string(),
        }
        .to_string();

        let sidebar = cx.entity().clone();
        let db_str = database.to_string();
        // Background task only fetches; the cache write happens on the
        // foreground task below, after the await. If the foreground task is
        // dropped (e.g. disconnect_profile evicts it), the cache write is
        // never executed and stale data from a previous account cannot land
        // in the cache.
        let background_task = cx.background_executor().spawn(async move {
            let catalog = match connection.metric_catalog() {
                Some(c) => c,
                None => return Err("driver does not support metric catalog".to_string()),
            };
            catalog.list_namespaces().map_err(|e| e.to_string())
        });

        let task = cx.spawn(async move |_this, cx| {
            let result = background_task.await;
            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_metric_namespace_fetches.remove(&profile_id);
                    match result {
                        Ok(namespaces) => {
                            cache.store_namespaces(profile_id, namespaces);
                            sidebar.metric_fetch_errors.remove(&parent_id);
                        }
                        Err(msg) => {
                            sidebar.metric_fetch_errors.insert(parent_id, msg.clone());
                            log::warn!(
                                "Failed to fetch metric namespaces for {}: {}",
                                profile_id,
                                msg
                            );
                        }
                    }
                    sidebar.rebuild_tree_with_overrides(cx);
                });
            })
            .log_if_dropped();
        });

        self.pending_metric_namespace_fetches
            .insert(profile_id, task);
        let _ = db_str; // used via closure capture above
    }

    /// Fetch metrics for a specific namespace if the cache doesn't have them yet.
    ///
    /// Mirrors `spawn_fetch_metric_namespaces` but keyed by `(profile_id, namespace)`.
    pub(super) fn spawn_fetch_metrics(
        &mut self,
        profile_id: Uuid,
        database: &str,
        namespace: &str,
        cx: &mut Context<Self>,
    ) {
        let cache = self.app_state.read(cx).metric_catalog_cache().clone();
        let ns: dbflux_core::MetricNamespace = namespace.to_string();

        // Cache hit — no fetch needed.
        if cache.peek_metrics(profile_id, &ns).is_some() {
            return;
        }

        let fetch_key = (profile_id, namespace.to_string());

        // Deduplicate in-flight fetches.
        if self.pending_metric_fetches.contains_key(&fetch_key) {
            return;
        }

        let connection = match self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        {
            Some(c) => c,
            None => return,
        };

        let parent_id = SchemaNodeId::MetricNamespaceFolder {
            profile_id,
            database: database.to_string(),
            namespace: ns.clone(),
        }
        .to_string();

        let sidebar = cx.entity().clone();
        // Background task only fetches; the cache write happens on the
        // foreground task below, after the await. Dropping the foreground task
        // (e.g. on disconnect) prevents stale data from landing in the cache.
        let background_task = cx.background_executor().spawn({
            let ns_clone = ns.clone();
            async move {
                let catalog = match connection.metric_catalog() {
                    Some(c) => c,
                    None => return Err("driver does not support metric catalog".to_string()),
                };
                // Fetch first page only; pagination is not required for sidebar display.
                catalog
                    .list_metrics(&ns_clone, None)
                    .map_err(|e| e.to_string())
            }
        });

        let ns_key = fetch_key.clone();
        let task = cx.spawn(async move |_this, cx| {
            let result = background_task.await;
            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_metric_fetches.remove(&ns_key);
                    match result {
                        Ok(page) => {
                            cache.store_metrics_page(
                                profile_id,
                                ns.clone(),
                                page.metrics,
                                page.next_token,
                            );
                            sidebar.metric_fetch_errors.remove(&parent_id);
                        }
                        Err(msg) => {
                            sidebar.metric_fetch_errors.insert(parent_id, msg.clone());
                            log::warn!(
                                "Failed to fetch metrics for {}/{}: {}",
                                profile_id,
                                ns_key.1,
                                msg
                            );
                        }
                    }
                    sidebar.rebuild_tree_with_overrides(cx);
                });
            })
            .log_if_dropped();
        });

        self.pending_metric_fetches.insert(fetch_key, task);
    }

    /// Fetch the upstream dashboard listing for a connection if not cached.
    ///
    /// Mirrors `spawn_fetch_metric_namespaces`: peek the cache, dedup in-flight
    /// fetches, run the async `DashboardSource::list_dashboards` call on the
    /// background executor, then write the result through the cache and rebuild
    /// the tree on the foreground. Nothing is persisted — the listing is
    /// session-scoped.
    pub(super) fn spawn_fetch_remote_dashboards(
        &mut self,
        profile_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        let cache = self.app_state.read(cx).remote_dashboard_cache().clone();

        // Cache hit — no fetch needed.
        if cache.peek(profile_id).is_some() {
            return;
        }

        // Deduplicate in-flight fetches.
        if self
            .pending_remote_dashboard_fetches
            .contains_key(&profile_id)
        {
            return;
        }

        let connection = match self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        {
            Some(c) => c,
            None => return,
        };

        let parent_id = SchemaNodeId::RemoteDashboardsFolder { profile_id }.to_string();
        let sidebar = cx.entity().clone();

        let background_task = cx.background_executor().spawn(async move {
            let source = match connection.dashboard_source() {
                Some(s) => s,
                None => return Err("driver does not support dashboard listing".to_string()),
            };
            source.list_dashboards().map_err(|e| e.to_string())
        });

        let task = cx.spawn(async move |_this, cx| {
            let result = background_task.await;
            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_remote_dashboard_fetches.remove(&profile_id);
                    match result {
                        Ok(dashboards) => {
                            cache.store(profile_id, dashboards);
                            sidebar.metric_fetch_errors.remove(&parent_id);
                        }
                        Err(msg) => {
                            sidebar.metric_fetch_errors.insert(parent_id, msg.clone());
                            log::warn!("Failed to list dashboards for {}: {}", profile_id, msg);
                        }
                    }
                    sidebar.rebuild_tree_with_overrides(cx);
                });
            })
            .log_if_dropped();
        });

        self.pending_remote_dashboard_fetches
            .insert(profile_id, task);
    }

    fn collection_node_is_event_stream(
        &self,
        profile_id: Uuid,
        database: &str,
        collection: &str,
        cx: &App,
    ) -> bool {
        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|connection| connection.schema_for_target_database(database))
            .and_then(|schema| {
                schema
                    .collections()
                    .iter()
                    .find(|item| item.name == collection)
            })
            .is_some_and(|item| item.presentation == CollectionPresentation::EventStream)
    }

    pub(super) fn rebuild_tree_with_overrides(&mut self, cx: &mut Context<Self>) {
        let selected_index = self.tree_state.read(cx).selected_index();
        self.active_databases = Self::extract_active_databases(self.app_state.read(cx));

        let items = self.build_tree_items_with_overrides(cx);
        self.prune_connection_selection(&items);
        self.visible_entry_count = Self::count_visible_entries(&items);
        self.gutter_metadata = compute_gutter_map(&items);

        self.syncing_expansion = true;
        self.tree_state.update(cx, |state, cx| {
            state.set_items(items, cx);
            if let Some(idx) = selected_index {
                let new_idx = idx.min(self.visible_entry_count.saturating_sub(1));
                state.set_selected_index(Some(new_idx), cx);
            }
        });
        self.syncing_expansion = false;
        cx.notify();
    }

    pub(super) fn refresh_tree(&mut self, cx: &mut Context<Self>) {
        let selected_index = self.tree_state.read(cx).selected_index();
        self.active_databases = Self::extract_active_databases(self.app_state.read(cx));

        self.cleanup_stale_overrides(cx);

        let items = self.build_tree_items_with_overrides(cx);
        self.prune_connection_selection(&items);
        self.visible_entry_count = Self::count_visible_entries(&items);
        self.gutter_metadata = compute_gutter_map(&items);

        if let Some(ref menu) = self.context_menu
            && Self::find_item_index_in_tree(&items, &menu.item_id, &mut 0).is_none()
        {
            self.context_menu = None;
        }

        self.syncing_expansion = true;
        self.tree_state.update(cx, |state, cx| {
            state.set_items(items, cx);

            if let Some(idx) = selected_index {
                let new_idx = idx.min(self.visible_entry_count.saturating_sub(1));
                state.set_selected_index(Some(new_idx), cx);
            }
        });
        self.syncing_expansion = false;
        cx.notify();
    }

    /// Remove cached schema data for a specific database, causing the next
    /// expansion to re-fetch from the driver.
    pub(super) fn invalidate_database_cache(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        self.app_state.update(cx, |state, _cx| {
            if let Some(conn) = state.connections_mut().get_mut(&profile_id) {
                conn.database_schemas.remove(db_name);
                conn.table_details.retain(|(db, _), _| db != db_name);
                conn.collection_children.retain(|(db, _), _| db != db_name);
                conn.schema_types.retain(|key, _| key.database != db_name);
                conn.schema_indexes.retain(|key, _| key.database != db_name);
                conn.schema_foreign_keys
                    .retain(|key, _| key.database != db_name);
                conn.database_connections.remove(db_name);

                if let Some(schema) = conn.schema.as_mut() {
                    Self::remove_database_from_snapshot(schema, db_name);
                }
            }
        });
    }

    /// Remove the cached column/index details for a single table or collection,
    /// so the next expansion re-fetches from the driver.
    pub(super) fn invalidate_object_cache(
        &mut self,
        profile_id: Uuid,
        cache_db: &str,
        target: &SchemaDropTarget,
        cx: &mut Context<Self>,
    ) {
        self.app_state.update(cx, |state, _cx| {
            if let Some(conn) = state.connections_mut().get_mut(&profile_id) {
                let key = (cache_db.to_string(), target.name.clone());
                conn.table_details.remove(&key);
                conn.collection_children.remove(&key);

                if target.kind == SchemaObjectKind::Table {
                    let target_schema = target.schema.as_deref();

                    conn.schema_indexes.retain(|key, indexes| {
                        if key.database != cache_db || key.schema.as_deref() != target_schema {
                            return true;
                        }

                        indexes.retain(|index| index.table_name != target.name);
                        !indexes.is_empty()
                    });

                    conn.schema_foreign_keys.retain(|key, foreign_keys| {
                        if key.database != cache_db || key.schema.as_deref() != target_schema {
                            return true;
                        }

                        foreign_keys.retain(|foreign_key| foreign_key.table_name != target.name);
                        !foreign_keys.is_empty()
                    });
                }

                if let Some(db_schema) = conn.database_schemas.get_mut(cache_db) {
                    Self::remove_object_from_db_schema(db_schema, target);
                }

                if let Some(db_conn) = conn.database_connections.get_mut(cache_db)
                    && let Some(schema) = db_conn.schema.as_mut()
                {
                    Self::remove_object_from_snapshot(schema, target);
                }

                if let Some(schema) = conn.schema.as_mut() {
                    Self::remove_object_from_snapshot(schema, target);
                }
            }
        });
    }

    fn cleanup_stale_overrides(&mut self, cx: &Context<Self>) {
        let state = self.app_state.read(cx);

        self.expansion_overrides.retain(|item_id, _expanded| {
            if self.loading_items.contains(item_id) {
                return true;
            }

            match parse_node_id(item_id) {
                Some(SchemaNodeId::TypesFolder {
                    profile_id,
                    database,
                    schema,
                }) => !state.needs_schema_types(profile_id, &database, Some(&schema)),
                Some(SchemaNodeId::SchemaIndexesFolder {
                    profile_id,
                    database,
                    schema,
                }) => !state.needs_schema_indexes(profile_id, &database, Some(&schema)),
                Some(SchemaNodeId::SchemaForeignKeysFolder {
                    profile_id,
                    database,
                    schema,
                }) => !state.needs_schema_foreign_keys(profile_id, &database, Some(&schema)),
                Some(SchemaNodeId::RoutinesFolder {
                    profile_id,
                    database,
                    schema,
                }) => !state.needs_schema_routines(profile_id, &database, Some(&schema)),
                _ => true,
            }
        });
    }
}
