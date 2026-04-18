use super::*;
use dbflux_core::{DbSchemaInfo, SchemaDropTarget, SchemaObjectKind};

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

        if matches!(parsed, Some(SchemaNodeId::Collection { .. })) {
            let pending = PendingAction::ExpandCollection {
                item_id: item_id.to_string(),
            };
            if matches!(
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

        if matches!(parsed, Some(SchemaNodeId::Database { .. })) {
            self.handle_database_click(item_id, cx);
        }

        true
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
                _ => true,
            }
        });
    }
}
