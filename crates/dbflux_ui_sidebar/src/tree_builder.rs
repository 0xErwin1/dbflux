use super::*;

impl Sidebar {
    pub(super) fn build_tree_items_with_overrides(&self, cx: &Context<Self>) -> Vec<TreeItem> {
        let items = Self::build_tree_items_with_errors(
            self.app_state.read(cx),
            &self.metric_fetch_errors,
            &self.instance_metrics_cache,
            &self.instance_inspectors_cache,
        );
        let items = self.apply_expansion_overrides(items);

        if self.connections_search_query.trim().is_empty() {
            return items;
        }

        Self::apply_tree_filter(items, self.connections_search_query.trim())
    }

    pub(super) fn extract_active_databases(state: &AppState) -> HashMap<Uuid, String> {
        state
            .connections()
            .iter()
            .filter_map(|(profile_id, connected)| {
                connected
                    .active_database
                    .clone()
                    .map(|db| (*profile_id, db))
            })
            .collect()
    }

    pub(crate) fn apply_tree_filter(items: Vec<TreeItem>, query: &str) -> Vec<TreeItem> {
        let query = query.to_ascii_lowercase();

        items
            .into_iter()
            .filter_map(|item| Self::filter_tree_item(item, &query))
            .collect()
    }

    fn filter_tree_item(item: TreeItem, query: &str) -> Option<TreeItem> {
        let item_id = item.id.to_string();
        let item_label = item.label.clone();
        let item_expanded = item.is_expanded();
        let item_matches = item_label.to_string().to_ascii_lowercase().contains(query);
        let original_children = item.children;

        if item_matches {
            return Some(
                TreeItem::new(item_id, item_label)
                    .expanded(item_expanded)
                    .children(original_children),
            );
        }

        let children: Vec<TreeItem> = original_children
            .into_iter()
            .filter_map(|child| Self::filter_tree_item(child, query))
            .collect();

        if children.is_empty() {
            return None;
        }

        Some(
            TreeItem::new(item_id, item_label)
                .expanded(true)
                .children(children),
        )
    }

    fn apply_expansion_overrides(&self, items: Vec<TreeItem>) -> Vec<TreeItem> {
        items
            .into_iter()
            .map(|item| self.apply_override_recursive(item))
            .collect()
    }

    /// Public re-export for callers that build their own tree (e.g. scripts)
    /// and need user collapse/expand overrides applied on top.
    pub(super) fn apply_expansion_overrides_public(&self, items: Vec<TreeItem>) -> Vec<TreeItem> {
        self.apply_expansion_overrides(items)
    }

    fn apply_override_recursive(&self, item: TreeItem) -> TreeItem {
        let item_id = item.id.to_string();
        let default_expanded = item.is_expanded();

        let mut children: Vec<TreeItem> = item
            .children
            .into_iter()
            .map(|c| self.apply_override_recursive(c))
            .collect();

        if self.loading_items.contains(&item_id) && children.is_empty() {
            children.push(TreeItem::new(
                format!("{}_loading", item_id),
                "Loading...".to_string(),
            ));
        }

        // Apply override if exists, otherwise keep default
        let expanded = self
            .expansion_overrides
            .get(&item_id)
            .copied()
            .unwrap_or(default_expanded);

        TreeItem::new(item_id, item.label.clone())
            .children(children)
            .expanded(expanded)
    }

    pub(super) fn build_tree_items(state: &AppStateEntity) -> Vec<TreeItem> {
        Self::build_tree_items_with_errors(state, &HashMap::new(), &HashMap::new(), &HashMap::new())
    }

    pub(super) fn build_tree_items_with_errors(
        state: &AppStateEntity,
        metric_fetch_errors: &HashMap<String, String>,
        instance_metrics_cache: &HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>>,
        instance_inspectors_cache: &HashMap<Uuid, Vec<dbflux_core::InstanceInspectorDef>>,
    ) -> Vec<TreeItem> {
        let root_nodes = state.connection_tree().root_nodes();
        Self::build_tree_nodes_recursive_with_errors(
            &root_nodes,
            state,
            metric_fetch_errors,
            instance_metrics_cache,
            instance_inspectors_cache,
        )
    }

    /// Build tree items for the Scripts tab from ScriptsDirectory entries.
    pub(super) fn build_scripts_tree_items(entries: &[dbflux_core::ScriptEntry]) -> Vec<TreeItem> {
        entries
            .iter()
            .map(Self::script_entry_to_tree_item)
            .collect()
    }

    fn script_entry_to_tree_item(entry: &dbflux_core::ScriptEntry) -> TreeItem {
        match entry {
            dbflux_core::ScriptEntry::Folder {
                path,
                name,
                children,
            } => {
                let id = SchemaNodeId::ScriptsFolder {
                    path: Some(path.to_string_lossy().to_string()),
                }
                .to_string();

                let child_items: Vec<TreeItem> = children
                    .iter()
                    .map(Self::script_entry_to_tree_item)
                    .collect();

                TreeItem::new(id, name.clone())
                    .expanded(true)
                    .children(child_items)
            }
            dbflux_core::ScriptEntry::File { path, name, .. } => {
                let id = SchemaNodeId::ScriptFile {
                    path: path.to_string_lossy().to_string(),
                }
                .to_string();

                TreeItem::new(id, name.clone())
            }
        }
    }

    fn build_tree_nodes_recursive_with_errors(
        nodes: &[&ConnectionTreeNode],
        state: &AppStateEntity,
        metric_fetch_errors: &HashMap<String, String>,
        instance_metrics_cache: &HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>>,
        instance_inspectors_cache: &HashMap<Uuid, Vec<dbflux_core::InstanceInspectorDef>>,
    ) -> Vec<TreeItem> {
        let mut items = Vec::new();

        for node in nodes {
            match node.kind {
                ConnectionTreeNodeKind::Folder => {
                    let children_nodes = state.connection_tree().children_of(node.id);
                    let children_refs: Vec<&ConnectionTreeNode> =
                        children_nodes.into_iter().collect();
                    let children = Self::build_tree_nodes_recursive_with_errors(
                        &children_refs,
                        state,
                        metric_fetch_errors,
                        instance_metrics_cache,
                        instance_inspectors_cache,
                    );

                    let folder_item = TreeItem::new(
                        SchemaNodeId::ConnectionFolder { node_id: node.id }.to_string(),
                        node.name.clone(),
                    )
                    .expanded(!node.collapsed)
                    .children(children);

                    items.push(folder_item);
                }

                ConnectionTreeNodeKind::ConnectionRef => {
                    if let Some(profile_id) = node.profile_id
                        && let Some(profile) = state.profiles().iter().find(|p| p.id == profile_id)
                    {
                        let profile_item = Self::build_profile_item_with_errors(
                            profile,
                            state,
                            metric_fetch_errors,
                            instance_metrics_cache,
                            instance_inspectors_cache,
                        );
                        items.push(profile_item);
                    }
                }
            }
        }

        items
    }

    fn build_profile_item_with_errors(
        profile: &dbflux_core::ConnectionProfile,
        state: &AppStateEntity,
        metric_fetch_errors: &HashMap<String, String>,
        instance_metrics_cache: &HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>>,
        instance_inspectors_cache: &HashMap<Uuid, Vec<dbflux_core::InstanceInspectorDef>>,
    ) -> TreeItem {
        let profile_id = profile.id;
        let is_connected = state.connections().contains_key(&profile_id);
        let is_active = state.active_connection_id() == Some(profile_id);
        let is_connecting = state.is_operation_pending(profile_id, None);

        let profile_label = if is_connecting {
            format!("{} (connecting...)", profile.name)
        } else {
            profile.name.clone()
        };

        let mut profile_item = TreeItem::new(
            SchemaNodeId::Profile { profile_id }.to_string(),
            profile_label,
        );

        if is_connected
            && let Some(connected) = state.connections().get(&profile_id)
            && let Some(ref schema) = connected.schema
        {
            let mut profile_children = Vec::new();

            let strategy = connected.connection.schema_loading_strategy();
            let uses_lazy_loading = strategy == SchemaLoadingStrategy::LazyPerDatabase;
            let is_document_db = schema.is_document();
            let is_time_series_db = schema.is_time_series();
            let conn_metadata = connected.connection.metadata();
            let conn_capabilities = conn_metadata.capabilities;

            // Surface the per-profile Dashboards / Saved Charts folders only
            // for drivers that opt in via `CHART_AUTHORING`. Drivers without
            // a natural chart-authoring UX (e.g. plain relational stores) keep
            // their sidebar focused on the native browsing model. Gating is
            // purely capability-driven — no driver_id or category branching.
            if conn_capabilities.contains(DriverCapabilities::CHART_AUTHORING) {
                profile_children.push(Self::build_dashboards_folder_item(profile_id, state));
                profile_children.push(Self::build_saved_charts_folder_item(profile_id, state));
            }

            // Drivers that can browse upstream dashboards get a read-only
            // listing container. Capability-gated — no driver_id branching.
            if conn_capabilities.contains(DriverCapabilities::DASHBOARD_SYNC) {
                profile_children.push(Self::build_remote_dashboards_folder_item(
                    profile_id,
                    state,
                    metric_fetch_errors,
                ));
            }

            let conn_category = conn_metadata.category;
            let supports_routines = conn_capabilities.contains(DriverCapabilities::ROUTINES);
            let metric_cache = state.metric_catalog_cache().clone();

            if schema.is_key_value() {
                let mut database_names: Vec<String> = schema
                    .keyspaces()
                    .iter()
                    .map(|space| format!("db{}", space.db_index))
                    .collect();

                if database_names.is_empty() {
                    if let Some(active_database) = connected.active_database.as_ref() {
                        database_names.push(active_database.clone());
                    } else {
                        database_names.push("db0".to_string());
                    }
                }

                let mut kv_db_items: Vec<TreeItem> = Vec::new();

                for database_name in database_names {
                    let is_pending = state.is_operation_pending(profile_id, Some(&database_name));
                    let is_active_db = connected.active_database.as_deref() == Some(&database_name);

                    let db_children = if is_pending {
                        vec![TreeItem::new(
                            SchemaNodeId::Loading {
                                profile_id,
                                database: database_name.clone(),
                            }
                            .to_string(),
                            "Loading...".to_string(),
                        )]
                    } else {
                        Vec::new()
                    };

                    let db_label = if is_pending {
                        format!("{} (loading...)", database_name)
                    } else {
                        database_name.clone()
                    };

                    kv_db_items.push(
                        TreeItem::new(
                            SchemaNodeId::Database {
                                profile_id,
                                name: database_name,
                            }
                            .to_string(),
                            db_label,
                        )
                        .expanded(uses_lazy_loading && is_active_db)
                        .children(db_children),
                    );
                }

                profile_children.push(Self::build_databases_folder_item(profile_id, kv_db_items));
            } else if !schema.databases().is_empty() {
                // See `should_collapse_database_wrapper`: when the connection
                // exposes a single trivial database (CloudWatch, DynamoDB,
                // single-DB SQL, etc.) the wrapper adds no information vs the
                // connection root. Children (Collections, Metrics, Tables)
                // already embed `database` in their node IDs so routing is
                // unaffected by the missing intermediate.
                let collapse_single_db = should_collapse_database_wrapper(schema.databases());
                let mut named_db_items: Vec<TreeItem> = Vec::new();
                for db in schema.databases() {
                    let is_pending = state.is_operation_pending(profile_id, Some(&db.name));
                    let is_active_db = connected.active_database.as_deref() == Some(&db.name);

                    let db_children = if uses_lazy_loading {
                        if let Some(db_schema) = connected.database_schemas.get(&db.name) {
                            if is_document_db {
                                Self::build_document_db_content(
                                    profile_id,
                                    &db.name,
                                    db_schema,
                                    &connected.table_details,
                                    &connected.collection_children,
                                    conn_capabilities,
                                    conn_category,
                                    Some(&metric_cache),
                                    metric_fetch_errors,
                                )
                            } else if is_time_series_db {
                                // Time-series lazy schemas are stored in database_schemas
                                // as a DbSchemaInfo whose tables carry measurement names.
                                // Route through the time-series builder the same way document
                                // databases route through build_document_db_content.
                                Self::build_time_series_db_content(profile_id, &db.name, schema)
                            } else {
                                Self::build_db_schema_content(
                                    profile_id,
                                    &db.name,
                                    None,
                                    db_schema,
                                    &connected.table_details,
                                    &connected.schema_types,
                                    &connected.schema_indexes,
                                    &connected.schema_foreign_keys,
                                    &connected.schema_routines,
                                    supports_routines,
                                    &connected.dependents_cache,
                                )
                            }
                        } else if is_pending {
                            vec![TreeItem::new(
                                SchemaNodeId::Loading {
                                    profile_id,
                                    database: db.name.clone(),
                                }
                                .to_string(),
                                "Loading...".to_string(),
                            )]
                        } else {
                            Vec::new()
                        }
                    } else if let Some(db_conn) = connected.database_connections.get(&db.name) {
                        if let Some(ref db_schema) = db_conn.schema {
                            Self::build_schema_children(
                                profile_id,
                                &db.name,
                                Some(&db.name),
                                db_schema,
                                &connected.table_details,
                                &connected.schema_types,
                                &connected.schema_indexes,
                                &connected.schema_foreign_keys,
                                &connected.schema_routines,
                                supports_routines,
                                &connected.dependents_cache,
                            )
                        } else {
                            Vec::new()
                        }
                    } else if db.is_current {
                        if is_document_db {
                            let tables = schema
                                .collections()
                                .iter()
                                .filter(|collection| {
                                    collection.database.as_deref().is_none()
                                        || collection.database.as_deref() == Some(db.name.as_str())
                                })
                                .map(|collection| TableInfo {
                                    name: collection.name.clone(),
                                    schema: Some(db.name.clone()),
                                    columns: None,
                                    indexes: collection.indexes.clone().map(IndexData::Document),
                                    foreign_keys: None,
                                    constraints: None,
                                    sample_fields: collection.sample_fields.clone(),
                                    presentation: collection.presentation,
                                    child_items: collection.child_items.clone(),
                                })
                                .collect::<Vec<_>>();

                            let db_schema = dbflux_core::DbSchemaInfo {
                                name: db.name.clone(),
                                tables,
                                views: Vec::new(),
                                custom_types: None,
                            };

                            Self::build_document_db_content(
                                profile_id,
                                &db.name,
                                &db_schema,
                                &connected.table_details,
                                &connected.collection_children,
                                conn_capabilities,
                                conn_category,
                                Some(&metric_cache),
                                metric_fetch_errors,
                            )
                        } else if is_time_series_db {
                            // InfluxDB uses SingleDatabase loading: the connection-level
                            // schema already contains all measurements for this bucket.
                            Self::build_time_series_db_content(profile_id, &db.name, schema)
                        } else {
                            Self::build_schema_children(
                                profile_id,
                                &db.name,
                                Some(&db.name),
                                schema,
                                &connected.table_details,
                                &connected.schema_types,
                                &connected.schema_indexes,
                                &connected.schema_foreign_keys,
                                &connected.schema_routines,
                                supports_routines,
                                &connected.dependents_cache,
                            )
                        }
                    } else if is_pending {
                        vec![TreeItem::new(
                            SchemaNodeId::Loading {
                                profile_id,
                                database: db.name.clone(),
                            }
                            .to_string(),
                            "Loading...".to_string(),
                        )]
                    } else {
                        Vec::new()
                    };

                    if collapse_single_db {
                        profile_children.extend(db_children);
                    } else {
                        let db_label = if is_pending {
                            format!("{} (loading...)", db.name)
                        } else {
                            db.name.clone()
                        };

                        let has_per_db_conn = connected.database_connections.contains_key(&db.name);
                        let is_expanded = if uses_lazy_loading {
                            is_active_db
                        } else {
                            db.is_current || has_per_db_conn
                        };

                        named_db_items.push(
                            TreeItem::new(
                                SchemaNodeId::Database {
                                    profile_id,
                                    name: db.name.clone(),
                                }
                                .to_string(),
                                db_label,
                            )
                            .expanded(is_expanded)
                            .children(db_children),
                        );
                    }
                }

                if !named_db_items.is_empty() {
                    profile_children.push(Self::build_databases_folder_item(
                        profile_id,
                        named_db_items,
                    ));
                }
            } else {
                // No databases defined - use active_database or first schema as fallback
                let database_name = connected
                    .active_database
                    .as_deref()
                    .or_else(|| schema.schemas().first().map(|s| s.name.as_str()))
                    .unwrap_or("default");

                profile_children = Self::build_schema_children(
                    profile_id,
                    database_name,
                    None,
                    schema,
                    &connected.table_details,
                    &connected.schema_types,
                    &connected.schema_indexes,
                    &connected.schema_foreign_keys,
                    &connected.schema_routines,
                    supports_routines,
                    &connected.dependents_cache,
                );
            }

            // Instance overview, metrics, and inspectors — appended after databases.
            // Sidebar order: Instance Overview, Instance Metrics, Instance Inspectors.
            // Capability-gated; no driver_id branching.
            let has_instance_metrics =
                conn_capabilities.contains(DriverCapabilities::INSTANCE_METRICS);
            let has_instance_inspector =
                conn_capabilities.contains(DriverCapabilities::INSTANCE_INSPECTOR);

            if has_instance_metrics || has_instance_inspector {
                profile_children.push(Self::build_instance_overview_leaf(profile_id));
            }

            if has_instance_metrics {
                let metric_children =
                    Self::build_instance_metric_leaf_children(profile_id, instance_metrics_cache);
                profile_children.push(Self::build_instance_metrics_folder_item(
                    profile_id,
                    metric_children,
                ));
            }

            if has_instance_inspector {
                let inspector_children = Self::build_instance_inspector_leaf_children(
                    profile_id,
                    instance_inspectors_cache,
                );
                profile_children.push(Self::build_instance_inspectors_folder_item(
                    profile_id,
                    inspector_children,
                ));
            }

            profile_item = profile_item.expanded(is_active).children(profile_children);
        }

        profile_item
    }

    // -----------------------------------------------------------------------
    // Dashboard and Saved Charts sidebar folder helpers
    // -----------------------------------------------------------------------

    /// Build the `DashboardsFolder` tree node for a connected profile.
    ///
    /// Children are one `DashboardItem` per dashboard returned by the manager,
    /// sorted by `updated_at` descending. When no dashboards exist a single
    /// non-clickable placeholder hints the user to right-click.
    fn build_dashboards_folder_item(profile_id: Uuid, state: &AppStateEntity) -> TreeItem {
        let children = Self::build_dashboard_children(profile_id, state);
        TreeItem::new(
            SchemaNodeId::DashboardsFolder { profile_id }.to_string(),
            "Dashboards".to_string(),
        )
        .expanded(false)
        .children(children)
    }

    /// Build the container that lists dashboards fetched live from the
    /// connection's upstream source (drivers advertising `DASHBOARD_SYNC`).
    ///
    /// The label is taken from the driver's `DashboardSource::container_label`
    /// so the UI never hard-codes a driver name. Children are lazy: a "Loading"
    /// placeholder until the listing cache is populated on first expansion.
    fn build_remote_dashboards_folder_item(
        profile_id: Uuid,
        state: &AppStateEntity,
        fetch_errors: &HashMap<String, String>,
    ) -> TreeItem {
        let label = state
            .connections()
            .get(&profile_id)
            .and_then(|conn| {
                conn.connection
                    .dashboard_source()
                    .map(|s| s.container_label().to_string())
            })
            .unwrap_or_else(|| "Dashboards".to_string());

        let folder_id = SchemaNodeId::RemoteDashboardsFolder { profile_id }.to_string();
        let children =
            Self::build_remote_dashboard_children(profile_id, state, &folder_id, fetch_errors);

        let label = match state.remote_dashboard_cache().peek(profile_id) {
            Some(list) => format!("{} ({})", label, list.len()),
            None => label,
        };

        TreeItem::new(folder_id, label)
            .expanded(false)
            .children(children)
    }

    /// Build the `RemoteDashboardItem` children for a `RemoteDashboardsFolder`.
    ///
    /// Reads the session listing cache. A cache miss yields a single "Loading"
    /// placeholder; the fetch is kicked off when the folder is expanded. A
    /// recorded fetch error yields an error node, and an empty listing yields a
    /// non-clickable hint.
    fn build_remote_dashboard_children(
        profile_id: Uuid,
        state: &AppStateEntity,
        folder_id: &str,
        fetch_errors: &HashMap<String, String>,
    ) -> Vec<TreeItem> {
        let Some(dashboards) = state.remote_dashboard_cache().peek(profile_id) else {
            if let Some(error) = fetch_errors.get(folder_id) {
                return vec![TreeItem::new(
                    format!("remote_dashboards_error:{profile_id}"),
                    format!("Error: {error} — collapse and expand to retry"),
                )];
            }
            return vec![Self::loading_placeholder(
                profile_id,
                "",
                "remote-dashboards-loading",
            )];
        };

        if dashboards.is_empty() {
            return vec![TreeItem::new(
                format!("remote_dashboards_empty:{profile_id}"),
                "No dashboards in this account/region".to_string(),
            )];
        }

        dashboards
            .iter()
            .map(|d| {
                TreeItem::new(
                    SchemaNodeId::RemoteDashboardItem {
                        profile_id,
                        name: d.name.clone(),
                    }
                    .to_string(),
                    d.name.clone(),
                )
            })
            .collect()
    }

    /// Build the `SavedChartsFolder` tree node for a connected profile.
    ///
    /// Children are one `SavedChartItem` per chart returned by the manager,
    /// sorted by `updated_at` descending. When no charts exist a placeholder
    /// is shown.
    fn build_saved_charts_folder_item(profile_id: Uuid, state: &AppStateEntity) -> TreeItem {
        let children = Self::build_saved_chart_children(profile_id, state);
        TreeItem::new(
            SchemaNodeId::SavedChartsFolder { profile_id }.to_string(),
            "Saved Charts".to_string(),
        )
        .expanded(false)
        .children(children)
    }

    /// Build the `DatabasesFolder` tree node for a connected profile.
    ///
    /// The folder is expanded by default so databases are immediately visible
    /// after connecting — matching the behaviour users had before the grouping
    /// folder was introduced.
    fn build_databases_folder_item(profile_id: Uuid, children: Vec<TreeItem>) -> TreeItem {
        TreeItem::new(
            SchemaNodeId::DatabasesFolder { profile_id }.to_string(),
            "Databases".to_string(),
        )
        .expanded(true)
        .children(children)
    }

    #[cfg(test)]
    pub(crate) fn build_databases_folder_item_for_test(
        profile_id: Uuid,
        children: Vec<TreeItem>,
    ) -> TreeItem {
        Self::build_databases_folder_item(profile_id, children)
    }

    /// Build the `InstanceMetricsFolder` folder item for a connected profile.
    ///
    /// `children` is populated from the session-scoped `instance_metrics_cache`
    /// on tree rebuilds after the first expansion. On the first expansion the
    /// cache is empty, so the folder shows a loading placeholder until
    /// `spawn_fetch_instance_catalog` completes and calls `rebuild_tree_with_overrides`.
    pub(crate) fn build_instance_metrics_folder_item(
        profile_id: Uuid,
        children: Vec<TreeItem>,
    ) -> TreeItem {
        TreeItem::new(
            SchemaNodeId::InstanceMetricsFolder { profile_id }.to_string(),
            "Instance Metrics".to_string(),
        )
        .expanded(false)
        .children(children)
    }

    /// Build the `InstanceInspectorsFolder` folder item for a connected profile.
    ///
    /// `children` is populated from the session-scoped `instance_inspectors_cache`
    /// on tree rebuilds after the first expansion. On the first expansion the
    /// cache is empty, so the folder shows a loading placeholder until
    /// `spawn_fetch_instance_catalog` completes and calls `rebuild_tree_with_overrides`.
    pub(crate) fn build_instance_inspectors_folder_item(
        profile_id: Uuid,
        children: Vec<TreeItem>,
    ) -> TreeItem {
        TreeItem::new(
            SchemaNodeId::InstanceInspectorsFolder { profile_id }.to_string(),
            "Instance Inspectors".to_string(),
        )
        .expanded(false)
        .children(children)
    }

    /// Build `InstanceMetricLeaf` children for the `InstanceMetricsFolder`.
    ///
    /// Returns a loading placeholder when the cache has no entry for `profile_id`
    /// (fetch not yet complete), the "No metrics available" placeholder when the
    /// cache entry is present but empty (probe completed, nothing to show), or one
    /// leaf per cached metric definition once the fetch resolves.
    pub(crate) fn build_instance_metric_leaf_children(
        profile_id: Uuid,
        cache: &HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>>,
    ) -> Vec<TreeItem> {
        let Some(metrics) = cache.get(&profile_id) else {
            return vec![TreeItem::new(
                format!("instance-metrics-loading:{profile_id}"),
                "Loading\u{2026}".to_string(),
            )];
        };

        if metrics.is_empty() {
            return vec![TreeItem::new(
                format!("instance-metrics-empty:{profile_id}"),
                "No metrics available".to_string(),
            )];
        }

        metrics
            .iter()
            .map(|m| {
                TreeItem::new(
                    SchemaNodeId::InstanceMetricLeaf {
                        profile_id,
                        metric_id: m.id.clone(),
                    }
                    .to_string(),
                    m.display_name.clone(),
                )
            })
            .collect()
    }

    /// Build `InstanceInspectorLeaf` children for the `InstanceInspectorsFolder`.
    ///
    /// Returns a loading placeholder when the cache has no entry for `profile_id`
    /// (fetch not yet complete), the "No inspectors available" placeholder when
    /// the cache entry is present but empty, or one leaf per cached inspector
    /// definition once the fetch resolves.
    pub(crate) fn build_instance_inspector_leaf_children(
        profile_id: Uuid,
        cache: &HashMap<Uuid, Vec<dbflux_core::InstanceInspectorDef>>,
    ) -> Vec<TreeItem> {
        let Some(inspectors) = cache.get(&profile_id) else {
            return vec![TreeItem::new(
                format!("instance-inspectors-loading:{profile_id}"),
                "Loading\u{2026}".to_string(),
            )];
        };

        if inspectors.is_empty() {
            return vec![TreeItem::new(
                format!("instance-inspectors-empty:{profile_id}"),
                "No inspectors available".to_string(),
            )];
        }

        inspectors
            .iter()
            .map(|i| {
                TreeItem::new(
                    SchemaNodeId::InstanceInspectorLeaf {
                        profile_id,
                        metric_id: i.id.clone(),
                    }
                    .to_string(),
                    i.display_name.clone(),
                )
            })
            .collect()
    }

    /// Build the `InstanceOverviewLeaf` sidebar item for a connected profile.
    ///
    /// This is a single non-folder leaf that opens the synthesized read-only
    /// "Instance Overview" dashboard. It appears above the `InstanceMetricsFolder`
    /// and `InstanceInspectorsFolder` when the driver advertises either capability.
    pub(crate) fn build_instance_overview_leaf(profile_id: Uuid) -> TreeItem {
        TreeItem::new(
            SchemaNodeId::InstanceOverviewLeaf { profile_id }.to_string(),
            "Instance Overview".to_string(),
        )
    }

    /// Build the `DashboardItem` children for the `DashboardsFolder`.
    ///
    /// Returns items sorted by `updated_at` descending (most recently updated
    /// first). When the list is empty a non-clickable placeholder node is
    /// returned so the folder does not appear empty to the user.
    fn build_dashboard_children(profile_id: Uuid, state: &AppStateEntity) -> Vec<TreeItem> {
        let mut dashboards = state.dashboards.dashboards_for_profile(profile_id);

        dashboards.sort_by_key(|d| std::cmp::Reverse(d.updated_at));

        if dashboards.is_empty() {
            let can_import = state
                .connections()
                .get(&profile_id)
                .map(|conn| conn.connection.metadata().capabilities)
                .is_some_and(|caps| {
                    caps.contains(dbflux_core::DriverCapabilities::DASHBOARD_IMPORT)
                });

            let hint = if can_import {
                "No dashboards yet — right-click to create or import"
            } else {
                "No dashboards yet — right-click to create"
            };

            return vec![TreeItem::new(
                format!("dashboards_empty:{profile_id}"),
                hint.to_string(),
            )];
        }

        dashboards
            .into_iter()
            .map(|d| {
                TreeItem::new(
                    SchemaNodeId::DashboardItem {
                        profile_id,
                        dashboard_id: d.id,
                    }
                    .to_string(),
                    d.name.clone(),
                )
            })
            .collect()
    }

    /// Build the `SavedChartItem` children for the `SavedChartsFolder`.
    ///
    /// Returns items sorted by `updated_at` descending. When the list is empty
    /// a non-clickable placeholder node is returned.
    fn build_saved_chart_children(profile_id: Uuid, state: &AppStateEntity) -> Vec<TreeItem> {
        let mut charts = state.saved_charts.charts_for_profile(profile_id);

        charts.sort_by_key(|c| std::cmp::Reverse(c.updated_at));

        if charts.is_empty() {
            return vec![TreeItem::new(
                format!("saved_charts_empty:{profile_id}"),
                "No saved charts yet — save a chart from a query result".to_string(),
            )];
        }

        charts
            .into_iter()
            .map(|c| {
                TreeItem::new(
                    SchemaNodeId::SavedChartItem {
                        profile_id,
                        chart_id: c.id,
                    }
                    .to_string(),
                    Self::saved_chart_display_label(c),
                )
            })
            .collect()
    }

    /// Resolve a non-empty label for a saved chart even when its `name` is
    /// blank — protects the sidebar from rendering invisible rows for charts
    /// imported before the title-fallback fix or for any future code path
    /// that persists an empty name.
    ///
    /// Order: persisted name -> joined distinct metric names (for Metric
    /// sources) -> "Untitled chart".
    fn saved_chart_display_label(chart: &dbflux_components::SavedChart) -> String {
        if !chart.name.trim().is_empty() {
            return chart.name.clone();
        }

        if let dbflux_components::SavedChartSource::Metric { series } = &chart.source
            && !series.is_empty()
        {
            let mut names: Vec<&str> = series.iter().map(|s| s.metric_name.as_str()).collect();
            names.sort_unstable();
            names.dedup();
            let joined = names.join(", ");
            if !joined.is_empty() {
                return joined;
            }
        }

        "Untitled chart".to_string()
    }

    pub(super) fn count_visible_entries(items: &[TreeItem]) -> usize {
        fn count_recursive(item: &TreeItem) -> usize {
            let mut count = 1;
            if item.is_expanded() && item.is_folder() {
                for child in &item.children {
                    count += count_recursive(child);
                }
            }
            count
        }

        items.iter().map(count_recursive).sum()
    }

    pub(super) fn find_item_index(&self, item_id: &str, cx: &Context<Self>) -> Option<usize> {
        match self.active_tab {
            SidebarTab::Connections => {
                let items = self.build_tree_items_with_overrides(cx);
                Self::find_item_index_in_tree(&items, item_id, &mut 0)
            }
            SidebarTab::Scripts => {
                let state = self.app_state.read(cx);
                let entries = match state.scripts_directory() {
                    Some(dir) => {
                        dbflux_core::filter_entries(dir.entries(), &self.scripts_search_query)
                    }
                    None => return None,
                };
                let items = Self::build_scripts_tree_items(&entries);
                Self::find_item_index_in_tree(&items, item_id, &mut 0)
            }
        }
    }

    pub(super) fn find_item_index_in_tree(
        items: &[TreeItem],
        target_id: &str,
        current_index: &mut usize,
    ) -> Option<usize> {
        for item in items {
            if item.id.as_ref() == target_id {
                return Some(*current_index);
            }
            *current_index += 1;

            if item.is_expanded()
                && item.is_folder()
                && let Some(idx) =
                    Self::find_item_index_in_tree(&item.children, target_id, current_index)
            {
                return Some(idx);
            }
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn build_schema_children(
        profile_id: Uuid,
        database_name: &str,
        target_database: Option<&str>,
        snapshot: &dbflux_core::SchemaSnapshot,
        table_details: &HashMap<(String, String), TableInfo>,
        schema_types: &HashMap<SchemaCacheKey, Vec<CustomTypeInfo>>,
        schema_indexes: &HashMap<SchemaCacheKey, Vec<SchemaIndexInfo>>,
        schema_foreign_keys: &HashMap<SchemaCacheKey, Vec<SchemaForeignKeyInfo>>,
        schema_routines: &HashMap<SchemaCacheKey, Vec<RoutineInfo>>,
        supports_routines: bool,
        dependents_cache: &HashMap<(String, String), Vec<RelationRef>>,
    ) -> Vec<TreeItem> {
        let mut children = Vec::new();

        for db_schema in snapshot.schemas() {
            let schema_content = Self::build_db_schema_content(
                profile_id,
                database_name,
                target_database,
                db_schema,
                table_details,
                schema_types,
                schema_indexes,
                schema_foreign_keys,
                schema_routines,
                supports_routines,
                dependents_cache,
            );

            children.push(
                TreeItem::new(
                    SchemaNodeId::Schema {
                        profile_id,
                        name: db_schema.name.clone(),
                    }
                    .to_string(),
                    db_schema.name.clone(),
                )
                .expanded(db_schema.name == "public")
                .children(schema_content),
            );
        }

        children
    }

    #[allow(clippy::too_many_arguments)]
    fn build_document_db_content(
        profile_id: Uuid,
        database_name: &str,
        db_schema: &dbflux_core::DbSchemaInfo,
        table_details: &HashMap<(String, String), TableInfo>,
        collection_children_cache: &HashMap<(String, String), dbflux_core::CollectionChildrenCache>,
        capabilities: DriverCapabilities,
        category: dbflux_core::DatabaseCategory,
        metric_catalog_cache: Option<&dbflux_app::MetricCatalogCache>,
        metric_fetch_errors: &HashMap<String, String>,
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();

        if !db_schema.tables.is_empty() {
            let collection_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|coll| {
                    Self::build_collection_item(
                        profile_id,
                        database_name,
                        coll,
                        table_details,
                        collection_children_cache,
                    )
                })
                .collect();

            content.push(
                TreeItem::new(
                    SchemaNodeId::CollectionsFolder {
                        profile_id,
                        database: database_name.to_string(),
                    }
                    .to_string(),
                    format!("{} ({})", category.container_name(), db_schema.tables.len()),
                )
                .expanded(category.default_expand_container())
                .children(collection_children),
            );
        }

        if capabilities.contains(DriverCapabilities::METRIC_CATALOG) {
            let parent_id = SchemaNodeId::MetricsFolder {
                profile_id,
                database: database_name.to_string(),
            }
            .to_string();

            let children = if let Some(err_msg) = metric_fetch_errors.get(&parent_id) {
                let retry_id = format!("metrics-retry|{}|{}", profile_id, database_name);
                vec![Self::error_retry_placeholder(&retry_id, err_msg)]
            } else {
                Self::build_metric_namespace_children(
                    profile_id,
                    database_name,
                    metric_catalog_cache,
                )
            };

            content.push(
                TreeItem::new(parent_id, "Metrics".to_string())
                    .expanded(false)
                    .children(children),
            );
        }

        let all_index_items: Vec<TreeItem> = db_schema
            .tables
            .iter()
            .filter_map(|coll| {
                let doc_indexes = match coll.indexes.as_ref()? {
                    IndexData::Document(v) => v,
                    IndexData::Relational(v) => {
                        return Some(
                            v.iter()
                                .map(|idx| {
                                    let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                                    let pk_marker = if idx.is_primary { " PK" } else { "" };
                                    let cols = idx.columns.join(", ");
                                    let label = format!(
                                        "{}.{} ({}){}{}",
                                        coll.name, idx.name, cols, unique_marker, pk_marker
                                    );

                                    TreeItem::new(
                                        SchemaNodeId::CollectionIndex {
                                            profile_id,
                                            collection: coll.name.to_string(),
                                            name: idx.name.clone(),
                                        }
                                        .to_string(),
                                        label,
                                    )
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                };

                Some(
                    doc_indexes
                        .iter()
                        .map(|idx| {
                            let label =
                                format!("{}.{}", coll.name, format_collection_index_label(idx));

                            TreeItem::new(
                                SchemaNodeId::CollectionIndex {
                                    profile_id,
                                    collection: coll.name.to_string(),
                                    name: idx.name.clone(),
                                }
                                .to_string(),
                                label,
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect();

        if !all_index_items.is_empty() {
            content.push(
                TreeItem::new(
                    SchemaNodeId::DatabaseIndexesFolder {
                        profile_id,
                        database: database_name.to_string(),
                    }
                    .to_string(),
                    format!("Indexes ({})", all_index_items.len()),
                )
                .expanded(false)
                .children(all_index_items),
            );
        }

        content
    }

    /// Build the namespace children for a `MetricsFolder` node.
    ///
    /// Peeks the `MetricCatalogCache`; if populated, emits one
    /// `MetricNamespaceFolder` child per cached namespace. On a cache miss
    /// (data not yet fetched) emits a single "Loading..." placeholder so the
    /// user sees feedback. The expansion handler triggers the background fetch.
    /// If `metric_fetch_errors` contains an entry for the parent MetricsFolder
    /// node id, an error placeholder is rendered instead.
    pub(crate) fn build_metric_namespace_children(
        profile_id: Uuid,
        database_name: &str,
        metric_catalog_cache: Option<&dbflux_app::MetricCatalogCache>,
    ) -> Vec<TreeItem> {
        let Some(cache) = metric_catalog_cache else {
            return vec![Self::loading_placeholder(
                profile_id,
                database_name,
                "metrics-loading",
            )];
        };

        let Some(namespaces) = cache.peek_namespaces(profile_id) else {
            return vec![Self::loading_placeholder(
                profile_id,
                database_name,
                "metrics-loading",
            )];
        };

        namespaces
            .iter()
            .map(|ns| {
                let leaf_children = Self::build_metric_leaf_children(
                    profile_id,
                    database_name,
                    ns,
                    metric_catalog_cache,
                );
                TreeItem::new(
                    SchemaNodeId::MetricNamespaceFolder {
                        profile_id,
                        database: database_name.to_string(),
                        namespace: ns.clone(),
                    }
                    .to_string(),
                    ns.clone(),
                )
                .expanded(false)
                .children(leaf_children)
            })
            .collect()
    }

    /// Build the metric leaf children for a `MetricNamespaceFolder` node.
    ///
    /// Peeks the cache for this `(profile_id, namespace)` pair. On a miss,
    /// returns a single loading placeholder. On a hit, returns one `MetricLeaf`
    /// per cached descriptor.
    pub(crate) fn build_metric_leaf_children(
        profile_id: Uuid,
        database_name: &str,
        namespace: &dbflux_core::MetricNamespace,
        metric_catalog_cache: Option<&dbflux_app::MetricCatalogCache>,
    ) -> Vec<TreeItem> {
        let Some(cache) = metric_catalog_cache else {
            return vec![Self::loading_placeholder(
                profile_id,
                database_name,
                &format!("metrics-ns-loading|{}", namespace),
            )];
        };

        let Some(page) = cache.peek_metrics(profile_id, namespace) else {
            return vec![Self::loading_placeholder(
                profile_id,
                database_name,
                &format!("metrics-ns-loading|{}", namespace),
            )];
        };

        // CloudWatch returns one descriptor per (metric_name, dimension_combo)
        // pair, so a 1000-instance AWS/EC2 account would otherwise produce 1000
        // identical "CPUUtilization" leaves. Deduplicate by metric_name; the
        // dimension picker inside the chart document handles dimension choice.
        // BTreeSet keeps the order alphabetical, which is also nicer UX.
        let unique_names: std::collections::BTreeSet<&str> = page
            .accumulated
            .iter()
            .map(|desc| desc.metric_name.as_str())
            .collect();

        unique_names
            .into_iter()
            .map(|metric_name| {
                TreeItem::new(
                    SchemaNodeId::MetricLeaf {
                        profile_id,
                        database: database_name.to_string(),
                        namespace: namespace.clone(),
                        metric_name: metric_name.to_string(),
                    }
                    .to_string(),
                    metric_name.to_string(),
                )
            })
            .collect()
    }

    /// Build a non-typed placeholder `TreeItem` used for Loading / error sentinel nodes.
    ///
    /// The sentinel ID is purposely not a valid `SchemaNodeId` so the expansion
    /// dispatcher ignores it rather than misrouting it.
    pub(crate) fn loading_placeholder(
        profile_id: Uuid,
        database_name: &str,
        suffix: &str,
    ) -> TreeItem {
        let id = format!("{}|{}|{}", suffix, profile_id, database_name);
        TreeItem::new(id, "Loading...".to_string())
    }

    /// Build an error retry placeholder child for metric sidebar nodes.
    ///
    /// The sentinel ID encodes the retry key so `execute_item` can route it
    /// back to the appropriate fetch helper.
    pub(crate) fn error_retry_placeholder(retry_sentinel_id: &str, error_msg: &str) -> TreeItem {
        let label = format!("Error: {} — click to retry", error_msg);
        TreeItem::new(retry_sentinel_id.to_string(), label)
    }

    /// Build sidebar children for a time-series database node (e.g. an InfluxDB bucket).
    ///
    /// Measurements are rendered as `Collection` nodes under a "Measurements (N)" folder so they
    /// participate in the existing open/query/context-menu flows without requiring new node-kind
    /// variants. The `DatabaseCategory::TimeSeries.container_name()` already returns "Measurements".
    fn build_time_series_db_content(
        profile_id: Uuid,
        database_name: &str,
        schema: &dbflux_core::SchemaSnapshot,
    ) -> Vec<TreeItem> {
        let measurements = schema.measurements();

        if measurements.is_empty() {
            return Vec::new();
        }

        let measurement_items: Vec<TreeItem> = measurements
            .iter()
            .map(|measurement| {
                TreeItem::new(
                    SchemaNodeId::Collection {
                        profile_id,
                        database: database_name.to_string(),
                        name: measurement.name.clone(),
                    }
                    .to_string(),
                    measurement.name.clone(),
                )
            })
            .collect();

        vec![
            TreeItem::new(
                SchemaNodeId::CollectionsFolder {
                    profile_id,
                    database: database_name.to_string(),
                }
                .to_string(),
                format!("Measurements ({})", measurements.len()),
            )
            .expanded(true)
            .children(measurement_items),
        ]
    }

    fn build_collection_item(
        profile_id: Uuid,
        database_name: &str,
        collection: &dbflux_core::TableInfo,
        table_details: &HashMap<(String, String), TableInfo>,
        collection_children_cache: &HashMap<(String, String), dbflux_core::CollectionChildrenCache>,
    ) -> TreeItem {
        let coll_name = &collection.name;
        let cache_key = (database_name.to_string(), coll_name.clone());
        let effective = table_details.get(&cache_key).unwrap_or(collection);
        let paged_children = collection_children_cache.get(&cache_key);
        let child_items = paged_children
            .map(|cache| cache.items.clone())
            .or_else(|| effective.child_items.clone());
        let has_more_children = paged_children
            .and_then(|cache| cache.next_page_token.as_ref())
            .is_some();
        let details_loaded = effective.sample_fields.is_some()
            || child_items.as_ref().is_some_and(|items| !items.is_empty());

        let (field_children, field_count) = if let Some(fields) = effective.sample_fields.as_ref() {
            (
                build_collection_field_items(profile_id, coll_name, fields),
                fields.len(),
            )
        } else {
            (Vec::new(), 0)
        };

        let (index_children, index_count) = if details_loaded {
            match effective.indexes.as_ref() {
                Some(IndexData::Document(doc_indexes)) => {
                    let children: Vec<TreeItem> = doc_indexes
                        .iter()
                        .map(|idx| {
                            let label = format_collection_index_label(idx);

                            TreeItem::new(
                                SchemaNodeId::CollectionIndex {
                                    profile_id,
                                    collection: coll_name.to_string(),
                                    name: idx.name.clone(),
                                }
                                .to_string(),
                                label,
                            )
                        })
                        .collect();

                    let count = children.len();
                    (children, count)
                }

                Some(IndexData::Relational(indexes)) => {
                    let children: Vec<TreeItem> = indexes
                        .iter()
                        .map(|idx| {
                            let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                            let pk_marker = if idx.is_primary { " PK" } else { "" };
                            let cols = idx.columns.join(", ");
                            let label =
                                format!("{} ({}){}{}", idx.name, cols, unique_marker, pk_marker);

                            TreeItem::new(
                                SchemaNodeId::CollectionIndex {
                                    profile_id,
                                    collection: coll_name.to_string(),
                                    name: idx.name.clone(),
                                }
                                .to_string(),
                                label,
                            )
                        })
                        .collect();

                    let count = children.len();
                    (children, count)
                }

                _ => (Vec::new(), 0),
            }
        } else {
            (Vec::new(), 0)
        };

        let collection_children = if effective.presentation == CollectionPresentation::EventStream {
            // Event-stream collections are leaves in the tree: streams are
            // browsed exclusively through the dedicated picker modal, never
            // inline. Suppressing children also removes the expand chevron.
            let _ = (child_items, has_more_children);
            Vec::new()
        } else {
            vec![
                TreeItem::new(
                    SchemaNodeId::CollectionFieldsFolder {
                        profile_id,
                        database: database_name.to_string(),
                        collection: coll_name.to_string(),
                    }
                    .to_string(),
                    format!("Fields ({})", field_count),
                )
                .expanded(false)
                .children(field_children),
                TreeItem::new(
                    SchemaNodeId::CollectionIndexesFolder {
                        profile_id,
                        database: database_name.to_string(),
                        collection: coll_name.to_string(),
                    }
                    .to_string(),
                    format!("Indexes ({})", index_count),
                )
                .expanded(false)
                .children(index_children),
            ]
        };

        TreeItem::new(
            SchemaNodeId::Collection {
                profile_id,
                database: database_name.to_string(),
                name: coll_name.to_string(),
            }
            .to_string(),
            coll_name.clone(),
        )
        .expanded(false)
        .children(collection_children)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_db_schema_content(
        profile_id: Uuid,
        database_name: &str,
        target_database: Option<&str>,
        db_schema: &dbflux_core::DbSchemaInfo,
        table_details: &HashMap<(String, String), TableInfo>,
        schema_types: &HashMap<SchemaCacheKey, Vec<CustomTypeInfo>>,
        schema_indexes: &HashMap<SchemaCacheKey, Vec<SchemaIndexInfo>>,
        schema_foreign_keys: &HashMap<SchemaCacheKey, Vec<SchemaForeignKeyInfo>>,
        schema_routines: &HashMap<SchemaCacheKey, Vec<RoutineInfo>>,
        supports_routines: bool,
        dependents_cache: &HashMap<(String, String), Vec<RelationRef>>,
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();
        let schema_name = &db_schema.name;

        if !db_schema.tables.is_empty() {
            let table_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|table| {
                    let item_schema = table.schema.as_deref().unwrap_or(schema_name);
                    Self::build_table_item(
                        profile_id,
                        target_database,
                        item_schema,
                        table,
                        table_details,
                        dependents_cache,
                    )
                })
                .collect();

            content.push(
                TreeItem::new(
                    SchemaNodeId::TablesFolder {
                        profile_id,
                        schema: schema_name.to_string(),
                    }
                    .to_string(),
                    format!("Tables ({})", db_schema.tables.len()),
                )
                .expanded(true)
                .children(table_children),
            );
        }

        if !db_schema.views.is_empty() {
            let view_children: Vec<TreeItem> = db_schema
                .views
                .iter()
                .map(|view| {
                    let item_schema = view.schema.as_deref().unwrap_or(schema_name);
                    TreeItem::new(
                        SchemaNodeId::View {
                            profile_id,
                            database: target_database.map(str::to_string),
                            schema: item_schema.to_string(),
                            name: view.name.clone(),
                        }
                        .to_string(),
                        view.name.clone(),
                    )
                })
                .collect();

            content.push(
                TreeItem::new(
                    SchemaNodeId::ViewsFolder {
                        profile_id,
                        schema: schema_name.to_string(),
                    }
                    .to_string(),
                    format!("Views ({})", db_schema.views.len()),
                )
                .expanded(true)
                .children(view_children),
            );
        }

        // Custom types (enums, domains, composites) - check cache first, then db_schema
        let types_cache_key = SchemaCacheKey::new(database_name, Some(schema_name));
        let cached_types = schema_types.get(&types_cache_key);

        let custom_types: Option<&Vec<CustomTypeInfo>> =
            cached_types.or(db_schema.custom_types.as_ref());

        // Item ID format: types_{profile_id}_{database}_{schema}
        let types_item_id = SchemaNodeId::TypesFolder {
            profile_id,
            database: database_name.to_string(),
            schema: schema_name.to_string(),
        }
        .to_string();

        if let Some(types) = custom_types {
            if !types.is_empty() {
                let type_children: Vec<TreeItem> = types
                    .iter()
                    .map(|t| {
                        let item_schema = t.schema.as_deref().unwrap_or(schema_name);
                        Self::build_custom_type_item(profile_id, item_schema, t)
                    })
                    .collect();

                content.push(
                    TreeItem::new(types_item_id, format!("Data Types ({})", types.len()))
                        .expanded(false)
                        .children(type_children),
                );
            } else {
                // Types loaded but empty - show folder without count
                content.push(
                    TreeItem::new(types_item_id, "Data Types (0)".to_string())
                        .expanded(false)
                        .children(vec![]),
                );
            }
        } else {
            // Placeholder so chevron appears; fetch triggers on expand
            let placeholder = TreeItem::new(
                SchemaNodeId::TypesLoadingFolder {
                    profile_id,
                    database: database_name.to_string(),
                    schema: schema_name.to_string(),
                }
                .to_string(),
                "Loading...".to_string(),
            );

            content.push(
                TreeItem::new(types_item_id, "Data Types".to_string())
                    .expanded(false)
                    .children(vec![placeholder]),
            );
        }

        // Schema-level Indexes folder
        let indexes_cache_key = SchemaCacheKey::new(database_name, Some(schema_name));
        let cached_indexes = schema_indexes.get(&indexes_cache_key);
        let indexes_item_id = SchemaNodeId::SchemaIndexesFolder {
            profile_id,
            database: database_name.to_string(),
            schema: schema_name.to_string(),
        }
        .to_string();

        if let Some(indexes) = cached_indexes {
            if !indexes.is_empty() {
                let index_children: Vec<TreeItem> = indexes
                    .iter()
                    .map(|idx| {
                        let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                        let pk_marker = if idx.is_primary { " PK" } else { "" };
                        let label = format!(
                            "{}.{} ({}){}{}",
                            idx.table_name,
                            idx.name,
                            idx.columns.join(", "),
                            unique_marker,
                            pk_marker
                        );
                        TreeItem::new(
                            SchemaNodeId::SchemaIndex {
                                profile_id,
                                schema: schema_name.to_string(),
                                name: idx.name.clone(),
                            }
                            .to_string(),
                            label,
                        )
                    })
                    .collect();

                content.push(
                    TreeItem::new(indexes_item_id, format!("Indexes ({})", indexes.len()))
                        .expanded(false)
                        .children(index_children),
                );
            } else {
                content.push(
                    TreeItem::new(indexes_item_id, "Indexes (0)".to_string())
                        .expanded(false)
                        .children(vec![]),
                );
            }
        } else {
            let placeholder = TreeItem::new(
                SchemaNodeId::SchemaIndexesLoadingFolder {
                    profile_id,
                    database: database_name.to_string(),
                    schema: schema_name.to_string(),
                }
                .to_string(),
                "Loading...".to_string(),
            );

            content.push(
                TreeItem::new(indexes_item_id, "Indexes".to_string())
                    .expanded(false)
                    .children(vec![placeholder]),
            );
        }

        // Schema-level Foreign Keys folder
        let fks_cache_key = SchemaCacheKey::new(database_name, Some(schema_name));
        let cached_fks = schema_foreign_keys.get(&fks_cache_key);
        let fks_item_id = SchemaNodeId::SchemaForeignKeysFolder {
            profile_id,
            database: database_name.to_string(),
            schema: schema_name.to_string(),
        }
        .to_string();

        if let Some(fks) = cached_fks {
            if !fks.is_empty() {
                let fk_children: Vec<TreeItem> = fks
                    .iter()
                    .map(|fk| {
                        let ref_table = if let Some(ref schema) = fk.referenced_schema {
                            format!("{}.{}", schema, fk.referenced_table)
                        } else {
                            fk.referenced_table.clone()
                        };
                        let label = format!(
                            "{}.{} -> {}",
                            fk.table_name,
                            fk.columns.join(", "),
                            ref_table
                        );
                        TreeItem::new(
                            SchemaNodeId::SchemaForeignKey {
                                profile_id,
                                schema: schema_name.to_string(),
                                name: fk.name.clone(),
                            }
                            .to_string(),
                            label,
                        )
                    })
                    .collect();

                content.push(
                    TreeItem::new(fks_item_id, format!("Foreign Keys ({})", fks.len()))
                        .expanded(false)
                        .children(fk_children),
                );
            } else {
                content.push(
                    TreeItem::new(fks_item_id, "Foreign Keys (0)".to_string())
                        .expanded(false)
                        .children(vec![]),
                );
            }
        } else {
            let placeholder = TreeItem::new(
                SchemaNodeId::SchemaForeignKeysLoadingFolder {
                    profile_id,
                    database: database_name.to_string(),
                    schema: schema_name.to_string(),
                }
                .to_string(),
                "Loading...".to_string(),
            );

            content.push(
                TreeItem::new(fks_item_id, "Foreign Keys".to_string())
                    .expanded(false)
                    .children(vec![placeholder]),
            );
        }

        // Schema-level Routines folder (gated on ROUTINES driver capability)
        if supports_routines {
            let routines_cache_key = SchemaCacheKey::new(database_name, Some(schema_name));
            let cached_routines = schema_routines.get(&routines_cache_key);
            let routines_item_id = SchemaNodeId::RoutinesFolder {
                profile_id,
                database: database_name.to_string(),
                schema: schema_name.to_string(),
            }
            .to_string();

            if let Some(routines) = cached_routines {
                if !routines.is_empty() {
                    let routine_children: Vec<TreeItem> = routines
                        .iter()
                        .map(|r| {
                            let kind_label = match r.kind {
                                dbflux_core::RoutineKind::Function => "fn",
                                dbflux_core::RoutineKind::Procedure => "proc",
                                dbflux_core::RoutineKind::Aggregate => "agg",
                                dbflux_core::RoutineKind::Window => "win",
                            };
                            let label = format!("{} ({})", r.name, kind_label);
                            TreeItem::new(
                                SchemaNodeId::Routine {
                                    profile_id,
                                    schema: schema_name.to_string(),
                                    specific_name: r.specific_name.clone(),
                                }
                                .to_string(),
                                label,
                            )
                        })
                        .collect();

                    content.push(
                        TreeItem::new(routines_item_id, format!("Routines ({})", routines.len()))
                            .expanded(false)
                            .children(routine_children),
                    );
                } else {
                    content.push(
                        TreeItem::new(routines_item_id, "Routines (0)".to_string())
                            .expanded(false)
                            .children(vec![]),
                    );
                }
            } else {
                let placeholder = TreeItem::new(
                    SchemaNodeId::RoutinesLoadingFolder {
                        profile_id,
                        database: database_name.to_string(),
                        schema: schema_name.to_string(),
                    }
                    .to_string(),
                    "Loading...".to_string(),
                );

                content.push(
                    TreeItem::new(routines_item_id, "Routines".to_string())
                        .expanded(false)
                        .children(vec![placeholder]),
                );
            }
        }

        content
    }

    fn build_custom_type_item(
        profile_id: Uuid,
        schema_name: &str,
        custom_type: &CustomTypeInfo,
    ) -> TreeItem {
        let kind_label = match custom_type.kind {
            CustomTypeKind::Enum => "enum",
            CustomTypeKind::Domain => "domain",
            CustomTypeKind::Composite => "composite",
        };

        let label = format!("{} ({})", custom_type.name, kind_label);

        let mut children = Vec::new();

        // For enums, show the values as children
        if let Some(ref values) = custom_type.enum_values {
            children = values
                .iter()
                .map(|v| {
                    TreeItem::new(
                        SchemaNodeId::EnumValue {
                            profile_id,
                            schema: schema_name.to_string(),
                            type_name: custom_type.name.clone(),
                            value: v.clone(),
                        }
                        .to_string(),
                        v.clone(),
                    )
                })
                .collect();
        }

        // For domains, show the base type as a child
        if let Some(ref base_type) = custom_type.base_type {
            children.push(TreeItem::new(
                SchemaNodeId::BaseType {
                    profile_id,
                    schema: schema_name.to_string(),
                    type_name: custom_type.name.clone(),
                }
                .to_string(),
                format!("Base: {}", base_type),
            ));
        }

        TreeItem::new(
            SchemaNodeId::CustomType {
                profile_id,
                schema: schema_name.to_string(),
                name: custom_type.name.clone(),
            }
            .to_string(),
            label,
        )
        .expanded(false)
        .children(children)
    }

    fn build_table_item(
        profile_id: Uuid,
        target_database: Option<&str>,
        schema_name: &str,
        table: &dbflux_core::TableInfo,
        table_details: &HashMap<(String, String), TableInfo>,
        dependents_cache: &HashMap<(String, String), Vec<RelationRef>>,
    ) -> TreeItem {
        // Must match the key used by cache_database().
        let cache_db = target_database.unwrap_or(schema_name);
        let cache_key = (cache_db.to_string(), table.name.clone());
        let effective_table = table_details.get(&cache_key).unwrap_or(table);
        let details_loaded = effective_table.columns.is_some();

        let columns = if details_loaded {
            effective_table.columns.as_deref().unwrap_or(&[])
        } else {
            &[]
        };

        let column_children: Vec<TreeItem> = columns
            .iter()
            .map(|col| {
                let pk_marker = if col.is_primary_key { " PK" } else { "" };
                let nullable = if col.nullable { "?" } else { "" };
                let label = format!("{}: {}{}{}", col.name, col.type_name, nullable, pk_marker);

                TreeItem::new(
                    SchemaNodeId::Column {
                        profile_id,
                        table: table.name.clone(),
                        name: col.name.clone(),
                    }
                    .to_string(),
                    label,
                )
            })
            .collect();

        let index_children: Vec<TreeItem> = if details_loaded {
            match effective_table.indexes.as_ref() {
                Some(IndexData::Relational(indexes)) => indexes
                    .iter()
                    .map(|idx| {
                        let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                        let pk_marker = if idx.is_primary { " PK" } else { "" };
                        let cols = idx.columns.join(", ");
                        let label =
                            format!("{} ({}){}{}", idx.name, cols, unique_marker, pk_marker);

                        TreeItem::new(
                            SchemaNodeId::Index {
                                profile_id,
                                table: table.name.clone(),
                                name: idx.name.clone(),
                            }
                            .to_string(),
                            label,
                        )
                    })
                    .collect(),
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };

        let fk_children: Vec<TreeItem> = if details_loaded {
            effective_table
                .foreign_keys
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|fk| {
                    let ref_table = if let Some(ref schema) = fk.referenced_schema {
                        format!("{}.{}", schema, fk.referenced_table)
                    } else {
                        fk.referenced_table.clone()
                    };

                    let label = format!(
                        "{} -> {}.{}",
                        fk.columns.join(", "),
                        ref_table,
                        fk.referenced_columns.join(", ")
                    );

                    TreeItem::new(
                        SchemaNodeId::ForeignKey {
                            profile_id,
                            table: table.name.clone(),
                            name: fk.name.clone(),
                        }
                        .to_string(),
                        label,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };

        let constraint_children: Vec<TreeItem> = if details_loaded {
            effective_table
                .constraints
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|c| {
                    let kind_label = match c.kind {
                        ConstraintKind::Check => "CHECK",
                        ConstraintKind::Unique => "UNIQUE",
                        ConstraintKind::Exclusion => "EXCLUDE",
                    };

                    let detail = if c.kind == ConstraintKind::Check {
                        c.check_clause.as_deref().unwrap_or("")
                    } else {
                        &c.columns.join(", ")
                    };

                    let label = format!("{} {} ({})", c.name, kind_label, detail);

                    TreeItem::new(
                        SchemaNodeId::Constraint {
                            profile_id,
                            table: table.name.clone(),
                            name: c.name.clone(),
                        }
                        .to_string(),
                        label,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };

        let column_count = column_children.len();
        let index_count = index_children.len();
        let fk_count = fk_children.len();
        let constraint_count = constraint_children.len();

        // Lookup key must match the cache write path in populate_dependents.
        // The cache key mirrors `table_details`: (database-or-schema, table).
        let dep_key = (
            target_database.unwrap_or(schema_name).to_string(),
            table.name.clone(),
        );
        let deps = dependents_cache
            .get(&dep_key)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let dependents_section: Option<TreeItem> = if !deps.is_empty() {
            let dep_items: Vec<TreeItem> = deps
                .iter()
                .map(|dep| {
                    let kind_label = match dep.kind {
                        dbflux_core::RelationKind::View => "View",
                        dbflux_core::RelationKind::MaterializedView => "Materialized View",
                        dbflux_core::RelationKind::ForeignKeyChild => "FK Child",
                        dbflux_core::RelationKind::Trigger => "Trigger",
                    };
                    let label = format!("{} ({})", dep.qualified_name, kind_label);

                    TreeItem::new(
                        SchemaNodeId::DependentItem {
                            profile_id,
                            schema: schema_name.to_string(),
                            table: table.name.clone(),
                            name: dep.qualified_name.clone(),
                        }
                        .to_string(),
                        label,
                    )
                })
                .collect();

            Some(
                TreeItem::new(
                    SchemaNodeId::DependentsFolder {
                        profile_id,
                        schema: schema_name.to_string(),
                        table: table.name.clone(),
                    }
                    .to_string(),
                    format!("Used by {} objects", deps.len()),
                )
                .expanded(false)
                .children(dep_items),
            )
        } else {
            None
        };

        let columns_folder_id = SchemaNodeId::ColumnsFolder {
            profile_id,
            schema: schema_name.to_string(),
            table: table.name.clone(),
        }
        .to_string();
        let indexes_folder_id = SchemaNodeId::IndexesFolder {
            profile_id,
            schema: schema_name.to_string(),
            table: table.name.clone(),
        }
        .to_string();
        let fks_folder_id = SchemaNodeId::ForeignKeysFolder {
            profile_id,
            schema: schema_name.to_string(),
            table: table.name.clone(),
        }
        .to_string();
        let constraints_folder_id = SchemaNodeId::ConstraintsFolder {
            profile_id,
            schema: schema_name.to_string(),
            table: table.name.clone(),
        }
        .to_string();

        // While table details are still loading we render a single Loading row
        // directly under the table instead of four section folders with stale
        // "(0)" counts. Once details land, the four sections appear with their
        // real counts and children.
        let mut table_sections = if details_loaded {
            vec![
                TreeItem::new(columns_folder_id, format!("Columns ({})", column_count))
                    .expanded(false)
                    .children(column_children),
                TreeItem::new(indexes_folder_id, format!("Indexes ({})", index_count))
                    .expanded(false)
                    .children(index_children),
                TreeItem::new(fks_folder_id, format!("Foreign Keys ({})", fk_count))
                    .expanded(false)
                    .children(fk_children),
                TreeItem::new(
                    constraints_folder_id,
                    format!("Constraints ({})", constraint_count),
                )
                .expanded(false)
                .children(constraint_children),
            ]
        } else {
            let table_loading_id =
                format!("T|{}|{}|{}_loading", profile_id, schema_name, table.name);
            vec![TreeItem::new(table_loading_id, "Loading…".to_string())]
        };

        if let Some(dep_folder) = dependents_section {
            table_sections.push(dep_folder);
        }

        TreeItem::new(
            SchemaNodeId::Table {
                profile_id,
                database: target_database.map(str::to_string),
                schema: schema_name.to_string(),
                name: table.name.clone(),
            }
            .to_string(),
            table.name.clone(),
        )
        .expanded(false)
        .children(table_sections)
    }
}

/// Return `true` when the sidebar should hide the database wrapper level for
/// a connection.
///
/// The wrapper exists to disambiguate multiple databases under one connection.
/// When a driver exposes a single trivial database (CloudWatch's `logs`,
/// DynamoDB's default region, a SQLite file, etc.) the wrapper carries no
/// information beyond what the connection node already shows, so children are
/// rendered directly under the connection.
///
/// Multi-database drivers (Postgres, MySQL, MongoDB) are unaffected: with two
/// or more databases the wrapper still discriminates between them.
fn should_collapse_database_wrapper(databases: &[dbflux_core::DatabaseInfo]) -> bool {
    databases.len() == 1
}

fn build_collection_field_items(
    profile_id: Uuid,
    collection_name: &str,
    fields: &[dbflux_core::FieldInfo],
) -> Vec<TreeItem> {
    fields
        .iter()
        .map(|field| {
            let label = format_field_label(field);

            let mut item = TreeItem::new(
                SchemaNodeId::CollectionField {
                    profile_id,
                    collection: collection_name.to_string(),
                    name: field.name.clone(),
                }
                .to_string(),
                label,
            );

            if let Some(ref nested) = field.nested_fields
                && !nested.is_empty()
            {
                let children = build_collection_field_items(profile_id, collection_name, nested);
                item = item.expanded(false).children(children);
            }

            item
        })
        .collect()
}

fn format_field_label(field: &dbflux_core::FieldInfo) -> String {
    let mut label = format!("{}: {}", field.name, field.common_type);

    if let Some(rate) = field.occurrence_rate
        && rate < 1.0
    {
        label.push_str(&format!(" ({:.0}%)", rate * 100.0));
    }

    label
}

fn format_collection_index_label(idx: &CollectionIndexInfo) -> String {
    let keys_str = idx
        .keys
        .iter()
        .map(|(field, dir)| {
            let dir_label = match dir {
                IndexDirection::Ascending => "ASC",
                IndexDirection::Descending => "DESC",
                IndexDirection::Text => "TEXT",
                IndexDirection::Hashed => "HASHED",
                IndexDirection::Geo2d => "2D",
                IndexDirection::Geo2dSphere => "2DSPHERE",
            };
            format!("{} {}", field, dir_label)
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut label = format!("{} ({})", idx.name, keys_str);

    if idx.is_unique {
        label.push_str(" UNIQUE");
    }
    if idx.is_sparse {
        label.push_str(" SPARSE");
    }
    if let Some(ttl) = idx.expire_after_seconds {
        label.push_str(&format!(" TTL:{}s", ttl));
    }

    label
}

#[cfg(test)]
mod tests {
    use super::Sidebar;
    use dbflux_core::{
        CollectionChildInfo, CollectionChildrenCache, CollectionPresentation, CustomTypeInfo,
        FieldInfo, TableInfo,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn metric_namespaces_render_from_cache() {
        use dbflux_app::MetricCatalogCache;
        use dbflux_core::{MetricNamespace, SchemaNodeId};

        let profile_id = Uuid::new_v4();
        let cache = MetricCatalogCache::new();
        let ns1: MetricNamespace = "AWS/EC2".to_string();
        let ns2: MetricNamespace = "AWS/S3".to_string();
        cache.store_namespaces(profile_id, vec![ns1.clone(), ns2.clone()]);

        let children =
            Sidebar::build_metric_namespace_children(profile_id, "default", Some(&*cache));

        assert_eq!(children.len(), 2, "One child per namespace");
        let ids: Vec<SchemaNodeId> = children
            .iter()
            .map(|item| item.id.as_ref().parse().expect("valid SchemaNodeId"))
            .collect();
        assert!(
            ids.iter().any(|id| matches!(
                id,
                SchemaNodeId::MetricNamespaceFolder { namespace, .. } if namespace == "AWS/EC2"
            )),
            "AWS/EC2 namespace must appear"
        );
        assert!(
            ids.iter().any(|id| matches!(
                id,
                SchemaNodeId::MetricNamespaceFolder { namespace, .. } if namespace == "AWS/S3"
            )),
            "AWS/S3 namespace must appear"
        );
    }

    #[test]
    fn metric_leaves_dedupe_by_metric_name() {
        use dbflux_app::MetricCatalogCache;
        use dbflux_core::{MetricDescriptor, MetricNamespace};

        let profile_id = Uuid::new_v4();
        let cache = MetricCatalogCache::new();
        let ns: MetricNamespace = "AWS/EC2".to_string();

        // Three CPUUtilization entries (one per instance) plus two NetworkIn entries
        // (one per instance). CloudWatch emits one descriptor per
        // (metric_name, dimension_combo); the sidebar must collapse them.
        let descriptors = vec![
            MetricDescriptor {
                metric_name: "CPUUtilization".to_string(),
                dimensions: vec![("InstanceId".to_string(), "i-1".to_string())],
            },
            MetricDescriptor {
                metric_name: "CPUUtilization".to_string(),
                dimensions: vec![("InstanceId".to_string(), "i-2".to_string())],
            },
            MetricDescriptor {
                metric_name: "CPUUtilization".to_string(),
                dimensions: vec![("InstanceId".to_string(), "i-3".to_string())],
            },
            MetricDescriptor {
                metric_name: "NetworkIn".to_string(),
                dimensions: vec![("InstanceId".to_string(), "i-1".to_string())],
            },
            MetricDescriptor {
                metric_name: "NetworkIn".to_string(),
                dimensions: vec![("InstanceId".to_string(), "i-2".to_string())],
            },
        ];
        cache.store_metrics_page(profile_id, ns.clone(), descriptors, None);

        let children =
            Sidebar::build_metric_leaf_children(profile_id, "default", &ns, Some(&*cache));

        assert_eq!(
            children.len(),
            2,
            "5 descriptors with 2 distinct metric_names must produce 2 leaves; got {}",
            children.len()
        );

        let labels: Vec<&str> = children.iter().map(|c| c.label.as_ref()).collect();
        assert!(
            labels.contains(&"CPUUtilization"),
            "CPUUtilization leaf must exist: {:?}",
            labels
        );
        assert!(
            labels.contains(&"NetworkIn"),
            "NetworkIn leaf must exist: {:?}",
            labels
        );
    }

    #[test]
    fn loading_placeholder_when_namespace_cache_miss() {
        use dbflux_app::MetricCatalogCache;
        use dbflux_core::SchemaNodeId;

        let profile_id = Uuid::new_v4();
        let cache = MetricCatalogCache::new();
        // No data stored — cache miss

        let children =
            Sidebar::build_metric_namespace_children(profile_id, "default", Some(&*cache));

        assert_eq!(
            children.len(),
            1,
            "Single loading placeholder on cache miss"
        );
        // Loading placeholder must not be a valid MetricNamespaceFolder
        let parsed = children[0].id.as_ref().parse::<SchemaNodeId>();
        assert!(
            parsed.is_err()
                || !matches!(parsed.unwrap(), SchemaNodeId::MetricNamespaceFolder { .. }),
            "Loading placeholder must not parse as MetricNamespaceFolder"
        );
        assert!(
            children[0].label.as_ref().contains("Loading"),
            "Placeholder label must contain 'Loading'"
        );
    }

    #[test]
    fn metrics_folder_appears_when_capability_present() {
        use dbflux_core::{DbSchemaInfo, DriverCapabilities, SchemaNodeId};

        let profile_id = Uuid::new_v4();
        let db_schema = DbSchemaInfo {
            name: "default".to_string(),
            tables: vec![],
            views: vec![],
            custom_types: None,
        };
        let capabilities = DriverCapabilities::METRIC_CATALOG;

        let content = Sidebar::build_document_db_content(
            profile_id,
            "default",
            &db_schema,
            &Default::default(),
            &Default::default(),
            capabilities,
            dbflux_core::DatabaseCategory::Document,
            None,
            &Default::default(),
        );

        let metrics_folder = content.iter().find(|item| {
            item.id
                .as_ref()
                .parse::<SchemaNodeId>()
                .ok()
                .is_some_and(|id| matches!(id, SchemaNodeId::MetricsFolder { .. }))
        });
        assert!(
            metrics_folder.is_some(),
            "Metrics folder must appear when METRIC_CATALOG capability is set"
        );
        let folder = metrics_folder.unwrap();
        assert_eq!(folder.label.as_ref(), "Metrics");
        assert!(!folder.is_expanded());
    }

    #[test]
    fn metrics_folder_absent_without_capability() {
        use dbflux_core::{DbSchemaInfo, DriverCapabilities, SchemaNodeId};

        let profile_id = Uuid::new_v4();
        let db_schema = DbSchemaInfo {
            name: "default".to_string(),
            tables: vec![],
            views: vec![],
            custom_types: None,
        };
        let capabilities = DriverCapabilities::empty();

        let content = Sidebar::build_document_db_content(
            profile_id,
            "default",
            &db_schema,
            &Default::default(),
            &Default::default(),
            capabilities,
            dbflux_core::DatabaseCategory::Document,
            None,
            &Default::default(),
        );

        let has_metrics_folder = content.iter().any(|item| {
            item.id
                .as_ref()
                .parse::<SchemaNodeId>()
                .ok()
                .is_some_and(|id| matches!(id, SchemaNodeId::MetricsFolder { .. }))
        });
        assert!(
            !has_metrics_folder,
            "Metrics folder must not appear when METRIC_CATALOG capability is absent"
        );
    }

    #[test]
    fn collection_item_builds_default_field_and_index_sections() {
        let item = Sidebar::build_collection_item(
            Uuid::new_v4(),
            "logs",
            &TableInfo {
                name: "/aws/lambda/app".to_string(),
                schema: None,
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: CollectionPresentation::DataGrid,
                child_items: None,
            },
            &Default::default(),
            &Default::default(),
        );

        assert_eq!(item.label.as_ref(), "/aws/lambda/app");
        assert_eq!(item.children.len(), 2);
        assert!(item.children[0].label.as_ref().starts_with("Fields"));
        assert!(item.children[1].label.as_ref().starts_with("Indexes"));
    }

    #[test]
    fn event_stream_collections_are_leaves_regardless_of_driver_child_items() {
        // Event-stream collections are now leaves in the tree; their streams
        // are reached exclusively through the picker modal so the row never
        // shows an expand chevron.
        let item = Sidebar::build_collection_item(
            Uuid::new_v4(),
            "logs",
            &TableInfo {
                name: "/aws/lambda/app".to_string(),
                schema: None,
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: Some(vec![FieldInfo {
                    name: "2026/04/25/[$LATEST]abc".to_string(),
                    common_type: "text".to_string(),
                    occurrence_rate: None,
                    nested_fields: None,
                }]),
                presentation: CollectionPresentation::EventStream,
                child_items: Some(vec![CollectionChildInfo {
                    id: "stream-1".to_string(),
                    label: "2026/04/25/[$LATEST]abc".to_string(),
                    last_event_ts_ms: Some(1_776_777_600_000),
                    presentation: CollectionPresentation::EventStream,
                }]),
            },
            &Default::default(),
            &Default::default(),
        );

        assert!(item.children.is_empty());
    }

    #[test]
    fn event_stream_collections_stay_leaves_even_with_pending_pagination() {
        // The presence of a `next_page_token` from the driver must not
        // promote an event-stream collection to an expandable folder.
        let profile_id = Uuid::new_v4();
        let collection = "/aws/lambda/app".to_string();
        let mut child_cache = HashMap::new();
        child_cache.insert(
            ("logs".to_string(), collection.clone()),
            CollectionChildrenCache {
                items: vec![CollectionChildInfo {
                    id: "stream-1".to_string(),
                    label: "stream-1".to_string(),
                    last_event_ts_ms: Some(1),
                    presentation: CollectionPresentation::EventStream,
                }],
                next_page_token: Some("next".to_string()),
            },
        );

        let item = Sidebar::build_collection_item(
            profile_id,
            "logs",
            &TableInfo {
                name: collection.clone(),
                schema: None,
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: CollectionPresentation::EventStream,
                child_items: None,
            },
            &Default::default(),
            &child_cache,
        );

        assert!(item.children.is_empty());
    }

    #[test]
    fn time_series_db_content_produces_measurements_folder_with_collection_leaves() {
        use dbflux_core::{
            DatabaseInfo, MeasurementInfo, SchemaNodeId, SchemaNodeKind, SchemaSnapshot,
            TimeSeriesSchema,
        };

        let profile_id = Uuid::new_v4();
        let schema = SchemaSnapshot::time_series(TimeSeriesSchema {
            databases: vec![DatabaseInfo {
                name: "monitoring".to_string(),
                is_current: true,
            }],
            current_database: Some("monitoring".to_string()),
            measurements: vec![
                MeasurementInfo {
                    name: "cpu".to_string(),
                    tags: vec!["host".to_string()],
                    fields: vec![],
                },
                MeasurementInfo {
                    name: "mem".to_string(),
                    tags: vec![],
                    fields: vec![],
                },
            ],
            retention_policies: vec![],
        });

        let result = Sidebar::build_time_series_db_content(profile_id, "monitoring", &schema);

        // Should produce exactly one "Measurements (N)" folder
        assert_eq!(result.len(), 1);
        let folder = &result[0];
        assert_eq!(folder.label.as_ref(), "Measurements (2)");
        assert!(folder.is_expanded());

        // Each measurement becomes a Collection leaf
        assert_eq!(folder.children.len(), 2);
        assert_eq!(folder.children[0].label.as_ref(), "cpu");
        assert_eq!(folder.children[1].label.as_ref(), "mem");

        // Verify children parse back as Collection nodes with the correct fields
        let id0: SchemaNodeId = folder.children[0].id.as_ref().parse().unwrap();
        let id1: SchemaNodeId = folder.children[1].id.as_ref().parse().unwrap();
        assert_eq!(id0.kind(), SchemaNodeKind::Collection);
        assert_eq!(id1.kind(), SchemaNodeKind::Collection);

        if let SchemaNodeId::Collection { database, name, .. } = id0 {
            assert_eq!(database, "monitoring");
            assert_eq!(name, "cpu");
        } else {
            panic!("expected Collection variant");
        }
    }

    #[test]
    fn time_series_db_content_returns_empty_when_no_measurements() {
        use dbflux_core::{SchemaSnapshot, TimeSeriesSchema};

        let profile_id = Uuid::new_v4();
        let schema = SchemaSnapshot::time_series(TimeSeriesSchema {
            databases: vec![],
            current_database: None,
            measurements: vec![],
            retention_policies: vec![],
        });

        let result = Sidebar::build_time_series_db_content(profile_id, "empty_bucket", &schema);
        assert!(result.is_empty());
    }

    #[test]
    fn build_db_schema_content_uses_per_table_schema_when_present() {
        use dbflux_core::{CustomTypeKind, DbSchemaInfo, SchemaNodeId, SchemaNodeKind, ViewInfo};

        let profile_id = Uuid::new_v4();
        let db_schema = DbSchemaInfo {
            name: "dbflux_test".to_string(),
            tables: vec![
                TableInfo {
                    name: "customers".to_string(),
                    schema: Some("sales".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                },
                TableInfo {
                    name: "employees".to_string(),
                    schema: Some("hr".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                },
                TableInfo {
                    name: "fallback".to_string(),
                    schema: None,
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                },
            ],
            views: vec![ViewInfo {
                name: "active_customers".to_string(),
                schema: Some("sales".to_string()),
            }],
            custom_types: Some(vec![
                CustomTypeInfo {
                    name: "address".to_string(),
                    schema: Some("sales".to_string()),
                    kind: CustomTypeKind::Composite,
                    enum_values: None,
                    base_type: None,
                },
                CustomTypeInfo {
                    name: "tier".to_string(),
                    schema: None,
                    kind: CustomTypeKind::Domain,
                    enum_values: None,
                    base_type: Some("varchar(32)".to_string()),
                },
            ]),
        };

        let content = Sidebar::build_db_schema_content(
            profile_id,
            "dbflux_test",
            Some("dbflux_test"),
            &db_schema,
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &Default::default(),
            false,
            &Default::default(),
        );

        let tables_folder = content
            .iter()
            .find(|item| item.label.as_ref().starts_with("Tables"))
            .expect("Tables folder present");
        assert_eq!(tables_folder.children.len(), 3);

        let expected_schemas = ["sales", "hr", "dbflux_test"];
        for (child, want) in tables_folder.children.iter().zip(expected_schemas.iter()) {
            let id: SchemaNodeId = child.id.as_ref().parse().expect("table id parses");
            assert_eq!(id.kind(), SchemaNodeKind::Table);
            match id {
                SchemaNodeId::Table { schema, .. } => assert_eq!(schema, *want),
                _ => unreachable!(),
            }
        }

        let views_folder = content
            .iter()
            .find(|item| item.label.as_ref().starts_with("Views"))
            .expect("Views folder present");
        assert_eq!(views_folder.children.len(), 1);
        let view_id: SchemaNodeId = views_folder.children[0]
            .id
            .as_ref()
            .parse()
            .expect("view id parses");
        match view_id {
            SchemaNodeId::View { schema, name, .. } => {
                assert_eq!(schema, "sales");
                assert_eq!(name, "active_customers");
            }
            _ => panic!("expected View variant"),
        }

        let types_folder = content
            .iter()
            .find(|item| item.label.as_ref().starts_with("Data Types"))
            .expect("Data Types folder present");
        assert_eq!(types_folder.children.len(), 2);

        let expected_type_schemas = ["sales", "dbflux_test"];
        for (child, want) in types_folder
            .children
            .iter()
            .zip(expected_type_schemas.iter())
        {
            let id: SchemaNodeId = child.id.as_ref().parse().expect("type id parses");
            match id {
                SchemaNodeId::CustomType { schema, .. } => assert_eq!(schema, *want),
                _ => panic!("expected CustomType variant"),
            }
        }
    }

    #[test]
    fn collapse_wrapper_when_single_database() {
        let dbs = vec![dbflux_core::DatabaseInfo {
            name: "logs".to_string(),
            is_current: true,
        }];
        assert!(
            super::should_collapse_database_wrapper(&dbs),
            "single database must collapse (CloudWatch/DynamoDB/SQLite case)"
        );
    }

    #[test]
    fn keep_wrapper_when_multiple_databases() {
        let dbs = vec![
            dbflux_core::DatabaseInfo {
                name: "postgres".to_string(),
                is_current: true,
            },
            dbflux_core::DatabaseInfo {
                name: "app_prod".to_string(),
                is_current: false,
            },
        ];
        assert!(
            !super::should_collapse_database_wrapper(&dbs),
            "multiple databases must remain visible to discriminate them"
        );
    }

    #[test]
    fn keep_wrapper_when_zero_databases() {
        let dbs: Vec<dbflux_core::DatabaseInfo> = vec![];
        assert!(
            !super::should_collapse_database_wrapper(&dbs),
            "zero databases must not trigger collapse path (falls through to fallback branch)"
        );
    }

    // -----------------------------------------------------------------------
    // M — Dashboard and Saved Charts folder helpers (Phase M)
    // -----------------------------------------------------------------------

    /// Helper: build an AppStateEntity with in-memory storage and insert a
    /// minimal profile row into `cfg_connection_profiles` so FK constraints
    /// on the viz tables are satisfied. Returns the entity and the profile UUID.
    fn make_state_with_profile() -> (dbflux_ui_base::AppStateEntity, Uuid) {
        use dbflux_storage::bootstrap::StorageRuntime;

        let rt = StorageRuntime::in_memory().unwrap();
        let profile_id = Uuid::new_v4();

        // Insert a row into cfg_connection_profiles so that FK constraints
        // on viz_dashboards.profile_id and viz_saved_charts.profile_id succeed.
        {
            let conn = rt.viz_connection();
            let guard = conn.lock().unwrap();
            guard
                .execute(
                    "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
                    rusqlite::params![profile_id.to_string(), "test"],
                )
                .unwrap();
        }

        let state = dbflux_ui_base::AppStateEntity::new_with_storage_runtime(rt);
        (state, profile_id)
    }

    #[test]
    fn test_build_dashboards_folder_item_id_contains_dashboards_folder() {
        let (state, profile_id) = make_state_with_profile();
        let item = Sidebar::build_dashboards_folder_item(profile_id, &state);
        assert!(
            item.id.as_ref().contains("DBF"),
            "DashboardsFolder ID must contain the 'DBF' prefix"
        );
    }

    #[test]
    fn test_build_saved_charts_folder_item_id_contains_saved_charts_folder() {
        let (state, profile_id) = make_state_with_profile();
        let item = Sidebar::build_saved_charts_folder_item(profile_id, &state);
        assert!(
            item.id.as_ref().contains("SCRF"),
            "SavedChartsFolder ID must contain the 'SCRF' prefix"
        );
    }

    #[test]
    fn test_remote_dashboard_children_show_loading_on_cache_miss() {
        let (state, profile_id) = make_state_with_profile();
        let folder_id =
            dbflux_core::SchemaNodeId::RemoteDashboardsFolder { profile_id }.to_string();
        let errors = HashMap::new();

        let children =
            Sidebar::build_remote_dashboard_children(profile_id, &state, &folder_id, &errors);

        assert_eq!(children.len(), 1);
        assert_eq!(children[0].label.as_ref(), "Loading...");
    }

    #[test]
    fn test_remote_dashboard_children_show_error_when_recorded() {
        let (state, profile_id) = make_state_with_profile();
        let folder_id =
            dbflux_core::SchemaNodeId::RemoteDashboardsFolder { profile_id }.to_string();
        let mut errors = HashMap::new();
        errors.insert(folder_id.clone(), "access denied".to_string());

        let children =
            Sidebar::build_remote_dashboard_children(profile_id, &state, &folder_id, &errors);

        assert_eq!(children.len(), 1);
        assert!(children[0].label.as_ref().contains("access denied"));
    }

    #[test]
    fn test_remote_dashboard_children_list_items_when_cached() {
        let (state, profile_id) = make_state_with_profile();
        state.remote_dashboard_cache().store(
            profile_id,
            vec![
                dbflux_core::DashboardRef {
                    name: "prod".to_string(),
                    last_modified: None,
                },
                dbflux_core::DashboardRef {
                    name: "staging".to_string(),
                    last_modified: None,
                },
            ],
        );

        let folder_id =
            dbflux_core::SchemaNodeId::RemoteDashboardsFolder { profile_id }.to_string();
        let errors = HashMap::new();
        let children =
            Sidebar::build_remote_dashboard_children(profile_id, &state, &folder_id, &errors);

        assert_eq!(children.len(), 2);
        assert_eq!(children[0].label.as_ref(), "prod");
        assert!(children[0].id.as_ref().starts_with("RDBI"));
    }

    #[test]
    fn test_build_dashboards_folder_empty_state_shows_placeholder() {
        let (state, profile_id) = make_state_with_profile();
        let children = Sidebar::build_dashboard_children(profile_id, &state);
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0].id.as_ref(),
            format!("dashboards_empty:{profile_id}")
        );
        assert!(children[0].label.to_string().contains("No dashboards yet"));
    }

    #[test]
    fn test_build_saved_charts_folder_empty_state_shows_placeholder() {
        let (state, profile_id) = make_state_with_profile();
        let children = Sidebar::build_saved_chart_children(profile_id, &state);
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0].id.as_ref(),
            format!("saved_charts_empty:{profile_id}")
        );
        assert!(
            children[0]
                .label
                .to_string()
                .contains("No saved charts yet")
        );
    }

    #[test]
    fn test_build_dashboard_children_one_item_per_row() {
        use dbflux_components::SavedChartRefreshPolicy;
        let (mut state, profile_id) = make_state_with_profile();

        state
            .dashboards
            .create_dashboard(
                "Dashboard A".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();
        state
            .dashboards
            .create_dashboard(
                "Dashboard B".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let children = Sidebar::build_dashboard_children(profile_id, &state);
        assert_eq!(children.len(), 2, "one DashboardItem per row");
    }

    #[test]
    fn test_build_dashboard_children_sorted_by_updated_at_desc() {
        use dbflux_components::SavedChartRefreshPolicy;
        use std::time::Duration;
        let (mut state, profile_id) = make_state_with_profile();

        let id_old = state
            .dashboards
            .create_dashboard(
                "Old".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        // Sleep briefly so timestamps differ (SQLite ms resolution).
        std::thread::sleep(Duration::from_millis(5));

        let id_new = state
            .dashboards
            .create_dashboard(
                "New".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let children = Sidebar::build_dashboard_children(profile_id, &state);
        assert_eq!(children.len(), 2);

        // "New" was created last so updated_at is greater → should appear first.
        let first_id: dbflux_core::SchemaNodeId = children[0].id.as_ref().parse().unwrap();
        assert!(
            matches!(first_id, dbflux_core::SchemaNodeId::DashboardItem { dashboard_id, .. } if dashboard_id == id_new),
            "most recently updated dashboard must be first"
        );
        let second_id: dbflux_core::SchemaNodeId = children[1].id.as_ref().parse().unwrap();
        assert!(
            matches!(second_id, dbflux_core::SchemaNodeId::DashboardItem { dashboard_id, .. } if dashboard_id == id_old)
        );
    }

    // -----------------------------------------------------------------------
    // Capability-aware empty hint tests (Gap 4)
    // -----------------------------------------------------------------------

    /// Minimal `Connection` implementation that lets tests control capability flags.
    struct CapabilityConnection {
        metadata: dbflux_core::DriverMetadata,
    }

    impl CapabilityConnection {
        fn with_capabilities(capabilities: dbflux_core::DriverCapabilities) -> Self {
            Self {
                metadata: dbflux_core::DriverMetadata {
                    id: "test".to_string(),
                    display_name: "Test".to_string(),
                    description: "test connection".to_string(),
                    category: dbflux_core::DatabaseCategory::Relational,
                    deployment_class: None,
                    query_language: dbflux_core::QueryLanguage::Sql,
                    capabilities,
                    default_port: None,
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
                },
            }
        }
    }

    impl dbflux_core::Connection for CapabilityConnection {
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
            Err(dbflux_core::DbError::NotSupported(
                "test connection".to_string(),
            ))
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
            Ok(dbflux_core::SchemaSnapshot::default())
        }

        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
            dbflux_core::SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
    }

    /// Build a minimal `ConnectedProfile` backed by `CapabilityConnection`.
    fn make_connected_profile(
        _profile_id: Uuid,
        capabilities: dbflux_core::DriverCapabilities,
    ) -> dbflux_core::ConnectedProfile {
        use std::path::PathBuf;
        use std::sync::Arc;
        dbflux_core::ConnectedProfile {
            profile: dbflux_core::ConnectionProfile::new(
                "test",
                dbflux_core::DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            ),
            connection: Arc::new(CapabilityConnection::with_capabilities(capabilities)),
            schema: None,
            mutation_policy: dbflux_core::MutationPolicy::default(),
            database_schemas: HashMap::new(),
            table_details: HashMap::new(),
            collection_children: HashMap::new(),
            schema_types: HashMap::new(),
            schema_indexes: HashMap::new(),
            schema_foreign_keys: HashMap::new(),
            schema_routines: HashMap::new(),
            dependents_cache: HashMap::new(),
            active_database: None,
            redis_key_cache: dbflux_core::RedisKeyCache::default(),
            database_connections: HashMap::new(),
            proxy_tunnel: None,
        }
    }

    #[test]
    fn dashboard_empty_hint_includes_import_when_driver_has_dashboard_import_capability() {
        let (mut state, profile_id) = make_state_with_profile();

        let connected = make_connected_profile(
            profile_id,
            dbflux_core::DriverCapabilities::DASHBOARD_IMPORT,
        );
        state.connections_mut().insert(profile_id, connected);

        let children = Sidebar::build_dashboard_children(profile_id, &state);
        assert_eq!(
            children.len(),
            1,
            "empty state must produce one placeholder"
        );
        let label = children[0].label.to_string().to_ascii_lowercase();
        assert!(
            label.contains("import"),
            "hint must mention 'import' when driver has DASHBOARD_IMPORT: {label:?}"
        );
    }

    #[test]
    fn dashboard_empty_hint_excludes_import_when_driver_lacks_dashboard_import_capability() {
        let (state, profile_id) = make_state_with_profile();
        // No connected profile inserted — connection map stays empty.

        let children = Sidebar::build_dashboard_children(profile_id, &state);
        assert_eq!(
            children.len(),
            1,
            "empty state must produce one placeholder"
        );
        let label = children[0].label.to_string().to_ascii_lowercase();
        assert!(
            !label.contains("import"),
            "hint must not mention 'import' without DASHBOARD_IMPORT: {label:?}"
        );
    }

    // ---- T24: capability-gate tests for InstanceMetricsFolder / InstanceInspectorsFolder ----
    //
    // The tests below reference `build_instance_metrics_folder_item` and
    // `build_instance_inspectors_folder_item` — functions that do not exist yet.
    // This causes a compile failure (RED) until T25 adds them.

    /// REQ-UI-1, REQ-UI-5: A driver with `INSTANCE_METRICS` must produce an
    /// `InstanceMetricsFolder` node when `build_instance_metrics_folder_item` is called.
    ///
    /// Fails to compile (RED) until T25 adds `build_instance_metrics_folder_item`.
    #[test]
    fn instance_metrics_folder_item_produces_correct_node_id() {
        use dbflux_core::SchemaNodeId;

        let profile_id = Uuid::new_v4();
        let item = Sidebar::build_instance_metrics_folder_item(profile_id, Vec::new());

        let node_id: SchemaNodeId = item
            .id
            .as_ref()
            .parse()
            .expect("folder item must have a valid SchemaNodeId");

        assert!(
            matches!(
                node_id,
                SchemaNodeId::InstanceMetricsFolder {
                    profile_id: pid,
                } if pid == profile_id
            ),
            "folder item must carry InstanceMetricsFolder node ID: {node_id:?}"
        );
    }

    /// REQ-UI-1, REQ-UI-5: A driver with `INSTANCE_INSPECTOR` must produce an
    /// `InstanceInspectorsFolder` node when `build_instance_inspectors_folder_item` is called.
    ///
    /// Fails to compile (RED) until T25 adds `build_instance_inspectors_folder_item`.
    #[test]
    fn instance_inspectors_folder_item_produces_correct_node_id() {
        use dbflux_core::SchemaNodeId;

        let profile_id = Uuid::new_v4();
        let item = Sidebar::build_instance_inspectors_folder_item(profile_id, Vec::new());

        let node_id: SchemaNodeId = item
            .id
            .as_ref()
            .parse()
            .expect("folder item must have a valid SchemaNodeId");

        assert!(
            matches!(
                node_id,
                SchemaNodeId::InstanceInspectorsFolder {
                    profile_id: pid,
                } if pid == profile_id
            ),
            "folder item must carry InstanceInspectorsFolder node ID: {node_id:?}"
        );
    }

    /// REQ-UI-5: A driver with `INSTANCE_METRICS` capability must include the
    /// `InstanceMetricsFolder` in the profile's child list. A driver without
    /// this capability must not include it.
    ///
    /// Tests the capability-gate predicate in isolation using a mock that mirrors
    /// `build_profile_item_with_errors` capability check pattern.
    #[test]
    fn capability_gate_controls_instance_metrics_folder_inclusion() {
        use dbflux_core::DriverCapabilities;

        let caps_with = DriverCapabilities::INSTANCE_METRICS;
        let caps_without = DriverCapabilities::empty();

        let should_show_with = caps_with.contains(DriverCapabilities::INSTANCE_METRICS);
        let should_show_without = caps_without.contains(DriverCapabilities::INSTANCE_METRICS);

        assert!(
            should_show_with,
            "INSTANCE_METRICS capability must enable the folder"
        );
        assert!(
            !should_show_without,
            "missing INSTANCE_METRICS capability must suppress the folder"
        );
    }

    /// REQ-UI-5: A driver with `INSTANCE_INSPECTOR` capability must include the
    /// `InstanceInspectorsFolder` in the profile's child list. A driver without
    /// this capability must not include it.
    #[test]
    fn capability_gate_controls_instance_inspectors_folder_inclusion() {
        use dbflux_core::DriverCapabilities;

        let caps_with = DriverCapabilities::INSTANCE_INSPECTOR;
        let caps_without = DriverCapabilities::empty();

        let should_show_with = caps_with.contains(DriverCapabilities::INSTANCE_INSPECTOR);
        let should_show_without = caps_without.contains(DriverCapabilities::INSTANCE_INSPECTOR);

        assert!(
            should_show_with,
            "INSTANCE_INSPECTOR capability must enable the folder"
        );
        assert!(
            !should_show_without,
            "missing INSTANCE_INSPECTOR capability must suppress the folder"
        );
    }

    // ---- RF1: expansion wiring — instance catalog leaf population ----

    /// REQ-UI-1 / RF1: when `instance_metrics_cache` is populated for a profile,
    /// `build_instance_metric_leaf_children` returns one `InstanceMetricLeaf` per
    /// cached definition and each leaf carries the correct `SchemaNodeId`.
    #[test]
    fn build_instance_metric_leaf_children_returns_leaves_from_cache() {
        use dbflux_core::{InstanceMetricDef, InstanceMetricUnit, SchemaNodeId};

        let profile_id = Uuid::new_v4();
        let defs = vec![
            InstanceMetricDef {
                id: "pg.cache_hit_ratio".to_string(),
                display_name: "Cache hit ratio".to_string(),
                group: "Cache".to_string(),
                unit: InstanceMetricUnit::Percent,
                description: None,
                default_refresh_secs: 15,
            },
            InstanceMetricDef {
                id: "pg.tx_commit_rate".to_string(),
                display_name: "Commits / sec".to_string(),
                group: "Throughput".to_string(),
                unit: InstanceMetricUnit::PerSecond,
                description: None,
                default_refresh_secs: 10,
            },
        ];

        let mut cache = HashMap::new();
        cache.insert(profile_id, defs.clone());

        let leaves = Sidebar::build_instance_metric_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            defs.len(),
            "must produce one leaf per cached metric definition"
        );

        for (leaf, def) in leaves.iter().zip(defs.iter()) {
            let node_id: SchemaNodeId = leaf
                .id
                .as_ref()
                .parse()
                .expect("leaf must have a valid SchemaNodeId");

            assert!(
                matches!(
                    &node_id,
                    SchemaNodeId::InstanceMetricLeaf { profile_id: pid, metric_id }
                        if *pid == profile_id && metric_id == &def.id
                ),
                "leaf must carry InstanceMetricLeaf node ID with correct profile and metric_id; got {node_id:?}"
            );

            assert_eq!(
                leaf.label, def.display_name,
                "leaf label must equal the metric display_name"
            );
        }
    }

    /// REQ-UI-1: when `instance_metrics_cache` has populated entries for a profile,
    /// `build_instance_metric_leaf_children` returns one leaf per definition (no placeholders).
    #[test]
    fn build_instance_metric_leaf_children_returns_no_placeholder_when_cache_populated() {
        use dbflux_core::{InstanceMetricDef, InstanceMetricUnit};

        let profile_id = Uuid::new_v4();
        let defs = vec![InstanceMetricDef {
            id: "pg.tps".to_string(),
            display_name: "TPS".to_string(),
            group: "Throughput".to_string(),
            unit: InstanceMetricUnit::PerSecond,
            description: None,
            default_refresh_secs: 15,
        }];
        let mut cache: HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>> = HashMap::new();
        cache.insert(profile_id, defs);

        let leaves = Sidebar::build_instance_metric_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            1,
            "must return exactly one leaf for one metric"
        );
        assert!(
            !leaves[0].id.to_string().contains("loading"),
            "populated cache must not produce a loading placeholder"
        );
    }

    /// REQ-UI-1 / RF1: when `instance_inspectors_cache` is populated for a profile,
    /// `build_instance_inspector_leaf_children` returns one `InstanceInspectorLeaf`
    /// per cached definition with the correct `SchemaNodeId`.
    #[test]
    fn build_instance_inspector_leaf_children_returns_leaves_from_cache() {
        use dbflux_core::{InstanceInspectorDef, SchemaNodeId};

        let profile_id = Uuid::new_v4();
        let defs = vec![InstanceInspectorDef {
            id: "pg.activity".to_string(),
            display_name: "Active queries".to_string(),
            description: None,
            default_refresh_secs: 10,
        }];

        let mut cache = HashMap::new();
        cache.insert(profile_id, defs.clone());

        let leaves = Sidebar::build_instance_inspector_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            1,
            "must produce one leaf for the single inspector"
        );

        let node_id: SchemaNodeId = leaves[0]
            .id
            .as_ref()
            .parse()
            .expect("leaf must have a valid SchemaNodeId");

        assert!(
            matches!(
                &node_id,
                SchemaNodeId::InstanceInspectorLeaf { profile_id: pid, metric_id }
                    if *pid == profile_id && metric_id == "pg.activity"
            ),
            "leaf must carry InstanceInspectorLeaf with correct ids; got {node_id:?}"
        );
    }

    // ---- BF1: loading placeholder tests ----

    /// BF1: when `instance_metrics_cache` has no entry for a profile,
    /// `build_instance_metric_leaf_children` must return a single loading
    /// placeholder so the parent folder is non-empty and shows a chevron.
    #[test]
    fn build_instance_metric_leaf_children_returns_loading_placeholder_on_cache_miss() {
        let profile_id = Uuid::new_v4();
        let cache: HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>> = HashMap::new();

        let leaves = Sidebar::build_instance_metric_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            1,
            "must return one loading placeholder when cache has no entry for this profile"
        );
        assert!(
            leaves[0].id.to_string().contains("loading"),
            "loading placeholder id must contain 'loading'; got {:?}",
            leaves[0].id
        );
    }

    /// BF1: when `instance_inspectors_cache` has no entry for a profile,
    /// `build_instance_inspector_leaf_children` must return a single loading
    /// placeholder so the parent folder is non-empty and shows a chevron.
    #[test]
    fn build_instance_inspector_leaf_children_returns_loading_placeholder_on_cache_miss() {
        let profile_id = Uuid::new_v4();
        let cache: HashMap<Uuid, Vec<dbflux_core::InstanceInspectorDef>> = HashMap::new();

        let leaves = Sidebar::build_instance_inspector_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            1,
            "must return one loading placeholder when cache has no entry for this profile"
        );
        assert!(
            leaves[0].id.to_string().contains("loading"),
            "loading placeholder id must contain 'loading'; got {:?}",
            leaves[0].id
        );
    }

    /// BF1: empty Vec in cache (probe completed, nothing returned) must still
    /// produce the "No metrics available" placeholder, not a loading placeholder.
    #[test]
    fn build_instance_metric_leaf_children_empty_cache_entry_shows_not_available() {
        let profile_id = Uuid::new_v4();
        let mut cache: HashMap<Uuid, Vec<dbflux_core::InstanceMetricDef>> = HashMap::new();
        cache.insert(profile_id, Vec::new());

        let leaves = Sidebar::build_instance_metric_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            1,
            "must return one placeholder for empty entry"
        );
        assert!(
            leaves[0].label.to_string().contains("No metrics"),
            "empty-cache placeholder must say 'No metrics available'; got {:?}",
            leaves[0].label
        );
    }

    /// BF1: empty Vec in cache (probe completed, nothing returned) must still
    /// produce the "No inspectors available" placeholder, not a loading placeholder.
    #[test]
    fn build_instance_inspector_leaf_children_empty_cache_entry_shows_not_available() {
        let profile_id = Uuid::new_v4();
        let mut cache: HashMap<Uuid, Vec<dbflux_core::InstanceInspectorDef>> = HashMap::new();
        cache.insert(profile_id, Vec::new());

        let leaves = Sidebar::build_instance_inspector_leaf_children(profile_id, &cache);

        assert_eq!(
            leaves.len(),
            1,
            "must return one placeholder for empty entry"
        );
        assert!(
            leaves[0].label.to_string().contains("No inspectors"),
            "empty-cache placeholder must say 'No inspectors available'; got {:?}",
            leaves[0].label
        );
    }

    // ---- UX3: DatabasesFolder tests ----

    /// `build_databases_folder_item` produces a node with `DatabasesFolder` ID and
    /// expands by default.
    #[test]
    fn databases_folder_item_produces_correct_node_id_and_is_expanded() {
        use dbflux_core::SchemaNodeId;
        use gpui_component::tree::TreeItem;

        let profile_id = Uuid::new_v4();
        let child = TreeItem::new(
            SchemaNodeId::Database {
                profile_id,
                name: "mydb".to_string(),
            }
            .to_string(),
            "mydb".to_string(),
        );

        let item = Sidebar::build_databases_folder_item_for_test(profile_id, vec![child]);

        let node_id: SchemaNodeId = item
            .id
            .as_ref()
            .parse()
            .expect("DatabasesFolder item must have a valid SchemaNodeId");

        assert!(
            matches!(
                node_id,
                SchemaNodeId::DatabasesFolder {
                    profile_id: pid,
                } if pid == profile_id
            ),
            "item must carry DatabasesFolder node ID: {node_id:?}"
        );
        assert!(
            item.is_expanded(),
            "DatabasesFolder must be expanded by default"
        );
        assert_eq!(
            item.children.len(),
            1,
            "DatabasesFolder must pass through its children"
        );
    }

    // ---- BF7: InstanceOverviewLeaf ----

    /// BF7: `build_instance_overview_leaf` must return a `TreeItem` whose node ID
    /// parses to `SchemaNodeId::InstanceOverviewLeaf { profile_id }`.
    #[test]
    fn build_instance_overview_leaf_carries_correct_node_id() {
        use dbflux_core::SchemaNodeId;

        let profile_id = Uuid::new_v4();
        let item = Sidebar::build_instance_overview_leaf(profile_id);

        let node_id: SchemaNodeId = item
            .id
            .as_ref()
            .parse()
            .expect("leaf must have a valid SchemaNodeId");

        assert!(
            matches!(
                &node_id,
                SchemaNodeId::InstanceOverviewLeaf { profile_id: pid } if *pid == profile_id
            ),
            "leaf must carry InstanceOverviewLeaf node ID: {node_id:?}"
        );
    }
}
