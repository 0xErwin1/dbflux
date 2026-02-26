use super::*;

impl Sidebar {
    pub(super) fn build_tree_items_with_overrides(&self, cx: &Context<Self>) -> Vec<TreeItem> {
        let items = Self::build_tree_items(self.app_state.read(cx));
        self.apply_expansion_overrides(items)
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

    fn apply_expansion_overrides(&self, items: Vec<TreeItem>) -> Vec<TreeItem> {
        items
            .into_iter()
            .map(|item| self.apply_override_recursive(item))
            .collect()
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

    pub(super) fn build_tree_items(state: &AppState) -> Vec<TreeItem> {
        let root_nodes = state.connection_tree().root_nodes();
        Self::build_tree_nodes_recursive(&root_nodes, state)
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

    fn build_tree_nodes_recursive(
        nodes: &[&ConnectionTreeNode],
        state: &AppState,
    ) -> Vec<TreeItem> {
        let mut items = Vec::new();

        for node in nodes {
            match node.kind {
                ConnectionTreeNodeKind::Folder => {
                    let children_nodes = state.connection_tree().children_of(node.id);
                    let children_refs: Vec<&ConnectionTreeNode> =
                        children_nodes.into_iter().collect();
                    let children = Self::build_tree_nodes_recursive(&children_refs, state);

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
                        let profile_item = Self::build_profile_item(profile, state);
                        items.push(profile_item);
                    }
                }
            }
        }

        items
    }

    fn build_profile_item(profile: &dbflux_core::ConnectionProfile, state: &AppState) -> TreeItem {
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

                    profile_children.push(
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
            } else if !schema.databases().is_empty() {
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
                                )
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
                            )
                        } else {
                            Vec::new()
                        }
                    } else if db.is_current {
                        Self::build_schema_children(
                            profile_id,
                            &db.name,
                            Some(&db.name),
                            schema,
                            &connected.table_details,
                            &connected.schema_types,
                            &connected.schema_indexes,
                            &connected.schema_foreign_keys,
                        )
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

                    profile_children.push(
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
                );
            }

            profile_item = profile_item.expanded(is_active).children(profile_children);
        }

        profile_item
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

    fn build_document_db_content(
        profile_id: Uuid,
        database_name: &str,
        db_schema: &dbflux_core::DbSchemaInfo,
        table_details: &HashMap<(String, String), TableInfo>,
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();

        if !db_schema.tables.is_empty() {
            let collection_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|coll| Self::build_collection_item(profile_id, database_name, coll, table_details))
                .collect();

            content.push(
                TreeItem::new(
                    SchemaNodeId::CollectionsFolder {
                        profile_id,
                        database: database_name.to_string(),
                    }
                    .to_string(),
                    format!("Collections ({})", db_schema.tables.len()),
                )
                .expanded(true)
                .children(collection_children),
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
                                    let unique_marker =
                                        if idx.is_unique { " UNIQUE" } else { "" };
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
                            let label = format!(
                                "{}.{}",
                                coll.name,
                                format_collection_index_label(idx)
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

    fn build_collection_item(
        profile_id: Uuid,
        database_name: &str,
        collection: &dbflux_core::TableInfo,
        table_details: &HashMap<(String, String), TableInfo>,
    ) -> TreeItem {
        let coll_name = &collection.name;
        let cache_key = (database_name.to_string(), coll_name.clone());
        let effective = table_details.get(&cache_key).unwrap_or(collection);
        let details_loaded = effective.sample_fields.is_some();

        let (field_children, field_count) = if let Some(fields) = effective.sample_fields.as_ref()
        {
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

        let collection_children = vec![
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
        ];

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
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();
        let schema_name = &db_schema.name;

        if !db_schema.tables.is_empty() {
            let table_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|table| {
                    Self::build_table_item(
                        profile_id,
                        target_database,
                        schema_name,
                        table,
                        table_details,
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
                    TreeItem::new(
                        SchemaNodeId::View {
                            profile_id,
                            database: target_database.map(str::to_string),
                            schema: schema_name.to_string(),
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
                    .map(|t| Self::build_custom_type_item(profile_id, schema_name, t))
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
                        let label = format!("{} ({}){}{}", idx.name, cols, unique_marker, pk_marker);

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

        let table_sections = vec![
            TreeItem::new(
                SchemaNodeId::ColumnsFolder {
                    profile_id,
                    schema: schema_name.to_string(),
                    table: table.name.clone(),
                }
                .to_string(),
                format!("Columns ({})", column_count),
            )
            .expanded(false)
            .children(column_children),
            TreeItem::new(
                SchemaNodeId::IndexesFolder {
                    profile_id,
                    schema: schema_name.to_string(),
                    table: table.name.clone(),
                }
                .to_string(),
                format!("Indexes ({})", index_count),
            )
            .expanded(false)
            .children(index_children),
            TreeItem::new(
                SchemaNodeId::ForeignKeysFolder {
                    profile_id,
                    schema: schema_name.to_string(),
                    table: table.name.clone(),
                }
                .to_string(),
                format!("Foreign Keys ({})", fk_count),
            )
            .expanded(false)
            .children(fk_children),
            TreeItem::new(
                SchemaNodeId::ConstraintsFolder {
                    profile_id,
                    schema: schema_name.to_string(),
                    table: table.name.clone(),
                }
                .to_string(),
                format!("Constraints ({})", constraint_count),
            )
            .expanded(false)
            .children(constraint_children),
        ];

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
                let children =
                    build_collection_field_items(profile_id, collection_name, nested);
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
