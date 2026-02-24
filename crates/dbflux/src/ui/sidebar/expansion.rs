use super::*;

impl Sidebar {
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
        let parsed = parse_node_id(item_id);

        // Expand immediately for Ready/Loading; bail on NotFound (prepare failed).
        if expanded && matches!(parsed, Some(SchemaNodeId::Table { .. })) {
            let pending = PendingAction::ViewSchema {
                item_id: item_id.to_string(),
            };
            if matches!(
                self.ensure_table_details(item_id, pending, cx),
                TableDetailsStatus::NotFound
            ) {
                return;
            }
        }

        // Data Types folder: fetch types if needed.
        if expanded
            && let Some(SchemaNodeId::TypesFolder {
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
                if !self.spawn_fetch_schema_types(
                    *profile_id,
                    database,
                    Some(schema),
                    pending,
                    cx,
                ) {
                    return;
                }
            }
        }

        // Schema-level Indexes folder: fetch if needed.
        if expanded
            && let Some(SchemaNodeId::SchemaIndexesFolder {
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
                    return;
                }
            }
        }

        // Schema-level Foreign Keys folder: fetch if needed.
        if expanded
            && let Some(SchemaNodeId::SchemaForeignKeysFolder {
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
                    return;
                }
            }
        }

        // When expanding a database, trigger schema fetch via handle_database_click
        // which properly dispatches based on the driver's schema_loading_strategy
        if expanded && matches!(parsed, Some(SchemaNodeId::Database { .. })) {
            self.handle_database_click(item_id, cx);
        }

        // Sync folder collapsed state with AppState
        if let Some(SchemaNodeId::ConnectionFolder { node_id }) = &parsed {
            self.app_state.update(cx, |state, _cx| {
                state.set_folder_collapsed(*node_id, !expanded);
            });
        }

        self.expansion_overrides
            .insert(item_id.to_string(), expanded);
        self.rebuild_tree_with_overrides(cx);
    }

    pub(super) fn rebuild_tree_with_overrides(&mut self, cx: &mut Context<Self>) {
        let selected_index = self.tree_state.read(cx).selected_index();
        self.active_databases = Self::extract_active_databases(self.app_state.read(cx));

        let items = self.build_tree_items_with_overrides(cx);
        self.visible_entry_count = Self::count_visible_entries(&items);

        self.tree_state.update(cx, |state, cx| {
            state.set_items(items, cx);
            if let Some(idx) = selected_index {
                let new_idx = idx.min(self.visible_entry_count.saturating_sub(1));
                state.set_selected_index(Some(new_idx), cx);
            }
        });
        cx.notify();
    }

    pub(super) fn refresh_tree(&mut self, cx: &mut Context<Self>) {
        let selected_index = self.tree_state.read(cx).selected_index();
        self.active_databases = Self::extract_active_databases(self.app_state.read(cx));

        // Clean up stale expansion overrides for schema-specific items
        // These should reset when switching databases/connections
        self.cleanup_stale_overrides(cx);

        let items = self.build_tree_items_with_overrides(cx);
        self.visible_entry_count = Self::count_visible_entries(&items);

        if let Some(ref menu) = self.context_menu
            && Self::find_item_index_in_tree(&items, &menu.item_id, &mut 0).is_none()
        {
            self.context_menu = None;
        }

        self.tree_state.update(cx, |state, cx| {
            state.set_items(items, cx);

            if let Some(idx) = selected_index {
                let new_idx = idx.min(self.visible_entry_count.saturating_sub(1));
                state.set_selected_index(Some(new_idx), cx);
            }
        });
        cx.notify();
    }

    fn cleanup_stale_overrides(&mut self, cx: &Context<Self>) {
        let state = self.app_state.read(cx);

        self.expansion_overrides
            .retain(|item_id, _expanded| match parse_node_id(item_id) {
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
            });
    }
}
