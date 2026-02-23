use super::*;

impl Sidebar {
    pub(super) fn view_table_schema(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.set_expanded(item_id, true, cx);
    }

    pub fn open_item_menu(&mut self, position: Point<Pixels>, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();

        let Some(entry) = entry else {
            return;
        };

        let item_id = entry.item().id.to_string();
        self.open_menu_for_item(&item_id, position, cx);
    }

    pub fn open_menu_for_item(
        &mut self,
        item_id: &str,
        position: Point<Pixels>,
        cx: &mut Context<Self>,
    ) {
        let node_kind = parse_node_kind(item_id);
        let items = self.build_context_menu_items(node_kind, item_id, cx);

        if items.is_empty() {
            return;
        }

        self.context_menu = Some(ContextMenuState {
            item_id: item_id.to_string(),
            selected_index: 0,
            items,
            parent_stack: Vec::new(),
            position,
        });
        cx.notify();
    }

    pub(super) fn build_context_menu_items(
        &self,
        node_kind: SchemaNodeKind,
        item_id: &str,
        cx: &App,
    ) -> Vec<ContextMenuItem> {
        match node_kind {
            SchemaNodeKind::Table | SchemaNodeKind::View => {
                let mut items = vec![
                    ContextMenuItem {
                        label: "Open".into(),
                        action: ContextMenuAction::Open,
                    },
                    ContextMenuItem {
                        label: "View Schema".into(),
                        action: ContextMenuAction::ViewSchema,
                    },
                ];

                // Get code generators from driver (if connected)
                let generators = self.get_code_generators_for_item(item_id, node_kind, cx);
                if !generators.is_empty() {
                    items.push(ContextMenuItem {
                        label: "Generate SQL".into(),
                        action: ContextMenuAction::Submenu(generators),
                    });
                }

                items
            }
            SchemaNodeKind::Collection => {
                vec![
                    ContextMenuItem {
                        label: "Open".into(),
                        action: ContextMenuAction::Open,
                    },
                    ContextMenuItem {
                        label: "Generate Query".into(),
                        action: ContextMenuAction::Submenu(vec![
                            ContextMenuItem {
                                label: "find".into(),
                                action: ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::Find,
                                ),
                            },
                            ContextMenuItem {
                                label: "insertOne".into(),
                                action: ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::InsertOne,
                                ),
                            },
                            ContextMenuItem {
                                label: "updateOne".into(),
                                action: ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::UpdateOne,
                                ),
                            },
                            ContextMenuItem {
                                label: "deleteOne".into(),
                                action: ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::DeleteOne,
                                ),
                            },
                        ]),
                    },
                ]
            }
            SchemaNodeKind::Profile => {
                let is_connected =
                    if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) {
                        self.app_state
                            .read(cx)
                            .connections()
                            .contains_key(&profile_id)
                    } else {
                        false
                    };

                let mut items = vec![];
                if is_connected {
                    items.push(ContextMenuItem {
                        label: "Disconnect".into(),
                        action: ContextMenuAction::Disconnect,
                    });
                    items.push(ContextMenuItem {
                        label: "Refresh".into(),
                        action: ContextMenuAction::Refresh,
                    });
                } else {
                    items.push(ContextMenuItem {
                        label: "Connect".into(),
                        action: ContextMenuAction::Connect,
                    });
                }
                items.push(ContextMenuItem {
                    label: "Edit".into(),
                    action: ContextMenuAction::Edit,
                });
                items.push(ContextMenuItem {
                    label: "Rename".into(),
                    action: ContextMenuAction::RenameFolder, // Reuse for profile rename
                });
                items.push(ContextMenuItem {
                    label: "Delete".into(),
                    action: ContextMenuAction::Delete,
                });

                // Add "Move to..." submenu with available folders
                let move_to_items = self.build_move_to_submenu(item_id, cx);
                if !move_to_items.is_empty() {
                    items.push(ContextMenuItem {
                        label: "Move to...".into(),
                        action: ContextMenuAction::Submenu(move_to_items),
                    });
                }

                items
            }
            SchemaNodeKind::Database => {
                let is_loaded = self.is_database_schema_loaded(item_id, cx);
                if is_loaded {
                    // Only show Close for databases that support it (MySQL/MariaDB)
                    if self.database_supports_close(item_id, cx) {
                        vec![ContextMenuItem {
                            label: "Close".into(),
                            action: ContextMenuAction::CloseDatabase,
                        }]
                    } else {
                        vec![]
                    }
                } else {
                    vec![ContextMenuItem {
                        label: "Open".into(),
                        action: ContextMenuAction::OpenDatabase,
                    }]
                }
            }
            SchemaNodeKind::ConnectionFolder => {
                let mut items = vec![
                    ContextMenuItem {
                        label: "New Connection".into(),
                        action: ContextMenuAction::NewConnection,
                    },
                    ContextMenuItem {
                        label: "New Folder".into(),
                        action: ContextMenuAction::NewFolder,
                    },
                    ContextMenuItem {
                        label: "Rename".into(),
                        action: ContextMenuAction::RenameFolder,
                    },
                    ContextMenuItem {
                        label: "Delete".into(),
                        action: ContextMenuAction::DeleteFolder,
                    },
                ];

                let move_to_items = self.build_move_to_submenu(item_id, cx);
                if !move_to_items.is_empty() {
                    items.push(ContextMenuItem {
                        label: "Move to...".into(),
                        action: ContextMenuAction::Submenu(move_to_items),
                    });
                }

                items
            }

            SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::CREATE_INDEX) {
                    submenu.push(ContextMenuItem {
                        label: "CREATE INDEX".into(),
                        action: ContextMenuAction::GenerateIndexSql(IndexSqlAction::Create),
                    });
                }

                if caps.contains(CodeGenCapabilities::DROP_INDEX) {
                    submenu.push(ContextMenuItem {
                        label: "DROP INDEX".into(),
                        action: ContextMenuAction::GenerateIndexSql(IndexSqlAction::Drop),
                    });
                }

                if caps.contains(CodeGenCapabilities::REINDEX) {
                    submenu.push(ContextMenuItem {
                        label: "REINDEX".into(),
                        action: ContextMenuAction::GenerateIndexSql(IndexSqlAction::Reindex),
                    });
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![ContextMenuItem {
                        label: "Generate SQL".into(),
                        action: ContextMenuAction::Submenu(submenu),
                    }]
                }
            }

            SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::ADD_FOREIGN_KEY) {
                    submenu.push(ContextMenuItem {
                        label: "ADD CONSTRAINT".into(),
                        action: ContextMenuAction::GenerateForeignKeySql(
                            ForeignKeySqlAction::AddConstraint,
                        ),
                    });
                }

                if caps.contains(CodeGenCapabilities::DROP_FOREIGN_KEY) {
                    submenu.push(ContextMenuItem {
                        label: "DROP CONSTRAINT".into(),
                        action: ContextMenuAction::GenerateForeignKeySql(
                            ForeignKeySqlAction::DropConstraint,
                        ),
                    });
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![ContextMenuItem {
                        label: "Generate SQL".into(),
                        action: ContextMenuAction::Submenu(submenu),
                    }]
                }
            }

            SchemaNodeKind::CustomType => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::CREATE_TYPE) {
                    submenu.push(ContextMenuItem {
                        label: "CREATE TYPE".into(),
                        action: ContextMenuAction::GenerateTypeSql(TypeSqlAction::Create),
                    });
                }

                if caps.contains(CodeGenCapabilities::ALTER_TYPE) && self.is_enum_type(item_id, cx)
                {
                    submenu.push(ContextMenuItem {
                        label: "ADD VALUE".into(),
                        action: ContextMenuAction::GenerateTypeSql(TypeSqlAction::AddEnumValue),
                    });
                }

                if caps.contains(CodeGenCapabilities::DROP_TYPE) {
                    submenu.push(ContextMenuItem {
                        label: "DROP TYPE".into(),
                        action: ContextMenuAction::GenerateTypeSql(TypeSqlAction::Drop),
                    });
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![ContextMenuItem {
                        label: "Generate SQL".into(),
                        action: ContextMenuAction::Submenu(submenu),
                    }]
                }
            }

            _ => vec![],
        }
    }

    /// Builds the "Move to..." submenu items for a profile or folder.
    fn build_move_to_submenu(&self, item_id: &str, cx: &App) -> Vec<ContextMenuItem> {
        let state = self.app_state.read(cx);
        let mut items = Vec::new();

        // Determine current node info (works for both profiles and folders)
        let (current_parent, current_node_id) = match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { profile_id }) => {
                let node = state.connection_tree().find_by_profile(profile_id);
                (node.and_then(|n| n.parent_id), node.map(|n| n.id))
            }
            Some(SchemaNodeId::ConnectionFolder { node_id }) => {
                let node = state.connection_tree().find_by_id(node_id);
                (node.and_then(|n| n.parent_id), Some(node_id))
            }
            _ => (None, None),
        };

        // Add "Root" option if not already at root
        if current_parent.is_some() {
            items.push(ContextMenuItem {
                label: "Root".into(),
                action: ContextMenuAction::MoveToFolder(None),
            });
        }

        // Add all folders (except self and descendants for folders)
        let descendants = current_node_id
            .map(|id| state.connection_tree().get_descendants(id))
            .unwrap_or_default();

        for folder in state.connection_tree().folders() {
            // Skip if this is the current parent
            if Some(folder.id) == current_parent {
                continue;
            }
            // Skip self (for folders)
            if Some(folder.id) == current_node_id {
                continue;
            }
            // Skip descendants (would create cycle)
            if descendants.contains(&folder.id) {
                continue;
            }

            items.push(ContextMenuItem {
                label: folder.name.clone(),
                action: ContextMenuAction::MoveToFolder(Some(folder.id)),
            });
        }

        items
    }

    pub(super) fn is_database_schema_loaded(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Database { profile_id, name }) = parse_node_id(item_id) else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };

        if conn.database_schemas.contains_key(&name) {
            return true;
        }

        if conn.database_connections.contains_key(&name) {
            return true;
        }

        conn.schema
            .as_ref()
            .and_then(|s| s.current_database())
            .is_some_and(|current| current == name)
    }

    /// Whether a database node supports Close (not available for the primary database).
    pub(super) fn database_supports_close(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Database { profile_id, name }) = parse_node_id(item_id) else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };

        let strategy = conn.connection.schema_loading_strategy();

        match strategy {
            SchemaLoadingStrategy::LazyPerDatabase => {
                conn.database_schemas.contains_key(&name)
            }
            SchemaLoadingStrategy::ConnectionPerDatabase => {
                conn.database_connections.contains_key(&name)
            }
            _ => false,
        }
    }

    pub fn context_menu_select_next(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && menu.selected_index < menu.items.len().saturating_sub(1)
        {
            menu.selected_index += 1;
            cx.notify();
        }
    }

    pub fn context_menu_select_prev(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && menu.selected_index > 0
        {
            menu.selected_index -= 1;
            cx.notify();
        }
    }

    pub fn context_menu_select_first(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && menu.selected_index != 0
        {
            menu.selected_index = 0;
            cx.notify();
        }
    }

    pub fn context_menu_select_last(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu {
            let last = menu.items.len().saturating_sub(1);
            if menu.selected_index != last {
                menu.selected_index = last;
                cx.notify();
            }
        }
    }

    pub fn context_menu_execute(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let Some(item) = menu.items.get(menu.selected_index).cloned() else {
            return;
        };

        let item_id = menu.item_id.clone();

        match item.action {
            ContextMenuAction::Submenu(sub_items) => {
                // Navigate into submenu
                let current_items = std::mem::take(&mut menu.items);
                let current_index = menu.selected_index;
                menu.parent_stack.push((current_items, current_index));
                menu.items = sub_items;
                menu.selected_index = 0;
                cx.notify();
                return;
            }
            ContextMenuAction::Open => {
                let node_kind = parse_node_kind(&item_id);
                if node_kind == SchemaNodeKind::Collection {
                    self.browse_collection(&item_id, cx);
                } else {
                    self.browse_table(&item_id, cx);
                }
            }
            ContextMenuAction::ViewSchema => {
                self.set_expanded(&item_id, true, cx);
            }
            ContextMenuAction::GenerateCode(generator_id) => {
                self.generate_code(&item_id, &generator_id, cx);
            }
            ContextMenuAction::Connect => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.connect_to_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Disconnect => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.disconnect_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Refresh => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.refresh_connection(profile_id, cx);
                }
            }
            ContextMenuAction::Edit => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.edit_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Delete => {
                self.show_delete_confirm_modal(&item_id, cx);
            }
            ContextMenuAction::OpenDatabase => {
                self.execute_item(&item_id, cx);
            }
            ContextMenuAction::CloseDatabase => {
                self.close_database(&item_id, cx);
            }
            ContextMenuAction::NewFolder => {
                self.create_folder_from_context(&item_id, cx);
            }
            ContextMenuAction::NewConnection => {
                self.create_connection_in_folder(&item_id, cx);
            }
            ContextMenuAction::RenameFolder => {
                self.pending_rename_item = Some(item_id.clone());
            }
            ContextMenuAction::DeleteFolder => {
                self.show_delete_confirm_modal(&item_id, cx);
            }
            ContextMenuAction::MoveToFolder(target_folder_id) => {
                self.move_item_to_folder(&item_id, target_folder_id, cx);
            }
            ContextMenuAction::GenerateIndexSql(action) => {
                self.generate_index_sql(&item_id, action, cx);
            }
            ContextMenuAction::GenerateForeignKeySql(action) => {
                self.generate_foreign_key_sql(&item_id, action, cx);
            }
            ContextMenuAction::GenerateTypeSql(action) => {
                self.generate_type_sql(&item_id, action, cx);
            }
            ContextMenuAction::GenerateCollectionCode(kind) => {
                self.generate_collection_code(&item_id, kind, cx);
            }
        }

        // Close menu after executing action
        self.context_menu = None;
        cx.notify();
    }

    /// Execute menu action at a specific index (for mouse clicks).
    pub fn context_menu_execute_at(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu {
            if index >= menu.items.len() {
                log::warn!(
                    "context_menu_execute_at: invalid index {} for {} items",
                    index,
                    menu.items.len()
                );
                return;
            }
            menu.selected_index = index;
        }
        self.context_menu_execute(cx);
    }

    pub fn context_menu_go_back(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(ref mut menu) = self.context_menu else {
            return false;
        };

        if let Some((parent_items, parent_index)) = menu.parent_stack.pop() {
            menu.items = parent_items;
            menu.selected_index = parent_index;
            cx.notify();
            true
        } else {
            false
        }
    }

    /// Go back to parent menu and execute action at given index.
    pub fn context_menu_parent_execute_at(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.context_menu_go_back(cx) {
            self.context_menu_execute_at(index, cx);
        }
    }

    pub fn close_context_menu(&mut self, cx: &mut Context<Self>) {
        if self.context_menu.is_some() {
            self.context_menu = None;
            cx.notify();
        }
    }

    pub fn has_context_menu_open(&self) -> bool {
        self.context_menu.is_some()
    }

    pub fn context_menu_state(&self) -> Option<&ContextMenuState> {
        self.context_menu.as_ref()
    }

    /// Returns an approximate position for the context menu based on the selected item.
    /// Used for keyboard-triggered menu opening (m key).
    pub fn selected_item_menu_position(&self, cx: &App) -> Point<Pixels> {
        let header_height = px(40.0);
        let row_height = px(28.0);
        let menu_x = px(180.0);

        let index = self.tree_state.read(cx).selected_index().unwrap_or(0);
        let y = header_height + (row_height * (index as f32));

        Point::new(menu_x, y)
    }
}
