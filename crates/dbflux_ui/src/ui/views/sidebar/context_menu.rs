use super::*;
use dbflux_core::DdlCapabilities;

impl Sidebar {
    fn append_menu_section(
        items: &mut Vec<ContextMenuItem>,
        section: impl IntoIterator<Item = ContextMenuItem>,
    ) {
        let mut section_items: Vec<ContextMenuItem> = section
            .into_iter()
            .filter(ContextMenuItem::is_selectable)
            .collect();

        if section_items.is_empty() {
            return;
        }

        if !items.is_empty() {
            items.push(ContextMenuItem::separator());
        }

        items.append(&mut section_items);
    }

    fn first_selectable_index(items: &[ContextMenuItem]) -> usize {
        items
            .iter()
            .position(ContextMenuItem::is_selectable)
            .unwrap_or(0)
    }

    fn last_selectable_index(items: &[ContextMenuItem]) -> usize {
        items
            .iter()
            .rposition(ContextMenuItem::is_selectable)
            .unwrap_or(0)
    }

    fn next_selectable_index(items: &[ContextMenuItem], current_index: usize) -> Option<usize> {
        items
            .iter()
            .enumerate()
            .skip(current_index.saturating_add(1))
            .find(|(_, item)| item.is_selectable())
            .map(|(index, _)| index)
    }

    fn previous_selectable_index(items: &[ContextMenuItem], current_index: usize) -> Option<usize> {
        items[..current_index]
            .iter()
            .rposition(ContextMenuItem::is_selectable)
    }

    pub(super) fn view_table_schema(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.set_expanded(item_id, true, cx);
    }

    pub fn open_item_menu(&mut self, position: Point<Pixels>, cx: &mut Context<Self>) {
        let entry = self.active_tree_state().read(cx).selected_entry().cloned();

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
            selected_index: Self::first_selectable_index(&items),
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
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item("Open", ContextMenuAction::Open)],
                );

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item("View Schema", ContextMenuAction::ViewSchema),
                        ContextMenuItem::item("Refresh", ContextMenuAction::RefreshObject),
                    ],
                );

                // Get code generators from driver (if connected)
                let generators = self.get_code_generators_for_item(item_id, node_kind, cx);
                if !generators.is_empty() {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            "Generate SQL",
                            ContextMenuAction::Submenu(generators),
                        )
                        .with_icon(AppIcon::Code)],
                    );
                }

                // Drop items gated on DDL capabilities
                if let Some(ddl) = self.get_ddl_capabilities(item_id, cx) {
                    let drop_allowed = match node_kind {
                        SchemaNodeKind::Table => ddl.supports_drop_table,
                        SchemaNodeKind::View => ddl.supports_drop_view,
                        _ => false,
                    };
                    if drop_allowed {
                        let label = match node_kind {
                            SchemaNodeKind::View => "Drop View",
                            _ => "Drop Table",
                        };
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::danger(label, ContextMenuAction::DropTable)],
                        );
                    }
                }

                items
            }
            SchemaNodeKind::Collection => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item("Open", ContextMenuAction::Open)],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        "Refresh",
                        ContextMenuAction::RefreshObject,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        "Generate Query",
                        ContextMenuAction::Submenu(vec![
                            ContextMenuItem::item(
                                "find",
                                ContextMenuAction::GenerateCollectionCode(CollectionCodeKind::Find),
                            ),
                            ContextMenuItem::item(
                                "insertOne",
                                ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::InsertOne,
                                ),
                            ),
                            ContextMenuItem::item(
                                "updateOne",
                                ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::UpdateOne,
                                ),
                            ),
                            ContextMenuItem::item(
                                "deleteOne",
                                ContextMenuAction::GenerateCollectionCode(
                                    CollectionCodeKind::DeleteOne,
                                ),
                            ),
                        ]),
                    )
                    .with_icon(AppIcon::Code)],
                );

                // Drop collection gated on DDL capabilities
                if self
                    .get_ddl_capabilities(item_id, cx)
                    .is_some_and(|ddl| ddl.supports_drop_table)
                {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::danger(
                            "Drop Collection",
                            ContextMenuAction::DropCollection,
                        )],
                    );
                }

                items
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

                let mut items = Vec::new();

                if is_connected {
                    Self::append_menu_section(
                        &mut items,
                        [
                            ContextMenuItem::item("Disconnect", ContextMenuAction::Disconnect),
                            ContextMenuItem::item("Refresh", ContextMenuAction::Refresh),
                        ],
                    );
                } else {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item("Connect", ContextMenuAction::Connect)],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item("Edit", ContextMenuAction::Edit),
                        ContextMenuItem::item("Duplicate", ContextMenuAction::Duplicate),
                        ContextMenuItem::item("Rename", ContextMenuAction::RenameFolder),
                    ],
                );

                // Add "Move to..." submenu with available folders
                let move_to_items = self.build_move_to_submenu(item_id, cx);
                if !move_to_items.is_empty() {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            "Move to...",
                            ContextMenuAction::Submenu(move_to_items),
                        )
                        .with_icon(AppIcon::Folder)],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger("Delete", ContextMenuAction::Delete)],
                );

                items
            }
            SchemaNodeKind::Database => {
                let is_loaded = self.is_database_schema_loaded(item_id, cx);
                let mut items = Vec::new();

                if is_loaded {
                    // Only show Close for databases that support it (MySQL/MariaDB)
                    if self.database_supports_close(item_id, cx) {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::item(
                                "Close",
                                ContextMenuAction::CloseDatabase,
                            )],
                        );
                    }

                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            "Refresh",
                            ContextMenuAction::RefreshDatabase,
                        )],
                    );

                    // Drop Database gated on DDL capabilities
                    if self
                        .get_ddl_capabilities(item_id, cx)
                        .is_some_and(|ddl| ddl.supports_drop_database)
                    {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::danger(
                                "Drop Database",
                                ContextMenuAction::DropDatabase,
                            )],
                        );
                    }
                } else {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            "Open",
                            ContextMenuAction::OpenDatabase,
                        )],
                    );
                }

                items
            }
            SchemaNodeKind::ConnectionFolder => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item("New Connection", ContextMenuAction::NewConnection),
                        ContextMenuItem::item("New Folder", ContextMenuAction::NewFolder),
                    ],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        "Rename",
                        ContextMenuAction::RenameFolder,
                    )],
                );

                let move_to_items = self.build_move_to_submenu(item_id, cx);
                if !move_to_items.is_empty() {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            "Move to...",
                            ContextMenuAction::Submenu(move_to_items),
                        )
                        .with_icon(AppIcon::Folder)],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        "Delete",
                        ContextMenuAction::DeleteFolder,
                    )],
                );

                items
            }

            SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::CREATE_INDEX) {
                    submenu.push(ContextMenuItem::item(
                        "CREATE INDEX",
                        ContextMenuAction::GenerateIndexSql(IndexSqlAction::Create),
                    ));
                }

                if caps.contains(CodeGenCapabilities::DROP_INDEX) {
                    submenu.push(ContextMenuItem::item(
                        "DROP INDEX",
                        ContextMenuAction::GenerateIndexSql(IndexSqlAction::Drop),
                    ));
                }

                if caps.contains(CodeGenCapabilities::REINDEX) {
                    submenu.push(ContextMenuItem::item(
                        "REINDEX",
                        ContextMenuAction::GenerateIndexSql(IndexSqlAction::Reindex),
                    ));
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![
                        ContextMenuItem::item("Generate SQL", ContextMenuAction::Submenu(submenu))
                            .with_icon(AppIcon::Code),
                    ]
                }
            }

            SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::ADD_FOREIGN_KEY) {
                    submenu.push(ContextMenuItem::item(
                        "ADD CONSTRAINT",
                        ContextMenuAction::GenerateForeignKeySql(
                            ForeignKeySqlAction::AddConstraint,
                        ),
                    ));
                }

                if caps.contains(CodeGenCapabilities::DROP_FOREIGN_KEY) {
                    submenu.push(ContextMenuItem::item(
                        "DROP CONSTRAINT",
                        ContextMenuAction::GenerateForeignKeySql(
                            ForeignKeySqlAction::DropConstraint,
                        ),
                    ));
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![
                        ContextMenuItem::item("Generate SQL", ContextMenuAction::Submenu(submenu))
                            .with_icon(AppIcon::Code),
                    ]
                }
            }

            SchemaNodeKind::CustomType => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::CREATE_TYPE)
                    && let Some(label) = self.create_type_sql_label(item_id, cx)
                {
                    submenu.push(ContextMenuItem::item(
                        label,
                        ContextMenuAction::GenerateTypeSql(TypeSqlAction::Create),
                    ));
                }

                if caps.contains(CodeGenCapabilities::ALTER_TYPE) && self.is_enum_type(item_id, cx)
                {
                    submenu.push(ContextMenuItem::item(
                        "ADD VALUE",
                        ContextMenuAction::GenerateTypeSql(TypeSqlAction::AddEnumValue),
                    ));
                }

                if caps.contains(CodeGenCapabilities::DROP_TYPE) {
                    submenu.push(ContextMenuItem::item(
                        "DROP TYPE",
                        ContextMenuAction::GenerateTypeSql(TypeSqlAction::Drop),
                    ));
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![
                        ContextMenuItem::item("Generate SQL", ContextMenuAction::Submenu(submenu))
                            .with_icon(AppIcon::Code),
                    ]
                }
            }

            SchemaNodeKind::ScriptsFolder => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item("New File", ContextMenuAction::NewScriptFile),
                        ContextMenuItem::item("New Folder", ContextMenuAction::NewScriptFolder),
                    ],
                );

                // Only show rename/delete for subfolders, not the root
                if let Some(SchemaNodeId::ScriptsFolder { path: Some(_) }) = parse_node_id(item_id)
                {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            "Rename",
                            ContextMenuAction::RenameScript,
                        )],
                    );

                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::danger(
                            "Delete",
                            ContextMenuAction::DeleteScript,
                        )],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            "Reveal in File Manager",
                            ContextMenuAction::RevealInFileManager,
                        ),
                        ContextMenuItem::item("Copy Path", ContextMenuAction::CopyPath),
                    ],
                );

                items
            }

            SchemaNodeKind::ScriptFile => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item("Open", ContextMenuAction::OpenScript)],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        "Rename",
                        ContextMenuAction::RenameScript,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            "Reveal in File Manager",
                            ContextMenuAction::RevealInFileManager,
                        ),
                        ContextMenuItem::item("Copy Path", ContextMenuAction::CopyPath),
                    ],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        "Delete",
                        ContextMenuAction::DeleteScript,
                    )],
                );

                items
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
            items.push(ContextMenuItem::item(
                "Root",
                ContextMenuAction::MoveToFolder(None),
            ));
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

            items.push(ContextMenuItem::item(
                folder.name.clone(),
                ContextMenuAction::MoveToFolder(Some(folder.id)),
            ));
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
            SchemaLoadingStrategy::LazyPerDatabase => conn.database_schemas.contains_key(&name),
            SchemaLoadingStrategy::ConnectionPerDatabase => {
                conn.database_connections.contains_key(&name)
            }
            _ => false,
        }
    }

    /// Extract DDL capabilities from the driver metadata for the given item.
    pub(super) fn get_ddl_capabilities(&self, item_id: &str, cx: &App) -> Option<DdlCapabilities> {
        let profile_id = Self::extract_profile_id_from_item(item_id)?;
        let state = self.app_state.read(cx);
        let conn = state.connections().get(&profile_id)?;
        conn.connection.metadata().ddl.clone()
    }

    pub fn context_menu_select_next(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && let Some(next_index) = Self::next_selectable_index(&menu.items, menu.selected_index)
        {
            menu.selected_index = next_index;
            cx.notify();
        }
    }

    pub fn context_menu_select_prev(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && let Some(previous_index) =
                Self::previous_selectable_index(&menu.items, menu.selected_index)
        {
            menu.selected_index = previous_index;
            cx.notify();
        }
    }

    pub fn context_menu_select_first(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu {
            let first = Self::first_selectable_index(&menu.items);

            if menu.selected_index != first {
                menu.selected_index = first;
                cx.notify();
            }
        }
    }

    pub fn context_menu_select_last(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu {
            let last = Self::last_selectable_index(&menu.items);

            if menu.selected_index != last {
                menu.selected_index = last;
                cx.notify();
            }
        }
    }

    pub fn context_menu_hover_at(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu {
            let Some(item) = menu.items.get(index) else {
                return;
            };

            if !item.is_selectable() || menu.selected_index == index {
                return;
            }

            menu.selected_index = index;
            cx.notify();
        }
    }

    pub fn context_menu_parent_hover_at(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && let Some((parent_items, parent_selected)) = menu.parent_stack.last_mut()
        {
            let Some(item) = parent_items.get(index) else {
                return;
            };

            if !item.is_selectable() || *parent_selected == index {
                return;
            }

            *parent_selected = index;
            cx.notify();
        }
    }

    pub fn context_menu_execute(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let Some(item) = menu.items.get(menu.selected_index).cloned() else {
            return;
        };

        if !item.is_selectable() {
            return;
        }

        let item_id = menu.item_id.clone();

        match item.action {
            ContextMenuAction::Submenu(sub_items) => {
                // Navigate into submenu
                let current_items = std::mem::take(&mut menu.items);
                let current_index = menu.selected_index;
                menu.parent_stack.push((current_items, current_index));
                menu.items = sub_items;
                menu.selected_index = Self::first_selectable_index(&menu.items);
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
            ContextMenuAction::Duplicate => {
                self.duplicate_profile(&item_id, cx);
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
            ContextMenuAction::OpenScript => {
                self.execute_item(&item_id, cx);
            }
            ContextMenuAction::RenameScript => {
                self.pending_rename_item = Some(item_id.clone());
            }
            ContextMenuAction::DeleteScript => {
                self.show_delete_confirm_modal(&item_id, cx);
            }
            ContextMenuAction::NewScriptFile => {
                let parent = Self::parent_dir_from_item_id(&item_id);
                self.create_script_file_in(parent, cx);
            }
            ContextMenuAction::NewScriptFolder => {
                let parent = Self::parent_dir_from_item_id(&item_id);
                self.create_script_folder_in(parent, cx);
            }
            ContextMenuAction::RevealInFileManager => {
                self.reveal_in_file_manager(&item_id);
            }
            ContextMenuAction::CopyPath => {
                self.copy_path_to_clipboard(&item_id, cx);
            }
            ContextMenuAction::RefreshDatabase => {
                self.refresh_schema_database(&item_id, cx);
            }
            ContextMenuAction::RefreshObject => {
                self.refresh_schema_object(&item_id, cx);
            }
            ContextMenuAction::DropDatabase => {
                self.show_ddl_confirm_modal(&item_id, "Database", cx);
            }
            ContextMenuAction::DropTable => {
                let node_kind = parse_node_kind(&item_id);
                let object_type = if node_kind == SchemaNodeKind::View {
                    "View"
                } else {
                    "Table"
                };
                self.show_ddl_confirm_modal(&item_id, object_type, cx);
            }
            ContextMenuAction::DropCollection => {
                self.show_ddl_confirm_modal(&item_id, "Collection", cx);
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

            if !menu.items[index].is_selectable() {
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

        let index = self
            .active_tree_state()
            .read(cx)
            .selected_index()
            .unwrap_or(0);
        let y = header_height + (row_height * (index as f32));

        Point::new(menu_x, y)
    }
}
