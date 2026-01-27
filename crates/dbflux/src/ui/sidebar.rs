use crate::app::{AppState, AppStateChanged};
use crate::ui::editor::EditorPane;
use crate::ui::results::ResultsPane;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use crate::ui::windows::settings::SettingsWindow;
use dbflux_core::{
    CodeGenScope, SchemaLoadingStrategy, SchemaSnapshot, TableInfo, TaskKind, ViewInfo,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Root;
use gpui_component::list::ListItem;
use gpui_component::tree::{TreeItem, TreeState, tree};
use gpui_component::{Icon, IconName};
use std::collections::HashMap;
use uuid::Uuid;

pub enum SidebarEvent {
    GenerateSql(String),
    RequestFocus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeNodeKind {
    Profile,
    Database,
    Schema,
    TablesFolder,
    ViewsFolder,
    Table,
    View,
    ColumnsFolder,
    IndexesFolder,
    Column,
    Index,
    Unknown,
}

impl TreeNodeKind {
    fn from_id(id: &str) -> Self {
        match id {
            _ if id.starts_with("profile_") => Self::Profile,
            _ if id.starts_with("db_") => Self::Database,
            _ if id.starts_with("schema_") => Self::Schema,
            _ if id.starts_with("tables_") => Self::TablesFolder,
            _ if id.starts_with("views_") => Self::ViewsFolder,
            _ if id.starts_with("table_") => Self::Table,
            _ if id.starts_with("view_") => Self::View,
            _ if id.starts_with("columns_") => Self::ColumnsFolder,
            _ if id.starts_with("indexes_") => Self::IndexesFolder,
            _ if id.starts_with("col_") => Self::Column,
            _ if id.starts_with("idx_") => Self::Index,
            _ => Self::Unknown,
        }
    }

    fn needs_click_handler(&self) -> bool {
        matches!(
            self,
            Self::Profile | Self::Database | Self::Table | Self::View
        )
    }

    fn shows_pointer_cursor(&self) -> bool {
        matches!(self, Self::Profile | Self::Database)
    }
}

#[derive(Clone)]
pub struct ContextMenuItem {
    pub label: String,
    pub action: ContextMenuAction,
}

#[derive(Clone)]
pub enum ContextMenuAction {
    Open,
    ViewSchema,
    GenerateCode(String),
    Connect,
    Disconnect,
    Edit,
    Delete,
    OpenDatabase,
    CloseDatabase,
    Submenu(Vec<ContextMenuItem>),
}

pub struct ContextMenuState {
    pub item_id: String,
    pub selected_index: usize,
    pub items: Vec<ContextMenuItem>,
    /// Stack of parent menus for submenu navigation
    pub parent_stack: Vec<(Vec<ContextMenuItem>, usize)>,
    /// Position where the menu should appear (captured from click or calculated)
    pub position: Point<Pixels>,
}

/// Parsed components from a tree item ID (table or view).
struct ItemIdParts {
    profile_id: Uuid,
    schema_name: String,
    object_name: String,
}

/// Action to execute after table details finish loading.
#[derive(Clone)]
enum PendingAction {
    ViewSchema {
        item_id: String,
    },
    GenerateCode {
        item_id: String,
        generator_id: String,
    },
}

/// Result of checking whether table details are available.
enum TableDetailsStatus {
    Ready,
    Loading,
    NotFound,
}

pub struct Sidebar {
    app_state: Entity<AppState>,
    #[allow(dead_code)]
    editor: Entity<EditorPane>,
    results: Entity<ResultsPane>,
    tree_state: Entity<TreeState>,
    pending_view_table: Option<(Uuid, String)>,
    pending_toast: Option<PendingToast>,
    connections_focused: bool,
    visible_entry_count: usize,
    /// User overrides for expansion state (item_id -> is_expanded)
    expansion_overrides: HashMap<String, bool>,
    /// State for the keyboard-triggered context menu
    context_menu: Option<ContextMenuState>,
    /// Action to execute after table details finish loading
    pending_action: Option<PendingAction>,
    /// Maps profile_id -> active database name (for styling in render)
    active_databases: HashMap<Uuid, String>,
    _subscriptions: Vec<Subscription>,
}

struct PendingToast {
    message: String,
    is_error: bool,
}

impl EventEmitter<SidebarEvent> for Sidebar {}

impl Sidebar {
    pub fn new(
        app_state: Entity<AppState>,
        editor: Entity<EditorPane>,
        results: Entity<ResultsPane>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let items = Self::build_tree_items(app_state.read(cx));
        let visible_entry_count = Self::count_visible_entries(&items);
        let tree_state = cx.new(|cx| TreeState::new(cx).items(items));

        let app_state_subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.refresh_tree(cx);
        });

        Self {
            app_state,
            editor,
            results,
            tree_state,
            pending_view_table: None,
            pending_toast: None,
            connections_focused: false,
            visible_entry_count,
            expansion_overrides: HashMap::new(),
            context_menu: None,
            pending_action: None,
            active_databases: HashMap::new(),
            _subscriptions: vec![app_state_subscription],
        }
    }

    pub fn set_connections_focused(&mut self, focused: bool, cx: &mut Context<Self>) {
        if self.connections_focused != focused {
            self.connections_focused = focused;
            cx.notify();
        }
    }

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        self.tree_state.update(cx, |state, cx| {
            let next = match state.selected_index() {
                Some(current) => (current + 1).min(self.visible_entry_count.saturating_sub(1)),
                None => 0,
            };
            state.set_selected_index(Some(next), cx);
            state.scroll_to_item(next, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        self.tree_state.update(cx, |state, cx| {
            let prev = match state.selected_index() {
                Some(current) => current.saturating_sub(1),
                None => self.visible_entry_count.saturating_sub(1),
            };
            state.set_selected_index(Some(prev), cx);
            state.scroll_to_item(prev, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(0), cx);
            state.scroll_to_item(0, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        let last = self.visible_entry_count.saturating_sub(1);
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(last), cx);
            state.scroll_to_item(last, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn expand_collapse(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_folder()
        {
            let item_id = entry.item().id.to_string();
            let currently_expanded = entry.is_expanded();
            self.set_expanded(&item_id, !currently_expanded, cx);
        }
    }

    pub fn collapse(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_folder()
            && entry.is_expanded()
        {
            let item_id = entry.item().id.to_string();
            self.set_expanded(&item_id, false, cx);
        }
    }

    pub fn expand(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_folder()
            && !entry.is_expanded()
        {
            let item_id = entry.item().id.to_string();
            self.set_expanded(&item_id, true, cx);
        }
    }

    fn set_expanded(&mut self, item_id: &str, expanded: bool, cx: &mut Context<Self>) {
        // When expanding a table, check if columns need to be lazy loaded
        if expanded && item_id.starts_with("table_") {
            let pending = PendingAction::ViewSchema {
                item_id: item_id.to_string(),
            };
            let status = self.ensure_table_details(item_id, pending, cx);

            // Only expand immediately if details are ready (cached)
            // If Loading, complete_pending_action will handle expansion after fetch
            if !matches!(status, TableDetailsStatus::Ready) {
                return;
            }
        }

        self.expansion_overrides
            .insert(item_id.to_string(), expanded);
        self.rebuild_tree_with_overrides(cx);
    }

    fn rebuild_tree_with_overrides(&mut self, cx: &mut Context<Self>) {
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

    pub fn execute(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry {
            let item_id = entry.item().id.to_string();
            self.execute_item(&item_id, cx);
        }
    }

    fn execute_item(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let node_kind = TreeNodeKind::from_id(item_id);

        match node_kind {
            TreeNodeKind::Table | TreeNodeKind::View => {
                self.browse_table(item_id, cx);
            }
            TreeNodeKind::Profile => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                {
                    let is_connected = self
                        .app_state
                        .read(cx)
                        .connections
                        .contains_key(&profile_id);
                    if is_connected {
                        self.app_state.update(cx, |state, cx| {
                            state.set_active_connection(profile_id);
                            cx.notify();
                        });
                    } else {
                        self.connect_to_profile(profile_id, cx);
                    }
                }
            }
            TreeNodeKind::Database => {
                self.handle_database_click(item_id, cx);
            }
            _ => {}
        }
    }

    fn handle_item_click(&mut self, item_id: &str, click_count: usize, cx: &mut Context<Self>) {
        cx.emit(SidebarEvent::RequestFocus);

        if let Some(idx) = self.find_item_index(item_id, cx) {
            self.tree_state.update(cx, |state, cx| {
                state.set_selected_index(Some(idx), cx);
            });
        }

        if click_count == 2 {
            self.execute_item(item_id, cx);
        } else {
            // Single click on Profile or Database: also toggle expansion
            // This keeps our expansion_overrides in sync with user intent
            let node_kind = TreeNodeKind::from_id(item_id);
            if matches!(node_kind, TreeNodeKind::Profile | TreeNodeKind::Database) {
                self.toggle_item_expansion(item_id, cx);
            }
        }

        cx.notify();
    }

    fn browse_table(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(parts) = Self::parse_table_or_view_id(item_id) {
            let qualified_name = format!("{}.{}", parts.schema_name, parts.object_name);
            self.pending_view_table = Some((parts.profile_id, qualified_name));
            cx.notify();
        }
    }

    fn toggle_item_expansion(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let items = self.build_tree_items_with_overrides(cx);
        let currently_expanded = Self::find_item_expanded(&items, item_id).unwrap_or(false);
        self.set_expanded(item_id, !currently_expanded, cx);
    }

    fn find_item_expanded(items: &[TreeItem], target_id: &str) -> Option<bool> {
        for item in items {
            if item.id.as_ref() == target_id {
                return Some(item.is_expanded());
            }
            if let Some(expanded) = Self::find_item_expanded(&item.children, target_id) {
                return Some(expanded);
            }
        }
        None
    }

    fn get_code_generators_for_item(
        &self,
        item_id: &str,
        node_kind: TreeNodeKind,
        cx: &App,
    ) -> Vec<ContextMenuItem> {
        let Some(parts) = Self::parse_table_or_view_id(item_id) else {
            return vec![];
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&parts.profile_id) else {
            return vec![];
        };

        let scope_filter = match node_kind {
            TreeNodeKind::Table => {
                |s: CodeGenScope| matches!(s, CodeGenScope::Table | CodeGenScope::TableOrView)
            }
            TreeNodeKind::View => {
                |s: CodeGenScope| matches!(s, CodeGenScope::View | CodeGenScope::TableOrView)
            }
            _ => return vec![],
        };

        let mut generators: Vec<_> = conn
            .connection
            .code_generators()
            .iter()
            .filter(|g| scope_filter(g.scope))
            .collect();

        generators.sort_by_key(|g| g.order);

        generators
            .into_iter()
            .map(|g| {
                let label = if g.destructive {
                    format!("\u{26A0} {}", g.label)
                } else {
                    g.label.to_string()
                };
                ContextMenuItem {
                    label,
                    action: ContextMenuAction::GenerateCode(g.id.to_string()),
                }
            })
            .collect()
    }

    fn generate_code(&mut self, item_id: &str, generator_id: &str, cx: &mut Context<Self>) {
        let is_view = item_id.starts_with("view_");

        // For views, generate code directly (no columns needed)
        if is_view {
            self.generate_code_for_view(item_id, generator_id, cx);
            return;
        }

        // For tables, ensure details are loaded first
        let pending = PendingAction::GenerateCode {
            item_id: item_id.to_string(),
            generator_id: generator_id.to_string(),
        };

        match self.ensure_table_details(item_id, pending, cx) {
            TableDetailsStatus::Ready => {
                self.generate_code_impl(item_id, generator_id, cx);
            }
            TableDetailsStatus::Loading => {
                // Will be handled by complete_pending_action when done
            }
            TableDetailsStatus::NotFound => {
                log::warn!("Code generation failed: table not found");
            }
        }
    }

    fn generate_code_for_view(
        &mut self,
        item_id: &str,
        generator_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(parts) = Self::parse_table_or_view_id(item_id) else {
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&parts.profile_id) else {
            return;
        };

        // Try to find view in database_schemas (MySQL/MariaDB)
        let view_from_db_schemas = conn
            .database_schemas
            .get(&parts.schema_name)
            .and_then(|db_schema| {
                db_schema
                    .views
                    .iter()
                    .find(|v| v.name == parts.object_name)
            });

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let view =
            view_from_db_schemas.or_else(|| Self::find_view_for_item(&parts, &conn.schema));

        let Some(view) = view else {
            log::warn!(
                "Code generation for view '{}' failed: view not found",
                parts.object_name
            );
            return;
        };

        // Create a TableInfo from the ViewInfo for code generation
        let table_info = TableInfo {
            name: view.name.clone(),
            schema: view.schema.clone(),
            columns: None,
            indexes: None,
        };

        match conn.connection.generate_code(generator_id, &table_info) {
            Ok(sql) => cx.emit(SidebarEvent::GenerateSql(sql)),
            Err(e) => {
                log::error!("Code generation for view failed: {}", e);
                self.pending_toast = Some(PendingToast {
                    message: format!("Code generation failed: {}", e),
                    is_error: true,
                });
                cx.notify();
            }
        }
    }

    fn generate_code_impl(&mut self, item_id: &str, generator_id: &str, cx: &mut Context<Self>) {
        let Some(parts) = Self::parse_table_or_view_id(item_id) else {
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&parts.profile_id) else {
            return;
        };

        // First check the table_details cache (populated by ensure_table_details)
        let cache_key = (parts.schema_name.clone(), parts.object_name.clone());
        if let Some(table) = conn.table_details.get(&cache_key) {
            match conn.connection.generate_code(generator_id, table) {
                Ok(sql) => cx.emit(SidebarEvent::GenerateSql(sql)),
                Err(e) => {
                    log::error!("Code generation failed: {}", e);
                    self.pending_toast = Some(PendingToast {
                        message: format!("Code generation failed: {}", e),
                        is_error: true,
                    });
                    cx.notify();
                }
            }
            return;
        }

        // Fallback: search in database_schemas (MySQL/MariaDB)
        let table_from_db_schemas = conn
            .database_schemas
            .get(&parts.schema_name)
            .and_then(|db_schema| {
                db_schema
                    .tables
                    .iter()
                    .find(|t| t.name == parts.object_name)
            });

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let table =
            table_from_db_schemas.or_else(|| Self::find_table_for_item(&parts, &conn.schema));

        let Some(table) = table else {
            log::warn!(
                "Code generation for '{}' failed: table not found",
                parts.object_name
            );
            return;
        };

        match conn.connection.generate_code(generator_id, table) {
            Ok(sql) => cx.emit(SidebarEvent::GenerateSql(sql)),
            Err(e) => {
                log::error!("Code generation failed: {}", e);
                self.pending_toast = Some(PendingToast {
                    message: format!("Code generation failed: {}", e),
                    is_error: true,
                });
                cx.notify();
            }
        }
    }

    fn find_table_for_item<'a>(
        parts: &ItemIdParts,
        schema: &'a Option<SchemaSnapshot>,
    ) -> Option<&'a TableInfo> {
        let schema = schema.as_ref()?;

        for db_schema in &schema.schemas {
            if db_schema.name == parts.schema_name {
                return db_schema
                    .tables
                    .iter()
                    .find(|t| t.name == parts.object_name);
            }
        }

        // For databases without schemas (fallback)
        schema.tables.iter().find(|t| t.name == parts.object_name)
    }

    fn find_view_for_item<'a>(
        parts: &ItemIdParts,
        schema: &'a Option<SchemaSnapshot>,
    ) -> Option<&'a ViewInfo> {
        let schema = schema.as_ref()?;

        for db_schema in &schema.schemas {
            if db_schema.name == parts.schema_name {
                return db_schema
                    .views
                    .iter()
                    .find(|v| v.name == parts.object_name);
            }
        }

        // For databases without schemas (fallback)
        schema.views.iter().find(|v| v.name == parts.object_name)
    }

    /// Check if a table has detailed schema (columns/indexes) loaded.
    /// If not, spawns a background task to fetch them and returns `Loading`.
    fn ensure_table_details(
        &mut self,
        item_id: &str,
        pending_action: PendingAction,
        cx: &mut Context<Self>,
    ) -> TableDetailsStatus {
        let Some(parts) = Self::parse_table_or_view_id(item_id) else {
            return TableDetailsStatus::NotFound;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&parts.profile_id) else {
            return TableDetailsStatus::NotFound;
        };

        // First check the table_details cache for detailed info
        let cache_key = (parts.schema_name.clone(), parts.object_name.clone());
        if conn.table_details.contains_key(&cache_key) {
            return TableDetailsStatus::Ready;
        }

        // Check database_schemas for a table that already has columns loaded
        if let Some(db_schema) = conn.database_schemas.get(&parts.schema_name)
            && let Some(table) = db_schema.tables.iter().find(|t| t.name == parts.object_name)
            && table.columns.is_some()
        {
            return TableDetailsStatus::Ready;
        }

        // Check schema.schemas (PostgreSQL/SQLite path)
        if let Some(ref schema) = conn.schema {
            for db_schema in &schema.schemas {
                if db_schema.name == parts.schema_name
                    && let Some(table) =
                        db_schema.tables.iter().find(|t| t.name == parts.object_name)
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
        let params = match self.app_state.read(cx).prepare_fetch_table_details(
            parts.profile_id,
            &parts.schema_name,
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
        let db_name = parts.schema_name.clone();
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

    /// Called when table details finish loading to execute the stored action.
    fn complete_pending_action(&mut self, cx: &mut Context<Self>) {
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
        }
    }

    fn view_table_schema(&mut self, item_id: &str, cx: &mut Context<Self>) {
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
        let node_kind = TreeNodeKind::from_id(item_id);
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

    fn build_context_menu_items(
        &self,
        node_kind: TreeNodeKind,
        item_id: &str,
        cx: &App,
    ) -> Vec<ContextMenuItem> {
        match node_kind {
            TreeNodeKind::Table | TreeNodeKind::View => {
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
            TreeNodeKind::Profile => {
                let is_connected = if let Some(profile_id_str) = item_id.strip_prefix("profile_") {
                    if let Ok(profile_id) = Uuid::parse_str(profile_id_str) {
                        self.app_state
                            .read(cx)
                            .connections
                            .contains_key(&profile_id)
                    } else {
                        false
                    }
                } else {
                    false
                };

                let mut items = vec![];
                if is_connected {
                    items.push(ContextMenuItem {
                        label: "Disconnect".into(),
                        action: ContextMenuAction::Disconnect,
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
                    label: "Delete".into(),
                    action: ContextMenuAction::Delete,
                });
                items
            }
            TreeNodeKind::Database => {
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
            _ => vec![],
        }
    }

    fn is_database_schema_loaded(&self, item_id: &str, cx: &App) -> bool {
        let Some(rest) = item_id.strip_prefix("db_") else {
            return false;
        };
        if rest.len() < 37 {
            return false;
        }
        let profile_id_str = &rest[..36];
        let db_name = &rest[37..];
        let Ok(profile_id) = Uuid::parse_str(profile_id_str) else {
            return false;
        };

        let state = self.app_state.read(cx);
        if let Some(conn) = state.connections.get(&profile_id) {
            conn.database_schemas.contains_key(db_name)
        } else {
            false
        }
    }

    fn database_supports_close(&self, item_id: &str, cx: &App) -> bool {
        let Some(rest) = item_id.strip_prefix("db_") else {
            return false;
        };
        if rest.len() < 37 {
            return false;
        }
        let profile_id_str = &rest[..36];
        let Ok(profile_id) = Uuid::parse_str(profile_id_str) else {
            return false;
        };

        let state = self.app_state.read(cx);
        if let Some(conn) = state.connections.get(&profile_id) {
            conn.connection.schema_loading_strategy() == SchemaLoadingStrategy::LazyPerDatabase
        } else {
            false
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
                self.browse_table(&item_id, cx);
            }
            ContextMenuAction::ViewSchema => {
                self.set_expanded(&item_id, true, cx);
            }
            ContextMenuAction::GenerateCode(generator_id) => {
                self.generate_code(&item_id, &generator_id, cx);
            }
            ContextMenuAction::Connect => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                {
                    self.connect_to_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Disconnect => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                {
                    self.disconnect_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Edit => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                {
                    self.edit_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Delete => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                {
                    self.delete_profile(profile_id, cx);
                }
            }
            ContextMenuAction::OpenDatabase => {
                self.handle_database_click(&item_id, cx);
            }
            ContextMenuAction::CloseDatabase => {
                self.close_database(&item_id, cx);
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

    /// Parse a table/view item ID into its components.
    ///
    /// Format: `{prefix}_{uuid}__{schema}__{name}` where prefix is "table" or "view".
    /// Uses `__` as separator to allow underscores in schema/table names.
    ///
    /// Uses `rsplit_once("__")` to handle table names containing `__`.
    /// Ambiguous if both schema and table contain `__` (rare).
    fn parse_table_or_view_id(item_id: &str) -> Option<ItemIdParts> {
        let rest = item_id
            .strip_prefix("table_")
            .or_else(|| item_id.strip_prefix("view_"))?;

        // UUID is 36 chars, followed by "__"
        if rest.len() < 38 {
            return None;
        }

        let uuid_str = rest.get(..36)?;
        let profile_id = Uuid::parse_str(uuid_str).ok()?;

        let after_uuid = rest.get(36..)?;
        let after_uuid = after_uuid.strip_prefix("__")?;
        let (schema_name, object_name) = after_uuid.rsplit_once("__")?;

        if schema_name.is_empty() || object_name.is_empty() {
            return None;
        }

        Some(ItemIdParts {
            profile_id,
            schema_name: schema_name.to_string(),
            object_name: object_name.to_string(),
        })
    }

    fn handle_database_click(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(rest) = item_id.strip_prefix("db_") else {
            return;
        };

        // UUID is 36 chars (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
        // Format: db_{uuid}_{dbname} where dbname may contain underscores
        if rest.len() < 37 {
            return;
        }

        let profile_id_str = &rest[..36];
        let db_name = &rest[37..]; // skip the underscore after UUID

        let Ok(profile_id) = Uuid::parse_str(profile_id_str) else {
            return;
        };

        let strategy = self
            .app_state
            .read(cx)
            .connections
            .get(&profile_id)
            .map(|c| c.connection.schema_loading_strategy());

        match strategy {
            Some(SchemaLoadingStrategy::LazyPerDatabase) => {
                self.handle_lazy_database_click(profile_id, db_name, cx);
            }
            Some(SchemaLoadingStrategy::ConnectionPerDatabase) => {
                self.handle_connection_per_database_click(profile_id, db_name, cx);
            }
            Some(SchemaLoadingStrategy::SingleDatabase) | None => {
                log::info!("Database click not applicable for this database type");
            }
        }
    }

    fn close_database(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(rest) = item_id.strip_prefix("db_") else {
            return;
        };

        if rest.len() < 37 {
            return;
        }

        let profile_id_str = &rest[..36];
        let db_name = &rest[37..];

        let Ok(profile_id) = Uuid::parse_str(profile_id_str) else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Some(conn) = state.connections.get_mut(&profile_id) {
                // Remove the database schema
                conn.database_schemas.remove(db_name);

                // If this was the active database, clear it
                if conn.active_database.as_deref() == Some(db_name) {
                    conn.active_database = None;
                }
            }
            cx.emit(AppStateChanged);
        });

        // Collapse the database node in the tree
        self.set_expanded(item_id, false, cx);

        self.refresh_tree(cx);
    }

    fn handle_lazy_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let needs_fetch = self
            .app_state
            .read(cx)
            .needs_database_schema(profile_id, db_name);

        // UI state only; driver issues USE at query time via QueryRequest.database
        self.app_state.update(cx, |state, cx| {
            state.set_active_database(profile_id, Some(db_name.to_string()));
            cx.emit(AppStateChanged);
        });

        if !needs_fetch {
            self.refresh_tree(cx);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let result = state.prepare_fetch_database_schema(profile_id, db_name);

            if result.is_ok() && !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Fetch database schema skipped: {}", e);
                self.refresh_tree(cx);
                return;
            }
        };

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result =
                state.start_task(TaskKind::LoadSchema, format!("Loading schema: {}", db_name));
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
                if cancel_token.is_cancelled() {
                    log::info!("Fetch database schema task was cancelled");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        None
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        Some(PendingToast {
                            message: format!("Failed to load schema: {}", e),
                            is_error: true,
                        })
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    if let Ok(res) = result {
                        state.set_database_schema(res.profile_id, res.database, res.schema);
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn handle_connection_per_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let params = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let result = state.prepare_switch_database(profile_id, db_name);

            if result.is_ok() && !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Switch database skipped: {}", e);
                return;
            }
        };

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result = state.start_task(
                TaskKind::SwitchDatabase,
                format!("Switching to database: {}", db_name),
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
                if cancel_token.is_cancelled() {
                    log::info!("Switch database task was cancelled, discarding result");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        None
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        Some(PendingToast {
                            message: format!("Failed to switch database: {}", e),
                            is_error: true,
                        })
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    if let Ok(res) = result {
                        state.apply_switch_database(
                            res.profile_id,
                            res.original_profile,
                            res.connection,
                            res.schema,
                        );
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn refresh_tree(&mut self, cx: &mut Context<Self>) {
        let selected_index = self.tree_state.read(cx).selected_index();
        self.active_databases = Self::extract_active_databases(self.app_state.read(cx));

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

    fn build_tree_items_with_overrides(&self, cx: &Context<Self>) -> Vec<TreeItem> {
        let items = Self::build_tree_items(self.app_state.read(cx));
        self.apply_expansion_overrides(items)
    }

    /// Extracts active database for each connection from AppState.
    fn extract_active_databases(state: &AppState) -> HashMap<Uuid, String> {
        state
            .connections
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

        let children: Vec<TreeItem> = item
            .children
            .into_iter()
            .map(|c| self.apply_override_recursive(c))
            .collect();

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

    fn build_tree_items(state: &AppState) -> Vec<TreeItem> {
        let mut items = Vec::new();

        for profile in &state.profiles {
            let profile_id = profile.id;
            let is_connected = state.connections.contains_key(&profile_id);
            let is_active = state.active_connection_id == Some(profile_id);
            let is_connecting = state.is_operation_pending(profile_id, None);

            let profile_label = if is_connecting {
                format!("{} (connecting...)", profile.name)
            } else {
                profile.name.clone()
            };

            let mut profile_item = TreeItem::new(format!("profile_{}", profile_id), profile_label);

            if is_connected
                && let Some(connected) = state.connections.get(&profile_id)
                && let Some(ref schema) = connected.schema
            {
                let mut profile_children = Vec::new();
                let strategy = connected.connection.schema_loading_strategy();
                let uses_lazy_loading = strategy == SchemaLoadingStrategy::LazyPerDatabase;

                if !schema.databases.is_empty() {
                    for db in &schema.databases {
                        let is_pending = state.is_operation_pending(profile_id, Some(&db.name));
                        let is_active_db = connected.active_database.as_deref() == Some(&db.name);

                        let db_children = if uses_lazy_loading {
                            if let Some(db_schema) = connected.database_schemas.get(&db.name) {
                                Self::build_db_schema_content(
                                    profile_id,
                                    db_schema,
                                    &connected.table_details,
                                )
                            } else if is_pending {
                                vec![TreeItem::new(
                                    format!("loading_{}_{}", profile_id, db.name),
                                    "Loading...".to_string(),
                                )]
                            } else {
                                Vec::new()
                            }
                        } else if db.is_current {
                            Self::build_schema_children(
                                profile_id,
                                schema,
                                &connected.table_details,
                            )
                        } else {
                            Vec::new()
                        };

                        let db_label = if is_pending {
                            format!("{} (loading...)", db.name)
                        } else {
                            db.name.clone()
                        };

                        let is_expanded = if uses_lazy_loading {
                            is_active_db
                        } else {
                            db.is_current
                        };

                        profile_children.push(
                            TreeItem::new(format!("db_{}_{}", profile_id, db.name), db_label)
                                .expanded(is_expanded)
                                .children(db_children),
                        );
                    }
                } else {
                    profile_children = Self::build_schema_children(
                        profile_id,
                        schema,
                        &connected.table_details,
                    );
                }

                profile_item = profile_item.expanded(is_active).children(profile_children);
            }

            items.push(profile_item);
        }

        items
    }

    fn count_visible_entries(items: &[TreeItem]) -> usize {
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

    fn find_item_index(&self, item_id: &str, cx: &Context<Self>) -> Option<usize> {
        let items = self.build_tree_items_with_overrides(cx);
        Self::find_item_index_in_tree(&items, item_id, &mut 0)
    }

    fn find_item_index_in_tree(
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

    fn build_schema_children(
        profile_id: Uuid,
        snapshot: &dbflux_core::SchemaSnapshot,
        table_details: &HashMap<(String, String), TableInfo>,
    ) -> Vec<TreeItem> {
        let mut children = Vec::new();

        for db_schema in &snapshot.schemas {
            let schema_content =
                Self::build_db_schema_content(profile_id, db_schema, table_details);

            children.push(
                TreeItem::new(
                    format!("schema_{}_{}", profile_id, db_schema.name),
                    db_schema.name.clone(),
                )
                .expanded(db_schema.name == "public")
                .children(schema_content),
            );
        }

        children
    }

    fn build_db_schema_content(
        profile_id: Uuid,
        db_schema: &dbflux_core::DbSchemaInfo,
        table_details: &HashMap<(String, String), TableInfo>,
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();

        if !db_schema.tables.is_empty() {
            let table_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|table| {
                    Self::build_table_item(profile_id, &db_schema.name, table, table_details)
                })
                .collect();

            content.push(
                TreeItem::new(
                    format!("tables_{}_{}", profile_id, db_schema.name),
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
                        format!("view_{}__{}__{}", profile_id, db_schema.name, view.name),
                        view.name.clone(),
                    )
                })
                .collect();

            content.push(
                TreeItem::new(
                    format!("views_{}_{}", profile_id, db_schema.name),
                    format!("Views ({})", db_schema.views.len()),
                )
                .expanded(true)
                .children(view_children),
            );
        }

        content
    }

    fn build_table_item(
        profile_id: Uuid,
        schema_name: &str,
        table: &dbflux_core::TableInfo,
        table_details: &HashMap<(String, String), TableInfo>,
    ) -> TreeItem {
        // Check if we have detailed info in the cache (lazy-loaded)
        let cache_key = (schema_name.to_string(), table.name.clone());
        let effective_table = table_details.get(&cache_key).unwrap_or(table);

        let mut table_sections: Vec<TreeItem> = Vec::new();
        let columns_not_loaded = effective_table.columns.is_none();

        // columns: None = not loaded yet, Some([]) = loaded but empty
        if let Some(ref columns) = effective_table.columns
            && !columns.is_empty()
        {
            let column_children: Vec<TreeItem> = columns
                .iter()
                .map(|col| {
                    let pk_marker = if col.is_primary_key { " PK" } else { "" };
                    let nullable = if col.nullable { "?" } else { "" };
                    let label = format!("{}: {}{}{}", col.name, col.type_name, nullable, pk_marker);
                    TreeItem::new(
                        format!("col_{}__{}__{}", profile_id, table.name, col.name),
                        label,
                    )
                })
                .collect();

            table_sections.push(
                TreeItem::new(
                    format!("columns_{}__{}__{}", profile_id, schema_name, table.name),
                    format!("Columns ({})", columns.len()),
                )
                .expanded(true)
                .children(column_children),
            );
        }

        // indexes: None = not loaded yet, Some([]) = loaded but empty
        if let Some(ref indexes) = effective_table.indexes
            && !indexes.is_empty()
        {
            let index_children: Vec<TreeItem> = indexes
                .iter()
                .map(|idx| {
                    let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                    let pk_marker = if idx.is_primary { " PK" } else { "" };
                    let cols = idx.columns.join(", ");
                    let label = format!("{} ({}){}{}", idx.name, cols, unique_marker, pk_marker);
                    TreeItem::new(
                        format!("idx_{}__{}__{}", profile_id, table.name, idx.name),
                        label,
                    )
                })
                .collect();

            table_sections.push(
                TreeItem::new(
                    format!("indexes_{}__{}__{}", profile_id, schema_name, table.name),
                    format!("Indexes ({})", indexes.len()),
                )
                .expanded(false)
                .children(index_children),
            );
        }

        // Add placeholder when columns not loaded yet (shows chevron indicator)
        if columns_not_loaded && table_sections.is_empty() {
            table_sections.push(TreeItem::new(
                format!("placeholder_{}__{}__{}", profile_id, schema_name, table.name),
                "Click to load schema...".to_string(),
            ));
        }

        TreeItem::new(
            format!("table_{}__{}__{}", profile_id, schema_name, table.name),
            table.name.clone(),
        )
        .expanded(false)
        .children(table_sections)
    }

    fn connect_to_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let (params, profile_name) = match self.app_state.update(cx, |state, _cx| {
            if state.is_operation_pending(profile_id, None) {
                return Err("Connection already pending".to_string());
            }

            let result = state.prepare_connect_profile(profile_id);

            if result.is_ok() && !state.start_pending_operation(profile_id, None) {
                return Err("Operation started by another thread".to_string());
            }

            result.map(|p| {
                let name = p.profile.name.clone();
                (p, name)
            })
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Connect skipped: {}", e);
                return;
            }
        };

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result =
                state.start_task(TaskKind::Connect, format!("Connecting to {}", profile_name));
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Connection task was cancelled, discarding result");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, None);
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = match &result {
                    Ok(res) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        Some(PendingToast {
                            message: format!("Connected to {}", res.profile.name),
                            is_error: false,
                        })
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        Some(PendingToast {
                            message: e.clone(),
                            is_error: true,
                        })
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, None);

                    if let Ok(res) = result {
                        state.apply_connect_profile(res.profile, res.connection, res.schema);
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn disconnect_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.disconnect(profile_id);
            log::info!("Disconnected profile {}", profile_id);
            cx.notify();
        });
        self.refresh_tree(cx);
    }

    fn delete_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.profiles.iter().position(|p| p.id == profile_id)
                && let Some(removed) = state.remove_profile(idx)
            {
                log::info!("Deleted profile: {}", removed.name);
            }
            cx.emit(crate::app::AppStateChanged);
        });
    }

    fn edit_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let profile = self
            .app_state
            .read(cx)
            .profiles
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            log::error!("Profile not found: {}", profile_id);
            return;
        };

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(600.0), px(550.0)), cx);

        cx.open_window(
            WindowOptions {
                titlebar: Some(TitlebarOptions {
                    title: Some("Edit Connection".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                ..Default::default()
            },
            |window, cx| {
                let manager = cx.new(|cx| {
                    ConnectionManagerWindow::new_for_edit(app_state, &profile, window, cx)
                });
                cx.new(|cx| Root::new(manager, window, cx))
            },
        )
        .ok();
    }

    pub fn render_menu_panel(
        theme: &gpui_component::Theme,
        items: &[ContextMenuItem],
        selected_index: Option<usize>,
        sidebar: Option<Entity<Self>>,
        panel_id: &str,
        is_parent_menu: bool,
    ) -> impl IntoElement {
        div()
            .min_w_40()
            .bg(theme.popover)
            .border_1()
            .border_color(theme.border)
            .rounded(Radii::MD)
            .shadow_lg()
            .py_1()
            .children(items.iter().enumerate().map(|(idx, item)| {
                let is_selected = selected_index == Some(idx);
                let is_submenu = matches!(item.action, ContextMenuAction::Submenu(_));
                let sidebar_for_click = sidebar.clone();
                let item_id = SharedString::from(format!("{}-item-{}", panel_id, idx));

                div()
                    .id(item_id)
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_4()
                    .px_3()
                    .py(px(6.0))
                    .text_size(FontSizes::SM)
                    .whitespace_nowrap()
                    .cursor_pointer()
                    .when(is_selected, |d| {
                        d.bg(theme.accent).text_color(theme.accent_foreground)
                    })
                    .when(!is_selected, |d| {
                        d.text_color(theme.foreground)
                            .hover(|d| d.bg(theme.list_active))
                    })
                    .when_some(sidebar_for_click, |d, sidebar| {
                        d.on_click(move |_, _, cx| {
                            if is_parent_menu {
                                sidebar
                                    .update(cx, |s, cx| s.context_menu_parent_execute_at(idx, cx));
                            } else {
                                sidebar.update(cx, |s, cx| s.context_menu_execute_at(idx, cx));
                            }
                        })
                    })
                    .child(item.label.clone())
                    .when(is_submenu, |d| {
                        d.child(div().text_color(theme.muted_foreground).child("›"))
                    })
            }))
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let app_state = self.app_state.clone();

        div()
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .id("settings-btn")
                    .w_full()
                    .h(Heights::ROW)
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                    .on_click(move |_, _, cx| {
                        if let Some(handle) = app_state.read(cx).settings_window {
                            if handle
                                .update(cx, |_root, window, _cx| window.activate_window())
                                .is_ok()
                            {
                                return;
                            }
                            app_state.update(cx, |state, _| {
                                state.settings_window = None;
                            });
                        }

                        let app_state_for_window = app_state.clone();
                        if let Ok(handle) = cx.open_window(
                            WindowOptions {
                                titlebar: Some(TitlebarOptions {
                                    title: Some("Settings".into()),
                                    ..Default::default()
                                }),
                                window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                                    None,
                                    size(px(950.0), px(700.0)),
                                    cx,
                                ))),
                                kind: WindowKind::Floating,
                                focus: true,
                                ..Default::default()
                            },
                            |window, cx| {
                                let settings = cx.new(|cx| {
                                    SettingsWindow::new(app_state_for_window, window, cx)
                                });
                                cx.new(|cx| Root::new(settings, window, cx))
                            },
                        ) {
                            app_state.update(cx, |state, _| {
                                state.settings_window = Some(handle);
                            });
                        }
                    })
                    .child(
                        Icon::new(IconName::Settings)
                            .size(px(14.0))
                            .text_color(theme.muted_foreground),
                    )
                    .child("Settings"),
            )
    }
}

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some((profile_id, table_name)) = self.pending_view_table.take() {
            self.results.update(cx, |results, cx| {
                results.view_table_for_connection(profile_id, &table_name, window, cx);
            });
        }

        if let Some(toast) = self.pending_toast.take() {
            use crate::ui::toast::ToastExt;
            if toast.is_error {
                cx.toast_error(toast.message, window);
            } else {
                cx.toast_success(toast.message, window);
            }
        }

        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id;
        let connections = state.connections.keys().copied().collect::<Vec<_>>();
        let active_databases = self.active_databases.clone();
        let sidebar_entity = cx.entity().clone();

        let color_teal: Hsla = gpui::rgb(0x4EC9B0).into();
        let color_yellow: Hsla = gpui::rgb(0xDCDCAA).into();
        let color_blue: Hsla = gpui::rgb(0x9CDCFE).into();
        let color_purple: Hsla = gpui::rgb(0xC586C0).into();
        let color_gray: Hsla = gpui::rgb(0x808080).into();
        let color_orange: Hsla = gpui::rgb(0xCE9178).into();
        let color_schema: Hsla = gpui::rgb(0x569CD6).into();
        let color_green: Hsla = gpui::green();

        div()
            .flex()
            .flex_col()
            .size_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::SM)
                    .h(Heights::TOOLBAR)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_weight(if self.connections_focused {
                                FontWeight::BOLD
                            } else {
                                FontWeight::SEMIBOLD
                            })
                            .text_color(if self.connections_focused {
                                theme.primary
                            } else {
                                theme.muted_foreground
                            })
                            .child("CONNECTIONS"),
                    )
                    .child(
                        div()
                            .id("add-connection")
                            .w(Heights::ICON_LG)
                            .h(Heights::ICON_LG)
                            .flex()
                            .items_center()
                            .justify_center()
                            .rounded(Radii::SM)
                            .text_size(FontSizes::LG)
                            .text_color(theme.muted_foreground)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                            .on_click(move |_, _, cx| {
                                let app_state = app_state.clone();
                                cx.open_window(
                                    WindowOptions {
                                        titlebar: Some(TitlebarOptions {
                                            title: Some("Connection Manager".into()),
                                            ..Default::default()
                                        }),
                                        window_bounds: Some(WindowBounds::Windowed(
                                            Bounds::centered(None, size(px(600.0), px(550.0)), cx),
                                        )),
                                        kind: WindowKind::Floating,
                                        ..Default::default()
                                    },
                                    |window, cx| {
                                        let manager = cx.new(|cx| {
                                            ConnectionManagerWindow::new(app_state, window, cx)
                                        });
                                        cx.new(|cx| Root::new(manager, window, cx))
                                    },
                                )
                                .ok();
                            })
                            .child("+"),
                    ),
            )
            .child(div().flex_1().overflow_hidden().child(tree(
                &self.tree_state,
                move |ix, entry, selected, _window, cx| {
                    let item = entry.item();
                    let item_id = item.id.clone();
                    let depth = entry.depth();

                    let node_kind = TreeNodeKind::from_id(&item_id);

                    let is_connected = if node_kind == TreeNodeKind::Profile {
                        item_id
                            .strip_prefix("profile_")
                            .and_then(|id_str| Uuid::parse_str(id_str).ok())
                            .is_some_and(|id| connections.contains(&id))
                    } else {
                        false
                    };

                    let is_active = if node_kind == TreeNodeKind::Profile {
                        item_id
                            .strip_prefix("profile_")
                            .and_then(|id_str| Uuid::parse_str(id_str).ok())
                            .is_some_and(|id| active_id == Some(id))
                    } else {
                        false
                    };

                    // Check if this database is the active one for its connection
                    let is_active_database = if node_kind == TreeNodeKind::Database {
                        item_id
                            .strip_prefix("db_")
                            .and_then(|rest| {
                                // Format: db_{profile_id}_{db_name}
                                let underscore_pos = rest.find('_')?;
                                let profile_id_str = &rest[..underscore_pos];
                                let db_name = &rest[underscore_pos + 1..];
                                let profile_id = Uuid::parse_str(profile_id_str).ok()?;
                                active_databases
                                    .get(&profile_id)
                                    .map(|active_db| active_db == db_name)
                            })
                            .unwrap_or(false)
                    } else {
                        false
                    };

                    let theme = cx.theme();
                    let indent = px(depth as f32 * 16.0);
                    let is_folder = entry.is_folder();
                    let is_expanded = entry.is_expanded();

                    let needs_chevron = is_folder
                        && matches!(
                            node_kind,
                            TreeNodeKind::Table
                                | TreeNodeKind::View
                                | TreeNodeKind::Schema
                                | TreeNodeKind::TablesFolder
                                | TreeNodeKind::ViewsFolder
                                | TreeNodeKind::ColumnsFolder
                                | TreeNodeKind::IndexesFolder
                                | TreeNodeKind::Database
                                | TreeNodeKind::Profile
                        );
                    let chevron: Option<&str> = if needs_chevron {
                        Some(if is_expanded { "▾" } else { "▸" })
                    } else {
                        None
                    };

                    let (icon, icon_color): (&str, Hsla) = match node_kind {
                        TreeNodeKind::Profile if is_connected => ("●", color_green),
                        TreeNodeKind::Profile => ("○", theme.muted_foreground),
                        TreeNodeKind::Database => ("⬡", color_orange),
                        TreeNodeKind::Schema => ("▣", color_schema),
                        TreeNodeKind::TablesFolder => ("▤", color_teal),
                        TreeNodeKind::ViewsFolder => ("◫", color_yellow),
                        TreeNodeKind::Table => ("▦", color_teal),
                        TreeNodeKind::View => ("◧", color_yellow),
                        TreeNodeKind::ColumnsFolder => ("◈", color_blue),
                        TreeNodeKind::IndexesFolder => ("◇", color_purple),
                        TreeNodeKind::Column => ("•", color_blue),
                        TreeNodeKind::Index => ("◆", color_purple),
                        TreeNodeKind::Unknown => ("", theme.muted_foreground),
                    };

                    let label_color: Hsla = match node_kind {
                        TreeNodeKind::Profile => theme.foreground,
                        TreeNodeKind::Database => color_orange,
                        TreeNodeKind::Schema => color_schema,
                        TreeNodeKind::TablesFolder
                        | TreeNodeKind::ViewsFolder
                        | TreeNodeKind::ColumnsFolder
                        | TreeNodeKind::IndexesFolder => color_gray,
                        TreeNodeKind::Table => color_teal,
                        TreeNodeKind::View => color_yellow,
                        TreeNodeKind::Column => color_blue,
                        TreeNodeKind::Index => color_purple,
                        TreeNodeKind::Unknown => theme.muted_foreground,
                    };

                    let is_table_or_view =
                        matches!(node_kind, TreeNodeKind::Table | TreeNodeKind::View);

                    let sidebar_for_mousedown = sidebar_entity.clone();
                    let item_id_for_mousedown = item_id.clone();
                    let sidebar_for_click = sidebar_entity.clone();
                    let item_id_for_click = item_id.clone();
                    let sidebar_for_chevron = sidebar_entity.clone();
                    let item_id_for_chevron = item_id.clone();

                    let mut list_item = ListItem::new(ix).selected(selected).py(Spacing::XS).child(
                        div()
                            .id(SharedString::from(format!("row-{}", item_id)))
                            .w_full()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .pl(indent)
                            .when(is_table_or_view, |el| {
                                let sidebar_md = sidebar_for_mousedown.clone();
                                let id_md = item_id_for_mousedown.clone();
                                let sidebar_cl = sidebar_for_click.clone();
                                let id_cl = item_id_for_click.clone();
                                el.on_mouse_down(MouseButton::Left, move |_, _, cx| {
                                    cx.stop_propagation();
                                    sidebar_md.update(cx, |this, cx| {
                                        if let Some(idx) = this.find_item_index(&id_md, cx) {
                                            this.tree_state.update(cx, |state, cx| {
                                                state.set_selected_index(Some(idx), cx);
                                            });
                                        }
                                        cx.emit(SidebarEvent::RequestFocus);
                                        cx.notify();
                                    });
                                })
                                .on_click(
                                    move |event, _window, cx| {
                                        if event.click_count() == 2 {
                                            sidebar_cl.update(cx, |this, cx| {
                                                this.browse_table(&id_cl, cx);
                                            });
                                        }
                                    },
                                )
                            })
                            .child(
                                div()
                                    .id(SharedString::from(format!("chevron-{}", item_id)))
                                    .w(px(12.0))
                                    .flex()
                                    .justify_center()
                                    .text_color(theme.muted_foreground)
                                    .when_some(chevron, |el, ch| {
                                        el.text_size(FontSizes::XS)
                                            .cursor_pointer()
                                            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                                cx.stop_propagation();
                                            })
                                            .on_click(move |_, _, cx| {
                                                cx.stop_propagation();
                                                sidebar_for_chevron.update(cx, |this, cx| {
                                                    this.toggle_item_expansion(
                                                        &item_id_for_chevron,
                                                        cx,
                                                    );
                                                });
                                            })
                                            .child(ch)
                                    }),
                            )
                            .child(
                                div()
                                    .w(Heights::ICON_SM)
                                    .flex()
                                    .justify_center()
                                    .text_size(FontSizes::SM)
                                    .text_color(icon_color)
                                    .child(icon),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .text_size(FontSizes::SM)
                                    .text_color(label_color)
                                    .when(node_kind == TreeNodeKind::Profile && is_active, |d| {
                                        d.font_weight(FontWeight::SEMIBOLD)
                                    })
                                    .when(is_active_database, |d| {
                                        d.font_weight(FontWeight::SEMIBOLD)
                                    })
                                    .when(
                                        matches!(
                                            node_kind,
                                            TreeNodeKind::TablesFolder
                                                | TreeNodeKind::ViewsFolder
                                                | TreeNodeKind::ColumnsFolder
                                                | TreeNodeKind::IndexesFolder
                                        ),
                                        |d| d.font_weight(FontWeight::MEDIUM),
                                    )
                                    .child(item.label.clone()),
                            ),
                    );

                    if node_kind.shows_pointer_cursor() {
                        list_item = list_item.cursor(CursorStyle::PointingHand);
                    }

                    if !is_table_or_view && node_kind.needs_click_handler() {
                        let item_id_for_click = item_id.clone();
                        let sidebar = sidebar_entity.clone();
                        list_item = list_item.on_click(move |event, _window, cx| {
                            cx.stop_propagation();
                            let click_count = event.click_count();
                            sidebar.update(cx, |this, cx| {
                                this.handle_item_click(&item_id_for_click, click_count, cx);
                            });
                        });
                    }

                    // Handle expansion for folder items that don't have special click handlers
                    // (Schema, TablesFolder, ViewsFolder, ColumnsFolder, IndexesFolder)
                    // This ensures our expansion_overrides stays in sync when users click to expand/collapse
                    let is_other_folder = is_folder
                        && matches!(
                            node_kind,
                            TreeNodeKind::Schema
                                | TreeNodeKind::TablesFolder
                                | TreeNodeKind::ViewsFolder
                                | TreeNodeKind::ColumnsFolder
                                | TreeNodeKind::IndexesFolder
                        );
                    if is_other_folder {
                        let item_id_for_folder = item_id.clone();
                        let sidebar_for_folder = sidebar_entity.clone();
                        list_item = list_item.on_click(move |_, _window, cx| {
                            cx.stop_propagation();
                            sidebar_for_folder.update(cx, |this, cx| {
                                this.toggle_item_expansion(&item_id_for_folder, cx);
                            });
                        });
                    }

                    if node_kind == TreeNodeKind::Profile {
                        let sidebar_for_menu = sidebar_entity.clone();
                        let item_id_for_menu = item_id.clone();
                        let hover_bg = theme.secondary;

                        list_item = list_item.suffix(move |_window, _cx| {
                            let sidebar = sidebar_for_menu.clone();
                            let item_id = item_id_for_menu.clone();

                            div()
                                .id(SharedString::from(format!("menu-btn-{}", item_id)))
                                .px_1()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(move |d| d.bg(hover_bg))
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .on_click(move |event, _, cx| {
                                    cx.stop_propagation();
                                    let position = event.position();
                                    sidebar.update(cx, |this, cx| {
                                        cx.emit(SidebarEvent::RequestFocus);
                                        this.open_menu_for_item(&item_id, position, cx);
                                    });
                                })
                                .child("⋯")
                        });
                    }

                    // Table/View menu
                    if matches!(node_kind, TreeNodeKind::Table | TreeNodeKind::View) {
                        let item_id_for_menu = item_id.clone();
                        let sidebar_for_menu = sidebar_entity.clone();
                        let hover_bg = theme.secondary;

                        list_item = list_item.suffix(move |_window, _cx| {
                            let sidebar = sidebar_for_menu.clone();
                            let item_id = item_id_for_menu.clone();

                            div()
                                .id(SharedString::from(format!("menu-btn-{}", item_id)))
                                .px_1()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(move |d| d.bg(hover_bg))
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .on_click(move |event, _, cx| {
                                    cx.stop_propagation();
                                    let position = event.position();
                                    sidebar.update(cx, |this, cx| {
                                        cx.emit(SidebarEvent::RequestFocus);
                                        this.open_menu_for_item(&item_id, position, cx);
                                    });
                                })
                                .child("⋯")
                        });
                    }

                    // Database menu (only show if not current database)
                    if node_kind == TreeNodeKind::Database && !is_active_database {
                        let item_id_for_menu = item_id.clone();
                        let sidebar_for_menu = sidebar_entity.clone();
                        let hover_bg = theme.secondary;

                        list_item = list_item.suffix(move |_window, _cx| {
                            let sidebar = sidebar_for_menu.clone();
                            let item_id = item_id_for_menu.clone();

                            div()
                                .id(SharedString::from(format!("menu-btn-{}", item_id)))
                                .px_1()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(move |d| d.bg(hover_bg))
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .on_click(move |event, _, cx| {
                                    cx.stop_propagation();
                                    let position = event.position();
                                    sidebar.update(cx, |this, cx| {
                                        cx.emit(SidebarEvent::RequestFocus);
                                        this.open_menu_for_item(&item_id, position, cx);
                                    });
                                })
                                .child("⋯")
                        });
                    }

                    list_item
                },
            )))
            .child(self.render_footer(cx))
    }
}

#[cfg(test)]
mod tests {
    use super::Sidebar;
    use uuid::Uuid;

    #[test]
    fn parse_table_id_valid() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("table_{uuid}__public__users");
        let parts = Sidebar::parse_table_or_view_id(&item_id).unwrap();
        assert_eq!(parts.profile_id, uuid);
        assert_eq!(parts.schema_name, "public");
        assert_eq!(parts.object_name, "users");
    }

    #[test]
    fn parse_view_id_valid() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("view_{uuid}__analytics__monthly_stats");
        let parts = Sidebar::parse_table_or_view_id(&item_id).unwrap();
        assert_eq!(parts.profile_id, uuid);
        assert_eq!(parts.schema_name, "analytics");
        assert_eq!(parts.object_name, "monthly_stats");
    }

    #[test]
    fn parse_table_id_with_underscores_in_table_name() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("table_{uuid}__public__user_accounts_archive");
        let parts = Sidebar::parse_table_or_view_id(&item_id).unwrap();
        assert_eq!(parts.schema_name, "public");
        assert_eq!(parts.object_name, "user_accounts_archive");
    }

    #[test]
    fn parse_table_id_with_double_underscore_in_table_name() {
        // Ambiguous: rsplit gives __ to schema, not table
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("table_{uuid}__public__user__accounts");
        let parts = Sidebar::parse_table_or_view_id(&item_id).unwrap();
        assert_eq!(parts.schema_name, "public__user");
        assert_eq!(parts.object_name, "accounts");
    }

    #[test]
    fn parse_table_id_with_double_underscore_only_in_schema() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("table_{uuid}__my__schema__users");
        let parts = Sidebar::parse_table_or_view_id(&item_id).unwrap();
        assert_eq!(parts.schema_name, "my__schema");
        assert_eq!(parts.object_name, "users");
    }

    #[test]
    fn parse_invalid_prefix() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("schema_{uuid}__public__users");
        assert!(Sidebar::parse_table_or_view_id(&item_id).is_none());
    }

    #[test]
    fn parse_invalid_uuid() {
        let item_id = "table_not-a-valid-uuid-at-all-here__public__users";
        assert!(Sidebar::parse_table_or_view_id(item_id).is_none());
    }

    #[test]
    fn parse_missing_schema() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("table_{uuid}____users");
        assert!(Sidebar::parse_table_or_view_id(&item_id).is_none());
    }

    #[test]
    fn parse_missing_name() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let item_id = format!("table_{uuid}__public__");
        assert!(Sidebar::parse_table_or_view_id(&item_id).is_none());
    }

    #[test]
    fn parse_too_short() {
        let item_id = "table_abc__public__users";
        assert!(Sidebar::parse_table_or_view_id(item_id).is_none());
    }
}
